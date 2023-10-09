//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/StickersManager.h"

#include "td/telegram/AccessRights.h"
#include "td/telegram/AuthManager.h"
#include "td/telegram/ConfigManager.h"
#include "td/telegram/ConfigShared.h"
#include "td/telegram/ContactsManager.h"
#include "td/telegram/DialogId.h"
#include "td/telegram/Document.h"
#include "td/telegram/DocumentsManager.h"
#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"

#include "td/telegram/logevent/LogEvent.h"

#include "td/telegram/misc.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/net/MtprotoHeader.h"
#include "td/telegram/net/NetQueryDispatcher.h"
#include "td/telegram/PhotoSizeSource.h"
#include "td/telegram/secret_api.h"
#include "td/telegram/StickerSetId.hpp"
#include "td/telegram/StickersManager.hpp"
#include "td/telegram/Td.h"
#include "td/telegram/td_api.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.h"

#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/MultiPromise.h"
#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/base64.h"
#include "td/utils/emoji.h"
#include "td/utils/format.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"
#include "td/utils/MimeType.h"
#include "td/utils/misc.h"
#include "td/utils/PathView.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StackAllocator.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <type_traits>
#include <unordered_set>

namespace td {
}  // namespace td
namespace td {

void StickersManager::on_get_installed_sticker_sets(bool is_masks,
                                                    tl_object_ptr<telegram_api::messages_AllStickers> &&stickers_ptr) {
  next_installed_sticker_sets_load_time_[is_masks] = Time::now_cached() + Random::fast(30 * 60, 50 * 60);

  CHECK(stickers_ptr != nullptr);
  int32 constructor_id = stickers_ptr->get_id();
  if (constructor_id == telegram_api::messages_allStickersNotModified::ID) {
    LOG(INFO) << (is_masks ? "Masks" : "Stickers") << " are not modified";
    return;
  }
  CHECK(constructor_id == telegram_api::messages_allStickers::ID);
  auto stickers = move_tl_object_as<telegram_api::messages_allStickers>(stickers_ptr);

  std::unordered_set<StickerSetId, StickerSetIdHash> uninstalled_sticker_sets(
      installed_sticker_set_ids_[is_masks].begin(), installed_sticker_set_ids_[is_masks].end());

  vector<StickerSetId> sets_to_load;
  vector<StickerSetId> installed_sticker_set_ids;
  vector<int32> debug_hashes;
  vector<int64> debug_sticker_set_ids;
  std::reverse(stickers->sets_.begin(), stickers->sets_.end());  // apply installed sticker sets in reverse order
  for (auto &set : stickers->sets_) {
    debug_hashes.push_back(set->hash_);
    debug_sticker_set_ids.push_back(set->id_);
    StickerSetId set_id = on_get_sticker_set(std::move(set), false, "on_get_installed_sticker_sets");
    if (!set_id.is_valid()) {
      continue;
    }

    auto sticker_set = get_sticker_set(set_id);
    CHECK(sticker_set != nullptr);
    LOG_IF(ERROR, !sticker_set->is_installed) << "Receive non-installed sticker set in getAllStickers";
    LOG_IF(ERROR, sticker_set->is_archived) << "Receive archived sticker set in getAllStickers";
    LOG_IF(ERROR, sticker_set->is_masks != is_masks) << "Receive sticker set of a wrong type in getAllStickers";
    CHECK(sticker_set->is_inited);

    if (sticker_set->is_installed && !sticker_set->is_archived && sticker_set->is_masks == is_masks) {
      installed_sticker_set_ids.push_back(set_id);
      uninstalled_sticker_sets.erase(set_id);
    }
    update_sticker_set(sticker_set, "on_get_installed_sticker_sets");

    if (!sticker_set->is_archived && !sticker_set->is_loaded) {
      sets_to_load.push_back(set_id);
    }
  }
  std::reverse(debug_hashes.begin(), debug_hashes.end());
  std::reverse(installed_sticker_set_ids.begin(), installed_sticker_set_ids.end());
  std::reverse(debug_sticker_set_ids.begin(), debug_sticker_set_ids.end());

  if (!sets_to_load.empty()) {
    load_sticker_sets(std::move(sets_to_load), Auto());
  }

  for (auto set_id : uninstalled_sticker_sets) {
    auto sticker_set = get_sticker_set(set_id);
    CHECK(sticker_set != nullptr);
    CHECK(sticker_set->is_installed && !sticker_set->is_archived);
    on_update_sticker_set(sticker_set, false, false, true);
    update_sticker_set(sticker_set, "on_get_installed_sticker_sets 2");
  }

  on_load_installed_sticker_sets_finished(is_masks, std::move(installed_sticker_set_ids));

  if (installed_sticker_sets_hash_[is_masks] != stickers->hash_) {
    LOG(ERROR) << "Sticker sets hash mismatch: server hash list = " << format::as_array(debug_hashes)
               << ", client hash list = "
               << format::as_array(
                      transform(installed_sticker_set_ids_[is_masks],
                                [this](StickerSetId sticker_set_id) { return get_sticker_set(sticker_set_id)->hash; }))
               << ", server sticker set list = " << format::as_array(debug_sticker_set_ids)
               << ", client sticker set list = " << format::as_array(installed_sticker_set_ids_[is_masks])
               << ", server hash = " << stickers->hash_ << ", client hash = " << installed_sticker_sets_hash_[is_masks];
  }
}

class SaveRecentStickerQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;
  string file_reference_;
  bool unsave_ = false;
  bool is_attached_;

 public:
  explicit SaveRecentStickerQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(bool is_attached, FileId file_id, tl_object_ptr<telegram_api::inputDocument> &&input_document,
            bool unsave) {
    CHECK(input_document != nullptr);
    CHECK(file_id.is_valid());
    file_id_ = file_id;
    file_reference_ = input_document->file_reference_.as_slice().str();
    unsave_ = unsave;
    is_attached_ = is_attached;

    int32 flags = 0;
    if (is_attached) {
      flags |= telegram_api::messages_saveRecentSticker::ATTACHED_MASK;
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_saveRecentSticker(flags, is_attached /*ignored*/, std::move(input_document), unsave)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_saveRecentSticker>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for save recent " << (is_attached_ ? "attached " : "") << "sticker: " << result;
    if (!result) {
      td_->stickers_manager_->reload_recent_stickers(is_attached_, true);
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      VLOG(file_references) << "Receive " << status << " for " << file_id_;
      td_->file_manager_->delete_file_reference(file_id_, file_reference_);
      td_->file_reference_manager_->repair_file_reference(
          file_id_, PromiseCreator::lambda([sticker_id = file_id_, is_attached = is_attached_, unsave = unsave_,
                                            promise = std::move(promise_)](Result<Unit> result) mutable {
            if (result.is_error()) {
              return promise.set_error(Status::Error(400, "Failed to find the sticker"));
            }

            send_closure(G()->stickers_manager(), &StickersManager::send_save_recent_sticker_query, is_attached,
                         sticker_id, unsave, std::move(promise));
          }));
      return;
    }

    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for save recent " << (is_attached_ ? "attached " : "") << "sticker: " << status;
    }
    td_->stickers_manager_->reload_recent_stickers(is_attached_, true);
    promise_.set_error(std::move(status));
  }
};

void StickersManager::send_save_recent_sticker_query(bool is_attached, FileId sticker_id, bool unsave,
                                                     Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  // TODO invokeAfter and log event
  auto file_view = td_->file_manager_->get_file_view(sticker_id);
  CHECK(file_view.has_remote_location());
  CHECK(file_view.remote_location().is_document());
  CHECK(!file_view.remote_location().is_web());
  td_->create_handler<SaveRecentStickerQuery>(std::move(promise))
      ->send(is_attached, sticker_id, file_view.remote_location().as_input_document(), unsave);
}

void StickersManager::add_recent_sticker_impl(bool is_attached, FileId sticker_id, bool add_on_server,
                                              Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());

  LOG(INFO) << "Add recent " << (is_attached ? "attached " : "") << "sticker " << sticker_id;
  if (!are_recent_stickers_loaded_[is_attached]) {
    load_recent_stickers(is_attached, PromiseCreator::lambda([is_attached, sticker_id, add_on_server,
                                                              promise = std::move(promise)](Result<> result) mutable {
                           if (result.is_ok()) {
                             send_closure(G()->stickers_manager(), &StickersManager::add_recent_sticker_impl,
                                          is_attached, sticker_id, add_on_server, std::move(promise));
                           } else {
                             promise.set_error(result.move_as_error());
                           }
                         }));
    return;
  }

  auto is_equal = [sticker_id](FileId file_id) {
    return file_id == sticker_id || (file_id.get_remote() == sticker_id.get_remote() && sticker_id.get_remote() != 0);
  };

  vector<FileId> &sticker_ids = recent_sticker_ids_[is_attached];
  if (!sticker_ids.empty() && is_equal(sticker_ids[0])) {
    if (sticker_ids[0].get_remote() == 0 && sticker_id.get_remote() != 0) {
      sticker_ids[0] = sticker_id;
      save_recent_stickers_to_database(is_attached);
    }

    return promise.set_value(Unit());
  }

  auto sticker = get_sticker(sticker_id);
  if (sticker == nullptr) {
    return promise.set_error(Status::Error(400, "Sticker not found"));
  }
  if (!sticker->set_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Stickers without sticker set can't be added to recent"));
  }

  auto file_view = td_->file_manager_->get_file_view(sticker_id);
  if (!file_view.has_remote_location()) {
    return promise.set_error(Status::Error(400, "Can save only sent stickers"));
  }
  if (file_view.remote_location().is_web()) {
    return promise.set_error(Status::Error(400, "Can't save web stickers"));
  }
  if (!file_view.remote_location().is_document()) {
    return promise.set_error(Status::Error(400, "Can't save encrypted stickers"));
  }

  auto it = std::find_if(sticker_ids.begin(), sticker_ids.end(), is_equal);
  if (it == sticker_ids.end()) {
    if (static_cast<int32>(sticker_ids.size()) == recent_stickers_limit_) {
      sticker_ids.back() = sticker_id;
    } else {
      sticker_ids.push_back(sticker_id);
    }
    it = sticker_ids.end() - 1;
  }
  std::rotate(sticker_ids.begin(), it, it + 1);
  if (sticker_ids[0].get_remote() == 0 && sticker_id.get_remote() != 0) {
    sticker_ids[0] = sticker_id;
  }

  send_update_recent_stickers(is_attached);
  if (add_on_server) {
    send_save_recent_sticker_query(is_attached, sticker_id, false, std::move(promise));
  }
}

void StickersManager::init() {
  if (!td_->auth_manager_->is_authorized() || td_->auth_manager_->is_bot() || G()->close_flag()) {
    return;
  }
  LOG(INFO) << "Init StickersManager";
  is_inited_ = true;

  {
    // add animated emoji sticker set
    auto &sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_emoji());
    if (G()->is_test_dc()) {
      init_special_sticker_set(sticker_set, 1258816259751954, 4879754868529595811, "emojies");
    } else {
      init_special_sticker_set(sticker_set, 1258816259751983, 5100237018658464041, "AnimatedEmojies");
    }
    load_special_sticker_set_info_from_binlog(sticker_set);
  }
  if (!G()->is_test_dc()) {
    // add animated emoji click sticker set
    auto &sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_emoji_click());
    load_special_sticker_set_info_from_binlog(sticker_set);
  }

  dice_emojis_str_ =
      G()->shared_config().get_option_string("dice_emojis", "🎲\x01🎯\x01🏀\x01⚽\x01⚽️\x01🎰\x01🎳");
  dice_emojis_ = full_split(dice_emojis_str_, '\x01');
  for (auto &dice_emoji : dice_emojis_) {
    auto &animated_dice_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_dice(dice_emoji));
    load_special_sticker_set_info_from_binlog(animated_dice_sticker_set);
  }
  send_closure(G()->td(), &Td::send_update, get_update_dice_emojis_object());

  on_update_dice_success_values();

  on_update_emoji_sounds();

  on_update_disable_animated_emojis();
  if (!disable_animated_emojis_) {
    load_special_sticker_set(add_special_sticker_set(SpecialStickerSetType::animated_emoji()));
  }

  if (G()->parameters().use_file_db) {
    auto old_featured_sticker_set_count_str = G()->td_db()->get_binlog_pmc()->get("old_featured_sticker_set_count");
    if (!old_featured_sticker_set_count_str.empty()) {
      old_featured_sticker_set_count_ = to_integer<int32>(old_featured_sticker_set_count_str);
    }
    if (!G()->td_db()->get_binlog_pmc()->get("invalidate_old_featured_sticker_sets").empty()) {
      invalidate_old_featured_sticker_sets();
    }
  } else {
    G()->td_db()->get_binlog_pmc()->erase("old_featured_sticker_set_count");
    G()->td_db()->get_binlog_pmc()->erase("invalidate_old_featured_sticker_sets");
  }

  G()->td_db()->get_binlog_pmc()->erase("animated_dice_sticker_set");        // legacy
  G()->shared_config().set_option_empty("animated_dice_sticker_set_name");   // legacy
  G()->shared_config().set_option_empty("animated_emoji_sticker_set_name");  // legacy
}

class ReloadSpecialStickerSetQuery final : public Td::ResultHandler {
  StickerSetId sticker_set_id_;
  SpecialStickerSetType type_;

 public:
  void send(StickerSetId sticker_set_id, SpecialStickerSetType type, int32 hash) {
    sticker_set_id_ = sticker_set_id;
    type_ = std::move(type);
    send_query(
        G()->net_query_creator().create(telegram_api::messages_getStickerSet(type_.get_input_sticker_set(), hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getStickerSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto set_ptr = result_ptr.move_as_ok();
    if (set_ptr->get_id() == telegram_api::messages_stickerSet::ID) {
      // sticker_set_id_ needs to be replaced always
      sticker_set_id_ = td_->stickers_manager_->on_get_messages_sticker_set(StickerSetId(), std::move(set_ptr), true,
                                                                            "ReloadSpecialStickerSetQuery");
    } else if (sticker_set_id_.is_valid()) {
      td_->stickers_manager_->on_get_messages_sticker_set(sticker_set_id_, std::move(set_ptr), false,
                                                          "ReloadSpecialStickerSetQuery");
    }
    if (sticker_set_id_.is_valid()) {
      td_->stickers_manager_->on_get_special_sticker_set(type_, sticker_set_id_);
    } else {
      on_error(Status::Error(500, "Failed to add special sticker set"));
    }
  }

  void on_error(Status status) final {
    LOG(WARNING) << "Receive error for ReloadSpecialStickerSetQuery: " << status;
    td_->stickers_manager_->on_load_special_sticker_set(type_, std::move(status));
  }
};

void StickersManager::reload_special_sticker_set(SpecialStickerSet &sticker_set, int32 hash) {
  if (sticker_set.is_being_reloaded_) {
    return;
  }
  sticker_set.is_being_reloaded_ = true;
  td_->create_handler<ReloadSpecialStickerSetQuery>()->send(sticker_set.id_, sticker_set.type_, hash);
}

void StickersManager::add_sticker_to_set(UserId user_id, string &short_name,
                                         tl_object_ptr<td_api::InputSticker> &&sticker, Promise<Unit> &&promise) {
  TRY_RESULT_PROMISE(promise, input_user, td_->contacts_manager_->get_input_user(user_id));

  short_name = strip_empty_characters(short_name, MAX_STICKER_SET_SHORT_NAME_LENGTH);
  if (short_name.empty()) {
    return promise.set_error(Status::Error(400, "Sticker set name can't be empty"));
  }

  auto r_file_id = prepare_input_sticker(sticker.get());
  if (r_file_id.is_error()) {
    return promise.set_error(r_file_id.move_as_error());
  }
  auto file_id = std::get<0>(r_file_id.ok());
  auto is_url = std::get<1>(r_file_id.ok());
  auto is_local = std::get<2>(r_file_id.ok());

  auto pending_add_sticker_to_set = make_unique<PendingAddStickerToSet>();
  pending_add_sticker_to_set->short_name = short_name;
  pending_add_sticker_to_set->file_id = file_id;
  pending_add_sticker_to_set->sticker = std::move(sticker);
  pending_add_sticker_to_set->promise = std::move(promise);

  int64 random_id;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || pending_add_sticker_to_sets_.find(random_id) != pending_add_sticker_to_sets_.end());
  pending_add_sticker_to_sets_[random_id] = std::move(pending_add_sticker_to_set);

  auto on_upload_promise = PromiseCreator::lambda([random_id](Result<Unit> result) {
    send_closure(G()->stickers_manager(), &StickersManager::on_added_sticker_uploaded, random_id, std::move(result));
  });

  if (is_url) {
    do_upload_sticker_file(user_id, file_id, nullptr, std::move(on_upload_promise));
  } else if (is_local) {
    upload_sticker_file(user_id, file_id, std::move(on_upload_promise));
  } else {
    on_upload_promise.set_value(Unit());
  }
}

void StickersManager::on_uploaded_sticker_file(FileId file_id, tl_object_ptr<telegram_api::MessageMedia> media,
                                               Promise<Unit> &&promise) {
  CHECK(media != nullptr);
  LOG(INFO) << "Receive uploaded sticker file " << to_string(media);
  if (media->get_id() != telegram_api::messageMediaDocument::ID) {
    return promise.set_error(Status::Error(400, "Can't upload sticker file: wrong file type"));
  }

  auto message_document = move_tl_object_as<telegram_api::messageMediaDocument>(media);
  auto document_ptr = std::move(message_document->document_);
  int32 document_id = document_ptr->get_id();
  if (document_id == telegram_api::documentEmpty::ID) {
    return promise.set_error(Status::Error(400, "Can't upload sticker file: empty file"));
  }
  CHECK(document_id == telegram_api::document::ID);

  FileView file_view = td_->file_manager_->get_file_view(file_id);
  bool is_animated = file_view.get_type() == FileType::Sticker;
  auto expected_document_type = is_animated ? Document::Type::Sticker : Document::Type::General;

  auto parsed_document = td_->documents_manager_->on_get_document(
      move_tl_object_as<telegram_api::document>(document_ptr), DialogId(), nullptr);
  if (parsed_document.type != expected_document_type) {
    return promise.set_error(Status::Error(400, "Wrong file type"));
  }

  if (parsed_document.file_id != file_id) {
    if (is_animated) {
      merge_stickers(parsed_document.file_id, file_id, false);
    } else {
      // must not delete the old document, because the file_id could be used for simultaneous URL uploads
      td_->documents_manager_->merge_documents(parsed_document.file_id, file_id, false);
    }
  }
  promise.set_value(Unit());
}

class GetEmojiKeywordsLanguageQuery final : public Td::ResultHandler {
  Promise<vector<string>> promise_;

 public:
  explicit GetEmojiKeywordsLanguageQuery(Promise<vector<string>> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<string> &&language_codes) {
    send_query(
        G()->net_query_creator().create(telegram_api::messages_getEmojiKeywordsLanguages(std::move(language_codes))));
  }
  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getEmojiKeywordsLanguages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result =
        transform(result_ptr.move_as_ok(), [](auto &&emoji_language) { return std::move(emoji_language->lang_code_); });
    promise_.set_value(std::move(result));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void StickersManager::load_language_codes(vector<string> language_codes, string key, Promise<Unit> &&promise) {
  auto &promises = load_language_codes_queries_[key];
  promises.push_back(std::move(promise));
  if (promises.size() != 1) {
    // query has already been sent, just wait for the result
    return;
  }

  auto query_promise =
      PromiseCreator::lambda([actor_id = actor_id(this), key = std::move(key)](Result<vector<string>> &&result) {
        send_closure(actor_id, &StickersManager::on_get_language_codes, key, std::move(result));
      });
  td_->create_handler<GetEmojiKeywordsLanguageQuery>(std::move(query_promise))->send(std::move(language_codes));
}

vector<string> StickersManager::get_sticker_emojis(const tl_object_ptr<td_api::InputFile> &input_file,
                                                   Promise<Unit> &&promise) {
  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Sticker, input_file, DialogId(), false, false);
  if (r_file_id.is_error()) {
    promise.set_error(Status::Error(400, r_file_id.error().message()));  // TODO do not drop error code
    return {};
  }

  FileId file_id = r_file_id.ok();

  auto sticker = get_sticker(file_id);
  if (sticker == nullptr) {
    promise.set_value(Unit());
    return {};
  }
  if (!sticker->set_id.is_valid()) {
    promise.set_value(Unit());
    return {};
  }

  auto file_view = td_->file_manager_->get_file_view(file_id);
  if (!file_view.has_remote_location()) {
    promise.set_value(Unit());
    return {};
  }
  if (!file_view.remote_location().is_document()) {
    promise.set_value(Unit());
    return {};
  }
  if (file_view.remote_location().is_web()) {
    promise.set_value(Unit());
    return {};
  }

  const StickerSet *sticker_set = get_sticker_set(sticker->set_id);
  if (update_sticker_set_cache(sticker_set, promise)) {
    return {};
  }

  promise.set_value(Unit());
  auto it = sticker_set->sticker_emojis_map_.find(file_id);
  if (it == sticker_set->sticker_emojis_map_.end()) {
    return {};
  }

  return it->second;
}

void StickersManager::get_animated_emoji(string emoji, bool is_recursive,
                                         Promise<td_api::object_ptr<td_api::animatedEmoji>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto &special_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_emoji());
  auto sticker_set = get_sticker_set(special_sticker_set.id_);
  if (sticker_set == nullptr || !sticker_set->was_loaded) {
    if (is_recursive) {
      return promise.set_value(nullptr);
    }

    pending_get_animated_emoji_queries_.push_back(
        PromiseCreator::lambda([actor_id = actor_id(this), emoji = std::move(emoji),
                                promise = std::move(promise)](Result<Unit> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            send_closure(actor_id, &StickersManager::get_animated_emoji, std::move(emoji), true, std::move(promise));
          }
        }));
    load_special_sticker_set(special_sticker_set);
    return;
  }

  promise.set_value(get_animated_emoji_object(get_animated_emoji_sticker(sticker_set, emoji),
                                              get_animated_emoji_sound_file_id(emoji)));
}

void StickersManager::send_update_recent_stickers(bool is_attached, bool from_database) {
  if (!are_recent_stickers_loaded_[is_attached]) {
    return;
  }

  vector<FileId> new_recent_sticker_file_ids;
  for (auto &sticker_id : recent_sticker_ids_[is_attached]) {
    append(new_recent_sticker_file_ids, get_sticker_file_ids(sticker_id));
  }
  std::sort(new_recent_sticker_file_ids.begin(), new_recent_sticker_file_ids.end());
  if (new_recent_sticker_file_ids != recent_sticker_file_ids_[is_attached]) {
    td_->file_manager_->change_files_source(get_recent_stickers_file_source_id(is_attached),
                                            recent_sticker_file_ids_[is_attached], new_recent_sticker_file_ids);
    recent_sticker_file_ids_[is_attached] = std::move(new_recent_sticker_file_ids);
  }

  recent_stickers_hash_[is_attached] = get_recent_stickers_hash(recent_sticker_ids_[is_attached]);
  send_closure(G()->td(), &Td::send_update, get_update_recent_stickers_object(is_attached));

  if (!from_database) {
    save_recent_stickers_to_database(is_attached != 0);
  }
}

bool StickersManager::update_sticker_set_cache(const StickerSet *sticker_set, Promise<Unit> &promise) {
  CHECK(sticker_set != nullptr);
  auto set_id = sticker_set->id;
  if (!sticker_set->is_loaded) {
    if (!sticker_set->was_loaded || td_->auth_manager_->is_bot()) {
      load_sticker_sets({set_id}, std::move(promise));
      return true;
    } else {
      load_sticker_sets({set_id}, Auto());
    }
  } else if (sticker_set->is_installed) {
    reload_installed_sticker_sets(sticker_set->is_masks, false);
  } else {
    if (G()->unix_time() >= sticker_set->expires_at) {
      if (td_->auth_manager_->is_bot()) {
        do_reload_sticker_set(set_id, get_input_sticker_set(sticker_set), sticker_set->hash, std::move(promise));
        return true;
      } else {
        do_reload_sticker_set(set_id, get_input_sticker_set(sticker_set), sticker_set->hash, Auto());
      }
    }
  }

  return false;
}

void StickersManager::on_update_recent_stickers_limit(int32 recent_stickers_limit) {
  if (recent_stickers_limit != recent_stickers_limit_) {
    if (recent_stickers_limit > 0) {
      LOG(INFO) << "Update recent stickers limit to " << recent_stickers_limit;
      recent_stickers_limit_ = recent_stickers_limit;
      for (int is_attached = 0; is_attached < 2; is_attached++) {
        if (static_cast<int32>(recent_sticker_ids_[is_attached].size()) > recent_stickers_limit) {
          recent_sticker_ids_[is_attached].resize(recent_stickers_limit);
          send_update_recent_stickers(is_attached != 0);
        }
      }
    } else {
      LOG(ERROR) << "Receive wrong recent stickers limit = " << recent_stickers_limit;
    }
  }
}

StickerSetId StickersManager::get_sticker_set(StickerSetId set_id, Promise<Unit> &&promise) {
  const StickerSet *sticker_set = get_sticker_set(set_id);
  if (sticker_set == nullptr) {
    if (set_id.get() == GREAT_MINDS_SET_ID) {
      do_reload_sticker_set(set_id, make_tl_object<telegram_api::inputStickerSetID>(set_id.get(), 0), 0,
                            std::move(promise));
      return StickerSetId();
    }

    promise.set_error(Status::Error(400, "Sticker set not found"));
    return StickerSetId();
  }

  if (update_sticker_set_cache(sticker_set, promise)) {
    return StickerSetId();
  }

  promise.set_value(Unit());
  return set_id;
}

int32 StickersManager::get_dice_success_animation_frame_number(const string &emoji, int32 value) const {
  if (td_->auth_manager_->is_bot()) {
    return std::numeric_limits<int32>::max();
  }
  if (value == 0 || !td::contains(dice_emojis_, emoji)) {
    return std::numeric_limits<int32>::max();
  }
  auto pos = static_cast<size_t>(std::find(dice_emojis_.begin(), dice_emojis_.end(), emoji) - dice_emojis_.begin());
  if (pos >= dice_success_values_.size()) {
    return std::numeric_limits<int32>::max();
  }

  auto &result = dice_success_values_[pos];
  return result.first == value ? result.second : std::numeric_limits<int32>::max();
}

void StickersManager::on_upload_sticker_file(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) {
  LOG(INFO) << "Sticker file " << file_id << " has been uploaded";

  auto it = being_uploaded_files_.find(file_id);
  CHECK(it != being_uploaded_files_.end());

  auto user_id = it->second.first;
  auto promise = std::move(it->second.second);

  being_uploaded_files_.erase(it);

  do_upload_sticker_file(user_id, file_id, std::move(input_file), std::move(promise));
}

FileId StickersManager::dup_sticker(FileId new_id, FileId old_id) {
  const Sticker *old_sticker = get_sticker(old_id);
  CHECK(old_sticker != nullptr);
  auto &new_sticker = stickers_[new_id];
  CHECK(!new_sticker);
  new_sticker = make_unique<Sticker>(*old_sticker);
  new_sticker->file_id = new_id;
  // there is no reason to dup m_thumbnail
  new_sticker->s_thumbnail.file_id = td_->file_manager_->dup_file_id(new_sticker->s_thumbnail.file_id);
  return new_id;
}

void StickersManager::on_get_installed_sticker_sets_failed(bool is_masks, Status error) {
  CHECK(error.is_error());
  next_installed_sticker_sets_load_time_[is_masks] = Time::now_cached() + Random::fast(5, 10);
  auto promises = std::move(load_installed_sticker_sets_queries_[is_masks]);
  load_installed_sticker_sets_queries_[is_masks].clear();
  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

void StickersManager::on_old_featured_sticker_sets_invalidated() {
  LOG(INFO) << "Invalidate old trending sticker sets";
  are_old_featured_sticker_sets_invalidated_ = true;

  if (!G()->parameters().use_file_db) {
    return;
  }

  G()->td_db()->get_binlog_pmc()->set("invalidate_old_featured_sticker_sets", "1");
}

const StickersManager::Sticker *StickersManager::get_sticker(FileId file_id) const {
  auto sticker = stickers_.find(file_id);
  if (sticker == stickers_.end()) {
    return nullptr;
  }

  CHECK(sticker->second->file_id == file_id);
  return sticker->second.get();
}

StickersManager::Sticker *StickersManager::get_sticker(FileId file_id) {
  auto sticker = stickers_.find(file_id);
  if (sticker == stickers_.end()) {
    return nullptr;
  }

  CHECK(sticker->second->file_id == file_id);
  return sticker->second.get();
}

string StickersManager::get_emoji_language_code_version_database_key(const string &language_code) {
  return PSTRING() << "emojiv$" << language_code;
}

void StickersManager::start_up() {
  init();
}
}  // namespace td
