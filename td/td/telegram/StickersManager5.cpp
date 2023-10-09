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

#include "td/telegram/DialogId.h"
#include "td/telegram/Document.h"
#include "td/telegram/DocumentsManager.h"

#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"

#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/MessagesManager.h"
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





#include <unordered_set>

namespace td {
}  // namespace td
namespace td {

void StickersManager::on_get_emoji_keywords_difference(
    const string &language_code, int32 from_version,
    Result<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> &&result) {
  if (G()->close_flag()) {
    result = G()->close_status();
  }
  if (result.is_error()) {
    if (!G()->is_expected_error(result.error())) {
      LOG(ERROR) << "Receive " << result.error() << " from GetEmojiKeywordsDifferenceQuery";
    }
    emoji_language_code_last_difference_times_[language_code] = Time::now_cached() - EMOJI_KEYWORDS_UPDATE_DELAY - 2;
    return;
  }

  auto version = get_emoji_language_code_version(language_code);
  CHECK(version == from_version);

  auto keywords = result.move_as_ok();
  LOG(INFO) << "Receive " << keywords->keywords_.size() << " emoji keywords difference for language " << language_code;
  LOG_IF(ERROR, language_code != keywords->lang_code_)
      << "Receive keywords for " << keywords->lang_code_ << " instead of " << language_code;
  LOG_IF(ERROR, keywords->from_version_ != from_version)
      << "Receive keywords from version " << keywords->from_version_ << " instead of " << from_version;
  if (keywords->version_ < version) {
    LOG(ERROR) << "Receive keywords of version " << keywords->version_ << ", but have of version " << version;
    keywords->version_ = version;
  }
  version = keywords->version_;
  std::unordered_map<string, string> key_values;
  key_values.emplace(get_emoji_language_code_version_database_key(language_code), to_string(version));
  key_values.emplace(get_emoji_language_code_last_difference_time_database_key(language_code),
                     to_string(G()->unix_time()));
  for (auto &keyword_ptr : keywords->keywords_) {
    switch (keyword_ptr->get_id()) {
      case telegram_api::emojiKeyword::ID: {
        auto keyword = telegram_api::move_object_as<telegram_api::emojiKeyword>(keyword_ptr);
        auto text = utf8_to_lower(keyword->keyword_);
        bool is_good = true;
        for (auto &emoji : keyword->emoticons_) {
          if (emoji.find('$') != string::npos) {
            LOG(ERROR) << "Receive emoji \"" << emoji << "\" from server for " << text;
            is_good = false;
          }
        }
        if (is_good) {
          vector<string> emojis = search_language_emojis(language_code, text, true);
          bool is_changed = false;
          for (auto &emoji : keyword->emoticons_) {
            if (!td::contains(emojis, emoji)) {
              emojis.push_back(emoji);
              is_changed = true;
            }
          }
          if (is_changed) {
            key_values.emplace(get_language_emojis_database_key(language_code, text), implode(emojis, '$'));
          } else {
            LOG(INFO) << "Emoji keywords not changed for \"" << text << "\" from version " << from_version
                      << " to version " << version;
          }
        }
        break;
      }
      case telegram_api::emojiKeywordDeleted::ID: {
        auto keyword = telegram_api::move_object_as<telegram_api::emojiKeywordDeleted>(keyword_ptr);
        auto text = utf8_to_lower(keyword->keyword_);
        vector<string> emojis = search_language_emojis(language_code, text, true);
        bool is_changed = false;
        for (auto &emoji : keyword->emoticons_) {
          if (td::remove(emojis, emoji)) {
            is_changed = true;
          }
        }
        if (is_changed) {
          key_values.emplace(get_language_emojis_database_key(language_code, text), implode(emojis, '$'));
        } else {
          LOG(INFO) << "Emoji keywords not changed for \"" << text << "\" from version " << from_version
                    << " to version " << version;
        }
        break;
      }
      default:
        UNREACHABLE();
    }
  }
  G()->td_db()->get_sqlite_pmc()->set_all(
      std::move(key_values), PromiseCreator::lambda([actor_id = actor_id(this), language_code, version](Unit) mutable {
        send_closure(actor_id, &StickersManager::finish_get_emoji_keywords_difference, std::move(language_code),
                     version);
      }));
}

void StickersManager::on_get_emoji_keywords(
    const string &language_code, Result<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> &&result) {
  auto it = load_emoji_keywords_queries_.find(language_code);
  CHECK(it != load_emoji_keywords_queries_.end());
  auto promises = std::move(it->second);
  CHECK(!promises.empty());
  load_emoji_keywords_queries_.erase(it);

  if (result.is_error()) {
    if (!G()->is_expected_error(result.error())) {
      LOG(ERROR) << "Receive " << result.error() << " from GetEmojiKeywordsQuery";
    }
    for (auto &promise : promises) {
      promise.set_error(result.error().clone());
    }
    return;
  }

  auto version = get_emoji_language_code_version(language_code);
  CHECK(version == 0);

  MultiPromiseActorSafe mpas{"SaveEmojiKeywordsMultiPromiseActor"};
  for (auto &promise : promises) {
    mpas.add_promise(std::move(promise));
  }

  auto lock = mpas.get_promise();

  auto keywords = result.move_as_ok();
  LOG(INFO) << "Receive " << keywords->keywords_.size() << " emoji keywords for language " << language_code;
  LOG_IF(ERROR, language_code != keywords->lang_code_)
      << "Receive keywords for " << keywords->lang_code_ << " instead of " << language_code;
  LOG_IF(ERROR, keywords->from_version_ != 0) << "Receive keywords from version " << keywords->from_version_;
  version = keywords->version_;
  if (version <= 0) {
    LOG(ERROR) << "Receive keywords of version " << version;
    version = 1;
  }
  for (auto &keyword_ptr : keywords->keywords_) {
    switch (keyword_ptr->get_id()) {
      case telegram_api::emojiKeyword::ID: {
        auto keyword = telegram_api::move_object_as<telegram_api::emojiKeyword>(keyword_ptr);
        auto text = utf8_to_lower(keyword->keyword_);
        bool is_good = true;
        for (auto &emoji : keyword->emoticons_) {
          if (emoji.find('$') != string::npos) {
            LOG(ERROR) << "Receive emoji \"" << emoji << "\" from server for " << text;
            is_good = false;
          }
        }
        if (is_good && !G()->close_flag()) {
          CHECK(G()->parameters().use_file_db);
          G()->td_db()->get_sqlite_pmc()->set(get_language_emojis_database_key(language_code, text),
                                              implode(keyword->emoticons_, '$'), mpas.get_promise());
        }
        break;
      }
      case telegram_api::emojiKeywordDeleted::ID:
        LOG(ERROR) << "Receive emojiKeywordDeleted in keywords for " << language_code;
        break;
      default:
        UNREACHABLE();
    }
  }
  if (!G()->close_flag()) {
    CHECK(G()->parameters().use_file_db);
    G()->td_db()->get_sqlite_pmc()->set(get_emoji_language_code_version_database_key(language_code), to_string(version),
                                        mpas.get_promise());
    G()->td_db()->get_sqlite_pmc()->set(get_emoji_language_code_last_difference_time_database_key(language_code),
                                        to_string(G()->unix_time()), mpas.get_promise());
  }
  emoji_language_code_versions_[language_code] = version;
  emoji_language_code_last_difference_times_[language_code] = static_cast<int32>(Time::now_cached());

  lock.set_value(Unit());
}

void StickersManager::on_load_special_sticker_set(const SpecialStickerSetType &type, Status result) {
  if (G()->close_flag()) {
    return;
  }

  auto &special_sticker_set = add_special_sticker_set(type);
  special_sticker_set.is_being_reloaded_ = false;
  if (!special_sticker_set.is_being_loaded_) {
    return;
  }

  if (result.is_error()) {
    LOG(INFO) << "Failed to load special sticker set " << type.type_ << ": " << result.error();

    // failed to load the special sticker set; repeat after some time
    create_actor<SleepActor>("RetryLoadSpecialStickerSetActor", Random::fast(300, 600),
                             PromiseCreator::lambda([actor_id = actor_id(this), type](Result<Unit> result) mutable {
                               send_closure(actor_id, &StickersManager::load_special_sticker_set_by_type,
                                            std::move(type));
                             }))
        .release();
    return;
  }

  special_sticker_set.is_being_loaded_ = false;

  if (type == SpecialStickerSetType::animated_emoji()) {
    auto promises = std::move(pending_get_animated_emoji_queries_);
    reset_to_empty(pending_get_animated_emoji_queries_);
    for (auto &promise : promises) {
      promise.set_value(Unit());
    }
    return;
  }

  CHECK(special_sticker_set.id_.is_valid());
  auto sticker_set = get_sticker_set(special_sticker_set.id_);
  CHECK(sticker_set != nullptr);
  CHECK(sticker_set->was_loaded);

  if (type == SpecialStickerSetType::animated_emoji_click()) {
    auto pending_get_requests = std::move(pending_get_animated_emoji_click_stickers_);
    reset_to_empty(pending_get_animated_emoji_click_stickers_);
    for (auto &pending_request : pending_get_requests) {
      choose_animated_emoji_click_sticker(sticker_set, pending_request.message_text_, pending_request.full_message_id_,
                                          pending_request.start_time_, std::move(pending_request.promise_));
    }
    auto pending_click_requests = std::move(pending_on_animated_emoji_message_clicked_);
    reset_to_empty(pending_on_animated_emoji_message_clicked_);
    for (auto &pending_request : pending_click_requests) {
      schedule_update_animated_emoji_clicked(sticker_set, pending_request.emoji_, pending_request.full_message_id_,
                                             std::move(pending_request.clicks_));
    }
    return;
  }

  auto emoji = type.get_dice_emoji();
  CHECK(!emoji.empty());

  auto it = dice_messages_.find(emoji);
  if (it == dice_messages_.end()) {
    return;
  }

  vector<FullMessageId> full_message_ids;
  for (const auto &full_message_id : it->second) {
    full_message_ids.push_back(full_message_id);
  }
  CHECK(!full_message_ids.empty());
  for (const auto &full_message_id : full_message_ids) {
    td_->messages_manager_->on_external_update_message_content(full_message_id);
  }
}

class StickersManager::UploadStickerFileCallback final : public FileManager::UploadCallback {
 public:
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->stickers_manager(), &StickersManager::on_upload_sticker_file, file_id,
                       std::move(input_file));
  }

  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    UNREACHABLE();
  }

  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }

  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->stickers_manager(), &StickersManager::on_upload_sticker_file_error, file_id,
                       std::move(error));
  }
};

StickersManager::StickersManager(Td *td, ActorShared<> parent) : td_(td), parent_(std::move(parent)) {
  upload_sticker_file_callback_ = std::make_shared<UploadStickerFileCallback>();

  on_update_animated_emoji_zoom();

  on_update_recent_stickers_limit(
      narrow_cast<int32>(G()->shared_config().get_option_integer("recent_stickers_limit", 200)));
  on_update_favorite_stickers_limit(
      narrow_cast<int32>(G()->shared_config().get_option_integer("favorite_stickers_limit", 5)));

  next_click_animated_emoji_message_time_ = Time::now();
  next_update_animated_emoji_clicked_time_ = Time::now();
}

void StickersManager::upload_sticker_file(UserId user_id, FileId file_id, Promise<Unit> &&promise) {
  FileId upload_file_id;
  if (td_->file_manager_->get_file_view(file_id).get_type() == FileType::Sticker) {
    CHECK(get_input_media(file_id, nullptr, nullptr, string()) == nullptr);
    upload_file_id = dup_sticker(td_->file_manager_->dup_file_id(file_id), file_id);
  } else {
    CHECK(td_->documents_manager_->get_input_media(file_id, nullptr, nullptr) == nullptr);
    upload_file_id = td_->documents_manager_->dup_document(td_->file_manager_->dup_file_id(file_id), file_id);
  }

  being_uploaded_files_[upload_file_id] = {user_id, std::move(promise)};
  LOG(INFO) << "Ask to upload sticker file " << upload_file_id;
  td_->file_manager_->upload(upload_file_id, upload_sticker_file_callback_, 2, 0);
}

class SetStickerPositionQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit SetStickerPositionQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(tl_object_ptr<telegram_api::inputDocument> &&input_document, int32 position) {
    send_query(G()->net_query_creator().create(
        telegram_api::stickers_changeStickerPosition(std::move(input_document), position)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_changeStickerPosition>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_get_messages_sticker_set(StickerSetId(), result_ptr.move_as_ok(), true,
                                                        "SetStickerPositionQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

void StickersManager::set_sticker_position_in_set(const tl_object_ptr<td_api::InputFile> &sticker, int32 position,
                                                  Promise<Unit> &&promise) {
  if (position < 0) {
    return promise.set_error(Status::Error(400, "Wrong sticker position specified"));
  }

  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Sticker, sticker, DialogId(), false, false);
  if (r_file_id.is_error()) {
    return promise.set_error(Status::Error(400, r_file_id.error().message()));  // TODO do not drop error code
  }

  auto file_id = r_file_id.move_as_ok();
  auto file_view = td_->file_manager_->get_file_view(file_id);
  if (!file_view.has_remote_location() || !file_view.main_remote_location().is_document() ||
      file_view.main_remote_location().is_web()) {
    return promise.set_error(Status::Error(400, "Wrong sticker file specified"));
  }

  td_->create_handler<SetStickerPositionQuery>(std::move(promise))
      ->send(file_view.main_remote_location().as_input_document(), position);
}

// -1 - order can't be applied, because some sticker sets aren't loaded or aren't installed,
// 0 - order wasn't changed, 1 - order was partly replaced by the new order, 2 - order was replaced by the new order
int StickersManager::apply_installed_sticker_sets_order(bool is_masks, const vector<StickerSetId> &sticker_set_ids) {
  if (!are_installed_sticker_sets_loaded_[is_masks]) {
    return -1;
  }

  vector<StickerSetId> &current_sticker_set_ids = installed_sticker_set_ids_[is_masks];
  if (sticker_set_ids == current_sticker_set_ids) {
    return 0;
  }

  std::unordered_set<StickerSetId, StickerSetIdHash> valid_set_ids(current_sticker_set_ids.begin(),
                                                                   current_sticker_set_ids.end());
  vector<StickerSetId> new_sticker_set_ids;
  for (auto sticker_set_id : sticker_set_ids) {
    auto it = valid_set_ids.find(sticker_set_id);
    if (it != valid_set_ids.end()) {
      new_sticker_set_ids.push_back(sticker_set_id);
      valid_set_ids.erase(it);
    } else {
      return -1;
    }
  }
  if (new_sticker_set_ids.empty()) {
    return 0;
  }
  if (!valid_set_ids.empty()) {
    vector<StickerSetId> missed_sticker_set_ids;
    for (auto sticker_set_id : current_sticker_set_ids) {
      auto it = valid_set_ids.find(sticker_set_id);
      if (it != valid_set_ids.end()) {
        missed_sticker_set_ids.push_back(sticker_set_id);
        valid_set_ids.erase(it);
      }
    }
    append(missed_sticker_set_ids, new_sticker_set_ids);
    new_sticker_set_ids = std::move(missed_sticker_set_ids);
  }
  CHECK(valid_set_ids.empty());

  if (new_sticker_set_ids == current_sticker_set_ids) {
    return 0;
  }
  current_sticker_set_ids = std::move(new_sticker_set_ids);

  need_update_installed_sticker_sets_[is_masks] = true;
  if (sticker_set_ids != current_sticker_set_ids) {
    return 1;
  }
  return 2;
}

class GetFavedStickersQuery final : public Td::ResultHandler {
  bool is_repair_ = false;

 public:
  void send(bool is_repair, int64 hash) {
    is_repair_ = is_repair;
    send_query(G()->net_query_creator().create(telegram_api::messages_getFavedStickers(hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getFavedStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    td_->stickers_manager_->on_get_favorite_stickers(is_repair_, std::move(ptr));
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for get favorite stickers: " << status;
    }
    td_->stickers_manager_->on_get_favorite_stickers_failed(is_repair_, std::move(status));
  }
};

void StickersManager::reload_favorite_stickers(bool force) {
  if (G()->close_flag()) {
    return;
  }

  auto &next_load_time = next_favorite_stickers_load_time_;
  if (!td_->auth_manager_->is_bot() && next_load_time >= 0 && (next_load_time < Time::now() || force)) {
    LOG_IF(INFO, force) << "Reload favorite stickers";
    next_load_time = -1;
    td_->create_handler<GetFavedStickersQuery>()->send(false, get_favorite_stickers_hash());
  }
}

void StickersManager::repair_favorite_stickers(Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Bots have no favorite stickers"));
  }

  repair_favorite_stickers_queries_.push_back(std::move(promise));
  if (repair_favorite_stickers_queries_.size() == 1u) {
    td_->create_handler<GetFavedStickersQuery>()->send(true, 0);
  }
}

class CheckStickerSetShortNameQuery final : public Td::ResultHandler {
  Promise<bool> promise_;

 public:
  explicit CheckStickerSetShortNameQuery(Promise<bool> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &short_name) {
    send_query(G()->net_query_creator().create(telegram_api::stickers_checkShortName(short_name)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_checkShortName>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void StickersManager::check_sticker_set_name(const string &name, Promise<CheckStickerSetNameResult> &&promise) {
  if (name.empty()) {
    return promise.set_value(CheckStickerSetNameResult::Invalid);
  }

  auto request_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<bool> result) mutable {
    if (result.is_error()) {
      auto error = result.move_as_error();
      if (error.message() == "SHORT_NAME_INVALID") {
        return promise.set_value(CheckStickerSetNameResult::Invalid);
      }
      if (error.message() == "SHORT_NAME_OCCUPIED") {
        return promise.set_value(CheckStickerSetNameResult::Occupied);
      }
      return promise.set_error(std::move(error));
    }

    promise.set_value(CheckStickerSetNameResult::Ok);
  });

  return td_->create_handler<CheckStickerSetShortNameQuery>(std::move(request_promise))->send(name);
}

void StickersManager::get_animated_emoji_click_sticker(const string &message_text, FullMessageId full_message_id,
                                                       Promise<td_api::object_ptr<td_api::sticker>> &&promise) {
  if (disable_animated_emojis_ || td_->auth_manager_->is_bot()) {
    return promise.set_value(nullptr);
  }

  auto &special_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_emoji_click());
  if (!special_sticker_set.id_.is_valid()) {
    // don't wait for the first load of the sticker set from the server
    load_special_sticker_set(special_sticker_set);
    return promise.set_value(nullptr);
  }

  auto sticker_set = get_sticker_set(special_sticker_set.id_);
  CHECK(sticker_set != nullptr);
  if (sticker_set->was_loaded) {
    return choose_animated_emoji_click_sticker(sticker_set, message_text, full_message_id, Time::now(),
                                               std::move(promise));
  }

  LOG(INFO) << "Waiting for an emoji click sticker set needed in " << full_message_id;
  load_special_sticker_set(special_sticker_set);

  PendingGetAnimatedEmojiClickSticker pending_request;
  pending_request.message_text_ = message_text;
  pending_request.full_message_id_ = full_message_id;
  pending_request.start_time_ = Time::now();
  pending_request.promise_ = std::move(promise);
  pending_get_animated_emoji_click_stickers_.push_back(std::move(pending_request));
}

void StickersManager::load_installed_sticker_sets(bool is_masks, Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    are_installed_sticker_sets_loaded_[is_masks] = true;
  }
  if (are_installed_sticker_sets_loaded_[is_masks]) {
    promise.set_value(Unit());
    return;
  }
  load_installed_sticker_sets_queries_[is_masks].push_back(std::move(promise));
  if (load_installed_sticker_sets_queries_[is_masks].size() == 1u) {
    if (G()->parameters().use_file_db) {
      LOG(INFO) << "Trying to load installed " << (is_masks ? "mask " : "") << "sticker sets from database";
      G()->td_db()->get_sqlite_pmc()->get(is_masks ? "sss1" : "sss0", PromiseCreator::lambda([is_masks](string value) {
                                            send_closure(G()->stickers_manager(),
                                                         &StickersManager::on_load_installed_sticker_sets_from_database,
                                                         is_masks, std::move(value));
                                          }));
    } else {
      LOG(INFO) << "Trying to load installed " << (is_masks ? "mask " : "") << "sticker sets from server";
      reload_installed_sticker_sets(is_masks, true);
    }
  }
}

void StickersManager::remove_recent_sticker(bool is_attached, const tl_object_ptr<td_api::InputFile> &input_file,
                                            Promise<Unit> &&promise) {
  if (!are_recent_stickers_loaded_[is_attached]) {
    load_recent_stickers(is_attached, std::move(promise));
    return;
  }

  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Sticker, input_file, DialogId(), false, false);
  if (r_file_id.is_error()) {
    return promise.set_error(Status::Error(400, r_file_id.error().message()));  // TODO do not drop error code
  }

  vector<FileId> &sticker_ids = recent_sticker_ids_[is_attached];
  FileId file_id = r_file_id.ok();
  if (!td::remove(sticker_ids, file_id)) {
    return promise.set_value(Unit());
  }

  auto sticker = get_sticker(file_id);
  if (sticker == nullptr) {
    return promise.set_error(Status::Error(400, "Sticker not found"));
  }

  send_save_recent_sticker_query(is_attached, file_id, true, std::move(promise));

  send_update_recent_stickers(is_attached);
}

std::pair<int32, vector<StickerSetId>> StickersManager::search_installed_sticker_sets(bool is_masks,
                                                                                      const string &query, int32 limit,
                                                                                      Promise<Unit> &&promise) {
  LOG(INFO) << "Search installed " << (is_masks ? "mask " : "") << "sticker sets with query = \"" << query
            << "\" and limit = " << limit;

  if (limit < 0) {
    promise.set_error(Status::Error(400, "Limit must be non-negative"));
    return {};
  }

  if (!are_installed_sticker_sets_loaded_[is_masks]) {
    load_installed_sticker_sets(is_masks, std::move(promise));
    return {};
  }
  reload_installed_sticker_sets(is_masks, false);

  std::pair<size_t, vector<int64>> result = installed_sticker_sets_hints_[is_masks].search(query, limit);
  promise.set_value(Unit());
  return {narrow_cast<int32>(result.first), convert_sticker_set_ids(result.second)};
}

void StickersManager::register_emoji(const string &emoji, FullMessageId full_message_id, const char *source) {
  CHECK(!emoji.empty());
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  LOG(INFO) << "Register emoji " << emoji << " from " << full_message_id << " from " << source;
  auto &emoji_messages = emoji_messages_[emoji];
  if (emoji_messages.full_message_ids.empty()) {
    emoji_messages.animated_emoji_sticker = get_animated_emoji_sticker(emoji);
    emoji_messages.sound_file_id = get_animated_emoji_sound_file_id(emoji);
  }
  bool is_inserted = emoji_messages.full_message_ids.insert(full_message_id).second;
  LOG_CHECK(is_inserted) << source << ' ' << emoji << ' ' << full_message_id;
}

void StickersManager::unregister_dice(const string &emoji, int32 value, FullMessageId full_message_id,
                                      const char *source) {
  CHECK(!emoji.empty());
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  LOG(INFO) << "Unregister dice " << emoji << " with value " << value << " from " << full_message_id << " from "
            << source;
  auto &message_ids = dice_messages_[emoji];
  auto is_deleted = message_ids.erase(full_message_id) > 0;
  LOG_CHECK(is_deleted) << source << " " << emoji << " " << value << " " << full_message_id;

  if (message_ids.empty()) {
    dice_messages_.erase(emoji);
  }
}

void StickersManager::after_get_difference() {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (td_->is_online()) {
    get_installed_sticker_sets(false, Auto());
    get_installed_sticker_sets(true, Auto());
    get_featured_sticker_sets(0, 1000, Auto());
    get_recent_stickers(false, Auto());
    get_recent_stickers(true, Auto());
    get_favorite_stickers(Auto());

    if (!disable_animated_emojis_) {
      reload_special_sticker_set_by_type(SpecialStickerSetType::animated_emoji());
      reload_special_sticker_set_by_type(SpecialStickerSetType::animated_emoji_click());
    }
  }
}

void StickersManager::on_send_animated_emoji_clicks(DialogId dialog_id, const string &emoji) {
  flush_sent_animated_emoji_clicks();

  if (!sent_animated_emoji_clicks_.empty() && sent_animated_emoji_clicks_.back().dialog_id == dialog_id &&
      sent_animated_emoji_clicks_.back().emoji == emoji) {
    sent_animated_emoji_clicks_.back().send_time = Time::now();
    return;
  }

  SentAnimatedEmojiClicks clicks;
  clicks.send_time = Time::now();
  clicks.dialog_id = dialog_id;
  clicks.emoji = emoji;
  sent_animated_emoji_clicks_.push_back(std::move(clicks));
}

td_api::object_ptr<td_api::updateTrendingStickerSets> StickersManager::get_update_trending_sticker_sets_object() const {
  auto total_count = static_cast<int32>(featured_sticker_set_ids_.size()) +
                     (old_featured_sticker_set_count_ == -1 ? 1 : old_featured_sticker_set_count_);
  return td_api::make_object<td_api::updateTrendingStickerSets>(
      get_sticker_sets_object(total_count, featured_sticker_set_ids_, 5));
}

vector<StickerSetId> StickersManager::get_installed_sticker_sets(bool is_masks, Promise<Unit> &&promise) {
  if (!are_installed_sticker_sets_loaded_[is_masks]) {
    load_installed_sticker_sets(is_masks, std::move(promise));
    return {};
  }
  reload_installed_sticker_sets(is_masks, false);

  promise.set_value(Unit());
  return installed_sticker_set_ids_[is_masks];
}

void StickersManager::load_special_sticker_set_by_type(SpecialStickerSetType type) {
  if (G()->close_flag()) {
    return;
  }

  auto &sticker_set = add_special_sticker_set(type);
  if (!sticker_set.is_being_loaded_) {
    return;
  }
  sticker_set.is_being_loaded_ = false;
  load_special_sticker_set(sticker_set);
}

tl_object_ptr<telegram_api::InputStickerSet> StickersManager::get_input_sticker_set(StickerSetId sticker_set_id) const {
  auto sticker_set = get_sticker_set(sticker_set_id);
  if (sticker_set == nullptr) {
    return nullptr;
  }

  return get_input_sticker_set(sticker_set);
}

tl_object_ptr<telegram_api::InputStickerSet> StickersManager::get_input_sticker_set(const StickerSet *set) {
  CHECK(set != nullptr);
  return make_tl_object<telegram_api::inputStickerSetID>(set->id.get(), set->access_hash);
}

void StickersManager::delete_sticker_thumbnail(FileId file_id) {
  auto &sticker = stickers_[file_id];
  CHECK(sticker != nullptr);
  sticker->s_thumbnail = PhotoSize();
}

void StickersManager::timeout_expired() {
  flush_pending_animated_emoji_clicks();
}
}  // namespace td
