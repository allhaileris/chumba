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

#include <algorithm>
#include <cmath>
#include <limits>
#include <type_traits>
#include <unordered_set>

namespace td {
}  // namespace td
namespace td {

StickerSetId StickersManager::on_get_messages_sticker_set(StickerSetId sticker_set_id,
                                                          tl_object_ptr<telegram_api::messages_StickerSet> &&set_ptr,
                                                          bool is_changed, const char *source) {
  LOG(INFO) << "Receive sticker set " << to_string(set_ptr);
  if (set_ptr->get_id() == telegram_api::messages_stickerSetNotModified::ID) {
    if (!sticker_set_id.is_valid()) {
      LOG(ERROR) << "Receive unexpected stickerSetNotModified from " << source;
    } else {
      auto s = get_sticker_set(sticker_set_id);
      CHECK(s != nullptr);
      CHECK(s->is_inited);
      CHECK(s->was_loaded);

      s->expires_at = G()->unix_time() +
                      (td_->auth_manager_->is_bot() ? Random::fast(10 * 60, 15 * 60) : Random::fast(30 * 60, 50 * 60));
    }
    return sticker_set_id;
  }
  auto set = move_tl_object_as<telegram_api::messages_stickerSet>(set_ptr);

  auto set_id = on_get_sticker_set(std::move(set->set_), is_changed, source);
  if (!set_id.is_valid()) {
    return set_id;
  }
  if (sticker_set_id.is_valid() && sticker_set_id != set_id) {
    LOG(ERROR) << "Expected " << sticker_set_id << ", but receive " << set_id << " from " << source;
    on_load_sticker_set_fail(sticker_set_id, Status::Error(500, "Internal Server Error: wrong sticker set received"));
    return StickerSetId();
  }

  auto s = get_sticker_set(set_id);
  CHECK(s != nullptr);
  CHECK(s->is_inited);

  s->expires_at = G()->unix_time() +
                  (td_->auth_manager_->is_bot() ? Random::fast(10 * 60, 15 * 60) : Random::fast(30 * 60, 50 * 60));

  if (s->is_loaded) {
    update_sticker_set(s, "on_get_messages_sticker_set");
    send_update_installed_sticker_sets();
    return set_id;
  }
  s->was_loaded = true;
  s->is_loaded = true;
  s->is_changed = true;

  vector<tl_object_ptr<telegram_api::stickerPack>> packs = std::move(set->packs_);
  vector<tl_object_ptr<telegram_api::Document>> documents = std::move(set->documents_);

  std::unordered_map<int64, FileId> document_id_to_sticker_id;

  s->sticker_ids.clear();
  bool is_bot = td_->auth_manager_->is_bot();
  for (auto &document_ptr : documents) {
    auto sticker_id = on_get_sticker_document(std::move(document_ptr));
    if (!sticker_id.second.is_valid()) {
      continue;
    }

    s->sticker_ids.push_back(sticker_id.second);
    if (!is_bot) {
      document_id_to_sticker_id.insert(sticker_id);
    }
  }
  if (static_cast<int32>(s->sticker_ids.size()) != s->sticker_count) {
    LOG(ERROR) << "Wrong sticker set size " << s->sticker_count << " instead of " << s->sticker_ids.size()
               << " specified in " << set_id << "/" << s->short_name << " from " << source;
    s->sticker_count = static_cast<int32>(s->sticker_ids.size());
  }

  if (!is_bot) {
    s->emoji_stickers_map_.clear();
    s->sticker_emojis_map_.clear();
    for (auto &pack : packs) {
      vector<FileId> stickers;
      stickers.reserve(pack->documents_.size());
      for (int64 document_id : pack->documents_) {
        auto it = document_id_to_sticker_id.find(document_id);
        if (it == document_id_to_sticker_id.end()) {
          LOG(ERROR) << "Can't find document with ID " << document_id << " in " << set_id << "/" << s->short_name
                     << " from " << source;
          continue;
        }

        stickers.push_back(it->second);
        s->sticker_emojis_map_[it->second].push_back(pack->emoticon_);
      }
      auto &sticker_ids = s->emoji_stickers_map_[remove_emoji_modifiers(pack->emoticon_).str()];
      for (auto sticker_id : stickers) {
        if (!td::contains(sticker_ids, sticker_id)) {
          sticker_ids.push_back(sticker_id);
        }
      }
    }
  }

  update_sticker_set(s, "on_get_messages_sticker_set 2");
  update_load_requests(s, true, Status::OK());
  send_update_installed_sticker_sets();

  if (set_id == add_special_sticker_set(SpecialStickerSetType::animated_emoji()).id_) {
    try_update_animated_emoji_messages();
  }

  return set_id;
}

class CreateNewStickerSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit CreateNewStickerSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(tl_object_ptr<telegram_api::InputUser> &&input_user, const string &title, const string &short_name,
            bool is_masks, bool is_animated, vector<tl_object_ptr<telegram_api::inputStickerSetItem>> &&input_stickers,
            const string &software) {
    CHECK(input_user != nullptr);

    int32 flags = 0;
    if (is_masks) {
      flags |= telegram_api::stickers_createStickerSet::MASKS_MASK;
    }
    if (is_animated) {
      flags |= telegram_api::stickers_createStickerSet::ANIMATED_MASK;
    }
    if (!software.empty()) {
      flags |= telegram_api::stickers_createStickerSet::SOFTWARE_MASK;
    }

    send_query(G()->net_query_creator().create(
        telegram_api::stickers_createStickerSet(flags, false /*ignored*/, false /*ignored*/, std::move(input_user),
                                                title, short_name, nullptr, std::move(input_stickers), software)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_createStickerSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_get_messages_sticker_set(StickerSetId(), result_ptr.move_as_ok(), true,
                                                        "CreateNewStickerSetQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

void StickersManager::on_new_stickers_uploaded(int64 random_id, Result<Unit> result) {
  auto it = pending_new_sticker_sets_.find(random_id);
  CHECK(it != pending_new_sticker_sets_.end());

  auto pending_new_sticker_set = std::move(it->second);
  CHECK(pending_new_sticker_set != nullptr);

  pending_new_sticker_sets_.erase(it);

  if (G()->close_flag()) {
    result = Global::request_aborted_error();
  }
  if (result.is_error()) {
    pending_new_sticker_set->promise.set_error(result.move_as_error());
    return;
  }

  CHECK(pending_new_sticker_set->upload_files_multipromise.promise_count() == 0);

  auto &promise = pending_new_sticker_set->promise;
  TRY_RESULT_PROMISE(promise, input_user, td_->contacts_manager_->get_input_user(pending_new_sticker_set->user_id));

  bool is_masks = pending_new_sticker_set->is_masks;
  bool is_animated = pending_new_sticker_set->is_animated;

  auto sticker_count = pending_new_sticker_set->stickers.size();
  vector<tl_object_ptr<telegram_api::inputStickerSetItem>> input_stickers;
  input_stickers.reserve(sticker_count);
  for (size_t i = 0; i < sticker_count; i++) {
    input_stickers.push_back(
        get_input_sticker(pending_new_sticker_set->stickers[i].get(), pending_new_sticker_set->file_ids[i]));
  }

  td_->create_handler<CreateNewStickerSetQuery>(std::move(pending_new_sticker_set->promise))
      ->send(std::move(input_user), pending_new_sticker_set->title, pending_new_sticker_set->short_name, is_masks,
             is_animated, std::move(input_stickers), pending_new_sticker_set->software);
}

class SendAnimatedEmojiClicksQuery final : public Td::ResultHandler {
  DialogId dialog_id_;
  string emoji_;

 public:
  void send(DialogId dialog_id, tl_object_ptr<telegram_api::InputPeer> &&input_peer,
            tl_object_ptr<telegram_api::sendMessageEmojiInteraction> &&action) {
    dialog_id_ = dialog_id;
    CHECK(input_peer != nullptr);
    CHECK(action != nullptr);
    emoji_ = action->emoticon_;

    int32 flags = 0;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_setTyping(flags, std::move(input_peer), 0, std::move(action))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_setTyping>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    // ignore result
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendAnimatedEmojiClicksQuery")) {
      LOG(INFO) << "Receive error for send animated emoji clicks: " << status;
    }

    td_->stickers_manager_->on_send_animated_emoji_clicks(dialog_id_, emoji_);
  }
};

void StickersManager::flush_pending_animated_emoji_clicks() {
  if (pending_animated_emoji_clicks_.empty()) {
    return;
  }
  auto clicks = std::move(pending_animated_emoji_clicks_);
  pending_animated_emoji_clicks_.clear();
  auto full_message_id = last_clicked_animated_emoji_full_message_id_;
  last_clicked_animated_emoji_full_message_id_ = FullMessageId();
  auto emoji = std::move(last_clicked_animated_emoji_);
  last_clicked_animated_emoji_.clear();

  if (td_->messages_manager_->is_message_edited_recently(full_message_id, 1)) {
    // includes deleted full_message_id
    return;
  }
  auto dialog_id = full_message_id.get_dialog_id();
  auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
  if (input_peer == nullptr) {
    return;
  }

  double start_time = clicks[0].second;
  auto data = json_encode<string>(json_object([&clicks, start_time](auto &o) {
    o("v", 1);
    o("a", json_array(clicks, [start_time](auto &click) {
        return json_object([&click, start_time](auto &o) {
          o("i", click.first);
          auto t = static_cast<int32>((click.second - start_time) * 100);
          o("t", JsonRaw(PSLICE() << (t / 100) << '.' << (t < 10 ? "0" : "") << (t % 100)));
        });
      }));
  }));

  td_->create_handler<SendAnimatedEmojiClicksQuery>()->send(
      dialog_id, std::move(input_peer),
      make_tl_object<telegram_api::sendMessageEmojiInteraction>(
          emoji, full_message_id.get_message_id().get_server_message_id().get(),
          make_tl_object<telegram_api::dataJSON>(data)));

  on_send_animated_emoji_clicks(dialog_id, emoji);
}

class GetRecentStickersQuery final : public Td::ResultHandler {
  bool is_repair_ = false;
  bool is_attached_ = false;

 public:
  void send(bool is_repair, bool is_attached, int64 hash) {
    is_repair_ = is_repair;
    is_attached_ = is_attached;
    int32 flags = 0;
    if (is_attached) {
      flags |= telegram_api::messages_getRecentStickers::ATTACHED_MASK;
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_getRecentStickers(flags, is_attached /*ignored*/, hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getRecentStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for get recent " << (is_attached_ ? "attached " : "")
               << "stickers: " << to_string(ptr);
    td_->stickers_manager_->on_get_recent_stickers(is_repair_, is_attached_, std::move(ptr));
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for get recent " << (is_attached_ ? "attached " : "") << "stickers: " << status;
    }
    td_->stickers_manager_->on_get_recent_stickers_failed(is_repair_, is_attached_, std::move(status));
  }
};

void StickersManager::reload_recent_stickers(bool is_attached, bool force) {
  if (G()->close_flag()) {
    return;
  }

  auto &next_load_time = next_recent_stickers_load_time_[is_attached];
  if (!td_->auth_manager_->is_bot() && next_load_time >= 0 && (next_load_time < Time::now() || force)) {
    LOG_IF(INFO, force) << "Reload recent " << (is_attached ? "attached " : "") << "stickers";
    next_load_time = -1;
    td_->create_handler<GetRecentStickersQuery>()->send(false, is_attached, recent_stickers_hash_[is_attached]);
  }
}

void StickersManager::repair_recent_stickers(bool is_attached, Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Bots have no recent stickers"));
  }

  repair_recent_stickers_queries_[is_attached].push_back(std::move(promise));
  if (repair_recent_stickers_queries_[is_attached].size() == 1u) {
    td_->create_handler<GetRecentStickersQuery>()->send(true, is_attached, 0);
  }
}

class SearchStickersQuery final : public Td::ResultHandler {
  string emoji_;

 public:
  void send(string emoji, int64 hash) {
    emoji_ = std::move(emoji);
    send_query(G()->net_query_creator().create(telegram_api::messages_getStickers(emoji_, hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for search stickers: " << to_string(ptr);
    td_->stickers_manager_->on_find_stickers_success(emoji_, std::move(ptr));
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for search stickers: " << status;
    }
    td_->stickers_manager_->on_find_stickers_fail(emoji_, std::move(status));
  }
};

vector<FileId> StickersManager::search_stickers(string emoji, int32 limit, Promise<Unit> &&promise) {
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }
  if (limit > MAX_FOUND_STICKERS) {
    limit = MAX_FOUND_STICKERS;
  }
  if (emoji.empty()) {
    promise.set_error(Status::Error(400, "Emoji must be non-empty"));
    return {};
  }

  remove_emoji_modifiers_in_place(emoji);
  if (emoji.empty()) {
    promise.set_value(Unit());
    return {};
  }

  auto it = found_stickers_.find(emoji);
  if (it != found_stickers_.end() && Time::now() < it->second.next_reload_time_) {
    promise.set_value(Unit());
    const auto &sticker_ids = it->second.sticker_ids_;
    auto result_size = min(static_cast<size_t>(limit), sticker_ids.size());
    return vector<FileId>(sticker_ids.begin(), sticker_ids.begin() + result_size);
  }

  auto &promises = search_stickers_queries_[emoji];
  promises.push_back(std::move(promise));
  if (promises.size() == 1u) {
    int64 hash = 0;
    if (it != found_stickers_.end()) {
      hash = get_recent_stickers_hash(it->second.sticker_ids_);
    }
    td_->create_handler<SearchStickersQuery>()->send(std::move(emoji), hash);
  }

  return {};
}

void StickersManager::on_update_emoji_sounds() {
  if (G()->close_flag() || !is_inited_ || td_->auth_manager_->is_bot()) {
    return;
  }

  auto emoji_sounds_str = G()->shared_config().get_option_string("emoji_sounds");
  if (emoji_sounds_str == emoji_sounds_str_) {
    return;
  }

  LOG(INFO) << "Change emoji sounds to " << emoji_sounds_str;
  emoji_sounds_str_ = std::move(emoji_sounds_str);

  vector<FileId> old_file_ids;
  for (auto &emoji_sound : emoji_sounds_) {
    old_file_ids.push_back(emoji_sound.second);
  }
  emoji_sounds_.clear();

  vector<FileId> new_file_ids;
  auto sounds = full_split(Slice(emoji_sounds_str_), ',');
  CHECK(sounds.size() % 2 == 0);
  for (size_t i = 0; i < sounds.size(); i += 2) {
    vector<Slice> parts = full_split(sounds[i + 1], ':');
    CHECK(parts.size() == 3);
    auto id = to_integer<int64>(parts[0]);
    auto access_hash = to_integer<int64>(parts[1]);
    auto dc_id = G()->net_query_dispatcher().get_main_dc_id();
    auto file_reference = base64url_decode(parts[2]).move_as_ok();
    int32 expected_size = 7000;
    auto suggested_file_name = PSTRING() << static_cast<uint64>(id) << '.'
                                         << MimeType::to_extension("audio/ogg", "oga");
    auto file_id = td_->file_manager_->register_remote(
        FullRemoteFileLocation(FileType::VoiceNote, id, access_hash, dc_id, std::move(file_reference)),
        FileLocationSource::FromServer, DialogId(), 0, expected_size, std::move(suggested_file_name));
    CHECK(file_id.is_valid());
    emoji_sounds_.emplace(remove_fitzpatrick_modifier(sounds[i]).str(), file_id);
    new_file_ids.push_back(file_id);
  }
  td_->file_manager_->change_files_source(get_app_config_file_source_id(), old_file_ids, new_file_ids);

  try_update_animated_emoji_messages();
}

void StickersManager::on_get_language_codes(const string &key, Result<vector<string>> &&result) {
  auto queries_it = load_language_codes_queries_.find(key);
  CHECK(queries_it != load_language_codes_queries_.end());
  CHECK(!queries_it->second.empty());
  auto promises = std::move(queries_it->second);
  load_language_codes_queries_.erase(queries_it);

  if (result.is_error()) {
    if (!G()->is_expected_error(result.error())) {
      LOG(ERROR) << "Receive " << result.error() << " from GetEmojiKeywordsLanguageQuery";
    }
    for (auto &promise : promises) {
      promise.set_error(result.error().clone());
    }
    return;
  }

  auto language_codes = result.move_as_ok();
  LOG(INFO) << "Receive language codes " << language_codes << " for emojis search with key " << key;
  td::remove_if(language_codes, [](const string &language_code) {
    if (language_code.empty() || language_code.find('$') != string::npos) {
      LOG(ERROR) << "Receive language_code \"" << language_code << '"';
      return true;
    }
    return false;
  });
  if (language_codes.empty()) {
    LOG(ERROR) << "Language codes list is empty";
    language_codes.emplace_back("en");
  }
  td::unique(language_codes);

  auto it = emoji_language_codes_.find(key);
  CHECK(it != emoji_language_codes_.end());
  if (it->second != language_codes) {
    LOG(INFO) << "Update emoji language codes for " << key << " to " << language_codes;
    if (!G()->close_flag()) {
      CHECK(G()->parameters().use_file_db);
      G()->td_db()->get_sqlite_pmc()->set(key, implode(language_codes, '$'), Auto());
    }
    it->second = std::move(language_codes);
  }

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

class GetEmojiUrlQuery final : public Td::ResultHandler {
  Promise<telegram_api::object_ptr<telegram_api::emojiURL>> promise_;

 public:
  explicit GetEmojiUrlQuery(Promise<telegram_api::object_ptr<telegram_api::emojiURL>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const string &language_code) {
    send_query(G()->net_query_creator().create(telegram_api::messages_getEmojiURL(language_code)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getEmojiURL>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

int64 StickersManager::get_emoji_suggestions_url(const string &language_code, Promise<Unit> &&promise) {
  int64 random_id = 0;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || emoji_suggestions_urls_.find(random_id) != emoji_suggestions_urls_.end());
  emoji_suggestions_urls_[random_id];  // reserve place for result

  auto query_promise =
      PromiseCreator::lambda([actor_id = actor_id(this), random_id, promise = std::move(promise)](
                                 Result<telegram_api::object_ptr<telegram_api::emojiURL>> &&result) mutable {
        send_closure(actor_id, &StickersManager::on_get_emoji_suggestions_url, random_id, std::move(promise),
                     std::move(result));
      });
  td_->create_handler<GetEmojiUrlQuery>(std::move(query_promise))->send(language_code);
  return random_id;
}

void StickersManager::load_special_sticker_set_info_from_binlog(SpecialStickerSet &sticker_set) {
  if (G()->parameters().use_file_db) {
    string sticker_set_string = G()->td_db()->get_binlog_pmc()->get(sticker_set.type_.type_);
    if (!sticker_set_string.empty()) {
      auto parts = full_split(sticker_set_string);
      if (parts.size() != 3) {
        LOG(ERROR) << "Can't parse " << sticker_set_string;
      } else {
        auto r_sticker_set_id = to_integer_safe<int64>(parts[0]);
        auto r_sticker_set_access_hash = to_integer_safe<int64>(parts[1]);
        auto sticker_set_name = parts[2];
        if (r_sticker_set_id.is_error() || r_sticker_set_access_hash.is_error() ||
            clean_username(sticker_set_name) != sticker_set_name || sticker_set_name.empty()) {
          LOG(ERROR) << "Can't parse " << sticker_set_string;
        } else {
          init_special_sticker_set(sticker_set, r_sticker_set_id.ok(), r_sticker_set_access_hash.ok(),
                                   std::move(sticker_set_name));
        }
      }
    }
  } else {
    G()->td_db()->get_binlog_pmc()->erase(sticker_set.type_.type_);
  }

  if (!sticker_set.id_.is_valid()) {
    return;
  }

  add_sticker_set(sticker_set.id_, sticker_set.access_hash_);
  short_name_to_sticker_set_id_.emplace(sticker_set.short_name_, sticker_set.id_);
}

void StickersManager::on_install_sticker_set(StickerSetId set_id, bool is_archived,
                                             tl_object_ptr<telegram_api::messages_StickerSetInstallResult> &&result) {
  StickerSet *sticker_set = get_sticker_set(set_id);
  CHECK(sticker_set != nullptr);
  on_update_sticker_set(sticker_set, true, is_archived, true);
  update_sticker_set(sticker_set, "on_install_sticker_set");

  switch (result->get_id()) {
    case telegram_api::messages_stickerSetInstallResultSuccess::ID:
      break;
    case telegram_api::messages_stickerSetInstallResultArchive::ID: {
      auto archived_sets = move_tl_object_as<telegram_api::messages_stickerSetInstallResultArchive>(result);
      for (auto &archived_set_ptr : archived_sets->sets_) {
        StickerSetId archived_sticker_set_id =
            on_get_sticker_set_covered(std::move(archived_set_ptr), true, "on_install_sticker_set");
        if (archived_sticker_set_id.is_valid()) {
          auto archived_sticker_set = get_sticker_set(archived_sticker_set_id);
          CHECK(archived_sticker_set != nullptr);
          update_sticker_set(archived_sticker_set, "on_install_sticker_set 2");
        }
      }
      break;
    }
    default:
      UNREACHABLE();
  }

  send_update_installed_sticker_sets();
}

FileId StickersManager::upload_sticker_file(UserId user_id, tl_object_ptr<td_api::InputSticker> &&sticker,
                                            Promise<Unit> &&promise) {
  bool is_bot = td_->auth_manager_->is_bot();
  if (!is_bot) {
    user_id = td_->contacts_manager_->get_my_id();
  }

  auto r_input_user = td_->contacts_manager_->get_input_user(user_id);
  if (r_input_user.is_error()) {
    promise.set_error(r_input_user.move_as_error());
    return FileId();
  }

  auto r_file_id = prepare_input_sticker(sticker.get());
  if (r_file_id.is_error()) {
    promise.set_error(r_file_id.move_as_error());
    return FileId();
  }
  auto file_id = std::get<0>(r_file_id.ok());
  auto is_url = std::get<1>(r_file_id.ok());
  auto is_local = std::get<2>(r_file_id.ok());

  if (is_url) {
    do_upload_sticker_file(user_id, file_id, nullptr, std::move(promise));
  } else if (is_local) {
    upload_sticker_file(user_id, file_id, std::move(promise));
  } else {
    promise.set_value(Unit());
  }

  return file_id;
}

void StickersManager::load_special_sticker_set(SpecialStickerSet &sticker_set) {
  CHECK(!td_->auth_manager_->is_bot());
  if (sticker_set.is_being_loaded_) {
    return;
  }
  sticker_set.is_being_loaded_ = true;
  LOG(INFO) << "Load " << sticker_set.type_.type_ << " " << sticker_set.id_;
  if (sticker_set.id_.is_valid()) {
    auto s = get_sticker_set(sticker_set.id_);
    CHECK(s != nullptr);
    if (s->was_loaded) {
      reload_special_sticker_set(sticker_set, s->is_loaded ? s->hash : 0);
      return;
    }

    auto promise = PromiseCreator::lambda([actor_id = actor_id(this), type = sticker_set.type_](Result<Unit> &&result) {
      send_closure(actor_id, &StickersManager::on_load_special_sticker_set, type,
                   result.is_ok() ? Status::OK() : result.move_as_error());
    });
    load_sticker_sets({sticker_set.id_}, std::move(promise));
  } else {
    reload_special_sticker_set(sticker_set, 0);
  }
}

Result<std::tuple<FileId, bool, bool, bool>> StickersManager::prepare_input_sticker(td_api::InputSticker *sticker) {
  if (sticker == nullptr) {
    return Status::Error(400, "Input sticker must be non-empty");
  }

  if (!clean_input_string(get_input_sticker_emojis(sticker))) {
    return Status::Error(400, "Emojis must be encoded in UTF-8");
  }

  switch (sticker->get_id()) {
    case td_api::inputStickerStatic::ID:
      return prepare_input_file(static_cast<td_api::inputStickerStatic *>(sticker)->sticker_, false, false);
    case td_api::inputStickerAnimated::ID:
      return prepare_input_file(static_cast<td_api::inputStickerAnimated *>(sticker)->sticker_, true, false);
    default:
      UNREACHABLE();
      return {};
  }
}

int64 StickersManager::get_recent_stickers_hash(const vector<FileId> &sticker_ids) const {
  vector<uint64> numbers;
  numbers.reserve(sticker_ids.size());
  for (auto sticker_id : sticker_ids) {
    auto sticker = get_sticker(sticker_id);
    CHECK(sticker != nullptr);
    auto file_view = td_->file_manager_->get_file_view(sticker_id);
    CHECK(file_view.has_remote_location());
    if (!file_view.remote_location().is_document()) {
      LOG(ERROR) << "Recent sticker remote location is not document: " << file_view.remote_location();
      continue;
    }
    numbers.push_back(file_view.remote_location().get_id());
  }
  return get_vector_hash(numbers);
}

void StickersManager::on_update_disable_animated_emojis() {
  if (G()->close_flag() || !is_inited_ || td_->auth_manager_->is_bot()) {
    return;
  }

  auto disable_animated_emojis = G()->shared_config().get_option_boolean("disable_animated_emoji");
  if (disable_animated_emojis == disable_animated_emojis_) {
    return;
  }
  disable_animated_emojis_ = disable_animated_emojis;
  if (!disable_animated_emojis_) {
    reload_special_sticker_set_by_type(SpecialStickerSetType::animated_emoji());
    reload_special_sticker_set_by_type(SpecialStickerSetType::animated_emoji_click());
  }
  try_update_animated_emoji_messages();
}

void StickersManager::set_old_featured_sticker_set_count(int32 count) {
  if (old_featured_sticker_set_count_ == count) {
    return;
  }

  on_old_featured_sticker_sets_invalidated();

  old_featured_sticker_set_count_ = count;
  need_update_featured_sticker_sets_ = true;

  if (!G()->parameters().use_file_db) {
    return;
  }

  LOG(INFO) << "Save old trending sticker set count " << count << " to binlog";
  G()->td_db()->get_binlog_pmc()->set("old_featured_sticker_set_count", to_string(count));
}

td_api::object_ptr<td_api::animatedEmoji> StickersManager::get_animated_emoji_object(
    std::pair<FileId, int> animated_sticker, FileId sound_file_id) const {
  if (!animated_sticker.first.is_valid()) {
    return nullptr;
  }
  return td_api::make_object<td_api::animatedEmoji>(
      get_sticker_object(animated_sticker.first, true), animated_sticker.second,
      sound_file_id.is_valid() ? td_->file_manager_->get_file_object(sound_file_id) : nullptr);
}

void StickersManager::finish_get_emoji_keywords_difference(string language_code, int32 version) {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Finished to get emoji keywords difference for language " << language_code;
  emoji_language_code_versions_[language_code] = version;
  emoji_language_code_last_difference_times_[language_code] = static_cast<int32>(Time::now_cached());
}

void StickersManager::init_special_sticker_set(SpecialStickerSet &sticker_set, int64 sticker_set_id, int64 access_hash,
                                               string name) {
  sticker_set.id_ = StickerSetId(sticker_set_id);
  sticker_set.access_hash_ = access_hash;
  sticker_set.short_name_ = std::move(name);
}

td_api::object_ptr<td_api::updateRecentStickers> StickersManager::get_update_recent_stickers_object(
    int is_attached) const {
  return td_api::make_object<td_api::updateRecentStickers>(
      is_attached != 0, td_->file_manager_->get_file_ids_object(recent_sticker_ids_[is_attached]));
}

void StickersManager::on_load_sticker_set_fail(StickerSetId sticker_set_id, const Status &error) {
  if (!sticker_set_id.is_valid()) {
    return;
  }
  update_load_requests(get_sticker_set(sticker_set_id), true, error);
}

string StickersManager::get_language_emojis_database_key(const string &language_code, const string &text) {
  return PSTRING() << "emoji$" << language_code << '$' << text;
}

int64 StickersManager::get_favorite_stickers_hash() const {
  return get_recent_stickers_hash(favorite_sticker_ids_);
}
}  // namespace td
