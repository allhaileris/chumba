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


#include "td/telegram/DialogId.h"
#include "td/telegram/Document.h"
#include "td/telegram/DocumentsManager.h"
#include "td/telegram/FileReferenceManager.h"
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

class SetStickerSetThumbnailQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit SetStickerSetThumbnailQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &short_name, tl_object_ptr<telegram_api::InputDocument> &&input_document) {
    send_query(G()->net_query_creator().create(telegram_api::stickers_setStickerSetThumb(
        make_tl_object<telegram_api::inputStickerSetShortName>(short_name), std::move(input_document))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_setStickerSetThumb>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_get_messages_sticker_set(StickerSetId(), result_ptr.move_as_ok(), true,
                                                        "SetStickerSetThumbnailQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

void StickersManager::do_set_sticker_set_thumbnail(UserId user_id, string short_name,
                                                   tl_object_ptr<td_api::InputFile> &&thumbnail,
                                                   Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto it = short_name_to_sticker_set_id_.find(short_name);
  const StickerSet *sticker_set = it == short_name_to_sticker_set_id_.end() ? nullptr : get_sticker_set(it->second);
  if (sticker_set == nullptr || !sticker_set->was_loaded) {
    return promise.set_error(Status::Error(400, "Sticker set not found"));
  }

  auto r_file_id = prepare_input_file(thumbnail, sticker_set->is_animated, true);
  if (r_file_id.is_error()) {
    return promise.set_error(r_file_id.move_as_error());
  }
  auto file_id = std::get<0>(r_file_id.ok());
  auto is_url = std::get<1>(r_file_id.ok());
  auto is_local = std::get<2>(r_file_id.ok());

  if (!file_id.is_valid()) {
    td_->create_handler<SetStickerSetThumbnailQuery>(std::move(promise))
        ->send(short_name, telegram_api::make_object<telegram_api::inputDocumentEmpty>());
    return;
  }

  auto pending_set_sticker_set_thumbnail = make_unique<PendingSetStickerSetThumbnail>();
  pending_set_sticker_set_thumbnail->short_name = short_name;
  pending_set_sticker_set_thumbnail->file_id = file_id;
  pending_set_sticker_set_thumbnail->promise = std::move(promise);

  int64 random_id;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 ||
           pending_set_sticker_set_thumbnails_.find(random_id) != pending_set_sticker_set_thumbnails_.end());
  pending_set_sticker_set_thumbnails_[random_id] = std::move(pending_set_sticker_set_thumbnail);

  auto on_upload_promise = PromiseCreator::lambda([random_id](Result<Unit> result) {
    send_closure(G()->stickers_manager(), &StickersManager::on_sticker_set_thumbnail_uploaded, random_id,
                 std::move(result));
  });

  if (is_url) {
    do_upload_sticker_file(user_id, file_id, nullptr, std::move(on_upload_promise));
  } else if (is_local) {
    upload_sticker_file(user_id, file_id, std::move(on_upload_promise));
  } else {
    on_upload_promise.set_value(Unit());
  }
}

void StickersManager::on_sticker_set_thumbnail_uploaded(int64 random_id, Result<Unit> result) {
  auto it = pending_set_sticker_set_thumbnails_.find(random_id);
  CHECK(it != pending_set_sticker_set_thumbnails_.end());

  auto pending_set_sticker_set_thumbnail = std::move(it->second);
  CHECK(pending_set_sticker_set_thumbnail != nullptr);

  pending_set_sticker_set_thumbnails_.erase(it);

  if (result.is_error()) {
    pending_set_sticker_set_thumbnail->promise.set_error(result.move_as_error());
    return;
  }

  FileView file_view = td_->file_manager_->get_file_view(pending_set_sticker_set_thumbnail->file_id);
  CHECK(file_view.has_remote_location());

  td_->create_handler<SetStickerSetThumbnailQuery>(std::move(pending_set_sticker_set_thumbnail->promise))
      ->send(pending_set_sticker_set_thumbnail->short_name, file_view.main_remote_location().as_input_document());
}

class UploadStickerFileQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;
  bool was_uploaded_ = false;

 public:
  explicit UploadStickerFileQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(tl_object_ptr<telegram_api::InputPeer> &&input_peer, FileId file_id,
            tl_object_ptr<telegram_api::InputMedia> &&input_media) {
    CHECK(input_peer != nullptr);
    CHECK(input_media != nullptr);
    file_id_ = file_id;
    was_uploaded_ = FileManager::extract_was_uploaded(input_media);
    send_query(G()->net_query_creator().create(
        telegram_api::messages_uploadMedia(std::move(input_peer), std::move(input_media))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_uploadMedia>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_uploaded_sticker_file(file_id_, result_ptr.move_as_ok(), std::move(promise_));
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    if (was_uploaded_) {
      CHECK(file_id_.is_valid());
      if (begins_with(status.message(), "FILE_PART_") && ends_with(status.message(), "_MISSING")) {
        // TODO td_->stickers_manager_->on_upload_sticker_file_part_missing(file_id_, to_integer<int32>(status.message().substr(10)));
        // return;
      } else {
        if (status.code() != 429 && status.code() < 500 && !G()->close_flag()) {
          td_->file_manager_->delete_partial_remote_location(file_id_);
        }
      }
    } else if (FileReferenceManager::is_file_reference_error(status)) {
      LOG(ERROR) << "Receive file reference error for UploadStickerFileQuery";
    }
    td_->file_manager_->cancel_upload(file_id_);
    promise_.set_error(std::move(status));
  }
};

void StickersManager::do_upload_sticker_file(UserId user_id, FileId file_id,
                                             tl_object_ptr<telegram_api::InputFile> &&input_file,
                                             Promise<Unit> &&promise) {
  DialogId dialog_id(user_id);
  auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
  if (input_peer == nullptr) {
    return promise.set_error(Status::Error(400, "Have no access to the user"));
  }

  FileView file_view = td_->file_manager_->get_file_view(file_id);
  bool is_animated = file_view.get_type() == FileType::Sticker;

  bool had_input_file = input_file != nullptr;
  auto input_media = is_animated ? get_input_media(file_id, std::move(input_file), nullptr, string())
                                 : td_->documents_manager_->get_input_media(file_id, std::move(input_file), nullptr);
  CHECK(input_media != nullptr);
  if (had_input_file && !FileManager::extract_was_uploaded(input_media)) {
    // if we had InputFile, but has failed to use it, then we need to immediately cancel file upload
    // so the next upload with the same file can succeed
    td_->file_manager_->cancel_upload(file_id);
  }

  td_->create_handler<UploadStickerFileQuery>(std::move(promise))
      ->send(std::move(input_peer), file_id, std::move(input_media));
}

Status StickersManager::on_animated_emoji_message_clicked(Slice emoji, FullMessageId full_message_id, string data) {
  if (td_->auth_manager_->is_bot() || disable_animated_emojis_) {
    return Status::OK();
  }

  TRY_RESULT(value, json_decode(data));
  if (value.type() != JsonValue::Type::Object) {
    return Status::Error("Expected an object");
  }
  auto &object = value.get_object();
  TRY_RESULT(version, get_json_object_int_field(object, "v", false));
  if (version != 1) {
    return Status::OK();
  }
  TRY_RESULT(array_value, get_json_object_field(object, "a", JsonValue::Type::Array, false));
  auto &array = array_value.get_array();
  if (array.size() > 20) {
    return Status::Error("Click array is too big");
  }
  vector<std::pair<int, double>> clicks;
  double previous_start_time = 0.0;
  double adjustment = 0.0;
  for (auto &click : array) {
    if (click.type() != JsonValue::Type::Object) {
      return Status::Error("Expected clicks as JSON objects");
    }
    auto &click_object = click.get_object();
    TRY_RESULT(index, get_json_object_int_field(click_object, "i", false));
    if (index <= 0 || index > 9) {
      return Status::Error("Wrong index");
    }
    TRY_RESULT(start_time, get_json_object_double_field(click_object, "t", false));
    if (!std::isfinite(start_time)) {
      return Status::Error("Receive invalid start time");
    }
    if (start_time < previous_start_time) {
      return Status::Error("Non-monotonic start time");
    }
    if (start_time > previous_start_time + 3) {
      return Status::Error("Too big delay between clicks");
    }
    previous_start_time = start_time;

    auto adjusted_start_time =
        clicks.empty() ? 0.0 : max(start_time + adjustment, clicks.back().second + MIN_ANIMATED_EMOJI_CLICK_DELAY);
    adjustment = adjusted_start_time - start_time;
    clicks.emplace_back(static_cast<int>(index), adjusted_start_time);
  }

  auto &special_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_emoji_click());
  if (special_sticker_set.id_.is_valid()) {
    auto sticker_set = get_sticker_set(special_sticker_set.id_);
    CHECK(sticker_set != nullptr);
    if (sticker_set->was_loaded) {
      schedule_update_animated_emoji_clicked(sticker_set, emoji, full_message_id, std::move(clicks));
      return Status::OK();
    }
  }

  LOG(INFO) << "Waiting for an emoji click sticker set needed in " << full_message_id;
  load_special_sticker_set(special_sticker_set);

  PendingOnAnimatedEmojiClicked pending_request;
  pending_request.emoji_ = emoji.str();
  pending_request.full_message_id_ = full_message_id;
  pending_request.clicks_ = std::move(clicks);
  pending_on_animated_emoji_message_clicked_.push_back(std::move(pending_request));
  return Status::OK();
}

tl_object_ptr<telegram_api::InputMedia> StickersManager::get_input_media(
    FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file,
    tl_object_ptr<telegram_api::InputFile> input_thumbnail, const string &emoji) const {
  auto file_view = td_->file_manager_->get_file_view(file_id);
  if (file_view.is_encrypted()) {
    return nullptr;
  }
  if (file_view.has_remote_location() && !file_view.main_remote_location().is_web() && input_file == nullptr) {
    int32 flags = 0;
    if (!emoji.empty()) {
      flags |= telegram_api::inputMediaDocument::QUERY_MASK;
    }
    return make_tl_object<telegram_api::inputMediaDocument>(flags, file_view.main_remote_location().as_input_document(),
                                                            0, emoji);
  }
  if (file_view.has_url()) {
    return make_tl_object<telegram_api::inputMediaDocumentExternal>(0, file_view.url(), 0);
  }

  if (input_file != nullptr) {
    const Sticker *s = get_sticker(file_id);
    CHECK(s != nullptr);

    vector<tl_object_ptr<telegram_api::DocumentAttribute>> attributes;
    if (s->dimensions.width != 0 && s->dimensions.height != 0) {
      attributes.push_back(
          make_tl_object<telegram_api::documentAttributeImageSize>(s->dimensions.width, s->dimensions.height));
    }
    attributes.push_back(make_tl_object<telegram_api::documentAttributeSticker>(
        0, false /*ignored*/, s->alt, make_tl_object<telegram_api::inputStickerSetEmpty>(), nullptr));

    int32 flags = 0;
    if (input_thumbnail != nullptr) {
      flags |= telegram_api::inputMediaUploadedDocument::THUMB_MASK;
    }
    auto mime_type = get_sticker_mime_type(s);
    if (!s->is_animated && !s->set_id.is_valid()) {
      auto suggested_path = file_view.suggested_path();
      const PathView path_view(suggested_path);
      if (path_view.extension() == "tgs") {
        mime_type = "application/x-tgsticker";
      }
    }
    return make_tl_object<telegram_api::inputMediaUploadedDocument>(
        flags, false /*ignored*/, false /*ignored*/, std::move(input_file), std::move(input_thumbnail), mime_type,
        std::move(attributes), vector<tl_object_ptr<telegram_api::InputDocument>>(), 0);
  } else {
    CHECK(!file_view.has_remote_location());
  }

  return nullptr;
}

StickerSetId StickersManager::on_get_sticker_set_covered(tl_object_ptr<telegram_api::StickerSetCovered> &&set_ptr,
                                                         bool is_changed, const char *source) {
  StickerSetId set_id;
  switch (set_ptr->get_id()) {
    case telegram_api::stickerSetCovered::ID: {
      auto covered_set = move_tl_object_as<telegram_api::stickerSetCovered>(set_ptr);
      set_id = on_get_sticker_set(std::move(covered_set->set_), is_changed, source);
      if (!set_id.is_valid()) {
        break;
      }

      auto sticker_set = get_sticker_set(set_id);
      CHECK(sticker_set != nullptr);
      CHECK(sticker_set->is_inited);
      if (sticker_set->was_loaded) {
        break;
      }
      if (sticker_set->sticker_count == 0) {
        break;
      }

      auto &sticker_ids = sticker_set->sticker_ids;

      auto sticker_id = on_get_sticker_document(std::move(covered_set->cover_)).second;
      if (sticker_id.is_valid() && !td::contains(sticker_ids, sticker_id)) {
        sticker_ids.push_back(sticker_id);
        sticker_set->is_changed = true;
      }

      break;
    }
    case telegram_api::stickerSetMultiCovered::ID: {
      auto multicovered_set = move_tl_object_as<telegram_api::stickerSetMultiCovered>(set_ptr);
      set_id = on_get_sticker_set(std::move(multicovered_set->set_), is_changed, source);
      if (!set_id.is_valid()) {
        break;
      }

      auto sticker_set = get_sticker_set(set_id);
      CHECK(sticker_set != nullptr);
      CHECK(sticker_set->is_inited);
      if (sticker_set->was_loaded) {
        break;
      }
      auto &sticker_ids = sticker_set->sticker_ids;

      for (auto &cover : multicovered_set->covers_) {
        auto sticker_id = on_get_sticker_document(std::move(cover)).second;
        if (sticker_id.is_valid() && !td::contains(sticker_ids, sticker_id)) {
          sticker_ids.push_back(sticker_id);
          sticker_set->is_changed = true;
        }
      }

      break;
    }
    default:
      UNREACHABLE();
  }
  return set_id;
}

class ClearRecentStickersQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  bool is_attached_;

 public:
  explicit ClearRecentStickersQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(bool is_attached) {
    is_attached_ = is_attached;

    int32 flags = 0;
    if (is_attached) {
      flags |= telegram_api::messages_clearRecentStickers::ATTACHED_MASK;
    }

    send_query(
        G()->net_query_creator().create(telegram_api::messages_clearRecentStickers(flags, is_attached /*ignored*/)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_clearRecentStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for clear recent " << (is_attached_ ? "attached " : "") << "stickers: " << result;
    if (!result) {
      td_->stickers_manager_->reload_recent_stickers(is_attached_, true);
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for clear recent " << (is_attached_ ? "attached " : "") << "stickers: " << status;
    }
    td_->stickers_manager_->reload_recent_stickers(is_attached_, true);
    promise_.set_error(std::move(status));
  }
};

void StickersManager::clear_recent_stickers(bool is_attached, Promise<Unit> &&promise) {
  if (!are_recent_stickers_loaded_[is_attached]) {
    load_recent_stickers(is_attached, std::move(promise));
    return;
  }

  vector<FileId> &sticker_ids = recent_sticker_ids_[is_attached];
  if (sticker_ids.empty()) {
    return promise.set_value(Unit());
  }

  // TODO invokeAfter
  td_->create_handler<ClearRecentStickersQuery>(std::move(promise))->send(is_attached);

  sticker_ids.clear();

  send_update_recent_stickers(is_attached);
}

void StickersManager::on_get_favorite_stickers(
    bool is_repair, tl_object_ptr<telegram_api::messages_FavedStickers> &&favorite_stickers_ptr) {
  CHECK(!td_->auth_manager_->is_bot());
  if (!is_repair) {
    next_favorite_stickers_load_time_ = Time::now_cached() + Random::fast(30 * 60, 50 * 60);
  }

  CHECK(favorite_stickers_ptr != nullptr);
  int32 constructor_id = favorite_stickers_ptr->get_id();
  if (constructor_id == telegram_api::messages_favedStickersNotModified::ID) {
    if (is_repair) {
      return on_get_favorite_stickers_failed(true, Status::Error(500, "Failed to reload favorite stickers"));
    }
    LOG(INFO) << "Favorite stickers are not modified";
    return;
  }
  CHECK(constructor_id == telegram_api::messages_favedStickers::ID);
  auto favorite_stickers = move_tl_object_as<telegram_api::messages_favedStickers>(favorite_stickers_ptr);

  // TODO use favorite_stickers->packs_

  vector<FileId> favorite_sticker_ids;
  favorite_sticker_ids.reserve(favorite_stickers->stickers_.size());
  for (auto &document_ptr : favorite_stickers->stickers_) {
    auto sticker_id = on_get_sticker_document(std::move(document_ptr)).second;
    if (!sticker_id.is_valid()) {
      continue;
    }

    favorite_sticker_ids.push_back(sticker_id);
  }

  if (is_repair) {
    auto promises = std::move(repair_favorite_stickers_queries_);
    repair_favorite_stickers_queries_.clear();
    for (auto &promise : promises) {
      promise.set_value(Unit());
    }
  } else {
    on_load_favorite_stickers_finished(std::move(favorite_sticker_ids));

    LOG_IF(ERROR, get_favorite_stickers_hash() != favorite_stickers->hash_) << "Favorite stickers hash mismatch";
  }
}

class GetEmojiKeywordsQuery final : public Td::ResultHandler {
  Promise<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> promise_;

 public:
  explicit GetEmojiKeywordsQuery(Promise<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const string &language_code) {
    send_query(G()->net_query_creator().create(telegram_api::messages_getEmojiKeywords(language_code)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getEmojiKeywords>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void StickersManager::load_emoji_keywords(const string &language_code, Promise<Unit> &&promise) {
  auto &promises = load_emoji_keywords_queries_[language_code];
  promises.push_back(std::move(promise));
  if (promises.size() != 1) {
    // query has already been sent, just wait for the result
    return;
  }

  auto query_promise = PromiseCreator::lambda(
      [actor_id = actor_id(this),
       language_code](Result<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> &&result) mutable {
        send_closure(actor_id, &StickersManager::on_get_emoji_keywords, language_code, std::move(result));
      });
  td_->create_handler<GetEmojiKeywordsQuery>(std::move(query_promise))->send(language_code);
}

tl_object_ptr<td_api::stickerSet> StickersManager::get_sticker_set_object(StickerSetId sticker_set_id) const {
  const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
  CHECK(sticker_set != nullptr);
  CHECK(sticker_set->was_loaded);
  sticker_set->was_update_sent = true;

  std::vector<tl_object_ptr<td_api::sticker>> stickers;
  std::vector<tl_object_ptr<td_api::emojis>> emojis;
  for (auto sticker_id : sticker_set->sticker_ids) {
    stickers.push_back(get_sticker_object(sticker_id));

    vector<string> sticker_emojis;
    auto it = sticker_set->sticker_emojis_map_.find(sticker_id);
    if (it != sticker_set->sticker_emojis_map_.end()) {
      sticker_emojis = it->second;
    }
    emojis.push_back(make_tl_object<td_api::emojis>(std::move(sticker_emojis)));
  }
  auto thumbnail = get_thumbnail_object(td_->file_manager_.get(), sticker_set->thumbnail,
                                        sticker_set->is_animated ? PhotoFormat::Tgs : PhotoFormat::Webp);
  return make_tl_object<td_api::stickerSet>(
      sticker_set->id.get(), sticker_set->title, sticker_set->short_name, std::move(thumbnail),
      get_sticker_minithumbnail(sticker_set->minithumbnail, sticker_set->id, -2, 1.0),
      sticker_set->is_installed && !sticker_set->is_archived, sticker_set->is_archived, sticker_set->is_official,
      sticker_set->is_animated, sticker_set->is_masks, sticker_set->is_viewed, std::move(stickers), std::move(emojis));
}

class SuggestStickerSetShortNameQuery final : public Td::ResultHandler {
  Promise<string> promise_;

 public:
  explicit SuggestStickerSetShortNameQuery(Promise<string> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &title) {
    send_query(G()->net_query_creator().create(telegram_api::stickers_suggestShortName(title)));
  }
  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_suggestShortName>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    promise_.set_value(std::move(ptr->short_name_));
  }

  void on_error(Status status) final {
    if (status.message() == "TITLE_INVALID") {
      return promise_.set_value(string());
    }
    promise_.set_error(std::move(status));
  }
};

void StickersManager::get_suggested_sticker_set_name(string title, Promise<string> &&promise) {
  title = strip_empty_characters(title, MAX_STICKER_SET_TITLE_LENGTH);
  if (title.empty()) {
    return promise.set_error(Status::Error(400, "Sticker set title can't be empty"));
  }

  td_->create_handler<SuggestStickerSetShortNameQuery>(std::move(promise))->send(title);
}

void StickersManager::load_recent_stickers(bool is_attached, Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    are_recent_stickers_loaded_[is_attached] = true;
  }
  if (are_recent_stickers_loaded_[is_attached]) {
    promise.set_value(Unit());
    return;
  }
  load_recent_stickers_queries_[is_attached].push_back(std::move(promise));
  if (load_recent_stickers_queries_[is_attached].size() == 1u) {
    if (G()->parameters().use_file_db) {
      LOG(INFO) << "Trying to load recent " << (is_attached ? "attached " : "") << "stickers from database";
      G()->td_db()->get_sqlite_pmc()->get(
          is_attached ? "ssr1" : "ssr0", PromiseCreator::lambda([is_attached](string value) {
            send_closure(G()->stickers_manager(), &StickersManager::on_load_recent_stickers_from_database, is_attached,
                         std::move(value));
          }));
    } else {
      LOG(INFO) << "Trying to load recent " << (is_attached ? "attached " : "") << "stickers from server";
      reload_recent_stickers(is_attached, true);
    }
  }
}

void StickersManager::try_update_animated_emoji_messages() {
  auto sticker_set = get_animated_emoji_sticker_set();
  vector<FullMessageId> full_message_ids;
  for (auto &it : emoji_messages_) {
    auto new_animated_sticker = get_animated_emoji_sticker(sticker_set, it.first);
    auto new_sound_file_id = get_animated_emoji_sound_file_id(it.first);
    if (new_animated_sticker != it.second.animated_emoji_sticker ||
        (new_animated_sticker.first.is_valid() && new_sound_file_id != it.second.sound_file_id)) {
      it.second.animated_emoji_sticker = new_animated_sticker;
      it.second.sound_file_id = new_sound_file_id;
      for (const auto &full_message_id : it.second.full_message_ids) {
        full_message_ids.push_back(full_message_id);
      }
    }
  }
  for (const auto &full_message_id : full_message_ids) {
    td_->messages_manager_->on_external_update_message_content(full_message_id);
  }
}

StickerSetId StickersManager::search_sticker_set(const string &short_name_to_search, Promise<Unit> &&promise) {
  string short_name = clean_username(short_name_to_search);
  auto it = short_name_to_sticker_set_id_.find(short_name);
  const StickerSet *sticker_set = it == short_name_to_sticker_set_id_.end() ? nullptr : get_sticker_set(it->second);

  if (sticker_set == nullptr) {
    auto set_to_load = make_tl_object<telegram_api::inputStickerSetShortName>(short_name);
    do_reload_sticker_set(StickerSetId(), std::move(set_to_load), 0, std::move(promise));
    return StickerSetId();
  }

  if (update_sticker_set_cache(sticker_set, promise)) {
    return StickerSetId();
  }

  promise.set_value(Unit());
  return sticker_set->id;
}

void StickersManager::on_upload_sticker_file_error(FileId file_id, Status status) {
  if (G()->close_flag()) {
    // do not fail upload if closing
    return;
  }

  LOG(WARNING) << "Sticker file " << file_id << " has upload error " << status;
  CHECK(status.is_error());

  auto it = being_uploaded_files_.find(file_id);
  CHECK(it != being_uploaded_files_.end());

  auto promise = std::move(it->second.second);

  being_uploaded_files_.erase(it);

  // TODO FILE_PART_X_MISSING support

  promise.set_error(Status::Error(status.code() > 0 ? status.code() : 500,
                                  status.message()));  // TODO CHECK that status has always a code
}

void StickersManager::add_favorite_sticker(const tl_object_ptr<td_api::InputFile> &input_file,
                                           Promise<Unit> &&promise) {
  if (!are_favorite_stickers_loaded_) {
    load_favorite_stickers(std::move(promise));
    return;
  }

  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Sticker, input_file, DialogId(), false, false);
  if (r_file_id.is_error()) {
    return promise.set_error(Status::Error(400, r_file_id.error().message()));  // TODO do not drop error code
  }

  add_favorite_sticker_impl(r_file_id.ok(), true, std::move(promise));
}

void StickersManager::on_find_stickers_fail(const string &emoji, Status &&error) {
  if (found_stickers_.count(emoji) != 0) {
    found_stickers_[emoji].cache_time_ = Random::fast(40, 80);
    return on_find_stickers_success(emoji, make_tl_object<telegram_api::messages_stickersNotModified>());
  }

  auto it = search_stickers_queries_.find(emoji);
  CHECK(it != search_stickers_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  search_stickers_queries_.erase(it);

  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

int32 StickersManager::get_emoji_language_code_version(const string &language_code) {
  auto it = emoji_language_code_versions_.find(language_code);
  if (it != emoji_language_code_versions_.end()) {
    return it->second;
  }
  auto &result = emoji_language_code_versions_[language_code];
  result = to_integer<int32>(
      G()->td_db()->get_sqlite_sync_pmc()->get(get_emoji_language_code_version_database_key(language_code)));
  return result;
}

tl_object_ptr<td_api::stickers> StickersManager::get_stickers_object(const vector<FileId> &sticker_ids) const {
  auto result = make_tl_object<td_api::stickers>();
  result->stickers_.reserve(sticker_ids.size());
  for (auto sticker_id : sticker_ids) {
    result->stickers_.push_back(get_sticker_object(sticker_id));
  }
  return result;
}

void StickersManager::on_update_sticker_sets() {
  // TODO better support
  archived_sticker_set_ids_[0].clear();
  total_archived_sticker_set_count_[0] = -1;
  reload_installed_sticker_sets(false, true);

  archived_sticker_set_ids_[1].clear();
  total_archived_sticker_set_count_[1] = -1;
  reload_installed_sticker_sets(true, true);
}

FileSourceId StickersManager::get_favorite_stickers_file_source_id() {
  if (!favorite_stickers_file_source_id_.is_valid()) {
    favorite_stickers_file_source_id_ = td_->file_reference_manager_->create_favorite_stickers_file_source();
  }
  return favorite_stickers_file_source_id_;
}

vector<int64> StickersManager::convert_sticker_set_ids(const vector<StickerSetId> &sticker_set_ids) {
  return transform(sticker_set_ids, [](StickerSetId sticker_set_id) { return sticker_set_id.get(); });
}

void StickersManager::add_recent_sticker_by_id(bool is_attached, FileId sticker_id) {
  // TODO log event
  add_recent_sticker_impl(is_attached, sticker_id, false, Auto());
}

string StickersManager::get_sticker_mime_type(const Sticker *s) {
  return s->is_animated ? "application/x-tgsticker" : "image/webp";
}
}  // namespace td
