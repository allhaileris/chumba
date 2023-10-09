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

class InstallStickerSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  StickerSetId set_id_;
  bool is_archived_;

 public:
  explicit InstallStickerSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(StickerSetId set_id, tl_object_ptr<telegram_api::InputStickerSet> &&input_set, bool is_archived) {
    set_id_ = set_id;
    is_archived_ = is_archived;
    send_query(
        G()->net_query_creator().create(telegram_api::messages_installStickerSet(std::move(input_set), is_archived)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_installStickerSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_install_sticker_set(set_id_, is_archived_, result_ptr.move_as_ok());

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

class UninstallStickerSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  StickerSetId set_id_;

 public:
  explicit UninstallStickerSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(StickerSetId set_id, tl_object_ptr<telegram_api::InputStickerSet> &&input_set) {
    set_id_ = set_id;
    send_query(G()->net_query_creator().create(telegram_api::messages_uninstallStickerSet(std::move(input_set))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_uninstallStickerSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    if (!result) {
      LOG(WARNING) << "Receive false in result to uninstallStickerSet";
    } else {
      td_->stickers_manager_->on_uninstall_sticker_set(set_id_);
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

void StickersManager::change_sticker_set(StickerSetId set_id, bool is_installed, bool is_archived,
                                         Promise<Unit> &&promise) {
  if (is_installed && is_archived) {
    return promise.set_error(Status::Error(400, "Sticker set can't be installed and archived simultaneously"));
  }
  const StickerSet *sticker_set = get_sticker_set(set_id);
  if (sticker_set == nullptr) {
    return promise.set_error(Status::Error(400, "Sticker set not found"));
  }
  if (!sticker_set->is_inited) {
    load_sticker_sets({set_id}, std::move(promise));
    return;
  }
  if (!are_installed_sticker_sets_loaded_[sticker_set->is_masks]) {
    load_installed_sticker_sets(sticker_set->is_masks, std::move(promise));
    return;
  }

  if (is_archived) {
    is_installed = true;
  }
  if (is_installed) {
    if (sticker_set->is_installed && is_archived == sticker_set->is_archived) {
      return promise.set_value(Unit());
    }

    td_->create_handler<InstallStickerSetQuery>(std::move(promise))
        ->send(set_id, get_input_sticker_set(sticker_set), is_archived);
    return;
  }

  if (!sticker_set->is_installed) {
    return promise.set_value(Unit());
  }

  td_->create_handler<UninstallStickerSetQuery>(std::move(promise))->send(set_id, get_input_sticker_set(sticker_set));
}

void StickersManager::choose_animated_emoji_click_sticker(const StickerSet *sticker_set, Slice message_text,
                                                          FullMessageId full_message_id, double start_time,
                                                          Promise<td_api::object_ptr<td_api::sticker>> &&promise) {
  CHECK(sticker_set->was_loaded);
  message_text = remove_emoji_modifiers(message_text);
  if (message_text.empty()) {
    return promise.set_error(Status::Error(400, "Message is not an animated emoji message"));
  }

  if (disable_animated_emojis_ || td_->auth_manager_->is_bot()) {
    return promise.set_value(nullptr);
  }

  auto now = Time::now();
  if (last_clicked_animated_emoji_ == message_text && last_clicked_animated_emoji_full_message_id_ == full_message_id &&
      next_click_animated_emoji_message_time_ >= now + 2 * MIN_ANIMATED_EMOJI_CLICK_DELAY) {
    return promise.set_value(nullptr);
  }

  auto all_sticker_ids = get_animated_emoji_click_stickers(sticker_set, message_text);
  vector<std::pair<int, FileId>> found_stickers;
  for (auto sticker_id : all_sticker_ids) {
    auto it = sticker_set->sticker_emojis_map_.find(sticker_id);
    if (it != sticker_set->sticker_emojis_map_.end()) {
      for (auto &emoji : it->second) {
        auto number = get_emoji_number(emoji);
        if (number > 0) {
          found_stickers.emplace_back(number, sticker_id);
        }
      }
    }
  }
  if (found_stickers.empty()) {
    return promise.set_value(nullptr);
  }

  if (last_clicked_animated_emoji_full_message_id_ != full_message_id) {
    flush_pending_animated_emoji_clicks();
    last_clicked_animated_emoji_full_message_id_ = full_message_id;
  }
  if (last_clicked_animated_emoji_ != message_text) {
    pending_animated_emoji_clicks_.clear();
    last_clicked_animated_emoji_ = message_text.str();
  }

  if (!pending_animated_emoji_clicks_.empty() && found_stickers.size() >= 2) {
    for (auto it = found_stickers.begin(); it != found_stickers.end(); ++it) {
      if (it->first == pending_animated_emoji_clicks_.back().first) {
        found_stickers.erase(it);
        break;
      }
    }
  }

  CHECK(!found_stickers.empty());
  auto result = found_stickers[Random::fast(0, narrow_cast<int>(found_stickers.size()) - 1)];

  pending_animated_emoji_clicks_.emplace_back(result.first, start_time);
  if (pending_animated_emoji_clicks_.size() == 5) {
    flush_pending_animated_emoji_clicks();
  } else {
    set_timeout_in(0.5);
  }
  if (now >= next_click_animated_emoji_message_time_) {
    next_click_animated_emoji_message_time_ = now + MIN_ANIMATED_EMOJI_CLICK_DELAY;
    promise.set_value(get_sticker_object(result.second, false, true));
  } else {
    create_actor<SleepActor>("SendClickAnimatedEmojiMessageResponse", next_click_animated_emoji_message_time_ - now,
                             PromiseCreator::lambda([actor_id = actor_id(this), sticker_id = result.second,
                                                     promise = std::move(promise)](Result<Unit> result) mutable {
                               send_closure(actor_id, &StickersManager::send_click_animated_emoji_message_response,
                                            sticker_id, std::move(promise));
                             }))
        .release();
    next_click_animated_emoji_message_time_ += MIN_ANIMATED_EMOJI_CLICK_DELAY;
  }
}

std::pair<int64, FileId> StickersManager::on_get_sticker_document(
    tl_object_ptr<telegram_api::Document> &&document_ptr) {
  int32 document_constructor_id = document_ptr->get_id();
  if (document_constructor_id == telegram_api::documentEmpty::ID) {
    LOG(ERROR) << "Empty sticker document received";
    return {};
  }
  CHECK(document_constructor_id == telegram_api::document::ID);
  auto document = move_tl_object_as<telegram_api::document>(document_ptr);

  if (!DcId::is_valid(document->dc_id_)) {
    LOG(ERROR) << "Wrong dc_id = " << document->dc_id_ << " in document " << to_string(document);
    return {};
  }
  auto dc_id = DcId::internal(document->dc_id_);

  Dimensions dimensions;
  tl_object_ptr<telegram_api::documentAttributeSticker> sticker;
  for (auto &attribute : document->attributes_) {
    switch (attribute->get_id()) {
      case telegram_api::documentAttributeImageSize::ID: {
        auto image_size = move_tl_object_as<telegram_api::documentAttributeImageSize>(attribute);
        dimensions = get_dimensions(image_size->w_, image_size->h_, "sticker documentAttributeImageSize");
        break;
      }
      case telegram_api::documentAttributeSticker::ID:
        sticker = move_tl_object_as<telegram_api::documentAttributeSticker>(attribute);
        break;
      default:
        continue;
    }
  }
  if (sticker == nullptr) {
    if (document->mime_type_ != "application/x-bad-tgsticker") {
      LOG(ERROR) << "Have no attributeSticker in sticker " << to_string(document);
    }
    return {};
  }

  bool is_animated = document->mime_type_ == "application/x-tgsticker";
  int64 document_id = document->id_;
  FileId sticker_id =
      td_->file_manager_->register_remote(FullRemoteFileLocation(FileType::Sticker, document_id, document->access_hash_,
                                                                 dc_id, document->file_reference_.as_slice().str()),
                                          FileLocationSource::FromServer, DialogId(), document->size_, 0,
                                          PSTRING() << document_id << (is_animated ? ".tgs" : ".webp"));

  PhotoSize thumbnail;
  string minithumbnail;
  auto thumbnail_format = has_webp_thumbnail(document->thumbs_) ? PhotoFormat::Webp : PhotoFormat::Jpeg;
  for (auto &thumb : document->thumbs_) {
    auto photo_size = get_photo_size(td_->file_manager_.get(), PhotoSizeSource::thumbnail(FileType::Thumbnail, 0),
                                     document_id, document->access_hash_, document->file_reference_.as_slice().str(),
                                     dc_id, DialogId(), std::move(thumb), thumbnail_format);
    if (photo_size.get_offset() == 0) {
      if (!thumbnail.file_id.is_valid()) {
        thumbnail = std::move(photo_size.get<0>());
      }
      break;
    } else {
      if (thumbnail_format == PhotoFormat::Webp) {
        minithumbnail = std::move(photo_size.get<1>());
      }
    }
  }

  create_sticker(sticker_id, std::move(minithumbnail), std::move(thumbnail), dimensions, std::move(sticker),
                 is_animated, nullptr);
  return {document_id, sticker_id};
}

class GetStickerSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  StickerSetId sticker_set_id_;
  string sticker_set_name_;

 public:
  explicit GetStickerSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(StickerSetId sticker_set_id, tl_object_ptr<telegram_api::InputStickerSet> &&input_sticker_set, int32 hash) {
    sticker_set_id_ = sticker_set_id;
    if (input_sticker_set->get_id() == telegram_api::inputStickerSetShortName::ID) {
      sticker_set_name_ =
          static_cast<const telegram_api::inputStickerSetShortName *>(input_sticker_set.get())->short_name_;
    }
    send_query(
        G()->net_query_creator().create(telegram_api::messages_getStickerSet(std::move(input_sticker_set), hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getStickerSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto set_ptr = result_ptr.move_as_ok();
    if (set_ptr->get_id() == telegram_api::messages_stickerSet::ID) {
      auto set = static_cast<telegram_api::messages_stickerSet *>(set_ptr.get());
      constexpr int64 GREAT_MINDS_COLOR_SET_ID = 151353307481243663;
      if (set->set_->id_ == GREAT_MINDS_COLOR_SET_ID) {
        string great_minds_name = "TelegramGreatMinds";
        if (sticker_set_id_.get() == StickersManager::GREAT_MINDS_SET_ID ||
            trim(to_lower(sticker_set_name_)) == to_lower(great_minds_name)) {
          set->set_->id_ = StickersManager::GREAT_MINDS_SET_ID;
          set->set_->short_name_ = std::move(great_minds_name);
        }
      }
    }

    td_->stickers_manager_->on_get_messages_sticker_set(sticker_set_id_, std::move(set_ptr), true,
                                                        "GetStickerSetQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for GetStickerSetQuery: " << status;
    td_->stickers_manager_->on_load_sticker_set_fail(sticker_set_id_, status);
    promise_.set_error(std::move(status));
  }
};

void StickersManager::do_reload_sticker_set(StickerSetId sticker_set_id,
                                            tl_object_ptr<telegram_api::InputStickerSet> &&input_sticker_set,
                                            int32 hash, Promise<Unit> &&promise) const {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  td_->create_handler<GetStickerSetQuery>(std::move(promise))->send(sticker_set_id, std::move(input_sticker_set), hash);
}

Result<std::tuple<FileId, bool, bool, bool>> StickersManager::prepare_input_file(
    const tl_object_ptr<td_api::InputFile> &input_file, bool is_animated, bool for_thumbnail) {
  auto r_file_id = td_->file_manager_->get_input_file_id(is_animated ? FileType::Sticker : FileType::Document,
                                                         input_file, DialogId(), for_thumbnail, false);
  if (r_file_id.is_error()) {
    return Status::Error(400, r_file_id.error().message());
  }
  auto file_id = r_file_id.move_as_ok();
  if (file_id.empty()) {
    return std::make_tuple(FileId(), false, false, false);
  }

  if (is_animated) {
    int32 width = for_thumbnail ? 100 : 512;
    create_sticker(file_id, string(), PhotoSize(), get_dimensions(width, width, "prepare_input_file"), nullptr, true,
                   nullptr);
  } else {
    td_->documents_manager_->create_document(file_id, string(), PhotoSize(), "sticker.png", "image/png", false);
  }

  FileView file_view = td_->file_manager_->get_file_view(file_id);
  if (file_view.is_encrypted()) {
    return Status::Error(400, "Can't use encrypted file");
  }

  if (file_view.has_remote_location() && file_view.main_remote_location().is_web()) {
    return Status::Error(400, "Can't use web file to create a sticker");
  }
  bool is_url = false;
  bool is_local = false;
  if (file_view.has_remote_location()) {
    CHECK(file_view.main_remote_location().is_document());
  } else {
    if (file_view.has_url()) {
      is_url = true;
    } else {
      auto max_file_size = [&] {
        if (for_thumbnail) {
          return is_animated ? MAX_ANIMATED_THUMBNAIL_FILE_SIZE : MAX_THUMBNAIL_FILE_SIZE;
        } else {
          return is_animated ? MAX_ANIMATED_STICKER_FILE_SIZE : MAX_STICKER_FILE_SIZE;
        }
      }();
      if (file_view.has_local_location() && file_view.expected_size() > max_file_size) {
        return Status::Error(400, "File is too big");
      }
      is_local = true;
    }
  }
  return std::make_tuple(file_id, is_url, is_local, is_animated);
}

tl_object_ptr<td_api::DiceStickers> StickersManager::get_dice_stickers_object(const string &emoji, int32 value) const {
  if (td_->auth_manager_->is_bot()) {
    return nullptr;
  }
  if (!td::contains(dice_emojis_, emoji)) {
    return nullptr;
  }

  auto it = special_sticker_sets_.find(SpecialStickerSetType::animated_dice(emoji));
  if (it == special_sticker_sets_.end()) {
    return nullptr;
  }

  auto sticker_set_id = it->second.id_;
  if (!sticker_set_id.is_valid()) {
    return nullptr;
  }

  auto sticker_set = get_sticker_set(sticker_set_id);
  CHECK(sticker_set != nullptr);
  if (!sticker_set->was_loaded) {
    return nullptr;
  }

  auto get_sticker = [&](int32 value) {
    return get_sticker_object(sticker_set->sticker_ids[value], true);
  };

  if (emoji == "🎰") {
    if (sticker_set->sticker_ids.size() < 21 || value < 0 || value > 64) {
      return nullptr;
    }

    int32 background_id = value == 1 || value == 22 || value == 43 || value == 64 ? 1 : 0;
    int32 lever_id = 2;
    int32 left_reel_id = value == 64 ? 3 : 8;
    int32 center_reel_id = value == 64 ? 9 : 14;
    int32 right_reel_id = value == 64 ? 15 : 20;
    if (value != 0 && value != 64) {
      left_reel_id = 4 + (value % 4);
      center_reel_id = 10 + ((value + 3) / 4 % 4);
      right_reel_id = 16 + ((value + 15) / 16 % 4);
    }
    return td_api::make_object<td_api::diceStickersSlotMachine>(get_sticker(background_id), get_sticker(lever_id),
                                                                get_sticker(left_reel_id), get_sticker(center_reel_id),
                                                                get_sticker(right_reel_id));
  }

  if (value >= 0 && value < static_cast<int32>(sticker_set->sticker_ids.size())) {
    return td_api::make_object<td_api::diceStickersRegular>(get_sticker(value));
  }
  return nullptr;
}

void StickersManager::on_get_recent_stickers(bool is_repair, bool is_attached,
                                             tl_object_ptr<telegram_api::messages_RecentStickers> &&stickers_ptr) {
  CHECK(!td_->auth_manager_->is_bot());
  if (!is_repair) {
    next_recent_stickers_load_time_[is_attached] = Time::now_cached() + Random::fast(30 * 60, 50 * 60);
  }

  CHECK(stickers_ptr != nullptr);
  int32 constructor_id = stickers_ptr->get_id();
  if (constructor_id == telegram_api::messages_recentStickersNotModified::ID) {
    if (is_repair) {
      return on_get_recent_stickers_failed(true, is_attached, Status::Error(500, "Failed to reload recent stickers"));
    }
    LOG(INFO) << (is_attached ? "Attached r" : "R") << "ecent stickers are not modified";
    return;
  }
  CHECK(constructor_id == telegram_api::messages_recentStickers::ID);
  auto stickers = move_tl_object_as<telegram_api::messages_recentStickers>(stickers_ptr);

  vector<FileId> recent_sticker_ids;
  recent_sticker_ids.reserve(stickers->stickers_.size());
  for (auto &document_ptr : stickers->stickers_) {
    auto sticker_id = on_get_sticker_document(std::move(document_ptr)).second;
    if (!sticker_id.is_valid()) {
      continue;
    }
    recent_sticker_ids.push_back(sticker_id);
  }

  if (is_repair) {
    auto promises = std::move(repair_recent_stickers_queries_[is_attached]);
    repair_recent_stickers_queries_[is_attached].clear();
    for (auto &promise : promises) {
      promise.set_value(Unit());
    }
  } else {
    on_load_recent_stickers_finished(is_attached, std::move(recent_sticker_ids));

    LOG_IF(ERROR, recent_stickers_hash_[is_attached] != stickers->hash_) << "Stickers hash mismatch";
  }
}

class GetOldFeaturedStickerSetsQuery final : public Td::ResultHandler {
  int32 offset_;
  int32 limit_;
  uint32 generation_;

 public:
  void send(int32 offset, int32 limit, uint32 generation) {
    offset_ = offset;
    limit_ = limit;
    generation_ = generation;
    send_query(G()->net_query_creator().create(telegram_api::messages_getOldFeaturedStickers(offset, limit, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getOldFeaturedStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for GetOldFeaturedStickerSetsQuery: " << to_string(ptr);
    td_->stickers_manager_->on_get_featured_sticker_sets(offset_, limit_, generation_, std::move(ptr));
  }

  void on_error(Status status) final {
    td_->stickers_manager_->on_get_featured_sticker_sets_failed(offset_, limit_, generation_, std::move(status));
  }
};

void StickersManager::reload_old_featured_sticker_sets(uint32 generation) {
  if (generation != 0 && generation != old_featured_sticker_set_generation_) {
    return;
  }
  td_->create_handler<GetOldFeaturedStickerSetsQuery>()->send(static_cast<int32>(old_featured_sticker_set_ids_.size()),
                                                              OLD_FEATURED_STICKER_SET_SLICE_SIZE,
                                                              old_featured_sticker_set_generation_);
}

std::pair<FileId, int> StickersManager::get_animated_emoji_sticker(const StickerSet *sticker_set, const string &emoji) {
  if (sticker_set == nullptr) {
    return {};
  }

  auto emoji_without_modifiers = remove_emoji_modifiers(emoji).str();
  auto it = sticker_set->emoji_stickers_map_.find(emoji_without_modifiers);
  if (it == sticker_set->emoji_stickers_map_.end()) {
    return {};
  }

  auto emoji_without_selectors = remove_emoji_selectors(emoji);
  // trying to find full emoji match
  for (const auto &sticker_id : it->second) {
    auto emoji_it = sticker_set->sticker_emojis_map_.find(sticker_id);
    CHECK(emoji_it != sticker_set->sticker_emojis_map_.end());
    for (auto &sticker_emoji : emoji_it->second) {
      if (remove_emoji_selectors(sticker_emoji) == emoji_without_selectors) {
        return {sticker_id, 0};
      }
    }
  }

  // trying to find match without Fitzpatrick modifiers
  int modifier_id = get_fitzpatrick_modifier(emoji_without_selectors);
  if (modifier_id > 0) {
    for (const auto &sticker_id : it->second) {
      auto emoji_it = sticker_set->sticker_emojis_map_.find(sticker_id);
      CHECK(emoji_it != sticker_set->sticker_emojis_map_.end());
      for (auto &sticker_emoji : emoji_it->second) {
        if (remove_emoji_selectors(sticker_emoji) == Slice(emoji_without_selectors).remove_suffix(4)) {
          return {sticker_id, modifier_id};
        }
      }
    }
  }

  // there is no match
  return {};
}

class GetFeaturedStickerSetsQuery final : public Td::ResultHandler {
 public:
  void send(int64 hash) {
    send_query(G()->net_query_creator().create(telegram_api::messages_getFeaturedStickers(hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getFeaturedStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for GetFeaturedStickerSetsQuery: " << to_string(ptr);
    td_->stickers_manager_->on_get_featured_sticker_sets(-1, -1, 0, std::move(ptr));
  }

  void on_error(Status status) final {
    td_->stickers_manager_->on_get_featured_sticker_sets_failed(-1, -1, 0, std::move(status));
  }
};

void StickersManager::reload_featured_sticker_sets(bool force) {
  if (G()->close_flag()) {
    return;
  }

  auto &next_load_time = next_featured_sticker_sets_load_time_;
  if (!td_->auth_manager_->is_bot() && next_load_time >= 0 && (next_load_time < Time::now() || force)) {
    LOG_IF(INFO, force) << "Reload trending sticker sets";
    next_load_time = -1;
    td_->create_handler<GetFeaturedStickerSetsQuery>()->send(featured_sticker_sets_hash_);
  }
}

void StickersManager::load_featured_sticker_sets(Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    are_featured_sticker_sets_loaded_ = true;
    old_featured_sticker_set_count_ = 0;
  }
  if (are_featured_sticker_sets_loaded_) {
    promise.set_value(Unit());
    return;
  }
  load_featured_sticker_sets_queries_.push_back(std::move(promise));
  if (load_featured_sticker_sets_queries_.size() == 1u) {
    if (G()->parameters().use_file_db) {
      LOG(INFO) << "Trying to load trending sticker sets from database";
      G()->td_db()->get_sqlite_pmc()->get("sssfeatured", PromiseCreator::lambda([](string value) {
                                            send_closure(G()->stickers_manager(),
                                                         &StickersManager::on_load_featured_sticker_sets_from_database,
                                                         std::move(value));
                                          }));
    } else {
      LOG(INFO) << "Trying to load trending sticker sets from server";
      reload_featured_sticker_sets(true);
    }
  }
}

void StickersManager::remove_favorite_sticker(const tl_object_ptr<td_api::InputFile> &input_file,
                                              Promise<Unit> &&promise) {
  if (!are_favorite_stickers_loaded_) {
    load_favorite_stickers(std::move(promise));
    return;
  }

  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Sticker, input_file, DialogId(), false, false);
  if (r_file_id.is_error()) {
    return promise.set_error(Status::Error(400, r_file_id.error().message()));  // TODO do not drop error code
  }

  FileId file_id = r_file_id.ok();
  if (!td::remove(favorite_sticker_ids_, file_id)) {
    return promise.set_value(Unit());
  }

  auto sticker = get_sticker(file_id);
  if (sticker == nullptr) {
    return promise.set_error(Status::Error(400, "Sticker not found"));
  }

  send_fave_sticker_query(file_id, true, std::move(promise));

  send_update_favorite_stickers();
}

void StickersManager::on_get_attached_sticker_sets(
    FileId file_id, vector<tl_object_ptr<telegram_api::StickerSetCovered>> &&sticker_sets) {
  vector<StickerSetId> &sticker_set_ids = attached_sticker_sets_[file_id];
  sticker_set_ids.clear();
  for (auto &sticker_set_covered : sticker_sets) {
    auto sticker_set_id =
        on_get_sticker_set_covered(std::move(sticker_set_covered), true, "on_get_attached_sticker_sets");
    if (sticker_set_id.is_valid()) {
      auto sticker_set = get_sticker_set(sticker_set_id);
      CHECK(sticker_set != nullptr);
      update_sticker_set(sticker_set, "on_get_attached_sticker_sets");

      sticker_set_ids.push_back(sticker_set_id);
    }
  }
  send_update_installed_sticker_sets();
}

void StickersManager::on_load_old_featured_sticker_sets_finished(uint32 generation,
                                                                 vector<StickerSetId> &&featured_sticker_set_ids) {
  if (generation != old_featured_sticker_set_generation_) {
    fix_old_featured_sticker_set_count();  // must never be needed
    return;
  }
  append(old_featured_sticker_set_ids_, std::move(featured_sticker_set_ids));
  fix_old_featured_sticker_set_count();
  auto promises = std::move(load_old_featured_sticker_sets_queries_);
  load_old_featured_sticker_sets_queries_.clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

const StickersManager::StickerSet *StickersManager::get_animated_emoji_sticker_set() {
  if (td_->auth_manager_->is_bot() || disable_animated_emojis_) {
    return nullptr;
  }
  auto &special_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_emoji());
  if (!special_sticker_set.id_.is_valid()) {
    load_special_sticker_set(special_sticker_set);
    return nullptr;
  }

  auto sticker_set = get_sticker_set(special_sticker_set.id_);
  CHECK(sticker_set != nullptr);
  if (!sticker_set->was_loaded) {
    load_special_sticker_set(special_sticker_set);
    return nullptr;
  }

  return sticker_set;
}

void StickersManager::on_get_emoji_suggestions_url(
    int64 random_id, Promise<Unit> &&promise, Result<telegram_api::object_ptr<telegram_api::emojiURL>> &&r_emoji_url) {
  auto it = emoji_suggestions_urls_.find(random_id);
  CHECK(it != emoji_suggestions_urls_.end());
  auto &result = it->second;
  CHECK(result.empty());

  if (r_emoji_url.is_error()) {
    emoji_suggestions_urls_.erase(it);
    return promise.set_error(r_emoji_url.move_as_error());
  }

  auto emoji_url = r_emoji_url.move_as_ok();
  result = std::move(emoji_url->url_);
  promise.set_value(Unit());
}

void StickersManager::on_find_sticker_sets_fail(const string &query, Status &&error) {
  CHECK(found_sticker_sets_.count(query) == 0);

  auto it = search_sticker_sets_queries_.find(query);
  CHECK(it != search_sticker_sets_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  search_sticker_sets_queries_.erase(it);

  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

vector<FileId> StickersManager::get_recent_stickers(bool is_attached, Promise<Unit> &&promise) {
  if (!are_recent_stickers_loaded_[is_attached]) {
    load_recent_stickers(is_attached, std::move(promise));
    return {};
  }
  reload_recent_stickers(is_attached, false);

  promise.set_value(Unit());
  return recent_sticker_ids_[is_attached];
}

void StickersManager::on_update_sticker_sets_order(bool is_masks, const vector<StickerSetId> &sticker_set_ids) {
  int result = apply_installed_sticker_sets_order(is_masks, sticker_set_ids);
  if (result < 0) {
    return reload_installed_sticker_sets(is_masks, true);
  }
  if (result > 0) {
    send_update_installed_sticker_sets();
  }
}

StickersManager::SpecialStickerSet &StickersManager::add_special_sticker_set(const SpecialStickerSetType &type) {
  auto &result = special_sticker_sets_[type];
  if (result.type_.is_empty()) {
    result.type_ = type;
  } else {
    CHECK(result.type_ == type);
  }
  return result;
}

void StickersManager::on_update_animated_emoji_zoom() {
  animated_emoji_zoom_ =
      static_cast<double>(G()->shared_config().get_option_integer("animated_emoji_zoom", 625000000)) * 1e-9;
}

td_api::object_ptr<td_api::updateDiceEmojis> StickersManager::get_update_dice_emojis_object() const {
  return td_api::make_object<td_api::updateDiceEmojis>(vector<string>(dice_emojis_));
}

string StickersManager::get_full_sticker_set_database_key(StickerSetId set_id) {
  return PSTRING() << "ssf" << set_id.get();
}
}  // namespace td
