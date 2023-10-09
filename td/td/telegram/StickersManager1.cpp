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

vector<FileId> StickersManager::get_stickers(string emoji, int32 limit, bool force, Promise<Unit> &&promise) {
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }
  if (!are_installed_sticker_sets_loaded_[0]) {
    load_installed_sticker_sets(false, std::move(promise));
    return {};
  }

  remove_emoji_modifiers_in_place(emoji);
  if (!emoji.empty()) {
    if (!are_recent_stickers_loaded_[0]) {
      load_recent_stickers(false, std::move(promise));
      return {};
    }
    if (!are_favorite_stickers_loaded_) {
      load_favorite_stickers(std::move(promise));
      return {};
    }
    /*
    if (!are_featured_sticker_sets_loaded_) {
      load_featured_sticker_sets(std::move(promise));
      return {};
    }
    */
  }

  vector<StickerSetId> sets_to_load;
  bool need_load = false;
  for (const auto &sticker_set_id : installed_sticker_set_ids_[0]) {
    const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    CHECK(sticker_set->is_inited);
    CHECK(!sticker_set->is_archived);
    if (!sticker_set->is_loaded) {
      sets_to_load.push_back(sticker_set_id);
      if (!sticker_set->was_loaded) {
        need_load = true;
      }
    }
  }

  vector<FileId> prepend_sticker_ids;
  if (!emoji.empty()) {
    prepend_sticker_ids.reserve(favorite_sticker_ids_.size() + recent_sticker_ids_[0].size());
    append(prepend_sticker_ids, recent_sticker_ids_[0]);
    for (auto sticker_id : favorite_sticker_ids_) {
      if (!td::contains(prepend_sticker_ids, sticker_id)) {
        prepend_sticker_ids.push_back(sticker_id);
      }
    }

    auto prefer_animated = [this](FileId lhs, FileId rhs) {
      const Sticker *lhs_s = get_sticker(lhs);
      const Sticker *rhs_s = get_sticker(rhs);
      CHECK(lhs_s != nullptr && rhs_s != nullptr);
      return lhs_s->is_animated && !rhs_s->is_animated;
    };
    // std::stable_sort(prepend_sticker_ids.begin(), prepend_sticker_ids.begin() + recent_sticker_ids_[0].size(),
    //                  prefer_animated);
    std::stable_sort(prepend_sticker_ids.begin() + recent_sticker_ids_[0].size(), prepend_sticker_ids.end(),
                     prefer_animated);

    LOG(INFO) << "Have " << recent_sticker_ids_[0] << " recent and " << favorite_sticker_ids_ << " favorite stickers";
    for (const auto &sticker_id : prepend_sticker_ids) {
      const Sticker *s = get_sticker(sticker_id);
      CHECK(s != nullptr);
      LOG(INFO) << "Have prepend sticker " << sticker_id << " from " << s->set_id;
      if (s->set_id.is_valid() && !td::contains(sets_to_load, s->set_id)) {
        const StickerSet *sticker_set = get_sticker_set(s->set_id);
        if (sticker_set == nullptr || !sticker_set->is_loaded) {
          sets_to_load.push_back(s->set_id);
          if (sticker_set == nullptr || !sticker_set->was_loaded) {
            need_load = true;
          }
        }
      }
    }
  }

  if (!sets_to_load.empty()) {
    if (need_load && !force) {
      load_sticker_sets(std::move(sets_to_load),
                        PromiseCreator::lambda([promise = std::move(promise)](Result<Unit> result) mutable {
                          if (result.is_error() && result.error().message() != "STICKERSET_INVALID") {
                            LOG(ERROR) << "Failed to load sticker sets: " << result.error();
                          }
                          promise.set_value(Unit());
                        }));
      return {};
    } else {
      load_sticker_sets(std::move(sets_to_load), Auto());
    }
  }

  vector<FileId> result;
  auto limit_size_t = static_cast<size_t>(limit);
  if (emoji.empty()) {
    for (const auto &sticker_set_id : installed_sticker_set_ids_[0]) {
      const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
      if (sticker_set == nullptr || !sticker_set->was_loaded) {
        continue;
      }

      append(result, sticker_set->sticker_ids);
      if (result.size() > limit_size_t) {
        result.resize(limit_size_t);
        break;
      }
    }
  } else {
    vector<const StickerSet *> examined_sticker_sets;
    for (const auto &sticker_set_id : installed_sticker_set_ids_[0]) {
      const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
      if (sticker_set == nullptr || !sticker_set->was_loaded) {
        continue;
      }

      if (!td::contains(examined_sticker_sets, sticker_set)) {
        examined_sticker_sets.push_back(sticker_set);
      }
    }
    std::stable_sort(
        examined_sticker_sets.begin(), examined_sticker_sets.end(),
        [](const StickerSet *lhs, const StickerSet *rhs) { return lhs->is_animated && !rhs->is_animated; });
    for (auto sticker_set : examined_sticker_sets) {
      auto it = sticker_set->emoji_stickers_map_.find(emoji);
      if (it != sticker_set->emoji_stickers_map_.end()) {
        LOG(INFO) << "Add " << it->second << " stickers from " << sticker_set->id;
        append(result, it->second);
      }
    }

    vector<FileId> sorted;
    sorted.reserve(min(limit_size_t, result.size()));
    auto recent_stickers_size = recent_sticker_ids_[0].size();
    const size_t MAX_RECENT_STICKERS = 5;
    for (size_t i = 0; i < prepend_sticker_ids.size(); i++) {
      if (sorted.size() == MAX_RECENT_STICKERS && i < recent_stickers_size) {
        LOG(INFO) << "Skip recent sticker " << prepend_sticker_ids[i];
        continue;
      }

      auto sticker_id = prepend_sticker_ids[i];
      bool is_good = false;
      auto it = std::find(result.begin(), result.end(), sticker_id);
      if (it != result.end()) {
        LOG(INFO) << "Found prepend sticker " << sticker_id << " in installed packs at position "
                  << (it - result.begin());
        *it = FileId();
        is_good = true;
      } else {
        const Sticker *s = get_sticker(sticker_id);
        CHECK(s != nullptr);
        if (remove_emoji_modifiers(s->alt) == emoji) {
          LOG(INFO) << "Found prepend sticker " << sticker_id << " main emoji matches";
          is_good = true;
        } else if (s->set_id.is_valid()) {
          const StickerSet *sticker_set = get_sticker_set(s->set_id);
          if (sticker_set != nullptr && sticker_set->was_loaded) {
            auto map_it = sticker_set->emoji_stickers_map_.find(emoji);
            if (map_it != sticker_set->emoji_stickers_map_.end()) {
              if (td::contains(map_it->second, sticker_id)) {
                LOG(INFO) << "Found prepend sticker " << sticker_id << " has matching emoji";
                is_good = true;
              }
            }
          }
        }
      }

      if (is_good) {
        sorted.push_back(sticker_id);
        if (sorted.size() == limit_size_t) {
          break;
        }
      }
    }
    if (sorted.size() != limit_size_t) {
      for (const auto &sticker_id : result) {
        if (sticker_id.is_valid()) {
          LOG(INFO) << "Add sticker " << sticker_id << " from installed sticker set";
          sorted.push_back(sticker_id);
          if (sorted.size() == limit_size_t) {
            break;
          }
        } else {
          LOG(INFO) << "Skip already added sticker";
        }
      }
    }

    result = std::move(sorted);
  }

  promise.set_value(Unit());
  return result;
}

class FaveStickerQuery final : public Td::ResultHandler {
  FileId file_id_;
  string file_reference_;
  bool unsave_ = false;

  Promise<Unit> promise_;

 public:
  explicit FaveStickerQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FileId file_id, tl_object_ptr<telegram_api::inputDocument> &&input_document, bool unsave) {
    CHECK(input_document != nullptr);
    CHECK(file_id.is_valid());
    file_id_ = file_id;
    file_reference_ = input_document->file_reference_.as_slice().str();
    unsave_ = unsave;

    send_query(G()->net_query_creator().create(telegram_api::messages_faveSticker(std::move(input_document), unsave)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_faveSticker>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for fave sticker: " << result;
    if (!result) {
      td_->stickers_manager_->reload_favorite_stickers(true);
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      VLOG(file_references) << "Receive " << status << " for " << file_id_;
      td_->file_manager_->delete_file_reference(file_id_, file_reference_);
      td_->file_reference_manager_->repair_file_reference(
          file_id_, PromiseCreator::lambda([sticker_id = file_id_, unsave = unsave_,
                                            promise = std::move(promise_)](Result<Unit> result) mutable {
            if (result.is_error()) {
              return promise.set_error(Status::Error(400, "Failed to find the sticker"));
            }

            send_closure(G()->stickers_manager(), &StickersManager::send_fave_sticker_query, sticker_id, unsave,
                         std::move(promise));
          }));
      return;
    }

    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for fave sticker: " << status;
    }
    td_->stickers_manager_->reload_favorite_stickers(true);
    promise_.set_error(std::move(status));
  }
};

void StickersManager::send_fave_sticker_query(FileId sticker_id, bool unsave, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  // TODO invokeAfter and log event
  auto file_view = td_->file_manager_->get_file_view(sticker_id);
  CHECK(file_view.has_remote_location());
  CHECK(file_view.remote_location().is_document());
  CHECK(!file_view.remote_location().is_web());
  td_->create_handler<FaveStickerQuery>(std::move(promise))
      ->send(sticker_id, file_view.remote_location().as_input_document(), unsave);
}

tl_object_ptr<td_api::sticker> StickersManager::get_sticker_object(FileId file_id, bool for_animated_emoji,
                                                                   bool for_clicked_animated_emoji) const {
  if (!file_id.is_valid()) {
    return nullptr;
  }

  auto it = stickers_.find(file_id);
  CHECK(it != stickers_.end());
  auto sticker = it->second.get();
  CHECK(sticker != nullptr);
  auto mask_position = sticker->point >= 0
                           ? make_tl_object<td_api::maskPosition>(get_mask_point_object(sticker->point),
                                                                  sticker->x_shift, sticker->y_shift, sticker->scale)
                           : nullptr;

  const PhotoSize &thumbnail = sticker->m_thumbnail.file_id.is_valid() ? sticker->m_thumbnail : sticker->s_thumbnail;
  auto thumbnail_format = PhotoFormat::Webp;
  int64 document_id = -1;
  if (!sticker->set_id.is_valid()) {
    auto sticker_file_view = td_->file_manager_->get_file_view(sticker->file_id);
    if (sticker_file_view.is_encrypted()) {
      // uploaded to secret chats stickers have JPEG thumbnail instead of server-generated WEBP
      thumbnail_format = PhotoFormat::Jpeg;
    } else {
      if (sticker_file_view.has_remote_location() && sticker_file_view.remote_location().is_document()) {
        document_id = sticker_file_view.remote_location().get_id();
      }

      if (thumbnail.file_id.is_valid()) {
        auto thumbnail_file_view = td_->file_manager_->get_file_view(thumbnail.file_id);
        if (ends_with(thumbnail_file_view.suggested_path(), ".jpg")) {
          thumbnail_format = PhotoFormat::Jpeg;
        }
      }
    }
  }
  auto thumbnail_object = get_thumbnail_object(td_->file_manager_.get(), thumbnail, thumbnail_format);
  int32 width = sticker->dimensions.width;
  int32 height = sticker->dimensions.height;
  double zoom = 1.0;
  if (sticker->is_animated && (for_animated_emoji || for_clicked_animated_emoji)) {
    zoom = for_clicked_animated_emoji ? 3 * animated_emoji_zoom_ : animated_emoji_zoom_;
    width = static_cast<int32>(width * zoom + 0.5);
    height = static_cast<int32>(height * zoom + 0.5);
  }
  return make_tl_object<td_api::sticker>(
      sticker->set_id.get(), width, height, sticker->alt, sticker->is_animated, sticker->is_mask,
      std::move(mask_position), get_sticker_minithumbnail(sticker->minithumbnail, sticker->set_id, document_id, zoom),
      std::move(thumbnail_object), td_->file_manager_->get_file_object(file_id));
}

void StickersManager::schedule_update_animated_emoji_clicked(const StickerSet *sticker_set, Slice emoji,
                                                             FullMessageId full_message_id,
                                                             vector<std::pair<int, double>> clicks) {
  if (clicks.empty()) {
    return;
  }
  if (td_->messages_manager_->is_message_edited_recently(full_message_id, 2)) {
    // includes deleted full_message_id
    return;
  }
  auto dialog_id = full_message_id.get_dialog_id();
  if (!td_->messages_manager_->have_input_peer(dialog_id, AccessRights::Write)) {
    return;
  }

  auto all_sticker_ids = get_animated_emoji_click_stickers(sticker_set, emoji);
  std::unordered_map<int, FileId> sticker_ids;
  for (auto sticker_id : all_sticker_ids) {
    auto it = sticker_set->sticker_emojis_map_.find(sticker_id);
    if (it != sticker_set->sticker_emojis_map_.end()) {
      for (auto &sticker_emoji : it->second) {
        auto number = get_emoji_number(sticker_emoji);
        if (number > 0) {
          sticker_ids[number] = sticker_id;
        }
      }
    }
  }

  auto now = Time::now();
  auto start_time = max(now, next_update_animated_emoji_clicked_time_);
  for (const auto &click : clicks) {
    auto index = click.first;
    auto sticker_id = sticker_ids[index];
    if (!sticker_id.is_valid()) {
      LOG(INFO) << "Failed to find sticker for " << emoji << " with index " << index;
      return;
    }
    create_actor<SleepActor>(
        "SendUpdateAnimatedEmojiClicked", start_time + click.second - now,
        PromiseCreator::lambda([actor_id = actor_id(this), full_message_id, sticker_id](Result<Unit> result) {
          send_closure(actor_id, &StickersManager::send_update_animated_emoji_clicked, full_message_id, sticker_id);
        }))
        .release();
  }
  next_update_animated_emoji_clicked_time_ = start_time + clicks.back().second + MIN_ANIMATED_EMOJI_CLICK_DELAY;
}

class DeleteStickerFromSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit DeleteStickerFromSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(tl_object_ptr<telegram_api::inputDocument> &&input_document) {
    send_query(G()->net_query_creator().create(telegram_api::stickers_removeStickerFromSet(std::move(input_document))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_removeStickerFromSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_get_messages_sticker_set(StickerSetId(), result_ptr.move_as_ok(), true,
                                                        "DeleteStickerFromSetQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

void StickersManager::remove_sticker_from_set(const tl_object_ptr<td_api::InputFile> &sticker,
                                              Promise<Unit> &&promise) {
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

  td_->create_handler<DeleteStickerFromSetQuery>(std::move(promise))
      ->send(file_view.main_remote_location().as_input_document());
}

class ReorderStickerSetsQuery final : public Td::ResultHandler {
  bool is_masks_;

 public:
  void send(bool is_masks, const vector<StickerSetId> &sticker_set_ids) {
    is_masks_ = is_masks;
    int32 flags = 0;
    if (is_masks) {
      flags |= telegram_api::messages_reorderStickerSets::MASKS_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::messages_reorderStickerSets(
        flags, is_masks /*ignored*/, StickersManager::convert_sticker_set_ids(sticker_set_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_reorderStickerSets>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    if (!result) {
      return on_error(Status::Error(400, "Result is false"));
    }
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for ReorderStickerSetsQuery: " << status;
    }
    td_->stickers_manager_->reload_installed_sticker_sets(is_masks_, true);
  }
};

void StickersManager::reorder_installed_sticker_sets(bool is_masks, const vector<StickerSetId> &sticker_set_ids,
                                                     Promise<Unit> &&promise) {
  auto result = apply_installed_sticker_sets_order(is_masks, sticker_set_ids);
  if (result < 0) {
    return promise.set_error(Status::Error(400, "Wrong sticker set list"));
  }
  if (result > 0) {
    td_->create_handler<ReorderStickerSetsQuery>()->send(is_masks, installed_sticker_set_ids_[is_masks]);
    send_update_installed_sticker_sets();
  }
  promise.set_value(Unit());
}

void StickersManager::on_find_stickers_success(const string &emoji,
                                               tl_object_ptr<telegram_api::messages_Stickers> &&stickers) {
  CHECK(stickers != nullptr);
  switch (stickers->get_id()) {
    case telegram_api::messages_stickersNotModified::ID: {
      auto it = found_stickers_.find(emoji);
      if (it == found_stickers_.end()) {
        return on_find_stickers_fail(emoji, Status::Error(500, "Receive messages.stickerNotModified"));
      }
      auto &found_stickers = it->second;
      found_stickers.next_reload_time_ = Time::now() + found_stickers.cache_time_;
      break;
    }
    case telegram_api::messages_stickers::ID: {
      auto received_stickers = move_tl_object_as<telegram_api::messages_stickers>(stickers);

      auto &found_stickers = found_stickers_[emoji];
      found_stickers.cache_time_ = 300;
      found_stickers.next_reload_time_ = Time::now() + found_stickers.cache_time_;
      found_stickers.sticker_ids_.clear();

      for (auto &sticker : received_stickers->stickers_) {
        FileId sticker_id = on_get_sticker_document(std::move(sticker)).second;
        if (sticker_id.is_valid()) {
          found_stickers.sticker_ids_.push_back(sticker_id);
        }
      }
      break;
    }
    default:
      UNREACHABLE();
  }

  auto it = search_stickers_queries_.find(emoji);
  CHECK(it != search_stickers_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  search_stickers_queries_.erase(it);

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

tl_object_ptr<td_api::stickerSetInfo> StickersManager::get_sticker_set_info_object(StickerSetId sticker_set_id,
                                                                                   size_t covers_limit) const {
  const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
  CHECK(sticker_set != nullptr);
  CHECK(sticker_set->is_inited);
  sticker_set->was_update_sent = true;

  std::vector<tl_object_ptr<td_api::sticker>> stickers;
  for (auto sticker_id : sticker_set->sticker_ids) {
    stickers.push_back(get_sticker_object(sticker_id));
    if (stickers.size() >= covers_limit) {
      break;
    }
  }

  auto thumbnail = get_thumbnail_object(td_->file_manager_.get(), sticker_set->thumbnail,
                                        sticker_set->is_animated ? PhotoFormat::Tgs : PhotoFormat::Webp);
  return make_tl_object<td_api::stickerSetInfo>(
      sticker_set->id.get(), sticker_set->title, sticker_set->short_name, std::move(thumbnail),
      get_sticker_minithumbnail(sticker_set->minithumbnail, sticker_set->id, -3, 1.0),
      sticker_set->is_installed && !sticker_set->is_archived, sticker_set->is_archived, sticker_set->is_official,
      sticker_set->is_animated, sticker_set->is_masks, sticker_set->is_viewed,
      sticker_set->was_loaded ? narrow_cast<int32>(sticker_set->sticker_ids.size()) : sticker_set->sticker_count,
      std::move(stickers));
}

StickerSetId StickersManager::add_sticker_set(tl_object_ptr<telegram_api::InputStickerSet> &&set_ptr) {
  CHECK(set_ptr != nullptr);
  switch (set_ptr->get_id()) {
    case telegram_api::inputStickerSetEmpty::ID:
      return StickerSetId();
    case telegram_api::inputStickerSetID::ID: {
      auto set = move_tl_object_as<telegram_api::inputStickerSetID>(set_ptr);
      StickerSetId set_id{set->id_};
      add_sticker_set(set_id, set->access_hash_);
      return set_id;
    }
    case telegram_api::inputStickerSetShortName::ID: {
      auto set = move_tl_object_as<telegram_api::inputStickerSetShortName>(set_ptr);
      LOG(ERROR) << "Receive sticker set by its short name";
      return search_sticker_set(set->short_name_, Auto());
    }
    case telegram_api::inputStickerSetAnimatedEmoji::ID:
    case telegram_api::inputStickerSetAnimatedEmojiAnimations::ID:
      LOG(ERROR) << "Receive special sticker set " << to_string(set_ptr);
      return add_special_sticker_set(SpecialStickerSetType(set_ptr)).id_;
    case telegram_api::inputStickerSetDice::ID:
      LOG(ERROR) << "Receive special sticker set " << to_string(set_ptr);
      return StickerSetId();
    default:
      UNREACHABLE();
      return StickerSetId();
  }
}

vector<FileId> StickersManager::get_attached_sticker_file_ids(const vector<int32> &int_file_ids) {
  vector<FileId> result;

  result.reserve(int_file_ids.size());
  for (auto int_file_id : int_file_ids) {
    FileId file_id(int_file_id, 0);
    const Sticker *s = get_sticker(file_id);
    if (s == nullptr) {
      LOG(WARNING) << "Can't find sticker " << file_id;
      continue;
    }
    if (!s->set_id.is_valid()) {
      // only stickers from sticker sets can be attached to files
      continue;
    }

    auto file_view = td_->file_manager_->get_file_view(file_id);
    CHECK(!file_view.empty());
    if (!file_view.has_remote_location()) {
      LOG(ERROR) << "Sticker " << file_id << " has no remote location";
      continue;
    }
    if (file_view.remote_location().is_web()) {
      LOG(ERROR) << "Sticker " << file_id << " is web";
      continue;
    }
    if (!file_view.remote_location().is_document()) {
      LOG(ERROR) << "Sticker " << file_id << " is encrypted";
      continue;
    }
    result.push_back(file_id);

    if (!td_->auth_manager_->is_bot()) {
      add_recent_sticker_by_id(true, file_id);
    }
  }

  return result;
}

void StickersManager::update_load_requests(StickerSet *sticker_set, bool with_stickers, const Status &status) {
  if (sticker_set == nullptr) {
    return;
  }
  if (with_stickers) {
    for (auto load_request_id : sticker_set->load_requests) {
      update_load_request(load_request_id, status);
    }

    sticker_set->load_requests.clear();
  }
  for (auto load_request_id : sticker_set->load_without_stickers_requests) {
    update_load_request(load_request_id, status);
  }

  sticker_set->load_without_stickers_requests.clear();

  if (status.message() == "STICKERSET_INVALID") {
    // the sticker set is likely to be deleted
    // clear short_name_to_sticker_set_id_ to allow next searchStickerSet request to succeed
    short_name_to_sticker_set_id_.erase(clean_username(sticker_set->short_name));
  }
}

void StickersManager::fix_old_featured_sticker_set_count() {
  auto known_count = static_cast<int32>(old_featured_sticker_set_ids_.size());
  if (old_featured_sticker_set_count_ < known_count) {
    if (old_featured_sticker_set_count_ >= 0) {
      LOG(ERROR) << "Have old trending sticker set count " << old_featured_sticker_set_count_ << ", but have "
                 << known_count << " old trending sticker sets";
    }
    set_old_featured_sticker_set_count(known_count);
  }
  if (old_featured_sticker_set_count_ > known_count && known_count % OLD_FEATURED_STICKER_SET_SLICE_SIZE != 0) {
    LOG(ERROR) << "Have " << known_count << " old sticker sets out of " << old_featured_sticker_set_count_;
    set_old_featured_sticker_set_count(known_count);
  }
}

void StickersManager::on_get_featured_sticker_sets_failed(int32 offset, int32 limit, uint32 generation, Status error) {
  CHECK(error.is_error());
  vector<Promise<Unit>> promises;
  if (offset >= 0) {
    if (generation != old_featured_sticker_set_generation_) {
      return;
    }
    promises = std::move(load_old_featured_sticker_sets_queries_);
    load_old_featured_sticker_sets_queries_.clear();
  } else {
    next_featured_sticker_sets_load_time_ = Time::now_cached() + Random::fast(5, 10);
    promises = std::move(load_featured_sticker_sets_queries_);
    load_featured_sticker_sets_queries_.clear();
  }

  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

StickersManager::StickerSet *StickersManager::add_sticker_set(StickerSetId sticker_set_id, int64 access_hash) {
  auto &s = sticker_sets_[sticker_set_id];
  if (s == nullptr) {
    s = make_unique<StickerSet>();

    s->id = sticker_set_id;
    s->access_hash = access_hash;
    s->is_changed = false;
    s->need_save_to_database = false;
  } else {
    CHECK(s->id == sticker_set_id);
    if (s->access_hash != access_hash) {
      LOG(INFO) << "Access hash of " << sticker_set_id << " changed";
      s->access_hash = access_hash;
      s->need_save_to_database = true;
    }
  }
  return s.get();
}

void StickersManager::on_get_recent_stickers_failed(bool is_repair, bool is_attached, Status error) {
  CHECK(error.is_error());
  if (!is_repair) {
    next_recent_stickers_load_time_[is_attached] = Time::now_cached() + Random::fast(5, 10);
  }
  auto &queries = is_repair ? repair_recent_stickers_queries_[is_attached] : load_recent_stickers_queries_[is_attached];
  auto promises = std::move(queries);
  queries.clear();
  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

void StickersManager::on_get_favorite_stickers_failed(bool is_repair, Status error) {
  CHECK(error.is_error());
  if (!is_repair) {
    next_favorite_stickers_load_time_ = Time::now_cached() + Random::fast(5, 10);
  }
  auto &queries = is_repair ? repair_favorite_stickers_queries_ : load_favorite_stickers_queries_;
  auto promises = std::move(queries);
  queries.clear();
  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

void StickersManager::add_sticker_thumbnail(Sticker *s, PhotoSize thumbnail) {
  if (!thumbnail.file_id.is_valid()) {
    return;
  }
  if (thumbnail.type == 'm') {
    s->m_thumbnail = std::move(thumbnail);
    return;
  }
  if (thumbnail.type == 's' || thumbnail.type == 't') {
    s->s_thumbnail = std::move(thumbnail);
    return;
  }
  LOG(ERROR) << "Receive sticker thumbnail of unsupported type " << thumbnail.type;
}

void StickersManager::send_update_featured_sticker_sets() {
  if (need_update_featured_sticker_sets_) {
    need_update_featured_sticker_sets_ = false;
    featured_sticker_sets_hash_ = get_featured_sticker_sets_hash();

    send_closure(G()->td(), &Td::send_update, get_update_trending_sticker_sets_object());
  }
}

void StickersManager::send_click_animated_emoji_message_response(
    FileId sticker_id, Promise<td_api::object_ptr<td_api::sticker>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  promise.set_value(get_sticker_object(sticker_id, false, true));
}

StickersManager::StickerSet *StickersManager::get_sticker_set(StickerSetId sticker_set_id) {
  auto sticker_set = sticker_sets_.find(sticker_set_id);
  if (sticker_set == sticker_sets_.end()) {
    return nullptr;
  }

  return sticker_set->second.get();
}

void StickersManager::add_favorite_sticker_by_id(FileId sticker_id) {
  // TODO log event
  add_favorite_sticker_impl(sticker_id, false, Auto());
}

void StickersManager::tear_down() {
  parent_.reset();
}
}  // namespace td
