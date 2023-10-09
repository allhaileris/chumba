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

StickerSetId StickersManager::on_get_sticker_set(tl_object_ptr<telegram_api::stickerSet> &&set, bool is_changed,
                                                 const char *source) {
  CHECK(set != nullptr);
  StickerSetId set_id{set->id_};
  StickerSet *s = add_sticker_set(set_id, set->access_hash_);

  bool is_installed = (set->flags_ & telegram_api::stickerSet::INSTALLED_DATE_MASK) != 0;
  bool is_archived = set->archived_;
  bool is_official = set->official_;
  bool is_animated = set->animated_;
  bool is_masks = set->masks_;

  PhotoSize thumbnail;
  string minithumbnail;
  for (auto &thumb : set->thumbs_) {
    auto photo_size =
        get_photo_size(td_->file_manager_.get(),
                       PhotoSizeSource::sticker_set_thumbnail(set_id.get(), s->access_hash, set->thumb_version_), 0, 0,
                       "", DcId::create(set->thumb_dc_id_), DialogId(), std::move(thumb),
                       is_animated ? PhotoFormat::Tgs : PhotoFormat::Webp);
    if (photo_size.get_offset() == 0) {
      if (!thumbnail.file_id.is_valid()) {
        thumbnail = std::move(photo_size.get<0>());
      }
    } else {
      minithumbnail = std::move(photo_size.get<1>());
    }
  }
  if (!s->is_inited) {
    LOG(INFO) << "Init " << set_id;
    s->is_inited = true;
    s->title = std::move(set->title_);
    s->short_name = std::move(set->short_name_);
    if (!td_->auth_manager_->is_bot()) {
      s->minithumbnail = std::move(minithumbnail);
    }
    s->thumbnail = std::move(thumbnail);
    s->is_thumbnail_reloaded = true;
    s->are_legacy_sticker_thumbnails_reloaded = true;
    s->sticker_count = set->count_;
    s->hash = set->hash_;
    s->is_official = is_official;
    s->is_animated = is_animated;
    s->is_masks = is_masks;
    s->is_changed = true;
  } else {
    CHECK(s->id == set_id);
    if (s->access_hash != set->access_hash_) {
      LOG(INFO) << "Access hash of " << set_id << " has changed";
      s->access_hash = set->access_hash_;
      s->need_save_to_database = true;
    }
    if (s->title != set->title_) {
      LOG(INFO) << "Title of " << set_id << " has changed";
      s->title = std::move(set->title_);
      s->is_changed = true;

      if (installed_sticker_sets_hints_[s->is_masks].has_key(set_id.get())) {
        installed_sticker_sets_hints_[s->is_masks].add(set_id.get(), PSLICE() << s->title << ' ' << s->short_name);
      }
    }
    if (s->short_name != set->short_name_) {
      LOG(ERROR) << "Short name of " << set_id << " has changed from \"" << s->short_name << "\" to \""
                 << set->short_name_ << "\" from " << source;
      short_name_to_sticker_set_id_.erase(clean_username(s->short_name));
      s->short_name = std::move(set->short_name_);
      s->is_changed = true;

      if (installed_sticker_sets_hints_[s->is_masks].has_key(set_id.get())) {
        installed_sticker_sets_hints_[s->is_masks].add(set_id.get(), PSLICE() << s->title << ' ' << s->short_name);
      }
    }
    if (s->minithumbnail != minithumbnail) {
      LOG(INFO) << "Minithumbnail of " << set_id << " has changed";
      s->minithumbnail = std::move(minithumbnail);
      s->is_changed = true;
    }
    if (s->thumbnail != thumbnail) {
      LOG(INFO) << "Thumbnail of " << set_id << " has changed from " << s->thumbnail << " to " << thumbnail;
      s->thumbnail = std::move(thumbnail);
      s->is_changed = true;
    }
    if (!s->is_thumbnail_reloaded || !s->are_legacy_sticker_thumbnails_reloaded) {
      LOG(INFO) << "Sticker thumbnails and thumbnail of " << set_id << " was reloaded";
      s->is_thumbnail_reloaded = true;
      s->are_legacy_sticker_thumbnails_reloaded = true;
      s->need_save_to_database = true;
    }

    if (s->sticker_count != set->count_ || s->hash != set->hash_) {
      LOG(INFO) << "Number of stickers in " << set_id << " changed from " << s->sticker_count << " to " << set->count_;
      s->is_loaded = false;

      s->sticker_count = set->count_;
      s->hash = set->hash_;
      if (s->was_loaded) {
        s->need_save_to_database = true;
      } else {
        s->is_changed = true;
      }
    }

    if (s->is_official != is_official) {
      LOG(INFO) << "Official flag of " << set_id << " changed to " << is_official;
      s->is_official = is_official;
      s->is_changed = true;
    }
    if (s->is_animated != is_animated) {
      LOG(ERROR) << "Animated type of " << set_id << "/" << s->short_name << " has changed from " << s->is_animated
                 << " to " << is_animated << " from " << source;
      s->is_animated = is_animated;
      s->is_changed = true;
    }
    LOG_IF(ERROR, s->is_masks != is_masks) << "Masks type of " << set_id << "/" << s->short_name << " has changed from "
                                           << s->is_masks << " to " << is_masks << " from " << source;
  }
  short_name_to_sticker_set_id_.emplace(clean_username(s->short_name), set_id);

  on_update_sticker_set(s, is_installed, is_archived, is_changed);

  return set_id;
}

class GetAttachedStickerSetsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;
  string file_reference_;

 public:
  explicit GetAttachedStickerSetsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FileId file_id, string &&file_reference,
            tl_object_ptr<telegram_api::InputStickeredMedia> &&input_stickered_media) {
    file_id_ = file_id;
    file_reference_ = std::move(file_reference);
    send_query(
        G()->net_query_creator().create(telegram_api::messages_getAttachedStickers(std::move(input_stickered_media))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getAttachedStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_get_attached_sticker_sets(file_id_, result_ptr.move_as_ok());

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      VLOG(file_references) << "Receive " << status << " for " << file_id_;
      td_->file_manager_->delete_file_reference(file_id_, file_reference_);
      td_->file_reference_manager_->repair_file_reference(
          file_id_,
          PromiseCreator::lambda([file_id = file_id_, promise = std::move(promise_)](Result<Unit> result) mutable {
            if (result.is_error()) {
              return promise.set_error(Status::Error(400, "Failed to find the file"));
            }

            send_closure(G()->stickers_manager(), &StickersManager::send_get_attached_stickers_query, file_id,
                         std::move(promise));
          }));
      return;
    }

    promise_.set_error(std::move(status));
  }
};

void StickersManager::send_get_attached_stickers_query(FileId file_id, Promise<Unit> &&promise) {
  auto file_view = td_->file_manager_->get_file_view(file_id);
  if (file_view.empty()) {
    return promise.set_error(Status::Error(400, "File not found"));
  }
  if (!file_view.has_remote_location() ||
      (!file_view.remote_location().is_document() && !file_view.remote_location().is_photo()) ||
      file_view.remote_location().is_web()) {
    return promise.set_value(Unit());
  }

  tl_object_ptr<telegram_api::InputStickeredMedia> input_stickered_media;
  string file_reference;
  if (file_view.main_remote_location().is_photo()) {
    auto input_photo = file_view.main_remote_location().as_input_photo();
    file_reference = input_photo->file_reference_.as_slice().str();
    input_stickered_media = make_tl_object<telegram_api::inputStickeredMediaPhoto>(std::move(input_photo));
  } else {
    auto input_document = file_view.main_remote_location().as_input_document();
    file_reference = input_document->file_reference_.as_slice().str();
    input_stickered_media = make_tl_object<telegram_api::inputStickeredMediaDocument>(std::move(input_document));
  }

  td_->create_handler<GetAttachedStickerSetsQuery>(std::move(promise))
      ->send(file_id, std::move(file_reference), std::move(input_stickered_media));
}

void StickersManager::add_favorite_sticker_impl(FileId sticker_id, bool add_on_server, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());

  if (!are_favorite_stickers_loaded_) {
    load_favorite_stickers(
        PromiseCreator::lambda([sticker_id, add_on_server, promise = std::move(promise)](Result<> result) mutable {
          if (result.is_ok()) {
            send_closure(G()->stickers_manager(), &StickersManager::add_favorite_sticker_impl, sticker_id,
                         add_on_server, std::move(promise));
          } else {
            promise.set_error(result.move_as_error());
          }
        }));
    return;
  }

  auto is_equal = [sticker_id](FileId file_id) {
    return file_id == sticker_id || (file_id.get_remote() == sticker_id.get_remote() && sticker_id.get_remote() != 0);
  };

  if (!favorite_sticker_ids_.empty() && is_equal(favorite_sticker_ids_[0])) {
    if (favorite_sticker_ids_[0].get_remote() == 0 && sticker_id.get_remote() != 0) {
      favorite_sticker_ids_[0] = sticker_id;
      save_favorite_stickers_to_database();
    }

    return promise.set_value(Unit());
  }

  auto sticker = get_sticker(sticker_id);
  if (sticker == nullptr) {
    return promise.set_error(Status::Error(400, "Sticker not found"));
  }
  if (!sticker->set_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Stickers without sticker set can't be favorite"));
  }

  auto file_view = td_->file_manager_->get_file_view(sticker_id);
  if (!file_view.has_remote_location()) {
    return promise.set_error(Status::Error(400, "Can add to favorites only sent stickers"));
  }
  if (file_view.remote_location().is_web()) {
    return promise.set_error(Status::Error(400, "Can't add to favorites web stickers"));
  }
  if (!file_view.remote_location().is_document()) {
    return promise.set_error(Status::Error(400, "Can't add to favorites encrypted stickers"));
  }

  auto it = std::find_if(favorite_sticker_ids_.begin(), favorite_sticker_ids_.end(), is_equal);
  if (it == favorite_sticker_ids_.end()) {
    if (static_cast<int32>(favorite_sticker_ids_.size()) == favorite_stickers_limit_) {
      favorite_sticker_ids_.back() = sticker_id;
    } else {
      favorite_sticker_ids_.push_back(sticker_id);
    }
    it = favorite_sticker_ids_.end() - 1;
  }
  std::rotate(favorite_sticker_ids_.begin(), it, it + 1);
  if (favorite_sticker_ids_[0].get_remote() == 0 && sticker_id.get_remote() != 0) {
    favorite_sticker_ids_[0] = sticker_id;
  }

  send_update_favorite_stickers();
  if (add_on_server) {
    send_fave_sticker_query(sticker_id, false, std::move(promise));
  }
}

void StickersManager::on_update_sticker_set(StickerSet *sticker_set, bool is_installed, bool is_archived,
                                            bool is_changed, bool from_database) {
  LOG(INFO) << "Update " << sticker_set->id << ": installed = " << is_installed << ", archived = " << is_archived
            << ", changed = " << is_changed << ", from_database = " << from_database;
  CHECK(sticker_set->is_inited);
  if (is_archived) {
    is_installed = true;
  }
  if (sticker_set->is_installed == is_installed && sticker_set->is_archived == is_archived) {
    return;
  }

  bool was_added = sticker_set->is_installed && !sticker_set->is_archived;
  bool was_archived = sticker_set->is_archived;
  sticker_set->is_installed = is_installed;
  sticker_set->is_archived = is_archived;
  if (!from_database) {
    sticker_set->is_changed = true;
  }

  bool is_added = sticker_set->is_installed && !sticker_set->is_archived;
  if (was_added != is_added) {
    vector<StickerSetId> &sticker_set_ids = installed_sticker_set_ids_[sticker_set->is_masks];
    need_update_installed_sticker_sets_[sticker_set->is_masks] = true;

    if (is_added) {
      installed_sticker_sets_hints_[sticker_set->is_masks].add(
          sticker_set->id.get(), PSLICE() << sticker_set->title << ' ' << sticker_set->short_name);
      sticker_set_ids.insert(sticker_set_ids.begin(), sticker_set->id);
    } else {
      installed_sticker_sets_hints_[sticker_set->is_masks].remove(sticker_set->id.get());
      td::remove(sticker_set_ids, sticker_set->id);
    }
  }
  if (was_archived != is_archived && is_changed) {
    int32 &total_count = total_archived_sticker_set_count_[sticker_set->is_masks];
    vector<StickerSetId> &sticker_set_ids = archived_sticker_set_ids_[sticker_set->is_masks];
    if (total_count < 0) {
      return;
    }

    if (is_archived) {
      if (!td::contains(sticker_set_ids, sticker_set->id)) {
        total_count++;
        sticker_set_ids.insert(sticker_set_ids.begin(), sticker_set->id);
      }
    } else {
      total_count--;
      if (total_count < 0) {
        LOG(ERROR) << "Total count of archived sticker sets became negative";
        total_count = 0;
      }
      td::remove(sticker_set_ids, sticker_set->id);
    }
  }
}

void StickersManager::on_get_archived_sticker_sets(
    bool is_masks, StickerSetId offset_sticker_set_id,
    vector<tl_object_ptr<telegram_api::StickerSetCovered>> &&sticker_sets, int32 total_count) {
  vector<StickerSetId> &sticker_set_ids = archived_sticker_set_ids_[is_masks];
  if (!sticker_set_ids.empty() && sticker_set_ids.back() == StickerSetId()) {
    return;
  }
  if (total_count < 0) {
    LOG(ERROR) << "Receive " << total_count << " as total count of archived sticker sets";
  }

  // if 0 sticker sets are received, then set offset_sticker_set_id was found and there are no stickers after it
  // or it wasn't found and there are no archived sets at all
  bool is_last =
      sticker_sets.empty() && (!offset_sticker_set_id.is_valid() ||
                               (!sticker_set_ids.empty() && offset_sticker_set_id == sticker_set_ids.back()));

  total_archived_sticker_set_count_[is_masks] = total_count;
  for (auto &sticker_set_covered : sticker_sets) {
    auto sticker_set_id =
        on_get_sticker_set_covered(std::move(sticker_set_covered), false, "on_get_archived_sticker_sets");
    if (sticker_set_id.is_valid()) {
      auto sticker_set = get_sticker_set(sticker_set_id);
      CHECK(sticker_set != nullptr);
      update_sticker_set(sticker_set, "on_get_archived_sticker_sets");

      if (!td::contains(sticker_set_ids, sticker_set_id)) {
        sticker_set_ids.push_back(sticker_set_id);
      }
    }
  }
  if (sticker_set_ids.size() >= static_cast<size_t>(total_count) || is_last) {
    if (sticker_set_ids.size() != static_cast<size_t>(total_count)) {
      LOG(ERROR) << "Expected total of " << total_count << " archived sticker sets, but " << sticker_set_ids.size()
                 << " found";
      total_archived_sticker_set_count_[is_masks] = static_cast<int32>(sticker_set_ids.size());
    }
    sticker_set_ids.push_back(StickerSetId());
  }
  send_update_installed_sticker_sets();
}

class GetEmojiKeywordsDifferenceQuery final : public Td::ResultHandler {
  Promise<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> promise_;

 public:
  explicit GetEmojiKeywordsDifferenceQuery(
      Promise<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const string &language_code, int32 version) {
    send_query(
        G()->net_query_creator().create(telegram_api::messages_getEmojiKeywordsDifference(language_code, version)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getEmojiKeywordsDifference>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void StickersManager::load_emoji_keywords_difference(const string &language_code) {
  LOG(INFO) << "Load emoji keywords difference for language " << language_code;
  emoji_language_code_last_difference_times_[language_code] =
      Time::now_cached() + 1e9;  // prevent simultaneous requests
  int32 from_version = get_emoji_language_code_version(language_code);
  auto query_promise = PromiseCreator::lambda(
      [actor_id = actor_id(this), language_code,
       from_version](Result<telegram_api::object_ptr<telegram_api::emojiKeywordsDifference>> &&result) mutable {
        send_closure(actor_id, &StickersManager::on_get_emoji_keywords_difference, language_code, from_version,
                     std::move(result));
      });
  td_->create_handler<GetEmojiKeywordsDifferenceQuery>(std::move(query_promise))->send(language_code, from_version);
}

void StickersManager::load_sticker_sets_without_stickers(vector<StickerSetId> &&sticker_set_ids,
                                                         Promise<Unit> &&promise) {
  if (sticker_set_ids.empty()) {
    promise.set_value(Unit());
    return;
  }

  auto load_request_id = current_sticker_set_load_request_++;
  StickerSetLoadRequest &load_request = sticker_set_load_requests_[load_request_id];
  load_request.promise = std::move(promise);
  load_request.left_queries = sticker_set_ids.size();

  for (auto sticker_set_id : sticker_set_ids) {
    StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    CHECK(!sticker_set->is_inited);

    if (!sticker_set->load_requests.empty()) {
      sticker_set->load_requests.push_back(load_request_id);
    } else {
      sticker_set->load_without_stickers_requests.push_back(load_request_id);
      if (sticker_set->load_without_stickers_requests.size() == 1u) {
        if (G()->parameters().use_file_db) {
          LOG(INFO) << "Trying to load " << sticker_set_id << " from database";
          G()->td_db()->get_sqlite_pmc()->get(
              get_sticker_set_database_key(sticker_set_id), PromiseCreator::lambda([sticker_set_id](string value) {
                send_closure(G()->stickers_manager(), &StickersManager::on_load_sticker_set_from_database,
                             sticker_set_id, false, std::move(value));
              }));
        } else {
          LOG(INFO) << "Trying to load " << sticker_set_id << " from server";
          do_reload_sticker_set(sticker_set_id, get_input_sticker_set(sticker_set), 0, Auto());
        }
      }
    }
  }
}

class SearchStickerSetsQuery final : public Td::ResultHandler {
  string query_;

 public:
  void send(string query) {
    query_ = std::move(query);
    send_query(
        G()->net_query_creator().create(telegram_api::messages_searchStickerSets(0, false /*ignored*/, query_, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_searchStickerSets>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for search sticker sets: " << to_string(ptr);
    td_->stickers_manager_->on_find_sticker_sets_success(query_, std::move(ptr));
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for search sticker sets: " << status;
    }
    td_->stickers_manager_->on_find_sticker_sets_fail(query_, std::move(status));
  }
};

vector<StickerSetId> StickersManager::search_sticker_sets(const string &query, Promise<Unit> &&promise) {
  auto q = clean_name(query, 1000);
  auto it = found_sticker_sets_.find(q);
  if (it != found_sticker_sets_.end()) {
    promise.set_value(Unit());
    return it->second;
  }

  auto &promises = search_sticker_sets_queries_[q];
  promises.push_back(std::move(promise));
  if (promises.size() == 1u) {
    td_->create_handler<SearchStickerSetsQuery>()->send(std::move(q));
  }

  return {};
}

void StickersManager::on_update_dice_emojis() {
  if (G()->close_flag()) {
    return;
  }
  if (td_->auth_manager_->is_bot()) {
    G()->shared_config().set_option_empty("dice_emojis");
    return;
  }
  if (!is_inited_) {
    return;
  }

  auto dice_emojis_str =
      G()->shared_config().get_option_string("dice_emojis", "🎲\x01🎯\x01🏀\x01⚽\x01⚽️\x01🎰\x01🎳");
  if (dice_emojis_str == dice_emojis_str_) {
    return;
  }
  dice_emojis_str_ = std::move(dice_emojis_str);
  auto new_dice_emojis = full_split(dice_emojis_str_, '\x01');
  for (auto &emoji : new_dice_emojis) {
    if (!td::contains(dice_emojis_, emoji)) {
      auto &special_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_dice(emoji));
      if (special_sticker_set.id_.is_valid()) {
        // drop information about the sticker set to reload it
        special_sticker_set.id_ = StickerSetId();
        special_sticker_set.access_hash_ = 0;
        special_sticker_set.short_name_.clear();
      }

      if (G()->parameters().use_file_db) {
        LOG(INFO) << "Load new dice sticker set for emoji " << emoji;
        load_special_sticker_set(special_sticker_set);
      }
    }
  }
  dice_emojis_ = std::move(new_dice_emojis);

  send_closure(G()->td(), &Td::send_update, get_update_dice_emojis_object());
}

bool StickersManager::has_input_media(FileId sticker_file_id, bool is_secret) const {
  auto file_view = td_->file_manager_->get_file_view(sticker_file_id);
  if (is_secret) {
    const Sticker *sticker = get_sticker(sticker_file_id);
    CHECK(sticker != nullptr);
    if (file_view.is_encrypted_secret()) {
      if (!file_view.encryption_key().empty() && file_view.has_remote_location() &&
          !sticker->s_thumbnail.file_id.is_valid()) {
        return true;
      }
    } else if (!file_view.is_encrypted()) {
      if (sticker->set_id.is_valid()) {
        // stickers within a set can be sent by id and access_hash
        return true;
      }
    }
  } else {
    if (file_view.is_encrypted()) {
      return false;
    }
    if (td_->auth_manager_->is_bot() && file_view.has_remote_location()) {
      return true;
    }
    // having remote location is not enough to have InputMedia, because the file may not have valid file_reference
    // also file_id needs to be duped, because upload can be called to repair the file_reference and every upload
    // request must have unique file_id
    if (/* file_view.has_remote_location() || */ file_view.has_url()) {
      return true;
    }
  }

  return false;
}

tl_object_ptr<td_api::stickerSets> StickersManager::get_sticker_sets_object(int32 total_count,
                                                                            const vector<StickerSetId> &sticker_set_ids,
                                                                            size_t covers_limit) const {
  vector<tl_object_ptr<td_api::stickerSetInfo>> result;
  result.reserve(sticker_set_ids.size());
  for (auto sticker_set_id : sticker_set_ids) {
    auto sticker_set_info = get_sticker_set_info_object(sticker_set_id, covers_limit);
    if (sticker_set_info->size_ != 0) {
      result.push_back(std::move(sticker_set_info));
    }
  }

  auto result_size = narrow_cast<int32>(result.size());
  if (total_count < result_size) {
    if (total_count != -1) {
      LOG(ERROR) << "Have total_count = " << total_count << ", but there are " << result_size << " results";
    }
    total_count = result_size;
  }
  return make_tl_object<td_api::stickerSets>(total_count, std::move(result));
}

void StickersManager::get_current_state(vector<td_api::object_ptr<td_api::Update>> &updates) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  for (int is_masks = 0; is_masks < 2; is_masks++) {
    if (are_installed_sticker_sets_loaded_[is_masks]) {
      updates.push_back(get_update_installed_sticker_sets_object(is_masks));
    }
  }
  if (are_featured_sticker_sets_loaded_) {
    updates.push_back(get_update_trending_sticker_sets_object());
  }
  for (int is_attached = 0; is_attached < 2; is_attached++) {
    if (are_recent_stickers_loaded_[is_attached]) {
      updates.push_back(get_update_recent_stickers_object(is_attached));
    }
  }
  if (are_favorite_stickers_loaded_) {
    updates.push_back(get_update_favorite_stickers_object());
  }
  if (!dice_emojis_.empty()) {
    updates.push_back(get_update_dice_emojis_object());
  }
}

void StickersManager::invalidate_old_featured_sticker_sets() {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Invalidate old featured sticker sets";
  if (G()->parameters().use_file_db) {
    G()->td_db()->get_binlog_pmc()->erase("invalidate_old_featured_sticker_sets");
    G()->td_db()->get_sqlite_pmc()->erase_by_prefix("sssoldfeatured", Auto());
  }
  are_old_featured_sticker_sets_invalidated_ = false;
  old_featured_sticker_set_ids_.clear();

  old_featured_sticker_set_generation_++;
  auto promises = std::move(load_old_featured_sticker_sets_queries_);
  load_old_featured_sticker_sets_queries_.clear();
  for (auto &promise : promises) {
    promise.set_error(Status::Error(400, "Trending sticker sets were updated"));
  }
}

void StickersManager::on_load_recent_stickers_finished(bool is_attached, vector<FileId> &&recent_sticker_ids,
                                                       bool from_database) {
  if (static_cast<int32>(recent_sticker_ids.size()) > recent_stickers_limit_) {
    recent_sticker_ids.resize(recent_stickers_limit_);
  }
  recent_sticker_ids_[is_attached] = std::move(recent_sticker_ids);
  are_recent_stickers_loaded_[is_attached] = true;
  send_update_recent_stickers(is_attached, from_database);
  auto promises = std::move(load_recent_stickers_queries_[is_attached]);
  load_recent_stickers_queries_[is_attached].clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

void StickersManager::update_load_request(uint32 load_request_id, const Status &status) {
  auto it = sticker_set_load_requests_.find(load_request_id);
  CHECK(it != sticker_set_load_requests_.end());
  CHECK(it->second.left_queries > 0);
  if (status.is_error() && it->second.error.is_ok()) {
    it->second.error = status.clone();
  }
  if (--it->second.left_queries == 0) {
    if (it->second.error.is_ok()) {
      it->second.promise.set_value(Unit());
    } else {
      it->second.promise.set_error(std::move(it->second.error));
    }
    sticker_set_load_requests_.erase(it);
  }
}

bool StickersManager::has_webp_thumbnail(const vector<tl_object_ptr<telegram_api::PhotoSize>> &thumbnails) {
  // server tries to always replace user-provided thumbnail with server-side WEBP thumbnail
  // but there can be some old sticker documents or some big stickers
  for (auto &size : thumbnails) {
    switch (size->get_id()) {
      case telegram_api::photoStrippedSize::ID:
      case telegram_api::photoSizeProgressive::ID:
        // WEBP thumbnail can't have stripped size or be progressive
        return false;
      default:
        break;
    }
  }
  return true;
}

vector<FileId> StickersManager::get_sticker_file_ids(FileId file_id) const {
  vector<FileId> result;
  auto sticker = get_sticker(file_id);
  CHECK(sticker != nullptr);
  result.push_back(file_id);
  if (sticker->s_thumbnail.file_id.is_valid()) {
    result.push_back(sticker->s_thumbnail.file_id);
  }
  if (sticker->m_thumbnail.file_id.is_valid()) {
    result.push_back(sticker->m_thumbnail.file_id);
  }
  return result;
}

void StickersManager::flush_sent_animated_emoji_clicks() {
  if (sent_animated_emoji_clicks_.empty()) {
    return;
  }
  auto min_send_time = Time::now() - 30.0;
  auto it = sent_animated_emoji_clicks_.begin();
  while (it != sent_animated_emoji_clicks_.end() && it->send_time <= min_send_time) {
    ++it;
  }
  sent_animated_emoji_clicks_.erase(sent_animated_emoji_clicks_.begin(), it);
}

bool StickersManager::is_sent_animated_emoji_click(DialogId dialog_id, Slice emoji) {
  flush_sent_animated_emoji_clicks();
  for (const auto &click : sent_animated_emoji_clicks_) {
    if (click.dialog_id == dialog_id && click.emoji == emoji) {
      return true;
    }
  }
  return false;
}

vector<FileId> StickersManager::get_favorite_stickers(Promise<Unit> &&promise) {
  if (!are_favorite_stickers_loaded_) {
    load_favorite_stickers(std::move(promise));
    return {};
  }
  reload_favorite_stickers(false);

  promise.set_value(Unit());
  return favorite_sticker_ids_;
}

FileId StickersManager::get_animated_emoji_sound_file_id(const string &emoji) const {
  auto it = emoji_sounds_.find(remove_fitzpatrick_modifier(emoji).str());
  if (it == emoji_sounds_.end()) {
    return {};
  }
  return it->second;
}

std::pair<FileId, int> StickersManager::get_animated_emoji_sticker(const string &emoji) {
  return get_animated_emoji_sticker(get_animated_emoji_sticker_set(), emoji);
}

}  // namespace td
