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

#include "td/telegram/ContactsManager.h"
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

void StickersManager::create_new_sticker_set(UserId user_id, string &title, string &short_name, bool is_masks,
                                             vector<tl_object_ptr<td_api::InputSticker>> &&stickers, string software,
                                             Promise<Unit> &&promise) {
  bool is_bot = td_->auth_manager_->is_bot();
  if (!is_bot) {
    user_id = td_->contacts_manager_->get_my_id();
  }

  TRY_RESULT_PROMISE(promise, input_user, td_->contacts_manager_->get_input_user(user_id));

  title = strip_empty_characters(title, MAX_STICKER_SET_TITLE_LENGTH);
  if (title.empty()) {
    return promise.set_error(Status::Error(400, "Sticker set title can't be empty"));
  }

  short_name = strip_empty_characters(short_name, MAX_STICKER_SET_SHORT_NAME_LENGTH);
  if (short_name.empty()) {
    return promise.set_error(Status::Error(400, "Sticker set name can't be empty"));
  }

  if (stickers.empty()) {
    return promise.set_error(Status::Error(400, "At least 1 sticker must be specified"));
  }

  vector<FileId> file_ids;
  file_ids.reserve(stickers.size());
  vector<FileId> local_file_ids;
  vector<FileId> url_file_ids;
  size_t animated_sticker_count = 0;
  for (auto &sticker : stickers) {
    auto r_file_id = prepare_input_sticker(sticker.get());
    if (r_file_id.is_error()) {
      return promise.set_error(r_file_id.move_as_error());
    }
    auto file_id = std::get<0>(r_file_id.ok());
    auto is_url = std::get<1>(r_file_id.ok());
    auto is_local = std::get<2>(r_file_id.ok());
    auto is_animated = std::get<3>(r_file_id.ok());
    if (is_animated) {
      animated_sticker_count++;
      if (is_url) {
        return promise.set_error(Status::Error(400, "Animated stickers can't be uploaded by URL"));
      }
    }

    file_ids.push_back(file_id);
    if (is_url) {
      url_file_ids.push_back(file_id);
    } else if (is_local) {
      local_file_ids.push_back(file_id);
    }
  }
  if (animated_sticker_count != stickers.size() && animated_sticker_count != 0) {
    return promise.set_error(Status::Error(400, "All stickers must be either animated or static"));
  }
  bool is_animated = animated_sticker_count == stickers.size();

  auto pending_new_sticker_set = make_unique<PendingNewStickerSet>();
  pending_new_sticker_set->user_id = user_id;
  pending_new_sticker_set->title = std::move(title);
  pending_new_sticker_set->short_name = short_name;
  pending_new_sticker_set->is_masks = is_masks;
  pending_new_sticker_set->is_animated = is_animated;
  pending_new_sticker_set->file_ids = std::move(file_ids);
  pending_new_sticker_set->stickers = std::move(stickers);
  pending_new_sticker_set->software = std::move(software);
  pending_new_sticker_set->promise = std::move(promise);

  auto &multipromise = pending_new_sticker_set->upload_files_multipromise;

  int64 random_id;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || pending_new_sticker_sets_.find(random_id) != pending_new_sticker_sets_.end());
  pending_new_sticker_sets_[random_id] = std::move(pending_new_sticker_set);

  multipromise.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), random_id](Result<Unit> result) {
    send_closure_later(actor_id, &StickersManager::on_new_stickers_uploaded, random_id, std::move(result));
  }));
  auto lock_promise = multipromise.get_promise();

  for (auto file_id : url_file_ids) {
    do_upload_sticker_file(user_id, file_id, nullptr, multipromise.get_promise());
  }

  for (auto file_id : local_file_ids) {
    upload_sticker_file(user_id, file_id, multipromise.get_promise());
  }

  lock_promise.set_value(Unit());
}

SecretInputMedia StickersManager::get_secret_input_media(FileId sticker_file_id,
                                                         tl_object_ptr<telegram_api::InputEncryptedFile> input_file,
                                                         BufferSlice thumbnail) const {
  const Sticker *sticker = get_sticker(sticker_file_id);
  CHECK(sticker != nullptr);
  auto file_view = td_->file_manager_->get_file_view(sticker_file_id);
  if (file_view.is_encrypted_secret()) {
    if (file_view.has_remote_location()) {
      input_file = file_view.main_remote_location().as_input_encrypted_file();
    }
    if (!input_file) {
      return {};
    }
    if (sticker->s_thumbnail.file_id.is_valid() && thumbnail.empty()) {
      return {};
    }
  } else if (!file_view.is_encrypted()) {
    if (!sticker->set_id.is_valid()) {
      // stickers without set can't be sent by id and access_hash
      return {};
    }
  } else {
    return {};
  }

  tl_object_ptr<secret_api::InputStickerSet> input_sticker_set = make_tl_object<secret_api::inputStickerSetEmpty>();
  if (sticker->set_id.is_valid()) {
    const StickerSet *sticker_set = get_sticker_set(sticker->set_id);
    CHECK(sticker_set != nullptr);
    if (sticker_set->is_inited) {
      input_sticker_set = make_tl_object<secret_api::inputStickerSetShortName>(sticker_set->short_name);
    } else {
      // TODO load sticker set
    }
  }

  vector<tl_object_ptr<secret_api::DocumentAttribute>> attributes;
  attributes.push_back(
      secret_api::make_object<secret_api::documentAttributeSticker>(sticker->alt, std::move(input_sticker_set)));
  if (sticker->dimensions.width != 0 && sticker->dimensions.height != 0) {
    attributes.push_back(secret_api::make_object<secret_api::documentAttributeImageSize>(sticker->dimensions.width,
                                                                                         sticker->dimensions.height));
  }

  if (file_view.is_encrypted_secret()) {
    auto &encryption_key = file_view.encryption_key();
    return SecretInputMedia{std::move(input_file),
                            make_tl_object<secret_api::decryptedMessageMediaDocument>(
                                std::move(thumbnail), sticker->s_thumbnail.dimensions.width,
                                sticker->s_thumbnail.dimensions.height, get_sticker_mime_type(sticker),
                                narrow_cast<int32>(file_view.size()), BufferSlice(encryption_key.key_slice()),
                                BufferSlice(encryption_key.iv_slice()), std::move(attributes), "")};
  } else {
    CHECK(!file_view.is_encrypted());
    auto &remote_location = file_view.remote_location();
    if (remote_location.is_web()) {
      // web stickers shouldn't have set_id
      LOG(ERROR) << "Have a web sticker in " << sticker->set_id;
      return {};
    }
    return SecretInputMedia{nullptr, make_tl_object<secret_api::decryptedMessageMediaExternalDocument>(
                                         remote_location.get_id(), remote_location.get_access_hash(), 0 /*date*/,
                                         get_sticker_mime_type(sticker), narrow_cast<int32>(file_view.size()),
                                         make_tl_object<secret_api::photoSizeEmpty>("t"),
                                         remote_location.get_dc_id().get_raw_id(), std::move(attributes))};
  }
}

class GetArchivedStickerSetsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  StickerSetId offset_sticker_set_id_;
  bool is_masks_;

 public:
  explicit GetArchivedStickerSetsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(bool is_masks, StickerSetId offset_sticker_set_id, int32 limit) {
    offset_sticker_set_id_ = offset_sticker_set_id;
    is_masks_ = is_masks;

    int32 flags = 0;
    if (is_masks_) {
      flags |= telegram_api::messages_getArchivedStickers::MASKS_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getArchivedStickers(flags, is_masks /*ignored*/, offset_sticker_set_id.get(), limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getArchivedStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetArchivedStickerSetsQuery: " << to_string(ptr);
    td_->stickers_manager_->on_get_archived_sticker_sets(is_masks_, offset_sticker_set_id_, std::move(ptr->sets_),
                                                         ptr->count_);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

std::pair<int32, vector<StickerSetId>> StickersManager::get_archived_sticker_sets(bool is_masks,
                                                                                  StickerSetId offset_sticker_set_id,
                                                                                  int32 limit, bool force,
                                                                                  Promise<Unit> &&promise) {
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }

  vector<StickerSetId> &sticker_set_ids = archived_sticker_set_ids_[is_masks];
  int32 total_count = total_archived_sticker_set_count_[is_masks];
  if (total_count >= 0) {
    auto offset_it = sticker_set_ids.begin();
    if (offset_sticker_set_id.is_valid()) {
      offset_it = std::find(sticker_set_ids.begin(), sticker_set_ids.end(), offset_sticker_set_id);
      if (offset_it == sticker_set_ids.end()) {
        offset_it = sticker_set_ids.begin();
      } else {
        ++offset_it;
      }
    }
    vector<StickerSetId> result;
    while (result.size() < static_cast<size_t>(limit)) {
      if (offset_it == sticker_set_ids.end()) {
        break;
      }
      auto sticker_set_id = *offset_it++;
      if (!sticker_set_id.is_valid()) {  // end of the list
        promise.set_value(Unit());
        return {total_count, std::move(result)};
      }
      result.push_back(sticker_set_id);
    }
    if (result.size() == static_cast<size_t>(limit) || force) {
      promise.set_value(Unit());
      return {total_count, std::move(result)};
    }
  }

  td_->create_handler<GetArchivedStickerSetsQuery>(std::move(promise))->send(is_masks, offset_sticker_set_id, limit);
  return {};
}

void StickersManager::on_load_installed_sticker_sets_finished(bool is_masks,
                                                              vector<StickerSetId> &&installed_sticker_set_ids,
                                                              bool from_database) {
  bool need_reload = false;
  vector<StickerSetId> old_installed_sticker_set_ids;
  if (!are_installed_sticker_sets_loaded_[is_masks] && !installed_sticker_set_ids_[is_masks].empty()) {
    old_installed_sticker_set_ids = std::move(installed_sticker_set_ids_[is_masks]);
  }
  installed_sticker_set_ids_[is_masks].clear();
  for (auto set_id : installed_sticker_set_ids) {
    CHECK(set_id.is_valid());

    auto sticker_set = get_sticker_set(set_id);
    CHECK(sticker_set != nullptr);
    CHECK(sticker_set->is_inited);
    CHECK(sticker_set->is_masks == is_masks);
    if (sticker_set->is_installed && !sticker_set->is_archived) {
      installed_sticker_set_ids_[is_masks].push_back(set_id);
    } else {
      need_reload = true;
    }
  }
  if (need_reload) {
    LOG(ERROR) << "Reload installed " << (is_masks ? "mask " : "") << "sticker sets, because only "
               << installed_sticker_set_ids_[is_masks].size() << " of " << installed_sticker_set_ids.size()
               << " are really installed after loading from " << (from_database ? "database" : "server");
    reload_installed_sticker_sets(is_masks, true);
  } else if (!old_installed_sticker_set_ids.empty() &&
             old_installed_sticker_set_ids != installed_sticker_set_ids_[is_masks]) {
    LOG(ERROR) << "Reload installed " << (is_masks ? "mask " : "") << "sticker sets, because they has changed from "
               << old_installed_sticker_set_ids << " to " << installed_sticker_set_ids_[is_masks]
               << " after loading from " << (from_database ? "database" : "server");
    reload_installed_sticker_sets(is_masks, true);
  }

  are_installed_sticker_sets_loaded_[is_masks] = true;
  need_update_installed_sticker_sets_[is_masks] = true;
  send_update_installed_sticker_sets(from_database);
  auto promises = std::move(load_installed_sticker_sets_queries_[is_masks]);
  load_installed_sticker_sets_queries_[is_masks].clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

StickerSetId StickersManager::on_get_input_sticker_set(FileId sticker_file_id,
                                                       tl_object_ptr<telegram_api::InputStickerSet> &&set_ptr,
                                                       MultiPromiseActor *load_data_multipromise_ptr) {
  if (set_ptr == nullptr) {
    return StickerSetId();
  }
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
      if (load_data_multipromise_ptr == nullptr) {
        LOG(ERROR) << "Receive sticker set " << set->short_name_ << " by its short name";
        return search_sticker_set(set->short_name_, Auto());
      }
      auto set_id = search_sticker_set(set->short_name_, load_data_multipromise_ptr->get_promise());
      if (!set_id.is_valid()) {
        load_data_multipromise_ptr->add_promise(PromiseCreator::lambda(
            [actor_id = actor_id(this), sticker_file_id, short_name = set->short_name_](Result<Unit> result) {
              if (result.is_ok()) {
                // just in case
                send_closure(actor_id, &StickersManager::on_resolve_sticker_set_short_name, sticker_file_id,
                             short_name);
              }
            }));
      }
      // always return empty StickerSetId, because we can't trust the set_id provided by the peer in the secret chat
      // the real sticker set id will be set in on_get_sticker if and only if the sticker is really from the set
      return StickerSetId();
    }
    case telegram_api::inputStickerSetAnimatedEmoji::ID:
    case telegram_api::inputStickerSetAnimatedEmojiAnimations::ID:
      return add_special_sticker_set(SpecialStickerSetType(set_ptr)).id_;
    case telegram_api::inputStickerSetDice::ID:
      return StickerSetId();
    default:
      UNREACHABLE();
      return StickerSetId();
  }
}

class AddStickerToSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit AddStickerToSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &short_name, tl_object_ptr<telegram_api::inputStickerSetItem> &&input_sticker) {
    send_query(G()->net_query_creator().create(telegram_api::stickers_addStickerToSet(
        make_tl_object<telegram_api::inputStickerSetShortName>(short_name), std::move(input_sticker))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stickers_addStickerToSet>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->stickers_manager_->on_get_messages_sticker_set(StickerSetId(), result_ptr.move_as_ok(), true,
                                                        "AddStickerToSetQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    CHECK(status.is_error());
    promise_.set_error(std::move(status));
  }
};

void StickersManager::on_added_sticker_uploaded(int64 random_id, Result<Unit> result) {
  auto it = pending_add_sticker_to_sets_.find(random_id);
  CHECK(it != pending_add_sticker_to_sets_.end());

  auto pending_add_sticker_to_set = std::move(it->second);
  CHECK(pending_add_sticker_to_set != nullptr);

  pending_add_sticker_to_sets_.erase(it);

  if (result.is_error()) {
    pending_add_sticker_to_set->promise.set_error(result.move_as_error());
    return;
  }

  td_->create_handler<AddStickerToSetQuery>(std::move(pending_add_sticker_to_set->promise))
      ->send(pending_add_sticker_to_set->short_name,
             get_input_sticker(pending_add_sticker_to_set->sticker.get(), pending_add_sticker_to_set->file_id));
}

void StickersManager::merge_stickers(FileId new_id, FileId old_id, bool can_delete_old) {
  CHECK(old_id.is_valid() && new_id.is_valid());
  CHECK(new_id != old_id);

  LOG(INFO) << "Merge stickers " << new_id << " and " << old_id;
  const Sticker *old_ = get_sticker(old_id);
  CHECK(old_ != nullptr);

  auto new_it = stickers_.find(new_id);
  if (new_it == stickers_.end()) {
    auto &old = stickers_[old_id];
    if (!can_delete_old) {
      dup_sticker(new_id, old_id);
    } else {
      old->file_id = new_id;
      stickers_.emplace(new_id, std::move(old));
    }
  } else {
    Sticker *new_ = new_it->second.get();
    CHECK(new_ != nullptr);

    if (old_->set_id == new_->set_id && (old_->alt != new_->alt || old_->set_id != new_->set_id ||
                                         (!old_->is_animated && !new_->is_animated && old_->dimensions.width != 0 &&
                                          old_->dimensions.height != 0 && old_->dimensions != new_->dimensions))) {
      LOG(ERROR) << "Sticker has changed: alt = (" << old_->alt << ", " << new_->alt << "), set_id = (" << old_->set_id
                 << ", " << new_->set_id << "), dimensions = (" << old_->dimensions << ", " << new_->dimensions << ")";
    }

    if (old_->s_thumbnail != new_->s_thumbnail) {
      //    LOG_STATUS(td_->file_manager_->merge(new_->s_thumbnail.file_id, old_->s_thumbnail.file_id));
    }
    if (old_->m_thumbnail != new_->m_thumbnail) {
      //    LOG_STATUS(td_->file_manager_->merge(new_->m_thumbnail.file_id, old_->m_thumbnail.file_id));
    }
  }
  LOG_STATUS(td_->file_manager_->merge(new_id, old_id));
  if (can_delete_old) {
    stickers_.erase(old_id);
  }
}

void StickersManager::set_sticker_set_thumbnail(UserId user_id, string &short_name,
                                                tl_object_ptr<td_api::InputFile> &&thumbnail, Promise<Unit> &&promise) {
  TRY_RESULT_PROMISE(promise, input_user, td_->contacts_manager_->get_input_user(user_id));

  short_name = clean_username(strip_empty_characters(short_name, MAX_STICKER_SET_SHORT_NAME_LENGTH));
  if (short_name.empty()) {
    return promise.set_error(Status::Error(400, "Sticker set name can't be empty"));
  }

  auto it = short_name_to_sticker_set_id_.find(short_name);
  const StickerSet *sticker_set = it == short_name_to_sticker_set_id_.end() ? nullptr : get_sticker_set(it->second);
  if (sticker_set != nullptr && sticker_set->was_loaded) {
    return do_set_sticker_set_thumbnail(user_id, short_name, std::move(thumbnail), std::move(promise));
  }

  do_reload_sticker_set(
      StickerSetId(), make_tl_object<telegram_api::inputStickerSetShortName>(short_name), 0,
      PromiseCreator::lambda([actor_id = actor_id(this), user_id, short_name, thumbnail = std::move(thumbnail),
                              promise = std::move(promise)](Result<Unit> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          send_closure(actor_id, &StickersManager::do_set_sticker_set_thumbnail, user_id, std::move(short_name),
                       std::move(thumbnail), std::move(promise));
        }
      }));
}

void StickersManager::create_sticker(FileId file_id, string minithumbnail, PhotoSize thumbnail, Dimensions dimensions,
                                     tl_object_ptr<telegram_api::documentAttributeSticker> sticker, bool is_animated,
                                     MultiPromiseActor *load_data_multipromise_ptr) {
  if (is_animated && dimensions.width == 0) {
    dimensions.width = 512;
    dimensions.height = 512;
  }

  auto s = make_unique<Sticker>();
  s->file_id = file_id;
  s->dimensions = dimensions;
  if (!td_->auth_manager_->is_bot()) {
    s->minithumbnail = std::move(minithumbnail);
  }
  add_sticker_thumbnail(s.get(), std::move(thumbnail));
  if (sticker != nullptr) {
    s->set_id = on_get_input_sticker_set(file_id, std::move(sticker->stickerset_), load_data_multipromise_ptr);
    s->alt = std::move(sticker->alt_);

    s->is_mask = (sticker->flags_ & telegram_api::documentAttributeSticker::MASK_MASK) != 0;
    if ((sticker->flags_ & telegram_api::documentAttributeSticker::MASK_COORDS_MASK) != 0) {
      CHECK(sticker->mask_coords_ != nullptr);
      int32 point = sticker->mask_coords_->n_;
      if (0 <= point && point <= 3) {
        s->point = sticker->mask_coords_->n_;
        s->x_shift = sticker->mask_coords_->x_;
        s->y_shift = sticker->mask_coords_->y_;
        s->scale = sticker->mask_coords_->zoom_;
      }
    }
  }
  s->is_animated = is_animated;
  on_get_sticker(std::move(s), sticker != nullptr);
}

void StickersManager::update_sticker_set(StickerSet *sticker_set, const char *source) {
  CHECK(sticker_set != nullptr);
  if (sticker_set->is_changed || sticker_set->need_save_to_database) {
    if (G()->parameters().use_file_db && !G()->close_flag()) {
      LOG(INFO) << "Save " << sticker_set->id << " to database from " << source;
      if (sticker_set->is_inited) {
        G()->td_db()->get_sqlite_pmc()->set(get_sticker_set_database_key(sticker_set->id),
                                            get_sticker_set_database_value(sticker_set, false, source), Auto());
      }
      if (sticker_set->was_loaded) {
        G()->td_db()->get_sqlite_pmc()->set(get_full_sticker_set_database_key(sticker_set->id),
                                            get_sticker_set_database_value(sticker_set, true, source), Auto());
      }
    }
    if (sticker_set->is_changed && sticker_set->was_loaded && sticker_set->was_update_sent) {
      send_closure(G()->td(), &Td::send_update,
                   td_api::make_object<td_api::updateStickerSet>(get_sticker_set_object(sticker_set->id)));
    }
    sticker_set->is_changed = false;
    sticker_set->need_save_to_database = false;
    if (sticker_set->is_inited) {
      update_load_requests(sticker_set, false, Status::OK());
    }
  }
}

void StickersManager::view_featured_sticker_sets(const vector<StickerSetId> &sticker_set_ids) {
  for (auto sticker_set_id : sticker_set_ids) {
    auto set = get_sticker_set(sticker_set_id);
    if (set != nullptr && !set->is_viewed) {
      if (td::contains(featured_sticker_set_ids_, sticker_set_id)) {
        need_update_featured_sticker_sets_ = true;
      }
      set->is_viewed = true;
      pending_viewed_featured_sticker_set_ids_.insert(sticker_set_id);
      update_sticker_set(set, "view_featured_sticker_sets");
    }
  }

  send_update_featured_sticker_sets();

  if (!pending_viewed_featured_sticker_set_ids_.empty() && !pending_featured_sticker_set_views_timeout_.has_timeout()) {
    LOG(INFO) << "Have pending viewed trending sticker sets";
    pending_featured_sticker_set_views_timeout_.set_callback(read_featured_sticker_sets);
    pending_featured_sticker_set_views_timeout_.set_callback_data(static_cast<void *>(td_));
    pending_featured_sticker_set_views_timeout_.set_timeout_in(MAX_FEATURED_STICKER_SET_VIEW_DELAY);
  }
}

void StickersManager::reload_special_sticker_set_by_type(SpecialStickerSetType type, bool is_recursive) {
  if (G()->close_flag()) {
    return;
  }

  auto &sticker_set = add_special_sticker_set(type);
  if (sticker_set.is_being_reloaded_) {
    return;
  }

  if (!sticker_set.id_.is_valid()) {
    return reload_special_sticker_set(sticker_set, 0);
  }

  const auto *s = get_sticker_set(sticker_set.id_);
  if (s != nullptr && s->is_inited && s->was_loaded) {
    return reload_special_sticker_set(sticker_set, s->is_loaded ? s->hash : 0);
  }
  if (!is_recursive) {
    auto promise = PromiseCreator::lambda([actor_id = actor_id(this), type = std::move(type)](Unit result) mutable {
      send_closure(actor_id, &StickersManager::reload_special_sticker_set_by_type, std::move(type), true);
    });
    return load_sticker_sets({sticker_set.id_}, std::move(promise));
  }

  reload_special_sticker_set(sticker_set, 0);
}

void StickersManager::on_load_featured_sticker_sets_finished(vector<StickerSetId> &&featured_sticker_set_ids) {
  if (!featured_sticker_set_ids_.empty() && featured_sticker_set_ids != featured_sticker_set_ids_) {
    // always invalidate old featured sticker sets when current featured sticker sets change
    on_old_featured_sticker_sets_invalidated();
  }
  featured_sticker_set_ids_ = std::move(featured_sticker_set_ids);
  are_featured_sticker_sets_loaded_ = true;
  need_update_featured_sticker_sets_ = true;
  send_update_featured_sticker_sets();
  auto promises = std::move(load_featured_sticker_sets_queries_);
  load_featured_sticker_sets_queries_.clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

double StickersManager::get_emoji_language_code_last_difference_time(const string &language_code) {
  auto it = emoji_language_code_last_difference_times_.find(language_code);
  if (it != emoji_language_code_last_difference_times_.end()) {
    return it->second;
  }
  auto &result = emoji_language_code_last_difference_times_[language_code];
  auto old_unix_time = to_integer<int32>(G()->td_db()->get_sqlite_sync_pmc()->get(
      get_emoji_language_code_last_difference_time_database_key(language_code)));
  int32 passed_time = max(static_cast<int32>(0), G()->unix_time() - old_unix_time);
  result = Time::now_cached() - passed_time;
  return result;
}

void StickersManager::unregister_emoji(const string &emoji, FullMessageId full_message_id, const char *source) {
  CHECK(!emoji.empty());
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  LOG(INFO) << "Unregister emoji " << emoji << " from " << full_message_id << " from " << source;
  auto it = emoji_messages_.find(emoji);
  CHECK(it != emoji_messages_.end());
  auto &full_message_ids = it->second.full_message_ids;
  auto is_deleted = full_message_ids.erase(full_message_id) > 0;
  LOG_CHECK(is_deleted) << source << ' ' << emoji << ' ' << full_message_id;

  if (full_message_ids.empty()) {
    emoji_messages_.erase(it);
  }
}

void StickersManager::on_resolve_sticker_set_short_name(FileId sticker_file_id, const string &short_name) {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Resolve sticker " << sticker_file_id << " set to " << short_name;
  StickerSetId set_id = search_sticker_set(short_name, Auto());
  if (set_id.is_valid()) {
    auto &s = stickers_[sticker_file_id];
    CHECK(s != nullptr);
    CHECK(s->file_id == sticker_file_id);
    if (s->set_id != set_id) {
      s->set_id = set_id;
    }
  }
}

tl_object_ptr<td_api::MaskPoint> StickersManager::get_mask_point_object(int32 point) {
  switch (point) {
    case 0:
      return td_api::make_object<td_api::maskPointForehead>();
    case 1:
      return td_api::make_object<td_api::maskPointEyes>();
    case 2:
      return td_api::make_object<td_api::maskPointMouth>();
    case 3:
      return td_api::make_object<td_api::maskPointChin>();
    default:
      UNREACHABLE();
      return nullptr;
  }
}

string &StickersManager::get_input_sticker_emojis(td_api::InputSticker *sticker) {
  CHECK(sticker != nullptr);
  auto constructor_id = sticker->get_id();
  if (constructor_id == td_api::inputStickerStatic::ID) {
    return static_cast<td_api::inputStickerStatic *>(sticker)->emojis_;
  }
  CHECK(constructor_id == td_api::inputStickerAnimated::ID);
  return static_cast<td_api::inputStickerAnimated *>(sticker)->emojis_;
}

void StickersManager::reload_sticker_set(StickerSetId sticker_set_id, int64 access_hash, Promise<Unit> &&promise) {
  do_reload_sticker_set(sticker_set_id,
                        make_tl_object<telegram_api::inputStickerSetID>(sticker_set_id.get(), access_hash), 0,
                        std::move(promise));
}

const StickersManager::StickerSet *StickersManager::get_sticker_set(StickerSetId sticker_set_id) const {
  auto sticker_set = sticker_sets_.find(sticker_set_id);
  if (sticker_set == sticker_sets_.end()) {
    return nullptr;
  }

  return sticker_set->second.get();
}

FileSourceId StickersManager::get_app_config_file_source_id() {
  if (!app_config_file_source_id_.is_valid()) {
    app_config_file_source_id_ = td_->file_reference_manager_->create_app_config_file_source();
  }
  return app_config_file_source_id_;
}

string StickersManager::get_emoji_language_code_last_difference_time_database_key(const string &language_code) {
  return PSTRING() << "emojid$" << language_code;
}
}  // namespace td
