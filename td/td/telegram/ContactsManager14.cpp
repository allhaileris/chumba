//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/ContactsManager.h"

#include "td/telegram/AuthManager.h"

#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogInviteLink.h"
#include "td/telegram/DialogLocation.h"
#include "td/telegram/FileReferenceManager.h"

#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/Global.h"


#include "td/telegram/InputGroupCallId.h"

#include "td/telegram/logevent/LogEvent.h"

#include "td/telegram/MessageSender.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/MessageTtl.h"
#include "td/telegram/MinChannel.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/NetQuery.h"

#include "td/telegram/PasswordManager.h"
#include "td/telegram/Photo.h"
#include "td/telegram/Photo.hpp"
#include "td/telegram/SecretChatLayer.h"

#include "td/telegram/ServerMessageId.h"
#include "td/telegram/StickerSetId.hpp"

#include "td/telegram/Td.h"

#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.hpp"
#include "td/telegram/UpdatesManager.h"
#include "td/telegram/Version.h"

#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"
#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/buffer.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"

#include <algorithm>
#include <limits>

#include <utility>

namespace td {
}  // namespace td
namespace td {

class GetUserPhotosQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  UserId user_id_;
  int32 offset_;
  int32 limit_;

 public:
  explicit GetUserPhotosQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(UserId user_id, tl_object_ptr<telegram_api::InputUser> &&input_user, int32 offset, int32 limit,
            int64 photo_id) {
    user_id_ = user_id;
    offset_ = offset;
    limit_ = limit;
    send_query(G()->net_query_creator().create(
        telegram_api::photos_getUserPhotos(std::move(input_user), offset, photo_id, limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::photos_getUserPhotos>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();

    LOG(INFO) << "Receive result for GetUserPhotosQuery: " << to_string(ptr);
    int32 constructor_id = ptr->get_id();
    if (constructor_id == telegram_api::photos_photos::ID) {
      auto photos = move_tl_object_as<telegram_api::photos_photos>(ptr);

      td_->contacts_manager_->on_get_users(std::move(photos->users_), "GetUserPhotosQuery");
      auto photos_size = narrow_cast<int32>(photos->photos_.size());
      td_->contacts_manager_->on_get_user_photos(user_id_, offset_, limit_, photos_size, std::move(photos->photos_));
    } else {
      CHECK(constructor_id == telegram_api::photos_photosSlice::ID);
      auto photos = move_tl_object_as<telegram_api::photos_photosSlice>(ptr);

      td_->contacts_manager_->on_get_users(std::move(photos->users_), "GetUserPhotosQuery");
      td_->contacts_manager_->on_get_user_photos(user_id_, offset_, limit_, photos->count_, std::move(photos->photos_));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

std::pair<int32, vector<const Photo *>> ContactsManager::get_user_profile_photos(UserId user_id, int32 offset,
                                                                                 int32 limit, Promise<Unit> &&promise) {
  std::pair<int32, vector<const Photo *>> result;
  result.first = -1;

  if (offset < 0) {
    promise.set_error(Status::Error(400, "Parameter offset must be non-negative"));
    return result;
  }
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return result;
  }
  if (limit > MAX_GET_PROFILE_PHOTOS) {
    limit = MAX_GET_PROFILE_PHOTOS;
  }

  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    promise.set_error(r_input_user.move_as_error());
    return result;
  }

  get_user_dialog_photo(user_id);  // apply pending user photo

  auto user_photos = &user_photos_[user_id];
  if (user_photos->getting_now) {
    promise.set_error(Status::Error(400, "Request for new profile photos has already been sent"));
    return result;
  }

  if (user_photos->count != -1) {  // know photo count
    CHECK(user_photos->offset != -1);
    result.first = user_photos->count;

    if (offset >= user_photos->count) {
      // offset if too big
      promise.set_value(Unit());
      return result;
    }

    if (limit > user_photos->count - offset) {
      limit = user_photos->count - offset;
    }

    int32 cache_begin = user_photos->offset;
    int32 cache_end = cache_begin + narrow_cast<int32>(user_photos->photos.size());
    if (cache_begin <= offset && offset + limit <= cache_end) {
      // answer query from cache
      for (int i = 0; i < limit; i++) {
        result.second.push_back(&user_photos->photos[i + offset - cache_begin]);
      }
      promise.set_value(Unit());
      return result;
    }

    if (cache_begin <= offset && offset < cache_end) {
      // adjust offset to the end of cache
      limit = offset + limit - cache_end;
      offset = cache_end;
    }
  }

  user_photos->getting_now = true;

  if (limit < MAX_GET_PROFILE_PHOTOS / 5) {
    limit = MAX_GET_PROFILE_PHOTOS / 5;  // make limit reasonable
  }

  td_->create_handler<GetUserPhotosQuery>(std::move(promise))
      ->send(user_id, r_input_user.move_as_ok(), offset, limit, 0);
  return result;
}

void ContactsManager::reload_user_profile_photo(UserId user_id, int64 photo_id, Promise<Unit> &&promise) {
  get_user_force(user_id);
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  // this request will be needed only to download the photo,
  // so there is no reason to combine different requests for a photo into one request
  td_->create_handler<GetUserPhotosQuery>(std::move(promise))
      ->send(user_id, r_input_user.move_as_ok(), -1, 1, photo_id);
}

void ContactsManager::on_chat_update(telegram_api::channelForbidden &channel, const char *source) {
  ChannelId channel_id(channel.id_);
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id << " from " << source << ": " << to_string(channel);
    return;
  }

  if (channel.flags_ == 0 && channel.access_hash_ == 0 && channel.title_.empty()) {
    Channel *c = get_channel_force(channel_id);
    LOG(ERROR) << "Receive empty " << to_string(channel) << " from " << source << ", have "
               << to_string(get_supergroup_object(channel_id, c));
    if (c == nullptr && !have_min_channel(channel_id)) {
      min_channels_[channel_id] = td::make_unique<MinChannel>();
    }
    return;
  }

  Channel *c = add_channel(channel_id, "on_channel_forbidden");
  if (c->status.is_banned()) {  // possibly uninited channel
    min_channels_.erase(channel_id);
  }
  if (c->access_hash != channel.access_hash_) {
    c->access_hash = channel.access_hash_;
    c->need_save_to_database = true;
  }
  on_update_channel_title(c, channel_id, std::move(channel.title_));
  on_update_channel_photo(c, channel_id, nullptr);
  if (c->date != 0) {
    c->date = 0;
    c->is_changed = true;
  }
  int32 unban_date = (channel.flags_ & CHANNEL_FLAG_HAS_UNBAN_DATE) != 0 ? channel.until_date_ : 0;
  on_update_channel_status(c, channel_id, DialogParticipantStatus::Banned(unban_date));
  // on_update_channel_username(c, channel_id, "");  // don't know if channel username is empty, so don't update it
  tl_object_ptr<telegram_api::chatBannedRights> banned_rights;  // == nullptr
  on_update_channel_default_permissions(c, channel_id, get_restricted_rights(std::move(banned_rights)));
  // on_update_channel_has_location(c, channel_id, false);
  on_update_channel_noforwards(c, channel_id, false);
  td_->messages_manager_->on_update_dialog_group_call(DialogId(channel_id), false, false, "receive channelForbidden");

  bool sign_messages = false;
  bool is_slow_mode_enabled = false;
  bool is_megagroup = (channel.flags_ & CHANNEL_FLAG_IS_MEGAGROUP) != 0;
  bool is_verified = false;
  bool is_scam = false;
  bool is_fake = false;

  {
    bool is_broadcast = (channel.flags_ & CHANNEL_FLAG_IS_BROADCAST) != 0;
    LOG_IF(ERROR, is_broadcast == is_megagroup)
        << "Receive wrong channel flag is_broadcast == is_megagroup == " << is_megagroup << " from " << source << ": "
        << oneline(to_string(channel));
  }

  if (is_megagroup) {
    sign_messages = true;
  }

  bool need_invalidate_channel_full = false;
  if (c->is_slow_mode_enabled != is_slow_mode_enabled || c->is_megagroup != is_megagroup ||
      !c->restriction_reasons.empty() || c->is_scam != is_scam || c->is_fake != is_fake) {
    // c->has_linked_channel = has_linked_channel;
    c->is_slow_mode_enabled = is_slow_mode_enabled;
    c->is_megagroup = is_megagroup;
    c->restriction_reasons.clear();
    c->is_scam = is_scam;
    c->is_fake = is_fake;

    c->is_changed = true;
    need_invalidate_channel_full = true;
  }
  if (c->sign_messages != sign_messages || c->is_verified != is_verified) {
    c->sign_messages = sign_messages;
    c->is_verified = is_verified;

    c->is_changed = true;
  }

  bool need_drop_participant_count = c->participant_count != 0;
  if (need_drop_participant_count) {
    c->participant_count = 0;
    c->is_changed = true;
  }

  if (c->cache_version != Channel::CACHE_VERSION) {
    c->cache_version = Channel::CACHE_VERSION;
    c->need_save_to_database = true;
  }
  c->is_received_from_server = true;
  update_channel(c, channel_id);

  if (need_drop_participant_count) {
    auto channel_full = get_channel_full(channel_id, true, "on_chat_update");
    if (channel_full != nullptr && channel_full->participant_count != 0) {
      channel_full->participant_count = 0;
      channel_full->administrator_count = 0;
      channel_full->is_changed = true;
      update_channel_full(channel_full, channel_id, "on_chat_update 2");
    }
  }
  if (need_invalidate_channel_full) {
    invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
  }
}

int32 ContactsManager::on_update_peer_located(vector<tl_object_ptr<telegram_api::PeerLocated>> &&peers,
                                              bool from_update) {
  auto now = G()->unix_time();
  bool need_update = false;
  int32 location_visibility_expire_date = -1;
  for (auto &peer_located_ptr : peers) {
    if (peer_located_ptr->get_id() == telegram_api::peerSelfLocated::ID) {
      auto peer_self_located = telegram_api::move_object_as<telegram_api::peerSelfLocated>(peer_located_ptr);
      if (peer_self_located->expires_ == 0 || peer_self_located->expires_ > G()->unix_time()) {
        location_visibility_expire_date = peer_self_located->expires_;
      }
      continue;
    }

    CHECK(peer_located_ptr->get_id() == telegram_api::peerLocated::ID);
    auto peer_located = telegram_api::move_object_as<telegram_api::peerLocated>(peer_located_ptr);
    DialogId dialog_id(peer_located->peer_);
    int32 expires_at = peer_located->expires_;
    int32 distance = peer_located->distance_;
    if (distance < 0 || distance > 50000000) {
      LOG(ERROR) << "Receive wrong distance to " << to_string(peer_located);
      continue;
    }
    if (expires_at <= now) {
      LOG(INFO) << "Skip expired result " << to_string(peer_located);
      continue;
    }

    auto dialog_type = dialog_id.get_type();
    if (dialog_type == DialogType::User) {
      auto user_id = dialog_id.get_user_id();
      if (!have_user(user_id)) {
        LOG(ERROR) << "Can't find " << user_id;
        continue;
      }
      if (expires_at < now + 86400) {
        user_nearby_timeout_.set_timeout_in(user_id.get(), expires_at - now + 1);
      }
    } else if (dialog_type == DialogType::Channel) {
      auto channel_id = dialog_id.get_channel_id();
      if (!have_channel(channel_id)) {
        LOG(ERROR) << "Can't find " << channel_id;
        continue;
      }
      if (expires_at != std::numeric_limits<int32>::max()) {
        LOG(ERROR) << "Receive expiring at " << expires_at << " group location in " << to_string(peer_located);
      }
      if (from_update) {
        LOG(ERROR) << "Receive nearby " << channel_id << " from update";
        continue;
      }
    } else {
      LOG(ERROR) << "Receive chat of wrong type in " << to_string(peer_located);
      continue;
    }

    td_->messages_manager_->force_create_dialog(dialog_id, "on_update_peer_located");

    if (from_update) {
      CHECK(dialog_type == DialogType::User);
      bool is_found = false;
      for (auto &dialog_nearby : users_nearby_) {
        if (dialog_nearby.dialog_id == dialog_id) {
          if (dialog_nearby.distance != distance) {
            dialog_nearby.distance = distance;
            need_update = true;
          }
          is_found = true;
          break;
        }
      }
      if (!is_found) {
        users_nearby_.emplace_back(dialog_id, distance);
        all_users_nearby_.insert(dialog_id.get_user_id());
        need_update = true;
      }
    } else {
      if (dialog_type == DialogType::User) {
        users_nearby_.emplace_back(dialog_id, distance);
        all_users_nearby_.insert(dialog_id.get_user_id());
      } else {
        channels_nearby_.emplace_back(dialog_id, distance);
      }
    }
  }
  if (need_update) {
    std::sort(users_nearby_.begin(), users_nearby_.end());
    send_update_users_nearby();
  }
  return location_visibility_expire_date;
}

void ContactsManager::on_update_chat_participant(ChatId chat_id, UserId user_id, int32 date,
                                                 DialogInviteLink invite_link,
                                                 tl_object_ptr<telegram_api::ChatParticipant> old_participant,
                                                 tl_object_ptr<telegram_api::ChatParticipant> new_participant) {
  if (!td_->auth_manager_->is_bot()) {
    LOG(ERROR) << "Receive updateChatParticipant by non-bot";
    return;
  }
  if (!chat_id.is_valid() || !user_id.is_valid() || date <= 0 ||
      (old_participant == nullptr && new_participant == nullptr)) {
    LOG(ERROR) << "Receive invalid updateChatParticipant in " << chat_id << " by " << user_id << " at " << date << ": "
               << to_string(old_participant) << " -> " << to_string(new_participant);
    return;
  }

  const Chat *c = get_chat(chat_id);
  if (c == nullptr) {
    LOG(ERROR) << "Receive updateChatParticipant in unknown " << chat_id;
    return;
  }

  DialogParticipant old_dialog_participant;
  DialogParticipant new_dialog_participant;
  if (old_participant != nullptr) {
    old_dialog_participant = DialogParticipant(std::move(old_participant), c->date, c->status.is_creator());
    if (new_participant == nullptr) {
      new_dialog_participant = DialogParticipant::left(old_dialog_participant.dialog_id_);
    } else {
      new_dialog_participant = DialogParticipant(std::move(new_participant), c->date, c->status.is_creator());
    }
  } else {
    new_dialog_participant = DialogParticipant(std::move(new_participant), c->date, c->status.is_creator());
    old_dialog_participant = DialogParticipant::left(new_dialog_participant.dialog_id_);
  }
  if (old_dialog_participant.dialog_id_ != new_dialog_participant.dialog_id_ || !old_dialog_participant.is_valid() ||
      !new_dialog_participant.is_valid()) {
    LOG(ERROR) << "Receive wrong updateChatParticipant: " << old_dialog_participant << " -> " << new_dialog_participant;
    return;
  }
  if (new_dialog_participant.dialog_id_ == DialogId(get_my_id()) &&
      new_dialog_participant.status_ != get_chat_status(chat_id) && false) {
    LOG(ERROR) << "Have status " << get_chat_status(chat_id) << " after receiving updateChatParticipant in " << chat_id
               << " by " << user_id << " at " << date << " from " << old_dialog_participant << " to "
               << new_dialog_participant;
  }

  send_update_chat_member(DialogId(chat_id), user_id, date, invite_link, old_dialog_participant,
                          new_dialog_participant);
}

class DismissSuggestionQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit DismissSuggestionQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(SuggestedAction action) {
    dialog_id_ = action.dialog_id_;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id_, AccessRights::Read);
    CHECK(input_peer != nullptr);

    send_query(G()->net_query_creator().create(
        telegram_api::help_dismissSuggestion(std::move(input_peer), action.get_suggested_action_str())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_dismissSuggestion>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "DismissSuggestionQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::dismiss_dialog_suggested_action(SuggestedAction action, Promise<Unit> &&promise) {
  auto dialog_id = action.dialog_id_;
  if (!td_->messages_manager_->have_dialog(dialog_id)) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!td_->messages_manager_->have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  auto it = dialog_suggested_actions_.find(dialog_id);
  if (it == dialog_suggested_actions_.end() || !td::contains(it->second, action)) {
    return promise.set_value(Unit());
  }

  auto action_str = action.get_suggested_action_str();
  if (action_str.empty()) {
    return promise.set_value(Unit());
  }

  auto &queries = dismiss_suggested_action_queries_[dialog_id];
  queries.push_back(std::move(promise));
  if (queries.size() == 1) {
    auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), action](Result<Unit> &&result) {
      send_closure(actor_id, &ContactsManager::on_dismiss_suggested_action, action, std::move(result));
    });
    td_->create_handler<DismissSuggestionQuery>(std::move(query_promise))->send(std::move(action));
  }
}

class EditChatAdminQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChatId chat_id_;

 public:
  explicit EditChatAdminQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChatId chat_id, tl_object_ptr<telegram_api::InputUser> &&input_user, bool is_administrator) {
    chat_id_ = chat_id;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_editChatAdmin(chat_id.get(), std::move(input_user), is_administrator)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editChatAdmin>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    if (!result) {
      LOG(ERROR) << "Receive false as result of messages.editChatAdmin";
      return on_error(Status::Error(400, "Can't edit chat administrators"));
    }

    // result will come in the updates
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("EditChatAdminQuery");
  }
};

void ContactsManager::send_edit_chat_admin_query(ChatId chat_id, UserId user_id, bool is_administrator,
                                                 Promise<Unit> &&promise) {
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  td_->create_handler<EditChatAdminQuery>(std::move(promise))
      ->send(chat_id, r_input_user.move_as_ok(), is_administrator);
}

Status ContactsManager::can_manage_dialog_invite_links(DialogId dialog_id, bool creator_only) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "can_manage_dialog_invite_links")) {
    return Status::Error(400, "Chat not found");
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return Status::Error(400, "Can't invite members to a private chat");
    case DialogType::Chat: {
      const Chat *c = get_chat(dialog_id.get_chat_id());
      if (c == nullptr) {
        return Status::Error(400, "Chat info not found");
      }
      if (!c->is_active) {
        return Status::Error(400, "Chat is deactivated");
      }
      bool have_rights = creator_only ? c->status.is_creator() : c->status.can_manage_invite_links();
      if (!have_rights) {
        return Status::Error(400, "Not enough rights to manage chat invite link");
      }
      break;
    }
    case DialogType::Channel: {
      const Channel *c = get_channel(dialog_id.get_channel_id());
      if (c == nullptr) {
        return Status::Error(400, "Chat info not found");
      }
      bool have_rights = creator_only ? c->status.is_creator() : c->status.can_manage_invite_links();
      if (!have_rights) {
        return Status::Error(400, "Not enough rights to manage chat invite link");
      }
      break;
    }
    case DialogType::SecretChat:
      return Status::Error(400, "Can't invite members to a secret chat");
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  return Status::OK();
}

void ContactsManager::on_update_chat_participant_count(Chat *c, ChatId chat_id, int32 participant_count, int32 version,
                                                       const string &debug_str) {
  if (version <= -1) {
    LOG(ERROR) << "Receive wrong version " << version << " in " << chat_id << debug_str;
    return;
  }

  if (version < c->version) {
    // some outdated data
    LOG(INFO) << "Receive number of members in " << chat_id << " with version " << version << debug_str
              << ", but current version is " << c->version;
    return;
  }

  if (c->participant_count != participant_count) {
    if (version == c->version && participant_count != 0) {
      // version is not changed when deleted user is removed from the chat
      LOG_IF(ERROR, c->participant_count != participant_count + 1)
          << "Number of members in " << chat_id << " has changed from " << c->participant_count << " to "
          << participant_count << ", but version " << c->version << " remains unchanged" << debug_str;
      repair_chat_participants(chat_id);
    }

    c->participant_count = participant_count;
    c->version = version;
    c->is_changed = true;
    return;
  }

  if (version > c->version) {
    c->version = version;
    c->need_save_to_database = true;
  }
}

void ContactsManager::on_update_channel_administrator_count(ChannelId channel_id, int32 administrator_count) {
  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_administrator_count");
  if (channel_full != nullptr && channel_full->administrator_count != administrator_count) {
    channel_full->administrator_count = administrator_count;
    channel_full->is_changed = true;

    if (channel_full->participant_count < channel_full->administrator_count) {
      channel_full->participant_count = channel_full->administrator_count;

      auto c = get_channel(channel_id);
      if (c != nullptr && c->participant_count != channel_full->participant_count) {
        c->participant_count = channel_full->participant_count;
        c->is_changed = true;
        update_channel(c, channel_id);
      }
    }

    update_channel_full(channel_full, channel_id, "on_update_channel_administrator_count");
  }
}

void ContactsManager::update_dialog_online_member_count(const vector<DialogParticipant> &participants,
                                                        DialogId dialog_id, bool is_from_server) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  int32 online_member_count = 0;
  int32 time = G()->unix_time();
  for (const auto &participant : participants) {
    if (participant.dialog_id_.get_type() != DialogType::User) {
      continue;
    }
    auto user_id = participant.dialog_id_.get_user_id();
    auto u = get_user(user_id);
    if (u != nullptr && !u->is_deleted && !u->is_bot) {
      if (get_user_was_online(u, user_id) > time) {
        online_member_count++;
      }
      if (is_from_server) {
        u->online_member_dialogs[dialog_id] = time;
      }
    }
  }
  td_->messages_manager_->on_update_dialog_online_member_count(dialog_id, online_member_count, is_from_server);
}

Result<ContactsManager::BotData> ContactsManager::get_bot_data(UserId user_id) const {
  auto p = users_.find(user_id);
  if (p == users_.end()) {
    return Status::Error(400, "Bot not found");
  }

  auto bot = p->second.get();
  if (!bot->is_bot) {
    return Status::Error(400, "User is not a bot");
  }
  if (bot->is_deleted) {
    return Status::Error(400, "Bot is deleted");
  }
  if (!bot->is_received) {
    return Status::Error(400, "Bot is inaccessible");
  }

  BotData bot_data;
  bot_data.username = bot->username;
  bot_data.can_join_groups = bot->can_join_groups;
  bot_data.can_read_all_group_messages = bot->can_read_all_group_messages;
  bot_data.is_inline = bot->is_inline_bot;
  bot_data.need_location = bot->need_location_bot;
  return bot_data;
}

void ContactsManager::get_channel_statistics_dc_id_impl(ChannelId channel_id, bool for_full_statistics,
                                                        Promise<DcId> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto channel_full = get_channel_full(channel_id, false, "get_channel_statistics_dc_id_impl");
  if (channel_full == nullptr) {
    return promise.set_error(Status::Error(400, "Chat full info not found"));
  }

  if (!channel_full->stats_dc_id.is_exact() || (for_full_statistics && !channel_full->can_view_statistics)) {
    return promise.set_error(Status::Error(400, "Chat statistics is not available"));
  }

  promise.set_value(DcId(channel_full->stats_dc_id));
}

void ContactsManager::on_deleted_contacts(const vector<UserId> &deleted_contact_user_ids) {
  LOG(INFO) << "Contacts deletion has finished for " << deleted_contact_user_ids;

  for (auto user_id : deleted_contact_user_ids) {
    auto u = get_user(user_id);
    CHECK(u != nullptr);
    if (!u->is_contact) {
      continue;
    }

    LOG(INFO) << "Drop contact with " << user_id;
    on_update_user_is_contact(u, user_id, false, false);
    CHECK(u->is_is_contact_changed);
    u->cache_version = 0;
    u->is_repaired = false;
    update_user(u, user_id);
    CHECK(!u->is_contact);
    CHECK(!contacts_hints_.has_key(user_id.get()));
  }
}

FileSourceId ContactsManager::get_chat_full_file_source_id(ChatId chat_id) {
  if (get_chat_full(chat_id) != nullptr) {
    VLOG(file_references) << "Don't need to create file source for full " << chat_id;
    // chat full was already added, source ID was registered and shouldn't be needed
    return FileSourceId();
  }

  auto &source_id = chat_full_file_source_ids_[chat_id];
  if (!source_id.is_valid()) {
    source_id = td_->file_reference_manager_->create_chat_full_file_source(chat_id);
  }
  VLOG(file_references) << "Return " << source_id << " for full " << chat_id;
  return source_id;
}

ContactsManager::ChannelFull *ContactsManager::get_channel_full(ChannelId channel_id, bool only_local,
                                                                const char *source) {
  auto p = channels_full_.find(channel_id);
  if (p == channels_full_.end()) {
    return nullptr;
  }

  auto channel_full = p->second.get();
  if (!only_local && channel_full->is_expired() && !td_->auth_manager_->is_bot()) {
    send_get_channel_full_query(channel_full, channel_id, Auto(), source);
  }

  return channel_full;
}

void ContactsManager::on_ignored_restriction_reasons_changed() {
  for (auto user_id : restricted_user_ids_) {
    send_closure(G()->td(), &Td::send_update,
                 td_api::make_object<td_api::updateUser>(get_user_object(user_id, get_user(user_id))));
  }
  for (auto channel_id : restricted_channel_ids_) {
    send_closure(
        G()->td(), &Td::send_update,
        td_api::make_object<td_api::updateSupergroup>(get_supergroup_object(channel_id, get_channel(channel_id))));
  }
}

void ContactsManager::on_invite_link_info_expire_timeout_callback(void *contacts_manager_ptr, int64 dialog_id_long) {
  if (G()->close_flag()) {
    return;
  }

  auto contacts_manager = static_cast<ContactsManager *>(contacts_manager_ptr);
  send_closure_later(contacts_manager->actor_id(contacts_manager), &ContactsManager::on_invite_link_info_expire_timeout,
                     DialogId(dialog_id_long));
}

void ContactsManager::save_user_to_database(User *u, UserId user_id) {
  CHECK(u != nullptr);
  if (u->is_being_saved) {
    return;
  }
  if (loaded_from_database_users_.count(user_id)) {
    save_user_to_database_impl(u, user_id, get_user_database_value(u));
    return;
  }
  if (load_user_from_database_queries_.count(user_id) != 0) {
    return;
  }

  load_user_from_database_impl(user_id, Auto());
}

ChannelId ContactsManager::get_linked_channel_id(ChannelId channel_id) const {
  auto channel_full = get_channel_full(channel_id);
  if (channel_full != nullptr) {
    return channel_full->linked_channel_id;
  }

  auto it = linked_channel_ids_.find(channel_id);
  if (it != linked_channel_ids_.end()) {
    return it->second;
  }

  return ChannelId();
}

void ContactsManager::on_update_channel_full_invite_link(
    ChannelFull *channel_full, tl_object_ptr<telegram_api::chatInviteExported> &&invite_link) {
  CHECK(channel_full != nullptr);
  if (update_permanent_invite_link(channel_full->invite_link, DialogInviteLink(std::move(invite_link)))) {
    channel_full->is_changed = true;
  }
}

const ContactsManager::ChannelFull *ContactsManager::get_channel_full_const(ChannelId channel_id) const {
  auto p = channels_full_.find(channel_id);
  if (p == channels_full_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

DialogParticipantStatus ContactsManager::get_channel_permissions(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return DialogParticipantStatus::Banned(0);
  }
  return get_channel_permissions(c);
}

string ContactsManager::get_user_username(UserId user_id) const {
  if (!user_id.is_valid()) {
    return string();
  }

  auto u = get_user(user_id);
  if (u == nullptr) {
    return string();
  }
  return u->username;
}

bool ContactsManager::is_user_online(UserId user_id, int32 tolerance) const {
  int32 was_online = get_user_was_online(get_user(user_id), user_id);
  return was_online > G()->unix_time() - tolerance;
}

int32 ContactsManager::get_secret_chat_ttl(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return 0;
  }
  return c->ttl;
}

bool ContactsManager::is_user_bot(UserId user_id) const {
  auto u = get_user(user_id);
  return u != nullptr && !u->is_deleted && u->is_bot;
}

bool ContactsManager::is_channel_public(ChannelId channel_id) const {
  return is_channel_public(get_channel(channel_id));
}
}  // namespace td
