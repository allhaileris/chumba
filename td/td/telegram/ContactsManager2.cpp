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
#include "td/telegram/files/FileManager.h"
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
#include "td/telegram/TdDb.h"
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


#include <limits>

#include <utility>

namespace td {
}  // namespace td
namespace td {

void ContactsManager::on_chat_update(telegram_api::channel &channel, const char *source) {
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

  bool is_min = (channel.flags_ & CHANNEL_FLAG_IS_MIN) != 0;
  bool has_access_hash = (channel.flags_ & CHANNEL_FLAG_HAS_ACCESS_HASH) != 0;
  auto access_hash = has_access_hash ? channel.access_hash_ : 0;

  bool has_linked_channel = (channel.flags_ & CHANNEL_FLAG_HAS_LINKED_CHAT) != 0;
  bool sign_messages = (channel.flags_ & CHANNEL_FLAG_SIGN_MESSAGES) != 0;
  bool is_slow_mode_enabled = (channel.flags_ & CHANNEL_FLAG_IS_SLOW_MODE_ENABLED) != 0;
  bool is_megagroup = (channel.flags_ & CHANNEL_FLAG_IS_MEGAGROUP) != 0;
  bool is_verified = (channel.flags_ & CHANNEL_FLAG_IS_VERIFIED) != 0;
  auto restriction_reasons = get_restriction_reasons(std::move(channel.restriction_reason_));
  bool is_scam = (channel.flags_ & CHANNEL_FLAG_IS_SCAM) != 0;
  bool is_fake = (channel.flags_ & CHANNEL_FLAG_IS_FAKE) != 0;
  bool is_gigagroup = (channel.flags_ & CHANNEL_FLAG_IS_GIGAGROUP) != 0;
  bool have_participant_count = (channel.flags_ & CHANNEL_FLAG_HAS_PARTICIPANT_COUNT) != 0;
  int32 participant_count = have_participant_count ? channel.participants_count_ : 0;

  if (have_participant_count) {
    auto channel_full = get_channel_full_const(channel_id);
    if (channel_full != nullptr && channel_full->administrator_count > participant_count) {
      participant_count = channel_full->administrator_count;
    }
  }

  {
    bool is_broadcast = (channel.flags_ & CHANNEL_FLAG_IS_BROADCAST) != 0;
    LOG_IF(ERROR, is_broadcast == is_megagroup)
        << "Receive wrong channel flag is_broadcast == is_megagroup == " << is_megagroup << " from " << source << ": "
        << oneline(to_string(channel));
  }

  if (is_megagroup) {
    LOG_IF(ERROR, sign_messages) << "Need to sign messages in the supergroup " << channel_id << " from " << source;
    sign_messages = true;
  } else {
    LOG_IF(ERROR, is_slow_mode_enabled) << "Slow mode enabled in the " << channel_id << " from " << source;
    LOG_IF(ERROR, is_gigagroup) << "Receive broadcast group as channel " << channel_id << " from " << source;
    is_slow_mode_enabled = false;
    is_gigagroup = false;
  }
  if (is_gigagroup) {
    remove_dialog_suggested_action(SuggestedAction{SuggestedAction::Type::ConvertToGigagroup, DialogId(channel_id)});
  }

  DialogParticipantStatus status = [&] {
    bool has_left = (channel.flags_ & CHANNEL_FLAG_USER_HAS_LEFT) != 0;
    bool is_creator = (channel.flags_ & CHANNEL_FLAG_USER_IS_CREATOR) != 0;

    if (is_creator) {
      bool is_anonymous = channel.admin_rights_ != nullptr &&
                          (channel.admin_rights_->flags_ & telegram_api::chatAdminRights::ANONYMOUS_MASK) != 0;
      return DialogParticipantStatus::Creator(!has_left, is_anonymous, string());
    } else if (channel.admin_rights_ != nullptr) {
      return get_dialog_participant_status(false, std::move(channel.admin_rights_), string());
    } else if (channel.banned_rights_ != nullptr) {
      return get_dialog_participant_status(!has_left, std::move(channel.banned_rights_));
    } else if (has_left) {
      return DialogParticipantStatus::Left();
    } else {
      return DialogParticipantStatus::Member();
    }
  }();

  if (is_min) {
    Channel *c = get_channel_force(channel_id);
    if (c != nullptr) {
      LOG(DEBUG) << "Receive known min " << channel_id;
      on_update_channel_title(c, channel_id, std::move(channel.title_));
      on_update_channel_username(c, channel_id, std::move(channel.username_));
      on_update_channel_photo(c, channel_id, std::move(channel.photo_));
      on_update_channel_default_permissions(c, channel_id,
                                            get_restricted_rights(std::move(channel.default_banned_rights_)));
      on_update_channel_has_location(c, channel_id, channel.has_geo_);
      on_update_channel_noforwards(c, channel_id, channel.noforwards_);

      if (c->has_linked_channel != has_linked_channel || c->is_slow_mode_enabled != is_slow_mode_enabled ||
          c->is_megagroup != is_megagroup || c->restriction_reasons != restriction_reasons || c->is_scam != is_scam ||
          c->is_fake != is_fake || c->is_gigagroup != is_gigagroup) {
        c->has_linked_channel = has_linked_channel;
        c->is_slow_mode_enabled = is_slow_mode_enabled;
        c->is_megagroup = is_megagroup;
        c->restriction_reasons = std::move(restriction_reasons);
        c->is_scam = is_scam;
        c->is_fake = is_fake;
        c->is_gigagroup = is_gigagroup;

        c->is_changed = true;
        invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
      }
      // sign_messages isn't known for min-channels
      if (c->is_verified != is_verified) {
        c->is_verified = is_verified;

        c->is_changed = true;
      }

      update_channel(c, channel_id);
    } else {
      auto min_channel = td::make_unique<MinChannel>();
      min_channel->photo_ =
          get_dialog_photo(td_->file_manager_.get(), DialogId(channel_id), access_hash, std::move(channel.photo_));
      if (td_->auth_manager_->is_bot()) {
        min_channel->photo_.minithumbnail.clear();
      }
      min_channel->title_ = std::move(channel.title_);
      min_channel->is_megagroup_ = is_megagroup;

      min_channels_[channel_id] = std::move(min_channel);
    }
    return;
  }
  if (!has_access_hash) {
    LOG(ERROR) << "Receive non-min " << channel_id << " without access_hash from " << source;
    return;
  }

  if (status.is_creator()) {
    // to correctly calculate is_ownership_transferred in on_update_channel_status
    get_channel_force(channel_id);
  }

  Channel *c = add_channel(channel_id, "on_channel");
  if (c->status.is_banned()) {  // possibly uninited channel
    min_channels_.erase(channel_id);
  }
  if (c->access_hash != access_hash) {
    c->access_hash = access_hash;
    c->need_save_to_database = true;
  }
  on_update_channel_title(c, channel_id, std::move(channel.title_));
  if (c->date != channel.date_) {
    c->date = channel.date_;
    c->is_changed = true;
  }
  on_update_channel_photo(c, channel_id, std::move(channel.photo_));
  on_update_channel_status(c, channel_id, std::move(status));
  on_update_channel_username(c, channel_id, std::move(channel.username_));  // uses status, must be called after
  on_update_channel_default_permissions(c, channel_id,
                                        get_restricted_rights(std::move(channel.default_banned_rights_)));
  on_update_channel_has_location(c, channel_id, channel.has_geo_);
  on_update_channel_noforwards(c, channel_id, channel.noforwards_);

  bool need_update_participant_count = have_participant_count && participant_count != c->participant_count;
  if (need_update_participant_count) {
    c->participant_count = participant_count;
    c->is_changed = true;
  }

  bool need_invalidate_channel_full = false;
  if (c->has_linked_channel != has_linked_channel || c->is_slow_mode_enabled != is_slow_mode_enabled ||
      c->is_megagroup != is_megagroup || c->restriction_reasons != restriction_reasons || c->is_scam != is_scam ||
      c->is_fake != is_fake || c->is_gigagroup != is_gigagroup) {
    c->has_linked_channel = has_linked_channel;
    c->is_slow_mode_enabled = is_slow_mode_enabled;
    c->is_megagroup = is_megagroup;
    c->restriction_reasons = std::move(restriction_reasons);
    c->is_scam = is_scam;
    c->is_fake = is_fake;
    c->is_gigagroup = is_gigagroup;

    c->is_changed = true;
    need_invalidate_channel_full = true;
  }
  if (c->is_verified != is_verified || c->sign_messages != sign_messages) {
    c->is_verified = is_verified;
    c->sign_messages = sign_messages;

    c->is_changed = true;
  }

  if (c->cache_version != Channel::CACHE_VERSION) {
    c->cache_version = Channel::CACHE_VERSION;
    c->need_save_to_database = true;
  }
  c->is_received_from_server = true;
  update_channel(c, channel_id);

  if (need_update_participant_count) {
    auto channel_full = get_channel_full(channel_id, true, "on_chat_update");
    if (channel_full != nullptr && channel_full->participant_count != participant_count) {
      channel_full->participant_count = participant_count;
      channel_full->is_changed = true;
      update_channel_full(channel_full, channel_id, "on_chat_update");
    }
  }

  if (need_invalidate_channel_full) {
    invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
  }

  bool has_active_group_call = (channel.flags_ & CHANNEL_FLAG_HAS_ACTIVE_GROUP_CALL) != 0;
  bool is_group_call_empty = (channel.flags_ & CHANNEL_FLAG_IS_GROUP_CALL_NON_EMPTY) == 0;
  td_->messages_manager_->on_update_dialog_group_call(DialogId(channel_id), has_active_group_call, is_group_call_empty,
                                                      "receive channel");
}

bool ContactsManager::delete_profile_photo_from_cache(UserId user_id, int64 profile_photo_id, bool send_updates) {
  CHECK(profile_photo_id != 0);

  // we have subsequence of user photos in user_photos_
  // ProfilePhoto in User and Photo in UserFull

  User *u = get_user_force(user_id);
  bool is_main_photo_deleted = u != nullptr && u->photo.id == profile_photo_id;

  // update photo list
  auto it = user_photos_.find(user_id);
  if (it != user_photos_.end() && it->second.count > 0) {
    auto user_photos = &it->second;
    auto old_size = user_photos->photos.size();
    if (td::remove_if(user_photos->photos,
                      [profile_photo_id](const auto &photo) { return photo.id.get() == profile_photo_id; })) {
      auto removed_photos = old_size - user_photos->photos.size();
      CHECK(removed_photos > 0);
      LOG_IF(ERROR, removed_photos != 1) << "Had " << removed_photos << " photos with ID " << profile_photo_id;
      user_photos->count -= narrow_cast<int32>(removed_photos);
      // offset was not changed
      CHECK(user_photos->count >= 0);
    } else {
      // failed to find photo to remove from cache
      // don't know how to adjust user_photos->offset, so drop photos cache
      LOG(INFO) << "Drop photos of " << user_id;
      user_photos->photos.clear();
      user_photos->count = -1;
      user_photos->offset = -1;
    }
  }

  // update Photo in UserFull
  auto user_full = get_user_full_force(user_id);
  if (user_full != nullptr && !user_full->photo.is_empty() &&
      (is_main_photo_deleted || user_full->photo.id.get() == profile_photo_id)) {
    if (it != user_photos_.end() && it->second.count != -1 && it->second.offset == 0 && !it->second.photos.empty()) {
      // found exact new photo
      if (it->second.photos[0] != user_full->photo) {
        user_full->photo = it->second.photos[0];
        user_full->is_changed = true;
      }
    } else {
      // repair UserFull photo
      user_full->expires_at = 0.0;
      user_full->photo = Photo();
      user_full->is_changed = true;

      load_user_full(user_id, true, Auto(), "delete_profile_photo_from_cache");
    }
    if (send_updates) {
      update_user_full(user_full, user_id, "delete_profile_photo_from_cache");
    }
  }

  // update ProfilePhoto in User
  if (is_main_photo_deleted) {
    bool need_reget_user = false;
    if (it != user_photos_.end() && it->second.count != -1 && it->second.offset == 0 && !it->second.photos.empty()) {
      // found exact new photo
      do_update_user_photo(u, user_id,
                           as_profile_photo(td_->file_manager_.get(), user_id, u->access_hash, it->second.photos[0]),
                           false, "delete_profile_photo_from_cache");
    } else {
      do_update_user_photo(u, user_id, ProfilePhoto(), false, "delete_profile_photo_from_cache 2");
      need_reget_user = it == user_photos_.end() || it->second.count != 0;
    }
    if (send_updates) {
      update_user(u, user_id);
    }
    return need_reget_user;
  }

  return false;
}

void ContactsManager::update_chat_full(ChatFull *chat_full, ChatId chat_id, const char *source, bool from_database) {
  CHECK(chat_full != nullptr);
  unavailable_chat_fulls_.erase(chat_id);  // don't needed anymore

  chat_full->need_send_update |= chat_full->is_changed;
  chat_full->need_save_to_database |= chat_full->is_changed;
  chat_full->is_changed = false;
  if (chat_full->need_send_update || chat_full->need_save_to_database) {
    LOG(INFO) << "Update full " << chat_id << " from " << source;
  }
  if (chat_full->need_send_update) {
    vector<DialogAdministrator> administrators;
    vector<UserId> bot_user_ids;
    for (const auto &participant : chat_full->participants) {
      if (participant.status_.is_administrator() && participant.dialog_id_.get_type() == DialogType::User) {
        administrators.emplace_back(participant.dialog_id_.get_user_id(), participant.status_.get_rank(),
                                    participant.status_.is_creator());
      }
      if (participant.dialog_id_.get_type() == DialogType::User) {
        auto user_id = participant.dialog_id_.get_user_id();
        if (is_user_bot(user_id)) {
          bot_user_ids.push_back(user_id);
        }
      }
    }
    td::remove_if(chat_full->bot_commands, [&bot_user_ids](const BotCommands &commands) {
      return !td::contains(bot_user_ids, commands.get_bot_user_id());
    });

    on_update_dialog_administrators(DialogId(chat_id), std::move(administrators), chat_full->version != -1,
                                    from_database);
    send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(chat_id),
                       std::move(bot_user_ids), from_database);

    {
      Chat *c = get_chat(chat_id);
      CHECK(c == nullptr || c->is_update_basic_group_sent);
    }
    if (!chat_full->is_update_chat_full_sent) {
      LOG(ERROR) << "Send partial updateBasicGroupFullInfo for " << chat_id << " from " << source;
      chat_full->is_update_chat_full_sent = true;
    }
    send_closure(
        G()->td(), &Td::send_update,
        make_tl_object<td_api::updateBasicGroupFullInfo>(get_basic_group_id_object(chat_id, "update_chat_full"),
                                                         get_basic_group_full_info_object(chat_full)));
    chat_full->need_send_update = false;
  }
  if (chat_full->need_save_to_database) {
    if (!from_database) {
      save_chat_full(chat_full, chat_id);
    }
    chat_full->need_save_to_database = false;
  }
}

class ToggleChannelSignaturesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit ToggleChannelSignaturesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, bool sign_messages) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::channels_toggleSignatures(std::move(input_channel), sign_messages)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_toggleSignatures>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ToggleChannelSignaturesQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ToggleChannelSignaturesQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::toggle_channel_sign_messages(ChannelId channel_id, bool sign_messages, Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }
  if (get_channel_type(c) == ChannelType::Megagroup) {
    return promise.set_error(Status::Error(400, "Message signatures can't be toggled in supergroups"));
  }
  if (!get_channel_permissions(c).can_change_info_and_settings()) {
    return promise.set_error(Status::Error(400, "Not enough rights to toggle channel sign messages"));
  }

  td_->create_handler<ToggleChannelSignaturesQuery>(std::move(promise))->send(channel_id, sign_messages);
}

void ContactsManager::on_update_chat_pinned_message(ChatId chat_id, MessageId pinned_message_id, int32 version) {
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id;
    return;
  }
  auto c = get_chat_force(chat_id);
  if (c == nullptr) {
    LOG(INFO) << "Ignoring update about unknown " << chat_id;
    return;
  }

  LOG(INFO) << "Receive updateChatPinnedMessage in " << chat_id << " with " << pinned_message_id << " and version "
            << version << ". Current version is " << c->version << "/" << c->pinned_message_version;

  if (c->status.is_left()) {
    // possible if updates come out of order
    repair_chat_participants(chat_id);  // just in case
    return;
  }
  if (version <= -1) {
    LOG(ERROR) << "Receive wrong version " << version << " for " << chat_id;
    return;
  }
  CHECK(c->version >= 0);

  if (version >= c->pinned_message_version) {
    if (version != c->version + 1 && version != c->version) {
      LOG(INFO) << "Pinned message of " << chat_id << " with version " << c->version
                << " has changed, but new version is " << version;
      repair_chat_participants(chat_id);
    } else if (version == c->version + 1) {
      c->version = version;
      c->need_save_to_database = true;
    }
    td_->messages_manager_->on_update_dialog_last_pinned_message_id(DialogId(chat_id), pinned_message_id);
    if (version > c->pinned_message_version) {
      LOG(INFO) << "Change pinned message version of " << chat_id << " from " << c->pinned_message_version << " to "
                << version;
      c->pinned_message_version = version;
      c->need_save_to_database = true;
    }
    update_chat(c, chat_id);
  }
}

void ContactsManager::on_update_channel_full_photo(ChannelFull *channel_full, ChannelId channel_id, Photo photo) {
  CHECK(channel_full != nullptr);
  if (photo != channel_full->photo) {
    channel_full->photo = std::move(photo);
    channel_full->is_changed = true;
  }
  if (channel_full->photo.is_empty()) {
    drop_channel_photos(channel_id, true, false, "on_update_channel_full_photo");
  }

  auto photo_file_ids = photo_get_file_ids(channel_full->photo);
  if (channel_full->registered_photo_file_ids == photo_file_ids) {
    return;
  }

  auto &file_source_id = channel_full->file_source_id;
  if (!file_source_id.is_valid()) {
    auto it = channel_full_file_source_ids_.find(channel_id);
    if (it != channel_full_file_source_ids_.end()) {
      VLOG(file_references) << "Move " << it->second << " inside of " << channel_id;
      file_source_id = it->second;
      channel_full_file_source_ids_.erase(it);
    } else {
      VLOG(file_references) << "Need to create new file source for full " << channel_id;
      file_source_id = td_->file_reference_manager_->create_channel_full_file_source(channel_id);
    }
  }

  for (auto &file_id : channel_full->registered_photo_file_ids) {
    td_->file_manager_->remove_file_source(file_id, file_source_id);
  }
  channel_full->registered_photo_file_ids = std::move(photo_file_ids);
  for (auto &file_id : channel_full->registered_photo_file_ids) {
    td_->file_manager_->add_file_source(file_id, file_source_id);
  }
}

void ContactsManager::speculative_delete_channel_participant(ChannelId channel_id, UserId deleted_user_id, bool by_me) {
  if (!deleted_user_id.is_valid()) {
    return;
  }

  auto it = cached_channel_participants_.find(channel_id);
  if (it != cached_channel_participants_.end()) {
    auto &participants = it->second;
    for (size_t i = 0; i < participants.size(); i++) {
      if (participants[i].dialog_id_ == DialogId(deleted_user_id)) {
        participants.erase(participants.begin() + i);
        update_channel_online_member_count(channel_id, false);
        break;
      }
    }
  }

  if (is_user_bot(deleted_user_id)) {
    auto channel_full = get_channel_full_force(channel_id, true, "speculative_delete_channel_participant");
    if (channel_full != nullptr && td::remove(channel_full->bot_user_ids, deleted_user_id)) {
      channel_full->need_save_to_database = true;
      update_channel_full(channel_full, channel_id, "speculative_delete_channel_participant");

      send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                         channel_full->bot_user_ids, false);
    }
  }

  speculative_add_channel_participant_count(channel_id, -1, by_me);
}

void ContactsManager::update_contacts_hints(const User *u, UserId user_id, bool from_database) {
  bool is_contact = is_user_contact(u, user_id, false);
  if (td_->auth_manager_->is_bot()) {
    LOG_IF(ERROR, is_contact) << "Bot has " << user_id << " in the contacts list";
    return;
  }

  int64 key = user_id.get();
  string old_value = contacts_hints_.key_to_string(key);
  string new_value = is_contact ? u->first_name + " " + u->last_name + " " + u->username : "";

  if (new_value != old_value) {
    if (is_contact) {
      contacts_hints_.add(key, new_value);
    } else {
      contacts_hints_.remove(key);
    }
  }

  if (G()->parameters().use_chat_info_db) {
    // update contacts database
    if (!are_contacts_loaded_) {
      if (!from_database && load_contacts_queries_.empty()) {
        search_contacts("", std::numeric_limits<int32>::max(), Auto());
      }
    } else {
      if (old_value.empty() == is_contact) {
        save_contacts_to_database();
      }
    }
  }
}

void ContactsManager::on_update_user_local_was_online(User *u, UserId user_id, int32 local_was_online) {
  CHECK(u != nullptr);
  if (u->is_deleted || u->is_bot || u->is_support || user_id == get_my_id()) {
    return;
  }
  if (u->was_online > G()->unix_time_cached()) {
    // if user is currently online, ignore local online
    return;
  }

  // bring users online for 30 seconds
  local_was_online += 30;
  if (local_was_online < G()->unix_time_cached() + 2 || local_was_online <= u->local_was_online ||
      local_was_online <= u->was_online) {
    return;
  }

  LOG(DEBUG) << "Update " << user_id << " local online from " << u->local_was_online << " to " << local_was_online;
  bool old_is_online = u->local_was_online > G()->unix_time_cached();
  u->local_was_online = local_was_online;
  u->is_status_changed = true;

  if (!old_is_online) {
    u->is_online_status_changed = true;
  }
}

void ContactsManager::load_chat_from_database_impl(ChatId chat_id, Promise<Unit> promise) {
  LOG(INFO) << "Load " << chat_id << " from database";
  auto &load_chat_queries = load_chat_from_database_queries_[chat_id];
  load_chat_queries.push_back(std::move(promise));
  if (load_chat_queries.size() == 1u) {
    G()->td_db()->get_sqlite_pmc()->get(get_chat_database_key(chat_id), PromiseCreator::lambda([chat_id](string value) {
                                          send_closure(G()->contacts_manager(),
                                                       &ContactsManager::on_load_chat_from_database, chat_id,
                                                       std::move(value), false);
                                        }));
  }
}

const DialogParticipant *ContactsManager::get_channel_participant_from_cache(ChannelId channel_id,
                                                                             DialogId participant_dialog_id) {
  auto channel_participants_it = channel_participants_.find(channel_id);
  if (channel_participants_it == channel_participants_.end()) {
    return nullptr;
  }

  auto &participants = channel_participants_it->second.participants_;
  CHECK(!participants.empty());
  auto it = participants.find(participant_dialog_id);
  if (it != participants.end()) {
    it->second.participant_.status_.update_restrictions();
    it->second.last_access_date_ = G()->unix_time();
    return &it->second.participant_;
  }
  return nullptr;
}

tl_object_ptr<td_api::secretChat> ContactsManager::get_secret_chat_object_const(SecretChatId secret_chat_id,
                                                                                const SecretChat *secret_chat) const {
  return td_api::make_object<td_api::secretChat>(secret_chat_id.get(),
                                                 get_user_id_object(secret_chat->user_id, "secretChat"),
                                                 get_secret_chat_state_object(secret_chat->state),
                                                 secret_chat->is_outbound, secret_chat->key_hash, secret_chat->layer);
}

void ContactsManager::on_clear_imported_contacts(vector<Contact> &&contacts, vector<size_t> contacts_unique_id,
                                                 std::pair<vector<size_t>, vector<Contact>> &&to_add,
                                                 Promise<Unit> &&promise) {
  LOG(INFO) << "Add " << to_add.first.size() << " contacts";
  next_all_imported_contacts_ = std::move(contacts);
  imported_contacts_unique_id_ = std::move(contacts_unique_id);
  imported_contacts_pos_ = std::move(to_add.first);

  do_import_contacts(std::move(to_add.second), 0, std::move(promise));
}

void ContactsManager::on_update_chat_photo(Chat *c, ChatId chat_id,
                                           tl_object_ptr<telegram_api::ChatPhoto> &&chat_photo_ptr) {
  DialogPhoto new_chat_photo =
      get_dialog_photo(td_->file_manager_.get(), DialogId(chat_id), 0, std::move(chat_photo_ptr));
  if (td_->auth_manager_->is_bot()) {
    new_chat_photo.minithumbnail.clear();
  }

  if (new_chat_photo != c->photo) {
    c->photo = new_chat_photo;
    c->is_photo_changed = true;
    c->need_save_to_database = true;
  }
}

void ContactsManager::on_update_channel_full_location(ChannelFull *channel_full, ChannelId channel_id,
                                                      const DialogLocation &location) {
  if (channel_full->location != location) {
    channel_full->location = location;
    channel_full->is_changed = true;
  }

  Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);
  on_update_channel_has_location(c, channel_id, !location.empty());
  update_channel(c, channel_id);
}

void ContactsManager::save_channel_full(const ChannelFull *channel_full, ChannelId channel_id) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }

  LOG(INFO) << "Trying to save to database full " << channel_id;
  CHECK(channel_full != nullptr);
  G()->td_db()->get_sqlite_pmc()->set(get_channel_full_database_key(channel_id),
                                      get_channel_full_database_value(channel_full), Auto());
}

void ContactsManager::on_update_channel_location(ChannelId channel_id, const DialogLocation &location) {
  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_location");
  if (channel_full != nullptr) {
    on_update_channel_full_location(channel_full, channel_id, location);
    update_channel_full(channel_full, channel_id, "on_update_channel_location");
  }
}

void ContactsManager::on_chat_update(telegram_api::chatEmpty &chat, const char *source) {
  ChatId chat_id(chat.id_);
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id << " from " << source;
    return;
  }

  if (!have_chat(chat_id)) {
    LOG(ERROR) << "Have no information about " << chat_id << " but received chatEmpty from " << source;
  }
}

string ContactsManager::get_channel_title(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    auto min_channel = get_min_channel(channel_id);
    if (min_channel != nullptr) {
      return min_channel->title_;
    }
    return string();
  }
  return c->title;
}

void ContactsManager::on_update_user_local_was_online(UserId user_id, int32 local_was_online) {
  CHECK(user_id.is_valid());

  User *u = get_user_force(user_id);
  if (u == nullptr) {
    return;
  }

  on_update_user_local_was_online(u, user_id, local_was_online);
  update_user(u, user_id);
}

DialogParticipantStatus ContactsManager::get_chat_permissions(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return DialogParticipantStatus::Banned(0);
  }
  return get_chat_permissions(c);
}

string ContactsManager::get_secret_chat_title(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return string();
  }
  return get_user_title(c->user_id);
}

void ContactsManager::send_update_users_nearby() const {
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateUsersNearby>(get_chats_nearby_object(users_nearby_)));
}

const DialogPhoto *ContactsManager::get_chat_dialog_photo(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return nullptr;
  }
  return &c->photo;
}

tl_object_ptr<td_api::basicGroup> ContactsManager::get_basic_group_object(ChatId chat_id) {
  return get_basic_group_object(chat_id, get_chat(chat_id));
}

string ContactsManager::get_chat_full_database_key(ChatId chat_id) {
  return PSTRING() << "grf" << chat_id.get();
}
}  // namespace td
