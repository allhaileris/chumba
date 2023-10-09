//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/ContactsManager.h"



#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogInviteLink.h"
#include "td/telegram/DialogLocation.h"


#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/Global.h"
#include "td/telegram/GroupCallManager.h"

#include "td/telegram/InputGroupCallId.h"

#include "td/telegram/logevent/LogEvent.h"

#include "td/telegram/MessageSender.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/MessageTtl.h"
#include "td/telegram/MinChannel.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/NetQuery.h"
#include "td/telegram/NotificationManager.h"
#include "td/telegram/PasswordManager.h"
#include "td/telegram/Photo.h"
#include "td/telegram/Photo.hpp"
#include "td/telegram/SecretChatLayer.h"

#include "td/telegram/ServerMessageId.h"
#include "td/telegram/StickerSetId.hpp"
#include "td/telegram/StickersManager.h"
#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.hpp"

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



namespace td {
}  // namespace td
namespace td {

class GetFullChannelQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit GetFullChannelQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, tl_object_ptr<telegram_api::InputChannel> &&input_channel) {
    channel_id_ = channel_id;
    send_query(G()->net_query_creator().create(telegram_api::channels_getFullChannel(std::move(input_channel))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getFullChannel>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    td_->contacts_manager_->on_get_users(std::move(ptr->users_), "GetFullChannelQuery");
    td_->contacts_manager_->on_get_chats(std::move(ptr->chats_), "GetFullChannelQuery");
    td_->contacts_manager_->on_get_chat_full(std::move(ptr->full_chat_), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetFullChannelQuery");
    td_->contacts_manager_->on_get_channel_full_failed(channel_id_);
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::on_get_chat_full(tl_object_ptr<telegram_api::ChatFull> &&chat_full_ptr, Promise<Unit> &&promise) {
  LOG(INFO) << "Receive " << to_string(chat_full_ptr);
  if (chat_full_ptr->get_id() == telegram_api::chatFull::ID) {
    auto chat = move_tl_object_as<telegram_api::chatFull>(chat_full_ptr);
    ChatId chat_id(chat->id_);
    if (!chat_id.is_valid()) {
      LOG(ERROR) << "Receive invalid " << chat_id;
      return promise.set_value(Unit());
    }

    {
      MessageId pinned_message_id;
      if ((chat->flags_ & CHAT_FULL_FLAG_HAS_PINNED_MESSAGE) != 0) {
        pinned_message_id = MessageId(ServerMessageId(chat->pinned_msg_id_));
      }
      Chat *c = get_chat(chat_id);
      if (c == nullptr) {
        LOG(ERROR) << "Can't find " << chat_id;
        return promise.set_value(Unit());
      } else if (c->version >= c->pinned_message_version) {
        LOG(INFO) << "Receive pinned " << pinned_message_id << " in " << chat_id << " with version " << c->version
                  << ". Current version is " << c->pinned_message_version;
        td_->messages_manager_->on_update_dialog_last_pinned_message_id(DialogId(chat_id), pinned_message_id);
        if (c->version > c->pinned_message_version) {
          c->pinned_message_version = c->version;
          c->need_save_to_database = true;
          update_chat(c, chat_id);
        }
      }
    }
    {
      FolderId folder_id;
      if ((chat->flags_ & CHAT_FULL_FLAG_HAS_FOLDER_ID) != 0) {
        folder_id = FolderId(chat->folder_id_);
      }
      td_->messages_manager_->on_update_dialog_folder_id(DialogId(chat_id), folder_id);
    }
    td_->messages_manager_->on_update_dialog_has_scheduled_server_messages(
        DialogId(chat_id), (chat->flags_ & CHAT_FULL_FLAG_HAS_SCHEDULED_MESSAGES) != 0);
    {
      InputGroupCallId input_group_call_id;
      if (chat->call_ != nullptr) {
        input_group_call_id = InputGroupCallId(chat->call_);
      }
      td_->messages_manager_->on_update_dialog_group_call_id(DialogId(chat_id), input_group_call_id);
    }
    {
      DialogId default_join_group_call_as_dialog_id;
      if (chat->groupcall_default_join_as_ != nullptr) {
        default_join_group_call_as_dialog_id = DialogId(chat->groupcall_default_join_as_);
      }
      // use send closure later to not create synchronously default_join_group_call_as_dialog_id
      send_closure_later(G()->messages_manager(),
                         &MessagesManager::on_update_dialog_default_join_group_call_as_dialog_id, DialogId(chat_id),
                         default_join_group_call_as_dialog_id, false);
    }
    {
      MessageTtl message_ttl;
      if ((chat->flags_ & CHAT_FULL_FLAG_HAS_MESSAGE_TTL) != 0) {
        message_ttl = MessageTtl(chat->ttl_period_);
      }
      td_->messages_manager_->on_update_dialog_message_ttl(DialogId(chat_id), message_ttl);
    }

    ChatFull *chat_full = add_chat_full(chat_id);
    on_update_chat_full_invite_link(chat_full, std::move(chat->exported_invite_));
    on_update_chat_full_photo(chat_full, chat_id,
                              get_photo(td_->file_manager_.get(), std::move(chat->chat_photo_), DialogId(chat_id)));
    if (chat_full->description != chat->about_) {
      chat_full->description = std::move(chat->about_);
      chat_full->is_changed = true;
      td_->group_call_manager_->on_update_dialog_about(DialogId(chat_id), chat_full->description, true);
    }
    if (chat_full->can_set_username != chat->can_set_username_) {
      chat_full->can_set_username = chat->can_set_username_;
      chat_full->is_changed = true;
    }

    on_get_chat_participants(std::move(chat->participants_), false);
    td_->messages_manager_->on_update_dialog_notify_settings(DialogId(chat_id), std::move(chat->notify_settings_),
                                                             "on_get_chat_full");

    td_->messages_manager_->on_update_dialog_theme_name(DialogId(chat_id), std::move(chat->theme_emoticon_));

    td_->messages_manager_->on_update_dialog_pending_join_requests(DialogId(chat_id), chat->requests_pending_,
                                                                   std::move(chat->recent_requesters_));

    auto bot_commands = get_bot_commands(std::move(chat->bot_info_), &chat_full->participants);
    if (chat_full->bot_commands != bot_commands) {
      chat_full->bot_commands = std::move(bot_commands);
      chat_full->is_changed = true;
    }

    chat_full->is_update_chat_full_sent = true;
    update_chat_full(chat_full, chat_id, "on_get_chat_full");
  } else {
    CHECK(chat_full_ptr->get_id() == telegram_api::channelFull::ID);
    auto channel = move_tl_object_as<telegram_api::channelFull>(chat_full_ptr);
    ChannelId channel_id(channel->id_);
    if (!channel_id.is_valid()) {
      LOG(ERROR) << "Receive invalid " << channel_id;
      return promise.set_value(Unit());
    }

    invalidated_channels_full_.erase(channel_id);

    if (!G()->close_flag()) {
      auto channel_full = get_channel_full(channel_id, true, "on_get_channel_full");
      if (channel_full != nullptr) {
        if (channel_full->repair_request_version != 0 &&
            channel_full->repair_request_version < channel_full->speculative_version) {
          LOG(INFO) << "Receive ChannelFull with request version " << channel_full->repair_request_version
                    << ", but current speculative version is " << channel_full->speculative_version;

          channel_full->repair_request_version = channel_full->speculative_version;

          auto input_channel = get_input_channel(channel_id);
          CHECK(input_channel != nullptr);
          td_->create_handler<GetFullChannelQuery>(std::move(promise))->send(channel_id, std::move(input_channel));
          return;
        }
        channel_full->repair_request_version = 0;
      }
    }

    td_->messages_manager_->on_update_dialog_notify_settings(DialogId(channel_id), std::move(channel->notify_settings_),
                                                             "on_get_channel_full");

    td_->messages_manager_->on_update_dialog_theme_name(DialogId(channel_id), std::move(channel->theme_emoticon_));

    td_->messages_manager_->on_update_dialog_pending_join_requests(DialogId(channel_id), channel->requests_pending_,
                                                                   std::move(channel->recent_requesters_));

    {
      MessageTtl message_ttl;
      if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_MESSAGE_TTL) != 0) {
        message_ttl = MessageTtl(channel->ttl_period_);
      }
      td_->messages_manager_->on_update_dialog_message_ttl(DialogId(channel_id), message_ttl);
    }

    auto c = get_channel(channel_id);
    if (c == nullptr) {
      LOG(ERROR) << channel_id << " not found";
      return promise.set_value(Unit());
    }

    ChannelFull *channel_full = add_channel_full(channel_id);

    bool have_participant_count = (channel->flags_ & CHANNEL_FULL_FLAG_HAS_PARTICIPANT_COUNT) != 0;
    auto participant_count = have_participant_count ? channel->participants_count_ : channel_full->participant_count;
    auto administrator_count = 0;
    if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_ADMINISTRATOR_COUNT) != 0) {
      administrator_count = channel->admins_count_;
    } else if (c->is_megagroup || c->status.is_administrator()) {
      // in megagroups and administered channels don't drop known number of administrators
      administrator_count = channel_full->administrator_count;
    }
    if (participant_count < administrator_count) {
      participant_count = administrator_count;
    }
    auto restricted_count = (channel->flags_ & CHANNEL_FULL_FLAG_HAS_BANNED_COUNT) != 0 ? channel->banned_count_ : 0;
    auto banned_count = (channel->flags_ & CHANNEL_FULL_FLAG_HAS_BANNED_COUNT) != 0 ? channel->kicked_count_ : 0;
    auto can_get_participants = (channel->flags_ & CHANNEL_FULL_FLAG_CAN_GET_PARTICIPANTS) != 0;
    auto can_set_username = (channel->flags_ & CHANNEL_FULL_FLAG_CAN_SET_USERNAME) != 0;
    auto can_set_sticker_set = (channel->flags_ & CHANNEL_FULL_FLAG_CAN_SET_STICKER_SET) != 0;
    auto can_set_location = (channel->flags_ & CHANNEL_FULL_FLAG_CAN_SET_LOCATION) != 0;
    auto is_all_history_available = (channel->flags_ & CHANNEL_FULL_FLAG_IS_ALL_HISTORY_HIDDEN) == 0;
    auto can_view_statistics = (channel->flags_ & CHANNEL_FULL_FLAG_CAN_VIEW_STATISTICS) != 0;
    StickerSetId sticker_set_id;
    if (channel->stickerset_ != nullptr) {
      sticker_set_id =
          td_->stickers_manager_->on_get_sticker_set(std::move(channel->stickerset_), true, "on_get_channel_full");
    }
    DcId stats_dc_id;
    if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_STATISTICS_DC_ID) != 0) {
      stats_dc_id = DcId::create(channel->stats_dc_);
    }
    if (!stats_dc_id.is_exact() && can_view_statistics) {
      LOG(ERROR) << "Receive can_view_statistics == true, but invalid statistics DC ID in " << channel_id;
      can_view_statistics = false;
    }

    channel_full->repair_request_version = 0;
    channel_full->expires_at = Time::now() + CHANNEL_FULL_EXPIRE_TIME;
    if (channel_full->participant_count != participant_count ||
        channel_full->administrator_count != administrator_count ||
        channel_full->restricted_count != restricted_count || channel_full->banned_count != banned_count ||
        channel_full->can_get_participants != can_get_participants ||
        channel_full->can_set_username != can_set_username ||
        channel_full->can_set_sticker_set != can_set_sticker_set ||
        channel_full->can_set_location != can_set_location ||
        channel_full->can_view_statistics != can_view_statistics || channel_full->stats_dc_id != stats_dc_id ||
        channel_full->sticker_set_id != sticker_set_id ||
        channel_full->is_all_history_available != is_all_history_available) {
      channel_full->participant_count = participant_count;
      channel_full->administrator_count = administrator_count;
      channel_full->restricted_count = restricted_count;
      channel_full->banned_count = banned_count;
      channel_full->can_get_participants = can_get_participants;
      channel_full->can_set_username = can_set_username;
      channel_full->can_set_sticker_set = can_set_sticker_set;
      channel_full->can_set_location = can_set_location;
      channel_full->can_view_statistics = can_view_statistics;
      channel_full->stats_dc_id = stats_dc_id;
      channel_full->is_all_history_available = is_all_history_available;
      channel_full->sticker_set_id = sticker_set_id;

      channel_full->is_changed = true;
    }
    if (channel_full->description != channel->about_) {
      channel_full->description = std::move(channel->about_);
      channel_full->is_changed = true;
      td_->group_call_manager_->on_update_dialog_about(DialogId(channel_id), channel_full->description, true);
    }

    if (have_participant_count && c->participant_count != participant_count) {
      c->participant_count = participant_count;
      c->is_changed = true;
      update_channel(c, channel_id);
    }
    if (!channel_full->is_can_view_statistics_inited) {
      channel_full->is_can_view_statistics_inited = true;
      channel_full->need_save_to_database = true;
    }

    on_update_channel_full_photo(
        channel_full, channel_id,
        get_photo(td_->file_manager_.get(), std::move(channel->chat_photo_), DialogId(channel_id)));

    td_->messages_manager_->on_read_channel_outbox(channel_id,
                                                   MessageId(ServerMessageId(channel->read_outbox_max_id_)));
    if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_AVAILABLE_MIN_MESSAGE_ID) != 0) {
      td_->messages_manager_->on_update_channel_max_unavailable_message_id(
          channel_id, MessageId(ServerMessageId(channel->available_min_id_)));
    }
    td_->messages_manager_->on_read_channel_inbox(channel_id, MessageId(ServerMessageId(channel->read_inbox_max_id_)),
                                                  channel->unread_count_, channel->pts_, "ChannelFull");

    on_update_channel_full_invite_link(channel_full, std::move(channel->exported_invite_));

    {
      auto is_blocked = (channel->flags_ & CHANNEL_FULL_FLAG_IS_BLOCKED) != 0;
      td_->messages_manager_->on_update_dialog_is_blocked(DialogId(channel_id), is_blocked);
    }
    {
      MessageId pinned_message_id;
      if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_PINNED_MESSAGE) != 0) {
        pinned_message_id = MessageId(ServerMessageId(channel->pinned_msg_id_));
      }
      td_->messages_manager_->on_update_dialog_last_pinned_message_id(DialogId(channel_id), pinned_message_id);
    }
    {
      FolderId folder_id;
      if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_FOLDER_ID) != 0) {
        folder_id = FolderId(channel->folder_id_);
      }
      td_->messages_manager_->on_update_dialog_folder_id(DialogId(channel_id), folder_id);
    }
    td_->messages_manager_->on_update_dialog_has_scheduled_server_messages(
        DialogId(channel_id), (channel->flags_ & CHANNEL_FULL_FLAG_HAS_SCHEDULED_MESSAGES) != 0);
    {
      InputGroupCallId input_group_call_id;
      if (channel->call_ != nullptr) {
        input_group_call_id = InputGroupCallId(channel->call_);
      }
      td_->messages_manager_->on_update_dialog_group_call_id(DialogId(channel_id), input_group_call_id);
    }
    {
      DialogId default_join_group_call_as_dialog_id;
      if (channel->groupcall_default_join_as_ != nullptr) {
        default_join_group_call_as_dialog_id = DialogId(channel->groupcall_default_join_as_);
      }
      // use send closure later to not create synchronously default_join_group_call_as_dialog_id
      send_closure_later(G()->messages_manager(),
                         &MessagesManager::on_update_dialog_default_join_group_call_as_dialog_id, DialogId(channel_id),
                         default_join_group_call_as_dialog_id, false);
    }
    {
      DialogId default_send_message_as_dialog_id;
      if (channel->default_send_as_ != nullptr) {
        default_send_message_as_dialog_id = DialogId(channel->default_send_as_);
      }
      // use send closure later to not create synchronously default_send_message_as_dialog_id
      send_closure_later(G()->messages_manager(), &MessagesManager::on_update_dialog_default_send_message_as_dialog_id,
                         DialogId(channel_id), default_send_message_as_dialog_id, false);
    }

    if (participant_count >= 190) {
      int32 online_member_count = 0;
      if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_ONLINE_MEMBER_COUNT) != 0) {
        online_member_count = channel->online_count_;
      }
      td_->messages_manager_->on_update_dialog_online_member_count(DialogId(channel_id), online_member_count, true);
    }

    vector<UserId> bot_user_ids;
    for (const auto &bot_info : channel->bot_info_) {
      UserId user_id(bot_info->user_id_);
      if (!is_user_bot(user_id)) {
        continue;
      }

      bot_user_ids.push_back(user_id);
    }
    on_update_channel_full_bot_user_ids(channel_full, channel_id, std::move(bot_user_ids));

    auto bot_commands = get_bot_commands(std::move(channel->bot_info_), nullptr);
    if (channel_full->bot_commands != bot_commands) {
      channel_full->bot_commands = std::move(bot_commands);
      channel_full->is_changed = true;
    }

    ChannelId linked_channel_id;
    if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_LINKED_CHANNEL_ID) != 0) {
      linked_channel_id = ChannelId(channel->linked_chat_id_);
      auto linked_channel = get_channel_force(linked_channel_id);
      if (linked_channel == nullptr || c->is_megagroup == linked_channel->is_megagroup ||
          channel_id == linked_channel_id) {
        LOG(ERROR) << "Failed to add a link between " << channel_id << " and " << linked_channel_id;
        linked_channel_id = ChannelId();
      }
    }
    on_update_channel_full_linked_channel_id(channel_full, channel_id, linked_channel_id);

    on_update_channel_full_location(channel_full, channel_id, DialogLocation(std::move(channel->location_)));

    if (c->is_megagroup) {
      int32 slow_mode_delay = 0;
      int32 slow_mode_next_send_date = 0;
      if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_SLOW_MODE_DELAY) != 0) {
        slow_mode_delay = channel->slowmode_seconds_;
      }
      if ((channel->flags_ & CHANNEL_FULL_FLAG_HAS_SLOW_MODE_NEXT_SEND_DATE) != 0) {
        slow_mode_next_send_date = channel->slowmode_next_send_date_;
      }
      on_update_channel_full_slow_mode_delay(channel_full, channel_id, slow_mode_delay, slow_mode_next_send_date);
    }

    ChatId migrated_from_chat_id;
    MessageId migrated_from_max_message_id;

    if ((channel->flags_ & CHANNEL_FULL_FLAG_MIGRATED_FROM) != 0) {
      migrated_from_chat_id = ChatId(channel->migrated_from_chat_id_);
      migrated_from_max_message_id = MessageId(ServerMessageId(channel->migrated_from_max_id_));
    }

    if (channel_full->migrated_from_chat_id != migrated_from_chat_id ||
        channel_full->migrated_from_max_message_id != migrated_from_max_message_id) {
      channel_full->migrated_from_chat_id = migrated_from_chat_id;
      channel_full->migrated_from_max_message_id = migrated_from_max_message_id;
      channel_full->is_changed = true;
    }

    channel_full->is_update_channel_full_sent = true;
    update_channel_full(channel_full, channel_id, "on_get_channel_full");

    if (linked_channel_id.is_valid()) {
      auto linked_channel_full = get_channel_full_force(linked_channel_id, true, "on_get_chat_full");
      on_update_channel_full_linked_channel_id(linked_channel_full, linked_channel_id, channel_id);
      if (linked_channel_full != nullptr) {
        update_channel_full(linked_channel_full, linked_channel_id, "on_get_channel_full 2");
      }
    }

    if (dismiss_suggested_action_queries_.count(DialogId(channel_id)) == 0) {
      auto it = dialog_suggested_actions_.find(DialogId(channel_id));
      if (it != dialog_suggested_actions_.end() || !channel->pending_suggestions_.empty()) {
        vector<SuggestedAction> suggested_actions;
        for (auto &action_str : channel->pending_suggestions_) {
          SuggestedAction suggested_action(action_str, DialogId(channel_id));
          if (!suggested_action.is_empty()) {
            if (suggested_action == SuggestedAction{SuggestedAction::Type::ConvertToGigagroup, DialogId(channel_id)} &&
                (c->is_gigagroup || c->default_permissions != RestrictedRights(false, false, false, false, false, false,
                                                                               false, false, false, false, false))) {
              LOG(INFO) << "Skip ConvertToGigagroup suggested action";
            } else {
              suggested_actions.push_back(suggested_action);
            }
          }
        }
        if (it == dialog_suggested_actions_.end()) {
          it = dialog_suggested_actions_.emplace(DialogId(channel_id), vector<SuggestedAction>()).first;
        }
        update_suggested_actions(it->second, std::move(suggested_actions));
        if (it->second.empty()) {
          dialog_suggested_actions_.erase(it);
        }
      }
    }
  }
  promise.set_value(Unit());
}

void ContactsManager::send_get_channel_full_query(ChannelFull *channel_full, ChannelId channel_id,
                                                  Promise<Unit> &&promise, const char *source) {
  auto input_channel = get_input_channel(channel_id);
  if (input_channel == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }

  if (!have_input_peer_channel(channel_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  if (channel_full != nullptr) {
    if (!promise) {
      if (channel_full->repair_request_version != 0) {
        LOG(INFO) << "Skip get full " << channel_id << " request from " << source;
        return;
      }
      channel_full->repair_request_version = channel_full->speculative_version;
    } else {
      channel_full->repair_request_version = std::numeric_limits<uint32>::max();
    }
  }

  LOG(INFO) << "Get full " << channel_id << " from " << source;
  auto send_query = PromiseCreator::lambda(
      [td = td_, channel_id, input_channel = std::move(input_channel)](Result<Promise<Unit>> &&promise) mutable {
        if (promise.is_ok() && !G()->close_flag()) {
          td->create_handler<GetFullChannelQuery>(promise.move_as_ok())->send(channel_id, std::move(input_channel));
        }
      });
  get_chat_full_queries_.add_query(DialogId(channel_id).get(), std::move(send_query), std::move(promise));
}

void ContactsManager::on_update_user_online(UserId user_id, tl_object_ptr<telegram_api::UserStatus> &&status) {
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  User *u = get_user_force(user_id);
  if (u != nullptr) {
    if (u->is_bot) {
      LOG(ERROR) << "Receive updateUserStatus about bot " << user_id;
      return;
    }
    on_update_user_online(u, user_id, std::move(status));
    update_user(u, user_id);

    if (user_id == get_my_id() &&
        was_online_remote_ != u->was_online) {  // only update was_online_remote_ from updateUserStatus
      was_online_remote_ = u->was_online;
      VLOG(notifications) << "Set was_online_remote to " << was_online_remote_;
      G()->td_db()->get_binlog_pmc()->set("my_was_online_remote", to_string(was_online_remote_));
    }
  } else {
    LOG(INFO) << "Ignore update user online about unknown " << user_id;
  }
}

void ContactsManager::on_channel_participant_cache_timeout(ChannelId channel_id) {
  if (G()->close_flag()) {
    return;
  }

  auto channel_participants_it = channel_participants_.find(channel_id);
  if (channel_participants_it == channel_participants_.end()) {
    return;
  }

  auto &participants = channel_participants_it->second.participants_;
  auto min_access_date = G()->unix_time() - CHANNEL_PARTICIPANT_CACHE_TIME;
  for (auto it = participants.begin(); it != participants.end();) {
    if (it->second.last_access_date_ < min_access_date) {
      it = participants.erase(it);
    } else {
      ++it;
    }
  }

  if (participants.empty()) {
    channel_participants_.erase(channel_participants_it);
  } else {
    channel_participant_cache_timeout_.set_timeout_in(channel_id.get(), CHANNEL_PARTICIPANT_CACHE_TIME);
  }
}

bool ContactsManager::is_chat_full_outdated(const ChatFull *chat_full, const Chat *c, ChatId chat_id) {
  CHECK(c != nullptr);
  CHECK(chat_full != nullptr);
  if (!c->is_active && chat_full->version == -1) {
    return false;
  }

  if (chat_full->version != c->version) {
    LOG(INFO) << "Have outdated ChatFull " << chat_id << " with current version " << chat_full->version
              << " and chat version " << c->version;
    return true;
  }

  if (c->is_active && c->status.can_manage_invite_links() && !chat_full->invite_link.is_valid()) {
    LOG(INFO) << "Have outdated invite link in " << chat_id;
    return true;
  }

  LOG(DEBUG) << "Full " << chat_id << " is up-to-date with version " << chat_full->version;
  return false;
}

bool ContactsManager::get_secret_chat(SecretChatId secret_chat_id, bool force, Promise<Unit> &&promise) {
  if (!secret_chat_id.is_valid()) {
    promise.set_error(Status::Error(400, "Invalid secret chat identifier"));
    return false;
  }

  if (!have_secret_chat(secret_chat_id)) {
    if (!force && G()->parameters().use_chat_info_db) {
      send_closure_later(actor_id(this), &ContactsManager::load_secret_chat_from_database, nullptr, secret_chat_id,
                         std::move(promise));
      return false;
    }

    promise.set_error(Status::Error(400, "Secret chat not found"));
    return false;
  }

  promise.set_value(Unit());
  return true;
}

void ContactsManager::save_user_to_database_impl(User *u, UserId user_id, string value) {
  CHECK(u != nullptr);
  CHECK(load_user_from_database_queries_.count(user_id) == 0);
  CHECK(!u->is_being_saved);
  u->is_being_saved = true;
  u->is_saved = true;
  u->is_status_saved = true;
  LOG(INFO) << "Trying to save to database " << user_id;
  G()->td_db()->get_sqlite_pmc()->set(
      get_user_database_key(user_id), std::move(value), PromiseCreator::lambda([user_id](Result<> result) {
        send_closure(G()->contacts_manager(), &ContactsManager::on_save_user_to_database, user_id, result.is_ok());
      }));
}

void ContactsManager::on_get_chats(vector<tl_object_ptr<telegram_api::Chat>> &&chats, const char *source) {
  for (auto &chat : chats) {
    auto constuctor_id = chat->get_id();
    if (constuctor_id == telegram_api::channel::ID || constuctor_id == telegram_api::channelForbidden::ID) {
      // apply info about megagroups before corresponding chats
      on_get_chat(std::move(chat), source);
      chat = nullptr;
    }
  }
  for (auto &chat : chats) {
    if (chat != nullptr) {
      on_get_chat(std::move(chat), source);
      chat = nullptr;
    }
  }
}

void ContactsManager::on_update_chat_migrated_to_channel_id(Chat *c, ChatId chat_id, ChannelId migrated_to_channel_id) {
  if (c->migrated_to_channel_id != migrated_to_channel_id && migrated_to_channel_id.is_valid()) {
    LOG_IF(ERROR, c->migrated_to_channel_id.is_valid())
        << "Upgraded supergroup ID for " << chat_id << " has changed from " << c->migrated_to_channel_id << " to "
        << migrated_to_channel_id;
    c->migrated_to_channel_id = migrated_to_channel_id;
    c->is_changed = true;
  }
}

void ContactsManager::finish_get_chat_participant(ChatId chat_id, UserId user_id,
                                                  Promise<DialogParticipant> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  const auto *participant = get_chat_participant(chat_id, user_id);
  if (participant == nullptr) {
    return promise.set_value(DialogParticipant::left(DialogId(user_id)));
  }

  promise.set_value(DialogParticipant(*participant));
}

void ContactsManager::load_secret_chat_from_database(SecretChat *c, SecretChatId secret_chat_id,
                                                     Promise<Unit> promise) {
  if (loaded_from_database_secret_chats_.count(secret_chat_id)) {
    promise.set_value(Unit());
    return;
  }

  CHECK(c == nullptr || !c->is_being_saved);
  load_secret_chat_from_database_impl(secret_chat_id, std::move(promise));
}

RestrictedRights ContactsManager::get_secret_chat_default_permissions(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return RestrictedRights(false, false, false, false, false, false, false, false, false, false, false);
  }
  return RestrictedRights(true, true, true, true, true, true, true, true, false, false, false);
}

void ContactsManager::remove_linked_channel_id(ChannelId channel_id) {
  if (!channel_id.is_valid()) {
    return;
  }

  auto it = linked_channel_ids_.find(channel_id);
  if (it != linked_channel_ids_.end()) {
    auto linked_channel_id = it->second;
    linked_channel_ids_.erase(it);
    linked_channel_ids_.erase(linked_channel_id);
  }
}

ContactsManager::ChannelFull *ContactsManager::add_channel_full(ChannelId channel_id) {
  CHECK(channel_id.is_valid());
  auto &channel_full_ptr = channels_full_[channel_id];
  if (channel_full_ptr == nullptr) {
    channel_full_ptr = make_unique<ChannelFull>();
  }
  return channel_full_ptr.get();
}

tl_object_ptr<td_api::messageStatistics> ContactsManager::convert_message_stats(
    tl_object_ptr<telegram_api::stats_messageStats> obj) {
  return make_tl_object<td_api::messageStatistics>(convert_stats_graph(std::move(obj->views_graph_)));
}

const ContactsManager::UserFull *ContactsManager::get_user_full(UserId user_id) const {
  auto p = users_full_.find(user_id);
  if (p == users_full_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

ContactsManager::ChatFull *ContactsManager::get_chat_full(ChatId chat_id) {
  auto p = chats_full_.find(chat_id);
  if (p == chats_full_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

ContactsManager::User *ContactsManager::get_user(UserId user_id) {
  auto p = users_.find(user_id);
  if (p == users_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

DialogParticipantStatus ContactsManager::get_chat_status(const Chat *c) {
  if (!c->is_active) {
    return DialogParticipantStatus::Banned(0);
  }
  return c->status;
}

UserId ContactsManager::get_anonymous_bot_user_id() {
  return UserId(static_cast<int64>(G()->is_test_dc() ? 552888 : 1087968824));
}

bool ContactsManager::have_min_channel(ChannelId channel_id) const {
  return min_channels_.count(channel_id) > 0;
}
}  // namespace td
