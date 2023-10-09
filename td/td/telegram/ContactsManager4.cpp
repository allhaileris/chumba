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


#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/Global.h"


#include "td/telegram/InputGroupCallId.h"
#include "td/telegram/LinkManager.h"
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



#include <tuple>


namespace td {
}  // namespace td
namespace td {

class GetChannelsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit GetChannelsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(tl_object_ptr<telegram_api::InputChannel> &&input_channel) {
    CHECK(input_channel != nullptr);
    if (input_channel->get_id() == telegram_api::inputChannel::ID) {
      channel_id_ = ChannelId(static_cast<const telegram_api::inputChannel *>(input_channel.get())->channel_id_);
    }

    vector<tl_object_ptr<telegram_api::InputChannel>> input_channels;
    input_channels.push_back(std::move(input_channel));
    send_query(G()->net_query_creator().create(telegram_api::channels_getChannels(std::move(input_channels))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getChannels>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    //    LOG(INFO) << "Receive result for GetChannelsQuery: " << to_string(result_ptr.ok());
    auto chats_ptr = result_ptr.move_as_ok();
    int32 constructor_id = chats_ptr->get_id();
    switch (constructor_id) {
      case telegram_api::messages_chats::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chats>(chats_ptr);
        td_->contacts_manager_->on_get_chats(std::move(chats->chats_), "GetChannelsQuery");
        break;
      }
      case telegram_api::messages_chatsSlice::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chatsSlice>(chats_ptr);
        LOG(ERROR) << "Receive chatsSlice in result of GetChannelsQuery";
        td_->contacts_manager_->on_get_chats(std::move(chats->chats_), "GetChannelsQuery");
        break;
      }
      default:
        UNREACHABLE();
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetChannelsQuery");
    promise_.set_error(std::move(status));
  }
};

bool ContactsManager::get_channel(ChannelId channel_id, int left_tries, Promise<Unit> &&promise) {
  if (!channel_id.is_valid()) {
    promise.set_error(Status::Error(400, "Invalid supergroup identifier"));
    return false;
  }

  if (!have_channel(channel_id)) {
    if (left_tries > 2 && G()->parameters().use_chat_info_db) {
      send_closure_later(actor_id(this), &ContactsManager::load_channel_from_database, nullptr, channel_id,
                         std::move(promise));
      return false;
    }

    if (left_tries > 1 && td_->auth_manager_->is_bot()) {
      td_->create_handler<GetChannelsQuery>(std::move(promise))->send(get_input_channel(channel_id));
      return false;
    }

    promise.set_error(Status::Error(400, "Supergroup not found"));
    return false;
  }

  promise.set_value(Unit());
  return true;
}

void ContactsManager::reload_channel(ChannelId channel_id, Promise<Unit> &&promise) {
  if (!channel_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid supergroup identifier"));
  }

  have_channel_force(channel_id);
  auto input_channel = get_input_channel(channel_id);
  if (input_channel == nullptr) {
    input_channel = make_tl_object<telegram_api::inputChannel>(channel_id.get(), 0);
  }

  // there is no much reason to combine different requests into one request
  // requests with 0 access_hash must not be merged
  td_->create_handler<GetChannelsQuery>(std::move(promise))->send(std::move(input_channel));
}

void ContactsManager::on_chat_update(telegram_api::chat &chat, const char *source) {
  auto debug_str = PSTRING() << " from " << source << " in " << oneline(to_string(chat));
  ChatId chat_id(chat.id_);
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id << debug_str;
    return;
  }

  DialogParticipantStatus status = [&] {
    bool is_creator = 0 != (chat.flags_ & CHAT_FLAG_USER_IS_CREATOR);
    bool has_left = 0 != (chat.flags_ & CHAT_FLAG_USER_HAS_LEFT);
    bool was_kicked = 0 != (chat.flags_ & CHAT_FLAG_USER_WAS_KICKED);
    if (was_kicked) {
      LOG_IF(ERROR, has_left) << "Kicked and left" << debug_str;  // only one of the flags can be set
      has_left = true;
    }

    if (is_creator) {
      return DialogParticipantStatus::Creator(!has_left, false, string());
    } else if (chat.admin_rights_ != nullptr) {
      return get_dialog_participant_status(false, std::move(chat.admin_rights_), string());
    } else if (was_kicked) {
      return DialogParticipantStatus::Banned(0);
    } else if (has_left) {
      return DialogParticipantStatus::Left();
    } else {
      return DialogParticipantStatus::Member();
    }
  }();

  bool is_active = 0 == (chat.flags_ & CHAT_FLAG_IS_DEACTIVATED);

  ChannelId migrated_to_channel_id;
  if (chat.flags_ & CHAT_FLAG_WAS_MIGRATED) {
    switch (chat.migrated_to_->get_id()) {
      case telegram_api::inputChannelEmpty::ID: {
        LOG(ERROR) << "Receive empty upgraded to supergroup for " << chat_id << debug_str;
        break;
      }
      case telegram_api::inputChannel::ID: {
        auto input_channel = move_tl_object_as<telegram_api::inputChannel>(chat.migrated_to_);
        migrated_to_channel_id = ChannelId(input_channel->channel_id_);
        if (!have_channel_force(migrated_to_channel_id)) {
          if (!migrated_to_channel_id.is_valid()) {
            LOG(ERROR) << "Receive invalid " << migrated_to_channel_id << debug_str;
          } else {
            // temporarily create the channel
            Channel *c = add_channel(migrated_to_channel_id, "on_chat_update");
            c->access_hash = input_channel->access_hash_;
            c->title = chat.title_;
            c->status = DialogParticipantStatus::Left();
            c->is_megagroup = true;

            // we definitely need to call update_channel, because client should know about every added channel
            update_channel(c, migrated_to_channel_id);

            // get info about the channel
            td_->create_handler<GetChannelsQuery>(Promise<>())->send(std::move(input_channel));
          }
        }
        break;
      }
      default:
        UNREACHABLE();
    }
  }

  Chat *c = get_chat_force(chat_id);  // to load versions
  if (c == nullptr) {
    c = add_chat(chat_id);
  }
  on_update_chat_title(c, chat_id, std::move(chat.title_));
  if (!status.is_left()) {
    on_update_chat_participant_count(c, chat_id, chat.participants_count_, chat.version_, debug_str);
  }
  if (c->date != chat.date_) {
    LOG_IF(ERROR, c->date != 0) << "Chat creation date has changed from " << c->date << " to " << chat.date_
                                << debug_str;
    c->date = chat.date_;
    c->need_save_to_database = true;
  }
  on_update_chat_status(c, chat_id, std::move(status));
  on_update_chat_default_permissions(c, chat_id, get_restricted_rights(std::move(chat.default_banned_rights_)),
                                     chat.version_);
  on_update_chat_photo(c, chat_id, std::move(chat.photo_));
  on_update_chat_active(c, chat_id, is_active);
  on_update_chat_noforwards(c, chat_id, chat.noforwards_);
  on_update_chat_migrated_to_channel_id(c, chat_id, migrated_to_channel_id);
  LOG_IF(INFO, !is_active && !migrated_to_channel_id.is_valid()) << chat_id << " is deactivated" << debug_str;
  if (c->cache_version != Chat::CACHE_VERSION) {
    c->cache_version = Chat::CACHE_VERSION;
    c->need_save_to_database = true;
  }
  c->is_received_from_server = true;
  update_chat(c, chat_id);

  bool has_active_group_call = (chat.flags_ & CHAT_FLAG_HAS_ACTIVE_GROUP_CALL) != 0;
  bool is_group_call_empty = (chat.flags_ & CHAT_FLAG_IS_GROUP_CALL_NON_EMPTY) == 0;
  td_->messages_manager_->on_update_dialog_group_call(DialogId(chat_id), has_active_group_call, is_group_call_empty,
                                                      "receive chat");
}

class GetMegagroupStatsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::ChatStatistics>> promise_;
  ChannelId channel_id_;

 public:
  explicit GetMegagroupStatsQuery(Promise<td_api::object_ptr<td_api::ChatStatistics>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, bool is_dark, DcId dc_id) {
    channel_id_ = channel_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    int32 flags = 0;
    if (is_dark) {
      flags |= telegram_api::stats_getMegagroupStats::DARK_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::stats_getMegagroupStats(flags, false /*ignored*/, std::move(input_channel)), dc_id));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stats_getMegagroupStats>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(td_->contacts_manager_->convert_megagroup_stats(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetMegagroupStatsQuery");
    promise_.set_error(std::move(status));
  }
};

class GetBroadcastStatsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::ChatStatistics>> promise_;
  ChannelId channel_id_;

 public:
  explicit GetBroadcastStatsQuery(Promise<td_api::object_ptr<td_api::ChatStatistics>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, bool is_dark, DcId dc_id) {
    channel_id_ = channel_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    int32 flags = 0;
    if (is_dark) {
      flags |= telegram_api::stats_getBroadcastStats::DARK_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::stats_getBroadcastStats(flags, false /*ignored*/, std::move(input_channel)), dc_id));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stats_getBroadcastStats>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = ContactsManager::convert_broadcast_stats(result_ptr.move_as_ok());
    for (auto &info : result->recent_message_interactions_) {
      td_->messages_manager_->on_update_message_interaction_info({DialogId(channel_id_), MessageId(info->message_id_)},
                                                                 info->view_count_, info->forward_count_, false,
                                                                 nullptr);
    }
    promise_.set_value(std::move(result));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetBroadcastStatsQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_get_channel_stats_query(DcId dc_id, ChannelId channel_id, bool is_dark,
                                                   Promise<td_api::object_ptr<td_api::ChatStatistics>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  const Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);
  if (c->is_megagroup) {
    td_->create_handler<GetMegagroupStatsQuery>(std::move(promise))->send(channel_id, is_dark, dc_id);
  } else {
    td_->create_handler<GetBroadcastStatsQuery>(std::move(promise))->send(channel_id, is_dark, dc_id);
  }
}

void ContactsManager::update_channel_full(ChannelFull *channel_full, ChannelId channel_id, const char *source,
                                          bool from_database) {
  CHECK(channel_full != nullptr);
  unavailable_channel_fulls_.erase(channel_id);  // don't needed anymore

  CHECK(channel_full->participant_count >= channel_full->administrator_count);

  if (channel_full->is_slow_mode_next_send_date_changed) {
    auto now = G()->server_time();
    if (channel_full->slow_mode_next_send_date > now + 3601) {
      channel_full->slow_mode_next_send_date = static_cast<int32>(now) + 3601;
    }
    if (channel_full->slow_mode_next_send_date <= now) {
      channel_full->slow_mode_next_send_date = 0;
    }
    if (channel_full->slow_mode_next_send_date == 0) {
      slow_mode_delay_timeout_.cancel_timeout(channel_id.get());
    } else {
      slow_mode_delay_timeout_.set_timeout_in(channel_id.get(), channel_full->slow_mode_next_send_date - now + 0.002);
    }
    channel_full->is_slow_mode_next_send_date_changed = false;
  }

  if (channel_full->need_save_to_database) {
    channel_full->is_changed |= td::remove_if(
        channel_full->bot_commands, [bot_user_ids = &channel_full->bot_user_ids](const BotCommands &commands) {
          return !td::contains(*bot_user_ids, commands.get_bot_user_id());
        });
  }

  channel_full->need_send_update |= channel_full->is_changed;
  channel_full->need_save_to_database |= channel_full->is_changed;
  channel_full->is_changed = false;
  if (channel_full->need_send_update || channel_full->need_save_to_database) {
    LOG(INFO) << "Update full " << channel_id << " from " << source;
  }
  if (channel_full->need_send_update) {
    if (channel_full->linked_channel_id.is_valid()) {
      td_->messages_manager_->force_create_dialog(DialogId(channel_full->linked_channel_id), "update_channel_full",
                                                  true);
    }

    {
      Channel *c = get_channel(channel_id);
      CHECK(c == nullptr || c->is_update_supergroup_sent);
    }
    if (!channel_full->is_update_channel_full_sent) {
      LOG(ERROR) << "Send partial updateSupergroupFullInfo for " << channel_id << " from " << source;
      channel_full->is_update_channel_full_sent = true;
    }
    send_closure(
        G()->td(), &Td::send_update,
        make_tl_object<td_api::updateSupergroupFullInfo>(get_supergroup_id_object(channel_id, "update_channel_full"),
                                                         get_supergroup_full_info_object(channel_full, channel_id)));
    channel_full->need_send_update = false;
  }
  if (channel_full->need_save_to_database) {
    if (!from_database) {
      save_channel_full(channel_full, channel_id);
    }
    channel_full->need_save_to_database = false;
  }
}

void ContactsManager::on_update_secret_chat(SecretChatId secret_chat_id, int64 access_hash, UserId user_id,
                                            SecretChatState state, bool is_outbound, int32 ttl, int32 date,
                                            string key_hash, int32 layer, FolderId initial_folder_id) {
  LOG(INFO) << "Update " << secret_chat_id << " with " << user_id << " and access_hash " << access_hash;
  auto *secret_chat = add_secret_chat(secret_chat_id);
  if (access_hash != secret_chat->access_hash) {
    secret_chat->access_hash = access_hash;
    secret_chat->need_save_to_database = true;
  }
  if (user_id.is_valid() && user_id != secret_chat->user_id) {
    if (secret_chat->user_id.is_valid()) {
      LOG(ERROR) << "Secret chat user has changed from " << secret_chat->user_id << " to " << user_id;
      auto &old_secret_chat_ids = secret_chats_with_user_[secret_chat->user_id];
      td::remove(old_secret_chat_ids, secret_chat_id);
    }
    secret_chat->user_id = user_id;
    secret_chats_with_user_[secret_chat->user_id].push_back(secret_chat_id);
    secret_chat->is_changed = true;
  }
  if (state != SecretChatState::Unknown && state != secret_chat->state) {
    secret_chat->state = state;
    secret_chat->is_changed = true;
    secret_chat->is_state_changed = true;
  }
  if (is_outbound != secret_chat->is_outbound) {
    secret_chat->is_outbound = is_outbound;
    secret_chat->is_changed = true;
  }

  if (ttl != -1 && ttl != secret_chat->ttl) {
    secret_chat->ttl = ttl;
    secret_chat->need_save_to_database = true;
    secret_chat->is_ttl_changed = true;
  }
  if (date != 0 && date != secret_chat->date) {
    secret_chat->date = date;
    secret_chat->need_save_to_database = true;
  }
  if (!key_hash.empty() && key_hash != secret_chat->key_hash) {
    secret_chat->key_hash = std::move(key_hash);
    secret_chat->is_changed = true;
  }
  if (layer != 0 && layer != secret_chat->layer) {
    secret_chat->layer = layer;
    secret_chat->is_changed = true;
  }
  if (initial_folder_id != FolderId() && initial_folder_id != secret_chat->initial_folder_id) {
    secret_chat->initial_folder_id = initial_folder_id;
    secret_chat->is_changed = true;
  }

  update_secret_chat(secret_chat, secret_chat_id);
}

class ImportChatInviteQuery final : public Td::ResultHandler {
  Promise<DialogId> promise_;

  string invite_link_;

 public:
  explicit ImportChatInviteQuery(Promise<DialogId> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &invite_link) {
    invite_link_ = invite_link;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_importChatInvite(LinkManager::get_dialog_invite_link_hash(invite_link_))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_importChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ImportChatInviteQuery: " << to_string(ptr);

    auto dialog_ids = UpdatesManager::get_chat_dialog_ids(ptr.get());
    if (dialog_ids.size() != 1u) {
      LOG(ERROR) << "Receive wrong result for ImportChatInviteQuery: " << to_string(ptr);
      return on_error(Status::Error(500, "Internal Server Error: failed to join chat via invite link"));
    }
    auto dialog_id = dialog_ids[0];

    td_->contacts_manager_->invalidate_invite_link_info(invite_link_);
    td_->updates_manager_->on_get_updates(
        std::move(ptr), PromiseCreator::lambda([promise = std::move(promise_), dialog_id](Unit) mutable {
          promise.set_value(std::move(dialog_id));
        }));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->invalidate_invite_link_info(invite_link_);
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::import_dialog_invite_link(const string &invite_link, Promise<DialogId> &&promise) {
  if (!DialogInviteLink::is_valid_invite_link(invite_link)) {
    return promise.set_error(Status::Error(400, "Wrong invite link"));
  }

  td_->create_handler<ImportChatInviteQuery>(std::move(promise))->send(invite_link);
}

class GetFullChatQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChatId chat_id_;

 public:
  explicit GetFullChatQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChatId chat_id) {
    send_query(G()->net_query_creator().create(telegram_api::messages_getFullChat(chat_id.get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getFullChat>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    td_->contacts_manager_->on_get_users(std::move(ptr->users_), "GetFullChatQuery");
    td_->contacts_manager_->on_get_chats(std::move(ptr->chats_), "GetFullChatQuery");
    td_->contacts_manager_->on_get_chat_full(std::move(ptr->full_chat_), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_chat_full_failed(chat_id_);
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_get_chat_full_query(ChatId chat_id, Promise<Unit> &&promise, const char *source) {
  LOG(INFO) << "Get full " << chat_id << " from " << source;
  auto send_query = PromiseCreator::lambda([td = td_, chat_id](Result<Promise<Unit>> &&promise) {
    if (promise.is_ok() && !G()->close_flag()) {
      td->create_handler<GetFullChatQuery>(promise.move_as_ok())->send(chat_id);
    }
  });

  get_chat_full_queries_.add_query(DialogId(chat_id).get(), std::move(send_query), std::move(promise));
}

class UpdateUsernameQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit UpdateUsernameQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &username) {
    send_query(G()->net_query_creator().create(telegram_api::account_updateUsername(username)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_updateUsername>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG(DEBUG) << "Receive result for UpdateUsernameQuery: " << to_string(result_ptr.ok());
    td_->contacts_manager_->on_get_user(result_ptr.move_as_ok(), "UpdateUsernameQuery");
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (status.message() == "USERNAME_NOT_MODIFIED" && !td_->auth_manager_->is_bot()) {
      promise_.set_value(Unit());
      return;
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_username(const string &username, Promise<Unit> &&promise) {
  if (!username.empty() && !is_valid_username(username)) {
    return promise.set_error(Status::Error(400, "Username is invalid"));
  }
  td_->create_handler<UpdateUsernameQuery>(std::move(promise))->send(username);
}

void ContactsManager::do_search_chat_participants(ChatId chat_id, const string &query, int32 limit,
                                                  DialogParticipantsFilter filter,
                                                  Promise<DialogParticipants> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto chat_full = get_chat_full(chat_id);
  if (chat_full == nullptr) {
    return promise.set_error(Status::Error(500, "Can't find basic group full info"));
  }

  vector<DialogId> dialog_ids;
  for (const auto &participant : chat_full->participants) {
    if (filter.is_dialog_participant_suitable(td_, participant)) {
      dialog_ids.push_back(participant.dialog_id_);
    }
  }

  int32 total_count;
  std::tie(total_count, dialog_ids) = search_among_dialogs(dialog_ids, query, limit);
  promise.set_value(DialogParticipants{total_count, transform(dialog_ids, [chat_full](DialogId dialog_id) {
                                         return *ContactsManager::get_chat_full_participant(chat_full, dialog_id);
                                       })});
}

void ContactsManager::search_chat_participants(ChatId chat_id, const string &query, int32 limit,
                                               DialogParticipantsFilter filter, Promise<DialogParticipants> &&promise) {
  if (limit < 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be non-negative"));
  }

  auto load_chat_full_promise = PromiseCreator::lambda([actor_id = actor_id(this), chat_id, query, limit, filter,
                                                        promise = std::move(promise)](Result<Unit> &&result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      send_closure(actor_id, &ContactsManager::do_search_chat_participants, chat_id, query, limit, filter,
                   std::move(promise));
    }
  });
  load_chat_full(chat_id, false, std::move(load_chat_full_promise), "search_chat_participants");
}

void ContactsManager::on_get_created_public_channels(PublicDialogType type,
                                                     vector<tl_object_ptr<telegram_api::Chat>> &&chats) {
  auto index = static_cast<int32>(type);
  auto channel_ids = get_channel_ids(std::move(chats), "on_get_created_public_channels");
  if (created_public_channels_inited_[index] && created_public_channels_[index] == channel_ids) {
    return;
  }
  for (auto channel_id : channel_ids) {
    td_->messages_manager_->force_create_dialog(DialogId(channel_id), "on_get_created_public_channels");
  }
  created_public_channels_[index] = std::move(channel_ids);
  created_public_channels_inited_[index] = true;

  if (type == PublicDialogType::HasUsername) {
    update_created_public_broadcasts();
  }

  save_created_public_channels(type);
}

void ContactsManager::do_invalidate_channel_full(ChannelFull *channel_full, ChannelId channel_id,
                                                 bool need_drop_slow_mode_delay) {
  CHECK(channel_full != nullptr);
  td_->messages_manager_->on_dialog_info_full_invalidated(DialogId(channel_id));
  if (channel_full->expires_at >= Time::now()) {
    channel_full->expires_at = 0.0;
    channel_full->need_save_to_database = true;
  }
  if (need_drop_slow_mode_delay && channel_full->slow_mode_delay != 0) {
    channel_full->slow_mode_delay = 0;
    channel_full->slow_mode_next_send_date = 0;
    channel_full->is_slow_mode_next_send_date_changed = true;
    channel_full->is_changed = true;
  }
}

bool ContactsManager::on_update_chat_full_participants_short(ChatFull *chat_full, ChatId chat_id, int32 version) {
  if (version <= -1) {
    LOG(ERROR) << "Receive wrong version " << version << " for " << chat_id;
    return false;
  }
  if (chat_full->version == -1) {
    // chat members are unknown, nothing to update
    return false;
  }

  if (chat_full->version + 1 == version) {
    chat_full->version = version;
    return true;
  }

  LOG(INFO) << "Number of members in " << chat_id << " with version " << chat_full->version
            << " has changed, but new version is " << version;
  repair_chat_participants(chat_id);
  return false;
}

tl_object_ptr<td_api::supergroup> ContactsManager::get_supergroup_object(ChannelId channel_id, const Channel *c) {
  if (c == nullptr) {
    return nullptr;
  }
  return td_api::make_object<td_api::supergroup>(
      channel_id.get(), c->username, c->date, get_channel_status(c).get_chat_member_status_object(),
      c->participant_count, c->has_linked_channel, c->has_location, c->sign_messages, c->is_slow_mode_enabled,
      !c->is_megagroup, c->is_gigagroup, c->is_verified, get_restriction_reason_description(c->restriction_reasons),
      c->is_scam, c->is_fake);
}

void ContactsManager::on_update_channel_full_bot_user_ids(ChannelFull *channel_full, ChannelId channel_id,
                                                          vector<UserId> &&bot_user_ids) {
  CHECK(channel_full != nullptr);
  if (channel_full->bot_user_ids != bot_user_ids) {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                       bot_user_ids, false);
    channel_full->bot_user_ids = std::move(bot_user_ids);
    channel_full->need_save_to_database = true;
  }
}

void ContactsManager::update_created_public_broadcasts() {
  CHECK(created_public_channels_inited_[0]);
  vector<ChannelId> channel_ids;
  for (auto &channel_id : created_public_channels_[0]) {
    auto c = get_channel(channel_id);
    if (!c->is_megagroup) {
      channel_ids.push_back(channel_id);
    }
  }
  send_closure_later(G()->messages_manager(), &MessagesManager::on_update_created_public_broadcasts,
                     std::move(channel_ids));
}

void ContactsManager::on_update_channel_status(Channel *c, ChannelId channel_id, DialogParticipantStatus &&status) {
  if (c->status != status) {
    LOG(INFO) << "Update " << channel_id << " status from " << c->status << " to " << status;
    if (c->is_update_supergroup_sent) {
      on_channel_status_changed(c, channel_id, c->status, status);
    }
    c->status = status;
    c->is_status_changed = true;
    c->is_changed = true;
  }
}

void ContactsManager::save_chat_full(const ChatFull *chat_full, ChatId chat_id) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }

  LOG(INFO) << "Trying to save to database full " << chat_id;
  CHECK(chat_full != nullptr);
  G()->td_db()->get_sqlite_pmc()->set(get_chat_full_database_key(chat_id), get_chat_full_database_value(chat_full),
                                      Auto());
}

void ContactsManager::on_update_chat_noforwards(Chat *c, ChatId chat_id, bool noforwards) {
  if (c->noforwards != noforwards) {
    LOG(INFO) << "Update " << chat_id << " has_protected_content from " << c->noforwards << " to " << noforwards;
    c->noforwards = noforwards;
    c->is_noforwards_changed = true;
    c->need_save_to_database = true;
  }
}

td_api::object_ptr<td_api::updateSecretChat> ContactsManager::get_update_unknown_secret_chat_object(
    SecretChatId secret_chat_id) {
  return td_api::make_object<td_api::updateSecretChat>(td_api::make_object<td_api::secretChat>(
      secret_chat_id.get(), 0, get_secret_chat_state_object(SecretChatState::Unknown), false, string(), 0));
}

void ContactsManager::add_min_channel(ChannelId channel_id, const MinChannel &min_channel) {
  if (have_channel(channel_id) || have_min_channel(channel_id)) {
    return;
  }
  min_channels_[channel_id] = td::make_unique<MinChannel>(min_channel);
}

void ContactsManager::on_update_phone_number_privacy() {
  // all UserFull.need_phone_number_privacy_exception can be outdated now,
  // so mark all of them as expired
  for (auto &it : users_full_) {
    it.second->expires_at = 0.0;
  }
}

void ContactsManager::on_update_chat_title(Chat *c, ChatId chat_id, string &&title) {
  if (c->title != title) {
    c->title = std::move(title);
    c->is_title_changed = true;
    c->need_save_to_database = true;
  }
}

bool ContactsManager::get_secret_chat_is_outbound(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return false;
  }
  return c->is_outbound;
}

tl_object_ptr<td_api::basicGroupFullInfo> ContactsManager::get_basic_group_full_info_object(ChatId chat_id) const {
  return get_basic_group_full_info_object(get_chat_full(chat_id));
}

string ContactsManager::get_secret_chat_database_key(SecretChatId secret_chat_id) {
  return PSTRING() << "sc" << secret_chat_id.get();
}

UserId ContactsManager::get_replies_bot_user_id() {
  return UserId(static_cast<int64>(G()->is_test_dc() ? 708513 : 1271266957));
}
}  // namespace td
