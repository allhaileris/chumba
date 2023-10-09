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

class CheckUsernameQuery final : public Td::ResultHandler {
  Promise<bool> promise_;

 public:
  explicit CheckUsernameQuery(Promise<bool> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &username) {
    send_query(G()->net_query_creator().create(telegram_api::account_checkUsername(username)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_checkUsername>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

class CheckChannelUsernameQuery final : public Td::ResultHandler {
  Promise<bool> promise_;
  ChannelId channel_id_;
  string username_;

 public:
  explicit CheckChannelUsernameQuery(Promise<bool> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, const string &username) {
    channel_id_ = channel_id;
    tl_object_ptr<telegram_api::InputChannel> input_channel;
    if (channel_id.is_valid()) {
      input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    } else {
      input_channel = make_tl_object<telegram_api::inputChannelEmpty>();
    }
    CHECK(input_channel != nullptr);
    send_query(
        G()->net_query_creator().create(telegram_api::channels_checkUsername(std::move(input_channel), username)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_checkUsername>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    if (channel_id_.is_valid()) {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "CheckChannelUsernameQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::check_dialog_username(DialogId dialog_id, const string &username,
                                            Promise<CheckDialogUsernameResult> &&promise) {
  if (dialog_id != DialogId() && !dialog_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User: {
      if (dialog_id.get_user_id() != get_my_id()) {
        return promise.set_error(Status::Error(400, "Can't check username for private chat with other user"));
      }
      break;
    }
    case DialogType::Channel: {
      auto c = get_channel(dialog_id.get_channel_id());
      if (c == nullptr) {
        return promise.set_error(Status::Error(400, "Chat not found"));
      }
      if (!get_channel_status(c).is_creator()) {
        return promise.set_error(Status::Error(400, "Not enough rights to change username"));
      }

      if (username == c->username) {
        return promise.set_value(CheckDialogUsernameResult::Ok);
      }
      break;
    }
    case DialogType::None:
      break;
    case DialogType::Chat:
    case DialogType::SecretChat:
      if (username.empty()) {
        return promise.set_value(CheckDialogUsernameResult::Ok);
      }
      return promise.set_error(Status::Error(400, "Chat can't have username"));
    default:
      UNREACHABLE();
      return;
  }

  if (username.empty()) {
    return promise.set_value(CheckDialogUsernameResult::Ok);
  }
  if (!is_valid_username(username)) {
    return promise.set_value(CheckDialogUsernameResult::Invalid);
  }

  auto request_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<bool> result) mutable {
    if (result.is_error()) {
      auto error = result.move_as_error();
      if (error.message() == "CHANNEL_PUBLIC_GROUP_NA") {
        return promise.set_value(CheckDialogUsernameResult::PublicGroupsUnavailable);
      }
      if (error.message() == "CHANNELS_ADMIN_PUBLIC_TOO_MUCH") {
        return promise.set_value(CheckDialogUsernameResult::PublicDialogsTooMuch);
      }
      if (error.message() == "USERNAME_INVALID") {
        return promise.set_value(CheckDialogUsernameResult::Invalid);
      }
      return promise.set_error(std::move(error));
    }

    promise.set_value(result.ok() ? CheckDialogUsernameResult::Ok : CheckDialogUsernameResult::Occupied);
  });

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->create_handler<CheckUsernameQuery>(std::move(request_promise))->send(username);
    case DialogType::Channel:
      return td_->create_handler<CheckChannelUsernameQuery>(std::move(request_promise))
          ->send(dialog_id.get_channel_id(), username);
    case DialogType::None:
      return td_->create_handler<CheckChannelUsernameQuery>(std::move(request_promise))->send(ChannelId(), username);
    case DialogType::Chat:
    case DialogType::SecretChat:
    default:
      UNREACHABLE();
  }
}

class GetChatInviteImportersQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLinkMembers>> promise_;
  DialogId dialog_id_;

 public:
  explicit GetChatInviteImportersQuery(Promise<td_api::object_ptr<td_api::chatInviteLinkMembers>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link, int32 offset_date, UserId offset_user_id, int32 limit) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    auto r_input_user = td_->contacts_manager_->get_input_user(offset_user_id);
    if (r_input_user.is_error()) {
      r_input_user = make_tl_object<telegram_api::inputUserEmpty>();
    }

    int32 flags = telegram_api::messages_getChatInviteImporters::LINK_MASK;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getChatInviteImporters(flags, false /*ignored*/, std::move(input_peer), invite_link,
                                                      string(), offset_date, r_input_user.move_as_ok(), limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getChatInviteImporters>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetChatInviteImportersQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetChatInviteImportersQuery");

    int32 total_count = result->count_;
    if (total_count < static_cast<int32>(result->importers_.size())) {
      LOG(ERROR) << "Receive wrong total count of invite link users " << total_count << " in " << dialog_id_;
      total_count = static_cast<int32>(result->importers_.size());
    }
    vector<td_api::object_ptr<td_api::chatInviteLinkMember>> invite_link_members;
    for (auto &importer : result->importers_) {
      UserId user_id(importer->user_id_);
      UserId approver_user_id(importer->approved_by_);
      if (!user_id.is_valid() || (!approver_user_id.is_valid() && approver_user_id != UserId()) ||
          importer->requested_) {
        LOG(ERROR) << "Receive invalid invite link importer: " << to_string(importer);
        total_count--;
        continue;
      }
      invite_link_members.push_back(td_api::make_object<td_api::chatInviteLinkMember>(
          td_->contacts_manager_->get_user_id_object(user_id, "chatInviteLinkMember"), importer->date_,
          td_->contacts_manager_->get_user_id_object(approver_user_id, "chatInviteLinkMember")));
    }
    promise_.set_value(td_api::make_object<td_api::chatInviteLinkMembers>(total_count, std::move(invite_link_members)));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetChatInviteImportersQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::get_dialog_invite_link_users(
    DialogId dialog_id, const string &invite_link, td_api::object_ptr<td_api::chatInviteLinkMember> offset_member,
    int32 limit, Promise<td_api::object_ptr<td_api::chatInviteLinkMembers>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }

  if (invite_link.empty()) {
    return promise.set_error(Status::Error(400, "Invite link must be non-empty"));
  }

  UserId offset_user_id;
  int32 offset_date = 0;
  if (offset_member != nullptr) {
    offset_user_id = UserId(offset_member->user_id_);
    offset_date = offset_member->joined_chat_date_;
  }

  td_->create_handler<GetChatInviteImportersQuery>(std::move(promise))
      ->send(dialog_id, invite_link, offset_date, offset_user_id, limit);
}

class ReportChannelSpamQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  DialogId sender_dialog_id_;

 public:
  explicit ReportChannelSpamQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, DialogId sender_dialog_id, const vector<MessageId> &message_ids) {
    channel_id_ = channel_id;
    sender_dialog_id_ = sender_dialog_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    auto input_peer = td_->messages_manager_->get_input_peer(sender_dialog_id, AccessRights::Know);
    CHECK(input_peer != nullptr);

    send_query(G()->net_query_creator().create(telegram_api::channels_reportSpam(
        std::move(input_channel), std::move(input_peer), MessagesManager::get_server_message_ids(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_reportSpam>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG_IF(INFO, !result) << "Report spam has failed in " << channel_id_;

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (sender_dialog_id_.get_type() != DialogType::Channel) {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ReportChannelSpamQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::report_channel_spam(ChannelId channel_id, const vector<MessageId> &message_ids,
                                          Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }
  if (!c->is_megagroup) {
    return promise.set_error(Status::Error(400, "Spam can be reported only in supergroups"));
  }
  if (!c->status.is_administrator()) {
    return promise.set_error(Status::Error(400, "Spam can be reported only by chat administrators"));
  }

  std::unordered_map<DialogId, vector<MessageId>, DialogIdHash> server_message_ids;
  for (auto &message_id : message_ids) {
    if (message_id.is_valid_scheduled()) {
      return promise.set_error(Status::Error(400, "Can't report scheduled messages"));
    }

    if (!message_id.is_valid()) {
      return promise.set_error(Status::Error(400, "Message not found"));
    }

    if (!message_id.is_server()) {
      continue;
    }

    auto sender_dialog_id = td_->messages_manager_->get_dialog_message_sender({DialogId(channel_id), message_id});
    if (sender_dialog_id.is_valid() && sender_dialog_id != DialogId(get_my_id()) &&
        td_->messages_manager_->have_input_peer(sender_dialog_id, AccessRights::Know)) {
      server_message_ids[sender_dialog_id].push_back(message_id);
    }
  }
  if (server_message_ids.empty()) {
    return promise.set_value(Unit());
  }

  MultiPromiseActorSafe mpas{"ReportSupergroupSpamMultiPromiseActor"};
  mpas.add_promise(std::move(promise));
  auto lock_promise = mpas.get_promise();

  for (auto &it : server_message_ids) {
    td_->create_handler<ReportChannelSpamQuery>(mpas.get_promise())->send(channel_id, it.first, it.second);
  }

  lock_promise.set_value(Unit());
}

class UpdateChannelUsernameQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  string username_;

 public:
  explicit UpdateChannelUsernameQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, const string &username) {
    channel_id_ = channel_id;
    username_ = username;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(
        G()->net_query_creator().create(telegram_api::channels_updateUsername(std::move(input_channel), username)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_updateUsername>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG(DEBUG) << "Receive result for UpdateChannelUsernameQuery: " << result;
    if (!result) {
      return on_error(Status::Error(500, "Supergroup username is not updated"));
    }

    td_->contacts_manager_->on_update_channel_username(channel_id_, std::move(username_));
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (status.message() == "USERNAME_NOT_MODIFIED" || status.message() == "CHAT_NOT_MODIFIED") {
      td_->contacts_manager_->on_update_channel_username(channel_id_, std::move(username_));
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "UpdateChannelUsernameQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_channel_username(ChannelId channel_id, const string &username, Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }
  if (!get_channel_status(c).is_creator()) {
    return promise.set_error(Status::Error(400, "Not enough rights to change supergroup username"));
  }

  if (!username.empty() && !is_valid_username(username)) {
    return promise.set_error(Status::Error(400, "Username is invalid"));
  }

  if (!username.empty() && c->username.empty()) {
    auto channel_full = get_channel_full(channel_id, false, "set_channel_username");
    if (channel_full != nullptr && !channel_full->can_set_username) {
      return promise.set_error(Status::Error(400, "Can't set supergroup username"));
    }
  }

  td_->create_handler<UpdateChannelUsernameQuery>(std::move(promise))->send(channel_id, username);
}

void ContactsManager::do_get_dialog_participant(DialogId dialog_id, DialogId participant_dialog_id,
                                                Promise<DialogParticipant> &&promise) {
  LOG(INFO) << "Receive GetChatMember request to get " << participant_dialog_id << " in " << dialog_id;
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "do_get_dialog_participant")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User: {
      auto my_user_id = get_my_id();
      auto peer_user_id = dialog_id.get_user_id();
      if (participant_dialog_id == DialogId(my_user_id)) {
        return promise.set_value(DialogParticipant::private_member(my_user_id, peer_user_id));
      }
      if (participant_dialog_id == dialog_id) {
        return promise.set_value(DialogParticipant::private_member(peer_user_id, my_user_id));
      }

      return promise.set_error(Status::Error(400, "Member not found"));
    }
    case DialogType::Chat:
      if (participant_dialog_id.get_type() != DialogType::User) {
        return promise.set_value(DialogParticipant::left(participant_dialog_id));
      }
      return get_chat_participant(dialog_id.get_chat_id(), participant_dialog_id.get_user_id(), std::move(promise));
    case DialogType::Channel:
      return get_channel_participant(dialog_id.get_channel_id(), participant_dialog_id, std::move(promise));
    case DialogType::SecretChat: {
      auto my_user_id = get_my_id();
      auto peer_user_id = get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      if (participant_dialog_id == DialogId(my_user_id)) {
        return promise.set_value(DialogParticipant::private_member(my_user_id, peer_user_id));
      }
      if (peer_user_id.is_valid() && participant_dialog_id == DialogId(peer_user_id)) {
        return promise.set_value(DialogParticipant::private_member(peer_user_id, my_user_id));
      }

      return promise.set_error(Status::Error(400, "Member not found"));
    }
    case DialogType::None:
    default:
      UNREACHABLE();
      return promise.set_error(Status::Error(500, "Wrong chat type"));
  }
}

void ContactsManager::transfer_dialog_ownership(DialogId dialog_id, UserId user_id, const string &password,
                                                Promise<Unit> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "transfer_dialog_ownership")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_user_force(user_id)) {
    return promise.set_error(Status::Error(400, "User not found"));
  }
  if (is_user_bot(user_id)) {
    return promise.set_error(Status::Error(400, "User is a bot"));
  }
  if (is_user_deleted(user_id)) {
    return promise.set_error(Status::Error(400, "User is deleted"));
  }
  if (password.empty()) {
    return promise.set_error(Status::Error(400, "PASSWORD_HASH_INVALID"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't transfer chat ownership"));
    case DialogType::Channel:
      send_closure(
          td_->password_manager_, &PasswordManager::get_input_check_password_srp, password,
          PromiseCreator::lambda([actor_id = actor_id(this), channel_id = dialog_id.get_channel_id(), user_id,
                                  promise = std::move(promise)](
                                     Result<tl_object_ptr<telegram_api::InputCheckPasswordSRP>> result) mutable {
            if (result.is_error()) {
              return promise.set_error(result.move_as_error());
            }
            send_closure(actor_id, &ContactsManager::transfer_channel_ownership, channel_id, user_id,
                         result.move_as_ok(), std::move(promise));
          }));
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

tl_object_ptr<td_api::chatStatisticsChannel> ContactsManager::convert_broadcast_stats(
    tl_object_ptr<telegram_api::stats_broadcastStats> obj) {
  CHECK(obj != nullptr);

  auto recent_message_interactions = transform(std::move(obj->recent_message_interactions_), [](auto &&interaction) {
    return make_tl_object<td_api::chatStatisticsMessageInteractionInfo>(
        MessageId(ServerMessageId(interaction->msg_id_)).get(), interaction->views_, interaction->forwards_);
  });

  return make_tl_object<td_api::chatStatisticsChannel>(
      convert_date_range(obj->period_), convert_stats_absolute_value(obj->followers_),
      convert_stats_absolute_value(obj->views_per_post_), convert_stats_absolute_value(obj->shares_per_post_),
      get_percentage_value(obj->enabled_notifications_->part_, obj->enabled_notifications_->total_),
      convert_stats_graph(std::move(obj->growth_graph_)), convert_stats_graph(std::move(obj->followers_graph_)),
      convert_stats_graph(std::move(obj->mute_graph_)), convert_stats_graph(std::move(obj->top_hours_graph_)),
      convert_stats_graph(std::move(obj->views_by_source_graph_)),
      convert_stats_graph(std::move(obj->new_followers_by_source_graph_)),
      convert_stats_graph(std::move(obj->languages_graph_)), convert_stats_graph(std::move(obj->interactions_graph_)),
      convert_stats_graph(std::move(obj->iv_interactions_graph_)), std::move(recent_message_interactions));
}

void ContactsManager::speculative_add_channel_participant_count(ChannelId channel_id, int32 delta_participant_count,
                                                                bool by_me) {
  if (by_me) {
    // Currently ignore all changes made by the current user, because they may be already counted
    invalidate_channel_full(channel_id, false);  // just in case
    return;
  }

  auto channel_full = get_channel_full_force(channel_id, true, "speculative_add_channel_participant_count");
  auto min_count = channel_full == nullptr ? 0 : channel_full->administrator_count;

  auto c = get_channel_force(channel_id);
  if (c != nullptr && c->participant_count != 0 &&
      speculative_add_count(c->participant_count, delta_participant_count, min_count)) {
    c->is_changed = true;
    update_channel(c, channel_id);
  }

  if (channel_full == nullptr) {
    return;
  }

  channel_full->is_changed |=
      speculative_add_count(channel_full->participant_count, delta_participant_count, min_count);

  if (channel_full->is_changed) {
    channel_full->speculative_version++;
  }

  update_channel_full(channel_full, channel_id, "speculative_add_channel_participant_count");
}

void ContactsManager::on_chat_update(telegram_api::chatForbidden &chat, const char *source) {
  ChatId chat_id(chat.id_);
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id << " from " << source;
    return;
  }

  bool is_uninited = get_chat_force(chat_id) == nullptr;
  Chat *c = add_chat(chat_id);
  on_update_chat_title(c, chat_id, std::move(chat.title_));
  // chat participant count will be updated in on_update_chat_status
  on_update_chat_photo(c, chat_id, nullptr);
  if (c->date != 0) {
    c->date = 0;  // removed in 38-th layer
    c->need_save_to_database = true;
  }
  on_update_chat_status(c, chat_id, DialogParticipantStatus::Banned(0));
  if (is_uninited) {
    on_update_chat_active(c, chat_id, true);
    on_update_chat_migrated_to_channel_id(c, chat_id, ChannelId());
  } else {
    // leave active and migrated to as is
  }
  if (c->cache_version != Chat::CACHE_VERSION) {
    c->cache_version = Chat::CACHE_VERSION;
    c->need_save_to_database = true;
  }
  c->is_received_from_server = true;
  update_chat(c, chat_id);
}

td_api::object_ptr<td_api::UserStatus> ContactsManager::get_user_status_object(UserId user_id, const User *u) const {
  if (u->is_bot) {
    return make_tl_object<td_api::userStatusOnline>(std::numeric_limits<int32>::max());
  }

  int32 was_online = get_user_was_online(u, user_id);
  switch (was_online) {
    case -3:
      return make_tl_object<td_api::userStatusLastMonth>();
    case -2:
      return make_tl_object<td_api::userStatusLastWeek>();
    case -1:
      return make_tl_object<td_api::userStatusRecently>();
    case 0:
      return make_tl_object<td_api::userStatusEmpty>();
    default: {
      int32 time = G()->unix_time();
      if (was_online > time) {
        return make_tl_object<td_api::userStatusOnline>(was_online);
      } else {
        return make_tl_object<td_api::userStatusOffline>(was_online);
      }
    }
  }
}

tl_object_ptr<td_api::chatMember> ContactsManager::get_chat_member_object(
    const DialogParticipant &dialog_participant) const {
  DialogId dialog_id = dialog_participant.dialog_id_;
  UserId participant_user_id;
  if (dialog_id.get_type() == DialogType::User) {
    participant_user_id = dialog_id.get_user_id();
  } else {
    td_->messages_manager_->force_create_dialog(dialog_id, "get_chat_member_object", true);
  }
  return td_api::make_object<td_api::chatMember>(
      get_message_sender_object_const(td_, dialog_id, "get_chat_member_object"),
      get_user_id_object(dialog_participant.inviter_user_id_, "chatMember.inviter_user_id"),
      dialog_participant.joined_date_, dialog_participant.status_.get_chat_member_status_object());
}

void ContactsManager::on_update_chat_default_permissions(Chat *c, ChatId chat_id, RestrictedRights default_permissions,
                                                         int32 version) {
  if (c->default_permissions != default_permissions && version >= c->default_permissions_version) {
    LOG(INFO) << "Update " << chat_id << " default permissions from " << c->default_permissions << " to "
              << default_permissions << " and version from " << c->default_permissions_version << " to " << version;
    c->default_permissions = default_permissions;
    c->default_permissions_version = version;
    c->is_default_permissions_changed = true;
    c->need_save_to_database = true;
  }
}

void ContactsManager::load_channel_from_database_impl(ChannelId channel_id, Promise<Unit> promise) {
  LOG(INFO) << "Load " << channel_id << " from database";
  auto &load_channel_queries = load_channel_from_database_queries_[channel_id];
  load_channel_queries.push_back(std::move(promise));
  if (load_channel_queries.size() == 1u) {
    G()->td_db()->get_sqlite_pmc()->get(
        get_channel_database_key(channel_id), PromiseCreator::lambda([channel_id](string value) {
          send_closure(G()->contacts_manager(), &ContactsManager::on_load_channel_from_database, channel_id,
                       std::move(value), false);
        }));
  }
}

void ContactsManager::on_update_channel_default_permissions(ChannelId channel_id,
                                                            RestrictedRights default_permissions) {
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id;
    return;
  }

  Channel *c = get_channel_force(channel_id);
  if (c != nullptr) {
    on_update_channel_default_permissions(c, channel_id, std::move(default_permissions));
    update_channel(c, channel_id);
  } else {
    LOG(INFO) << "Ignore update channel default permissions about unknown " << channel_id;
  }
}

UserId ContactsManager::load_my_id() {
  auto id_string = G()->td_db()->get_binlog_pmc()->get("my_id");
  if (!id_string.empty()) {
    UserId my_id(to_integer<int64>(id_string));
    if (my_id.is_valid()) {
      return my_id;
    }

    my_id = UserId(to_integer<int64>(Slice(id_string).substr(5)));
    if (my_id.is_valid()) {
      G()->td_db()->get_binlog_pmc()->set("my_id", to_string(my_id.get()));
      return my_id;
    }

    LOG(ERROR) << "Wrong my ID = \"" << id_string << "\" stored in database";
  }
  return UserId();
}

void ContactsManager::drop_chat_photos(ChatId chat_id, bool is_empty, bool drop_chat_full_photo, const char *source) {
  if (drop_chat_full_photo) {
    auto chat_full = get_chat_full(chat_id);  // must not load ChatFull
    if (chat_full == nullptr) {
      return;
    }

    on_update_chat_full_photo(chat_full, chat_id, Photo());
    if (!is_empty) {
      reload_chat_full(chat_id, Auto());
    }
    update_chat_full(chat_full, chat_id, "drop_chat_photos");
  }
}

tl_object_ptr<telegram_api::InputChannel> ContactsManager::get_input_channel(ChannelId channel_id) const {
  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    if (td_->auth_manager_->is_bot() && channel_id.is_valid()) {
      return make_tl_object<telegram_api::inputChannel>(channel_id.get(), 0);
    }
    return nullptr;
  }

  return make_tl_object<telegram_api::inputChannel>(channel_id.get(), c->access_hash);
}

ContactsManager::ChannelType ContactsManager::get_channel_type(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    auto min_channel = get_min_channel(channel_id);
    if (min_channel != nullptr) {
      return min_channel->is_megagroup_ ? ChannelType::Megagroup : ChannelType::Broadcast;
    }
    return ChannelType::Unknown;
  }
  return get_channel_type(c);
}

tl_object_ptr<td_api::statisticalValue> ContactsManager::convert_stats_absolute_value(
    const tl_object_ptr<telegram_api::statsAbsValueAndPrev> &obj) {
  return make_tl_object<td_api::statisticalValue>(obj->current_, obj->previous_,
                                                  get_percentage_value(obj->current_ - obj->previous_, obj->previous_));
}

vector<td_api::object_ptr<td_api::chatNearby>> ContactsManager::get_chats_nearby_object(
    const vector<DialogNearby> &dialogs_nearby) {
  return transform(dialogs_nearby, [](const DialogNearby &dialog_nearby) {
    return td_api::make_object<td_api::chatNearby>(dialog_nearby.dialog_id.get(), dialog_nearby.distance);
  });
}

ContactsManager::UserFull *ContactsManager::add_user_full(UserId user_id) {
  CHECK(user_id.is_valid());
  auto &user_full_ptr = users_full_[user_id];
  if (user_full_ptr == nullptr) {
    user_full_ptr = make_unique<UserFull>();
  }
  return user_full_ptr.get();
}

void ContactsManager::remove_inactive_channel(ChannelId channel_id) {
  if (inactive_channels_inited_ && td::remove(inactive_channels_, channel_id)) {
    LOG(DEBUG) << "Remove " << channel_id << " from list of inactive channels";
  }
}

void ContactsManager::on_update_chat_active(Chat *c, ChatId chat_id, bool is_active) {
  if (c->is_active != is_active) {
    c->is_active = is_active;
    c->is_is_active_changed = true;
    c->is_changed = true;
  }
}

bool ContactsManager::get_channel_has_protected_content(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return false;
  }
  return c->noforwards;
}

ContactsManager::Chat *ContactsManager::get_chat(ChatId chat_id) {
  auto p = chats_.find(chat_id);
  if (p == chats_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

tl_object_ptr<td_api::user> ContactsManager::get_user_object(UserId user_id) const {
  return get_user_object(user_id, get_user(user_id));
}

bool ContactsManager::have_secret_chat(SecretChatId secret_chat_id) const {
  return secret_chats_.count(secret_chat_id) > 0;
}
}  // namespace td
