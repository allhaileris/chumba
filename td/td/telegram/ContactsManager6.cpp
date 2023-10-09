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
#include "td/telegram/GroupCallManager.h"

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






namespace td {
}  // namespace td
namespace td {

class EditChannelBannedQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  DialogId participant_dialog_id_;

 public:
  explicit EditChannelBannedQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, DialogId participant_dialog_id, tl_object_ptr<telegram_api::InputPeer> &&input_peer,
            const DialogParticipantStatus &status) {
    channel_id_ = channel_id;
    participant_dialog_id_ = participant_dialog_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::channels_editBanned(
        std::move(input_channel), std::move(input_peer), status.get_chat_banned_rights())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_editBanned>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditChannelBannedQuery: " << to_string(ptr);
    td_->contacts_manager_->invalidate_channel_full(channel_id_, false);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (participant_dialog_id_.get_type() != DialogType::Channel) {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "EditChannelBannedQuery");
    }
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("EditChannelBannedQuery");
  }
};

class LeaveChannelQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit LeaveChannelQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::channels_leaveChannel(std::move(input_channel))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_leaveChannel>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for LeaveChannelQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "LeaveChannelQuery");
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("LeaveChannelQuery");
  }
};

void ContactsManager::restrict_channel_participant(ChannelId channel_id, DialogId participant_dialog_id,
                                                   DialogParticipantStatus &&status,
                                                   DialogParticipantStatus &&old_status, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Restrict " << participant_dialog_id << " in " << channel_id << " from " << old_status << " to "
            << status;
  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!c->status.is_member() && !c->status.is_creator()) {
    if (participant_dialog_id == DialogId(get_my_id())) {
      if (status.is_member()) {
        return promise.set_error(Status::Error(400, "Can't unrestrict self"));
      }
      return promise.set_value(Unit());
    } else {
      return promise.set_error(Status::Error(400, "Not in the chat"));
    }
  }
  auto input_peer = td_->messages_manager_->get_input_peer(participant_dialog_id, AccessRights::Know);
  if (input_peer == nullptr) {
    return promise.set_error(Status::Error(400, "Member not found"));
  }

  if (participant_dialog_id == DialogId(get_my_id())) {
    if (status.is_restricted() || status.is_banned()) {
      return promise.set_error(Status::Error(400, "Can't restrict self"));
    }
    if (status.is_member()) {
      return promise.set_error(Status::Error(400, "Can't unrestrict self"));
    }

    // leave the channel
    speculative_add_channel_user(channel_id, participant_dialog_id.get_user_id(), status, c->status);
    td_->create_handler<LeaveChannelQuery>(std::move(promise))->send(channel_id);
    return;
  }

  switch (participant_dialog_id.get_type()) {
    case DialogType::User:
      // ok;
      break;
    case DialogType::Channel:
      if (status.is_administrator() || status.is_member() || status.is_restricted()) {
        return promise.set_error(Status::Error(400, "Other chats can be only banned or unbanned"));
      }
      break;
    default:
      return promise.set_error(Status::Error(400, "Can't restrict the chat"));
  }

  CHECK(!old_status.is_creator());
  CHECK(!status.is_creator());

  if (!get_channel_permissions(c).can_restrict_members()) {
    return promise.set_error(Status::Error(400, "Not enough rights to restrict/unrestrict chat member"));
  }

  if (old_status.is_member() && !status.is_member() && !status.is_banned()) {
    // we can't make participant Left without kicking it first
    auto on_result_promise = PromiseCreator::lambda([actor_id = actor_id(this), channel_id, participant_dialog_id,
                                                     status = std::move(status),
                                                     promise = std::move(promise)](Result<> result) mutable {
      if (result.is_error()) {
        return promise.set_error(result.move_as_error());
      }

      create_actor<SleepActor>(
          "RestrictChannelParticipantSleepActor", 1.0,
          PromiseCreator::lambda([actor_id, channel_id, participant_dialog_id, status = std::move(status),
                                  promise = std::move(promise)](Result<> result) mutable {
            if (result.is_error()) {
              return promise.set_error(result.move_as_error());
            }

            send_closure(actor_id, &ContactsManager::restrict_channel_participant, channel_id, participant_dialog_id,
                         std::move(status), DialogParticipantStatus::Banned(0), std::move(promise));
          }))
          .release();
    });

    promise = std::move(on_result_promise);
    status = DialogParticipantStatus::Banned(G()->unix_time() + 60);
  }

  if (participant_dialog_id.get_type() == DialogType::User) {
    speculative_add_channel_user(channel_id, participant_dialog_id.get_user_id(), status, old_status);
  }
  td_->create_handler<EditChannelBannedQuery>(std::move(promise))
      ->send(channel_id, participant_dialog_id, std::move(input_peer), status);
}

void ContactsManager::on_get_chat_participants(tl_object_ptr<telegram_api::ChatParticipants> &&participants_ptr,
                                               bool from_update) {
  switch (participants_ptr->get_id()) {
    case telegram_api::chatParticipantsForbidden::ID: {
      auto participants = move_tl_object_as<telegram_api::chatParticipantsForbidden>(participants_ptr);
      ChatId chat_id(participants->chat_id_);
      if (!chat_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << chat_id;
        return;
      }

      if (!have_chat_force(chat_id)) {
        LOG(ERROR) << chat_id << " not found";
        return;
      }

      if (from_update) {
        drop_chat_full(chat_id);
      }
      break;
    }
    case telegram_api::chatParticipants::ID: {
      auto participants = move_tl_object_as<telegram_api::chatParticipants>(participants_ptr);
      ChatId chat_id(participants->chat_id_);
      if (!chat_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << chat_id;
        return;
      }

      const Chat *c = get_chat_force(chat_id);
      if (c == nullptr) {
        LOG(ERROR) << chat_id << " not found";
        return;
      }

      ChatFull *chat_full = get_chat_full_force(chat_id, "telegram_api::chatParticipants");
      if (chat_full == nullptr) {
        LOG(INFO) << "Ignore update of members for unknown full " << chat_id;
        return;
      }

      UserId new_creator_user_id;
      vector<DialogParticipant> new_participants;
      new_participants.reserve(participants->participants_.size());

      for (auto &participant_ptr : participants->participants_) {
        DialogParticipant dialog_participant(std::move(participant_ptr), c->date, c->status.is_creator());
        if (!dialog_participant.is_valid()) {
          LOG(ERROR) << "Receive invalid " << dialog_participant;
          continue;
        }

        LOG_IF(ERROR, !td_->messages_manager_->have_dialog_info(dialog_participant.dialog_id_))
            << "Have no information about " << dialog_participant.dialog_id_ << " as a member of " << chat_id;
        LOG_IF(ERROR, !have_user(dialog_participant.inviter_user_id_))
            << "Have no information about " << dialog_participant.inviter_user_id_ << " as a member of " << chat_id;
        if (dialog_participant.joined_date_ < c->date) {
          LOG_IF(ERROR, dialog_participant.joined_date_ < c->date - 30 && c->date >= 1486000000)
              << "Wrong join date = " << dialog_participant.joined_date_ << " for " << dialog_participant.dialog_id_
              << ", " << chat_id << " was created at " << c->date;
          dialog_participant.joined_date_ = c->date;
        }
        if (dialog_participant.status_.is_creator() && dialog_participant.dialog_id_.get_type() == DialogType::User) {
          new_creator_user_id = dialog_participant.dialog_id_.get_user_id();
        }
        new_participants.push_back(std::move(dialog_participant));
      }

      if (chat_full->creator_user_id != new_creator_user_id) {
        if (new_creator_user_id.is_valid() && chat_full->creator_user_id.is_valid()) {
          LOG(ERROR) << "Group creator has changed from " << chat_full->creator_user_id << " to " << new_creator_user_id
                     << " in " << chat_id;
        }
        chat_full->creator_user_id = new_creator_user_id;
        chat_full->is_changed = true;
      }

      on_update_chat_full_participants(chat_full, chat_id, std::move(new_participants), participants->version_,
                                       from_update);
      if (from_update) {
        update_chat_full(chat_full, chat_id, "on_get_chat_participants");
      }
      break;
    }
    default:
      UNREACHABLE();
  }
}

class GetCreatedPublicChannelsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  PublicDialogType type_;

 public:
  explicit GetCreatedPublicChannelsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(PublicDialogType type, bool check_limit) {
    type_ = type;
    int32 flags = 0;
    if (type_ == PublicDialogType::IsLocationBased) {
      flags |= telegram_api::channels_getAdminedPublicChannels::BY_LOCATION_MASK;
    }
    if (check_limit) {
      flags |= telegram_api::channels_getAdminedPublicChannels::CHECK_LIMIT_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::channels_getAdminedPublicChannels(flags, false /*ignored*/, false /*ignored*/)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getAdminedPublicChannels>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto chats_ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetCreatedPublicChannelsQuery: " << to_string(chats_ptr);
    int32 constructor_id = chats_ptr->get_id();
    switch (constructor_id) {
      case telegram_api::messages_chats::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chats>(chats_ptr);
        td_->contacts_manager_->on_get_created_public_channels(type_, std::move(chats->chats_));
        break;
      }
      case telegram_api::messages_chatsSlice::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chatsSlice>(chats_ptr);
        LOG(ERROR) << "Receive chatsSlice in result of GetCreatedPublicChannelsQuery";
        td_->contacts_manager_->on_get_created_public_channels(type_, std::move(chats->chats_));
        break;
      }
      default:
        UNREACHABLE();
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::reload_created_public_dialogs(PublicDialogType type,
                                                    Promise<td_api::object_ptr<td_api::chats>> &&promise) {
  auto index = static_cast<int32>(type);
  get_created_public_channels_queries_[index].push_back(std::move(promise));
  if (get_created_public_channels_queries_[index].size() == 1) {
    auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), type](Result<Unit> &&result) {
      send_closure(actor_id, &ContactsManager::finish_get_created_public_dialogs, type, std::move(result));
    });
    td_->create_handler<GetCreatedPublicChannelsQuery>(std::move(query_promise))->send(type, false);
  }
}

void ContactsManager::check_created_public_dialogs_limit(PublicDialogType type, Promise<Unit> &&promise) {
  td_->create_handler<GetCreatedPublicChannelsQuery>(std::move(promise))->send(type, true);
}

class GetChatAdminWithInvitesQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLinkCounts>> promise_;
  DialogId dialog_id_;

 public:
  explicit GetChatAdminWithInvitesQuery(Promise<td_api::object_ptr<td_api::chatInviteLinkCounts>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_getAdminsWithInvites(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getAdminsWithInvites>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetChatAdminWithInvitesQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetChatAdminWithInvitesQuery");

    vector<td_api::object_ptr<td_api::chatInviteLinkCount>> invite_link_counts;
    for (auto &admin : result->admins_) {
      UserId user_id(admin->admin_id_);
      if (!user_id.is_valid()) {
        LOG(ERROR) << "Receive invalid invite link creator " << user_id << " in " << dialog_id_;
        continue;
      }
      invite_link_counts.push_back(td_api::make_object<td_api::chatInviteLinkCount>(
          td_->contacts_manager_->get_user_id_object(user_id, "chatInviteLinkCount"), admin->invites_count_,
          admin->revoked_invites_count_));
    }
    promise_.set_value(td_api::make_object<td_api::chatInviteLinkCounts>(std::move(invite_link_counts)));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetChatAdminWithInvitesQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::get_dialog_invite_link_counts(
    DialogId dialog_id, Promise<td_api::object_ptr<td_api::chatInviteLinkCounts>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id, true));

  td_->create_handler<GetChatAdminWithInvitesQuery>(std::move(promise))->send(dialog_id);
}

void ContactsManager::speculative_add_channel_participants(ChannelId channel_id, const vector<UserId> &added_user_ids,
                                                           UserId inviter_user_id, int32 date, bool by_me) {
  auto it = cached_channel_participants_.find(channel_id);
  auto channel_full = get_channel_full_force(channel_id, true, "speculative_add_channel_participants");
  bool is_participants_cache_changed = false;

  int32 delta_participant_count = 0;
  for (auto user_id : added_user_ids) {
    if (!user_id.is_valid()) {
      continue;
    }

    delta_participant_count++;

    if (it != cached_channel_participants_.end()) {
      auto &participants = it->second;
      bool is_found = false;
      for (auto &participant : participants) {
        if (participant.dialog_id_ == DialogId(user_id)) {
          is_found = true;
          break;
        }
      }
      if (!is_found) {
        is_participants_cache_changed = true;
        participants.emplace_back(DialogId(user_id), inviter_user_id, date, DialogParticipantStatus::Member());
      }
    }

    if (channel_full != nullptr && is_user_bot(user_id) && !td::contains(channel_full->bot_user_ids, user_id)) {
      channel_full->bot_user_ids.push_back(user_id);
      channel_full->need_save_to_database = true;
      reload_channel_full(channel_id, Promise<Unit>(), "speculative_add_channel_participants");

      send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                         channel_full->bot_user_ids, false);
    }
  }
  if (is_participants_cache_changed) {
    update_channel_online_member_count(channel_id, false);
  }
  if (channel_full != nullptr) {
    update_channel_full(channel_full, channel_id, "speculative_add_channel_participants");
  }
  if (delta_participant_count == 0) {
    return;
  }

  speculative_add_channel_participant_count(channel_id, delta_participant_count, by_me);
}

class DeleteChannelQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit DeleteChannelQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::channels_deleteChannel(std::move(input_channel))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_deleteChannel>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for DeleteChannelQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "DeleteChannelQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::delete_channel(ChannelId channel_id, Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!get_channel_status(c).is_creator()) {
    return promise.set_error(Status::Error(400, "Not enough rights to delete the chat"));
  }

  td_->create_handler<DeleteChannelQuery>(std::move(promise))->send(channel_id);
}

class LoadAsyncGraphQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::StatisticalGraph>> promise_;

 public:
  explicit LoadAsyncGraphQuery(Promise<td_api::object_ptr<td_api::StatisticalGraph>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const string &token, int64 x, DcId dc_id) {
    int32 flags = 0;
    if (x != 0) {
      flags |= telegram_api::stats_loadAsyncGraph::X_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::stats_loadAsyncGraph(flags, token, x), dc_id));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stats_loadAsyncGraph>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    promise_.set_value(ContactsManager::convert_stats_graph(std::move(result)));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_load_async_graph_query(DcId dc_id, string token, int64 x,
                                                  Promise<td_api::object_ptr<td_api::StatisticalGraph>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  td_->create_handler<LoadAsyncGraphQuery>(std::move(promise))->send(token, x, dc_id);
}

class GetSupportUserQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetSupportUserQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::help_getSupport()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_getSupport>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetSupportUserQuery: " << to_string(ptr);

    td_->contacts_manager_->on_get_user(std::move(ptr->user_), "GetSupportUserQuery", false, true);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

UserId ContactsManager::get_support_user(Promise<Unit> &&promise) {
  if (support_user_id_.is_valid()) {
    promise.set_value(Unit());
    return support_user_id_;
  }

  td_->create_handler<GetSupportUserQuery>(std::move(promise))->send();
  return UserId();
}

void ContactsManager::export_dialog_invite_link(DialogId dialog_id, string title, int32 expire_date, int32 usage_limit,
                                                bool creates_join_request, bool is_permanent,
                                                Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise) {
  get_me(PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, title = std::move(title), expire_date,
                                 usage_limit, creates_join_request, is_permanent,
                                 promise = std::move(promise)](Result<Unit> &&result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      send_closure(actor_id, &ContactsManager::export_dialog_invite_link_impl, dialog_id, std::move(title), expire_date,
                   usage_limit, creates_join_request, is_permanent, std::move(promise));
    }
  }));
}

void ContactsManager::on_update_user_is_contact(User *u, UserId user_id, bool is_contact, bool is_mutual_contact) {
  UserId my_id = get_my_id();
  if (user_id == my_id) {
    is_mutual_contact = is_contact;
  }
  if (!is_contact && is_mutual_contact) {
    LOG(ERROR) << "Receive is_mutual_contact == true for non-contact " << user_id;
    is_mutual_contact = false;
  }

  if (u->is_contact != is_contact || u->is_mutual_contact != is_mutual_contact) {
    LOG(DEBUG) << "Update " << user_id << " is_contact from (" << u->is_contact << ", " << u->is_mutual_contact
               << ") to (" << is_contact << ", " << is_mutual_contact << ")";
    if (u->is_contact != is_contact) {
      u->is_is_contact_changed = true;
    }
    u->is_contact = is_contact;
    u->is_mutual_contact = is_mutual_contact;
    u->is_changed = true;
  }
}

void ContactsManager::add_channel_participant_to_cache(ChannelId channel_id,
                                                       const DialogParticipant &dialog_participant,
                                                       bool allow_replace) {
  auto &participants = channel_participants_[channel_id];
  if (participants.participants_.empty()) {
    channel_participant_cache_timeout_.set_timeout_in(channel_id.get(), CHANNEL_PARTICIPANT_CACHE_TIME);
  }
  auto &participant_info = participants.participants_[dialog_participant.dialog_id_];
  if (participant_info.last_access_date_ > 0 && !allow_replace) {
    return;
  }
  participant_info.participant_ = dialog_participant;
  participant_info.last_access_date_ = G()->unix_time();
}

vector<DialogId> ContactsManager::get_dialog_ids(vector<tl_object_ptr<telegram_api::Chat>> &&chats,
                                                 const char *source) {
  vector<DialogId> dialog_ids;
  for (auto &chat : chats) {
    auto channel_id = get_channel_id(chat);
    if (!channel_id.is_valid()) {
      auto chat_id = get_chat_id(chat);
      if (!chat_id.is_valid()) {
        LOG(ERROR) << "Receive invalid chat from " << source << " in " << to_string(chat);
      } else {
        dialog_ids.push_back(DialogId(chat_id));
      }
    } else {
      dialog_ids.push_back(DialogId(channel_id));
    }
    on_get_chat(std::move(chat), source);
  }
  return dialog_ids;
}

void ContactsManager::on_update_channel_description(ChannelId channel_id, string &&description) {
  CHECK(channel_id.is_valid());
  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_description");
  if (channel_full == nullptr) {
    return;
  }
  if (channel_full->description != description) {
    channel_full->description = std::move(description);
    channel_full->is_changed = true;
    update_channel_full(channel_full, channel_id, "on_update_channel_description");
    td_->group_call_manager_->on_update_dialog_about(DialogId(channel_id), channel_full->description, true);
  }
}

ContactsManager::User *ContactsManager::get_user_force_impl(UserId user_id) {
  if (!user_id.is_valid()) {
    return nullptr;
  }

  User *u = get_user(user_id);
  if (u != nullptr) {
    return u;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (loaded_from_database_users_.count(user_id)) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load " << user_id << " from database";
  on_load_user_from_database(user_id, G()->td_db()->get_sqlite_sync_pmc()->get(get_user_database_key(user_id)), true);
  return get_user(user_id);
}

int32 ContactsManager::get_secret_chat_id_object(SecretChatId secret_chat_id, const char *source) const {
  if (secret_chat_id.is_valid() && get_secret_chat(secret_chat_id) == nullptr &&
      unknown_secret_chats_.count(secret_chat_id) == 0) {
    LOG(ERROR) << "Have no info about " << secret_chat_id << " from " << source;
    unknown_secret_chats_.insert(secret_chat_id);
    send_closure(G()->td(), &Td::send_update, get_update_unknown_secret_chat_object(secret_chat_id));
  }
  return secret_chat_id.get();
}

void ContactsManager::on_update_channel_slow_mode_next_send_date(ChannelId channel_id, int32 slow_mode_next_send_date) {
  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_slow_mode_next_send_date");
  if (channel_full != nullptr) {
    on_update_channel_full_slow_mode_next_send_date(channel_full, slow_mode_next_send_date);
    update_channel_full(channel_full, channel_id, "on_update_channel_slow_mode_next_send_date");
  }
}

int64 ContactsManager::get_user_id_object(UserId user_id, const char *source) const {
  if (user_id.is_valid() && get_user(user_id) == nullptr && unknown_users_.count(user_id) == 0) {
    LOG(ERROR) << "Have no info about " << user_id << " from " << source;
    unknown_users_.insert(user_id);
    send_closure(G()->td(), &Td::send_update, get_update_unknown_user_object(user_id));
  }
  return user_id.get();
}

tl_object_ptr<td_api::users> ContactsManager::get_users_object(int32 total_count,
                                                               const vector<UserId> &user_ids) const {
  if (total_count == -1) {
    total_count = narrow_cast<int32>(user_ids.size());
  }
  return td_api::make_object<td_api::users>(total_count, get_user_ids_object(user_ids, "get_users_object"));
}

bool ContactsManager::have_input_user(UserId user_id) const {
  if (user_id == get_my_id()) {
    return true;
  }

  const User *u = get_user(user_id);
  if (u == nullptr || u->access_hash == -1 || u->is_min_access_hash) {
    if (td_->auth_manager_->is_bot() && user_id.is_valid()) {
      return true;
    }
    return false;
  }

  return true;
}

const DialogParticipant *ContactsManager::get_chat_full_participant(const ChatFull *chat_full, DialogId dialog_id) {
  for (const auto &dialog_participant : chat_full->participants) {
    if (dialog_participant.dialog_id_ == dialog_id) {
      return &dialog_participant;
    }
  }
  return nullptr;
}

ContactsManager::User *ContactsManager::add_user(UserId user_id, const char *source) {
  CHECK(user_id.is_valid());
  auto &user_ptr = users_[user_id];
  if (user_ptr == nullptr) {
    user_ptr = make_unique<User>();
  }
  return user_ptr.get();
}

void ContactsManager::reload_user_full(UserId user_id) {
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_ok()) {
    send_get_user_full_query(user_id, r_input_user.move_as_ok(), Auto(), "reload_user_full");
  }
}

void ContactsManager::update_chat_online_member_count(const ChatFull *chat_full, ChatId chat_id, bool is_from_server) {
  update_dialog_online_member_count(chat_full->participants, DialogId(chat_id), is_from_server);
}

bool ContactsManager::is_appointed_chat_administrator(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return false;
  }
  return c->status.is_administrator();
}

int32 ContactsManager::get_channel_participant_count(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return 0;
  }
  return c->participant_count;
}

string ContactsManager::get_dialog_administrators_database_key(DialogId dialog_id) {
  return PSTRING() << "adm" << (-dialog_id.get());
}

UserId ContactsManager::get_channel_bot_user_id() {
  return UserId(static_cast<int64>(G()->is_test_dc() ? 936174 : 136817688));
}
}  // namespace td
