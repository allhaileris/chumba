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

class GetChannelParticipantQuery final : public Td::ResultHandler {
  Promise<DialogParticipant> promise_;
  ChannelId channel_id_;
  DialogId participant_dialog_id_;

 public:
  explicit GetChannelParticipantQuery(Promise<DialogParticipant> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, DialogId participant_dialog_id, tl_object_ptr<telegram_api::InputPeer> &&input_peer) {
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      return promise_.set_error(Status::Error(400, "Supergroup not found"));
    }

    CHECK(input_peer != nullptr);

    channel_id_ = channel_id;
    participant_dialog_id_ = participant_dialog_id;
    send_query(G()->net_query_creator().create(
        telegram_api::channels_getParticipant(std::move(input_channel), std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getParticipant>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto participant = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetChannelParticipantQuery: " << to_string(participant);

    td_->contacts_manager_->on_get_users(std::move(participant->users_), "GetChannelParticipantQuery");
    td_->contacts_manager_->on_get_chats(std::move(participant->chats_), "GetChannelParticipantQuery");
    DialogParticipant result(std::move(participant->participant_));
    if (!result.is_valid()) {
      LOG(ERROR) << "Receive invalid " << result;
      return promise_.set_error(Status::Error(500, "Receive invalid chat member"));
    }
    promise_.set_value(std::move(result));
  }

  void on_error(Status status) final {
    if (status.message() == "USER_NOT_PARTICIPANT") {
      promise_.set_value(DialogParticipant::left(participant_dialog_id_));
      return;
    }

    if (participant_dialog_id_.get_type() != DialogType::Channel) {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetChannelParticipantQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_channel_participant_status(ChannelId channel_id, DialogId participant_dialog_id,
                                                     DialogParticipantStatus status, Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }

  if (participant_dialog_id == DialogId(get_my_id())) {
    // fast path is needed, because get_channel_status may return Creator, while GetChannelParticipantQuery returning Left
    return set_channel_participant_status_impl(channel_id, participant_dialog_id, std::move(status),
                                               get_channel_status(c), std::move(promise));
  }
  if (participant_dialog_id.get_type() != DialogType::User) {
    if (status.is_administrator() || status.is_member() || status.is_restricted()) {
      return promise.set_error(Status::Error(400, "Other chats can be only banned or unbanned"));
    }
    // always pretend that old_status is different
    return restrict_channel_participant(
        channel_id, participant_dialog_id, std::move(status),
        status.is_banned() ? DialogParticipantStatus::Left() : DialogParticipantStatus::Banned(0), std::move(promise));
  }

  auto input_peer = td_->messages_manager_->get_input_peer(participant_dialog_id, AccessRights::Read);
  if (input_peer == nullptr) {
    return promise.set_error(Status::Error(400, "Member not found"));
  }

  auto on_result_promise =
      PromiseCreator::lambda([actor_id = actor_id(this), channel_id, participant_dialog_id, status,
                              promise = std::move(promise)](Result<DialogParticipant> r_dialog_participant) mutable {
        // ResultHandlers are cleared before managers, so it is safe to capture this
        if (r_dialog_participant.is_error()) {
          return promise.set_error(r_dialog_participant.move_as_error());
        }

        send_closure(actor_id, &ContactsManager::set_channel_participant_status_impl, channel_id, participant_dialog_id,
                     std::move(status), r_dialog_participant.ok().status_, std::move(promise));
      });

  td_->create_handler<GetChannelParticipantQuery>(std::move(on_result_promise))
      ->send(channel_id, participant_dialog_id, std::move(input_peer));
}

void ContactsManager::get_channel_participant(ChannelId channel_id, DialogId participant_dialog_id,
                                              Promise<DialogParticipant> &&promise) {
  LOG(INFO) << "Trying to get " << participant_dialog_id << " as member of " << channel_id;

  auto input_peer = td_->messages_manager_->get_input_peer(participant_dialog_id, AccessRights::Know);
  if (input_peer == nullptr) {
    return promise.set_error(Status::Error(400, "Member not found"));
  }

  if (have_channel_participant_cache(channel_id)) {
    auto *participant = get_channel_participant_from_cache(channel_id, participant_dialog_id);
    if (participant != nullptr) {
      return promise.set_value(DialogParticipant{*participant});
    }
  }

  auto on_result_promise = PromiseCreator::lambda([actor_id = actor_id(this), channel_id, promise = std::move(promise)](
                                                      Result<DialogParticipant> r_dialog_participant) mutable {
    TRY_RESULT_PROMISE(promise, dialog_participant, std::move(r_dialog_participant));
    send_closure(actor_id, &ContactsManager::finish_get_channel_participant, channel_id, std::move(dialog_participant),
                 std::move(promise));
  });

  td_->create_handler<GetChannelParticipantQuery>(std::move(on_result_promise))
      ->send(channel_id, participant_dialog_id, std::move(input_peer));
}

class RevokeChatInviteLinkQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLinks>> promise_;
  DialogId dialog_id_;

 public:
  explicit RevokeChatInviteLinkQuery(Promise<td_api::object_ptr<td_api::chatInviteLinks>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    int32 flags = telegram_api::messages_editExportedChatInvite::REVOKED_MASK;
    send_query(G()->net_query_creator().create(telegram_api::messages_editExportedChatInvite(
        flags, false /*ignored*/, std::move(input_peer), invite_link, 0, 0, false, string())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editExportedChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for RevokeChatInviteLinkQuery: " << to_string(result);

    vector<td_api::object_ptr<td_api::chatInviteLink>> links;
    switch (result->get_id()) {
      case telegram_api::messages_exportedChatInvite::ID: {
        auto invite = move_tl_object_as<telegram_api::messages_exportedChatInvite>(result);

        td_->contacts_manager_->on_get_users(std::move(invite->users_), "RevokeChatInviteLinkQuery");

        DialogInviteLink invite_link(std::move(invite->invite_));
        if (!invite_link.is_valid()) {
          return on_error(Status::Error(500, "Receive invalid invite link"));
        }
        links.push_back(invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
        break;
      }
      case telegram_api::messages_exportedChatInviteReplaced::ID: {
        auto invite = move_tl_object_as<telegram_api::messages_exportedChatInviteReplaced>(result);

        td_->contacts_manager_->on_get_users(std::move(invite->users_), "RevokeChatInviteLinkQuery");

        DialogInviteLink invite_link(std::move(invite->invite_));
        DialogInviteLink new_invite_link(std::move(invite->new_invite_));
        if (!invite_link.is_valid() || !new_invite_link.is_valid()) {
          return on_error(Status::Error(500, "Receive invalid invite link"));
        }
        if (new_invite_link.get_creator_user_id() == td_->contacts_manager_->get_my_id() &&
            new_invite_link.is_permanent()) {
          td_->contacts_manager_->on_get_permanent_dialog_invite_link(dialog_id_, new_invite_link);
        }
        links.push_back(invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
        links.push_back(new_invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
        break;
      }
      default:
        UNREACHABLE();
    }
    auto total_count = static_cast<int32>(links.size());
    promise_.set_value(td_api::make_object<td_api::chatInviteLinks>(total_count, std::move(links)));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "RevokeChatInviteLinkQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::revoke_dialog_invite_link(DialogId dialog_id, const string &invite_link,
                                                Promise<td_api::object_ptr<td_api::chatInviteLinks>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));

  if (invite_link.empty()) {
    return promise.set_error(Status::Error(400, "Invite link must be non-empty"));
  }

  td_->create_handler<RevokeChatInviteLinkQuery>(std::move(promise))->send(dialog_id, invite_link);
}

class TogglePrehistoryHiddenQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  bool is_all_history_available_;

 public:
  explicit TogglePrehistoryHiddenQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, bool is_all_history_available) {
    channel_id_ = channel_id;
    is_all_history_available_ = is_all_history_available;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::channels_togglePreHistoryHidden(std::move(input_channel), !is_all_history_available)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_togglePreHistoryHidden>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for TogglePrehistoryHiddenQuery: " << to_string(ptr);

    td_->updates_manager_->on_get_updates(
        std::move(ptr),
        PromiseCreator::lambda([actor_id = G()->contacts_manager(), promise = std::move(promise_),
                                channel_id = channel_id_,
                                is_all_history_available = is_all_history_available_](Unit result) mutable {
          send_closure(actor_id, &ContactsManager::on_update_channel_is_all_history_available, channel_id,
                       is_all_history_available, std::move(promise));
        }));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "TogglePrehistoryHiddenQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::toggle_channel_is_all_history_available(ChannelId channel_id, bool is_all_history_available,
                                                              Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }
  if (!get_channel_permissions(c).can_change_info_and_settings()) {
    return promise.set_error(Status::Error(400, "Not enough rights to toggle all supergroup history availability"));
  }
  if (get_channel_type(c) != ChannelType::Megagroup) {
    return promise.set_error(Status::Error(400, "Message history can be hidden in supergroups only"));
  }
  if (c->has_linked_channel && !is_all_history_available) {
    return promise.set_error(Status::Error(400, "Message history can't be hidden in discussion supergroups"));
  }
  // it can be toggled in public chats, but will not affect them

  td_->create_handler<TogglePrehistoryHiddenQuery>(std::move(promise))->send(channel_id, is_all_history_available);
}

class EditLocationQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  DialogLocation location_;

 public:
  explicit EditLocationQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, const DialogLocation &location) {
    channel_id_ = channel_id;
    location_ = location;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    send_query(G()->net_query_creator().create(telegram_api::channels_editLocation(
        std::move(input_channel), location_.get_input_geo_point(), location_.get_address())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_editLocation>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG_IF(INFO, !result) << "Edit chat location has failed";

    td_->contacts_manager_->on_update_channel_location(channel_id_, location_);
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "EditLocationQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_channel_location(DialogId dialog_id, const DialogLocation &location,
                                           Promise<Unit> &&promise) {
  if (location.empty()) {
    return promise.set_error(Status::Error(400, "Invalid chat location specified"));
  }

  if (!dialog_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid chat identifier specified"));
  }
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "set_channel_location")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (dialog_id.get_type() != DialogType::Channel) {
    return promise.set_error(Status::Error(400, "Chat is not a supergroup"));
  }

  auto channel_id = dialog_id.get_channel_id();
  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!c->is_megagroup) {
    return promise.set_error(Status::Error(400, "Chat is not a supergroup"));
  }
  if (!c->status.is_creator()) {
    return promise.set_error(Status::Error(400, "Not enough rights in the supergroup"));
  }

  td_->create_handler<EditLocationQuery>(std::move(promise))->send(channel_id, location);
}

class HideChatJoinRequestQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit HideChatJoinRequestQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, UserId user_id, bool approve) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    auto r_input_user = td_->contacts_manager_->get_input_user(user_id);
    if (r_input_user.is_error()) {
      return on_error(r_input_user.move_as_error());
    }

    int32 flags = 0;
    if (approve) {
      flags |= telegram_api::messages_hideChatJoinRequest::APPROVED_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::messages_hideChatJoinRequest(
        flags, false /*ignored*/, std::move(input_peer), r_input_user.move_as_ok())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_hideChatJoinRequest>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for HideChatJoinRequestQuery: " << to_string(result);
    td_->updates_manager_->on_get_updates(std::move(result), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "HideChatJoinRequestQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::process_dialog_join_request(DialogId dialog_id, UserId user_id, bool approve,
                                                  Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));
  td_->create_handler<HideChatJoinRequestQuery>(std::move(promise))->send(dialog_id, user_id, approve);
}

class DeleteExportedChatInviteQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit DeleteExportedChatInviteQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_deleteExportedChatInvite(std::move(input_peer), invite_link)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteExportedChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "DeleteExportedChatInviteQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::delete_revoked_dialog_invite_link(DialogId dialog_id, const string &invite_link,
                                                        Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));

  if (invite_link.empty()) {
    return promise.set_error(Status::Error(400, "Invite link must be non-empty"));
  }

  td_->create_handler<DeleteExportedChatInviteQuery>(std::move(promise))->send(dialog_id, invite_link);
}

void ContactsManager::set_dialog_participant_status(DialogId dialog_id, DialogId participant_dialog_id,
                                                    DialogParticipantStatus &&status, Promise<Unit> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "set_dialog_participant_status")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Chat member status can't be changed in private chats"));
    case DialogType::Chat:
      if (participant_dialog_id.get_type() != DialogType::User) {
        if (status == DialogParticipantStatus::Left()) {
          return promise.set_value(Unit());
        } else {
          return promise.set_error(Status::Error(400, "Chats can't be members of basic groups"));
        }
      }
      return set_chat_participant_status(dialog_id.get_chat_id(), participant_dialog_id.get_user_id(), status,
                                         std::move(promise));
    case DialogType::Channel:
      return set_channel_participant_status(dialog_id.get_channel_id(), participant_dialog_id, status,
                                            std::move(promise));
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Chat member status can't be changed in secret chats"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void ContactsManager::on_get_permanent_dialog_invite_link(DialogId dialog_id, const DialogInviteLink &invite_link) {
  switch (dialog_id.get_type()) {
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto chat_full = get_chat_full_force(chat_id, "on_get_permanent_dialog_invite_link");
      if (chat_full != nullptr && update_permanent_invite_link(chat_full->invite_link, invite_link)) {
        chat_full->is_changed = true;
        update_chat_full(chat_full, chat_id, "on_get_permanent_dialog_invite_link");
      }
      break;
    }
    case DialogType::Channel: {
      auto channel_id = dialog_id.get_channel_id();
      auto channel_full = get_channel_full_force(channel_id, true, "on_get_permanent_dialog_invite_link");
      if (channel_full != nullptr && update_permanent_invite_link(channel_full->invite_link, invite_link)) {
        channel_full->is_changed = true;
        update_channel_full(channel_full, channel_id, "on_get_permanent_dialog_invite_link");
      }
      break;
    }
    case DialogType::User:
    case DialogType::SecretChat:
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void ContactsManager::on_update_channel_full_slow_mode_next_send_date(ChannelFull *channel_full,
                                                                      int32 slow_mode_next_send_date) {
  if (slow_mode_next_send_date < 0) {
    LOG(ERROR) << "Receive slow mode next send date " << slow_mode_next_send_date;
    slow_mode_next_send_date = 0;
  }
  if (channel_full->slow_mode_delay == 0 && slow_mode_next_send_date > 0) {
    LOG(ERROR) << "Slow mode is disabled, but next send date is " << slow_mode_next_send_date;
    slow_mode_next_send_date = 0;
  }

  if (slow_mode_next_send_date != 0) {
    auto now = G()->unix_time();
    if (slow_mode_next_send_date <= now) {
      slow_mode_next_send_date = 0;
    }
    if (slow_mode_next_send_date > now + 3601) {
      slow_mode_next_send_date = now + 3601;
    }
  }
  if (channel_full->slow_mode_next_send_date != slow_mode_next_send_date) {
    channel_full->slow_mode_next_send_date = slow_mode_next_send_date;
    channel_full->is_slow_mode_next_send_date_changed = true;
    channel_full->is_changed = true;
  }
}

td_api::object_ptr<td_api::CanTransferOwnershipResult> ContactsManager::get_can_transfer_ownership_result_object(
    CanTransferOwnershipResult result) {
  switch (result.type) {
    case CanTransferOwnershipResult::Type::Ok:
      return td_api::make_object<td_api::canTransferOwnershipResultOk>();
    case CanTransferOwnershipResult::Type::PasswordNeeded:
      return td_api::make_object<td_api::canTransferOwnershipResultPasswordNeeded>();
    case CanTransferOwnershipResult::Type::PasswordTooFresh:
      return td_api::make_object<td_api::canTransferOwnershipResultPasswordTooFresh>(result.retry_after);
    case CanTransferOwnershipResult::Type::SessionTooFresh:
      return td_api::make_object<td_api::canTransferOwnershipResultSessionTooFresh>(result.retry_after);
    default:
      UNREACHABLE();
      return nullptr;
  }
}

ContactsManager::Chat *ContactsManager::get_chat_force(ChatId chat_id) {
  if (!chat_id.is_valid()) {
    return nullptr;
  }

  Chat *c = get_chat(chat_id);
  if (c != nullptr) {
    if (c->migrated_to_channel_id.is_valid() && !have_channel_force(c->migrated_to_channel_id)) {
      LOG(ERROR) << "Can't find " << c->migrated_to_channel_id << " from " << chat_id;
    }

    return c;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (loaded_from_database_chats_.count(chat_id)) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load " << chat_id << " from database";
  on_load_chat_from_database(chat_id, G()->td_db()->get_sqlite_sync_pmc()->get(get_chat_database_key(chat_id)), true);
  return get_chat(chat_id);
}

void ContactsManager::get_channel_statistics(DialogId dialog_id, bool is_dark,
                                             Promise<td_api::object_ptr<td_api::ChatStatistics>> &&promise) {
  auto dc_id_promise = PromiseCreator::lambda(
      [actor_id = actor_id(this), dialog_id, is_dark, promise = std::move(promise)](Result<DcId> r_dc_id) mutable {
        if (r_dc_id.is_error()) {
          return promise.set_error(r_dc_id.move_as_error());
        }
        send_closure(actor_id, &ContactsManager::send_get_channel_stats_query, r_dc_id.move_as_ok(),
                     dialog_id.get_channel_id(), is_dark, std::move(promise));
      });
  get_channel_statistics_dc_id(dialog_id, true, std::move(dc_id_promise));
}

void ContactsManager::on_load_administrator_users_finished(
    DialogId dialog_id, vector<DialogAdministrator> administrators, Result<> result,
    Promise<td_api::object_ptr<td_api::chatAdministrators>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (result.is_error()) {
    return reload_dialog_administrators(dialog_id, {}, std::move(promise));
  }

  auto it = dialog_administrators_.emplace(dialog_id, std::move(administrators)).first;
  reload_dialog_administrators(dialog_id, it->second, Auto());  // update administrators cache
  promise.set_value(get_chat_administrators_object(it->second));
}

void ContactsManager::finish_get_channel_participant(ChannelId channel_id, DialogParticipant &&dialog_participant,
                                                     Promise<DialogParticipant> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Receive a member " << dialog_participant.dialog_id_ << " of a channel " << channel_id;

  dialog_participant.status_.update_restrictions();
  if (have_channel_participant_cache(channel_id)) {
    add_channel_participant_to_cache(channel_id, dialog_participant, false);
  }
  promise.set_value(std::move(dialog_participant));
}

void ContactsManager::on_invite_link_info_expire_timeout(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  auto access_it = dialog_access_by_invite_link_.find(dialog_id);
  if (access_it == dialog_access_by_invite_link_.end()) {
    return;
  }
  auto expires_in = access_it->second.accessible_before - G()->unix_time() - 1;
  if (expires_in >= 3) {
    invite_link_info_expire_timeout_.set_timeout_in(dialog_id.get(), expires_in);
    return;
  }

  remove_dialog_access_by_invite_link(dialog_id);
}

void ContactsManager::on_update_user_name(UserId user_id, string &&first_name, string &&last_name, string &&username) {
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  User *u = get_user_force(user_id);
  if (u != nullptr) {
    on_update_user_name(u, user_id, std::move(first_name), std::move(last_name), std::move(username));
    update_user(u, user_id);
  } else {
    LOG(INFO) << "Ignore update user name about unknown " << user_id;
  }
}

void ContactsManager::invalidate_user_full(UserId user_id) {
  auto user_full = get_user_full_force(user_id);
  if (user_full != nullptr) {
    td_->messages_manager_->on_dialog_info_full_invalidated(DialogId(user_id));

    if (!user_full->is_expired()) {
      user_full->expires_at = 0.0;
      user_full->need_save_to_database = true;

      update_user_full(user_full, user_id, "invalidate_user_full");
    }
  }
}

void ContactsManager::update_channel_online_member_count(ChannelId channel_id, bool is_from_server) {
  if (get_channel_type(channel_id) != ChannelType::Megagroup) {
    return;
  }

  auto it = cached_channel_participants_.find(channel_id);
  if (it == cached_channel_participants_.end()) {
    return;
  }
  update_dialog_online_member_count(it->second, DialogId(channel_id), is_from_server);
}

void ContactsManager::on_update_channel_noforwards(Channel *c, ChannelId channel_id, bool noforwards) {
  if (c->noforwards != noforwards) {
    LOG(INFO) << "Update " << channel_id << " has_protected_content from " << c->noforwards << " to " << noforwards;
    c->noforwards = noforwards;
    c->is_noforwards_changed = true;
    c->need_save_to_database = true;
  }
}

tl_object_ptr<td_api::basicGroup> ContactsManager::get_basic_group_object(ChatId chat_id, const Chat *c) {
  if (c == nullptr) {
    return nullptr;
  }
  if (c->migrated_to_channel_id.is_valid()) {
    get_channel_force(c->migrated_to_channel_id);
  }
  return get_basic_group_object_const(chat_id, c);
}

bool ContactsManager::speculative_add_count(int32 &count, int32 delta_count, int32 min_count) {
  auto new_count = count + delta_count;
  if (new_count < min_count) {
    new_count = min_count;
  }
  if (new_count == count) {
    return false;
  }

  count = new_count;
  return true;
}

const DialogPhoto *ContactsManager::get_secret_chat_dialog_photo(SecretChatId secret_chat_id) {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return nullptr;
  }
  return get_user_dialog_photo(c->user_id);
}

bool ContactsManager::have_input_peer_channel(ChannelId channel_id, AccessRights access_rights) const {
  const Channel *c = get_channel(channel_id);
  return have_input_peer_channel(c, channel_id, access_rights);
}

const ContactsManager::Chat *ContactsManager::get_chat(ChatId chat_id) const {
  auto p = chats_.find(chat_id);
  if (p == chats_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

ContactsManager::ChannelType ContactsManager::get_channel_type(const Channel *c) {
  if (c->is_megagroup) {
    return ChannelType::Megagroup;
  }
  return ChannelType::Broadcast;
}

const ContactsManager::ChannelFull *ContactsManager::get_channel_full(ChannelId channel_id) const {
  return get_channel_full_const(channel_id);
}

bool ContactsManager::have_channel_force(ChannelId channel_id) {
  return get_channel_force(channel_id) != nullptr;
}

bool ContactsManager::have_min_user(UserId user_id) const {
  return users_.count(user_id) > 0;
}
}  // namespace td
