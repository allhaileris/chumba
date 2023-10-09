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




#include <utility>

namespace td {
}  // namespace td
namespace td {

class GetChatJoinRequestsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatJoinRequests>> promise_;
  DialogId dialog_id_;
  bool is_full_list_ = false;

 public:
  explicit GetChatJoinRequestsQuery(Promise<td_api::object_ptr<td_api::chatJoinRequests>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link, const string &query, int32 offset_date,
            UserId offset_user_id, int32 limit) {
    dialog_id_ = dialog_id;
    is_full_list_ =
        invite_link.empty() && query.empty() && offset_date == 0 && !offset_user_id.is_valid() && limit >= 3;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    auto r_input_user = td_->contacts_manager_->get_input_user(offset_user_id);
    if (r_input_user.is_error()) {
      r_input_user = make_tl_object<telegram_api::inputUserEmpty>();
    }

    int32 flags = telegram_api::messages_getChatInviteImporters::REQUESTED_MASK;
    if (!invite_link.empty()) {
      flags |= telegram_api::messages_getChatInviteImporters::LINK_MASK;
    }
    if (!query.empty()) {
      flags |= telegram_api::messages_getChatInviteImporters::Q_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getChatInviteImporters(flags, false /*ignored*/, std::move(input_peer), invite_link,
                                                      query, offset_date, r_input_user.move_as_ok(), limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getChatInviteImporters>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetChatJoinRequestsQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetChatJoinRequestsQuery");

    int32 total_count = result->count_;
    if (total_count < static_cast<int32>(result->importers_.size())) {
      LOG(ERROR) << "Receive wrong total count of join requests " << total_count << " in " << dialog_id_;
      total_count = static_cast<int32>(result->importers_.size());
    }
    vector<td_api::object_ptr<td_api::chatJoinRequest>> join_requests;
    vector<int64> recent_requesters;
    for (auto &request : result->importers_) {
      UserId user_id(request->user_id_);
      UserId approver_user_id(request->approved_by_);
      if (!user_id.is_valid() || approver_user_id.is_valid() || !request->requested_) {
        LOG(ERROR) << "Receive invalid join request: " << to_string(request);
        total_count--;
        continue;
      }
      if (recent_requesters.size() < 3) {
        recent_requesters.push_back(user_id.get());
      }
      join_requests.push_back(td_api::make_object<td_api::chatJoinRequest>(
          td_->contacts_manager_->get_user_id_object(user_id, "chatJoinRequest"), request->date_, request->about_));
    }
    if (is_full_list_) {
      td_->messages_manager_->on_update_dialog_pending_join_requests(dialog_id_, total_count,
                                                                     std::move(recent_requesters));
    }
    promise_.set_value(td_api::make_object<td_api::chatJoinRequests>(total_count, std::move(join_requests)));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetChatJoinRequestsQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::get_dialog_join_requests(DialogId dialog_id, const string &invite_link, const string &query,
                                               td_api::object_ptr<td_api::chatJoinRequest> offset_request, int32 limit,
                                               Promise<td_api::object_ptr<td_api::chatJoinRequests>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }

  UserId offset_user_id;
  int32 offset_date = 0;
  if (offset_request != nullptr) {
    offset_user_id = UserId(offset_request->user_id_);
    offset_date = offset_request->date_;
  }

  td_->create_handler<GetChatJoinRequestsQuery>(std::move(promise))
      ->send(dialog_id, invite_link, query, offset_date, offset_user_id, limit);
}

void ContactsManager::update_channel(Channel *c, ChannelId channel_id, bool from_binlog, bool from_database) {
  CHECK(c != nullptr);
  if (c->is_photo_changed) {
    td_->messages_manager_->on_dialog_photo_updated(DialogId(channel_id));
    drop_channel_photos(channel_id, !c->photo.small_file_id.is_valid(), true, "update_channel");
    c->is_photo_changed = false;
  }
  if (c->is_title_changed) {
    td_->messages_manager_->on_dialog_title_updated(DialogId(channel_id));
    c->is_title_changed = false;
  }
  if (c->is_status_changed) {
    c->status.update_restrictions();
    auto until_date = c->status.get_until_date();
    int32 left_time = 0;
    if (until_date > 0) {
      left_time = until_date - G()->unix_time_cached() + 1;
      CHECK(left_time > 0);
    }
    if (left_time > 0 && left_time < 366 * 86400) {
      channel_unban_timeout_.set_timeout_in(channel_id.get(), left_time);
    } else {
      channel_unban_timeout_.cancel_timeout(channel_id.get());
    }

    if (c->is_megagroup) {
      update_dialogs_for_discussion(DialogId(channel_id), c->status.is_administrator() && c->status.can_pin_messages());
    }
    if (!c->status.is_member()) {
      remove_inactive_channel(channel_id);
    }
    if (!c->status.can_manage_invite_links()) {
      td_->messages_manager_->drop_dialog_pending_join_requests(DialogId(channel_id));
    }
    c->is_status_changed = false;
  }
  if (c->is_username_changed) {
    if (c->status.is_creator()) {
      update_created_public_channels(c, channel_id);
    }
    c->is_username_changed = false;
  }
  if (c->is_default_permissions_changed) {
    td_->messages_manager_->on_dialog_default_permissions_updated(DialogId(channel_id));
    if (c->default_permissions !=
        RestrictedRights(false, false, false, false, false, false, false, false, false, false, false)) {
      remove_dialog_suggested_action(SuggestedAction{SuggestedAction::Type::ConvertToGigagroup, DialogId(channel_id)});
    }
    c->is_default_permissions_changed = false;
  }
  if (c->is_has_location_changed) {
    if (c->status.is_creator()) {
      update_created_public_channels(c, channel_id);
    }
    c->is_has_location_changed = false;
  }
  if (c->is_creator_changed) {
    update_created_public_channels(c, channel_id);
    c->is_creator_changed = false;
  }
  if (c->is_noforwards_changed) {
    td_->messages_manager_->on_dialog_has_protected_content_updated(DialogId(channel_id));
    c->is_noforwards_changed = false;
  }

  if (!td_->auth_manager_->is_bot()) {
    if (c->restriction_reasons.empty()) {
      restricted_channel_ids_.erase(channel_id);
    } else {
      restricted_channel_ids_.insert(channel_id);
    }
  }

  LOG(DEBUG) << "Update " << channel_id << ": need_save_to_database = " << c->need_save_to_database
             << ", is_changed = " << c->is_changed;
  c->need_save_to_database |= c->is_changed;
  if (c->need_save_to_database) {
    if (!from_database) {
      c->is_saved = false;
    }
    c->need_save_to_database = false;
  }
  if (c->is_changed) {
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateSupergroup>(get_supergroup_object(channel_id, c)));
    c->is_changed = false;
    c->is_update_supergroup_sent = true;
  }

  if (!from_database) {
    save_channel(c, channel_id, from_binlog);
  }

  bool have_read_access = have_input_peer_channel(c, channel_id, AccessRights::Read);
  bool is_member = c->status.is_member();
  if (c->had_read_access && !have_read_access) {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_deleted, DialogId(channel_id),
                       Promise<Unit>());
  } else if (!from_database && c->was_member != is_member) {
    DialogId dialog_id(channel_id);
    send_closure_later(G()->messages_manager(), &MessagesManager::force_create_dialog, dialog_id, "update channel",
                       true, true);
  }
  c->had_read_access = have_read_access;
  c->was_member = is_member;

  if (c->cache_version != Channel::CACHE_VERSION && !c->is_repaired &&
      have_input_peer_channel(c, channel_id, AccessRights::Read) && !G()->close_flag()) {
    c->is_repaired = true;

    LOG(INFO) << "Repairing cache of " << channel_id;
    reload_channel(channel_id, Promise<Unit>());
  }
}

class EditChatInviteLinkQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLink>> promise_;
  DialogId dialog_id_;

 public:
  explicit EditChatInviteLinkQuery(Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link, const string &title, int32 expire_date, int32 usage_limit,
            bool creates_join_request) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    int32 flags = telegram_api::messages_editExportedChatInvite::EXPIRE_DATE_MASK |
                  telegram_api::messages_editExportedChatInvite::USAGE_LIMIT_MASK |
                  telegram_api::messages_editExportedChatInvite::REQUEST_NEEDED_MASK |
                  telegram_api::messages_editExportedChatInvite::TITLE_MASK;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_editExportedChatInvite(flags, false /*ignored*/, std::move(input_peer), invite_link,
                                                      expire_date, usage_limit, creates_join_request, title)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editExportedChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditChatInviteLinkQuery: " << to_string(result);

    if (result->get_id() != telegram_api::messages_exportedChatInvite::ID) {
      return on_error(Status::Error(500, "Receive unexpected response from server"));
    }

    auto invite = move_tl_object_as<telegram_api::messages_exportedChatInvite>(result);

    td_->contacts_manager_->on_get_users(std::move(invite->users_), "EditChatInviteLinkQuery");

    DialogInviteLink invite_link(std::move(invite->invite_));
    if (!invite_link.is_valid()) {
      return on_error(Status::Error(500, "Receive invalid invite link"));
    }
    promise_.set_value(invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditChatInviteLinkQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::edit_dialog_invite_link(DialogId dialog_id, const string &invite_link, string title,
                                              int32 expire_date, int32 usage_limit, bool creates_join_request,
                                              Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));
  if (creates_join_request && usage_limit > 0) {
    return promise.set_error(
        Status::Error(400, "Member limit can't be specified for links requiring administrator approval"));
  }

  if (invite_link.empty()) {
    return promise.set_error(Status::Error(400, "Invite link must be non-empty"));
  }

  auto new_title = clean_name(std::move(title), MAX_INVITE_LINK_TITLE_LENGTH);
  td_->create_handler<EditChatInviteLinkQuery>(std::move(promise))
      ->send(dialog_id, invite_link, new_title, expire_date, usage_limit, creates_join_request);
}

class GetExportedChatInviteQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLink>> promise_;
  DialogId dialog_id_;

 public:
  explicit GetExportedChatInviteQuery(Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_getExportedChatInvite(std::move(input_peer), invite_link)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getExportedChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    if (result_ptr.ok()->get_id() != telegram_api::messages_exportedChatInvite::ID) {
      LOG(ERROR) << "Receive wrong result for GetExportedChatInviteQuery: " << to_string(result_ptr.ok());
      return on_error(Status::Error(500, "Receive unexpected response"));
    }

    auto result = move_tl_object_as<telegram_api::messages_exportedChatInvite>(result_ptr.ok_ref());
    LOG(INFO) << "Receive result for GetExportedChatInviteQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetExportedChatInviteQuery");

    DialogInviteLink invite_link(std::move(result->invite_));
    if (!invite_link.is_valid()) {
      LOG(ERROR) << "Receive invalid invite link in " << dialog_id_;
      return on_error(Status::Error(500, "Receive invalid invite link"));
    }
    promise_.set_value(invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetExportedChatInviteQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::get_dialog_invite_link(DialogId dialog_id, const string &invite_link,
                                             Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id, false));

  if (invite_link.empty()) {
    return promise.set_error(Status::Error(400, "Invite link must be non-empty"));
  }

  td_->create_handler<GetExportedChatInviteQuery>(std::move(promise))->send(dialog_id, invite_link);
}

class AddChatUserQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit AddChatUserQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChatId chat_id, tl_object_ptr<telegram_api::InputUser> &&input_user, int32 forward_limit) {
    send_query(G()->net_query_creator().create(
        telegram_api::messages_addChatUser(chat_id.get(), std::move(input_user), forward_limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_addChatUser>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for AddChatUserQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("AddChatUserQuery");
  }
};

void ContactsManager::add_chat_participant(ChatId chat_id, UserId user_id, int32 forward_limit,
                                           Promise<Unit> &&promise) {
  const Chat *c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!c->is_active) {
    return promise.set_error(Status::Error(400, "Chat is deactivated"));
  }
  if (forward_limit < 0) {
    return promise.set_error(Status::Error(400, "Can't forward negative number of messages"));
  }
  if (user_id != get_my_id()) {
    if (!get_chat_permissions(c).can_invite_users()) {
      return promise.set_error(Status::Error(400, "Not enough rights to invite members to the group chat"));
    }
  } else if (c->status.is_banned()) {
    return promise.set_error(Status::Error(400, "User was kicked from the chat"));
  }
  // TODO upper bound on forward_limit

  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  // TODO invoke after
  td_->create_handler<AddChatUserQuery>(std::move(promise))->send(chat_id, r_input_user.move_as_ok(), forward_limit);
}

class GetFullUserQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetFullUserQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(tl_object_ptr<telegram_api::InputUser> &&input_user) {
    send_query(G()->net_query_creator().create(telegram_api::users_getFullUser(std::move(input_user))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::users_getFullUser>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for GetFullUserQuery: " << to_string(ptr);
    td_->contacts_manager_->on_get_users(std::move(ptr->users_), "GetFullUserQuery");
    td_->contacts_manager_->on_get_chats(std::move(ptr->chats_), "GetFullUserQuery");
    td_->contacts_manager_->on_get_user_full(std::move(ptr->full_user_));
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_get_user_full_query(UserId user_id, tl_object_ptr<telegram_api::InputUser> &&input_user,
                                               Promise<Unit> &&promise, const char *source) {
  LOG(INFO) << "Get full " << user_id << " from " << source;
  auto send_query =
      PromiseCreator::lambda([td = td_, input_user = std::move(input_user)](Result<Promise<Unit>> &&promise) mutable {
        if (promise.is_ok() && !G()->close_flag()) {
          td->create_handler<GetFullUserQuery>(promise.move_as_ok())->send(std::move(input_user));
        }
      });
  get_user_full_queries_.add_query(user_id.get(), std::move(send_query), std::move(promise));
}

class MigrateChatQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit MigrateChatQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChatId chat_id) {
    send_query(G()->net_query_creator().create(telegram_api::messages_migrateChat(chat_id.get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_migrateChat>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for MigrateChatQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("MigrateChatQuery");
  }
};

ChannelId ContactsManager::migrate_chat_to_megagroup(ChatId chat_id, Promise<Unit> &promise) {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    promise.set_error(Status::Error(400, "Chat info not found"));
    return ChannelId();
  }

  if (!c->status.is_creator()) {
    promise.set_error(Status::Error(400, "Need creator rights in the chat"));
    return ChannelId();
  }

  if (c->migrated_to_channel_id.is_valid()) {
    return c->migrated_to_channel_id;
  }

  td_->create_handler<MigrateChatQuery>(std::move(promise))->send(chat_id);
  return ChannelId();
}

void ContactsManager::on_update_chat_status(Chat *c, ChatId chat_id, DialogParticipantStatus status) {
  if (c->status != status) {
    LOG(INFO) << "Update " << chat_id << " status from " << c->status << " to " << status;
    bool need_reload_group_call = c->status.can_manage_calls() != status.can_manage_calls();
    bool need_drop_invite_link = c->status.can_manage_invite_links() && !status.can_manage_invite_links();

    c->status = std::move(status);
    c->is_status_changed = true;

    if (c->status.is_left()) {
      c->participant_count = 0;
      c->version = -1;
      c->default_permissions_version = -1;
      c->pinned_message_version = -1;

      drop_chat_full(chat_id);
    } else if (need_drop_invite_link) {
      ChatFull *chat_full = get_chat_full_force(chat_id, "on_update_chat_status");
      if (chat_full != nullptr) {
        on_update_chat_full_invite_link(chat_full, nullptr);
        update_chat_full(chat_full, chat_id, "on_update_chat_status");
      }
    }
    if (need_reload_group_call) {
      send_closure_later(G()->messages_manager(), &MessagesManager::on_update_dialog_group_call_rights,
                         DialogId(chat_id));
    }

    c->is_changed = true;
  }
}

std::pair<vector<UserId>, vector<int32>> ContactsManager::import_contacts(const vector<Contact> &contacts,
                                                                          int64 &random_id, Promise<Unit> &&promise) {
  if (!are_contacts_loaded_) {
    load_contacts(std::move(promise));
    return {};
  }

  LOG(INFO) << "Asked to import " << contacts.size() << " contacts with random_id = " << random_id;
  if (random_id != 0) {
    // request has already been sent before
    auto it = imported_contacts_.find(random_id);
    CHECK(it != imported_contacts_.end());
    auto result = std::move(it->second);
    imported_contacts_.erase(it);

    promise.set_value(Unit());
    return result;
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || imported_contacts_.find(random_id) != imported_contacts_.end());
  imported_contacts_[random_id];  // reserve place for result

  do_import_contacts(contacts, random_id, std::move(promise));
  return {};
}

td_api::object_ptr<td_api::CheckChatUsernameResult> ContactsManager::get_check_chat_username_result_object(
    CheckDialogUsernameResult result) {
  switch (result) {
    case CheckDialogUsernameResult::Ok:
      return td_api::make_object<td_api::checkChatUsernameResultOk>();
    case CheckDialogUsernameResult::Invalid:
      return td_api::make_object<td_api::checkChatUsernameResultUsernameInvalid>();
    case CheckDialogUsernameResult::Occupied:
      return td_api::make_object<td_api::checkChatUsernameResultUsernameOccupied>();
    case CheckDialogUsernameResult::PublicDialogsTooMuch:
      return td_api::make_object<td_api::checkChatUsernameResultPublicChatsTooMuch>();
    case CheckDialogUsernameResult::PublicGroupsUnavailable:
      return td_api::make_object<td_api::checkChatUsernameResultPublicGroupsUnavailable>();
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void ContactsManager::load_user_from_database_impl(UserId user_id, Promise<Unit> promise) {
  LOG(INFO) << "Load " << user_id << " from database";
  auto &load_user_queries = load_user_from_database_queries_[user_id];
  load_user_queries.push_back(std::move(promise));
  if (load_user_queries.size() == 1u) {
    G()->td_db()->get_sqlite_pmc()->get(get_user_database_key(user_id), PromiseCreator::lambda([user_id](string value) {
                                          send_closure(G()->contacts_manager(),
                                                       &ContactsManager::on_load_user_from_database, user_id,
                                                       std::move(value), false);
                                        }));
  }
}

void ContactsManager::load_statistics_graph(DialogId dialog_id, string token, int64 x,
                                            Promise<td_api::object_ptr<td_api::StatisticalGraph>> &&promise) {
  auto dc_id_promise = PromiseCreator::lambda([actor_id = actor_id(this), token = std::move(token), x,
                                               promise = std::move(promise)](Result<DcId> r_dc_id) mutable {
    if (r_dc_id.is_error()) {
      return promise.set_error(r_dc_id.move_as_error());
    }
    send_closure(actor_id, &ContactsManager::send_load_async_graph_query, r_dc_id.move_as_ok(), std::move(token), x,
                 std::move(promise));
  });
  get_channel_statistics_dc_id(dialog_id, false, std::move(dc_id_promise));
}

void ContactsManager::on_channel_username_changed(const Channel *c, ChannelId channel_id, const string &old_username,
                                                  const string &new_username) {
  bool have_channel_full = get_channel_full(channel_id) != nullptr;
  if (old_username.empty() || new_username.empty()) {
    // moving channel from private to public can change availability of chat members
    invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
  }

  // must not load ChannelFull, because must not change the Channel
  CHECK(have_channel_full == (get_channel_full(channel_id) != nullptr));
}

void ContactsManager::save_chat_to_database_impl(Chat *c, ChatId chat_id, string value) {
  CHECK(c != nullptr);
  CHECK(load_chat_from_database_queries_.count(chat_id) == 0);
  CHECK(!c->is_being_saved);
  c->is_being_saved = true;
  c->is_saved = true;
  LOG(INFO) << "Trying to save to database " << chat_id;
  G()->td_db()->get_sqlite_pmc()->set(
      get_chat_database_key(chat_id), std::move(value), PromiseCreator::lambda([chat_id](Result<> result) {
        send_closure(G()->contacts_manager(), &ContactsManager::on_save_chat_to_database, chat_id, result.is_ok());
      }));
}

td_api::object_ptr<td_api::updateSupergroup> ContactsManager::get_update_unknown_supergroup_object(
    ChannelId channel_id) const {
  auto min_channel = get_min_channel(channel_id);
  return td_api::make_object<td_api::updateSupergroup>(td_api::make_object<td_api::supergroup>(
      channel_id.get(), string(), 0, DialogParticipantStatus::Banned(0).get_chat_member_status_object(), 0, false,
      false, false, false, min_channel == nullptr ? true : !min_channel->is_megagroup_, false, false, string(), false,
      false));
}

void ContactsManager::on_get_contacts_finished(size_t expected_contact_count) {
  LOG(INFO) << "Finished to get " << contacts_hints_.size() << " contacts out of expected " << expected_contact_count;
  are_contacts_loaded_ = true;
  auto promises = std::move(load_contacts_queries_);
  load_contacts_queries_.clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
  if (expected_contact_count != contacts_hints_.size()) {
    save_contacts_to_database();
  }
}

int64 ContactsManager::get_basic_group_id_object(ChatId chat_id, const char *source) const {
  if (chat_id.is_valid() && get_chat(chat_id) == nullptr && unknown_chats_.count(chat_id) == 0) {
    LOG(ERROR) << "Have no info about " << chat_id << " from " << source;
    unknown_chats_.insert(chat_id);
    send_closure(G()->td(), &Td::send_update, get_update_unknown_basic_group_object(chat_id));
  }
  return chat_id.get();
}

void ContactsManager::save_user_full(const UserFull *user_full, UserId user_id) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }

  LOG(INFO) << "Trying to save to database full " << user_id;
  CHECK(user_full != nullptr);
  G()->td_db()->get_sqlite_pmc()->set(get_user_full_database_key(user_id), get_user_full_database_value(user_full),
                                      Auto());
}

int32 ContactsManager::get_channel_slow_mode_delay(ChannelId channel_id) {
  auto channel_full = get_channel_full_const(channel_id);
  if (channel_full == nullptr) {
    channel_full = get_channel_full_force(channel_id, false, "get_channel_slow_mode_delay");
    if (channel_full == nullptr) {
      return 0;
    }
  }
  return channel_full->slow_mode_delay;
}

DialogParticipantStatus ContactsManager::get_channel_permissions(const Channel *c) const {
  c->status.update_restrictions();
  if (!c->is_megagroup) {
    // there is no restrictions in broadcast channels
    return c->status;
  }
  return c->status.apply_restrictions(c->default_permissions, td_->auth_manager_->is_bot());
}

void ContactsManager::save_next_contacts_sync_date() {
  if (G()->close_flag()) {
    return;
  }
  if (!G()->parameters().use_chat_info_db) {
    return;
  }
  G()->td_db()->get_binlog_pmc()->set("next_contacts_sync_date", to_string(next_contacts_sync_date_));
}

UserId ContactsManager::get_me(Promise<Unit> &&promise) {
  auto my_id = get_my_id();
  if (!have_user_force(my_id)) {
    send_get_me_query(td_, std::move(promise));
    return UserId();
  }

  promise.set_value(Unit());
  return my_id;
}

bool ContactsManager::have_input_peer_user(UserId user_id, AccessRights access_rights) const {
  if (user_id == get_my_id()) {
    return true;
  }
  return have_input_peer_user(get_user(user_id), access_rights);
}

bool ContactsManager::is_update_about_username_change_received(UserId user_id) const {
  const User *u = get_user(user_id);
  if (u != nullptr) {
    return u->is_contact;
  } else {
    return false;
  }
}

int32 ContactsManager::get_chat_participant_count(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return 0;
  }
  return c->participant_count;
}

void ContactsManager::reload_chat_full(ChatId chat_id, Promise<Unit> &&promise) {
  send_get_chat_full_query(chat_id, std::move(promise), "reload_chat_full");
}

bool ContactsManager::have_channel(ChannelId channel_id) const {
  return channels_.count(channel_id) > 0;
}

void ContactsManager::tear_down() {
  parent_.reset();
}
}  // namespace td
