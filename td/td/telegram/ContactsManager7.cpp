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

#include <algorithm>
#include <limits>
#include <tuple>
#include <utility>

namespace td {
}  // namespace td
namespace td {

class CheckChatInviteQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  string invite_link_;

 public:
  explicit CheckChatInviteQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &invite_link) {
    invite_link_ = invite_link;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_checkChatInvite(LinkManager::get_dialog_invite_link_hash(invite_link_))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_checkChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for CheckChatInviteQuery: " << to_string(ptr);

    td_->contacts_manager_->on_get_dialog_invite_link_info(invite_link_, std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::check_dialog_invite_link(const string &invite_link, Promise<Unit> &&promise) const {
  if (invite_link_infos_.count(invite_link) > 0) {
    return promise.set_value(Unit());
  }

  if (!DialogInviteLink::is_valid_invite_link(invite_link)) {
    return promise.set_error(Status::Error(400, "Wrong invite link"));
  }

  td_->create_handler<CheckChatInviteQuery>(std::move(promise))->send(invite_link);
}

void ContactsManager::on_get_dialog_invite_link_info(const string &invite_link,
                                                     tl_object_ptr<telegram_api::ChatInvite> &&chat_invite_ptr,
                                                     Promise<Unit> &&promise) {
  CHECK(chat_invite_ptr != nullptr);
  switch (chat_invite_ptr->get_id()) {
    case telegram_api::chatInviteAlready::ID:
    case telegram_api::chatInvitePeek::ID: {
      telegram_api::object_ptr<telegram_api::Chat> chat = nullptr;
      int32 accessible_before = 0;
      if (chat_invite_ptr->get_id() == telegram_api::chatInviteAlready::ID) {
        auto chat_invite_already = move_tl_object_as<telegram_api::chatInviteAlready>(chat_invite_ptr);
        chat = std::move(chat_invite_already->chat_);
      } else {
        auto chat_invite_peek = move_tl_object_as<telegram_api::chatInvitePeek>(chat_invite_ptr);
        chat = std::move(chat_invite_peek->chat_);
        accessible_before = chat_invite_peek->expires_;
      }
      auto chat_id = get_chat_id(chat);
      if (chat_id != ChatId() && !chat_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << chat_id;
        chat_id = ChatId();
      }
      auto channel_id = get_channel_id(chat);
      if (channel_id != ChannelId() && !channel_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << channel_id;
        channel_id = ChannelId();
      }
      if (!channel_id.is_valid() || accessible_before < 0) {
        LOG(ERROR) << "Receive expires = " << accessible_before << " for invite link " << invite_link << " to "
                   << to_string(chat);
        accessible_before = 0;
      }
      on_get_chat(std::move(chat), "chatInviteAlready");

      CHECK(chat_id == ChatId() || channel_id == ChannelId());

      // the access is already expired, reget the info
      if (accessible_before != 0 && accessible_before <= G()->unix_time() + 1) {
        td_->create_handler<CheckChatInviteQuery>(std::move(promise))->send(invite_link);
        return;
      }

      DialogId dialog_id = chat_id.is_valid() ? DialogId(chat_id) : DialogId(channel_id);
      auto &invite_link_info = invite_link_infos_[invite_link];
      if (invite_link_info == nullptr) {
        invite_link_info = make_unique<InviteLinkInfo>();
      }
      invite_link_info->dialog_id = dialog_id;
      if (accessible_before != 0) {
        auto &access = dialog_access_by_invite_link_[dialog_id];
        access.invite_links.insert(invite_link);
        if (access.accessible_before < accessible_before) {
          access.accessible_before = accessible_before;

          auto expires_in = accessible_before - G()->unix_time() - 1;
          invite_link_info_expire_timeout_.set_timeout_in(dialog_id.get(), expires_in);
        }
      }
      break;
    }
    case telegram_api::chatInvite::ID: {
      auto chat_invite = move_tl_object_as<telegram_api::chatInvite>(chat_invite_ptr);
      vector<UserId> participant_user_ids;
      for (auto &user : chat_invite->participants_) {
        auto user_id = get_user_id(user);
        if (!user_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << user_id;
          continue;
        }

        on_get_user(std::move(user), "chatInvite");
        participant_user_ids.push_back(user_id);
      }

      auto &invite_link_info = invite_link_infos_[invite_link];
      if (invite_link_info == nullptr) {
        invite_link_info = make_unique<InviteLinkInfo>();
      }
      invite_link_info->dialog_id = DialogId();
      invite_link_info->title = chat_invite->title_;
      invite_link_info->photo = get_photo(td_->file_manager_.get(), std::move(chat_invite->photo_), DialogId());
      invite_link_info->description = std::move(chat_invite->about_);
      invite_link_info->participant_count = chat_invite->participants_count_;
      invite_link_info->participant_user_ids = std::move(participant_user_ids);
      invite_link_info->creates_join_request = std::move(chat_invite->request_needed_);
      invite_link_info->is_chat = (chat_invite->flags_ & CHAT_INVITE_FLAG_IS_CHANNEL) == 0;
      invite_link_info->is_channel = (chat_invite->flags_ & CHAT_INVITE_FLAG_IS_CHANNEL) != 0;

      bool is_broadcast = (chat_invite->flags_ & CHAT_INVITE_FLAG_IS_BROADCAST) != 0;
      bool is_public = (chat_invite->flags_ & CHAT_INVITE_FLAG_IS_PUBLIC) != 0;
      bool is_megagroup = (chat_invite->flags_ & CHAT_INVITE_FLAG_IS_MEGAGROUP) != 0;

      if (!invite_link_info->is_channel) {
        if (is_broadcast || is_public || is_megagroup) {
          LOG(ERROR) << "Receive wrong chat invite: " << to_string(chat_invite);
          is_public = is_megagroup = false;
        }
      } else {
        LOG_IF(ERROR, is_broadcast == is_megagroup) << "Receive wrong chat invite: " << to_string(chat_invite);
      }

      invite_link_info->is_public = is_public;
      invite_link_info->is_megagroup = is_megagroup;
      break;
    }
    default:
      UNREACHABLE();
  }
  promise.set_value(Unit());
}

class GetExportedChatInvitesQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLinks>> promise_;
  DialogId dialog_id_;

 public:
  explicit GetExportedChatInvitesQuery(Promise<td_api::object_ptr<td_api::chatInviteLinks>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, UserId creator_user_id, bool is_revoked, int32 offset_date,
            const string &offset_invite_link, int32 limit) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    auto r_input_user = td_->contacts_manager_->get_input_user(creator_user_id);
    CHECK(r_input_user.is_ok());

    int32 flags = 0;
    if (!offset_invite_link.empty() || offset_date != 0) {
      flags |= telegram_api::messages_getExportedChatInvites::OFFSET_DATE_MASK;
      flags |= telegram_api::messages_getExportedChatInvites::OFFSET_LINK_MASK;
    }
    if (is_revoked) {
      flags |= telegram_api::messages_getExportedChatInvites::REVOKED_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::messages_getExportedChatInvites(
        flags, false /*ignored*/, std::move(input_peer), r_input_user.move_as_ok(), offset_date, offset_invite_link,
        limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getExportedChatInvites>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetExportedChatInvitesQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetExportedChatInvitesQuery");

    int32 total_count = result->count_;
    if (total_count < static_cast<int32>(result->invites_.size())) {
      LOG(ERROR) << "Receive wrong total count of invite links " << total_count << " in " << dialog_id_;
      total_count = static_cast<int32>(result->invites_.size());
    }
    vector<td_api::object_ptr<td_api::chatInviteLink>> invite_links;
    for (auto &invite : result->invites_) {
      DialogInviteLink invite_link(std::move(invite));
      if (!invite_link.is_valid()) {
        LOG(ERROR) << "Receive invalid invite link in " << dialog_id_;
        total_count--;
        continue;
      }
      invite_links.push_back(invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
    }
    promise_.set_value(td_api::make_object<td_api::chatInviteLinks>(total_count, std::move(invite_links)));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetExportedChatInvitesQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::get_dialog_invite_links(DialogId dialog_id, UserId creator_user_id, bool is_revoked,
                                              int32 offset_date, const string &offset_invite_link, int32 limit,
                                              Promise<td_api::object_ptr<td_api::chatInviteLinks>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id, creator_user_id != get_my_id()));

  if (!have_input_user(creator_user_id)) {
    return promise.set_error(Status::Error(400, "Administrator user not found"));
  }

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }

  td_->create_handler<GetExportedChatInvitesQuery>(std::move(promise))
      ->send(dialog_id, creator_user_id, is_revoked, offset_date, offset_invite_link, limit);
}

class UpdateProfileQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  int32 flags_;
  string first_name_;
  string last_name_;
  string about_;

 public:
  explicit UpdateProfileQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(int32 flags, const string &first_name, const string &last_name, const string &about) {
    flags_ = flags;
    first_name_ = first_name;
    last_name_ = last_name;
    about_ = about;
    send_query(
        G()->net_query_creator().create(telegram_api::account_updateProfile(flags, first_name, last_name, about)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_updateProfile>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG(DEBUG) << "Receive result for UpdateProfileQuery: " << to_string(result_ptr.ok());
    td_->contacts_manager_->on_get_user(result_ptr.move_as_ok(), "UpdateProfileQuery");
    td_->contacts_manager_->on_update_profile_success(flags_, first_name_, last_name_, about_);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_name(const string &first_name, const string &last_name, Promise<Unit> &&promise) {
  auto new_first_name = clean_name(first_name, MAX_NAME_LENGTH);
  auto new_last_name = clean_name(last_name, MAX_NAME_LENGTH);
  if (new_first_name.empty()) {
    return promise.set_error(Status::Error(400, "First name must be non-empty"));
  }

  const User *u = get_user(get_my_id());
  int32 flags = 0;
  // TODO we can already send request for changing first_name and last_name and wanting to set initial values
  // TODO need to be rewritten using invoke after and cancelling previous request
  if (u == nullptr || u->first_name != new_first_name) {
    flags |= ACCOUNT_UPDATE_FIRST_NAME;
  }
  if (u == nullptr || u->last_name != new_last_name) {
    flags |= ACCOUNT_UPDATE_LAST_NAME;
  }
  if (flags == 0) {
    return promise.set_value(Unit());
  }

  td_->create_handler<UpdateProfileQuery>(std::move(promise))->send(flags, new_first_name, new_last_name, "");
}

void ContactsManager::set_bio(const string &bio, Promise<Unit> &&promise) {
  auto new_bio = strip_empty_characters(bio, MAX_BIO_LENGTH);
  for (auto &c : new_bio) {
    if (c == '\n') {
      c = ' ';
    }
  }

  const UserFull *user_full = get_user_full(get_my_id());
  int32 flags = 0;
  // TODO we can already send request for changing bio and wanting to set initial values
  // TODO need to be rewritten using invoke after and cancelling previous request
  if (user_full == nullptr || user_full->about != new_bio) {
    flags |= ACCOUNT_UPDATE_ABOUT;
  }
  if (flags == 0) {
    return promise.set_value(Unit());
  }

  td_->create_handler<UpdateProfileQuery>(std::move(promise))->send(flags, "", "", new_bio);
}

class AddContactQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  UserId user_id_;

 public:
  explicit AddContactQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(UserId user_id, tl_object_ptr<telegram_api::InputUser> &&input_user, const Contact &contact,
            bool share_phone_number) {
    user_id_ = user_id;
    int32 flags = 0;
    if (share_phone_number) {
      flags |= telegram_api::contacts_addContact::ADD_PHONE_PRIVACY_EXCEPTION_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::contacts_addContact(flags, false /*ignored*/, std::move(input_user), contact.get_first_name(),
                                          contact.get_last_name(), contact.get_phone_number())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_addContact>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for AddContactQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->contacts_manager_->reload_contacts(true);
    td_->messages_manager_->reget_dialog_action_bar(DialogId(user_id_), "AddContactQuery");
  }
};

void ContactsManager::add_contact(Contact contact, bool share_phone_number, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (!are_contacts_loaded_) {
    load_contacts(PromiseCreator::lambda([actor_id = actor_id(this), contact = std::move(contact), share_phone_number,
                                          promise = std::move(promise)](Result<Unit> &&) mutable {
      send_closure(actor_id, &ContactsManager::add_contact, std::move(contact), share_phone_number, std::move(promise));
    }));
    return;
  }

  LOG(INFO) << "Add " << contact << " with share_phone_number = " << share_phone_number;

  auto user_id = contact.get_user_id();
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  td_->create_handler<AddContactQuery>(std::move(promise))
      ->send(user_id, r_input_user.move_as_ok(), contact, share_phone_number);
}

class HideAllChatJoinRequestsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit HideAllChatJoinRequestsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &invite_link, bool approve) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    int32 flags = 0;
    if (approve) {
      flags |= telegram_api::messages_hideAllChatJoinRequests::APPROVED_MASK;
    }
    if (!invite_link.empty()) {
      flags |= telegram_api::messages_hideAllChatJoinRequests::LINK_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_hideAllChatJoinRequests(flags, false /*ignored*/, std::move(input_peer), invite_link)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_hideAllChatJoinRequests>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for HideAllChatJoinRequestsQuery: " << to_string(result);
    td_->updates_manager_->on_get_updates(std::move(result), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "HideAllChatJoinRequestsQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::process_dialog_join_requests(DialogId dialog_id, const string &invite_link, bool approve,
                                                   Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));
  td_->create_handler<HideAllChatJoinRequestsQuery>(std::move(promise))->send(dialog_id, invite_link, approve);
}

bool ContactsManager::have_input_peer_channel(const Channel *c, ChannelId channel_id, AccessRights access_rights,
                                              bool from_linked) const {
  if (c == nullptr) {
    return false;
  }
  if (access_rights == AccessRights::Know) {
    return true;
  }
  if (c->status.is_administrator()) {
    return true;
  }
  if (c->status.is_banned()) {
    return false;
  }
  if (c->status.is_member()) {
    return true;
  }

  bool is_public = is_channel_public(c);
  if (access_rights == AccessRights::Read) {
    if (is_public) {
      return true;
    }
    if (!from_linked && c->has_linked_channel) {
      auto linked_channel_id = get_linked_channel_id(channel_id);
      if (linked_channel_id.is_valid() && have_channel(linked_channel_id)) {
        if (have_input_peer_channel(get_channel(linked_channel_id), linked_channel_id, access_rights, true)) {
          return true;
        }
      } else {
        return true;
      }
    }
    if (!from_linked && dialog_access_by_invite_link_.count(DialogId(channel_id))) {
      return true;
    }
  } else {
    if (!from_linked && c->is_megagroup && !td_->auth_manager_->is_bot() && c->has_linked_channel) {
      auto linked_channel_id = get_linked_channel_id(channel_id);
      if (linked_channel_id.is_valid() && (is_public || have_channel(linked_channel_id))) {
        return is_public ||
               have_input_peer_channel(get_channel(linked_channel_id), linked_channel_id, AccessRights::Read, true);
      } else {
        return true;
      }
    }
  }
  return false;
}

void ContactsManager::on_save_user_to_database(UserId user_id, bool success) {
  if (G()->close_flag()) {
    return;
  }

  User *u = get_user(user_id);
  CHECK(u != nullptr);
  LOG_CHECK(u->is_being_saved) << user_id << " " << u->is_saved << " " << u->is_status_saved << " "
                               << load_user_from_database_queries_.count(user_id) << " " << u->is_received << " "
                               << u->is_deleted << " " << u->is_bot << " " << u->need_save_to_database << " "
                               << u->is_changed << " " << u->is_status_changed << " " << u->is_name_changed << " "
                               << u->is_username_changed << " " << u->is_photo_changed << " "
                               << u->is_is_contact_changed << " " << u->is_is_deleted_changed;
  CHECK(load_user_from_database_queries_.count(user_id) == 0);
  u->is_being_saved = false;

  if (!success) {
    LOG(ERROR) << "Failed to save " << user_id << " to database";
    u->is_saved = false;
    u->is_status_saved = false;
  } else {
    LOG(INFO) << "Successfully saved " << user_id << " to database";
  }
  if (u->is_saved && u->is_status_saved) {
    if (u->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), u->log_event_id);
      u->log_event_id = 0;
    }
  } else {
    save_user(u, user_id, u->log_event_id != 0);
  }
}

void ContactsManager::on_update_chat_full_participants(ChatFull *chat_full, ChatId chat_id,
                                                       vector<DialogParticipant> participants, int32 version,
                                                       bool from_update) {
  if (version <= -1) {
    LOG(ERROR) << "Receive members with wrong version " << version << " in " << chat_id;
    return;
  }

  if (version < chat_full->version) {
    // some outdated data
    LOG(WARNING) << "Receive members of " << chat_id << " with version " << version << " but current version is "
                 << chat_full->version;
    return;
  }

  if ((chat_full->participants.size() != participants.size() && version == chat_full->version) ||
      (from_update && version != chat_full->version + 1)) {
    LOG(INFO) << "Members of " << chat_id << " has changed";
    // this is possible in very rare situations
    repair_chat_participants(chat_id);
  }

  chat_full->participants = std::move(participants);
  chat_full->version = version;
  chat_full->is_changed = true;
  update_chat_online_member_count(chat_full, chat_id, true);
}

std::pair<int32, vector<UserId>> ContactsManager::search_contacts(const string &query, int32 limit,
                                                                  Promise<Unit> &&promise) {
  LOG(INFO) << "Search contacts with query = \"" << query << "\" and limit = " << limit;

  if (limit < 0) {
    promise.set_error(Status::Error(400, "Limit must be non-negative"));
    return {};
  }

  if (!are_contacts_loaded_) {
    load_contacts(std::move(promise));
    return {};
  }
  reload_contacts(false);

  std::pair<size_t, vector<int64>> result;
  if (query.empty()) {
    result = contacts_hints_.search_empty(limit);
  } else {
    result = contacts_hints_.search(query, limit);
  }

  vector<UserId> user_ids;
  user_ids.reserve(result.second.size());
  for (auto key : result.second) {
    user_ids.emplace_back(key);
  }

  promise.set_value(Unit());
  return {narrow_cast<int32>(result.first), std::move(user_ids)};
}

void ContactsManager::save_secret_chat_to_database_impl(SecretChat *c, SecretChatId secret_chat_id, string value) {
  CHECK(c != nullptr);
  CHECK(load_secret_chat_from_database_queries_.count(secret_chat_id) == 0);
  CHECK(!c->is_being_saved);
  c->is_being_saved = true;
  c->is_saved = true;
  LOG(INFO) << "Trying to save to database " << secret_chat_id;
  G()->td_db()->get_sqlite_pmc()->set(get_secret_chat_database_key(secret_chat_id), std::move(value),
                                      PromiseCreator::lambda([secret_chat_id](Result<> result) {
                                        send_closure(G()->contacts_manager(),
                                                     &ContactsManager::on_save_secret_chat_to_database, secret_chat_id,
                                                     result.is_ok());
                                      }));
}

void ContactsManager::drop_channel_photos(ChannelId channel_id, bool is_empty, bool drop_channel_full_photo,
                                          const char *source) {
  if (drop_channel_full_photo) {
    auto channel_full = get_channel_full(channel_id, true, "drop_channel_photos");  // must not load ChannelFull
    if (channel_full == nullptr) {
      return;
    }

    on_update_channel_full_photo(channel_full, channel_id, Photo());
    if (!is_empty) {
      if (channel_full->expires_at > 0.0) {
        channel_full->expires_at = 0.0;
        channel_full->need_save_to_database = true;
      }
      send_get_channel_full_query(channel_full, channel_id, Auto(), "drop_channel_photos");
    }
    update_channel_full(channel_full, channel_id, "drop_channel_photos");
  }
}

void ContactsManager::on_save_chat_to_database(ChatId chat_id, bool success) {
  if (G()->close_flag()) {
    return;
  }

  Chat *c = get_chat(chat_id);
  CHECK(c != nullptr);
  CHECK(c->is_being_saved);
  CHECK(load_chat_from_database_queries_.count(chat_id) == 0);
  c->is_being_saved = false;

  if (!success) {
    LOG(ERROR) << "Failed to save " << chat_id << " to database";
    c->is_saved = false;
  } else {
    LOG(INFO) << "Successfully saved " << chat_id << " to database";
  }
  if (c->is_saved) {
    if (c->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), c->log_event_id);
      c->log_event_id = 0;
    }
  } else {
    save_chat(c, chat_id, c->log_event_id != 0);
  }
}

ContactsManager::Channel *ContactsManager::get_channel_force(ChannelId channel_id) {
  if (!channel_id.is_valid()) {
    return nullptr;
  }

  Channel *c = get_channel(channel_id);
  if (c != nullptr) {
    return c;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (loaded_from_database_channels_.count(channel_id)) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load " << channel_id << " from database";
  on_load_channel_from_database(channel_id,
                                G()->td_db()->get_sqlite_sync_pmc()->get(get_channel_database_key(channel_id)), true);
  return get_channel(channel_id);
}

int64 ContactsManager::get_supergroup_id_object(ChannelId channel_id, const char *source) const {
  if (channel_id.is_valid() && get_channel(channel_id) == nullptr && unknown_channels_.count(channel_id) == 0) {
    if (have_min_channel(channel_id)) {
      LOG(INFO) << "Have only min " << channel_id << " received from " << source;
    } else {
      LOG(ERROR) << "Have no info about " << channel_id << " received from " << source;
    }
    unknown_channels_.insert(channel_id);
    send_closure(G()->td(), &Td::send_update, get_update_unknown_supergroup_object(channel_id));
  }
  return channel_id.get();
}

tl_object_ptr<td_api::SecretChatState> ContactsManager::get_secret_chat_state_object(SecretChatState state) {
  switch (state) {
    case SecretChatState::Waiting:
      return make_tl_object<td_api::secretChatStatePending>();
    case SecretChatState::Active:
      return make_tl_object<td_api::secretChatStateReady>();
    case SecretChatState::Closed:
    case SecretChatState::Unknown:
      return make_tl_object<td_api::secretChatStateClosed>();
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void ContactsManager::on_update_channel_username(Channel *c, ChannelId channel_id, string &&username) {
  td_->messages_manager_->on_dialog_username_updated(DialogId(channel_id), c->username, username);
  if (c->username != username) {
    if (c->is_update_supergroup_sent) {
      on_channel_username_changed(c, channel_id, c->username, username);
    }

    c->username = std::move(username);
    c->is_username_changed = true;
    c->is_changed = true;
  }
}

void ContactsManager::on_update_user_is_blocked(UserId user_id, bool is_blocked) {
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  UserFull *user_full = get_user_full_force(user_id);
  if (user_full == nullptr || user_full->is_blocked == is_blocked) {
    return;
  }
  on_update_user_full_is_blocked(user_full, user_id, is_blocked);
  update_user_full(user_full, user_id, "on_update_user_is_blocked");
}

void ContactsManager::on_user_online_timeout_callback(void *contacts_manager_ptr, int64 user_id_long) {
  if (G()->close_flag()) {
    return;
  }

  auto contacts_manager = static_cast<ContactsManager *>(contacts_manager_ptr);
  send_closure_later(contacts_manager->actor_id(contacts_manager), &ContactsManager::on_user_online_timeout,
                     UserId(user_id_long));
}

tl_object_ptr<telegram_api::InputPeer> ContactsManager::get_input_peer_chat(ChatId chat_id,
                                                                            AccessRights access_rights) const {
  auto c = get_chat(chat_id);
  if (!have_input_peer_chat(c, access_rights)) {
    return nullptr;
  }

  return make_tl_object<telegram_api::inputPeerChat>(chat_id.get());
}

void ContactsManager::load_chat_from_database(Chat *c, ChatId chat_id, Promise<Unit> promise) {
  if (loaded_from_database_chats_.count(chat_id)) {
    promise.set_value(Unit());
    return;
  }

  CHECK(c == nullptr || !c->is_being_saved);
  load_chat_from_database_impl(chat_id, std::move(promise));
}

void ContactsManager::for_each_secret_chat_with_user(UserId user_id, const std::function<void(SecretChatId)> &f) {
  auto it = secret_chats_with_user_.find(user_id);
  if (it != secret_chats_with_user_.end()) {
    for (auto secret_chat_id : it->second) {
      f(secret_chat_id);
    }
  }
}

ContactsManager::SecretChat *ContactsManager::get_secret_chat(SecretChatId secret_chat_id) {
  auto it = secret_chats_.find(secret_chat_id);
  if (it == secret_chats_.end()) {
    return nullptr;
  }
  return it->second.get();
}

ContactsManager::Channel *ContactsManager::get_channel(ChannelId channel_id) {
  auto p = channels_.find(channel_id);
  if (p == channels_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

const ContactsManager::User *ContactsManager::get_user(UserId user_id) const {
  auto p = users_.find(user_id);
  if (p == users_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

string ContactsManager::get_channel_username(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return string();
  }
  return c->username;
}

bool ContactsManager::is_user_support(UserId user_id) const {
  auto u = get_user(user_id);
  return u != nullptr && !u->is_deleted && u->is_support;
}

string ContactsManager::get_user_full_database_key(UserId user_id) {
  return PSTRING() << "usf" << user_id.get();
}
}  // namespace td
