//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/ContactsManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ConfigShared.h"
#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogInviteLink.h"
#include "td/telegram/DialogLocation.h"


#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/Global.h"

#include "td/telegram/InlineQueriesManager.h"
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






namespace td {
}  // namespace td
namespace td {

class ImportContactsQuery final : public Td::ResultHandler {
  int64 random_id_ = 0;
  size_t sent_size_ = 0;

 public:
  void send(vector<tl_object_ptr<telegram_api::inputPhoneContact>> &&input_phone_contacts, int64 random_id) {
    random_id_ = random_id;
    sent_size_ = input_phone_contacts.size();
    send_query(G()->net_query_creator().create(telegram_api::contacts_importContacts(std::move(input_phone_contacts))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_importContacts>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ImportContactsQuery: " << to_string(ptr);
    if (sent_size_ == ptr->retry_contacts_.size()) {
      return on_error(Status::Error(429, "Too Many Requests: retry after 3600"));
    }
    td_->contacts_manager_->on_imported_contacts(random_id_, std::move(ptr));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_imported_contacts(random_id_, std::move(status));
  }
};

void ContactsManager::do_import_contacts(vector<Contact> contacts, int64 random_id, Promise<Unit> &&promise) {
  size_t size = contacts.size();
  if (size == 0) {
    on_import_contacts_finished(random_id, {}, {});
    return promise.set_value(Unit());
  }

  vector<tl_object_ptr<telegram_api::inputPhoneContact>> input_phone_contacts;
  input_phone_contacts.reserve(size);
  for (size_t i = 0; i < size; i++) {
    input_phone_contacts.push_back(contacts[i].get_input_phone_contact(static_cast<int64>(i)));
  }

  auto task = make_unique<ImportContactsTask>();
  task->promise_ = std::move(promise);
  task->input_contacts_ = std::move(contacts);
  task->imported_user_ids_.resize(size);
  task->unimported_contact_invites_.resize(size);

  bool is_added = import_contact_tasks_.emplace(random_id, std::move(task)).second;
  CHECK(is_added);

  td_->create_handler<ImportContactsQuery>()->send(std::move(input_phone_contacts), random_id);
}

void ContactsManager::on_imported_contacts(int64 random_id,
                                           Result<tl_object_ptr<telegram_api::contacts_importedContacts>> result) {
  auto it = import_contact_tasks_.find(random_id);
  CHECK(it != import_contact_tasks_.end());
  CHECK(it->second != nullptr);

  auto task = it->second.get();
  if (result.is_error()) {
    auto promise = std::move(task->promise_);
    import_contact_tasks_.erase(it);
    return promise.set_error(result.move_as_error());
  }

  auto imported_contacts = result.move_as_ok();
  on_get_users(std::move(imported_contacts->users_), "on_imported_contacts");

  for (auto &imported_contact : imported_contacts->imported_) {
    int64 client_id = imported_contact->client_id_;
    if (client_id < 0 || client_id >= static_cast<int64>(task->imported_user_ids_.size())) {
      LOG(ERROR) << "Wrong client_id " << client_id << " returned";
      continue;
    }

    task->imported_user_ids_[static_cast<size_t>(client_id)] = UserId(imported_contact->user_id_);
  }
  for (auto &popular_contact : imported_contacts->popular_invites_) {
    int64 client_id = popular_contact->client_id_;
    if (client_id < 0 || client_id >= static_cast<int64>(task->unimported_contact_invites_.size())) {
      LOG(ERROR) << "Wrong client_id " << client_id << " returned";
      continue;
    }
    if (popular_contact->importers_ < 0) {
      LOG(ERROR) << "Wrong number of importers " << popular_contact->importers_ << " returned";
      continue;
    }

    task->unimported_contact_invites_[static_cast<size_t>(client_id)] = popular_contact->importers_;
  }

  if (!imported_contacts->retry_contacts_.empty()) {
    auto total_size = static_cast<int64>(task->input_contacts_.size());
    vector<tl_object_ptr<telegram_api::inputPhoneContact>> input_phone_contacts;
    input_phone_contacts.reserve(imported_contacts->retry_contacts_.size());
    for (auto &client_id : imported_contacts->retry_contacts_) {
      if (client_id < 0 || client_id >= total_size) {
        LOG(ERROR) << "Wrong client_id " << client_id << " returned";
        continue;
      }
      auto i = static_cast<size_t>(client_id);
      input_phone_contacts.push_back(task->input_contacts_[i].get_input_phone_contact(client_id));
    }
    td_->create_handler<ImportContactsQuery>()->send(std::move(input_phone_contacts), random_id);
    return;
  }

  auto promise = std::move(task->promise_);
  on_import_contacts_finished(random_id, std::move(task->imported_user_ids_),
                              std::move(task->unimported_contact_invites_));
  import_contact_tasks_.erase(it);
  promise.set_value(Unit());
}

void ContactsManager::update_user(User *u, UserId user_id, bool from_binlog, bool from_database) {
  CHECK(u != nullptr);
  if (u->is_name_changed || u->is_username_changed || u->is_is_contact_changed) {
    update_contacts_hints(u, user_id, from_database);
    u->is_username_changed = false;
  }
  if (u->is_is_contact_changed) {
    td_->messages_manager_->on_dialog_user_is_contact_updated(DialogId(user_id), u->is_contact);
    if (is_user_contact(u, user_id, false)) {
      auto user_full = get_user_full(user_id);
      if (user_full != nullptr && user_full->need_phone_number_privacy_exception) {
        on_update_user_full_need_phone_number_privacy_exception(user_full, user_id, false);
        update_user_full(user_full, user_id, "update_user");
      }
    }
    u->is_is_contact_changed = false;
  }
  if (u->is_is_deleted_changed) {
    td_->messages_manager_->on_dialog_user_is_deleted_updated(DialogId(user_id), u->is_deleted);
    if (u->is_deleted) {
      auto user_full = get_user_full(user_id);  // must not load user_full from database before sending updateUser
      if (user_full != nullptr) {
        drop_user_full(user_id);
      }
    }
    u->is_is_deleted_changed = false;
  }
  if (u->is_name_changed) {
    auto messages_manager = td_->messages_manager_.get();
    messages_manager->on_dialog_title_updated(DialogId(user_id));
    for_each_secret_chat_with_user(user_id, [messages_manager](SecretChatId secret_chat_id) {
      messages_manager->on_dialog_title_updated(DialogId(secret_chat_id));
    });
    u->is_name_changed = false;
  }
  if (u->is_photo_changed) {
    auto messages_manager = td_->messages_manager_.get();
    messages_manager->on_dialog_photo_updated(DialogId(user_id));
    for_each_secret_chat_with_user(user_id, [messages_manager](SecretChatId secret_chat_id) {
      messages_manager->on_dialog_photo_updated(DialogId(secret_chat_id));
    });
    u->is_photo_changed = false;
  }
  if (u->is_status_changed && user_id != get_my_id()) {
    auto left_time = get_user_was_online(u, user_id) - G()->server_time_cached();
    if (left_time >= 0 && left_time < 30 * 86400) {
      left_time += 2.0;  // to guarantee expiration
      LOG(DEBUG) << "Set online timeout for " << user_id << " in " << left_time;
      user_online_timeout_.set_timeout_in(user_id.get(), left_time);
    } else {
      LOG(DEBUG) << "Cancel online timeout for " << user_id;
      user_online_timeout_.cancel_timeout(user_id.get());
    }
  }
  if (!td_->auth_manager_->is_bot()) {
    if (u->restriction_reasons.empty()) {
      restricted_user_ids_.erase(user_id);
    } else {
      restricted_user_ids_.insert(user_id);
    }
  }

  if (u->is_deleted) {
    td_->inline_queries_manager_->remove_recent_inline_bot(user_id, Promise<>());
  }

  LOG(DEBUG) << "Update " << user_id << ": need_save_to_database = " << u->need_save_to_database
             << ", is_changed = " << u->is_changed << ", is_status_changed = " << u->is_status_changed;
  u->need_save_to_database |= u->is_changed;
  if (u->need_save_to_database) {
    if (!from_database) {
      u->is_saved = false;
    }
    u->need_save_to_database = false;
  }
  if (u->is_changed) {
    send_closure(G()->td(), &Td::send_update, make_tl_object<td_api::updateUser>(get_user_object(user_id, u)));
    u->is_changed = false;
    u->is_status_changed = false;
    u->is_update_user_sent = true;
  }
  if (u->is_status_changed) {
    if (!from_database) {
      u->is_status_saved = false;
    }
    CHECK(u->is_update_user_sent);
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateUserStatus>(user_id.get(), get_user_status_object(user_id, u)));
    u->is_status_changed = false;
  }
  if (u->is_online_status_changed) {
    update_user_online_member_count(u);
    u->is_online_status_changed = false;
  }

  if (!from_database) {
    save_user(u, user_id, from_binlog);
  }

  if (u->cache_version != User::CACHE_VERSION && !u->is_repaired && have_input_peer_user(u, AccessRights::Read) &&
      !G()->close_flag()) {
    u->is_repaired = true;

    LOG(INFO) << "Repairing cache of " << user_id;
    reload_user(user_id, Promise<Unit>());
  }
}

class EditChatAboutQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  string about_;

  void on_success() {
    switch (dialog_id_.get_type()) {
      case DialogType::Chat:
        return td_->contacts_manager_->on_update_chat_description(dialog_id_.get_chat_id(), std::move(about_));
      case DialogType::Channel:
        return td_->contacts_manager_->on_update_channel_description(dialog_id_.get_channel_id(), std::move(about_));
      case DialogType::User:
      case DialogType::SecretChat:
      case DialogType::None:
        UNREACHABLE();
    }
  }

 public:
  explicit EditChatAboutQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &about) {
    dialog_id_ = dialog_id;
    about_ = about;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }
    send_query(G()->net_query_creator().create(telegram_api::messages_editChatAbout(std::move(input_peer), about)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editChatAbout>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG(DEBUG) << "Receive result for EditChatAboutQuery: " << result;
    if (!result) {
      return on_error(Status::Error(500, "Chat description is not updated"));
    }

    on_success();
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_ABOUT_NOT_MODIFIED" || status.message() == "CHAT_NOT_MODIFIED") {
      on_success();
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditChatAboutQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_chat_description(ChatId chat_id, const string &description, Promise<Unit> &&promise) {
  auto new_description = strip_empty_characters(description, MAX_DESCRIPTION_LENGTH);
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!get_chat_permissions(c).can_change_info_and_settings()) {
    return promise.set_error(Status::Error(400, "Not enough rights to set chat description"));
  }

  td_->create_handler<EditChatAboutQuery>(std::move(promise))->send(DialogId(chat_id), new_description);
}

void ContactsManager::set_channel_description(ChannelId channel_id, const string &description,
                                              Promise<Unit> &&promise) {
  auto new_description = strip_empty_characters(description, MAX_DESCRIPTION_LENGTH);
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!get_channel_permissions(c).can_change_info_and_settings()) {
    return promise.set_error(Status::Error(400, "Not enough rights to set chat description"));
  }

  td_->create_handler<EditChatAboutQuery>(std::move(promise))->send(DialogId(channel_id), new_description);
}

void ContactsManager::on_channel_status_changed(Channel *c, ChannelId channel_id,
                                                const DialogParticipantStatus &old_status,
                                                const DialogParticipantStatus &new_status) {
  CHECK(c->is_update_supergroup_sent);
  bool have_channel_full = get_channel_full(channel_id) != nullptr;

  bool need_reload_group_call = old_status.can_manage_calls() != new_status.can_manage_calls();
  if (old_status.can_manage_invite_links() && !new_status.can_manage_invite_links()) {
    auto channel_full = get_channel_full(channel_id, true, "on_channel_status_changed");
    if (channel_full != nullptr) {  // otherwise invite_link will be dropped when the channel is loaded
      on_update_channel_full_invite_link(channel_full, nullptr);
      do_invalidate_channel_full(channel_full, channel_id, !c->is_slow_mode_enabled);
      update_channel_full(channel_full, channel_id, "on_channel_status_changed");
    }
  } else {
    invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
  }

  if (old_status.is_creator() != new_status.is_creator()) {
    c->is_creator_changed = true;

    send_get_channel_full_query(nullptr, channel_id, Auto(), "update channel owner");
    reload_dialog_administrators(DialogId(channel_id), {}, Auto());
    remove_dialog_suggested_action(SuggestedAction{SuggestedAction::Type::ConvertToGigagroup, DialogId(channel_id)});
  }

  if (old_status.is_member() != new_status.is_member() || new_status.is_banned()) {
    remove_dialog_access_by_invite_link(DialogId(channel_id));

    if (new_status.is_member() || new_status.is_creator()) {
      reload_channel_full(channel_id,
                          PromiseCreator::lambda([channel_id](Unit) { LOG(INFO) << "Reloaded full " << channel_id; }),
                          "on_channel_status_changed");
    }
  }
  if (need_reload_group_call) {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_update_dialog_group_call_rights,
                       DialogId(channel_id));
  }
  if (td_->auth_manager_->is_bot() && old_status.is_administrator() && !new_status.is_administrator()) {
    channel_participants_.erase(channel_id);
  }
  if (td_->auth_manager_->is_bot() && old_status.is_member() && !new_status.is_member() &&
      !G()->parameters().use_message_db) {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_deleted, DialogId(channel_id),
                       Promise<Unit>());
  }

  // must not load ChannelFull, because must not change the Channel
  CHECK(have_channel_full == (get_channel_full(channel_id) != nullptr));
}

void ContactsManager::on_update_chat_delete_user(ChatId chat_id, UserId user_id, int32 version) {
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id;
    return;
  }
  if (!have_user(user_id)) {
    LOG(ERROR) << "Can't find " << user_id;
    return;
  }
  LOG(INFO) << "Receive updateChatParticipantDelete from " << chat_id << " with " << user_id << " and version "
            << version;

  ChatFull *chat_full = get_chat_full_force(chat_id, "on_update_chat_delete_user");
  if (chat_full == nullptr) {
    LOG(INFO) << "Ignoring update about members of " << chat_id;
    return;
  }
  const Chat *c = get_chat_force(chat_id);
  if (c == nullptr) {
    LOG(ERROR) << "Receive updateChatParticipantDelete for unknown " << chat_id;
    repair_chat_participants(chat_id);
    return;
  }
  if (user_id == get_my_id()) {
    LOG_IF(WARNING, c->status.is_member()) << "User was removed from " << chat_id
                                           << " but it is not left the group. Possible if updates comes out of order";
    return;
  }
  if (c->status.is_left()) {
    // possible if updates come out of order
    LOG(INFO) << "Receive updateChatParticipantDelete for left " << chat_id;

    repair_chat_participants(chat_id);
    return;
  }
  if (on_update_chat_full_participants_short(chat_full, chat_id, version)) {
    for (size_t i = 0; i < chat_full->participants.size(); i++) {
      if (chat_full->participants[i].dialog_id_ == DialogId(user_id)) {
        chat_full->participants[i] = chat_full->participants.back();
        chat_full->participants.resize(chat_full->participants.size() - 1);
        chat_full->is_changed = true;
        update_chat_online_member_count(chat_full, chat_id, false);
        update_chat_full(chat_full, chat_id, "on_update_chat_delete_user");

        if (static_cast<int32>(chat_full->participants.size()) != c->participant_count) {
          repair_chat_participants(chat_id);
        }
        return;
      }
    }
    LOG(ERROR) << "Can't find basic group member " << user_id << " in " << chat_id << " to be removed";
    repair_chat_participants(chat_id);
  }
}

void ContactsManager::get_dialog_administrators(DialogId dialog_id,
                                                Promise<td_api::object_ptr<td_api::chatAdministrators>> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "get_dialog_administrators")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::SecretChat:
      return promise.set_value(td_api::make_object<td_api::chatAdministrators>());
    case DialogType::Chat:
    case DialogType::Channel:
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
      return;
  }

  auto it = dialog_administrators_.find(dialog_id);
  if (it != dialog_administrators_.end()) {
    reload_dialog_administrators(dialog_id, it->second, Auto());  // update administrators cache
    return promise.set_value(get_chat_administrators_object(it->second));
  }

  if (G()->parameters().use_chat_info_db) {
    LOG(INFO) << "Load administrators of " << dialog_id << " from database";
    G()->td_db()->get_sqlite_pmc()->get(get_dialog_administrators_database_key(dialog_id),
                                        PromiseCreator::lambda([actor_id = actor_id(this), dialog_id,
                                                                promise = std::move(promise)](string value) mutable {
                                          send_closure(actor_id,
                                                       &ContactsManager::on_load_dialog_administrators_from_database,
                                                       dialog_id, std::move(value), std::move(promise));
                                        }));
    return;
  }

  reload_dialog_administrators(dialog_id, {}, std::move(promise));
}

void ContactsManager::set_my_online_status(bool is_online, bool send_update, bool is_local) {
  if (td_->auth_manager_->is_bot()) {
    return;  // just in case
  }

  auto my_id = get_my_id();
  User *u = get_user_force(my_id);
  if (u != nullptr) {
    int32 new_online;
    int32 now = G()->unix_time();
    if (is_online) {
      new_online = now + 300;
    } else {
      new_online = now - 1;
    }

    if (is_local) {
      LOG(INFO) << "Update my local online from " << my_was_online_local_ << " to " << new_online;
      if (!is_online) {
        new_online = min(new_online, u->was_online);
      }
      if (new_online != my_was_online_local_) {
        my_was_online_local_ = new_online;
        u->is_status_changed = true;
        u->is_online_status_changed = true;
      }
    } else {
      if (my_was_online_local_ != 0 || new_online != u->was_online) {
        LOG(INFO) << "Update my online from " << u->was_online << " to " << new_online;
        my_was_online_local_ = 0;
        u->was_online = new_online;
        u->is_status_changed = true;
        u->is_online_status_changed = true;
      }
    }

    if (was_online_local_ != new_online) {
      was_online_local_ = new_online;
      VLOG(notifications) << "Set was_online_local to " << was_online_local_;
      G()->td_db()->get_binlog_pmc()->set("my_was_online_local", to_string(was_online_local_));
    }

    if (send_update) {
      update_user(u, my_id);
    }
  }
}

vector<BotCommands> ContactsManager::get_bot_commands(vector<tl_object_ptr<telegram_api::botInfo>> &&bot_infos,
                                                      const vector<DialogParticipant> *participants) {
  vector<BotCommands> result;
  if (td_->auth_manager_->is_bot()) {
    return result;
  }
  for (auto &bot_info : bot_infos) {
    if (bot_info->commands_.empty()) {
      continue;
    }

    auto user_id = UserId(bot_info->user_id_);
    if (!have_user_force(user_id)) {
      LOG(ERROR) << "Receive unknown " << user_id;
      continue;
    }
    if (!is_user_bot(user_id)) {
      if (!is_user_deleted(user_id)) {
        LOG(ERROR) << "Receive non-bot " << user_id;
      }
      continue;
    }
    if (participants != nullptr) {
      bool is_participant = false;
      for (auto &participant : *participants) {
        if (participant.dialog_id_ == DialogId(user_id)) {
          is_participant = true;
          break;
        }
      }
      if (!is_participant) {
        LOG(ERROR) << "Skip commands of non-member bot " << user_id;
        continue;
      }
    }
    result.emplace_back(user_id, std::move(bot_info->commands_));
  }
  return result;
}

void ContactsManager::load_contacts(Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    are_contacts_loaded_ = true;
    saved_contact_count_ = 0;
  }
  if (are_contacts_loaded_ && saved_contact_count_ != -1) {
    LOG(INFO) << "Contacts are already loaded";
    promise.set_value(Unit());
    return;
  }
  load_contacts_queries_.push_back(std::move(promise));
  if (load_contacts_queries_.size() == 1u) {
    if (G()->parameters().use_chat_info_db && next_contacts_sync_date_ > 0 && saved_contact_count_ != -1) {
      LOG(INFO) << "Load contacts from database";
      G()->td_db()->get_sqlite_pmc()->get(
          "user_contacts", PromiseCreator::lambda([](string value) {
            send_closure(G()->contacts_manager(), &ContactsManager::on_load_contacts_from_database, std::move(value));
          }));
    } else {
      LOG(INFO) << "Load contacts from server";
      reload_contacts(true);
    }
  } else {
    LOG(INFO) << "Load contacts request has already been sent";
  }
}

ContactsManager::ChannelFull *ContactsManager::get_channel_full_force(ChannelId channel_id, bool only_local,
                                                                      const char *source) {
  if (!have_channel_force(channel_id)) {
    return nullptr;
  }

  ChannelFull *channel_full = get_channel_full(channel_id, only_local, source);
  if (channel_full != nullptr) {
    return channel_full;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (!unavailable_channel_fulls_.insert(channel_id).second) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load full " << channel_id << " from database from " << source;
  on_load_channel_full_from_database(
      channel_id, G()->td_db()->get_sqlite_sync_pmc()->get(get_channel_full_database_key(channel_id)), source);
  return get_channel_full(channel_id, only_local, source);
}

void ContactsManager::get_channel_message_statistics(FullMessageId full_message_id, bool is_dark,
                                                     Promise<td_api::object_ptr<td_api::messageStatistics>> &&promise) {
  auto dc_id_promise = PromiseCreator::lambda([actor_id = actor_id(this), full_message_id, is_dark,
                                               promise = std::move(promise)](Result<DcId> r_dc_id) mutable {
    if (r_dc_id.is_error()) {
      return promise.set_error(r_dc_id.move_as_error());
    }
    send_closure(actor_id, &ContactsManager::send_get_channel_message_stats_query, r_dc_id.move_as_ok(),
                 full_message_id, is_dark, std::move(promise));
  });
  get_channel_statistics_dc_id(full_message_id.get_dialog_id(), false, std::move(dc_id_promise));
}

ContactsManager::ChatFull *ContactsManager::get_chat_full_force(ChatId chat_id, const char *source) {
  if (!have_chat_force(chat_id)) {
    return nullptr;
  }

  ChatFull *chat_full = get_chat_full(chat_id);
  if (chat_full != nullptr) {
    return chat_full;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (!unavailable_chat_fulls_.insert(chat_id).second) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load full " << chat_id << " from database from " << source;
  on_load_chat_full_from_database(chat_id,
                                  G()->td_db()->get_sqlite_sync_pmc()->get(get_chat_full_database_key(chat_id)));
  return get_chat_full(chat_id);
}

ContactsManager::UserFull *ContactsManager::get_user_full_force(UserId user_id) {
  if (!have_user_force(user_id)) {
    return nullptr;
  }

  UserFull *user_full = get_user_full(user_id);
  if (user_full != nullptr) {
    return user_full;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (!unavailable_user_fulls_.insert(user_id).second) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load full " << user_id << " from database";
  on_load_user_full_from_database(user_id,
                                  G()->td_db()->get_sqlite_sync_pmc()->get(get_user_full_database_key(user_id)));
  return get_user_full(user_id);
}

void ContactsManager::set_location_visibility_expire_date(int32 expire_date) {
  if (location_visibility_expire_date_ == expire_date) {
    return;
  }

  LOG(INFO) << "Set set_location_visibility_expire_date to " << expire_date;
  location_visibility_expire_date_ = expire_date;
  if (expire_date == 0) {
    G()->td_db()->get_binlog_pmc()->erase("location_visibility_expire_date");
  } else {
    G()->td_db()->get_binlog_pmc()->set("location_visibility_expire_date", to_string(expire_date));
  }
  // the caller must call update_is_location_visible() itself
}

bool ContactsManager::can_get_channel_message_statistics(DialogId dialog_id) const {
  if (dialog_id.get_type() != DialogType::Channel) {
    return false;
  }

  auto channel_id = dialog_id.get_channel_id();
  const Channel *c = get_channel(channel_id);
  if (c == nullptr || c->is_megagroup) {
    return false;
  }

  if (td_->auth_manager_->is_bot()) {
    return false;
  }

  auto channel_full = get_channel_full(channel_id);
  if (channel_full != nullptr) {
    return channel_full->stats_dc_id.is_exact();
  }

  return c->status.is_administrator();
}

void ContactsManager::on_upload_profile_photo_error(FileId file_id, Status status) {
  LOG(INFO) << "File " << file_id << " has upload error " << status;
  CHECK(status.is_error());

  auto it = uploaded_profile_photos_.find(file_id);
  CHECK(it != uploaded_profile_photos_.end());

  auto promise = std::move(it->second.promise);

  uploaded_profile_photos_.erase(it);

  promise.set_error(std::move(status));  // TODO check that status has valid error code
}

void ContactsManager::on_update_user_phone_number(UserId user_id, string &&phone_number) {
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  User *u = get_user_force(user_id);
  if (u != nullptr) {
    on_update_user_phone_number(u, user_id, std::move(phone_number));
    update_user(u, user_id);
  } else {
    LOG(INFO) << "Ignore update user phone number about unknown " << user_id;
  }
}

void ContactsManager::save_chat_to_database(Chat *c, ChatId chat_id) {
  CHECK(c != nullptr);
  if (c->is_being_saved) {
    return;
  }
  if (loaded_from_database_chats_.count(chat_id)) {
    save_chat_to_database_impl(c, chat_id, get_chat_database_value(c));
    return;
  }
  if (load_chat_from_database_queries_.count(chat_id) != 0) {
    return;
  }

  load_chat_from_database_impl(chat_id, Auto());
}

void ContactsManager::update_is_location_visible() {
  auto expire_date = pending_location_visibility_expire_date_ != -1 ? pending_location_visibility_expire_date_
                                                                    : location_visibility_expire_date_;
  G()->shared_config().set_option_boolean("is_location_visible", expire_date != 0);
}

int32 ContactsManager::get_imported_contact_count(Promise<Unit> &&promise) {
  LOG(INFO) << "Get imported contact count";

  if (!are_contacts_loaded_ || saved_contact_count_ == -1) {
    load_contacts(std::move(promise));
    return 0;
  }
  reload_contacts(false);

  promise.set_value(Unit());
  return saved_contact_count_;
}

void ContactsManager::reload_channel_full(ChannelId channel_id, Promise<Unit> &&promise, const char *source) {
  send_get_channel_full_query(get_channel_full(channel_id, true, "reload_channel_full"), channel_id, std::move(promise),
                              source);
}

FolderId ContactsManager::get_secret_chat_initial_folder_id(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return FolderId::main();
  }
  return c->initial_folder_id;
}

string ContactsManager::get_user_private_forward_name(UserId user_id) {
  auto user_full = get_user_full_force(user_id);
  if (user_full != nullptr) {
    return user_full->private_forward_name;
  }
  return string();
}

ChannelId ContactsManager::get_chat_migrated_to_channel_id(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return ChannelId();
  }
  return c->migrated_to_channel_id;
}

void ContactsManager::on_get_users(vector<tl_object_ptr<telegram_api::User>> &&users, const char *source) {
  for (auto &user : users) {
    on_get_user(std::move(user), source);
  }
}

void ContactsManager::repair_chat_participants(ChatId chat_id) {
  send_get_chat_full_query(chat_id, Auto(), "repair_chat_participants");
}

string ContactsManager::get_channel_full_database_key(ChannelId channel_id) {
  return PSTRING() << "chf" << channel_id.get();
}
}  // namespace td
