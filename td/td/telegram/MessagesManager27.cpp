//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChatId.h"

#include "td/telegram/ContactsManager.h"
#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogActionBar.h"
#include "td/telegram/DialogDb.h"
#include "td/telegram/DialogFilter.h"
#include "td/telegram/DialogFilter.hpp"
#include "td/telegram/DialogLocation.h"
#include "td/telegram/DraftMessage.h"
#include "td/telegram/DraftMessage.hpp"

#include "td/telegram/files/FileId.hpp"
#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"
#include "td/telegram/GroupCallManager.h"

#include "td/telegram/InputMessageText.h"

#include "td/telegram/Location.h"
#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/MessageContent.h"
#include "td/telegram/MessageEntity.h"
#include "td/telegram/MessageEntity.hpp"
#include "td/telegram/MessageReplyInfo.hpp"
#include "td/telegram/MessagesDb.h"
#include "td/telegram/MessageSender.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/net/NetActor.h"
#include "td/telegram/net/NetQuery.h"
#include "td/telegram/NotificationGroupType.h"
#include "td/telegram/NotificationManager.h"
#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"

#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"


#include "td/telegram/Version.h"


#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"
#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/format.h"
#include "td/utils/misc.h"
#include "td/utils/PathView.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"




#include <tuple>

#include <unordered_map>



namespace td {
}  // namespace td
namespace td {

FullMessageId MessagesManager::on_get_message(MessageInfo &&message_info, bool from_update, bool is_channel_message,
                                              bool have_previous, bool have_next, const char *source) {
  DialogId dialog_id;
  unique_ptr<Message> new_message;
  std::tie(dialog_id, new_message) = create_message(std::move(message_info), is_channel_message);
  if (new_message == nullptr) {
    return FullMessageId();
  }
  MessageId message_id = new_message->message_id;

  new_message->have_previous = have_previous;
  new_message->have_next = have_next;

  bool need_update = from_update;
  bool need_update_dialog_pos = false;

  MessageId old_message_id = find_old_message_id(dialog_id, message_id);
  bool is_sent_message = false;
  if (old_message_id.is_valid() || old_message_id.is_valid_scheduled()) {
    LOG(INFO) << "Found temporary " << old_message_id << " for " << FullMessageId{dialog_id, message_id};
    Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);

    if (!from_update && !message_id.is_scheduled()) {
      if (message_id <= d->last_new_message_id) {
        if (get_message_force(d, message_id, "receive missed unsent message not from update") != nullptr) {
          LOG(ERROR) << "New " << old_message_id << "/" << message_id << " in " << dialog_id << " from " << source
                     << " has identifier less than last_new_message_id = " << d->last_new_message_id;
          return FullMessageId();
        }
        // if there is no message yet, then it is likely was missed because of a server bug and is being repaired via
        // get_message_from_server from after_get_difference
        // TODO move to INFO
        LOG(ERROR) << "Receive " << old_message_id << "/" << message_id << " in " << dialog_id << " from " << source
                   << " with identifier less than last_new_message_id = " << d->last_new_message_id
                   << " and trying to add it anyway";
      } else {
        // TODO move to INFO
        LOG(ERROR) << "Ignore " << old_message_id << "/" << message_id << " received not through update from " << source
                   << ": " << oneline(to_string(get_message_object(dialog_id, new_message.get(), "on_get_message")));
        if (dialog_id.get_type() == DialogType::Channel && have_input_peer(dialog_id, AccessRights::Read)) {
          channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
        }
        return FullMessageId();
      }
    }

    delete_update_message_id(dialog_id, message_id);

    if (!new_message->is_outgoing && dialog_id != get_my_dialog_id()) {
      // sent message is not from me
      LOG(ERROR) << "Sent in " << dialog_id << " " << message_id << " is sent by " << new_message->sender_user_id << "/"
                 << new_message->sender_dialog_id;
      return FullMessageId();
    }

    being_readded_message_id_ = {dialog_id, old_message_id};
    unique_ptr<Message> old_message =
        delete_message(d, old_message_id, false, &need_update_dialog_pos, "add sent message");
    if (old_message == nullptr) {
      delete_sent_message_on_server(dialog_id, new_message->message_id);
      being_readded_message_id_ = FullMessageId();
      return FullMessageId();
    }
    old_message_id = old_message->message_id;

    need_update = false;

    if (old_message_id.is_valid() && message_id.is_valid() && message_id < old_message_id &&
        !has_qts_messages(dialog_id)) {
      LOG(ERROR) << "Sent " << old_message_id << " to " << dialog_id << " as " << message_id;
    }

    set_message_id(new_message, old_message_id);
    new_message->from_database = false;
    new_message->have_previous = false;
    new_message->have_next = false;
    update_message(d, old_message.get(), std::move(new_message), &need_update_dialog_pos, false);
    new_message = std::move(old_message);

    set_message_id(new_message, message_id);
    send_update_message_send_succeeded(d, old_message_id, new_message.get());

    if (!message_id.is_scheduled()) {
      is_sent_message = true;
    }

    if (!from_update) {
      new_message->have_previous = have_previous;
      new_message->have_next = have_next;
    } else {
      new_message->have_previous = true;
      new_message->have_next = true;
    }
  }

  const Message *m = add_message_to_dialog(dialog_id, std::move(new_message), from_update, &need_update,
                                           &need_update_dialog_pos, source);
  being_readded_message_id_ = FullMessageId();
  Dialog *d = get_dialog(dialog_id);
  if (m == nullptr) {
    if (need_update_dialog_pos && d != nullptr) {
      send_update_chat_last_message(d, "on_get_message");
    }
    if (old_message_id.is_valid() || old_message_id.is_valid_scheduled()) {
      CHECK(d != nullptr);
      if (!old_message_id.is_valid() || !message_id.is_valid() || old_message_id <= message_id) {
        LOG(ERROR) << "Failed to add just sent " << old_message_id << " to " << dialog_id << " as " << message_id
                   << " from " << source << ": " << debug_add_message_to_dialog_fail_reason_;
      }
      send_update_delete_messages(dialog_id, {message_id.get()}, true, false);
    }

    return FullMessageId();
  }

  CHECK(d != nullptr);

  auto pcc_it = pending_created_dialogs_.find(dialog_id);
  if (from_update && pcc_it != pending_created_dialogs_.end()) {
    pcc_it->second.set_value(Unit());

    pending_created_dialogs_.erase(pcc_it);
  }

  if (need_update) {
    send_update_new_message(d, m);
  }

  if (is_sent_message) {
    try_add_active_live_location(dialog_id, m);

    // add_message_to_dialog will not update counts, because need_update == false
    update_message_count_by_index(d, +1, m);
  }

  if (is_sent_message || (need_update && !message_id.is_scheduled())) {
    update_reply_count_by_message(d, +1, m);
    update_forward_count(dialog_id, m);
  }

  if (dialog_id.get_type() == DialogType::Channel && !have_input_peer(dialog_id, AccessRights::Read)) {
    auto p = delete_message(d, message_id, false, &need_update_dialog_pos, "get a message in inaccessible chat");
    CHECK(p.get() == m);
    // CHECK(d->messages == nullptr);
    send_update_delete_messages(dialog_id, {p->message_id.get()}, false, false);
    // don't need to update dialog pos
    return FullMessageId();
  }

  if (m->message_id.is_scheduled()) {
    send_update_chat_has_scheduled_messages(d, false);
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "on_get_message");
  }

  // set dialog reply markup only after updateNewMessage and updateChatLastMessage are sent
  if (need_update && m->reply_markup != nullptr && !m->message_id.is_scheduled() &&
      m->reply_markup->type != ReplyMarkup::Type::InlineKeyboard && m->reply_markup->is_personal &&
      !td_->auth_manager_->is_bot()) {
    set_dialog_reply_markup(d, message_id);
  }

  return FullMessageId(dialog_id, message_id);
}

void MessagesManager::open_dialog(Dialog *d) {
  CHECK(!td_->auth_manager_->is_bot());
  DialogId dialog_id = d->dialog_id;
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return;
  }
  recently_opened_dialogs_.add_dialog(dialog_id);
  if (d->is_opened) {
    return;
  }
  d->is_opened = true;
  d->was_opened = true;

  auto min_message_id = MessageId(ServerMessageId(1));
  if (d->last_message_id == MessageId() && d->last_read_outbox_message_id < min_message_id && d->messages != nullptr &&
      d->messages->message_id < min_message_id) {
    Message *m = d->messages.get();
    while (m->right != nullptr) {
      m = m->right.get();
    }
    if (m->message_id < min_message_id) {
      read_history_inbox(dialog_id, m->message_id, -1, "open_dialog");
    }
  }

  if (d->has_unload_timeout) {
    LOG(INFO) << "Cancel unload timeout for " << dialog_id;
    pending_unload_dialog_timeout_.cancel_timeout(dialog_id.get());
    d->has_unload_timeout = false;
  }

  if (d->new_secret_chat_notification_id.is_valid()) {
    remove_new_secret_chat_notification(d, true);
  }

  get_dialog_pinned_message(dialog_id, Auto());

  if (d->active_group_call_id.is_valid()) {
    td_->group_call_manager_->reload_group_call(d->active_group_call_id, Auto());
  }
  if (d->need_drop_default_send_message_as_dialog_id) {
    CHECK(d->default_send_message_as_dialog_id.is_valid());
    d->need_drop_default_send_message_as_dialog_id = false;
    d->default_send_message_as_dialog_id = DialogId();
    LOG(INFO) << "Set message sender in " << d->dialog_id << " to " << d->default_send_message_as_dialog_id;
    on_dialog_updated(dialog_id, "open_dialog");
    send_update_chat_message_sender(d);
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat:
      td_->contacts_manager_->repair_chat_participants(dialog_id.get_chat_id());
      reget_dialog_action_bar(dialog_id, "open_dialog", false);
      break;
    case DialogType::Channel:
      if (!is_broadcast_channel(dialog_id)) {
        auto participant_count = td_->contacts_manager_->get_channel_participant_count(dialog_id.get_channel_id());
        if (participant_count < 195) {  // include unknown participant_count
          td_->contacts_manager_->get_channel_participants(dialog_id.get_channel_id(),
                                                           td_api::make_object<td_api::supergroupMembersFilterRecent>(),
                                                           string(), 0, 200, 200, Auto());
        }
      }
      get_channel_difference(dialog_id, d->pts, true, "open_dialog");
      reget_dialog_action_bar(dialog_id, "open_dialog", false);
      break;
    case DialogType::SecretChat: {
      // to repair dialog action bar
      auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      if (user_id.is_valid()) {
        td_->contacts_manager_->reload_user_full(user_id);
      }
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  if (!td_->auth_manager_->is_bot()) {
    auto online_count_it = dialog_online_member_counts_.find(dialog_id);
    if (online_count_it != dialog_online_member_counts_.end()) {
      auto &info = online_count_it->second;
      CHECK(!info.is_update_sent);
      if (Time::now() - info.updated_time < ONLINE_MEMBER_COUNT_CACHE_EXPIRE_TIME) {
        info.is_update_sent = true;
        send_update_chat_online_member_count(dialog_id, info.online_member_count);
      }
    }

    if (d->has_scheduled_database_messages && !d->is_has_scheduled_database_messages_checked) {
      CHECK(G()->parameters().use_message_db);

      LOG(INFO) << "Send check has_scheduled_database_messages request";
      d->is_has_scheduled_database_messages_checked = true;
      G()->td_db()->get_messages_db_async()->get_scheduled_messages(
          dialog_id, 1,
          PromiseCreator::lambda([actor_id = actor_id(this), dialog_id](vector<MessagesDbDialogMessage> messages) {
            if (messages.empty()) {
              send_closure(actor_id, &MessagesManager::set_dialog_has_scheduled_database_messages, dialog_id, false);
            }
          }));
    }
  }
}

void MessagesManager::delete_messages(DialogId dialog_id, const vector<MessageId> &input_message_ids, bool revoke,
                                      Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  Dialog *d = get_dialog_force(dialog_id, "delete_messages");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat is not found"));
  }

  if (input_message_ids.empty()) {
    return promise.set_value(Unit());
  }

  auto dialog_type = dialog_id.get_type();
  bool is_secret = dialog_type == DialogType::SecretChat;

  vector<MessageId> message_ids;
  message_ids.reserve(input_message_ids.size());
  vector<MessageId> deleted_server_message_ids;

  vector<MessageId> deleted_scheduled_server_message_ids;
  for (auto message_id : input_message_ids) {
    if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
      return promise.set_error(Status::Error(400, "Invalid message identifier"));
    }

    message_id = get_persistent_message_id(d, message_id);
    message_ids.push_back(message_id);
    auto m = get_message_force(d, message_id, "delete_messages");
    if (m != nullptr) {
      if (m->message_id.is_scheduled()) {
        if (m->message_id.is_scheduled_server()) {
          deleted_scheduled_server_message_ids.push_back(m->message_id);
        }
      } else {
        if (m->message_id.is_server() || is_secret) {
          deleted_server_message_ids.push_back(m->message_id);
        }
      }
    }
  }

  bool is_bot = td_->auth_manager_->is_bot();
  for (auto message_id : message_ids) {
    auto m = get_message(d, message_id);
    if (!can_delete_message(dialog_id, m)) {
      return promise.set_error(Status::Error(400, "Message can't be deleted"));
    }
    if (is_bot && !message_id.is_scheduled() && message_id.is_server() && !can_revoke_message(dialog_id, m)) {
      return promise.set_error(Status::Error(400, "Message can't be deleted for everyone"));
    }
  }

  MultiPromiseActorSafe mpas{"DeleteMessagesMultiPromiseActor"};
  mpas.add_promise(std::move(promise));

  auto lock = mpas.get_promise();
  delete_messages_on_server(dialog_id, std::move(deleted_server_message_ids), revoke, 0, mpas.get_promise());
  delete_scheduled_messages_on_server(dialog_id, std::move(deleted_scheduled_server_message_ids), 0,
                                      mpas.get_promise());
  lock.set_value(Unit());

  bool need_update_dialog_pos = false;
  bool need_update_chat_has_scheduled_messages = false;
  vector<int64> deleted_message_ids;
  for (auto message_id : message_ids) {
    auto m = delete_message(d, message_id, true, &need_update_dialog_pos, DELETE_MESSAGE_USER_REQUEST_SOURCE);
    if (m == nullptr) {
      LOG(INFO) << "Can't delete " << message_id << " because it is not found";
    } else {
      need_update_chat_has_scheduled_messages |= m->message_id.is_scheduled();
      deleted_message_ids.push_back(m->message_id.get());
    }
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "delete_messages");
  }
  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);

  if (need_update_chat_has_scheduled_messages) {
    send_update_chat_has_scheduled_messages(d, true);
  }
}

void MessagesManager::delete_dialog_messages_by_date(DialogId dialog_id, int32 min_date, int32 max_date, bool revoke,
                                                     Promise<Unit> &&promise) {
  bool is_bot = td_->auth_manager_->is_bot();
  CHECK(!is_bot);

  Dialog *d = get_dialog_force(dialog_id, "delete_dialog_messages_by_date");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  if (min_date > max_date) {
    return promise.set_error(Status::Error(400, "Wrong date interval specified"));
  }

  const int32 telegram_launch_date = 1376438400;
  if (max_date < telegram_launch_date) {
    return promise.set_value(Unit());
  }
  if (min_date < telegram_launch_date) {
    min_date = telegram_launch_date;
  }

  auto current_date = max(G()->unix_time(), 1635000000);
  if (min_date >= current_date - 30) {
    return promise.set_value(Unit());
  }
  if (max_date >= current_date - 30) {
    max_date = current_date - 31;
  }
  CHECK(min_date <= max_date);

  switch (dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat:
      if (revoke) {
        return promise.set_error(Status::Error(400, "Bulk message revocation is unsupported in basic group chats"));
      }
      break;
    case DialogType::Channel:
      return promise.set_error(Status::Error(400, "Bulk message deletion is unsupported in supergroup chats"));
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Bulk message deletion is unsupported in secret chats"));
    case DialogType::None:
    default:
      UNREACHABLE();
      break;
  }

  // TODO delete in database by dates

  vector<MessageId> message_ids;
  find_messages_by_date(d->messages.get(), min_date, max_date, message_ids);

  bool need_update_dialog_pos = false;
  vector<int64> deleted_message_ids;
  for (auto message_id : message_ids) {
    auto m = delete_message(d, message_id, true, &need_update_dialog_pos, DELETE_MESSAGE_USER_REQUEST_SOURCE);
    CHECK(m != nullptr);
    deleted_message_ids.push_back(m->message_id.get());
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "delete_dialog_messages_by_date");
  }
  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);

  delete_dialog_messages_by_date_on_server(dialog_id, min_date, max_date, revoke, 0, std::move(promise));
}

class StartImportHistoryQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit StartImportHistoryQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int64 import_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);

    send_query(
        G()->net_query_creator().create(telegram_api::messages_startHistoryImport(std::move(input_peer), import_id)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_startHistoryImport>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    if (!result_ptr.ok()) {
      return on_error(Status::Error(500, "Import history returned false"));
    }
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "StartImportHistoryQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::on_imported_message_attachments_uploaded(int64 random_id, Result<Unit> &&result) {
  if (G()->close_flag()) {
    result = Global::request_aborted_error();
  }

  auto it = pending_message_imports_.find(random_id);
  CHECK(it != pending_message_imports_.end());

  auto pending_message_import = std::move(it->second);
  CHECK(pending_message_import != nullptr);

  pending_message_imports_.erase(it);

  if (result.is_error()) {
    pending_message_import->promise.set_error(result.move_as_error());
    return;
  }

  CHECK(pending_message_import->upload_files_multipromise.promise_count() == 0);

  auto promise = std::move(pending_message_import->promise);
  auto dialog_id = pending_message_import->dialog_id;

  TRY_STATUS_PROMISE(promise, can_send_message(dialog_id));

  td_->create_handler<StartImportHistoryQuery>(std::move(promise))->send(dialog_id, pending_message_import->import_id);
}

void MessagesManager::on_get_recent_locations(DialogId dialog_id, int32 limit, int32 total_count,
                                              vector<tl_object_ptr<telegram_api::Message>> &&messages,
                                              Promise<td_api::object_ptr<td_api::messages>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Receive " << messages.size() << " recent locations in " << dialog_id;
  vector<MessageId> result;
  for (auto &message : messages) {
    auto new_full_message_id = on_get_message(std::move(message), false, dialog_id.get_type() == DialogType::Channel,
                                              false, false, false, "get recent locations");
    if (new_full_message_id != FullMessageId()) {
      if (new_full_message_id.get_dialog_id() != dialog_id) {
        LOG(ERROR) << "Receive " << new_full_message_id << " instead of a message in " << dialog_id;
        total_count--;
        continue;
      }
      auto m = get_message(new_full_message_id);
      CHECK(m != nullptr);
      if (m->content->get_type() != MessageContentType::LiveLocation) {
        LOG(ERROR) << "Receive a message of wrong type " << m->content->get_type() << " in on_get_recent_locations in "
                   << dialog_id;
        total_count--;
        continue;
      }

      result.push_back(m->message_id);
    } else {
      total_count--;
    }
  }
  if (total_count < static_cast<int32>(result.size())) {
    LOG(ERROR) << "Receive " << result.size() << " valid messages out of " << total_count << " in " << messages.size()
               << " messages";
    total_count = static_cast<int32>(result.size());
  }
  promise.set_value(get_messages_object(total_count, dialog_id, result, true, "on_get_recent_locations"));
}

td_api::object_ptr<td_api::ChatType> MessagesManager::get_chat_type_object(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_api::make_object<td_api::chatTypePrivate>(
          td_->contacts_manager_->get_user_id_object(dialog_id.get_user_id(), "chatTypePrivate"));
    case DialogType::Chat:
      return td_api::make_object<td_api::chatTypeBasicGroup>(
          td_->contacts_manager_->get_basic_group_id_object(dialog_id.get_chat_id(), "chatTypeBasicGroup"));
    case DialogType::Channel: {
      auto channel_id = dialog_id.get_channel_id();
      auto channel_type = td_->contacts_manager_->get_channel_type(channel_id);
      return td_api::make_object<td_api::chatTypeSupergroup>(
          td_->contacts_manager_->get_supergroup_id_object(channel_id, "chatTypeSupergroup"),
          channel_type != ContactsManager::ChannelType::Megagroup);
    }
    case DialogType::SecretChat: {
      auto secret_chat_id = dialog_id.get_secret_chat_id();
      auto user_id = td_->contacts_manager_->get_secret_chat_user_id(secret_chat_id);
      return td_api::make_object<td_api::chatTypeSecret>(
          td_->contacts_manager_->get_secret_chat_id_object(secret_chat_id, "chatTypeSecret"),
          td_->contacts_manager_->get_user_id_object(user_id, "chatTypeSecret"));
    }
    case DialogType::None:
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void MessagesManager::set_dialog_pinned_message_notification(Dialog *d, MessageId message_id, const char *source) {
  CHECK(d != nullptr);
  CHECK(!message_id.is_scheduled());
  auto old_message_id = d->pinned_message_notification_message_id;
  if (old_message_id == message_id) {
    return;
  }
  VLOG(notifications) << "Change pinned message notification in " << d->dialog_id << " from " << old_message_id
                      << " to " << message_id;
  if (old_message_id.is_valid()) {
    auto m = get_message_force(d, old_message_id, source);
    if (m != nullptr && m->notification_id.is_valid() && is_message_notification_active(d, m)) {
      // Can't remove pinned_message_notification_message_id  before the call,
      // because the notification needs to be still active inside remove_message_notification_id
      remove_message_notification_id(d, m, true, false, true);
      on_message_changed(d, m, false, source);
    } else {
      send_closure_later(G()->notification_manager(), &NotificationManager::remove_temporary_notification_by_message_id,
                         d->mention_notification_group.group_id, old_message_id, false, source);
    }
  }
  d->pinned_message_notification_message_id = message_id;
  on_dialog_updated(d->dialog_id, source);
}

void MessagesManager::on_update_secret_chat_state(SecretChatId secret_chat_id, SecretChatState state) {
  if (state == SecretChatState::Closed && !td_->auth_manager_->is_bot()) {
    DialogId dialog_id(secret_chat_id);
    Dialog *d = get_dialog_force(dialog_id, "on_update_secret_chat_state");
    if (d != nullptr) {
      if (d->new_secret_chat_notification_id.is_valid()) {
        remove_new_secret_chat_notification(d, true);
      }
      if (d->message_notification_group.group_id.is_valid() && get_dialog_pending_notification_count(d, false) == 0 &&
          !d->message_notification_group.last_notification_id.is_valid()) {
        CHECK(d->message_notification_group.last_notification_date == 0);
        d->message_notification_group.try_reuse = true;
        d->message_notification_group.is_changed = true;
        on_dialog_updated(d->dialog_id, "on_update_secret_chat_state");
      }
      CHECK(!d->mention_notification_group.group_id.is_valid());  // there can't be unread mentions in secret chats
    }
  }
}

void MessagesManager::ttl_loop(double now) {
  std::unordered_map<DialogId, std::vector<MessageId>, DialogIdHash> to_delete;
  while (!ttl_heap_.empty() && ttl_heap_.top_key() < now) {
    TtlNode *ttl_node = TtlNode::from_heap_node(ttl_heap_.pop());
    auto full_message_id = ttl_node->full_message_id_;
    auto dialog_id = full_message_id.get_dialog_id();
    if (dialog_id.get_type() == DialogType::SecretChat || ttl_node->by_ttl_period_) {
      to_delete[dialog_id].push_back(full_message_id.get_message_id());
    } else {
      auto d = get_dialog(dialog_id);
      CHECK(d != nullptr);
      auto m = get_message(d, full_message_id.get_message_id());
      CHECK(m != nullptr);
      on_message_ttl_expired(d, m);
      on_message_changed(d, m, true, "ttl_loop");
    }
  }
  for (auto &it : to_delete) {
    delete_dialog_messages(it.first, it.second, false, true, "ttl_loop");
  }
  ttl_update_timeout(now);
}

tl_object_ptr<td_api::MessageSendingState> MessagesManager::get_message_sending_state_object(const Message *m) const {
  CHECK(m != nullptr);
  if (m->message_id.is_yet_unsent()) {
    return td_api::make_object<td_api::messageSendingStatePending>();
  }
  if (m->is_failed_to_send) {
    auto can_retry = can_resend_message(m);
    auto need_another_sender =
        can_retry && m->send_error_code == 400 && m->send_error_message == CSlice("SEND_AS_PEER_INVALID");
    return td_api::make_object<td_api::messageSendingStateFailed>(m->send_error_code, m->send_error_message, can_retry,
                                                                  need_another_sender,
                                                                  max(m->try_resend_at - Time::now(), 0.0));
  }
  return nullptr;
}

void MessagesManager::add_message_file_sources(DialogId dialog_id, const Message *m) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (dialog_id.get_type() != DialogType::SecretChat && m->is_content_secret) {
    // return;
  }

  auto file_ids = get_message_content_file_ids(m->content.get(), td_);
  if (file_ids.empty()) {
    return;
  }

  // do not create file_source_id for messages without file_ids
  auto file_source_id = get_message_file_source_id(FullMessageId(dialog_id, m->message_id));
  if (file_source_id.is_valid()) {
    for (auto file_id : file_ids) {
      td_->file_manager_->add_file_source(file_id, file_source_id);
    }
  }
}

void MessagesManager::send_update_chat_unread_mention_count(const Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_unread_mention_count";
  LOG(INFO) << "Update unread mention message count in " << d->dialog_id << " to " << d->unread_mention_count;
  on_dialog_updated(d->dialog_id, "send_update_chat_unread_mention_count");
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatUnreadMentionCount>(d->dialog_id.get(), d->unread_mention_count));
}

bool MessagesManager::need_message_changed_warning(const Message *old_message) {
  if (old_message->edit_date > 0) {
    // message was edited
    return false;
  }
  if (old_message->message_id.is_yet_unsent() &&
      (old_message->forward_info != nullptr || old_message->had_forward_info ||
       old_message->real_forward_from_dialog_id.is_valid())) {
    // original message may be edited
    return false;
  }
  if (old_message->ttl > 0) {
    // message can expire
    return false;
  }
  return true;
}

td_api::object_ptr<td_api::updateScopeNotificationSettings>
MessagesManager::get_update_scope_notification_settings_object(NotificationSettingsScope scope) const {
  auto notification_settings = get_scope_notification_settings(scope);
  CHECK(notification_settings != nullptr);
  return td_api::make_object<td_api::updateScopeNotificationSettings>(
      get_notification_settings_scope_object(scope), get_scope_notification_settings_object(notification_settings));
}

void MessagesManager::on_pending_read_history_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::do_read_history_on_server,
                     DialogId(dialog_id_int));
}

tl_object_ptr<td_api::MessageSchedulingState> MessagesManager::get_message_scheduling_state_object(int32 send_date) {
  if (send_date == SCHEDULE_WHEN_ONLINE_DATE) {
    return td_api::make_object<td_api::messageSchedulingStateSendWhenOnline>();
  }
  return td_api::make_object<td_api::messageSchedulingStateSendAtDate>(send_date);
}

tl_object_ptr<telegram_api::InputPeer> MessagesManager::get_send_message_as_input_peer(const Message *m) const {
  if (!m->has_explicit_sender) {
    return nullptr;
  }
  return get_input_peer(get_message_sender(m), AccessRights::Write);
}

DialogId MessagesManager::get_message_sender(const Message *m) {
  CHECK(m != nullptr);
  return m->sender_dialog_id.is_valid() ? m->sender_dialog_id : DialogId(m->sender_user_id);
}
}  // namespace td
