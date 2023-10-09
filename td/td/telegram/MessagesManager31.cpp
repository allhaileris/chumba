//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChatId.h"
#include "td/telegram/ConfigShared.h"
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

#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"


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

#include "td/telegram/UpdatesManager.h"
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



#include <limits>






namespace td {
}  // namespace td
namespace td {

class GetDialogUnreadMarksQuery final : public Td::ResultHandler {
 public:
  void send() {
    send_query(G()->net_query_creator().create(telegram_api::messages_getDialogUnreadMarks()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getDialogUnreadMarks>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto results = result_ptr.move_as_ok();
    for (auto &result : results) {
      td_->messages_manager_->on_update_dialog_is_marked_as_unread(DialogId(result), true);
    }

    G()->td_db()->get_binlog_pmc()->set("fetched_marks_as_unread", "1");
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for GetDialogUnreadMarksQuery: " << status;
    }
    status.ignore();
  }
};

void MessagesManager::after_get_difference() {
  CHECK(!td_->updates_manager_->running_get_difference());

  running_get_difference_ = false;

  if (!pending_on_get_dialogs_.empty()) {
    LOG(INFO) << "Apply postponed results of getDialogs";
    for (auto &res : pending_on_get_dialogs_) {
      on_get_dialogs(res.folder_id, std::move(res.dialogs), res.total_count, std::move(res.messages),
                     std::move(res.promise));
    }
    pending_on_get_dialogs_.clear();
  }

  if (!postponed_chat_read_inbox_updates_.empty()) {
    LOG(INFO) << "Send postponed chat read inbox updates";
    auto dialog_ids = std::move(postponed_chat_read_inbox_updates_);
    for (auto dialog_id : dialog_ids) {
      send_update_chat_read_inbox(get_dialog(dialog_id), false, "after_get_difference");
    }
  }
  while (!postponed_unread_message_count_updates_.empty()) {
    auto *list = get_dialog_list(*postponed_unread_message_count_updates_.begin());
    CHECK(list != nullptr);
    send_update_unread_message_count(*list, DialogId(), true, "after_get_difference");
  }
  while (!postponed_unread_chat_count_updates_.empty()) {
    auto *list = get_dialog_list(*postponed_unread_chat_count_updates_.begin());
    CHECK(list != nullptr);
    send_update_unread_chat_count(*list, DialogId(), true, "after_get_difference");
  }

  vector<FullMessageId> update_message_ids_to_delete;
  for (auto &it : update_message_ids_) {
    // there can be unhandled updateMessageId updates after getDifference even for ordinary chats,
    // because despite updates coming during getDifference have already been applied,
    // some of them could be postponed because of pts gap
    auto full_message_id = it.first;
    auto dialog_id = full_message_id.get_dialog_id();
    auto message_id = full_message_id.get_message_id();
    CHECK(message_id.is_valid());
    CHECK(message_id.is_server());
    switch (dialog_id.get_type()) {
      case DialogType::Channel:
        // get channel difference may prevent updates from being applied
        if (running_get_channel_difference(dialog_id)) {
          break;
        }
      // fallthrough
      case DialogType::User:
      case DialogType::Chat: {
        if (!have_message_force({dialog_id, it.second}, "after get difference")) {
          // The sent message has already been deleted by the user or sent to inaccessible channel.
          // The sent message may never be received, but we will need updateMessageId in case the message is received
          // to delete it from the server and not add to the chat.
          // But if the chat is inaccessible or the message is in an inaccessible chat part, then we will not be able to
          // add the message or delete it from the server. In this case we forget updateMessageId for such messages in
          // order to not check them over and over.
          const Dialog *d = get_dialog(dialog_id);
          if (!have_input_peer(dialog_id, AccessRights::Read) ||
              (d != nullptr &&
               message_id <= td::max(d->last_clear_history_message_id, d->max_unavailable_message_id))) {
            update_message_ids_to_delete.push_back(it.first);
          }
          break;
        }

        const Dialog *d = get_dialog(dialog_id);
        CHECK(d != nullptr);
        if (dialog_id.get_type() == DialogType::Channel || message_id <= d->last_new_message_id) {
          LOG(ERROR) << "Receive updateMessageId from " << it.second << " to " << full_message_id
                     << " but not receive corresponding message, last_new_message_id = " << d->last_new_message_id;
        }
        if (dialog_id.get_type() != DialogType::Channel && message_id <= d->last_new_message_id) {
          dump_debug_message_op(get_dialog(dialog_id));
        }
        if (message_id <= d->last_new_message_id) {
          get_message_from_server(it.first, PromiseCreator::lambda([full_message_id](Result<Unit> result) {
                                    if (result.is_error()) {
                                      LOG(WARNING)
                                          << "Failed to get missing " << full_message_id << ": " << result.error();
                                    } else {
                                      LOG(WARNING) << "Successfully get missing " << full_message_id;
                                    }
                                  }),
                                  "get missing");
        } else if (dialog_id.get_type() == DialogType::Channel) {
          LOG(INFO) << "Schedule getDifference in " << dialog_id.get_channel_id();
          channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
        }
        break;
      }
      case DialogType::SecretChat:
        break;
      case DialogType::None:
      default:
        UNREACHABLE();
        break;
    }
  }
  for (const auto &full_message_id : update_message_ids_to_delete) {
    update_message_ids_.erase(full_message_id);
  }

  if (!td_->auth_manager_->is_bot()) {
    if (!G()->td_db()->get_binlog_pmc()->isset("fetched_marks_as_unread")) {
      td_->create_handler<GetDialogUnreadMarksQuery>()->send();
    }

    load_notification_settings();

    auto dialog_list_id = DialogListId(FolderId::archive());
    auto *list = get_dialog_list(dialog_list_id);
    CHECK(list != nullptr);
    if (!list->is_dialog_unread_count_inited_) {
      int32 limit = list->are_pinned_dialogs_inited_ ? static_cast<int32>(list->pinned_dialogs_.size())
                                                     : get_pinned_dialogs_limit(dialog_list_id);
      LOG(INFO) << "Loading chat list in " << dialog_list_id << " to init total unread count";
      get_dialogs_from_list(dialog_list_id, limit + 2, Auto());
    }
  }
}

class GetMessagePublicForwardsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::foundMessages>> promise_;
  DialogId dialog_id_;
  int32 limit_;

 public:
  explicit GetMessagePublicForwardsQuery(Promise<td_api::object_ptr<td_api::foundMessages>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DcId dc_id, FullMessageId full_message_id, int32 offset_date, DialogId offset_dialog_id,
            ServerMessageId offset_message_id, int32 limit) {
    dialog_id_ = full_message_id.get_dialog_id();
    limit_ = limit;

    auto input_peer = MessagesManager::get_input_peer_force(offset_dialog_id);
    CHECK(input_peer != nullptr);

    send_query(
        G()->net_query_creator().create(telegram_api::stats_getMessagePublicForwards(
                                            td_->contacts_manager_->get_input_channel(dialog_id_.get_channel_id()),
                                            full_message_id.get_message_id().get_server_message_id().get(), offset_date,
                                            std::move(input_peer), offset_message_id.get(), limit),
                                        dc_id));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stats_getMessagePublicForwards>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetMessagePublicForwardsQuery");
    td_->messages_manager_->get_channel_differences_if_needed(
        std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(),
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_message_public_forwards, info.total_count,
                         std::move(info.messages), std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetMessagePublicForwardsQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::send_get_message_public_forwards_query(
    DcId dc_id, FullMessageId full_message_id, string offset, int32 limit,
    Promise<td_api::object_ptr<td_api::foundMessages>> &&promise) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "send_get_message_public_forwards_query");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  const Message *m = get_message_force(d, full_message_id.get_message_id(), "send_get_message_public_forwards_query");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (m->view_count == 0 || m->forward_info != nullptr || m->had_forward_info || m->message_id.is_scheduled() ||
      !m->message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Message forwards are inaccessible"));
  }

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }
  if (limit > MAX_SEARCH_MESSAGES) {
    limit = MAX_SEARCH_MESSAGES;
  }

  int32 offset_date = std::numeric_limits<int32>::max();
  DialogId offset_dialog_id;
  ServerMessageId offset_message_id;

  if (!offset.empty()) {
    auto parts = full_split(offset, ',');
    if (parts.size() != 3) {
      return promise.set_error(Status::Error(400, "Invalid offset specified"));
    }
    auto r_offset_date = to_integer_safe<int32>(parts[0]);
    auto r_offset_dialog_id = to_integer_safe<int64>(parts[1]);
    auto r_offset_message_id = to_integer_safe<int32>(parts[2]);
    if (r_offset_date.is_error() || r_offset_dialog_id.is_error() || r_offset_message_id.is_error()) {
      return promise.set_error(Status::Error(400, "Invalid offset specified"));
    }

    offset_date = r_offset_date.ok();
    offset_dialog_id = DialogId(r_offset_dialog_id.ok());
    offset_message_id = ServerMessageId(r_offset_message_id.ok());
  }

  td_->create_handler<GetMessagePublicForwardsQuery>(std::move(promise))
      ->send(dc_id, full_message_id, offset_date, offset_dialog_id, offset_message_id, limit);
}

class StartBotQuery final : public Td::ResultHandler {
  int64 random_id_;
  DialogId dialog_id_;

 public:
  NetQueryRef send(tl_object_ptr<telegram_api::InputUser> bot_input_user, DialogId dialog_id,
                   tl_object_ptr<telegram_api::InputPeer> input_peer, const string &parameter, int64 random_id) {
    CHECK(bot_input_user != nullptr);
    CHECK(input_peer != nullptr);
    random_id_ = random_id;
    dialog_id_ = dialog_id;

    auto query = G()->net_query_creator().create(
        telegram_api::messages_startBot(std::move(bot_input_user), std::move(input_peer), random_id, parameter));
    if (G()->shared_config().get_option_boolean("use_quick_ack")) {
      query->quick_ack_promise_ = PromiseCreator::lambda(
          [random_id](Unit) {
            send_closure(G()->messages_manager(), &MessagesManager::on_send_message_get_quick_ack, random_id);
          },
          PromiseCreator::Ignore());
    }
    auto send_query_ref = query.get_weak();
    send_query(std::move(query));
    return send_query_ref;
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_startBot>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for StartBotQuery for " << random_id_ << ": " << to_string(ptr);
    // Result may contain messageActionChatAddUser
    // td_->messages_manager_->check_send_message_result(random_id_, dialog_id_, ptr.get(), "StartBot");
    td_->updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for StartBotQuery: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, message should be re-sent
      return;
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "StartBotQuery");
    td_->messages_manager_->on_send_message_fail(random_id_, std::move(status));
  }
};

void MessagesManager::do_send_bot_start_message(UserId bot_user_id, DialogId dialog_id, const string &parameter,
                                                const Message *m) {
  LOG(INFO) << "Do send bot start " << FullMessageId(dialog_id, m->message_id) << " to bot " << bot_user_id;

  int64 random_id = begin_send_message(dialog_id, m);
  telegram_api::object_ptr<telegram_api::InputPeer> input_peer = dialog_id.get_type() == DialogType::User
                                                                     ? make_tl_object<telegram_api::inputPeerEmpty>()
                                                                     : get_input_peer(dialog_id, AccessRights::Write);
  if (input_peer == nullptr) {
    return on_send_message_fail(random_id, Status::Error(400, "Have no info about the chat"));
  }
  auto r_bot_input_user = td_->contacts_manager_->get_input_user(bot_user_id);
  if (r_bot_input_user.is_error()) {
    return on_send_message_fail(random_id, r_bot_input_user.move_as_error());
  }

  m->send_query_ref = td_->create_handler<StartBotQuery>()->send(r_bot_input_user.move_as_ok(), dialog_id,
                                                                 std::move(input_peer), parameter, random_id);
}

class SetChatThemeQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit SetChatThemeQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &theme_name) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::messages_setChatTheme(std::move(input_peer), theme_name)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_setChatTheme>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SetChatThemeQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SetChatThemeQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::set_dialog_theme(DialogId dialog_id, const string &theme_name, Promise<Unit> &&promise) {
  auto d = get_dialog_force(dialog_id, "set_dialog_theme");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Write)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat:
    case DialogType::Channel:
      return promise.set_error(Status::Error(400, "Can't change theme in the chat"));
    case DialogType::SecretChat: {
      auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      if (!user_id.is_valid()) {
        return promise.set_error(Status::Error(400, "Can't access the user"));
      }
      dialog_id = DialogId(user_id);
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  // TODO this can be wrong if there were previous change theme requests
  if (d->theme_name == theme_name) {
    return promise.set_value(Unit());
  }

  // TODO invoke after
  td_->create_handler<SetChatThemeQuery>(std::move(promise))->send(dialog_id, theme_name);
}

MessageId MessagesManager::get_message_id_by_random_id(Dialog *d, int64 random_id, const char *source) {
  CHECK(d != nullptr);
  CHECK(d->dialog_id.get_type() == DialogType::SecretChat);
  if (random_id == 0) {
    return MessageId();
  }
  auto it = d->random_id_to_message_id.find(random_id);
  if (it == d->random_id_to_message_id.end()) {
    if (G()->parameters().use_message_db) {
      auto r_value = G()->td_db()->get_messages_db_sync()->get_message_by_random_id(d->dialog_id, random_id);
      if (r_value.is_ok()) {
        debug_add_message_to_dialog_fail_reason_ = "not called";
        Message *m = on_get_message_from_database(d, r_value.ok(), false, "get_message_id_by_random_id");
        if (m != nullptr) {
          LOG_CHECK(m->random_id == random_id)
              << random_id << " " << m->random_id << " " << d->random_id_to_message_id[random_id] << " "
              << d->random_id_to_message_id[m->random_id] << " " << m->message_id << " " << source << " "
              << m->from_database << get_message(d, m->message_id) << " " << m << " "
              << debug_add_message_to_dialog_fail_reason_;
          LOG_CHECK(d->random_id_to_message_id.count(random_id))
              << source << " " << random_id << " " << m->message_id << " " << m->is_failed_to_send << " "
              << m->is_outgoing << " " << m->from_database << " " << get_message(d, m->message_id) << " " << m << " "
              << debug_add_message_to_dialog_fail_reason_;
          LOG_CHECK(d->random_id_to_message_id[random_id] == m->message_id)
              << source << " " << random_id << " " << d->random_id_to_message_id[random_id] << " " << m->message_id
              << " " << m->is_failed_to_send << " " << m->is_outgoing << " " << m->from_database << " "
              << get_message(d, m->message_id) << " " << m << " " << debug_add_message_to_dialog_fail_reason_;
          return m->message_id;
        }
      }
    }

    return MessageId();
  }

  return it->second;
}

void MessagesManager::remove_all_dialog_notifications(Dialog *d, bool from_mentions, const char *source) {
  // removes up to group_info.last_notification_id
  NotificationGroupInfo &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  if (group_info.group_id.is_valid() && group_info.last_notification_id.is_valid() &&
      group_info.max_removed_notification_id != group_info.last_notification_id) {
    VLOG(notifications) << "Set max_removed_notification_id in " << group_info.group_id << '/' << d->dialog_id << " to "
                        << group_info.last_notification_id << " from " << source;
    group_info.max_removed_notification_id = group_info.last_notification_id;
    if (d->max_notification_message_id > group_info.max_removed_message_id) {
      group_info.max_removed_message_id = d->max_notification_message_id.get_prev_server_message_id();
    }
    if (!d->pending_new_message_notifications.empty()) {
      for (auto &it : d->pending_new_message_notifications) {
        it.first = DialogId();
      }
      flush_pending_new_message_notifications(d->dialog_id, from_mentions, DialogId(UserId(static_cast<int64>(2))));
    }
    // remove_message_notifications will be called by NotificationManager
    send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification_group,
                       group_info.group_id, group_info.last_notification_id, MessageId(), 0, true, Promise<Unit>());
    if (d->new_secret_chat_notification_id.is_valid() && &group_info == &d->message_notification_group) {
      remove_new_secret_chat_notification(d, false);
    } else {
      bool is_changed = set_dialog_last_notification(d->dialog_id, group_info, 0, NotificationId(), source);
      CHECK(is_changed);
    }
  }
}

void MessagesManager::flush_pending_new_message_notifications(DialogId dialog_id, bool from_mentions,
                                                              DialogId settings_dialog_id) {
  // flush pending notifications even while closing

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  auto &pending_notifications =
      from_mentions ? d->pending_new_mention_notifications : d->pending_new_message_notifications;
  if (pending_notifications.empty()) {
    VLOG(notifications) << "Have no pending notifications in " << dialog_id << " to flush";
    return;
  }
  for (auto &it : pending_notifications) {
    if (it.first == settings_dialog_id || !settings_dialog_id.is_valid()) {
      it.first = DialogId();
    }
  }

  VLOG(notifications) << "Flush pending notifications in " << dialog_id
                      << " because of received notification settings in " << settings_dialog_id;
  auto it = pending_notifications.begin();
  while (it != pending_notifications.end() && it->first == DialogId()) {
    auto m = get_message(d, it->second);
    if (m != nullptr && add_new_message_notification(d, m, true)) {
      on_message_changed(d, m, false, "flush_pending_new_message_notifications");
    }
    ++it;
  }

  if (it == pending_notifications.end()) {
    reset_to_empty(pending_notifications);
  } else {
    pending_notifications.erase(pending_notifications.begin(), it);
  }
}

void MessagesManager::on_update_delete_scheduled_messages(DialogId dialog_id,
                                                          vector<ScheduledServerMessageId> &&server_message_ids) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive deleted scheduled messages in invalid " << dialog_id;
    return;
  }

  Dialog *d = get_dialog_force(dialog_id, "on_update_delete_scheduled_messages");
  if (d == nullptr) {
    LOG(INFO) << "Skip updateDeleteScheduledMessages in unknown " << dialog_id;
    return;
  }

  vector<int64> deleted_message_ids;
  for (auto server_message_id : server_message_ids) {
    if (!server_message_id.is_valid()) {
      LOG(ERROR) << "Incoming update tries to delete scheduled message " << server_message_id.get();
      continue;
    }

    auto message = do_delete_scheduled_message(d, MessageId(server_message_id, std::numeric_limits<int32>::max()), true,
                                               "on_update_delete_scheduled_messages");
    if (message != nullptr) {
      deleted_message_ids.push_back(message->message_id.get());
    }
  }

  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);

  send_update_chat_has_scheduled_messages(d, true);
}

Result<int32> MessagesManager::get_message_schedule_date(
    td_api::object_ptr<td_api::MessageSchedulingState> &&scheduling_state) {
  if (scheduling_state == nullptr) {
    return 0;
  }

  switch (scheduling_state->get_id()) {
    case td_api::messageSchedulingStateSendWhenOnline::ID: {
      auto send_date = SCHEDULE_WHEN_ONLINE_DATE;
      return send_date;
    }
    case td_api::messageSchedulingStateSendAtDate::ID: {
      auto send_at_date = td_api::move_object_as<td_api::messageSchedulingStateSendAtDate>(scheduling_state);
      auto send_date = send_at_date->send_date_;
      if (send_date <= 0) {
        return Status::Error(400, "Invalid send date specified");
      }
      if (send_date <= G()->unix_time() + 10) {
        return 0;
      }
      if (send_date - G()->unix_time() > 367 * 86400) {
        return Status::Error(400, "Send date is too far in the future");
      }
      return send_date;
    }
    default:
      UNREACHABLE();
      return 0;
  }
}

void MessagesManager::click_animated_emoji_message(FullMessageId full_message_id,
                                                   Promise<td_api::object_ptr<td_api::sticker>> &&promise) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "click_animated_emoji_message");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  auto message_id = get_persistent_message_id(d, full_message_id.get_message_id());
  auto *m = get_message_force(d, message_id, "click_animated_emoji_message");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (m->message_id.is_scheduled() || dialog_id.get_type() != DialogType::User || !m->message_id.is_server()) {
    return promise.set_value(nullptr);
  }

  get_message_content_animated_emoji_click_sticker(m->content.get(), full_message_id, td_, std::move(promise));
}

td_api::object_ptr<td_api::chatPosition> MessagesManager::get_chat_position_object(DialogListId dialog_list_id,
                                                                                   const Dialog *d) const {
  if (td_->auth_manager_->is_bot()) {
    return nullptr;
  }

  auto *list = get_dialog_list(dialog_list_id);
  if (list == nullptr) {
    return nullptr;
  }

  auto position = get_dialog_position_in_list(list, d);
  if (position.public_order == 0) {
    return nullptr;
  }

  auto chat_source = position.is_sponsored ? sponsored_dialog_source_.get_chat_source_object() : nullptr;
  return td_api::make_object<td_api::chatPosition>(dialog_list_id.get_chat_list_object(), position.public_order,
                                                   position.is_pinned, std::move(chat_source));
}

void MessagesManager::get_channel_difference_if_needed(DialogId dialog_id, MessagesInfo &&messages_info,
                                                       Promise<MessagesInfo> &&promise) {
  for (auto &message : messages_info.messages) {
    if (need_channel_difference_to_add_message(dialog_id, message)) {
      return run_after_channel_difference(
          dialog_id,
          PromiseCreator::lambda([messages_info = std::move(messages_info), promise = std::move(promise)](
                                     Unit ignored) mutable { promise.set_value(std::move(messages_info)); }));
    }
  }
  promise.set_value(std::move(messages_info));
}

void MessagesManager::send_update_message_content(const Dialog *d, Message *m, bool is_message_in_dialog,
                                                  const char *source) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  if (is_message_in_dialog) {
    delete_bot_command_message_id(d->dialog_id, m->message_id);
    try_add_bot_command_message_id(d->dialog_id, m);
    reregister_message_reply(d, m);
    update_message_max_reply_media_timestamp(d, m, false);  // because the message reply can be just registered
    update_message_max_own_media_timestamp(d, m);
  }
  send_update_message_content_impl(d->dialog_id, m, source);
}

MessagesManager::DialogList &MessagesManager::add_dialog_list(DialogListId dialog_list_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (dialog_list_id.is_folder() && dialog_list_id.get_folder_id() != FolderId::archive()) {
    dialog_list_id = DialogListId(FolderId::main());
  }
  if (dialog_lists_.find(dialog_list_id) == dialog_lists_.end()) {
    LOG(INFO) << "Create " << dialog_list_id;
  }
  auto &list = dialog_lists_[dialog_list_id];
  list.dialog_list_id = dialog_list_id;
  return list;
}

void MessagesManager::read_message_content_from_updates(MessageId message_id) {
  if (!message_id.is_valid() || !message_id.is_server()) {
    LOG(ERROR) << "Incoming update tries to read content of " << message_id;
    return;
  }

  Dialog *d = get_dialog_by_message_id(message_id);
  if (d != nullptr) {
    Message *m = get_message(d, message_id);
    CHECK(m != nullptr);
    read_message_content(d, m, false, "read_message_content_from_updates");
  }
}

void MessagesManager::hide_dialog_action_bar(Dialog *d) {
  CHECK(d->dialog_id.get_type() != DialogType::SecretChat);
  if (!d->know_action_bar) {
    return;
  }
  if (d->need_repair_action_bar) {
    d->need_repair_action_bar = false;
    on_dialog_updated(d->dialog_id, "hide_dialog_action_bar");
  }
  if (d->action_bar == nullptr) {
    return;
  }

  d->action_bar = nullptr;
  send_update_chat_action_bar(d);
}

vector<MessageId> MessagesManager::get_message_ids(const vector<int64> &input_message_ids) {
  vector<MessageId> message_ids;
  message_ids.reserve(input_message_ids.size());
  for (auto &input_message_id : input_message_ids) {
    message_ids.push_back(MessageId(input_message_id));
  }
  return message_ids;
}

void MessagesManager::on_message_reply_info_changed(DialogId dialog_id, const Message *m) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (is_visible_message_reply_info(dialog_id, m)) {
    send_update_message_interaction_info(dialog_id, m);
  }
}

MessagesManager::DialogListView MessagesManager::get_dialog_lists(const Dialog *d) {
  return DialogListView(this, get_dialog_list_ids(d));
}
}  // namespace td
