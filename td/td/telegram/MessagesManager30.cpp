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
#include "td/telegram/SponsoredMessageManager.h"
#include "td/telegram/Td.h"

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





#include <type_traits>




namespace td {
}  // namespace td
namespace td {

Status MessagesManager::view_messages(DialogId dialog_id, MessageId top_thread_message_id,
                                      const vector<MessageId> &message_ids, bool force_read) {
  CHECK(!td_->auth_manager_->is_bot());

  Dialog *d = get_dialog_force(dialog_id, "view_messages");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  for (auto message_id : message_ids) {
    if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
      if (message_id.is_valid_sponsored()) {
        if (d->is_opened) {
          td_->sponsored_message_manager_->view_sponsored_message(dialog_id, message_id);
        }
        continue;
      }
      return Status::Error(400, "Invalid message identifier");
    }
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }

  if (top_thread_message_id != MessageId()) {
    if (!top_thread_message_id.is_valid() || !top_thread_message_id.is_server()) {
      return Status::Error(400, "Invalid message thread ID specified");
    }
    if (dialog_id.get_type() != DialogType::Channel || is_broadcast_channel(dialog_id)) {
      return Status::Error(400, "There are no message threads in the chat");
    }
  }

  bool need_read = force_read || d->is_opened;
  MessageId max_message_id;  // max server or local viewed message_id
  vector<MessageId> read_content_message_ids;
  for (auto message_id : message_ids) {
    if (!message_id.is_valid()) {
      continue;
    }

    auto *m = get_message_force(d, message_id, "view_messages");
    if (m != nullptr) {
      if (m->message_id.is_server() && m->view_count > 0) {
        d->pending_viewed_message_ids.insert(m->message_id);
      }

      if (!m->message_id.is_yet_unsent() && m->message_id > max_message_id) {
        max_message_id = m->message_id;
      }

      auto message_content_type = m->content->get_type();
      if (message_content_type == MessageContentType::LiveLocation) {
        on_message_live_location_viewed(d, m);
      }

      if (need_read && message_content_type != MessageContentType::VoiceNote &&
          message_content_type != MessageContentType::VideoNote &&
          update_message_contains_unread_mention(d, m, false, "view_messages")) {
        CHECK(m->message_id.is_server());
        read_content_message_ids.push_back(m->message_id);
        on_message_changed(d, m, true, "view_messages");
      }
    } else if (!message_id.is_yet_unsent() && message_id > max_message_id &&
               message_id <= d->max_notification_message_id) {
      max_message_id = message_id;
    }
  }
  if (!d->pending_viewed_message_ids.empty()) {
    pending_message_views_timeout_.add_timeout_in(dialog_id.get(), MAX_MESSAGE_VIEW_DELAY);
    d->increment_view_counter |= d->is_opened;
  }
  if (!read_content_message_ids.empty()) {
    read_message_contents_on_server(dialog_id, std::move(read_content_message_ids), 0, Auto());
  }

  if (!need_read) {
    return Status::OK();
  }

  if (top_thread_message_id.is_valid()) {
    MessageId prev_last_read_inbox_message_id;
    MessageId max_thread_message_id;
    Message *top_m = get_message_force(d, top_thread_message_id, "view_messages 2");
    if (top_m != nullptr && is_active_message_reply_info(dialog_id, top_m->reply_info)) {
      prev_last_read_inbox_message_id = top_m->reply_info.last_read_inbox_message_id;
      if (top_m->reply_info.update_max_message_ids(MessageId(), max_message_id, MessageId())) {
        on_message_reply_info_changed(dialog_id, top_m);
        on_message_changed(d, top_m, true, "view_messages 3");
      }
      max_thread_message_id = top_m->reply_info.max_message_id;

      if (is_discussion_message(dialog_id, top_m)) {
        auto linked_dialog_id = top_m->forward_info->from_dialog_id;
        auto linked_d = get_dialog(linked_dialog_id);
        CHECK(linked_d != nullptr);
        CHECK(linked_dialog_id.get_type() == DialogType::Channel);
        auto *linked_m = get_message_force(linked_d, top_m->forward_info->from_message_id, "view_messages 4");
        if (linked_m != nullptr && is_active_message_reply_info(linked_dialog_id, linked_m->reply_info)) {
          if (linked_m->reply_info.last_read_inbox_message_id < prev_last_read_inbox_message_id) {
            prev_last_read_inbox_message_id = linked_m->reply_info.last_read_inbox_message_id;
          }
          if (linked_m->reply_info.update_max_message_ids(MessageId(), max_message_id, MessageId())) {
            on_message_reply_info_changed(linked_dialog_id, linked_m);
            on_message_changed(linked_d, linked_m, true, "view_messages 5");
          }
          if (linked_m->reply_info.max_message_id > max_thread_message_id) {
            max_thread_message_id = linked_m->reply_info.max_message_id;
          }
        }
      }
    }

    if (max_message_id.get_prev_server_message_id().get() >
        prev_last_read_inbox_message_id.get_prev_server_message_id().get()) {
      read_message_thread_history_on_server(d, top_thread_message_id, max_message_id.get_prev_server_message_id(),
                                            max_thread_message_id.get_prev_server_message_id());
    }

    return Status::OK();
  }

  if (max_message_id > d->last_read_inbox_message_id) {
    const MessageId last_read_message_id = max_message_id;
    const MessageId prev_last_read_inbox_message_id = d->last_read_inbox_message_id;
    MessageId read_history_on_server_message_id;
    if (dialog_id.get_type() != DialogType::SecretChat) {
      if (last_read_message_id.get_prev_server_message_id().get() >
          prev_last_read_inbox_message_id.get_prev_server_message_id().get()) {
        read_history_on_server_message_id = last_read_message_id.get_prev_server_message_id();
      }
    } else {
      if (last_read_message_id > prev_last_read_inbox_message_id) {
        read_history_on_server_message_id = last_read_message_id;
      }
    }

    if (read_history_on_server_message_id.is_valid()) {
      // add dummy timeout to not try to repair unread_count in read_history_inbox before server request succeeds
      // the timeout will be overwritten in the read_history_on_server call
      pending_read_history_timeout_.add_timeout_in(dialog_id.get(), 0);
    }
    read_history_inbox(d->dialog_id, last_read_message_id, -1, "view_messages");
    if (read_history_on_server_message_id.is_valid()) {
      // call read_history_on_server after read_history_inbox to not have delay before request if all messages are read
      read_history_on_server(d, read_history_on_server_message_id);
    }
  }
  if (d->is_marked_as_unread) {
    set_dialog_is_marked_as_unread(d, false);
  }

  return Status::OK();
}

class UpdatePeerSettingsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit UpdatePeerSettingsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, bool is_spam_dialog) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_value(Unit());
    }

    if (is_spam_dialog) {
      send_query(G()->net_query_creator().create(telegram_api::messages_reportSpam(std::move(input_peer))));
    } else {
      send_query(G()->net_query_creator().create(telegram_api::messages_hidePeerSettingsBar(std::move(input_peer))));
    }
  }

  void on_result(BufferSlice packet) final {
    static_assert(std::is_same<telegram_api::messages_reportSpam::ReturnType,
                               telegram_api::messages_hidePeerSettingsBar::ReturnType>::value,
                  "");
    auto result_ptr = fetch_result<telegram_api::messages_reportSpam>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->messages_manager_->on_get_peer_settings(dialog_id_, make_tl_object<telegram_api::peerSettings>(), true);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for update peer settings: " << status;
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "UpdatePeerSettingsQuery");
    td_->messages_manager_->reget_dialog_action_bar(dialog_id_, "UpdatePeerSettingsQuery");
    promise_.set_error(std::move(status));
  }
};

class ReportEncryptedSpamQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit ReportEncryptedSpamQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_encrypted_chat(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    send_query(G()->net_query_creator().create(telegram_api::messages_reportEncryptedSpam(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_reportEncryptedSpam>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->messages_manager_->on_get_peer_settings(dialog_id_, make_tl_object<telegram_api::peerSettings>(), true);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for report encrypted spam: " << status;
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ReportEncryptedSpamQuery");
    td_->messages_manager_->reget_dialog_action_bar(
        DialogId(td_->contacts_manager_->get_secret_chat_user_id(dialog_id_.get_secret_chat_id())),
        "ReportEncryptedSpamQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::toggle_dialog_report_spam_state_on_server(DialogId dialog_id, bool is_spam_dialog,
                                                                uint64 log_event_id, Promise<Unit> &&promise) {
  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_toggle_dialog_report_spam_state_on_server_log_event(dialog_id, is_spam_dialog);
  }

  auto new_promise = get_erase_log_event_promise(log_event_id, std::move(promise));
  promise = std::move(new_promise);  // to prevent self-move

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      return td_->create_handler<UpdatePeerSettingsQuery>(std::move(promise))->send(dialog_id, is_spam_dialog);
    case DialogType::SecretChat:
      if (is_spam_dialog) {
        return td_->create_handler<ReportEncryptedSpamQuery>(std::move(promise))->send(dialog_id);
      } else {
        auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
        if (!user_id.is_valid()) {
          return promise.set_error(Status::Error(400, "Peer user not found"));
        }
        return td_->create_handler<UpdatePeerSettingsQuery>(std::move(promise))->send(DialogId(user_id), false);
      }
    case DialogType::None:
    default:
      UNREACHABLE();
      return;
  }
}

class UpdateDialogPinnedMessageQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit UpdateDialogPinnedMessageQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId message_id, bool is_unpin, bool disable_notification, bool only_for_self) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      LOG(INFO) << "Can't update pinned message in " << dialog_id;
      return on_error(Status::Error(400, "Can't update pinned message"));
    }

    int32 flags = 0;
    if (disable_notification) {
      flags |= telegram_api::messages_updatePinnedMessage::SILENT_MASK;
    }
    if (is_unpin) {
      flags |= telegram_api::messages_updatePinnedMessage::UNPIN_MASK;
    }
    if (only_for_self) {
      flags |= telegram_api::messages_updatePinnedMessage::PM_ONESIDE_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_updatePinnedMessage(flags, false /*ignored*/, false /*ignored*/, false /*ignored*/,
                                                   std::move(input_peer), message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_updatePinnedMessage>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for UpdateDialogPinnedMessageQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "UpdateDialogPinnedMessageQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::pin_dialog_message(DialogId dialog_id, MessageId message_id, bool disable_notification,
                                         bool only_for_self, bool is_unpin, Promise<Unit> &&promise) {
  auto d = get_dialog_force(dialog_id, "pin_dialog_message");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  TRY_STATUS_PROMISE(promise, can_pin_messages(dialog_id));

  const Message *m = get_message_force(d, message_id, "pin_dialog_message");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Scheduled message can't be pinned"));
  }
  if (!message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Message can't be pinned"));
  }

  if (is_service_message_content(m->content->get_type())) {
    return promise.set_error(Status::Error(400, "A service message can't be pinned"));
  }

  if (only_for_self && dialog_id.get_type() != DialogType::User) {
    return promise.set_error(Status::Error(400, "Messages can't be pinned only for self in the chat"));
  }

  // TODO log event
  td_->create_handler<UpdateDialogPinnedMessageQuery>(std::move(promise))
      ->send(dialog_id, message_id, is_unpin, disable_notification, only_for_self);
}

class SendScreenshotNotificationQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  int64 random_id_;
  DialogId dialog_id_;

 public:
  explicit SendScreenshotNotificationQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int64 random_id) {
    random_id_ = random_id;
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);

    auto query = G()->net_query_creator().create(
        telegram_api::messages_sendScreenshotNotification(std::move(input_peer), 0, random_id));
    send_query(std::move(query));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_sendScreenshotNotification>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SendScreenshotNotificationQuery for " << random_id_ << ": " << to_string(ptr);
    td_->messages_manager_->check_send_message_result(random_id_, dialog_id_, ptr.get(),
                                                      "SendScreenshotNotificationQuery");
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for SendScreenshotNotificationQuery: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, messages should be re-sent
      return;
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendScreenshotNotificationQuery");
    td_->messages_manager_->on_send_message_fail(random_id_, status.clone());
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::do_send_screenshot_taken_notification_message(DialogId dialog_id, const Message *m,
                                                                    uint64 log_event_id) {
  LOG(INFO) << "Do send screenshot taken notification " << FullMessageId(dialog_id, m->message_id);
  CHECK(dialog_id.get_type() == DialogType::User);

  if (log_event_id == 0) {
    log_event_id = save_send_screenshot_taken_notification_message_log_event(dialog_id, m);
  }

  int64 random_id = begin_send_message(dialog_id, m);
  td_->create_handler<SendScreenshotNotificationQuery>(get_erase_log_event_promise(log_event_id))
      ->send(dialog_id, random_id);
}

class GetDialogNotifySettingsQuery final : public Td::ResultHandler {
  DialogId dialog_id_;

 public:
  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;
    auto input_notify_peer = td_->messages_manager_->get_input_notify_peer(dialog_id);
    CHECK(input_notify_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::account_getNotifySettings(std::move(input_notify_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_getNotifySettings>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    td_->messages_manager_->on_update_dialog_notify_settings(dialog_id_, std::move(ptr),
                                                             "GetDialogNotifySettingsQuery");
    td_->messages_manager_->on_get_dialog_notification_settings_query_finished(dialog_id_, Status::OK());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetDialogNotifySettingsQuery");
    td_->messages_manager_->on_get_dialog_notification_settings_query_finished(dialog_id_, std::move(status));
  }
};

void MessagesManager::send_get_dialog_notification_settings_query(DialogId dialog_id, Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot() || dialog_id.get_type() == DialogType::SecretChat) {
    LOG(WARNING) << "Can't get notification settings for " << dialog_id;
    return promise.set_error(Status::Error(500, "Wrong getDialogNotificationSettings query"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    LOG(WARNING) << "Have no access to " << dialog_id << " to get notification settings";
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  auto &promises = get_dialog_notification_settings_queries_[dialog_id];
  promises.push_back(std::move(promise));
  if (promises.size() != 1) {
    // query has already been sent, just wait for the result
    return;
  }

  td_->create_handler<GetDialogNotifySettingsQuery>()->send(dialog_id);
}

bool MessagesManager::can_set_game_score(DialogId dialog_id, const Message *m) const {
  if (m == nullptr) {
    return false;
  }
  if (m->content->get_type() != MessageContentType::Game) {
    return false;
  }
  if (m->message_id.is_scheduled()) {
    return false;
  }
  if (m->message_id.is_yet_unsent()) {
    return false;
  }
  if (m->message_id.is_local()) {
    return false;
  }
  if (m->via_bot_user_id.is_valid() && m->via_bot_user_id != td_->contacts_manager_->get_my_id()) {
    return false;
  }

  if (!td_->auth_manager_->is_bot()) {
    return false;
  }
  if (m->reply_markup == nullptr || m->reply_markup->type != ReplyMarkup::Type::InlineKeyboard ||
      m->reply_markup->inline_keyboard.empty()) {
    return false;
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (!m->is_outgoing && dialog_id != get_my_dialog_id()) {
        return false;
      }
      break;
    case DialogType::Chat:
      if (!m->is_outgoing) {
        return false;
      }
      break;
    case DialogType::Channel: {
      if (m->via_bot_user_id.is_valid()) {
        // outgoing via_bot messages can always be edited
        break;
      }
      auto channel_id = dialog_id.get_channel_id();
      auto channel_status = td_->contacts_manager_->get_channel_permissions(channel_id);
      if (m->is_channel_post) {
        if (!channel_status.can_edit_messages() && !(channel_status.can_post_messages() && m->is_outgoing)) {
          return false;
        }
      } else {
        if (!m->is_outgoing) {
          return false;
        }
      }
      break;
    }
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }

  return true;
}

bool MessagesManager::delete_newer_server_messages_at_the_end(Dialog *d, MessageId max_message_id) {
  vector<MessageId> message_ids;
  find_newer_messages(d->messages.get(), max_message_id, message_ids);
  if (message_ids.empty()) {
    return false;
  }

  bool need_update_dialog_pos = false;
  vector<int64> deleted_message_ids;
  for (auto message_id : message_ids) {
    CHECK(message_id > max_message_id);
    if (message_id.is_server()) {
      auto message =
          delete_message(d, message_id, true, &need_update_dialog_pos, "delete_newer_server_messages_at_the_end 1");
      CHECK(message != nullptr);
      deleted_message_ids.push_back(message->message_id.get());
    }
  }
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "delete_newer_server_messages_at_the_end 2");
  }

  if (!deleted_message_ids.empty()) {
    send_update_delete_messages(d->dialog_id, std::move(deleted_message_ids), true, false);

    message_ids.clear();
    find_newer_messages(d->messages.get(), max_message_id, message_ids);
  }

  // connect all messages with ID > max_message_id
  for (size_t i = 0; i + 1 < message_ids.size(); i++) {
    auto m = get_message(d, message_ids[i]);
    CHECK(m != nullptr);
    if (!m->have_next) {
      m->have_next = true;
      attach_message_to_next(d, message_ids[i], "delete_newer_server_messages_at_the_end 3");
    }
  }

  return !message_ids.empty();
}

Result<FullMessageId> MessagesManager::get_top_thread_full_message_id(DialogId dialog_id, const Message *m) const {
  CHECK(m != nullptr);
  if (m->message_id.is_scheduled()) {
    return Status::Error(400, "Message is scheduled");
  }
  if (dialog_id.get_type() != DialogType::Channel) {
    return Status::Error(400, "Chat can't have message threads");
  }
  if (!m->reply_info.is_empty() && m->reply_info.is_comment) {
    if (!is_visible_message_reply_info(dialog_id, m)) {
      return Status::Error(400, "Message has no comments");
    }
    if (m->message_id.is_yet_unsent()) {
      return Status::Error(400, "Message is not sent yet");
    }
    return FullMessageId{DialogId(m->reply_info.channel_id), m->linked_top_thread_message_id};
  } else {
    if (!m->top_thread_message_id.is_valid()) {
      return Status::Error(400, "Message has no thread");
    }
    if (!m->message_id.is_server()) {
      return Status::Error(400, "Message thread is unavailable for the message");
    }
    if (m->top_thread_message_id != m->message_id &&
        !td_->contacts_manager_->get_channel_has_linked_channel(dialog_id.get_channel_id())) {
      return Status::Error(400, "Root message must be used to get the message thread");
    }
    return FullMessageId{dialog_id, m->top_thread_message_id};
  }
}

void MessagesManager::set_poll_answer(FullMessageId full_message_id, vector<int32> &&option_ids,
                                      Promise<Unit> &&promise) {
  auto m = get_message_force(full_message_id, "set_poll_answer");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (!have_input_peer(full_message_id.get_dialog_id(), AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }
  if (m->content->get_type() != MessageContentType::Poll) {
    return promise.set_error(Status::Error(400, "Message is not a poll"));
  }
  if (m->message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Can't answer polls from scheduled messages"));
  }
  if (!m->message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Poll can't be answered"));
  }

  set_message_content_poll_answer(td_, m->content.get(), full_message_id, std::move(option_ids), std::move(promise));
}

Status MessagesManager::open_message_content(FullMessageId full_message_id) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "open_message_content");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  auto *m = get_message_force(d, full_message_id.get_message_id(), "open_message_content");
  if (m == nullptr) {
    return Status::Error(400, "Message not found");
  }

  if (m->message_id.is_scheduled() || m->message_id.is_yet_unsent() || m->is_outgoing) {
    return Status::OK();
  }

  if (read_message_content(d, m, true, "open_message_content") &&
      (m->message_id.is_server() || dialog_id.get_type() == DialogType::SecretChat)) {
    read_message_contents_on_server(dialog_id, {m->message_id}, 0, Auto());
  }

  if (m->content->get_type() == MessageContentType::LiveLocation) {
    on_message_live_location_viewed(d, m);
  }

  return Status::OK();
}

void MessagesManager::on_update_live_location_viewed(FullMessageId full_message_id) {
  LOG(DEBUG) << "Live location was viewed in " << full_message_id;
  if (!are_active_live_location_messages_loaded_) {
    get_active_live_location_messages(PromiseCreator::lambda([actor_id = actor_id(this), full_message_id](Unit result) {
      send_closure(actor_id, &MessagesManager::on_update_live_location_viewed, full_message_id);
    }));
    return;
  }

  auto active_live_location_message_ids = get_active_live_location_messages(Auto());
  if (!td::contains(active_live_location_message_ids, full_message_id)) {
    LOG(DEBUG) << "Can't find " << full_message_id << " in " << active_live_location_message_ids;
    return;
  }

  send_update_message_live_location_viewed(full_message_id);
}

void MessagesManager::delete_update_message_id(DialogId dialog_id, MessageId message_id) {
  if (message_id.is_scheduled()) {
    CHECK(message_id.is_scheduled_server());
    auto dialog_it = update_scheduled_message_ids_.find(dialog_id);
    CHECK(dialog_it != update_scheduled_message_ids_.end());
    auto erased_count = dialog_it->second.erase(message_id.get_scheduled_server_message_id());
    CHECK(erased_count > 0);
    if (dialog_it->second.empty()) {
      update_scheduled_message_ids_.erase(dialog_it);
    }
  } else {
    CHECK(message_id.is_server());
    auto erased_count = update_message_ids_.erase(FullMessageId(dialog_id, message_id));
    CHECK(erased_count > 0);
  }
}

void MessagesManager::set_dialog_first_database_message_id(Dialog *d, MessageId first_database_message_id,
                                                           const char *source) {
  CHECK(!first_database_message_id.is_scheduled());
  if (first_database_message_id == d->first_database_message_id) {
    return;
  }

  LOG(INFO) << "Set " << d->dialog_id << " first database message to " << first_database_message_id << " from "
            << source;
  d->first_database_message_id = first_database_message_id;
  on_dialog_updated(d->dialog_id, "set_dialog_first_database_message_id");
}

void MessagesManager::delete_random_id_to_message_id_correspondence(Dialog *d, int64 random_id, MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(d->dialog_id.get_type() == DialogType::SecretChat);
  CHECK(message_id.is_valid());
  auto it = d->random_id_to_message_id.find(random_id);
  if (it != d->random_id_to_message_id.end() && it->second == message_id) {
    LOG(INFO) << "Delete correspondence from random_id " << random_id << " to " << message_id << " in " << d->dialog_id;
    d->random_id_to_message_id.erase(it);
  }
}

bool MessagesManager::is_dialog_mention_notifications_disabled(const Dialog *d) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(d != nullptr);
  if (d->notification_settings.use_default_disable_mention_notifications) {
    auto scope = get_dialog_notification_setting_scope(d->dialog_id);
    return get_scope_notification_settings(scope)->disable_mention_notifications;
  }

  return d->notification_settings.disable_mention_notifications;
}

const MessagesManager::DialogList *MessagesManager::get_dialog_list(DialogListId dialog_list_id) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (dialog_list_id.is_folder() && dialog_list_id.get_folder_id() != FolderId::archive()) {
    dialog_list_id = DialogListId(FolderId::main());
  }
  auto it = dialog_lists_.find(dialog_list_id);
  if (it == dialog_lists_.end()) {
    return nullptr;
  }
  return &it->second;
}

bool MessagesManager::is_old_channel_update(DialogId dialog_id, int32 new_pts) {
  CHECK(dialog_id.get_type() == DialogType::Channel);

  const Dialog *d = get_dialog_force(dialog_id, "is_old_channel_update");
  return new_pts <= (d == nullptr ? load_channel_pts(dialog_id) : d->pts);
}

MessagesManager::NotificationGroupInfo &MessagesManager::get_notification_group_info(Dialog *d, const Message *m) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  return is_from_mention_notification_group(d, m) ? d->mention_notification_group : d->message_notification_group;
}

void MessagesManager::fail_send_message(FullMessageId full_message_id, Status error) {
  fail_send_message(full_message_id, error.code(), error.message().str());
}
}  // namespace td
