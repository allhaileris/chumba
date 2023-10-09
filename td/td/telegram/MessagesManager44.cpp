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

#include "td/telegram/InlineQueriesManager.h"
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

#include <algorithm>
#include <cstring>
#include <limits>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

Result<MessageId> MessagesManager::add_local_message(
    DialogId dialog_id, td_api::object_ptr<td_api::MessageSender> &&sender, MessageId reply_to_message_id,
    bool disable_notification, tl_object_ptr<td_api::InputMessageContent> &&input_message_content) {
  if (input_message_content == nullptr) {
    return Status::Error(400, "Can't add local message without content");
  }

  LOG(INFO) << "Begin to add local message to " << dialog_id << " in reply to " << reply_to_message_id;
  Dialog *d = get_dialog_force(dialog_id, "add_local_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }
  TRY_RESULT(message_content, process_input_message_content(dialog_id, std::move(input_message_content)));
  if (message_content.content->get_type() == MessageContentType::Poll) {
    return Status::Error(400, "Can't add local poll message");
  }
  if (message_content.content->get_type() == MessageContentType::Game) {
    return Status::Error(400, "Can't add local game message");
  }
  if (message_content.content->get_type() == MessageContentType::Dice) {
    return Status::Error(400, "Can't add local dice message");
  }

  bool is_channel_post = is_broadcast_channel(dialog_id);
  UserId sender_user_id;
  DialogId sender_dialog_id;
  if (sender != nullptr) {
    TRY_RESULT_ASSIGN(sender_dialog_id, get_message_sender_dialog_id(td_, sender, true, false));
    auto sender_dialog_type = sender_dialog_id.get_type();
    if (sender_dialog_type == DialogType::User) {
      sender_user_id = sender_dialog_id.get_user_id();
      sender_dialog_id = DialogId();
    } else if (sender_dialog_type != DialogType::Channel) {
      return Status::Error(400, "Sender chat must be a supergroup or channel");
    }
  } else if (is_channel_post) {
    sender_dialog_id = dialog_id;
  } else {
    return Status::Error(400, "The message must have a sender");
  }
  if (is_channel_post && sender_user_id.is_valid()) {
    return Status::Error(400, "Channel post can't have a sender user");
  }
  if (is_channel_post && sender_dialog_id != dialog_id) {
    return Status::Error(400, "Channel post must have the channel as a sender");
  }

  auto dialog_type = dialog_id.get_type();
  auto my_id = td_->contacts_manager_->get_my_id();
  if (sender_user_id != my_id) {
    if (dialog_type == DialogType::User && DialogId(sender_user_id) != dialog_id) {
      return Status::Error(400, "Wrong sender user");
    }
    if (dialog_type == DialogType::SecretChat) {
      auto peer_user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      if (!peer_user_id.is_valid() || sender_user_id != peer_user_id) {
        return Status::Error(400, "Wrong sender user");
      }
    }
  }

  MessageId message_id = get_next_local_message_id(d);

  auto m = make_unique<Message>();
  set_message_id(m, message_id);
  if (is_channel_post) {
    // sender of the post can be hidden
    if (td_->contacts_manager_->get_channel_sign_messages(dialog_id.get_channel_id())) {
      m->author_signature = td_->contacts_manager_->get_user_title(sender_user_id);
    }
    m->sender_dialog_id = sender_dialog_id;
  } else {
    m->sender_user_id = sender_user_id;
    m->sender_dialog_id = sender_dialog_id;
  }
  m->date = G()->unix_time();
  m->reply_to_message_id = get_reply_to_message_id(d, MessageId(), reply_to_message_id, false);
  if (m->reply_to_message_id.is_valid() && !message_id.is_scheduled()) {
    const Message *reply_m = get_message(d, m->reply_to_message_id);
    if (reply_m != nullptr) {
      m->top_thread_message_id = reply_m->top_thread_message_id;
    }
  }
  m->is_channel_post = is_channel_post;
  m->is_outgoing = dialog_id != DialogId(my_id) && sender_user_id == my_id;
  m->disable_notification = disable_notification;
  m->from_background = false;
  m->view_count = 0;
  m->forward_count = 0;
  m->content = std::move(message_content.content);
  m->disable_web_page_preview = message_content.disable_web_page_preview;
  m->clear_draft = message_content.clear_draft;
  if (dialog_type == DialogType::SecretChat) {
    m->ttl = td_->contacts_manager_->get_secret_chat_ttl(dialog_id.get_secret_chat_id());
    if (is_service_message_content(m->content->get_type())) {
      m->ttl = 0;
    }
  } else if (message_content.ttl > 0) {
    m->ttl = message_content.ttl;
  }
  m->is_content_secret = is_secret_message_content(m->ttl, m->content->get_type());
  m->send_emoji = std::move(message_content.emoji);

  m->have_previous = true;
  m->have_next = true;

  bool need_update = true;
  bool need_update_dialog_pos = false;
  auto result =
      add_message_to_dialog(d, std::move(m), true, &need_update, &need_update_dialog_pos, "add local message");
  LOG_CHECK(result != nullptr) << message_id << " " << debug_add_message_to_dialog_fail_reason_;
  register_new_local_message_id(d, result);

  if (is_message_auto_read(dialog_id, result->is_outgoing)) {
    if (result->is_outgoing) {
      read_history_outbox(dialog_id, message_id);
    } else {
      read_history_inbox(dialog_id, message_id, 0, "add_local_message");
    }
  }

  if (message_content.clear_draft) {
    update_dialog_draft_message(d, nullptr, false, !need_update_dialog_pos);
  }

  send_update_new_message(d, result);
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "add_local_message");
  }

  return message_id;
}

void MessagesManager::set_dialog_last_read_inbox_message_id(Dialog *d, MessageId message_id, int32 server_unread_count,
                                                            int32 local_unread_count, bool force_update,
                                                            const char *source) {
  CHECK(!message_id.is_scheduled());

  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG(INFO) << "Update last read inbox message in " << d->dialog_id << " from " << d->last_read_inbox_message_id
            << " to " << message_id << " and update unread message count from " << d->server_unread_count << " + "
            << d->local_unread_count << " to " << server_unread_count << " + " << local_unread_count << " from "
            << source;
  if (message_id != MessageId::min()) {
    d->last_read_inbox_message_id = message_id;
    d->is_last_read_inbox_message_id_inited = true;
  }
  int32 old_unread_count = d->server_unread_count + d->local_unread_count;
  d->server_unread_count = server_unread_count;
  d->local_unread_count = local_unread_count;

  if (need_unread_counter(d->order)) {
    const int32 new_unread_count = d->server_unread_count + d->local_unread_count;
    for (auto &list : get_dialog_lists(d)) {
      int32 delta = new_unread_count - old_unread_count;
      if (delta != 0 && list.is_message_unread_count_inited_) {
        list.unread_message_total_count_ += delta;
        if (is_dialog_muted(d)) {
          list.unread_message_muted_count_ += delta;
        }
        send_update_unread_message_count(list, d->dialog_id, force_update, source);
      }
      delta = static_cast<int32>(new_unread_count != 0) - static_cast<int32>(old_unread_count != 0);
      if (delta != 0 && list.is_dialog_unread_count_inited_) {
        if (d->is_marked_as_unread) {
          list.unread_dialog_marked_count_ -= delta;
        } else {
          list.unread_dialog_total_count_ += delta;
        }
        if (is_dialog_muted(d)) {
          if (d->is_marked_as_unread) {
            list.unread_dialog_muted_marked_count_ -= delta;
          } else {
            list.unread_dialog_muted_count_ += delta;
          }
        }
        send_update_unread_chat_count(list, d->dialog_id, force_update, source);
      }
    }

    bool was_unread = old_unread_count != 0 || d->is_marked_as_unread;
    bool is_unread = new_unread_count != 0 || d->is_marked_as_unread;
    if (!dialog_filters_.empty() && was_unread != is_unread) {
      update_dialog_lists(d, get_dialog_positions(d), true, false, "set_dialog_last_read_inbox_message_id");
    }
  }

  if (message_id != MessageId::min() && d->last_read_inbox_message_id.is_valid() &&
      (d->order != DEFAULT_ORDER || is_dialog_sponsored(d))) {
    VLOG(notifications) << "Remove some notifications in " << d->dialog_id
                        << " after updating last read inbox message to " << message_id
                        << " and unread message count to " << server_unread_count << " + " << local_unread_count
                        << " from " << source;
    if (d->message_notification_group.group_id.is_valid()) {
      auto total_count = get_dialog_pending_notification_count(d, false);
      if (total_count == 0) {
        set_dialog_last_notification(d->dialog_id, d->message_notification_group, 0, NotificationId(), source);
      }
      if (!d->pending_new_message_notifications.empty()) {
        for (auto &it : d->pending_new_message_notifications) {
          if (it.second <= message_id) {
            it.first = DialogId();
          }
        }
        flush_pending_new_message_notifications(d->dialog_id, false, DialogId(UserId(static_cast<int64>(1))));
      }
      total_count -= static_cast<int32>(d->pending_new_message_notifications.size());
      if (total_count < 0) {
        LOG(ERROR) << "Total message notification count is " << total_count << " in " << d->dialog_id
                   << " with old unread_count = " << old_unread_count << " and " << d->pending_new_message_notifications
                   << " pending new message notifications after reading history up to " << message_id;
        total_count = 0;
      }
      send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification_group,
                         d->message_notification_group.group_id, NotificationId(), d->last_read_inbox_message_id,
                         total_count, Slice(source) == Slice("view_messages"), Promise<Unit>());
    }

    if (d->mention_notification_group.group_id.is_valid() && d->pinned_message_notification_message_id.is_valid() &&
        d->pinned_message_notification_message_id <= d->last_read_inbox_message_id) {
      // remove pinned message notification when it is read
      remove_dialog_pinned_message_notification(d, source);
    }
  }

  send_update_chat_read_inbox(d, force_update, source);
}

Result<MessageId> MessagesManager::send_inline_query_result_message(DialogId dialog_id, MessageId top_thread_message_id,
                                                                    MessageId reply_to_message_id,
                                                                    tl_object_ptr<td_api::messageSendOptions> &&options,
                                                                    int64 query_id, const string &result_id,
                                                                    bool hide_via_bot) {
  LOG(INFO) << "Begin to send inline query result message to " << dialog_id << " in reply to " << reply_to_message_id;

  Dialog *d = get_dialog_force(dialog_id, "send_inline_query_result_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  TRY_STATUS(can_send_message(dialog_id));
  TRY_RESULT(message_send_options, process_message_send_options(dialog_id, std::move(options)));
  bool to_secret = false;
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
      // ok
      break;
    case DialogType::Channel: {
      auto channel_status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      if (!channel_status.can_use_inline_bots()) {
        return Status::Error(400, "Can't use inline bots in the chat");
      }
      break;
    }
    case DialogType::SecretChat:
      to_secret = true;
      // ok
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  const InlineMessageContent *content = td_->inline_queries_manager_->get_inline_message_content(query_id, result_id);
  if (content == nullptr) {
    return Status::Error(400, "Inline query result not found");
  }

  reply_to_message_id = get_reply_to_message_id(d, top_thread_message_id, reply_to_message_id, false);
  TRY_STATUS(can_use_message_send_options(message_send_options, content->message_content, 0));
  TRY_STATUS(can_send_message_content(dialog_id, content->message_content.get(), false, td_));
  TRY_STATUS(can_use_top_thread_message_id(d, top_thread_message_id, reply_to_message_id));

  bool need_update_dialog_pos = false;
  Message *m = get_message_to_send(d, top_thread_message_id, reply_to_message_id, message_send_options,
                                   dup_message_content(td_, dialog_id, content->message_content.get(),
                                                       MessageContentDupType::SendViaBot, MessageCopyOptions()),
                                   &need_update_dialog_pos, false, nullptr, true);
  m->hide_via_bot = hide_via_bot;
  if (!hide_via_bot) {
    m->via_bot_user_id = td_->inline_queries_manager_->get_inline_bot_user_id(query_id);
  }
  if (content->message_reply_markup != nullptr && !to_secret) {
    m->reply_markup = make_unique<ReplyMarkup>(*content->message_reply_markup);
  }
  m->disable_web_page_preview = content->disable_web_page_preview;
  m->clear_draft = true;

  if (top_thread_message_id.is_valid()) {
    set_dialog_draft_message(dialog_id, top_thread_message_id, nullptr).ignore();
  } else {
    update_dialog_draft_message(d, nullptr, false, !need_update_dialog_pos);
  }

  send_update_new_message(d, m);
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "send_inline_query_result_message");
  }

  if (to_secret) {
    save_send_message_log_event(dialog_id, m);
    do_send_message(dialog_id, m);
    return m->message_id;
  }

  save_send_inline_query_result_message_log_event(dialog_id, m, query_id, result_id);
  do_send_inline_query_result_message(dialog_id, m, query_id, result_id);
  return m->message_id;
}

class ReadDiscussionQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit ReadDiscussionQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId top_thread_message_id, MessageId max_message_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::messages_readDiscussion(
        std::move(input_peer), top_thread_message_id.get_server_message_id().get(),
        max_message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_readDiscussion>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ReadDiscussionQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::read_message_thread_history_on_server_impl(Dialog *d, MessageId top_thread_message_id,
                                                                 MessageId max_message_id) {
  CHECK(d != nullptr);
  auto dialog_id = d->dialog_id;
  CHECK(dialog_id.get_type() == DialogType::Channel);

  const Message *m = get_message_force(d, top_thread_message_id, "read_message_thread_history_on_server_impl");
  if (m != nullptr) {
    auto message_id = m->reply_info.last_read_inbox_message_id.get_prev_server_message_id();
    if (message_id > max_message_id) {
      max_message_id = message_id;
    }
  }

  Promise<> promise;
  if (d->read_history_log_event_ids[top_thread_message_id.get()].log_event_id != 0) {
    d->read_history_log_event_ids[top_thread_message_id.get()].generation++;
    promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), dialog_id, top_thread_message_id,
         generation = d->read_history_log_event_ids[top_thread_message_id.get()].generation](Result<Unit> result) {
          if (!G()->close_flag()) {
            send_closure(actor_id, &MessagesManager::on_read_history_finished, dialog_id, top_thread_message_id,
                         generation);
          }
        });
  }

  if (!max_message_id.is_valid() || !have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_value(Unit());
  }

  LOG(INFO) << "Send read history request in thread of " << top_thread_message_id << " in " << dialog_id << " up to "
            << max_message_id;
  td_->create_handler<ReadDiscussionQuery>(std::move(promise))->send(dialog_id, top_thread_message_id, max_message_id);
}

bool MessagesManager::can_revoke_message(DialogId dialog_id, const Message *m) const {
  if (m == nullptr) {
    return true;
  }
  if (m->message_id.is_local()) {
    return false;
  }
  if (dialog_id == get_my_dialog_id()) {
    return false;
  }
  if (m->message_id.is_scheduled()) {
    return false;
  }
  if (m->message_id.is_yet_unsent()) {
    return true;
  }
  CHECK(m->message_id.is_server());

  const int32 DEFAULT_REVOKE_TIME_LIMIT = td_->auth_manager_->is_bot() ? 2 * 86400 : std::numeric_limits<int32>::max();
  auto content_type = m->content->get_type();
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      bool can_revoke_incoming = G()->shared_config().get_option_boolean("revoke_pm_inbox", true);
      int64 revoke_time_limit =
          G()->shared_config().get_option_integer("revoke_pm_time_limit", DEFAULT_REVOKE_TIME_LIMIT);

      if (G()->unix_time_cached() - m->date < 86400 && content_type == MessageContentType::Dice) {
        return false;
      }
      return ((m->is_outgoing && !is_service_message_content(content_type)) ||
              (can_revoke_incoming && content_type != MessageContentType::ScreenshotTaken)) &&
             G()->unix_time_cached() - m->date <= revoke_time_limit;
    }
    case DialogType::Chat: {
      bool is_appointed_administrator =
          td_->contacts_manager_->is_appointed_chat_administrator(dialog_id.get_chat_id());
      int64 revoke_time_limit = G()->shared_config().get_option_integer("revoke_time_limit", DEFAULT_REVOKE_TIME_LIMIT);

      return ((m->is_outgoing && !is_service_message_content(content_type)) || is_appointed_administrator) &&
             G()->unix_time_cached() - m->date <= revoke_time_limit;
    }
    case DialogType::Channel:
      return true;  // any server message that can be deleted will be deleted for all participants
    case DialogType::SecretChat:
      // all non-service messages will be deleted for everyone if secret chat is active
      return td_->contacts_manager_->get_secret_chat_state(dialog_id.get_secret_chat_id()) == SecretChatState::Active &&
             !is_service_message_content(content_type);
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }
}

void MessagesManager::send_update_chat_has_scheduled_messages(Dialog *d, bool from_deletion) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (d->scheduled_messages == nullptr) {
    if (d->has_scheduled_database_messages) {
      if (d->has_loaded_scheduled_messages_from_database) {
        set_dialog_has_scheduled_database_messages_impl(d, false);
      } else {
        CHECK(G()->parameters().use_message_db);
        repair_dialog_scheduled_messages(d);
      }
    }
    if (d->has_scheduled_server_messages) {
      if (from_deletion && d->scheduled_messages_sync_generation > 0) {
        set_dialog_has_scheduled_server_messages(d, false);
      } else {
        d->last_repair_scheduled_messages_generation = 0;
        repair_dialog_scheduled_messages(d);
      }
    }
  }

  LOG(INFO) << "In " << d->dialog_id << " have scheduled messages on server = " << d->has_scheduled_server_messages
            << ", in database = " << d->has_scheduled_database_messages
            << " and in memory = " << (d->scheduled_messages != nullptr)
            << "; was loaded from database = " << d->has_loaded_scheduled_messages_from_database;
  bool has_scheduled_messages = get_dialog_has_scheduled_messages(d);
  if (has_scheduled_messages == d->last_sent_has_scheduled_messages) {
    return;
  }
  d->last_sent_has_scheduled_messages = has_scheduled_messages;

  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_has_scheduled_messages";
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateChatHasScheduledMessages>(d->dialog_id.get(), has_scheduled_messages));
}

void MessagesManager::unload_dialog(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  if (!d->has_unload_timeout) {
    LOG(INFO) << "Don't need to unload " << dialog_id;
    // possible right after the dialog was opened
    return;
  }

  if (!is_message_unload_enabled()) {
    // just in case
    LOG(INFO) << "Message unload is disabled in " << dialog_id;
    d->has_unload_timeout = false;
    return;
  }

  vector<MessageId> to_unload_message_ids;
  bool has_left_to_unload_messages = false;
  find_unloadable_messages(d, G()->unix_time_cached() - get_unload_dialog_delay() + 2, d->messages.get(),
                           to_unload_message_ids, has_left_to_unload_messages);

  vector<int64> unloaded_message_ids;
  for (auto message_id : to_unload_message_ids) {
    unload_message(d, message_id);
    unloaded_message_ids.push_back(message_id.get());
  }

  if (!unloaded_message_ids.empty()) {
    if (!G()->parameters().use_message_db && !d->is_empty) {
      d->have_full_history = false;
    }

    send_closure_later(
        G()->td(), &Td::send_update,
        make_tl_object<td_api::updateDeleteMessages>(dialog_id.get(), std::move(unloaded_message_ids), false, true));
  }

  if (has_left_to_unload_messages) {
    LOG(DEBUG) << "Need to unload more messages in " << dialog_id;
    pending_unload_dialog_timeout_.add_timeout_in(d->dialog_id.get(), get_next_unload_dialog_delay());
  } else {
    d->has_unload_timeout = false;
  }
}

void MessagesManager::on_active_dialog_action_timeout(DialogId dialog_id) {
  LOG(DEBUG) << "Receive active dialog action timeout in " << dialog_id;
  auto actions_it = active_dialog_actions_.find(dialog_id);
  if (actions_it == active_dialog_actions_.end()) {
    return;
  }
  CHECK(!actions_it->second.empty());

  auto now = Time::now();
  DialogId prev_typing_dialog_id;
  while (actions_it->second[0].start_time + DIALOG_ACTION_TIMEOUT < now + 0.1) {
    CHECK(actions_it->second[0].typing_dialog_id != prev_typing_dialog_id);
    prev_typing_dialog_id = actions_it->second[0].typing_dialog_id;
    on_dialog_action(dialog_id, actions_it->second[0].top_thread_message_id, actions_it->second[0].typing_dialog_id,
                     DialogAction(), 0);

    actions_it = active_dialog_actions_.find(dialog_id);
    if (actions_it == active_dialog_actions_.end()) {
      return;
    }
    CHECK(!actions_it->second.empty());
  }

  LOG(DEBUG) << "Schedule next action timeout in " << dialog_id;
  active_dialog_action_timeout_.add_timeout_in(dialog_id.get(),
                                               actions_it->second[0].start_time + DIALOG_ACTION_TIMEOUT - now);
}

void MessagesManager::suffix_load_query_ready(DialogId dialog_id) {
  LOG(INFO) << "Finished suffix load query in " << dialog_id;
  auto *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  bool is_unchanged = d->suffix_load_first_message_id_ == d->suffix_load_query_message_id_;
  suffix_load_update_first_message_id(d);
  if (is_unchanged && d->suffix_load_first_message_id_ == d->suffix_load_query_message_id_) {
    LOG(INFO) << "Finished suffix load in " << dialog_id;
    d->suffix_load_done_ = true;
  }
  d->suffix_load_has_query_ = false;

  // Remove ready queries
  auto *m = get_message_force(d, d->suffix_load_first_message_id_, "suffix_load_query_ready");
  auto ready_it = std::partition(d->suffix_load_queries_.begin(), d->suffix_load_queries_.end(),
                                 [&](auto &value) { return !(d->suffix_load_done_ || value.second(m)); });
  for (auto it = ready_it; it != d->suffix_load_queries_.end(); ++it) {
    it->first.set_value(Unit());
  }
  d->suffix_load_queries_.erase(ready_it, d->suffix_load_queries_.end());

  suffix_load_loop(d);
}

void MessagesManager::try_reuse_notification_group(NotificationGroupInfo &group_info) {
  if (!group_info.try_reuse) {
    return;
  }
  if (group_info.is_changed) {
    LOG(ERROR) << "Failed to reuse changed " << group_info.group_id;
    return;
  }
  group_info.try_reuse = false;
  if (!group_info.group_id.is_valid()) {
    LOG(ERROR) << "Failed to reuse invalid " << group_info.group_id;
    return;
  }
  CHECK(group_info.last_notification_id == NotificationId());
  CHECK(group_info.last_notification_date == 0);
  send_closure_later(G()->notification_manager(), &NotificationManager::try_reuse_notification_group_id,
                     group_info.group_id);
  notification_group_id_to_dialog_id_.erase(group_info.group_id);
  group_info.group_id = NotificationGroupId();
  group_info.max_removed_notification_id = NotificationId();
  group_info.max_removed_message_id = MessageId();
}

MessagesManager::Dialog *MessagesManager::add_dialog(DialogId dialog_id, const char *source) {
  LOG(DEBUG) << "Creating " << dialog_id << " from " << source;
  CHECK(!have_dialog(dialog_id));
  LOG_CHECK(dialog_id.is_valid()) << source;

  if (G()->parameters().use_message_db) {
    // TODO preload dialog asynchronously, remove loading from this function
    auto r_value = G()->td_db()->get_dialog_db_sync()->get_dialog(dialog_id);
    if (r_value.is_ok()) {
      LOG(INFO) << "Synchronously loaded " << dialog_id << " from database from " << source;
      return add_new_dialog(parse_dialog(dialog_id, r_value.ok(), source), true, source);
    }
  }

  auto d = make_unique<Dialog>();
  d->dialog_id = dialog_id;
  invalidate_message_indexes(d.get());

  return add_new_dialog(std::move(d), false, source);
}

vector<DialogId> MessagesManager::get_pinned_dialog_ids(DialogListId dialog_list_id) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (dialog_list_id.is_filter()) {
    const auto *filter = get_dialog_filter(dialog_list_id.get_filter_id());
    if (filter == nullptr) {
      return {};
    }
    return transform(filter->pinned_dialog_ids, [](auto &input_dialog) { return input_dialog.get_dialog_id(); });
  }

  auto *list = get_dialog_list(dialog_list_id);
  if (list == nullptr || !list->are_pinned_dialogs_inited_) {
    return {};
  }
  return transform(list->pinned_dialogs_, [](auto &pinned_dialog) { return pinned_dialog.get_dialog_id(); });
}

Status MessagesManager::toggle_dialog_is_marked_as_unread(DialogId dialog_id, bool is_marked_as_unread) {
  Dialog *d = get_dialog_force(dialog_id, "toggle_dialog_is_marked_as_unread");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }

  if (is_marked_as_unread == d->is_marked_as_unread) {
    return Status::OK();
  }

  set_dialog_is_marked_as_unread(d, is_marked_as_unread);

  toggle_dialog_is_marked_as_unread_on_server(dialog_id, is_marked_as_unread, 0);
  return Status::OK();
}

void MessagesManager::on_pending_updated_dialog_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  // no check for G()->close_flag() to save dialogs even while closing

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  // TODO it is unsafe to save dialog to database before binlog is flushed

  // no send_closure_later, because messages_manager can be not an actor while closing
  messages_manager->save_dialog_to_database(DialogId(dialog_id_int));
}

void MessagesManager::run_after_channel_difference(DialogId dialog_id, Promise<Unit> &&promise) {
  CHECK(dialog_id.get_type() == DialogType::Channel);
  CHECK(have_input_peer(dialog_id, AccessRights::Read));

  run_after_get_channel_difference_[dialog_id].push_back(std::move(promise));

  const Dialog *d = get_dialog(dialog_id);
  get_channel_difference(dialog_id, d == nullptr ? load_channel_pts(dialog_id) : d->pts, true,
                         "run_after_channel_difference");
}

string MessagesManager::get_notification_settings_scope_database_key(NotificationSettingsScope scope) {
  switch (scope) {
    case NotificationSettingsScope::Private:
      return "nsfpc";
    case NotificationSettingsScope::Group:
      return "nsfgc";
    case NotificationSettingsScope::Channel:
      return "nsfcc";
    default:
      UNREACHABLE();
      return "";
  }
}

td_api::object_ptr<td_api::MessageSender> MessagesManager::get_default_message_sender_object(const Dialog *d) const {
  auto as_dialog_id = d->default_send_message_as_dialog_id;
  return as_dialog_id.is_valid()
             ? get_message_sender_object_const(td_, as_dialog_id, "get_default_message_sender_object")
             : nullptr;
}

std::pair<int32, vector<DialogId>> MessagesManager::get_recently_opened_dialogs(int32 limit, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  return recently_opened_dialogs_.get_dialogs(limit, std::move(promise));
}

bool MessagesManager::has_incoming_notification(DialogId dialog_id, const Message *m) const {
  if (m->is_from_scheduled) {
    return true;
  }
  return !m->is_outgoing && dialog_id != get_my_dialog_id();
}
}  // namespace td
