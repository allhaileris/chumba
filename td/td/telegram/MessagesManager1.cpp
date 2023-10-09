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










namespace td {
}  // namespace td
namespace td {

// keep synced with add_scheduled_message_to_dialog
MessagesManager::Message *MessagesManager::add_message_to_dialog(Dialog *d, unique_ptr<Message> message,
                                                                 bool from_update, bool *need_update,
                                                                 bool *need_update_dialog_pos, const char *source) {
  CHECK(message != nullptr);
  CHECK(d != nullptr);
  CHECK(need_update != nullptr);
  CHECK(need_update_dialog_pos != nullptr);
  CHECK(source != nullptr);
  debug_add_message_to_dialog_fail_reason_ = "success";

  auto debug_have_previous = message->have_previous;
  auto debug_have_next = message->have_next;

  DialogId dialog_id = d->dialog_id;
  MessageId message_id = message->message_id;

  if (!has_message_sender_user_id(dialog_id, message.get()) && !message->sender_dialog_id.is_valid()) {
    if (is_broadcast_channel(dialog_id)) {
      message->sender_dialog_id = dialog_id;
    } else {
      if (is_discussion_message(dialog_id, message.get())) {
        message->sender_dialog_id = message->forward_info->from_dialog_id;
      } else {
        LOG(ERROR) << "Failed to repair sender chat in " << message_id << " in " << dialog_id;
      }
    }
  }
  auto dialog_type = dialog_id.get_type();
  if (message->sender_user_id == ContactsManager::get_anonymous_bot_user_id() &&
      !message->sender_dialog_id.is_valid() && dialog_type == DialogType::Channel && !is_broadcast_channel(dialog_id)) {
    message->sender_user_id = UserId();
    message->sender_dialog_id = dialog_id;
  }

  if (!message->top_thread_message_id.is_valid() && !is_broadcast_channel(dialog_id) &&
      is_visible_message_reply_info(dialog_id, message.get()) && !message_id.is_scheduled()) {
    message->top_thread_message_id = message_id;
  }

  if (!message_id.is_scheduled() && message_id <= d->last_clear_history_message_id) {
    LOG(INFO) << "Skip adding cleared " << message_id << " to " << dialog_id << " from " << source;
    if (message->from_database) {
      delete_message_from_database(d, message_id, message.get(), true);
    }
    debug_add_message_to_dialog_fail_reason_ = "cleared full history";
    return nullptr;
  }
  if (d->deleted_message_ids.count(message->reply_to_message_id)) {
    // LOG(INFO) << "Remove reply to deleted " << message->reply_to_message_id << " in " << message_id << " from " << dialog_id << " from " << source;
    // we don't want to lose information that the message was a reply
    // message->reply_to_message_id = MessageId();
  }

  LOG(INFO) << "Adding " << message_id << " of type " << message->content->get_type() << " to " << dialog_id << " from "
            << source << ". Last new is " << d->last_new_message_id << ", last is " << d->last_message_id
            << ", from_update = " << from_update << ", have_previous = " << message->have_previous
            << ", have_next = " << message->have_next;

  if (!message_id.is_valid()) {
    if (message_id.is_valid_scheduled()) {
      return add_scheduled_message_to_dialog(d, std::move(message), from_update, need_update, source);
    }
    LOG(ERROR) << "Receive " << message_id << " in " << dialog_id << " from " << source;
    CHECK(!message->from_database);
    debug_add_message_to_dialog_fail_reason_ = "invalid message identifier";
    return nullptr;
  }

  if (*need_update) {
    CHECK(from_update);
  }

  if (d->deleted_message_ids.count(message_id)) {
    LOG(INFO) << "Skip adding deleted " << message_id << " to " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "adding deleted message";
    return nullptr;
  }

  auto message_content_type = message->content->get_type();
  if (is_debug_message_op_enabled()) {
    d->debug_message_op.emplace_back(Dialog::MessageOp::Add, message_id, message_content_type, from_update,
                                     message->have_previous, message->have_next, source);
  }

  message->last_access_date = G()->unix_time_cached();

  if (from_update) {
    CHECK(message->have_next);
    CHECK(message->have_previous);
    if (message_id <= d->last_new_message_id && dialog_type != DialogType::Channel) {
      if (!G()->parameters().use_message_db) {
        if (td_->auth_manager_->is_bot() && Time::now() > start_time_ + 300 &&
            MessageId(ServerMessageId(100)) <= message_id && message_id <= MessageId(ServerMessageId(1000)) &&
            d->last_new_message_id >= MessageId(ServerMessageId(2147483000))) {
          LOG(FATAL) << "Force restart because of message_id overflow in " << dialog_id << " from "
                     << d->last_new_message_id << " to " << message_id;
        }
        if (!has_qts_messages(dialog_id)) {
          LOG(ERROR) << "New " << message_id << " in " << dialog_id << " from " << source
                     << " has identifier less than last_new_message_id = " << d->last_new_message_id;
          dump_debug_message_op(d);
        }
      }
    }
  }

  if (!from_update && !message->is_failed_to_send) {
    MessageId max_message_id;
    if (message_id.is_server()) {
      if (d->being_added_message_id.is_valid()) {
        // if a too new message not from update has failed to preload before being_added_message_id was set,
        // then it should fail to load even after it is set and last_new_message_id has changed
        max_message_id = d->being_updated_last_new_message_id;
      } else {
        max_message_id = d->last_new_message_id;
      }
    } else if (message_id.is_local()) {
      if (d->being_added_message_id.is_valid()) {
        max_message_id = d->being_updated_last_database_message_id;
      } else {
        max_message_id = d->last_database_message_id;
      }
    }
    if (max_message_id != MessageId() && message_id > max_message_id) {
      if (!message->from_database) {
        LOG(ERROR) << "Ignore " << message_id << " in " << dialog_id << " received not through update from " << source
                   << ". The maximum allowed is " << max_message_id << ", last is " << d->last_message_id
                   << ", being added message is " << d->being_added_message_id << ", channel difference "
                   << debug_channel_difference_dialog_ << " "
                   << to_string(get_message_object(dialog_id, message.get(), "add_message_to_dialog"));
        dump_debug_message_op(d, 3);

        if (need_channel_difference_to_add_message(dialog_id, nullptr)) {
          LOG(INFO) << "Schedule getDifference in " << dialog_id.get_channel_id();
          channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
        }
      } else {
        LOG(INFO) << "Ignore " << message_id << " in " << dialog_id << " received not through update from " << source;
      }
      debug_add_message_to_dialog_fail_reason_ = "too new message not from update";
      return nullptr;
    }
  }
  if ((message_id.is_server() || (message_id.is_local() && dialog_type == DialogType::SecretChat)) &&
      message_id <= d->max_unavailable_message_id) {
    LOG(INFO) << "Can't add an unavailable " << message_id << " to " << dialog_id << " from " << source;
    if (message->from_database) {
      delete_message_from_database(d, message_id, message.get(), true);
    }
    debug_add_message_to_dialog_fail_reason_ = "ignore unavailable message";
    return nullptr;
  }

  if (message_content_type == MessageContentType::ChatDeleteHistory) {
    {
      auto m = delete_message(d, message_id, true, need_update_dialog_pos, "message chat delete history");
      if (m != nullptr) {
        send_update_delete_messages(dialog_id, {m->message_id.get()}, true, false);
      }
    }
    int32 last_message_date = 0;
    if (d->last_message_id != MessageId()) {
      auto last_message = get_message(d, d->last_message_id);
      CHECK(last_message != nullptr);
      last_message_date = last_message->date - 1;
    } else {
      last_message_date = d->last_clear_history_date;
    }
    if (message->date > last_message_date) {
      set_dialog_last_clear_history_date(d, message->date, message_id, "update_last_clear_history_date");
      *need_update_dialog_pos = true;
    }
    LOG(INFO) << "Process MessageChatDeleteHistory in " << message_id << " in " << dialog_id << " with date "
              << message->date << " from " << source;
    if (message_id > d->max_unavailable_message_id) {
      set_dialog_max_unavailable_message_id(dialog_id, message_id, false, "message chat delete history");
    }
    CHECK(!message->from_database);
    debug_add_message_to_dialog_fail_reason_ = "skip adding MessageChatDeleteHistory";
    return nullptr;
  }

  if (*need_update && message_id <= d->last_new_message_id && !td_->auth_manager_->is_bot()) {
    *need_update = false;
  }

  if (message->reply_markup != nullptr &&
      (message->reply_markup->type == ReplyMarkup::Type::RemoveKeyboard ||
       (message->reply_markup->type == ReplyMarkup::Type::ForceReply && !message->reply_markup->is_personal)) &&
      !td_->auth_manager_->is_bot()) {
    if (from_update && message->reply_markup->is_personal) {  // if this keyboard is for us
      if (d->reply_markup_message_id != MessageId() && message_id > d->reply_markup_message_id) {
        const Message *old_message = get_message_force(d, d->reply_markup_message_id, "add_message_to_dialog 1");
        if (old_message == nullptr ||
            (old_message->sender_user_id.is_valid() && old_message->sender_user_id == message->sender_user_id)) {
          set_dialog_reply_markup(d, MessageId());
        }
      }
    }
    message->had_reply_markup = message->reply_markup->is_personal;
    message->reply_markup = nullptr;
  }

  bool auto_attach = message->have_previous && message->have_next &&
                     (from_update || message_id.is_local() || message_id.is_yet_unsent());

  {
    Message *m = message->from_database ? get_message(d, message_id)
                                        : get_message_force(d, message_id, "add_message_to_dialog 2");
    if (m != nullptr) {
      CHECK(m->message_id == message_id);
      CHECK(message->message_id == message_id);
      LOG(INFO) << "Adding already existing " << message_id << " in " << dialog_id << " from " << source;
      if (*need_update) {
        *need_update = false;
        if (!G()->parameters().use_message_db) {
          // can happen if the message is received first through getMessage in an unknown chat without
          // last_new_message_id and only after that received through getDifference or getChannelDifference
          if (d->last_new_message_id.is_valid()) {
            LOG(ERROR) << "Receive again " << (message->is_outgoing ? "outgoing" : "incoming")
                       << (message->forward_info == nullptr ? " not" : "") << " forwarded " << message_id
                       << " with content of type " << message_content_type << " in " << dialog_id << " from " << source
                       << ", current last new is " << d->last_new_message_id << ", last is " << d->last_message_id;
            dump_debug_message_op(d, 1);
          }
        }
      }
      if (auto_attach) {
        CHECK(message->have_previous);
        CHECK(message->have_next);
        message->have_previous = false;
        message->have_next = false;
      }
      if (!message->from_database && (from_update || message->edit_date >= m->edit_date)) {
        const int32 INDEX_MASK_MASK = ~message_search_filter_index_mask(MessageSearchFilter::UnreadMention);
        auto old_index_mask = get_message_index_mask(dialog_id, m) & INDEX_MASK_MASK;
        bool was_deleted = delete_active_live_location(dialog_id, m);
        auto old_file_ids = get_message_content_file_ids(m->content.get(), td_);
        bool need_send_update = update_message(d, m, std::move(message), need_update_dialog_pos, true);
        if (!need_send_update) {
          LOG(INFO) << message_id << " in " << dialog_id << " is not changed";
        }
        auto new_index_mask = get_message_index_mask(dialog_id, m) & INDEX_MASK_MASK;
        if (was_deleted) {
          try_add_active_live_location(dialog_id, m);
        }
        change_message_files(dialog_id, m, old_file_ids);
        if (need_send_update && m->notification_id.is_valid() && is_message_notification_active(d, m)) {
          auto &group_info = get_notification_group_info(d, m);
          if (group_info.group_id.is_valid()) {
            send_closure_later(G()->notification_manager(), &NotificationManager::edit_notification,
                               group_info.group_id, m->notification_id, create_new_message_notification(m->message_id));
          }
        }
        if (need_send_update && m->is_pinned && d->pinned_message_notification_message_id.is_valid() &&
            d->mention_notification_group.group_id.is_valid()) {
          auto pinned_message = get_message_force(d, d->pinned_message_notification_message_id, "after update_message");
          if (pinned_message != nullptr && pinned_message->notification_id.is_valid() &&
              is_message_notification_active(d, pinned_message) &&
              get_message_content_pinned_message_id(pinned_message->content.get()) == message_id) {
            send_closure_later(G()->notification_manager(), &NotificationManager::edit_notification,
                               d->mention_notification_group.group_id, pinned_message->notification_id,
                               create_new_message_notification(pinned_message->message_id));
          }
        }
        update_message_count_by_index(d, -1, old_index_mask & ~new_index_mask);
        update_message_count_by_index(d, +1, new_index_mask & ~old_index_mask);
      }
      return m;
    }
  }

  if (*need_update && !td_->auth_manager_->is_bot()) {
    if (message_content_type == MessageContentType::PinMessage) {
      if (is_dialog_pinned_message_notifications_disabled(d) ||
          !get_message_content_pinned_message_id(message->content.get()).is_valid()) {
        // treat message pin without pinned message as an ordinary message
        message->contains_mention = false;
      }
    } else if (message->contains_mention && is_dialog_mention_notifications_disabled(d)) {
      // disable mention notification
      message->is_mention_notification_disabled = true;
    }
  }

  if (message->contains_unread_mention && message_id <= d->last_read_all_mentions_message_id) {
    LOG(INFO) << "Ignore unread mention in " << message_id;
    message->contains_unread_mention = false;
    if (message->from_database) {
      on_message_changed(d, message.get(), false, "add already read mention message to dialog");
    }
  }

  if (*need_update && may_need_message_notification(d, message.get())) {
    // notification group must be created here because it may force adding new messages from database
    // in get_message_notification_group_force
    get_dialog_notification_group_id(d->dialog_id, get_notification_group_info(d, message.get()));
  }
  if (*need_update || (!d->last_new_message_id.is_valid() && !message_id.is_yet_unsent() && from_update)) {
    auto pinned_message_id = get_message_content_pinned_message_id(message->content.get());
    if (pinned_message_id.is_valid() && pinned_message_id < message_id &&
        have_message_force(d, pinned_message_id, "preload pinned message")) {
      LOG(INFO) << "Preloaded pinned " << pinned_message_id << " from database";
    }

    if (d->pinned_message_notification_message_id.is_valid() &&
        d->pinned_message_notification_message_id != message_id &&
        have_message_force(d, d->pinned_message_notification_message_id, "preload previously pinned message")) {
      LOG(INFO) << "Preloaded previously pinned " << d->pinned_message_notification_message_id << " from database";
    }
  }
  if (from_update && message->top_thread_message_id.is_valid() && message->top_thread_message_id != message_id &&
      message_id.is_server() && have_message_force(d, message->top_thread_message_id, "preload top reply message")) {
    LOG(INFO) << "Preloaded top thread " << message->top_thread_message_id << " from database";

    Message *top_m = get_message(d, message->top_thread_message_id);
    CHECK(top_m != nullptr);
    if (is_active_message_reply_info(dialog_id, top_m->reply_info) && is_discussion_message(dialog_id, top_m)) {
      FullMessageId top_full_message_id{top_m->forward_info->from_dialog_id, top_m->forward_info->from_message_id};
      if (have_message_force(top_full_message_id, "preload discussed message")) {
        LOG(INFO) << "Preloaded discussed " << top_full_message_id << " from database";
      }
    }
  }

  // there must be no two recursive calls to add_message_to_dialog
  LOG_CHECK(!d->being_added_message_id.is_valid())
      << d->dialog_id << " " << d->being_added_message_id << " " << message_id << " " << *need_update << " "
      << d->pinned_message_notification_message_id << " " << d->last_new_message_id << " " << source;
  LOG_CHECK(!d->being_deleted_message_id.is_valid())
      << d->being_deleted_message_id << " " << message_id << " " << source;

  d->being_added_message_id = message_id;
  d->being_updated_last_new_message_id = d->last_new_message_id;
  d->being_updated_last_database_message_id = d->last_database_message_id;

  if (d->new_secret_chat_notification_id.is_valid()) {
    remove_new_secret_chat_notification(d, true);
  }

  if (message->message_id > d->max_added_message_id) {
    d->max_added_message_id = message->message_id;
  }

  if (d->have_full_history && !message->from_database && !from_update && !message_id.is_local() &&
      !message_id.is_yet_unsent()) {
    LOG(ERROR) << "Have full history in " << dialog_id << ", but receive unknown " << message_id
               << " with content of type " << message_content_type << " from " << source << ". Last new is "
               << d->last_new_message_id << ", last is " << d->last_message_id << ", first database is "
               << d->first_database_message_id << ", last database is " << d->last_database_message_id
               << ", last read inbox is " << d->last_read_inbox_message_id << ", last read outbox is "
               << d->last_read_inbox_message_id << ", last read all mentions is "
               << d->last_read_all_mentions_message_id << ", max unavailable is " << d->max_unavailable_message_id
               << ", last assigned is " << d->last_assigned_message_id;
    d->have_full_history = false;
    on_dialog_updated(dialog_id, "drop have_full_history");
  }

  if (!d->is_opened && d->messages != nullptr && is_message_unload_enabled() && !d->has_unload_timeout) {
    LOG(INFO) << "Schedule unload of " << dialog_id;
    pending_unload_dialog_timeout_.add_timeout_in(dialog_id.get(), get_next_unload_dialog_delay());
    d->has_unload_timeout = true;
  }

  if (message->ttl > 0 && message->ttl_expires_at != 0) {
    auto now = Time::now();
    if (message->ttl_expires_at <= now) {
      if (dialog_type == DialogType::SecretChat) {
        LOG(INFO) << "Can't add " << message_id << " with expired TTL to " << dialog_id << " from " << source;
        delete_message_from_database(d, message_id, message.get(), true);
        debug_add_message_to_dialog_fail_reason_ = "delete expired by TTL message";
        d->being_added_message_id = MessageId();
        return nullptr;
      } else {
        on_message_ttl_expired_impl(d, message.get());
        message_content_type = message->content->get_type();
        if (message->from_database) {
          on_message_changed(d, message.get(), false, "add expired message to dialog");
        }
      }
    } else {
      ttl_register_message(dialog_id, message.get(), now);
    }
  }
  if (message->ttl_period > 0) {
    CHECK(dialog_type != DialogType::SecretChat);
    auto server_time = G()->server_time();
    if (message->date + message->ttl_period <= server_time) {
      LOG(INFO) << "Can't add " << message_id << " with expired TTL period to " << dialog_id << " from " << source;
      delete_message_from_database(d, message_id, message.get(), true);
      debug_add_message_to_dialog_fail_reason_ = "delete expired by TTL period message";
      d->being_added_message_id = MessageId();
      return nullptr;
    } else {
      ttl_period_register_message(dialog_id, message.get(), server_time);
    }
  }

  if (message->from_database && !message->are_media_timestamp_entities_found) {
    auto text = get_message_content_text_mutable(message->content.get());
    if (text != nullptr) {
      fix_formatted_text(text->text, text->entities, true, true, true, false, false).ensure();
      // always call to save are_media_timestamp_entities_found flag
      on_message_changed(d, message.get(), false, "save media timestamp entities");
    }
  }
  message->are_media_timestamp_entities_found = true;

  LOG(INFO) << "Adding not found " << message_id << " to " << dialog_id << " from " << source;
  if (d->is_empty) {
    d->is_empty = false;
    *need_update_dialog_pos = true;
  }

  if (dialog_type == DialogType::Channel && !message->contains_unread_mention) {
    auto channel_read_media_period =
        G()->shared_config().get_option_integer("channels_read_media_period", (G()->is_test_dc() ? 300 : 7 * 86400));
    if (message->date < G()->unix_time_cached() - channel_read_media_period) {
      update_opened_message_content(message->content.get());
    }
  }

  if (G()->parameters().use_file_db && message_id.is_yet_unsent() && !message->via_bot_user_id.is_valid() &&
      !message->hide_via_bot && !message->is_copy) {
    auto queue_id = get_sequence_dispatcher_id(dialog_id, message_content_type);
    if (queue_id & 1) {
      LOG(INFO) << "Add " << message_id << " from " << source << " to queue " << queue_id;
      yet_unsent_media_queues_[queue_id][message_id];  // reserve place for promise
      if (!td_->auth_manager_->is_bot()) {
        pending_send_dialog_action_timeout_.add_timeout_in(dialog_id.get(), 1.0);
      }
    }
  }

  if (!(d->have_full_history && auto_attach) && d->last_message_id.is_valid() &&
      d->last_message_id < MessageId(ServerMessageId(1)) && message_id >= MessageId(ServerMessageId(1))) {
    set_dialog_last_message_id(d, MessageId(), "add_message_to_dialog");

    set_dialog_first_database_message_id(d, MessageId(), "add_message_to_dialog");
    set_dialog_last_database_message_id(d, MessageId(), source);
    d->have_full_history = false;
    invalidate_message_indexes(d);
    d->local_unread_count = 0;  // read all local messages. They will not be reachable anymore

    on_dialog_updated(dialog_id, "add gap to dialog");

    send_update_chat_last_message(d, "add gap to dialog");
    *need_update_dialog_pos = false;
  }

  if (from_update && message_id > d->last_new_message_id && !message_id.is_yet_unsent()) {
    if (dialog_type == DialogType::SecretChat || message_id.is_server()) {
      // can delete messages, therefore must be called before message attaching/adding
      set_dialog_last_new_message_id(d, message_id, "add_message_to_dialog");
    }
  }

  bool is_attached = false;
  if (auto_attach) {
    auto it = MessagesIterator(d, message_id);
    Message *previous_message = *it;
    if (previous_message != nullptr) {
      auto previous_message_id = previous_message->message_id;
      CHECK(previous_message_id < message_id);
      if (previous_message->have_next || (d->last_message_id.is_valid() && previous_message_id >= d->last_message_id)) {
        if (message_id.is_server() && previous_message_id.is_server() && previous_message->have_next) {
          ++it;
          auto next_message = *it;
          if (next_message != nullptr) {
            if (next_message->message_id.is_server() && !has_qts_messages(dialog_id)) {
              LOG(ERROR) << "Attach " << message_id << " from " << source << " before " << next_message->message_id
                         << " and after " << previous_message_id << " in " << dialog_id;
              dump_debug_message_op(d);
            }
          } else {
            LOG(ERROR) << "Have_next is true, but there is no next message after " << previous_message_id << " from "
                       << source << " in " << dialog_id;
            dump_debug_message_op(d);
          }
        }

        LOG(INFO) << "Attach " << message_id << " to the previous " << previous_message_id << " in " << dialog_id;
        message->have_previous = true;
        message->have_next = previous_message->have_next;
        previous_message->have_next = true;
        is_attached = true;
      }
    }
    if (!is_attached && !message_id.is_yet_unsent()) {
      // message may be attached to the next message if there is no previous message
      Message *cur = d->messages.get();
      Message *next_message = nullptr;
      while (cur != nullptr) {
        if (cur->message_id < message_id) {
          cur = cur->right.get();
        } else {
          next_message = cur;
          cur = cur->left.get();
        }
      }
      if (next_message != nullptr) {
        CHECK(!next_message->have_previous);
        LOG(INFO) << "Attach " << message_id << " to the next " << next_message->message_id << " in " << dialog_id;
        if (from_update && !next_message->message_id.is_yet_unsent()) {
          LOG(ERROR) << "Attach " << message_id << " from " << source << " to the next " << next_message->message_id
                     << " in " << dialog_id;
        }
        message->have_next = true;
        message->have_previous = next_message->have_previous;
        next_message->have_previous = true;
        is_attached = true;
      }
    }
    if (!is_attached) {
      LOG(INFO) << "Can't auto-attach " << message_id << " in " << dialog_id;
      message->have_previous = false;
      message->have_next = false;
    }
  }

  if (!td_->auth_manager_->is_bot()) {
    if (*need_update) {
      // notification must be added before updating unread_count to have correct total notification count
      // in get_message_notification_group_force
      add_new_message_notification(d, message.get(), false);
    } else {
      if (message->from_database && message->notification_id.is_valid() &&
          is_from_mention_notification_group(d, message.get()) && is_message_notification_active(d, message.get()) &&
          is_dialog_mention_notifications_disabled(d) && message_id != d->pinned_message_notification_message_id) {
        auto notification_id = message->notification_id;
        VLOG(notifications) << "Remove mention " << notification_id << " in " << message_id << " in " << dialog_id;
        message->notification_id = NotificationId();
        if (d->mention_notification_group.last_notification_id == notification_id) {
          // last notification is deleted, need to find new last notification
          fix_dialog_last_notification_id(d, true, message_id);
        }

        send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification,
                           d->mention_notification_group.group_id, notification_id, false, false, Promise<Unit>(),
                           "remove disabled mention notification");

        on_message_changed(d, message.get(), false, "remove_mention_notification");
      }
    }
  }

  if (*need_update && message_id > d->last_read_inbox_message_id && !td_->auth_manager_->is_bot()) {
    if (has_incoming_notification(dialog_id, message.get())) {
      int32 server_unread_count = d->server_unread_count;
      int32 local_unread_count = d->local_unread_count;
      if (message_id.is_server()) {
        server_unread_count++;
      } else {
        local_unread_count++;
      }
      set_dialog_last_read_inbox_message_id(d, MessageId::min(), server_unread_count, local_unread_count, false,
                                            source);
    } else {
      // if non-scheduled outgoing message has identifier one greater than last_read_inbox_message_id,
      // then definitely there are no unread incoming messages before it
      if (message_id.is_server() && d->last_read_inbox_message_id.is_valid() &&
          d->last_read_inbox_message_id.is_server() &&
          message_id.get_server_message_id().get() == d->last_read_inbox_message_id.get_server_message_id().get() + 1) {
        read_history_inbox(dialog_id, message_id, 0, "add_message_to_dialog");
      }
    }
  }
  if (*need_update && message->contains_unread_mention) {
    set_dialog_unread_mention_count(d, d->unread_mention_count + 1);
    send_update_chat_unread_mention_count(d);
  }
  if (*need_update) {
    update_message_count_by_index(d, +1, message.get());
  }
  if (auto_attach && message_id > d->last_message_id && message_id >= d->last_new_message_id) {
    set_dialog_last_message_id(d, message_id, "add_message_to_dialog");
    *need_update_dialog_pos = true;
  }
  if (auto_attach && !message_id.is_yet_unsent() && message_id >= d->last_new_message_id &&
      (d->last_new_message_id.is_valid() ||
       (message_id.is_local() && d->last_message_id.is_valid() &&
        (message_id >= d->last_message_id ||
         (d->last_database_message_id.is_valid() && message_id > d->last_database_message_id))))) {
    CHECK(message_id <= d->last_message_id);
    if (message_id > d->last_database_message_id) {
      set_dialog_last_database_message_id(d, message_id, "add_message_to_dialog");
      if (!d->first_database_message_id.is_valid()) {
        set_dialog_first_database_message_id(d, message_id, "add_message_to_dialog");
        try_restore_dialog_reply_markup(d, message.get());
      }
    }
  }

  const Message *m = message.get();
  if (m->message_id.is_yet_unsent() && m->reply_to_message_id.is_valid() && !m->reply_to_message_id.is_yet_unsent()) {
    replied_by_yet_unsent_messages_[FullMessageId{dialog_id, m->reply_to_message_id}]++;
  }

  if (!m->from_database && !m->message_id.is_yet_unsent()) {
    add_message_to_database(d, m, "add_message_to_dialog");
  }

  if (from_update && dialog_type == DialogType::Channel) {
    auto now = max(G()->unix_time_cached(), m->date);
    if (m->date < now - 2 * 86400 && Slice(source) == Slice("updateNewChannelMessage")) {
      // if the message is pretty old, we might have missed the update that the message has already been read
      repair_channel_server_unread_count(d);
    }
    if (m->date + 3600 >= now && m->is_outgoing) {
      auto channel_id = dialog_id.get_channel_id();
      auto slow_mode_delay = td_->contacts_manager_->get_channel_slow_mode_delay(channel_id);
      auto status = td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id());
      if (m->date + slow_mode_delay > now && !status.is_administrator()) {
        td_->contacts_manager_->on_update_channel_slow_mode_next_send_date(channel_id, m->date + slow_mode_delay);
      }
    }
    if (m->date > now - 14 * 86400) {
      td_->contacts_manager_->remove_inactive_channel(dialog_id.get_channel_id());
    }
  }

  if (!is_attached && !m->have_next && !m->have_previous) {
    MessagesIterator it(d, m->message_id);
    if (*it != nullptr && (*it)->have_next) {
      // need to drop a connection between messages
      auto previous_message = *it;
      ++it;
      auto next_message = *it;
      if (next_message != nullptr) {
        if (next_message->message_id.is_server() &&
            !(td_->auth_manager_->is_bot() && Slice(source) == Slice("GetChannelMessagesQuery"))) {
          LOG(ERROR) << "Can't attach " << m->message_id << " from " << source << " from "
                     << (m->from_database ? "database" : "server") << " before " << next_message->message_id
                     << " and after " << previous_message->message_id << " in " << dialog_id;
          dump_debug_message_op(d);
        }

        next_message->have_previous = false;
        previous_message->have_next = false;
      } else {
        LOG(ERROR) << "Have_next is true, but there is no next message after " << previous_message->message_id
                   << " from " << source << " in " << dialog_id;
        dump_debug_message_op(d);
      }
    }
  }

  if (message_content_type == MessageContentType::ContactRegistered && !d->has_contact_registered_message) {
    d->has_contact_registered_message = true;
    on_dialog_updated(dialog_id, "update_has_contact_registered_message");
  }

  reget_message_from_server_if_needed(dialog_id, m);

  add_message_file_sources(dialog_id, m);

  register_message_content(td_, m->content.get(), {dialog_id, m->message_id}, "add_message_to_dialog");

  register_message_reply(d, m);

  if (*need_update && m->message_id.is_server() && message_content_type == MessageContentType::PinMessage) {
    auto pinned_message_id = get_message_content_pinned_message_id(m->content.get());
    if (d->is_last_pinned_message_id_inited && pinned_message_id > d->last_pinned_message_id) {
      set_dialog_last_pinned_message_id(d, pinned_message_id);
    }
  }
  if (*need_update && m->message_id.is_server() && message_content_type == MessageContentType::ChatSetTheme) {
    set_dialog_theme_name(d, get_message_content_theme_name(m->content.get()));
  }

  if (from_update) {
    speculatively_update_active_group_call_id(d, m);
    speculatively_update_channel_participants(dialog_id, m);
    update_sent_message_contents(dialog_id, m);
    update_used_hashtags(dialog_id, m);
    update_top_dialogs(dialog_id, m);
    cancel_dialog_action(dialog_id, m);
    update_has_outgoing_messages(dialog_id, m);

    if (!td_->auth_manager_->is_bot() && d->messages == nullptr && !m->is_outgoing && dialog_id != get_my_dialog_id()) {
      switch (dialog_type) {
        case DialogType::User:
          td_->contacts_manager_->invalidate_user_full(dialog_id.get_user_id());
          td_->contacts_manager_->reload_user_full(dialog_id.get_user_id());
          break;
        case DialogType::Chat:
        case DialogType::Channel:
          // nothing to do
          break;
        case DialogType::SecretChat: {
          auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
          if (user_id.is_valid()) {
            td_->contacts_manager_->invalidate_user_full(user_id);
            td_->contacts_manager_->reload_user_full(user_id);
          }
          break;
        }
        case DialogType::None:
        default:
          UNREACHABLE();
      }
    }
  }

  Message *result_message = treap_insert_message(&d->messages, std::move(message));
  CHECK(result_message != nullptr);
  CHECK(result_message == m);
  CHECK(d->messages != nullptr);

  if (!is_attached) {
    if (m->have_next) {
      LOG_CHECK(!m->have_previous) << auto_attach << " " << dialog_id << " " << m->message_id << " " << from_update
                                   << " " << *need_update << " " << d->being_updated_last_new_message_id << " "
                                   << d->last_new_message_id << " " << d->being_updated_last_database_message_id << " "
                                   << d->last_database_message_id << " " << debug_have_previous << " "
                                   << debug_have_next << " " << source;
      attach_message_to_next(d, m->message_id, source);
    } else if (m->have_previous) {
      attach_message_to_previous(d, m->message_id, source);
    }
  }

  if (m->message_id.is_yet_unsent() && m->top_thread_message_id.is_valid()) {
    auto is_inserted = d->yet_unsent_thread_message_ids[m->top_thread_message_id].insert(m->message_id).second;
    CHECK(is_inserted);
  }

  switch (dialog_type) {
    case DialogType::User:
    case DialogType::Chat:
      if (m->message_id.is_server()) {
        message_id_to_dialog_id_[m->message_id] = dialog_id;
      }
      break;
    case DialogType::Channel:
      // nothing to do
      break;
    case DialogType::SecretChat:
      add_random_id_to_message_id_correspondence(d, m->random_id, m->message_id);
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  if (m->notification_id.is_valid()) {
    add_notification_id_to_message_id_correspondence(d, m->notification_id, m->message_id);
  }

  try_add_bot_command_message_id(dialog_id, m);

  // must be called after the message is added to correctly update replies
  update_message_max_reply_media_timestamp(d, result_message, false);
  update_message_max_own_media_timestamp(d, result_message);

  result_message->debug_source = source;
  d->being_added_message_id = MessageId();

  if (!td_->auth_manager_->is_bot() && from_update && d->reply_markup_message_id != MessageId()) {
    auto deleted_user_id = get_message_content_deleted_user_id(m->content.get());
    if (deleted_user_id.is_valid()) {  // do not check for is_user_bot to allow deleted bots
      const Message *old_message = get_message_force(d, d->reply_markup_message_id, "add_message_to_dialog 3");
      if (old_message == nullptr || old_message->sender_user_id == deleted_user_id) {
        LOG(INFO) << "Remove reply markup in " << dialog_id << ", because bot " << deleted_user_id
                  << " isn't a member of the chat";
        set_dialog_reply_markup(d, MessageId());
      }
    }
  }

  return result_message;
}
}  // namespace td
