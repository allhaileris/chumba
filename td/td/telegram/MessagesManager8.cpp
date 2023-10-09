//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"


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










namespace td {
}  // namespace td
namespace td {

void MessagesManager::on_get_history(DialogId dialog_id, MessageId from_message_id, MessageId old_last_new_message_id,
                                     int32 offset, int32 limit, bool from_the_end,
                                     vector<tl_object_ptr<telegram_api::Message>> &&messages, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Receive " << messages.size() << " history messages " << (from_the_end ? "from the end " : "") << "in "
            << dialog_id << " from " << from_message_id << " with offset " << offset << " and limit " << limit;
  CHECK(-limit < offset && offset <= 0);
  CHECK(offset < 0 || from_the_end);
  CHECK(!from_message_id.is_scheduled());

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  MessageId last_received_message_id = messages.empty() ? MessageId() : get_message_id(messages[0], false);
  if (old_last_new_message_id < d->last_new_message_id && (from_the_end || old_last_new_message_id < from_message_id) &&
      last_received_message_id < d->last_new_message_id) {
    // new server messages were added to the dialog since the request was sent, but weren't received
    // they should have been received, so we must repeat the request to get them
    if (from_the_end) {
      get_history_from_the_end_impl(d, false, false, std::move(promise));
    } else {
      get_history_impl(d, from_message_id, offset, limit, false, false, std::move(promise));
    }
    return;
  }

  // the server can return less messages than requested if some of messages are deleted during request
  // but if it happens, it is likely that there are no more messages on the server
  bool have_full_history = from_the_end && narrow_cast<int32>(messages.size()) < limit && messages.size() <= 1;

  if (messages.empty()) {
    if (have_full_history) {
      d->have_full_history = true;
      on_dialog_updated(dialog_id, "set have_full_history");
    }

    if (from_the_end && d->have_full_history && d->messages == nullptr) {
      if (!d->last_database_message_id.is_valid()) {
        set_dialog_is_empty(d, "on_get_history empty");
      } else {
        LOG(INFO) << "Skip marking " << dialog_id << " as empty, because it probably has messages from "
                  << d->first_database_message_id << " to " << d->last_database_message_id << " in the database";
      }
    }

    // be aware that in some cases an empty answer may be returned, because of the race of getHistory and deleteMessages
    // and not because there are no more messages
    promise.set_value(Unit());
    return;
  }

  if (messages.size() > 1) {
    // check that messages are received in decreasing message_id order
    MessageId cur_message_id = MessageId::max();
    for (const auto &message : messages) {
      MessageId message_id = get_message_id(message, false);
      if (message_id >= cur_message_id) {
        string error = PSTRING() << "Receive messages in the wrong order in history of " << dialog_id << " from "
                                 << from_message_id << " with offset " << offset << ", limit " << limit
                                 << ", from_the_end = " << from_the_end << ": ";
        for (const auto &debug_message : messages) {
          error += to_string(debug_message);
        }
        // TODO move to ERROR
        LOG(FATAL) << error;
        promise.set_value(Unit());
        return;
      }
      cur_message_id = message_id;
    }
  }

  // be aware that returned messages are guaranteed to be consecutive messages, but if !from_the_end they
  // may be older (if some messages was deleted) or newer (if some messages were added) than an expected answer
  // be aware that any subset of the returned messages may be already deleted and returned as MessageEmpty
  bool is_channel_message = dialog_id.get_type() == DialogType::Channel;
  MessageId first_added_message_id;
  MessageId last_added_message_id;
  bool have_next = false;

  if (narrow_cast<int32>(messages.size()) < limit + offset && messages.size() <= 1) {
    MessageId first_received_message_id = get_message_id(messages.back(), false);
    if (first_received_message_id >= from_message_id && d->first_database_message_id.is_valid() &&
        first_received_message_id >= d->first_database_message_id) {
      // it is likely that there are no more history messages on the server
      have_full_history = true;
    }
  }

  bool prev_have_full_history = d->have_full_history;
  MessageId prev_last_new_message_id = d->last_new_message_id;
  MessageId prev_first_database_message_id = d->first_database_message_id;
  MessageId prev_last_database_message_id = d->last_database_message_id;
  MessageId prev_last_message_id = d->last_message_id;
  if (from_the_end) {
    // delete all server messages with ID > last_received_message_id
    // there were no new messages received after the getHistory request was sent, so they are already deleted message
    if (delete_newer_server_messages_at_the_end(d, last_received_message_id)) {
      have_next = true;
    }
  }

  for (auto &message : messages) {
    if (!have_next && from_the_end && get_message_id(message, false) < d->last_message_id) {
      // last message in the dialog should be attached to the next message if there is some
      have_next = true;
    }

    auto message_dialog_id = get_message_dialog_id(message);
    if (message_dialog_id != dialog_id) {
      LOG(ERROR) << "Receive " << get_message_id(message, false) << " in wrong " << message_dialog_id << " instead of "
                 << dialog_id << ": " << oneline(to_string(message));
      continue;
    }

    auto full_message_id =
        on_get_message(std::move(message), false, is_channel_message, false, false, have_next, "get history");
    auto message_id = full_message_id.get_message_id();
    if (message_id.is_valid()) {
      if (!last_added_message_id.is_valid()) {
        last_added_message_id = message_id;
      }

      if (!have_next) {
        have_next = true;
      } else if (first_added_message_id.is_valid()) {
        Message *next_message = get_message(d, first_added_message_id);
        CHECK(next_message != nullptr);
        if (!next_message->have_previous) {
          LOG(INFO) << "Fix have_previous for " << first_added_message_id;
          next_message->have_previous = true;
          attach_message_to_previous(d, first_added_message_id, "on_get_history");
        }
      }
      first_added_message_id = message_id;
      if (!message_id.is_yet_unsent()) {
        // message should be already saved to database in on_get_message
        // add_message_to_database(d, get_message(d, message_id), "on_get_history");
      }
    }
  }

  if (from_the_end && last_added_message_id.is_valid() && last_added_message_id != last_received_message_id) {
    CHECK(last_added_message_id < last_received_message_id);
    delete_newer_server_messages_at_the_end(d, last_added_message_id);
  }

  if (have_full_history) {
    d->have_full_history = true;
    on_dialog_updated(dialog_id, "set have_full_history 2");
  }

  //  LOG_IF(ERROR, d->first_message_id.is_valid() && d->first_message_id > first_received_message_id)
  //      << "Receive " << first_received_message_id << ", but first chat message is " << d->first_message_id;

  if (from_the_end && !d->last_new_message_id.is_valid()) {
    set_dialog_last_new_message_id(
        d, last_added_message_id.is_valid() ? last_added_message_id : last_received_message_id, "on_get_history");
  }
  bool intersect_last_database_message_ids =
      last_added_message_id >= d->first_database_message_id && d->last_database_message_id >= first_added_message_id;
  bool need_update_database_message_ids =
      last_added_message_id.is_valid() && (from_the_end || intersect_last_database_message_ids);
  if (from_the_end && last_added_message_id.is_valid() && last_added_message_id > d->last_message_id) {
    CHECK(d->last_new_message_id.is_valid());
    set_dialog_last_message_id(d, last_added_message_id, "on_get_history");
    send_update_chat_last_message(d, "on_get_history");
  }

  if (need_update_database_message_ids) {
    if (from_the_end && !intersect_last_database_message_ids && d->last_database_message_id.is_valid()) {
      if (d->last_database_message_id < first_added_message_id || last_added_message_id == d->last_message_id) {
        set_dialog_first_database_message_id(d, MessageId(), "on_get_history 1");
        set_dialog_last_database_message_id(d, MessageId(), "on_get_history 1");
      } else {
        auto min_message_id = td::min(d->first_database_message_id, d->last_message_id);
        LOG_CHECK(last_added_message_id < min_message_id)
            << need_update_database_message_ids << ' ' << first_added_message_id << ' ' << last_added_message_id << ' '
            << d->first_database_message_id << ' ' << d->last_database_message_id << ' ' << d->last_new_message_id
            << ' ' << d->last_message_id << ' ' << prev_first_database_message_id << ' '
            << prev_last_database_message_id << ' ' << prev_last_new_message_id << ' ' << prev_last_message_id;
        if (min_message_id <= last_added_message_id.get_next_message_id(MessageType::Server)) {
          // connect local messages with last received server message
          set_dialog_first_database_message_id(d, last_added_message_id, "on_get_history 2");
        } else {
          LOG(WARNING) << "Have last " << d->last_message_id << " and first database " << d->first_database_message_id
                       << " in " << dialog_id << ", but received history from the end only up to "
                       << last_added_message_id;
          // can't connect messages, because there can be unknown server messages after last_added_message_id
        }
      }
    }
    if (!d->last_database_message_id.is_valid()) {
      CHECK(d->last_message_id.is_valid());
      MessagesConstIterator it(d, d->last_message_id);
      MessageId new_first_database_message_id;
      while (*it != nullptr) {
        auto message_id = (*it)->message_id;
        if (message_id.is_server() || message_id.is_local()) {
          if (!d->last_database_message_id.is_valid()) {
            set_dialog_last_database_message_id(d, message_id, "on_get_history");
          }
          new_first_database_message_id = message_id;
          try_restore_dialog_reply_markup(d, *it);
        }
        --it;
      }
      if (new_first_database_message_id.is_valid()) {
        set_dialog_first_database_message_id(d, new_first_database_message_id, "on_get_history");
      }
    } else {
      LOG_CHECK(d->last_new_message_id.is_valid())
          << dialog_id << " " << from_the_end << " " << d->first_database_message_id << " "
          << d->last_database_message_id << " " << first_added_message_id << " " << last_added_message_id << " "
          << d->last_message_id << " " << d->last_new_message_id << " " << d->have_full_history << " "
          << prev_last_new_message_id << " " << prev_first_database_message_id << " " << prev_last_database_message_id
          << " " << prev_last_message_id << " " << prev_have_full_history << " " << d->debug_last_new_message_id << " "
          << d->debug_first_database_message_id << " " << d->debug_last_database_message_id << " " << from_message_id
          << " " << offset << " " << limit << " " << messages.size() << " " << last_received_message_id << " "
          << d->debug_set_dialog_last_database_message_id;
      CHECK(d->first_database_message_id.is_valid());
      {
        MessagesConstIterator it(d, d->first_database_message_id);
        if (*it != nullptr && ((*it)->message_id == d->first_database_message_id || (*it)->have_next)) {
          MessageId new_first_database_message_id = d->first_database_message_id;
          while (*it != nullptr) {
            auto message_id = (*it)->message_id;
            if ((message_id.is_server() || message_id.is_local()) && message_id < new_first_database_message_id) {
              new_first_database_message_id = message_id;
              try_restore_dialog_reply_markup(d, *it);
            }
            --it;
          }
          if (new_first_database_message_id != d->first_database_message_id) {
            set_dialog_first_database_message_id(d, new_first_database_message_id, "on_get_history 2");
          }
        }
      }
      {
        MessagesConstIterator it(d, d->last_database_message_id);
        if (*it != nullptr && ((*it)->message_id == d->last_database_message_id || (*it)->have_next)) {
          MessageId new_last_database_message_id = d->last_database_message_id;
          while (*it != nullptr) {
            auto message_id = (*it)->message_id;
            if ((message_id.is_server() || message_id.is_local()) && message_id > new_last_database_message_id) {
              new_last_database_message_id = message_id;
            }
            ++it;
          }
          if (new_last_database_message_id != d->last_database_message_id) {
            set_dialog_last_database_message_id(d, new_last_database_message_id, "on_get_history 2");
          }
        }
      }
    }
    LOG_CHECK(d->first_database_message_id.is_valid())
        << dialog_id << " " << from_the_end << " " << d->first_database_message_id << " " << d->last_database_message_id
        << " " << first_added_message_id << " " << last_added_message_id << " " << d->last_message_id << " "
        << d->last_new_message_id << " " << d->have_full_history << " " << prev_last_new_message_id << " "
        << prev_first_database_message_id << " " << prev_last_database_message_id << " " << prev_last_message_id << " "
        << prev_have_full_history << " " << d->debug_last_new_message_id << " " << d->debug_first_database_message_id
        << " " << d->debug_last_database_message_id << " " << from_message_id << " " << offset << " " << limit << " "
        << messages.size() << " " << last_received_message_id << " " << d->debug_set_dialog_last_database_message_id;
    CHECK(d->last_database_message_id.is_valid());

    for (auto &first_message_id : d->first_database_message_id_by_index) {
      if (first_added_message_id < first_message_id && first_message_id <= last_added_message_id) {
        first_message_id = first_added_message_id;
      }
    }
  }
  promise.set_value(Unit());
}

class GetAllScheduledMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  uint32 generation_;

 public:
  explicit GetAllScheduledMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int64 hash, uint32 generation) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    dialog_id_ = dialog_id;
    generation_ = generation;

    send_query(
        G()->net_query_creator().create(telegram_api::messages_getScheduledHistory(std::move(input_peer), hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getScheduledHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    if (result_ptr.ok()->get_id() == telegram_api::messages_messagesNotModified::ID) {
      td_->messages_manager_->on_get_scheduled_server_messages(dialog_id_, generation_, Auto(), true);
    } else {
      auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetAllScheduledMessagesQuery");
      td_->messages_manager_->on_get_scheduled_server_messages(dialog_id_, generation_, std::move(info.messages),
                                                               false);
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetAllScheduledMessagesQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::load_dialog_scheduled_messages(DialogId dialog_id, bool from_database, int64 hash,
                                                     Promise<Unit> &&promise) {
  if (G()->parameters().use_message_db && from_database) {
    LOG(INFO) << "Load scheduled messages from database in " << dialog_id;
    auto &queries = load_scheduled_messages_from_database_queries_[dialog_id];
    queries.push_back(std::move(promise));
    if (queries.size() == 1) {
      G()->td_db()->get_messages_db_async()->get_scheduled_messages(
          dialog_id, 1000,
          PromiseCreator::lambda([dialog_id, actor_id = actor_id(this)](vector<MessagesDbDialogMessage> messages) {
            send_closure(actor_id, &MessagesManager::on_get_scheduled_messages_from_database, dialog_id,
                         std::move(messages));
          }));
    }
  } else {
    td_->create_handler<GetAllScheduledMessagesQuery>(std::move(promise))
        ->send(dialog_id, hash, scheduled_messages_sync_generation_);
  }
}

void MessagesManager::on_send_dialog_action_timeout(DialogId dialog_id) {
  LOG(INFO) << "Receive send_chat_action timeout in " << dialog_id;
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  if (can_send_message(dialog_id).is_error()) {
    return;
  }

  auto queue_id = get_sequence_dispatcher_id(dialog_id, MessageContentType::Photo);
  CHECK(queue_id & 1);

  auto queue_it = yet_unsent_media_queues_.find(queue_id);
  if (queue_it == yet_unsent_media_queues_.end()) {
    return;
  }

  pending_send_dialog_action_timeout_.add_timeout_in(dialog_id.get(), 4.0);

  CHECK(!queue_it->second.empty());
  const Message *m = get_message(d, queue_it->second.begin()->first);
  if (m == nullptr) {
    return;
  }
  CHECK(m->message_id.is_yet_unsent());
  if (m->forward_info != nullptr || m->had_forward_info || m->message_id.is_scheduled() ||
      m->sender_dialog_id.is_valid()) {
    return;
  }

  auto file_id = get_message_content_upload_file_id(m->content.get());
  if (!file_id.is_valid()) {
    LOG(ERROR) << "Have no file in "
               << to_string(get_message_content_object(m->content.get(), td_, dialog_id, m->date, m->is_content_secret,
                                                       false, -1));
    return;
  }
  auto file_view = td_->file_manager_->get_file_view(file_id);
  if (!file_view.is_uploading()) {
    return;
  }
  int64 total_size = file_view.expected_size();
  int64 uploaded_size = file_view.remote_size();
  int32 progress = 0;
  if (total_size > 0 && uploaded_size > 0) {
    if (uploaded_size > total_size) {
      uploaded_size = total_size;  // just in case
    }
    progress = static_cast<int32>(100 * uploaded_size / total_size);
  }

  DialogAction action = DialogAction::get_uploading_action(m->content->get_type(), progress);
  if (action == DialogAction()) {
    return;
  }
  LOG(INFO) << "Send " << action << " in " << dialog_id;
  send_dialog_action(dialog_id, m->top_thread_message_id, std::move(action), Promise<Unit>());
}

FullMessageId MessagesManager::get_replied_message(DialogId dialog_id, MessageId message_id, bool force,
                                                   Promise<Unit> &&promise) {
  LOG(INFO) << "Get replied message to " << message_id << " in " << dialog_id;
  Dialog *d = get_dialog_force(dialog_id, "get_replied_message");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return FullMessageId();
  }

  message_id = get_persistent_message_id(d, message_id);
  auto m = get_message_force(d, message_id, "get_replied_message");
  if (m == nullptr) {
    if (force) {
      promise.set_value(Unit());
    } else {
      get_message_force_from_server(d, message_id, std::move(promise));
    }
    return FullMessageId();
  }

  tl_object_ptr<telegram_api::InputMessage> input_message;
  auto replied_message_id = get_replied_message_id(dialog_id, m);
  if (replied_message_id.get_dialog_id() != dialog_id) {
    dialog_id = replied_message_id.get_dialog_id();
    if (!have_dialog_info_force(dialog_id)) {
      promise.set_value(Unit());
      return {};
    }
    if (!have_input_peer(dialog_id, AccessRights::Read)) {
      promise.set_value(Unit());
      return {};
    }

    force_create_dialog(dialog_id, "get_replied_message");
    d = get_dialog_force(dialog_id, "get_replied_message");
    if (d == nullptr) {
      promise.set_error(Status::Error(500, "Chat with replied message not found"));
      return {};
    }
  } else if (m->message_id.is_valid() && m->message_id.is_server()) {
    input_message = make_tl_object<telegram_api::inputMessageReplyTo>(m->message_id.get_server_message_id().get());
  }
  get_message_force_from_server(d, replied_message_id.get_message_id(), std::move(promise), std::move(input_message));

  return replied_message_id;
}

bool MessagesManager::update_message_is_pinned(Dialog *d, Message *m, bool is_pinned, const char *source) {
  CHECK(m != nullptr);
  CHECK(!m->message_id.is_scheduled());
  if (m->is_pinned == is_pinned) {
    return false;
  }

  LOG(INFO) << "Update message is_pinned of " << m->message_id << " in " << d->dialog_id << " to " << is_pinned
            << " from " << source;
  auto old_index_mask = get_message_index_mask(d->dialog_id, m);
  m->is_pinned = is_pinned;
  auto new_index_mask = get_message_index_mask(d->dialog_id, m);
  update_message_count_by_index(d, -1, old_index_mask & ~new_index_mask);
  update_message_count_by_index(d, +1, new_index_mask & ~old_index_mask);

  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateMessageIsPinned>(d->dialog_id.get(), m->message_id.get(), is_pinned));
  if (is_pinned) {
    if (d->is_last_pinned_message_id_inited && m->message_id > d->last_pinned_message_id) {
      set_dialog_last_pinned_message_id(d, m->message_id);
    }
  } else {
    if (d->is_last_pinned_message_id_inited && m->message_id == d->last_pinned_message_id) {
      if (d->message_count_by_index[message_search_filter_index(MessageSearchFilter::Pinned)] == 0) {
        set_dialog_last_pinned_message_id(d, MessageId());
      } else {
        drop_dialog_last_pinned_message_id(d);
      }
    }
  }
  return true;
}

void MessagesManager::speculatively_update_channel_participants(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  if (!m->message_id.is_any_server() || dialog_id.get_type() != DialogType::Channel || !m->sender_user_id.is_valid()) {
    return;
  }

  auto channel_id = dialog_id.get_channel_id();
  UserId my_user_id(td_->contacts_manager_->get_my_id());
  bool by_me = m->sender_user_id == my_user_id;
  switch (m->content->get_type()) {
    case MessageContentType::ChatAddUsers:
      send_closure_later(G()->contacts_manager(), &ContactsManager::speculative_add_channel_participants, channel_id,
                         get_message_content_added_user_ids(m->content.get()), m->sender_user_id, m->date, by_me);
      break;
    case MessageContentType::ChatJoinedByLink:
      send_closure_later(G()->contacts_manager(), &ContactsManager::speculative_add_channel_participants, channel_id,
                         vector<UserId>{m->sender_user_id}, m->sender_user_id, m->date, by_me);
      break;
    case MessageContentType::ChatDeleteUser:
      send_closure_later(G()->contacts_manager(), &ContactsManager::speculative_delete_channel_participant, channel_id,
                         get_message_content_deleted_user_id(m->content.get()), by_me);
      break;
    default:
      break;
  }
}

void MessagesManager::on_update_read_message_comments(DialogId dialog_id, MessageId message_id,
                                                      MessageId max_message_id, MessageId last_read_inbox_message_id,
                                                      MessageId last_read_outbox_message_id) {
  Dialog *d = get_dialog_force(dialog_id, "on_update_read_message_comments");
  if (d == nullptr) {
    LOG(INFO) << "Ignore update of read message comments in unknown " << dialog_id << " in updateReadDiscussion";
    return;
  }

  auto m = get_message_force(d, message_id, "on_update_read_message_comments");
  if (m == nullptr || !m->message_id.is_server() || m->top_thread_message_id != m->message_id ||
      !is_active_message_reply_info(dialog_id, m->reply_info)) {
    return;
  }
  if (m->reply_info.update_max_message_ids(max_message_id, last_read_inbox_message_id, last_read_outbox_message_id)) {
    on_message_reply_info_changed(dialog_id, m);
    on_message_changed(d, m, true, "on_update_read_message_comments");
  }
}

int32 MessagesManager::calc_new_unread_count_from_last_unread(Dialog *d, MessageId max_message_id,
                                                              MessageType type) const {
  CHECK(!max_message_id.is_scheduled());
  MessagesConstIterator it(d, max_message_id);
  if (*it == nullptr || (*it)->message_id != max_message_id) {
    return -1;
  }

  int32 unread_count = type == MessageType::Server ? d->server_unread_count : d->local_unread_count;
  while (*it != nullptr && (*it)->message_id > d->last_read_inbox_message_id) {
    if (has_incoming_notification(d->dialog_id, *it) && (*it)->message_id.get_type() == type) {
      unread_count--;
    }
    --it;
  }
  if (*it == nullptr || (*it)->message_id != d->last_read_inbox_message_id) {
    return -1;
  }

  LOG(INFO) << "Found " << unread_count << " unread messages in " << d->dialog_id << " from last unread message";
  return unread_count;
}

void MessagesManager::ttl_unregister_message(DialogId dialog_id, const Message *m, const char *source) {
  if (m->ttl_expires_at == 0) {
    return;
  }
  CHECK(!m->message_id.is_scheduled());

  auto it = ttl_nodes_.find(TtlNode(dialog_id, m->message_id, false));

  // expect m->ttl == 0, but m->ttl_expires_at > 0 from binlog
  LOG_CHECK(it != ttl_nodes_.end()) << dialog_id << " " << m->message_id << " " << source << " " << G()->close_flag()
                                    << " " << m->ttl << " " << m->ttl_expires_at << " " << Time::now() << " "
                                    << m->from_database;

  auto *heap_node = it->as_heap_node();
  if (heap_node->in_heap()) {
    ttl_heap_.erase(heap_node);
  }
  ttl_nodes_.erase(it);
  ttl_update_timeout(Time::now());
}

MessageId MessagesManager::find_old_message_id(DialogId dialog_id, MessageId message_id) const {
  if (message_id.is_scheduled()) {
    CHECK(message_id.is_scheduled_server());
    auto dialog_it = update_scheduled_message_ids_.find(dialog_id);
    if (dialog_it != update_scheduled_message_ids_.end()) {
      auto it = dialog_it->second.find(message_id.get_scheduled_server_message_id());
      if (it != dialog_it->second.end()) {
        return it->second;
      }
    }
  } else {
    CHECK(message_id.is_server());
    auto it = update_message_ids_.find(FullMessageId(dialog_id, message_id));
    if (it != update_message_ids_.end()) {
      return it->second;
    }
  }
  return MessageId();
}

uint64 MessagesManager::get_sequence_dispatcher_id(DialogId dialog_id, MessageContentType message_content_type) {
  switch (message_content_type) {
    case MessageContentType::Animation:
    case MessageContentType::Audio:
    case MessageContentType::Document:
    case MessageContentType::Photo:
    case MessageContentType::Sticker:
    case MessageContentType::Video:
    case MessageContentType::VideoNote:
    case MessageContentType::VoiceNote:
      return static_cast<uint64>(dialog_id.get() * 2 + 1);
    default:
      return static_cast<uint64>(dialog_id.get() * 2 + 2);
  }
}

bool MessagesManager::get_dialog_has_scheduled_messages(const Dialog *d) const {
  if (!have_input_peer(d->dialog_id, AccessRights::Read)) {
    return false;
  }
  if (is_broadcast_channel(d->dialog_id) &&
      !td_->contacts_manager_->get_channel_status(d->dialog_id.get_channel_id()).can_post_messages()) {
    return false;
  }
  // TODO send updateChatHasScheduledMessage when can_post_messages changes

  return d->has_scheduled_server_messages || d->has_scheduled_database_messages || d->scheduled_messages != nullptr;
}

void MessagesManager::send_update_message_edited(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  cancel_dialog_action(dialog_id, m);
  auto edit_date = m->hide_edit_date ? 0 : m->edit_date;
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateMessageEdited>(dialog_id.get(), m->message_id.get(), edit_date,
                                                           get_reply_markup_object(m->reply_markup)));
}

void MessagesManager::on_pending_message_views_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::on_pending_message_views_timeout,
                     DialogId(dialog_id_int));
}

void MessagesManager::on_load_active_live_location_messages_finished() {
  are_active_live_location_messages_loaded_ = true;
  auto promises = std::move(load_active_live_location_messages_queries_);
  load_active_live_location_messages_queries_.clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

int64 MessagesManager::get_dialog_order(MessageId message_id, int32 message_date) {
  CHECK(!message_id.is_scheduled());
  return (static_cast<int64>(message_date) << 32) +
         message_id.get_prev_server_message_id().get_server_message_id().get();
}

bool MessagesManager::is_dialog_opened(DialogId dialog_id) const {
  const Dialog *d = get_dialog(dialog_id);
  return d != nullptr && d->is_opened;
}
}  // namespace td
