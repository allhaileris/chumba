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

#include <limits>



#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

FullMessageId MessagesManager::on_send_message_success(int64 random_id, MessageId new_message_id, int32 date,
                                                       int32 ttl_period, FileId new_file_id, const char *source) {
  CHECK(source != nullptr);
  // do not try to run getDifference from this function
  if (DROP_SEND_MESSAGE_UPDATES) {
    return {};
  }
  if (!new_message_id.is_valid()) {
    LOG(ERROR) << "Receive " << new_message_id << " as sent message from " << source;
    on_send_message_fail(
        random_id,
        Status::Error(500, "Internal Server Error: receive invalid message identifier as sent message identifier"));
    return {};
  }
  if (new_message_id.is_yet_unsent()) {
    LOG(ERROR) << "Receive " << new_message_id << " as sent message from " << source;
    on_send_message_fail(random_id,
                         Status::Error(500, "Internal Server Error: receive yet unsent message as sent message"));
    return {};
  }

  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    LOG(ERROR) << "Result from sendMessage for " << new_message_id << " with random_id " << random_id << " sent at "
               << date << " comes from " << source << " after updateNewMessageId, but was not discarded by pts";
    return {};
  }

  auto dialog_id = it->second.get_dialog_id();
  auto old_message_id = it->second.get_message_id();

  if (new_message_id.is_local() && dialog_id.get_type() != DialogType::SecretChat) {
    LOG(ERROR) << "Receive " << new_message_id << " as sent message from " << source;
    on_send_message_fail(random_id, Status::Error(500, "Internal Server Error: receive local as sent message"));
    return {};
  }

  being_sent_messages_.erase(it);

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  bool need_update_dialog_pos = false;
  being_readded_message_id_ = {dialog_id, old_message_id};
  unique_ptr<Message> sent_message = delete_message(d, old_message_id, false, &need_update_dialog_pos, source);
  if (sent_message == nullptr) {
    delete_sent_message_on_server(dialog_id, new_message_id);
    being_readded_message_id_ = FullMessageId();
    return {};
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // LOG(ERROR) << "Found " << old_message_id << " in inaccessible " << dialog_id;
    // dump_debug_message_op(d, 5);
  }

  // imitation of update_message(d, sent_message.get(), std::move(new_message), &need_update_dialog_pos, false);
  if (date <= 0) {
    LOG(ERROR) << "Receive " << new_message_id << " in " << dialog_id << " with wrong date " << date << " from "
               << source;
  } else {
    LOG_CHECK(sent_message->date > 0) << old_message_id << ' ' << sent_message->message_id << ' ' << new_message_id
                                      << ' ' << sent_message->date << ' ' << date << ' ' << source;
    sent_message->date = date;
    CHECK(d->last_message_id != old_message_id);
  }

  sent_message->ttl_period = ttl_period;

  // reply_to message may be already deleted
  // but can't use get_message_force for check, because the message can be already unloaded from the memory
  // if (get_message_force(d, sent_message->reply_to_message_id, "on_send_message_success 2") == nullptr) {
  //   sent_message->reply_to_message_id = MessageId();
  // }

  if (merge_message_content_file_id(td_, sent_message->content.get(), new_file_id)) {
    send_update_message_content(d, sent_message.get(), false, source);
  }

  if (old_message_id.is_valid() && new_message_id < old_message_id && !has_qts_messages(dialog_id)) {
    LOG(ERROR) << "Sent " << old_message_id << " to " << dialog_id << " as " << new_message_id;
  }

  set_message_id(sent_message, new_message_id);

  sent_message->from_database = false;
  sent_message->have_previous = true;
  sent_message->have_next = true;

  send_update_message_send_succeeded(d, old_message_id, sent_message.get());

  bool need_update = true;
  Message *m = add_message_to_dialog(d, std::move(sent_message), true, &need_update, &need_update_dialog_pos, source);
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "on_send_message_success");
  }

  if (m == nullptr) {
    if (!(old_message_id.is_valid() && new_message_id < old_message_id) &&
        !(ttl_period > 0 && date + ttl_period <= G()->server_time())) {
      // if message ID has decreased, which could happen if some messages were lost,
      // or the message has already been deleted after TTL period, then the error is expected
      LOG(ERROR) << "Failed to add just sent " << old_message_id << " to " << dialog_id << " as " << new_message_id
                 << " from " << source << ": " << debug_add_message_to_dialog_fail_reason_;
    }
    send_update_delete_messages(dialog_id, {new_message_id.get()}, true, false);
    being_readded_message_id_ = FullMessageId();
    return {};
  }

  try_add_active_live_location(dialog_id, m);
  update_reply_count_by_message(d, +1, m);
  update_forward_count(dialog_id, m);
  being_readded_message_id_ = FullMessageId();
  return {dialog_id, new_message_id};
}

class GetDiscussionMessageQuery final : public Td::ResultHandler {
  Promise<MessageThreadInfo> promise_;
  DialogId dialog_id_;
  MessageId message_id_;
  DialogId expected_dialog_id_;
  MessageId expected_message_id_;

 public:
  explicit GetDiscussionMessageQuery(Promise<MessageThreadInfo> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId message_id, DialogId expected_dialog_id, MessageId expected_message_id) {
    dialog_id_ = dialog_id;
    message_id_ = message_id;
    expected_dialog_id_ = expected_dialog_id;
    expected_message_id_ = expected_message_id;
    CHECK(expected_dialog_id_.is_valid());
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getDiscussionMessage(std::move(input_peer), message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getDiscussionMessage>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->messages_manager_->process_discussion_message(result_ptr.move_as_ok(), dialog_id_, message_id_,
                                                       expected_dialog_id_, expected_message_id_, std::move(promise_));
  }

  void on_error(Status status) final {
    if (expected_dialog_id_ == dialog_id_) {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetDiscussionMessageQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_message_thread(DialogId dialog_id, MessageId message_id,
                                         Promise<MessageThreadInfo> &&promise) {
  LOG(INFO) << "Get message thread from " << message_id << " in " << dialog_id;
  Dialog *d = get_dialog_force(dialog_id, "get_message_thread");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }
  if (dialog_id.get_type() != DialogType::Channel) {
    return promise.set_error(Status::Error(400, "Chat is not a supergroup or a channel"));
  }
  if (message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Scheduled messages can't have message threads"));
  }

  auto m = get_message_force(d, message_id, "get_message_thread");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  TRY_RESULT_PROMISE(promise, top_thread_full_message_id, get_top_thread_full_message_id(dialog_id, m));

  auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, message_id,
                                               promise = std::move(promise)](Result<MessageThreadInfo> result) mutable {
    if (result.is_error()) {
      return promise.set_error(result.move_as_error());
    }
    send_closure(actor_id, &MessagesManager::on_get_discussion_message, dialog_id, message_id, result.move_as_ok(),
                 std::move(promise));
  });

  td_->create_handler<GetDiscussionMessageQuery>(std::move(query_promise))
      ->send(dialog_id, message_id, top_thread_full_message_id.get_dialog_id(),
             top_thread_full_message_id.get_message_id());
}

void MessagesManager::on_get_message_link_message(MessageLinkInfo &&info, DialogId dialog_id,
                                                  Promise<MessageLinkInfo> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto message_id = info.message_id;
  Message *m = get_message_force({dialog_id, message_id}, "on_get_message_link_message");
  if (info.comment_message_id == MessageId() || m == nullptr || !is_broadcast_channel(dialog_id) ||
      !m->reply_info.is_comment || !is_active_message_reply_info(dialog_id, m->reply_info)) {
    return promise.set_value(std::move(info));
  }

  if (td_->contacts_manager_->have_channel_force(m->reply_info.channel_id)) {
    force_create_dialog(DialogId(m->reply_info.channel_id), "on_get_message_link_message");
    on_get_message_link_discussion_message(std::move(info), DialogId(m->reply_info.channel_id), std::move(promise));
    return;
  }

  auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), info = std::move(info),
                                               promise = std::move(promise)](Result<MessageThreadInfo> result) mutable {
    if (result.is_error() || result.ok().message_ids.empty()) {
      return promise.set_value(std::move(info));
    }
    send_closure(actor_id, &MessagesManager::on_get_message_link_discussion_message, std::move(info),
                 result.ok().dialog_id, std::move(promise));
  });

  td_->create_handler<GetDiscussionMessageQuery>(std::move(query_promise))
      ->send(dialog_id, message_id, DialogId(m->reply_info.channel_id), MessageId());
}

void MessagesManager::on_get_secret_message(SecretChatId secret_chat_id, UserId user_id, MessageId message_id,
                                            int32 date, unique_ptr<EncryptedFile> file,
                                            tl_object_ptr<secret_api::decryptedMessage> message, Promise<> promise) {
  LOG(DEBUG) << "On get " << to_string(message);
  CHECK(message != nullptr);
  CHECK(secret_chat_id.is_valid());
  CHECK(user_id.is_valid());
  CHECK(message_id.is_valid());
  CHECK(date > 0);

  auto pending_secret_message = make_unique<PendingSecretMessage>();
  pending_secret_message->success_promise = std::move(promise);
  MessageInfo &message_info = pending_secret_message->message_info;
  message_info.dialog_id = DialogId(secret_chat_id);
  message_info.message_id = message_id;
  message_info.sender_user_id = user_id;
  message_info.date = date;
  message_info.random_id = message->random_id_;
  message_info.ttl = message->ttl_;

  Dialog *d = get_dialog_force(message_info.dialog_id, "on_get_secret_message");
  if (d == nullptr && have_dialog_info_force(message_info.dialog_id)) {
    force_create_dialog(message_info.dialog_id, "on_get_secret_message", true, true);
    d = get_dialog(message_info.dialog_id);
  }
  if (d == nullptr) {
    LOG(ERROR) << "Ignore secret message in unknown " << message_info.dialog_id;
    pending_secret_message->success_promise.set_error(Status::Error(500, "Chat not found"));
    return;
  }

  pending_secret_message->load_data_multipromise.add_promise(Auto());
  auto lock_promise = pending_secret_message->load_data_multipromise.get_promise();

  int32 flags = MESSAGE_FLAG_HAS_UNREAD_CONTENT | MESSAGE_FLAG_HAS_FROM_ID;
  if ((message->flags_ & secret_api::decryptedMessage::REPLY_TO_RANDOM_ID_MASK) != 0) {
    message_info.reply_to_message_id = get_message_id_by_random_id(
        get_dialog(message_info.dialog_id), message->reply_to_random_id_, "on_get_secret_message");
    if (message_info.reply_to_message_id.is_valid()) {
      flags |= MESSAGE_FLAG_IS_REPLY;
    }
  }
  if ((message->flags_ & secret_api::decryptedMessage::ENTITIES_MASK) != 0) {
    flags |= MESSAGE_FLAG_HAS_ENTITIES;
  }
  if ((message->flags_ & secret_api::decryptedMessage::MEDIA_MASK) != 0) {
    flags |= MESSAGE_FLAG_HAS_MEDIA;
  }
  if ((message->flags_ & secret_api::decryptedMessage::SILENT_MASK) != 0) {
    flags |= MESSAGE_FLAG_IS_SILENT;
  }

  if (!clean_input_string(message->via_bot_name_)) {
    LOG(WARNING) << "Receive invalid bot username " << message->via_bot_name_;
    message->via_bot_name_.clear();
  }
  if (!message->via_bot_name_.empty()) {
    auto request_promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), via_bot_username = message->via_bot_name_, message_info_ptr = &message_info,
         promise = pending_secret_message->load_data_multipromise.get_promise()](Unit) mutable {
          send_closure(actor_id, &MessagesManager::on_resolve_secret_chat_message_via_bot_username, via_bot_username,
                       message_info_ptr, std::move(promise));
        });
    search_public_dialog(message->via_bot_name_, false, std::move(request_promise));
  }
  if ((message->flags_ & secret_api::decryptedMessage::GROUPED_ID_MASK) != 0 && message->grouped_id_ != 0) {
    message_info.media_album_id = message->grouped_id_;
    flags |= MESSAGE_FLAG_HAS_MEDIA_ALBUM_ID;
  }

  message_info.flags = flags;
  message_info.content = get_secret_message_content(
      td_, std::move(message->message_), std::move(file), std::move(message->media_), std::move(message->entities_),
      message_info.dialog_id, pending_secret_message->load_data_multipromise);

  add_secret_message(std::move(pending_secret_message), std::move(lock_promise));
}

vector<MessageId> MessagesManager::get_dialog_scheduled_messages(DialogId dialog_id, bool force, bool ignore_result,
                                                                 Promise<Unit> &&promise) {
  LOG(INFO) << "Get scheduled messages in " << dialog_id;
  if (G()->close_flag()) {
    promise.set_error(Global::request_aborted_error());
    return {};
  }

  Dialog *d = get_dialog_force(dialog_id, "get_dialog_scheduled_messages");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return {};
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return {};
  }
  if (is_broadcast_channel(dialog_id) &&
      !td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id()).can_post_messages()) {
    promise.set_error(Status::Error(400, "Not enough rights to get scheduled messages"));
    return {};
  }

  if (!d->has_loaded_scheduled_messages_from_database) {
    load_dialog_scheduled_messages(dialog_id, true, 0, std::move(promise));
    return {};
  }

  vector<MessageId> message_ids;
  find_old_messages(d->scheduled_messages.get(),
                    MessageId(ScheduledServerMessageId(), std::numeric_limits<int32>::max(), true), message_ids);
  std::reverse(message_ids.begin(), message_ids.end());

  if (G()->parameters().use_message_db) {
    bool has_scheduled_database_messages = false;
    for (auto &message_id : message_ids) {
      CHECK(message_id.is_valid_scheduled());
      if (!message_id.is_yet_unsent()) {
        has_scheduled_database_messages = true;
        break;
      }
    }
    set_dialog_has_scheduled_database_messages(d->dialog_id, has_scheduled_database_messages);
  }

  if (d->scheduled_messages_sync_generation != scheduled_messages_sync_generation_) {
    vector<uint64> numbers;
    for (auto &message_id : message_ids) {
      if (!message_id.is_scheduled_server()) {
        continue;
      }

      numbers.push_back(message_id.get_scheduled_server_message_id().get());
      const Message *m = get_message(d, message_id);
      CHECK(m != nullptr);
      CHECK(m->message_id.get_scheduled_server_message_id() == message_id.get_scheduled_server_message_id());
      numbers.push_back(m->edit_date);
      numbers.push_back(m->date);
    }
    auto hash = get_vector_hash(numbers);

    if (!force && (d->has_scheduled_server_messages ||
                   (d->scheduled_messages_sync_generation == 0 && !G()->parameters().use_message_db))) {
      load_dialog_scheduled_messages(dialog_id, false, hash, std::move(promise));
      return {};
    }
    load_dialog_scheduled_messages(dialog_id, false, hash, Promise<Unit>());
  }

  if (!ignore_result) {
    d->sent_scheduled_messages = true;
  }

  promise.set_value(Unit());
  return message_ids;
}

void MessagesManager::cancel_send_message_query(DialogId dialog_id, Message *m) {
  CHECK(m != nullptr);
  CHECK(m->content != nullptr);
  CHECK(m->message_id.is_valid() || m->message_id.is_valid_scheduled());
  CHECK(m->message_id.is_yet_unsent());
  LOG(INFO) << "Cancel send message query for " << m->message_id;

  cancel_upload_message_content_files(m->content.get());

  CHECK(m->edited_content == nullptr);

  if (!m->send_query_ref.empty()) {
    LOG(INFO) << "Cancel send query for " << m->message_id;
    cancel_query(m->send_query_ref);
    m->send_query_ref = NetQueryRef();
  }

  if (m->send_message_log_event_id != 0) {
    LOG(INFO) << "Delete send message log event for " << m->message_id;
    binlog_erase(G()->td_db()->get_binlog(), m->send_message_log_event_id);
    m->send_message_log_event_id = 0;
  }

  if (m->reply_to_message_id.is_valid() && !m->reply_to_message_id.is_yet_unsent()) {
    auto it = replied_by_yet_unsent_messages_.find({dialog_id, m->reply_to_message_id});
    CHECK(it != replied_by_yet_unsent_messages_.end());
    it->second--;
    CHECK(it->second >= 0);
    if (it->second == 0) {
      replied_by_yet_unsent_messages_.erase(it);
    }
  }

  if (m->media_album_id != 0) {
    send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id, dialog_id,
                       m->message_id, Status::OK());
  }

  if (!m->message_id.is_scheduled() && G()->parameters().use_file_db &&
      !m->is_copy) {  // ResourceManager::Mode::Baseline
    auto queue_id = get_sequence_dispatcher_id(dialog_id, m->content->get_type());
    if (queue_id & 1) {
      auto queue_it = yet_unsent_media_queues_.find(queue_id);
      if (queue_it != yet_unsent_media_queues_.end()) {
        auto &queue = queue_it->second;
        LOG(INFO) << "Delete " << m->message_id << " from queue " << queue_id;
        if (queue.erase(m->message_id) != 0) {
          if (queue.empty()) {
            yet_unsent_media_queues_.erase(queue_it);
          } else {
            // send later, because do_delete_all_dialog_messages can be called right now
            send_closure_later(actor_id(this), &MessagesManager::on_yet_unsent_media_queue_updated, dialog_id);
          }
        }
      }
    }
  }
}

void MessagesManager::sort_dialog_filter_input_dialog_ids(DialogFilter *dialog_filter, const char *source) const {
  auto sort_input_dialog_ids = [contacts_manager =
                                    td_->contacts_manager_.get()](vector<InputDialogId> &input_dialog_ids) {
    std::sort(input_dialog_ids.begin(), input_dialog_ids.end(),
              [contacts_manager](InputDialogId lhs, InputDialogId rhs) {
                auto get_order = [contacts_manager](InputDialogId input_dialog_id) {
                  auto dialog_id = input_dialog_id.get_dialog_id();
                  if (dialog_id.get_type() != DialogType::SecretChat) {
                    return dialog_id.get() * 10;
                  }
                  auto user_id = contacts_manager->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
                  return DialogId(user_id).get() * 10 + 1;
                };
                return get_order(lhs) < get_order(rhs);
              });
  };

  if (!dialog_filter->include_contacts && !dialog_filter->include_non_contacts && !dialog_filter->include_bots &&
      !dialog_filter->include_groups && !dialog_filter->include_channels) {
    dialog_filter->excluded_dialog_ids.clear();
  }

  sort_input_dialog_ids(dialog_filter->excluded_dialog_ids);
  sort_input_dialog_ids(dialog_filter->included_dialog_ids);

  std::unordered_set<DialogId, DialogIdHash> all_dialog_ids;
  for (auto input_dialog_ids :
       {&dialog_filter->pinned_dialog_ids, &dialog_filter->excluded_dialog_ids, &dialog_filter->included_dialog_ids}) {
    for (const auto &input_dialog_id : *input_dialog_ids) {
      LOG_CHECK(all_dialog_ids.insert(input_dialog_id.get_dialog_id()).second)
          << source << ' ' << input_dialog_id.get_dialog_id() << ' ' << dialog_filter;
    }
  }
}

MessagesManager::Message *MessagesManager::get_message_to_send(
    Dialog *d, MessageId top_thread_message_id, MessageId reply_to_message_id, const MessageSendOptions &options,
    unique_ptr<MessageContent> &&content, bool *need_update_dialog_pos, bool suppress_reply_info,
    unique_ptr<MessageForwardInfo> forward_info, bool is_copy, DialogId send_as_dialog_id) {
  d->was_opened = true;

  auto message = create_message_to_send(d, top_thread_message_id, reply_to_message_id, options, std::move(content),
                                        suppress_reply_info, std::move(forward_info), is_copy, send_as_dialog_id);

  MessageId message_id = options.schedule_date != 0 ? get_next_yet_unsent_scheduled_message_id(d, options.schedule_date)
                                                    : get_next_yet_unsent_message_id(d);
  set_message_id(message, message_id);

  message->have_previous = true;
  message->have_next = true;

  message->random_id = generate_new_random_id();

  bool need_update = false;
  CHECK(have_input_peer(d->dialog_id, AccessRights::Read));
  auto result =
      add_message_to_dialog(d, std::move(message), true, &need_update, need_update_dialog_pos, "send message");
  LOG_CHECK(result != nullptr) << message_id << " " << debug_add_message_to_dialog_fail_reason_;
  if (result->message_id.is_scheduled()) {
    send_update_chat_has_scheduled_messages(d, false);
  }
  return result;
}

bool MessagesManager::set_dialog_filters_order(vector<unique_ptr<DialogFilter>> &dialog_filters,
                                               vector<DialogFilterId> dialog_filter_ids) {
  auto old_dialog_filter_ids = get_dialog_filter_ids(dialog_filters);
  if (old_dialog_filter_ids == dialog_filter_ids) {
    return false;
  }
  LOG(INFO) << "Reorder chat filters from " << old_dialog_filter_ids << " to " << dialog_filter_ids;

  if (dialog_filter_ids.size() != old_dialog_filter_ids.size()) {
    for (auto dialog_filter_id : old_dialog_filter_ids) {
      if (!td::contains(dialog_filter_ids, dialog_filter_id)) {
        dialog_filter_ids.push_back(dialog_filter_id);
      }
    }
    CHECK(dialog_filter_ids.size() == old_dialog_filter_ids.size());
  }
  if (old_dialog_filter_ids == dialog_filter_ids) {
    return false;
  }

  CHECK(dialog_filter_ids.size() == dialog_filters.size());
  for (size_t i = 0; i < dialog_filters.size(); i++) {
    for (size_t j = i; j < dialog_filters.size(); j++) {
      if (dialog_filters[j]->dialog_filter_id == dialog_filter_ids[i]) {
        if (i != j) {
          std::swap(dialog_filters[i], dialog_filters[j]);
        }
        break;
      }
    }
    CHECK(dialog_filters[i]->dialog_filter_id == dialog_filter_ids[i]);
  }
  return true;
}

void MessagesManager::reset_all_notification_settings() {
  CHECK(!td_->auth_manager_->is_bot());
  DialogNotificationSettings new_dialog_settings;
  ScopeNotificationSettings new_scope_settings;
  new_dialog_settings.is_synchronized = true;
  new_scope_settings.is_synchronized = true;

  update_scope_notification_settings(NotificationSettingsScope::Private, &users_notification_settings_,
                                     new_scope_settings);
  update_scope_notification_settings(NotificationSettingsScope::Group, &chats_notification_settings_,
                                     new_scope_settings);
  update_scope_notification_settings(NotificationSettingsScope::Channel, &channels_notification_settings_,
                                     new_scope_settings);

  for (auto &dialog : dialogs_) {
    Dialog *d = dialog.second.get();
    update_dialog_notification_settings(d->dialog_id, &d->notification_settings, new_dialog_settings);
  }
  reset_all_notification_settings_on_server(0);
}

bool MessagesManager::need_delete_message_files(DialogId dialog_id, const Message *m) const {
  if (m == nullptr) {
    return false;
  }

  auto dialog_type = dialog_id.get_type();
  if (!m->message_id.is_scheduled() && !m->message_id.is_server() && dialog_type != DialogType::SecretChat) {
    return false;
  }
  if (being_readded_message_id_ == FullMessageId{dialog_id, m->message_id}) {
    return false;
  }

  if (m->forward_info != nullptr && m->forward_info->from_dialog_id.is_valid() &&
      m->forward_info->from_message_id.is_valid()) {
    // this function must not try to load the message, because it can be called from
    // do_delete_message or add_scheduled_message_to_dialog
    const Message *old_m = get_message({m->forward_info->from_dialog_id, m->forward_info->from_message_id});
    if (old_m != nullptr && get_message_file_ids(old_m) == get_message_file_ids(m)) {
      return false;
    }
  }

  return true;
}

void MessagesManager::set_dialog_last_pinned_message_id(Dialog *d, MessageId pinned_message_id) {
  CHECK(d != nullptr);
  Message *m = get_message_force(d, pinned_message_id, "set_dialog_last_pinned_message_id");
  if (m != nullptr && update_message_is_pinned(d, m, true, "set_dialog_last_pinned_message_id")) {
    on_message_changed(d, m, true, "set_dialog_last_pinned_message_id");
  }

  if (d->is_last_pinned_message_id_inited && d->last_pinned_message_id == pinned_message_id) {
    return;
  }
  d->last_pinned_message_id = pinned_message_id;
  d->is_last_pinned_message_id_inited = true;
  on_dialog_updated(d->dialog_id, "set_dialog_last_pinned_message_id");

  LOG(INFO) << "Set " << d->dialog_id << " pinned message to " << pinned_message_id;
}

void MessagesManager::get_dialogs_from_list(DialogListId dialog_list_id, int32 limit,
                                            Promise<td_api::object_ptr<td_api::chats>> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());

  if (get_dialog_list(dialog_list_id) == nullptr) {
    return promise.set_error(Status::Error(400, "Chat list not found"));
  }

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }

  auto task_id = ++current_get_dialogs_task_id_;
  auto &task = get_dialogs_tasks_[task_id];
  task.dialog_list_id = dialog_list_id;
  task.retry_count = 5;
  task.limit = limit;
  task.promise = std::move(promise);
  get_dialogs_from_list_impl(task_id);
}

void MessagesManager::load_notification_settings() {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (!users_notification_settings_.is_synchronized) {
    send_get_scope_notification_settings_query(NotificationSettingsScope::Private, Promise<>());
  }
  if (!chats_notification_settings_.is_synchronized) {
    send_get_scope_notification_settings_query(NotificationSettingsScope::Group, Promise<>());
  }
  if (!channels_notification_settings_.is_synchronized) {
    send_get_scope_notification_settings_query(NotificationSettingsScope::Channel, Promise<>());
  }
}

void MessagesManager::set_dialog_theme_name(Dialog *d, string theme_name) {
  CHECK(d != nullptr);
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  bool is_changed = d->theme_name != theme_name;
  if (!is_changed && d->is_theme_name_inited) {
    return;
  }
  d->theme_name = std::move(theme_name);
  d->is_theme_name_inited = true;

  if (is_changed) {
    LOG(INFO) << "Set " << d->dialog_id << " theme to \"" << d->theme_name << '"';
    send_update_chat_theme(d);
  } else {
    on_dialog_updated(d->dialog_id, "set_dialog_theme_name");
  }
}

void MessagesManager::on_dialog_has_protected_content_updated(DialogId dialog_id) {
  auto d = get_dialog(dialog_id);  // called from update_chat, must not create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    send_closure(G()->td(), &Td::send_update,
                 td_api::make_object<td_api::updateChatHasProtectedContent>(
                     dialog_id.get(), get_dialog_has_protected_content(dialog_id)));
  }
}

void MessagesManager::send_update_chat_video_chat(const Dialog *d) {
  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_video_chat";
  on_dialog_updated(d->dialog_id, "send_update_chat_video_chat");
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateChatVideoChat>(d->dialog_id.get(), get_video_chat_object(d)));
}

void MessagesManager::unload_message(Dialog *d, MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(message_id.is_valid());
  bool need_update_dialog_pos = false;
  auto m = do_delete_message(d, message_id, false, true, &need_update_dialog_pos, "unload_message");
  CHECK(!need_update_dialog_pos);
}

bool MessagesManager::set_dialog_is_pinned(DialogId dialog_id, bool is_pinned) {
  if (td_->auth_manager_->is_bot()) {
    return false;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  return set_dialog_is_pinned(DialogListId(d->folder_id), d, is_pinned);
}

MessagesManager::Dialog::~Dialog() {
  if (!G()->close_flag()) {
    LOG(ERROR) << "Destroy " << dialog_id;
  }
}

bool MessagesManager::is_dialog_muted(const Dialog *d) const {
  return get_dialog_mute_until(d) != 0;
}
}  // namespace td
