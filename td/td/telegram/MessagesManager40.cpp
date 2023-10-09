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

MessagesManager::Message *MessagesManager::add_scheduled_message_to_dialog(Dialog *d, unique_ptr<Message> message,
                                                                           bool from_update, bool *need_update,
                                                                           const char *source) {
  CHECK(message != nullptr);
  CHECK(d != nullptr);
  CHECK(need_update != nullptr);
  CHECK(source != nullptr);

  DialogId dialog_id = d->dialog_id;
  MessageId message_id = message->message_id;
  CHECK(message_id.is_valid_scheduled());
  CHECK(!message->notification_id.is_valid());
  CHECK(!message->removed_notification_id.is_valid());

  message->top_thread_message_id = MessageId();

  if (d->deleted_message_ids.count(message_id)) {
    LOG(INFO) << "Skip adding deleted " << message_id << " to " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "adding deleted scheduled message";
    return nullptr;
  }

  if (message_id.is_scheduled_server() &&
      d->deleted_scheduled_server_message_ids.count(message_id.get_scheduled_server_message_id())) {
    LOG(INFO) << "Skip adding deleted " << message_id << " to " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "adding deleted scheduled server message";
    return nullptr;
  }

  if (dialog_id.get_type() == DialogType::SecretChat) {
    LOG(ERROR) << "Tried to add " << message_id << " to " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "skip adding scheduled message to secret chat";
    return nullptr;
  }
  if (message->ttl != 0 || message->ttl_expires_at != 0) {
    LOG(ERROR) << "Tried to add " << message_id << " with TTL " << message->ttl << "/" << message->ttl_expires_at
               << " to " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "skip adding secret scheduled message";
    return nullptr;
  }
  if (message->ttl_period != 0) {
    LOG(ERROR) << "Tried to add " << message_id << " with TTL period " << message->ttl_period << " to " << dialog_id
               << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "skip adding auto-deleting scheduled message";
    return nullptr;
  }
  if (td_->auth_manager_->is_bot()) {
    LOG(ERROR) << "Bot tried to add " << message_id << " to " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "skip adding scheduled message by bot";
    return nullptr;
  }

  auto message_content_type = message->content->get_type();
  if (is_service_message_content(message_content_type) || message_content_type == MessageContentType::LiveLocation ||
      message_content_type == MessageContentType::ExpiredPhoto ||
      message_content_type == MessageContentType::ExpiredVideo) {
    LOG(ERROR) << "Tried to add " << message_id << " of type " << message_content_type << " to " << dialog_id
               << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "skip adding message of unexpected type";
    return nullptr;
  }

  {
    Message *m = message->from_database ? get_message(d, message_id)
                                        : get_message_force(d, message_id, "add_scheduled_message_to_dialog");
    if (m != nullptr) {
      auto old_message_id = m->message_id;
      LOG(INFO) << "Adding already existing " << old_message_id << " in " << dialog_id << " from " << source;
      set_message_id(message, old_message_id);
      if (!message->from_database) {
        auto old_file_ids = get_message_content_file_ids(m->content.get(), td_);
        bool need_update_dialog_pos = false;
        update_message(d, m, std::move(message), &need_update_dialog_pos, true);
        CHECK(need_update_dialog_pos == false);
        change_message_files(dialog_id, m, old_file_ids);
      }
      if (old_message_id != message_id) {
        being_readded_message_id_ = {dialog_id, old_message_id};
        message = do_delete_scheduled_message(d, old_message_id, false, "add_scheduled_message_to_dialog");
        CHECK(message != nullptr);
        send_update_delete_messages(dialog_id, {message->message_id.get()}, false, false);
        set_message_id(message, message_id);
        message->from_database = false;
      } else {
        *need_update = false;
        return m;
      }
    }
  }

  LOG(INFO) << "Adding not found " << message_id << " to " << dialog_id << " from " << source;

  const Message *m = message.get();
  if (m->message_id.is_yet_unsent() && m->reply_to_message_id.is_valid() && !m->reply_to_message_id.is_yet_unsent()) {
    replied_by_yet_unsent_messages_[FullMessageId{dialog_id, m->reply_to_message_id}]++;
  }

  if (!m->from_database && !m->message_id.is_yet_unsent()) {
    add_message_to_database(d, m, "add_scheduled_message_to_dialog");
  }

  reget_message_from_server_if_needed(dialog_id, m);

  add_message_file_sources(dialog_id, m);

  register_message_content(td_, m->content.get(), {dialog_id, m->message_id}, "add_scheduled_message_to_dialog");

  // must be called after register_message_content, which loads web page
  update_message_max_reply_media_timestamp(d, message.get(), false);
  update_message_max_own_media_timestamp(d, message.get());

  register_message_reply(d, m);

  if (from_update) {
    update_sent_message_contents(dialog_id, m);
    update_used_hashtags(dialog_id, m);
    update_has_outgoing_messages(dialog_id, m);
  }

  if (m->message_id.is_scheduled_server()) {
    int32 &date = d->scheduled_message_date[m->message_id.get_scheduled_server_message_id()];
    CHECK(date == 0);
    date = m->date;
  }

  Message *result_message = treap_insert_message(&d->scheduled_messages, std::move(message));
  CHECK(result_message != nullptr);
  CHECK(d->scheduled_messages != nullptr);
  being_readded_message_id_ = FullMessageId();
  return result_message;
}

Result<vector<MessageId>> MessagesManager::resend_messages(DialogId dialog_id, vector<MessageId> message_ids) {
  if (message_ids.empty()) {
    return Status::Error(400, "There are no messages to resend");
  }

  Dialog *d = get_dialog_force(dialog_id, "resend_messages");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  TRY_STATUS(can_send_message(dialog_id));

  MessageId last_message_id;
  for (auto &message_id : message_ids) {
    message_id = get_persistent_message_id(d, message_id);
    const Message *m = get_message_force(d, message_id, "resend_messages");
    if (m == nullptr) {
      return Status::Error(400, "Message not found");
    }
    if (!m->is_failed_to_send) {
      return Status::Error(400, "Message is not failed to send");
    }
    if (!can_resend_message(m)) {
      return Status::Error(400, "Message can't be re-sent");
    }
    if (m->try_resend_at > Time::now()) {
      return Status::Error(400, "Message can't be re-sent yet");
    }
    if (last_message_id != MessageId()) {
      if (m->message_id.is_scheduled() != last_message_id.is_scheduled()) {
        return Status::Error(400, "Messages must be all scheduled or ordinary");
      }
      if (m->message_id <= last_message_id) {
        return Status::Error(400, "Message identifiers must be in a strictly increasing order");
      }
    }
    last_message_id = m->message_id;
  }

  vector<unique_ptr<MessageContent>> new_contents(message_ids.size());
  std::unordered_map<int64, std::pair<int64, int32>> new_media_album_ids;
  for (size_t i = 0; i < message_ids.size(); i++) {
    MessageId message_id = message_ids[i];
    const Message *m = get_message(d, message_id);
    CHECK(m != nullptr);

    unique_ptr<MessageContent> content =
        dup_message_content(td_, dialog_id, m->content.get(), MessageContentDupType::Send, MessageCopyOptions());
    if (content == nullptr) {
      LOG(INFO) << "Can't resend " << m->message_id;
      continue;
    }

    auto can_send_status = can_send_message_content(dialog_id, content.get(), false, td_);
    if (can_send_status.is_error()) {
      LOG(INFO) << "Can't resend " << m->message_id << ": " << can_send_status.message();
      continue;
    }

    new_contents[i] = std::move(content);

    if (m->media_album_id != 0) {
      auto &new_media_album_id = new_media_album_ids[m->media_album_id];
      new_media_album_id.second++;
      if (new_media_album_id.second == 2) {  // have at least 2 messages in the new album
        CHECK(new_media_album_id.first == 0);
        new_media_album_id.first = generate_new_media_album_id();
      }
      if (new_media_album_id.second == MAX_GROUPED_MESSAGES + 1) {
        CHECK(new_media_album_id.first != 0);
        new_media_album_id.first = 0;  // just in case
      }
    }
  }

  vector<MessageId> result(message_ids.size());
  bool need_update_dialog_pos = false;
  for (size_t i = 0; i < message_ids.size(); i++) {
    if (new_contents[i] == nullptr) {
      continue;
    }

    being_readded_message_id_ = {dialog_id, message_ids[i]};
    unique_ptr<Message> message = delete_message(d, message_ids[i], true, &need_update_dialog_pos, "resend_messages");
    CHECK(message != nullptr);
    send_update_delete_messages(dialog_id, {message->message_id.get()}, true, false);

    auto need_another_sender =
        message->send_error_code == 400 && message->send_error_message == CSlice("SEND_AS_PEER_INVALID");
    MessageSendOptions options(message->disable_notification, message->from_background,
                               get_message_schedule_date(message.get()));
    Message *m = get_message_to_send(
        d, message->top_thread_message_id,
        get_reply_to_message_id(d, message->top_thread_message_id, message->reply_to_message_id, false), options,
        std::move(new_contents[i]), &need_update_dialog_pos, false, nullptr, message->is_copy,
        need_another_sender ? DialogId() : get_message_sender(message.get()));
    m->reply_markup = std::move(message->reply_markup);
    m->via_bot_user_id = message->via_bot_user_id;
    m->disable_web_page_preview = message->disable_web_page_preview;
    m->clear_draft = false;  // never clear draft in resend
    m->ttl = message->ttl;
    m->is_content_secret = message->is_content_secret;
    m->media_album_id = new_media_album_ids[message->media_album_id].first;
    m->send_emoji = message->send_emoji;
    m->has_explicit_sender |= message->has_explicit_sender;

    save_send_message_log_event(dialog_id, m);
    do_send_message(dialog_id, m);

    send_update_new_message(d, m);

    result[i] = m->message_id;
    being_readded_message_id_ = FullMessageId();
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "resend_messages");
  }

  return result;
}

Status MessagesManager::toggle_dialog_is_pinned(DialogListId dialog_list_id, DialogId dialog_id, bool is_pinned) {
  if (td_->auth_manager_->is_bot()) {
    return Status::Error(400, "Bots can't change chat pin state");
  }

  Dialog *d = get_dialog_force(dialog_id, "toggle_dialog_is_pinned");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }
  if (d->order == DEFAULT_ORDER && is_pinned) {
    return Status::Error(400, "The chat can't be pinned");
  }

  auto *list = get_dialog_list(dialog_list_id);
  if (list == nullptr) {
    return Status::Error(400, "Chat list not found");
  }
  if (!list->are_pinned_dialogs_inited_) {
    return Status::Error(400, "Pinned chats must be loaded first");
  }

  bool was_pinned = is_dialog_pinned(dialog_list_id, dialog_id);
  if (is_pinned == was_pinned) {
    return Status::OK();
  }

  if (dialog_list_id.is_filter()) {
    CHECK(is_update_chat_filters_sent_);
    auto dialog_filter_id = dialog_list_id.get_filter_id();
    auto old_dialog_filter = get_dialog_filter(dialog_filter_id);
    CHECK(old_dialog_filter != nullptr);
    auto new_dialog_filter = make_unique<DialogFilter>(*old_dialog_filter);
    auto is_changed_dialog = [dialog_id](InputDialogId input_dialog_id) {
      return dialog_id == input_dialog_id.get_dialog_id();
    };
    if (is_pinned) {
      new_dialog_filter->pinned_dialog_ids.insert(new_dialog_filter->pinned_dialog_ids.begin(),
                                                  get_input_dialog_id(dialog_id));
      td::remove_if(new_dialog_filter->included_dialog_ids, is_changed_dialog);
      td::remove_if(new_dialog_filter->excluded_dialog_ids, is_changed_dialog);
    } else {
      bool is_removed = td::remove_if(new_dialog_filter->pinned_dialog_ids, is_changed_dialog);
      CHECK(is_removed);
      new_dialog_filter->included_dialog_ids.push_back(get_input_dialog_id(dialog_id));
    }

    TRY_STATUS(new_dialog_filter->check_limits());
    sort_dialog_filter_input_dialog_ids(new_dialog_filter.get(), "toggle_dialog_is_pinned");

    edit_dialog_filter(std::move(new_dialog_filter), "toggle_dialog_is_pinned");
    save_dialog_filters();
    send_update_chat_filters();

    if (dialog_id.get_type() != DialogType::SecretChat) {
      synchronize_dialog_filters();
    }

    return Status::OK();
  }

  CHECK(dialog_list_id.is_folder());
  auto folder_id = dialog_list_id.get_folder_id();
  if (is_pinned) {
    if (d->folder_id != folder_id) {
      return Status::Error(400, "Chat not in the list");
    }

    auto pinned_dialog_ids = get_pinned_dialog_ids(dialog_list_id);
    auto pinned_dialog_count = pinned_dialog_ids.size();
    auto secret_pinned_dialog_count =
        std::count_if(pinned_dialog_ids.begin(), pinned_dialog_ids.end(),
                      [](DialogId dialog_id) { return dialog_id.get_type() == DialogType::SecretChat; });
    size_t dialog_count = dialog_id.get_type() == DialogType::SecretChat
                              ? secret_pinned_dialog_count
                              : pinned_dialog_count - secret_pinned_dialog_count;

    if (dialog_count >= static_cast<size_t>(get_pinned_dialogs_limit(dialog_list_id))) {
      return Status::Error(400, "The maximum number of pinned chats exceeded");
    }
  }

  if (set_dialog_is_pinned(dialog_list_id, d, is_pinned)) {
    toggle_dialog_is_pinned_on_server(dialog_id, is_pinned, 0);
  }
  return Status::OK();
}

class ToggleDialogIsBlockedActor final : public NetActorOnce {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  bool is_blocked_;

 public:
  explicit ToggleDialogIsBlockedActor(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, bool is_blocked, uint64 sequence_dispatcher_id) {
    dialog_id_ = dialog_id;
    is_blocked_ = is_blocked;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Know);
    CHECK(input_peer != nullptr && input_peer->get_id() != telegram_api::inputPeerEmpty::ID);
    auto query = is_blocked ? G()->net_query_creator().create(telegram_api::contacts_block(std::move(input_peer)))
                            : G()->net_query_creator().create(telegram_api::contacts_unblock(std::move(input_peer)));
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    static_assert(
        std::is_same<telegram_api::contacts_block::ReturnType, telegram_api::contacts_unblock::ReturnType>::value, "");
    auto result_ptr = fetch_result<telegram_api::contacts_block>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG_IF(WARNING, !result) << "Block/Unblock " << dialog_id_ << " has failed";

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ToggleDialogIsBlockedActor")) {
      LOG(ERROR) << "Receive error for ToggleDialogIsBlockedActor: " << status;
    }
    if (!G()->close_flag()) {
      td_->messages_manager_->on_update_dialog_is_blocked(dialog_id_, !is_blocked_);
      td_->messages_manager_->get_dialog_info_full(dialog_id_, Auto(), "ToggleDialogIsBlockedActor");
      td_->messages_manager_->reget_dialog_action_bar(dialog_id_, "ToggleDialogIsBlockedActor");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::toggle_dialog_is_blocked_on_server(DialogId dialog_id, bool is_blocked, uint64 log_event_id) {
  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_toggle_dialog_is_blocked_on_server_log_event(dialog_id, is_blocked);
  }

  send_closure(td_->create_net_actor<ToggleDialogIsBlockedActor>(get_erase_log_event_promise(log_event_id)),
               &ToggleDialogIsBlockedActor::send, dialog_id, is_blocked,
               get_sequence_dispatcher_id(dialog_id, MessageContentType::Text));
}

void MessagesManager::on_get_scheduled_messages_from_database(DialogId dialog_id,
                                                              vector<MessagesDbDialogMessage> &&messages) {
  if (G()->close_flag()) {
    auto it = load_scheduled_messages_from_database_queries_.find(dialog_id);
    CHECK(it != load_scheduled_messages_from_database_queries_.end());
    CHECK(!it->second.empty());
    auto promises = std::move(it->second);
    load_scheduled_messages_from_database_queries_.erase(it);

    for (auto &promise : promises) {
      promise.set_error(Global::request_aborted_error());
    }
    return;
  }
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  d->has_loaded_scheduled_messages_from_database = true;

  LOG(INFO) << "Receive " << messages.size() << " scheduled messages from database in " << dialog_id;

  Dependencies dependencies;
  vector<MessageId> added_message_ids;
  for (auto &message_slice : messages) {
    auto message = parse_message(dialog_id, message_slice.message_id, message_slice.data, true);
    if (message == nullptr) {
      continue;
    }
    message->from_database = true;

    if (get_message(d, message->message_id) != nullptr) {
      continue;
    }

    bool need_update = false;
    Message *m = add_scheduled_message_to_dialog(d, std::move(message), false, &need_update,
                                                 "on_get_scheduled_messages_from_database");
    if (m != nullptr) {
      add_message_dependencies(dependencies, m);
      added_message_ids.push_back(m->message_id);
    }
  }
  resolve_dependencies_force(td_, dependencies, "on_get_scheduled_messages_from_database");

  // for (auto message_id : added_message_ids) {
  //   send_update_new_message(d, get_message(d, message_id));
  // }
  send_update_chat_has_scheduled_messages(d, false);

  auto it = load_scheduled_messages_from_database_queries_.find(dialog_id);
  CHECK(it != load_scheduled_messages_from_database_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  load_scheduled_messages_from_database_queries_.erase(it);

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

void MessagesManager::remove_dialog_newer_messages(Dialog *d, MessageId from_message_id, const char *source) {
  LOG(INFO) << "Remove messages in " << d->dialog_id << " newer than " << from_message_id << " from " << source;
  CHECK(!d->last_new_message_id.is_valid());

  delete_all_dialog_messages_from_database(d, MessageId::max(), "remove_dialog_newer_messages");
  set_dialog_first_database_message_id(d, MessageId(), "remove_dialog_newer_messages");
  set_dialog_last_database_message_id(d, MessageId(), source);
  if (d->dialog_id.get_type() != DialogType::SecretChat && !d->is_empty) {
    d->have_full_history = false;
  }
  invalidate_message_indexes(d);

  vector<MessageId> to_delete_message_ids;
  find_newer_messages(d->messages.get(), from_message_id, to_delete_message_ids);
  td::remove_if(to_delete_message_ids, [](MessageId message_id) { return message_id.is_yet_unsent(); });
  if (!to_delete_message_ids.empty()) {
    LOG(INFO) << "Delete " << format::as_array(to_delete_message_ids) << " newer than " << from_message_id << " in "
              << d->dialog_id << " from " << source;

    vector<int64> deleted_message_ids;
    bool need_update_dialog_pos = false;
    for (auto message_id : to_delete_message_ids) {
      auto message = delete_message(d, message_id, false, &need_update_dialog_pos, "remove_dialog_newer_messages");
      if (message != nullptr) {
        deleted_message_ids.push_back(message->message_id.get());
      }
    }
    if (need_update_dialog_pos) {
      send_update_chat_last_message(d, "remove_dialog_newer_messages");
    }
    send_update_delete_messages(d->dialog_id, std::move(deleted_message_ids), false, false);
  }
}

MessageId MessagesManager::get_reply_to_message_id(Dialog *d, MessageId top_thread_message_id, MessageId message_id,
                                                   bool for_draft) {
  CHECK(d != nullptr);
  if (!message_id.is_valid()) {
    if (!for_draft && message_id == MessageId() && top_thread_message_id.is_valid() &&
        top_thread_message_id.is_server() &&
        get_message_force(d, top_thread_message_id, "get_reply_to_message_id 1") != nullptr) {
      return top_thread_message_id;
    }
    return MessageId();
  }
  message_id = get_persistent_message_id(d, message_id);
  const Message *m = get_message_force(d, message_id, "get_reply_to_message_id 2");
  if (m == nullptr || m->message_id.is_yet_unsent() ||
      (m->message_id.is_local() && d->dialog_id.get_type() != DialogType::SecretChat)) {
    if (message_id.is_server() && d->dialog_id.get_type() != DialogType::SecretChat &&
        message_id > d->last_new_message_id && message_id <= d->max_notification_message_id) {
      // allow to reply yet unreceived server message
      return message_id;
    }
    if (!for_draft && top_thread_message_id.is_valid() && top_thread_message_id.is_server() &&
        get_message_force(d, top_thread_message_id, "get_reply_to_message_id 3") != nullptr) {
      return top_thread_message_id;
    }

    // TODO local replies to local messages can be allowed
    // TODO replies to yet unsent messages can be allowed with special handling of them on application restart
    return MessageId();
  }
  return m->message_id;
}

void MessagesManager::set_dialog_last_new_message_id(Dialog *d, MessageId last_new_message_id, const char *source) {
  CHECK(!last_new_message_id.is_scheduled());

  LOG_CHECK(last_new_message_id > d->last_new_message_id)
      << last_new_message_id << " " << d->last_new_message_id << " " << source;
  CHECK(d->dialog_id.get_type() == DialogType::SecretChat || last_new_message_id.is_server());
  if (!d->last_new_message_id.is_valid()) {
    remove_dialog_newer_messages(d, last_new_message_id, source);

    auto last_new_message = get_message(d, last_new_message_id);
    if (last_new_message != nullptr) {
      add_message_to_database(d, last_new_message, "set_dialog_last_new_message_id");
      set_dialog_first_database_message_id(d, last_new_message_id, "set_dialog_last_new_message_id");
      set_dialog_last_database_message_id(d, last_new_message_id, "set_dialog_last_new_message_id");
      try_restore_dialog_reply_markup(d, last_new_message);
    }
  }

  LOG(INFO) << "Set " << d->dialog_id << " last new message to " << last_new_message_id << " from " << source;
  d->last_new_message_id = last_new_message_id;
  on_dialog_updated(d->dialog_id, source);
}

bool MessagesManager::can_delete_channel_message(const DialogParticipantStatus &status, const Message *m, bool is_bot) {
  if (m == nullptr) {
    return true;
  }
  if (m->message_id.is_local() || m->message_id.is_yet_unsent()) {
    return true;
  }
  if (m->message_id.is_scheduled()) {
    if (m->is_channel_post) {
      return status.can_post_messages();
    }
    return true;
  }

  if (is_bot && G()->unix_time_cached() >= m->date + 2 * 86400) {
    // bots can't delete messages older than 2 days
    return false;
  }

  CHECK(m->message_id.is_server());
  if (m->message_id.get_server_message_id().get() == 1) {
    return false;
  }
  auto content_type = m->content->get_type();
  if (content_type == MessageContentType::ChannelMigrateFrom || content_type == MessageContentType::ChannelCreate) {
    return false;
  }

  if (status.can_delete_messages()) {
    return true;
  }

  if (!m->is_outgoing) {
    return false;
  }

  if (m->is_channel_post || is_service_message_content(content_type)) {
    return status.can_post_messages();
  }

  return true;
}

void MessagesManager::suffix_load_loop(Dialog *d) {
  if (d->suffix_load_has_query_) {
    return;
  }

  if (d->suffix_load_queries_.empty()) {
    return;
  }
  CHECK(!d->suffix_load_done_);

  auto dialog_id = d->dialog_id;
  auto from_message_id = d->suffix_load_first_message_id_;
  LOG(INFO) << "Send suffix load query in " << dialog_id << " from " << from_message_id;
  auto promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id](Result<Unit> result) {
    send_closure(actor_id, &MessagesManager::suffix_load_query_ready, dialog_id);
  });
  d->suffix_load_has_query_ = true;
  d->suffix_load_query_message_id_ = from_message_id;
  if (from_message_id.is_valid()) {
    get_history_impl(d, from_message_id, -1, 100, true, true, std::move(promise));
  } else {
    CHECK(from_message_id == MessageId());
    get_history_from_the_end_impl(d, true, true, std::move(promise));
  }
}

void MessagesManager::on_get_secret_chat_total_count(DialogListId dialog_list_id, int32 total_count) {
  if (G()->close_flag()) {
    return;
  }

  CHECK(!td_->auth_manager_->is_bot());
  auto *list = get_dialog_list(dialog_list_id);
  if (list == nullptr) {
    // just in case
    return;
  }
  CHECK(total_count >= 0);
  if (list->secret_chat_total_count_ != total_count) {
    auto old_dialog_total_count = get_dialog_total_count(*list);
    list->secret_chat_total_count_ = total_count;
    if (list->is_dialog_unread_count_inited_) {
      if (old_dialog_total_count != get_dialog_total_count(*list)) {
        send_update_unread_chat_count(*list, DialogId(), true, "on_get_secret_chat_total_count");
      } else {
        save_unread_chat_count(*list);
      }
    }
  }
}

void MessagesManager::add_secret_message(unique_ptr<PendingSecretMessage> pending_secret_message,
                                         Promise<Unit> lock_promise) {
  auto &multipromise = pending_secret_message->load_data_multipromise;
  multipromise.set_ignore_errors(true);
  int64 token = pending_secret_messages_.add(std::move(pending_secret_message));

  multipromise.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), token](Result<Unit> result) {
    if (result.is_ok()) {
      send_closure(actor_id, &MessagesManager::on_add_secret_message_ready, token);
    }
  }));

  if (!lock_promise) {
    lock_promise = multipromise.get_promise();
  }
  lock_promise.set_value(Unit());
}

void MessagesManager::on_upload_dialog_photo_error(FileId file_id, Status status) {
  if (G()->close_flag()) {
    // do not fail upload if closing
    return;
  }

  LOG(INFO) << "File " << file_id << " has upload error " << status;
  CHECK(status.is_error());

  auto it = being_uploaded_dialog_photos_.find(file_id);
  if (it == being_uploaded_dialog_photos_.end()) {
    // just in case, as in on_upload_media_error
    return;
  }

  Promise<Unit> promise = std::move(it->second.promise);

  being_uploaded_dialog_photos_.erase(it);

  promise.set_error(std::move(status));
}

bool MessagesManager::get_dialog_has_protected_content(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return false;
    case DialogType::Chat:
      return td_->contacts_manager_->get_chat_has_protected_content(dialog_id.get_chat_id());
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_has_protected_content(dialog_id.get_channel_id());
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
    default:
      UNREACHABLE();
      return true;
  }
}

int64 MessagesManager::begin_send_message(DialogId dialog_id, const Message *m) {
  LOG(INFO) << "Begin to send " << FullMessageId(dialog_id, m->message_id) << " with random_id = " << m->random_id;
  CHECK(m->random_id != 0 && being_sent_messages_.find(m->random_id) == being_sent_messages_.end());
  CHECK(m->message_id.is_yet_unsent());
  being_sent_messages_[m->random_id] = FullMessageId(dialog_id, m->message_id);
  return m->random_id;
}

void MessagesManager::on_pending_send_dialog_action_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::on_send_dialog_action_timeout,
                     DialogId(dialog_id_int));
}

void MessagesManager::on_channel_get_difference_timeout(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  CHECK(dialog_id.get_type() == DialogType::Channel);
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  get_channel_difference(dialog_id, d->pts, true, "on_channel_get_difference_timeout");
}

MessagesManager::Message *MessagesManager::get_message(FullMessageId full_message_id) {
  Dialog *d = get_dialog(full_message_id.get_dialog_id());
  if (d == nullptr) {
    return nullptr;
  }

  return get_message(d, full_message_id.get_message_id());
}

bool MessagesManager::have_message_force(Dialog *d, MessageId message_id, const char *source) {
  return get_message_force(d, message_id, source) != nullptr;
}
}  // namespace td
