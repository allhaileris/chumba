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

#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"
#include "td/telegram/SecretChatsManager.h"
#include "td/telegram/SequenceDispatcher.h"

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






#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

Result<MessagesManager::ForwardedMessages> MessagesManager::get_forwarded_messages(
    DialogId to_dialog_id, DialogId from_dialog_id, const vector<MessageId> &message_ids,
    tl_object_ptr<td_api::messageSendOptions> &&options, bool in_game_share,
    vector<MessageCopyOptions> &&copy_options) {
  CHECK(copy_options.size() == message_ids.size());
  if (message_ids.size() > 100) {  // TODO replace with const from config or implement mass-forward
    return Status::Error(400, "Too much messages to forward");
  }
  if (message_ids.empty()) {
    return Status::Error(400, "There are no messages to forward");
  }

  Dialog *from_dialog = get_dialog_force(from_dialog_id, "forward_messages from");
  if (from_dialog == nullptr) {
    return Status::Error(400, "Chat to forward messages from not found");
  }
  if (!have_input_peer(from_dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat to forward messages from");
  }
  if (from_dialog_id.get_type() == DialogType::SecretChat) {
    return Status::Error(400, "Can't forward messages from secret chats");
  }
  if (get_dialog_has_protected_content(from_dialog_id)) {
    for (const auto &copy_option : copy_options) {
      if (!copy_option.send_copy || !td_->auth_manager_->is_bot()) {
        return Status::Error(400, "Administrators of the chat restricted message forwarding");
      }
    }
  }

  Dialog *to_dialog = get_dialog_force(to_dialog_id, "forward_messages to");
  if (to_dialog == nullptr) {
    return Status::Error(400, "Chat to forward messages to not found");
  }

  TRY_STATUS(can_send_message(to_dialog_id));
  TRY_RESULT(message_send_options, process_message_send_options(to_dialog_id, std::move(options)));

  {
    MessageId last_message_id;
    for (auto message_id : message_ids) {
      if (message_id.is_valid_scheduled()) {
        return Status::Error(400, "Can't forward scheduled messages");
      }
      if (message_id.is_scheduled() || !message_id.is_valid()) {
        return Status::Error(400, "Invalid message identifier");
      }

      if (message_id <= last_message_id) {
        return Status::Error(400, "Message identifiers must be in a strictly increasing order");
      }
      last_message_id = message_id;
    }
  }

  bool to_secret = to_dialog_id.get_type() == DialogType::SecretChat;

  ForwardedMessages result;
  result.to_dialog = to_dialog;
  result.from_dialog = from_dialog;
  result.message_send_options = message_send_options;
  auto &copied_messages = result.copied_messages;
  auto &forwarded_message_contents = result.forwarded_message_contents;

  std::unordered_map<int64, std::pair<int64, int32>> new_copied_media_album_ids;
  std::unordered_map<int64, std::pair<int64, int32>> new_forwarded_media_album_ids;

  for (size_t i = 0; i < message_ids.size(); i++) {
    MessageId message_id = get_persistent_message_id(from_dialog, message_ids[i]);

    const Message *forwarded_message = get_message_force(from_dialog, message_id, "get_forwarded_messages");
    if (forwarded_message == nullptr) {
      LOG(INFO) << "Can't find " << message_id << " to forward";
      continue;
    }
    CHECK(message_id.is_valid());
    CHECK(message_id == forwarded_message->message_id);

    if (!can_forward_message(from_dialog_id, forwarded_message)) {
      LOG(INFO) << "Can't forward " << message_id;
      continue;
    }

    bool need_copy = !message_id.is_server() || to_secret || copy_options[i].send_copy;
    if (!(need_copy && td_->auth_manager_->is_bot()) && !can_save_message(from_dialog_id, forwarded_message)) {
      LOG(INFO) << "Forward of " << message_id << " is restricted";
      continue;
    }

    auto type = need_copy ? MessageContentDupType::Copy : MessageContentDupType::Forward;
    auto top_thread_message_id = copy_options[i].top_thread_message_id;
    auto reply_to_message_id = copy_options[i].reply_to_message_id;
    auto reply_markup = std::move(copy_options[i].reply_markup);
    unique_ptr<MessageContent> content =
        dup_message_content(td_, to_dialog_id, forwarded_message->content.get(), type, std::move(copy_options[i]));
    if (content == nullptr) {
      LOG(INFO) << "Can't forward " << message_id;
      continue;
    }

    reply_to_message_id = get_reply_to_message_id(to_dialog, top_thread_message_id, reply_to_message_id, false);

    auto can_send_status = can_send_message_content(to_dialog_id, content.get(), !need_copy, td_);
    if (can_send_status.is_error()) {
      LOG(INFO) << "Can't forward " << message_id << ": " << can_send_status.message();
      continue;
    }

    auto can_use_options_status = can_use_message_send_options(message_send_options, content, 0);
    if (can_use_options_status.is_error()) {
      LOG(INFO) << "Can't forward " << message_id << ": " << can_send_status.message();
      continue;
    }

    if (can_use_top_thread_message_id(to_dialog, top_thread_message_id, reply_to_message_id).is_error()) {
      LOG(INFO) << "Ignore invalid message thread ID " << top_thread_message_id;
      top_thread_message_id = MessageId();
    }

    if (forwarded_message->media_album_id != 0) {
      auto &new_media_album_id = need_copy ? new_copied_media_album_ids[forwarded_message->media_album_id]
                                           : new_forwarded_media_album_ids[forwarded_message->media_album_id];
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

    if (need_copy) {
      copied_messages.push_back({std::move(content), top_thread_message_id, reply_to_message_id,
                                 std::move(reply_markup), forwarded_message->media_album_id,
                                 get_message_disable_web_page_preview(forwarded_message), i});
    } else {
      forwarded_message_contents.push_back({std::move(content), forwarded_message->media_album_id, i});
    }
  }

  if (2 <= forwarded_message_contents.size() && forwarded_message_contents.size() <= MAX_GROUPED_MESSAGES) {
    std::unordered_set<MessageContentType, MessageContentTypeHash> message_content_types;
    std::unordered_set<DialogId, DialogIdHash> sender_dialog_ids;
    for (auto &message_content : forwarded_message_contents) {
      message_content_types.insert(message_content.content->get_type());

      MessageId message_id = get_persistent_message_id(from_dialog, message_ids[message_content.index]);
      sender_dialog_ids.insert(get_message_original_sender(get_message(from_dialog, message_id)));
    }
    if (message_content_types.size() == 1 && is_homogenous_media_group_content(*message_content_types.begin()) &&
        sender_dialog_ids.size() == 1 && *sender_dialog_ids.begin() != DialogId()) {
      new_forwarded_media_album_ids[0].first = generate_new_media_album_id();
      for (auto &message : forwarded_message_contents) {
        message.media_album_id = 0;
      }
    }
  }
  for (auto &message : forwarded_message_contents) {
    message.media_album_id = new_forwarded_media_album_ids[message.media_album_id].first;
  }

  if (2 <= copied_messages.size() && copied_messages.size() <= MAX_GROUPED_MESSAGES) {
    std::unordered_set<MessageContentType, MessageContentTypeHash> message_content_types;
    for (auto &copied_message : copied_messages) {
      message_content_types.insert(copied_message.content->get_type());
    }
    if (message_content_types.size() == 1 && is_homogenous_media_group_content(*message_content_types.begin())) {
      new_copied_media_album_ids[0].first = generate_new_media_album_id();
      for (auto &message : copied_messages) {
        message.media_album_id = 0;
      }
    }
  }
  for (auto &message : copied_messages) {
    message.media_album_id = new_copied_media_album_ids[message.media_album_id].first;
  }
  return std::move(result);
}

class SetHistoryTtlQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit SetHistoryTtlQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int32 period) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);

    send_query(G()->net_query_creator().create(telegram_api::messages_setHistoryTTL(std::move(input_peer), period)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_setHistoryTTL>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SetHistoryTtlQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SetHistoryTtlQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::set_dialog_message_ttl(DialogId dialog_id, int32 ttl, Promise<Unit> &&promise) {
  if (ttl < 0) {
    return promise.set_error(Status::Error(400, "Message auto-delete time can't be negative"));
  }

  Dialog *d = get_dialog_force(dialog_id, "set_dialog_message_ttl");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Write)) {
    return promise.set_error(Status::Error(400, "Have no write access to the chat"));
  }

  LOG(INFO) << "Begin to set message TTL in " << dialog_id << " to " << ttl;

  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (dialog_id == get_my_dialog_id() ||
          dialog_id == DialogId(ContactsManager::get_service_notifications_user_id())) {
        return promise.set_error(Status::Error(400, "Message auto-delete time in the chat can't be changed"));
      }
      break;
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto status = td_->contacts_manager_->get_chat_permissions(chat_id);
      if (!status.can_delete_messages()) {
        return promise.set_error(
            Status::Error(400, "Not enough rights to change message auto-delete time in the chat"));
      }
      break;
    }
    case DialogType::Channel: {
      auto status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      if (!status.can_change_info_and_settings()) {
        return promise.set_error(
            Status::Error(400, "Not enough rights to change message auto-delete time in the chat"));
      }
      break;
    }
    case DialogType::SecretChat:
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  if (dialog_id.get_type() != DialogType::SecretChat) {
    // TODO invoke after
    td_->create_handler<SetHistoryTtlQuery>(std::move(promise))->send(dialog_id, ttl);
  } else {
    bool need_update_dialog_pos = false;
    Message *m = get_message_to_send(d, MessageId(), MessageId(), MessageSendOptions(),
                                     create_chat_set_ttl_message_content(ttl), &need_update_dialog_pos);

    send_update_new_message(d, m);
    if (need_update_dialog_pos) {
      send_update_chat_last_message(d, "set_dialog_message_ttl");
    }

    int64 random_id = begin_send_message(dialog_id, m);

    send_closure(td_->secret_chats_manager_, &SecretChatsManager::send_set_ttl_message, dialog_id.get_secret_chat_id(),
                 ttl, random_id, std::move(promise));
  }
}

class GetMessagesViewsQuery final : public Td::ResultHandler {
  DialogId dialog_id_;
  vector<MessageId> message_ids_;

 public:
  void send(DialogId dialog_id, vector<MessageId> &&message_ids, bool increment_view_counter) {
    dialog_id_ = dialog_id;
    message_ids_ = std::move(message_ids);

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_getMessagesViews(
        std::move(input_peer), MessagesManager::get_server_message_ids(message_ids_), increment_view_counter)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getMessagesViews>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    auto interaction_infos = std::move(result->views_);
    if (message_ids_.size() != interaction_infos.size()) {
      return on_error(Status::Error(500, "Wrong number of message views returned"));
    }
    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetMessagesViewsQuery");
    td_->contacts_manager_->on_get_chats(std::move(result->chats_), "GetMessagesViewsQuery");
    for (size_t i = 0; i < message_ids_.size(); i++) {
      FullMessageId full_message_id{dialog_id_, message_ids_[i]};

      auto *info = interaction_infos[i].get();
      auto flags = info->flags_;
      auto view_count = (flags & telegram_api::messageViews::VIEWS_MASK) != 0 ? info->views_ : 0;
      auto forward_count = (flags & telegram_api::messageViews::FORWARDS_MASK) != 0 ? info->forwards_ : 0;
      td_->messages_manager_->on_update_message_interaction_info(full_message_id, view_count, forward_count, true,
                                                                 std::move(info->replies_));
    }
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetMessagesViewsQuery")) {
      LOG(ERROR) << "Receive error for GetMessagesViewsQuery: " << status;
    }
  }
};

void MessagesManager::on_pending_message_views_timeout(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  const size_t MAX_MESSAGE_VIEWS = 100;  // server side limit
  vector<MessageId> message_ids;
  message_ids.reserve(min(d->pending_viewed_message_ids.size(), MAX_MESSAGE_VIEWS));
  for (auto message_id : d->pending_viewed_message_ids) {
    message_ids.push_back(message_id);
    if (message_ids.size() >= MAX_MESSAGE_VIEWS) {
      td_->create_handler<GetMessagesViewsQuery>()->send(dialog_id, std::move(message_ids), d->increment_view_counter);
      message_ids.clear();
    }
  }
  if (!message_ids.empty()) {
    td_->create_handler<GetMessagesViewsQuery>()->send(dialog_id, std::move(message_ids), d->increment_view_counter);
  }
  d->pending_viewed_message_ids.clear();
  d->increment_view_counter = false;
}

void MessagesManager::block_message_sender_from_replies(MessageId message_id, bool need_delete_message,
                                                        bool need_delete_all_messages, bool report_spam,
                                                        Promise<Unit> &&promise) {
  auto dialog_id = DialogId(ContactsManager::get_replies_bot_user_id());
  Dialog *d = get_dialog_force(dialog_id, "block_message_sender_from_replies");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Not enough rights"));
  }

  auto *m = get_message_force(d, message_id, "block_message_sender_from_replies");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (m->is_outgoing || m->message_id.is_scheduled() || !m->message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Wrong message specified"));
  }

  UserId sender_user_id;
  if (m->forward_info != nullptr) {
    sender_user_id = m->forward_info->sender_user_id;
  }
  bool need_update_dialog_pos = false;
  vector<int64> deleted_message_ids;
  if (need_delete_message) {
    auto p = delete_message(d, message_id, true, &need_update_dialog_pos, "block_message_sender_from_replies");
    CHECK(p.get() == m);
    deleted_message_ids.push_back(p->message_id.get());
  }
  if (need_delete_all_messages && sender_user_id.is_valid()) {
    vector<MessageId> message_ids;
    find_messages(d->messages.get(), message_ids, [sender_user_id](const Message *m) {
      return !m->is_outgoing && m->forward_info != nullptr && m->forward_info->sender_user_id == sender_user_id;
    });

    for (auto user_message_id : message_ids) {
      auto p = delete_message(d, user_message_id, true, &need_update_dialog_pos, "block_message_sender_from_replies 2");
      deleted_message_ids.push_back(p->message_id.get());
    }
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "block_message_sender_from_replies");
  }

  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);

  block_message_sender_from_replies_on_server(message_id, need_delete_message, need_delete_all_messages, report_spam, 0,
                                              std::move(promise));
}

MessagesManager::MessagesInfo MessagesManager::get_messages_info(
    tl_object_ptr<telegram_api::messages_Messages> &&messages_ptr, const char *source) {
  CHECK(messages_ptr != nullptr);
  LOG(DEBUG) << "Receive result for " << source << ": " << to_string(messages_ptr);

  vector<tl_object_ptr<telegram_api::User>> users;
  vector<tl_object_ptr<telegram_api::Chat>> chats;
  MessagesInfo result;
  switch (messages_ptr->get_id()) {
    case telegram_api::messages_messages::ID: {
      auto messages = move_tl_object_as<telegram_api::messages_messages>(messages_ptr);

      users = std::move(messages->users_);
      chats = std::move(messages->chats_);
      result.total_count = narrow_cast<int32>(messages->messages_.size());
      result.messages = std::move(messages->messages_);
      break;
    }
    case telegram_api::messages_messagesSlice::ID: {
      auto messages = move_tl_object_as<telegram_api::messages_messagesSlice>(messages_ptr);

      users = std::move(messages->users_);
      chats = std::move(messages->chats_);
      result.total_count = messages->count_;
      result.messages = std::move(messages->messages_);
      break;
    }
    case telegram_api::messages_channelMessages::ID: {
      auto messages = move_tl_object_as<telegram_api::messages_channelMessages>(messages_ptr);

      users = std::move(messages->users_);
      chats = std::move(messages->chats_);
      result.total_count = messages->count_;
      result.messages = std::move(messages->messages_);
      result.is_channel_messages = true;
      break;
    }
    case telegram_api::messages_messagesNotModified::ID:
      LOG(ERROR) << "Server returned messagesNotModified in response to " << source;
      break;
    default:
      UNREACHABLE();
      break;
  }

  td_->contacts_manager_->on_get_users(std::move(users), source);
  td_->contacts_manager_->on_get_chats(std::move(chats), source);

  return result;
}

void MessagesManager::finish_add_secret_message(unique_ptr<PendingSecretMessage> pending_secret_message) {
  if (G()->close_flag()) {
    return;
  }

  if (pending_secret_message->type == PendingSecretMessage::Type::DeleteMessages) {
    return finish_delete_secret_messages(pending_secret_message->dialog_id,
                                         std::move(pending_secret_message->random_ids),
                                         std::move(pending_secret_message->success_promise));
  }
  if (pending_secret_message->type == PendingSecretMessage::Type::DeleteHistory) {
    return finish_delete_secret_chat_history(
        pending_secret_message->dialog_id, pending_secret_message->remove_from_dialog_list,
        pending_secret_message->last_message_id, std::move(pending_secret_message->success_promise));
  }

  auto d = get_dialog(pending_secret_message->message_info.dialog_id);
  CHECK(d != nullptr);
  auto random_id = pending_secret_message->message_info.random_id;
  auto message_id = get_message_id_by_random_id(d, random_id, "finish_add_secret_message");
  if (message_id.is_valid()) {
    if (message_id != pending_secret_message->message_info.message_id) {
      LOG(WARNING) << "Ignore duplicate " << pending_secret_message->message_info.message_id
                   << " received earlier with " << message_id << " and random_id " << random_id;
    }
  } else {
    on_get_message(std::move(pending_secret_message->message_info), true, false, true, true,
                   "finish add secret message");
  }
  pending_secret_message->success_promise.set_value(Unit());  // TODO: set after message is saved
}

void MessagesManager::on_load_folder_dialog_list(FolderId folder_id, Result<Unit> &&result) {
  if (G()->close_flag()) {
    return;
  }
  CHECK(!td_->auth_manager_->is_bot());

  const auto &folder = *get_dialog_folder(folder_id);
  if (result.is_ok()) {
    LOG(INFO) << "Successfully loaded chats in " << folder_id;
    if (folder.last_server_dialog_date_ == MAX_DIALOG_DATE) {
      return;
    }

    bool need_new_get_dialog_list = false;
    for (const auto &list_it : dialog_lists_) {
      auto &list = list_it.second;
      if (!list.load_list_queries_.empty() && has_dialogs_from_folder(list, folder)) {
        LOG(INFO) << "Need to load more chats in " << folder_id << " for " << list_it.first;
        need_new_get_dialog_list = true;
      }
    }
    if (need_new_get_dialog_list) {
      load_folder_dialog_list(folder_id, int32{MAX_GET_DIALOGS}, false);
    }
    return;
  }

  LOG(WARNING) << "Failed to load chats in " << folder_id << ": " << result.error();
  vector<Promise<Unit>> promises;
  for (auto &list_it : dialog_lists_) {
    auto &list = list_it.second;
    if (!list.load_list_queries_.empty() && has_dialogs_from_folder(list, folder)) {
      append(promises, std::move(list.load_list_queries_));
      list.load_list_queries_.clear();
    }
  }

  for (auto &promise : promises) {
    promise.set_error(result.error().clone());
  }
}

Result<MessagesManager::MessageSendOptions> MessagesManager::process_message_send_options(
    DialogId dialog_id, tl_object_ptr<td_api::messageSendOptions> &&options) const {
  MessageSendOptions result;
  if (options != nullptr) {
    result.disable_notification = options->disable_notification_;
    result.from_background = options->from_background_;
    TRY_RESULT_ASSIGN(result.schedule_date, get_message_schedule_date(std::move(options->scheduling_state_)));
  }

  auto dialog_type = dialog_id.get_type();
  if (result.schedule_date != 0) {
    if (dialog_type == DialogType::SecretChat) {
      return Status::Error(400, "Can't schedule messages in secret chats");
    }
    if (td_->auth_manager_->is_bot()) {
      return Status::Error(400, "Bots can't send scheduled messages");
    }
  }
  if (result.schedule_date == SCHEDULE_WHEN_ONLINE_DATE) {
    if (dialog_type != DialogType::User) {
      return Status::Error(400, "Messages can be scheduled till online only in private chats");
    }
    if (dialog_id == get_my_dialog_id()) {
      return Status::Error(400, "Can't scheduled till online messages in chat with self");
    }
  }

  return result;
}

void MessagesManager::update_dialog_pinned_messages_from_updates(DialogId dialog_id,
                                                                 const vector<MessageId> &message_ids, bool is_pin) {
  Dialog *d = get_dialog_force(dialog_id, "update_dialog_pinned_messages_from_updates");
  if (d == nullptr) {
    LOG(INFO) << "Ignore updatePinnedMessages for unknown " << dialog_id;
    return;
  }

  for (auto message_id : message_ids) {
    if (!message_id.is_valid() || (!message_id.is_server() && dialog_id.get_type() != DialogType::SecretChat)) {
      LOG(ERROR) << "Incoming update tries to pin/unpin " << message_id << " in " << dialog_id;
      continue;
    }

    Message *m = get_message_force(d, message_id, "update_dialog_pinned_messages_from_updates");
    if (m != nullptr && update_message_is_pinned(d, m, is_pin, "update_dialog_pinned_messages_from_updates")) {
      on_message_changed(d, m, true, "update_dialog_pinned_messages_from_updates");
    }
  }
}

void MessagesManager::on_update_dialog_draft_message(DialogId dialog_id,
                                                     tl_object_ptr<telegram_api::DraftMessage> &&draft_message) {
  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive update chat draft in invalid " << dialog_id;
    return;
  }
  auto d = get_dialog_force(dialog_id, "on_update_dialog_draft_message");
  if (d == nullptr) {
    LOG(INFO) << "Ignore update chat draft in unknown " << dialog_id;
    if (!have_input_peer(dialog_id, AccessRights::Read)) {
      LOG(ERROR) << "Have no read access to " << dialog_id << " to repair chat draft message";
    } else {
      send_get_dialog_query(dialog_id, Auto(), 0, "on_update_dialog_draft_message");
    }
    return;
  }
  update_dialog_draft_message(d, get_draft_message(td_->contacts_manager_.get(), std::move(draft_message)), true, true);
}

void MessagesManager::update_forward_count(DialogId dialog_id, MessageId message_id, int32 update_date) {
  CHECK(!td_->auth_manager_->is_bot());
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  Message *m = get_message_force(d, message_id, "update_forward_count");
  if (m != nullptr && !m->message_id.is_scheduled() && m->message_id.is_server() && m->view_count > 0 &&
      m->interaction_info_update_date < update_date) {
    if (m->forward_count == 0) {
      m->forward_count++;
      send_update_message_interaction_info(dialog_id, m);
      on_message_changed(d, m, true, "update_forward_count");
    }

    if (d->pending_viewed_message_ids.insert(m->message_id).second) {
      pending_message_views_timeout_.add_timeout_in(dialog_id.get(), 0.0);
    }
  }
}

void MessagesManager::set_dialog_last_database_message_id(Dialog *d, MessageId last_database_message_id,
                                                          const char *source, bool is_loaded_from_database) {
  CHECK(!last_database_message_id.is_scheduled());
  if (last_database_message_id == d->last_database_message_id) {
    return;
  }

  LOG(INFO) << "Set " << d->dialog_id << " last database message to " << last_database_message_id << " from " << source;
  d->debug_set_dialog_last_database_message_id = source;
  d->last_database_message_id = last_database_message_id;
  if (!is_loaded_from_database) {
    on_dialog_updated(d->dialog_id, "set_dialog_last_database_message_id");
  }
}

void MessagesManager::on_resolved_username(const string &username, DialogId dialog_id) {
  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Resolve username \"" << username << "\" to invalid " << dialog_id;
    return;
  }

  auto it = resolved_usernames_.find(clean_username(username));
  if (it != resolved_usernames_.end()) {
    LOG_IF(ERROR, it->second.dialog_id != dialog_id)
        << "Resolve username \"" << username << "\" to " << dialog_id << ", but have it in " << it->second.dialog_id;
    return;
  }

  inaccessible_resolved_usernames_[clean_username(username)] = dialog_id;
}

DialogId MessagesManager::get_message_original_sender(const Message *m) {
  CHECK(m != nullptr);
  if (m->forward_info != nullptr) {
    auto forward_info = m->forward_info.get();
    if (forward_info->is_imported || is_forward_info_sender_hidden(forward_info)) {
      return DialogId();
    }
    if (forward_info->message_id.is_valid() || forward_info->sender_dialog_id.is_valid()) {
      return forward_info->sender_dialog_id;
    }
    return DialogId(forward_info->sender_user_id);
  }
  return get_message_sender(m);
}

bool MessagesManager::can_get_message_statistics(DialogId dialog_id, const Message *m) const {
  if (td_->auth_manager_->is_bot()) {
    return false;
  }
  if (m == nullptr || m->message_id.is_scheduled() || !m->message_id.is_server() || m->view_count == 0 ||
      m->had_forward_info || (m->forward_info != nullptr && m->forward_info->message_id.is_valid())) {
    return false;
  }
  return td_->contacts_manager_->can_get_channel_message_statistics(dialog_id);
}

MessagesManager::Dialog *MessagesManager::get_service_notifications_dialog() {
  UserId service_notifications_user_id = td_->contacts_manager_->add_service_notifications_user();
  DialogId service_notifications_dialog_id(service_notifications_user_id);
  force_create_dialog(service_notifications_dialog_id, "get_service_notifications_dialog");
  return get_dialog(service_notifications_dialog_id);
}

void MessagesManager::on_authorization_success() {
  CHECK(td_->auth_manager_->is_authorized());
  authorization_date_ = G()->shared_config().get_option_integer("authorization_date");

  if (td_->auth_manager_->is_bot()) {
    disable_get_dialog_filter_ = true;
    return;
  }

  create_folders();

  reload_dialog_filters();
}

int64 MessagesManager::generate_new_media_album_id() {
  int64 media_album_id = 0;
  do {
    media_album_id = Random::secure_int64();
  } while (media_album_id >= 0 || pending_message_group_sends_.count(media_album_id) != 0);
  return media_album_id;
}

void MessagesManager::reload_voice_chat_on_search(const string &username) {
  reload_voice_chat_on_search_usernames_.insert(clean_username(username));
}
}  // namespace td
