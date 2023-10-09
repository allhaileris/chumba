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
#include "td/telegram/FileReferenceManager.h"
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





namespace td {
}  // namespace td
namespace td {

MessagesManager::MessageInfo MessagesManager::parse_telegram_api_message(
    tl_object_ptr<telegram_api::Message> message_ptr, bool is_scheduled, const char *source) const {
  LOG(DEBUG) << "Receive from " << source << " " << to_string(message_ptr);
  LOG_CHECK(message_ptr != nullptr) << source;

  MessageInfo message_info;
  message_info.message_id = get_message_id(message_ptr, is_scheduled);
  switch (message_ptr->get_id()) {
    case telegram_api::messageEmpty::ID:
      message_info.message_id = MessageId();
      break;
    case telegram_api::message::ID: {
      auto message = move_tl_object_as<telegram_api::message>(message_ptr);

      message_info.dialog_id = DialogId(message->peer_id_);
      if (message->from_id_ != nullptr) {
        message_info.sender_dialog_id = DialogId(message->from_id_);
      } else {
        message_info.sender_dialog_id = message_info.dialog_id;
      }
      message_info.date = message->date_;
      message_info.forward_header = std::move(message->fwd_from_);
      message_info.reply_header = std::move(message->reply_to_);
      if (message->flags_ & MESSAGE_FLAG_IS_SENT_VIA_BOT) {
        message_info.via_bot_user_id = UserId(message->via_bot_id_);
        if (!message_info.via_bot_user_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << message_info.via_bot_user_id << " from " << source;
          message_info.via_bot_user_id = UserId();
        }
      }
      if (message->flags_ & MESSAGE_FLAG_HAS_INTERACTION_INFO) {
        message_info.view_count = message->views_;
        message_info.forward_count = message->forwards_;
      }
      if (message->flags_ & MESSAGE_FLAG_HAS_REPLY_INFO) {
        message_info.reply_info = std::move(message->replies_);
      }
      if (message->flags_ & MESSAGE_FLAG_HAS_EDIT_DATE) {
        message_info.edit_date = message->edit_date_;
      }
      if (message->flags_ & MESSAGE_FLAG_HAS_MEDIA_ALBUM_ID) {
        message_info.media_album_id = message->grouped_id_;
      }
      if (message->flags_ & MESSAGE_FLAG_HAS_TTL_PERIOD) {
        message_info.ttl_period = message->ttl_period_;
      }
      message_info.flags = message->flags_;
      bool is_content_read = (message->flags_ & MESSAGE_FLAG_HAS_UNREAD_CONTENT) == 0;
      if (is_message_auto_read(message_info.dialog_id, (message->flags_ & MESSAGE_FLAG_IS_OUT) != 0)) {
        is_content_read = true;
      }
      if (is_scheduled) {
        is_content_read = false;
      }
      auto new_source = PSTRING() << FullMessageId(message_info.dialog_id, message_info.message_id) << " from "
                                  << source;
      message_info.content = get_message_content(
          td_,
          get_message_text(td_->contacts_manager_.get(), std::move(message->message_), std::move(message->entities_),
                           true, td_->auth_manager_->is_bot(),
                           message_info.forward_header ? message_info.forward_header->date_ : message_info.date,
                           message_info.media_album_id != 0, new_source.c_str()),
          std::move(message->media_), message_info.dialog_id, is_content_read, message_info.via_bot_user_id,
          &message_info.ttl, &message_info.disable_web_page_preview);
      message_info.reply_markup =
          message->flags_ & MESSAGE_FLAG_HAS_REPLY_MARKUP ? std::move(message->reply_markup_) : nullptr;
      message_info.restriction_reasons = get_restriction_reasons(std::move(message->restriction_reason_));
      message_info.author_signature = std::move(message->post_author_);
      break;
    }
    case telegram_api::messageService::ID: {
      auto message = move_tl_object_as<telegram_api::messageService>(message_ptr);

      message_info.dialog_id = DialogId(message->peer_id_);
      if (message->from_id_ != nullptr) {
        message_info.sender_dialog_id = DialogId(message->from_id_);
      } else {
        message_info.sender_dialog_id = message_info.dialog_id;
      }
      message_info.date = message->date_;
      if (message->flags_ & MESSAGE_FLAG_HAS_TTL_PERIOD) {
        message_info.ttl_period = message->ttl_period_;
      }
      message_info.flags = message->flags_;

      DialogId reply_in_dialog_id;
      MessageId reply_to_message_id;
      if (message->reply_to_ != nullptr) {
        reply_to_message_id = MessageId(ServerMessageId(message->reply_to_->reply_to_msg_id_));
        auto reply_to_peer_id = std::move(message->reply_to_->reply_to_peer_id_);
        if (reply_to_peer_id != nullptr) {
          reply_in_dialog_id = DialogId(reply_to_peer_id);
          if (!reply_in_dialog_id.is_valid()) {
            LOG(ERROR) << "Receive reply in invalid " << to_string(reply_to_peer_id);
            reply_to_message_id = MessageId();
            reply_in_dialog_id = DialogId();
          }
        }
      }
      message_info.content = get_action_message_content(td_, std::move(message->action_), message_info.dialog_id,
                                                        reply_in_dialog_id, reply_to_message_id);
      break;
    }
    default:
      UNREACHABLE();
      break;
  }
  if (message_info.sender_dialog_id.is_valid() && message_info.sender_dialog_id.get_type() == DialogType::User) {
    message_info.sender_user_id = message_info.sender_dialog_id.get_user_id();
    message_info.sender_dialog_id = DialogId();
  }
  return message_info;
}

Result<MessagesManager::MessagePushNotificationInfo> MessagesManager::get_message_push_notification_info(
    DialogId dialog_id, MessageId message_id, int64 random_id, UserId sender_user_id, DialogId sender_dialog_id,
    int32 date, bool is_from_scheduled, bool contains_mention, bool is_pinned, bool is_from_binlog) {
  if (!is_from_scheduled && dialog_id == get_my_dialog_id()) {
    return Status::Error("Ignore notification in chat with self");
  }
  if (td_->auth_manager_->is_bot()) {
    return Status::Error("Ignore notification sent to bot");
  }

  Dialog *d = get_dialog_force(dialog_id, "get_message_push_notification_info");
  if (d == nullptr) {
    return Status::Error(406, "Ignore notification in unknown chat");
  }
  if (sender_dialog_id.is_valid() && !have_dialog_force(sender_dialog_id, "get_message_push_notification_info")) {
    return Status::Error(406, "Ignore notification sent by unknown chat");
  }

  if (is_from_scheduled && dialog_id != get_my_dialog_id() &&
      G()->shared_config().get_option_boolean("disable_sent_scheduled_message_notifications")) {
    return Status::Error("Ignore notification about sent scheduled message");
  }

  bool is_new_pinned = is_pinned && message_id.is_valid() && message_id > d->max_notification_message_id;
  CHECK(!message_id.is_scheduled());
  if (message_id.is_valid()) {
    if (message_id <= d->last_new_message_id) {
      return Status::Error("Ignore notification about known message");
    }
    if (!is_from_binlog && message_id == d->max_notification_message_id) {
      return Status::Error("Ignore previously added message push notification");
    }
    if (!is_from_binlog && message_id < d->max_notification_message_id) {
      return Status::Error("Ignore out of order message push notification");
    }
    if (message_id <= d->last_read_inbox_message_id) {
      return Status::Error("Ignore notification about read message");
    }
    if (message_id <= d->last_clear_history_message_id) {
      return Status::Error("Ignore notification about message from cleared chat history");
    }
    if (d->deleted_message_ids.count(message_id)) {
      return Status::Error("Ignore notification about deleted message");
    }
    if (message_id <= d->max_unavailable_message_id) {
      return Status::Error("Ignore notification about unavailable message");
    }
  }
  if (random_id != 0) {
    CHECK(dialog_id.get_type() == DialogType::SecretChat);
    if (get_message_id_by_random_id(d, random_id, "get_message_push_notification_info").is_valid()) {
      return Status::Error(406, "Ignore notification about known secret message");
    }
  }

  if (is_pinned) {
    contains_mention = !is_dialog_pinned_message_notifications_disabled(d);
  } else if (contains_mention && is_dialog_mention_notifications_disabled(d)) {
    contains_mention = false;
  }
  if (dialog_id.get_type() == DialogType::User) {
    contains_mention = false;
  }

  DialogId settings_dialog_id = dialog_id;
  Dialog *settings_dialog = d;
  if (contains_mention) {
    auto real_sender_dialog_id = sender_dialog_id.is_valid() ? sender_dialog_id : DialogId(sender_user_id);
    if (real_sender_dialog_id.is_valid()) {
      settings_dialog_id = real_sender_dialog_id;
      settings_dialog = get_dialog_force(settings_dialog_id, "get_message_push_notification_info");
    }
  }

  bool have_settings;
  int32 mute_until;
  std::tie(have_settings, mute_until) = get_dialog_mute_until(settings_dialog_id, settings_dialog);
  if (have_settings && mute_until > date) {
    if (is_new_pinned) {
      remove_dialog_pinned_message_notification(d, "get_message_push_notification_info");
    }
    return Status::Error("Ignore notification in muted chat");
  }

  if (is_dialog_message_notification_disabled(settings_dialog_id, date)) {
    if (is_new_pinned) {
      remove_dialog_pinned_message_notification(d, "get_message_push_notification_info");
    }
    return Status::Error("Ignore notification in chat, because notifications are disabled in the chat");
  }

  auto group_id = get_dialog_notification_group_id(
      dialog_id, contains_mention ? d->mention_notification_group : d->message_notification_group);
  if (!group_id.is_valid()) {
    return Status::Error("Can't assign notification group ID");
  }

  if (message_id.is_valid() && message_id > d->max_notification_message_id) {
    if (is_new_pinned) {
      set_dialog_pinned_message_notification(d, contains_mention ? message_id : MessageId(),
                                             "get_message_push_notification_info");
    }
    d->max_notification_message_id = message_id;
    on_dialog_updated(dialog_id, "set_max_notification_message_id");
  }

  MessagePushNotificationInfo result;
  result.group_id = group_id;
  result.group_type = contains_mention ? NotificationGroupType::Mentions : NotificationGroupType::Messages;
  result.settings_dialog_id = settings_dialog_id;
  return result;
}

class InitHistoryImportQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;
  DialogId dialog_id_;
  vector<FileId> attached_file_ids_;

 public:
  explicit InitHistoryImportQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, FileId file_id, tl_object_ptr<telegram_api::InputFile> &&input_file,
            vector<FileId> attached_file_ids) {
    CHECK(input_file != nullptr);
    file_id_ = file_id;
    dialog_id_ = dialog_id;
    attached_file_ids_ = std::move(attached_file_ids);

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::messages_initHistoryImport(
        std::move(input_peer), std::move(input_file), narrow_cast<int32>(attached_file_ids_.size()))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_initHistoryImport>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->file_manager_->delete_partial_remote_location(file_id_);

    auto ptr = result_ptr.move_as_ok();
    td_->messages_manager_->start_import_messages(dialog_id_, ptr->id_, std::move(attached_file_ids_),
                                                  std::move(promise_));
  }

  void on_error(Status status) final {
    if (FileReferenceManager::is_file_reference_error(status)) {
      LOG(ERROR) << "Receive file reference error " << status;
    }
    if (begins_with(status.message(), "FILE_PART_") && ends_with(status.message(), "_MISSING")) {
      // TODO support FILE_PART_*_MISSING
    }

    td_->file_manager_->delete_partial_remote_location(file_id_);

    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "InitHistoryImportQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::on_upload_imported_messages(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) {
  LOG(INFO) << "File " << file_id << " has been uploaded";

  auto it = being_uploaded_imported_messages_.find(file_id);
  if (it == being_uploaded_imported_messages_.end()) {
    // just in case, as in on_upload_media
    return;
  }

  CHECK(it->second != nullptr);
  DialogId dialog_id = it->second->dialog_id;
  vector<FileId> attached_file_ids = std::move(it->second->attached_file_ids);
  bool is_reupload = it->second->is_reupload;
  Promise<Unit> promise = std::move(it->second->promise);

  being_uploaded_imported_messages_.erase(it);

  TRY_STATUS_PROMISE(promise, can_send_message(dialog_id));

  FileView file_view = td_->file_manager_->get_file_view(file_id);
  CHECK(!file_view.is_encrypted());
  if (input_file == nullptr && file_view.has_remote_location()) {
    if (file_view.main_remote_location().is_web()) {
      return promise.set_error(Status::Error(400, "Can't use web file"));
    }
    if (is_reupload) {
      return promise.set_error(Status::Error(400, "Failed to reupload the file"));
    }

    CHECK(file_view.get_type() == FileType::Document);
    // delete file reference and forcely reupload the file
    auto file_reference = FileManager::extract_file_reference(file_view.main_remote_location().as_input_document());
    td_->file_manager_->delete_file_reference(file_id, file_reference);
    upload_imported_messages(dialog_id, file_id, std::move(attached_file_ids), true, std::move(promise), {-1});
    return;
  }
  CHECK(input_file != nullptr);

  td_->create_handler<InitHistoryImportQuery>(std::move(promise))
      ->send(dialog_id, file_id, std::move(input_file), std::move(attached_file_ids));
}

void MessagesManager::set_dialog_max_unavailable_message_id(DialogId dialog_id, MessageId max_unavailable_message_id,
                                                            bool from_update, const char *source) {
  CHECK(!max_unavailable_message_id.is_scheduled());

  Dialog *d = get_dialog_force(dialog_id, source);
  if (d != nullptr) {
    if (d->last_new_message_id.is_valid() && max_unavailable_message_id > d->last_new_message_id && from_update) {
      if (!td_->auth_manager_->is_bot()) {
        LOG(ERROR) << "Tried to set " << dialog_id << " max unavailable message to " << max_unavailable_message_id
                   << " from " << source << ", but last new message is " << d->last_new_message_id;
      }
      max_unavailable_message_id = d->last_new_message_id;
    }

    if (d->max_unavailable_message_id == max_unavailable_message_id) {
      return;
    }

    if (max_unavailable_message_id.is_valid() && max_unavailable_message_id.is_yet_unsent()) {
      LOG(ERROR) << "Tried to update " << dialog_id << " last read outbox message with " << max_unavailable_message_id
                 << " from " << source;
      return;
    }
    LOG(INFO) << "Set max unavailable message to " << max_unavailable_message_id << " in " << dialog_id << " from "
              << source;

    on_dialog_updated(dialog_id, "set_dialog_max_unavailable_message_id");

    if (d->max_unavailable_message_id > max_unavailable_message_id) {
      d->max_unavailable_message_id = max_unavailable_message_id;
      return;
    }

    d->max_unavailable_message_id = max_unavailable_message_id;

    vector<MessageId> message_ids;
    find_old_messages(d->messages.get(), max_unavailable_message_id, message_ids);

    vector<int64> deleted_message_ids;
    bool need_update_dialog_pos = false;
    for (auto message_id : message_ids) {
      if (message_id.is_yet_unsent()) {
        continue;
      }

      auto m = get_message(d, message_id);
      CHECK(m != nullptr);
      CHECK(m->message_id <= max_unavailable_message_id);
      CHECK(m->message_id == message_id);
      auto p =
          delete_message(d, message_id, !from_update, &need_update_dialog_pos, "set_dialog_max_unavailable_message_id");
      CHECK(p.get() == m);
      deleted_message_ids.push_back(p->message_id.get());
    }

    if (need_update_dialog_pos) {
      send_update_chat_last_message(d, "set_dialog_max_unavailable_message_id");
    }

    send_update_delete_messages(dialog_id, std::move(deleted_message_ids), !from_update, false);

    if (d->server_unread_count + d->local_unread_count > 0) {
      read_history_inbox(dialog_id, max_unavailable_message_id, -1, "set_dialog_max_unavailable_message_id");
    }
  } else {
    LOG(INFO) << "Receive max unavailable message in unknown " << dialog_id << " from " << source;
  }
}

void MessagesManager::close_dialog(Dialog *d) {
  if (!d->is_opened) {
    return;
  }
  d->is_opened = false;

  auto dialog_id = d->dialog_id;
  if (have_input_peer(dialog_id, AccessRights::Write)) {
    if (pending_draft_message_timeout_.has_timeout(dialog_id.get())) {
      pending_draft_message_timeout_.set_timeout_in(dialog_id.get(), 0.0);
    }
  } else {
    pending_draft_message_timeout_.cancel_timeout(dialog_id.get());
  }

  if (have_input_peer(dialog_id, AccessRights::Read)) {
    if (pending_message_views_timeout_.has_timeout(dialog_id.get())) {
      pending_message_views_timeout_.set_timeout_in(dialog_id.get(), 0.0);
    }
    if (pending_read_history_timeout_.has_timeout(dialog_id.get())) {
      pending_read_history_timeout_.set_timeout_in(dialog_id.get(), 0.0);
    }
  } else {
    pending_message_views_timeout_.cancel_timeout(dialog_id.get());
    d->pending_viewed_message_ids.clear();
    d->increment_view_counter = false;

    pending_read_history_timeout_.cancel_timeout(dialog_id.get());
  }

  if (is_message_unload_enabled()) {
    CHECK(!d->has_unload_timeout);
    pending_unload_dialog_timeout_.set_timeout_in(dialog_id.get(), get_next_unload_dialog_delay());
    d->has_unload_timeout = true;
  }

  for (auto &it : d->pending_viewed_live_locations) {
    auto live_location_task_id = it.second;
    auto erased_count = viewed_live_location_tasks_.erase(live_location_task_id);
    CHECK(erased_count > 0);
  }
  d->pending_viewed_live_locations.clear();

  switch (dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat:
      break;
    case DialogType::Channel:
      channel_get_difference_timeout_.cancel_timeout(dialog_id.get());
      break;
    case DialogType::SecretChat:
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  if (!td_->auth_manager_->is_bot()) {
    auto online_count_it = dialog_online_member_counts_.find(dialog_id);
    if (online_count_it != dialog_online_member_counts_.end()) {
      auto &info = online_count_it->second;
      info.is_update_sent = false;
    }
    update_dialog_online_member_count_timeout_.set_timeout_in(dialog_id.get(), ONLINE_MEMBER_COUNT_CACHE_EXPIRE_TIME);
  }
}

void MessagesManager::stop_poll(FullMessageId full_message_id, td_api::object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                Promise<Unit> &&promise) {
  auto m = get_message_force(full_message_id, "stop_poll");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (!have_input_peer(full_message_id.get_dialog_id(), AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }
  if (m->content->get_type() != MessageContentType::Poll) {
    return promise.set_error(Status::Error(400, "Message is not a poll"));
  }
  if (get_message_content_poll_is_closed(td_, m->content.get())) {
    return promise.set_error(Status::Error(400, "Poll has already been closed"));
  }
  if (!can_edit_message(full_message_id.get_dialog_id(), m, true)) {
    return promise.set_error(Status::Error(400, "Poll can't be stopped"));
  }
  if (m->message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Can't stop polls from scheduled messages"));
  }
  if (!m->message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Poll can't be stopped"));
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false,
                                             has_message_sender_user_id(full_message_id.get_dialog_id(), m));
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  stop_message_content_poll(td_, m->content.get(), full_message_id, r_new_reply_markup.move_as_ok(),
                            std::move(promise));
}

Status MessagesManager::can_use_top_thread_message_id(Dialog *d, MessageId top_thread_message_id,
                                                      MessageId reply_to_message_id) {
  if (top_thread_message_id == MessageId()) {
    return Status::OK();
  }

  if (!top_thread_message_id.is_valid() || !top_thread_message_id.is_server()) {
    return Status::Error(400, "Invalid message thread ID specified");
  }
  if (d->dialog_id.get_type() != DialogType::Channel || is_broadcast_channel(d->dialog_id)) {
    return Status::Error(400, "Chat doesn't have threads");
  }
  if (reply_to_message_id.is_valid()) {
    const Message *reply_m = get_message_force(d, reply_to_message_id, "can_use_top_thread_message_id 1");
    if (reply_m != nullptr && top_thread_message_id != reply_m->top_thread_message_id) {
      if (reply_m->top_thread_message_id.is_valid() || reply_m->media_album_id == 0) {
        return Status::Error(400, "The message to reply is not in the specified message thread");
      }

      // if the message is in an album and not in the thread, it can be in the album of top_thread_message_id
      const Message *top_m = get_message_force(d, top_thread_message_id, "can_use_top_thread_message_id 2");
      if (top_m != nullptr &&
          (top_m->media_album_id != reply_m->media_album_id || top_m->top_thread_message_id != top_m->message_id)) {
        return Status::Error(400, "The message to reply is not in the specified message thread root album");
      }
    }
  }

  return Status::OK();
}

void MessagesManager::update_message_reply_count(Dialog *d, MessageId message_id, DialogId replier_dialog_id,
                                                 MessageId reply_message_id, int32 update_date, int diff,
                                                 bool is_recursive) {
  if (d == nullptr) {
    return;
  }

  Message *m = get_message(d, message_id);
  if (m == nullptr || !is_active_message_reply_info(d->dialog_id, m->reply_info)) {
    return;
  }
  LOG(INFO) << "Update reply count to " << message_id << " in " << d->dialog_id << " by " << diff << " from "
            << reply_message_id << " sent by " << replier_dialog_id;
  if (m->interaction_info_update_date < update_date &&
      m->reply_info.add_reply(replier_dialog_id, reply_message_id, diff)) {
    on_message_reply_info_changed(d->dialog_id, m);
    on_message_changed(d, m, true, "update_message_reply_count_by_message");
  }

  if (!is_recursive && is_discussion_message(d->dialog_id, m)) {
    update_message_reply_count(get_dialog(m->forward_info->from_dialog_id), m->forward_info->from_message_id,
                               replier_dialog_id, reply_message_id, update_date, diff, true);
  }
}

void MessagesManager::do_delete_all_dialog_messages(Dialog *d, unique_ptr<Message> &message,
                                                    bool is_permanently_deleted, vector<int64> &deleted_message_ids) {
  if (message == nullptr) {
    return;
  }
  const Message *m = message.get();
  MessageId message_id = m->message_id;

  if (is_debug_message_op_enabled()) {
    d->debug_message_op.emplace_back(Dialog::MessageOp::Delete, m->message_id, m->content->get_type(), false,
                                     m->have_previous, m->have_next, "delete all messages");
  }

  LOG(INFO) << "Delete " << message_id;
  deleted_message_ids.push_back(message_id.get());

  do_delete_all_dialog_messages(d, message->right, is_permanently_deleted, deleted_message_ids);
  do_delete_all_dialog_messages(d, message->left, is_permanently_deleted, deleted_message_ids);

  delete_active_live_location(d->dialog_id, m);
  remove_message_file_sources(d->dialog_id, m);

  on_message_deleted(d, message.get(), is_permanently_deleted, "do_delete_all_dialog_messages");

  message = nullptr;
}

void MessagesManager::view_message_live_location_on_server(int64 task_id) {
  if (G()->close_flag()) {
    return;
  }

  auto it = viewed_live_location_tasks_.find(task_id);
  if (it == viewed_live_location_tasks_.end()) {
    return;
  }

  auto full_message_id = it->second;
  Dialog *d = get_dialog(full_message_id.get_dialog_id());
  const Message *m = get_message_force(d, full_message_id.get_message_id(), "view_message_live_location_on_server");
  if (m == nullptr || get_message_content_live_location_period(m->content.get()) <= G()->unix_time() - m->date + 1) {
    // the message was deleted or live location is expired
    viewed_live_location_tasks_.erase(it);
    auto erased_count = d->pending_viewed_live_locations.erase(full_message_id.get_message_id());
    CHECK(erased_count > 0);
    return;
  }

  view_message_live_location_on_server_impl(task_id, full_message_id);
}

bool MessagesManager::have_dialog_info_force(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      UserId user_id = dialog_id.get_user_id();
      return td_->contacts_manager_->have_user_force(user_id);
    }
    case DialogType::Chat: {
      ChatId chat_id = dialog_id.get_chat_id();
      return td_->contacts_manager_->have_chat_force(chat_id);
    }
    case DialogType::Channel: {
      ChannelId channel_id = dialog_id.get_channel_id();
      return td_->contacts_manager_->have_channel_force(channel_id);
    }
    case DialogType::SecretChat: {
      SecretChatId secret_chat_id = dialog_id.get_secret_chat_id();
      return td_->contacts_manager_->have_secret_chat_force(secret_chat_id);
    }
    case DialogType::None:
    default:
      return false;
  }
}

td_api::object_ptr<td_api::updateUnreadMessageCount> MessagesManager::get_update_unread_message_count_object(
    const DialogList &list) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(list.is_message_unread_count_inited_);
  int32 unread_count = list.unread_message_total_count_;
  int32 unread_unmuted_count = list.unread_message_total_count_ - list.unread_message_muted_count_;
  CHECK(unread_count >= 0);
  CHECK(unread_unmuted_count >= 0);
  return td_api::make_object<td_api::updateUnreadMessageCount>(list.dialog_list_id.get_chat_list_object(), unread_count,
                                                               unread_unmuted_count);
}

unique_ptr<MessagesManager::Message> MessagesManager::treap_delete_message(unique_ptr<Message> *v) {
  unique_ptr<Message> result = std::move(*v);
  unique_ptr<Message> left = std::move(result->left);
  unique_ptr<Message> right = std::move(result->right);

  while (left != nullptr || right != nullptr) {
    if (left == nullptr || (right != nullptr && right->random_y > left->random_y)) {
      *v = std::move(right);
      v = &((*v)->left);
      right = std::move(*v);
    } else {
      *v = std::move(left);
      v = &((*v)->right);
      left = std::move(*v);
    }
  }
  CHECK(*v == nullptr);

  return result;
}

bool MessagesManager::is_visible_message_reply_info(DialogId dialog_id, const Message *m) const {
  CHECK(m != nullptr);
  if (!m->message_id.is_valid()) {
    return false;
  }
  bool is_broadcast = is_broadcast_channel(dialog_id);
  if (!m->message_id.is_server() && !(is_broadcast && m->message_id.is_yet_unsent())) {
    return false;
  }
  if (is_broadcast && (m->had_reply_markup || m->reply_markup != nullptr)) {
    return false;
  }
  return is_active_message_reply_info(dialog_id, m->reply_info);
}

void MessagesManager::on_get_public_message_link(FullMessageId full_message_id, bool for_group, string url,
                                                 string html) {
  LOG_IF(ERROR, url.empty() && html.empty()) << "Receive empty public link for " << full_message_id;
  auto dialog_id = full_message_id.get_dialog_id();
  auto message_id = full_message_id.get_message_id();
  message_embedding_codes_[for_group][dialog_id].embedding_codes_[message_id] = std::move(html);
}

void MessagesManager::schedule_scope_unmute(NotificationSettingsScope scope, int32 mute_until) {
  auto now = G()->unix_time_cached();
  if (mute_until >= now && mute_until < now + 366 * 86400) {
    dialog_unmute_timeout_.set_timeout_in(static_cast<int64>(scope) + 1, mute_until - now + 1);
  } else {
    dialog_unmute_timeout_.cancel_timeout(static_cast<int64>(scope) + 1);
  }
}

MessageId MessagesManager::find_message_by_date(const Message *m, int32 date) {
  if (m == nullptr) {
    return MessageId();
  }

  if (m->date > date) {
    return find_message_by_date(m->left.get(), date);
  }

  auto message_id = find_message_by_date(m->right.get(), date);
  if (message_id.is_valid()) {
    return message_id;
  }

  return m->message_id;
}

bool MessagesManager::is_dialog_inited(const Dialog *d) {
  return d != nullptr && d->notification_settings.is_synchronized && d->is_last_read_inbox_message_id_inited &&
         d->is_last_read_outbox_message_id_inited;
}

void MessagesManager::send_update_chat_last_message(Dialog *d, const char *source) {
  update_dialog_pos(d, source, false);
  send_update_chat_last_message_impl(d, source);
}
}  // namespace td
