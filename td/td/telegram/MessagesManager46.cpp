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

MessagesManager::MessageNotificationGroup MessagesManager::get_message_notification_group_force(
    NotificationGroupId group_id) {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(group_id.is_valid());
  Dialog *d = nullptr;
  auto it = notification_group_id_to_dialog_id_.find(group_id);
  if (it != notification_group_id_to_dialog_id_.end()) {
    d = get_dialog(it->second);
    CHECK(d != nullptr);
  } else if (G()->parameters().use_message_db) {
    auto *dialog_db = G()->td_db()->get_dialog_db_sync();
    dialog_db->begin_read_transaction().ensure();
    auto r_value = dialog_db->get_notification_group(group_id);
    if (r_value.is_ok()) {
      VLOG(notifications) << "Loaded " << r_value.ok() << " from database by " << group_id;
      d = get_dialog_force(r_value.ok().dialog_id, "get_message_notification_group_force");
    } else {
      LOG_CHECK(r_value.error().message() == "Not found") << r_value.error();
      VLOG(notifications) << "Failed to load " << group_id << " from database";
    }
    dialog_db->commit_transaction().ensure();
  }

  if (d == nullptr) {
    return MessageNotificationGroup();
  }
  if (d->message_notification_group.group_id != group_id && d->mention_notification_group.group_id != group_id) {
    if (d->dialog_id.get_type() == DialogType::SecretChat && !d->message_notification_group.group_id.is_valid() &&
        !d->mention_notification_group.group_id.is_valid()) {
      // the group was reused, but wasn't deleted from the database, trying to resave it
      auto &group_info = d->message_notification_group;
      group_info.group_id = group_id;
      group_info.is_changed = true;
      group_info.try_reuse = true;
      save_dialog_to_database(d->dialog_id);
      group_info.group_id = NotificationGroupId();
      group_info.is_changed = false;
      group_info.try_reuse = false;
    }
  }

  LOG_CHECK(d->message_notification_group.group_id == group_id || d->mention_notification_group.group_id == group_id)
      << group_id << " " << d->message_notification_group.group_id << " " << d->mention_notification_group.group_id
      << " " << d->dialog_id << " " << notification_group_id_to_dialog_id_[group_id] << " "
      << notification_group_id_to_dialog_id_[d->message_notification_group.group_id] << " "
      << notification_group_id_to_dialog_id_[d->mention_notification_group.group_id];

  bool from_mentions = d->mention_notification_group.group_id == group_id;
  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;

  MessageNotificationGroup result;
  VLOG(notifications) << "Found " << (from_mentions ? "Mentions " : "Messages ") << group_info.group_id << '/'
                      << d->dialog_id << " by " << group_id << " with " << d->unread_mention_count
                      << " unread mentions, pinned " << d->pinned_message_notification_message_id
                      << ", new secret chat " << d->new_secret_chat_notification_id << " and "
                      << d->server_unread_count + d->local_unread_count << " unread messages";
  result.dialog_id = d->dialog_id;
  result.total_count = get_dialog_pending_notification_count(d, from_mentions);
  auto pending_notification_count =
      from_mentions ? d->pending_new_mention_notifications.size() : d->pending_new_message_notifications.size();
  result.total_count -= static_cast<int32>(pending_notification_count);
  if (result.total_count < 0) {
    LOG(ERROR) << "Total notification count is " << result.total_count << " in " << d->dialog_id << " with "
               << pending_notification_count << " pending new notifications";
    result.total_count = 0;
  }
  if (d->new_secret_chat_notification_id.is_valid()) {
    CHECK(d->dialog_id.get_type() == DialogType::SecretChat);
    result.type = NotificationGroupType::SecretChat;
    result.notifications.emplace_back(d->new_secret_chat_notification_id,
                                      td_->contacts_manager_->get_secret_chat_date(d->dialog_id.get_secret_chat_id()),
                                      false, create_new_secret_chat_notification());
  } else {
    result.type = from_mentions ? NotificationGroupType::Mentions : NotificationGroupType::Messages;
    result.notifications = get_message_notifications_from_database_force(
        d, from_mentions, static_cast<int32>(td_->notification_manager_->get_max_notification_group_size()));
  }

  int32 last_notification_date = 0;
  NotificationId last_notification_id;
  if (!result.notifications.empty()) {
    last_notification_date = result.notifications[0].date;
    last_notification_id = result.notifications[0].notification_id;
  }
  if (last_notification_date != group_info.last_notification_date ||
      last_notification_id != group_info.last_notification_id) {
    LOG(ERROR) << "Fix last notification date in " << d->dialog_id << " from " << group_info.last_notification_date
               << " to " << last_notification_date << " and last notification identifier from "
               << group_info.last_notification_id << " to " << last_notification_id << " in " << group_id << " of type "
               << result.type;
    set_dialog_last_notification(d->dialog_id, group_info, last_notification_date, last_notification_id,
                                 "get_message_notification_group_force");
  }

  std::reverse(result.notifications.begin(), result.notifications.end());

  return result;
}

class GetCommonDialogsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  UserId user_id_;
  int64 offset_chat_id_ = 0;

 public:
  explicit GetCommonDialogsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(UserId user_id, int64 offset_chat_id, int32 limit) {
    user_id_ = user_id;
    offset_chat_id_ = offset_chat_id;

    auto r_input_user = td_->contacts_manager_->get_input_user(user_id);
    CHECK(r_input_user.is_ok());

    send_query(G()->net_query_creator().create(
        telegram_api::messages_getCommonChats(r_input_user.move_as_ok(), offset_chat_id, limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getCommonChats>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto chats_ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetCommonDialogsQuery: " << to_string(chats_ptr);
    switch (chats_ptr->get_id()) {
      case telegram_api::messages_chats::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chats>(chats_ptr);
        td_->messages_manager_->on_get_common_dialogs(user_id_, offset_chat_id_, std::move(chats->chats_),
                                                      narrow_cast<int32>(chats->chats_.size()));
        break;
      }
      case telegram_api::messages_chatsSlice::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chatsSlice>(chats_ptr);
        td_->messages_manager_->on_get_common_dialogs(user_id_, offset_chat_id_, std::move(chats->chats_),
                                                      chats->count_);
        break;
      }
      default:
        UNREACHABLE();
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

std::pair<int32, vector<DialogId>> MessagesManager::get_common_dialogs(UserId user_id, DialogId offset_dialog_id,
                                                                       int32 limit, bool force,
                                                                       Promise<Unit> &&promise) {
  if (!td_->contacts_manager_->have_input_user(user_id)) {
    promise.set_error(Status::Error(400, "Have no access to the user"));
    return {};
  }

  if (user_id == td_->contacts_manager_->get_my_id()) {
    promise.set_error(Status::Error(400, "Can't get common chats with self"));
    return {};
  }
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }
  if (limit > MAX_GET_DIALOGS) {
    limit = MAX_GET_DIALOGS;
  }

  int64 offset_chat_id = 0;
  switch (offset_dialog_id.get_type()) {
    case DialogType::Chat:
      offset_chat_id = offset_dialog_id.get_chat_id().get();
      break;
    case DialogType::Channel:
      offset_chat_id = offset_dialog_id.get_channel_id().get();
      break;
    case DialogType::None:
      if (offset_dialog_id == DialogId()) {
        break;
      }
    // fallthrough
    case DialogType::User:
    case DialogType::SecretChat:
      promise.set_error(Status::Error(400, "Wrong offset_chat_id"));
      return {};
    default:
      UNREACHABLE();
      break;
  }

  auto it = found_common_dialogs_.find(user_id);
  if (it != found_common_dialogs_.end() && !it->second.dialog_ids.empty()) {
    int32 total_count = it->second.total_count;
    vector<DialogId> &common_dialog_ids = it->second.dialog_ids;
    bool use_cache = (!it->second.is_outdated && it->second.received_date >= Time::now() - 3600) || force ||
                     offset_chat_id != 0 || common_dialog_ids.size() >= static_cast<size_t>(MAX_GET_DIALOGS);
    // use cache if it is up-to-date, or we required to use it or we can't update it
    if (use_cache) {
      auto offset_it = common_dialog_ids.begin();
      if (offset_dialog_id != DialogId()) {
        offset_it = std::find(common_dialog_ids.begin(), common_dialog_ids.end(), offset_dialog_id);
        if (offset_it == common_dialog_ids.end()) {
          promise.set_error(Status::Error(400, "Wrong offset_chat_id"));
          return {};
        }
        ++offset_it;
      }
      vector<DialogId> result;
      while (result.size() < static_cast<size_t>(limit)) {
        if (offset_it == common_dialog_ids.end()) {
          break;
        }
        auto dialog_id = *offset_it++;
        if (dialog_id == DialogId()) {  // end of the list
          promise.set_value(Unit());
          return {total_count, std::move(result)};
        }
        result.push_back(dialog_id);
      }
      if (result.size() == static_cast<size_t>(limit) || force) {
        promise.set_value(Unit());
        return {total_count, std::move(result)};
      }
    }
  }

  td_->create_handler<GetCommonDialogsQuery>(std::move(promise))->send(user_id, offset_chat_id, MAX_GET_DIALOGS);
  return {};
}

class EditDialogPhotoQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;
  bool was_uploaded_ = false;
  string file_reference_;
  DialogId dialog_id_;

 public:
  explicit EditDialogPhotoQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, FileId file_id, tl_object_ptr<telegram_api::InputChatPhoto> &&input_chat_photo) {
    CHECK(input_chat_photo != nullptr);
    file_id_ = file_id;
    was_uploaded_ = FileManager::extract_was_uploaded(input_chat_photo);
    file_reference_ = FileManager::extract_file_reference(input_chat_photo);
    dialog_id_ = dialog_id;

    switch (dialog_id.get_type()) {
      case DialogType::Chat:
        send_query(G()->net_query_creator().create(
            telegram_api::messages_editChatPhoto(dialog_id.get_chat_id().get(), std::move(input_chat_photo))));
        break;
      case DialogType::Channel: {
        auto channel_id = dialog_id.get_channel_id();
        auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
        CHECK(input_channel != nullptr);
        send_query(G()->net_query_creator().create(
            telegram_api::channels_editPhoto(std::move(input_channel), std::move(input_chat_photo))));
        break;
      }
      default:
        UNREACHABLE();
    }
  }

  void on_result(BufferSlice packet) final {
    static_assert(std::is_same<telegram_api::messages_editChatPhoto::ReturnType,
                               telegram_api::channels_editPhoto::ReturnType>::value,
                  "");
    auto result_ptr = fetch_result<telegram_api::messages_editChatPhoto>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditDialogPhotoQuery: " << to_string(ptr);

    if (file_id_.is_valid() && was_uploaded_) {
      td_->file_manager_->delete_partial_remote_location(file_id_);
    }

    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (file_id_.is_valid() && was_uploaded_) {
      td_->file_manager_->delete_partial_remote_location(file_id_);
    }
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      if (file_id_.is_valid() && !was_uploaded_) {
        VLOG(file_references) << "Receive " << status << " for " << file_id_;
        td_->file_manager_->delete_file_reference(file_id_, file_reference_);
        td_->messages_manager_->upload_dialog_photo(dialog_id_, file_id_, false, 0.0, false, std::move(promise_), {-1});
        return;
      } else {
        LOG(ERROR) << "Receive file reference error, but file_id = " << file_id_
                   << ", was_uploaded = " << was_uploaded_;
      }
    }

    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditDialogPhotoQuery");
    }
    td_->updates_manager_->get_difference("EditDialogPhotoQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::send_edit_dialog_photo_query(DialogId dialog_id, FileId file_id,
                                                   tl_object_ptr<telegram_api::InputChatPhoto> &&input_chat_photo,
                                                   Promise<Unit> &&promise) {
  // TODO invoke after
  td_->create_handler<EditDialogPhotoQuery>(std::move(promise))->send(dialog_id, file_id, std::move(input_chat_photo));
}

class UpdateDialogNotifySettingsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit UpdateDialogNotifySettingsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const DialogNotificationSettings &new_settings) {
    dialog_id_ = dialog_id;

    auto input_notify_peer = td_->messages_manager_->get_input_notify_peer(dialog_id);
    if (input_notify_peer == nullptr) {
      return on_error(Status::Error(500, "Can't update chat notification settings"));
    }

    int32 flags = 0;
    if (!new_settings.use_default_mute_until) {
      flags |= telegram_api::inputPeerNotifySettings::MUTE_UNTIL_MASK;
    }
    if (!new_settings.use_default_sound) {
      flags |= telegram_api::inputPeerNotifySettings::SOUND_MASK;
    }
    if (!new_settings.use_default_show_preview) {
      flags |= telegram_api::inputPeerNotifySettings::SHOW_PREVIEWS_MASK;
    }
    if (new_settings.silent_send_message) {
      flags |= telegram_api::inputPeerNotifySettings::SILENT_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::account_updateNotifySettings(
        std::move(input_notify_peer), make_tl_object<telegram_api::inputPeerNotifySettings>(
                                          flags, new_settings.show_preview, new_settings.silent_send_message,
                                          new_settings.mute_until, new_settings.sound))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_updateNotifySettings>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      return on_error(Status::Error(400, "Receive false as result"));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "UpdateDialogNotifySettingsQuery")) {
      LOG(INFO) << "Receive error for set chat notification settings: " << status;
    }

    if (!td_->auth_manager_->is_bot() && td_->messages_manager_->get_input_notify_peer(dialog_id_) != nullptr) {
      // trying to repair notification settings for this dialog
      td_->messages_manager_->send_get_dialog_notification_settings_query(dialog_id_, Promise<>());
    }

    promise_.set_error(std::move(status));
  }
};

void MessagesManager::send_update_dialog_notification_settings_query(const Dialog *d, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(d != nullptr);
  // TODO do not send two queries simultaneously or use SequenceDispatcher
  td_->create_handler<UpdateDialogNotifySettingsQuery>(std::move(promise))
      ->send(d->dialog_id, d->notification_settings);
}

class BlockFromRepliesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit BlockFromRepliesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(MessageId message_id, bool need_delete_message, bool need_delete_all_messages, bool report_spam) {
    int32 flags = 0;
    if (need_delete_message) {
      flags |= telegram_api::contacts_blockFromReplies::DELETE_MESSAGE_MASK;
    }
    if (need_delete_all_messages) {
      flags |= telegram_api::contacts_blockFromReplies::DELETE_HISTORY_MASK;
    }
    if (report_spam) {
      flags |= telegram_api::contacts_blockFromReplies::REPORT_SPAM_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::contacts_blockFromReplies(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_blockFromReplies>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for BlockFromRepliesQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::block_message_sender_from_replies_on_server(MessageId message_id, bool need_delete_message,
                                                                  bool need_delete_all_messages, bool report_spam,
                                                                  uint64 log_event_id, Promise<Unit> &&promise) {
  if (log_event_id == 0) {
    log_event_id = save_block_message_sender_from_replies_on_server_log_event(message_id, need_delete_message,
                                                                              need_delete_all_messages, report_spam);
  }

  td_->create_handler<BlockFromRepliesQuery>(get_erase_log_event_promise(log_event_id, std::move(promise)))
      ->send(message_id, need_delete_message, need_delete_all_messages, report_spam);
}

void MessagesManager::fix_dialog_last_notification_id(Dialog *d, bool from_mentions, MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(!message_id.is_scheduled());
  MessagesConstIterator it(d, message_id);
  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  VLOG(notifications) << "Trying to fix last notification identifier in " << group_info.group_id << " from "
                      << d->dialog_id << " from " << message_id << "/" << group_info.last_notification_id;
  if (*it != nullptr && ((*it)->message_id == message_id || (*it)->have_next)) {
    while (*it != nullptr) {
      const Message *m = *it;
      if (is_from_mention_notification_group(d, m) == from_mentions && m->notification_id.is_valid() &&
          is_message_notification_active(d, m) && m->message_id != message_id) {
        bool is_fixed = set_dialog_last_notification(d->dialog_id, group_info, m->date, m->notification_id,
                                                     "fix_dialog_last_notification_id");
        CHECK(is_fixed);
        return;
      }
      --it;
    }
  }
  if (G()->parameters().use_message_db) {
    get_message_notifications_from_database(
        d->dialog_id, group_info.group_id, group_info.last_notification_id, message_id, 1,
        PromiseCreator::lambda(
            [actor_id = actor_id(this), dialog_id = d->dialog_id, from_mentions,
             prev_last_notification_id = group_info.last_notification_id](Result<vector<Notification>> result) {
              send_closure(actor_id, &MessagesManager::do_fix_dialog_last_notification_id, dialog_id, from_mentions,
                           prev_last_notification_id, std::move(result));
            }));
  }
}

class CheckHistoryImportPeerQuery final : public Td::ResultHandler {
  Promise<string> promise_;
  DialogId dialog_id_;

 public:
  explicit CheckHistoryImportPeerQuery(Promise<string> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::messages_checkHistoryImportPeer(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_checkHistoryImportPeer>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for CheckHistoryImportPeerQuery: " << to_string(ptr);
    promise_.set_value(std::move(ptr->confirm_text_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "CheckHistoryImportPeerQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_message_import_confirmation_text(DialogId dialog_id, Promise<string> &&promise) {
  TRY_STATUS_PROMISE(promise, can_import_messages(dialog_id));

  td_->create_handler<CheckHistoryImportPeerQuery>(std::move(promise))->send(dialog_id);
}

MessagesManager::Message *MessagesManager::add_message_to_dialog(DialogId dialog_id, unique_ptr<Message> message,
                                                                 bool from_update, bool *need_update,
                                                                 bool *need_update_dialog_pos, const char *source) {
  CHECK(message != nullptr);
  CHECK(dialog_id.get_type() != DialogType::None);
  CHECK(need_update_dialog_pos != nullptr);

  MessageId message_id = message->message_id;
  if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
    LOG(ERROR) << "Receive " << message_id << " in " << dialog_id << " from " << source;
    debug_add_message_to_dialog_fail_reason_ = "invalid message identifier";
    return nullptr;
  }

  Dialog *d = get_dialog_force(dialog_id, source);
  if (d == nullptr) {
    if (from_update) {
      CHECK(!being_added_by_new_message_dialog_id_.is_valid());
      being_added_by_new_message_dialog_id_ = dialog_id;
    }
    d = add_dialog(dialog_id, "add_message_to_dialog");
    *need_update_dialog_pos = true;
    being_added_by_new_message_dialog_id_ = DialogId();
  } else {
    CHECK(d->dialog_id == dialog_id);
  }
  return add_message_to_dialog(d, std::move(message), from_update, need_update, need_update_dialog_pos, source);
}

void MessagesManager::get_dialog_info_full(DialogId dialog_id, Promise<Unit> &&promise, const char *source) {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      send_closure_later(td_->contacts_manager_actor_, &ContactsManager::load_user_full, dialog_id.get_user_id(), false,
                         std::move(promise), source);
      return;
    case DialogType::Chat:
      send_closure_later(td_->contacts_manager_actor_, &ContactsManager::load_chat_full, dialog_id.get_chat_id(), false,
                         std::move(promise), source);
      return;
    case DialogType::Channel:
      send_closure_later(td_->contacts_manager_actor_, &ContactsManager::load_channel_full, dialog_id.get_channel_id(),
                         false, std::move(promise), source);
      return;
    case DialogType::SecretChat:
      return promise.set_value(Unit());
    case DialogType::None:
    default:
      UNREACHABLE();
      return promise.set_error(Status::Error(500, "Wrong chat type"));
  }
}

MessagesManager::Message *MessagesManager::treap_insert_message(unique_ptr<Message> *v, unique_ptr<Message> message) {
  auto message_id = message->message_id;
  while (*v != nullptr && (*v)->random_y >= message->random_y) {
    if ((*v)->message_id < message_id) {
      v = &(*v)->right;
    } else if ((*v)->message_id == message_id) {
      UNREACHABLE();
    } else {
      v = &(*v)->left;
    }
  }

  unique_ptr<Message> *left = &message->left;
  unique_ptr<Message> *right = &message->right;

  unique_ptr<Message> cur = std::move(*v);
  while (cur != nullptr) {
    if (cur->message_id < message_id) {
      *left = std::move(cur);
      left = &((*left)->right);
      cur = std::move(*left);
    } else {
      *right = std::move(cur);
      right = &((*right)->left);
      cur = std::move(*right);
    }
  }
  CHECK(*left == nullptr);
  CHECK(*right == nullptr);
  *v = std::move(message);
  return v->get();
}

void MessagesManager::on_upload_media_error(FileId file_id, Status status) {
  if (G()->close_flag()) {
    // do not fail upload if closing
    return;
  }

  LOG(WARNING) << "File " << file_id << " has upload error " << status;
  CHECK(status.is_error());

  auto it = being_uploaded_files_.find(file_id);
  if (it == being_uploaded_files_.end()) {
    // callback may be called just before the file upload was canceled
    return;
  }

  auto full_message_id = it->second.first;

  being_uploaded_files_.erase(it);

  bool is_edit = full_message_id.get_message_id().is_any_server();
  if (is_edit) {
    fail_edit_message_media(full_message_id, Status::Error(status.code() > 0 ? status.code() : 500, status.message()));
  } else {
    fail_send_message(full_message_id, std::move(status));
  }
}

const DialogPhoto *MessagesManager::get_dialog_photo(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->get_user_dialog_photo(dialog_id.get_user_id());
    case DialogType::Chat:
      return td_->contacts_manager_->get_chat_dialog_photo(dialog_id.get_chat_id());
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_dialog_photo(dialog_id.get_channel_id());
    case DialogType::SecretChat:
      return td_->contacts_manager_->get_secret_chat_dialog_photo(dialog_id.get_secret_chat_id());
    case DialogType::None:
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void MessagesManager::on_update_dialog_last_pinned_message_id(DialogId dialog_id, MessageId pinned_message_id) {
  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive pinned message in invalid " << dialog_id;
    return;
  }
  if (!pinned_message_id.is_valid() && pinned_message_id != MessageId()) {
    LOG(ERROR) << "Receive as pinned message " << pinned_message_id;
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_last_pinned_message_id");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  set_dialog_last_pinned_message_id(d, pinned_message_id);
}

void MessagesManager::save_auth_notification_ids() {
  auto min_date = G()->unix_time() - AUTH_NOTIFICATION_ID_CACHE_TIME;
  vector<string> ids;
  for (auto &it : auth_notification_id_date_) {
    auto date = it.second;
    if (date < min_date) {
      continue;
    }
    ids.push_back(it.first);
    ids.push_back(to_string(date));
  }

  if (ids.empty()) {
    G()->td_db()->get_binlog_pmc()->erase("auth_notification_ids");
    return;
  }

  G()->td_db()->get_binlog_pmc()->set("auth_notification_ids", implode(ids, ','));
}

int64 MessagesManager::get_dialog_base_order(const Dialog *d) const {
  if (td_->auth_manager_->is_bot()) {
    return 0;  // to not call get_dialog_list
  }
  if (is_dialog_sponsored(d)) {
    return SPONSORED_DIALOG_ORDER;
  }
  if (d->order == DEFAULT_ORDER) {
    return 0;
  }
  auto pinned_order = get_dialog_pinned_order(DialogListId(FolderId::main()), d->dialog_id);
  if (pinned_order != DEFAULT_ORDER) {
    return pinned_order;
  }
  return d->order;
}

void MessagesManager::find_messages(const Message *m, vector<MessageId> &message_ids,
                                    const std::function<bool(const Message *)> &condition) {
  if (m == nullptr) {
    return;
  }

  find_messages(m->left.get(), message_ids, condition);

  if (condition(m)) {
    message_ids.push_back(m->message_id);
  }

  find_messages(m->right.get(), message_ids, condition);
}

void MessagesManager::send_update_new_message(const Dialog *d, const Message *m) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(d->is_update_new_chat_sent);
  send_closure(
      G()->td(), &Td::send_update,
      make_tl_object<td_api::updateNewMessage>(get_message_object(d->dialog_id, m, "send_update_new_message")));
}

vector<int32> MessagesManager::get_scheduled_server_message_ids(const vector<MessageId> &message_ids) {
  return transform(message_ids,
                   [](MessageId message_id) { return message_id.get_scheduled_server_message_id().get(); });
}

void MessagesManager::erase_delete_messages_log_event(uint64 log_event_id) {
  if (!G()->close_flag()) {
    binlog_erase(G()->td_db()->get_binlog(), log_event_id);
  }
}
}  // namespace td
