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
#include "td/telegram/SecretChatsManager.h"
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





#include <unordered_set>


namespace td {
}  // namespace td
namespace td {

class ReadHistoryQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit ReadHistoryQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId max_message_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::messages_readHistory(std::move(input_peer), max_message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_readHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto affected_messages = result_ptr.move_as_ok();
    CHECK(affected_messages->get_id() == telegram_api::messages_affectedMessages::ID);
    LOG(INFO) << "Receive result for ReadHistoryQuery: " << to_string(affected_messages);

    if (affected_messages->pts_count_ > 0) {
      td_->updates_manager_->add_pending_pts_update(make_tl_object<dummyUpdate>(), affected_messages->pts_,
                                                    affected_messages->pts_count_, Time::now(), Promise<Unit>(),
                                                    "read history query");
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ReadHistoryQuery")) {
      LOG(ERROR) << "Receive error for ReadHistoryQuery: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

class ReadChannelHistoryQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit ReadChannelHistoryQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, MessageId max_message_id) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    send_query(G()->net_query_creator().create(
        telegram_api::channels_readHistory(std::move(input_channel), max_message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_readHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ReadChannelHistoryQuery")) {
      LOG(ERROR) << "Receive error for ReadChannelHistoryQuery: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::read_history_on_server_impl(Dialog *d, MessageId max_message_id) {
  CHECK(d != nullptr);
  auto dialog_id = d->dialog_id;

  {
    auto message_id = d->last_read_inbox_message_id;
    if (dialog_id.get_type() != DialogType::SecretChat) {
      message_id = message_id.get_prev_server_message_id();
    }
    if (message_id > max_message_id) {
      max_message_id = message_id;
    }
  }

  Promise<> promise;
  if (d->read_history_log_event_ids[0].log_event_id != 0) {
    d->read_history_log_event_ids[0].generation++;
    promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id,
                                      generation = d->read_history_log_event_ids[0].generation](Result<Unit> result) {
      if (!G()->close_flag()) {
        send_closure(actor_id, &MessagesManager::on_read_history_finished, dialog_id, MessageId(), generation);
      }
    });
  }
  if (d->need_repair_server_unread_count && need_unread_counter(d->order)) {
    repair_server_unread_count(dialog_id, d->server_unread_count);
  }

  if (!max_message_id.is_valid() || !have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_value(Unit());
  }

  LOG(INFO) << "Send read history request in " << dialog_id << " up to " << max_message_id;
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
      td_->create_handler<ReadHistoryQuery>(std::move(promise))->send(dialog_id, max_message_id);
      break;
    case DialogType::Channel: {
      auto channel_id = dialog_id.get_channel_id();
      td_->create_handler<ReadChannelHistoryQuery>(std::move(promise))->send(channel_id, max_message_id);
      break;
    }
    case DialogType::SecretChat: {
      auto secret_chat_id = dialog_id.get_secret_chat_id();
      auto date = d->last_read_inbox_message_date;
      auto *m = get_message_force(d, max_message_id, "read_history_on_server_impl");
      if (m != nullptr && m->date > date) {
        date = m->date;
      }
      if (date == 0) {
        LOG(ERROR) << "Don't know last read inbox message date in " << dialog_id;
        return promise.set_value(Unit());
      }
      send_closure(G()->secret_chats_manager(), &SecretChatsManager::send_read_history, secret_chat_id, date,
                   std::move(promise));
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void MessagesManager::on_get_message_notifications_from_database(DialogId dialog_id, bool from_mentions,
                                                                 NotificationId initial_from_notification_id,
                                                                 int32 limit,
                                                                 Result<vector<MessagesDbDialogMessage>> result,
                                                                 Promise<vector<Notification>> promise) {
  if (G()->close_flag()) {
    result = Global::request_aborted_error();
  }
  if (result.is_error()) {
    return promise.set_error(result.move_as_error());
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  if (!group_info.group_id.is_valid()) {
    return promise.set_error(Status::Error("Notification group was deleted"));
  }

  auto messages = result.move_as_ok();
  vector<Notification> res;
  res.reserve(messages.size());
  NotificationId from_notification_id;
  MessageId from_message_id;
  VLOG(notifications) << "Loaded " << messages.size() << " messages with notifications in " << group_info.group_id
                      << '/' << dialog_id << " from database";
  for (auto &message : messages) {
    auto m = on_get_message_from_database(d, message, false, "on_get_message_notifications_from_database");
    if (m == nullptr) {
      VLOG(notifications) << "Receive from database a broken message";
      continue;
    }

    auto notification_id = m->notification_id.is_valid() ? m->notification_id : m->removed_notification_id;
    if (!notification_id.is_valid()) {
      LOG(ERROR) << "Can't find notification identifier for " << m->message_id << " in " << d->dialog_id
                 << " with from_mentions = " << from_mentions;
      continue;
    }
    CHECK(m->message_id.is_valid());

    bool is_correct = true;
    if (from_notification_id.is_valid() && notification_id.get() >= from_notification_id.get()) {
      LOG(ERROR) << "Receive " << m->message_id << "/" << notification_id << " after " << from_message_id << "/"
                 << from_notification_id;
      is_correct = false;
    } else {
      from_notification_id = notification_id;
    }
    if (from_message_id.is_valid() && m->message_id >= from_message_id) {
      LOG(ERROR) << "Receive " << m->message_id << "/" << notification_id << " after " << from_message_id << "/"
                 << from_notification_id;
      is_correct = false;
    } else {
      from_message_id = m->message_id;
    }

    if (notification_id.get() <= group_info.max_removed_notification_id.get() ||
        m->message_id <= group_info.max_removed_message_id ||
        (!from_mentions && m->message_id <= d->last_read_inbox_message_id)) {
      // if message still has notification_id, but it was removed via max_removed_notification_id,
      // or max_removed_message_id, or last_read_inbox_message_id,
      // then there will be no more messages with active notifications
      from_notification_id = NotificationId();  // stop requesting database
      break;
    }

    if (!m->notification_id.is_valid()) {
      // notification_id can be empty if it is deleted in memory, but not in the database
      VLOG(notifications) << "Receive from database " << m->message_id << " with removed "
                          << m->removed_notification_id;
      continue;
    }

    if (is_from_mention_notification_group(d, m) != from_mentions) {
      VLOG(notifications) << "Receive from database " << m->message_id << " with " << m->notification_id
                          << " from another category";
      continue;
    }

    if (!is_message_notification_active(d, m)) {
      CHECK(from_mentions);
      CHECK(!m->contains_unread_mention);
      CHECK(m->message_id != d->pinned_message_notification_message_id);
      // skip read mentions
      continue;
    }

    if (is_correct) {
      // skip mention messages returned among unread messages
      CHECK(m->date > 0);
      res.emplace_back(m->notification_id, m->date, m->disable_notification,
                       create_new_message_notification(m->message_id));
    } else {
      remove_message_notification_id(d, m, true, false);
      on_message_changed(d, m, false, "on_get_message_notifications_from_database");
    }
  }
  if (!res.empty() || !from_notification_id.is_valid() || static_cast<size_t>(limit) > messages.size()) {
    if (from_mentions) {
      try_add_pinned_message_notification(d, res, initial_from_notification_id, limit);
    }

    std::reverse(res.begin(), res.end());
    return promise.set_value(std::move(res));
  }

  // try again from adjusted from_notification_id and from_message_id
  do_get_message_notifications_from_database(d, from_mentions, initial_from_notification_id, from_notification_id,
                                             from_message_id, limit, std::move(promise));
}

class ReportProfilePhotoQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  FileId file_id_;
  string file_reference_;
  ReportReason report_reason_;

 public:
  explicit ReportProfilePhotoQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, FileId file_id, tl_object_ptr<telegram_api::InputPhoto> &&input_photo,
            ReportReason &&report_reason) {
    dialog_id_ = dialog_id;
    file_id_ = file_id;
    file_reference_ = FileManager::extract_file_reference(input_photo);
    report_reason_ = std::move(report_reason);

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    send_query(G()->net_query_creator().create(telegram_api::account_reportProfilePhoto(
        std::move(input_peer), std::move(input_photo), report_reason_.get_input_report_reason(),
        report_reason_.get_message())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_reportProfilePhoto>(packet);
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
    LOG(INFO) << "Receive error for report chat photo: " << status;
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      VLOG(file_references) << "Receive " << status << " for " << file_id_;
      td_->file_manager_->delete_file_reference(file_id_, file_reference_);
      td_->file_reference_manager_->repair_file_reference(
          file_id_,
          PromiseCreator::lambda([dialog_id = dialog_id_, file_id = file_id_, report_reason = std::move(report_reason_),
                                  promise = std::move(promise_)](Result<Unit> result) mutable {
            if (result.is_error()) {
              LOG(INFO) << "Reported photo " << file_id << " is likely to be deleted";
              return promise.set_value(Unit());
            }
            send_closure(G()->messages_manager(), &MessagesManager::report_dialog_photo, dialog_id, file_id,
                         std::move(report_reason), std::move(promise));
          }));
      return;
    }

    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ReportProfilePhotoQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::report_dialog_photo(DialogId dialog_id, FileId file_id, ReportReason &&reason,
                                          Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "report_dialog_photo");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  if (!can_report_dialog(dialog_id)) {
    return promise.set_error(Status::Error(400, "Chat photo can't be reported"));
  }

  auto file_view = td_->file_manager_->get_file_view(file_id);
  if (file_view.empty()) {
    return promise.set_error(Status::Error(400, "Unknown file ID"));
  }
  if (file_view.get_type() != FileType::Photo || !file_view.has_remote_location() ||
      !file_view.remote_location().is_photo()) {
    return promise.set_error(Status::Error(400, "Only full chat photos can be reported"));
  }

  td_->create_handler<ReportProfilePhotoQuery>(std::move(promise))
      ->send(dialog_id, file_id, file_view.remote_location().as_input_photo(), std::move(reason));
}

void MessagesManager::delete_dialog_filter(DialogFilterId dialog_filter_id, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  LOG(INFO) << "Delete " << dialog_filter_id << " from " << source;
  for (auto it = dialog_filters_.begin(); it != dialog_filters_.end(); ++it) {
    if ((*it)->dialog_filter_id == dialog_filter_id) {
      auto dialog_list_id = DialogListId(dialog_filter_id);
      auto *list = get_dialog_list(dialog_list_id);
      CHECK(list != nullptr);
      auto folder_ids = get_dialog_list_folder_ids(*list);
      CHECK(!folder_ids.empty());

      for (auto folder_id : folder_ids) {
        auto *folder = get_dialog_folder(folder_id);
        CHECK(folder != nullptr);
        for (const auto &dialog_date : folder->ordered_dialogs_) {
          if (dialog_date.get_order() == DEFAULT_ORDER) {
            break;
          }

          auto dialog_id = dialog_date.get_dialog_id();
          Dialog *d = get_dialog(dialog_id);
          CHECK(d != nullptr);

          const DialogPositionInList old_position = get_dialog_position_in_list(list, d);

          if (is_dialog_in_list(d, dialog_list_id)) {
            remove_dialog_from_list(d, dialog_list_id);

            if (old_position.public_order != 0) {
              send_update_chat_position(dialog_list_id, d, source);
            }
          }
        }
      }

      if (G()->parameters().use_message_db) {
        postponed_unread_message_count_updates_.erase(dialog_list_id);
        postponed_unread_chat_count_updates_.erase(dialog_list_id);

        if (list->is_message_unread_count_inited_) {
          list->unread_message_total_count_ = 0;
          list->unread_message_muted_count_ = 0;
          send_update_unread_message_count(*list, DialogId(), true, source, true);
          G()->td_db()->get_binlog_pmc()->erase(PSTRING() << "unread_message_count" << dialog_list_id.get());
        }
        if (list->is_dialog_unread_count_inited_) {
          list->unread_dialog_total_count_ = 0;
          list->unread_dialog_muted_count_ = 0;
          list->unread_dialog_marked_count_ = 0;
          list->unread_dialog_muted_marked_count_ = 0;
          list->in_memory_dialog_total_count_ = 0;
          list->server_dialog_total_count_ = 0;
          list->secret_chat_total_count_ = 0;
          send_update_unread_chat_count(*list, DialogId(), true, source, true);
          G()->td_db()->get_binlog_pmc()->erase(PSTRING() << "unread_dialog_count" << dialog_list_id.get());
        }
      }

      auto promises = std::move(list->load_list_queries_);
      for (auto &promise : promises) {
        promise.set_error(Status::Error(400, "Chat list not found"));
      }

      dialog_lists_.erase(dialog_list_id);
      dialog_filters_.erase(it);
      return;
    }
  }
  UNREACHABLE();
}

void MessagesManager::remove_dialog_mention_notifications(Dialog *d) {
  auto notification_group_id = d->mention_notification_group.group_id;
  if (!notification_group_id.is_valid()) {
    return;
  }
  if (d->unread_mention_count == 0) {
    return;
  }
  CHECK(!d->being_added_message_id.is_valid());

  VLOG(notifications) << "Remove mention notifications in " << d->dialog_id;

  vector<MessageId> message_ids;
  std::unordered_set<NotificationId, NotificationIdHash> removed_notification_ids_set;
  find_messages(d->messages.get(), message_ids, [](const Message *m) { return m->contains_unread_mention; });
  VLOG(notifications) << "Found unread mentions in " << message_ids;
  for (auto &message_id : message_ids) {
    auto m = get_message(d, message_id);
    CHECK(m != nullptr);
    CHECK(m->message_id.is_valid());
    if (m->notification_id.is_valid() && is_message_notification_active(d, m) &&
        is_from_mention_notification_group(d, m)) {
      removed_notification_ids_set.insert(m->notification_id);
    }
  }

  message_ids = td_->notification_manager_->get_notification_group_message_ids(notification_group_id);
  VLOG(notifications) << "Found active mention notifications in " << message_ids;
  for (auto &message_id : message_ids) {
    CHECK(!message_id.is_scheduled());
    if (message_id != d->pinned_message_notification_message_id) {
      auto m = get_message_force(d, message_id, "remove_dialog_mention_notifications");
      if (m != nullptr && m->notification_id.is_valid() && is_message_notification_active(d, m)) {
        CHECK(is_from_mention_notification_group(d, m));
        removed_notification_ids_set.insert(m->notification_id);
      }
    }
  }

  vector<NotificationId> removed_notification_ids(removed_notification_ids_set.begin(),
                                                  removed_notification_ids_set.end());
  for (size_t i = 0; i < removed_notification_ids.size(); i++) {
    send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification, notification_group_id,
                       removed_notification_ids[i], false, i + 1 == removed_notification_ids.size(), Promise<Unit>(),
                       "remove_dialog_mention_notifications");
  }
}

void MessagesManager::on_get_message_viewers(DialogId dialog_id, vector<UserId> user_ids, bool is_recursive,
                                             Promise<td_api::object_ptr<td_api::users>> &&promise) {
  if (!is_recursive) {
    bool need_participant_list = false;
    for (auto user_id : user_ids) {
      if (!user_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << user_id << " as viewer of a message in " << dialog_id;
        continue;
      }
      if (!td_->contacts_manager_->have_user_force(user_id)) {
        need_participant_list = true;
      }
    }
    if (need_participant_list) {
      auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, user_ids = std::move(user_ids),
                                                   promise = std::move(promise)](Unit result) mutable {
        send_closure(actor_id, &MessagesManager::on_get_message_viewers, dialog_id, std::move(user_ids), true,
                     std::move(promise));
      });

      switch (dialog_id.get_type()) {
        case DialogType::Chat:
          return td_->contacts_manager_->reload_chat_full(dialog_id.get_chat_id(), std::move(query_promise));
        case DialogType::Channel:
          return td_->contacts_manager_->get_channel_participants(
              dialog_id.get_channel_id(), td_api::make_object<td_api::supergroupMembersFilterRecent>(), string(), 0,
              200, 200, PromiseCreator::lambda([query_promise = std::move(query_promise)](DialogParticipants) mutable {
                query_promise.set_value(Unit());
              }));
        default:
          UNREACHABLE();
          return;
      }
    }
  }
  promise.set_value(td_->contacts_manager_->get_users_object(-1, user_ids));
}

void MessagesManager::send_update_chat_read_inbox(const Dialog *d, bool force, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_read_inbox from "
                                        << source;
  on_dialog_updated(d->dialog_id, source);
  if (!force && (running_get_difference_ || running_get_channel_difference(d->dialog_id) ||
                 get_channel_difference_to_log_event_id_.count(d->dialog_id) != 0)) {
    LOG(INFO) << "Postpone updateChatReadInbox in " << d->dialog_id << "(" << get_dialog_title(d->dialog_id) << ") to "
              << d->server_unread_count << " + " << d->local_unread_count << " from " << source;
    postponed_chat_read_inbox_updates_.insert(d->dialog_id);
  } else {
    postponed_chat_read_inbox_updates_.erase(d->dialog_id);
    LOG(INFO) << "Send updateChatReadInbox in " << d->dialog_id << "(" << get_dialog_title(d->dialog_id) << ") to "
              << d->server_unread_count << " + " << d->local_unread_count << " from " << source;
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateChatReadInbox>(d->dialog_id.get(), d->last_read_inbox_message_id.get(),
                                                             d->server_unread_count + d->local_unread_count));
  }
}

bool MessagesManager::can_unload_message(const Dialog *d, const Message *m) const {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());
  // don't want to unload messages from opened dialogs
  // don't want to unload messages to which there are replies in yet unsent messages
  // don't want to unload message with active reply markup
  // don't want to unload the newest pinned message
  // don't want to unload last edited message, because server can send updateEditChannelMessage again
  // can't unload from memory last dialog, last database messages, yet unsent messages, being edited media messages and active live locations
  // can't unload messages in dialog with active suffix load query
  FullMessageId full_message_id{d->dialog_id, m->message_id};
  return !d->is_opened && m->message_id != d->last_message_id && m->message_id != d->last_database_message_id &&
         !m->message_id.is_yet_unsent() && active_live_location_full_message_ids_.count(full_message_id) == 0 &&
         replied_by_yet_unsent_messages_.count(full_message_id) == 0 && m->edited_content == nullptr &&
         d->suffix_load_queries_.empty() && m->message_id != d->reply_markup_message_id &&
         m->message_id != d->last_pinned_message_id && m->message_id != d->last_edited_message_id;
}

void MessagesManager::preload_folder_dialog_list(FolderId folder_id) {
  if (G()->close_flag()) {
    LOG(INFO) << "Skip chat list preload in " << folder_id << " because of closing";
    return;
  }
  CHECK(!td_->auth_manager_->is_bot());

  auto &folder = *get_dialog_folder(folder_id);
  CHECK(G()->parameters().use_message_db);
  if (folder.load_folder_dialog_list_multipromise_.promise_count() != 0) {
    LOG(INFO) << "Skip chat list preload in " << folder_id << ", because there is a pending load chat list request";
    return;
  }

  if (folder.last_loaded_database_dialog_date_ < folder.last_database_server_dialog_date_) {
    // if there are some dialogs in database, preload some of them
    load_folder_dialog_list(folder_id, 20, true);
  } else if (folder.folder_last_dialog_date_ != MAX_DIALOG_DATE) {
    // otherwise load more dialogs from the server
    load_folder_dialog_list(folder_id, MAX_GET_DIALOGS, false);
  } else {
    recalc_unread_count(DialogListId(folder_id), -1, false);
  }
}

void MessagesManager::set_dialog_has_bots(Dialog *d, bool has_bots) {
  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in set_dialog_has_bots";

  LOG(INFO) << "Set " << d->dialog_id << " has_bots to " << has_bots;

  auto old_skip_bot_commands = need_skip_bot_commands(d->dialog_id, nullptr);
  d->has_bots = has_bots;
  d->is_has_bots_inited = true;
  auto new_skip_bot_commands = need_skip_bot_commands(d->dialog_id, nullptr);
  if (old_skip_bot_commands != new_skip_bot_commands) {
    auto it = dialog_bot_command_message_ids_.find(d->dialog_id);
    if (it != dialog_bot_command_message_ids_.end()) {
      for (auto message_id : it->second.message_ids) {
        auto m = get_message(d, message_id);
        LOG_CHECK(m != nullptr) << d->dialog_id << ' ' << message_id;
        send_update_message_content_impl(d->dialog_id, m, "set_dialog_has_bots");
      }
    }
  }
}

void MessagesManager::update_dialog_mention_notification_count(const Dialog *d) {
  CHECK(d != nullptr);
  if (td_->auth_manager_->is_bot() || !d->mention_notification_group.group_id.is_valid()) {
    return;
  }
  auto total_count =
      get_dialog_pending_notification_count(d, true) - static_cast<int32>(d->pending_new_mention_notifications.size());
  if (total_count < 0) {
    LOG(ERROR) << "Total mention notification count is " << total_count << " in " << d->dialog_id << " with "
               << d->pending_new_mention_notifications << " pending new mention notifications";
    total_count = 0;
  }
  send_closure_later(G()->notification_manager(), &NotificationManager::set_notification_total_count,
                     d->mention_notification_group.group_id, total_count);
}

void MessagesManager::on_message_ttl_expired(Dialog *d, Message *m) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->ttl > 0);
  CHECK(d->dialog_id.get_type() != DialogType::SecretChat);
  ttl_unregister_message(d->dialog_id, m, "on_message_ttl_expired");
  unregister_message_content(td_, m->content.get(), {d->dialog_id, m->message_id}, "on_message_ttl_expired");
  remove_message_file_sources(d->dialog_id, m);
  on_message_ttl_expired_impl(d, m);
  register_message_content(td_, m->content.get(), {d->dialog_id, m->message_id}, "on_message_ttl_expired");
  send_update_message_content(d, m, true, "on_message_ttl_expired");
  // the caller must call on_message_changed
}

void MessagesManager::send_update_chat_pending_join_requests(const Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_pending_join_requests";
  on_dialog_updated(d->dialog_id, "send_update_chat_pending_join_requests");
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateChatPendingJoinRequests>(d->dialog_id.get(),
                                                                          get_chat_join_requests_info_object(d)));
}

void MessagesManager::save_sponsored_dialog() {
  if (!G()->parameters().use_message_db) {
    return;
  }

  LOG(INFO) << "Save sponsored " << sponsored_dialog_id_ << " with source " << sponsored_dialog_source_;
  if (sponsored_dialog_id_.is_valid()) {
    G()->td_db()->get_binlog_pmc()->set(
        "sponsored_dialog_id",
        PSTRING() << sponsored_dialog_id_.get() << ' ' << sponsored_dialog_source_.DialogSource::serialize());
  } else {
    G()->td_db()->get_binlog_pmc()->erase("sponsored_dialog_id");
  }
}

void MessagesManager::send_update_message_interaction_info(DialogId dialog_id, const Message *m) const {
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || !m->is_update_sent) {
    return;
  }

  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateMessageInteractionInfo>(dialog_id.get(), m->message_id.get(),
                                                                    get_message_interaction_info_object(dialog_id, m)));
}

void MessagesManager::update_used_hashtags(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || (!m->is_outgoing && dialog_id != get_my_dialog_id()) ||
      m->via_bot_user_id.is_valid() || m->hide_via_bot || m->forward_info != nullptr || m->had_forward_info) {
    return;
  }

  ::td::update_used_hashtags(td_, m->content.get());
}

void MessagesManager::set_dialog_unread_mention_count(Dialog *d, int32 unread_mention_count) {
  CHECK(d->unread_mention_count != unread_mention_count);
  CHECK(unread_mention_count >= 0);

  d->unread_mention_count = unread_mention_count;
  d->message_count_by_index[message_search_filter_index(MessageSearchFilter::UnreadMention)] = unread_mention_count;
}

void MessagesManager::send_update_chat_filters() {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  is_update_chat_filters_sent_ = true;
  send_closure(G()->td(), &Td::send_update, get_update_chat_filters_object());
}

bool MessagesManager::have_dialog_force(DialogId dialog_id, const char *source) {
  return loaded_dialogs_.count(dialog_id) > 0 || get_dialog_force(dialog_id, source) != nullptr;
}
}  // namespace td
