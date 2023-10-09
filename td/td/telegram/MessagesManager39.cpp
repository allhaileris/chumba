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

#include <limits>


#include <unordered_map>



namespace td {
}  // namespace td
namespace td {

void MessagesManager::on_get_channel_dialog(DialogId dialog_id, MessageId last_message_id,
                                            MessageId read_inbox_max_message_id, int32 server_unread_count,
                                            int32 unread_mention_count, MessageId read_outbox_max_message_id,
                                            vector<tl_object_ptr<telegram_api::Message>> &&messages) {
  std::unordered_map<FullMessageId, tl_object_ptr<telegram_api::Message>, FullMessageIdHash> full_message_id_to_message;
  for (auto &message : messages) {
    auto message_id = get_message_id(message, false);
    auto message_dialog_id = get_message_dialog_id(message);
    if (!message_dialog_id.is_valid()) {
      message_dialog_id = dialog_id;
    }
    auto full_message_id = FullMessageId(message_dialog_id, message_id);
    full_message_id_to_message[full_message_id] = std::move(message);
  }

  FullMessageId last_full_message_id(dialog_id, last_message_id);
  if (last_message_id.is_valid()) {
    if (full_message_id_to_message.count(last_full_message_id) == 0) {
      LOG(ERROR) << "Last " << last_message_id << " in " << dialog_id << " not found. Have:";
      for (auto &message : full_message_id_to_message) {
        LOG(ERROR) << to_string(message.second);
      }
      return;
    }
  }
  CHECK(!last_message_id.is_scheduled());

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  // TODO gaps support
  // There are many ways of handling a gap in a channel:
  // 1) Delete all known messages in the chat, begin from scratch. It is easy to implement, but suddenly disappearing
  //    messages looks awful for the user.
  // 2) Save all messages loaded in the memory until application restart, but delete all messages from the database.
  //    Messages left in the memory must be lazily updated using calls to getHistory. It looks much smoothly for the
  //    user, he will need to redownload messages only after client restart. Unsynchronized messages left in the
  //    memory shouldn't be saved to database, results of getHistory and getMessage must be used to update state of
  //    deleted and edited messages left in the memory.
  // 3) Save all messages loaded in the memory and stored in the database without saving that some messages form
  //    continuous ranges. Messages in the database will be excluded from results of getChatHistory and
  //    searchChatMessages after application restart and will be available only through getMessage.
  //    Every message should still be checked using getHistory. It has more disadvantages over 2) than advantages.
  // 4) Save all messages with saving all data about continuous message ranges. Messages from the database may be used
  //    as results of getChatHistory and (if implemented continuous ranges support for searching shared media)
  //    searchChatMessages. The messages should still be lazily checked using getHistory, but they are still available
  //    offline. It is the best way for gaps support, but it is pretty hard to implement correctly.
  // It should be also noted that some messages like outgoing live location messages shouldn't be deleted.

  if (last_message_id > d->last_new_message_id) {
    // TODO properly support last_message_id <= d->last_new_message_id
    set_dialog_first_database_message_id(d, MessageId(), "on_get_channel_dialog 6");
    set_dialog_last_database_message_id(d, MessageId(), "on_get_channel_dialog 7");
    d->have_full_history = false;
    d->is_empty = false;
  }
  invalidate_message_indexes(d);

  on_dialog_updated(dialog_id, "on_get_channel_dialog 10");

  // TODO properly support last_message_id <= d->last_new_message_id
  if (last_message_id > d->last_new_message_id) {  // if last message is really a new message
    if (!d->last_new_message_id.is_valid() && last_message_id <= d->max_added_message_id) {
      auto prev_message_id = MessageId(ServerMessageId(last_message_id.get_server_message_id().get() - 1));
      remove_dialog_newer_messages(d, prev_message_id, "on_get_channel_dialog 15");
    }
    d->last_new_message_id = MessageId();
    set_dialog_last_message_id(d, MessageId(), "on_get_channel_dialog 20");
    send_update_chat_last_message(d, "on_get_channel_dialog 30");
    auto added_full_message_id = on_get_message(std::move(full_message_id_to_message[last_full_message_id]), true, true,
                                                false, true, true, "channel difference too long");
    if (added_full_message_id.get_message_id().is_valid()) {
      if (added_full_message_id.get_message_id() == d->last_new_message_id) {
        CHECK(last_full_message_id == added_full_message_id);
        CHECK(d->last_message_id == d->last_new_message_id);
      } else {
        LOG(ERROR) << added_full_message_id << " doesn't became last new message, which is " << d->last_new_message_id;
        dump_debug_message_op(d, 2);
      }
    } else if (last_message_id > d->last_new_message_id) {
      set_dialog_last_new_message_id(d, last_message_id,
                                     "on_get_channel_dialog 40");  // skip updates about some messages
    }
  }

  if (d->last_read_inbox_message_id.is_valid() && !d->last_read_inbox_message_id.is_server() &&
      read_inbox_max_message_id == d->last_read_inbox_message_id.get_prev_server_message_id()) {
    read_inbox_max_message_id = d->last_read_inbox_message_id;
  }
  if (d->server_unread_count != server_unread_count || d->last_read_inbox_message_id != read_inbox_max_message_id) {
    set_dialog_last_read_inbox_message_id(d, read_inbox_max_message_id, server_unread_count, d->local_unread_count,
                                          false, "on_get_channel_dialog 50");
  }
  if (d->unread_mention_count != unread_mention_count) {
    set_dialog_unread_mention_count(d, unread_mention_count);
    update_dialog_mention_notification_count(d);
    send_update_chat_unread_mention_count(d);
  }

  if (d->last_read_outbox_message_id != read_outbox_max_message_id) {
    set_dialog_last_read_outbox_message_id(d, read_outbox_max_message_id);
  }
}

class GetNotifySettingsExceptionsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetNotifySettingsExceptionsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(NotificationSettingsScope scope, bool filter_scope, bool compare_sound) {
    int32 flags = 0;
    tl_object_ptr<telegram_api::InputNotifyPeer> input_notify_peer;
    if (filter_scope) {
      flags |= telegram_api::account_getNotifyExceptions::PEER_MASK;
      input_notify_peer = get_input_notify_peer(scope);
    }
    if (compare_sound) {
      flags |= telegram_api::account_getNotifyExceptions::COMPARE_SOUND_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::account_getNotifyExceptions(flags, false /* ignored */, std::move(input_notify_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_getNotifyExceptions>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto updates_ptr = result_ptr.move_as_ok();
    auto dialog_ids = UpdatesManager::get_update_notify_settings_dialog_ids(updates_ptr.get());
    vector<tl_object_ptr<telegram_api::User>> users;
    vector<tl_object_ptr<telegram_api::Chat>> chats;
    switch (updates_ptr->get_id()) {
      case telegram_api::updatesCombined::ID: {
        auto updates = static_cast<telegram_api::updatesCombined *>(updates_ptr.get());
        users = std::move(updates->users_);
        chats = std::move(updates->chats_);
        reset_to_empty(updates->users_);
        reset_to_empty(updates->chats_);
        break;
      }
      case telegram_api::updates::ID: {
        auto updates = static_cast<telegram_api::updates *>(updates_ptr.get());
        users = std::move(updates->users_);
        chats = std::move(updates->chats_);
        reset_to_empty(updates->users_);
        reset_to_empty(updates->chats_);
        break;
      }
    }
    td_->contacts_manager_->on_get_users(std::move(users), "GetNotifySettingsExceptionsQuery");
    td_->contacts_manager_->on_get_chats(std::move(chats), "GetNotifySettingsExceptionsQuery");
    for (auto &dialog_id : dialog_ids) {
      td_->messages_manager_->force_create_dialog(dialog_id, "GetNotifySettingsExceptionsQuery");
    }
    td_->updates_manager_->on_get_updates(std::move(updates_ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

vector<DialogId> MessagesManager::get_dialog_notification_settings_exceptions(NotificationSettingsScope scope,
                                                                              bool filter_scope, bool compare_sound,
                                                                              bool force, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  bool have_all_dialogs = true;
  for (const auto &list : dialog_folders_) {
    if (list.second.folder_last_dialog_date_ != MAX_DIALOG_DATE) {
      have_all_dialogs = false;
    }
  }

  if (have_all_dialogs || force) {
    vector<DialogDate> ordered_dialogs;
    auto my_dialog_id = get_my_dialog_id();
    for (const auto &list : dialog_folders_) {
      for (const auto &dialog_date : list.second.ordered_dialogs_) {
        auto dialog_id = dialog_date.get_dialog_id();
        if (filter_scope && get_dialog_notification_setting_scope(dialog_id) != scope) {
          continue;
        }
        if (dialog_id == my_dialog_id) {
          continue;
        }

        const Dialog *d = get_dialog(dialog_id);
        CHECK(d != nullptr);
        LOG_CHECK(d->folder_id == list.first)
            << list.first << ' ' << dialog_id << ' ' << d->folder_id << ' ' << d->order;
        if (d->order == DEFAULT_ORDER) {
          break;
        }
        if (are_default_dialog_notification_settings(d->notification_settings, compare_sound)) {
          continue;
        }
        if (is_dialog_message_notification_disabled(dialog_id, std::numeric_limits<int32>::max())) {
          continue;
        }
        ordered_dialogs.push_back(DialogDate(get_dialog_base_order(d), dialog_id));
      }
    }
    std::sort(ordered_dialogs.begin(), ordered_dialogs.end());

    vector<DialogId> result;
    for (auto &dialog_date : ordered_dialogs) {
      CHECK(result.empty() || result.back() != dialog_date.get_dialog_id());
      result.push_back(dialog_date.get_dialog_id());
    }
    promise.set_value(Unit());
    return result;
  }

  for (const auto &folder : dialog_folders_) {
    load_folder_dialog_list(folder.first, MAX_GET_DIALOGS, true);
  }

  td_->create_handler<GetNotifySettingsExceptionsQuery>(std::move(promise))->send(scope, filter_scope, compare_sound);
  return {};
}

class GetSearchCountersQuery final : public Td::ResultHandler {
  Promise<int32> promise_;
  DialogId dialog_id_;
  MessageSearchFilter filter_;

 public:
  explicit GetSearchCountersQuery(Promise<int32> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageSearchFilter filter) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Can't access the chat"));
    }

    dialog_id_ = dialog_id;
    filter_ = filter;

    CHECK(filter != MessageSearchFilter::Empty);
    CHECK(filter != MessageSearchFilter::UnreadMention);
    CHECK(filter != MessageSearchFilter::FailedToSend);
    vector<telegram_api::object_ptr<telegram_api::MessagesFilter>> filters;
    filters.push_back(get_input_messages_filter(filter));
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getSearchCounters(std::move(input_peer), std::move(filters))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getSearchCounters>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    if (result.size() != 1 || result[0]->filter_->get_id() != get_input_messages_filter(filter_)->get_id()) {
      LOG(ERROR) << "Receive unexpected response for get message count in " << dialog_id_ << " with filter " << filter_
                 << ": " << to_string(result);
      return on_error(Status::Error(500, "Receive wrong response"));
    }

    td_->messages_manager_->on_get_dialog_message_count(dialog_id_, filter_, result[0]->count_, std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetSearchCountersQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_dialog_message_count(DialogId dialog_id, MessageSearchFilter filter, bool return_local,
                                               Promise<int32> &&promise) {
  LOG(INFO) << "Get " << (return_local ? "local " : "") << "number of messages in " << dialog_id << " filtered by "
            << filter;

  const Dialog *d = get_dialog_force(dialog_id, "get_dialog_message_count");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (filter == MessageSearchFilter::Empty) {
    return promise.set_error(Status::Error(400, "Can't use searchMessagesFilterEmpty"));
  }

  auto dialog_type = dialog_id.get_type();
  int32 message_count = d->message_count_by_index[message_search_filter_index(filter)];
  if (message_count == -1 && filter == MessageSearchFilter::UnreadMention) {
    message_count = d->unread_mention_count;
  }
  if (message_count != -1 || return_local || dialog_type == DialogType::SecretChat ||
      filter == MessageSearchFilter::FailedToSend) {
    return promise.set_value(std::move(message_count));
  }

  LOG(INFO) << "Get number of messages in " << dialog_id << " filtered by " << filter << " from the server";

  switch (dialog_type) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      td_->create_handler<GetSearchCountersQuery>(std::move(promise))->send(dialog_id, filter);
      break;
    case DialogType::None:
    case DialogType::SecretChat:
    default:
      UNREACHABLE();
  }
}

class DeleteParticipantHistoryQuery final : public Td::ResultHandler {
  Promise<AffectedHistory> promise_;
  ChannelId channel_id_;
  DialogId sender_dialog_id_;

 public:
  explicit DeleteParticipantHistoryQuery(Promise<AffectedHistory> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, DialogId sender_dialog_id) {
    channel_id_ = channel_id;
    sender_dialog_id_ = sender_dialog_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      return promise_.set_error(Status::Error(400, "Chat is not accessible"));
    }
    auto input_peer = td_->messages_manager_->get_input_peer(sender_dialog_id, AccessRights::Know);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Message sender is not accessible"));
    }

    send_query(G()->net_query_creator().create(
        telegram_api::channels_deleteParticipantHistory(std::move(input_channel), std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_deleteParticipantHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(AffectedHistory(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    if (sender_dialog_id_.get_type() != DialogType::Channel) {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "DeleteParticipantHistoryQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::delete_all_channel_messages_by_sender_on_server(ChannelId channel_id, DialogId sender_dialog_id,
                                                                      uint64 log_event_id, Promise<Unit> &&promise) {
  if (log_event_id == 0 && G()->parameters().use_chat_info_db) {
    log_event_id = save_delete_all_channel_messages_by_sender_on_server_log_event(channel_id, sender_dialog_id);
  }

  AffectedHistoryQuery query = [td = td_, sender_dialog_id](DialogId dialog_id,
                                                            Promise<AffectedHistory> &&query_promise) {
    td->create_handler<DeleteParticipantHistoryQuery>(std::move(query_promise))
        ->send(dialog_id.get_channel_id(), sender_dialog_id);
  };
  run_affected_history_query_until_complete(DialogId(channel_id), std::move(query),
                                            sender_dialog_id.get_type() != DialogType::User,
                                            get_erase_log_event_promise(log_event_id, std::move(promise)));
}

void MessagesManager::set_dialog_online_member_count(DialogId dialog_id, int32 online_member_count, bool is_from_server,
                                                     const char *source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  Dialog *d = get_dialog(dialog_id);
  if (d == nullptr) {
    return;
  }

  if (online_member_count < 0) {
    LOG(ERROR) << "Receive online_member_count = " << online_member_count << " in " << dialog_id;
    online_member_count = 0;
  }

  switch (dialog_id.get_type()) {
    case DialogType::Chat: {
      auto participant_count = td_->contacts_manager_->get_chat_participant_count(dialog_id.get_chat_id());
      if (online_member_count > participant_count) {
        online_member_count = participant_count;
      }
      break;
    }
    case DialogType::Channel: {
      auto participant_count = td_->contacts_manager_->get_channel_participant_count(dialog_id.get_channel_id());
      if (participant_count != 0 && online_member_count > participant_count) {
        online_member_count = participant_count;
      }
      break;
    }
    default:
      break;
  }

  auto &info = dialog_online_member_counts_[dialog_id];
  LOG(INFO) << "Change number of online members from " << info.online_member_count << " to " << online_member_count
            << " in " << dialog_id << " from " << source;
  bool need_update = d->is_opened && (!info.is_update_sent || info.online_member_count != online_member_count);
  info.online_member_count = online_member_count;
  info.updated_time = Time::now();

  if (need_update) {
    info.is_update_sent = true;
    send_update_chat_online_member_count(dialog_id, online_member_count);
  }
  if (d->is_opened) {
    if (is_from_server) {
      update_dialog_online_member_count_timeout_.set_timeout_in(dialog_id.get(), ONLINE_MEMBER_COUNT_UPDATE_TIME);
    } else {
      update_dialog_online_member_count_timeout_.add_timeout_in(dialog_id.get(), ONLINE_MEMBER_COUNT_UPDATE_TIME);
    }
  }
}

class ReorderPinnedDialogsQuery final : public Td::ResultHandler {
  FolderId folder_id_;
  Promise<Unit> promise_;

 public:
  explicit ReorderPinnedDialogsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FolderId folder_id, const vector<DialogId> &dialog_ids) {
    folder_id_ = folder_id;
    int32 flags = telegram_api::messages_reorderPinnedDialogs::FORCE_MASK;
    send_query(G()->net_query_creator().create(telegram_api::messages_reorderPinnedDialogs(
        flags, true /*ignored*/, folder_id.get(),
        td_->messages_manager_->get_input_dialog_peers(dialog_ids, AccessRights::Read))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_reorderPinnedDialogs>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    if (!result) {
      return on_error(Status::Error(400, "Result is false"));
    }
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for ReorderPinnedDialogsQuery: " << status;
    }
    td_->messages_manager_->on_update_pinned_dialogs(folder_id_);
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::reorder_pinned_dialogs_on_server(FolderId folder_id, const vector<DialogId> &dialog_ids,
                                                       uint64 log_event_id) {
  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_reorder_pinned_dialogs_on_server_log_event(folder_id, dialog_ids);
  }

  td_->create_handler<ReorderPinnedDialogsQuery>(get_erase_log_event_promise(log_event_id))
      ->send(folder_id, dialog_ids);
}

void MessagesManager::start_import_messages(DialogId dialog_id, int64 import_id, vector<FileId> &&attached_file_ids,
                                            Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  TRY_STATUS_PROMISE(promise, can_send_message(dialog_id));

  auto pending_message_import = make_unique<PendingMessageImport>();
  pending_message_import->dialog_id = dialog_id;
  pending_message_import->import_id = import_id;
  pending_message_import->promise = std::move(promise);

  auto &multipromise = pending_message_import->upload_files_multipromise;

  int64 random_id;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || pending_message_imports_.find(random_id) != pending_message_imports_.end());
  pending_message_imports_[random_id] = std::move(pending_message_import);

  multipromise.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), random_id](Result<Unit> result) {
    send_closure_later(actor_id, &MessagesManager::on_imported_message_attachments_uploaded, random_id,
                       std::move(result));
  }));
  auto lock_promise = multipromise.get_promise();

  for (auto attached_file_id : attached_file_ids) {
    upload_imported_message_attachment(dialog_id, import_id, td_->file_manager_->dup_file_id(attached_file_id), false,
                                       multipromise.get_promise());
  }

  lock_promise.set_value(Unit());
}

void MessagesManager::delete_sent_message_on_server(DialogId dialog_id, MessageId message_id) {
  // being sent message was deleted by the user or is in an inaccessible channel
  // don't need to send an update to the user, because the message has already been deleted
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    LOG(INFO) << "Ignore sent " << message_id << " in inaccessible " << dialog_id;
    return;
  }

  LOG(INFO) << "Delete already deleted sent " << message_id << " in " << dialog_id << " from server";
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  if (get_message_force(d, message_id, "delete_sent_message_on_server") != nullptr) {
    delete_messages(dialog_id, {message_id}, true, Auto());
  } else {
    if (message_id.is_valid()) {
      CHECK(message_id.is_server());
      delete_messages_on_server(dialog_id, {message_id}, true, 0, Auto());
    } else {
      CHECK(message_id.is_scheduled_server());
      delete_scheduled_messages_on_server(dialog_id, {message_id}, 0, Auto());
    }

    bool need_update_dialog_pos = false;
    auto m = delete_message(d, message_id, true, &need_update_dialog_pos, "delete_sent_message_on_server");
    CHECK(m == nullptr);
    CHECK(need_update_dialog_pos == false);
  }
}

int32 MessagesManager::calc_new_unread_count(Dialog *d, MessageId max_message_id, MessageType type,
                                             int32 hint_unread_count) const {
  CHECK(!max_message_id.is_scheduled());
  if (d->is_empty) {
    return 0;
  }

  if (!d->last_read_inbox_message_id.is_valid()) {
    return calc_new_unread_count_from_the_end(d, max_message_id, type, hint_unread_count);
  }

  if (!d->last_message_id.is_valid() ||
      (d->last_message_id.get() - max_message_id.get() > max_message_id.get() - d->last_read_inbox_message_id.get())) {
    int32 unread_count = calc_new_unread_count_from_last_unread(d, max_message_id, type);
    return unread_count >= 0 ? unread_count
                             : calc_new_unread_count_from_the_end(d, max_message_id, type, hint_unread_count);
  } else {
    int32 unread_count = calc_new_unread_count_from_the_end(d, max_message_id, type, hint_unread_count);
    return unread_count >= 0 ? unread_count : calc_new_unread_count_from_last_unread(d, max_message_id, type);
  }
}

Result<ServerMessageId> MessagesManager::get_invoice_message_id(FullMessageId full_message_id) {
  auto m = get_message_force(full_message_id, "get_invoice_message_id");
  if (m == nullptr) {
    return Status::Error(400, "Message not found");
  }
  if (m->content->get_type() != MessageContentType::Invoice) {
    return Status::Error(400, "Message has no invoice");
  }
  if (m->message_id.is_scheduled()) {
    return Status::Error(400, "Wrong scheduled message identifier");
  }
  if (!m->message_id.is_server()) {
    return Status::Error(400, "Wrong message identifier");
  }
  if (m->reply_markup == nullptr || m->reply_markup->inline_keyboard.empty() ||
      m->reply_markup->inline_keyboard[0].empty() ||
      m->reply_markup->inline_keyboard[0][0].type != InlineKeyboardButton::Type::Buy) {
    return Status::Error(400, "Message has no Pay button");
  }

  return m->message_id.get_server_message_id();
}

void MessagesManager::on_update_scope_notify_settings(
    NotificationSettingsScope scope, tl_object_ptr<telegram_api::peerNotifySettings> &&peer_notify_settings) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto old_notification_settings = get_scope_notification_settings(scope);
  CHECK(old_notification_settings != nullptr);

  const ScopeNotificationSettings notification_settings = ::td::get_scope_notification_settings(
      std::move(peer_notify_settings), old_notification_settings->disable_pinned_message_notifications,
      old_notification_settings->disable_mention_notifications);
  if (!notification_settings.is_synchronized) {
    return;
  }

  update_scope_notification_settings(scope, old_notification_settings, notification_settings);
}

void MessagesManager::set_dialog_reply_markup(Dialog *d, MessageId message_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(!message_id.is_scheduled());

  if (d->reply_markup_message_id != message_id) {
    on_dialog_updated(d->dialog_id, "set_dialog_reply_markup");
  }

  d->need_restore_reply_markup = false;

  if (d->reply_markup_message_id.is_valid() || message_id.is_valid()) {
    LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in set_dialog_reply_markup";
    d->reply_markup_message_id = message_id;
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateChatReplyMarkup>(d->dialog_id.get(), message_id.get()));
  }
}

void MessagesManager::drop_dialog_last_pinned_message_id(Dialog *d) {
  d->last_pinned_message_id = MessageId();
  d->is_last_pinned_message_id_inited = false;
  on_dialog_updated(d->dialog_id, "drop_dialog_last_pinned_message_id");

  LOG(INFO) << "Drop " << d->dialog_id << " pinned message";

  create_actor<SleepActor>(
      "ReloadDialogFullInfoActor", 1.0,
      PromiseCreator::lambda([actor_id = actor_id(this), dialog_id = d->dialog_id](Result<Unit> result) {
        send_closure(actor_id, &MessagesManager::reload_dialog_info_full, dialog_id);
      }))
      .release();
}

void MessagesManager::update_forward_count(DialogId dialog_id, const Message *m) {
  if (!td_->auth_manager_->is_bot() && m->forward_info != nullptr && m->forward_info->sender_dialog_id.is_valid() &&
      m->forward_info->message_id.is_valid() &&
      (!is_discussion_message(dialog_id, m) || m->forward_info->sender_dialog_id != m->forward_info->from_dialog_id ||
       m->forward_info->message_id != m->forward_info->from_message_id)) {
    update_forward_count(m->forward_info->sender_dialog_id, m->forward_info->message_id, m->date);
  }
}

int32 MessagesManager::load_channel_pts(DialogId dialog_id) const {
  if (G()->ignore_background_updates() || !have_input_peer(dialog_id, AccessRights::Read)) {
    G()->td_db()->get_binlog_pmc()->erase(get_channel_pts_key(dialog_id));  // just in case
    return 0;
  }
  auto pts = to_integer<int32>(G()->td_db()->get_binlog_pmc()->get(get_channel_pts_key(dialog_id)));
  LOG(INFO) << "Load " << dialog_id << " pts = " << pts;
  return pts;
}

void MessagesManager::delete_bot_command_message_id(DialogId dialog_id, MessageId message_id) {
  if (message_id.is_scheduled()) {
    return;
  }
  auto it = dialog_bot_command_message_ids_.find(dialog_id);
  if (it == dialog_bot_command_message_ids_.end()) {
    return;
  }
  if (it->second.message_ids.erase(message_id) && it->second.message_ids.empty()) {
    dialog_bot_command_message_ids_.erase(it);
  }
}

unique_ptr<MessagesManager::Message> *MessagesManager::treap_find_message(unique_ptr<Message> *v,
                                                                          MessageId message_id) {
  return const_cast<unique_ptr<Message> *>(treap_find_message(static_cast<const unique_ptr<Message> *>(v), message_id));
}

Status MessagesManager::can_use_message_send_options(const MessageSendOptions &options,
                                                     const InputMessageContent &content) {
  return can_use_message_send_options(options, content.content, content.ttl);
}

bool MessagesManager::is_dialog_in_list(const Dialog *d, DialogListId dialog_list_id) {
  return td::contains(d->dialog_list_ids, dialog_list_id);
}
}  // namespace td
