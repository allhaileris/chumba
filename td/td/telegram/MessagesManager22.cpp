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



#include <limits>






namespace td {
}  // namespace td
namespace td {

class GetHistoryQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  MessageId from_message_id_;
  MessageId old_last_new_message_id_;
  int32 offset_;
  int32 limit_;
  bool from_the_end_;

 public:
  explicit GetHistoryQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId from_message_id, MessageId old_last_new_message_id, int32 offset,
            int32 limit) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Can't access the chat"));
    }
    CHECK(offset < 0);

    dialog_id_ = dialog_id;
    from_message_id_ = from_message_id;
    old_last_new_message_id_ = old_last_new_message_id;
    offset_ = offset;
    limit_ = limit;
    from_the_end_ = false;
    send_query(G()->net_query_creator().create(telegram_api::messages_getHistory(
        std::move(input_peer), from_message_id.get_server_message_id().get(), 0, offset, limit, 0, 0, 0)));
  }

  void send_get_from_the_end(DialogId dialog_id, MessageId old_last_new_message_id, int32 limit) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Can't access the chat"));
    }

    dialog_id_ = dialog_id;
    old_last_new_message_id_ = old_last_new_message_id;
    offset_ = 0;
    limit_ = limit;
    from_the_end_ = true;
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getHistory(std::move(input_peer), 0, 0, 0, limit, 0, 0, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetHistoryQuery");
    td_->messages_manager_->get_channel_difference_if_needed(
        dialog_id_, std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(), dialog_id = dialog_id_,
                                from_message_id = from_message_id_, old_last_new_message_id = old_last_new_message_id_,
                                offset = offset_, limit = limit_, from_the_end = from_the_end_,
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            // TODO use info.total_count, info.pts
            send_closure(actor_id, &MessagesManager::on_get_history, dialog_id, from_message_id,
                         old_last_new_message_id, offset, limit, from_the_end, std::move(info.messages),
                         std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetHistoryQuery")) {
      LOG(ERROR) << "Receive error for GetHistoryQuery in " << dialog_id_ << ": " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_history_from_the_end_impl(const Dialog *d, bool from_database, bool only_local,
                                                    Promise<Unit> &&promise) {
  CHECK(d != nullptr);
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto dialog_id = d->dialog_id;
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // can't get history in dialogs without read access
    return promise.set_value(Unit());
  }
  if (!d->first_database_message_id.is_valid() && !d->have_full_history) {
    from_database = false;
  }
  int32 limit = MAX_GET_HISTORY;
  if (from_database && G()->parameters().use_message_db) {
    if (!promise) {
      // repair last database message ID
      limit = 10;
    }
    LOG(INFO) << "Get history from the end of " << dialog_id << " from database";
    MessagesDbMessagesQuery db_query;
    db_query.dialog_id = dialog_id;
    db_query.from_message_id = MessageId::max();
    db_query.limit = limit;
    G()->td_db()->get_messages_db_async()->get_messages(
        db_query, PromiseCreator::lambda([dialog_id, old_last_database_message_id = d->last_database_message_id,
                                          only_local, limit, actor_id = actor_id(this), promise = std::move(promise)](
                                             vector<MessagesDbDialogMessage> messages) mutable {
          send_closure(actor_id, &MessagesManager::on_get_history_from_database, dialog_id, MessageId::max(),
                       old_last_database_message_id, 0, limit, true, only_local, std::move(messages),
                       std::move(promise));
        }));
  } else {
    if (only_local || dialog_id.get_type() == DialogType::SecretChat || d->last_message_id.is_valid()) {
      // if last message is known, there are no reasons to get message history from server from the end
      promise.set_value(Unit());
      return;
    }
    if (!promise && !G()->parameters().use_message_db) {
      // repair last message ID
      limit = 10;
    }

    LOG(INFO) << "Get history from the end of " << dialog_id << " from server";
    td_->create_handler<GetHistoryQuery>(std::move(promise))
        ->send_get_from_the_end(dialog_id, d->last_new_message_id, limit);
  }
}

void MessagesManager::get_history_impl(const Dialog *d, MessageId from_message_id, int32 offset, int32 limit,
                                       bool from_database, bool only_local, Promise<Unit> &&promise) {
  CHECK(d != nullptr);
  CHECK(from_message_id.is_valid());
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto dialog_id = d->dialog_id;
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // can't get history in dialogs without read access
    return promise.set_value(Unit());
  }
  if ((!d->first_database_message_id.is_valid() || from_message_id <= d->first_database_message_id) &&
      !d->have_full_history) {
    from_database = false;
  }
  if (from_database && G()->parameters().use_message_db) {
    LOG(INFO) << "Get history in " << dialog_id << " from " << from_message_id << " with offset " << offset
              << " and limit " << limit << " from database";
    MessagesDbMessagesQuery db_query;
    db_query.dialog_id = dialog_id;
    db_query.from_message_id = from_message_id;
    db_query.offset = offset;
    db_query.limit = limit;
    G()->td_db()->get_messages_db_async()->get_messages(
        db_query,
        PromiseCreator::lambda([dialog_id, from_message_id, old_last_database_message_id = d->last_database_message_id,
                                offset, limit, only_local, actor_id = actor_id(this),
                                promise = std::move(promise)](vector<MessagesDbDialogMessage> messages) mutable {
          send_closure(actor_id, &MessagesManager::on_get_history_from_database, dialog_id, from_message_id,
                       old_last_database_message_id, offset, limit, false, only_local, std::move(messages),
                       std::move(promise));
        }));
  } else {
    if (only_local || dialog_id.get_type() == DialogType::SecretChat) {
      return promise.set_value(Unit());
    }

    LOG(INFO) << "Get history in " << dialog_id << " from " << from_message_id << " with offset " << offset
              << " and limit " << limit << " from server";
    td_->create_handler<GetHistoryQuery>(std::move(promise))
        ->send(dialog_id, from_message_id.get_next_server_message_id(), d->last_new_message_id, offset, limit);
  }
}

void MessagesManager::process_pts_update(tl_object_ptr<telegram_api::Update> &&update) {
  switch (update->get_id()) {
    case dummyUpdate::ID:
      LOG(INFO) << "Process dummyUpdate";
      break;
    case telegram_api::updateNewMessage::ID: {
      auto update_new_message = move_tl_object_as<telegram_api::updateNewMessage>(update);
      LOG(INFO) << "Process updateNewMessage";
      on_get_message(std::move(update_new_message->message_), true, false, false, true, true, "updateNewMessage");
      break;
    }
    case updateSentMessage::ID: {
      auto update_sent_message = move_tl_object_as<updateSentMessage>(update);
      LOG(INFO) << "Process updateSentMessage " << update_sent_message->random_id_;
      on_send_message_success(update_sent_message->random_id_, update_sent_message->message_id_,
                              update_sent_message->date_, update_sent_message->ttl_period_, FileId(),
                              "process updateSentMessage");
      break;
    }
    case telegram_api::updateReadMessagesContents::ID: {
      auto read_contents_update = move_tl_object_as<telegram_api::updateReadMessagesContents>(update);
      LOG(INFO) << "Process updateReadMessageContents";
      for (auto &message_id : read_contents_update->messages_) {
        read_message_content_from_updates(MessageId(ServerMessageId(message_id)));
      }
      break;
    }
    case telegram_api::updateEditMessage::ID: {
      auto update_edit_message = move_tl_object_as<telegram_api::updateEditMessage>(update);
      auto full_message_id = on_get_message(std::move(update_edit_message->message_), false, false, false, false, false,
                                            "updateEditMessage");
      LOG(INFO) << "Process updateEditMessage";
      on_message_edited(full_message_id, update_edit_message->pts_);
      break;
    }
    case telegram_api::updateDeleteMessages::ID: {
      auto delete_update = move_tl_object_as<telegram_api::updateDeleteMessages>(update);
      LOG(INFO) << "Process updateDeleteMessages";
      vector<MessageId> message_ids;
      for (auto message : delete_update->messages_) {
        message_ids.push_back(MessageId(ServerMessageId(message)));
      }
      delete_messages_from_updates(message_ids);
      break;
    }
    case telegram_api::updateReadHistoryInbox::ID: {
      auto read_update = move_tl_object_as<telegram_api::updateReadHistoryInbox>(update);
      LOG(INFO) << "Process updateReadHistoryInbox";
      DialogId dialog_id(read_update->peer_);
      FolderId folder_id;
      if ((read_update->flags_ & telegram_api::updateReadHistoryInbox::FOLDER_ID_MASK) != 0) {
        folder_id = FolderId(read_update->folder_id_);
      }
      on_update_dialog_folder_id(dialog_id, folder_id);
      read_history_inbox(dialog_id, MessageId(ServerMessageId(read_update->max_id_)),
                         -1 /*read_update->still_unread_count*/, "updateReadHistoryInbox");
      break;
    }
    case telegram_api::updateReadHistoryOutbox::ID: {
      auto read_update = move_tl_object_as<telegram_api::updateReadHistoryOutbox>(update);
      LOG(INFO) << "Process updateReadHistoryOutbox";
      read_history_outbox(DialogId(read_update->peer_), MessageId(ServerMessageId(read_update->max_id_)));
      break;
    }
    case telegram_api::updatePinnedMessages::ID: {
      auto pinned_messages_update = move_tl_object_as<telegram_api::updatePinnedMessages>(update);
      LOG(INFO) << "Process updatePinnedMessages";
      vector<MessageId> message_ids;
      for (auto message : pinned_messages_update->messages_) {
        message_ids.push_back(MessageId(ServerMessageId(message)));
      }
      update_dialog_pinned_messages_from_updates(DialogId(pinned_messages_update->peer_), message_ids,
                                                 pinned_messages_update->pinned_);
      break;
    }
    default:
      UNREACHABLE();
  }
  CHECK(!td_->updates_manager_->running_get_difference());
}

class GetRecentLocationsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::messages>> promise_;
  DialogId dialog_id_;
  int32 limit_;

 public:
  explicit GetRecentLocationsQuery(Promise<td_api::object_ptr<td_api::messages>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int32 limit) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Have no info about the chat"));
    }

    dialog_id_ = dialog_id;
    limit_ = limit;

    send_query(
        G()->net_query_creator().create(telegram_api::messages_getRecentLocations(std::move(input_peer), limit, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getRecentLocations>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetRecentLocationsQuery");
    td_->messages_manager_->get_channel_difference_if_needed(
        dialog_id_, std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(), dialog_id = dialog_id_, limit = limit_,
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_recent_locations, dialog_id, limit, info.total_count,
                         std::move(info.messages), std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetRecentLocationsQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::search_dialog_recent_location_messages(DialogId dialog_id, int32 limit,
                                                             Promise<td_api::object_ptr<td_api::messages>> &&promise) {
  LOG(INFO) << "Search recent location messages in " << dialog_id << " with limit " << limit;

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }
  if (limit > MAX_SEARCH_MESSAGES) {
    limit = MAX_SEARCH_MESSAGES;
  }

  const Dialog *d = get_dialog_force(dialog_id, "search_dialog_recent_location_messages");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      return td_->create_handler<GetRecentLocationsQuery>(std::move(promise))->send(dialog_id, limit);
    case DialogType::SecretChat:
      return promise.set_value(get_messages_object(0, vector<td_api::object_ptr<td_api::message>>(), true));
    default:
      UNREACHABLE();
      promise.set_error(Status::Error(500, "Search messages is not supported"));
  }
}

unique_ptr<MessagesManager::MessageForwardInfo> MessagesManager::create_message_forward_info(
    DialogId from_dialog_id, DialogId to_dialog_id, const Message *forwarded_message) const {
  auto content_type = forwarded_message->content->get_type();
  if (content_type == MessageContentType::Game || content_type == MessageContentType::Audio) {
    return nullptr;
  }

  auto my_id = td_->contacts_manager_->get_my_id();
  auto message_id = forwarded_message->message_id;

  DialogId saved_from_dialog_id;
  MessageId saved_from_message_id;
  if (to_dialog_id == DialogId(my_id)) {
    saved_from_dialog_id = from_dialog_id;
    saved_from_message_id = message_id;
  }

  if (forwarded_message->forward_info != nullptr) {
    auto forward_info = make_unique<MessageForwardInfo>(*forwarded_message->forward_info);
    forward_info->from_dialog_id = saved_from_dialog_id;
    forward_info->from_message_id = saved_from_message_id;
    return forward_info;
  }

  if (from_dialog_id != DialogId(my_id) || content_type == MessageContentType::Dice) {
    if (forwarded_message->is_channel_post) {
      if (is_broadcast_channel(from_dialog_id)) {
        auto author_signature = forwarded_message->sender_user_id.is_valid()
                                    ? td_->contacts_manager_->get_user_title(forwarded_message->sender_user_id)
                                    : forwarded_message->author_signature;
        return td::make_unique<MessageForwardInfo>(UserId(), forwarded_message->date, from_dialog_id,
                                                   forwarded_message->message_id, std::move(author_signature), "",
                                                   saved_from_dialog_id, saved_from_message_id, "", false);
      } else {
        LOG(ERROR) << "Don't know how to forward a channel post not from a channel";
      }
    } else if (forwarded_message->sender_user_id.is_valid() || forwarded_message->sender_dialog_id.is_valid()) {
      return td::make_unique<MessageForwardInfo>(
          forwarded_message->sender_user_id, forwarded_message->date, forwarded_message->sender_dialog_id, MessageId(),
          "", forwarded_message->author_signature, saved_from_dialog_id, saved_from_message_id, "", false);
    } else {
      LOG(ERROR) << "Don't know how to forward a non-channel post message without forward info and sender";
    }
  }
  return nullptr;
}

void MessagesManager::remove_message_dialog_notifications(Dialog *d, MessageId max_message_id, bool from_mentions,
                                                          const char *source) {
  // removes up to max_message_id
  CHECK(!max_message_id.is_scheduled());
  NotificationGroupInfo &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  if (!group_info.group_id.is_valid()) {
    return;
  }

  VLOG(notifications) << "Remove message dialog notifications in " << group_info.group_id << '/' << d->dialog_id
                      << " up to " << max_message_id << " from " << source;

  if (!d->pending_new_message_notifications.empty()) {
    for (auto &it : d->pending_new_message_notifications) {
      if (it.second <= max_message_id) {
        it.first = DialogId();
      }
    }
    flush_pending_new_message_notifications(d->dialog_id, from_mentions, DialogId(UserId(static_cast<int64>(3))));
  }

  auto max_notification_message_id = max_message_id;
  if (d->last_message_id.is_valid() && max_notification_message_id >= d->last_message_id) {
    max_notification_message_id = d->last_message_id;
    set_dialog_last_notification(d->dialog_id, group_info, 0, NotificationId(),
                                 "remove_message_dialog_notifications 1");
  } else if (max_notification_message_id == MessageId::max()) {
    max_notification_message_id = get_next_local_message_id(d);
    set_dialog_last_notification(d->dialog_id, group_info, 0, NotificationId(),
                                 "remove_message_dialog_notifications 2");
  } else {
    LOG(FATAL) << "TODO support notification deletion up to " << max_message_id << " if will be ever needed";
  }

  send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification_group, group_info.group_id,
                     NotificationId(), max_notification_message_id, 0, true, Promise<Unit>());
}

Result<string> MessagesManager::get_login_button_url(FullMessageId full_message_id, int64 button_id) {
  Dialog *d = get_dialog_force(full_message_id.get_dialog_id(), "get_login_button_url");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  if (!have_input_peer(d->dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }

  auto m = get_message_force(d, full_message_id.get_message_id(), "get_login_button_url");
  if (m == nullptr) {
    return Status::Error(400, "Message not found");
  }
  if (m->reply_markup == nullptr || m->reply_markup->type != ReplyMarkup::Type::InlineKeyboard) {
    return Status::Error(400, "Message has no inline keyboard");
  }
  if (m->message_id.is_scheduled()) {
    return Status::Error(400, "Can't use login buttons from scheduled messages");
  }
  if (!m->message_id.is_server()) {
    // it shouldn't have UrlAuth buttons anyway
    return Status::Error(400, "Message is not server");
  }
  if (d->dialog_id.get_type() == DialogType::SecretChat) {
    // secret chat messages can't have reply markup, so this shouldn't happen now
    return Status::Error(400, "Message is in a secret chat");
  }
  if (button_id < std::numeric_limits<int32>::min() || button_id > std::numeric_limits<int32>::max()) {
    return Status::Error(400, "Invalid button identifier specified");
  }

  for (auto &row : m->reply_markup->inline_keyboard) {
    for (auto &button : row) {
      if (button.type == InlineKeyboardButton::Type::UrlAuth && button.id == button_id) {
        return button.data;
      }
    }
  }

  return Status::Error(400, "Button not found");
}

void MessagesManager::remove_dialog_action_bar(DialogId dialog_id, Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "remove_dialog_action_bar");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  if (dialog_id.get_type() == DialogType::SecretChat) {
    dialog_id = DialogId(td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id()));
    d = get_dialog_force(dialog_id, "remove_dialog_action_bar 2");
    if (d == nullptr) {
      return promise.set_error(Status::Error(400, "Chat with the user not found"));
    }
    if (!have_input_peer(dialog_id, AccessRights::Read)) {
      return promise.set_error(Status::Error(400, "Can't access the chat"));
    }
  }

  if (!d->know_action_bar) {
    return promise.set_error(Status::Error(400, "Can't update chat action bar"));
  }
  if (d->need_repair_action_bar) {
    d->need_repair_action_bar = false;
    on_dialog_updated(dialog_id, "remove_dialog_action_bar");
  }
  if (d->action_bar == nullptr) {
    return promise.set_value(Unit());
  }

  d->action_bar = nullptr;
  send_update_chat_action_bar(d);

  toggle_dialog_report_spam_state_on_server(dialog_id, false, 0, std::move(promise));
}

void MessagesManager::update_message_count_by_index(Dialog *d, int diff, int32 index_mask) {
  if (index_mask == 0) {
    return;
  }

  LOG(INFO) << "Update message count by " << diff << " in index mask " << index_mask;
  int i = 0;
  for (auto &message_count : d->message_count_by_index) {
    if (((index_mask >> i) & 1) != 0 && message_count != -1) {
      message_count += diff;
      if (message_count < 0) {
        if (d->dialog_id.get_type() == DialogType::SecretChat ||
            i == message_search_filter_index(MessageSearchFilter::FailedToSend)) {
          message_count = 0;
        } else {
          message_count = -1;
        }
      }
      on_dialog_updated(d->dialog_id, "update_message_count_by_index");
    }
    i++;
  }

  i = static_cast<int>(MessageSearchFilter::Call) - 1;
  for (auto &message_count : calls_db_state_.message_count_by_index) {
    if (((index_mask >> i) & 1) != 0 && message_count != -1) {
      message_count += diff;
      if (message_count < 0) {
        if (d->dialog_id.get_type() == DialogType::SecretChat) {
          message_count = 0;
        } else {
          message_count = -1;
        }
      }
      save_calls_db_state();
    }
    i++;
  }
}

void MessagesManager::on_send_secret_message_success(int64 random_id, MessageId message_id, int32 date,
                                                     unique_ptr<EncryptedFile> file, Promise<> promise) {
  promise.set_value(Unit());  // TODO: set after message is saved

  FileId new_file_id;
  if (file != nullptr) {
    if (!DcId::is_valid(file->dc_id_)) {
      LOG(ERROR) << "Wrong dc_id = " << file->dc_id_ << " in file " << *file;
    } else {
      DialogId owner_dialog_id;
      auto it = being_sent_messages_.find(random_id);
      if (it != being_sent_messages_.end()) {
        owner_dialog_id = it->second.get_dialog_id();
      }

      new_file_id = td_->file_manager_->register_remote(
          FullRemoteFileLocation(FileType::Encrypted, file->id_, file->access_hash_, DcId::internal(file->dc_id_), ""),
          FileLocationSource::FromServer, owner_dialog_id, 0, file->size_, to_string(static_cast<uint64>(file->id_)));
    }
  }

  on_send_message_success(random_id, message_id, date, 0, new_file_id, "process send_secret_message_success");
}

td_api::object_ptr<td_api::updateUnreadChatCount> MessagesManager::get_update_unread_chat_count_object(
    const DialogList &list) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(list.is_dialog_unread_count_inited_);
  int32 unread_count = list.unread_dialog_total_count_;
  int32 unread_unmuted_count = unread_count - list.unread_dialog_muted_count_;
  int32 unread_marked_count = list.unread_dialog_marked_count_;
  int32 unread_unmuted_marked_count = unread_marked_count - list.unread_dialog_muted_marked_count_;
  CHECK(unread_count >= 0);
  CHECK(unread_unmuted_count >= 0);
  CHECK(unread_marked_count >= 0);
  CHECK(unread_unmuted_marked_count >= 0);
  return td_api::make_object<td_api::updateUnreadChatCount>(
      list.dialog_list_id.get_chat_list_object(), get_dialog_total_count(list), unread_count, unread_unmuted_count,
      unread_marked_count, unread_unmuted_marked_count);
}

void MessagesManager::delete_notification_id_to_message_id_correspondence(Dialog *d, NotificationId notification_id,
                                                                          MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(notification_id.is_valid());
  CHECK(message_id.is_valid());
  auto it = d->notification_id_to_message_id.find(notification_id);
  if (it != d->notification_id_to_message_id.end() && it->second == message_id) {
    VLOG(notifications) << "Delete correspondence from " << notification_id << " to " << message_id << " in "
                        << d->dialog_id;
    d->notification_id_to_message_id.erase(it);
  } else {
    LOG(ERROR) << "Can't find " << notification_id << " in " << d->dialog_id << " with " << message_id;
  }
}

MessageId MessagesManager::get_next_yet_unsent_scheduled_message_id(Dialog *d, int32 date) {
  CHECK(date > 0);

  MessageId message_id(ScheduledServerMessageId(1), date);

  auto it = MessagesConstIterator(d, MessageId(ScheduledServerMessageId(), date + 1, true));
  if (*it != nullptr && (*it)->message_id > message_id) {
    message_id = (*it)->message_id;
  }

  auto &last_assigned_message_id = d->last_assigned_scheduled_message_id[date];
  if (last_assigned_message_id != MessageId() && last_assigned_message_id > message_id) {
    message_id = last_assigned_message_id;
  }

  last_assigned_message_id = message_id.get_next_message_id(MessageType::YetUnsent);
  return last_assigned_message_id;
}

void MessagesManager::ttl_read_history_impl(DialogId dialog_id, bool is_outgoing, MessageId from_message_id,
                                            MessageId till_message_id, double view_date) {
  CHECK(!from_message_id.is_scheduled());
  CHECK(!till_message_id.is_scheduled());

  auto *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  auto now = Time::now();
  for (auto it = MessagesIterator(d, from_message_id); *it && (*it)->message_id >= till_message_id; --it) {
    auto *m = *it;
    if (m->is_outgoing == is_outgoing) {
      ttl_on_view(d, m, view_date, now);
    }
  }
}

bool MessagesManager::is_removed_from_dialog_list(const Dialog *d) const {
  switch (d->dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat:
      return !td_->contacts_manager_->get_chat_is_active(d->dialog_id.get_chat_id());
    case DialogType::Channel:
      return !td_->contacts_manager_->get_channel_status(d->dialog_id.get_channel_id()).is_member();
    case DialogType::SecretChat:
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
      break;
  }
  return false;
}

void MessagesManager::ttl_period_unregister_message(DialogId dialog_id, const Message *m) {
  if (m->ttl_period == 0) {
    return;
  }
  CHECK(!m->message_id.is_scheduled());

  auto it = ttl_nodes_.find(TtlNode(dialog_id, m->message_id, true));

  CHECK(it != ttl_nodes_.end());

  auto *heap_node = it->as_heap_node();
  if (heap_node->in_heap()) {
    ttl_heap_.erase(heap_node);
  }
  ttl_nodes_.erase(it);
  ttl_update_timeout(Time::now());
}

const unique_ptr<MessagesManager::Message> *MessagesManager::treap_find_message(const unique_ptr<Message> *v,
                                                                                MessageId message_id) {
  while (*v != nullptr) {
    if ((*v)->message_id < message_id) {
      v = &(*v)->right;
    } else if ((*v)->message_id > message_id) {
      v = &(*v)->left;
    } else {
      break;
    }
  }
  return v;
}

const DialogFilter *MessagesManager::get_dialog_filter(DialogFilterId dialog_filter_id) const {
  CHECK(!disable_get_dialog_filter_);
  for (const auto &filter : dialog_filters_) {
    if (filter->dialog_filter_id == dialog_filter_id) {
      return filter.get();
    }
  }
  return nullptr;
}

void MessagesManager::after_set_typing_query(DialogId dialog_id, int32 generation) {
  auto it = set_typing_query_.find(dialog_id);
  if (it != set_typing_query_.end() && (!it->second.is_alive() || it->second.generation() == generation)) {
    set_typing_query_.erase(it);
  }
}

bool MessagesManager::is_dialog_sponsored(const Dialog *d) const {
  return d->order == DEFAULT_ORDER && d->dialog_id == sponsored_dialog_id_;
}
}  // namespace td
