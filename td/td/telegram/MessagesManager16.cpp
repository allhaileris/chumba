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
#include "td/telegram/GroupCallManager.h"

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




#include <unordered_map>

#include <utility>

namespace td {
}  // namespace td
namespace td {

class GetMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<tl_object_ptr<telegram_api::InputMessage>> &&message_ids) {
    send_query(G()->net_query_creator().create(telegram_api::messages_getMessages(std::move(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetMessagesQuery");
    LOG_IF(ERROR, info.is_channel_messages) << "Receive channel messages in GetMessagesQuery";
    td_->messages_manager_->on_get_messages(std::move(info.messages), info.is_channel_messages, false,
                                            std::move(promise_), "GetMessagesQuery");
  }

  void on_error(Status status) final {
    if (status.message() == "MESSAGE_IDS_EMPTY") {
      promise_.set_value(Unit());
      return;
    }
    promise_.set_error(std::move(status));
  }
};

class GetChannelMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  MessageId last_new_message_id_;

 public:
  explicit GetChannelMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, tl_object_ptr<telegram_api::InputChannel> &&input_channel,
            vector<tl_object_ptr<telegram_api::InputMessage>> &&message_ids, MessageId last_new_message_id) {
    channel_id_ = channel_id;
    last_new_message_id_ = last_new_message_id;
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::channels_getMessages(std::move(input_channel), std::move(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetChannelMessagesQuery");
    LOG_IF(ERROR, !info.is_channel_messages) << "Receive ordinary messages in GetChannelMessagesQuery";
    // messages with invalid big identifiers can be received as messageEmpty
    // bots can receive messageEmpty because of their privacy mode
    if (last_new_message_id_.is_valid() && !td_->auth_manager_->is_bot()) {
      vector<MessageId> empty_message_ids;
      for (auto &message : info.messages) {
        if (message->get_id() == telegram_api::messageEmpty::ID) {
          auto message_id = MessagesManager::get_message_id(message, false);
          if (message_id.is_valid() && message_id <= last_new_message_id_) {
            empty_message_ids.push_back(message_id);
          }
        }
      }
      td_->messages_manager_->on_get_empty_messages(DialogId(channel_id_), empty_message_ids);
    }
    td_->messages_manager_->get_channel_difference_if_needed(
        DialogId(channel_id_), std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(),
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_messages, std::move(info.messages),
                         info.is_channel_messages, false, std::move(promise), "GetChannelMessagesQuery");
          }
        }));
  }

  void on_error(Status status) final {
    if (status.message() == "MESSAGE_IDS_EMPTY") {
      promise_.set_value(Unit());
      return;
    }
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetChannelMessagesQuery");
    promise_.set_error(std::move(status));
  }
};

class GetScheduledMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit GetScheduledMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, tl_object_ptr<telegram_api::InputPeer> &&input_peer, vector<int32> &&message_ids) {
    dialog_id_ = dialog_id;
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::messages_getScheduledMessages(std::move(input_peer), std::move(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getScheduledMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetScheduledMessagesQuery");
    LOG_IF(ERROR, info.is_channel_messages != (dialog_id_.get_type() == DialogType::Channel))
        << "Receive wrong messages constructor in GetScheduledMessagesQuery";
    td_->messages_manager_->on_get_messages(std::move(info.messages), info.is_channel_messages, true,
                                            std::move(promise_), "GetScheduledMessagesQuery");
  }

  void on_error(Status status) final {
    if (status.message() == "MESSAGE_IDS_EMPTY") {
      promise_.set_value(Unit());
      return;
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetScheduledMessagesQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_messages_from_server(vector<FullMessageId> &&message_ids, Promise<Unit> &&promise,
                                               const char *source,
                                               tl_object_ptr<telegram_api::InputMessage> input_message) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (message_ids.empty()) {
    LOG(ERROR) << "Empty message_ids from " << source;
    return promise.set_error(Status::Error(500, "There are no messages specified to fetch"));
  }

  if (input_message != nullptr) {
    CHECK(message_ids.size() == 1);
  }

  vector<tl_object_ptr<telegram_api::InputMessage>> ordinary_message_ids;
  std::unordered_map<ChannelId, vector<tl_object_ptr<telegram_api::InputMessage>>, ChannelIdHash> channel_message_ids;
  std::unordered_map<DialogId, vector<int32>, DialogIdHash> scheduled_message_ids;
  for (auto &full_message_id : message_ids) {
    auto dialog_id = full_message_id.get_dialog_id();
    auto message_id = full_message_id.get_message_id();
    if (!message_id.is_valid() || !message_id.is_server()) {
      if (message_id.is_valid_scheduled() && message_id.is_scheduled_server()) {
        scheduled_message_ids[dialog_id].push_back(message_id.get_scheduled_server_message_id().get());
      }
      continue;
    }

    if (input_message == nullptr) {
      input_message = make_tl_object<telegram_api::inputMessageID>(message_id.get_server_message_id().get());
    }

    switch (dialog_id.get_type()) {
      case DialogType::User:
      case DialogType::Chat:
        ordinary_message_ids.push_back(std::move(input_message));
        break;
      case DialogType::Channel:
        channel_message_ids[dialog_id.get_channel_id()].push_back(std::move(input_message));
        break;
      case DialogType::SecretChat:
        LOG(ERROR) << "Can't get " << full_message_id << " from server from " << source;
        break;
      case DialogType::None:
      default:
        UNREACHABLE();
        break;
    }
  }

  MultiPromiseActorSafe mpas{"GetMessagesOnServerMultiPromiseActor"};
  mpas.add_promise(std::move(promise));
  auto lock = mpas.get_promise();

  if (!ordinary_message_ids.empty()) {
    td_->create_handler<GetMessagesQuery>(mpas.get_promise())->send(std::move(ordinary_message_ids));
  }

  for (auto &it : scheduled_message_ids) {
    auto dialog_id = it.first;
    have_dialog_force(dialog_id, "get_messages_from_server");
    auto input_peer = get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      LOG(ERROR) << "Can't find info about " << dialog_id << " to get a message from it from " << source;
      mpas.get_promise().set_error(Status::Error(400, "Can't access the chat"));
      continue;
    }
    td_->create_handler<GetScheduledMessagesQuery>(mpas.get_promise())
        ->send(dialog_id, std::move(input_peer), std::move(it.second));
  }

  for (auto &it : channel_message_ids) {
    td_->contacts_manager_->have_channel_force(it.first);
    auto input_channel = td_->contacts_manager_->get_input_channel(it.first);
    if (input_channel == nullptr) {
      LOG(ERROR) << "Can't find info about " << it.first << " to get a message from it from " << source;
      mpas.get_promise().set_error(Status::Error(400, "Can't access the chat"));
      continue;
    }
    const auto *d = get_dialog_force(DialogId(it.first));
    td_->create_handler<GetChannelMessagesQuery>(mpas.get_promise())
        ->send(it.first, std::move(input_channel), std::move(it.second),
               d == nullptr ? MessageId() : d->last_new_message_id);
  }
  lock.set_value(Unit());
}

void MessagesManager::delete_all_dialog_messages(Dialog *d, bool remove_from_dialog_list, bool is_permanently_deleted) {
  CHECK(d != nullptr);
  LOG(INFO) << "Delete all messages in " << d->dialog_id
            << " with remove_from_dialog_list = " << remove_from_dialog_list
            << " and is_permanently_deleted = " << is_permanently_deleted;
  if (is_debug_message_op_enabled()) {
    d->debug_message_op.emplace_back(Dialog::MessageOp::DeleteAll, MessageId(), MessageContentType::None,
                                     remove_from_dialog_list, false, false, "");
  }

  if (d->server_unread_count + d->local_unread_count > 0) {
    MessageId max_message_id =
        d->last_database_message_id.is_valid() ? d->last_database_message_id : d->last_new_message_id;
    if (max_message_id.is_valid()) {
      read_history_inbox(d->dialog_id, max_message_id, -1, "delete_all_dialog_messages 1");
    }
    if (d->server_unread_count != 0 || d->local_unread_count != 0) {
      set_dialog_last_read_inbox_message_id(d, MessageId::min(), 0, 0, true, "delete_all_dialog_messages 2");
    }
  }

  if (d->unread_mention_count > 0) {
    set_dialog_unread_mention_count(d, 0);
    send_update_chat_unread_mention_count(d);
  }

  bool has_last_message_id = d->last_message_id != MessageId();
  int32 last_message_date = 0;
  MessageId last_clear_history_message_id;
  if (!remove_from_dialog_list) {
    if (has_last_message_id) {
      auto m = get_message(d, d->last_message_id);
      CHECK(m != nullptr);
      last_message_date = m->date;
      last_clear_history_message_id = d->last_message_id;
    } else {
      last_message_date = d->last_clear_history_date;
      last_clear_history_message_id = d->last_clear_history_message_id;
    }
  }

  vector<int64> deleted_message_ids;
  do_delete_all_dialog_messages(d, d->messages, is_permanently_deleted, deleted_message_ids);
  delete_all_dialog_messages_from_database(d, MessageId::max(), "delete_all_dialog_messages 3");
  if (is_permanently_deleted) {
    for (auto id : deleted_message_ids) {
      d->deleted_message_ids.insert(MessageId{id});
    }
  }

  if (d->reply_markup_message_id != MessageId()) {
    set_dialog_reply_markup(d, MessageId());
  }

  set_dialog_first_database_message_id(d, MessageId(), "delete_all_dialog_messages 4");
  set_dialog_last_database_message_id(d, MessageId(), "delete_all_dialog_messages 5");
  set_dialog_last_clear_history_date(d, last_message_date, last_clear_history_message_id,
                                     "delete_all_dialog_messages 6");
  d->last_read_all_mentions_message_id = MessageId();                            // it is not needed anymore
  d->message_notification_group.max_removed_notification_id = NotificationId();  // it is not needed anymore
  d->message_notification_group.max_removed_message_id = MessageId();            // it is not needed anymore
  d->mention_notification_group.max_removed_notification_id = NotificationId();  // it is not needed anymore
  d->mention_notification_group.max_removed_message_id = MessageId();            // it is not needed anymore
  std::fill(d->message_count_by_index.begin(), d->message_count_by_index.end(), 0);
  d->notification_id_to_message_id.clear();

  if (has_last_message_id) {
    set_dialog_last_message_id(d, MessageId(), "delete_all_dialog_messages 7");
    send_update_chat_last_message(d, "delete_all_dialog_messages 8");
  }
  if (remove_from_dialog_list) {
    set_dialog_order(d, DEFAULT_ORDER, true, false, "delete_all_dialog_messages 9");
  } else {
    update_dialog_pos(d, "delete_all_dialog_messages 10");
  }

  on_dialog_updated(d->dialog_id, "delete_all_dialog_messages 11");

  send_update_delete_messages(d->dialog_id, std::move(deleted_message_ids), is_permanently_deleted, false);
}

MessagesManager::Message *MessagesManager::on_get_message_from_database(Dialog *d, DialogId dialog_id,
                                                                        MessageId expected_message_id,
                                                                        const BufferSlice &value, bool is_scheduled,
                                                                        const char *source) {
  if (value.empty()) {
    return nullptr;
  }

  auto m = parse_message(dialog_id, expected_message_id, value, is_scheduled);
  if (m == nullptr) {
    return nullptr;
  }

  if (d == nullptr) {
    LOG(ERROR) << "Can't find " << dialog_id << ", but have a message from it from " << source;
    if (!dialog_id.is_valid()) {
      LOG(ERROR) << "Got message in invalid " << dialog_id << " from " << source;
      return nullptr;
    }

    if (m->message_id.is_valid() && m->message_id.is_any_server() &&
        (dialog_id.get_type() == DialogType::User || dialog_id.get_type() == DialogType::Chat)) {
      get_message_from_server({dialog_id, m->message_id}, Auto(), "on_get_message_from_database 1");
    }

    force_create_dialog(dialog_id, source);
    d = get_dialog_force(dialog_id, source);
    CHECK(d != nullptr);
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return nullptr;
  }

  auto old_message = get_message(d, m->message_id);
  if (old_message != nullptr) {
    // data in the database is always outdated, so return a message from the memory
    if (dialog_id.get_type() == DialogType::SecretChat) {
      CHECK(!is_scheduled);
      // just in case restore random_id to message_id corespondence
      // can be needed if there was newer unloaded message with the same random_id
      add_random_id_to_message_id_correspondence(d, old_message->random_id, old_message->message_id);
    }

    if (old_message->notification_id.is_valid() && !is_scheduled) {
      add_notification_id_to_message_id_correspondence(d, old_message->notification_id, old_message->message_id);
    }

    return old_message;
  }

  Dependencies dependencies;
  add_message_dependencies(dependencies, m.get());
  if (!resolve_dependencies_force(td_, dependencies, "on_get_message_from_database") &&
      dialog_id.get_type() != DialogType::SecretChat) {
    get_message_from_server({dialog_id, m->message_id}, Auto(), "on_get_message_from_database 2");
  }

  m->have_previous = false;
  m->have_next = false;
  m->from_database = true;
  bool need_update = false;
  bool need_update_dialog_pos = false;
  auto result = add_message_to_dialog(d, std::move(m), false, &need_update, &need_update_dialog_pos, source);
  if (need_update_dialog_pos) {
    LOG(ERROR) << "Need update dialog pos after load " << (result == nullptr ? MessageId() : result->message_id)
               << " in " << dialog_id << " from " << source;
    send_update_chat_last_message(d, source);
  }
  return result;
}

void MessagesManager::on_update_dialog_group_call(DialogId dialog_id, bool has_active_group_call,
                                                  bool is_group_call_empty, const char *source, bool force) {
  LOG(INFO) << "Update voice chat in " << dialog_id << " with has_active_voice_chat = " << has_active_group_call
            << " and is_voice_chat_empty = " << is_group_call_empty << " from " << source;
  CHECK(dialog_id.is_valid());
  Dialog *d = get_dialog(dialog_id);  // must not create the Dialog, because it is called from on_get_chat
  if (d == nullptr) {
    LOG(INFO) << "Can't find " << dialog_id;
    pending_dialog_group_call_updates_[dialog_id] = {has_active_group_call, is_group_call_empty};
    return;
  }

  if (!has_active_group_call) {
    is_group_call_empty = false;
  }
  if (d->active_group_call_id.is_valid() && has_active_group_call && is_group_call_empty &&
      (td_->group_call_manager_->is_group_call_being_joined(d->active_group_call_id) ||
       td_->group_call_manager_->is_group_call_joined(d->active_group_call_id))) {
    LOG(INFO) << "Fix is_group_call_empty to false";
    is_group_call_empty = false;
  }
  if (d->has_active_group_call == has_active_group_call && d->is_group_call_empty == is_group_call_empty) {
    return;
  }
  if (!force && d->active_group_call_id.is_valid() && has_active_group_call &&
      td_->group_call_manager_->is_group_call_being_joined(d->active_group_call_id)) {
    LOG(INFO) << "Ignore update in a being joined group call";
    return;
  }

  if (d->has_active_group_call && !has_active_group_call && d->active_group_call_id.is_valid()) {
    d->active_group_call_id = InputGroupCallId();
    d->has_active_group_call = false;
    d->is_group_call_empty = false;
    send_update_chat_video_chat(d);
  } else if (d->has_active_group_call && has_active_group_call) {
    d->is_group_call_empty = is_group_call_empty;
    send_update_chat_video_chat(d);
  } else {
    d->has_active_group_call = has_active_group_call;
    d->is_group_call_empty = is_group_call_empty;
    on_dialog_updated(dialog_id, "on_update_dialog_group_call");

    if (has_active_group_call && !d->active_group_call_id.is_valid() && !td_->auth_manager_->is_bot()) {
      repair_dialog_active_group_call_id(dialog_id);
    }
  }
}

void MessagesManager::fix_pending_join_requests(DialogId dialog_id, int32 &pending_join_request_count,
                                                vector<UserId> &pending_join_request_user_ids) const {
  td::remove_if(pending_join_request_user_ids, [](UserId user_id) { return !user_id.is_valid(); });

  bool need_drop_pending_join_requests = [&] {
    if (pending_join_request_count < 0) {
      return true;
    }
    switch (dialog_id.get_type()) {
      case DialogType::User:
      case DialogType::SecretChat:
        return true;
      case DialogType::Chat: {
        auto chat_id = dialog_id.get_chat_id();
        auto status = td_->contacts_manager_->get_chat_status(chat_id);
        if (!status.can_manage_invite_links()) {
          return true;
        }
        break;
      }
      case DialogType::Channel: {
        auto channel_id = dialog_id.get_channel_id();
        auto status = td_->contacts_manager_->get_channel_permissions(channel_id);
        if (!status.can_manage_invite_links()) {
          return true;
        }
        break;
      }
      case DialogType::None:
      default:
        UNREACHABLE();
    }
    return false;
  }();
  if (need_drop_pending_join_requests) {
    pending_join_request_count = 0;
    pending_join_request_user_ids.clear();
  } else if (static_cast<size_t>(pending_join_request_count) < pending_join_request_user_ids.size()) {
    LOG(ERROR) << "Fix pending join request count from " << pending_join_request_count << " to "
               << pending_join_request_user_ids.size();
    pending_join_request_count = narrow_cast<int32>(pending_join_request_user_ids.size());
  }

  static constexpr size_t MAX_PENDING_JOIN_REQUESTS = 3;
  if (pending_join_request_user_ids.size() > MAX_PENDING_JOIN_REQUESTS) {
    pending_join_request_user_ids.resize(MAX_PENDING_JOIN_REQUESTS);
  }
}

void MessagesManager::on_read_channel_inbox(ChannelId channel_id, MessageId max_message_id, int32 server_unread_count,
                                            int32 pts, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(!max_message_id.is_scheduled());
  if (!max_message_id.is_valid() && server_unread_count <= 0) {
    return;
  }

  DialogId dialog_id(channel_id);
  Dialog *d = get_dialog_force(dialog_id, source);
  if (d == nullptr) {
    LOG(INFO) << "Receive read inbox in unknown " << dialog_id << " from " << source;
    return;
  }

  /*
  // dropping unread count can make things worse, so don't drop it
  if (server_unread_count > 0 && G()->parameters().use_message_db && d->is_last_read_inbox_message_id_inited) {
    server_unread_count = -1;
  }
  */

  if (d->pts == pts) {
    read_history_inbox(dialog_id, max_message_id, server_unread_count, source);
  } else if (d->pts > pts) {
    // outdated update, need to repair server_unread_count from the server
    repair_channel_server_unread_count(d);
  } else {
    // update from the future, keep it until it can be applied
    if (pts >= d->pending_read_channel_inbox_pts) {
      if (d->pending_read_channel_inbox_pts == 0) {
        channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
      }
      d->pending_read_channel_inbox_pts = pts;
      d->pending_read_channel_inbox_max_message_id = max_message_id;
      d->pending_read_channel_inbox_server_unread_count = server_unread_count;
      on_dialog_updated(dialog_id, "on_read_channel_inbox");
    }
  }
}

class ResetNotifySettingsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit ResetNotifySettingsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::account_resetNotifySettings()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_resetNotifySettings>(packet);
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
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for reset notification settings: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::reset_all_notification_settings_on_server(uint64 log_event_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (log_event_id == 0) {
    log_event_id = save_reset_all_notification_settings_on_server_log_event();
  }

  LOG(INFO) << "Reset all notification settings";
  td_->create_handler<ResetNotifySettingsQuery>(get_erase_log_event_promise(log_event_id))->send();
}

void MessagesManager::on_dialog_linked_channel_updated(DialogId dialog_id, ChannelId old_linked_channel_id,
                                                       ChannelId new_linked_channel_id) const {
  CHECK(dialog_id.get_type() == DialogType::Channel);
  if (!is_broadcast_channel(dialog_id)) {
    return;
  }
  auto d = get_dialog(dialog_id);  // no need to create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    vector<MessageId> message_ids;
    find_messages(d->messages.get(), message_ids, [old_linked_channel_id, new_linked_channel_id](const Message *m) {
      return !m->reply_info.is_empty() && m->reply_info.channel_id.is_valid() &&
             (m->reply_info.channel_id == old_linked_channel_id || m->reply_info.channel_id == new_linked_channel_id);
    });
    LOG(INFO) << "Found discussion messages " << message_ids;
    for (auto message_id : message_ids) {
      send_update_message_interaction_info(dialog_id, get_message(d, message_id));
      if (message_id == d->last_message_id) {
        send_update_chat_last_message_impl(d, "on_dialog_linked_channel_updated");
      }
    }
  }
}

void MessagesManager::send_update_chat_last_message_impl(const Dialog *d, const char *source) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_last_message from "
                                        << source;
  LOG(INFO) << "Send updateChatLastMessage in " << d->dialog_id << " to " << d->last_message_id << " from " << source;
  const auto *m = get_message(d, d->last_message_id);
  auto message_object = get_message_object(d->dialog_id, m, "send_update_chat_last_message_impl");
  auto positions_object = get_chat_positions_object(d);
  auto update = td_api::make_object<td_api::updateChatLastMessage>(d->dialog_id.get(), std::move(message_object),
                                                                   std::move(positions_object));
  send_closure(G()->td(), &Td::send_update, std::move(update));
}

void MessagesManager::fix_server_reply_to_message_id(DialogId dialog_id, MessageId message_id,
                                                     DialogId reply_in_dialog_id, MessageId &reply_to_message_id) {
  CHECK(!reply_to_message_id.is_scheduled());
  if (!reply_to_message_id.is_valid()) {
    if (reply_to_message_id != MessageId()) {
      LOG(ERROR) << "Receive reply to " << reply_to_message_id << " for " << message_id << " in " << dialog_id;
      reply_to_message_id = MessageId();
    }
    return;
  }

  if (!message_id.is_scheduled() && !reply_in_dialog_id.is_valid() && reply_to_message_id >= message_id) {
    if (!has_qts_messages(dialog_id)) {
      LOG(ERROR) << "Receive reply to wrong " << reply_to_message_id << " in " << message_id << " in " << dialog_id;
    }
    reply_to_message_id = MessageId();
  }
}

vector<DialogId> MessagesManager::search_dialogs_on_server(const string &query, int32 limit, Promise<Unit> &&promise) {
  LOG(INFO) << "Search chats on server with query \"" << query << "\" and limit " << limit;

  if (limit < 0) {
    promise.set_error(Status::Error(400, "Limit must be non-negative"));
    return {};
  }
  if (limit > MAX_GET_DIALOGS) {
    limit = MAX_GET_DIALOGS;
  }

  if (query.empty()) {
    promise.set_value(Unit());
    return {};
  }

  auto it = found_on_server_dialogs_.find(query);
  if (it != found_on_server_dialogs_.end()) {
    promise.set_value(Unit());
    return sort_dialogs_by_order(it->second, limit);
  }

  send_search_public_dialogs_query(query, std::move(promise));
  return {};
}

void MessagesManager::ttl_db_on_result(Result<std::pair<std::vector<MessagesDbMessage>, int32>> r_result, bool dummy) {
  if (G()->close_flag()) {
    return;
  }

  auto result = r_result.move_as_ok();
  ttl_db_has_query_ = false;
  ttl_db_expires_from_ = ttl_db_expires_till_;
  ttl_db_expires_till_ = result.second;

  LOG(INFO) << "Receive ttl_db query result " << tag("new expires_till", ttl_db_expires_till_)
            << tag("got messages", result.first.size());
  for (auto &dialog_message : result.first) {
    on_get_message_from_database(dialog_message, false, "ttl_db_on_result");
  }
  ttl_db_loop(G()->server_time());
}

void MessagesManager::set_dialog_has_scheduled_server_messages(Dialog *d, bool has_scheduled_server_messages) {
  CHECK(d != nullptr);
  CHECK(d->has_scheduled_server_messages != has_scheduled_server_messages);
  d->has_scheduled_server_messages = has_scheduled_server_messages;
  repair_dialog_scheduled_messages(d);
  on_dialog_updated(d->dialog_id, "set_dialog_has_scheduled_server_messages");

  LOG(INFO) << "Set " << d->dialog_id << " has_scheduled_server_messages to " << has_scheduled_server_messages;

  send_update_chat_has_scheduled_messages(d, false);
}

td_api::object_ptr<td_api::chatJoinRequestsInfo> MessagesManager::get_chat_join_requests_info_object(
    const Dialog *d) const {
  if (d->pending_join_request_count == 0) {
    return nullptr;
  }
  return td_api::make_object<td_api::chatJoinRequestsInfo>(
      d->pending_join_request_count, td_->contacts_manager_->get_user_ids_object(d->pending_join_request_user_ids,
                                                                                 "get_chat_join_requests_info_object"));
}

void MessagesManager::on_preload_folder_dialog_list_timeout_callback(void *messages_manager_ptr, int64 folder_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::preload_folder_dialog_list,
                     FolderId(narrow_cast<int32>(folder_id_int)));
}

void MessagesManager::cancel_send_deleted_message(DialogId dialog_id, Message *m, bool is_permanently_deleted) {
  CHECK(m != nullptr);
  if (m->message_id.is_yet_unsent()) {
    cancel_send_message_query(dialog_id, m);
  } else if (is_permanently_deleted || !m->message_id.is_scheduled()) {
    cancel_edit_message_media(dialog_id, m, "Message was deleted");
  }
}

bool MessagesManager::is_broadcast_channel(DialogId dialog_id) const {
  if (dialog_id.get_type() != DialogType::Channel) {
    return false;
  }

  return td_->contacts_manager_->get_channel_type(dialog_id.get_channel_id()) ==
         ContactsManager::ChannelType::Broadcast;
}

vector<int32> MessagesManager::get_server_message_ids(const vector<MessageId> &message_ids) {
  return transform(message_ids, [](MessageId message_id) { return message_id.get_server_message_id().get(); });
}

void MessagesManager::clear_recently_found_dialogs() {
  recently_found_dialogs_.clear_dialogs();
}
}  // namespace td
