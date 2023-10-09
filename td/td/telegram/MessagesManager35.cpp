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










namespace td {
}  // namespace td
namespace td {

class ForwardMessagesActor final : public NetActorOnce {
  Promise<Unit> promise_;
  vector<int64> random_ids_;
  DialogId from_dialog_id_;
  DialogId to_dialog_id_;

 public:
  explicit ForwardMessagesActor(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(int32 flags, DialogId to_dialog_id, DialogId from_dialog_id,
            tl_object_ptr<telegram_api::InputPeer> as_input_peer, const vector<MessageId> &message_ids,
            vector<int64> &&random_ids, int32 schedule_date, uint64 sequence_dispatcher_id) {
    random_ids_ = random_ids;
    from_dialog_id_ = from_dialog_id;
    to_dialog_id_ = to_dialog_id;

    auto to_input_peer = td_->messages_manager_->get_input_peer(to_dialog_id, AccessRights::Write);
    if (to_input_peer == nullptr) {
      on_error(Status::Error(400, "Have no write access to the chat"));
      stop();
      return;
    }

    auto from_input_peer = td_->messages_manager_->get_input_peer(from_dialog_id, AccessRights::Read);
    if (from_input_peer == nullptr) {
      on_error(Status::Error(400, "Can't access the chat to forward messages from"));
      stop();
      return;
    }

    if (as_input_peer != nullptr) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_SEND_AS;
    }

    auto query = G()->net_query_creator().create(telegram_api::messages_forwardMessages(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        std::move(from_input_peer), MessagesManager::get_server_message_ids(message_ids), std::move(random_ids),
        std::move(to_input_peer), schedule_date, std::move(as_input_peer)));
    if (G()->shared_config().get_option_boolean("use_quick_ack")) {
      query->quick_ack_promise_ = PromiseCreator::lambda(
          [random_ids = random_ids_](Unit) {
            for (auto random_id : random_ids) {
              send_closure(G()->messages_manager(), &MessagesManager::on_send_message_get_quick_ack, random_id);
            }
          },
          PromiseCreator::Ignore());
    }
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_forwardMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ForwardMessages for " << format::as_array(random_ids_) << ": " << to_string(ptr);
    auto sent_random_ids = UpdatesManager::get_sent_messages_random_ids(ptr.get());
    bool is_result_wrong = false;
    auto sent_random_ids_size = sent_random_ids.size();
    for (auto &random_id : random_ids_) {
      auto it = sent_random_ids.find(random_id);
      if (it == sent_random_ids.end()) {
        if (random_ids_.size() == 1) {
          is_result_wrong = true;
        }
        td_->messages_manager_->on_send_message_fail(random_id, Status::Error(400, "Message was not forwarded"));
      } else {
        sent_random_ids.erase(it);
      }
    }
    if (!sent_random_ids.empty()) {
      is_result_wrong = true;
    }
    if (!is_result_wrong) {
      auto sent_messages = UpdatesManager::get_new_messages(ptr.get());
      if (sent_random_ids_size != sent_messages.size()) {
        is_result_wrong = true;
      }
      for (auto &sent_message : sent_messages) {
        if (MessagesManager::get_message_dialog_id(*sent_message) != to_dialog_id_) {
          is_result_wrong = true;
        }
      }
    }
    if (is_result_wrong) {
      LOG(ERROR) << "Receive wrong result for forwarding messages with random_ids " << format::as_array(random_ids_)
                 << " to " << to_dialog_id_ << ": " << oneline(to_string(ptr));
      td_->updates_manager_->schedule_get_difference("Wrong forwardMessages result");
    }

    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for forward messages: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, messages should be re-sent
      return;
    }
    // no on_get_dialog_error call, because two dialogs are involved
    if (status.code() == 400 && status.message() == CSlice("CHAT_FORWARDS_RESTRICTED")) {
      td_->contacts_manager_->reload_dialog_info(from_dialog_id_, Promise<Unit>());
    }
    if (status.code() == 400 && status.message() == CSlice("SEND_AS_PEER_INVALID")) {
      td_->messages_manager_->reload_dialog_info_full(to_dialog_id_);
    }
    for (auto &random_id : random_ids_) {
      td_->messages_manager_->on_send_message_fail(random_id, status.clone());
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::do_forward_messages(DialogId to_dialog_id, DialogId from_dialog_id,
                                          const vector<Message *> &messages, const vector<MessageId> &message_ids,
                                          uint64 log_event_id) {
  CHECK(messages.size() == message_ids.size());
  if (messages.empty()) {
    return;
  }

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_forward_messages_log_event(to_dialog_id, from_dialog_id, messages, message_ids);
  }

  auto schedule_date = get_message_schedule_date(messages[0]);

  int32 flags = 0;
  if (messages[0]->disable_notification) {
    flags |= SEND_MESSAGE_FLAG_DISABLE_NOTIFICATION;
  }
  if (messages[0]->from_background) {
    flags |= SEND_MESSAGE_FLAG_FROM_BACKGROUND;
  }
  if (messages[0]->in_game_share) {
    flags |= SEND_MESSAGE_FLAG_WITH_MY_SCORE;
  }
  if (schedule_date != 0) {
    flags |= SEND_MESSAGE_FLAG_HAS_SCHEDULE_DATE;
  }
  if (messages[0]->has_explicit_sender) {
    flags |= SEND_MESSAGE_FLAG_HAS_SEND_AS;
  }

  vector<int64> random_ids =
      transform(messages, [this, to_dialog_id](const Message *m) { return begin_send_message(to_dialog_id, m); });
  send_closure(td_->create_net_actor<ForwardMessagesActor>(get_erase_log_event_promise(log_event_id)),
               &ForwardMessagesActor::send, flags, to_dialog_id, from_dialog_id,
               get_send_message_as_input_peer(messages[0]), message_ids, std::move(random_ids), schedule_date,
               get_sequence_dispatcher_id(to_dialog_id, MessageContentType::None));
}

class ReadMessagesContentsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit ReadMessagesContentsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<MessageId> &&message_ids) {
    send_query(G()->net_query_creator().create(
        telegram_api::messages_readMessageContents(MessagesManager::get_server_message_ids(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_readMessageContents>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto affected_messages = result_ptr.move_as_ok();
    CHECK(affected_messages->get_id() == telegram_api::messages_affectedMessages::ID);

    if (affected_messages->pts_count_ > 0) {
      td_->updates_manager_->add_pending_pts_update(make_tl_object<dummyUpdate>(), affected_messages->pts_,
                                                    affected_messages->pts_count_, Time::now(), Promise<Unit>(),
                                                    "read messages content query");
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for read message contents: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

class ReadChannelMessagesContentsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit ReadChannelMessagesContentsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, vector<MessageId> &&message_ids) {
    channel_id_ = channel_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      LOG(ERROR) << "Have no input channel for " << channel_id;
      return on_error(Status::Error(500, "Can't read channel message contents"));
    }

    send_query(G()->net_query_creator().create(telegram_api::channels_readMessageContents(
        std::move(input_channel), MessagesManager::get_server_message_ids(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_readMessageContents>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG_IF(ERROR, !result) << "Read channel messages contents failed";

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ReadChannelMessagesContentsQuery")) {
      LOG(ERROR) << "Receive error for read messages contents in " << channel_id_ << ": " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::read_message_contents_on_server(DialogId dialog_id, vector<MessageId> message_ids,
                                                      uint64 log_event_id, Promise<Unit> &&promise,
                                                      bool skip_log_event) {
  CHECK(!message_ids.empty());

  LOG(INFO) << "Read contents of " << format::as_array(message_ids) << " in " << dialog_id << " on server";

  if (log_event_id == 0 && G()->parameters().use_message_db && !skip_log_event) {
    log_event_id = save_read_message_contents_on_server_log_event(dialog_id, message_ids);
  }

  auto new_promise = get_erase_log_event_promise(log_event_id, std::move(promise));
  promise = std::move(new_promise);  // to prevent self-move

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
      td_->create_handler<ReadMessagesContentsQuery>(std::move(promise))->send(std::move(message_ids));
      break;
    case DialogType::Channel:
      td_->create_handler<ReadChannelMessagesContentsQuery>(std::move(promise))
          ->send(dialog_id.get_channel_id(), std::move(message_ids));
      break;
    case DialogType::SecretChat: {
      CHECK(message_ids.size() == 1);
      auto m = get_message_force({dialog_id, message_ids[0]}, "read_message_contents_on_server");
      if (m != nullptr) {
        send_closure(G()->secret_chats_manager(), &SecretChatsManager::send_open_message,
                     dialog_id.get_secret_chat_id(), m->random_id, std::move(promise));
      } else {
        promise.set_error(Status::Error(400, "Message not found"));
      }
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

class GetDialogQuery final : public Td::ResultHandler {
  DialogId dialog_id_;

 public:
  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;
    send_query(G()->net_query_creator().create(telegram_api::messages_getPeerDialogs(
        td_->messages_manager_->get_input_dialog_peers({dialog_id}, AccessRights::Read))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getPeerDialogs>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetDialogQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetDialogQuery");
    td_->contacts_manager_->on_get_chats(std::move(result->chats_), "GetDialogQuery");
    td_->messages_manager_->on_get_dialogs(FolderId(), std::move(result->dialogs_), -1, std::move(result->messages_),
                                           PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(),
                                                                   dialog_id = dialog_id_](Result<> result) {
                                             send_closure(actor_id, &MessagesManager::on_get_dialog_query_finished,
                                                          dialog_id,
                                                          result.is_error() ? result.move_as_error() : Status::OK());
                                           }));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetDialogQuery");
    td_->messages_manager_->on_get_dialog_query_finished(dialog_id_, std::move(status));
  }
};

void MessagesManager::send_get_dialog_query(DialogId dialog_id, Promise<Unit> &&promise, uint64 log_event_id,
                                            const char *source) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (td_->auth_manager_->is_bot() || dialog_id.get_type() == DialogType::SecretChat) {
    if (log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), log_event_id);
    }
    return promise.set_error(Status::Error(500, "Wrong getDialog query"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    if (log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), log_event_id);
    }
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  auto &promises = get_dialog_queries_[dialog_id];
  promises.push_back(std::move(promise));
  if (promises.size() != 1) {
    if (log_event_id != 0) {
      LOG(INFO) << "Duplicate getDialog query for " << dialog_id << " from " << source;
      binlog_erase(G()->td_db()->get_binlog(), log_event_id);
    }
    // query has already been sent, just wait for the result
    return;
  }

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_reget_dialog_log_event(dialog_id);
  }
  if (log_event_id != 0) {
    auto result = get_dialog_query_log_event_id_.emplace(dialog_id, log_event_id);
    CHECK(result.second);
  }
  if (G()->close_flag()) {
    // request will be sent after restart
    return;
  }

  LOG(INFO) << "Send get " << dialog_id << " query from " << source;
  td_->create_handler<GetDialogQuery>()->send(dialog_id);
}

MessagesManager::FoundMessages MessagesManager::offline_search_messages(DialogId dialog_id, const string &query,
                                                                        string offset, int32 limit,
                                                                        MessageSearchFilter filter, int64 &random_id,
                                                                        Promise<> &&promise) {
  if (!G()->parameters().use_message_db) {
    promise.set_error(Status::Error(400, "Message database is required to search messages in secret chats"));
    return {};
  }

  if (random_id != 0) {
    // request has already been sent before
    auto it = found_fts_messages_.find(random_id);
    CHECK(it != found_fts_messages_.end());
    auto result = std::move(it->second);
    found_fts_messages_.erase(it);
    promise.set_value(Unit());
    return result;
  }

  if (query.empty()) {
    promise.set_value(Unit());
    return {};
  }
  if (dialog_id != DialogId() && !have_dialog_force(dialog_id, "offline_search_messages")) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return {};
  }
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Limit must be positive"));
    return {};
  }
  if (limit > MAX_SEARCH_MESSAGES) {
    limit = MAX_SEARCH_MESSAGES;
  }

  MessagesDbFtsQuery fts_query;
  fts_query.query = query;
  fts_query.dialog_id = dialog_id;
  fts_query.filter = filter;
  if (!offset.empty()) {
    auto r_from_search_id = to_integer_safe<int64>(offset);
    if (r_from_search_id.is_error()) {
      promise.set_error(Status::Error(400, "Invalid offset specified"));
      return {};
    }
    fts_query.from_search_id = r_from_search_id.ok();
  }
  fts_query.limit = limit;

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || found_fts_messages_.find(random_id) != found_fts_messages_.end());
  found_fts_messages_[random_id];  // reserve place for result

  G()->td_db()->get_messages_db_async()->get_messages_fts(
      std::move(fts_query),
      PromiseCreator::lambda([random_id, offset = std::move(offset), limit,
                              promise = std::move(promise)](Result<MessagesDbFtsResult> fts_result) mutable {
        send_closure(G()->messages_manager(), &MessagesManager::on_messages_db_fts_result, std::move(fts_result),
                     std::move(offset), limit, random_id, std::move(promise));
      }));

  return {};
}

void MessagesManager::remove_message_notification_id(Dialog *d, Message *m, bool is_permanent, bool force_update,
                                                     bool ignore_pinned_message_notification_removal) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());
  if (!m->notification_id.is_valid()) {
    return;
  }

  auto from_mentions = is_from_mention_notification_group(d, m);
  auto &group_info = get_notification_group_info(d, m);
  if (!group_info.group_id.is_valid()) {
    return;
  }

  bool had_active_notification = is_message_notification_active(d, m);

  auto notification_id = m->notification_id;
  VLOG(notifications) << "Remove " << notification_id << " from " << m->message_id << " in " << group_info.group_id
                      << '/' << d->dialog_id << " from database, was_active = " << had_active_notification
                      << ", is_permanent = " << is_permanent;
  delete_notification_id_to_message_id_correspondence(d, notification_id, m->message_id);
  m->removed_notification_id = m->notification_id;
  m->notification_id = NotificationId();
  if (d->pinned_message_notification_message_id == m->message_id && is_permanent &&
      !ignore_pinned_message_notification_removal) {
    remove_dialog_pinned_message_notification(
        d, "remove_message_notification_id");  // must be called after notification_id is removed
  }
  if (group_info.last_notification_id == notification_id) {
    // last notification is deleted, need to find new last notification
    fix_dialog_last_notification_id(d, from_mentions, m->message_id);
  }

  if (is_permanent) {
    if (had_active_notification) {
      send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification, group_info.group_id,
                         notification_id, is_permanent, force_update, Promise<Unit>(),
                         "remove_message_notification_id");
    }

    // on_message_changed will be called by the caller
    // don't need to call there to not save twice/or to save just deleted message
  } else {
    on_message_changed(d, m, false, "remove_message_notification_id");
  }
}

void MessagesManager::load_messages_impl(const Dialog *d, MessageId from_message_id, int32 offset, int32 limit,
                                         int left_tries, bool only_local, Promise<Unit> &&promise) {
  CHECK(d != nullptr);
  CHECK(offset <= 0);
  CHECK(left_tries > 0);
  auto dialog_id = d->dialog_id;
  LOG(INFO) << "Load " << (only_local ? "local " : "") << "messages in " << dialog_id << " from " << from_message_id
            << " with offset = " << offset << " and limit = " << limit << ". " << left_tries << " tries left";
  only_local |= dialog_id.get_type() == DialogType::SecretChat;
  if (!only_local && d->have_full_history) {
    LOG(INFO) << "Have full history in " << dialog_id << ", so don't need to get chat history from server";
    only_local = true;
  }
  bool from_database = (left_tries > 2 || only_local) && G()->parameters().use_message_db;

  if (from_message_id == MessageId()) {
    get_history_from_the_end_impl(d, from_database, only_local, std::move(promise));
    return;
  }
  if ((!d->first_database_message_id.is_valid() || from_message_id <= d->first_database_message_id) &&
      !d->have_full_history) {
    from_database = false;
  }
  if (offset >= -1) {
    // get history before some server or local message
    limit = min(max(limit + offset + 1, MAX_GET_HISTORY / 2), MAX_GET_HISTORY);
    offset = -1;
  } else {
    // get history around some server or local message
    int32 messages_to_load = max(MAX_GET_HISTORY, limit);
    int32 max_add = max(messages_to_load - limit - 2, 0);
    offset -= max_add;
    limit = MAX_GET_HISTORY;
  }
  get_history_impl(d, from_message_id, offset, limit, from_database, only_local, std::move(promise));
}

void MessagesManager::on_secret_chat_screenshot_taken(SecretChatId secret_chat_id, UserId user_id, MessageId message_id,
                                                      int32 date, int64 random_id, Promise<> promise) {
  LOG(DEBUG) << "On screenshot taken in " << secret_chat_id;
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
  message_info.random_id = random_id;
  message_info.flags = MESSAGE_FLAG_HAS_FROM_ID;
  message_info.content = create_screenshot_taken_message_content();

  Dialog *d = get_dialog_force(message_info.dialog_id, "on_secret_chat_screenshot_taken");
  if (d == nullptr && have_dialog_info_force(message_info.dialog_id)) {
    force_create_dialog(message_info.dialog_id, "on_get_secret_message", true, true);
    d = get_dialog(message_info.dialog_id);
  }
  if (d == nullptr) {
    LOG(ERROR) << "Ignore secret message in unknown " << message_info.dialog_id;
    pending_secret_message->success_promise.set_error(Status::Error(500, "Chat not found"));
    return;
  }

  add_secret_message(std::move(pending_secret_message));
}

void MessagesManager::on_get_blocked_dialogs(int32 offset, int32 limit, int32 total_count,
                                             vector<tl_object_ptr<telegram_api::peerBlocked>> &&blocked_peers,
                                             Promise<td_api::object_ptr<td_api::messageSenders>> &&promise) {
  LOG(INFO) << "Receive " << blocked_peers.size() << " blocked chats from offset " << offset << " out of "
            << total_count;
  auto peers = transform(std::move(blocked_peers), [](tl_object_ptr<telegram_api::peerBlocked> &&blocked_peer) {
    return std::move(blocked_peer->peer_id_);
  });
  auto dialog_ids = get_message_sender_dialog_ids(td_, std::move(peers));
  if (!dialog_ids.empty() && offset + dialog_ids.size() > static_cast<size_t>(total_count)) {
    LOG(ERROR) << "Fix total count of blocked chats from " << total_count << " to " << offset + dialog_ids.size();
    total_count = offset + narrow_cast<int32>(dialog_ids.size());
  }

  auto senders = transform(dialog_ids, [td = td_](DialogId dialog_id) {
    return get_message_sender_object(td, dialog_id, "on_get_blocked_dialogs");
  });
  promise.set_value(td_api::make_object<td_api::messageSenders>(total_count, std::move(senders)));
}

void MessagesManager::delete_secret_chat_history(SecretChatId secret_chat_id, bool remove_from_dialog_list,
                                                 MessageId last_message_id, Promise<> promise) {
  LOG(DEBUG) << "Delete history in " << secret_chat_id << " up to " << last_message_id;
  CHECK(secret_chat_id.is_valid());
  CHECK(!last_message_id.is_scheduled());

  DialogId dialog_id(secret_chat_id);
  if (!have_dialog_force(dialog_id, "delete_secret_chat_history")) {
    LOG(ERROR) << "Ignore delete history in unknown " << dialog_id;
    promise.set_value(Unit());
    return;
  }

  auto pending_secret_message = make_unique<PendingSecretMessage>();
  pending_secret_message->success_promise = std::move(promise);
  pending_secret_message->type = PendingSecretMessage::Type::DeleteHistory;
  pending_secret_message->dialog_id = dialog_id;
  pending_secret_message->last_message_id = last_message_id;
  pending_secret_message->remove_from_dialog_list = remove_from_dialog_list;

  add_secret_message(std::move(pending_secret_message));
}

void MessagesManager::on_scope_unmute(NotificationSettingsScope scope) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  auto notification_settings = get_scope_notification_settings(scope);
  CHECK(notification_settings != nullptr);

  if (notification_settings->mute_until == 0) {
    return;
  }

  auto now = G()->unix_time();
  if (notification_settings->mute_until > now) {
    LOG(ERROR) << "Failed to unmute " << scope << " in " << now << ", will be unmuted in "
               << notification_settings->mute_until;
    schedule_scope_unmute(scope, notification_settings->mute_until);
    return;
  }

  LOG(INFO) << "Unmute " << scope;
  update_scope_unmute_timeout(scope, notification_settings->mute_until, 0);
  send_closure(G()->td(), &Td::send_update, get_update_scope_notification_settings_object(scope));
  save_scope_notification_settings(scope, *notification_settings);
}

void MessagesManager::send_update_secret_chats_with_user_action_bar(const Dialog *d) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (d->dialog_id.get_type() != DialogType::User) {
    return;
  }

  td_->contacts_manager_->for_each_secret_chat_with_user(
      d->dialog_id.get_user_id(), [this, user_d = d](SecretChatId secret_chat_id) {
        DialogId dialog_id(secret_chat_id);
        auto secret_chat_d = get_dialog(dialog_id);  // must not create the dialog
        if (secret_chat_d != nullptr && secret_chat_d->is_update_new_chat_sent) {
          send_closure(
              G()->td(), &Td::send_update,
              td_api::make_object<td_api::updateChatActionBar>(dialog_id.get(), get_chat_action_bar_object(user_d)));
        }
      });
}

void MessagesManager::do_repair_dialog_active_group_call_id(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  bool need_repair_active_group_call_id = d->has_active_group_call && !d->active_group_call_id.is_valid();
  bool need_repair_expected_group_call_id =
      d->has_expected_active_group_call_id && d->active_group_call_id != d->expected_active_group_call_id;
  d->has_expected_active_group_call_id = false;
  if (!need_repair_active_group_call_id && !need_repair_expected_group_call_id) {
    return;
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return;
  }

  reload_dialog_info_full(dialog_id);
}

NotificationSettingsScope MessagesManager::get_dialog_notification_setting_scope(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::SecretChat:
      return NotificationSettingsScope::Private;
    case DialogType::Chat:
      return NotificationSettingsScope::Group;
    case DialogType::Channel:
      return is_broadcast_channel(dialog_id) ? NotificationSettingsScope::Channel : NotificationSettingsScope::Group;
    case DialogType::None:
    default:
      UNREACHABLE();
      return NotificationSettingsScope::Private;
  }
}

void MessagesManager::finish_delete_secret_chat_history(DialogId dialog_id, bool remove_from_dialog_list,
                                                        MessageId last_message_id, Promise<> promise) {
  LOG(DEBUG) << "Delete history in " << dialog_id << " up to " << last_message_id;
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  // TODO: probably last_message_id is not needed
  delete_all_dialog_messages(d, remove_from_dialog_list, true);
  promise.set_value(Unit());  // TODO: set after event is saved
}

void MessagesManager::on_dialog_default_permissions_updated(DialogId dialog_id) {
  auto d = get_dialog(dialog_id);  // called from update_user, must not create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    send_closure(G()->td(), &Td::send_update,
                 td_api::make_object<td_api::updateChatPermissions>(
                     dialog_id.get(), get_dialog_default_permissions(dialog_id).get_chat_permissions_object()));
  }
}

void MessagesManager::on_failed_dialog_messages_search(DialogId dialog_id, int64 random_id) {
  if (!dialog_id.is_valid()) {
    auto it = found_call_messages_.find(random_id);
    CHECK(it != found_call_messages_.end());
    found_call_messages_.erase(it);
    return;
  }

  auto it = found_dialog_messages_.find(random_id);
  CHECK(it != found_dialog_messages_.end());
  found_dialog_messages_.erase(it);
}

void MessagesManager::create_folders() {
  LOG(INFO) << "Create folders";
  dialog_folders_[FolderId::main()].folder_id = FolderId::main();
  dialog_folders_[FolderId::archive()].folder_id = FolderId::archive();

  add_dialog_list(DialogListId(FolderId::main()));
  add_dialog_list(DialogListId(FolderId::archive()));
}

void MessagesManager::on_failed_get_message_search_result_calendar(DialogId dialog_id, int64 random_id) {
  auto it = found_dialog_message_calendars_.find(random_id);
  CHECK(it != found_dialog_message_calendars_.end());
  found_dialog_message_calendars_.erase(it);
}

MessageId MessagesManager::get_next_yet_unsent_message_id(Dialog *d) {
  return get_next_message_id(d, MessageType::YetUnsent);
}

void MessagesManager::hangup() {
  postponed_channel_updates_.clear();
  stop();
}
}  // namespace td
