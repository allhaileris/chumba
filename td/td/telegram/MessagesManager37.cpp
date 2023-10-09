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

class DeleteMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit DeleteMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, vector<int32> &&server_message_ids, bool revoke) {
    dialog_id_ = dialog_id;
    int32 flags = 0;
    if (revoke) {
      flags |= telegram_api::messages_deleteMessages::REVOKE_MASK;
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_deleteMessages(flags, false /*ignored*/, std::move(server_message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto affected_messages = result_ptr.move_as_ok();
    if (affected_messages->pts_count_ > 0) {
      td_->updates_manager_->add_pending_pts_update(make_tl_object<dummyUpdate>(), affected_messages->pts_,
                                                    affected_messages->pts_count_, Time::now(), std::move(promise_),
                                                    "delete messages query");
    } else {
      promise_.set_value(Unit());
    }
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      // MESSAGE_DELETE_FORBIDDEN can be returned in group chats when administrator rights was removed
      // MESSAGE_DELETE_FORBIDDEN can be returned in private chats for bots when revoke time limit exceeded
      if (status.message() != "MESSAGE_DELETE_FORBIDDEN" ||
          (dialog_id_.get_type() == DialogType::User && !td_->auth_manager_->is_bot())) {
        LOG(ERROR) << "Receive error for delete messages: " << status;
      }
    }
    promise_.set_error(std::move(status));
  }
};

class DeleteChannelMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit DeleteChannelMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, vector<int32> &&server_message_ids) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::channels_deleteMessages(std::move(input_channel), std::move(server_message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_deleteMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto affected_messages = result_ptr.move_as_ok();
    if (affected_messages->pts_count_ > 0) {
      td_->messages_manager_->add_pending_channel_update(DialogId(channel_id_), make_tl_object<dummyUpdate>(),
                                                         affected_messages->pts_, affected_messages->pts_count_,
                                                         std::move(promise_), "DeleteChannelMessagesQuery");
    } else {
      promise_.set_value(Unit());
    }
  }

  void on_error(Status status) final {
    if (!td_->contacts_manager_->on_get_channel_error(channel_id_, status, "DeleteChannelMessagesQuery") &&
        status.message() != "MESSAGE_DELETE_FORBIDDEN") {
      LOG(ERROR) << "Receive error for delete channel messages: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::delete_messages_on_server(DialogId dialog_id, vector<MessageId> message_ids, bool revoke,
                                                uint64 log_event_id, Promise<Unit> &&promise) {
  if (message_ids.empty()) {
    return promise.set_value(Unit());
  }
  LOG(INFO) << (revoke ? "Revoke " : "Delete ") << format::as_array(message_ids) << " in " << dialog_id
            << " from server";

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_delete_messages_on_server_log_event(dialog_id, message_ids, revoke);
  }

  MultiPromiseActorSafe mpas{"DeleteMessagesOnServerMultiPromiseActor"};
  mpas.add_promise(std::move(promise));
  if (log_event_id != 0) {
    mpas.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), log_event_id](Unit) {
      send_closure(actor_id, &MessagesManager::erase_delete_messages_log_event, log_event_id);
    }));
  }
  mpas.set_ignore_errors(true);
  auto lock = mpas.get_promise();
  auto dialog_type = dialog_id.get_type();
  switch (dialog_type) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel: {
      auto server_message_ids = MessagesManager::get_server_message_ids(message_ids);
      const size_t MAX_SLICE_SIZE = 100;  // server side limit
      for (size_t i = 0; i < server_message_ids.size(); i += MAX_SLICE_SIZE) {
        auto end_i = i + MAX_SLICE_SIZE;
        auto end = end_i < server_message_ids.size() ? server_message_ids.begin() + end_i : server_message_ids.end();
        if (dialog_type != DialogType::Channel) {
          td_->create_handler<DeleteMessagesQuery>(mpas.get_promise())
              ->send(dialog_id, {server_message_ids.begin() + i, end}, revoke);
        } else {
          td_->create_handler<DeleteChannelMessagesQuery>(mpas.get_promise())
              ->send(dialog_id.get_channel_id(), {server_message_ids.begin() + i, end});
        }
      }
      break;
    }
    case DialogType::SecretChat: {
      vector<int64> random_ids;
      auto d = get_dialog_force(dialog_id, "delete_messages_on_server");
      CHECK(d != nullptr);
      for (auto &message_id : message_ids) {
        auto *m = get_message(d, message_id);
        if (m != nullptr) {
          random_ids.push_back(m->random_id);
        }
      }
      if (!random_ids.empty()) {
        send_closure(G()->secret_chats_manager(), &SecretChatsManager::delete_messages, dialog_id.get_secret_chat_id(),
                     std::move(random_ids), mpas.get_promise());
      }
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  lock.set_value(Unit());
}

class GetSearchResultPositionsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::messagePositions>> promise_;
  DialogId dialog_id_;
  MessageSearchFilter filter_;

 public:
  explicit GetSearchResultPositionsQuery(Promise<td_api::object_ptr<td_api::messagePositions>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageSearchFilter filter, MessageId from_message_id, int32 limit) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Can't access the chat"));
    }
    dialog_id_ = dialog_id;
    filter_ = filter;

    send_query(G()->net_query_creator().create(
        telegram_api::messages_getSearchResultsPositions(std::move(input_peer), get_input_messages_filter(filter),
                                                         from_message_id.get_server_message_id().get(), limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getSearchResultsPositions>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->messages_manager_->on_get_dialog_sparse_message_positions(dialog_id_, filter_, result_ptr.move_as_ok(),
                                                                   std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetSearchResultPositionsQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_dialog_sparse_message_positions(
    DialogId dialog_id, MessageSearchFilter filter, MessageId from_message_id, int32 limit,
    Promise<td_api::object_ptr<td_api::messagePositions>> &&promise) {
  const Dialog *d = get_dialog_force(dialog_id, "get_dialog_sparse_message_positions");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (limit < 50 || limit > 2000) {  // server-side limits
    return promise.set_error(Status::Error(400, "Invalid limit specified"));
  }

  CHECK(filter != MessageSearchFilter::Call && filter != MessageSearchFilter::MissedCall);
  if (filter == MessageSearchFilter::Empty || filter == MessageSearchFilter::Mention ||
      filter == MessageSearchFilter::UnreadMention || filter == MessageSearchFilter::Pinned) {
    return promise.set_error(Status::Error(400, "The filter is not supported"));
  }

  if (from_message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Invalid from_message_id specified"));
  }
  if (!from_message_id.is_valid() || from_message_id > d->last_new_message_id) {
    if (d->last_new_message_id.is_valid()) {
      from_message_id = d->last_new_message_id.get_next_message_id(MessageType::Server);
    } else {
      from_message_id = MessageId::max();
    }
  } else {
    from_message_id = from_message_id.get_next_server_message_id();
  }

  if (filter == MessageSearchFilter::FailedToSend || dialog_id.get_type() == DialogType::SecretChat) {
    if (!G()->parameters().use_message_db) {
      return promise.set_error(Status::Error(400, "Unsupported without message database"));
    }

    LOG(INFO) << "Get sparse message positions from database";
    auto new_promise =
        PromiseCreator::lambda([promise = std::move(promise)](Result<MessagesDbMessagePositions> result) mutable {
          if (result.is_error()) {
            return promise.set_error(result.move_as_error());
          }

          auto positions = result.move_as_ok();
          promise.set_value(td_api::make_object<td_api::messagePositions>(
              positions.total_count, transform(positions.positions, [](const MessagesDbMessagePosition &position) {
                return td_api::make_object<td_api::messagePosition>(position.position, position.message_id.get(),
                                                                    position.date);
              })));
        });
    MessagesDbGetDialogSparseMessagePositionsQuery db_query;
    db_query.dialog_id = dialog_id;
    db_query.filter = filter;
    db_query.from_message_id = from_message_id;
    db_query.limit = limit;
    G()->td_db()->get_messages_db_async()->get_dialog_sparse_message_positions(db_query, std::move(new_promise));
    return;
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      td_->create_handler<GetSearchResultPositionsQuery>(std::move(promise))
          ->send(dialog_id, filter, from_message_id, limit);
      break;
    case DialogType::SecretChat:
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

class CreateChannelQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  int64 random_id_;

 public:
  explicit CreateChannelQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &title, bool is_megagroup, const string &about, const DialogLocation &location,
            bool for_import, int64 random_id) {
    int32 flags = 0;
    if (is_megagroup) {
      flags |= telegram_api::channels_createChannel::MEGAGROUP_MASK;
    } else {
      flags |= telegram_api::channels_createChannel::BROADCAST_MASK;
    }
    if (!location.empty()) {
      flags |= telegram_api::channels_createChannel::GEO_POINT_MASK;
    }
    if (for_import) {
      flags |= telegram_api::channels_createChannel::FOR_IMPORT_MASK;
    }

    random_id_ = random_id;
    send_query(G()->net_query_creator().create(
        telegram_api::channels_createChannel(flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, title,
                                             about, location.get_input_geo_point(), location.get_address())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_createChannel>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for CreateChannelQuery: " << to_string(ptr);
    td_->messages_manager_->on_create_new_dialog_success(random_id_, std::move(ptr), DialogType::Channel,
                                                         std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_create_new_dialog_fail(random_id_, std::move(status), std::move(promise_));
  }
};

DialogId MessagesManager::create_new_channel_chat(const string &title, bool is_megagroup, const string &description,
                                                  const DialogLocation &location, bool for_import, int64 &random_id,
                                                  Promise<Unit> &&promise) {
  LOG(INFO) << "Trying to create " << (is_megagroup ? "supergroup" : "broadcast") << " with title \"" << title
            << "\", description \"" << description << "\" and " << location;

  if (random_id != 0) {
    // request has already been sent before
    auto it = created_dialogs_.find(random_id);
    CHECK(it != created_dialogs_.end());
    auto dialog_id = it->second;
    CHECK(dialog_id.get_type() == DialogType::Channel);
    CHECK(have_dialog(dialog_id));

    created_dialogs_.erase(it);

    // set default notification settings to newly created chat
    on_update_dialog_notify_settings(dialog_id, make_tl_object<telegram_api::peerNotifySettings>(),
                                     "create_new_channel_chat");

    promise.set_value(Unit());
    return dialog_id;
  }

  auto new_title = clean_name(title, MAX_TITLE_LENGTH);
  if (new_title.empty()) {
    promise.set_error(Status::Error(400, "Title can't be empty"));
    return DialogId();
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || created_dialogs_.find(random_id) != created_dialogs_.end());
  created_dialogs_[random_id];  // reserve place for result

  td_->create_handler<CreateChannelQuery>(std::move(promise))
      ->send(new_title, is_megagroup, strip_empty_characters(description, MAX_DESCRIPTION_LENGTH), location, for_import,
             random_id);
  return DialogId();
}

bool MessagesManager::update_message_interaction_info(DialogId dialog_id, Message *m, int32 view_count,
                                                      int32 forward_count, bool has_reply_info,
                                                      MessageReplyInfo &&reply_info, const char *source) {
  CHECK(m != nullptr);
  m->interaction_info_update_date = G()->unix_time();  // doesn't force message saving
  if (m->message_id.is_valid_scheduled()) {
    has_reply_info = false;
  }
  bool need_update_reply_info = has_reply_info && m->reply_info.need_update_to(reply_info);
  if (has_reply_info && m->reply_info.channel_id == reply_info.channel_id) {
    if (need_update_reply_info) {
      reply_info.update_max_message_ids(m->reply_info);
    } else {
      if (m->reply_info.update_max_message_ids(reply_info) && view_count <= m->view_count &&
          forward_count <= m->forward_count) {
        on_message_reply_info_changed(dialog_id, m);
        on_message_changed(get_dialog(dialog_id), m, true, "update_message_interaction_info");
      }
    }
  }
  if (view_count > m->view_count || forward_count > m->forward_count || need_update_reply_info) {
    LOG(DEBUG) << "Update interaction info of " << FullMessageId{dialog_id, m->message_id} << " from " << m->view_count
               << '/' << m->forward_count << "/" << m->reply_info << " to " << view_count << '/' << forward_count << "/"
               << reply_info;
    bool need_update = false;
    if (view_count > m->view_count) {
      m->view_count = view_count;
      need_update = true;
    }
    if (forward_count > m->forward_count) {
      m->forward_count = forward_count;
      need_update = true;
    }
    if (need_update_reply_info) {
      if (m->reply_info.channel_id != reply_info.channel_id) {
        if (m->reply_info.channel_id.is_valid() && reply_info.channel_id.is_valid() && m->message_id.is_server()) {
          LOG(ERROR) << "Reply info of " << FullMessageId{dialog_id, m->message_id} << " changed from " << m->reply_info
                     << " to " << reply_info << " from " << source;
        }
      }
      m->reply_info = std::move(reply_info);
      if (!m->top_thread_message_id.is_valid() && !is_broadcast_channel(dialog_id) &&
          is_visible_message_reply_info(dialog_id, m)) {
        m->top_thread_message_id = m->message_id;
      }
      need_update |= is_visible_message_reply_info(dialog_id, m);
    }
    if (need_update) {
      send_update_message_interaction_info(dialog_id, m);
    }
    return true;
  }
  return false;
}

class GetMessageReadParticipantsQuery final : public Td::ResultHandler {
  Promise<vector<UserId>> promise_;
  DialogId dialog_id_;

 public:
  explicit GetMessageReadParticipantsQuery(Promise<vector<UserId>> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId message_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::messages_getMessageReadParticipants(
        std::move(input_peer), message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getMessageReadParticipants>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(UserId::get_user_ids(result_ptr.ok()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetMessageReadParticipantsQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_message_viewers(FullMessageId full_message_id,
                                          Promise<td_api::object_ptr<td_api::users>> &&promise) {
  TRY_STATUS_PROMISE(promise, can_get_message_viewers(full_message_id));

  auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id = full_message_id.get_dialog_id(),
                                               promise = std::move(promise)](Result<vector<UserId>> result) mutable {
    if (result.is_error()) {
      return promise.set_error(result.move_as_error());
    }
    send_closure(actor_id, &MessagesManager::on_get_message_viewers, dialog_id, result.move_as_ok(), false,
                 std::move(promise));
  });

  td_->create_handler<GetMessageReadParticipantsQuery>(std::move(query_promise))
      ->send(full_message_id.get_dialog_id(), full_message_id.get_message_id());
}

void MessagesManager::on_upload_message_media_success(DialogId dialog_id, MessageId message_id,
                                                      tl_object_ptr<telegram_api::MessageMedia> &&media) {
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  CHECK(message_id.is_valid() || message_id.is_valid_scheduled());
  CHECK(message_id.is_yet_unsent());
  Message *m = get_message(d, message_id);
  if (m == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel
    // don't need to send error to the user, because the message has already been deleted
    // and there is nothing to be deleted from the server
    LOG(INFO) << "Fail to send already deleted by the user or sent to inaccessible chat "
              << FullMessageId{dialog_id, message_id};
    return;
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return;  // the message should be deleted soon
  }

  auto caption = get_message_content_caption(m->content.get());
  auto content = get_message_content(td_, caption == nullptr ? FormattedText() : *caption, std::move(media), dialog_id,
                                     false, UserId(), nullptr, nullptr);

  if (update_message_content(dialog_id, m, std::move(content), true, true, true) &&
      m->message_id == d->last_message_id) {
    send_update_chat_last_message_impl(d, "on_upload_message_media_success");
  }

  auto input_media = get_input_media(m->content.get(), td_, m->ttl, m->send_emoji, true);
  Status result;
  if (input_media == nullptr) {
    result = Status::Error(400, "Failed to upload file");
  }

  send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id, dialog_id,
                     m->message_id, std::move(result));
}

vector<NotificationGroupKey> MessagesManager::get_message_notification_group_keys_from_database(
    NotificationGroupKey from_group_key, int32 limit) {
  if (!G()->parameters().use_message_db) {
    return {};
  }

  VLOG(notifications) << "Trying to load " << limit << " message notification groups from database from "
                      << from_group_key;

  auto *dialog_db = G()->td_db()->get_dialog_db_sync();
  dialog_db->begin_read_transaction().ensure();
  Result<vector<NotificationGroupKey>> r_notification_group_keys =
      dialog_db->get_notification_groups_by_last_notification_date(from_group_key, limit);
  r_notification_group_keys.ensure();
  auto group_keys = r_notification_group_keys.move_as_ok();

  vector<NotificationGroupKey> result;
  for (auto &group_key : group_keys) {
    CHECK(group_key.dialog_id.is_valid());
    const Dialog *d = get_dialog_force(group_key.dialog_id, "get_message_notification_group_keys_from_database");
    if (d == nullptr || (d->message_notification_group.group_id != group_key.group_id &&
                         d->mention_notification_group.group_id != group_key.group_id)) {
      continue;
    }

    CHECK(d->dialog_id == group_key.dialog_id);
    CHECK(notification_group_id_to_dialog_id_[group_key.group_id] == d->dialog_id);

    VLOG(notifications) << "Loaded " << group_key << " from database";
    result.push_back(group_key);
  }
  dialog_db->commit_transaction().ensure();
  return result;
}

void MessagesManager::unpin_all_dialog_messages(DialogId dialog_id, Promise<Unit> &&promise) {
  auto d = get_dialog_force(dialog_id, "unpin_all_dialog_messages");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  TRY_STATUS_PROMISE(promise, can_pin_messages(dialog_id));

  vector<MessageId> message_ids;
  find_messages(d->messages.get(), message_ids, [](const Message *m) { return m->is_pinned; });

  vector<int64> deleted_message_ids;
  for (auto message_id : message_ids) {
    auto m = get_message(d, message_id);
    CHECK(m != nullptr);

    m->is_pinned = false;
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateMessageIsPinned>(d->dialog_id.get(), m->message_id.get(), m->is_pinned));
    on_message_changed(d, m, true, "unpin_all_dialog_messages");
  }

  set_dialog_last_pinned_message_id(d, MessageId());
  if (d->message_count_by_index[message_search_filter_index(MessageSearchFilter::Pinned)] != 0) {
    d->message_count_by_index[message_search_filter_index(MessageSearchFilter::Pinned)] = 0;
    on_dialog_updated(dialog_id, "unpin_all_dialog_messages");
  }

  unpin_all_dialog_messages_on_server(dialog_id, 0, std::move(promise));
}

MessageId MessagesManager::get_message_id(const tl_object_ptr<telegram_api::Message> &message_ptr, bool is_scheduled) {
  switch (message_ptr->get_id()) {
    case telegram_api::messageEmpty::ID: {
      auto message = static_cast<const telegram_api::messageEmpty *>(message_ptr.get());
      return is_scheduled ? MessageId() : MessageId(ServerMessageId(message->id_));
    }
    case telegram_api::message::ID: {
      auto message = static_cast<const telegram_api::message *>(message_ptr.get());
      return is_scheduled ? MessageId(ScheduledServerMessageId(message->id_), message->date_)
                          : MessageId(ServerMessageId(message->id_));
    }
    case telegram_api::messageService::ID: {
      auto message = static_cast<const telegram_api::messageService *>(message_ptr.get());
      return is_scheduled ? MessageId(ScheduledServerMessageId(message->id_), message->date_)
                          : MessageId(ServerMessageId(message->id_));
    }
    default:
      UNREACHABLE();
      return MessageId();
  }
}

void MessagesManager::get_channel_differences_if_needed(MessagesInfo &&messages_info, Promise<MessagesInfo> &&promise) {
  MultiPromiseActorSafe mpas{"GetChannelDifferencesIfNeededMultiPromiseActor"};
  mpas.add_promise(Promise<Unit>());
  mpas.set_ignore_errors(true);
  auto lock = mpas.get_promise();
  for (auto &message : messages_info.messages) {
    if (message == nullptr) {
      continue;
    }

    auto dialog_id = get_message_dialog_id(message);
    if (need_channel_difference_to_add_message(dialog_id, message)) {
      run_after_channel_difference(dialog_id, mpas.get_promise());
    }
  }
  // must be added after messages_info is checked
  mpas.add_promise(PromiseCreator::lambda([messages_info = std::move(messages_info), promise = std::move(promise)](
                                              Unit ignored) mutable { promise.set_value(std::move(messages_info)); }));
  lock.set_value(Unit());
}

void MessagesManager::on_update_dialog_group_call_id(DialogId dialog_id, InputGroupCallId input_group_call_id) {
  auto d = get_dialog_force(dialog_id, "on_update_dialog_group_call_id");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (d->active_group_call_id != input_group_call_id) {
    LOG(INFO) << "Update active group call in " << dialog_id << " to " << input_group_call_id;
    d->active_group_call_id = input_group_call_id;
    bool has_active_group_call = input_group_call_id.is_valid();
    if (has_active_group_call != d->has_active_group_call) {
      d->has_active_group_call = has_active_group_call;
      if (!has_active_group_call) {
        d->is_group_call_empty = false;
      }
    }
    send_update_chat_video_chat(d);
  }
}

FileSourceId MessagesManager::get_message_file_source_id(FullMessageId full_message_id) {
  if (td_->auth_manager_->is_bot()) {
    return FileSourceId();
  }

  auto dialog_id = full_message_id.get_dialog_id();
  auto message_id = full_message_id.get_message_id();
  if (!dialog_id.is_valid() || !(message_id.is_valid() || message_id.is_valid_scheduled()) ||
      dialog_id.get_type() == DialogType::SecretChat || !message_id.is_any_server()) {
    return FileSourceId();
  }

  auto &file_source_id = full_message_id_to_file_source_id_[full_message_id];
  if (!file_source_id.is_valid()) {
    file_source_id = td_->file_reference_manager_->create_message_file_source(full_message_id);
  }
  return file_source_id;
}

bool MessagesManager::can_forward_message(DialogId from_dialog_id, const Message *m) {
  if (m == nullptr) {
    return false;
  }
  if (m->ttl > 0) {
    return false;
  }
  if (m->message_id.is_scheduled()) {
    return false;
  }
  switch (from_dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      // ok
      break;
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }

  return can_forward_message_content(m->content.get());
}

void MessagesManager::on_get_sponsored_dialog(tl_object_ptr<telegram_api::Peer> peer, DialogSource source,
                                              vector<tl_object_ptr<telegram_api::User>> users,
                                              vector<tl_object_ptr<telegram_api::Chat>> chats) {
  CHECK(peer != nullptr);

  td_->contacts_manager_->on_get_users(std::move(users), "on_get_sponsored_dialog");
  td_->contacts_manager_->on_get_chats(std::move(chats), "on_get_sponsored_dialog");

  set_sponsored_dialog(DialogId(peer), std::move(source));
}

void MessagesManager::send_update_chat_message_ttl(const Dialog *d) {
  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_message_ttl";
  on_dialog_updated(d->dialog_id, "send_update_chat_message_ttl");
  send_closure(
      G()->td(), &Td::send_update,
      td_api::make_object<td_api::updateChatMessageTtl>(d->dialog_id.get(), d->message_ttl.get_message_ttl_object()));
}

MessagesManager::Message *MessagesManager::on_get_message_from_database(const MessagesDbMessage &message,
                                                                        bool is_scheduled, const char *source) {
  return on_get_message_from_database(get_dialog_force(message.dialog_id, source), message.dialog_id,
                                      message.message_id, message.data, is_scheduled, source);
}

td_api::object_ptr<td_api::chatFilter> MessagesManager::get_chat_filter_object(DialogFilterId dialog_filter_id) const {
  CHECK(!td_->auth_manager_->is_bot());
  auto filter = get_dialog_filter(dialog_filter_id);
  if (filter == nullptr) {
    return nullptr;
  }

  return get_chat_filter_object(filter);
}

Status MessagesManager::remove_recently_found_dialog(DialogId dialog_id) {
  if (!have_dialog_force(dialog_id, "remove_recently_found_dialog")) {
    return Status::Error(400, "Chat not found");
  }
  recently_found_dialogs_.remove_dialog(dialog_id);
  return Status::OK();
}

DialogId MessagesManager::get_my_dialog_id() const {
  return DialogId(td_->contacts_manager_->get_my_id());
}

bool MessagesManager::need_unread_counter(int64 dialog_order) {
  return dialog_order != DEFAULT_ORDER;
}
}  // namespace td
