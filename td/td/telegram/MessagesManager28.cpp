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
#include "td/telegram/TdDb.h"
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

void MessagesManager::process_get_channel_difference_updates(
    DialogId dialog_id, int32 new_pts, vector<tl_object_ptr<telegram_api::Message>> &&new_messages,
    vector<tl_object_ptr<telegram_api::Update>> &&other_updates) {
  LOG(INFO) << "In get channel difference for " << dialog_id << " receive " << new_messages.size() << " messages and "
            << other_updates.size() << " other updates";
  CHECK(!debug_channel_difference_dialog_.is_valid());
  debug_channel_difference_dialog_ = dialog_id;

  // identifiers of edited and deleted messages
  std::unordered_set<MessageId, MessageIdHash> changed_message_ids;
  for (auto &update_ptr : other_updates) {
    bool is_good_update = true;
    switch (update_ptr->get_id()) {
      case telegram_api::updateMessageID::ID: {
        // in channels.getDifference updateMessageID can't be received for scheduled messages
        auto sent_message_update = move_tl_object_as<telegram_api::updateMessageID>(update_ptr);
        on_update_message_id(sent_message_update->random_id_, MessageId(ServerMessageId(sent_message_update->id_)),
                             "get_channel_difference");
        update_ptr = nullptr;
        break;
      }
      case telegram_api::updateDeleteChannelMessages::ID: {
        auto *update = static_cast<const telegram_api::updateDeleteChannelMessages *>(update_ptr.get());
        if (DialogId(ChannelId(update->channel_id_)) != dialog_id) {
          is_good_update = false;
        } else {
          for (auto &message : update->messages_) {
            changed_message_ids.insert(MessageId(ServerMessageId(message)));
          }
        }
        break;
      }
      case telegram_api::updateEditChannelMessage::ID: {
        auto *update = static_cast<const telegram_api::updateEditChannelMessage *>(update_ptr.get());
        auto full_message_id = get_full_message_id(update->message_, false);
        if (full_message_id.get_dialog_id() != dialog_id) {
          is_good_update = false;
        } else {
          changed_message_ids.insert(full_message_id.get_message_id());
        }
        break;
      }
      case telegram_api::updatePinnedChannelMessages::ID: {
        auto *update = static_cast<const telegram_api::updatePinnedChannelMessages *>(update_ptr.get());
        if (DialogId(ChannelId(update->channel_id_)) != dialog_id) {
          is_good_update = false;
        }
        break;
      }
      default:
        is_good_update = false;
        break;
    }
    if (!is_good_update) {
      LOG(ERROR) << "Receive wrong update in channelDifference of " << dialog_id << ": " << to_string(update_ptr);
      update_ptr = nullptr;
    }
  }

  auto is_edited_message = [](const tl_object_ptr<telegram_api::Message> &message) {
    if (message->get_id() != telegram_api::message::ID) {
      return false;
    }
    return static_cast<const telegram_api::message *>(message.get())->edit_date_ > 0;
  };

  for (auto &message : new_messages) {
    if (is_edited_message(message)) {
      changed_message_ids.insert(get_message_id(message, false));
    }
  }

  // extract awaited sent messages, which were edited or deleted after that
  auto postponed_updates_it = postponed_channel_updates_.find(dialog_id);
  struct AwaitedMessage {
    tl_object_ptr<telegram_api::Message> message;
    Promise<Unit> promise;
  };
  std::map<MessageId, AwaitedMessage> awaited_messages;
  if (postponed_updates_it != postponed_channel_updates_.end()) {
    auto &updates = postponed_updates_it->second;
    while (!updates.empty()) {
      auto it = updates.begin();
      auto update_pts = it->second.pts;
      if (update_pts > new_pts) {
        break;
      }

      auto update = std::move(it->second.update);
      auto promise = std::move(it->second.promise);
      updates.erase(it);

      if (update->get_id() == telegram_api::updateNewChannelMessage::ID) {
        auto update_new_channel_message = static_cast<telegram_api::updateNewChannelMessage *>(update.get());
        auto message_id = get_message_id(update_new_channel_message->message_, false);
        FullMessageId full_message_id(dialog_id, message_id);
        if (update_message_ids_.find(full_message_id) != update_message_ids_.end() &&
            changed_message_ids.find(message_id) != changed_message_ids.end()) {
          changed_message_ids.erase(message_id);
          AwaitedMessage awaited_message;
          awaited_message.message = std::move(update_new_channel_message->message_);
          awaited_message.promise = std::move(promise);
          awaited_messages.emplace(message_id, std::move(awaited_message));
          continue;
        }
      }

      LOG(INFO) << "Skip to be applied from getChannelDifference " << to_string(update);
      promise.set_value(Unit());
    }
    if (updates.empty()) {
      postponed_channel_updates_.erase(postponed_updates_it);
    }
  }

  // if last message is pretty old, we might have missed the update
  bool need_repair_unread_count =
      !new_messages.empty() && get_message_date(new_messages[0]) < G()->unix_time() - 2 * 86400;

  auto it = awaited_messages.begin();
  for (auto &message : new_messages) {
    auto message_id = get_message_id(message, false);
    while (it != awaited_messages.end() && it->first < message_id) {
      on_get_message(std::move(it->second.message), true, true, false, true, true, "postponed channel update");
      it->second.promise.set_value(Unit());
      ++it;
    }
    Promise<Unit> promise;
    if (it != awaited_messages.end() && it->first == message_id) {
      if (is_edited_message(message)) {
        // the new message is edited, apply postponed one and move this to updateEditChannelMessage
        other_updates.push_back(make_tl_object<telegram_api::updateEditChannelMessage>(std::move(message), new_pts, 0));
        message = std::move(it->second.message);
        promise = std::move(it->second.promise);
      } else {
        it->second.promise.set_value(Unit());
      }
      ++it;
    }
    on_get_message(std::move(message), true, true, false, true, true, "get channel difference");
    promise.set_value(Unit());
  }
  while (it != awaited_messages.end()) {
    on_get_message(std::move(it->second.message), true, true, false, true, true, "postponed channel update 2");
    it->second.promise.set_value(Unit());
    ++it;
  }

  for (auto &update : other_updates) {
    if (update != nullptr) {
      process_channel_update(std::move(update));
    }
  }
  LOG_CHECK(!running_get_channel_difference(dialog_id)) << '"' << active_get_channel_differencies_[dialog_id] << '"';

  if (need_repair_unread_count) {
    repair_channel_server_unread_count(get_dialog(dialog_id));
  }

  CHECK(debug_channel_difference_dialog_ == dialog_id);
  debug_channel_difference_dialog_ = DialogId();
}

class SaveDefaultSendAsActor final : public NetActorOnce {
  Promise<Unit> promise_;

 public:
  explicit SaveDefaultSendAsActor(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, DialogId send_as_dialog_id, uint64 sequence_dispatcher_id) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    auto send_as_input_peer = td_->messages_manager_->get_input_peer(send_as_dialog_id, AccessRights::Read);
    CHECK(send_as_input_peer != nullptr);

    auto query = G()->net_query_creator().create(
        telegram_api::messages_saveDefaultSendAs(std::move(input_peer), std::move(send_as_input_peer)));
    query->debug("send to MessagesManager::MultiSequenceDispatcher");
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_saveDefaultSendAs>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto success = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SaveDefaultSendAsActor: " << success;

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    // td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SaveDefaultSendAsActor");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::set_dialog_default_send_message_as_dialog_id(DialogId dialog_id,
                                                                   DialogId message_sender_dialog_id,
                                                                   Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "set_dialog_default_send_message_as_dialog_id");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!d->default_send_message_as_dialog_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Can't change message sender in the chat"));
  }
  // checked in on_update_dialog_default_send_message_as_dialog_id
  CHECK(dialog_id.get_type() == DialogType::Channel && !is_broadcast_channel(dialog_id));
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  bool is_anonymous = is_anonymous_administrator(dialog_id, nullptr);
  switch (message_sender_dialog_id.get_type()) {
    case DialogType::User:
      if (message_sender_dialog_id != DialogId(td_->contacts_manager_->get_my_id())) {
        return promise.set_error(Status::Error(400, "Can't send messages as another user"));
      }
      if (is_anonymous) {
        return promise.set_error(Status::Error(400, "Can't send messages as self"));
      }
      break;
    case DialogType::Chat:
    case DialogType::Channel:
    case DialogType::SecretChat:
      if (is_anonymous && dialog_id == message_sender_dialog_id) {
        break;
      }
      if (!is_broadcast_channel(message_sender_dialog_id) ||
          td_->contacts_manager_->get_channel_username(message_sender_dialog_id.get_channel_id()).empty()) {
        return promise.set_error(Status::Error(400, "Message sender chat must be a public channel"));
      }
      break;
    default:
      return promise.set_error(Status::Error(400, "Invalid message sender specified"));
  }
  if (!have_input_peer(message_sender_dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access specified message sender chat"));
  }

  {
    auto it = set_typing_query_.find(dialog_id);
    if (it != set_typing_query_.end()) {
      if (!it->second.empty()) {
        cancel_query(it->second);
      }
      set_typing_query_.erase(it);
    }
  }

  // TODO save order with all types of messages
  send_closure(td_->create_net_actor<SaveDefaultSendAsActor>(std::move(promise)), &SaveDefaultSendAsActor::send,
               dialog_id, message_sender_dialog_id, get_sequence_dispatcher_id(dialog_id, MessageContentType::Text));

  on_update_dialog_default_send_message_as_dialog_id(dialog_id, message_sender_dialog_id, true);
}

void MessagesManager::delete_dialog_messages_by_sender(DialogId dialog_id, DialogId sender_dialog_id,
                                                       Promise<Unit> &&promise) {
  bool is_bot = td_->auth_manager_->is_bot();
  CHECK(!is_bot);

  Dialog *d = get_dialog_force(dialog_id, "delete_dialog_messages_by_sender");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Write)) {
    return promise.set_error(Status::Error(400, "Not enough rights"));
  }

  if (!have_input_peer(sender_dialog_id, AccessRights::Know)) {
    return promise.set_error(Status::Error(400, "Message sender not found"));
  }

  ChannelId channel_id;
  DialogParticipantStatus channel_status = DialogParticipantStatus::Left();
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::SecretChat:
      return promise.set_error(
          Status::Error(400, "All messages from a sender can be deleted only in supergroup chats"));
    case DialogType::Channel: {
      channel_id = dialog_id.get_channel_id();
      auto channel_type = td_->contacts_manager_->get_channel_type(channel_id);
      if (channel_type != ContactsManager::ChannelType::Megagroup) {
        return promise.set_error(Status::Error(400, "The method is available only for supergroup chats"));
      }
      channel_status = td_->contacts_manager_->get_channel_permissions(channel_id);
      if (!channel_status.can_delete_messages()) {
        return promise.set_error(Status::Error(400, "Need delete messages administator right in the supergroup chat"));
      }
      channel_id = dialog_id.get_channel_id();
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
      break;
  }
  CHECK(channel_id.is_valid());

  if (sender_dialog_id.get_type() == DialogType::SecretChat) {
    return promise.set_value(Unit());
  }

  if (G()->parameters().use_message_db) {
    LOG(INFO) << "Delete all messages from " << sender_dialog_id << " in " << dialog_id << " from database";
    G()->td_db()->get_messages_db_async()->delete_dialog_messages_by_sender(dialog_id, sender_dialog_id,
                                                                            Auto());  // TODO Promise
  }

  vector<MessageId> message_ids;
  find_messages(d->messages.get(), message_ids,
                [sender_dialog_id](const Message *m) { return sender_dialog_id == get_message_sender(m); });

  vector<int64> deleted_message_ids;
  bool need_update_dialog_pos = false;
  for (auto message_id : message_ids) {
    auto m = get_message(d, message_id);
    CHECK(m != nullptr);
    CHECK(m->message_id == message_id);
    if (can_delete_channel_message(channel_status, m, is_bot)) {
      auto p = delete_message(d, message_id, true, &need_update_dialog_pos, "delete_dialog_messages_by_sender");
      CHECK(p.get() == m);
      deleted_message_ids.push_back(p->message_id.get());
    }
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "delete_dialog_messages_by_sender");
  }

  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);

  delete_all_channel_messages_by_sender_on_server(channel_id, sender_dialog_id, 0, std::move(promise));
}

void MessagesManager::fix_forwarded_message(Message *m, DialogId to_dialog_id, const Message *forwarded_message,
                                            int64 media_album_id) const {
  m->via_bot_user_id = forwarded_message->via_bot_user_id;
  m->media_album_id = media_album_id;
  if (forwarded_message->view_count > 0 && m->forward_info != nullptr && m->view_count == 0 &&
      !(m->message_id.is_scheduled() && is_broadcast_channel(to_dialog_id))) {
    m->view_count = forwarded_message->view_count;
    m->forward_count = forwarded_message->forward_count;
    m->interaction_info_update_date = G()->unix_time();
  }

  if (m->content->get_type() == MessageContentType::Game) {
    // via_bot_user_id in games is present unless the message is sent by the bot
    if (m->via_bot_user_id == UserId()) {
      // if there is no via_bot_user_id, then the original message was sent by the game owner
      m->via_bot_user_id = forwarded_message->sender_user_id;
    }
    if (m->via_bot_user_id == td_->contacts_manager_->get_my_id()) {
      // if via_bot_user_id is the current bot user, then there should be
      m->via_bot_user_id = UserId();
    }
  }
  if (forwarded_message->reply_markup != nullptr &&
      forwarded_message->reply_markup->type == ReplyMarkup::Type::InlineKeyboard &&
      to_dialog_id.get_type() != DialogType::SecretChat) {
    bool need_reply_markup = true;
    for (auto &row : forwarded_message->reply_markup->inline_keyboard) {
      for (auto &button : row) {
        if (button.type == InlineKeyboardButton::Type::Url || button.type == InlineKeyboardButton::Type::UrlAuth) {
          // ok
          continue;
        }
        if (m->via_bot_user_id.is_valid() && (button.type == InlineKeyboardButton::Type::SwitchInline ||
                                              button.type == InlineKeyboardButton::Type::SwitchInlineCurrentDialog)) {
          // ok
          continue;
        }

        need_reply_markup = false;
      }
    }
    if (need_reply_markup) {
      m->reply_markup = make_unique<ReplyMarkup>(*forwarded_message->reply_markup);
      for (auto &row : m->reply_markup->inline_keyboard) {
        for (auto &button : row) {
          if (button.type == InlineKeyboardButton::Type::SwitchInlineCurrentDialog) {
            button.type = InlineKeyboardButton::Type::SwitchInline;
          }
          if (!button.forward_text.empty()) {
            button.text = std::move(button.forward_text);
            button.forward_text.clear();
          }
        }
      }
    }
  }
}

void MessagesManager::on_send_media_group_file_reference_error(DialogId dialog_id, vector<int64> random_ids) {
  int64 media_album_id = 0;
  vector<MessageId> message_ids;
  vector<Message *> messages;
  for (auto &random_id : random_ids) {
    auto it = being_sent_messages_.find(random_id);
    if (it == being_sent_messages_.end()) {
      // we can't receive fail more than once
      // but message can be successfully sent before
      LOG(ERROR) << "Receive file reference invalid error about successfully sent message with random_id = "
                 << random_id;
      continue;
    }

    auto full_message_id = it->second;

    being_sent_messages_.erase(it);

    Message *m = get_message(full_message_id);
    if (m == nullptr) {
      // message has already been deleted by the user or sent to inaccessible channel
      // don't need to send error to the user, because the message has already been deleted
      // and there is nothing to be deleted from the server
      LOG(INFO) << "Fail to send already deleted by the user or sent to inaccessible chat " << full_message_id;
      continue;
    }

    CHECK(m->media_album_id != 0);
    CHECK(media_album_id == 0 || media_album_id == m->media_album_id);
    media_album_id = m->media_album_id;

    CHECK(dialog_id == full_message_id.get_dialog_id());
    message_ids.push_back(m->message_id);
    messages.push_back(m);
  }

  CHECK(dialog_id.get_type() != DialogType::SecretChat);

  if (message_ids.empty()) {
    // all messages was deleted, nothing to do
    return;
  }

  auto &request = pending_message_group_sends_[media_album_id];
  CHECK(!request.dialog_id.is_valid());
  CHECK(request.finished_count == 0);
  CHECK(request.results.empty());
  request.dialog_id = dialog_id;
  request.message_ids = std::move(message_ids);
  request.is_finished.resize(request.message_ids.size(), false);
  for (size_t i = 0; i < request.message_ids.size(); i++) {
    request.results.push_back(Status::OK());
  }

  for (auto m : messages) {
    do_send_message(dialog_id, m, {-1});
  }
}

void MessagesManager::process_discussion_message(
    telegram_api::object_ptr<telegram_api::messages_discussionMessage> &&result, DialogId dialog_id,
    MessageId message_id, DialogId expected_dialog_id, MessageId expected_message_id,
    Promise<MessageThreadInfo> promise) {
  LOG(INFO) << "Receive discussion message for " << message_id << " in " << dialog_id << ": " << to_string(result);
  td_->contacts_manager_->on_get_users(std::move(result->users_), "process_discussion_message");
  td_->contacts_manager_->on_get_chats(std::move(result->chats_), "process_discussion_message");

  for (auto &message : result->messages_) {
    auto message_dialog_id = get_message_dialog_id(message);
    if (message_dialog_id != expected_dialog_id) {
      return promise.set_error(Status::Error(500, "Expected messages in a different chat"));
    }
  }

  for (auto &message : result->messages_) {
    if (need_channel_difference_to_add_message(expected_dialog_id, message)) {
      return run_after_channel_difference(
          expected_dialog_id, PromiseCreator::lambda([actor_id = actor_id(this), result = std::move(result), dialog_id,
                                                      message_id, expected_dialog_id, expected_message_id,
                                                      promise = std::move(promise)](Unit ignored) mutable {
            send_closure(actor_id, &MessagesManager::process_discussion_message_impl, std::move(result), dialog_id,
                         message_id, expected_dialog_id, expected_message_id, std::move(promise));
          }));
    }
  }

  process_discussion_message_impl(std::move(result), dialog_id, message_id, expected_dialog_id, expected_message_id,
                                  std::move(promise));
}

class GetScopeNotifySettingsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  NotificationSettingsScope scope_;

 public:
  explicit GetScopeNotifySettingsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(NotificationSettingsScope scope) {
    scope_ = scope;
    auto input_notify_peer = get_input_notify_peer(scope);
    CHECK(input_notify_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::account_getNotifySettings(std::move(input_notify_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_getNotifySettings>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    td_->messages_manager_->on_update_scope_notify_settings(scope_, std::move(ptr));

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::send_get_scope_notification_settings_query(NotificationSettingsScope scope,
                                                                 Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    LOG(ERROR) << "Can't get notification settings for " << scope;
    return promise.set_error(Status::Error(500, "Wrong getScopeNotificationSettings query"));
  }

  td_->create_handler<GetScopeNotifySettingsQuery>(std::move(promise))->send(scope);
}

void MessagesManager::on_messages_db_fts_result(Result<MessagesDbFtsResult> result, string offset, int32 limit,
                                                int64 random_id, Promise<Unit> &&promise) {
  if (G()->close_flag()) {
    result = Global::request_aborted_error();
  }
  if (result.is_error()) {
    found_fts_messages_.erase(random_id);
    return promise.set_error(result.move_as_error());
  }
  auto fts_result = result.move_as_ok();

  auto it = found_fts_messages_.find(random_id);
  CHECK(it != found_fts_messages_.end());
  auto &res = it->second.full_message_ids;

  res.reserve(fts_result.messages.size());
  for (auto &message : fts_result.messages) {
    auto m = on_get_message_from_database(message, false, "on_messages_db_fts_result");
    if (m != nullptr) {
      res.emplace_back(message.dialog_id, m->message_id);
    }
  }

  it->second.next_offset = fts_result.next_search_id <= 1 ? string() : to_string(fts_result.next_search_id);
  it->second.total_count = offset.empty() && fts_result.messages.size() < static_cast<size_t>(limit)
                               ? static_cast<int32>(fts_result.messages.size())
                               : -1;

  promise.set_value(Unit());
}

bool MessagesManager::can_resend_message(const Message *m) const {
  if (m->send_error_code != 429 && m->send_error_message != "Message is too old to be re-sent automatically" &&
      m->send_error_message != "SCHEDULE_TOO_MUCH" && m->send_error_message != "SEND_AS_PEER_INVALID") {
    return false;
  }
  if (m->is_bot_start_message) {
    return false;
  }
  if (m->forward_info != nullptr || m->real_forward_from_dialog_id.is_valid()) {
    // TODO implement resending of forwarded messages
    return false;
  }
  auto content_type = m->content->get_type();
  if (m->via_bot_user_id.is_valid() || m->hide_via_bot) {
    // via bot message
    if (!can_have_input_media(td_, m->content.get())) {
      return false;
    }

    // resend via_bot message as an ordinary message if error code is 429
    // TODO support other error codes
  }

  if (content_type == MessageContentType::ChatSetTtl || content_type == MessageContentType::ScreenshotTaken) {
    // TODO implement resending of ChatSetTtl and ScreenshotTaken messages
    return false;
  }
  return true;
}

Result<td_api::object_ptr<td_api::message>> MessagesManager::forward_message(
    DialogId to_dialog_id, DialogId from_dialog_id, MessageId message_id,
    tl_object_ptr<td_api::messageSendOptions> &&options, bool in_game_share, MessageCopyOptions &&copy_options) {
  bool need_copy = copy_options.send_copy;
  vector<MessageCopyOptions> all_copy_options;
  all_copy_options.push_back(std::move(copy_options));
  TRY_RESULT(result, forward_messages(to_dialog_id, from_dialog_id, {message_id}, std::move(options), in_game_share,
                                      std::move(all_copy_options), false));
  CHECK(result->messages_.size() == 1);
  if (result->messages_[0] == nullptr) {
    return Status::Error(400,
                         need_copy ? Slice("The message can't be copied") : Slice("The message can't be forwarded"));
  }
  return std::move(result->messages_[0]);
}

void MessagesManager::send_update_chat_position(DialogListId dialog_list_id, const Dialog *d,
                                                const char *source) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_position";
  LOG(INFO) << "Send updateChatPosition for " << d->dialog_id << " in " << dialog_list_id << " from " << source;
  auto position = get_chat_position_object(dialog_list_id, d);
  if (position == nullptr) {
    position = td_api::make_object<td_api::chatPosition>(dialog_list_id.get_chat_list_object(), 0, false, nullptr);
  }
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatPosition>(d->dialog_id.get(), std::move(position)));
}

void MessagesManager::send_update_chat_draft_message(const Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_draft_message";
  on_dialog_updated(d->dialog_id, "send_update_chat_draft_message");
  if (d->draft_message == nullptr || can_send_message(d->dialog_id).is_ok()) {
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateChatDraftMessage>(
                     d->dialog_id.get(), get_draft_message_object(d->draft_message), get_chat_positions_object(d)));
  }
}

vector<tl_object_ptr<telegram_api::InputDialogPeer>> MessagesManager::get_input_dialog_peers(
    const vector<DialogId> &dialog_ids, AccessRights access_rights) const {
  vector<tl_object_ptr<telegram_api::InputDialogPeer>> input_dialog_peers;
  input_dialog_peers.reserve(dialog_ids.size());
  for (auto &dialog_id : dialog_ids) {
    auto input_dialog_peer = get_input_dialog_peer(dialog_id, access_rights);
    if (input_dialog_peer == nullptr) {
      LOG(ERROR) << "Have no access to " << dialog_id;
      continue;
    }
    input_dialog_peers.push_back(std::move(input_dialog_peer));
  }
  return input_dialog_peers;
}

Status MessagesManager::set_scope_notification_settings(
    NotificationSettingsScope scope, tl_object_ptr<td_api::scopeNotificationSettings> &&notification_settings) {
  CHECK(!td_->auth_manager_->is_bot());
  TRY_RESULT(new_settings, ::td::get_scope_notification_settings(std::move(notification_settings)));
  if (update_scope_notification_settings(scope, get_scope_notification_settings(scope), new_settings)) {
    update_scope_notification_settings_on_server(scope, 0);
  }
  return Status::OK();
}

void MessagesManager::send_update_chat_read_outbox(const Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_read_outbox";
  on_dialog_updated(d->dialog_id, "send_update_chat_read_outbox");
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatReadOutbox>(d->dialog_id.get(), d->last_read_outbox_message_id.get()));
}

void MessagesManager::schedule_dialog_unmute(DialogId dialog_id, bool use_default, int32 mute_until) {
  auto now = G()->unix_time_cached();
  if (!use_default && mute_until >= now && mute_until < now + 366 * 86400) {
    dialog_unmute_timeout_.set_timeout_in(dialog_id.get(), mute_until - now + 1);
  } else {
    dialog_unmute_timeout_.cancel_timeout(dialog_id.get());
  }
}

void MessagesManager::on_message_live_location_viewed_on_server(int64 task_id) {
  if (G()->close_flag()) {
    return;
  }

  auto it = viewed_live_location_tasks_.find(task_id);
  if (it == viewed_live_location_tasks_.end()) {
    return;
  }

  pending_message_live_location_view_timeout_.add_timeout_in(task_id, LIVE_LOCATION_VIEW_PERIOD);
}

int32 MessagesManager::get_message_own_max_media_timestamp(const Message *m) const {
  auto duration = get_message_content_media_duration(m->content.get(), td_);
  return duration == 0 ? std::numeric_limits<int32>::max() : duration;
}

void MessagesManager::on_failed_messages_search(int64 random_id) {
  auto it = found_messages_.find(random_id);
  CHECK(it != found_messages_.end());
  found_messages_.erase(it);
}
}  // namespace td
