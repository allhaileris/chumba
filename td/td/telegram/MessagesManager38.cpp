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
#include "td/telegram/NotificationManager.h"
#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"

#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/TopDialogCategory.h"

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

tl_object_ptr<td_api::messages> MessagesManager::get_dialog_history(DialogId dialog_id, MessageId from_message_id,
                                                                    int32 offset, int32 limit, int left_tries,
                                                                    bool only_local, Promise<Unit> &&promise) {
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return nullptr;
  }
  if (limit > MAX_GET_HISTORY) {
    limit = MAX_GET_HISTORY;
  }
  if (offset > 0) {
    promise.set_error(Status::Error(400, "Parameter offset must be non-positive"));
    return nullptr;
  }
  if (offset <= -MAX_GET_HISTORY) {
    promise.set_error(Status::Error(400, "Parameter offset must be greater than -100"));
    return nullptr;
  }
  if (offset < -limit) {
    promise.set_error(Status::Error(400, "Parameter offset must be greater than or equal to -limit"));
    return nullptr;
  }
  bool is_limit_increased = false;
  if (limit == -offset) {
    limit++;
    is_limit_increased = true;
  }
  CHECK(0 < limit && limit <= MAX_GET_HISTORY);
  CHECK(-limit < offset && offset <= 0);

  if (from_message_id == MessageId() || from_message_id.get() > MessageId::max().get()) {
    from_message_id = MessageId::max();
  }
  if (!from_message_id.is_valid()) {
    promise.set_error(Status::Error(400, "Invalid value of parameter from_message_id specified"));
    return nullptr;
  }

  const Dialog *d = get_dialog_force(dialog_id, "get_dialog_history");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return nullptr;
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return nullptr;
  }

  LOG(INFO) << "Get " << (only_local ? "local " : "") << "history in " << dialog_id << " from " << from_message_id
            << " with offset " << offset << " and limit " << limit << ", " << left_tries
            << " tries left. Last read inbox message is " << d->last_read_inbox_message_id
            << ", last read outbox message is " << d->last_read_outbox_message_id
            << ", have_full_history = " << d->have_full_history;

  MessagesConstIterator p(d, from_message_id);
  LOG(DEBUG) << "Iterator points to " << (*p ? (*p)->message_id : MessageId());
  bool from_the_end = (d->last_message_id != MessageId() && from_message_id > d->last_message_id) ||
                      from_message_id >= MessageId::max();

  if (from_the_end) {
    limit += offset;
    offset = 0;
    if (d->last_message_id == MessageId()) {
      p = MessagesConstIterator();
    }
  } else {
    bool have_a_gap = false;
    if (*p == nullptr) {
      // there is no gap if from_message_id is less than first message in the dialog
      if (left_tries == 0 && d->messages != nullptr && offset < 0) {
        const Message *cur = d->messages.get();
        while (cur->left != nullptr) {
          cur = cur->left.get();
        }
        CHECK(cur->message_id > from_message_id);
        from_message_id = cur->message_id;
        p = MessagesConstIterator(d, from_message_id);
      } else {
        have_a_gap = true;
      }
    } else if ((*p)->message_id != from_message_id) {
      CHECK((*p)->message_id < from_message_id);
      if (!(*p)->have_next && (d->last_message_id == MessageId() || (*p)->message_id < d->last_message_id)) {
        have_a_gap = true;
      }
    }

    if (have_a_gap) {
      LOG(INFO) << "Have a gap near message to get chat history from";
      p = MessagesConstIterator();
    }
    if (*p != nullptr && (*p)->message_id == from_message_id) {
      if (offset < 0) {
        offset++;
      } else {
        --p;
      }
    }

    while (*p != nullptr && offset < 0) {
      ++p;
      if (*p) {
        ++offset;
        from_message_id = (*p)->message_id;
      }
    }

    if (offset < 0 && ((d->last_message_id != MessageId() && from_message_id >= d->last_message_id) ||
                       (!have_a_gap && left_tries == 0))) {
      CHECK(!have_a_gap);
      limit += offset;
      offset = 0;
      p = MessagesConstIterator(d, from_message_id);
    }

    if (!have_a_gap && offset < 0) {
      offset--;
    }
  }

  LOG(INFO) << "Iterator after applying offset points to " << (*p ? (*p)->message_id : MessageId())
            << ", offset = " << offset << ", limit = " << limit << ", from_the_end = " << from_the_end;
  vector<tl_object_ptr<td_api::message>> messages;
  if (*p != nullptr && offset == 0) {
    while (*p != nullptr && messages.size() < static_cast<size_t>(limit)) {
      messages.push_back(get_message_object(dialog_id, *p, "get_dialog_history"));
      from_message_id = (*p)->message_id;
      from_the_end = false;
      --p;
    }
  }

  if (!messages.empty()) {
    // maybe need some messages
    CHECK(offset == 0);
    preload_newer_messages(d, MessageId(messages[0]->id_));
    preload_older_messages(d, MessageId(messages.back()->id_));
  } else if (messages.size() < static_cast<size_t>(limit) && left_tries != 0 &&
             !(d->is_empty && d->have_full_history && left_tries < 3)) {
    // there can be more messages in the database or on the server, need to load them
    if (from_the_end) {
      from_message_id = MessageId();
    }
    send_closure_later(actor_id(this), &MessagesManager::load_messages, dialog_id, from_message_id, offset,
                       limit - static_cast<int32>(messages.size()), left_tries, only_local, std::move(promise));
    return nullptr;
  }

  LOG(INFO) << "Have " << messages.size() << " messages out of requested "
            << (is_limit_increased ? "increased " : "exact ") << limit;
  if (is_limit_increased && static_cast<size_t>(limit) == messages.size()) {
    messages.pop_back();
  }

  LOG(INFO) << "Return " << messages.size() << " messages in result to getChatHistory";
  promise.set_value(Unit());                                   // can return some messages
  return get_messages_object(-1, std::move(messages), false);  // TODO return real total_count of messages in the dialog
}

vector<Notification> MessagesManager::get_message_notifications_from_database_force(Dialog *d, bool from_mentions,
                                                                                    int32 limit) {
  CHECK(d != nullptr);
  if (!G()->parameters().use_message_db || td_->auth_manager_->is_bot()) {
    return {};
  }

  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  auto from_notification_id = NotificationId::max();
  auto from_message_id = MessageId::max();
  vector<Notification> res;
  if (!from_mentions && from_message_id <= d->last_read_inbox_message_id) {
    return res;
  }
  while (true) {
    auto result = do_get_message_notifications_from_database_force(d, from_mentions, from_notification_id,
                                                                   from_message_id, limit);
    if (result.is_error()) {
      break;
    }
    auto messages = result.move_as_ok();
    if (messages.empty()) {
      break;
    }

    bool is_found = false;
    VLOG(notifications) << "Loaded " << messages.size() << (from_mentions ? " mention" : "")
                        << " messages with notifications from database in " << group_info.group_id << '/'
                        << d->dialog_id;
    for (auto &message : messages) {
      auto m = on_get_message_from_database(d, message, false, "get_message_notifications_from_database_force");
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
      if (notification_id.get() >= from_notification_id.get()) {
        // possible if two messages have the same notification_id
        LOG(ERROR) << "Have nonmonotonic notification identifiers: " << d->dialog_id << " " << m->message_id << " "
                   << notification_id << " " << from_message_id << " " << from_notification_id;
        is_correct = false;
      } else {
        from_notification_id = notification_id;
        is_found = true;
      }
      if (m->message_id >= from_message_id) {
        LOG(ERROR) << "Have nonmonotonic message identifiers: " << d->dialog_id << " " << m->message_id << " "
                   << notification_id << " " << from_message_id << " " << from_notification_id;
        is_correct = false;
      } else {
        from_message_id = m->message_id;
        is_found = true;
      }

      if (notification_id.get() <= group_info.max_removed_notification_id.get() ||
          m->message_id <= group_info.max_removed_message_id ||
          (!from_mentions && m->message_id <= d->last_read_inbox_message_id)) {
        // if message still has notification_id, but it was removed via max_removed_notification_id,
        // or max_removed_message_id, or last_read_inbox_message_id,
        // then there will be no more messages with active notifications
        is_found = false;
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
                            << " from another group";
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
        res.emplace_back(m->notification_id, m->date, m->disable_notification,
                         create_new_message_notification(m->message_id));
      } else {
        remove_message_notification_id(d, m, true, false);
        on_message_changed(d, m, false, "get_message_notifications_from_database_force");
      }
    }
    if (!res.empty() || !is_found) {
      break;
    }
  }
  if (from_mentions) {
    try_add_pinned_message_notification(d, res, NotificationId::max(), limit);
  }
  return res;
}

unique_ptr<MessagesManager::MessageForwardInfo> MessagesManager::get_message_forward_info(
    tl_object_ptr<telegram_api::messageFwdHeader> &&forward_header) {
  if (forward_header == nullptr) {
    return nullptr;
  }

  if (forward_header->date_ <= 0) {
    LOG(ERROR) << "Wrong date in message forward header: " << oneline(to_string(forward_header));
    return nullptr;
  }

  auto flags = forward_header->flags_;
  DialogId sender_dialog_id;
  MessageId message_id;
  string author_signature = std::move(forward_header->post_author_);
  DialogId from_dialog_id;
  MessageId from_message_id;
  string sender_name = std::move(forward_header->from_name_);
  bool is_imported = forward_header->imported_;
  if (forward_header->from_id_ != nullptr) {
    sender_dialog_id = DialogId(forward_header->from_id_);
    if (!sender_dialog_id.is_valid()) {
      LOG(ERROR) << "Receive invalid sender identifier in message forward header: "
                 << oneline(to_string(forward_header));
      sender_dialog_id = DialogId();
    }
  }
  if ((flags & telegram_api::messageFwdHeader::CHANNEL_POST_MASK) != 0) {
    message_id = MessageId(ServerMessageId(forward_header->channel_post_));
    if (!message_id.is_valid()) {
      LOG(ERROR) << "Receive " << message_id << " in message forward header: " << oneline(to_string(forward_header));
      message_id = MessageId();
    }
  }
  if (forward_header->saved_from_peer_ != nullptr) {
    from_dialog_id = DialogId(forward_header->saved_from_peer_);
    from_message_id = MessageId(ServerMessageId(forward_header->saved_from_msg_id_));
    if (!from_dialog_id.is_valid() || !from_message_id.is_valid()) {
      LOG(ERROR) << "Receive " << from_message_id << " in " << from_dialog_id
                 << " in message forward header: " << oneline(to_string(forward_header));
      from_dialog_id = DialogId();
      from_message_id = MessageId();
    }
  }

  UserId sender_user_id;
  if (sender_dialog_id.get_type() == DialogType::User) {
    sender_user_id = sender_dialog_id.get_user_id();
    sender_dialog_id = DialogId();
  }
  if (!sender_dialog_id.is_valid()) {
    if (sender_user_id.is_valid()) {
      if (message_id.is_valid()) {
        LOG(ERROR) << "Receive non-empty message identifier in message forward header: "
                   << oneline(to_string(forward_header));
        message_id = MessageId();
      }
    } else if (sender_name.empty()) {
      LOG(ERROR) << "Receive wrong message forward header: " << oneline(to_string(forward_header));
      return nullptr;
    }
  } else if (sender_dialog_id.get_type() != DialogType::Channel) {
    LOG(ERROR) << "Receive wrong message forward header with non-channel sender: "
               << oneline(to_string(forward_header));
    return nullptr;
  } else {
    auto channel_id = sender_dialog_id.get_channel_id();
    LOG_IF(ERROR, td_->contacts_manager_->have_min_channel(channel_id)) << "Receive forward from min " << channel_id;
    force_create_dialog(sender_dialog_id, "message forward info", true);
    CHECK(!sender_user_id.is_valid());
  }
  if (from_dialog_id.is_valid()) {
    force_create_dialog(from_dialog_id, "message forward from info", true);
  }

  return td::make_unique<MessageForwardInfo>(sender_user_id, forward_header->date_, sender_dialog_id, message_id,
                                             std::move(author_signature), std::move(sender_name), from_dialog_id,
                                             from_message_id, std::move(forward_header->psa_type_), is_imported);
}

void MessagesManager::do_get_message_notifications_from_database(Dialog *d, bool from_mentions,
                                                                 NotificationId initial_from_notification_id,
                                                                 NotificationId from_notification_id,
                                                                 MessageId from_message_id, int32 limit,
                                                                 Promise<vector<Notification>> promise) {
  CHECK(G()->parameters().use_message_db);
  CHECK(!from_message_id.is_scheduled());

  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  if (from_notification_id.get() <= group_info.max_removed_notification_id.get() ||
      from_message_id <= group_info.max_removed_message_id ||
      (!from_mentions && from_message_id <= d->last_read_inbox_message_id)) {
    return promise.set_value(vector<Notification>());
  }

  auto dialog_id = d->dialog_id;
  auto new_promise =
      PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, from_mentions, initial_from_notification_id, limit,
                              promise = std::move(promise)](Result<vector<MessagesDbDialogMessage>> result) mutable {
        send_closure(actor_id, &MessagesManager::on_get_message_notifications_from_database, dialog_id, from_mentions,
                     initial_from_notification_id, limit, std::move(result), std::move(promise));
      });

  auto *db = G()->td_db()->get_messages_db_async();
  if (!from_mentions) {
    VLOG(notifications) << "Trying to load " << limit << " messages with notifications in " << group_info.group_id
                        << '/' << dialog_id << " from " << from_notification_id;
    return db->get_messages_from_notification_id(d->dialog_id, from_notification_id, limit, std::move(new_promise));
  } else {
    VLOG(notifications) << "Trying to load " << limit << " messages with unread mentions in " << group_info.group_id
                        << '/' << dialog_id << " from " << from_message_id;

    // ignore first_db_message_id, notifications can be nonconsecutive
    MessagesDbMessagesQuery db_query;
    db_query.dialog_id = dialog_id;
    db_query.filter = MessageSearchFilter::UnreadMention;
    db_query.from_message_id = from_message_id;
    db_query.offset = 0;
    db_query.limit = limit;
    return db->get_messages(db_query, std::move(new_promise));
  }
}

void MessagesManager::update_top_dialogs(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  auto dialog_type = dialog_id.get_type();
  if (td_->auth_manager_->is_bot() || (!m->is_outgoing && dialog_id != get_my_dialog_id()) ||
      dialog_type == DialogType::SecretChat || !m->message_id.is_any_server()) {
    return;
  }

  bool is_forward = m->forward_info != nullptr || m->had_forward_info;
  if (m->via_bot_user_id.is_valid() && !is_forward) {
    // forwarded game messages can't be distinguished from sent via bot game messages, so increase rating anyway
    on_dialog_used(TopDialogCategory::BotInline, DialogId(m->via_bot_user_id), m->date);
  }

  if (is_forward) {
    auto &last_forward_date = last_outgoing_forwarded_message_date_[dialog_id];
    if (last_forward_date < m->date) {
      TopDialogCategory category =
          dialog_type == DialogType::User ? TopDialogCategory::ForwardUsers : TopDialogCategory::ForwardChats;
      on_dialog_used(category, dialog_id, m->date);
      last_forward_date = m->date;
    }
  }

  TopDialogCategory category = TopDialogCategory::Size;
  switch (dialog_type) {
    case DialogType::User: {
      if (td_->contacts_manager_->is_user_bot(dialog_id.get_user_id())) {
        category = TopDialogCategory::BotPM;
      } else {
        category = TopDialogCategory::Correspondent;
      }
      break;
    }
    case DialogType::Chat:
      category = TopDialogCategory::Group;
      break;
    case DialogType::Channel:
      switch (td_->contacts_manager_->get_channel_type(dialog_id.get_channel_id())) {
        case ContactsManager::ChannelType::Broadcast:
          category = TopDialogCategory::Channel;
          break;
        case ContactsManager::ChannelType::Megagroup:
          category = TopDialogCategory::Group;
          break;
        case ContactsManager::ChannelType::Unknown:
          break;
        default:
          UNREACHABLE();
          break;
      }
      break;
    case DialogType::SecretChat:
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  if (category != TopDialogCategory::Size) {
    on_dialog_used(category, dialog_id, m->date);
  }
}

Status MessagesManager::toggle_message_sender_is_blocked(const td_api::object_ptr<td_api::MessageSender> &sender,
                                                         bool is_blocked) {
  TRY_RESULT(dialog_id, get_message_sender_dialog_id(td_, sender, true, false));
  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (dialog_id == get_my_dialog_id()) {
        return Status::Error(400, is_blocked ? Slice("Can't block self") : Slice("Can't unblock self"));
      }
      break;
    case DialogType::Chat:
      return Status::Error(400, "Basic group chats can't be blocked");
    case DialogType::Channel:
      // ok
      break;
    case DialogType::SecretChat: {
      auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      if (!user_id.is_valid() || !td_->contacts_manager_->have_user_force(user_id)) {
        return Status::Error(400, "The secret chat can't be blocked");
      }
      dialog_id = DialogId(user_id);
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  Dialog *d = get_dialog_force(dialog_id, "toggle_message_sender_is_blocked");
  if (!have_input_peer(dialog_id, AccessRights::Know)) {
    return Status::Error(400, "Message sender isn't accessible");
  }
  if (d != nullptr) {
    if (is_blocked == d->is_blocked) {
      return Status::OK();
    }
    set_dialog_is_blocked(d, is_blocked);
  } else {
    CHECK(dialog_id.get_type() == DialogType::User);
    td_->contacts_manager_->on_update_user_is_blocked(dialog_id.get_user_id(), is_blocked);
  }

  toggle_dialog_is_blocked_on_server(dialog_id, is_blocked, 0);
  return Status::OK();
}

void MessagesManager::on_message_deleted(Dialog *d, Message *m, bool is_permanently_deleted, const char *source) {
  // also called for unloaded messages

  if (m->message_id.is_yet_unsent() && m->top_thread_message_id.is_valid()) {
    auto it = d->yet_unsent_thread_message_ids.find(m->top_thread_message_id);
    CHECK(it != d->yet_unsent_thread_message_ids.end());
    auto is_deleted = it->second.erase(m->message_id) > 0;
    CHECK(is_deleted);
    if (it->second.empty()) {
      d->yet_unsent_thread_message_ids.erase(it);
    }
  }

  cancel_send_deleted_message(d->dialog_id, m, is_permanently_deleted);

  CHECK(m->message_id.is_valid());
  switch (d->dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
      if (m->message_id.is_server()) {
        message_id_to_dialog_id_.erase(m->message_id);
      }
      break;
    case DialogType::Channel:
      // nothing to do
      break;
    case DialogType::SecretChat:
      delete_random_id_to_message_id_correspondence(d, m->random_id, m->message_id);
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  ttl_unregister_message(d->dialog_id, m, source);
  ttl_period_unregister_message(d->dialog_id, m);
  delete_bot_command_message_id(d->dialog_id, m->message_id);
  unregister_message_content(td_, m->content.get(), {d->dialog_id, m->message_id}, "on_message_deleted");
  unregister_message_reply(d, m);
  if (m->notification_id.is_valid()) {
    delete_notification_id_to_message_id_correspondence(d, m->notification_id, m->message_id);
  }
}

void MessagesManager::edit_dialog_filter(DialogFilterId dialog_filter_id, td_api::object_ptr<td_api::chatFilter> filter,
                                         Promise<td_api::object_ptr<td_api::chatFilterInfo>> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  auto old_dialog_filter = get_dialog_filter(dialog_filter_id);
  if (old_dialog_filter == nullptr) {
    return promise.set_error(Status::Error(400, "Chat filter not found"));
  }
  CHECK(is_update_chat_filters_sent_);

  auto r_dialog_filter = create_dialog_filter(dialog_filter_id, std::move(filter));
  if (r_dialog_filter.is_error()) {
    return promise.set_error(r_dialog_filter.move_as_error());
  }
  auto new_dialog_filter = r_dialog_filter.move_as_ok();
  CHECK(new_dialog_filter != nullptr);
  auto chat_filter_info = new_dialog_filter->get_chat_filter_info_object();

  if (*new_dialog_filter == *old_dialog_filter) {
    return promise.set_value(std::move(chat_filter_info));
  }

  edit_dialog_filter(std::move(new_dialog_filter), "edit_dialog_filter");
  save_dialog_filters();
  send_update_chat_filters();

  synchronize_dialog_filters();
  promise.set_value(std::move(chat_filter_info));
}

void MessagesManager::register_new_local_message_id(Dialog *d, const Message *m) {
  if (m == nullptr) {
    return;
  }
  if (m->message_id.is_scheduled()) {
    return;
  }
  CHECK(m->message_id.is_local());
  if (m->top_thread_message_id.is_valid() && m->top_thread_message_id != m->message_id) {
    Message *top_m = get_message_force(d, m->top_thread_message_id, "register_new_local_message_id");
    if (top_m != nullptr && top_m->top_thread_message_id == top_m->message_id) {
      auto it = std::lower_bound(top_m->local_thread_message_ids.begin(), top_m->local_thread_message_ids.end(),
                                 m->message_id);
      if (it == top_m->local_thread_message_ids.end() || *it != m->message_id) {
        top_m->local_thread_message_ids.insert(it, m->message_id);
        if (top_m->local_thread_message_ids.size() >= 1000) {
          top_m->local_thread_message_ids.erase(top_m->local_thread_message_ids.begin());
        }
        on_message_changed(d, top_m, false, "register_new_local_message_id");
      }
    }
  }
}

void MessagesManager::delete_secret_messages(SecretChatId secret_chat_id, std::vector<int64> random_ids,
                                             Promise<> promise) {
  LOG(DEBUG) << "On delete messages in " << secret_chat_id << " with random_ids " << random_ids;
  CHECK(secret_chat_id.is_valid());

  DialogId dialog_id(secret_chat_id);
  if (!have_dialog_force(dialog_id, "delete_secret_messages")) {
    LOG(ERROR) << "Ignore delete secret messages in unknown " << dialog_id;
    promise.set_value(Unit());
    return;
  }

  auto pending_secret_message = make_unique<PendingSecretMessage>();
  pending_secret_message->success_promise = std::move(promise);
  pending_secret_message->type = PendingSecretMessage::Type::DeleteMessages;
  pending_secret_message->dialog_id = dialog_id;
  pending_secret_message->random_ids = std::move(random_ids);

  add_secret_message(std::move(pending_secret_message));
}

bool MessagesManager::have_dialog_info(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      UserId user_id = dialog_id.get_user_id();
      return td_->contacts_manager_->have_user(user_id);
    }
    case DialogType::Chat: {
      ChatId chat_id = dialog_id.get_chat_id();
      return td_->contacts_manager_->have_chat(chat_id);
    }
    case DialogType::Channel: {
      ChannelId channel_id = dialog_id.get_channel_id();
      return td_->contacts_manager_->have_channel(channel_id);
    }
    case DialogType::SecretChat: {
      SecretChatId secret_chat_id = dialog_id.get_secret_chat_id();
      return td_->contacts_manager_->have_secret_chat(secret_chat_id);
    }
    case DialogType::None:
    default:
      return false;
  }
}

void MessagesManager::set_dialog_has_scheduled_database_messages_impl(Dialog *d, bool has_scheduled_database_messages) {
  CHECK(d != nullptr);
  if (d->has_scheduled_database_messages == has_scheduled_database_messages) {
    return;
  }

  if (d->has_scheduled_database_messages && d->scheduled_messages != nullptr &&
      !d->scheduled_messages->message_id.is_yet_unsent()) {
    // to prevent race between add_message_to_database and check of has_scheduled_database_messages
    return;
  }

  CHECK(G()->parameters().use_message_db);

  d->has_scheduled_database_messages = has_scheduled_database_messages;
  on_dialog_updated(d->dialog_id, "set_dialog_has_scheduled_database_messages");
}

int32 MessagesManager::get_scope_mute_until(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::SecretChat:
      return users_notification_settings_.mute_until;
    case DialogType::Chat:
      return chats_notification_settings_.mute_until;
    case DialogType::Channel:
      return is_broadcast_channel(dialog_id) ? channels_notification_settings_.mute_until
                                             : chats_notification_settings_.mute_until;
    case DialogType::None:
    default:
      UNREACHABLE();
      return 0;
  }
}

bool MessagesManager::is_dialog_pinned(DialogListId dialog_list_id, DialogId dialog_id) const {
  if (get_dialog_pinned_order(dialog_list_id, dialog_id) != DEFAULT_ORDER) {
    return true;
  }
  if (dialog_list_id.is_filter()) {
    const auto *filter = get_dialog_filter(dialog_list_id.get_filter_id());
    if (filter != nullptr) {
      for (const auto &input_dialog_id : filter->pinned_dialog_ids) {
        if (input_dialog_id.get_dialog_id() == dialog_id) {
          return true;
        }
      }
    }
  }
  return false;
}

bool MessagesManager::need_send_update_chat_position(const DialogPositionInList &old_position,
                                                     const DialogPositionInList &new_position) {
  if (old_position.public_order != new_position.public_order) {
    return true;
  }
  if (old_position.public_order == 0) {
    return false;
  }
  return old_position.is_pinned != new_position.is_pinned || old_position.is_sponsored != new_position.is_sponsored;
}

void MessagesManager::on_channel_get_difference_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::on_channel_get_difference_timeout,
                     DialogId(dialog_id_int));
}

int32 MessagesManager::get_dialog_mute_until(const Dialog *d) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(d != nullptr);
  return d->notification_settings.use_default_mute_until ? get_scope_mute_until(d->dialog_id)
                                                         : d->notification_settings.mute_until;
}

void MessagesManager::update_dialogs_hints(const Dialog *d) {
  if (!td_->auth_manager_->is_bot() && d->order != DEFAULT_ORDER) {
    dialogs_hints_.add(-d->dialog_id.get(), get_dialog_title(d->dialog_id) + ' ' + get_dialog_username(d->dialog_id));
  }
}

int32 MessagesManager::get_next_unload_dialog_delay() const {
  auto delay = get_unload_dialog_delay();
  return Random::fast(delay / 4, delay / 2);
}
}  // namespace td
