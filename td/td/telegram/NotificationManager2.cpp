//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/NotificationManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChannelId.h"
#include "td/telegram/ChatId.h"

#include "td/telegram/ContactsManager.h"
#include "td/telegram/DeviceTokenManager.h"
#include "td/telegram/Document.h"
#include "td/telegram/Document.hpp"


#include "td/telegram/Global.h"
#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/ConnectionCreator.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/Photo.h"
#include "td/telegram/Photo.hpp"
#include "td/telegram/SecretChatId.h"
#include "td/telegram/ServerMessageId.h"
#include "td/telegram/StateManager.h"
#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.h"

#include "td/mtproto/AuthKey.h"
#include "td/mtproto/mtproto_api.h"
#include "td/mtproto/PacketInfo.h"
#include "td/mtproto/Transport.h"

#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"

#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/as.h"
#include "td/utils/base64.h"
#include "td/utils/buffer.h"
#include "td/utils/format.h"
#include "td/utils/Gzip.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/tl_parsers.h"
#include "td/utils/utf8.h"







namespace td {
}  // namespace td
namespace td {

class NotificationManager::AddMessagePushNotificationLogEvent {
 public:
  DialogId dialog_id_;
  MessageId message_id_;
  int64 random_id_;
  UserId sender_user_id_;
  DialogId sender_dialog_id_;
  string sender_name_;
  int32 date_;
  bool is_from_scheduled_;
  bool contains_mention_;
  bool is_silent_;
  string loc_key_;
  string arg_;
  Photo photo_;
  Document document_;
  NotificationId notification_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    bool has_message_id = message_id_.is_valid();
    bool has_random_id = random_id_ != 0;
    bool has_sender = sender_user_id_.is_valid();
    bool has_sender_name = !sender_name_.empty();
    bool has_arg = !arg_.empty();
    bool has_photo = !photo_.is_empty();
    bool has_document = !document_.empty();
    bool has_sender_dialog_id = sender_dialog_id_.is_valid();
    BEGIN_STORE_FLAGS();
    STORE_FLAG(contains_mention_);
    STORE_FLAG(is_silent_);
    STORE_FLAG(has_message_id);
    STORE_FLAG(has_random_id);
    STORE_FLAG(has_sender);
    STORE_FLAG(has_sender_name);
    STORE_FLAG(has_arg);
    STORE_FLAG(has_photo);
    STORE_FLAG(has_document);
    STORE_FLAG(is_from_scheduled_);
    STORE_FLAG(has_sender_dialog_id);
    END_STORE_FLAGS();
    td::store(dialog_id_, storer);
    if (has_message_id) {
      td::store(message_id_, storer);
    }
    if (has_random_id) {
      td::store(random_id_, storer);
    }
    if (has_sender) {
      td::store(sender_user_id_, storer);
    }
    if (has_sender_name) {
      td::store(sender_name_, storer);
    }
    td::store(date_, storer);
    td::store(loc_key_, storer);
    if (has_arg) {
      td::store(arg_, storer);
    }
    if (has_photo) {
      td::store(photo_, storer);
    }
    if (has_document) {
      td::store(document_, storer);
    }
    td::store(notification_id_, storer);
    if (has_sender_dialog_id) {
      td::store(sender_dialog_id_, storer);
    }
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    bool has_message_id;
    bool has_random_id;
    bool has_sender;
    bool has_sender_name;
    bool has_arg;
    bool has_photo;
    bool has_document;
    bool has_sender_dialog_id;
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(contains_mention_);
    PARSE_FLAG(is_silent_);
    PARSE_FLAG(has_message_id);
    PARSE_FLAG(has_random_id);
    PARSE_FLAG(has_sender);
    PARSE_FLAG(has_sender_name);
    PARSE_FLAG(has_arg);
    PARSE_FLAG(has_photo);
    PARSE_FLAG(has_document);
    PARSE_FLAG(is_from_scheduled_);
    PARSE_FLAG(has_sender_dialog_id);
    END_PARSE_FLAGS();
    td::parse(dialog_id_, parser);
    if (has_message_id) {
      td::parse(message_id_, parser);
    }
    if (has_random_id) {
      td::parse(random_id_, parser);
    } else {
      random_id_ = 0;
    }
    if (has_sender) {
      td::parse(sender_user_id_, parser);
    }
    if (has_sender_name) {
      td::parse(sender_name_, parser);
    }
    td::parse(date_, parser);
    td::parse(loc_key_, parser);
    if (has_arg) {
      td::parse(arg_, parser);
    }
    if (has_photo) {
      td::parse(photo_, parser);
    }
    if (has_document) {
      td::parse(document_, parser);
    }
    td::parse(notification_id_, parser);
    if (has_sender_dialog_id) {
      td::parse(sender_dialog_id_, parser);
    }
  }
};

void NotificationManager::add_message_push_notification(DialogId dialog_id, MessageId message_id, int64 random_id,
                                                        UserId sender_user_id, DialogId sender_dialog_id,
                                                        string sender_name, int32 date, bool is_from_scheduled,
                                                        bool contains_mention, bool initial_is_silent, bool is_silent,
                                                        string loc_key, string arg, Photo photo, Document document,
                                                        NotificationId notification_id, uint64 log_event_id,
                                                        Promise<Unit> promise) {
  auto is_pinned = begins_with(loc_key, "PINNED_");
  auto r_info = td_->messages_manager_->get_message_push_notification_info(
      dialog_id, message_id, random_id, sender_user_id, sender_dialog_id, date, is_from_scheduled, contains_mention,
      is_pinned, log_event_id != 0);
  if (r_info.is_error()) {
    VLOG(notifications) << "Don't need message push notification for " << message_id << "/" << random_id << " from "
                        << dialog_id << " sent by " << sender_user_id << "/" << sender_dialog_id << " at " << date
                        << ": " << r_info.error();
    if (log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), log_event_id);
    }
    if (r_info.error().code() == 406) {
      promise.set_error(r_info.move_as_error());
    } else {
      promise.set_error(Status::Error(200, "Immediate success"));
    }
    return;
  }

  auto info = r_info.move_as_ok();
  CHECK(info.group_id.is_valid());

  if (dialog_id.get_type() == DialogType::SecretChat) {
    VLOG(notifications) << "Skip notification in secret " << dialog_id;
    // TODO support secret chat notifications
    // main problem: there is no message_id yet
    // also don't forget to delete newSecretChat notification
    CHECK(log_event_id == 0);
    return promise.set_error(Status::Error(406, "Secret chat push notifications are unsupported"));
  }
  CHECK(random_id == 0);

  if (is_disabled() || max_notification_group_count_ == 0) {
    CHECK(log_event_id == 0);
    return promise.set_error(Status::Error(200, "Immediate success"));
  }

  if (!notification_id.is_valid()) {
    CHECK(log_event_id == 0);
    notification_id = get_next_notification_id();
    if (!notification_id.is_valid()) {
      return promise.set_value(Unit());
    }
  } else {
    CHECK(log_event_id != 0);
  }

  if (sender_user_id.is_valid() && !td_->contacts_manager_->have_user_force(sender_user_id)) {
    int32 flags = USER_FLAG_IS_INACCESSIBLE;
    auto user_name = sender_user_id.get() == 136817688 ? "Channel" : sender_name;
    auto user = telegram_api::make_object<telegram_api::user>(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        sender_user_id.get(), 0, user_name, string(), string(), string(), nullptr, nullptr, 0, Auto(), string(),
        string());
    td_->contacts_manager_->on_get_user(std::move(user), "add_message_push_notification");
  }

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    AddMessagePushNotificationLogEvent log_event{
        dialog_id, message_id,        random_id,        sender_user_id,    sender_dialog_id, sender_name,
        date,      is_from_scheduled, contains_mention, initial_is_silent, loc_key,          arg,
        photo,     document,          notification_id};
    log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::AddMessagePushNotification,
                              get_log_event_storer(log_event));
  }

  auto group_id = info.group_id;
  CHECK(group_id.is_valid());

  bool is_outgoing =
      sender_user_id.is_valid() ? td_->contacts_manager_->get_my_id() == sender_user_id : is_from_scheduled;
  if (log_event_id != 0) {
    VLOG(notifications) << "Register temporary " << notification_id << " with log event " << log_event_id;
    temporary_notification_log_event_ids_[notification_id] = log_event_id;
    temporary_notifications_[FullMessageId(dialog_id, message_id)] = {group_id,         notification_id, sender_user_id,
                                                                      sender_dialog_id, sender_name,     is_outgoing};
    temporary_notification_message_ids_[notification_id] = FullMessageId(dialog_id, message_id);
  }
  push_notification_promises_[notification_id].push_back(std::move(promise));

  auto group_type = info.group_type;
  auto settings_dialog_id = info.settings_dialog_id;
  VLOG(notifications) << "Add message push " << notification_id << " of type " << loc_key << " for " << message_id
                      << "/" << random_id << " in " << dialog_id << ", sent by " << sender_user_id << "/"
                      << sender_dialog_id << "/\"" << sender_name << "\" at " << date << " with arg " << arg
                      << ", photo " << photo << " and document " << document << " to " << group_id << " of type "
                      << group_type << " with settings from " << settings_dialog_id;

  add_notification(
      group_id, group_type, dialog_id, date, settings_dialog_id, initial_is_silent, is_silent, 0, notification_id,
      create_new_push_message_notification(sender_user_id, sender_dialog_id, sender_name, is_outgoing, message_id,
                                           std::move(loc_key), std::move(arg), std::move(photo), std::move(document)),
      "add_message_push_notification");
}

class NotificationManager::EditMessagePushNotificationLogEvent {
 public:
  DialogId dialog_id_;
  MessageId message_id_;
  int32 edit_date_;
  string loc_key_;
  string arg_;
  Photo photo_;
  Document document_;

  template <class StorerT>
  void store(StorerT &storer) const {
    bool has_message_id = message_id_.is_valid();
    bool has_arg = !arg_.empty();
    bool has_photo = !photo_.is_empty();
    bool has_document = !document_.empty();
    BEGIN_STORE_FLAGS();
    STORE_FLAG(has_message_id);
    STORE_FLAG(has_arg);
    STORE_FLAG(has_photo);
    STORE_FLAG(has_document);
    END_STORE_FLAGS();
    td::store(dialog_id_, storer);
    if (has_message_id) {
      td::store(message_id_, storer);
    }
    td::store(edit_date_, storer);
    td::store(loc_key_, storer);
    if (has_arg) {
      td::store(arg_, storer);
    }
    if (has_photo) {
      td::store(photo_, storer);
    }
    if (has_document) {
      td::store(document_, storer);
    }
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    bool has_message_id;
    bool has_arg;
    bool has_photo;
    bool has_document;
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(has_message_id);
    PARSE_FLAG(has_arg);
    PARSE_FLAG(has_photo);
    PARSE_FLAG(has_document);
    END_PARSE_FLAGS();
    td::parse(dialog_id_, parser);
    if (has_message_id) {
      td::parse(message_id_, parser);
    }
    td::parse(edit_date_, parser);
    td::parse(loc_key_, parser);
    if (has_arg) {
      td::parse(arg_, parser);
    }
    if (has_photo) {
      td::parse(photo_, parser);
    }
    if (has_document) {
      td::parse(document_, parser);
    }
  }
};

void NotificationManager::edit_message_push_notification(DialogId dialog_id, MessageId message_id, int32 edit_date,
                                                         string loc_key, string arg, Photo photo, Document document,
                                                         uint64 log_event_id, Promise<Unit> promise) {
  if (is_disabled() || max_notification_group_count_ == 0) {
    CHECK(log_event_id == 0);
    return promise.set_error(Status::Error(200, "Immediate success"));
  }

  auto it = temporary_notifications_.find(FullMessageId(dialog_id, message_id));
  if (it == temporary_notifications_.end()) {
    VLOG(notifications) << "Ignore edit of message push notification for " << message_id << " in " << dialog_id
                        << " edited at " << edit_date;
    return promise.set_error(Status::Error(200, "Immediate success"));
  }

  auto group_id = it->second.group_id;
  auto notification_id = it->second.notification_id;
  auto sender_user_id = it->second.sender_user_id;
  auto sender_dialog_id = it->second.sender_dialog_id;
  auto sender_name = it->second.sender_name;
  auto is_outgoing = it->second.is_outgoing;
  CHECK(group_id.is_valid());
  CHECK(notification_id.is_valid());

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    EditMessagePushNotificationLogEvent log_event{dialog_id, message_id, edit_date, loc_key, arg, photo, document};
    auto storer = get_log_event_storer(log_event);
    auto &cur_log_event_id = temporary_edit_notification_log_event_ids_[notification_id];
    if (cur_log_event_id == 0) {
      log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::EditMessagePushNotification, storer);
      cur_log_event_id = log_event_id;
      VLOG(notifications) << "Add edit message push notification log event " << log_event_id;
    } else {
      auto new_log_event_id = binlog_rewrite(G()->td_db()->get_binlog(), cur_log_event_id,
                                             LogEvent::HandlerType::EditMessagePushNotification, storer);
      VLOG(notifications) << "Rewrite edit message push notification log event " << cur_log_event_id << " with "
                          << new_log_event_id;
    }
  } else if (log_event_id != 0) {
    VLOG(notifications) << "Register edit of temporary " << notification_id << " with log event " << log_event_id;
    temporary_edit_notification_log_event_ids_[notification_id] = log_event_id;
  }

  push_notification_promises_[notification_id].push_back(std::move(promise));

  edit_notification(group_id, notification_id,
                    create_new_push_message_notification(sender_user_id, sender_dialog_id, std::move(sender_name),
                                                         is_outgoing, message_id, std::move(loc_key), std::move(arg),
                                                         std::move(photo), std::move(document)));
}

void NotificationManager::on_binlog_events(vector<BinlogEvent> &&events) {
  VLOG(notifications) << "Begin to process " << events.size() << " binlog events";
  for (auto &event : events) {
    if (!G()->parameters().use_message_db || is_disabled() || max_notification_group_count_ == 0) {
      binlog_erase(G()->td_db()->get_binlog(), event.id_);
      break;
    }

    switch (event.type_) {
      case LogEvent::HandlerType::AddMessagePushNotification: {
        CHECK(is_inited_);
        AddMessagePushNotificationLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        add_message_push_notification(
            log_event.dialog_id_, log_event.message_id_, log_event.random_id_, log_event.sender_user_id_,
            log_event.sender_dialog_id_, log_event.sender_name_, log_event.date_, log_event.is_from_scheduled_,
            log_event.contains_mention_, log_event.is_silent_, true, log_event.loc_key_, log_event.arg_,
            log_event.photo_, log_event.document_, log_event.notification_id_, event.id_,
            PromiseCreator::lambda([](Result<Unit> result) {
              if (result.is_error() && result.error().code() != 200 && result.error().code() != 406) {
                LOG(ERROR) << "Receive error " << result.error() << ", while processing message push notification";
              }
            }));
        break;
      }
      case LogEvent::HandlerType::EditMessagePushNotification: {
        CHECK(is_inited_);
        EditMessagePushNotificationLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        edit_message_push_notification(
            log_event.dialog_id_, log_event.message_id_, log_event.edit_date_, log_event.loc_key_, log_event.arg_,
            log_event.photo_, log_event.document_, event.id_, PromiseCreator::lambda([](Result<Unit> result) {
              if (result.is_error() && result.error().code() != 200 && result.error().code() != 406) {
                LOG(ERROR) << "Receive error " << result.error() << ", while processing edit message push notification";
              }
            }));
        break;
      }
      default:
        LOG(FATAL) << "Unsupported log event type " << event.type_;
    }
  }
  if (is_inited_) {
    flush_all_pending_notifications();
  }
  is_binlog_processed_ = true;
  try_send_update_active_notifications();
  VLOG(notifications) << "Finish processing binlog events";
}

NotificationGroupKey NotificationManager::get_last_updated_group_key() const {
  size_t left = max_notification_group_count_;
  auto it = groups_.begin();
  while (it != groups_.end() && left > 1) {
    ++it;
    left--;
  }
  if (it == groups_.end()) {
    return NotificationGroupKey();
  }
  return it->first;
}

bool NotificationManager::is_disabled() const {
  return !td_->auth_manager_->is_authorized() || td_->auth_manager_->is_bot() || G()->close_flag();
}
}  // namespace td
