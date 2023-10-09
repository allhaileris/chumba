//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/NotificationManager.h"


#include "td/telegram/ChannelId.h"
#include "td/telegram/ChatId.h"
#include "td/telegram/ConfigShared.h"

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

string NotificationManager::convert_loc_key(const string &loc_key) {
  if (loc_key.size() <= 8) {
    if (loc_key == "MESSAGES" || loc_key == "ALBUM") {
      return "MESSAGES";
    }
    return string();
  }
  switch (loc_key[8]) {
    case 'A':
      if (loc_key == "PINNED_GAME") {
        return "PINNED_MESSAGE_GAME";
      }
      if (loc_key == "PINNED_GAME_SCORE") {
        return "PINNED_MESSAGE_GAME_SCORE";
      }
      if (loc_key == "CHAT_CREATED") {
        return "MESSAGE_BASIC_GROUP_CHAT_CREATE";
      }
      if (loc_key == "MESSAGE_AUDIO") {
        return "MESSAGE_VOICE_NOTE";
      }
      break;
    case 'C':
      if (loc_key == "MESSAGE_CONTACT") {
        return "MESSAGE_CONTACT";
      }
      break;
    case 'D':
      if (loc_key == "MESSAGE_DOC") {
        return "MESSAGE_DOCUMENT";
      }
      if (loc_key == "MESSAGE_DOCS") {
        return "MESSAGE_DOCUMENTS";
      }
      if (loc_key == "ENCRYPTED_MESSAGE") {
        return "MESSAGE";
      }
      break;
    case 'E':
      if (loc_key == "PINNED_GEO") {
        return "PINNED_MESSAGE_LOCATION";
      }
      if (loc_key == "PINNED_GEOLIVE") {
        return "PINNED_MESSAGE_LIVE_LOCATION";
      }
      if (loc_key == "CHAT_DELETE_MEMBER") {
        return "MESSAGE_CHAT_DELETE_MEMBER";
      }
      if (loc_key == "CHAT_DELETE_YOU") {
        return "MESSAGE_CHAT_DELETE_MEMBER_YOU";
      }
      if (loc_key == "PINNED_TEXT") {
        return "PINNED_MESSAGE_TEXT";
      }
      break;
    case 'F':
      if (loc_key == "MESSAGE_FWDS") {
        return "MESSAGE_FORWARDS";
      }
      break;
    case 'G':
      if (loc_key == "MESSAGE_GAME") {
        return "MESSAGE_GAME";
      }
      if (loc_key == "MESSAGE_GAME_SCORE") {
        return "MESSAGE_GAME_SCORE";
      }
      if (loc_key == "MESSAGE_GEO") {
        return "MESSAGE_LOCATION";
      }
      if (loc_key == "MESSAGE_GEOLIVE") {
        return "MESSAGE_LIVE_LOCATION";
      }
      if (loc_key == "MESSAGE_GIF") {
        return "MESSAGE_ANIMATION";
      }
      break;
    case 'H':
      if (loc_key == "PINNED_PHOTO") {
        return "PINNED_MESSAGE_PHOTO";
      }
      break;
    case 'I':
      if (loc_key == "PINNED_VIDEO") {
        return "PINNED_MESSAGE_VIDEO";
      }
      if (loc_key == "PINNED_GIF") {
        return "PINNED_MESSAGE_ANIMATION";
      }
      if (loc_key == "MESSAGE_INVOICE") {
        return "MESSAGE_INVOICE";
      }
      break;
    case 'J':
      if (loc_key == "CONTACT_JOINED") {
        return "MESSAGE_CONTACT_REGISTERED";
      }
      break;
    case 'L':
      if (loc_key == "CHAT_TITLE_EDITED") {
        return "MESSAGE_CHAT_CHANGE_TITLE";
      }
      break;
    case 'N':
      if (loc_key == "CHAT_JOINED") {
        return "MESSAGE_CHAT_JOIN_BY_LINK";
      }
      if (loc_key == "MESSAGE_NOTEXT") {
        return "MESSAGE";
      }
      if (loc_key == "MESSAGE_NOTHEME") {
        return "MESSAGE_CHAT_CHANGE_THEME";
      }
      if (loc_key == "PINNED_INVOICE") {
        return "PINNED_MESSAGE_INVOICE";
      }
      break;
    case 'O':
      if (loc_key == "PINNED_DOC") {
        return "PINNED_MESSAGE_DOCUMENT";
      }
      if (loc_key == "PINNED_POLL") {
        return "PINNED_MESSAGE_POLL";
      }
      if (loc_key == "PINNED_CONTACT") {
        return "PINNED_MESSAGE_CONTACT";
      }
      if (loc_key == "PINNED_NOTEXT") {
        return "PINNED_MESSAGE";
      }
      if (loc_key == "PINNED_ROUND") {
        return "PINNED_MESSAGE_VIDEO_NOTE";
      }
      break;
    case 'P':
      if (loc_key == "MESSAGE_PHOTO") {
        return "MESSAGE_PHOTO";
      }
      if (loc_key == "MESSAGE_PHOTOS") {
        return "MESSAGE_PHOTOS";
      }
      if (loc_key == "MESSAGE_PHOTO_SECRET") {
        return "MESSAGE_SECRET_PHOTO";
      }
      if (loc_key == "MESSAGE_PLAYLIST") {
        return "MESSAGE_AUDIOS";
      }
      if (loc_key == "MESSAGE_POLL") {
        return "MESSAGE_POLL";
      }
      break;
    case 'Q':
      if (loc_key == "MESSAGE_QUIZ") {
        return "MESSAGE_QUIZ";
      }
      break;
    case 'R':
      if (loc_key == "MESSAGE_ROUND") {
        return "MESSAGE_VIDEO_NOTE";
      }
      break;
    case 'S':
      if (loc_key == "MESSAGE_SCREENSHOT") {
        return "MESSAGE_SCREENSHOT_TAKEN";
      }
      if (loc_key == "MESSAGE_STICKER") {
        return "MESSAGE_STICKER";
      }
      break;
    case 'T':
      if (loc_key == "CHAT_LEFT") {
        return "MESSAGE_CHAT_DELETE_MEMBER_LEFT";
      }
      if (loc_key == "MESSAGE_TEXT") {
        return "MESSAGE_TEXT";
      }
      if (loc_key == "PINNED_STICKER") {
        return "PINNED_MESSAGE_STICKER";
      }
      if (loc_key == "CHAT_PHOTO_EDITED") {
        return "MESSAGE_CHAT_CHANGE_PHOTO";
      }
      if (loc_key == "MESSAGE_THEME") {
        return "MESSAGE_CHAT_CHANGE_THEME";
      }
      break;
    case 'U':
      if (loc_key == "PINNED_AUDIO") {
        return "PINNED_MESSAGE_VOICE_NOTE";
      }
      if (loc_key == "PINNED_QUIZ") {
        return "PINNED_MESSAGE_QUIZ";
      }
      if (loc_key == "CHAT_RETURNED") {
        return "MESSAGE_CHAT_ADD_MEMBERS_RETURNED";
      }
      break;
    case 'V':
      if (loc_key == "MESSAGE_VIDEO") {
        return "MESSAGE_VIDEO";
      }
      if (loc_key == "MESSAGE_VIDEOS") {
        return "MESSAGE_VIDEOS";
      }
      if (loc_key == "MESSAGE_VIDEO_SECRET") {
        return "MESSAGE_SECRET_VIDEO";
      }
      break;
    case '_':
      if (loc_key == "CHAT_ADD_MEMBER") {
        return "MESSAGE_CHAT_ADD_MEMBERS";
      }
      if (loc_key == "CHAT_ADD_YOU") {
        return "MESSAGE_CHAT_ADD_MEMBERS_YOU";
      }
      if (loc_key == "CHAT_REQ_JOINED") {
        return "MESSAGE_CHAT_JOIN_BY_REQUEST";
      }
      break;
  }
  return string();
}

NotificationManager::NotificationGroups::iterator NotificationManager::get_group_force(NotificationGroupId group_id,
                                                                                       bool send_update) {
  auto group_it = get_group(group_id);
  if (group_it != groups_.end()) {
    return group_it;
  }

  if (td::contains(call_notification_group_ids_, group_id)) {
    return groups_.end();
  }

  auto message_group = td_->messages_manager_->get_message_notification_group_force(group_id);
  if (!message_group.dialog_id.is_valid()) {
    return groups_.end();
  }

  NotificationGroupKey group_key(group_id, message_group.dialog_id, 0);
  for (auto &notification : message_group.notifications) {
    if (notification.date > group_key.last_notification_date) {
      group_key.last_notification_date = notification.date;
    }
    if (notification.notification_id.get() > current_notification_id_.get()) {
      LOG(ERROR) << "Fix current notification identifier from " << current_notification_id_ << " to "
                 << notification.notification_id;
      current_notification_id_ = notification.notification_id;
      G()->td_db()->get_binlog_pmc()->set("notification_id_current", to_string(current_notification_id_.get()));
    }
  }
  if (group_id.get() > current_notification_group_id_.get()) {
    LOG(ERROR) << "Fix current notification group identifier from " << current_notification_group_id_ << " to "
               << group_id;
    current_notification_group_id_ = group_id;
    G()->td_db()->get_binlog_pmc()->set("notification_group_id_current",
                                        to_string(current_notification_group_id_.get()));
  }

  NotificationGroup group;
  group.type = message_group.type;
  group.total_count = message_group.total_count;
  group.notifications = std::move(message_group.notifications);

  VLOG(notifications) << "Finish to load " << group_id << " of type " << message_group.type << " with total_count "
                      << message_group.total_count << " and notifications " << group.notifications;

  if (send_update && group_key.last_notification_date != 0) {
    auto last_group_key = get_last_updated_group_key();
    if (group_key < last_group_key) {
      if (last_group_key.last_notification_date != 0) {
        send_remove_group_update(last_group_key, groups_[last_group_key], vector<int32>());
      }
      send_add_group_update(group_key, group);
    }
  }
  return add_group(std::move(group_key), std::move(group), "get_group_force");
}

void NotificationManager::load_message_notifications_from_database(const NotificationGroupKey &group_key,
                                                                   NotificationGroup &group, size_t desired_size) {
  if (!G()->parameters().use_message_db) {
    return;
  }
  if (group.is_loaded_from_database || group.is_being_loaded_from_database ||
      group.type == NotificationGroupType::Calls) {
    return;
  }
  if (group.total_count == 0) {
    return;
  }

  VLOG(notifications) << "Trying to load up to " << desired_size << " notifications in " << group_key.group_id
                      << " with " << group.notifications.size() << " current notifications";

  group.is_being_loaded_from_database = true;

  CHECK(desired_size > group.notifications.size());
  size_t limit = desired_size - group.notifications.size();
  auto first_notification_id = get_first_notification_id(group);
  auto from_notification_id = first_notification_id.is_valid() ? first_notification_id : NotificationId::max();
  auto first_message_id = get_first_message_id(group);
  auto from_message_id = first_message_id.is_valid() ? first_message_id : MessageId::max();
  send_closure(G()->messages_manager(), &MessagesManager::get_message_notifications_from_database, group_key.dialog_id,
               group_key.group_id, from_notification_id, from_message_id, static_cast<int32>(limit),
               PromiseCreator::lambda([actor_id = actor_id(this), group_id = group_key.group_id,
                                       limit](Result<vector<Notification>> r_notifications) {
                 send_closure_later(actor_id, &NotificationManager::on_get_message_notifications_from_database,
                                    group_id, limit, std::move(r_notifications));
               }));
}

StringBuilder &operator<<(StringBuilder &string_builder, const NotificationManager::NotificationUpdate &update) {
  if (update.update == nullptr) {
    return string_builder << "null";
  }
  switch (update.update->get_id()) {
    case td_api::updateNotification::ID: {
      auto p = static_cast<const td_api::updateNotification *>(update.update);
      return string_builder << "update[" << NotificationId(p->notification_->id_) << " from "
                            << NotificationGroupId(p->notification_group_id_) << ']';
    }
    case td_api::updateNotificationGroup::ID: {
      auto p = static_cast<const td_api::updateNotificationGroup *>(update.update);
      vector<int32> added_notification_ids;
      for (auto &notification : p->added_notifications_) {
        added_notification_ids.push_back(notification->id_);
      }

      return string_builder << "update[" << NotificationGroupId(p->notification_group_id_) << " of type "
                            << get_notification_group_type(p->type_) << " from " << DialogId(p->chat_id_)
                            << " with settings from " << DialogId(p->notification_settings_chat_id_)
                            << (p->is_silent_ ? "   silently" : " with sound") << "; total_count = " << p->total_count_
                            << ", add " << added_notification_ids << ", remove " << p->removed_notification_ids_;
    }
    default:
      UNREACHABLE();
      return string_builder << "unknown";
  }
}

void NotificationManager::destroy_all_notifications() {
  if (is_destroyed_) {
    return;
  }
  is_being_destroyed_ = true;

  size_t cur_pos = 0;
  for (auto it = groups_.begin(); it != groups_.end() && cur_pos < max_notification_group_count_; ++it, cur_pos++) {
    auto &group_key = it->first;
    auto &group = it->second;

    if (group_key.last_notification_date == 0) {
      break;
    }

    VLOG(notifications) << "Destroy " << group_key.group_id;
    send_remove_group_update(group_key, group, vector<int32>());
  }

  flush_all_pending_updates(true, "destroy_all_notifications");
  if (delayed_notification_update_count_ != 0) {
    on_delayed_notification_update_count_changed(-delayed_notification_update_count_, 0, "destroy_all_notifications");
  }
  if (unreceived_notification_update_count_ != 0) {
    on_unreceived_notification_update_count_changed(-unreceived_notification_update_count_, 0,
                                                    "destroy_all_notifications");
  }

  while (!push_notification_promises_.empty()) {
    on_notification_processed(push_notification_promises_.begin()->first);
  }

  is_destroyed_ = true;
}

int32 NotificationManager::load_message_notification_groups_from_database(int32 limit, bool send_update) {
  CHECK(limit > 0);
  if (last_loaded_notification_group_key_.last_notification_date == 0) {
    // everything was already loaded
    return 0;
  }

  vector<NotificationGroupKey> group_keys = td_->messages_manager_->get_message_notification_group_keys_from_database(
      last_loaded_notification_group_key_, limit);
  last_loaded_notification_group_key_ =
      group_keys.size() == static_cast<size_t>(limit) ? group_keys.back() : NotificationGroupKey();

  int32 result = 0;
  for (auto &group_key : group_keys) {
    auto group_it = get_group_force(group_key.group_id, send_update);
    LOG_CHECK(group_it != groups_.end()) << call_notification_group_ids_ << " " << group_keys << " "
                                         << current_notification_group_id_ << " " << limit;
    CHECK(group_it->first.dialog_id.is_valid());
    if (!(last_loaded_notification_group_key_ < group_it->first)) {
      result++;
    }
  }
  return result;
}

void NotificationManager::add_update_notification(NotificationGroupId notification_group_id, DialogId dialog_id,
                                                  const Notification &notification) {
  auto notification_object = get_notification_object(dialog_id, notification);
  if (notification_object->type_ == nullptr) {
    return;
  }

  add_update(notification_group_id.get(), td_api::make_object<td_api::updateNotification>(
                                              notification_group_id.get(), std::move(notification_object)));
  if (!notification.type->can_be_delayed()) {
    force_flush_pending_updates(notification_group_id, "add_update_notification");
  }
}

void NotificationManager::try_send_update_active_notifications() {
  if (max_notification_group_count_ == 0) {
    return;
  }
  if (!is_binlog_processed_ || !is_inited_) {
    return;
  }

  auto update = get_update_active_notifications();
  VLOG(notifications) << "Send " << as_active_notifications_update(update.get());
  send_closure(G()->td(), &Td::send_update, std::move(update));

  while (!push_notification_promises_.empty()) {
    on_notification_processed(push_notification_promises_.begin()->first);
  }
}

MessageId NotificationManager::get_first_message_id(const NotificationGroup &group) {
  // it's fine to return MessageId() if first notification has no message_id, because
  // non-message notification can't be mixed with message notifications
  if (!group.notifications.empty()) {
    return group.notifications[0].type->get_message_id();
  }
  if (!group.pending_notifications.empty()) {
    return group.pending_notifications[0].type->get_message_id();
  }
  return MessageId();
}

td_api::object_ptr<td_api::updateHavePendingNotifications> NotificationManager::get_update_have_pending_notifications()
    const {
  return td_api::make_object<td_api::updateHavePendingNotifications>(delayed_notification_update_count_ != 0,
                                                                     unreceived_notification_update_count_ != 0);
}

void NotificationManager::on_online_cloud_timeout_changed() {
  if (is_disabled()) {
    return;
  }

  online_cloud_timeout_ms_ = narrow_cast<int32>(
      G()->shared_config().get_option_integer("online_cloud_timeout_ms", DEFAULT_ONLINE_CLOUD_TIMEOUT_MS));
  VLOG(notifications) << "Set online_cloud_timeout_ms to " << online_cloud_timeout_ms_;
}

void NotificationManager::load_group_force(NotificationGroupId group_id) {
  if (is_disabled() || max_notification_group_count_ == 0) {
    return;
  }

  auto group_it = get_group_force(group_id, true);
  CHECK(group_it != groups_.end());
}

void NotificationManager::tear_down() {
  parent_.reset();
}

}  // namespace td
