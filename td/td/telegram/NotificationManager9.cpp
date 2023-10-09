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

#include <algorithm>
#include <iterator>
#include <limits>
#include <unordered_map>
#include <unordered_set>

namespace td {
}  // namespace td
namespace td {

void NotificationManager::remove_temporary_notifications(NotificationGroupId group_id, const char *source) {
  CHECK(group_id.is_valid());

  if (is_disabled() || max_notification_group_count_ == 0) {
    return;
  }

  auto group_it = get_group(group_id);
  if (group_it == groups_.end()) {
    return;
  }

  if (get_temporary_notification_total_count(group_it->second) == 0) {
    return;
  }

  VLOG(notifications) << "Remove temporary notifications in " << group_id << " from " << source;

  auto &group = group_it->second;
  while (!group.pending_notifications.empty() && group.pending_notifications.back().type->is_temporary()) {
    VLOG(notifications) << "Remove temporary " << group.pending_notifications.back() << " from " << group_id;
    // notification is still pending, just delete it
    on_notification_removed(group.pending_notifications.back().notification_id);
    group.pending_notifications.pop_back();
    if (group.pending_notifications.empty()) {
      group.pending_notifications_flush_time = 0;
      flush_pending_notifications_timeout_.cancel_timeout(group_id.get());
      on_delayed_notification_update_count_changed(-1, group_id.get(), "remove_temporary_notifications");
    }
  }

  auto old_group_size = group.notifications.size();
  size_t notification_pos = old_group_size;
  for (size_t pos = 0; pos < notification_pos; pos++) {
    if (group.notifications[pos].type->is_temporary()) {
      notification_pos = pos;
    }
  }
  auto removed_notification_count = narrow_cast<int32>(old_group_size - notification_pos);
  if (removed_notification_count == 0) {
    CHECK(get_temporary_notification_total_count(group_it->second) == 0);
    return;
  }

  if (group.total_count < removed_notification_count) {
    LOG(ERROR) << "Total notification count became negative in " << group_id << " after removing "
               << removed_notification_count << " temporary notificaitions";
    group.total_count = 0;
  } else {
    group.total_count -= removed_notification_count;
  }

  vector<int32> removed_notification_ids;
  for (auto i = notification_pos; i < old_group_size; i++) {
    LOG_CHECK(group.notifications[i].type->is_temporary())
        << notification_pos << ' ' << i << ' ' << old_group_size << ' ' << removed_notification_count << ' '
        << group.notifications[i] << ' ' << group << ' ' << group_it->first;
    VLOG(notifications) << "Remove temporary " << group.notifications[i] << " from " << group_id;
    auto notification_id = group.notifications[i].notification_id;
    on_notification_removed(notification_id);
    if (i + max_notification_group_size_ >= old_group_size) {
      removed_notification_ids.push_back(notification_id.get());
    }
  }
  group.notifications.erase(group.notifications.begin() + notification_pos, group.notifications.end());
  CHECK(!removed_notification_ids.empty());

  vector<td_api::object_ptr<td_api::notification>> added_notifications;
  if (old_group_size >= max_notification_group_size_) {
    size_t added_notification_count = 0;
    for (size_t i = min(old_group_size - max_notification_group_size_, notification_pos);
         i-- > 0 && added_notification_count++ < removed_notification_ids.size();) {
      added_notifications.push_back(get_notification_object(group_it->first.dialog_id, group.notifications[i]));
      if (added_notifications.back()->type_ == nullptr) {
        added_notifications.pop_back();
      }
    }
    if (added_notification_count < removed_notification_ids.size() &&
        max_notification_group_size_ > group.notifications.size()) {
      load_message_notifications_from_database(group_it->first, group, keep_notification_group_size_);
    }
    std::reverse(added_notifications.begin(), added_notifications.end());
  }
  CHECK(get_temporary_notification_total_count(group_it->second) == 0);

  on_notifications_removed(std::move(group_it), std::move(added_notifications), std::move(removed_notification_ids),
                           false);

  remove_added_notifications_from_pending_updates(
      group_id, [](const td_api::object_ptr<td_api::notification> &notification) {
        return notification->get_id() == td_api::notificationTypeNewPushMessage::ID;
      });
}

void NotificationManager::on_notification_group_size_max_changed() {
  if (is_disabled()) {
    return;
  }

  auto new_max_notification_group_size = narrow_cast<int32>(
      G()->shared_config().get_option_integer("notification_group_size_max", DEFAULT_GROUP_SIZE_MAX));
  CHECK(MIN_NOTIFICATION_GROUP_SIZE_MAX <= new_max_notification_group_size &&
        new_max_notification_group_size <= MAX_NOTIFICATION_GROUP_SIZE_MAX);

  auto new_max_notification_group_size_size_t = static_cast<size_t>(new_max_notification_group_size);
  if (new_max_notification_group_size_size_t == max_notification_group_size_) {
    return;
  }

  auto new_keep_notification_group_size =
      new_max_notification_group_size_size_t +
      clamp(new_max_notification_group_size_size_t, EXTRA_GROUP_SIZE / 2, EXTRA_GROUP_SIZE);

  VLOG(notifications) << "Change max notification group size from " << max_notification_group_size_ << " to "
                      << new_max_notification_group_size;

  if (max_notification_group_size_ != 0) {
    flush_all_notifications();

    size_t left = max_notification_group_count_;
    for (auto it = groups_.begin(); it != groups_.end() && left > 0; ++it, left--) {
      auto &group_key = it->first;
      auto &group = it->second;
      CHECK(group.pending_notifications.empty());
      CHECK(pending_updates_.count(group_key.group_id.get()) == 0);

      if (group_key.last_notification_date == 0) {
        break;
      }

      vector<td_api::object_ptr<td_api::notification>> added_notifications;
      vector<int32> removed_notification_ids;
      auto notification_count = group.notifications.size();
      if (new_max_notification_group_size_size_t < max_notification_group_size_) {
        if (notification_count <= new_max_notification_group_size_size_t) {
          VLOG(notifications) << "There is no need to update " << group_key.group_id;
          continue;
        }
        for (size_t i = notification_count - min(notification_count, max_notification_group_size_);
             i < notification_count - new_max_notification_group_size_size_t; i++) {
          removed_notification_ids.push_back(group.notifications[i].notification_id.get());
        }
        CHECK(!removed_notification_ids.empty());
      } else {
        if (new_max_notification_group_size_size_t > notification_count) {
          load_message_notifications_from_database(group_key, group, new_keep_notification_group_size);
        }
        if (notification_count <= max_notification_group_size_) {
          VLOG(notifications) << "There is no need to update " << group_key.group_id;
          continue;
        }
        for (size_t i = notification_count - min(notification_count, new_max_notification_group_size_size_t);
             i < notification_count - max_notification_group_size_; i++) {
          added_notifications.push_back(get_notification_object(group_key.dialog_id, group.notifications[i]));
          if (added_notifications.back()->type_ == nullptr) {
            added_notifications.pop_back();
          }
        }
        if (added_notifications.empty()) {
          continue;
        }
      }
      if (!is_destroyed_) {
        auto update = td_api::make_object<td_api::updateNotificationGroup>(
            group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(),
            group_key.dialog_id.get(), true, group.total_count, std::move(added_notifications),
            std::move(removed_notification_ids));
        VLOG(notifications) << "Send " << as_notification_update(update.get());
        send_closure(G()->td(), &Td::send_update, std::move(update));
      }
    }
  }

  max_notification_group_size_ = new_max_notification_group_size_size_t;
  keep_notification_group_size_ = new_keep_notification_group_size;
}

void NotificationManager::edit_notification(NotificationGroupId group_id, NotificationId notification_id,
                                            unique_ptr<NotificationType> type) {
  if (is_disabled() || max_notification_group_count_ == 0) {
    return;
  }
  if (!group_id.is_valid()) {
    return;
  }

  CHECK(notification_id.is_valid());
  CHECK(type != nullptr);
  VLOG(notifications) << "Edit " << notification_id << ": " << *type;

  auto group_it = get_group(group_id);
  if (group_it == groups_.end()) {
    return;
  }
  auto &group = group_it->second;
  for (size_t i = 0; i < group.notifications.size(); i++) {
    auto &notification = group.notifications[i];
    if (notification.notification_id == notification_id) {
      if (notification.type->get_message_id() != type->get_message_id() ||
          notification.type->is_temporary() != type->is_temporary()) {
        LOG(ERROR) << "Ignore edit of " << notification_id << " with " << *type << ", because previous type is "
                   << *notification.type;
        return;
      }

      notification.type = std::move(type);
      if (i + max_notification_group_size_ >= group.notifications.size() &&
          !(get_last_updated_group_key() < group_it->first)) {
        CHECK(group_it->first.last_notification_date != 0);
        add_update_notification(group_it->first.group_id, group_it->first.dialog_id, notification);
      }
      return;
    }
  }
  for (auto &notification : group.pending_notifications) {
    if (notification.notification_id == notification_id) {
      if (notification.type->get_message_id() != type->get_message_id() ||
          notification.type->is_temporary() != type->is_temporary()) {
        LOG(ERROR) << "Ignore edit of " << notification_id << " with " << *type << ", because previous type is "
                   << *notification.type;
        return;
      }

      notification.type = std::move(type);
      return;
    }
  }
}

void NotificationManager::try_reuse_notification_group_id(NotificationGroupId group_id) {
  if (is_disabled()) {
    return;
  }
  if (!group_id.is_valid()) {
    return;
  }

  VLOG(notifications) << "Trying to reuse " << group_id;
  if (group_id != current_notification_group_id_) {
    // may be implemented in the future
    return;
  }

  auto group_it = get_group(group_id);
  if (group_it != groups_.end()) {
    LOG_CHECK(group_it->first.last_notification_date == 0 && group_it->second.total_count == 0)
        << running_get_difference_ << " " << delayed_notification_update_count_ << " "
        << unreceived_notification_update_count_ << " " << pending_updates_[group_id.get()].size() << " "
        << group_it->first << " " << group_it->second;
    CHECK(group_it->second.notifications.empty());
    CHECK(group_it->second.pending_notifications.empty());
    CHECK(!group_it->second.is_being_loaded_from_database);
    delete_group(std::move(group_it));

    CHECK(running_get_chat_difference_.count(group_id.get()) == 0);

    flush_pending_notifications_timeout_.cancel_timeout(group_id.get());
    flush_pending_updates_timeout_.cancel_timeout(group_id.get());
    if (pending_updates_.erase(group_id.get()) == 1) {
      on_delayed_notification_update_count_changed(-1, group_id.get(), "try_reuse_notification_group_id");
    }
  }

  current_notification_group_id_ = NotificationGroupId(current_notification_group_id_.get() - 1);
  G()->td_db()->get_binlog_pmc()->set("notification_group_id_current", to_string(current_notification_group_id_.get()));
}

void NotificationManager::add_call_notification(DialogId dialog_id, CallId call_id) {
  CHECK(dialog_id.is_valid());
  CHECK(call_id.is_valid());
  if (is_disabled() || max_notification_group_count_ == 0) {
    return;
  }

  auto group_id = get_call_notification_group_id(dialog_id);
  if (!group_id.is_valid()) {
    VLOG(notifications) << "Ignore notification about " << call_id << " in " << dialog_id;
    return;
  }

  G()->td().get_actor_unsafe()->messages_manager_->force_create_dialog(dialog_id, "add_call_notification");

  auto &active_notifications = active_call_notifications_[dialog_id];
  if (active_notifications.size() >= MAX_CALL_NOTIFICATIONS) {
    VLOG(notifications) << "Ignore notification about " << call_id << " in " << dialog_id << " and " << group_id;
    return;
  }

  auto notification_id = get_next_notification_id();
  if (!notification_id.is_valid()) {
    return;
  }
  active_notifications.push_back(ActiveCallNotification{call_id, notification_id});

  add_notification(group_id, NotificationGroupType::Calls, dialog_id, G()->unix_time() + 120, dialog_id, false, false,
                   0, notification_id, create_new_call_notification(call_id), "add_call_notification");
}

void NotificationManager::after_get_difference_impl() {
  if (running_get_difference_) {
    return;
  }

  VLOG(notifications) << "After get difference";

  vector<NotificationGroupId> to_remove_temporary_notifications_group_ids;
  for (auto &group_it : groups_) {
    const auto &group_key = group_it.first;
    const auto &group = group_it.second;
    if (running_get_chat_difference_.count(group_key.group_id.get()) == 0 &&
        get_temporary_notification_total_count(group) > 0) {
      to_remove_temporary_notifications_group_ids.push_back(group_key.group_id);
    }
  }
  for (auto group_id : reversed(to_remove_temporary_notifications_group_ids)) {
    remove_temporary_notifications(group_id, "after_get_difference");
  }

  flush_all_pending_updates(false, "after_get_difference");
}

StringBuilder &operator<<(StringBuilder &string_builder, const NotificationManager::ActiveNotificationsUpdate &update) {
  if (update.update == nullptr) {
    return string_builder << "null";
  }
  string_builder << "update[\n";
  for (auto &group : update.update->groups_) {
    vector<int32> added_notification_ids;
    for (auto &notification : group->notifications_) {
      added_notification_ids.push_back(notification->id_);
    }

    string_builder << "    [" << NotificationGroupId(group->id_) << " of type "
                   << get_notification_group_type(group->type_) << " from " << DialogId(group->chat_id_)
                   << "; total_count = " << group->total_count_ << ", restore " << added_notification_ids << "]\n";
  }
  return string_builder << ']';
}

void NotificationManager::flush_all_pending_notifications() {
  std::multimap<int32, NotificationGroupId> group_ids;
  for (auto &group_it : groups_) {
    if (!group_it.second.pending_notifications.empty()) {
      group_ids.emplace(group_it.second.pending_notifications.back().date, group_it.first.group_id);
    }
  }

  // flush groups in order of last notification date
  VLOG(notifications) << "Flush pending notifications in " << group_ids.size() << " notification groups";
  for (auto &it : group_ids) {
    flush_pending_notifications_timeout_.cancel_timeout(it.second.get());
    flush_pending_notifications(it.second);
  }
}

MessageId NotificationManager::get_last_message_id(const NotificationGroup &group) {
  // it's fine to return MessageId() if last notification has no message_id, because
  // non-message notification can't be mixed with message notifications
  if (!group.pending_notifications.empty()) {
    return group.pending_notifications.back().type->get_message_id();
  }
  if (!group.notifications.empty()) {
    return group.notifications.back().type->get_message_id();
  }
  return MessageId();
}

void NotificationManager::on_notification_processed(NotificationId notification_id) {
  auto promise_it = push_notification_promises_.find(notification_id);
  if (promise_it != push_notification_promises_.end()) {
    auto promises = std::move(promise_it->second);
    push_notification_promises_.erase(promise_it);

    for (auto &promise : promises) {
      promise.set_value(Unit());
    }
  }
}

void NotificationManager::add_update_notification_group(td_api::object_ptr<td_api::updateNotificationGroup> update) {
  auto group_id = update->notification_group_id_;
  if (update->notification_settings_chat_id_ == 0) {
    update->notification_settings_chat_id_ = update->chat_id_;
  }
  add_update(group_id, std::move(update));
}

void NotificationManager::delete_group(NotificationGroups::iterator &&group_it) {
  auto erased_count = group_keys_.erase(group_it->first.group_id);
  CHECK(erased_count > 0);
  groups_.erase(group_it);
}

string NotificationManager::get_is_contact_registered_notifications_synchronized_key() {
  return "notifications_contact_registered_sync_state";
}
}  // namespace td
