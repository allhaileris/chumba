//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/NotificationManager.h"


#include "td/telegram/ChannelId.h"
#include "td/telegram/ChatId.h"


#include "td/telegram/DeviceTokenManager.h"
#include "td/telegram/Document.h"
#include "td/telegram/Document.hpp"


#include "td/telegram/Global.h"
#include "td/telegram/logevent/LogEvent.h"

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

void NotificationManager::add_notifications_to_group_begin(NotificationGroups::iterator group_it,
                                                           vector<Notification> notifications) {
  CHECK(group_it != groups_.end());

  td::remove_if(notifications, [dialog_id = group_it->first.dialog_id](const Notification &notification) {
    return notification.type->get_notification_type_object(dialog_id) == nullptr;
  });

  if (notifications.empty()) {
    return;
  }
  VLOG(notifications) << "Add to beginning of " << group_it->first << " of size "
                      << group_it->second.notifications.size() << ' ' << notifications;

  auto group_key = group_it->first;
  auto final_group_key = group_key;
  for (auto &notification : notifications) {
    if (notification.date > final_group_key.last_notification_date) {
      final_group_key.last_notification_date = notification.date;
    }
  }
  LOG_CHECK(final_group_key.last_notification_date != 0) << final_group_key << ' ' << *group_it << ' ' << notifications;

  bool is_position_changed = final_group_key.last_notification_date != group_key.last_notification_date;

  NotificationGroup group = std::move(group_it->second);
  if (is_position_changed) {
    VLOG(notifications) << "Position of notification group is changed from " << group_key << " to " << final_group_key;
    delete_group(std::move(group_it));
  }

  auto last_group_key = get_last_updated_group_key();
  bool was_updated = false;
  bool is_updated = false;
  if (is_position_changed) {
    was_updated = group_key.last_notification_date != 0 && group_key < last_group_key;
    is_updated = final_group_key.last_notification_date != 0 && final_group_key < last_group_key;
  } else {
    CHECK(group_key.last_notification_date != 0);
    was_updated = is_updated = !(last_group_key < group_key);
  }

  if (!is_updated) {
    CHECK(!was_updated);
    VLOG(notifications) << "There is no need to send updateNotificationGroup in " << group_key
                        << ", because of newer notification groups";
    group.notifications.insert(group.notifications.begin(), std::make_move_iterator(notifications.begin()),
                               std::make_move_iterator(notifications.end()));
  } else {
    if (!was_updated) {
      if (last_group_key.last_notification_date != 0) {
        // need to remove last notification group to not exceed max_notification_group_count_
        send_remove_group_update(last_group_key, groups_[last_group_key], vector<int32>());
      }
      send_add_group_update(group_key, group);
    }

    vector<Notification> new_notifications;
    vector<td_api::object_ptr<td_api::notification>> added_notifications;
    new_notifications.reserve(notifications.size());
    added_notifications.reserve(notifications.size());
    for (auto &notification : notifications) {
      added_notifications.push_back(get_notification_object(group_key.dialog_id, notification));
      CHECK(added_notifications.back()->type_ != nullptr);
      new_notifications.push_back(std::move(notification));
    }
    notifications = std::move(new_notifications);

    size_t old_notification_count = group.notifications.size();
    auto updated_notification_count = old_notification_count < max_notification_group_size_
                                          ? max_notification_group_size_ - old_notification_count
                                          : 0;
    if (added_notifications.size() > updated_notification_count) {
      added_notifications.erase(added_notifications.begin(), added_notifications.end() - updated_notification_count);
    }
    auto new_notification_count = old_notification_count < keep_notification_group_size_
                                      ? keep_notification_group_size_ - old_notification_count
                                      : 0;
    if (new_notification_count > notifications.size()) {
      new_notification_count = notifications.size();
    }
    if (new_notification_count != 0) {
      VLOG(notifications) << "Add " << new_notification_count << " notifications to " << group_key.group_id
                          << " with current size " << group.notifications.size();
      group.notifications.insert(group.notifications.begin(),
                                 std::make_move_iterator(notifications.end() - new_notification_count),
                                 std::make_move_iterator(notifications.end()));
    }

    if (!added_notifications.empty()) {
      add_update_notification_group(td_api::make_object<td_api::updateNotificationGroup>(
          group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(), 0, true,
          group.total_count, std::move(added_notifications), vector<int32>()));
    }
  }

  if (is_position_changed) {
    add_group(std::move(final_group_key), std::move(group), "add_notifications_to_group_begin");
  } else {
    CHECK(group_it->first.last_notification_date == 0 || !group.notifications.empty());
    group_it->second = std::move(group);
  }
}

bool NotificationManager::do_flush_pending_notifications(NotificationGroupKey &group_key, NotificationGroup &group,
                                                         vector<PendingNotification> &pending_notifications) {
  // no check for G()->close_flag() to flush pending notifications even while closing
  if (pending_notifications.empty()) {
    return false;
  }

  VLOG(notifications) << "Do flush " << pending_notifications.size() << " pending notifications in " << group_key
                      << " with known " << group.notifications.size() << " from total of " << group.total_count
                      << " notifications";

  size_t old_notification_count = group.notifications.size();
  size_t shown_notification_count = min(old_notification_count, max_notification_group_size_);

  bool force_update = false;
  vector<td_api::object_ptr<td_api::notification>> added_notifications;
  added_notifications.reserve(pending_notifications.size());
  for (auto &pending_notification : pending_notifications) {
    Notification notification(pending_notification.notification_id, pending_notification.date,
                              pending_notification.initial_is_silent, std::move(pending_notification.type));
    added_notifications.push_back(get_notification_object(group_key.dialog_id, notification));
    CHECK(added_notifications.back()->type_ != nullptr);

    if (!notification.type->can_be_delayed()) {
      force_update = true;
    }
    group.notifications.push_back(std::move(notification));
  }
  group.total_count += narrow_cast<int32>(added_notifications.size());
  if (added_notifications.size() > max_notification_group_size_) {
    added_notifications.erase(added_notifications.begin(), added_notifications.end() - max_notification_group_size_);
  }

  vector<int32> removed_notification_ids;
  if (shown_notification_count + added_notifications.size() > max_notification_group_size_) {
    auto removed_notification_count =
        shown_notification_count + added_notifications.size() - max_notification_group_size_;
    removed_notification_ids.reserve(removed_notification_count);
    for (size_t i = 0; i < removed_notification_count; i++) {
      removed_notification_ids.push_back(
          group.notifications[old_notification_count - shown_notification_count + i].notification_id.get());
    }
  }

  if (!added_notifications.empty()) {
    add_update_notification_group(td_api::make_object<td_api::updateNotificationGroup>(
        group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(),
        pending_notifications[0].settings_dialog_id.get(), pending_notifications[0].is_silent, group.total_count,
        std::move(added_notifications), std::move(removed_notification_ids)));
  } else {
    CHECK(removed_notification_ids.empty());
  }
  pending_notifications.clear();
  return force_update;
}

void NotificationManager::remove_call_notification(DialogId dialog_id, CallId call_id) {
  CHECK(dialog_id.is_valid());
  CHECK(call_id.is_valid());
  if (is_disabled() || max_notification_group_count_ == 0) {
    return;
  }

  auto group_id_it = dialog_id_to_call_notification_group_id_.find(dialog_id);
  if (group_id_it == dialog_id_to_call_notification_group_id_.end()) {
    VLOG(notifications) << "Ignore removing notification about " << call_id << " in " << dialog_id;
    return;
  }
  auto group_id = group_id_it->second;
  CHECK(group_id.is_valid());

  auto &active_notifications = active_call_notifications_[dialog_id];
  for (auto it = active_notifications.begin(); it != active_notifications.end(); ++it) {
    if (it->call_id == call_id) {
      remove_notification(group_id, it->notification_id, true, true, Promise<Unit>(), "remove_call_notification");
      active_notifications.erase(it);
      if (active_notifications.empty()) {
        VLOG(notifications) << "Reuse call " << group_id;
        active_call_notifications_.erase(dialog_id);
        available_call_notification_group_ids_.insert(group_id);
        dialog_id_to_call_notification_group_id_.erase(dialog_id);

        flush_pending_notifications_timeout_.cancel_timeout(group_id.get());
        flush_pending_notifications(group_id);
        force_flush_pending_updates(group_id, "reuse call group_id");

        auto group_it = get_group(group_id);
        LOG_CHECK(group_it->first.dialog_id == dialog_id)
            << group_id << ' ' << dialog_id << ' ' << group_it->first << ' ' << group_it->second;
        CHECK(group_it->first.last_notification_date == 0);
        CHECK(group_it->second.total_count == 0);
        CHECK(group_it->second.notifications.empty());
        CHECK(group_it->second.pending_notifications.empty());
        CHECK(group_it->second.type == NotificationGroupType::Calls);
        CHECK(!group_it->second.is_being_loaded_from_database);
        CHECK(pending_updates_.count(group_id.get()) == 0);
        delete_group(std::move(group_it));
      }
      return;
    }
  }

  VLOG(notifications) << "Failed to find " << call_id << " in " << dialog_id << " and " << group_id;
}

void NotificationManager::on_notification_removed(NotificationId notification_id) {
  VLOG(notifications) << "In on_notification_removed with " << notification_id;

  auto add_it = temporary_notification_log_event_ids_.find(notification_id);
  if (add_it == temporary_notification_log_event_ids_.end()) {
    return;
  }

  auto edit_it = temporary_edit_notification_log_event_ids_.find(notification_id);
  if (edit_it != temporary_edit_notification_log_event_ids_.end()) {
    VLOG(notifications) << "Remove from binlog edit of " << notification_id << " with log event " << edit_it->second;
    if (!is_being_destroyed_) {
      binlog_erase(G()->td_db()->get_binlog(), edit_it->second);
    }
    temporary_edit_notification_log_event_ids_.erase(edit_it);
  }

  VLOG(notifications) << "Remove from binlog " << notification_id << " with log event " << add_it->second;
  if (!is_being_destroyed_) {
    binlog_erase(G()->td_db()->get_binlog(), add_it->second);
  }
  temporary_notification_log_event_ids_.erase(add_it);

  auto erased_notification_count = temporary_notifications_.erase(temporary_notification_message_ids_[notification_id]);
  auto erased_message_id_count = temporary_notification_message_ids_.erase(notification_id);
  CHECK(erased_notification_count > 0);
  CHECK(erased_message_id_count > 0);

  on_notification_processed(notification_id);
}

td_api::object_ptr<td_api::updateActiveNotifications> NotificationManager::get_update_active_notifications() const {
  auto needed_groups = max_notification_group_count_;
  vector<td_api::object_ptr<td_api::notificationGroup>> groups;
  for (auto &group : groups_) {
    if (needed_groups == 0 || group.first.last_notification_date == 0) {
      break;
    }
    needed_groups--;

    vector<td_api::object_ptr<td_api::notification>> notifications;
    for (auto &notification : reversed(group.second.notifications)) {
      auto notification_object = get_notification_object(group.first.dialog_id, notification);
      if (notification_object->type_ != nullptr) {
        notifications.push_back(std::move(notification_object));
      }
      if (notifications.size() == max_notification_group_size_) {
        break;
      }
    }
    if (!notifications.empty()) {
      std::reverse(notifications.begin(), notifications.end());
      groups.push_back(td_api::make_object<td_api::notificationGroup>(
          group.first.group_id.get(), get_notification_group_type_object(group.second.type),
          group.first.dialog_id.get(), group.second.total_count, std::move(notifications)));
    }
  }

  return td_api::make_object<td_api::updateActiveNotifications>(std::move(groups));
}

MessageId NotificationManager::get_last_message_id_by_notification_id(const NotificationGroup &group,
                                                                      NotificationId max_notification_id) {
  for (auto &notification : reversed(group.pending_notifications)) {
    if (notification.notification_id.get() <= max_notification_id.get()) {
      auto message_id = notification.type->get_message_id();
      if (message_id.is_valid()) {
        return message_id;
      }
    }
  }
  for (auto &notification : reversed(group.notifications)) {
    if (notification.notification_id.get() <= max_notification_id.get()) {
      auto message_id = notification.type->get_message_id();
      if (message_id.is_valid()) {
        return message_id;
      }
    }
  }
  return MessageId();
}

Result<string> NotificationManager::decrypt_push_payload(int64 encryption_key_id, string encryption_key,
                                                         string payload) {
  mtproto::AuthKey auth_key(encryption_key_id, std::move(encryption_key));
  mtproto::PacketInfo packet_info;
  packet_info.version = 2;
  packet_info.type = mtproto::PacketInfo::EndToEnd;
  packet_info.is_creator = true;
  packet_info.check_mod4 = false;

  TRY_RESULT(result, mtproto::Transport::read(payload, auth_key, &packet_info));
  if (result.type() != mtproto::Transport::ReadResult::Packet) {
    return Status::Error(400, "Wrong packet type");
  }
  if (result.packet().size() < 4) {
    return Status::Error(400, "Packet is too small");
  }
  return result.packet().substr(4).str();
}

void NotificationManager::on_contact_registered_notifications_sync(bool is_disabled, Result<Unit> result) {
  CHECK(contact_registered_notifications_sync_state_ == SyncState::Pending);
  if (is_disabled != disable_contact_registered_notifications_) {
    return run_contact_registered_notifications_sync();
  }
  if (result.is_ok()) {
    // everything is synchronized
    set_contact_registered_notifications_sync_state(SyncState::Completed);
  } else {
    // let's resend the query forever
    run_contact_registered_notifications_sync();
  }
}

NotificationId NotificationManager::get_next_notification_id() {
  if (is_disabled()) {
    return NotificationId();
  }
  if (current_notification_id_.get() == std::numeric_limits<int32>::max()) {
    LOG(ERROR) << "Notification identifier overflowed";
    return NotificationId();
  }

  current_notification_id_ = NotificationId(current_notification_id_.get() + 1);
  G()->td_db()->get_binlog_pmc()->set("notification_id_current", to_string(current_notification_id_.get()));
  return current_notification_id_;
}

int32 NotificationManager::get_temporary_notification_total_count(const NotificationGroup &group) {
  int32 result = 0;
  for (auto &notification : reversed(group.notifications)) {
    if (!notification.type->is_temporary()) {
      break;
    }
    result++;
  }
  for (auto &pending_notification : reversed(group.pending_notifications)) {
    if (!pending_notification.type->is_temporary()) {
      break;
    }
    result++;
  }
  return result;
}

NotificationId NotificationManager::get_first_notification_id(const NotificationGroup &group) {
  if (!group.notifications.empty()) {
    return group.notifications[0].notification_id;
  }
  if (!group.pending_notifications.empty()) {
    return group.pending_notifications[0].notification_id;
  }
  return NotificationId();
}

NotificationManager::ActiveNotificationsUpdate NotificationManager::as_active_notifications_update(
    const td_api::updateActiveNotifications *update) {
  return ActiveNotificationsUpdate{update};
}

size_t NotificationManager::get_max_notification_group_size() const {
  return max_notification_group_size_;
}
}  // namespace td
