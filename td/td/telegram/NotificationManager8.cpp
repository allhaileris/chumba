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

void NotificationManager::flush_pending_notifications(NotificationGroupId group_id) {
  auto group_it = get_group(group_id);
  if (group_it == groups_.end()) {
    return;
  }

  td::remove_if(group_it->second.pending_notifications,
                [dialog_id = group_it->first.dialog_id](const PendingNotification &pending_notification) {
                  return pending_notification.type->get_notification_type_object(dialog_id) == nullptr;
                });

  if (group_it->second.pending_notifications.empty()) {
    return;
  }

  auto group_key = group_it->first;
  auto group = std::move(group_it->second);

  delete_group(std::move(group_it));

  auto final_group_key = group_key;
  for (auto &pending_notification : group.pending_notifications) {
    if (pending_notification.date >= final_group_key.last_notification_date) {
      final_group_key.last_notification_date = pending_notification.date;
    }
  }
  CHECK(final_group_key.last_notification_date != 0);

  VLOG(notifications) << "Flush pending notifications in " << group_key << " up to "
                      << final_group_key.last_notification_date;

  auto last_group_key = get_last_updated_group_key();
  bool was_updated = group_key.last_notification_date != 0 && group_key < last_group_key;
  bool is_updated = final_group_key < last_group_key;
  bool force_update = false;

  NotificationGroupId removed_group_id;
  if (!is_updated) {
    CHECK(!was_updated);
    VLOG(notifications) << "There is no need to send updateNotificationGroup in " << group_key
                        << ", because of newer notification groups";
    group.total_count += narrow_cast<int32>(group.pending_notifications.size());
    for (auto &pending_notification : group.pending_notifications) {
      group.notifications.emplace_back(pending_notification.notification_id, pending_notification.date,
                                       pending_notification.initial_is_silent, std::move(pending_notification.type));
    }
  } else {
    if (!was_updated) {
      if (last_group_key.last_notification_date != 0) {
        // need to remove last notification group to not exceed max_notification_group_count_
        removed_group_id = last_group_key.group_id;
        send_remove_group_update(last_group_key, groups_[last_group_key], vector<int32>());
      }
      send_add_group_update(group_key, group);
    }

    DialogId notification_settings_dialog_id;
    bool is_silent = false;

    // split notifications by groups with common settings
    vector<PendingNotification> grouped_notifications;
    for (auto &pending_notification : group.pending_notifications) {
      if (notification_settings_dialog_id != pending_notification.settings_dialog_id ||
          is_silent != pending_notification.is_silent) {
        if (do_flush_pending_notifications(group_key, group, grouped_notifications)) {
          force_update = true;
        }
        notification_settings_dialog_id = pending_notification.settings_dialog_id;
        is_silent = pending_notification.is_silent;
      }
      grouped_notifications.push_back(std::move(pending_notification));
    }
    if (do_flush_pending_notifications(group_key, group, grouped_notifications)) {
      force_update = true;
    }
  }

  group.pending_notifications_flush_time = 0;
  group.pending_notifications.clear();
  on_delayed_notification_update_count_changed(-1, group_id.get(), "flush_pending_notifications");
  // if we can delete a lot of notifications simultaneously
  if (group.notifications.size() > keep_notification_group_size_ + EXTRA_GROUP_SIZE &&
      group.type != NotificationGroupType::Calls) {
    // keep only keep_notification_group_size_ last notifications in memory
    for (auto it = group.notifications.begin(); it != group.notifications.end() - keep_notification_group_size_; ++it) {
      on_notification_removed(it->notification_id);
    }
    group.notifications.erase(group.notifications.begin(), group.notifications.end() - keep_notification_group_size_);
    group.is_loaded_from_database = false;
  }

  add_group(std::move(final_group_key), std::move(group), "flush_pending_notifications");

  if (force_update) {
    if (removed_group_id.is_valid()) {
      force_flush_pending_updates(removed_group_id, "flush_pending_notifications 1");
    }
    force_flush_pending_updates(group_key.group_id, "flush_pending_notifications 2");
  }
}

void NotificationManager::on_notifications_removed(
    NotificationGroups::iterator &&group_it, vector<td_api::object_ptr<td_api::notification>> &&added_notifications,
    vector<int32> &&removed_notification_ids, bool force_update) {
  VLOG(notifications) << "In on_notifications_removed for " << group_it->first.group_id << " with "
                      << added_notifications.size() << " added notifications and " << removed_notification_ids.size()
                      << " removed notifications, new total_count = " << group_it->second.total_count;
  auto group_key = group_it->first;
  auto final_group_key = group_key;
  final_group_key.last_notification_date = 0;
  for (auto &notification : group_it->second.notifications) {
    if (notification.date > final_group_key.last_notification_date) {
      final_group_key.last_notification_date = notification.date;
    }
  }

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
    was_updated = is_updated = group_key.last_notification_date != 0 && !(last_group_key < group_key);
  }

  if (!was_updated) {
    CHECK(!is_updated);
    if (final_group_key.last_notification_date == 0 && group.total_count == 0) {
      // send update about empty invisible group anyway
      add_update_notification_group(td_api::make_object<td_api::updateNotificationGroup>(
          group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(), 0, true,
          0, vector<td_api::object_ptr<td_api::notification>>(), vector<int32>()));
    } else {
      VLOG(notifications) << "There is no need to send updateNotificationGroup about " << group_key.group_id;
    }
  } else {
    if (is_updated) {
      // group is still visible
      add_update_notification_group(td_api::make_object<td_api::updateNotificationGroup>(
          group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(), 0, true,
          group.total_count, std::move(added_notifications), std::move(removed_notification_ids)));
    } else {
      // group needs to be removed
      send_remove_group_update(group_key, group, std::move(removed_notification_ids));
      if (last_group_key.last_notification_date != 0) {
        // need to add new last notification group
        send_add_group_update(last_group_key, groups_[last_group_key]);
      }
    }
  }

  if (is_position_changed) {
    add_group(std::move(final_group_key), std::move(group), "on_notifications_removed");

    last_group_key = get_last_updated_group_key();
  } else {
    CHECK(group_it->first.last_notification_date == 0 || !group.notifications.empty());
    group_it->second = std::move(group);
  }

  if (force_update) {
    force_flush_pending_updates(group_key.group_id, "on_notifications_removed");
  }

  if (last_loaded_notification_group_key_ < last_group_key) {
    load_message_notification_groups_from_database(td::max(static_cast<int32>(max_notification_group_count_), 10) / 2,
                                                   true);
  }
}

void NotificationManager::remove_added_notifications_from_pending_updates(
    NotificationGroupId group_id,
    const std::function<bool(const td_api::object_ptr<td_api::notification> &notification)> &is_removed) {
  auto it = pending_updates_.find(group_id.get());
  if (it == pending_updates_.end()) {
    return;
  }

  std::unordered_set<int32> removed_notification_ids;
  for (auto &update : it->second) {
    if (update == nullptr) {
      continue;
    }
    if (update->get_id() == td_api::updateNotificationGroup::ID) {
      auto update_ptr = static_cast<td_api::updateNotificationGroup *>(update.get());
      if (!removed_notification_ids.empty() && !update_ptr->removed_notification_ids_.empty()) {
        td::remove_if(update_ptr->removed_notification_ids_, [&removed_notification_ids](auto &notification_id) {
          return removed_notification_ids.count(notification_id) == 1;
        });
      }
      for (auto &notification : update_ptr->added_notifications_) {
        if (is_removed(notification)) {
          removed_notification_ids.insert(notification->id_);
          VLOG(notifications) << "Remove " << NotificationId(notification->id_) << " in " << group_id;
          notification = nullptr;
        }
      }
      td::remove_if(update_ptr->added_notifications_, [](auto &notification) { return notification == nullptr; });
    } else {
      CHECK(update->get_id() == td_api::updateNotification::ID);
      auto update_ptr = static_cast<td_api::updateNotification *>(update.get());
      if (is_removed(update_ptr->notification_)) {
        removed_notification_ids.insert(update_ptr->notification_->id_);
        VLOG(notifications) << "Remove " << NotificationId(update_ptr->notification_->id_) << " in " << group_id;
        update = nullptr;
      }
    }
  }
}

void NotificationManager::on_get_message_notifications_from_database(NotificationGroupId group_id, size_t limit,
                                                                     Result<vector<Notification>> r_notifications) {
  auto group_it = get_group(group_id);
  CHECK(group_it != groups_.end());
  auto &group = group_it->second;
  CHECK(group.is_being_loaded_from_database == true);
  group.is_being_loaded_from_database = false;

  if (r_notifications.is_error()) {
    group.is_loaded_from_database = true;  // do not try again to load it
    return;
  }
  auto notifications = r_notifications.move_as_ok();

  CHECK(limit > 0);
  if (notifications.empty()) {
    group.is_loaded_from_database = true;
  }

  auto first_notification_id = get_first_notification_id(group);
  if (first_notification_id.is_valid()) {
    while (!notifications.empty() && notifications.back().notification_id.get() >= first_notification_id.get()) {
      // possible if notifications was added after the database request was sent
      notifications.pop_back();
    }
  }
  auto first_message_id = get_first_message_id(group);
  if (first_message_id.is_valid()) {
    while (!notifications.empty() && notifications.back().type->get_message_id() >= first_message_id) {
      // possible if notifications was added after the database request was sent
      notifications.pop_back();
    }
  }

  add_notifications_to_group_begin(std::move(group_it), std::move(notifications));

  group_it = get_group(group_id);
  CHECK(group_it != groups_.end());
  if (max_notification_group_size_ > group_it->second.notifications.size()) {
    load_message_notifications_from_database(group_it->first, group_it->second, keep_notification_group_size_);
  }
}

void NotificationManager::flush_all_pending_updates(bool include_delayed_chats, const char *source) {
  VLOG(notifications) << "Flush all pending notification updates "
                      << (include_delayed_chats ? "with delayed chats " : "") << "from " << source;
  if (!include_delayed_chats && running_get_difference_) {
    return;
  }

  vector<NotificationGroupKey> ready_group_keys;
  for (const auto &it : pending_updates_) {
    if (include_delayed_chats || running_get_chat_difference_.count(it.first) == 0) {
      auto group_it = get_group(NotificationGroupId(it.first));
      CHECK(group_it != groups_.end());
      ready_group_keys.push_back(group_it->first);
    }
  }

  // flush groups in reverse order to not exceed max_notification_group_count_
  VLOG(notifications) << "Flush pending updates in " << ready_group_keys.size() << " notification groups";
  std::sort(ready_group_keys.begin(), ready_group_keys.end());
  for (const auto &group_key : reversed(ready_group_keys)) {
    force_flush_pending_updates(group_key.group_id, "flush_all_pending_updates");
  }
  if (include_delayed_chats) {
    CHECK(pending_updates_.empty());
  }
}

void NotificationManager::send_add_group_update(const NotificationGroupKey &group_key, const NotificationGroup &group) {
  VLOG(notifications) << "Add " << group_key.group_id;
  auto total_size = group.notifications.size();
  auto added_size = min(total_size, max_notification_group_size_);
  vector<td_api::object_ptr<td_api::notification>> added_notifications;
  added_notifications.reserve(added_size);
  for (size_t i = total_size - added_size; i < total_size; i++) {
    added_notifications.push_back(get_notification_object(group_key.dialog_id, group.notifications[i]));
    if (added_notifications.back()->type_ == nullptr) {
      added_notifications.pop_back();
    }
  }

  if (!added_notifications.empty()) {
    add_update_notification_group(td_api::make_object<td_api::updateNotificationGroup>(
        group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(), 0, true,
        group.total_count, std::move(added_notifications), vector<int32>()));
  }
}

void NotificationManager::add_update(int32 group_id, td_api::object_ptr<td_api::Update> update) {
  if (!is_binlog_processed_ || !is_inited_) {
    return;
  }
  VLOG(notifications) << "Add " << as_notification_update(update.get());
  auto &updates = pending_updates_[group_id];
  if (updates.empty()) {
    on_delayed_notification_update_count_changed(1, group_id, "add_update");
  }
  updates.push_back(std::move(update));
  if (!running_get_difference_ && running_get_chat_difference_.count(group_id) == 0) {
    flush_pending_updates_timeout_.add_timeout_in(group_id, MIN_UPDATE_DELAY_MS * 1e-3);
  } else {
    flush_pending_updates_timeout_.set_timeout_in(group_id, MAX_UPDATE_DELAY_MS * 1e-3);
  }
}

void NotificationManager::after_get_chat_difference_impl(NotificationGroupId group_id) {
  if (running_get_chat_difference_.count(group_id.get()) == 1) {
    return;
  }

  VLOG(notifications) << "Flush updates after get chat difference in " << group_id;
  CHECK(group_id.is_valid());
  if (!running_get_difference_ && pending_updates_.count(group_id.get()) == 1) {
    remove_temporary_notifications(group_id, "after_get_chat_difference");
    force_flush_pending_updates(group_id, "after_get_chat_difference");
  }
}

void NotificationManager::set_contact_registered_notifications_sync_state(SyncState new_state) {
  if (is_disabled()) {
    return;
  }

  contact_registered_notifications_sync_state_ = new_state;
  string value;
  value += static_cast<char>(static_cast<int32>(new_state) + '0');
  value += static_cast<char>(static_cast<int32>(disable_contact_registered_notifications_) + '0');
  G()->td_db()->get_binlog_pmc()->set(get_is_contact_registered_notifications_synchronized_key(), value);
}

void NotificationManager::on_notification_cloud_delay_changed() {
  if (is_disabled()) {
    return;
  }

  notification_cloud_delay_ms_ = narrow_cast<int32>(
      G()->shared_config().get_option_integer("notification_cloud_delay_ms", DEFAULT_ONLINE_CLOUD_DELAY_MS));
  VLOG(notifications) << "Set notification_cloud_delay_ms to " << notification_cloud_delay_ms_;
}

void NotificationManager::send_update_have_pending_notifications() const {
  if (is_destroyed_ || !is_inited_ || !is_binlog_processed_) {
    return;
  }

  auto update = get_update_have_pending_notifications();
  VLOG(notifications) << "Send " << oneline(to_string(update));
  send_closure(G()->td(), &Td::send_update, std::move(update));
}

void NotificationManager::force_flush_pending_updates(NotificationGroupId group_id, const char *source) {
  flush_pending_updates_timeout_.cancel_timeout(group_id.get());
  flush_pending_updates(group_id.get(), source);
}

NotificationId NotificationManager::get_max_notification_id() const {
  return current_notification_id_;
}
}  // namespace td
