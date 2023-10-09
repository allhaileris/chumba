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
#include "td/telegram/ContactsManager.h"
#include "td/telegram/DeviceTokenManager.h"
#include "td/telegram/Document.h"
#include "td/telegram/Document.hpp"

#include "td/telegram/files/FileManager.h"
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



#include <limits>



namespace td {
}  // namespace td
namespace td {

void NotificationManager::remove_notification_group(NotificationGroupId group_id, NotificationId max_notification_id,
                                                    MessageId max_message_id, int32 new_total_count, bool force_update,
                                                    Promise<Unit> &&promise) {
  if (!group_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Group identifier is invalid"));
  }
  if (!max_notification_id.is_valid() && !max_message_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Notification identifier is invalid"));
  }

  if (is_disabled() || max_notification_group_count_ == 0) {
    return promise.set_value(Unit());
  }

  if (new_total_count == 0) {
    remove_temporary_notifications(group_id, "remove_notification_group");
  }

  VLOG(notifications) << "Remove " << group_id << " up to " << max_notification_id << " or " << max_message_id
                      << " with new_total_count = " << new_total_count << " and force_update = " << force_update;

  auto group_it = get_group_force(group_id);
  if (group_it == groups_.end()) {
    VLOG(notifications) << "Can't find " << group_id;
    return promise.set_value(Unit());
  }

  if (max_notification_id.is_valid()) {
    if (max_notification_id.get() > current_notification_id_.get()) {
      max_notification_id = current_notification_id_;
    }
    if (group_it->second.type != NotificationGroupType::Calls) {
      td_->messages_manager_->remove_message_notifications(
          group_it->first.dialog_id, group_id, max_notification_id,
          get_last_message_id_by_notification_id(group_it->second, max_notification_id));
    }
  }

  auto pending_delete_end = group_it->second.pending_notifications.begin();
  for (auto it = group_it->second.pending_notifications.begin(); it != group_it->second.pending_notifications.end();
       ++it) {
    if (it->notification_id.get() <= max_notification_id.get() ||
        (max_message_id.is_valid() && it->type->get_message_id() <= max_message_id)) {
      pending_delete_end = it + 1;
      on_notification_removed(it->notification_id);
    }
  }
  if (pending_delete_end != group_it->second.pending_notifications.begin()) {
    group_it->second.pending_notifications.erase(group_it->second.pending_notifications.begin(), pending_delete_end);
    if (group_it->second.pending_notifications.empty()) {
      group_it->second.pending_notifications_flush_time = 0;
      flush_pending_notifications_timeout_.cancel_timeout(group_id.get());
      on_delayed_notification_update_count_changed(-1, group_id.get(), "remove_notification_group");
    }
  }
  if (new_total_count != -1) {
    new_total_count += get_temporary_notification_total_count(group_it->second);
    new_total_count -= static_cast<int32>(group_it->second.pending_notifications.size());
    if (new_total_count < 0) {
      LOG(ERROR) << "Have wrong new_total_count " << new_total_count << " + "
                 << group_it->second.pending_notifications.size();
    }
  }

  auto old_group_size = group_it->second.notifications.size();
  auto notification_delete_end = old_group_size;
  for (size_t pos = 0; pos < notification_delete_end; pos++) {
    auto &notification = group_it->second.notifications[pos];
    if (notification.notification_id.get() > max_notification_id.get() &&
        (!max_message_id.is_valid() || notification.type->get_message_id() > max_message_id)) {
      notification_delete_end = pos;
    } else {
      on_notification_removed(notification.notification_id);
    }
  }

  bool is_found = notification_delete_end != 0;

  vector<int32> removed_notification_ids;
  if (is_found && notification_delete_end + max_notification_group_size_ > old_group_size) {
    for (size_t i = old_group_size >= max_notification_group_size_ ? old_group_size - max_notification_group_size_ : 0;
         i < notification_delete_end; i++) {
      removed_notification_ids.push_back(group_it->second.notifications[i].notification_id.get());
    }
  }

  VLOG(notifications) << "Need to delete " << notification_delete_end << " from "
                      << group_it->second.notifications.size() << " notifications";
  if (is_found) {
    group_it->second.notifications.erase(group_it->second.notifications.begin(),
                                         group_it->second.notifications.begin() + notification_delete_end);
  }
  if (group_it->second.type == NotificationGroupType::Calls ||
      group_it->second.type == NotificationGroupType::SecretChat) {
    new_total_count = static_cast<int32>(group_it->second.notifications.size());
  }
  if (group_it->second.total_count == new_total_count) {
    new_total_count = -1;
  }
  if (new_total_count != -1) {
    group_it->second.total_count = new_total_count;
  }

  if (new_total_count != -1 || !removed_notification_ids.empty()) {
    on_notifications_removed(std::move(group_it), vector<td_api::object_ptr<td_api::notification>>(),
                             std::move(removed_notification_ids), force_update);
  } else {
    VLOG(notifications) << "Have new_total_count = " << new_total_count << ", " << removed_notification_ids.size()
                        << " removed notifications and force_update = " << force_update;
    if (force_update) {
      force_flush_pending_updates(group_id, "remove_notification_group");
    }
  }

  if (max_notification_id.is_valid()) {
    remove_added_notifications_from_pending_updates(
        group_id, [max_notification_id](const td_api::object_ptr<td_api::notification> &notification) {
          return notification->id_ <= max_notification_id.get();
        });
  } else {
    remove_added_notifications_from_pending_updates(
        group_id, [max_message_id](const td_api::object_ptr<td_api::notification> &notification) {
          return notification->type_->get_id() == td_api::notificationTypeNewMessage::ID &&
                 static_cast<const td_api::notificationTypeNewMessage *>(notification->type_.get())->message_->id_ <=
                     max_message_id.get();
        });
  }

  promise.set_value(Unit());
}

void NotificationManager::on_notification_group_count_max_changed(bool send_updates) {
  if (is_disabled()) {
    return;
  }

  auto new_max_notification_group_count = narrow_cast<int32>(
      G()->shared_config().get_option_integer("notification_group_count_max", DEFAULT_GROUP_COUNT_MAX));
  CHECK(MIN_NOTIFICATION_GROUP_COUNT_MAX <= new_max_notification_group_count &&
        new_max_notification_group_count <= MAX_NOTIFICATION_GROUP_COUNT_MAX);

  auto new_max_notification_group_count_size_t = static_cast<size_t>(new_max_notification_group_count);
  if (new_max_notification_group_count_size_t == max_notification_group_count_) {
    return;
  }

  VLOG(notifications) << "Change max notification group count from " << max_notification_group_count_ << " to "
                      << new_max_notification_group_count;

  bool is_increased = new_max_notification_group_count_size_t > max_notification_group_count_;
  if (send_updates) {
    flush_all_notifications();

    size_t cur_pos = 0;
    size_t min_group_count = min(new_max_notification_group_count_size_t, max_notification_group_count_);
    size_t max_group_count = max(new_max_notification_group_count_size_t, max_notification_group_count_);
    for (auto it = groups_.begin(); it != groups_.end() && cur_pos < max_group_count; ++it, cur_pos++) {
      if (cur_pos < min_group_count) {
        continue;
      }

      auto &group_key = it->first;
      auto &group = it->second;
      CHECK(group.pending_notifications.empty());
      CHECK(pending_updates_.count(group_key.group_id.get()) == 0);

      if (group_key.last_notification_date == 0) {
        break;
      }

      if (is_increased) {
        send_add_group_update(group_key, group);
      } else {
        send_remove_group_update(group_key, group, vector<int32>());
      }
    }

    flush_all_pending_updates(true, "on_notification_group_size_max_changed end");

    if (new_max_notification_group_count == 0) {
      last_loaded_notification_group_key_ = NotificationGroupKey();
      last_loaded_notification_group_key_.last_notification_date = std::numeric_limits<int32>::max();
      CHECK(pending_updates_.empty());
      groups_.clear();
      group_keys_.clear();
    }
  }

  max_notification_group_count_ = new_max_notification_group_count_size_t;
  if (is_increased && last_loaded_notification_group_key_ < get_last_updated_group_key()) {
    load_message_notification_groups_from_database(td::max(new_max_notification_group_count, 5), true);
  }
}

int32 NotificationManager::get_notification_delay_ms(DialogId dialog_id, const PendingNotification &notification,
                                                     int32 min_delay_ms) const {
  if (dialog_id.get_type() == DialogType::SecretChat) {
    return MIN_NOTIFICATION_DELAY_MS;  // there is no reason to delay notifications in secret chats
  }
  if (!notification.type->can_be_delayed()) {
    return MIN_NOTIFICATION_DELAY_MS;
  }

  auto delay_ms = [&] {
    auto online_info = td_->contacts_manager_->get_my_online_status();
    if (!online_info.is_online_local && online_info.is_online_remote) {
      // If we are offline, but online from some other client, then delay notification
      // for 'notification_cloud_delay' seconds.
      return notification_cloud_delay_ms_;
    }

    if (!online_info.is_online_local &&
        online_info.was_online_remote > max(static_cast<double>(online_info.was_online_local),
                                            G()->server_time_cached() - online_cloud_timeout_ms_ * 1e-3)) {
      // If we are offline, but was online from some other client in last 'online_cloud_timeout' seconds
      // after we had gone offline, then delay notification for 'notification_cloud_delay' seconds.
      return notification_cloud_delay_ms_;
    }

    if (online_info.is_online_remote) {
      // If some other client is online, then delay notification for 'notification_default_delay' seconds.
      return notification_default_delay_ms_;
    }

    // otherwise send update without additional delay
    return 0;
  }();

  auto passed_time_ms =
      static_cast<int32>(clamp(G()->server_time_cached() - notification.date - 1, 0.0, 1000000.0) * 1000);
  return max(max(min_delay_ms, delay_ms) - passed_time_ms, MIN_NOTIFICATION_DELAY_MS);
}

void NotificationManager::remove_temporary_notification_by_message_id(NotificationGroupId group_id,
                                                                      MessageId message_id, bool force_update,
                                                                      const char *source) {
  if (!group_id.is_valid()) {
    return;
  }

  VLOG(notifications) << "Remove notification for " << message_id << " in " << group_id << " from " << source;
  CHECK(message_id.is_valid());

  auto group_it = get_group(group_id);
  if (group_it == groups_.end()) {
    return;
  }

  auto remove_notification_by_message_id = [&](auto &notifications) {
    for (auto &notification : notifications) {
      if (notification.type->get_message_id() == message_id) {
        for (auto file_id : notification.type->get_file_ids(td_)) {
          this->td_->file_manager_->delete_file(file_id, Promise<>(), "remove_temporary_notification_by_message_id");
        }
        return this->remove_notification(group_id, notification.notification_id, true, force_update, Auto(),
                                         "remove_temporary_notification_by_message_id");
      }
    }
  };

  remove_notification_by_message_id(group_it->second.pending_notifications);
  remove_notification_by_message_id(group_it->second.notifications);
}

void NotificationManager::on_flush_pending_notifications_timeout_callback(void *notification_manager_ptr,
                                                                          int64 group_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto notification_manager = static_cast<NotificationManager *>(notification_manager_ptr);
  VLOG(notifications) << "Ready to flush pending notifications for notification group " << group_id_int;
  if (group_id_int > 0) {
    send_closure_later(notification_manager->actor_id(notification_manager),
                       &NotificationManager::flush_pending_notifications,
                       NotificationGroupId(narrow_cast<int32>(group_id_int)));
  } else if (group_id_int == 0) {
    send_closure_later(notification_manager->actor_id(notification_manager),
                       &NotificationManager::after_get_difference_impl);
  } else {
    send_closure_later(notification_manager->actor_id(notification_manager),
                       &NotificationManager::after_get_chat_difference_impl,
                       NotificationGroupId(narrow_cast<int32>(-group_id_int)));
  }
}

void NotificationManager::on_unreceived_notification_update_count_changed(int32 diff, int32 notification_group_id,
                                                                          const char *source) {
  bool had_unreceived = unreceived_notification_update_count_ != 0;
  unreceived_notification_update_count_ += diff;
  CHECK(unreceived_notification_update_count_ >= 0);
  VLOG(notifications) << "Update unreceived notification count with diff " << diff << " to "
                      << unreceived_notification_update_count_ << " from group " << notification_group_id << " and "
                      << source;
  bool have_unreceived = unreceived_notification_update_count_ != 0;
  if (had_unreceived != have_unreceived) {
    send_update_have_pending_notifications();
  }
}

void NotificationManager::save_announcement_ids() {
  auto min_date = G()->unix_time() - ANNOUNCEMENT_ID_CACHE_TIME;
  vector<int32> ids;
  for (auto &it : announcement_id_date_) {
    auto id = it.first;
    auto date = it.second;
    if (date < min_date) {
      continue;
    }
    ids.push_back(id);
    ids.push_back(date);
  }

  VLOG(notifications) << "Save announcement ids " << ids;
  if (ids.empty()) {
    G()->td_db()->get_binlog_pmc()->erase("notification_announcement_ids");
    return;
  }

  auto notification_announcement_ids_string = implode(transform(ids, [](int32 id) { return to_string(id); }), ',');
  G()->td_db()->get_binlog_pmc()->set("notification_announcement_ids", notification_announcement_ids_string);
}

void NotificationManager::after_get_chat_difference(NotificationGroupId group_id) {
  if (is_disabled()) {
    return;
  }

  VLOG(notifications) << "After get chat difference in " << group_id;
  CHECK(group_id.is_valid());
  auto erased_count = running_get_chat_difference_.erase(group_id.get());
  if (erased_count == 1) {
    flush_pending_notifications_timeout_.set_timeout_in(-group_id.get(), MIN_NOTIFICATION_DELAY_MS * 1e-3);
    on_unreceived_notification_update_count_changed(-1, group_id.get(), "after_get_chat_difference");
  }
}

void NotificationManager::send_remove_group_update(const NotificationGroupKey &group_key,
                                                   const NotificationGroup &group,
                                                   vector<int32> &&removed_notification_ids) {
  VLOG(notifications) << "Remove " << group_key.group_id;
  auto update = get_remove_group_update(group_key, group, std::move(removed_notification_ids));
  if (update != nullptr) {
    add_update_notification_group(std::move(update));
  }
}

void NotificationManager::on_get_disable_contact_registered_notifications(bool is_disabled) {
  if (disable_contact_registered_notifications_ == is_disabled) {
    return;
  }
  disable_contact_registered_notifications_ = is_disabled;

  if (is_disabled) {
    G()->shared_config().set_option_boolean("disable_contact_registered_notifications", is_disabled);
  } else {
    G()->shared_config().set_option_empty("disable_contact_registered_notifications");
  }
}

void NotificationManager::get_current_state(vector<td_api::object_ptr<td_api::Update>> &updates) const {
  if (is_disabled() || max_notification_group_count_ == 0 || is_destroyed_) {
    return;
  }

  updates.push_back(get_update_active_notifications());
  updates.push_back(get_update_have_pending_notifications());
}

NotificationManager::NotificationUpdate NotificationManager::as_notification_update(const td_api::Update *update) {
  return NotificationUpdate{update};
}

void NotificationManager::flush_all_notifications() {
  flush_all_pending_notifications();
  flush_all_pending_updates(true, "flush_all_notifications");
}
}  // namespace td
