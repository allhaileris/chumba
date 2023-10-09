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

void NotificationManager::remove_notification(NotificationGroupId group_id, NotificationId notification_id,
                                              bool is_permanent, bool force_update, Promise<Unit> &&promise,
                                              const char *source) {
  if (!group_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Notification group identifier is invalid"));
  }
  if (!notification_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Notification identifier is invalid"));
  }

  if (is_disabled() || max_notification_group_count_ == 0) {
    return promise.set_value(Unit());
  }

  VLOG(notifications) << "Remove " << notification_id << " from " << group_id << " with is_permanent = " << is_permanent
                      << ", force_update = " << force_update << " from " << source;

  auto group_it = get_group_force(group_id);
  if (group_it == groups_.end()) {
    return promise.set_value(Unit());
  }

  if (!is_permanent && group_it->second.type != NotificationGroupType::Calls) {
    td_->messages_manager_->remove_message_notification(group_it->first.dialog_id, group_id, notification_id);
  }

  for (auto it = group_it->second.pending_notifications.begin(); it != group_it->second.pending_notifications.end();
       ++it) {
    if (it->notification_id == notification_id) {
      // notification is still pending, just delete it
      on_notification_removed(notification_id);
      group_it->second.pending_notifications.erase(it);
      if (group_it->second.pending_notifications.empty()) {
        group_it->second.pending_notifications_flush_time = 0;
        flush_pending_notifications_timeout_.cancel_timeout(group_id.get());
        on_delayed_notification_update_count_changed(-1, group_id.get(), "remove_notification");
      }
      return promise.set_value(Unit());
    }
  }

  bool is_found = false;
  auto old_group_size = group_it->second.notifications.size();
  size_t notification_pos = old_group_size;
  for (size_t pos = 0; pos < notification_pos; pos++) {
    if (group_it->second.notifications[pos].notification_id == notification_id) {
      on_notification_removed(notification_id);
      notification_pos = pos;
      is_found = true;
    }
  }

  bool have_all_notifications = group_it->second.type == NotificationGroupType::Calls ||
                                group_it->second.type == NotificationGroupType::SecretChat;
  bool is_total_count_changed = false;
  if ((!have_all_notifications && is_permanent) || (have_all_notifications && is_found)) {
    if (group_it->second.total_count == 0) {
      LOG(ERROR) << "Total notification count became negative in " << group_it->second << " after removing "
                 << notification_id << " with is_permanent = " << is_permanent << ", is_found = " << is_found
                 << ", force_update = " << force_update << " from " << source;
    } else {
      group_it->second.total_count--;
      is_total_count_changed = true;
    }
  }
  if (is_found) {
    group_it->second.notifications.erase(group_it->second.notifications.begin() + notification_pos);
  }

  vector<td_api::object_ptr<td_api::notification>> added_notifications;
  vector<int32> removed_notification_ids;
  CHECK(max_notification_group_size_ > 0);
  if (is_found && notification_pos + max_notification_group_size_ >= old_group_size) {
    removed_notification_ids.push_back(notification_id.get());
    if (old_group_size >= max_notification_group_size_ + 1) {
      added_notifications.push_back(
          get_notification_object(group_it->first.dialog_id,
                                  group_it->second.notifications[old_group_size - max_notification_group_size_ - 1]));
      if (added_notifications.back()->type_ == nullptr) {
        added_notifications.pop_back();
      }
    }
    if (added_notifications.empty() && max_notification_group_size_ > group_it->second.notifications.size()) {
      load_message_notifications_from_database(group_it->first, group_it->second, keep_notification_group_size_);
    }
  }

  if (is_total_count_changed || !removed_notification_ids.empty()) {
    on_notifications_removed(std::move(group_it), std::move(added_notifications), std::move(removed_notification_ids),
                             force_update);
  }

  remove_added_notifications_from_pending_updates(
      group_id, [notification_id](const td_api::object_ptr<td_api::notification> &notification) {
        return notification->id_ == notification_id.get();
      });

  promise.set_value(Unit());
}

void NotificationManager::add_notification(NotificationGroupId group_id, NotificationGroupType group_type,
                                           DialogId dialog_id, int32 date, DialogId notification_settings_dialog_id,
                                           bool initial_is_silent, bool is_silent, int32 min_delay_ms,
                                           NotificationId notification_id, unique_ptr<NotificationType> type,
                                           const char *source) {
  if (is_disabled() || max_notification_group_count_ == 0) {
    on_notification_removed(notification_id);
    return;
  }

  CHECK(group_id.is_valid());
  CHECK(dialog_id.is_valid());
  CHECK(notification_settings_dialog_id.is_valid());
  LOG_CHECK(notification_id.is_valid()) << notification_id << " " << source;
  CHECK(type != nullptr);
  VLOG(notifications) << "Add " << notification_id << " to " << group_id << " of type " << group_type << " in "
                      << dialog_id << " with settings from " << notification_settings_dialog_id
                      << (is_silent ? "   silently" : " with sound") << ": " << *type;

  if (!type->is_temporary()) {
    remove_temporary_notifications(group_id, "add_notification");
  }

  auto group_it = get_group_force(group_id);
  if (group_it == groups_.end()) {
    group_it = add_group(NotificationGroupKey(group_id, dialog_id, 0), NotificationGroup(), "add_notification");
  }
  if (group_it->second.notifications.empty() && group_it->second.pending_notifications.empty()) {
    group_it->second.type = group_type;
  }
  CHECK(group_it->second.type == group_type);

  NotificationGroup &group = group_it->second;
  if (notification_id.get() <= get_last_notification_id(group).get()) {
    LOG(ERROR) << "Failed to add " << notification_id << " to " << group_id << " of type " << group_type << " in "
               << dialog_id << ", because have already added " << get_last_notification_id(group);
    on_notification_removed(notification_id);
    return;
  }
  auto message_id = type->get_message_id();
  if (message_id.is_valid() && message_id <= get_last_message_id(group)) {
    LOG(ERROR) << "Failed to add " << notification_id << " of type " << *type << " to " << group_id << " of type "
               << group_type << " in " << dialog_id << ", because have already added notification about "
               << get_last_message_id(group);
    on_notification_removed(notification_id);
    return;
  }

  PendingNotification notification;
  notification.date = date;
  notification.settings_dialog_id = notification_settings_dialog_id;
  notification.initial_is_silent = initial_is_silent;
  notification.is_silent = is_silent;
  notification.notification_id = notification_id;
  notification.type = std::move(type);

  auto delay_ms = get_notification_delay_ms(dialog_id, notification, min_delay_ms);
  VLOG(notifications) << "Delay " << notification_id << " for " << delay_ms << " milliseconds";
  auto flush_time = delay_ms * 0.001 + Time::now();

  if (group.pending_notifications_flush_time == 0 || flush_time < group.pending_notifications_flush_time) {
    group.pending_notifications_flush_time = flush_time;
    flush_pending_notifications_timeout_.set_timeout_at(group_id.get(), group.pending_notifications_flush_time);
  }
  if (group.pending_notifications.empty()) {
    on_delayed_notification_update_count_changed(1, group_id.get(), source);
  }
  group.pending_notifications.push_back(std::move(notification));
}

class SetContactSignUpNotificationQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit SetContactSignUpNotificationQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(bool is_disabled) {
    send_query(G()->net_query_creator().create(telegram_api::account_setContactSignUpNotification(is_disabled)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_setContactSignUpNotification>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for set contact sign up notification: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void NotificationManager::run_contact_registered_notifications_sync() {
  if (is_disabled()) {
    return;
  }

  auto is_disabled = disable_contact_registered_notifications_;
  if (contact_registered_notifications_sync_state_ == SyncState::NotSynced && !is_disabled) {
    set_contact_registered_notifications_sync_state(SyncState::Completed);
    return;
  }
  if (contact_registered_notifications_sync_state_ != SyncState::Pending) {
    set_contact_registered_notifications_sync_state(SyncState::Pending);
  }

  VLOG(notifications) << "Send SetContactSignUpNotificationQuery with " << is_disabled;
  auto promise = PromiseCreator::lambda([actor_id = actor_id(this), is_disabled](Result<Unit> result) {
    send_closure(actor_id, &NotificationManager::on_contact_registered_notifications_sync, is_disabled,
                 std::move(result));
  });
  td_->create_handler<SetContactSignUpNotificationQuery>(std::move(promise))->send(is_disabled);
}

NotificationGroupId NotificationManager::get_call_notification_group_id(DialogId dialog_id) {
  auto it = dialog_id_to_call_notification_group_id_.find(dialog_id);
  if (it != dialog_id_to_call_notification_group_id_.end()) {
    return it->second;
  }

  if (available_call_notification_group_ids_.empty()) {
    // need to reserve new group_id for calls
    if (call_notification_group_ids_.size() >= MAX_CALL_NOTIFICATION_GROUPS) {
      return {};
    }
    NotificationGroupId last_group_id;
    if (!call_notification_group_ids_.empty()) {
      last_group_id = call_notification_group_ids_.back();
    }
    NotificationGroupId next_notification_group_id;
    do {
      next_notification_group_id = get_next_notification_group_id();
      if (!next_notification_group_id.is_valid()) {
        return {};
      }
    } while (last_group_id.get() >= next_notification_group_id.get());  // just in case
    VLOG(notifications) << "Add call " << next_notification_group_id;

    call_notification_group_ids_.push_back(next_notification_group_id);
    auto call_notification_group_ids_string = implode(
        transform(call_notification_group_ids_, [](NotificationGroupId group_id) { return to_string(group_id.get()); }),
        ',');
    G()->td_db()->get_binlog_pmc()->set("notification_call_group_ids", call_notification_group_ids_string);
    available_call_notification_group_ids_.insert(next_notification_group_id);
  }

  auto available_it = available_call_notification_group_ids_.begin();
  auto group_id = *available_it;
  available_call_notification_group_ids_.erase(available_it);
  dialog_id_to_call_notification_group_id_[dialog_id] = group_id;
  return group_id;
}

class GetContactSignUpNotificationQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetContactSignUpNotificationQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::account_getContactSignUpNotification()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_getContactSignUpNotification>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->notification_manager_->on_get_disable_contact_registered_notifications(result_ptr.ok());
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for get contact sign up notification: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void NotificationManager::get_disable_contact_registered_notifications(Promise<Unit> &&promise) {
  if (is_disabled()) {
    promise.set_value(Unit());
    return;
  }

  td_->create_handler<GetContactSignUpNotificationQuery>(std::move(promise))->send();
}

td_api::object_ptr<td_api::updateNotificationGroup> NotificationManager::get_remove_group_update(
    const NotificationGroupKey &group_key, const NotificationGroup &group,
    vector<int32> &&removed_notification_ids) const {
  auto total_size = group.notifications.size();
  CHECK(removed_notification_ids.size() <= max_notification_group_size_);
  auto removed_size = min(total_size, max_notification_group_size_ - removed_notification_ids.size());
  removed_notification_ids.reserve(removed_size + removed_notification_ids.size());
  for (size_t i = total_size - removed_size; i < total_size; i++) {
    removed_notification_ids.push_back(group.notifications[i].notification_id.get());
  }

  if (removed_notification_ids.empty()) {
    return nullptr;
  }
  return td_api::make_object<td_api::updateNotificationGroup>(
      group_key.group_id.get(), get_notification_group_type_object(group.type), group_key.dialog_id.get(),
      group_key.dialog_id.get(), true, group.total_count, vector<td_api::object_ptr<td_api::notification>>(),
      std::move(removed_notification_ids));
}

NotificationManager::NotificationGroups::iterator NotificationManager::add_group(NotificationGroupKey &&group_key,
                                                                                 NotificationGroup &&group,
                                                                                 const char *source) {
  if (group.notifications.empty()) {
    LOG_CHECK(group_key.last_notification_date == 0) << "Trying to add empty " << group_key << " from " << source;
  }
  bool is_inserted = group_keys_.emplace(group_key.group_id, group_key).second;
  CHECK(is_inserted);
  return groups_.emplace(std::move(group_key), std::move(group)).first;
}

void NotificationManager::on_flush_pending_updates_timeout_callback(void *notification_manager_ptr,
                                                                    int64 group_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto notification_manager = static_cast<NotificationManager *>(notification_manager_ptr);
  send_closure_later(notification_manager->actor_id(notification_manager), &NotificationManager::flush_pending_updates,
                     narrow_cast<int32>(group_id_int), "timeout");
}

NotificationManager::NotificationManager(Td *td, ActorShared<> parent) : td_(td), parent_(std::move(parent)) {
  flush_pending_notifications_timeout_.set_callback(on_flush_pending_notifications_timeout_callback);
  flush_pending_notifications_timeout_.set_callback_data(static_cast<void *>(this));

  flush_pending_updates_timeout_.set_callback(on_flush_pending_updates_timeout_callback);
  flush_pending_updates_timeout_.set_callback_data(static_cast<void *>(this));
}

void NotificationManager::on_notification_default_delay_changed() {
  if (is_disabled()) {
    return;
  }

  notification_default_delay_ms_ = narrow_cast<int32>(
      G()->shared_config().get_option_integer("notification_default_delay_ms", DEFAULT_DEFAULT_DELAY_MS));
  VLOG(notifications) << "Set notification_default_delay_ms to " << notification_default_delay_ms_;
}

void NotificationManager::after_get_difference() {
  if (is_disabled()) {
    return;
  }

  CHECK(running_get_difference_);
  running_get_difference_ = false;
  on_unreceived_notification_update_count_changed(-1, 0, "after_get_difference");
  flush_pending_notifications_timeout_.set_timeout_in(0, MIN_NOTIFICATION_DELAY_MS * 1e-3);
}

void NotificationManager::before_get_difference() {
  if (is_disabled()) {
    return;
  }
  if (running_get_difference_) {
    return;
  }

  running_get_difference_ = true;
  on_unreceived_notification_update_count_changed(1, 0, "before_get_difference");
}

int VERBOSITY_NAME(notifications) = VERBOSITY_NAME(INFO);
}  // namespace td
