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



#include <limits>



namespace td {
}  // namespace td
namespace td {

void NotificationManager::init() {
  if (is_disabled()) {
    return;
  }

  disable_contact_registered_notifications_ =
      G()->shared_config().get_option_boolean("disable_contact_registered_notifications");
  auto sync_state = G()->td_db()->get_binlog_pmc()->get(get_is_contact_registered_notifications_synchronized_key());
  if (sync_state.empty()) {
    sync_state = "00";
  }
  contact_registered_notifications_sync_state_ = static_cast<SyncState>(sync_state[0] - '0');
  VLOG(notifications) << "Loaded disable_contact_registered_notifications = "
                      << disable_contact_registered_notifications_ << " in state " << sync_state;
  if (contact_registered_notifications_sync_state_ != SyncState::Completed ||
      sync_state[1] != static_cast<int32>(disable_contact_registered_notifications_) + '0') {
    run_contact_registered_notifications_sync();
  }

  current_notification_id_ =
      NotificationId(to_integer<int32>(G()->td_db()->get_binlog_pmc()->get("notification_id_current")));
  current_notification_group_id_ =
      NotificationGroupId(to_integer<int32>(G()->td_db()->get_binlog_pmc()->get("notification_group_id_current")));

  VLOG(notifications) << "Loaded current " << current_notification_id_ << " and " << current_notification_group_id_;

  on_notification_group_count_max_changed(false);
  on_notification_group_size_max_changed();

  on_online_cloud_timeout_changed();
  on_notification_cloud_delay_changed();
  on_notification_default_delay_changed();

  last_loaded_notification_group_key_.last_notification_date = std::numeric_limits<int32>::max();
  if (max_notification_group_count_ != 0) {
    int32 loaded_groups = 0;
    auto needed_groups = static_cast<int32>(max_notification_group_count_);
    do {
      loaded_groups += load_message_notification_groups_from_database(needed_groups, false);
    } while (loaded_groups < needed_groups && last_loaded_notification_group_key_.last_notification_date != 0);
  }

  auto call_notification_group_ids_string = G()->td_db()->get_binlog_pmc()->get("notification_call_group_ids");
  if (!call_notification_group_ids_string.empty()) {
    auto call_notification_group_ids = transform(full_split(call_notification_group_ids_string, ','), [](Slice str) {
      return NotificationGroupId{to_integer_safe<int32>(str).ok()};
    });
    VLOG(notifications) << "Load call_notification_group_ids = " << call_notification_group_ids;
    for (auto &group_id : call_notification_group_ids) {
      if (group_id.get() > current_notification_group_id_.get()) {
        LOG(ERROR) << "Fix current notification group identifier from " << current_notification_group_id_ << " to "
                   << group_id;
        current_notification_group_id_ = group_id;
        G()->td_db()->get_binlog_pmc()->set("notification_group_id_current",
                                            to_string(current_notification_group_id_.get()));
      }
      auto it = get_group_force(group_id);
      if (it != groups_.end()) {
        LOG(ERROR) << "Have " << it->first << " " << it->second << " as a call notification group";
      } else {
        call_notification_group_ids_.push_back(group_id);
        available_call_notification_group_ids_.insert(group_id);
      }
    }
  }

  auto notification_announcement_ids_string = G()->td_db()->get_binlog_pmc()->get("notification_announcement_ids");
  if (!notification_announcement_ids_string.empty()) {
    VLOG(notifications) << "Load announcement ids = " << notification_announcement_ids_string;
    auto ids = transform(full_split(notification_announcement_ids_string, ','),
                         [](Slice str) { return to_integer_safe<int32>(str).ok(); });
    CHECK(ids.size() % 2 == 0);
    bool is_changed = false;
    auto min_date = G()->unix_time() - ANNOUNCEMENT_ID_CACHE_TIME;
    for (size_t i = 0; i < ids.size(); i += 2) {
      auto id = ids[i];
      auto date = ids[i + 1];
      if (date < min_date) {
        is_changed = true;
        continue;
      }
      announcement_id_date_.emplace(id, date);
    }
    if (is_changed) {
      save_announcement_ids();
    }
  }

  class StateCallback final : public StateManager::Callback {
   public:
    explicit StateCallback(ActorId<NotificationManager> parent) : parent_(std::move(parent)) {
    }
    bool on_online(bool is_online) final {
      if (is_online) {
        send_closure(parent_, &NotificationManager::flush_all_pending_notifications);
      }
      return parent_.is_alive();
    }

   private:
    ActorId<NotificationManager> parent_;
  };
  send_closure(G()->state_manager(), &StateManager::add_callback, make_unique<StateCallback>(actor_id(this)));

  is_inited_ = true;
  try_send_update_active_notifications();
}

void NotificationManager::process_push_notification(string payload, Promise<Unit> &&user_promise) {
  auto promise = PromiseCreator::lambda([user_promise = std::move(user_promise)](Result<Unit> &&result) mutable {
    if (result.is_error()) {
      if (result.error().code() == 200) {
        user_promise.set_value(Unit());
      } else {
        user_promise.set_error(result.move_as_error());
      }
    } else {
      create_actor<SleepActor>("FinishProcessPushNotificationActor", 0.01, std::move(user_promise)).release();
    }
  });

  if (is_disabled() || payload == "{}") {
    return promise.set_error(Status::Error(200, "Immediate success"));
  }

  auto r_receiver_id = get_push_receiver_id(payload);
  if (r_receiver_id.is_error()) {
    VLOG(notifications) << "Failed to get push notification receiver from \"" << format::escaped(payload)
                        << "\":" << r_receiver_id.is_error();
    return promise.set_error(r_receiver_id.move_as_error());
  }

  auto receiver_id = r_receiver_id.move_as_ok();
  auto encryption_keys = td_->device_token_manager_->get_actor_unsafe()->get_encryption_keys();
  VLOG(notifications) << "Process push notification \"" << format::escaped(payload)
                      << "\" with receiver_id = " << receiver_id << " and " << encryption_keys.size()
                      << " encryption keys";
  bool was_encrypted = false;
  for (auto &key : encryption_keys) {
    VLOG(notifications) << "Have key " << key.first;
    // VLOG(notifications) << "Have key " << key.first << ": \"" << format::escaped(key.second) << '"';
    if (key.first == receiver_id) {
      if (!key.second.empty()) {
        auto r_payload = decrypt_push(key.first, key.second.str(), std::move(payload));
        if (r_payload.is_error()) {
          LOG(ERROR) << "Failed to decrypt push: " << r_payload.error();
          return promise.set_error(Status::Error(400, "Failed to decrypt push payload"));
        }
        payload = r_payload.move_as_ok();
        was_encrypted = true;
      }
      receiver_id = 0;
      break;
    }
  }

  if (!td_->is_online()) {
    // reset online flag to false to immediately check all connections aliveness
    send_closure(G()->state_manager(), &StateManager::on_online, false);
  }

  if (receiver_id == 0 || receiver_id == G()->get_my_id()) {
    auto status = process_push_notification_payload(payload, was_encrypted, promise);
    if (status.is_error()) {
      if (status.code() == 406 || status.code() == 200) {
        return promise.set_error(std::move(status));
      }

      LOG(ERROR) << "Receive error " << status << ", while parsing push payload " << payload;
      return promise.set_error(Status::Error(400, status.message()));
    }
    // promise will be set after updateNotificationGroup is sent to the client
    return;
  }

  VLOG(notifications) << "Failed to process push notification";
  promise.set_error(Status::Error(200, "Immediate success"));
}

Result<int64> NotificationManager::get_push_receiver_id(string payload) {
  if (payload == "{}") {
    return static_cast<int64>(0);
  }

  auto r_json_value = json_decode(payload);
  if (r_json_value.is_error()) {
    return Status::Error(400, "Failed to parse payload as JSON object");
  }

  auto json_value = r_json_value.move_as_ok();
  if (json_value.type() != JsonValue::Type::Object) {
    return Status::Error(400, "Expected JSON object");
  }

  auto data = std::move(json_value.get_object());
  if (has_json_object_field(data, "data")) {
    auto r_data_data = get_json_object_field(data, "data", JsonValue::Type::Object, false);
    if (r_data_data.is_error()) {
      return Status::Error(400, r_data_data.error().message());
    }
    auto data_data = r_data_data.move_as_ok();
    data = std::move(data_data.get_object());
  }

  for (auto &field_value : data) {
    if (field_value.first == "p") {
      auto encrypted_payload = std::move(field_value.second);
      if (encrypted_payload.type() != JsonValue::Type::String) {
        return Status::Error(400, "Expected encrypted payload as a String");
      }
      Slice encrypted_data = encrypted_payload.get_string();
      if (encrypted_data.size() < 12) {
        return Status::Error(400, "Encrypted payload is too small");
      }
      auto r_decoded = base64url_decode(encrypted_data.substr(0, 12));
      if (r_decoded.is_error()) {
        return Status::Error(400, "Failed to base64url-decode payload");
      }
      CHECK(r_decoded.ok().size() == 9);
      return as<int64>(r_decoded.ok().c_str());
    }
    if (field_value.first == "user_id") {
      auto user_id = std::move(field_value.second);
      if (user_id.type() != JsonValue::Type::String && user_id.type() != JsonValue::Type::Number) {
        return Status::Error(400, "Expected user_id as a String or a Number");
      }
      Slice user_id_str = user_id.type() == JsonValue::Type::String ? user_id.get_string() : user_id.get_number();
      auto r_user_id = to_integer_safe<int64>(user_id_str);
      if (r_user_id.is_error()) {
        return Status::Error(400, PSLICE() << "Failed to get user_id from " << user_id_str);
      }
      if (r_user_id.ok() <= 0) {
        return Status::Error(400, PSLICE() << "Receive wrong user_id " << user_id_str);
      }
      return r_user_id.ok();
    }
  }

  return static_cast<int64>(0);
}

void NotificationManager::set_notification_total_count(NotificationGroupId group_id, int32 new_total_count) {
  if (!group_id.is_valid()) {
    return;
  }
  if (is_disabled() || max_notification_group_count_ == 0) {
    return;
  }

  auto group_it = get_group_force(group_id);
  if (group_it == groups_.end()) {
    VLOG(notifications) << "Can't find " << group_id;
    return;
  }

  new_total_count += get_temporary_notification_total_count(group_it->second);
  new_total_count -= static_cast<int32>(group_it->second.pending_notifications.size());
  if (new_total_count < 0) {
    LOG(ERROR) << "Have wrong new_total_count " << new_total_count << " after removing "
               << group_it->second.pending_notifications.size() << " pending notifications";
    return;
  }
  if (new_total_count < static_cast<int32>(group_it->second.notifications.size())) {
    LOG(ERROR) << "Have wrong new_total_count " << new_total_count << " less than number of known notifications "
               << group_it->second.notifications.size();
    return;
  }

  CHECK(group_it->second.type != NotificationGroupType::Calls);
  if (group_it->second.total_count == new_total_count) {
    return;
  }

  VLOG(notifications) << "Set total_count in " << group_id << " to " << new_total_count;
  group_it->second.total_count = new_total_count;

  on_notifications_removed(std::move(group_it), vector<td_api::object_ptr<td_api::notification>>(), vector<int32>(),
                           false);
}

Result<string> NotificationManager::decrypt_push(int64 encryption_key_id, string encryption_key, string push) {
  auto r_json_value = json_decode(push);
  if (r_json_value.is_error()) {
    return Status::Error(400, "Failed to parse payload as JSON object");
  }

  auto json_value = r_json_value.move_as_ok();
  if (json_value.type() != JsonValue::Type::Object) {
    return Status::Error(400, "Expected JSON object");
  }

  for (auto &field_value : json_value.get_object()) {
    if (field_value.first == "p") {
      auto encrypted_payload = std::move(field_value.second);
      if (encrypted_payload.type() != JsonValue::Type::String) {
        return Status::Error(400, "Expected encrypted payload as a String");
      }
      Slice encrypted_data = encrypted_payload.get_string();
      if (encrypted_data.size() < 12) {
        return Status::Error(400, "Encrypted payload is too small");
      }
      auto r_decoded = base64url_decode(encrypted_data);
      if (r_decoded.is_error()) {
        return Status::Error(400, "Failed to base64url-decode payload");
      }
      return decrypt_push_payload(encryption_key_id, std::move(encryption_key), r_decoded.move_as_ok());
    }
  }
  return Status::Error(400, "No 'p'(payload) field found in push");
}

vector<MessageId> NotificationManager::get_notification_group_message_ids(NotificationGroupId group_id) {
  CHECK(group_id.is_valid());
  if (is_disabled() || max_notification_group_count_ == 0) {
    return {};
  }

  auto group_it = get_group_force(group_id);
  if (group_it == groups_.end()) {
    return {};
  }

  vector<MessageId> message_ids;
  for (auto &notification : group_it->second.notifications) {
    auto message_id = notification.type->get_message_id();
    if (message_id.is_valid()) {
      message_ids.push_back(message_id);
    }
  }
  for (auto &notification : group_it->second.pending_notifications) {
    auto message_id = notification.type->get_message_id();
    if (message_id.is_valid()) {
      message_ids.push_back(message_id);
    }
  }

  return message_ids;
}

void NotificationManager::on_delayed_notification_update_count_changed(int32 diff, int32 notification_group_id,
                                                                       const char *source) {
  bool had_delayed = delayed_notification_update_count_ != 0;
  delayed_notification_update_count_ += diff;
  CHECK(delayed_notification_update_count_ >= 0);
  VLOG(notifications) << "Update delayed notification count with diff " << diff << " to "
                      << delayed_notification_update_count_ << " from group " << notification_group_id << " and "
                      << source;
  bool have_delayed = delayed_notification_update_count_ != 0;
  if (had_delayed != have_delayed) {
    send_update_have_pending_notifications();
  }
}

NotificationGroupId NotificationManager::get_next_notification_group_id() {
  if (is_disabled()) {
    return NotificationGroupId();
  }
  if (current_notification_group_id_.get() == std::numeric_limits<int32>::max()) {
    LOG(ERROR) << "Notification group identifier overflowed";
    return NotificationGroupId();
  }

  current_notification_group_id_ = NotificationGroupId(current_notification_group_id_.get() + 1);
  G()->td_db()->get_binlog_pmc()->set("notification_group_id_current", to_string(current_notification_group_id_.get()));
  return current_notification_group_id_;
}

void NotificationManager::on_disable_contact_registered_notifications_changed() {
  if (is_disabled()) {
    return;
  }

  auto is_disabled = G()->shared_config().get_option_boolean("disable_contact_registered_notifications");

  if (is_disabled == disable_contact_registered_notifications_) {
    return;
  }

  disable_contact_registered_notifications_ = is_disabled;
  if (contact_registered_notifications_sync_state_ == SyncState::Completed) {
    run_contact_registered_notifications_sync();
  }
}

void NotificationManager::before_get_chat_difference(NotificationGroupId group_id) {
  if (is_disabled()) {
    return;
  }

  VLOG(notifications) << "Before get chat difference in " << group_id;
  CHECK(group_id.is_valid());
  if (running_get_chat_difference_.insert(group_id.get()).second) {
    on_unreceived_notification_update_count_changed(1, group_id.get(), "before_get_chat_difference");
  }
}

NotificationId NotificationManager::get_last_notification_id(const NotificationGroup &group) {
  if (!group.pending_notifications.empty()) {
    return group.pending_notifications.back().notification_id;
  }
  if (!group.notifications.empty()) {
    return group.notifications.back().notification_id;
  }
  return NotificationId();
}

NotificationManager::NotificationGroups::iterator NotificationManager::get_group(NotificationGroupId group_id) {
  auto group_keys_it = group_keys_.find(group_id);
  if (group_keys_it != group_keys_.end()) {
    return groups_.find(group_keys_it->second);
  }
  return groups_.end();
}

void NotificationManager::start_up() {
  init();
}
}  // namespace td
