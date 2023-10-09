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


#include <unordered_map>
#include <unordered_set>

namespace td {
}  // namespace td
namespace td {

void NotificationManager::flush_pending_updates(int32 group_id, const char *source) {
  // no check for G()->close_flag() to flush pending notifications even while closing
  auto it = pending_updates_.find(group_id);
  if (it == pending_updates_.end()) {
    return;
  }

  auto updates = std::move(it->second);
  pending_updates_.erase(it);

  if (is_destroyed_) {
    return;
  }

  VLOG(notifications) << "Send " << updates.size() << " pending updates in " << NotificationGroupId(group_id)
                      << " from " << source;
  for (auto &update : updates) {
    VLOG(notifications) << "Have " << as_notification_update(update.get());
  }

  td::remove_if(updates, [](auto &update) { return update == nullptr; });

  // if a notification was added, then deleted and then re-added we need to keep
  // first addition, because it can be with sound,
  // deletion, because number of notification should never exceed max_notification_group_size_,
  // and second addition, because we has kept the deletion

  // calculate last state of all notifications
  std::unordered_set<int32> added_notification_ids;
  std::unordered_set<int32> edited_notification_ids;
  std::unordered_set<int32> removed_notification_ids;
  for (auto &update : updates) {
    CHECK(update != nullptr);
    if (update->get_id() == td_api::updateNotificationGroup::ID) {
      auto update_ptr = static_cast<td_api::updateNotificationGroup *>(update.get());
      for (auto &notification : update_ptr->added_notifications_) {
        auto notification_id = notification->id_;
        bool is_inserted = added_notification_ids.insert(notification_id).second;
        CHECK(is_inserted);                                          // there must be no additions after addition
        CHECK(edited_notification_ids.count(notification_id) == 0);  // there must be no additions after edit
        removed_notification_ids.erase(notification_id);
      }
      for (auto &notification_id : update_ptr->removed_notification_ids_) {
        added_notification_ids.erase(notification_id);
        edited_notification_ids.erase(notification_id);
        if (!removed_notification_ids.insert(notification_id).second) {
          // sometimes there can be deletion of notification without previous addition, because the notification
          // has already been deleted at the time of addition and get_notification_object_type was nullptr
          VLOG(notifications) << "Remove duplicated deletion of " << notification_id;
          notification_id = 0;
        }
      }
      td::remove_if(update_ptr->removed_notification_ids_, [](auto &notification_id) { return notification_id == 0; });
    } else {
      CHECK(update->get_id() == td_api::updateNotification::ID);
      auto update_ptr = static_cast<td_api::updateNotification *>(update.get());
      auto notification_id = update_ptr->notification_->id_;
      CHECK(removed_notification_ids.count(notification_id) == 0);  // there must be no edits of deleted notifications
      added_notification_ids.erase(notification_id);
      edited_notification_ids.insert(notification_id);
    }
  }

  // we need to keep only additions of notifications from added_notification_ids/edited_notification_ids and
  // all edits of notifications from edited_notification_ids
  // deletions of a notification can be removed, only if the addition of the notification has already been deleted
  // deletions of all unkept notifications can be moved to the first updateNotificationGroup
  // after that at every moment there are no more active notifications than in the last moment,
  // so left deletions after add/edit can be safely removed and following additions can be treated as edits
  // we still need to keep deletions coming first, because we can't have 2 consequent additions
  // from all additions of the same notification, we need to preserve the first, because it can be with sound,
  // all other additions and edits can be merged to the first addition/edit
  // i.e. in edit+delete+add chain we want to remove deletion and merge addition to the edit

  auto group_key = group_keys_[NotificationGroupId(group_id)];
  bool is_hidden = group_key.last_notification_date == 0 || get_last_updated_group_key() < group_key;
  bool is_changed = true;
  while (is_changed) {
    is_changed = false;

    size_t cur_pos = 0;
    std::unordered_map<int32, size_t> first_add_notification_pos;
    std::unordered_map<int32, size_t> first_edit_notification_pos;
    std::unordered_set<int32> can_be_deleted_notification_ids;
    std::vector<int32> moved_deleted_notification_ids;
    size_t first_notification_group_pos = 0;

    for (auto &update : updates) {
      cur_pos++;

      CHECK(update != nullptr);
      if (update->get_id() == td_api::updateNotificationGroup::ID) {
        auto update_ptr = static_cast<td_api::updateNotificationGroup *>(update.get());

        for (auto &notification : update_ptr->added_notifications_) {
          auto notification_id = notification->id_;
          bool is_needed =
              added_notification_ids.count(notification_id) != 0 || edited_notification_ids.count(notification_id) != 0;
          if (!is_needed) {
            VLOG(notifications) << "Remove unneeded addition of " << notification_id << " in update " << cur_pos;
            can_be_deleted_notification_ids.insert(notification_id);
            notification = nullptr;
            is_changed = true;
            continue;
          }

          auto edit_it = first_edit_notification_pos.find(notification_id);
          if (edit_it != first_edit_notification_pos.end()) {
            VLOG(notifications) << "Move addition of " << notification_id << " in update " << cur_pos
                                << " to edit in update " << edit_it->second;
            CHECK(edit_it->second < cur_pos);
            auto previous_update_ptr = static_cast<td_api::updateNotification *>(updates[edit_it->second - 1].get());
            CHECK(previous_update_ptr->notification_->id_ == notification_id);
            previous_update_ptr->notification_->type_ = std::move(notification->type_);
            is_changed = true;
            notification = nullptr;
            continue;
          }
          auto add_it = first_add_notification_pos.find(notification_id);
          if (add_it != first_add_notification_pos.end()) {
            VLOG(notifications) << "Move addition of " << notification_id << " in update " << cur_pos << " to update "
                                << add_it->second;
            CHECK(add_it->second < cur_pos);
            auto previous_update_ptr =
                static_cast<td_api::updateNotificationGroup *>(updates[add_it->second - 1].get());
            bool is_found = false;
            for (auto &prev_notification : previous_update_ptr->added_notifications_) {
              if (prev_notification->id_ == notification_id) {
                prev_notification->type_ = std::move(notification->type_);
                is_found = true;
                break;
              }
            }
            CHECK(is_found);
            is_changed = true;
            notification = nullptr;
            continue;
          }

          // it is a first addition/edit of needed notification
          first_add_notification_pos[notification_id] = cur_pos;
        }
        td::remove_if(update_ptr->added_notifications_, [](auto &notification) { return notification == nullptr; });
        if (update_ptr->added_notifications_.empty() && !update_ptr->is_silent_) {
          update_ptr->is_silent_ = true;
          is_changed = true;
        }

        for (auto &notification_id : update_ptr->removed_notification_ids_) {
          bool is_needed =
              added_notification_ids.count(notification_id) != 0 || edited_notification_ids.count(notification_id) != 0;
          if (can_be_deleted_notification_ids.count(notification_id) == 1) {
            CHECK(!is_needed);
            VLOG(notifications) << "Remove unneeded deletion of " << notification_id << " in update " << cur_pos;
            notification_id = 0;
            is_changed = true;
            continue;
          }
          if (!is_needed) {
            if (first_notification_group_pos != 0) {
              VLOG(notifications) << "Need to keep deletion of " << notification_id << " in update " << cur_pos
                                  << ", but can move it to the first updateNotificationGroup at pos "
                                  << first_notification_group_pos;
              moved_deleted_notification_ids.push_back(notification_id);
              notification_id = 0;
              is_changed = true;
            }
            continue;
          }

          if (first_add_notification_pos.count(notification_id) != 0 ||
              first_edit_notification_pos.count(notification_id) != 0) {
            // the notification will be re-added, and we will be able to merge the addition with previous update, so we can just remove the deletion
            VLOG(notifications) << "Remove unneeded deletion in update " << cur_pos;
            notification_id = 0;
            is_changed = true;
            continue;
          }

          // we need to keep the deletion, because otherwise we will have 2 consequent additions
        }
        td::remove_if(update_ptr->removed_notification_ids_,
                      [](auto &notification_id) { return notification_id == 0; });

        if (update_ptr->removed_notification_ids_.empty() && update_ptr->added_notifications_.empty()) {
          for (size_t i = cur_pos - 1; i > 0; i--) {
            if (updates[i - 1] != nullptr && updates[i - 1]->get_id() == td_api::updateNotificationGroup::ID) {
              VLOG(notifications) << "Move total_count from empty update " << cur_pos << " to update " << i;
              auto previous_update_ptr = static_cast<td_api::updateNotificationGroup *>(updates[i - 1].get());
              previous_update_ptr->type_ = std::move(update_ptr->type_);
              previous_update_ptr->total_count_ = update_ptr->total_count_;
              is_changed = true;
              update = nullptr;
              break;
            }
          }
          if (update != nullptr && cur_pos == 1) {
            bool is_empty_group =
                added_notification_ids.empty() && edited_notification_ids.empty() && update_ptr->total_count_ == 0;
            if (updates.size() > 1 || (is_hidden && !is_empty_group)) {
              VLOG(notifications) << "Remove empty update " << cur_pos;
              CHECK(moved_deleted_notification_ids.empty());
              is_changed = true;
              update = nullptr;
            }
          }
        }

        if (first_notification_group_pos == 0 && update != nullptr) {
          first_notification_group_pos = cur_pos;
        }
      } else {
        CHECK(update->get_id() == td_api::updateNotification::ID);
        auto update_ptr = static_cast<td_api::updateNotification *>(update.get());
        auto notification_id = update_ptr->notification_->id_;
        bool is_needed =
            added_notification_ids.count(notification_id) != 0 || edited_notification_ids.count(notification_id) != 0;
        if (!is_needed) {
          VLOG(notifications) << "Remove unneeded update " << cur_pos;
          is_changed = true;
          update = nullptr;
          continue;
        }
        auto edit_it = first_edit_notification_pos.find(notification_id);
        if (edit_it != first_edit_notification_pos.end()) {
          VLOG(notifications) << "Move edit of " << notification_id << " in update " << cur_pos << " to update "
                              << edit_it->second;
          CHECK(edit_it->second < cur_pos);
          auto previous_update_ptr = static_cast<td_api::updateNotification *>(updates[edit_it->second - 1].get());
          CHECK(previous_update_ptr->notification_->id_ == notification_id);
          previous_update_ptr->notification_->type_ = std::move(update_ptr->notification_->type_);
          is_changed = true;
          update = nullptr;
          continue;
        }
        auto add_it = first_add_notification_pos.find(notification_id);
        if (add_it != first_add_notification_pos.end()) {
          VLOG(notifications) << "Move edit of " << notification_id << " in update " << cur_pos << " to update "
                              << add_it->second;
          CHECK(add_it->second < cur_pos);
          auto previous_update_ptr = static_cast<td_api::updateNotificationGroup *>(updates[add_it->second - 1].get());
          bool is_found = false;
          for (auto &notification : previous_update_ptr->added_notifications_) {
            if (notification->id_ == notification_id) {
              notification->type_ = std::move(update_ptr->notification_->type_);
              is_found = true;
              break;
            }
          }
          CHECK(is_found);
          is_changed = true;
          update = nullptr;
          continue;
        }

        // it is a first addition/edit of needed notification
        first_edit_notification_pos[notification_id] = cur_pos;
      }
    }
    if (!moved_deleted_notification_ids.empty()) {
      CHECK(first_notification_group_pos != 0);
      auto &update = updates[first_notification_group_pos - 1];
      CHECK(update->get_id() == td_api::updateNotificationGroup::ID);
      auto update_ptr = static_cast<td_api::updateNotificationGroup *>(update.get());
      append(update_ptr->removed_notification_ids_, std::move(moved_deleted_notification_ids));
      auto old_size = update_ptr->removed_notification_ids_.size();
      td::unique(update_ptr->removed_notification_ids_);
      CHECK(old_size == update_ptr->removed_notification_ids_.size());
    }

    td::remove_if(updates, [](auto &update) { return update == nullptr; });
    if (updates.empty()) {
      VLOG(notifications) << "There are no updates to send in " << NotificationGroupId(group_id);
      break;
    }

    auto has_common_notifications = [](const vector<td_api::object_ptr<td_api::notification>> &notifications,
                                       const vector<int32> &notification_ids) {
      for (auto &notification : notifications) {
        if (td::contains(notification_ids, notification->id_)) {
          return true;
        }
      }
      return false;
    };

    size_t last_update_pos = 0;
    for (size_t i = 1; i < updates.size(); i++) {
      if (updates[last_update_pos]->get_id() == td_api::updateNotificationGroup::ID &&
          updates[i]->get_id() == td_api::updateNotificationGroup::ID) {
        auto last_update_ptr = static_cast<td_api::updateNotificationGroup *>(updates[last_update_pos].get());
        auto update_ptr = static_cast<td_api::updateNotificationGroup *>(updates[i].get());
        if ((last_update_ptr->notification_settings_chat_id_ == update_ptr->notification_settings_chat_id_ ||
             last_update_ptr->added_notifications_.empty()) &&
            !has_common_notifications(last_update_ptr->added_notifications_, update_ptr->removed_notification_ids_) &&
            !has_common_notifications(update_ptr->added_notifications_, last_update_ptr->removed_notification_ids_)) {
          // combine updates
          VLOG(notifications) << "Combine " << as_notification_update(last_update_ptr) << " and "
                              << as_notification_update(update_ptr);
          CHECK(last_update_ptr->notification_group_id_ == update_ptr->notification_group_id_);
          CHECK(last_update_ptr->chat_id_ == update_ptr->chat_id_);
          if (last_update_ptr->is_silent_ && !update_ptr->is_silent_) {
            last_update_ptr->is_silent_ = false;
          }
          last_update_ptr->notification_settings_chat_id_ = update_ptr->notification_settings_chat_id_;
          last_update_ptr->type_ = std::move(update_ptr->type_);
          last_update_ptr->total_count_ = update_ptr->total_count_;
          append(last_update_ptr->added_notifications_, std::move(update_ptr->added_notifications_));
          append(last_update_ptr->removed_notification_ids_, std::move(update_ptr->removed_notification_ids_));
          updates[i] = nullptr;
          is_changed = true;
          continue;
        }
      }
      last_update_pos++;
      if (last_update_pos != i) {
        updates[last_update_pos] = std::move(updates[i]);
      }
    }
    updates.resize(last_update_pos + 1);
  }

  for (auto &update : updates) {
    CHECK(update != nullptr);
    if (update->get_id() == td_api::updateNotificationGroup::ID) {
      auto update_ptr = static_cast<td_api::updateNotificationGroup *>(update.get());
      std::sort(update_ptr->added_notifications_.begin(), update_ptr->added_notifications_.end(),
                [](const auto &lhs, const auto &rhs) { return lhs->id_ < rhs->id_; });
      std::sort(update_ptr->removed_notification_ids_.begin(), update_ptr->removed_notification_ids_.end());
    }
    VLOG(notifications) << "Send " << as_notification_update(update.get());
    send_closure(G()->td(), &Td::send_update, std::move(update));
  }
  on_delayed_notification_update_count_changed(-1, group_id, "flush_pending_updates");

  auto group_it = get_group_force(NotificationGroupId(group_id));
  CHECK(group_it != groups_.end());
  NotificationGroup &group = group_it->second;
  for (auto &notification : group.notifications) {
    on_notification_processed(notification.notification_id);
  }
}
}  // namespace td
