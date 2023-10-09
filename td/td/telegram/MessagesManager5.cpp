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

#include "td/telegram/TdParameters.h"

#include "td/telegram/UpdatesManager.h"
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








#include <utility>

namespace td {
}  // namespace td
namespace td {

void MessagesManager::fix_new_dialog(Dialog *d, unique_ptr<Message> &&last_database_message,
                                     MessageId last_database_message_id, int64 order, int32 last_clear_history_date,
                                     MessageId last_clear_history_message_id,
                                     DialogId default_join_group_call_as_dialog_id,
                                     DialogId default_send_message_as_dialog_id,
                                     bool need_drop_default_send_message_as_dialog_id, bool is_loaded_from_database) {
  CHECK(d != nullptr);
  auto dialog_id = d->dialog_id;
  auto dialog_type = dialog_id.get_type();

  if (!td_->auth_manager_->is_bot() && dialog_type == DialogType::SecretChat) {
    auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
    if (user_id.is_valid()) {
      force_create_dialog(DialogId(user_id), "add chat with user to load/store action_bar and is_blocked");

      Dialog *user_d = get_dialog_force(DialogId(user_id), "fix_new_dialog");
      if (user_d != nullptr && d->is_blocked != user_d->is_blocked) {
        set_dialog_is_blocked(d, user_d->is_blocked);
      }
    }
  }

  if (d->is_empty && !d->have_full_history) {
    LOG(ERROR) << "Drop invalid flag is_empty";
    d->is_empty = false;
  }

  if (being_added_dialog_id_ != dialog_id && !td_->auth_manager_->is_bot() && !is_dialog_inited(d) &&
      dialog_type != DialogType::SecretChat && have_input_peer(dialog_id, AccessRights::Read)) {
    // asynchronously get dialog from the server
    send_get_dialog_query(dialog_id, Auto(), 0, "fix_new_dialog 20");
  }

  if (being_added_dialog_id_ != dialog_id && !d->is_is_blocked_inited && !td_->auth_manager_->is_bot()) {
    // asynchronously get is_blocked from the server
    get_dialog_info_full(dialog_id, Auto(), "fix_new_dialog init is_blocked");
  } else if (being_added_dialog_id_ != dialog_id && !d->is_has_bots_inited && !td_->auth_manager_->is_bot()) {
    // asynchronously get has_bots from the server
    get_dialog_info_full(dialog_id, Auto(), "fix_new_dialog init has_bots");
  } else if (being_added_dialog_id_ != dialog_id && !d->is_theme_name_inited && !td_->auth_manager_->is_bot()) {
    // asynchronously get dialog theme identifier from the server
    get_dialog_info_full(dialog_id, Auto(), "fix_new_dialog init theme_name");
  } else if (being_added_dialog_id_ != dialog_id && !d->is_last_pinned_message_id_inited &&
             !td_->auth_manager_->is_bot()) {
    // asynchronously get dialog pinned message from the server
    get_dialog_pinned_message(dialog_id, Auto());
  } else if (being_added_dialog_id_ != dialog_id && !d->is_folder_id_inited && !td_->auth_manager_->is_bot() &&
             order != DEFAULT_ORDER) {
    // asynchronously get dialog folder identifier from the server
    get_dialog_info_full(dialog_id, Auto(), "fix_new_dialog init folder_id");
  } else if (!d->is_message_ttl_inited && !td_->auth_manager_->is_bot() &&
             have_input_peer(dialog_id, AccessRights::Write)) {
    // asynchronously get dialog message TTL from the server
    get_dialog_info_full(dialog_id, Auto(), "fix_new_dialog init message_ttl");
  }
  if ((!d->know_action_bar || d->need_repair_action_bar) && !td_->auth_manager_->is_bot() &&
      dialog_type != DialogType::SecretChat && dialog_id != get_my_dialog_id() &&
      have_input_peer(dialog_id, AccessRights::Read)) {
    // asynchronously get action bar from the server
    reget_dialog_action_bar(dialog_id, "fix_new_dialog", false);
  }
  if (d->has_active_group_call && !d->active_group_call_id.is_valid() && !td_->auth_manager_->is_bot()) {
    repair_dialog_active_group_call_id(dialog_id);
  }

  if (d->notification_settings.is_synchronized && !d->notification_settings.is_use_default_fixed &&
      have_input_peer(dialog_id, AccessRights::Read) && !td_->auth_manager_->is_bot()) {
    LOG(INFO) << "Reget notification settings of " << dialog_id;
    if (dialog_type == DialogType::SecretChat) {
      if (d->notification_settings.mute_until == 0 && users_notification_settings_.mute_until == 0) {
        d->notification_settings.use_default_mute_until = true;
      }
      if (d->notification_settings.sound == "default" && users_notification_settings_.sound == "default") {
        d->notification_settings.use_default_sound = true;
      }
      if (d->notification_settings.show_preview && users_notification_settings_.show_preview) {
        d->notification_settings.use_default_show_preview = true;
      }
      d->notification_settings.is_use_default_fixed = true;
      on_dialog_updated(dialog_id, "reget notification settings");
    } else {
      send_get_dialog_notification_settings_query(dialog_id, Promise<>());
    }
  }
  if (td_->auth_manager_->is_bot() || d->notification_settings.use_default_mute_until ||
      d->notification_settings.mute_until <= G()->unix_time()) {
    d->notification_settings.mute_until = 0;
  } else {
    schedule_dialog_unmute(dialog_id, false, d->notification_settings.mute_until);
  }
  if (d->pinned_message_notification_message_id.is_valid()) {
    auto pinned_message_id = d->pinned_message_notification_message_id;
    if (!d->mention_notification_group.group_id.is_valid()) {
      LOG(ERROR) << "Have pinned message notification in " << pinned_message_id << " in " << dialog_id
                 << ", but there is no mention notification group";
      d->pinned_message_notification_message_id = MessageId();
      on_dialog_updated(dialog_id, "fix pinned message notification");
    } else if (is_dialog_pinned_message_notifications_disabled(d) ||
               pinned_message_id <= d->last_read_inbox_message_id ||
               pinned_message_id <= d->mention_notification_group.max_removed_message_id) {
      VLOG(notifications) << "Remove disabled pinned message notification in " << pinned_message_id << " in "
                          << dialog_id;
      send_closure_later(G()->notification_manager(), &NotificationManager::remove_temporary_notification_by_message_id,
                         d->mention_notification_group.group_id, pinned_message_id, true,
                         "fix pinned message notification");
      d->pinned_message_notification_message_id = MessageId();
      on_dialog_updated(dialog_id, "fix pinned message notification 2");
    }
  }
  if (d->new_secret_chat_notification_id.is_valid()) {
    auto &group_info = d->message_notification_group;
    if (d->new_secret_chat_notification_id.get() <= group_info.max_removed_notification_id.get() ||
        (group_info.last_notification_date == 0 && group_info.max_removed_notification_id.get() == 0)) {
      VLOG(notifications) << "Fix removing new secret chat " << d->new_secret_chat_notification_id << " in "
                          << dialog_id;
      d->new_secret_chat_notification_id = NotificationId();
      on_dialog_updated(dialog_id, "fix new secret chat notification identifier");
    }
  }

  {
    auto it = pending_add_dialog_last_database_message_dependent_dialogs_.find(dialog_id);
    if (it != pending_add_dialog_last_database_message_dependent_dialogs_.end()) {
      auto pending_dialog_ids = std::move(it->second);
      pending_add_dialog_last_database_message_dependent_dialogs_.erase(it);

      for (auto &pending_dialog_id : pending_dialog_ids) {
        auto &counter_message = pending_add_dialog_last_database_message_[pending_dialog_id];
        CHECK(counter_message.first > 0);
        counter_message.first--;
        if (counter_message.first == 0) {
          add_dialog_last_database_message(get_dialog(pending_dialog_id), std::move(counter_message.second));
          pending_add_dialog_last_database_message_.erase(pending_dialog_id);
        }
      }
    }
  }

  {
    auto it = pending_add_default_join_group_call_as_dialog_id_.find(dialog_id);
    if (it != pending_add_default_join_group_call_as_dialog_id_.end()) {
      auto pending_dialog_ids = std::move(it->second);
      pending_add_default_join_group_call_as_dialog_id_.erase(it);

      for (auto &pending_dialog_id : pending_dialog_ids) {
        on_update_dialog_default_join_group_call_as_dialog_id(pending_dialog_id, dialog_id, false);
      }
    }
  }

  {
    auto it = pending_add_default_send_message_as_dialog_id_.find(dialog_id);
    if (it != pending_add_default_send_message_as_dialog_id_.end()) {
      auto pending_dialog_ids = std::move(it->second);
      pending_add_default_send_message_as_dialog_id_.erase(it);

      for (auto &pending_dialog_id : pending_dialog_ids) {
        Dialog *pending_d = get_dialog(pending_dialog_id.first);
        CHECK(pending_d != nullptr);
        if (!pending_d->default_send_message_as_dialog_id.is_valid()) {
          LOG(INFO) << "Set postponed message sender in " << pending_dialog_id << " to " << dialog_id;
          pending_d->need_drop_default_send_message_as_dialog_id = pending_dialog_id.second;
          pending_d->default_send_message_as_dialog_id = dialog_id;
          send_update_chat_message_sender(pending_d);
        }
      }
    }
  }

  if (dialog_id != being_added_dialog_id_) {
    set_dialog_last_clear_history_date(d, last_clear_history_date, last_clear_history_message_id, "fix_new_dialog 8",
                                       is_loaded_from_database);

    set_dialog_order(d, order, false, is_loaded_from_database, "fix_new_dialog 9");
  }

  if (dialog_type != DialogType::SecretChat && d->last_new_message_id.is_valid() &&
      !d->last_new_message_id.is_server()) {
    // fix wrong last_new_message_id
    d->last_new_message_id = d->last_new_message_id.get_prev_server_message_id();
    on_dialog_updated(dialog_id, "fix_new_dialog 10");
  }

  bool need_get_history = true;

  // add last database message to dialog
  MessageId last_message_id;
  if (last_database_message != nullptr) {
    need_get_history = false;
    last_message_id = last_database_message->message_id;
  } else if (last_database_message_id.is_valid()) {
    last_message_id = last_database_message_id;
  }

  if (!d->last_new_message_id.is_valid() && d->last_new_message_id != MessageId()) {
    LOG(ERROR) << "Drop invalid last_new_message_id " << d->last_new_message_id << " in " << dialog_id;
    d->last_new_message_id = MessageId();
  }
  if (last_message_id.is_valid()) {
    if ((last_message_id.is_server() || dialog_type == DialogType::SecretChat) && !d->last_new_message_id.is_valid()) {
      LOG(ERROR) << "Bugfixing wrong last_new_message_id to " << last_message_id << " in " << dialog_id;
      // must be called before set_dialog_first_database_message_id and set_dialog_last_database_message_id
      set_dialog_last_new_message_id(d, last_message_id, "fix_new_dialog 1");
    }
    if (!d->first_database_message_id.is_valid() || d->first_database_message_id > last_message_id) {
      LOG(ERROR) << "Bugfixing wrong first_database_message_id from " << d->first_database_message_id << " to "
                 << last_message_id << " in " << dialog_id;
      set_dialog_first_database_message_id(d, last_message_id, "fix_new_dialog 2");
    }
    set_dialog_last_database_message_id(d, last_message_id, "fix_new_dialog 3", is_loaded_from_database);
  } else if (d->first_database_message_id.is_valid()) {
    // ensure that first_database_message_id <= last_database_message_id
    if (d->first_database_message_id <= d->last_new_message_id) {
      set_dialog_last_database_message_id(d, d->last_new_message_id, "fix_new_dialog 4");
    } else {
      // can't fix last_database_message_id, drop first_database_message_id; it shouldn't happen anyway
      set_dialog_first_database_message_id(d, MessageId(), "fix_new_dialog 5");
    }
  }
  d->debug_first_database_message_id = d->first_database_message_id;
  d->debug_last_database_message_id = d->last_database_message_id;
  d->debug_last_new_message_id = d->last_new_message_id;

  if (last_database_message != nullptr) {
    Dependencies dependencies;
    add_message_dependencies(dependencies, last_database_message.get());

    int32 dependent_dialog_count = 0;
    for (auto &other_dialog_id : dependencies.dialog_ids) {
      if (other_dialog_id.is_valid() && !have_dialog(other_dialog_id)) {
        LOG(INFO) << "Postpone adding of last message in " << dialog_id << " because of cyclic dependency with "
                  << other_dialog_id;
        pending_add_dialog_last_database_message_dependent_dialogs_[other_dialog_id].push_back(dialog_id);
        dependent_dialog_count++;
      }
    };

    if (dependent_dialog_count == 0) {
      add_dialog_last_database_message(d, std::move(last_database_message));
    } else {
      // can't add message immediately, because need to notify first about adding of dependent dialogs
      d->pending_last_message_date = last_database_message->date;
      d->pending_last_message_id = last_database_message->message_id;
      pending_add_dialog_last_database_message_[dialog_id] = {dependent_dialog_count, std::move(last_database_message)};
    }
  } else if (last_database_message_id.is_valid()) {
    auto date = DialogDate(order, dialog_id).get_date();
    if (date < MIN_PINNED_DIALOG_DATE) {
      d->pending_last_message_date = date;
      d->pending_last_message_id = last_database_message_id;
    }
  }

  if (default_join_group_call_as_dialog_id != d->default_join_group_call_as_dialog_id) {
    CHECK(default_join_group_call_as_dialog_id.is_valid());
    CHECK(default_join_group_call_as_dialog_id.get_type() != DialogType::User);
    CHECK(!d->default_join_group_call_as_dialog_id.is_valid());
    if (!have_dialog(default_join_group_call_as_dialog_id)) {
      LOG(INFO) << "Postpone adding of default join voice chat as " << default_join_group_call_as_dialog_id << " in "
                << dialog_id;
      pending_add_default_join_group_call_as_dialog_id_[default_join_group_call_as_dialog_id].push_back(dialog_id);
    } else {
      on_update_dialog_default_join_group_call_as_dialog_id(dialog_id, default_join_group_call_as_dialog_id, false);
    }
  }

  if (default_send_message_as_dialog_id != d->default_send_message_as_dialog_id) {
    CHECK(default_send_message_as_dialog_id.is_valid());
    CHECK(default_send_message_as_dialog_id.get_type() != DialogType::User);
    CHECK(!d->default_send_message_as_dialog_id.is_valid());
    if (!have_dialog(default_send_message_as_dialog_id)) {
      LOG(INFO) << "Postpone setting of message sender " << default_send_message_as_dialog_id << " in " << dialog_id;
      pending_add_default_send_message_as_dialog_id_[default_send_message_as_dialog_id].emplace_back(
          dialog_id, need_drop_default_send_message_as_dialog_id);
    } else {
      LOG(INFO) << "Set message sender in " << dialog_id << " to " << default_send_message_as_dialog_id;
      d->need_drop_default_send_message_as_dialog_id = need_drop_default_send_message_as_dialog_id;
      d->default_send_message_as_dialog_id = default_send_message_as_dialog_id;
      send_update_chat_message_sender(d);
    }
  }

  switch (dialog_type) {
    case DialogType::User:
      break;
    case DialogType::Chat:
      if (d->last_read_inbox_message_id < d->last_read_outbox_message_id) {
        LOG(INFO) << "Last read outbox message is " << d->last_read_outbox_message_id << " in " << dialog_id
                  << ", but last read inbox message is " << d->last_read_inbox_message_id;
        // can't fix last_read_inbox_message_id by last_read_outbox_message_id because last_read_outbox_message_id is
        // just a message identifier not less than an identifier of last read outgoing message and less than
        // an identifier of first unread outgoing message, so it may not point to the outgoing message
        // read_history_inbox(dialog_id, d->last_read_outbox_message_id, d->server_unread_count, "fix_new_dialog 6");
      }
      break;
    case DialogType::Channel:
      break;
    case DialogType::SecretChat:
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  if (d->delete_last_message_date != 0) {
    if (d->last_message_id.is_valid()) {
      LOG(ERROR) << "Last " << d->deleted_last_message_id << " in " << dialog_id << " was deleted at "
                 << d->delete_last_message_date << ", but have last " << d->last_message_id;
      d->delete_last_message_date = 0;
      d->deleted_last_message_id = MessageId();
      d->is_last_message_deleted_locally = false;
      on_dialog_updated(dialog_id, "update_delete_last_message_date");
    } else {
      need_get_history = true;
    }
  }

  if (!G()->parameters().use_message_db) {
    d->has_loaded_scheduled_messages_from_database = true;
  }

  if (dialog_id != being_added_dialog_id_) {
    update_dialog_pos(d, "fix_new_dialog 7", true, is_loaded_from_database);
  }
  if (is_loaded_from_database && d->order != order && order < MAX_ORDINARY_DIALOG_ORDER &&
      !td_->contacts_manager_->is_dialog_info_received_from_server(dialog_id) && !d->had_last_yet_unsent_message) {
    LOG(ERROR) << dialog_id << " has order " << d->order << " instead of saved to database order " << order;
  }

  LOG(INFO) << "Loaded " << dialog_id << " with last new " << d->last_new_message_id << ", first database "
            << d->first_database_message_id << ", last database " << d->last_database_message_id << ", last "
            << d->last_message_id << " with order " << d->order;
  VLOG(notifications) << "Have " << dialog_id << " with message " << d->message_notification_group.group_id
                      << " with last " << d->message_notification_group.last_notification_id << " sent at "
                      << d->message_notification_group.last_notification_date << ", max removed "
                      << d->message_notification_group.max_removed_notification_id << "/"
                      << d->message_notification_group.max_removed_message_id << " and new secret chat "
                      << d->new_secret_chat_notification_id;
  VLOG(notifications) << "Have " << dialog_id << " with mention " << d->mention_notification_group.group_id
                      << " with last " << d->mention_notification_group.last_notification_id << " sent at "
                      << d->mention_notification_group.last_notification_date << ", max removed "
                      << d->mention_notification_group.max_removed_notification_id << "/"
                      << d->mention_notification_group.max_removed_message_id << " and pinned "
                      << d->pinned_message_notification_message_id;
  VLOG(notifications) << "In " << dialog_id << " have last_read_inbox_message_id = " << d->last_read_inbox_message_id
                      << ", last_new_message_id = " << d->last_new_message_id
                      << ", max_notification_message_id = " << d->max_notification_message_id;

  if (d->messages != nullptr) {
    CHECK(d->messages->message_id == last_message_id);
    CHECK(d->messages->left == nullptr);
    CHECK(d->messages->right == nullptr);
  }

  // must be after update_dialog_pos, because uses d->order
  // must be after checks that dialog has at most one message, because read_history_inbox can load
  // pinned message to remove its notification
  if (d->pending_read_channel_inbox_pts != 0 && !td_->auth_manager_->is_bot() &&
      have_input_peer(dialog_id, AccessRights::Read) && need_unread_counter(d->order)) {
    if (d->pts == d->pending_read_channel_inbox_pts) {
      d->pending_read_channel_inbox_pts = 0;
      read_history_inbox(dialog_id, d->pending_read_channel_inbox_max_message_id,
                         d->pending_read_channel_inbox_server_unread_count, "fix_new_dialog 12");
      on_dialog_updated(dialog_id, "fix_new_dialog 13");
    } else if (d->pts > d->pending_read_channel_inbox_pts) {
      d->need_repair_channel_server_unread_count = true;
      d->pending_read_channel_inbox_pts = 0;
      on_dialog_updated(dialog_id, "fix_new_dialog 14");
    } else {
      channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
    }
  } else {
    d->pending_read_channel_inbox_pts = 0;
  }
  if (need_get_history && !td_->auth_manager_->is_bot() && dialog_id != being_added_dialog_id_ &&
      dialog_id != being_added_by_new_message_dialog_id_ && have_input_peer(dialog_id, AccessRights::Read) &&
      (d->order != DEFAULT_ORDER || is_dialog_sponsored(d))) {
    get_history_from_the_end_impl(d, true, false, Auto());
  }
  if (d->need_repair_server_unread_count && need_unread_counter(d->order)) {
    CHECK(dialog_type != DialogType::SecretChat);
    repair_server_unread_count(dialog_id, d->server_unread_count);
  }
  if (d->need_repair_channel_server_unread_count) {
    repair_channel_server_unread_count(d);
  }
}

void MessagesManager::set_dialog_is_blocked(Dialog *d, bool is_blocked) {
  CHECK(d != nullptr);
  CHECK(d->is_blocked != is_blocked);
  d->is_blocked = is_blocked;
  d->is_is_blocked_inited = true;
  on_dialog_updated(d->dialog_id, "set_dialog_is_blocked");

  LOG(INFO) << "Set " << d->dialog_id << " is_blocked to " << is_blocked;
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in set_dialog_is_blocked";
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatIsBlocked>(d->dialog_id.get(), is_blocked));

  if (d->dialog_id.get_type() == DialogType::User) {
    td_->contacts_manager_->on_update_user_is_blocked(d->dialog_id.get_user_id(), is_blocked);

    if (d->know_action_bar) {
      if (is_blocked) {
        if (d->action_bar != nullptr) {
          d->action_bar = nullptr;
          send_update_chat_action_bar(d);
        }
      } else {
        repair_dialog_action_bar(d, "on_dialog_user_is_blocked_updated");
      }
    }

    td_->contacts_manager_->for_each_secret_chat_with_user(
        d->dialog_id.get_user_id(), [this, is_blocked](SecretChatId secret_chat_id) {
          DialogId dialog_id(secret_chat_id);
          auto d = get_dialog(dialog_id);  // must not create the dialog
          if (d != nullptr && d->is_update_new_chat_sent && d->is_blocked != is_blocked) {
            set_dialog_is_blocked(d, is_blocked);
          }
        });
  }
}

Status MessagesManager::can_pin_messages(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto status = td_->contacts_manager_->get_chat_permissions(chat_id);
      if (!status.can_pin_messages() ||
          (td_->auth_manager_->is_bot() && !td_->contacts_manager_->is_appointed_chat_administrator(chat_id))) {
        return Status::Error(400, "Not enough rights to manage pinned messages in the chat");
      }
      break;
    }
    case DialogType::Channel: {
      auto status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      bool can_pin = is_broadcast_channel(dialog_id) ? status.can_edit_messages() : status.can_pin_messages();
      if (!can_pin) {
        return Status::Error(400, "Not enough rights to manage pinned messages in the chat");
      }
      break;
    }
    case DialogType::SecretChat:
      return Status::Error(400, "Secret chats can't have pinned messages");
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  if (!have_input_peer(dialog_id, AccessRights::Write)) {
    return Status::Error(400, "Not enough rights");
  }

  return Status::OK();
}

Status MessagesManager::can_send_message(DialogId dialog_id) const {
  if (!have_input_peer(dialog_id, AccessRights::Write)) {
    return Status::Error(400, "Have no write access to the chat");
  }

  if (dialog_id.get_type() == DialogType::Channel) {
    auto channel_id = dialog_id.get_channel_id();
    auto channel_type = td_->contacts_manager_->get_channel_type(channel_id);
    auto channel_status = td_->contacts_manager_->get_channel_permissions(channel_id);

    switch (channel_type) {
      case ContactsManager::ChannelType::Unknown:
      case ContactsManager::ChannelType::Megagroup:
        if (!channel_status.can_send_messages()) {
          return Status::Error(400, "Have no rights to send a message");
        }
        break;
      case ContactsManager::ChannelType::Broadcast: {
        if (!channel_status.can_post_messages()) {
          return Status::Error(400, "Need administrator rights in the channel chat");
        }
        break;
      }
      default:
        UNREACHABLE();
    }
  }
  return Status::OK();
}

bool MessagesManager::set_dialog_last_notification(DialogId dialog_id, NotificationGroupInfo &group_info,
                                                   int32 last_notification_date, NotificationId last_notification_id,
                                                   const char *source) {
  if (group_info.last_notification_date != last_notification_date ||
      group_info.last_notification_id != last_notification_id) {
    VLOG(notifications) << "Set " << group_info.group_id << '/' << dialog_id << " last notification to "
                        << last_notification_id << " sent at " << last_notification_date << " from " << source;
    group_info.last_notification_date = last_notification_date;
    group_info.last_notification_id = last_notification_id;
    group_info.is_changed = true;
    on_dialog_updated(dialog_id, "set_dialog_last_notification");
    return true;
  }
  return false;
}

RestrictedRights MessagesManager::get_dialog_default_permissions(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->get_user_default_permissions(dialog_id.get_user_id());
    case DialogType::Chat:
      return td_->contacts_manager_->get_chat_default_permissions(dialog_id.get_chat_id());
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_default_permissions(dialog_id.get_channel_id());
    case DialogType::SecretChat:
      return td_->contacts_manager_->get_secret_chat_default_permissions(dialog_id.get_secret_chat_id());
    case DialogType::None:
    default:
      UNREACHABLE();
      return RestrictedRights(false, false, false, false, false, false, false, false, false, false, false);
  }
}

Result<MessageCopyOptions> MessagesManager::process_message_copy_options(
    DialogId dialog_id, tl_object_ptr<td_api::messageCopyOptions> &&options) const {
  if (options == nullptr || !options->send_copy_) {
    return MessageCopyOptions();
  }
  MessageCopyOptions result;
  result.send_copy = true;
  result.replace_caption = options->replace_caption_;
  if (result.replace_caption) {
    TRY_RESULT_ASSIGN(result.new_caption,
                      process_input_caption(td_->contacts_manager_.get(), dialog_id, std::move(options->new_caption_),
                                            td_->auth_manager_->is_bot()));
  }
  return std::move(result);
}

MessageId MessagesManager::get_persistent_message_id(const Dialog *d, MessageId message_id) {
  if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
    return MessageId();
  }
  if (message_id.is_yet_unsent()) {
    // it is possible that user tries to do something with an already sent message by its temporary id
    // we need to use real message in this case and transparently replace message_id
    auto it = d->yet_unsent_message_id_to_persistent_message_id.find(message_id);
    if (it != d->yet_unsent_message_id_to_persistent_message_id.end()) {
      return it->second;
    }
  }

  return message_id;
}

void MessagesManager::on_create_new_dialog_fail(int64 random_id, Status error, Promise<Unit> &&promise) {
  LOG(INFO) << "Clean up creation of group or channel chat";
  auto it = created_dialogs_.find(random_id);
  CHECK(it != created_dialogs_.end());
  CHECK(it->second == DialogId());
  created_dialogs_.erase(it);

  CHECK(error.is_error());
  promise.set_error(std::move(error));

  // repairing state by running get difference
  td_->updates_manager_->get_difference("on_create_new_dialog_fail");
}

Status MessagesManager::can_get_message_viewers(FullMessageId full_message_id) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "get_message_viewers");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  auto m = get_message_force(d, full_message_id.get_message_id(), "get_message_viewers");
  if (m == nullptr) {
    return Status::Error(400, "Message not found");
  }

  return can_get_message_viewers(dialog_id, m);
}

bool MessagesManager::need_skip_bot_commands(DialogId dialog_id, const Message *m) const {
  if (td_->auth_manager_->is_bot()) {
    return false;
  }

  if (m != nullptr && m->message_id.is_scheduled()) {
    return true;
  }

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  return (d->is_has_bots_inited && !d->has_bots) || is_broadcast_channel(dialog_id);
}

void MessagesManager::suffix_load_till_date(Dialog *d, int32 date, Promise<> promise) {
  LOG(INFO) << "Load suffix of " << d->dialog_id << " till date " << date;
  auto condition = [date](const Message *m) {
    return m != nullptr && m->date < date;
  };
  suffix_load_add_query(d, std::make_pair(std::move(promise), std::move(condition)));
}

Status MessagesManager::open_dialog(DialogId dialog_id) {
  Dialog *d = get_dialog_force(dialog_id, "open_dialog");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  open_dialog(d);
  return Status::OK();
}

const MessagesManager::Dialog *MessagesManager::get_dialog(DialogId dialog_id) const {
  auto it = dialogs_.find(dialog_id);
  return it == dialogs_.end() ? nullptr : it->second.get();
}
}  // namespace td
