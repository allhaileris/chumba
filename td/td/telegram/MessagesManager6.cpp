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

#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"

#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
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

#include <algorithm>




#include <unordered_map>
#include <unordered_set>


namespace td {
}  // namespace td
namespace td {

void MessagesManager::on_get_dialogs(FolderId folder_id, vector<tl_object_ptr<telegram_api::Dialog>> &&dialog_folders,
                                     int32 total_count, vector<tl_object_ptr<telegram_api::Message>> &&messages,
                                     Promise<Unit> &&promise) {
  if (td_->updates_manager_->running_get_difference()) {
    LOG(INFO) << "Postpone result of getDialogs";
    pending_on_get_dialogs_.push_back(PendingOnGetDialogs{folder_id, std::move(dialog_folders), total_count,
                                                          std::move(messages), std::move(promise)});
    return;
  }
  bool from_dialog_list = total_count >= 0;
  bool from_get_dialog = total_count == -1;
  bool from_pinned_dialog_list = total_count == -2;

  if (from_get_dialog && dialog_folders.size() == 1 && dialog_folders[0]->get_id() == telegram_api::dialog::ID) {
    DialogId dialog_id(static_cast<const telegram_api::dialog *>(dialog_folders[0].get())->peer_);
    if (running_get_channel_difference(dialog_id)) {
      LOG(INFO) << "Postpone result of channels getDialogs for " << dialog_id;
      pending_channel_on_get_dialogs_.emplace(
          dialog_id, PendingOnGetDialogs{folder_id, std::move(dialog_folders), total_count, std::move(messages),
                                         std::move(promise)});
      return;
    }
  }

  vector<tl_object_ptr<telegram_api::dialog>> dialogs;
  for (auto &dialog_folder : dialog_folders) {
    switch (dialog_folder->get_id()) {
      case telegram_api::dialog::ID:
        dialogs.push_back(telegram_api::move_object_as<telegram_api::dialog>(dialog_folder));
        break;
      case telegram_api::dialogFolder::ID: {
        auto folder = telegram_api::move_object_as<telegram_api::dialogFolder>(dialog_folder);
        if (from_pinned_dialog_list) {
          // TODO update unread_muted_peers_count:int unread_unmuted_peers_count:int
          // unread_muted_messages_count:int unread_unmuted_messages_count:int
          FolderId folder_folder_id(folder->folder_->id_);
          if (folder_folder_id == FolderId::archive()) {
            // archive is expected in pinned dialogs list
            break;
          }
        }
        LOG(ERROR) << "Receive unexpected " << to_string(folder);
        break;
      }
      default:
        UNREACHABLE();
    }
  }

  if (from_get_dialog) {
    LOG(INFO) << "Process " << dialogs.size() << " chats";
  } else if (from_pinned_dialog_list) {
    LOG(INFO) << "Process " << dialogs.size() << " pinned chats in " << folder_id;
  } else {
    LOG(INFO) << "Process " << dialogs.size() << " chats out of " << total_count << " in " << folder_id;
  }
  std::unordered_map<FullMessageId, DialogDate, FullMessageIdHash> full_message_id_to_dialog_date;
  std::unordered_map<FullMessageId, tl_object_ptr<telegram_api::Message>, FullMessageIdHash> full_message_id_to_message;
  for (auto &message : messages) {
    auto full_message_id = get_full_message_id(message, false);
    if (from_dialog_list) {
      auto message_date = get_message_date(message);
      int64 order = get_dialog_order(full_message_id.get_message_id(), message_date);
      full_message_id_to_dialog_date.emplace(full_message_id, DialogDate(order, full_message_id.get_dialog_id()));
    }
    full_message_id_to_message[full_message_id] = std::move(message);
  }

  DialogDate max_dialog_date = MIN_DIALOG_DATE;
  for (auto &dialog : dialogs) {
    //    LOG(INFO) << to_string(dialog);
    DialogId dialog_id(dialog->peer_);
    bool has_pts = (dialog->flags_ & DIALOG_FLAG_HAS_PTS) != 0;

    if (!dialog_id.is_valid()) {
      LOG(ERROR) << "Receive wrong " << dialog_id;
      return promise.set_error(Status::Error(500, "Wrong query result returned: receive wrong chat identifier"));
    }
    switch (dialog_id.get_type()) {
      case DialogType::User:
      case DialogType::Chat:
        if (has_pts) {
          LOG(ERROR) << "Receive user or group " << dialog_id << " with pts";
          return promise.set_error(
              Status::Error(500, "Wrong query result returned: receive user or basic group chat with pts"));
        }
        break;
      case DialogType::Channel:
        if (!has_pts) {
          LOG(ERROR) << "Receive channel " << dialog_id << " without pts";
          return promise.set_error(
              Status::Error(500, "Wrong query result returned: receive supergroup chat without pts"));
        }
        break;
      case DialogType::SecretChat:
      case DialogType::None:
      default:
        UNREACHABLE();
        return promise.set_error(Status::Error(500, "UNREACHABLE"));
    }

    if (from_dialog_list) {
      MessageId last_message_id(ServerMessageId(dialog->top_message_));
      if (last_message_id.is_valid()) {
        FullMessageId full_message_id(dialog_id, last_message_id);
        auto it = full_message_id_to_dialog_date.find(full_message_id);
        if (it == full_message_id_to_dialog_date.end()) {
          LOG(ERROR) << "Last " << last_message_id << " in " << dialog_id << " not found";
          return promise.set_error(Status::Error(500, "Wrong query result returned: last message not found"));
        }
        FolderId dialog_folder_id((dialog->flags_ & DIALOG_FLAG_HAS_FOLDER_ID) != 0 ? dialog->folder_id_ : 0);
        if (dialog_folder_id != folder_id) {
          LOG(ERROR) << "Receive " << dialog_id << " in " << dialog_folder_id << " instead of " << folder_id;
          continue;
        }

        DialogDate dialog_date = it->second;
        CHECK(dialog_date.get_dialog_id() == dialog_id);

        if (dialog_date.get_date() > 0 && max_dialog_date < dialog_date) {
          max_dialog_date = dialog_date;
        }
      } else {
        LOG(ERROR) << "Receive " << last_message_id << " as last chat message";
        continue;
      }
    }
  }

  if (from_dialog_list && total_count < narrow_cast<int32>(dialogs.size())) {
    LOG(ERROR) << "Receive chat total_count = " << total_count << ", but " << dialogs.size() << " chats";
    total_count = narrow_cast<int32>(dialogs.size());
  }

  vector<DialogId> added_dialog_ids;
  for (auto &dialog : dialogs) {
    MessageId last_message_id(ServerMessageId(dialog->top_message_));
    if (!last_message_id.is_valid() && from_dialog_list) {
      // skip dialogs without messages
      total_count--;
      continue;
    }

    DialogId dialog_id(dialog->peer_);
    if (td::contains(added_dialog_ids, dialog_id)) {
      LOG(ERROR) << "Receive " << dialog_id << " twice in result of getChats with total_count = " << total_count;
      continue;
    }
    added_dialog_ids.push_back(dialog_id);
    Dialog *d = get_dialog_force(dialog_id, "on_get_dialogs");
    bool need_update_dialog_pos = false;
    CHECK(!being_added_dialog_id_.is_valid());
    being_added_dialog_id_ = dialog_id;
    if (d == nullptr) {
      d = add_dialog(dialog_id, "on_get_dialogs");
      need_update_dialog_pos = true;
    } else {
      LOG(INFO) << "Receive already created " << dialog_id;
      CHECK(d->dialog_id == dialog_id);
    }
    bool is_new = d->last_new_message_id == MessageId();
    auto positions = get_dialog_positions(d);

    set_dialog_folder_id(d, FolderId((dialog->flags_ & DIALOG_FLAG_HAS_FOLDER_ID) != 0 ? dialog->folder_id_ : 0));

    on_update_dialog_notify_settings(dialog_id, std::move(dialog->notify_settings_), "on_get_dialogs");
    if (!d->notification_settings.is_synchronized && !td_->auth_manager_->is_bot()) {
      LOG(ERROR) << "Failed to synchronize settings in " << dialog_id;
      d->notification_settings.is_synchronized = true;
      on_dialog_updated(dialog_id, "set notification_settings.is_synchronized");
    }

    if (dialog->unread_count_ < 0) {
      LOG(ERROR) << "Receive " << dialog->unread_count_ << " as number of unread messages in " << dialog_id;
      dialog->unread_count_ = 0;
    }
    MessageId read_inbox_max_message_id = MessageId(ServerMessageId(dialog->read_inbox_max_id_));
    if (!read_inbox_max_message_id.is_valid() && read_inbox_max_message_id != MessageId()) {
      LOG(ERROR) << "Receive " << read_inbox_max_message_id << " as last read inbox message in " << dialog_id;
      read_inbox_max_message_id = MessageId();
    }
    MessageId read_outbox_max_message_id = MessageId(ServerMessageId(dialog->read_outbox_max_id_));
    if (!read_outbox_max_message_id.is_valid() && read_outbox_max_message_id != MessageId()) {
      LOG(ERROR) << "Receive " << read_outbox_max_message_id << " as last read outbox message in " << dialog_id;
      read_outbox_max_message_id = MessageId();
    }
    if (dialog->unread_mentions_count_ < 0) {
      LOG(ERROR) << "Receive " << dialog->unread_mentions_count_ << " as number of unread mention messages in "
                 << dialog_id;
      dialog->unread_mentions_count_ = 0;
    }
    if (!d->is_is_blocked_inited && !td_->auth_manager_->is_bot()) {
      // asynchronously get is_blocked from the server
      // TODO add is_blocked to telegram_api::dialog
      get_dialog_info_full(dialog_id, Auto(), "on_get_dialogs init is_blocked");
    } else if (!d->is_has_bots_inited && !td_->auth_manager_->is_bot()) {
      // asynchronously get has_bots from the server
      // TODO add has_bots to telegram_api::dialog
      get_dialog_info_full(dialog_id, Auto(), "on_get_dialogs init has_bots");
    } else if (!d->is_theme_name_inited && !td_->auth_manager_->is_bot()) {
      // asynchronously get theme_name from the server
      // TODO add theme_name to telegram_api::dialog
      get_dialog_info_full(dialog_id, Auto(), "on_get_dialogs init theme_name");
    } else if (!d->is_last_pinned_message_id_inited && !td_->auth_manager_->is_bot()) {
      // asynchronously get dialog pinned message from the server
      get_dialog_pinned_message(dialog_id, Auto());
    }

    need_update_dialog_pos |= update_dialog_draft_message(
        d, get_draft_message(td_->contacts_manager_.get(), std::move(dialog->draft_)), true, false);
    if (is_new) {
      bool has_pts = (dialog->flags_ & DIALOG_FLAG_HAS_PTS) != 0;
      if (last_message_id.is_valid()) {
        FullMessageId full_message_id(dialog_id, last_message_id);
        auto last_message = std::move(full_message_id_to_message[full_message_id]);
        if (last_message == nullptr) {
          LOG(ERROR) << "Last " << full_message_id << " not found";
        } else if (!has_pts || d->pts == 0 || dialog->pts_ <= d->pts || d->is_channel_difference_finished) {
          auto added_full_message_id =
              on_get_message(std::move(last_message), false, has_pts, false, false, false, "get chats");
          CHECK(d->last_new_message_id == MessageId());
          set_dialog_last_new_message_id(d, last_message_id, "on_get_dialogs");
          if (d->last_new_message_id > d->last_message_id && added_full_message_id.get_message_id().is_valid()) {
            CHECK(added_full_message_id.get_message_id() == d->last_new_message_id);
            set_dialog_last_message_id(d, d->last_new_message_id, "on_get_dialogs");
            send_update_chat_last_message(d, "on_get_dialogs");
          }
        } else {
          get_channel_difference(dialog_id, d->pts, true, "on_get_dialogs");
        }
      }

      if (has_pts && !running_get_channel_difference(dialog_id)) {
        set_channel_pts(d, dialog->pts_, "get channel");
      }
    }
    bool is_marked_as_unread = dialog->unread_mark_;
    if (is_marked_as_unread != d->is_marked_as_unread) {
      set_dialog_is_marked_as_unread(d, is_marked_as_unread);
    }

    if (need_update_dialog_pos) {
      update_dialog_pos(d, "on_get_dialogs");
    }

    if (!td_->auth_manager_->is_bot() && !from_pinned_dialog_list) {
      // set is_pinned only after updating dialog pos to ensure that order is initialized
      bool is_pinned = (dialog->flags_ & DIALOG_FLAG_IS_PINNED) != 0;
      bool was_pinned = is_dialog_pinned(DialogListId(d->folder_id), dialog_id);
      if (is_pinned != was_pinned) {
        set_dialog_is_pinned(DialogListId(d->folder_id), d, is_pinned);
      }
    }

    if (!G()->parameters().use_message_db || is_new || !d->is_last_read_inbox_message_id_inited ||
        d->need_repair_server_unread_count) {
      if (d->last_read_inbox_message_id.is_valid() && !d->last_read_inbox_message_id.is_server() &&
          read_inbox_max_message_id == d->last_read_inbox_message_id.get_prev_server_message_id()) {
        read_inbox_max_message_id = d->last_read_inbox_message_id;
      }
      if (d->need_repair_server_unread_count &&
          (d->last_read_inbox_message_id <= read_inbox_max_message_id || !need_unread_counter(d->order) ||
           !have_input_peer(dialog_id, AccessRights::Read))) {
        LOG(INFO) << "Repaired server unread count in " << dialog_id << " from " << d->last_read_inbox_message_id << "/"
                  << d->server_unread_count << " to " << read_inbox_max_message_id << "/" << dialog->unread_count_;
        d->need_repair_server_unread_count = false;
        on_dialog_updated(dialog_id, "repaired dialog server unread count");
      }
      if (d->need_repair_server_unread_count) {
        auto &previous_message_id = previous_repaired_read_inbox_max_message_id_[dialog_id];
        if (previous_message_id >= read_inbox_max_message_id) {
          // protect from sending the request in a loop
          LOG(ERROR) << "Failed to repair server unread count in " << dialog_id
                     << ", because receive read_inbox_max_message_id = " << read_inbox_max_message_id << " after "
                     << previous_message_id << ", but messages are read up to " << d->last_read_inbox_message_id;
          d->need_repair_server_unread_count = false;
          on_dialog_updated(dialog_id, "failed to repair dialog server unread count");
        } else {
          LOG(INFO) << "Have last_read_inbox_message_id = " << d->last_read_inbox_message_id << ", but received only "
                    << read_inbox_max_message_id << " from the server, trying to repair server unread count again";
          previous_message_id = read_inbox_max_message_id;
          repair_server_unread_count(dialog_id, d->server_unread_count);
        }
      }
      if (!d->need_repair_server_unread_count) {
        previous_repaired_read_inbox_max_message_id_.erase(dialog_id);
      }
      if ((d->server_unread_count != dialog->unread_count_ &&
           d->last_read_inbox_message_id == read_inbox_max_message_id) ||
          d->last_read_inbox_message_id < read_inbox_max_message_id) {
        set_dialog_last_read_inbox_message_id(d, read_inbox_max_message_id, dialog->unread_count_,
                                              d->local_unread_count, true, "on_get_dialogs");
      }
      if (!d->is_last_read_inbox_message_id_inited) {
        d->is_last_read_inbox_message_id_inited = true;
        on_dialog_updated(dialog_id, "set is_last_read_inbox_message_id_inited");
      }
    }

    if (!G()->parameters().use_message_db || is_new || !d->is_last_read_outbox_message_id_inited) {
      if (d->last_read_outbox_message_id < read_outbox_max_message_id) {
        set_dialog_last_read_outbox_message_id(d, read_outbox_max_message_id);
      }
      if (!d->is_last_read_outbox_message_id_inited) {
        d->is_last_read_outbox_message_id_inited = true;
        on_dialog_updated(dialog_id, "set is_last_read_outbox_message_id_inited");
      }
    }

    if (!G()->parameters().use_message_db || is_new) {
      if (d->unread_mention_count != dialog->unread_mentions_count_) {
        set_dialog_unread_mention_count(d, dialog->unread_mentions_count_);
        update_dialog_mention_notification_count(d);
        send_update_chat_unread_mention_count(d);
      }
    }

    being_added_dialog_id_ = DialogId();

    update_dialog_lists(d, std::move(positions), true, false, "on_get_dialogs");
  }

  if (from_dialog_list) {
    CHECK(!td_->auth_manager_->is_bot());
    CHECK(total_count >= 0);

    auto &folder_list = add_dialog_list(DialogListId(folder_id));
    if (folder_list.server_dialog_total_count_ != total_count) {
      auto old_dialog_total_count = get_dialog_total_count(folder_list);
      folder_list.server_dialog_total_count_ = total_count;
      if (folder_list.is_dialog_unread_count_inited_) {
        if (old_dialog_total_count != get_dialog_total_count(folder_list)) {
          send_update_unread_chat_count(folder_list, DialogId(), true, "on_get_dialogs");
        } else {
          save_unread_chat_count(folder_list);
        }
      }
    }

    auto *folder = get_dialog_folder(folder_id);
    CHECK(folder != nullptr);
    if (dialogs.empty()) {
      // if there are no more dialogs on the server
      max_dialog_date = MAX_DIALOG_DATE;
    }
    if (folder->last_server_dialog_date_ < max_dialog_date) {
      folder->last_server_dialog_date_ = max_dialog_date;
      update_last_dialog_date(folder_id);
    } else if (promise) {
      LOG(ERROR) << "Last server dialog date didn't increased from " << folder->last_server_dialog_date_ << " to "
                 << max_dialog_date << " after receiving " << dialogs.size() << " chats " << added_dialog_ids
                 << " from " << total_count << " in " << folder_id
                 << ". last_dialog_date = " << folder->folder_last_dialog_date_
                 << ", last_loaded_database_dialog_date = " << folder->last_loaded_database_dialog_date_;
    }
  }
  if (from_pinned_dialog_list) {
    CHECK(!td_->auth_manager_->is_bot());
    auto *folder_list = get_dialog_list(DialogListId(folder_id));
    CHECK(folder_list != nullptr);
    auto pinned_dialog_ids = remove_secret_chat_dialog_ids(get_pinned_dialog_ids(DialogListId(folder_id)));
    bool are_pinned_dialogs_saved = folder_list->are_pinned_dialogs_inited_;
    folder_list->are_pinned_dialogs_inited_ = true;
    if (pinned_dialog_ids != added_dialog_ids) {
      LOG(INFO) << "Update pinned chats order from " << format::as_array(pinned_dialog_ids) << " to "
                << format::as_array(added_dialog_ids);
      std::unordered_set<DialogId, DialogIdHash> old_pinned_dialog_ids(pinned_dialog_ids.begin(),
                                                                       pinned_dialog_ids.end());

      std::reverse(pinned_dialog_ids.begin(), pinned_dialog_ids.end());
      std::reverse(added_dialog_ids.begin(), added_dialog_ids.end());
      auto old_it = pinned_dialog_ids.begin();
      for (auto dialog_id : added_dialog_ids) {
        old_pinned_dialog_ids.erase(dialog_id);
        while (old_it < pinned_dialog_ids.end()) {
          if (*old_it == dialog_id) {
            break;
          }
          ++old_it;
        }
        if (old_it < pinned_dialog_ids.end()) {
          // leave dialog where it is
          ++old_it;
          continue;
        }
        if (set_dialog_is_pinned(dialog_id, true)) {
          are_pinned_dialogs_saved = true;
        }
      }
      for (auto dialog_id : old_pinned_dialog_ids) {
        Dialog *d = get_dialog_force(dialog_id, "on_get_dialogs pinned");
        if (d == nullptr) {
          LOG(ERROR) << "Failed to find " << dialog_id << " to unpin in " << folder_id;
          force_create_dialog(dialog_id, "from_pinned_dialog_list", true);
          d = get_dialog_force(dialog_id, "on_get_dialogs pinned 2");
        }
        if (d != nullptr && set_dialog_is_pinned(DialogListId(folder_id), d, false)) {
          are_pinned_dialogs_saved = true;
        }
      }
    } else {
      LOG(INFO) << "Pinned chats are not changed";
    }
    update_list_last_pinned_dialog_date(*folder_list);

    if (!are_pinned_dialogs_saved && G()->parameters().use_message_db) {
      LOG(INFO) << "Save empty pinned chat list in " << folder_id;
      G()->td_db()->get_binlog_pmc()->set(PSTRING() << "pinned_dialog_ids" << folder_id.get(), "");
    }
  }
  promise.set_value(Unit());
}

void MessagesManager::delete_messages_from_updates(const vector<MessageId> &message_ids) {
  std::unordered_map<DialogId, vector<int64>, DialogIdHash> deleted_message_ids;
  std::unordered_map<DialogId, bool, DialogIdHash> need_update_dialog_pos;
  for (auto message_id : message_ids) {
    if (!message_id.is_valid() || !message_id.is_server()) {
      LOG(ERROR) << "Incoming update tries to delete " << message_id;
      continue;
    }

    Dialog *d = get_dialog_by_message_id(message_id);
    if (d != nullptr) {
      auto message = delete_message(d, message_id, true, &need_update_dialog_pos[d->dialog_id], "updates");
      CHECK(message != nullptr);
      LOG_CHECK(message->message_id == message_id) << message_id << " " << message->message_id << " " << d->dialog_id;
      deleted_message_ids[d->dialog_id].push_back(message->message_id.get());
    }
    if (last_clear_history_message_id_to_dialog_id_.count(message_id)) {
      d = get_dialog(last_clear_history_message_id_to_dialog_id_[message_id]);
      CHECK(d != nullptr);
      auto message = delete_message(d, message_id, true, &need_update_dialog_pos[d->dialog_id], "updates");
      CHECK(message == nullptr);
    }
  }
  for (auto &it : need_update_dialog_pos) {
    if (it.second) {
      auto dialog_id = it.first;
      Dialog *d = get_dialog(dialog_id);
      CHECK(d != nullptr);
      send_update_chat_last_message(d, "delete_messages_from_updates");
    }
  }
  for (auto &it : deleted_message_ids) {
    auto dialog_id = it.first;
    send_update_delete_messages(dialog_id, std::move(it.second), true, false);
  }
}

bool MessagesManager::update_message_contains_unread_mention(Dialog *d, Message *m, bool contains_unread_mention,
                                                             const char *source) {
  LOG_CHECK(m != nullptr) << source;
  CHECK(!m->message_id.is_scheduled());
  if (!contains_unread_mention && m->contains_unread_mention) {
    remove_message_notification_id(d, m, true, true);  // should be called before contains_unread_mention is updated

    m->contains_unread_mention = false;
    if (d->unread_mention_count == 0) {
      if (is_dialog_inited(d)) {
        LOG(ERROR) << "Unread mention count of " << d->dialog_id << " became negative from " << source;
      }
    } else {
      set_dialog_unread_mention_count(d, d->unread_mention_count - 1);
      on_dialog_updated(d->dialog_id, "update_message_contains_unread_mention");
    }
    LOG(INFO) << "Update unread mention message count in " << d->dialog_id << " to " << d->unread_mention_count
              << " by reading " << m->message_id << " from " << source;

    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateMessageMentionRead>(d->dialog_id.get(), m->message_id.get(),
                                                                  d->unread_mention_count));
    return true;
  }
  return false;
}

MessagesManager::Message *MessagesManager::get_message_force(Dialog *d, MessageId message_id, const char *source) {
  if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
    return nullptr;
  }

  auto result = get_message(d, message_id);
  if (result != nullptr) {
    return result;
  }

  if (!G()->parameters().use_message_db || message_id.is_yet_unsent()) {
    return nullptr;
  }

  if (d->deleted_message_ids.count(message_id)) {
    return nullptr;
  }

  if (message_id.is_scheduled()) {
    if (d->has_loaded_scheduled_messages_from_database) {
      return nullptr;
    }
    if (message_id.is_scheduled_server() &&
        d->deleted_scheduled_server_message_ids.count(message_id.get_scheduled_server_message_id())) {
      return nullptr;
    }
  }

  LOG(INFO) << "Trying to load " << FullMessageId{d->dialog_id, message_id} << " from database from " << source;

  auto r_value = G()->td_db()->get_messages_db_sync()->get_message({d->dialog_id, message_id});
  if (r_value.is_error()) {
    return nullptr;
  }
  return on_get_message_from_database(d, r_value.ok(), message_id.is_scheduled(), source);
}

void MessagesManager::read_secret_chat_outbox(SecretChatId secret_chat_id, int32 up_to_date, int32 read_date) {
  if (!secret_chat_id.is_valid()) {
    LOG(ERROR) << "Receive read secret chat outbox in the invalid " << secret_chat_id;
    return;
  }
  auto dialog_id = DialogId(secret_chat_id);
  Dialog *d = get_dialog_force(dialog_id, "read_secret_chat_outbox");
  if (d == nullptr) {
    return;
  }

  if (read_date > 0) {
    auto user_id = td_->contacts_manager_->get_secret_chat_user_id(secret_chat_id);
    if (user_id.is_valid()) {
      td_->contacts_manager_->on_update_user_local_was_online(user_id, read_date);
    }
  }

  // TODO: protect with log event
  suffix_load_till_date(
      d, up_to_date,
      PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, up_to_date, read_date](Result<Unit> result) {
        send_closure(actor_id, &MessagesManager::read_secret_chat_outbox_inner, dialog_id, up_to_date, read_date);
      }));
}

void MessagesManager::set_dialog_last_message_id(Dialog *d, MessageId last_message_id, const char *source) {
  CHECK(!last_message_id.is_scheduled());

  LOG(INFO) << "Set " << d->dialog_id << " last message to " << last_message_id << " from " << source;
  d->last_message_id = last_message_id;

  if (!last_message_id.is_valid()) {
    d->suffix_load_first_message_id_ = MessageId();
    d->suffix_load_done_ = false;
  }
  if (last_message_id.is_valid() && d->delete_last_message_date != 0) {
    d->delete_last_message_date = 0;
    d->deleted_last_message_id = MessageId();
    d->is_last_message_deleted_locally = false;
    on_dialog_updated(d->dialog_id, "update_delete_last_message_date");
  }
  if (d->pending_last_message_date != 0) {
    d->pending_last_message_date = 0;
    d->pending_last_message_id = MessageId();
  }
}

void MessagesManager::set_dialog_pending_join_requests(Dialog *d, int32 pending_join_request_count,
                                                       vector<UserId> pending_join_request_user_ids) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  fix_pending_join_requests(d->dialog_id, pending_join_request_count, pending_join_request_user_ids);
  if (d->pending_join_request_count == pending_join_request_count &&
      d->pending_join_request_user_ids == pending_join_request_user_ids) {
    return;
  }
  d->pending_join_request_count = pending_join_request_count;
  d->pending_join_request_user_ids = std::move(pending_join_request_user_ids);
  send_update_chat_pending_join_requests(d);
}

void MessagesManager::on_dialog_unmute_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  if (1 <= dialog_id_int && dialog_id_int <= 3) {
    send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::on_scope_unmute,
                       static_cast<NotificationSettingsScope>(dialog_id_int - 1));
  } else {
    send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::on_dialog_unmute,
                       DialogId(dialog_id_int));
  }
}

tl_object_ptr<td_api::messages> MessagesManager::get_messages_object(int32 total_count,
                                                                     const vector<FullMessageId> &full_message_ids,
                                                                     bool skip_not_found, const char *source) {
  auto message_objects = transform(full_message_ids, [this, source](FullMessageId full_message_id) {
    return get_message_object(full_message_id, source);
  });
  return get_messages_object(total_count, std::move(message_objects), skip_not_found);
}

void MessagesManager::set_dialog_last_read_outbox_message_id(Dialog *d, MessageId message_id) {
  CHECK(!message_id.is_scheduled());

  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG(INFO) << "Update last read outbox message in " << d->dialog_id << " from " << d->last_read_outbox_message_id
            << " to " << message_id;
  d->last_read_outbox_message_id = message_id;
  d->is_last_read_outbox_message_id_inited = true;
  send_update_chat_read_outbox(d);
}

void MessagesManager::on_dialog_photo_updated(DialogId dialog_id) {
  auto d = get_dialog(dialog_id);  // called from update_user, must not create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    send_closure(
        G()->td(), &Td::send_update,
        make_tl_object<td_api::updateChatPhoto>(
            dialog_id.get(), get_chat_photo_info_object(td_->file_manager_.get(), get_dialog_photo(dialog_id))));
  }
}

void MessagesManager::drop_dialog_pending_join_requests(DialogId dialog_id) {
  CHECK(dialog_id.is_valid());
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  auto d = get_dialog(dialog_id);  // called from update_chat/channel, must not create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    set_dialog_pending_join_requests(d, 0, {});
  }
}

void MessagesManager::get_history_from_the_end(DialogId dialog_id, bool from_database, bool only_local,
                                               Promise<Unit> &&promise) {
  get_history_from_the_end_impl(get_dialog(dialog_id), from_database, only_local, std::move(promise));
}

tl_object_ptr<td_api::message> MessagesManager::get_message_object(FullMessageId full_message_id, const char *source) {
  return get_message_object(full_message_id.get_dialog_id(), get_message_force(full_message_id, source), source);
}

void MessagesManager::hide_dialog_action_bar(DialogId dialog_id) {
  Dialog *d = get_dialog_force(dialog_id, "hide_dialog_action_bar");
  if (d == nullptr) {
    return;
  }
  hide_dialog_action_bar(d);
}
}  // namespace td
