//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChatId.h"
#include "td/telegram/ConfigShared.h"
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
#include "td/telegram/GroupCallManager.h"

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
#include <cstring>
#include <limits>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

void MessagesManager::on_dialog_action(DialogId dialog_id, MessageId top_thread_message_id, DialogId typing_dialog_id,
                                       DialogAction action, int32 date, MessageContentType message_content_type) {
  if (td_->auth_manager_->is_bot() || !typing_dialog_id.is_valid()) {
    return;
  }
  if (top_thread_message_id != MessageId() && !top_thread_message_id.is_valid()) {
    LOG(ERROR) << "Ignore " << action << " in the message thread of " << top_thread_message_id;
    return;
  }

  auto dialog_type = dialog_id.get_type();
  if (action == DialogAction::get_speaking_action()) {
    if ((dialog_type != DialogType::Chat && dialog_type != DialogType::Channel) || top_thread_message_id.is_valid()) {
      LOG(ERROR) << "Receive " << action << " in thread of " << top_thread_message_id << " in " << dialog_id;
      return;
    }
    const Dialog *d = get_dialog_force(dialog_id, "on_dialog_action");
    if (d != nullptr && d->active_group_call_id.is_valid()) {
      auto group_call_id = td_->group_call_manager_->get_group_call_id(d->active_group_call_id, dialog_id);
      td_->group_call_manager_->on_user_speaking_in_group_call(group_call_id, typing_dialog_id, date);
    }
    return;
  }

  if (is_broadcast_channel(dialog_id)) {
    return;
  }

  auto typing_dialog_type = typing_dialog_id.get_type();
  if (typing_dialog_type != DialogType::User && dialog_type != DialogType::Chat && dialog_type != DialogType::Channel) {
    LOG(ERROR) << "Ignore " << action << " of " << typing_dialog_id << " in " << dialog_id;
    return;
  }

  {
    auto message_import_progress = action.get_importing_messages_action_progress();
    if (message_import_progress >= 0) {
      // TODO
      return;
    }
  }

  {
    auto clicking_info = action.get_clicking_animated_emoji_action_info();
    if (!clicking_info.data.empty()) {
      if (date > G()->unix_time() - 10 && dialog_type == DialogType::User && dialog_id == typing_dialog_id) {
        FullMessageId full_message_id{dialog_id, MessageId(ServerMessageId(clicking_info.message_id))};
        auto *m = get_message_force(full_message_id, "on_dialog_action");
        if (m != nullptr) {
          on_message_content_animated_emoji_clicked(m->content.get(), full_message_id, td_, clicking_info.emoji,
                                                    std::move(clicking_info.data));
        }
      }
      return;
    }
  }

  if (is_unsent_animated_emoji_click(td_, dialog_id, action)) {
    LOG(DEBUG) << "Ignore unsent " << action;
    return;
  }

  if (!have_dialog(dialog_id)) {
    LOG(DEBUG) << "Ignore " << action << " in unknown " << dialog_id;
    return;
  }

  if (typing_dialog_type == DialogType::User) {
    if (!td_->contacts_manager_->have_min_user(typing_dialog_id.get_user_id())) {
      LOG(DEBUG) << "Ignore " << action << " of unknown " << typing_dialog_id.get_user_id();
      return;
    }
  } else {
    if (!have_dialog_info_force(typing_dialog_id)) {
      LOG(DEBUG) << "Ignore " << action << " of unknown " << typing_dialog_id;
      return;
    }
    force_create_dialog(typing_dialog_id, "on_dialog_action", true);
    if (!have_dialog(typing_dialog_id)) {
      LOG(ERROR) << "Failed to create typing " << typing_dialog_id;
      return;
    }
  }

  bool is_canceled = action == DialogAction();
  if ((!is_canceled || message_content_type != MessageContentType::None) && typing_dialog_type == DialogType::User) {
    td_->contacts_manager_->on_update_user_local_was_online(typing_dialog_id.get_user_id(), date);
  }

  if (dialog_type == DialogType::User || dialog_type == DialogType::SecretChat) {
    CHECK(typing_dialog_type == DialogType::User);
    auto user_id = typing_dialog_id.get_user_id();
    if (!td_->contacts_manager_->is_user_bot(user_id) && !td_->contacts_manager_->is_user_status_exact(user_id) &&
        !get_dialog(dialog_id)->is_opened && !is_canceled) {
      return;
    }
  }

  if (is_canceled) {
    auto actions_it = active_dialog_actions_.find(dialog_id);
    if (actions_it == active_dialog_actions_.end()) {
      return;
    }

    auto &active_actions = actions_it->second;
    auto it = std::find_if(
        active_actions.begin(), active_actions.end(),
        [typing_dialog_id](const ActiveDialogAction &action) { return action.typing_dialog_id == typing_dialog_id; });
    if (it == active_actions.end()) {
      return;
    }

    if (!(typing_dialog_type == DialogType::User &&
          td_->contacts_manager_->is_user_bot(typing_dialog_id.get_user_id())) &&
        !it->action.is_canceled_by_message_of_type(message_content_type)) {
      return;
    }

    LOG(DEBUG) << "Cancel action of " << typing_dialog_id << " in " << dialog_id;
    top_thread_message_id = it->top_thread_message_id;
    active_actions.erase(it);
    if (active_actions.empty()) {
      active_dialog_actions_.erase(dialog_id);
      LOG(DEBUG) << "Cancel action timeout in " << dialog_id;
      active_dialog_action_timeout_.cancel_timeout(dialog_id.get());
    }
  } else {
    if (date < G()->unix_time_cached() - DIALOG_ACTION_TIMEOUT - 60) {
      LOG(DEBUG) << "Ignore too old action of " << typing_dialog_id << " in " << dialog_id << " sent at " << date;
      return;
    }
    auto &active_actions = active_dialog_actions_[dialog_id];
    auto it = std::find_if(
        active_actions.begin(), active_actions.end(),
        [typing_dialog_id](const ActiveDialogAction &action) { return action.typing_dialog_id == typing_dialog_id; });
    MessageId prev_top_thread_message_id;
    DialogAction prev_action;
    if (it != active_actions.end()) {
      LOG(DEBUG) << "Re-add action of " << typing_dialog_id << " in " << dialog_id;
      prev_top_thread_message_id = it->top_thread_message_id;
      prev_action = it->action;
      active_actions.erase(it);
    } else {
      LOG(DEBUG) << "Add action of " << typing_dialog_id << " in " << dialog_id;
    }

    active_actions.emplace_back(top_thread_message_id, typing_dialog_id, action, Time::now());
    if (top_thread_message_id == prev_top_thread_message_id && action == prev_action) {
      return;
    }
    if (top_thread_message_id != prev_top_thread_message_id && prev_top_thread_message_id.is_valid()) {
      send_update_chat_action(dialog_id, prev_top_thread_message_id, typing_dialog_id, DialogAction());
    }
    if (active_actions.size() == 1u) {
      LOG(DEBUG) << "Set action timeout in " << dialog_id;
      active_dialog_action_timeout_.set_timeout_in(dialog_id.get(), DIALOG_ACTION_TIMEOUT);
    }
  }

  if (top_thread_message_id.is_valid()) {
    send_update_chat_action(dialog_id, MessageId(), typing_dialog_id, action);
  }
  send_update_chat_action(dialog_id, top_thread_message_id, typing_dialog_id, action);
}

void MessagesManager::fail_send_message(FullMessageId full_message_id, int error_code, const string &error_message) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  MessageId old_message_id = full_message_id.get_message_id();
  CHECK(old_message_id.is_valid() || old_message_id.is_valid_scheduled());
  CHECK(old_message_id.is_yet_unsent());

  bool need_update_dialog_pos = false;
  being_readded_message_id_ = full_message_id;
  unique_ptr<Message> message = delete_message(d, old_message_id, false, &need_update_dialog_pos, "fail send message");
  if (message == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel
    // don't need to send update to the user, because the message has already been deleted
    // and there is nothing to be deleted from the server
    being_readded_message_id_ = FullMessageId();
    return;
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // LOG(ERROR) << "Found " << old_message_id << " in inaccessible " << dialog_id;
    // dump_debug_message_op(d, 5);
  }

  MessageId new_message_id =
      old_message_id.get_next_message_id(MessageType::Local);  // trying to not change message place
  if (!old_message_id.is_scheduled()) {
    if (get_message_force(d, new_message_id, "fail_send_message") != nullptr ||
        d->deleted_message_ids.count(new_message_id) || new_message_id <= d->last_clear_history_message_id) {
      new_message_id = get_next_local_message_id(d);
    } else if (new_message_id > d->last_assigned_message_id) {
      d->last_assigned_message_id = new_message_id;
    }
  } else {
    // check deleted_message_ids, because the new_message_id is not a server scheduled
    while (get_message_force(d, new_message_id, "fail_send_message") != nullptr ||
           d->deleted_message_ids.count(new_message_id)) {
      new_message_id = new_message_id.get_next_message_id(MessageType::Local);
    }
  }

  set_message_id(message, new_message_id);
  if (old_message_id.is_scheduled()) {
    CHECK(message->message_id.is_valid_scheduled());
  } else {
    CHECK(message->message_id.is_valid());
  }
  if (message->forward_info == nullptr && message->view_count == 1) {
    message->view_count = 0;
  }
  message->is_failed_to_send = true;
  message->send_error_code = error_code;
  message->send_error_message = error_message;
  message->try_resend_at = 0.0;
  Slice retry_after_prefix("Too Many Requests: retry after ");
  if (error_code == 429 && begins_with(error_message, retry_after_prefix)) {
    auto r_retry_after = to_integer_safe<int32>(error_message.substr(retry_after_prefix.size()));
    if (r_retry_after.is_ok() && r_retry_after.ok() > 0) {
      message->try_resend_at = Time::now() + r_retry_after.ok();
    }
  }
  update_failed_to_send_message_content(td_, message->content);

  message->from_database = false;
  message->have_previous = true;
  message->have_next = true;

  bool need_update = false;
  Message *m = add_message_to_dialog(dialog_id, std::move(message), false, &need_update, &need_update_dialog_pos,
                                     "fail_send_message");
  LOG_CHECK(m != nullptr) << "Failed to add failed to send " << new_message_id << " to " << dialog_id << " due to "
                          << debug_add_message_to_dialog_fail_reason_;
  if (!m->message_id.is_scheduled()) {
    // add_message_to_dialog will not update counts, because need_update == false
    update_message_count_by_index(d, +1, m);
    update_reply_count_by_message(d, +1, m);  // no-op because the message isn't server
  }
  register_new_local_message_id(d, m);

  LOG(INFO) << "Send updateMessageSendFailed for " << full_message_id;
  if (!td_->auth_manager_->is_bot()) {
    d->yet_unsent_message_id_to_persistent_message_id.emplace(old_message_id, m->message_id);
  }
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateMessageSendFailed>(get_message_object(dialog_id, m, "fail_send_message"),
                                                               old_message_id.get(), error_code, error_message));
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "fail_send_message");
  }
  being_readded_message_id_ = FullMessageId();
}

void MessagesManager::on_get_dialogs_from_database(FolderId folder_id, int32 limit, DialogDbGetDialogsResult &&dialogs,
                                                   Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  CHECK(!td_->auth_manager_->is_bot());
  auto &folder = *get_dialog_folder(folder_id);
  LOG(INFO) << "Receive " << dialogs.dialogs.size() << " from expected " << limit << " chats in " << folder_id
            << " in from database with next order " << dialogs.next_order << " and next " << dialogs.next_dialog_id;
  int32 new_get_dialogs_limit = 0;
  int32 have_more_dialogs_in_database = (limit == static_cast<int32>(dialogs.dialogs.size()));
  if (have_more_dialogs_in_database && limit < folder.load_dialog_list_limit_max_) {
    new_get_dialogs_limit = folder.load_dialog_list_limit_max_ - limit;
  }
  folder.load_dialog_list_limit_max_ = 0;

  size_t dialogs_skipped = 0;
  for (auto &dialog : dialogs.dialogs) {
    Dialog *d = on_load_dialog_from_database(DialogId(), std::move(dialog), "on_get_dialogs_from_database");
    if (d == nullptr) {
      dialogs_skipped++;
      continue;
    }
    if (d->folder_id != folder_id) {
      LOG(WARNING) << "Skip " << d->dialog_id << " received from database, because it is in " << d->folder_id
                   << " instead of " << folder_id;
      dialogs_skipped++;
      continue;
    }

    LOG(INFO) << "Loaded from database " << d->dialog_id << " with order " << d->order;
  }

  DialogDate max_dialog_date(dialogs.next_order, dialogs.next_dialog_id);
  if (!have_more_dialogs_in_database) {
    folder.last_loaded_database_dialog_date_ = MAX_DIALOG_DATE;
    LOG(INFO) << "Set last loaded database dialog date to " << folder.last_loaded_database_dialog_date_;
    folder.last_server_dialog_date_ = max(folder.last_server_dialog_date_, folder.last_database_server_dialog_date_);
    LOG(INFO) << "Set last server dialog date to " << folder.last_server_dialog_date_;
    update_last_dialog_date(folder_id);
  } else if (folder.last_loaded_database_dialog_date_ < max_dialog_date) {
    folder.last_loaded_database_dialog_date_ = min(max_dialog_date, folder.last_database_server_dialog_date_);
    LOG(INFO) << "Set last loaded database dialog date to " << folder.last_loaded_database_dialog_date_;
    folder.last_server_dialog_date_ = max(folder.last_server_dialog_date_, folder.last_loaded_database_dialog_date_);
    LOG(INFO) << "Set last server dialog date to " << folder.last_server_dialog_date_;
    update_last_dialog_date(folder_id);

    for (const auto &list_it : dialog_lists_) {
      auto &list = list_it.second;
      if (!list.load_list_queries_.empty() && has_dialogs_from_folder(list, folder) && new_get_dialogs_limit < limit) {
        new_get_dialogs_limit = limit;
      }
    }
  } else {
    LOG(ERROR) << "Last loaded database dialog date didn't increased, skipped " << dialogs_skipped << " chats out of "
               << dialogs.dialogs.size();
  }

  if (!(folder.last_loaded_database_dialog_date_ < folder.last_database_server_dialog_date_)) {
    // have_more_dialogs_in_database = false;
    new_get_dialogs_limit = 0;
  }

  if (new_get_dialogs_limit == 0) {
    preload_folder_dialog_list_timeout_.add_timeout_in(folder_id.get(), 0.2);
    promise.set_value(Unit());
  } else {
    load_folder_dialog_list_from_database(folder_id, new_get_dialogs_limit, std::move(promise));
  }
}

Status MessagesManager::can_get_message_viewers(DialogId dialog_id, const Message *m) const {
  if (td_->auth_manager_->is_bot()) {
    return Status::Error(400, "User is bot");
  }
  CHECK(m != nullptr);
  if (!m->is_outgoing) {
    return Status::Error(400, "Can't get viewers of incoming messages");
  }
  if (G()->unix_time() - m->date > G()->shared_config().get_option_integer("chat_read_mark_expire_period", 7 * 86400)) {
    return Status::Error(400, "Message is too old");
  }

  int32 participant_count = 0;
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return Status::Error(400, "Can't get message viewers in private chats");
    case DialogType::Chat:
      if (!td_->contacts_manager_->get_chat_is_active(dialog_id.get_chat_id())) {
        return Status::Error(400, "Chat is deactivated");
      }
      participant_count = td_->contacts_manager_->get_chat_participant_count(dialog_id.get_chat_id());
      break;
    case DialogType::Channel:
      if (is_broadcast_channel(dialog_id)) {
        return Status::Error(400, "Can't get message viewers in channel chats");
      }
      participant_count = td_->contacts_manager_->get_channel_participant_count(dialog_id.get_channel_id());
      break;
    case DialogType::SecretChat:
      return Status::Error(400, "Can't get message viewers in secret chats");
    case DialogType::None:
    default:
      UNREACHABLE();
      return Status::OK();
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }
  if (participant_count == 0) {
    return Status::Error(400, "Chat is empty or have unknown number of members");
  }
  if (participant_count > G()->shared_config().get_option_integer("chat_read_mark_size_threshold", 100)) {
    return Status::Error(400, "Chat is too big");
  }

  if (m->message_id.is_scheduled()) {
    return Status::Error(400, "Scheduled messages can't have viewers");
  }
  if (m->message_id.is_yet_unsent()) {
    return Status::Error(400, "Yet unsent messages can't have viewers");
  }
  if (m->message_id.is_local()) {
    return Status::Error(400, "Local messages can't have viewers");
  }
  CHECK(m->message_id.is_server());

  if (m->content->get_type() == MessageContentType::Poll &&
      get_message_content_poll_is_anonymous(td_, m->content.get())) {
    return Status::Error(400, "Anonymous poll viewers are unavailable");
  }

  return Status::OK();
}

void MessagesManager::get_current_state(vector<td_api::object_ptr<td_api::Update>> &updates) const {
  if (!td_->auth_manager_->is_bot()) {
    if (!dialog_filters_.empty()) {
      updates.push_back(get_update_chat_filters_object());
    }
    if (G()->parameters().use_message_db) {
      for (const auto &it : dialog_lists_) {
        auto &list = it.second;
        if (list.is_message_unread_count_inited_) {
          updates.push_back(get_update_unread_message_count_object(list));
        }
        if (list.is_dialog_unread_count_inited_) {
          updates.push_back(get_update_unread_chat_count_object(list));
        }
      }
    }

    vector<NotificationSettingsScope> scopes{NotificationSettingsScope::Private, NotificationSettingsScope::Group,
                                             NotificationSettingsScope::Channel};
    for (auto scope : scopes) {
      auto current_settings = get_scope_notification_settings(scope);
      CHECK(current_settings != nullptr);
      if (current_settings->is_synchronized) {
        updates.push_back(get_update_scope_notification_settings_object(scope));
      }
    }
  }

  vector<td_api::object_ptr<td_api::Update>> last_message_updates;
  for (auto &it : dialogs_) {
    const Dialog *d = it.second.get();
    auto update = td_api::make_object<td_api::updateNewChat>(get_chat_object(d));
    if (update->chat_->last_message_ != nullptr && update->chat_->last_message_->forward_info_ != nullptr) {
      last_message_updates.push_back(td_api::make_object<td_api::updateChatLastMessage>(
          d->dialog_id.get(), std::move(update->chat_->last_message_), get_chat_positions_object(d)));
    }
    updates.push_back(std::move(update));

    if (d->is_opened) {
      auto info_it = dialog_online_member_counts_.find(d->dialog_id);
      if (info_it != dialog_online_member_counts_.end() && info_it->second.is_update_sent) {
        updates.push_back(td_api::make_object<td_api::updateChatOnlineMemberCount>(
            d->dialog_id.get(), info_it->second.online_member_count));
      }
    }
  }
  append(updates, std::move(last_message_updates));
}

td_api::object_ptr<td_api::messageForwardInfo> MessagesManager::get_message_forward_info_object(
    const unique_ptr<MessageForwardInfo> &forward_info) const {
  if (forward_info == nullptr) {
    return nullptr;
  }

  auto origin = [&]() -> td_api::object_ptr<td_api::MessageForwardOrigin> {
    if (forward_info->is_imported) {
      return td_api::make_object<td_api::messageForwardOriginMessageImport>(forward_info->sender_name);
    }
    if (is_forward_info_sender_hidden(forward_info.get())) {
      return td_api::make_object<td_api::messageForwardOriginHiddenUser>(
          forward_info->sender_name.empty() ? forward_info->author_signature : forward_info->sender_name);
    }
    if (forward_info->message_id.is_valid()) {
      return td_api::make_object<td_api::messageForwardOriginChannel>(
          forward_info->sender_dialog_id.get(), forward_info->message_id.get(), forward_info->author_signature);
    }
    if (forward_info->sender_dialog_id.is_valid()) {
      return td_api::make_object<td_api::messageForwardOriginChat>(
          forward_info->sender_dialog_id.get(),
          forward_info->sender_name.empty() ? forward_info->author_signature : forward_info->sender_name);
    }
    return td_api::make_object<td_api::messageForwardOriginUser>(
        td_->contacts_manager_->get_user_id_object(forward_info->sender_user_id, "messageForwardOriginUser"));
  }();

  return td_api::make_object<td_api::messageForwardInfo>(std::move(origin), forward_info->date, forward_info->psa_type,
                                                         forward_info->from_dialog_id.get(),
                                                         forward_info->from_message_id.get());
}

vector<FullMessageId> MessagesManager::get_active_live_location_messages(Promise<Unit> &&promise) {
  if (!G()->parameters().use_message_db) {
    are_active_live_location_messages_loaded_ = true;
  }

  if (!are_active_live_location_messages_loaded_) {
    load_active_live_location_messages_queries_.push_back(std::move(promise));
    if (load_active_live_location_messages_queries_.size() == 1u) {
      LOG(INFO) << "Trying to load active live location messages from database";
      G()->td_db()->get_sqlite_pmc()->get(
          "di_active_live_location_messages", PromiseCreator::lambda([](string value) {
            send_closure(G()->messages_manager(),
                         &MessagesManager::on_load_active_live_location_full_message_ids_from_database,
                         std::move(value));
          }));
    }
    return {};
  }

  promise.set_value(Unit());
  vector<FullMessageId> result;
  for (auto &full_message_id : active_live_location_full_message_ids_) {
    auto m = get_message(full_message_id);
    CHECK(m != nullptr);
    CHECK(m->content->get_type() == MessageContentType::LiveLocation);
    CHECK(!m->message_id.is_scheduled());

    if (m->is_failed_to_send) {
      continue;
    }

    auto live_period = get_message_content_live_location_period(m->content.get());
    if (live_period <= G()->unix_time() - m->date) {  // bool is_expired flag?
      // live location is expired
      continue;
    }
    result.push_back(full_message_id);
  }

  return result;
}

void MessagesManager::on_get_dialog_message_by_date_from_database(DialogId dialog_id, int32 date, int64 random_id,
                                                                  Result<MessagesDbDialogMessage> result,
                                                                  Promise<Unit> promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  if (result.is_ok()) {
    Message *m = on_get_message_from_database(d, result.ok(), false, "on_get_dialog_message_by_date_from_database");
    if (m != nullptr) {
      auto message_id = find_message_by_date(d->messages.get(), date);
      if (!message_id.is_valid()) {
        LOG(ERROR) << "Failed to find " << m->message_id << " in " << dialog_id << " by date " << date;
        message_id = m->message_id;
      }
      get_dialog_message_by_date_results_[random_id] = {dialog_id, message_id};
      promise.set_value(Unit());
      return;
    }
    // TODO if m == nullptr, we need to just adjust it to the next non-nullptr message, not get from server
  }

  return get_dialog_message_by_date_from_server(d, date, random_id, true, std::move(promise));
}

void MessagesManager::set_dialog_description(DialogId dialog_id, const string &description, Promise<Unit> &&promise) {
  LOG(INFO) << "Receive setChatDescription request to set description of " << dialog_id << " to \"" << description
            << '"';

  if (!have_dialog_force(dialog_id, "set_dialog_description")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't change private chat description"));
    case DialogType::Chat:
      return td_->contacts_manager_->set_chat_description(dialog_id.get_chat_id(), description, std::move(promise));
    case DialogType::Channel:
      return td_->contacts_manager_->set_channel_description(dialog_id.get_channel_id(), description,
                                                             std::move(promise));
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't change secret chat description"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void MessagesManager::do_send_secret_media(DialogId dialog_id, Message *m, FileId file_id, FileId thumbnail_file_id,
                                           tl_object_ptr<telegram_api::InputEncryptedFile> input_encrypted_file,
                                           BufferSlice thumbnail) {
  CHECK(dialog_id.get_type() == DialogType::SecretChat);
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());
  CHECK(m->message_id.is_yet_unsent());

  bool have_input_file = input_encrypted_file != nullptr;
  LOG(INFO) << "Do send secret media file " << file_id << " with thumbnail " << thumbnail_file_id
            << ", have_input_file = " << have_input_file;

  on_secret_message_media_uploaded(
      dialog_id, m,
      get_secret_input_media(m->content.get(), td_, std::move(input_encrypted_file), std::move(thumbnail)), file_id,
      thumbnail_file_id);
}

void MessagesManager::on_load_recommended_dialog_filters(
    Result<Unit> &&result, vector<RecommendedDialogFilter> &&filters,
    Promise<td_api::object_ptr<td_api::recommendedChatFilters>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  if (result.is_error()) {
    return promise.set_error(result.move_as_error());
  }
  CHECK(!td_->auth_manager_->is_bot());

  auto chat_filters = transform(filters, [this](const RecommendedDialogFilter &filter) {
    return td_api::make_object<td_api::recommendedChatFilter>(get_chat_filter_object(filter.dialog_filter.get()),
                                                              filter.description);
  });
  recommended_dialog_filters_ = std::move(filters);
  promise.set_value(td_api::make_object<td_api::recommendedChatFilters>(std::move(chat_filters)));
}

void MessagesManager::attach_message_to_previous(Dialog *d, MessageId message_id, const char *source) {
  CHECK(d != nullptr);
  CHECK(message_id.is_valid());
  MessagesIterator it(d, message_id);
  Message *m = *it;
  CHECK(m != nullptr);
  CHECK(m->message_id == message_id);
  LOG_CHECK(m->have_previous) << d->dialog_id << " " << message_id << " " << source;
  --it;
  LOG_CHECK(*it != nullptr) << d->dialog_id << " " << message_id << " " << source;
  LOG(INFO) << "Attach " << message_id << " to the previous " << (*it)->message_id << " in " << d->dialog_id;
  if ((*it)->have_next) {
    m->have_next = true;
  } else {
    (*it)->have_next = true;
  }
}

void MessagesManager::send_update_chat_action_bar(Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (d->action_bar != nullptr && d->action_bar->is_empty()) {
    d->action_bar = nullptr;
  }

  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_action_bar";
  on_dialog_updated(d->dialog_id, "send_update_chat_action_bar");
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateChatActionBar>(d->dialog_id.get(), get_chat_action_bar_object(d)));

  send_update_secret_chats_with_user_action_bar(d);
}

void MessagesManager::on_update_dialog_online_member_count_timeout_callback(void *messages_manager_ptr,
                                                                            int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager),
                     &MessagesManager::on_update_dialog_online_member_count_timeout, DialogId(dialog_id_int));
}

void MessagesManager::suffix_load_add_query(Dialog *d,
                                            std::pair<Promise<>, std::function<bool(const Message *)>> query) {
  suffix_load_update_first_message_id(d);
  auto *m = get_message_force(d, d->suffix_load_first_message_id_, "suffix_load_add_query");
  if (d->suffix_load_done_ || query.second(m)) {
    query.first.set_value(Unit());
  } else {
    d->suffix_load_queries_.emplace_back(std::move(query));
    suffix_load_loop(d);
  }
}

void MessagesManager::get_message_from_server(FullMessageId full_message_id, Promise<Unit> &&promise,
                                              const char *source,
                                              tl_object_ptr<telegram_api::InputMessage> input_message) {
  get_messages_from_server({full_message_id}, std::move(promise), source, std::move(input_message));
}

bool MessagesManager::is_group_dialog(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::Chat:
      return true;
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_type(dialog_id.get_channel_id()) ==
             ContactsManager::ChannelType::Megagroup;
    default:
      return false;
  }
}

vector<DialogId> MessagesManager::remove_secret_chat_dialog_ids(vector<DialogId> dialog_ids) {
  td::remove_if(dialog_ids, [](DialogId dialog_id) { return dialog_id.get_type() == DialogType::SecretChat; });
  return dialog_ids;
}

void MessagesManager::loop() {
  auto token = get_link_token();
  if (token == YieldType::TtlDb) {
    ttl_db_loop(G()->server_time());
  } else {
    ttl_loop(Time::now());
  }
}
}  // namespace td
