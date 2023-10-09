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
#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileId.hpp"
#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
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



#include <limits>


#include <unordered_map>



namespace td {
}  // namespace td
namespace td {

tl_object_ptr<td_api::message> MessagesManager::get_message_object(DialogId dialog_id, const Message *m,
                                                                   const char *source, bool for_event_log) const {
  if (m == nullptr) {
    return nullptr;
  }
  LOG_CHECK(have_dialog(dialog_id)) << source;

  m->is_update_sent = true;

  auto sending_state = get_message_sending_state_object(m);

  if (for_event_log) {
    CHECK(m->message_id.is_server());
    CHECK(sending_state == nullptr);
  }

  bool can_delete = can_delete_message(dialog_id, m);
  bool is_scheduled = m->message_id.is_scheduled();
  DialogId my_dialog_id = get_my_dialog_id();
  bool can_delete_for_self = false;
  bool can_delete_for_all_users = can_delete && can_revoke_message(dialog_id, m);
  if (can_delete) {
    switch (dialog_id.get_type()) {
      case DialogType::User:
      case DialogType::Chat:
        // TODO allow to delete yet unsent message just for self
        can_delete_for_self = !m->message_id.is_yet_unsent() || dialog_id == my_dialog_id;
        break;
      case DialogType::Channel:
      case DialogType::SecretChat:
        can_delete_for_self = !can_delete_for_all_users;
        break;
      case DialogType::None:
      default:
        UNREACHABLE();
    }
  }
  if (for_event_log) {
    can_delete_for_self = false;
    can_delete_for_all_users = false;
  } else if (is_scheduled) {
    can_delete_for_self = (dialog_id == my_dialog_id);
    can_delete_for_all_users = !can_delete_for_self;
  }

  bool is_outgoing = m->is_outgoing;
  if (dialog_id == my_dialog_id) {
    // in Saved Messages all non-forwarded messages must be outgoing
    // a forwarded message is outgoing, only if it doesn't have from_dialog_id and its sender isn't hidden
    // i.e. a message is incoming only if it's a forwarded message with known from_dialog_id or with a hidden sender
    auto forward_info = m->forward_info.get();
    is_outgoing = is_scheduled || forward_info == nullptr ||
                  (!forward_info->from_dialog_id.is_valid() && !is_forward_info_sender_hidden(forward_info));
  }

  int32 ttl = m->ttl;
  double ttl_expires_in = 0;
  if (!for_event_log) {
    if (m->ttl_expires_at != 0) {
      ttl_expires_in = clamp(m->ttl_expires_at - Time::now(), 1e-3, ttl - 1e-3);
    } else {
      ttl_expires_in = ttl;
    }
    if (ttl == 0 && m->ttl_period != 0) {
      ttl = m->ttl_period;
      ttl_expires_in = clamp(m->date + m->ttl_period - G()->server_time(), 1e-3, ttl - 1e-3);
    }
  } else {
    ttl = 0;
  }
  auto sender = get_message_sender_object_const(td_, m->sender_user_id, m->sender_dialog_id, source);
  auto scheduling_state = is_scheduled ? get_message_scheduling_state_object(m->date) : nullptr;
  auto forward_info = get_message_forward_info_object(m->forward_info);
  auto interaction_info = get_message_interaction_info_object(dialog_id, m);
  auto can_be_saved = can_save_message(dialog_id, m);
  auto can_be_edited = for_event_log ? false : can_edit_message(dialog_id, m, false, td_->auth_manager_->is_bot());
  auto can_be_forwarded = for_event_log ? false : can_forward_message(dialog_id, m) && can_be_saved;
  auto can_get_statistics = for_event_log ? false : can_get_message_statistics(dialog_id, m);
  auto can_get_message_thread = for_event_log ? false : get_top_thread_full_message_id(dialog_id, m).is_ok();
  auto can_get_viewers = for_event_log ? false : can_get_message_viewers(dialog_id, m).is_ok();
  auto can_get_media_timestamp_links = for_event_log ? false : can_get_media_timestamp_link(dialog_id, m).is_ok();
  auto via_bot_user_id = td_->contacts_manager_->get_user_id_object(m->via_bot_user_id, "via_bot_user_id");
  auto media_album_id = for_event_log ? static_cast<int64>(0) : m->media_album_id;
  auto reply_to_message_id = for_event_log ? static_cast<int64>(0) : m->reply_to_message_id.get();
  auto reply_in_dialog_id =
      reply_to_message_id == 0 ? DialogId() : (m->reply_in_dialog_id.is_valid() ? m->reply_in_dialog_id : dialog_id);
  auto top_thread_message_id = for_event_log || is_scheduled ? static_cast<int64>(0) : m->top_thread_message_id.get();
  auto contains_unread_mention = for_event_log ? false : m->contains_unread_mention;
  auto date = is_scheduled ? 0 : m->date;
  auto edit_date = m->hide_edit_date ? 0 : m->edit_date;
  auto is_pinned = is_scheduled ? false : m->is_pinned;
  auto has_timestamped_media = for_event_log || reply_to_message_id == 0 || m->max_own_media_timestamp >= 0;
  auto reply_markup = get_reply_markup_object(m->reply_markup);

  auto live_location_date = m->is_failed_to_send ? 0 : m->date;
  auto skip_bot_commands = for_event_log ? true : need_skip_bot_commands(dialog_id, m);
  auto max_media_timestamp =
      for_event_log ? get_message_own_max_media_timestamp(m) : get_message_max_media_timestamp(m);
  auto content = get_message_content_object(m->content.get(), td_, dialog_id, live_location_date, m->is_content_secret,
                                            skip_bot_commands, max_media_timestamp);
  return make_tl_object<td_api::message>(
      m->message_id.get(), std::move(sender), dialog_id.get(), std::move(sending_state), std::move(scheduling_state),
      is_outgoing, is_pinned, can_be_edited, can_be_forwarded, can_be_saved, can_delete_for_self,
      can_delete_for_all_users, can_get_statistics, can_get_message_thread, can_get_viewers,
      can_get_media_timestamp_links, has_timestamped_media, m->is_channel_post, contains_unread_mention, date,
      edit_date, std::move(forward_info), std::move(interaction_info), reply_in_dialog_id.get(), reply_to_message_id,
      top_thread_message_id, ttl, ttl_expires_in, via_bot_user_id, m->author_signature, media_album_id,
      get_restriction_reason_description(m->restriction_reasons), std::move(content), std::move(reply_markup));
}

void MessagesManager::after_get_channel_difference(DialogId dialog_id, bool success) {
  LOG(INFO) << "After " << (success ? "" : "un") << "successful get channel difference in " << dialog_id;
  LOG_CHECK(!running_get_channel_difference(dialog_id)) << '"' << active_get_channel_differencies_[dialog_id] << '"';

  auto log_event_it = get_channel_difference_to_log_event_id_.find(dialog_id);
  if (log_event_it != get_channel_difference_to_log_event_id_.end()) {
    if (!G()->close_flag()) {
      binlog_erase(G()->td_db()->get_binlog(), log_event_it->second);
    }
    get_channel_difference_to_log_event_id_.erase(log_event_it);
  }

  auto d = get_dialog(dialog_id);
  bool have_access = have_input_peer(dialog_id, AccessRights::Read);
  auto pts = d != nullptr ? d->pts : load_channel_pts(dialog_id);
  auto postponed_updates_it = postponed_channel_updates_.find(dialog_id);
  if (postponed_updates_it != postponed_channel_updates_.end()) {
    auto &updates = postponed_updates_it->second;
    LOG(INFO) << "Begin to apply " << updates.size() << " postponed channel updates";
    while (!updates.empty()) {
      auto it = updates.begin();
      auto update = std::move(it->second.update);
      auto update_pts = it->second.pts;
      auto update_pts_count = it->second.pts_count;
      auto promise = std::move(it->second.promise);
      updates.erase(it);

      auto old_size = updates.size();
      auto update_id = update->get_id();
      if (have_access) {
        add_pending_channel_update(dialog_id, std::move(update), update_pts, update_pts_count, std::move(promise),
                                   "apply postponed channel updates", true);
      } else {
        promise.set_value(Unit());
      }
      if (updates.size() != old_size || running_get_channel_difference(dialog_id)) {
        if (success && update_pts - 10000 < pts && update_pts_count == 1) {
          // if getChannelDifference was successful and update pts is near channel pts,
          // we hope that the update eventually can be applied
          LOG(INFO) << "Can't apply postponed channel updates";
        } else {
          // otherwise protect from getChannelDifference repeating calls by dropping postponed updates
          LOG(WARNING) << "Failed to apply postponed updates of type " << update_id << " in " << dialog_id
                       << " with pts " << pts << ", update pts is " << update_pts << ", update pts count is "
                       << update_pts_count;
          vector<Promise<Unit>> update_promises;
          for (auto &postponed_update : updates) {
            update_promises.push_back(std::move(postponed_update.second.promise));
          }
          updates.clear();
          for (auto &update_promise : update_promises) {
            update_promise.set_value(Unit());
          }
        }
        break;
      }
    }
    if (updates.empty()) {
      postponed_channel_updates_.erase(postponed_updates_it);
    }
    LOG(INFO) << "Finish to apply postponed channel updates";
  }

  if (d != nullptr) {
    d->is_channel_difference_finished = true;

    if (d->message_notification_group.group_id.is_valid()) {
      send_closure_later(G()->notification_manager(), &NotificationManager::after_get_chat_difference,
                         d->message_notification_group.group_id);
    }
    if (d->mention_notification_group.group_id.is_valid()) {
      send_closure_later(G()->notification_manager(), &NotificationManager::after_get_chat_difference,
                         d->mention_notification_group.group_id);
    }
  } else {
    is_channel_difference_finished_.insert(dialog_id);
  }

  if (postponed_chat_read_inbox_updates_.erase(dialog_id) > 0) {
    send_update_chat_read_inbox(d, true, "after_get_channel_difference");
  }

  auto promise_it = run_after_get_channel_difference_.find(dialog_id);
  if (promise_it != run_after_get_channel_difference_.end()) {
    vector<Promise<Unit>> promises = std::move(promise_it->second);
    run_after_get_channel_difference_.erase(promise_it);

    for (auto &promise : promises) {
      promise.set_value(Unit());
    }
  }

  auto it = pending_channel_on_get_dialogs_.find(dialog_id);
  if (it != pending_channel_on_get_dialogs_.end()) {
    LOG(INFO) << "Apply postponed results of channel getDialogs for " << dialog_id;
    PendingOnGetDialogs res = std::move(it->second);
    pending_channel_on_get_dialogs_.erase(it);

    on_get_dialogs(res.folder_id, std::move(res.dialogs), res.total_count, std::move(res.messages),
                   std::move(res.promise));
  }

  if (d != nullptr && !td_->auth_manager_->is_bot() && have_access && !d->last_message_id.is_valid() && !d->is_empty &&
      (d->order != DEFAULT_ORDER || is_dialog_sponsored(d))) {
    get_history_from_the_end_impl(d, true, false, Auto());
  }
}

bool MessagesManager::update_dialog_notification_settings(DialogId dialog_id,
                                                          DialogNotificationSettings *current_settings,
                                                          const DialogNotificationSettings &new_settings) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return false;
  }

  bool need_update_server = current_settings->mute_until != new_settings.mute_until ||
                            current_settings->sound != new_settings.sound ||
                            current_settings->show_preview != new_settings.show_preview ||
                            current_settings->use_default_mute_until != new_settings.use_default_mute_until ||
                            current_settings->use_default_sound != new_settings.use_default_sound ||
                            current_settings->use_default_show_preview != new_settings.use_default_show_preview;
  bool need_update_local =
      current_settings->use_default_disable_pinned_message_notifications !=
          new_settings.use_default_disable_pinned_message_notifications ||
      current_settings->disable_pinned_message_notifications != new_settings.disable_pinned_message_notifications ||
      current_settings->use_default_disable_mention_notifications !=
          new_settings.use_default_disable_mention_notifications ||
      current_settings->disable_mention_notifications != new_settings.disable_mention_notifications;

  bool is_changed = need_update_server || need_update_local ||
                    current_settings->is_synchronized != new_settings.is_synchronized ||
                    current_settings->is_use_default_fixed != new_settings.is_use_default_fixed;

  if (is_changed) {
    Dialog *d = get_dialog(dialog_id);
    LOG_CHECK(d != nullptr) << "Wrong " << dialog_id << " in update_dialog_notification_settings";
    bool was_dialog_mentions_disabled = is_dialog_mention_notifications_disabled(d);

    VLOG(notifications) << "Update notification settings in " << dialog_id << " from " << *current_settings << " to "
                        << new_settings;

    update_dialog_unmute_timeout(d, current_settings->use_default_mute_until, current_settings->mute_until,
                                 new_settings.use_default_mute_until, new_settings.mute_until);

    *current_settings = new_settings;
    on_dialog_updated(dialog_id, "update_dialog_notification_settings");

    if (is_dialog_muted(d)) {
      // no check for was_muted to clean pending message notifications in chats with unsynchronized settings
      remove_all_dialog_notifications(d, false, "update_dialog_notification_settings 2");
    }
    if (is_dialog_pinned_message_notifications_disabled(d) && d->mention_notification_group.group_id.is_valid() &&
        d->pinned_message_notification_message_id.is_valid()) {
      remove_dialog_pinned_message_notification(d, "update_dialog_notification_settings 3");
    }
    if (was_dialog_mentions_disabled != is_dialog_mention_notifications_disabled(d)) {
      if (was_dialog_mentions_disabled) {
        update_dialog_mention_notification_count(d);
      } else {
        remove_dialog_mention_notifications(d);
      }
    }

    if (need_update_server || need_update_local) {
      send_closure(G()->td(), &Td::send_update,
                   make_tl_object<td_api::updateChatNotificationSettings>(
                       dialog_id.get(), get_chat_notification_settings_object(current_settings)));
    }
  }
  return need_update_server;
}

void MessagesManager::on_get_scheduled_server_messages(DialogId dialog_id, uint32 generation,
                                                       vector<tl_object_ptr<telegram_api::Message>> &&messages,
                                                       bool is_not_modified) {
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  if (generation < d->scheduled_messages_sync_generation) {
    LOG(INFO) << "Ignore scheduled messages with old generation " << generation << " instead of "
              << d->scheduled_messages_sync_generation << " in " << dialog_id;
    return;
  }
  d->scheduled_messages_sync_generation = generation;

  if (is_not_modified) {
    LOG(INFO) << "Scheduled messages are mot modified in " << dialog_id;
    return;
  }

  vector<MessageId> old_message_ids;
  find_old_messages(d->scheduled_messages.get(),
                    MessageId(ScheduledServerMessageId(), std::numeric_limits<int32>::max(), true), old_message_ids);
  std::unordered_map<ScheduledServerMessageId, MessageId, ScheduledServerMessageIdHash> old_server_message_ids;
  for (auto &message_id : old_message_ids) {
    if (message_id.is_scheduled_server()) {
      old_server_message_ids[message_id.get_scheduled_server_message_id()] = message_id;
    }
  }

  bool is_channel_message = dialog_id.get_type() == DialogType::Channel;
  bool has_scheduled_server_messages = false;
  for (auto &message : messages) {
    auto message_dialog_id = get_message_dialog_id(message);
    if (message_dialog_id != dialog_id) {
      LOG(ERROR) << "Receive " << get_message_id(message, true) << " in wrong " << message_dialog_id << " instead of "
                 << dialog_id << ": " << oneline(to_string(message));
      continue;
    }

    auto full_message_id = on_get_message(std::move(message), d->sent_scheduled_messages, is_channel_message, true,
                                          false, false, "on_get_scheduled_server_messages");
    auto message_id = full_message_id.get_message_id();
    if (message_id.is_valid_scheduled()) {
      CHECK(message_id.is_scheduled_server());
      old_server_message_ids.erase(message_id.get_scheduled_server_message_id());
      has_scheduled_server_messages = true;
    }
  }
  on_update_dialog_has_scheduled_server_messages(dialog_id, has_scheduled_server_messages);

  for (const auto &it : old_server_message_ids) {
    auto message_id = it.second;
    auto message = do_delete_scheduled_message(d, message_id, true, "on_get_scheduled_server_messages");
    CHECK(message != nullptr);
    send_update_delete_messages(dialog_id, {message->message_id.get()}, true, false);
  }

  send_update_chat_has_scheduled_messages(d, false);
}

void MessagesManager::on_create_new_dialog_success(int64 random_id, tl_object_ptr<telegram_api::Updates> &&updates,
                                                   DialogType expected_type, Promise<Unit> &&promise) {
  auto sent_messages = UpdatesManager::get_new_messages(updates.get());
  auto sent_messages_random_ids = UpdatesManager::get_sent_messages_random_ids(updates.get());
  if (sent_messages.size() != 1u || sent_messages_random_ids.size() != 1u) {
    LOG(ERROR) << "Receive wrong result for create group or channel chat " << oneline(to_string(updates));
    return on_create_new_dialog_fail(random_id, Status::Error(500, "Unsupported server response"), std::move(promise));
  }

  auto message = *sent_messages.begin();
  // int64 message_random_id = *sent_messages_random_ids.begin();
  // TODO check that message_random_id equals random_id after messages_createChat will be updated

  auto dialog_id = get_message_dialog_id(*message);
  if (dialog_id.get_type() != expected_type) {
    return on_create_new_dialog_fail(random_id, Status::Error(500, "Chat of wrong type has been created"),
                                     std::move(promise));
  }

  auto it = created_dialogs_.find(random_id);
  CHECK(it != created_dialogs_.end());
  CHECK(it->second == DialogId());

  it->second = dialog_id;

  const Dialog *d = get_dialog(dialog_id);
  if (d != nullptr && d->last_new_message_id.is_valid()) {
    // dialog have been already created and at least one non-temporary message was added,
    // i.e. we are not interested in the creation of dialog by searchMessages
    // then messages have already been added, so just set promise
    return promise.set_value(Unit());
  }

  if (pending_created_dialogs_.find(dialog_id) == pending_created_dialogs_.end()) {
    pending_created_dialogs_.emplace(dialog_id, std::move(promise));
  } else {
    LOG(ERROR) << dialog_id << " returned twice as result of chat creation";
    return on_create_new_dialog_fail(random_id, Status::Error(500, "Chat was created earlier"), std::move(promise));
  }

  td_->updates_manager_->on_get_updates(std::move(updates), Promise<Unit>());
}

void MessagesManager::on_dialog_user_is_deleted_updated(DialogId dialog_id, bool is_deleted) {
  CHECK(dialog_id.get_type() == DialogType::User);
  auto d = get_dialog(dialog_id);  // called from update_user, must not create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    if (d->know_action_bar) {
      if (is_deleted) {
        if (d->action_bar != nullptr && d->action_bar->on_user_deleted()) {
          send_update_chat_action_bar(d);
        }
      } else {
        repair_dialog_action_bar(d, "on_dialog_user_is_deleted_updated");
      }
    }

    if (!dialog_filters_.empty() && d->order != DEFAULT_ORDER) {
      update_dialog_lists(d, get_dialog_positions(d), true, false, "on_dialog_user_is_deleted_updated");
      td_->contacts_manager_->for_each_secret_chat_with_user(
          dialog_id.get_user_id(), [this](SecretChatId secret_chat_id) {
            DialogId dialog_id(secret_chat_id);
            auto d = get_dialog(dialog_id);  // must not create the dialog
            if (d != nullptr && d->is_update_new_chat_sent && d->order != DEFAULT_ORDER) {
              update_dialog_lists(d, get_dialog_positions(d), true, false, "on_dialog_user_is_deleted_updated");
            }
          });
    }

    if (is_deleted && d->has_bots) {
      set_dialog_has_bots(d, false);
      td_->contacts_manager_->for_each_secret_chat_with_user(
          dialog_id.get_user_id(), [this](SecretChatId secret_chat_id) {
            DialogId dialog_id(secret_chat_id);
            auto d = get_dialog(dialog_id);  // must not create the dialog
            if (d != nullptr && d->is_update_new_chat_sent && d->has_bots) {
              set_dialog_has_bots(d, false);
            }
          });
    }
  }
}

void MessagesManager::on_get_dialog_message_count(DialogId dialog_id, MessageSearchFilter filter, int32 total_count,
                                                  Promise<int32> &&promise) {
  LOG(INFO) << "Receive " << total_count << " message count in " << dialog_id << " with filter " << filter;
  if (total_count < 0) {
    LOG(ERROR) << "Receive total message count = " << total_count << " in " << dialog_id << " with filter " << filter;
    total_count = 0;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  CHECK(filter != MessageSearchFilter::Empty);
  CHECK(filter != MessageSearchFilter::UnreadMention);
  CHECK(filter != MessageSearchFilter::FailedToSend);

  auto &old_message_count = d->message_count_by_index[message_search_filter_index(filter)];
  if (old_message_count != total_count) {
    old_message_count = total_count;
    on_dialog_updated(dialog_id, "on_get_dialog_message_count");
  }

  if (total_count == 0) {
    auto &old_first_database_message_id = d->first_database_message_id_by_index[message_search_filter_index(filter)];
    if (old_first_database_message_id != MessageId::min()) {
      old_first_database_message_id = MessageId::min();
      on_dialog_updated(dialog_id, "on_get_dialog_message_count");
    }
    if (filter == MessageSearchFilter::Pinned) {
      set_dialog_last_pinned_message_id(d, MessageId());
    }
  }
  promise.set_value(std::move(total_count));
}

class ClearAllDraftsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit ClearAllDraftsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::messages_clearAllDrafts()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_clearAllDrafts>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG(INFO) << "Receive result for ClearAllDraftsQuery: " << result_ptr.ok();
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for ClearAllDraftsQuery: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::clear_all_draft_messages(bool exclude_secret_chats, Promise<Unit> &&promise) {
  if (!exclude_secret_chats) {
    for (auto &dialog : dialogs_) {
      Dialog *d = dialog.second.get();
      if (d->dialog_id.get_type() == DialogType::SecretChat) {
        update_dialog_draft_message(d, nullptr, false, true);
      }
    }
  }
  td_->create_handler<ClearAllDraftsQuery>(std::move(promise))->send();
}

bool MessagesManager::get_messages(DialogId dialog_id, const vector<MessageId> &message_ids, Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "get_messages");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return false;
  }

  bool is_secret = dialog_id.get_type() == DialogType::SecretChat;
  vector<FullMessageId> missed_message_ids;
  for (auto message_id : message_ids) {
    if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
      promise.set_error(Status::Error(400, "Invalid message identifier"));
      return false;
    }

    auto *m = get_message_force(d, message_id, "get_messages");
    if (m == nullptr && message_id.is_any_server() && !is_secret) {
      missed_message_ids.emplace_back(dialog_id, message_id);
      continue;
    }
  }

  if (!missed_message_ids.empty()) {
    get_messages_from_server(std::move(missed_message_ids), std::move(promise), "get_messages");
    return false;
  }

  promise.set_value(Unit());
  return true;
}

Status MessagesManager::set_dialog_notification_settings(
    DialogId dialog_id, tl_object_ptr<td_api::chatNotificationSettings> &&notification_settings) {
  CHECK(!td_->auth_manager_->is_bot());
  auto current_settings = get_dialog_notification_settings(dialog_id, false);
  if (current_settings == nullptr) {
    return Status::Error(400, "Wrong chat identifier specified");
  }
  if (dialog_id == get_my_dialog_id()) {
    return Status::Error(400, "Notification settings of the Saved Messages chat can't be changed");
  }

  TRY_RESULT(new_settings, ::td::get_dialog_notification_settings(std::move(notification_settings),
                                                                  current_settings->silent_send_message));
  if (update_dialog_notification_settings(dialog_id, current_settings, new_settings)) {
    update_dialog_notification_settings_on_server(dialog_id, false);
  }
  return Status::OK();
}

tl_object_ptr<telegram_api::InputPeer> MessagesManager::get_input_peer_force(DialogId dialog_id) {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      UserId user_id = dialog_id.get_user_id();
      return make_tl_object<telegram_api::inputPeerUser>(user_id.get(), 0);
    }
    case DialogType::Chat: {
      ChatId chat_id = dialog_id.get_chat_id();
      return make_tl_object<telegram_api::inputPeerChat>(chat_id.get());
    }
    case DialogType::Channel: {
      ChannelId channel_id = dialog_id.get_channel_id();
      return make_tl_object<telegram_api::inputPeerChannel>(channel_id.get(), 0);
    }
    case DialogType::SecretChat:
    case DialogType::None:
      return make_tl_object<telegram_api::inputPeerEmpty>();
    default:
      UNREACHABLE();
      return nullptr;
  }
}

bool MessagesManager::need_delete_file(FullMessageId full_message_id, FileId file_id) const {
  if (being_readded_message_id_ == full_message_id) {
    return false;
  }

  auto main_file_id = td_->file_manager_->get_file_view(file_id).file_id();
  auto full_message_ids = td_->file_reference_manager_->get_some_message_file_sources(main_file_id);
  LOG(INFO) << "Receive " << full_message_ids << " as sources for file " << main_file_id << "/" << file_id << " from "
            << full_message_id;
  for (const auto &other_full_messsage_id : full_message_ids) {
    if (other_full_messsage_id != full_message_id) {
      return false;
    }
  }

  return true;
}

void MessagesManager::register_message_reply(const Dialog *d, const Message *m) {
  if (!m->reply_to_message_id.is_valid() || td_->auth_manager_->is_bot()) {
    return;
  }

  if (has_media_timestamps(get_message_content_text(m->content.get()), 0, std::numeric_limits<int32>::max())) {
    LOG(INFO) << "Register " << m->message_id << " in " << d->dialog_id << " as reply to " << m->reply_to_message_id;
    FullMessageId full_message_id{d->dialog_id, m->reply_to_message_id};
    bool is_inserted = replied_by_media_timestamp_messages_[full_message_id].insert(m->message_id).second;
    CHECK(is_inserted);
  }
}

void MessagesManager::suffix_load_update_first_message_id(Dialog *d) {
  if (!d->suffix_load_first_message_id_.is_valid()) {
    if (!d->last_message_id.is_valid()) {
      return;
    }

    d->suffix_load_first_message_id_ = d->last_message_id;
  }
  auto it = MessagesConstIterator(d, d->suffix_load_first_message_id_);
  CHECK(*it != nullptr);
  CHECK((*it)->message_id == d->suffix_load_first_message_id_);
  while ((*it)->have_previous) {
    --it;
  }
  d->suffix_load_first_message_id_ = (*it)->message_id;
}

void MessagesManager::send_update_message_content(DialogId dialog_id, Message *m, bool is_message_in_dialog,
                                                  const char *source) {
  Dialog *d = get_dialog(dialog_id);
  LOG_CHECK(d != nullptr) << "Send updateMessageContent in unknown " << dialog_id << " from " << source
                          << " with load count " << loaded_dialogs_.count(dialog_id);
  send_update_message_content(d, m, is_message_in_dialog, source);
}

vector<DialogId> MessagesManager::get_peers_dialog_ids(vector<tl_object_ptr<telegram_api::Peer>> &&peers) {
  vector<DialogId> result;
  result.reserve(peers.size());
  for (auto &peer : peers) {
    DialogId dialog_id(peer);
    if (dialog_id.is_valid()) {
      force_create_dialog(dialog_id, "get_peers_dialog_ids");
      result.push_back(dialog_id);
    }
  }
  return result;
}

void MessagesManager::on_update_message_forward_count(FullMessageId full_message_id, int32 forward_count) {
  if (forward_count < 0) {
    LOG(ERROR) << "Receive " << forward_count << " forwards in updateChannelMessageForwards for " << full_message_id;
    return;
  }
  update_message_interaction_info(full_message_id, -1, forward_count, false, nullptr);
}

bool MessagesManager::can_save_message(DialogId dialog_id, const Message *m) const {
  if (m == nullptr || m->noforwards || m->is_content_secret) {
    return false;
  }
  return !get_dialog_has_protected_content(dialog_id);
}

int32 MessagesManager::get_message_max_media_timestamp(const Message *m) {
  return m->max_own_media_timestamp >= 0 ? m->max_own_media_timestamp : m->max_reply_media_timestamp;
}
}  // namespace td
