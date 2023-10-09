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

void MessagesManager::add_pending_channel_update(DialogId dialog_id, tl_object_ptr<telegram_api::Update> &&update,
                                                 int32 new_pts, int32 pts_count, Promise<Unit> &&promise,
                                                 const char *source, bool is_postponed_update) {
  LOG(INFO) << "Receive from " << source << " pending " << to_string(update);
  CHECK(update != nullptr);
  if (dialog_id.get_type() != DialogType::Channel) {
    LOG(ERROR) << "Receive channel update in invalid " << dialog_id << " from " << source << ": "
               << oneline(to_string(update));
    promise.set_value(Unit());
    return;
  }
  if (pts_count < 0 || new_pts <= pts_count) {
    LOG(ERROR) << "Receive channel update from " << source << " with wrong pts = " << new_pts
               << " or pts_count = " << pts_count << ": " << oneline(to_string(update));
    promise.set_value(Unit());
    return;
  }

  auto channel_id = dialog_id.get_channel_id();
  if (!td_->contacts_manager_->have_channel(channel_id) && td_->contacts_manager_->have_min_channel(channel_id)) {
    td_->updates_manager_->schedule_get_difference("add_pending_channel_update 1");
    promise.set_value(Unit());
    return;
  }

  // TODO need to save all updates that can change result of running queries not associated with pts (for example
  // getHistory) and apply them to result of these queries

  Dialog *d = get_dialog_force(dialog_id, "add_pending_channel_update 2");
  if (d == nullptr) {
    auto pts = load_channel_pts(dialog_id);
    if (pts > 0) {
      if (!td_->contacts_manager_->have_channel(channel_id)) {
        // do not create dialog if there is no info about the channel
        LOG(INFO) << "There is no info about " << channel_id << ", so ignore " << oneline(to_string(update));
        promise.set_value(Unit());
        return;
      }

      if (new_pts <= pts && new_pts >= pts - 19999) {
        LOG(INFO) << "There is no need to process an update with pts " << new_pts << " in " << dialog_id << " with pts "
                  << pts;
        promise.set_value(Unit());
        return;
      }

      if (new_pts > pts && pts != new_pts - pts_count) {
        LOG(INFO) << "Found a gap in unknown " << dialog_id << " with pts = " << pts << ". new_pts = " << new_pts
                  << ", pts_count = " << pts_count << " in update from " << source;
        add_postponed_channel_update(dialog_id, std::move(update), new_pts, pts_count, std::move(promise));
        get_channel_difference(dialog_id, pts, true, "add_pending_channel_update 3");
        return;
      }

      d = add_dialog(dialog_id, "add_pending_channel_update 4");
      CHECK(d != nullptr);
      CHECK(d->pts == pts);
      update_dialog_pos(d, "add_pending_channel_update 5");
    }
  }
  if (d == nullptr) {
    // if there is no dialog, it can be created by the update
    LOG(INFO) << "Receive pending update from " << source << " about unknown " << dialog_id;
    if (running_get_channel_difference(dialog_id)) {
      add_postponed_channel_update(dialog_id, std::move(update), new_pts, pts_count, std::move(promise));
      return;
    }
  } else {
    int32 old_pts = d->pts;
    if (new_pts <= old_pts) {  // very old or unuseful update
      if (new_pts < old_pts - 19999 && !is_postponed_update) {
        // restore channel pts after delete_first_messages
        auto now = Time::now();
        if (now > last_channel_pts_jump_warning_time_ + 1) {
          LOG(ERROR) << "Restore pts in " << d->dialog_id << " from " << source << " after delete_first_messages from "
                     << old_pts << " to " << new_pts << " is temporarily disabled, pts_count = " << pts_count
                     << ", update is from " << source << ": " << oneline(to_string(update));
          last_channel_pts_jump_warning_time_ = now;
        }
        get_channel_difference(dialog_id, old_pts, true, "add_pending_channel_update old");
      }

      if (update->get_id() == telegram_api::updateNewChannelMessage::ID) {
        auto update_new_channel_message = static_cast<telegram_api::updateNewChannelMessage *>(update.get());
        auto message_id = get_message_id(update_new_channel_message->message_, false);
        FullMessageId full_message_id(dialog_id, message_id);
        if (update_message_ids_.find(full_message_id) != update_message_ids_.end()) {
          // apply sent channel message
          on_get_message(std::move(update_new_channel_message->message_), true, true, false, true, true,
                         "updateNewChannelMessage with an awaited message");
          promise.set_value(Unit());
          return;
        }
      }
      if (update->get_id() == updateSentMessage::ID) {
        auto update_sent_message = static_cast<updateSentMessage *>(update.get());
        if (being_sent_messages_.count(update_sent_message->random_id_) > 0) {
          // apply sent channel message
          on_send_message_success(update_sent_message->random_id_, update_sent_message->message_id_,
                                  update_sent_message->date_, update_sent_message->ttl_period_, FileId(),
                                  "process old updateSentChannelMessage");
          promise.set_value(Unit());
          return;
        }
      }

      LOG_IF(WARNING, new_pts == old_pts && pts_count == 0)
          << "Receive from " << source << " useless channel update " << oneline(to_string(update));
      LOG(INFO) << "Skip already applied channel update";
      promise.set_value(Unit());
      return;
    }

    if (running_get_channel_difference(dialog_id)) {
      LOG(INFO) << "Postpone channel update, because getChannelDifference is run";
      add_postponed_channel_update(dialog_id, std::move(update), new_pts, pts_count, std::move(promise));
      return;
    }

    if (old_pts != new_pts - pts_count) {
      LOG(INFO) << "Found a gap in the " << dialog_id << " with pts = " << old_pts << ". new_pts = " << new_pts
                << ", pts_count = " << pts_count << " in update from " << source;
      if (d->was_opened || td_->contacts_manager_->get_channel_status(channel_id).is_member() ||
          is_dialog_sponsored(d)) {
        add_postponed_channel_update(dialog_id, std::move(update), new_pts, pts_count, std::move(promise));
        get_channel_difference(dialog_id, old_pts, true, "add_pending_channel_update pts mismatch");
      } else {
        promise.set_value(Unit());
      }
      return;
    }
  }

  if (d == nullptr || pts_count > 0) {
    process_channel_update(std::move(update));
    LOG_CHECK(!running_get_channel_difference(dialog_id)) << '"' << active_get_channel_differencies_[dialog_id] << '"';
  } else {
    LOG_IF(INFO, update->get_id() != dummyUpdate::ID)
        << "Skip useless channel update from " << source << ": " << to_string(update);
  }

  if (d == nullptr) {
    d = get_dialog(dialog_id);
    if (d == nullptr) {
      LOG(INFO) << "Update didn't created " << dialog_id;
      promise.set_value(Unit());
      return;
    }
  }

  CHECK(new_pts > d->pts);
  set_channel_pts(d, new_pts, source);
  promise.set_value(Unit());
}

void MessagesManager::recalc_unread_count(DialogListId dialog_list_id, int32 old_dialog_total_count, bool force) {
  if (G()->close_flag() || td_->auth_manager_->is_bot() || !G()->parameters().use_message_db) {
    return;
  }

  auto *list_ptr = get_dialog_list(dialog_list_id);
  CHECK(list_ptr != nullptr);
  auto &list = *list_ptr;
  if (!list.need_unread_count_recalc_ && !force) {
    return;
  }
  LOG(INFO) << "Recalculate unread counts in " << dialog_list_id;
  list.need_unread_count_recalc_ = false;
  list.is_message_unread_count_inited_ = true;
  list.is_dialog_unread_count_inited_ = true;

  int32 message_total_count = 0;
  int32 message_muted_count = 0;
  int32 dialog_total_count = 0;
  int32 dialog_muted_count = 0;
  int32 dialog_marked_count = 0;
  int32 dialog_muted_marked_count = 0;
  int32 server_dialog_total_count = 0;
  int32 secret_chat_total_count = 0;
  for (auto folder_id : get_dialog_list_folder_ids(list)) {
    const auto &folder = *get_dialog_folder(folder_id);
    for (const auto &dialog_date : folder.ordered_dialogs_) {
      if (dialog_date.get_order() == DEFAULT_ORDER) {
        break;
      }

      auto dialog_id = dialog_date.get_dialog_id();
      Dialog *d = get_dialog(dialog_id);
      CHECK(d != nullptr);
      if (!is_dialog_in_list(d, dialog_list_id)) {
        continue;
      }

      int unread_count = d->server_unread_count + d->local_unread_count;
      if (need_unread_counter(d->order) && (unread_count > 0 || d->is_marked_as_unread)) {
        message_total_count += unread_count;
        dialog_total_count++;
        if (unread_count == 0 && d->is_marked_as_unread) {
          dialog_marked_count++;
        }

        LOG(DEBUG) << "Have " << unread_count << " messages in " << dialog_id;
        if (is_dialog_muted(d)) {
          message_muted_count += unread_count;
          dialog_muted_count++;
          if (unread_count == 0 && d->is_marked_as_unread) {
            dialog_muted_marked_count++;
          }
        }
      }
      if (d->order != DEFAULT_ORDER) {  // must not count sponsored dialog, which is added independently
        if (dialog_id.get_type() == DialogType::SecretChat) {
          secret_chat_total_count++;
        } else {
          server_dialog_total_count++;
        }
      }
    }
  }

  if (list.unread_message_total_count_ != message_total_count ||
      list.unread_message_muted_count_ != message_muted_count) {
    list.unread_message_total_count_ = message_total_count;
    list.unread_message_muted_count_ = message_muted_count;
    send_update_unread_message_count(list, DialogId(), true, "recalc_unread_count");
  }

  if (old_dialog_total_count == -1) {
    old_dialog_total_count = get_dialog_total_count(list);
  }
  bool need_save = false;
  if (list.list_last_dialog_date_ == MAX_DIALOG_DATE) {
    if (server_dialog_total_count != list.server_dialog_total_count_ ||
        secret_chat_total_count != list.secret_chat_total_count_) {
      list.server_dialog_total_count_ = server_dialog_total_count;
      list.secret_chat_total_count_ = secret_chat_total_count;
      need_save = true;
    }
  } else {
    if (list.server_dialog_total_count_ == -1) {
      // recalc_unread_count is called only after getDialogs request; it is unneeded to call getDialogs again
      repair_server_dialog_total_count(dialog_list_id);
    }

    if (list.secret_chat_total_count_ == -1) {
      repair_secret_chat_total_count(dialog_list_id);
    }
  }
  if (list.unread_dialog_total_count_ != dialog_total_count || list.unread_dialog_muted_count_ != dialog_muted_count ||
      list.unread_dialog_marked_count_ != dialog_marked_count ||
      list.unread_dialog_muted_marked_count_ != dialog_muted_marked_count ||
      old_dialog_total_count != get_dialog_total_count(list)) {
    list.unread_dialog_total_count_ = dialog_total_count;
    list.unread_dialog_muted_count_ = dialog_muted_count;
    list.unread_dialog_marked_count_ = dialog_marked_count;
    list.unread_dialog_muted_marked_count_ = dialog_muted_marked_count;
    send_update_unread_chat_count(list, DialogId(), true, "recalc_unread_count");
  } else if (need_save) {
    save_unread_chat_count(list);
  }
}

void MessagesManager::on_search_dialog_messages_db_result(int64 random_id, DialogId dialog_id,
                                                          MessageId from_message_id, MessageId first_db_message_id,
                                                          MessageSearchFilter filter, int32 offset, int32 limit,
                                                          Result<vector<MessagesDbDialogMessage>> r_messages,
                                                          Promise<Unit> promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (r_messages.is_error()) {
    LOG(ERROR) << "Failed to get messages from the database: " << r_messages.error();
    if (first_db_message_id != MessageId::min() && dialog_id.get_type() != DialogType::SecretChat &&
        filter != MessageSearchFilter::FailedToSend) {
      found_dialog_messages_.erase(random_id);
    }
    return promise.set_value(Unit());
  }
  CHECK(!from_message_id.is_scheduled());
  CHECK(!first_db_message_id.is_scheduled());

  auto messages = r_messages.move_as_ok();

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  auto it = found_dialog_messages_.find(random_id);
  CHECK(it != found_dialog_messages_.end());
  auto &res = it->second.second;

  res.reserve(messages.size());
  for (auto &message : messages) {
    auto m = on_get_message_from_database(d, message, false, "on_search_dialog_messages_db_result");
    if (m != nullptr && first_db_message_id <= m->message_id) {
      if (filter == MessageSearchFilter::UnreadMention && !m->contains_unread_mention) {
        // skip already read by d->last_read_all_mentions_message_id mentions
      } else {
        CHECK(!m->message_id.is_scheduled());
        res.push_back(m->message_id);
      }
    }
  }

  auto &message_count = d->message_count_by_index[message_search_filter_index(filter)];
  auto result_size = narrow_cast<int32>(res.size());
  bool from_the_end =
      from_message_id == MessageId::max() || (offset < 0 && (result_size == 0 || res[0] < from_message_id));
  if ((message_count != -1 && message_count < result_size) ||
      (message_count > result_size && from_the_end && first_db_message_id == MessageId::min() &&
       result_size < limit + offset)) {
    LOG(INFO) << "Fix found message count in " << dialog_id << " from " << message_count << " to " << result_size;
    message_count = result_size;
    if (filter == MessageSearchFilter::UnreadMention) {
      d->unread_mention_count = message_count;
      update_dialog_mention_notification_count(d);
      send_update_chat_unread_mention_count(d);
    }
    on_dialog_updated(dialog_id, "on_search_dialog_messages_db_result");
  }
  it->second.first = message_count;
  if (res.empty() && first_db_message_id != MessageId::min() && dialog_id.get_type() != DialogType::SecretChat) {
    LOG(INFO) << "No messages found in database";
    found_dialog_messages_.erase(it);
  } else {
    LOG(INFO) << "Found " << res.size() << " messages out of " << message_count << " in database";
    if (from_the_end && filter == MessageSearchFilter::Pinned) {
      set_dialog_last_pinned_message_id(d, res.empty() ? MessageId() : res[0]);
    }
  }
  promise.set_value(Unit());
}

void MessagesManager::on_get_message_search_result_calendar(
    DialogId dialog_id, MessageId from_message_id, MessageSearchFilter filter, int64 random_id, int32 total_count,
    vector<tl_object_ptr<telegram_api::Message>> &&messages,
    vector<tl_object_ptr<telegram_api::searchResultsCalendarPeriod>> &&periods, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto it = found_dialog_message_calendars_.find(random_id);
  CHECK(it != found_dialog_message_calendars_.end());

  int32 received_message_count = 0;
  for (auto &message : messages) {
    auto new_full_message_id = on_get_message(std::move(message), false, dialog_id.get_type() == DialogType::Channel,
                                              false, false, false, "on_get_message_search_result_calendar");
    if (new_full_message_id == FullMessageId()) {
      total_count--;
      continue;
    }

    if (new_full_message_id.get_dialog_id() != dialog_id) {
      LOG(ERROR) << "Receive " << new_full_message_id << " instead of a message in " << dialog_id;
      total_count--;
      continue;
    }

    received_message_count++;
  }
  if (total_count < received_message_count) {
    LOG(ERROR) << "Receive " << received_message_count << " valid messages out of " << total_count << " in "
               << messages.size() << " messages";
    total_count = received_message_count;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  auto &old_message_count = d->message_count_by_index[message_search_filter_index(filter)];
  if (old_message_count != total_count) {
    old_message_count = total_count;
    on_dialog_updated(dialog_id, "on_get_message_search_result_calendar");
  }

  vector<td_api::object_ptr<td_api::messageCalendarDay>> days;
  for (auto &period : periods) {
    auto message_id = MessageId(ServerMessageId(period->min_msg_id_));
    const auto *m = get_message(d, message_id);
    if (m == nullptr) {
      LOG(ERROR) << "Failed to find " << message_id;
      continue;
    }
    if (period->count_ <= 0) {
      LOG(ERROR) << "Receive " << to_string(period);
      continue;
    }
    days.push_back(td_api::make_object<td_api::messageCalendarDay>(
        period->count_, get_message_object(dialog_id, m, "on_get_message_search_result_calendar")));
  }
  it->second = td_api::make_object<td_api::messageCalendar>(total_count, std::move(days));
  promise.set_value(Unit());
}

void MessagesManager::try_add_pinned_message_notification(Dialog *d, vector<Notification> &res,
                                                          NotificationId max_notification_id, int32 limit) {
  CHECK(d != nullptr);
  auto message_id = d->pinned_message_notification_message_id;
  if (!message_id.is_valid() || message_id > d->last_new_message_id) {
    CHECK(!message_id.is_scheduled());
    return;
  }

  auto m = get_message_force(d, message_id, "try_add_pinned_message_notification");
  if (m != nullptr && m->notification_id.get() > d->mention_notification_group.max_removed_notification_id.get() &&
      m->message_id > d->mention_notification_group.max_removed_message_id &&
      m->message_id > d->last_read_inbox_message_id && !is_dialog_pinned_message_notifications_disabled(d)) {
    if (m->notification_id.get() < max_notification_id.get()) {
      VLOG(notifications) << "Add " << m->notification_id << " about pinned " << message_id << " in " << d->dialog_id;
      auto pinned_message_id = get_message_content_pinned_message_id(m->content.get());
      if (pinned_message_id.is_valid()) {
        get_message_force(d, pinned_message_id, "try_add_pinned_message_notification 2");  // preload pinned message
      }

      auto pos = res.size();
      res.emplace_back(m->notification_id, m->date, m->disable_notification,
                       create_new_message_notification(message_id));
      while (pos > 0 && res[pos - 1].type->get_message_id() < message_id) {
        std::swap(res[pos - 1], res[pos]);
        pos--;
      }
      if (pos > 0 && res[pos - 1].type->get_message_id() == message_id) {
        res.erase(res.begin() + pos);  // notification was already there
      }
      if (res.size() > static_cast<size_t>(limit)) {
        res.pop_back();
        CHECK(res.size() == static_cast<size_t>(limit));
      }
    }
  } else {
    remove_dialog_pinned_message_notification(d, "try_add_pinned_message_notification");
  }
}

class ReadMentionsQuery final : public Td::ResultHandler {
  Promise<AffectedHistory> promise_;
  DialogId dialog_id_;

 public:
  explicit ReadMentionsQuery(Promise<AffectedHistory> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id_, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Chat is not accessible"));
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_readMentions(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_readMentions>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(AffectedHistory(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ReadMentionsQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::read_all_dialog_mentions_on_server(DialogId dialog_id, uint64 log_event_id,
                                                         Promise<Unit> &&promise) {
  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_read_all_dialog_mentions_on_server_log_event(dialog_id);
  }

  AffectedHistoryQuery query = [td = td_](DialogId dialog_id, Promise<AffectedHistory> &&query_promise) {
    td->create_handler<ReadMentionsQuery>(std::move(query_promise))->send(dialog_id);
  };
  run_affected_history_query_until_complete(dialog_id, std::move(query), false,
                                            get_erase_log_event_promise(log_event_id, std::move(promise)));
}

void MessagesManager::on_message_live_location_viewed(Dialog *d, const Message *m) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->content->get_type() == MessageContentType::LiveLocation);
  CHECK(!m->message_id.is_scheduled());

  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  switch (d->dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      // ok
      break;
    case DialogType::SecretChat:
      return;
    default:
      UNREACHABLE();
      return;
  }
  if (!d->is_opened) {
    return;
  }

  if (m->is_outgoing || !m->message_id.is_server() || m->via_bot_user_id.is_valid() || !m->sender_user_id.is_valid() ||
      td_->contacts_manager_->is_user_bot(m->sender_user_id) || m->forward_info != nullptr) {
    return;
  }

  auto live_period = get_message_content_live_location_period(m->content.get());
  if (live_period <= G()->unix_time() - m->date + 1) {
    // live location is expired
    return;
  }

  auto &live_location_task_id = d->pending_viewed_live_locations[m->message_id];
  if (live_location_task_id != 0) {
    return;
  }

  live_location_task_id = ++viewed_live_location_task_id_;
  auto &full_message_id = viewed_live_location_tasks_[live_location_task_id];
  full_message_id = FullMessageId(d->dialog_id, m->message_id);
  view_message_live_location_on_server_impl(live_location_task_id, full_message_id);
}

DialogId MessagesManager::migrate_dialog_to_megagroup(DialogId dialog_id, Promise<Unit> &&promise) {
  LOG(INFO) << "Trying to convert " << dialog_id << " to supergroup";

  if (dialog_id.get_type() != DialogType::Chat) {
    promise.set_error(Status::Error(400, "Only basic group chats can be converted to supergroup"));
    return DialogId();
  }

  auto channel_id = td_->contacts_manager_->migrate_chat_to_megagroup(dialog_id.get_chat_id(), promise);
  if (!channel_id.is_valid()) {
    return DialogId();
  }

  if (!td_->contacts_manager_->have_channel(channel_id)) {
    LOG(ERROR) << "Can't find info about supergroup to which the group has migrated";
    promise.set_error(Status::Error(400, "Supergroup is not found"));
    return DialogId();
  }

  auto new_dialog_id = DialogId(channel_id);
  Dialog *d = get_dialog_force(new_dialog_id, "migrate_dialog_to_megagroup");
  if (d == nullptr) {
    d = add_dialog(new_dialog_id, "migrate_dialog_to_megagroup");
    if (d->pts == 0) {
      d->pts = 1;
      if (is_debug_message_op_enabled()) {
        d->debug_message_op.emplace_back(Dialog::MessageOp::SetPts, d->pts, "migrate");
      }
    }
    update_dialog_pos(d, "migrate_dialog_to_megagroup");
  }

  promise.set_value(Unit());
  return new_dialog_id;
}

void MessagesManager::update_has_outgoing_messages(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || (!m->is_outgoing && dialog_id != get_my_dialog_id())) {
    return;
  }

  Dialog *d = nullptr;
  switch (dialog_id.get_type()) {
    case DialogType::User:
      d = get_dialog(dialog_id);
      break;
    case DialogType::Chat:
    case DialogType::Channel:
      break;
    case DialogType::SecretChat: {
      auto user_id = td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      if (user_id.is_valid()) {
        d = get_dialog_force(DialogId(user_id), "update_has_outgoing_messages");
      }
      break;
    }
    default:
      UNREACHABLE();
  }
  if (d == nullptr || d->has_outgoing_messages) {
    return;
  }

  d->has_outgoing_messages = true;
  on_dialog_updated(dialog_id, "update_has_outgoing_messages");

  if (d->action_bar != nullptr && d->action_bar->on_outgoing_message()) {
    send_update_chat_action_bar(d);
  }
}

void MessagesManager::on_update_dialog_has_scheduled_server_messages(DialogId dialog_id,
                                                                     bool has_scheduled_server_messages) {
  CHECK(dialog_id.is_valid());
  if (td_->auth_manager_->is_bot() || dialog_id.get_type() == DialogType::SecretChat) {
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_has_scheduled_server_messages");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  LOG(INFO) << "Receive has_scheduled_server_messages = " << has_scheduled_server_messages << " in " << dialog_id;
  if (d->has_scheduled_server_messages != has_scheduled_server_messages) {
    set_dialog_has_scheduled_server_messages(d, has_scheduled_server_messages);
  } else if (has_scheduled_server_messages !=
             (d->has_scheduled_database_messages || d->scheduled_messages != nullptr)) {
    repair_dialog_scheduled_messages(d);
  }
}

void MessagesManager::on_update_dialog_filter(unique_ptr<DialogFilter> dialog_filter, Status result) {
  CHECK(!td_->auth_manager_->is_bot());
  if (result.is_error()) {
    // TODO rollback dialog_filters_ changes if error isn't 429
  } else {
    bool is_edited = false;
    for (auto &filter : server_dialog_filters_) {
      if (filter->dialog_filter_id == dialog_filter->dialog_filter_id) {
        if (*filter != *dialog_filter) {
          filter = std::move(dialog_filter);
        }
        is_edited = true;
        break;
      }
    }

    if (!is_edited) {
      server_dialog_filters_.push_back(std::move(dialog_filter));
    }
    save_dialog_filters();
  }

  are_dialog_filters_being_synchronized_ = false;
  synchronize_dialog_filters();
}

void MessagesManager::preload_newer_messages(const Dialog *d, MessageId max_message_id) {
  CHECK(d != nullptr);
  CHECK(max_message_id.is_valid());
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  MessagesConstIterator p(d, max_message_id);
  int32 limit = MAX_GET_HISTORY * 3 / 10;
  while (*p != nullptr && limit-- > 0) {
    ++p;
    if (*p) {
      max_message_id = (*p)->message_id;
    }
  }
  if (limit > 0 && (d->last_message_id == MessageId() || max_message_id < d->last_message_id)) {
    // need to preload some new messages
    LOG(INFO) << "Preloading newer after " << max_message_id;
    load_messages_impl(d, max_message_id, -MAX_GET_HISTORY + 1, MAX_GET_HISTORY, 3, false, Promise<Unit>());
  }
}

bool MessagesManager::can_report_dialog(DialogId dialog_id) const {
  // doesn't include possibility of report from action bar
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->can_report_user(dialog_id.get_user_id());
    case DialogType::Chat:
      return false;
    case DialogType::Channel:
      return !td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id()).is_creator();
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }
}

void MessagesManager::send_update_message_send_succeeded(Dialog *d, MessageId old_message_id, const Message *m) const {
  CHECK(m != nullptr);
  CHECK(d->is_update_new_chat_sent);
  if (!td_->auth_manager_->is_bot()) {
    d->yet_unsent_message_id_to_persistent_message_id.emplace(old_message_id, m->message_id);
  }
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateMessageSendSucceeded>(
                   get_message_object(d->dialog_id, m, "send_update_message_send_succeeded"), old_message_id.get()));
}

int32 MessagesManager::get_unload_dialog_delay() const {
  constexpr int32 DIALOG_UNLOAD_DELAY = 60;        // seconds
  constexpr int32 DIALOG_UNLOAD_BOT_DELAY = 1800;  // seconds

  CHECK(is_message_unload_enabled());
  auto default_unload_delay = td_->auth_manager_->is_bot() ? DIALOG_UNLOAD_BOT_DELAY : DIALOG_UNLOAD_DELAY;
  return narrow_cast<int32>(G()->shared_config().get_option_integer("message_unload_delay", default_unload_delay));
}

void MessagesManager::update_sent_message_contents(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || (!m->is_outgoing && dialog_id != get_my_dialog_id()) ||
      dialog_id.get_type() == DialogType::SecretChat || m->message_id.is_local() || m->forward_info != nullptr ||
      m->had_forward_info) {
    return;
  }

  on_sent_message_content(td_, m->content.get());
}

const DialogFilter *MessagesManager::get_server_dialog_filter(DialogFilterId dialog_filter_id) const {
  CHECK(!disable_get_dialog_filter_);
  for (const auto &filter : server_dialog_filters_) {
    if (filter->dialog_filter_id == dialog_filter_id) {
      return filter.get();
    }
  }
  return nullptr;
}

bool MessagesManager::has_message_sender_user_id(DialogId dialog_id, const Message *m) const {
  if (!m->sender_user_id.is_valid()) {
    return false;
  }
  if (td_->auth_manager_->is_bot() && is_discussion_message(dialog_id, m)) {
    return false;
  }
  return true;
}

double MessagesManager::get_dialog_filters_cache_time() {
  return DIALOG_FILTERS_CACHE_TIME * 0.0001 * Random::fast(9000, 11000);
}

MessagesManager::~MessagesManager() = default;
}  // namespace td
