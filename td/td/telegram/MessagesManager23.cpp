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
#include "td/telegram/PublicDialogType.h"
#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"
#include "td/telegram/SecretChatsManager.h"
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

#include <limits>

#include <type_traits>


#include <utility>

namespace td {
}  // namespace td
namespace td {

class GetSearchResultCalendarQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  MessageId from_message_id_;
  MessageSearchFilter filter_;
  int64 random_id_;

 public:
  explicit GetSearchResultCalendarQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId from_message_id, MessageSearchFilter filter, int64 random_id) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    dialog_id_ = dialog_id;
    from_message_id_ = from_message_id;
    filter_ = filter;
    random_id_ = random_id;

    send_query(G()->net_query_creator().create(telegram_api::messages_getSearchResultsCalendar(
        std::move(input_peer), get_input_messages_filter(filter), from_message_id.get_server_message_id().get(), 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getSearchResultsCalendar>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetSearchResultCalendarQuery: " << to_string(result);
    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetSearchResultCalendarQuery");
    td_->contacts_manager_->on_get_chats(std::move(result->chats_), "GetSearchResultCalendarQuery");

    MessagesManager::MessagesInfo info;
    info.messages = std::move(result->messages_);
    info.total_count = result->count_;
    info.is_channel_messages = dialog_id_.get_type() == DialogType::Channel;

    td_->messages_manager_->get_channel_difference_if_needed(
        dialog_id_, std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(), dialog_id = dialog_id_,
                                from_message_id = from_message_id_, filter = filter_, random_id = random_id_,
                                periods = std::move(result->periods_),
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_message_search_result_calendar, dialog_id, from_message_id,
                         filter, random_id, info.total_count, std::move(info.messages), std::move(periods),
                         std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetSearchResultCalendarQuery");
    td_->messages_manager_->on_failed_get_message_search_result_calendar(dialog_id_, random_id_);
    promise_.set_error(std::move(status));
  }
};

td_api::object_ptr<td_api::messageCalendar> MessagesManager::get_dialog_message_calendar(DialogId dialog_id,
                                                                                         MessageId from_message_id,
                                                                                         MessageSearchFilter filter,
                                                                                         int64 &random_id, bool use_db,
                                                                                         Promise<Unit> &&promise) {
  if (random_id != 0) {
    // request has already been sent before
    auto it = found_dialog_message_calendars_.find(random_id);
    if (it != found_dialog_message_calendars_.end()) {
      auto result = std::move(it->second);
      found_dialog_message_calendars_.erase(it);
      promise.set_value(Unit());
      return result;
    }
    random_id = 0;
  }
  LOG(INFO) << "Get message calendar in " << dialog_id << " filtered by " << filter << " from " << from_message_id;

  if (from_message_id.get() > MessageId::max().get()) {
    from_message_id = MessageId::max();
  }

  if (!from_message_id.is_valid() && from_message_id != MessageId()) {
    promise.set_error(Status::Error(400, "Parameter from_message_id must be identifier of a chat message or 0"));
    return {};
  }
  from_message_id = from_message_id.get_next_server_message_id();

  const Dialog *d = get_dialog_force(dialog_id, "get_dialog_message_calendar");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return {};
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return {};
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || found_dialog_message_calendars_.find(random_id) != found_dialog_message_calendars_.end());
  found_dialog_message_calendars_[random_id];  // reserve place for result

  CHECK(filter != MessageSearchFilter::Call && filter != MessageSearchFilter::MissedCall);
  if (filter == MessageSearchFilter::Empty || filter == MessageSearchFilter::Mention ||
      filter == MessageSearchFilter::UnreadMention) {
    promise.set_error(Status::Error(400, "The filter is not supported"));
    return {};
  }

  // Trying to use database
  if (use_db && G()->parameters().use_message_db) {
    MessageId first_db_message_id = get_first_database_message_id_by_index(d, filter);
    int32 message_count = d->message_count_by_index[message_search_filter_index(filter)];
    auto fixed_from_message_id = from_message_id;
    if (fixed_from_message_id == MessageId()) {
      fixed_from_message_id = MessageId::max();
    }
    LOG(INFO) << "Get message calendar in " << dialog_id << " from " << fixed_from_message_id << ", have up to "
              << first_db_message_id << ", message_count = " << message_count;
    if (first_db_message_id < fixed_from_message_id && message_count != -1) {
      LOG(INFO) << "Get message calendar from database in " << dialog_id << " from " << fixed_from_message_id;
      auto new_promise =
          PromiseCreator::lambda([random_id, dialog_id, fixed_from_message_id, first_db_message_id, filter,
                                  promise = std::move(promise)](Result<MessagesDbCalendar> r_calendar) mutable {
            send_closure(G()->messages_manager(), &MessagesManager::on_get_message_calendar_from_database, random_id,
                         dialog_id, fixed_from_message_id, first_db_message_id, filter, std::move(r_calendar),
                         std::move(promise));
          });
      MessagesDbDialogCalendarQuery db_query;
      db_query.dialog_id = dialog_id;
      db_query.filter = filter;
      db_query.from_message_id = fixed_from_message_id;
      db_query.tz_offset = static_cast<int32>(G()->shared_config().get_option_integer("utc_time_offset"));
      G()->td_db()->get_messages_db_async()->get_dialog_message_calendar(db_query, std::move(new_promise));
      return {};
    }
  }
  if (filter == MessageSearchFilter::FailedToSend) {
    promise.set_value(Unit());
    return {};
  }

  LOG(DEBUG) << "Get message calendar from server in " << dialog_id << " from " << from_message_id;

  switch (dialog_id.get_type()) {
    case DialogType::None:
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      td_->create_handler<GetSearchResultCalendarQuery>(std::move(promise))
          ->send(dialog_id, from_message_id, filter, random_id);
      break;
    case DialogType::SecretChat:
      promise.set_value(Unit());
      break;
    default:
      UNREACHABLE();
      promise.set_error(Status::Error(500, "Search messages is not supported"));
  }
  return {};
}

class ReportPeerQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit ReportPeerQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const vector<MessageId> &message_ids, ReportReason &&report_reason) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    if (message_ids.empty()) {
      send_query(G()->net_query_creator().create(telegram_api::account_reportPeer(
          std::move(input_peer), report_reason.get_input_report_reason(), report_reason.get_message())));
    } else {
      send_query(G()->net_query_creator().create(
          telegram_api::messages_report(std::move(input_peer), MessagesManager::get_server_message_ids(message_ids),
                                        report_reason.get_input_report_reason(), report_reason.get_message())));
    }
  }

  void on_result(BufferSlice packet) final {
    static_assert(
        std::is_same<telegram_api::account_reportPeer::ReturnType, telegram_api::messages_report::ReturnType>::value,
        "");
    auto result_ptr = fetch_result<telegram_api::account_reportPeer>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      return on_error(Status::Error(400, "Receive false as result"));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for report peer: " << status;
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ReportPeerQuery");
    td_->messages_manager_->reget_dialog_action_bar(dialog_id_, "ReportPeerQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::report_dialog(DialogId dialog_id, const vector<MessageId> &message_ids, ReportReason &&reason,
                                    Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "report_dialog");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  Dialog *user_d = d;
  bool is_dialog_spam_report = false;
  bool can_report_spam = false;
  if (reason.is_spam() && message_ids.empty()) {
    // report from action bar
    if (dialog_id.get_type() == DialogType::SecretChat) {
      auto user_dialog_id = DialogId(td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id()));
      user_d = get_dialog_force(user_dialog_id, "report_dialog 2");
      if (user_d == nullptr) {
        return promise.set_error(Status::Error(400, "Chat with the user not found"));
      }
    }
    is_dialog_spam_report = user_d->know_action_bar;
    can_report_spam = user_d->action_bar != nullptr && user_d->action_bar->can_report_spam();
  }

  if (is_dialog_spam_report && can_report_spam) {
    hide_dialog_action_bar(user_d);
    return toggle_dialog_report_spam_state_on_server(dialog_id, true, 0, std::move(promise));
  }

  if (!can_report_dialog(dialog_id)) {
    if (is_dialog_spam_report) {
      return promise.set_value(Unit());
    }

    return promise.set_error(Status::Error(400, "Chat can't be reported"));
  }

  vector<MessageId> server_message_ids;
  for (auto message_id : message_ids) {
    if (message_id.is_scheduled()) {
      return promise.set_error(Status::Error(400, "Can't report scheduled messages"));
    }
    if (message_id.is_valid() && message_id.is_server()) {
      server_message_ids.push_back(message_id);
    }
  }

  if (dialog_id.get_type() == DialogType::Channel && reason.is_unrelated_location()) {
    hide_dialog_action_bar(d);
  }

  td_->create_handler<ReportPeerQuery>(std::move(promise))->send(dialog_id, server_message_ids, std::move(reason));
}

class GetChannelDifferenceQuery final : public Td::ResultHandler {
  DialogId dialog_id_;
  int32 pts_;
  int32 limit_;

 public:
  void send(DialogId dialog_id, tl_object_ptr<telegram_api::InputChannel> &&input_channel, int32 pts, int32 limit,
            bool force) {
    CHECK(pts >= 0);
    dialog_id_ = dialog_id;
    pts_ = pts;
    limit_ = limit;
    CHECK(input_channel != nullptr);

    int32 flags = 0;
    if (force) {
      flags |= telegram_api::updates_getChannelDifference::FORCE_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::updates_getChannelDifference(
        flags, false /*ignored*/, std::move(input_channel), make_tl_object<telegram_api::channelMessagesFilterEmpty>(),
        pts, limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::updates_getChannelDifference>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->messages_manager_->on_get_channel_difference(dialog_id_, pts_, limit_, result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetChannelDifferenceQuery")) {
      LOG(ERROR) << "Receive error for GetChannelDifferenceQuery for " << dialog_id_ << " with pts " << pts_
                 << " and limit " << limit_ << ": " << status;
    }
    td_->messages_manager_->on_get_channel_difference(dialog_id_, pts_, limit_, nullptr);
  }
};

void MessagesManager::do_get_channel_difference(DialogId dialog_id, int32 pts, bool force,
                                                tl_object_ptr<telegram_api::InputChannel> &&input_channel,
                                                const char *source) {
  auto inserted = active_get_channel_differencies_.emplace(dialog_id, source);
  if (!inserted.second) {
    LOG(INFO) << "Skip running channels.getDifference for " << dialog_id << " from " << source
              << " because it has already been run";
    return;
  }
  // must work even we know nothing about the dialog

  // can be called multiple times before after_get_channel_difference
  const Dialog *d = get_dialog(dialog_id);
  if (d != nullptr) {
    if (d->message_notification_group.group_id.is_valid()) {
      send_closure_later(G()->notification_manager(), &NotificationManager::before_get_chat_difference,
                         d->message_notification_group.group_id);
    }
    if (d->mention_notification_group.group_id.is_valid()) {
      send_closure_later(G()->notification_manager(), &NotificationManager::before_get_chat_difference,
                         d->mention_notification_group.group_id);
    }
  }

  int32 limit = td_->auth_manager_->is_bot() ? MAX_BOT_CHANNEL_DIFFERENCE : MAX_CHANNEL_DIFFERENCE;
  if (pts <= 0) {
    pts = 1;
    limit = MIN_CHANNEL_DIFFERENCE;
  }

  LOG(INFO) << "-----BEGIN GET CHANNEL DIFFERENCE----- for " << dialog_id << " with pts " << pts << " and limit "
            << limit << " from " << source;

  td_->create_handler<GetChannelDifferenceQuery>()->send(dialog_id, std::move(input_channel), pts, limit, force);
}

void MessagesManager::get_dialog_send_message_as_dialog_ids(
    DialogId dialog_id, Promise<td_api::object_ptr<td_api::messageSenders>> &&promise, bool is_recursive) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  const Dialog *d = get_dialog_force(dialog_id, "get_group_call_join_as");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access chat"));
  }
  if (!d->default_send_message_as_dialog_id.is_valid()) {
    return promise.set_value(td_api::make_object<td_api::messageSenders>());
  }
  CHECK(dialog_id.get_type() == DialogType::Channel);

  if (created_public_broadcasts_inited_) {
    auto senders = td_api::make_object<td_api::messageSenders>();
    if (!created_public_broadcasts_.empty()) {
      auto add_sender = [&senders, td = td_](DialogId dialog_id) {
        senders->total_count_++;
        senders->senders_.push_back(get_message_sender_object_const(td, dialog_id, "add_sender"));
      };
      if (is_anonymous_administrator(dialog_id, nullptr)) {
        add_sender(dialog_id);
      } else {
        add_sender(get_my_dialog_id());
      }
      auto sorted_channel_ids = transform(created_public_broadcasts_, [&](ChannelId channel_id) {
        auto participant_count = td_->contacts_manager_->get_channel_participant_count(channel_id);
        return std::make_pair(-participant_count, channel_id.get());
      });
      std::sort(sorted_channel_ids.begin(), sorted_channel_ids.end());

      for (auto channel_id : sorted_channel_ids) {
        add_sender(DialogId(ChannelId(channel_id.second)));
      }
    }
    return promise.set_value(std::move(senders));
  }

  CHECK(!is_recursive);
  auto new_promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, promise = std::move(promise)](
                                                Result<td_api::object_ptr<td_api::chats>> &&result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      send_closure_later(actor_id, &MessagesManager::get_dialog_send_message_as_dialog_ids, dialog_id,
                         std::move(promise), true);
    }
  });
  td_->contacts_manager_->get_created_public_dialogs(PublicDialogType::HasUsername, std::move(new_promise), true);
}

void MessagesManager::set_dialog_is_empty(Dialog *d, const char *source) {
  LOG(INFO) << "Set " << d->dialog_id << " is_empty to true from " << source;
  CHECK(d->have_full_history);
  d->is_empty = true;

  if (d->server_unread_count + d->local_unread_count > 0) {
    MessageId max_message_id =
        d->last_database_message_id.is_valid() ? d->last_database_message_id : d->last_new_message_id;
    if (max_message_id.is_valid()) {
      read_history_inbox(d->dialog_id, max_message_id, -1, "set_dialog_is_empty");
    }
    if (d->server_unread_count != 0 || d->local_unread_count != 0) {
      set_dialog_last_read_inbox_message_id(d, MessageId::min(), 0, 0, true, "set_dialog_is_empty");
    }
  }
  if (d->unread_mention_count > 0) {
    set_dialog_unread_mention_count(d, 0);
    send_update_chat_unread_mention_count(d);
  }
  if (d->reply_markup_message_id != MessageId()) {
    set_dialog_reply_markup(d, MessageId());
  }
  std::fill(d->message_count_by_index.begin(), d->message_count_by_index.end(), 0);
  d->notification_id_to_message_id.clear();

  if (d->delete_last_message_date != 0) {
    if (d->is_last_message_deleted_locally && d->last_clear_history_date == 0) {
      set_dialog_last_clear_history_date(d, d->delete_last_message_date, d->deleted_last_message_id,
                                         "set_dialog_is_empty");
    }
    d->delete_last_message_date = 0;
    d->deleted_last_message_id = MessageId();
    d->is_last_message_deleted_locally = false;

    on_dialog_updated(d->dialog_id, "set_dialog_is_empty");
  }
  if (d->pending_last_message_date != 0) {
    d->pending_last_message_date = 0;
    d->pending_last_message_id = MessageId();
  }
  if (d->last_database_message_id.is_valid()) {
    set_dialog_first_database_message_id(d, MessageId(), "set_dialog_is_empty");
    set_dialog_last_database_message_id(d, MessageId(), "set_dialog_is_empty");
  }

  update_dialog_pos(d, source);
}

class SearchPublicDialogsQuery final : public Td::ResultHandler {
  string query_;

 public:
  void send(const string &query) {
    query_ = query;
    send_query(G()->net_query_creator().create(telegram_api::contacts_search(query, 3 /* ignored server-side */)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_search>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto dialogs = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SearchPublicDialogsQuery: " << to_string(dialogs);
    td_->contacts_manager_->on_get_users(std::move(dialogs->users_), "SearchPublicDialogsQuery");
    td_->contacts_manager_->on_get_chats(std::move(dialogs->chats_), "SearchPublicDialogsQuery");
    td_->messages_manager_->on_get_public_dialogs_search_result(query_, std::move(dialogs->my_results_),
                                                                std::move(dialogs->results_));
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for SearchPublicDialogsQuery: " << status;
    }
    td_->messages_manager_->on_failed_public_dialogs_search(query_, std::move(status));
  }
};

void MessagesManager::send_search_public_dialogs_query(const string &query, Promise<Unit> &&promise) {
  auto &promises = search_public_dialogs_queries_[query];
  promises.push_back(std::move(promise));
  if (promises.size() != 1) {
    // query has already been sent, just wait for the result
    return;
  }

  td_->create_handler<SearchPublicDialogsQuery>()->send(query);
}

Status MessagesManager::send_screenshot_taken_notification_message(DialogId dialog_id) {
  auto dialog_type = dialog_id.get_type();
  if (dialog_type != DialogType::User && dialog_type != DialogType::SecretChat) {
    return Status::Error(400, "Notification about taken screenshot can be sent only in private and secret chats");
  }

  LOG(INFO) << "Begin to send notification about taken screenshot in " << dialog_id;

  Dialog *d = get_dialog_force(dialog_id, "send_screenshot_taken_notification_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  TRY_STATUS(can_send_message(dialog_id));

  if (dialog_type == DialogType::User) {
    bool need_update_dialog_pos = false;
    const Message *m = get_message_to_send(d, MessageId(), MessageId(), MessageSendOptions(),
                                           create_screenshot_taken_message_content(), &need_update_dialog_pos);

    do_send_screenshot_taken_notification_message(dialog_id, m, 0);

    send_update_new_message(d, m);
    if (need_update_dialog_pos) {
      send_update_chat_last_message(d, "send_screenshot_taken_notification_message");
    }
  } else {
    send_closure(td_->secret_chats_manager_, &SecretChatsManager::notify_screenshot_taken,
                 dialog_id.get_secret_chat_id(),
                 Promise<>());  // TODO Promise
  }

  return Status::OK();
}

void MessagesManager::ttl_db_loop(double server_now) {
  LOG(INFO) << "Begin ttl_db loop: " << tag("expires_from", ttl_db_expires_from_)
            << tag("expires_till", ttl_db_expires_till_) << tag("has_query", ttl_db_has_query_);
  if (ttl_db_has_query_) {
    return;
  }

  auto now = static_cast<int32>(server_now);

  if (ttl_db_expires_till_ < 0) {
    LOG(INFO) << "Finish ttl_db loop";
    return;
  }

  if (now < ttl_db_expires_from_) {
    ttl_db_slot_.set_event(EventCreator::yield(actor_shared(this, YieldType::TtlDb)));
    auto wakeup_in = ttl_db_expires_from_ - server_now;
    ttl_db_slot_.set_timeout_in(wakeup_in);
    LOG(INFO) << "Set ttl_db timeout in " << wakeup_in;
    return;
  }

  ttl_db_has_query_ = true;
  int32 limit = 50;
  LOG(INFO) << "Send ttl_db query " << tag("expires_from", ttl_db_expires_from_)
            << tag("expires_till", ttl_db_expires_till_) << tag("limit", limit);
  G()->td_db()->get_messages_db_async()->get_expiring_messages(
      ttl_db_expires_from_, ttl_db_expires_till_, limit,
      PromiseCreator::lambda(
          [actor_id = actor_id(this)](Result<std::pair<std::vector<MessagesDbMessage>, int32>> result) {
            send_closure(actor_id, &MessagesManager::ttl_db_on_result, std::move(result), false);
          }));
}

void MessagesManager::on_dialog_bots_updated(DialogId dialog_id, vector<UserId> bot_user_ids, bool from_database) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto d = from_database ? get_dialog(dialog_id) : get_dialog_force(dialog_id, "on_dialog_bots_updated");
  if (d == nullptr) {
    return;
  }

  bool has_bots = !bot_user_ids.empty();
  if (!d->is_has_bots_inited || d->has_bots != has_bots) {
    set_dialog_has_bots(d, has_bots);
    on_dialog_updated(dialog_id, "on_dialog_bots_updated");
  }

  if (d->reply_markup_message_id != MessageId()) {
    const Message *m = get_message_force(d, d->reply_markup_message_id, "on_dialog_bots_updated");
    if (m == nullptr || (m->sender_user_id.is_valid() && !td::contains(bot_user_ids, m->sender_user_id))) {
      LOG(INFO) << "Remove reply markup in " << dialog_id << ", because bot "
                << (m == nullptr ? UserId() : m->sender_user_id) << " isn't a member of the chat";
      set_dialog_reply_markup(d, MessageId());
    }
  }
}

void MessagesManager::change_message_files(DialogId dialog_id, const Message *m, const vector<FileId> &old_file_ids) {
  if (dialog_id.get_type() != DialogType::SecretChat && m->is_content_secret) {
    // return;
  }

  auto new_file_ids = get_message_content_file_ids(m->content.get(), td_);
  if (new_file_ids == old_file_ids) {
    return;
  }

  FullMessageId full_message_id{dialog_id, m->message_id};
  if (need_delete_message_files(dialog_id, m)) {
    for (auto file_id : old_file_ids) {
      if (!td::contains(new_file_ids, file_id) && need_delete_file(full_message_id, file_id)) {
        send_closure(G()->file_manager(), &FileManager::delete_file, file_id, Promise<>(), "change_message_files");
      }
    }
  }

  auto file_source_id = get_message_file_source_id(full_message_id);
  if (file_source_id.is_valid()) {
    td_->file_manager_->change_files_source(file_source_id, old_file_ids, new_file_ids);
  }
}

td_api::object_ptr<td_api::ChatActionBar> MessagesManager::get_chat_action_bar_object(const Dialog *d) const {
  CHECK(d != nullptr);
  auto dialog_type = d->dialog_id.get_type();
  if (dialog_type == DialogType::SecretChat) {
    auto user_id = td_->contacts_manager_->get_secret_chat_user_id(d->dialog_id.get_secret_chat_id());
    if (!user_id.is_valid()) {
      return nullptr;
    }
    const Dialog *user_d = get_dialog(DialogId(user_id));
    if (user_d == nullptr || user_d->action_bar == nullptr) {
      return nullptr;
    }
    return user_d->action_bar->get_chat_action_bar_object(DialogType::User, d->folder_id != FolderId::archive());
  }

  if (d->action_bar == nullptr) {
    return nullptr;
  }
  return d->action_bar->get_chat_action_bar_object(dialog_type, false);
}

void MessagesManager::reregister_message_reply(const Dialog *d, const Message *m) {
  if (!m->reply_to_message_id.is_valid() || td_->auth_manager_->is_bot()) {
    return;
  }

  auto it = replied_by_media_timestamp_messages_.find({d->dialog_id, m->reply_to_message_id});
  bool was_registered = it != replied_by_media_timestamp_messages_.end() && it->second.count(m->message_id) > 0;
  bool need_register =
      has_media_timestamps(get_message_content_text(m->content.get()), 0, std::numeric_limits<int32>::max());
  if (was_registered == need_register) {
    return;
  }
  if (was_registered) {
    unregister_message_reply(d, m);
  } else {
    register_message_reply(d, m);
  }
}

void MessagesManager::repair_dialog_active_group_call_id(DialogId dialog_id) {
  if (have_input_peer(dialog_id, AccessRights::Read)) {
    LOG(INFO) << "Repair active voice chat ID in " << dialog_id;
    create_actor<SleepActor>("RepairChatActiveVoiceChatId", 1.0,
                             PromiseCreator::lambda([actor_id = actor_id(this), dialog_id](Result<Unit> result) {
                               send_closure(actor_id, &MessagesManager::do_repair_dialog_active_group_call_id,
                                            dialog_id);
                             }))
        .release();
  }
}

bool MessagesManager::is_forward_info_sender_hidden(const MessageForwardInfo *forward_info) {
  CHECK(forward_info != nullptr);
  if (forward_info->is_imported) {
    return false;
  }
  if (!forward_info->sender_name.empty()) {
    return true;
  }
  DialogId hidden_sender_dialog_id(ChannelId(static_cast<int64>(G()->is_test_dc() ? 10460537 : 1228946795)));
  return forward_info->sender_dialog_id == hidden_sender_dialog_id && !forward_info->author_signature.empty() &&
         !forward_info->message_id.is_valid();
}

void MessagesManager::view_message_live_location_on_server_impl(int64 task_id, FullMessageId full_message_id) {
  auto promise = PromiseCreator::lambda([actor_id = actor_id(this), task_id](Unit result) {
    send_closure(actor_id, &MessagesManager::on_message_live_location_viewed_on_server, task_id);
  });
  read_message_contents_on_server(full_message_id.get_dialog_id(), {full_message_id.get_message_id()}, 0,
                                  std::move(promise), true);
}

void MessagesManager::find_newer_messages(const Message *m, MessageId min_message_id, vector<MessageId> &message_ids) {
  if (m == nullptr) {
    return;
  }

  if (m->message_id > min_message_id) {
    find_newer_messages(m->left.get(), min_message_id, message_ids);

    message_ids.push_back(m->message_id);
  }

  find_newer_messages(m->right.get(), min_message_id, message_ids);
}

void MessagesManager::set_dialog_has_scheduled_database_messages(DialogId dialog_id,
                                                                 bool has_scheduled_database_messages) {
  if (G()->close_flag()) {
    return;
  }
  return set_dialog_has_scheduled_database_messages_impl(get_dialog(dialog_id), has_scheduled_database_messages);
}

void MessagesManager::fix_dialog_action_bar(const Dialog *d, DialogActionBar *action_bar) {
  if (action_bar == nullptr) {
    return;
  }

  CHECK(d != nullptr);
  action_bar->fix(td_, d->dialog_id, d->is_blocked, d->folder_id);
}

tl_object_ptr<td_api::chats> MessagesManager::get_chats_object(const std::pair<int32, vector<DialogId>> &dialog_ids) {
  return get_chats_object(dialog_ids.first, dialog_ids.second);
}
}  // namespace td
