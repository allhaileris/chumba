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










namespace td {
}  // namespace td
namespace td {

MessagesManager::Dialog *MessagesManager::add_new_dialog(unique_ptr<Dialog> &&d, bool is_loaded_from_database,
                                                         const char *source) {
  auto dialog_id = d->dialog_id;
  LOG_CHECK(is_inited_) << dialog_id << ' ' << is_loaded_from_database << ' ' << source;
  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (dialog_id == get_my_dialog_id() && d->last_read_inbox_message_id == MessageId::max() &&
          d->last_read_outbox_message_id == MessageId::max()) {
        d->last_read_inbox_message_id = d->last_new_message_id;
        d->last_read_outbox_message_id = d->last_new_message_id;
      }
      d->has_bots = dialog_id.get_user_id() != ContactsManager::get_replies_bot_user_id() &&
                    td_->contacts_manager_->is_user_bot(dialog_id.get_user_id());
      d->is_has_bots_inited = true;
      break;
    case DialogType::Chat:
      d->is_is_blocked_inited = true;
      break;
    case DialogType::Channel: {
      auto channel_type = td_->contacts_manager_->get_channel_type(dialog_id.get_channel_id());
      if (channel_type == ContactsManager::ChannelType::Broadcast) {
        d->last_read_outbox_message_id = MessageId::max();
        d->is_last_read_outbox_message_id_inited = true;
      }

      auto pts = load_channel_pts(dialog_id);
      if (pts > 0) {
        d->pts = pts;
        if (is_debug_message_op_enabled()) {
          d->debug_message_op.emplace_back(Dialog::MessageOp::SetPts, pts, "add_new_dialog");
        }
      }
      break;
    }
    case DialogType::SecretChat:
      if (d->last_new_message_id.get() <= MessageId::min().get()) {
        LOG(INFO) << "Set " << dialog_id << " last new message in add_new_dialog from " << source;
        d->last_new_message_id = MessageId::min().get_next_message_id(MessageType::Local);
      }

      if (!d->notification_settings.is_secret_chat_show_preview_fixed) {
        d->notification_settings.use_default_show_preview = true;
        d->notification_settings.show_preview = false;
        d->notification_settings.is_secret_chat_show_preview_fixed = true;
        on_dialog_updated(dialog_id, "fix secret chat show preview");
      }

      d->have_full_history = true;
      d->need_restore_reply_markup = false;
      d->is_last_read_inbox_message_id_inited = true;
      d->is_last_read_outbox_message_id_inited = true;
      d->is_last_pinned_message_id_inited = true;
      d->is_theme_name_inited = true;
      d->is_is_blocked_inited = true;
      if (!d->is_folder_id_inited && !td_->auth_manager_->is_bot()) {
        do_set_dialog_folder_id(
            d.get(), td_->contacts_manager_->get_secret_chat_initial_folder_id(dialog_id.get_secret_chat_id()));
      }
      d->message_ttl = MessageTtl(td_->contacts_manager_->get_secret_chat_ttl(dialog_id.get_secret_chat_id()));
      d->is_message_ttl_inited = true;
      d->has_bots = td_->contacts_manager_->is_user_bot(
          td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id()));
      d->is_has_bots_inited = true;

      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  if (!is_loaded_from_database) {
    on_dialog_updated(dialog_id, "add_new_dialog");
  }
  if (td_->auth_manager_->is_bot()) {
    d->notification_settings.is_synchronized = true;
  }
  if (is_channel_difference_finished_.erase(dialog_id)) {
    d->is_channel_difference_finished = true;
  }

  unique_ptr<Message> last_database_message = std::move(d->messages);
  MessageId last_database_message_id = d->last_database_message_id;
  d->last_database_message_id = MessageId();
  int64 order = d->order;
  d->order = DEFAULT_ORDER;
  int32 last_clear_history_date = d->last_clear_history_date;
  MessageId last_clear_history_message_id = d->last_clear_history_message_id;
  d->last_clear_history_date = 0;
  d->last_clear_history_message_id = MessageId();
  DialogId default_join_group_call_as_dialog_id = d->default_join_group_call_as_dialog_id;
  if (default_join_group_call_as_dialog_id != dialog_id &&
      default_join_group_call_as_dialog_id.get_type() != DialogType::User &&
      !have_dialog(default_join_group_call_as_dialog_id)) {
    d->default_join_group_call_as_dialog_id = DialogId();
  }
  DialogId default_send_message_as_dialog_id = d->default_send_message_as_dialog_id;
  bool need_drop_default_send_message_as_dialog_id = d->need_drop_default_send_message_as_dialog_id;
  if (default_send_message_as_dialog_id != dialog_id &&
      default_send_message_as_dialog_id.get_type() != DialogType::User &&
      !have_dialog(default_send_message_as_dialog_id)) {
    d->need_drop_default_send_message_as_dialog_id = false;
    d->default_send_message_as_dialog_id = DialogId();
  }

  if (d->message_notification_group.group_id.is_valid()) {
    notification_group_id_to_dialog_id_.emplace(d->message_notification_group.group_id, dialog_id);
  }
  if (d->mention_notification_group.group_id.is_valid()) {
    notification_group_id_to_dialog_id_.emplace(d->mention_notification_group.group_id, dialog_id);
  }
  if (pending_dialog_group_call_updates_.count(dialog_id) > 0) {
    auto it = pending_dialog_group_call_updates_.find(dialog_id);
    bool has_active_group_call = it->second.first;
    bool is_group_call_empty = it->second.second;
    pending_dialog_group_call_updates_.erase(it);
    if (d->has_active_group_call != has_active_group_call || d->is_group_call_empty != is_group_call_empty) {
      if (!has_active_group_call) {
        d->active_group_call_id = InputGroupCallId();
      }
      d->has_active_group_call = has_active_group_call;
      d->is_group_call_empty = is_group_call_empty;
      on_dialog_updated(dialog_id, "pending update_dialog_group_call");
    }
  }
  fix_pending_join_requests(dialog_id, d->pending_join_request_count, d->pending_join_request_user_ids);

  if (!is_loaded_from_database) {
    CHECK(order == DEFAULT_ORDER);
    CHECK(last_database_message == nullptr);
  }

  auto dialog_it = dialogs_.emplace(dialog_id, std::move(d)).first;

  loaded_dialogs_.erase(dialog_id);

  Dialog *dialog = dialog_it->second.get();

  fix_dialog_action_bar(dialog, dialog->action_bar.get());

  send_update_new_chat(dialog);

  fix_new_dialog(dialog, std::move(last_database_message), last_database_message_id, order, last_clear_history_date,
                 last_clear_history_message_id, default_join_group_call_as_dialog_id, default_send_message_as_dialog_id,
                 need_drop_default_send_message_as_dialog_id, is_loaded_from_database);

  return dialog;
}

void MessagesManager::read_history_inbox(DialogId dialog_id, MessageId max_message_id, int32 unread_count,
                                         const char *source) {
  CHECK(!max_message_id.is_scheduled());

  if (td_->auth_manager_->is_bot()) {
    return;
  }

  Dialog *d = get_dialog_force(dialog_id, "read_history_inbox");
  if (d != nullptr) {
    if (d->need_repair_channel_server_unread_count) {
      d->need_repair_channel_server_unread_count = false;
      on_dialog_updated(dialog_id, "read_history_inbox");
    }

    // there can be updateReadHistoryInbox up to message 0, if messages where read and then all messages where deleted
    if (!max_message_id.is_valid() && max_message_id != MessageId()) {
      LOG(ERROR) << "Receive read inbox update in " << dialog_id << " up to " << max_message_id << " from " << source;
      return;
    }
    if (d->is_last_read_inbox_message_id_inited && max_message_id <= d->last_read_inbox_message_id) {
      LOG(INFO) << "Receive read inbox update in " << dialog_id << " up to " << max_message_id << " from " << source
                << ", but all messages have already been read up to " << d->last_read_inbox_message_id;
      if (max_message_id == d->last_read_inbox_message_id && unread_count >= 0 &&
          unread_count != d->server_unread_count) {
        set_dialog_last_read_inbox_message_id(d, MessageId::min(), unread_count, d->local_unread_count, true, source);
      }
      return;
    }

    if (max_message_id != MessageId() && max_message_id.is_yet_unsent()) {
      LOG(ERROR) << "Tried to update last read inbox message in " << dialog_id << " with " << max_message_id << " from "
                 << source;
      return;
    }

    if (max_message_id != MessageId() && unread_count > 0 && max_message_id >= d->last_new_message_id &&
        max_message_id >= d->last_message_id && max_message_id >= d->last_database_message_id) {
      if (d->last_new_message_id.is_valid()) {
        LOG(ERROR) << "Have unknown " << unread_count << " unread messages up to " << max_message_id << " in "
                   << dialog_id << " with last_new_message_id = " << d->last_new_message_id
                   << ", last_message_id = " << d->last_message_id
                   << ", last_database_message_id = " << d->last_database_message_id << " from " << source;
      }
      unread_count = 0;
    }

    LOG_IF(INFO, d->last_new_message_id.is_valid() && max_message_id > d->last_new_message_id &&
                     max_message_id > d->max_notification_message_id && max_message_id.is_server() &&
                     dialog_id.get_type() != DialogType::Channel && !running_get_difference_)
        << "Receive read inbox update up to unknown " << max_message_id << " in " << dialog_id << " from " << source
        << ". Last new is " << d->last_new_message_id << ", unread_count = " << unread_count
        << ". Possible only for deleted incoming message";

    if (dialog_id.get_type() == DialogType::SecretChat) {
      ttl_read_history(d, false, max_message_id, d->last_read_inbox_message_id, Time::now());
    }

    if (max_message_id > d->last_new_message_id && dialog_id.get_type() == DialogType::Channel) {
      LOG(INFO) << "Schedule getDifference in " << dialog_id.get_channel_id();
      channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
    }

    int32 server_unread_count = calc_new_unread_count(d, max_message_id, MessageType::Server, unread_count);
    int32 local_unread_count =
        d->local_unread_count == 0 ? 0 : calc_new_unread_count(d, max_message_id, MessageType::Local, -1);

    if (server_unread_count < 0) {
      server_unread_count = unread_count >= 0 ? unread_count : d->server_unread_count;
      if (dialog_id.get_type() != DialogType::SecretChat && have_input_peer(dialog_id, AccessRights::Read) &&
          need_unread_counter(d->order)) {
        d->need_repair_server_unread_count = true;
        repair_server_unread_count(dialog_id, server_unread_count);
      }
    }
    if (local_unread_count < 0) {
      // TODO repair local unread count
      local_unread_count = d->local_unread_count;
    }

    set_dialog_last_read_inbox_message_id(d, max_message_id, server_unread_count, local_unread_count, true, source);

    if (d->is_marked_as_unread && max_message_id != MessageId()) {
      set_dialog_is_marked_as_unread(d, false);
    }
  } else {
    LOG(INFO) << "Receive read inbox about unknown " << dialog_id << " from " << source;
  }
}

void MessagesManager::update_dialog_pos(Dialog *d, const char *source, bool need_send_update,
                                        bool is_loaded_from_database) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  LOG(INFO) << "Trying to update " << d->dialog_id << " order from " << source;

  int64 new_order = DEFAULT_ORDER;
  if (!is_removed_from_dialog_list(d)) {
    if (d->last_message_id != MessageId()) {
      auto m = get_message(d, d->last_message_id);
      CHECK(m != nullptr);
      LOG(INFO) << "Last message at " << m->date << " found";
      int64 last_message_order = get_dialog_order(m->message_id, m->date);
      if (last_message_order > new_order) {
        new_order = last_message_order;
      }
    } else if (d->delete_last_message_date > 0) {
      LOG(INFO) << "Deleted last " << d->deleted_last_message_id << " at " << d->delete_last_message_date << " found";
      int64 delete_order = get_dialog_order(d->deleted_last_message_id, d->delete_last_message_date);
      if (delete_order > new_order) {
        new_order = delete_order;
      }
    } else if (d->last_clear_history_date > 0) {
      LOG(INFO) << "Clear history at " << d->last_clear_history_date << " found";
      int64 clear_order = get_dialog_order(d->last_clear_history_message_id, d->last_clear_history_date);
      if (clear_order > new_order) {
        new_order = clear_order;
      }
    }
    if (d->pending_last_message_date > 0) {
      LOG(INFO) << "Pending last " << d->pending_last_message_id << " at " << d->pending_last_message_date << " found";
      int64 pending_order = get_dialog_order(d->pending_last_message_id, d->pending_last_message_date);
      if (pending_order > new_order) {
        new_order = pending_order;
      }
    }
    if (d->draft_message != nullptr && can_send_message(d->dialog_id).is_ok()) {
      LOG(INFO) << "Draft message at " << d->draft_message->date << " found";
      int64 draft_order = get_dialog_order(MessageId(), d->draft_message->date);
      if (draft_order > new_order) {
        new_order = draft_order;
      }
    }
    auto dialog_type = d->dialog_id.get_type();
    if (dialog_type == DialogType::Channel) {
      auto date = td_->contacts_manager_->get_channel_date(d->dialog_id.get_channel_id());
      LOG(INFO) << "Join of channel at " << date << " found";
      int64 join_order = get_dialog_order(MessageId(), date);
      if (join_order > new_order) {
        new_order = join_order;
      }
    }
    if (dialog_type == DialogType::SecretChat) {
      auto date = td_->contacts_manager_->get_secret_chat_date(d->dialog_id.get_secret_chat_id());
      if (date != 0 && !is_deleted_secret_chat(d)) {
        LOG(INFO) << "Creation of secret chat at " << date << " found";
        int64 creation_order = get_dialog_order(MessageId(), date);
        if (creation_order > new_order) {
          new_order = creation_order;
        }
      }
    }
    if (new_order == DEFAULT_ORDER && !d->is_empty) {
      LOG(INFO) << "There are no known messages in the chat, just leave it where it is";
      new_order = d->order;
    }
  }

  if (set_dialog_order(d, new_order, need_send_update, is_loaded_from_database, source)) {
    on_dialog_updated(d->dialog_id, "update_dialog_pos");
  }
}

class DeleteMessagesByDateQuery final : public Td::ResultHandler {
  Promise<AffectedHistory> promise_;
  DialogId dialog_id_;

 public:
  explicit DeleteMessagesByDateQuery(Promise<AffectedHistory> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int32 min_date, int32 max_date, bool revoke) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id_, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Chat is not accessible"));
    }

    int32 flags = telegram_api::messages_deleteHistory::JUST_CLEAR_MASK |
                  telegram_api::messages_deleteHistory::MIN_DATE_MASK |
                  telegram_api::messages_deleteHistory::MAX_DATE_MASK;
    if (revoke) {
      flags |= telegram_api::messages_deleteHistory::REVOKE_MASK;
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_deleteHistory(
        flags, false /*ignored*/, false /*ignored*/, std::move(input_peer), 0, min_date, max_date)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(AffectedHistory(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "DeleteMessagesByDateQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::delete_dialog_messages_by_date_on_server(DialogId dialog_id, int32 min_date, int32 max_date,
                                                               bool revoke, uint64 log_event_id,
                                                               Promise<Unit> &&promise) {
  if (log_event_id == 0 && G()->parameters().use_chat_info_db) {
    log_event_id = save_delete_dialog_messages_by_date_on_server_log_event(dialog_id, min_date, max_date, revoke);
  }

  AffectedHistoryQuery query = [td = td_, min_date, max_date, revoke](DialogId dialog_id,
                                                                      Promise<AffectedHistory> &&query_promise) {
    td->create_handler<DeleteMessagesByDateQuery>(std::move(query_promise))
        ->send(dialog_id, min_date, max_date, revoke);
  };
  run_affected_history_query_until_complete(dialog_id, std::move(query), true,
                                            get_erase_log_event_promise(log_event_id, std::move(promise)));
}

void MessagesManager::remove_message_notification(DialogId dialog_id, NotificationGroupId group_id,
                                                  NotificationId notification_id) {
  Dialog *d = get_dialog_force(dialog_id, "remove_message_notification");
  if (d == nullptr) {
    LOG(ERROR) << "Can't find " << dialog_id;
    return;
  }
  if (d->message_notification_group.group_id != group_id && d->mention_notification_group.group_id != group_id) {
    LOG(ERROR) << "There is no " << group_id << " in " << dialog_id;
    return;
  }
  if (notification_id == NotificationId::max() || !notification_id.is_valid()) {
    return;  // there can be no notification with this ID
  }

  bool from_mentions = d->mention_notification_group.group_id == group_id;
  if (d->new_secret_chat_notification_id.is_valid()) {
    if (!from_mentions && d->new_secret_chat_notification_id == notification_id) {
      return remove_new_secret_chat_notification(d, false);
    }
    return;
  }

  auto it = d->notification_id_to_message_id.find(notification_id);
  if (it != d->notification_id_to_message_id.end()) {
    auto m = get_message(d, it->second);
    CHECK(m != nullptr);
    CHECK(m->notification_id == notification_id);
    CHECK(!m->message_id.is_scheduled());
    if (is_from_mention_notification_group(d, m) == from_mentions && is_message_notification_active(d, m)) {
      remove_message_notification_id(d, m, false, false);
    }
    return;
  }

  if (G()->parameters().use_message_db) {
    G()->td_db()->get_messages_db_async()->get_messages_from_notification_id(
        dialog_id, NotificationId(notification_id.get() + 1), 1,
        PromiseCreator::lambda([dialog_id, from_mentions, notification_id,
                                actor_id = actor_id(this)](vector<MessagesDbDialogMessage> result) {
          send_closure(actor_id, &MessagesManager::do_remove_message_notification, dialog_id, from_mentions,
                       notification_id, std::move(result));
        }));
  }
}

void MessagesManager::on_get_message_public_forwards(int32 total_count,
                                                     vector<tl_object_ptr<telegram_api::Message>> &&messages,
                                                     Promise<td_api::object_ptr<td_api::foundMessages>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Receive " << messages.size() << " forwarded messages";
  vector<td_api::object_ptr<td_api::message>> result;
  FullMessageId last_full_message_id;
  for (auto &message : messages) {
    auto dialog_id = get_message_dialog_id(message);
    auto new_full_message_id = on_get_message(std::move(message), false, dialog_id.get_type() == DialogType::Channel,
                                              false, false, false, "get message public forwards");
    if (new_full_message_id != FullMessageId()) {
      CHECK(dialog_id == new_full_message_id.get_dialog_id());
      result.push_back(get_message_object(new_full_message_id, "on_get_message_public_forwards"));
      CHECK(result.back() != nullptr);
      last_full_message_id = new_full_message_id;
    } else {
      total_count--;
    }
  }
  if (total_count < static_cast<int32>(result.size())) {
    LOG(ERROR) << "Receive " << result.size() << " valid messages out of " << total_count << " in " << messages.size()
               << " messages";
    total_count = static_cast<int32>(result.size());
  }
  string next_offset;
  if (!result.empty()) {
    auto m = get_message(last_full_message_id);
    CHECK(m != nullptr);
    next_offset = PSTRING() << m->date << "," << last_full_message_id.get_dialog_id().get() << ","
                            << m->message_id.get_server_message_id().get();
  }

  promise.set_value(td_api::make_object<td_api::foundMessages>(total_count, std::move(result), next_offset));
}

void MessagesManager::on_get_affected_history(DialogId dialog_id, AffectedHistoryQuery query,
                                              bool get_affected_messages, AffectedHistory affected_history,
                                              Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (affected_history.pts_count_ > 0) {
    if (get_affected_messages) {
      affected_history.pts_count_ = 0;
    }
    auto update_promise = affected_history.is_final_ ? std::move(promise) : Promise<Unit>();
    if (dialog_id.get_type() == DialogType::Channel) {
      add_pending_channel_update(dialog_id, make_tl_object<dummyUpdate>(), affected_history.pts_,
                                 affected_history.pts_count_, std::move(update_promise), "on_get_affected_history");
    } else {
      td_->updates_manager_->add_pending_pts_update(make_tl_object<dummyUpdate>(), affected_history.pts_,
                                                    affected_history.pts_count_, Time::now(), std::move(update_promise),
                                                    "on_get_affected_history");
    }
  } else if (affected_history.is_final_) {
    promise.set_value(Unit());
  }

  if (!affected_history.is_final_) {
    run_affected_history_query_until_complete(dialog_id, std::move(query), get_affected_messages, std::move(promise));
  }
}

bool MessagesManager::is_message_notification_disabled(const Dialog *d, const Message *m) const {
  CHECK(d != nullptr);
  CHECK(m != nullptr);

  if (!has_incoming_notification(d->dialog_id, m) || td_->auth_manager_->is_bot()) {
    return true;
  }
  if (m->is_from_scheduled && d->dialog_id != get_my_dialog_id() &&
      G()->shared_config().get_option_boolean("disable_sent_scheduled_message_notifications")) {
    return true;
  }
  if (m->forward_info != nullptr && m->forward_info->is_imported) {
    return true;
  }

  switch (m->content->get_type()) {
    case MessageContentType::ChatDeleteHistory:
    case MessageContentType::ChatMigrateTo:
    case MessageContentType::Unsupported:
    case MessageContentType::ExpiredPhoto:
    case MessageContentType::ExpiredVideo:
    case MessageContentType::PassportDataSent:
    case MessageContentType::PassportDataReceived:
      VLOG(notifications) << "Disable notification for " << m->message_id << " in " << d->dialog_id
                          << " with content of type " << m->content->get_type();
      return true;
    case MessageContentType::ContactRegistered:
      if (m->disable_notification) {
        return true;
      }
      break;
    default:
      break;
  }

  return is_dialog_message_notification_disabled(d->dialog_id, m->date);
}

void MessagesManager::on_update_dialog_is_pinned(FolderId folder_id, DialogId dialog_id, bool is_pinned) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive pin of invalid " << dialog_id;
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_is_pinned");
  if (d == nullptr) {
    LOG(INFO) << "Can't apply updateDialogPinned in " << folder_id << " with unknown " << dialog_id;
    on_update_pinned_dialogs(folder_id);
    return;
  }
  if (d->order == DEFAULT_ORDER) {
    // the chat can't be pinned or is already unpinned
    // don't change it's folder_id
    LOG(INFO) << "Can't apply updateDialogPinned in " << folder_id << " with " << dialog_id;
    return;
  }

  auto *list = get_dialog_list(DialogListId(folder_id));
  CHECK(list != nullptr);
  if (!list->are_pinned_dialogs_inited_) {
    return;
  }

  set_dialog_folder_id(d, folder_id);
  set_dialog_is_pinned(DialogListId(folder_id), d, is_pinned);
}

bool MessagesManager::is_active_message_reply_info(DialogId dialog_id, const MessageReplyInfo &info) const {
  if (info.is_empty()) {
    return false;
  }
  if (dialog_id.get_type() != DialogType::Channel) {
    return false;
  }

  if (!info.is_comment) {
    return true;
  }
  if (!is_broadcast_channel(dialog_id)) {
    return true;
  }

  auto channel_id = dialog_id.get_channel_id();
  if (!td_->contacts_manager_->get_channel_has_linked_channel(channel_id)) {
    return false;
  }

  auto linked_channel_id = td_->contacts_manager_->get_channel_linked_channel_id(channel_id);
  if (!linked_channel_id.is_valid()) {
    // keep the comment button while linked channel is unknown
    send_closure_later(G()->contacts_manager(), &ContactsManager::load_channel_full, channel_id, false, Promise<Unit>(),
                       "is_active_message_reply_info");
    return true;
  }

  return linked_channel_id == info.channel_id;
}

void MessagesManager::delete_pending_message_web_page(FullMessageId full_message_id) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  Message *m = get_message(d, full_message_id.get_message_id());
  CHECK(m != nullptr);

  MessageContent *content = m->content.get();
  CHECK(has_message_content_web_page(content));
  unregister_message_content(td_, content, full_message_id, "delete_pending_message_web_page");
  remove_message_content_web_page(content);
  register_message_content(td_, content, full_message_id, "delete_pending_message_web_page");

  // don't need to send an updateMessageContent, because the web page was pending

  on_message_changed(d, m, false, "delete_pending_message_web_page");
}

int32 MessagesManager::get_message_flags(const Message *m) {
  int32 flags = 0;
  if (m->reply_to_message_id.is_valid()) {
    flags |= SEND_MESSAGE_FLAG_IS_REPLY;
  }
  if (m->disable_web_page_preview) {
    flags |= SEND_MESSAGE_FLAG_DISABLE_WEB_PAGE_PREVIEW;
  }
  if (m->reply_markup != nullptr) {
    flags |= SEND_MESSAGE_FLAG_HAS_REPLY_MARKUP;
  }
  if (m->disable_notification) {
    flags |= SEND_MESSAGE_FLAG_DISABLE_NOTIFICATION;
  }
  if (m->from_background) {
    flags |= SEND_MESSAGE_FLAG_FROM_BACKGROUND;
  }
  if (m->clear_draft) {
    flags |= SEND_MESSAGE_FLAG_CLEAR_DRAFT;
  }
  if (m->message_id.is_scheduled()) {
    flags |= SEND_MESSAGE_FLAG_HAS_SCHEDULE_DATE;
  }
  return flags;
}

tl_object_ptr<telegram_api::InputDialogPeer> MessagesManager::get_input_dialog_peer(DialogId dialog_id,
                                                                                    AccessRights access_rights) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
    case DialogType::None:
      return make_tl_object<telegram_api::inputDialogPeer>(get_input_peer(dialog_id, access_rights));
    case DialogType::SecretChat:
      return nullptr;
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void MessagesManager::add_random_id_to_message_id_correspondence(Dialog *d, int64 random_id, MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(d->dialog_id.get_type() == DialogType::SecretChat);
  CHECK(message_id.is_valid());
  auto it = d->random_id_to_message_id.find(random_id);
  if (it == d->random_id_to_message_id.end() || it->second < message_id) {
    LOG(INFO) << "Add correspondence from random_id " << random_id << " to " << message_id << " in " << d->dialog_id;
    d->random_id_to_message_id[random_id] = message_id;
  }
}

vector<FolderId> MessagesManager::get_dialog_list_folder_ids(const DialogList &list) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (list.dialog_list_id.is_folder()) {
    return {list.dialog_list_id.get_folder_id()};
  }
  if (list.dialog_list_id.is_filter()) {
    auto dialog_filter_id = list.dialog_list_id.get_filter_id();
    return get_dialog_filter_folder_ids(get_dialog_filter(dialog_filter_id));
  }
  UNREACHABLE();
  return {};
}

void MessagesManager::send_update_chat_message_sender(const Dialog *d) {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(d != nullptr);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_message_sender";
  send_closure(
      G()->td(), &Td::send_update,
      td_api::make_object<td_api::updateChatMessageSender>(d->dialog_id.get(), get_default_message_sender_object(d)));
}

MessagesManager::Message *MessagesManager::get_message_force(FullMessageId full_message_id, const char *source) {
  Dialog *d = get_dialog_force(full_message_id.get_dialog_id(), source);
  if (d == nullptr) {
    return nullptr;
  }

  return get_message_force(d, full_message_id.get_message_id(), source);
}

const MessagesManager::Message *MessagesManager::get_message(FullMessageId full_message_id) const {
  const Dialog *d = get_dialog(full_message_id.get_dialog_id());
  if (d == nullptr) {
    return nullptr;
  }

  return get_message(d, full_message_id.get_message_id());
}

bool MessagesManager::is_deleted_secret_chat(DialogId dialog_id) const {
  return is_deleted_secret_chat(get_dialog(dialog_id));
}

void MessagesManager::tear_down() {
  parent_.reset();
}
}  // namespace td
