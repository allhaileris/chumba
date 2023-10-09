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
#include "td/telegram/WebPageId.h"

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

void MessagesManager::on_get_channel_difference(
    DialogId dialog_id, int32 request_pts, int32 request_limit,
    tl_object_ptr<telegram_api::updates_ChannelDifference> &&difference_ptr) {
  LOG(INFO) << "----- END  GET CHANNEL DIFFERENCE----- for " << dialog_id;
  CHECK(active_get_channel_differencies_.count(dialog_id) == 1);
  active_get_channel_differencies_.erase(dialog_id);
  auto d = get_dialog_force(dialog_id, "on_get_channel_difference");

  if (difference_ptr == nullptr) {
    bool have_access = have_input_peer(dialog_id, AccessRights::Read);
    if (have_access) {
      if (d == nullptr) {
        force_create_dialog(dialog_id, "on_get_channel_difference failed");
      }
      auto &delay = channel_get_difference_retry_timeouts_[dialog_id];
      if (delay == 0) {
        delay = 1;
      }
      channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), delay);
      delay *= 2;
      if (delay > 60) {
        delay = Random::fast(60, 80);
      }
    } else {
      after_get_channel_difference(dialog_id, false);
    }
    return;
  }

  channel_get_difference_retry_timeouts_.erase(dialog_id);

  LOG(INFO) << "Receive result of getChannelDifference for " << dialog_id << " with pts = " << request_pts
            << " and limit = " << request_limit << ": " << to_string(difference_ptr);

  bool have_new_messages = false;
  switch (difference_ptr->get_id()) {
    case telegram_api::updates_channelDifferenceEmpty::ID:
      if (d == nullptr) {
        // no need to create the dialog
        after_get_channel_difference(dialog_id, true);
        return;
      }
      break;
    case telegram_api::updates_channelDifference::ID: {
      auto difference = static_cast<telegram_api::updates_channelDifference *>(difference_ptr.get());
      have_new_messages = !difference->new_messages_.empty();
      td_->contacts_manager_->on_get_users(std::move(difference->users_), "updates.channelDifference");
      td_->contacts_manager_->on_get_chats(std::move(difference->chats_), "updates.channelDifference");
      break;
    }
    case telegram_api::updates_channelDifferenceTooLong::ID: {
      auto difference = static_cast<telegram_api::updates_channelDifferenceTooLong *>(difference_ptr.get());
      have_new_messages = difference->dialog_->get_id() == telegram_api::dialog::ID && !difference->messages_.empty();
      td_->contacts_manager_->on_get_users(std::move(difference->users_), "updates.channelDifferenceTooLong");
      td_->contacts_manager_->on_get_chats(std::move(difference->chats_), "updates.channelDifferenceTooLong");
      break;
    }
    default:
      UNREACHABLE();
  }

  bool need_update_dialog_pos = false;
  if (d == nullptr) {
    if (have_new_messages) {
      CHECK(!being_added_by_new_message_dialog_id_.is_valid());
      being_added_by_new_message_dialog_id_ = dialog_id;
    }
    d = add_dialog(dialog_id, "on_get_channel_difference");
    being_added_by_new_message_dialog_id_ = DialogId();
    need_update_dialog_pos = true;
  }

  int32 cur_pts = d->pts <= 0 ? 1 : d->pts;
  LOG_IF(ERROR, cur_pts != request_pts) << "Channel pts has changed from " << request_pts << " to " << d->pts << " in "
                                        << dialog_id << " during getChannelDifference";

  bool is_final = true;
  int32 timeout = 0;
  switch (difference_ptr->get_id()) {
    case telegram_api::updates_channelDifferenceEmpty::ID: {
      auto difference = move_tl_object_as<telegram_api::updates_channelDifferenceEmpty>(difference_ptr);
      int32 flags = difference->flags_;
      is_final = (flags & CHANNEL_DIFFERENCE_FLAG_IS_FINAL) != 0;
      LOG_IF(ERROR, !is_final) << "Receive channelDifferenceEmpty as result of getChannelDifference with pts = "
                               << request_pts << " and limit = " << request_limit << " in " << dialog_id
                               << ", but it is not final";
      if (flags & CHANNEL_DIFFERENCE_FLAG_HAS_TIMEOUT) {
        timeout = difference->timeout_;
      }

      // bots can receive channelDifferenceEmpty with pts bigger than known pts
      LOG_IF(ERROR, request_pts != difference->pts_ && !td_->auth_manager_->is_bot())
          << "Receive channelDifferenceEmpty as result of getChannelDifference with pts = " << request_pts
          << " and limit = " << request_limit << " in " << dialog_id << ", but pts has changed from " << request_pts
          << " to " << difference->pts_;
      set_channel_pts(d, difference->pts_, "channel difference empty");
      break;
    }
    case telegram_api::updates_channelDifference::ID: {
      auto difference = move_tl_object_as<telegram_api::updates_channelDifference>(difference_ptr);

      int32 flags = difference->flags_;
      is_final = (flags & CHANNEL_DIFFERENCE_FLAG_IS_FINAL) != 0;
      if (flags & CHANNEL_DIFFERENCE_FLAG_HAS_TIMEOUT) {
        timeout = difference->timeout_;
      }

      auto new_pts = difference->pts_;
      if (request_pts >= new_pts && request_pts > 1 && (request_pts > new_pts || !td_->auth_manager_->is_bot())) {
        LOG(ERROR) << "Receive channelDifference as result of getChannelDifference with pts = " << request_pts
                   << " and limit = " << request_limit << " in " << dialog_id << ", but pts has changed from " << d->pts
                   << " to " << new_pts << ". Difference: " << oneline(to_string(difference));
        new_pts = request_pts + 1;
      }

      if (difference->new_messages_.size() > 1) {
        // check that new messages are received in increasing message_id order
        MessageId cur_message_id;
        for (const auto &message : difference->new_messages_) {
          auto message_id = get_message_id(message, false);
          if (message_id <= cur_message_id) {
            // TODO move to ERROR
            LOG(FATAL) << "Receive " << cur_message_id << " after " << message_id << " in channelDifference of "
                       << dialog_id << " with pts " << request_pts << " and limit " << request_limit << ": "
                       << to_string(difference);
            after_get_channel_difference(dialog_id, false);
            return;
          }
          cur_message_id = message_id;
        }
      }

      process_get_channel_difference_updates(dialog_id, new_pts, std::move(difference->new_messages_),
                                             std::move(difference->other_updates_));

      set_channel_pts(d, new_pts, "channel difference");
      break;
    }
    case telegram_api::updates_channelDifferenceTooLong::ID: {
      auto difference = move_tl_object_as<telegram_api::updates_channelDifferenceTooLong>(difference_ptr);

      tl_object_ptr<telegram_api::dialog> dialog;
      switch (difference->dialog_->get_id()) {
        case telegram_api::dialog::ID:
          dialog = telegram_api::move_object_as<telegram_api::dialog>(difference->dialog_);
          break;
        case telegram_api::dialogFolder::ID:
          return after_get_channel_difference(dialog_id, false);
        default:
          UNREACHABLE();
          return;
      }

      CHECK(dialog != nullptr);
      if ((dialog->flags_ & telegram_api::dialog::PTS_MASK) == 0) {
        LOG(ERROR) << "Receive " << dialog_id << " without pts";
        return after_get_channel_difference(dialog_id, false);
      }

      int32 flags = difference->flags_;
      is_final = (flags & CHANNEL_DIFFERENCE_FLAG_IS_FINAL) != 0;
      if (flags & CHANNEL_DIFFERENCE_FLAG_HAS_TIMEOUT) {
        timeout = difference->timeout_;
      }

      auto new_pts = dialog->pts_;
      if (request_pts > new_pts - request_limit) {
        LOG(ERROR) << "Receive channelDifferenceTooLong as result of getChannelDifference with pts = " << request_pts
                   << " and limit = " << request_limit << " in " << dialog_id << ", but pts has changed from " << d->pts
                   << " to " << new_pts << ". Difference: " << oneline(to_string(difference));
        if (request_pts >= new_pts) {
          new_pts = request_pts + 1;
        }
      }

      set_dialog_folder_id(d, FolderId((dialog->flags_ & DIALOG_FLAG_HAS_FOLDER_ID) != 0 ? dialog->folder_id_ : 0));

      on_update_dialog_notify_settings(dialog_id, std::move(dialog->notify_settings_),
                                       "updates.channelDifferenceTooLong");

      bool is_marked_as_unread = dialog->unread_mark_;
      if (is_marked_as_unread != d->is_marked_as_unread) {
        set_dialog_is_marked_as_unread(d, is_marked_as_unread);
      }

      update_dialog_draft_message(d, get_draft_message(td_->contacts_manager_.get(), std::move(dialog->draft_)), true,
                                  false);

      on_get_channel_dialog(dialog_id, MessageId(ServerMessageId(dialog->top_message_)),
                            MessageId(ServerMessageId(dialog->read_inbox_max_id_)), dialog->unread_count_,
                            dialog->unread_mentions_count_, MessageId(ServerMessageId(dialog->read_outbox_max_id_)),
                            std::move(difference->messages_));
      update_dialog_pos(d, "updates.channelDifferenceTooLong");

      if (!td_->auth_manager_->is_bot()) {
        // set is_pinned only after updating dialog pos to ensure that order is initialized
        bool is_pinned = (dialog->flags_ & DIALOG_FLAG_IS_PINNED) != 0;
        bool was_pinned = is_dialog_pinned(DialogListId(d->folder_id), dialog_id);
        if (is_pinned != was_pinned) {
          set_dialog_is_pinned(DialogListId(d->folder_id), d, is_pinned);
        }
      }

      set_channel_pts(d, new_pts, "channel difference too long");
      break;
    }
    default:
      UNREACHABLE();
  }

  if (need_update_dialog_pos) {
    update_dialog_pos(d, "on_get_channel_difference");
  }

  if (!is_final) {
    LOG_IF(ERROR, timeout > 0) << "Have timeout in nonfinal ChannelDifference in " << dialog_id;
    get_channel_difference(dialog_id, d->pts, true, "on_get_channel_difference");
    return;
  }

  LOG_IF(ERROR, timeout == 0) << "Have no timeout in final ChannelDifference in " << dialog_id;
  if (timeout > 0 && d->is_opened) {
    channel_get_difference_timeout_.add_timeout_in(dialog_id.get(), timeout);
  }
  after_get_channel_difference(dialog_id, true);
}

Result<MessageId> MessagesManager::send_bot_start_message(UserId bot_user_id, DialogId dialog_id,
                                                          const string &parameter) {
  LOG(INFO) << "Begin to send bot start message to " << dialog_id;
  if (td_->auth_manager_->is_bot()) {
    return Status::Error(400, "Bot can't send start message to another bot");
  }

  TRY_RESULT(bot_data, td_->contacts_manager_->get_bot_data(bot_user_id));

  Dialog *d = get_dialog_force(dialog_id, "send_bot_start_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  bool is_chat_with_bot = false;
  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (dialog_id.get_user_id() != bot_user_id) {
        return Status::Error(400, "Can't send start message to a private chat other than chat with the bot");
      }
      is_chat_with_bot = true;
      break;
    case DialogType::Chat: {
      if (!bot_data.can_join_groups) {
        return Status::Error(400, "Bot can't join groups");
      }

      auto chat_id = dialog_id.get_chat_id();
      if (!td_->contacts_manager_->have_input_peer_chat(chat_id, AccessRights::Write)) {
        return Status::Error(400, "Can't access the chat");
      }
      auto status = td_->contacts_manager_->get_chat_permissions(chat_id);
      if (!status.can_invite_users()) {
        return Status::Error(400, "Need administrator rights to invite a bot to the group chat");
      }
      break;
    }
    case DialogType::Channel: {
      auto channel_id = dialog_id.get_channel_id();
      if (!td_->contacts_manager_->have_input_peer_channel(channel_id, AccessRights::Write)) {
        return Status::Error(400, "Can't access the chat");
      }
      switch (td_->contacts_manager_->get_channel_type(channel_id)) {
        case ContactsManager::ChannelType::Megagroup:
          if (!bot_data.can_join_groups) {
            return Status::Error(400, "The bot can't join groups");
          }
          break;
        case ContactsManager::ChannelType::Broadcast:
          return Status::Error(400, "Bots can't be invited to channel chats. Add them as administrators instead");
        case ContactsManager::ChannelType::Unknown:
        default:
          UNREACHABLE();
      }
      auto status = td_->contacts_manager_->get_channel_permissions(channel_id);
      if (!status.can_invite_users()) {
        return Status::Error(400, "Need administrator rights to invite a bot to the supergroup chat");
      }
      break;
    }
    case DialogType::SecretChat:
      return Status::Error(400, "Can't send bot start message to a secret chat");
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  string text = "/start";
  if (!is_chat_with_bot) {
    text += '@';
    text += bot_data.username;
  }

  vector<MessageEntity> text_entities;
  text_entities.emplace_back(MessageEntity::Type::BotCommand, 0, narrow_cast<int32>(text.size()));
  bool need_update_dialog_pos = false;
  Message *m = get_message_to_send(d, MessageId(), MessageId(), MessageSendOptions(),
                                   create_text_message_content(text, std::move(text_entities), WebPageId()),
                                   &need_update_dialog_pos);
  m->is_bot_start_message = true;

  send_update_new_message(d, m);
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "send_bot_start_message");
  }

  if (parameter.empty() && is_chat_with_bot) {
    save_send_message_log_event(dialog_id, m);
    do_send_message(dialog_id, m);
  } else {
    save_send_bot_start_message_log_event(bot_user_id, dialog_id, parameter, m);
    do_send_bot_start_message(bot_user_id, dialog_id, parameter, m);
  }
  return m->message_id;
}

bool MessagesManager::set_dialog_order(Dialog *d, int64 new_order, bool need_send_update, bool is_loaded_from_database,
                                       const char *source) {
  if (td_->auth_manager_->is_bot()) {
    return false;
  }

  CHECK(d != nullptr);
  DialogId dialog_id = d->dialog_id;
  DialogDate old_date(d->order, dialog_id);
  DialogDate new_date(new_order, dialog_id);

  if (old_date == new_date) {
    LOG(INFO) << "Order of " << d->dialog_id << " from " << d->folder_id << " is still " << new_order << " from "
              << source;
  } else {
    LOG(INFO) << "Update order of " << dialog_id << " from " << d->folder_id << " from " << d->order << " to "
              << new_order << " from " << source;
  }

  auto folder_ptr = get_dialog_folder(d->folder_id);
  LOG_CHECK(folder_ptr != nullptr) << is_inited_ << ' ' << G()->close_flag() << ' ' << dialog_id << ' ' << d->folder_id
                                   << ' ' << is_loaded_from_database << ' ' << td_->auth_manager_->is_authorized()
                                   << ' ' << td_->auth_manager_->was_authorized() << ' ' << source;
  auto &folder = *folder_ptr;
  if (old_date == new_date) {
    if (new_order == DEFAULT_ORDER) {
      // first addition of a new left dialog
      if (folder.ordered_dialogs_.insert(new_date).second) {
        for (const auto &dialog_list : dialog_lists_) {
          if (get_dialog_pinned_order(&dialog_list.second, d->dialog_id) != DEFAULT_ORDER) {
            set_dialog_is_pinned(dialog_list.first, d, false);
          }
        }
      }
    }

    return false;
  }

  auto dialog_positions = get_dialog_positions(d);

  if (folder.ordered_dialogs_.erase(old_date) == 0) {
    LOG_IF(ERROR, d->order != DEFAULT_ORDER) << dialog_id << " not found in the chat list from " << source;
  }

  folder.ordered_dialogs_.insert(new_date);

  bool is_added = (d->order == DEFAULT_ORDER);
  bool is_removed = (new_order == DEFAULT_ORDER);

  d->order = new_order;

  if (is_added) {
    update_dialogs_hints(d);
  }
  update_dialogs_hints_rating(d);

  update_dialog_lists(d, std::move(dialog_positions), need_send_update, is_loaded_from_database, source);

  if (!is_loaded_from_database) {
    auto dialog_type = dialog_id.get_type();
    if (dialog_type == DialogType::Channel && is_added && being_added_dialog_id_ != dialog_id) {
      repair_channel_server_unread_count(d);
      LOG(INFO) << "Schedule getDifference in " << dialog_id.get_channel_id();
      channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
    }
    if (dialog_type == DialogType::Channel && is_removed) {
      remove_all_dialog_notifications(d, false, source);
      remove_all_dialog_notifications(d, true, source);
      clear_active_dialog_actions(dialog_id);
    }
  }

  return true;
}

class DeleteScheduledMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit DeleteScheduledMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, vector<MessageId> &&message_ids) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }
    send_query(G()->net_query_creator().create(telegram_api::messages_deleteScheduledMessages(
        std::move(input_peer), MessagesManager::get_scheduled_server_message_ids(message_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteScheduledMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for DeleteScheduledMessagesQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "DeleteScheduledMessagesQuery")) {
      LOG(ERROR) << "Receive error for delete scheduled messages: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::delete_scheduled_messages_on_server(DialogId dialog_id, vector<MessageId> message_ids,
                                                          uint64 log_event_id, Promise<Unit> &&promise) {
  if (message_ids.empty()) {
    return promise.set_value(Unit());
  }
  LOG(INFO) << "Delete " << format::as_array(message_ids) << " in " << dialog_id << " from server";

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_delete_scheduled_messages_on_server_log_event(dialog_id, message_ids);
  }

  auto new_promise = get_erase_log_event_promise(log_event_id, std::move(promise));
  promise = std::move(new_promise);  // to prevent self-move

  td_->create_handler<DeleteScheduledMessagesQuery>(std::move(promise))->send(dialog_id, std::move(message_ids));
}

void MessagesManager::set_dialog_last_clear_history_date(Dialog *d, int32 date, MessageId last_clear_history_message_id,
                                                         const char *source, bool is_loaded_from_database) {
  CHECK(!last_clear_history_message_id.is_scheduled());

  if (d->last_clear_history_message_id == last_clear_history_message_id && d->last_clear_history_date == date) {
    return;
  }

  LOG(INFO) << "Set " << d->dialog_id << " last clear history date to " << date << " of "
            << last_clear_history_message_id << " from " << source;
  if (d->last_clear_history_message_id.is_valid()) {
    switch (d->dialog_id.get_type()) {
      case DialogType::User:
      case DialogType::Chat:
        last_clear_history_message_id_to_dialog_id_.erase(d->last_clear_history_message_id);
        break;
      case DialogType::Channel:
      case DialogType::SecretChat:
        // nothing to do
        break;
      case DialogType::None:
      default:
        UNREACHABLE();
    }
  }

  d->last_clear_history_date = date;
  d->last_clear_history_message_id = last_clear_history_message_id;
  if (!is_loaded_from_database) {
    on_dialog_updated(d->dialog_id, "set_dialog_last_clear_history_date");
  }

  if (d->last_clear_history_message_id.is_valid()) {
    switch (d->dialog_id.get_type()) {
      case DialogType::User:
      case DialogType::Chat:
        last_clear_history_message_id_to_dialog_id_[d->last_clear_history_message_id] = d->dialog_id;
        break;
      case DialogType::Channel:
      case DialogType::SecretChat:
        // nothing to do
        break;
      case DialogType::None:
      default:
        UNREACHABLE();
    }
  }
}

class UpdateDialogFiltersOrderQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit UpdateDialogFiltersOrderQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const vector<DialogFilterId> &dialog_filter_ids) {
    send_query(G()->net_query_creator().create(telegram_api::messages_updateDialogFiltersOrder(
        transform(dialog_filter_ids, [](auto dialog_filter_id) { return dialog_filter_id.get(); }))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_updateDialogFiltersOrder>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG(INFO) << "Receive result for UpdateDialogFiltersOrderQuery: " << result_ptr.ok();
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::reorder_dialog_filters_on_server(vector<DialogFilterId> dialog_filter_ids) {
  CHECK(!td_->auth_manager_->is_bot());
  are_dialog_filters_being_synchronized_ = true;
  auto promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_filter_ids](Result<Unit> result) mutable {
    send_closure(actor_id, &MessagesManager::on_reorder_dialog_filters, std::move(dialog_filter_ids),
                 result.is_error() ? result.move_as_error() : Status::OK());
  });
  td_->create_handler<UpdateDialogFiltersOrderQuery>(std::move(promise))->send(dialog_filter_ids);
}

vector<DialogId> MessagesManager::sort_dialogs_by_order(const vector<DialogId> &dialog_ids, int32 limit) const {
  CHECK(!td_->auth_manager_->is_bot());
  auto fake_order = static_cast<int64>(dialog_ids.size()) + 1;
  auto dialog_dates = transform(dialog_ids, [this, &fake_order](DialogId dialog_id) {
    const Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);
    auto order = get_dialog_base_order(d);
    if (is_dialog_inited(d) || order != DEFAULT_ORDER) {
      return DialogDate(order, dialog_id);
    }
    // if the dialog is not inited yet, we need to assume that server knows better and the dialog needs to be returned
    return DialogDate(fake_order--, dialog_id);
  });
  if (static_cast<size_t>(limit) >= dialog_dates.size()) {
    std::sort(dialog_dates.begin(), dialog_dates.end());
  } else {
    std::partial_sort(dialog_dates.begin(), dialog_dates.begin() + limit, dialog_dates.end());
    dialog_dates.resize(limit, MAX_DIALOG_DATE);
  }
  while (!dialog_dates.empty() && dialog_dates.back().get_order() == DEFAULT_ORDER) {
    dialog_dates.pop_back();
  }
  return transform(dialog_dates, [](auto dialog_date) { return dialog_date.get_dialog_id(); });
}

void MessagesManager::on_update_dialog_notify_settings(
    DialogId dialog_id, tl_object_ptr<telegram_api::peerNotifySettings> &&peer_notify_settings, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  VLOG(notifications) << "Receive notification settings for " << dialog_id << " from " << source << ": "
                      << to_string(peer_notify_settings);

  DialogNotificationSettings *current_settings = get_dialog_notification_settings(dialog_id, true);
  if (current_settings == nullptr) {
    return;
  }

  const DialogNotificationSettings notification_settings = ::td::get_dialog_notification_settings(
      std::move(peer_notify_settings), current_settings->use_default_disable_pinned_message_notifications,
      current_settings->disable_pinned_message_notifications,
      current_settings->use_default_disable_mention_notifications, current_settings->disable_mention_notifications);
  if (!notification_settings.is_synchronized) {
    return;
  }

  update_dialog_notification_settings(dialog_id, current_settings, notification_settings);
}

void MessagesManager::find_unloadable_messages(const Dialog *d, int32 unload_before_date, const Message *m,
                                               vector<MessageId> &message_ids,
                                               bool &has_left_to_unload_messages) const {
  if (m == nullptr) {
    return;
  }

  find_unloadable_messages(d, unload_before_date, m->left.get(), message_ids, has_left_to_unload_messages);

  if (can_unload_message(d, m)) {
    if (m->last_access_date <= unload_before_date) {
      message_ids.push_back(m->message_id);
    } else {
      has_left_to_unload_messages = true;
    }
  }

  if (has_left_to_unload_messages && m->date > unload_before_date) {
    // we aren't interested in unloading too new messages
    return;
  }

  find_unloadable_messages(d, unload_before_date, m->right.get(), message_ids, has_left_to_unload_messages);
}

void MessagesManager::on_update_channel_max_unavailable_message_id(ChannelId channel_id,
                                                                   MessageId max_unavailable_message_id) {
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive max_unavailable_message_id in invalid " << channel_id;
    return;
  }

  DialogId dialog_id(channel_id);
  CHECK(!max_unavailable_message_id.is_scheduled());
  if (!max_unavailable_message_id.is_valid() && max_unavailable_message_id != MessageId()) {
    LOG(ERROR) << "Receive wrong max_unavailable_message_id: " << max_unavailable_message_id;
    max_unavailable_message_id = MessageId();
  }
  set_dialog_max_unavailable_message_id(dialog_id, max_unavailable_message_id, true,
                                        "on_update_channel_max_unavailable_message_id");
}

void MessagesManager::read_channel_message_content_from_updates(Dialog *d, MessageId message_id) {
  CHECK(d != nullptr);
  if (!message_id.is_valid() || !message_id.is_server()) {
    LOG(ERROR) << "Incoming update tries to read content of " << message_id << " in " << d->dialog_id;
    return;
  }

  Message *m = get_message_force(d, message_id, "read_channel_message_content_from_updates");
  if (m != nullptr) {
    read_message_content(d, m, false, "read_channel_message_content_from_updates");
  } else if (message_id > d->last_new_message_id) {
    get_channel_difference(d->dialog_id, d->pts, true, "read_channel_message_content_from_updates");
  }
}

void MessagesManager::on_update_message_interaction_info(FullMessageId full_message_id, int32 view_count,
                                                         int32 forward_count, bool has_reply_info,
                                                         tl_object_ptr<telegram_api::messageReplies> &&reply_info) {
  if (view_count < 0 || forward_count < 0) {
    LOG(ERROR) << "Receive " << view_count << "/" << forward_count << " interaction counters for " << full_message_id;
    return;
  }
  update_message_interaction_info(full_message_id, view_count, forward_count, has_reply_info, std::move(reply_info));
}

void MessagesManager::find_messages_by_date(const Message *m, int32 min_date, int32 max_date,
                                            vector<MessageId> &message_ids) {
  if (m == nullptr) {
    return;
  }

  if (m->date >= min_date) {
    find_messages_by_date(m->left.get(), min_date, max_date, message_ids);
    if (m->date <= max_date) {
      message_ids.push_back(m->message_id);
    }
  }
  if (m->date <= max_date) {
    find_messages_by_date(m->right.get(), min_date, max_date, message_ids);
  }
}

void MessagesManager::load_dialog_filter(DialogFilterId dialog_filter_id, bool force, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  if (!dialog_filter_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid chat filter identifier specified"));
  }

  auto filter = get_dialog_filter(dialog_filter_id);
  if (filter == nullptr) {
    return promise.set_value(Unit());
  }

  load_dialog_filter(filter, force, std::move(promise));
}

bool MessagesManager::is_message_edited_recently(FullMessageId full_message_id, int32 seconds) {
  if (seconds < 0) {
    return false;
  }
  if (!full_message_id.get_message_id().is_valid()) {
    return false;
  }

  auto m = get_message_force(full_message_id, "is_message_edited_recently");
  if (m == nullptr) {
    return true;
  }

  return m->edit_date >= G()->unix_time() - seconds;
}

MessagesManager::DialogFolder *MessagesManager::get_dialog_folder(FolderId folder_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (folder_id != FolderId::archive()) {
    folder_id = FolderId::main();
  }
  auto it = dialog_folders_.find(folder_id);
  if (it == dialog_folders_.end()) {
    return nullptr;
  }
  return &it->second;
}

Status MessagesManager::close_dialog(DialogId dialog_id) {
  Dialog *d = get_dialog_force(dialog_id, "close_dialog");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  close_dialog(d);
  return Status::OK();
}

int64 MessagesManager::get_next_pinned_dialog_order() {
  current_pinned_dialog_order_++;
  LOG(INFO) << "Assign pinned_order = " << current_pinned_dialog_order_;
  return current_pinned_dialog_order_;
}
}  // namespace td
