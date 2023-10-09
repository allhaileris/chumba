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

void MessagesManager::on_get_history_from_database(DialogId dialog_id, MessageId from_message_id,
                                                   MessageId old_last_database_message_id, int32 offset, int32 limit,
                                                   bool from_the_end, bool only_local,
                                                   vector<MessagesDbDialogMessage> &&messages,
                                                   Promise<Unit> &&promise) {
  CHECK(-limit < offset && offset <= 0);
  CHECK(offset < 0 || from_the_end);
  CHECK(!from_message_id.is_scheduled());
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    LOG(WARNING) << "Ignore result of get_history_from_database in " << dialog_id;
    promise.set_value(Unit());
    return;
  }

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  LOG(INFO) << "Receive " << messages.size() << " history messages from database "
            << (from_the_end ? "from the end " : "") << "in " << dialog_id << " from " << from_message_id
            << " with offset " << offset << " and limit " << limit << ". First database message is "
            << d->first_database_message_id << ", last database message is " << d->last_database_message_id
            << ", have_full_history = " << d->have_full_history;

  if (old_last_database_message_id < d->last_database_message_id && old_last_database_message_id < from_message_id) {
    // new messages where added to the database since the request was sent
    // they should have been received from the database, so we must repeat the request to get them
    if (from_the_end) {
      get_history_from_the_end_impl(d, true, only_local, std::move(promise));
    } else {
      get_history_impl(d, from_message_id, offset, limit, true, only_local, std::move(promise));
    }
    return;
  }

  if (messages.empty() && from_the_end && d->messages == nullptr) {
    if (d->have_full_history) {
      set_dialog_is_empty(d, "on_get_history_from_database empty");
    } else if (d->last_database_message_id.is_valid()) {
      set_dialog_first_database_message_id(d, MessageId(), "on_get_history_from_database empty");
      set_dialog_last_database_message_id(d, MessageId(), "on_get_history_from_database empty");
    }
  }

  bool have_next = false;
  bool need_update = false;
  bool need_update_dialog_pos = false;
  bool added_new_message = false;
  MessageId first_added_message_id;
  MessageId last_added_message_id;
  Message *next_message = nullptr;
  Dependencies dependencies;
  bool is_first = true;
  bool had_full_history = d->have_full_history;
  auto debug_first_database_message_id = d->first_database_message_id;
  auto debug_last_message_id = d->last_message_id;
  auto debug_last_new_message_id = d->last_new_message_id;
  auto last_received_message_id = MessageId::max();
  size_t pos = 0;
  for (auto &message_slice : messages) {
    if (!d->first_database_message_id.is_valid() && !d->have_full_history) {
      break;
    }
    auto message = parse_message(dialog_id, message_slice.message_id, message_slice.data, false);
    if (message == nullptr) {
      if (d->have_full_history) {
        d->have_full_history = false;
        d->is_empty = false;  // just in case
        on_dialog_updated(dialog_id, "drop have_full_history in on_get_history_from_database");
      }
      break;
    }
    if (message->message_id >= last_received_message_id) {
      // TODO move to ERROR
      LOG(FATAL) << "Receive " << message->message_id << " after " << last_received_message_id
                 << " from database in the history of " << dialog_id << " from " << from_message_id << " with offset "
                 << offset << ", limit " << limit << ", from_the_end = " << from_the_end;
      break;
    }
    last_received_message_id = message->message_id;

    if (message->message_id < d->first_database_message_id) {
      if (d->have_full_history) {
        LOG(ERROR) << "Have full history in the " << dialog_id << " and receive " << message->message_id
                   << " from database, but first database message is " << d->first_database_message_id;
      } else {
        break;
      }
    }
    if (!have_next && (from_the_end || (is_first && offset < -1 && message->message_id <= from_message_id)) &&
        message->message_id < d->last_message_id) {
      // last message in the dialog must be attached to the next local message
      have_next = true;
    }

    message->have_previous = false;
    message->have_next = have_next;
    message->from_database = true;

    auto old_message = get_message(d, message->message_id);
    Message *m = old_message ? old_message
                             : add_message_to_dialog(d, std::move(message), false, &need_update,
                                                     &need_update_dialog_pos, "on_get_history_from_database");
    if (m != nullptr) {
      first_added_message_id = m->message_id;
      if (!have_next) {
        last_added_message_id = m->message_id;
      }
      if (old_message == nullptr) {
        add_message_dependencies(dependencies, m);
        added_new_message = true;
      } else if (m->message_id != from_message_id) {
        added_new_message = true;
      }
      if (next_message != nullptr && !next_message->have_previous) {
        LOG_CHECK(m->message_id < next_message->message_id)
            << m->message_id << ' ' << next_message->message_id << ' ' << last_received_message_id << ' ' << dialog_id
            << ' ' << from_message_id << ' ' << offset << ' ' << limit << ' ' << from_the_end << ' ' << only_local
            << ' ' << messages.size() << ' ' << debug_first_database_message_id << ' ' << last_added_message_id << ' '
            << added_new_message << ' ' << pos << ' ' << m << ' ' << next_message << ' ' << old_message << ' '
            << to_string(get_message_object(dialog_id, m, "on_get_history_from_database"))
            << to_string(get_message_object(dialog_id, next_message, "on_get_history_from_database"));
        LOG(INFO) << "Fix have_previous for " << next_message->message_id;
        next_message->have_previous = true;
        attach_message_to_previous(
            d, next_message->message_id,
            (PSLICE() << "on_get_history_from_database 1 " << m->message_id << ' ' << from_message_id << ' ' << offset
                      << ' ' << limit << ' ' << d->first_database_message_id << ' ' << d->have_full_history << ' '
                      << pos)
                .c_str());
      }

      have_next = true;
      next_message = m;
    }
    is_first = false;
    pos++;
  }
  resolve_dependencies_force(td_, dependencies, "on_get_history_from_database");

  if (from_the_end && !last_added_message_id.is_valid() && d->first_database_message_id.is_valid() &&
      !d->have_full_history) {
    if (last_received_message_id <= d->first_database_message_id) {
      // database definitely has no messages from first_database_message_id to last_database_message_id; drop them
      set_dialog_first_database_message_id(d, MessageId(), "on_get_history_from_database 8");
      set_dialog_last_database_message_id(d, MessageId(), "on_get_history_from_database 9");
    } else {
      CHECK(last_received_message_id.is_valid());
      // if a message was received, but wasn't added, then it is likely to be already deleted
      // if it is less than d->last_database_message_id, then we can adjust d->last_database_message_id and
      // try again database search without chance to loop
      if (last_received_message_id < d->last_database_message_id) {
        set_dialog_last_database_message_id(d, last_received_message_id, "on_get_history_from_database 12");

        get_history_from_the_end_impl(d, true, only_local, std::move(promise));
        return;
      }

      if (limit > 1) {
        // we expected to have messages [first_database_message_id, last_database_message_id] in the database, but
        // received no messages or newer messages [last_received_message_id, ...], none of which can be added
        // first_database_message_id and last_database_message_id are very wrong, so it is better to drop them,
        // pretending that the database has no usable messages
        if (last_received_message_id == MessageId::max()) {
          LOG(ERROR) << "Receive no usable messages in " << dialog_id
                     << " from database from the end, but expected messages from " << d->last_database_message_id
                     << " up to " << d->first_database_message_id;
        } else {
          LOG(ERROR) << "Receive " << messages.size() << " unusable messages up to " << last_received_message_id
                     << " in " << dialog_id << " from database from the end, but expected messages from "
                     << d->last_database_message_id << " up to " << d->first_database_message_id;
        }
        set_dialog_first_database_message_id(d, MessageId(), "on_get_history_from_database 13");
        set_dialog_last_database_message_id(d, MessageId(), "on_get_history_from_database 14");
      }
    }
  }

  if (!added_new_message && !only_local && dialog_id.get_type() != DialogType::SecretChat) {
    if (from_the_end) {
      from_message_id = MessageId();
    }
    load_messages_impl(d, from_message_id, offset, limit, 1, false, std::move(promise));
    return;
  }

  if (from_the_end && last_added_message_id.is_valid()) {
    // CHECK(d->first_database_message_id.is_valid());
    // CHECK(last_added_message_id >= d->first_database_message_id);
    if ((had_full_history || d->have_full_history) && !d->last_new_message_id.is_valid() &&
        (last_added_message_id.is_server() || d->dialog_id.get_type() == DialogType::SecretChat)) {
      LOG(ERROR) << "Trying to hard fix " << d->dialog_id << " last new message to " << last_added_message_id
                 << " from on_get_history_from_database 2";
      d->last_new_message_id = last_added_message_id;
      on_dialog_updated(d->dialog_id, "on_get_history_from_database 3");
    }
    if (last_added_message_id > d->last_message_id && d->last_new_message_id.is_valid()) {
      set_dialog_last_message_id(d, last_added_message_id, "on_get_history_from_database 4");
      need_update_dialog_pos = true;
    }
    if (last_added_message_id != d->last_database_message_id && d->last_new_message_id.is_valid()) {
      auto debug_last_database_message_id = d->last_database_message_id;
      set_dialog_last_database_message_id(d, last_added_message_id, "on_get_history_from_database 5");
      if (last_added_message_id < d->first_database_message_id || !d->first_database_message_id.is_valid()) {
        CHECK(next_message != nullptr);
        LOG_CHECK(had_full_history || d->have_full_history)
            << had_full_history << ' ' << d->have_full_history << ' ' << next_message->message_id << ' '
            << last_added_message_id << ' ' << d->first_database_message_id << ' ' << debug_first_database_message_id
            << ' ' << d->last_database_message_id << ' ' << debug_last_database_message_id << ' ' << dialog_id << ' '
            << d->last_new_message_id << ' ' << debug_last_new_message_id << ' ' << d->last_message_id << ' '
            << debug_last_message_id;
        CHECK(next_message->message_id <= d->last_database_message_id);
        LOG(ERROR) << "Fix first database message in " << dialog_id << " from " << d->first_database_message_id
                   << " to " << next_message->message_id;
        set_dialog_first_database_message_id(d, next_message->message_id, "on_get_history_from_database 6");
      }
    }
  }
  if (first_added_message_id.is_valid() && first_added_message_id != d->first_database_message_id &&
      last_received_message_id < d->first_database_message_id && d->last_new_message_id.is_valid() &&
      !d->have_full_history) {
    CHECK(first_added_message_id > d->first_database_message_id);
    set_dialog_first_database_message_id(d, first_added_message_id, "on_get_history_from_database 10");
    if (d->last_database_message_id < d->first_database_message_id) {
      set_dialog_last_database_message_id(d, d->first_database_message_id, "on_get_history_from_database 11");
    }
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "on_get_history_from_database 7");
  }

  promise.set_value(Unit());
}

Result<InputMessageContent> MessagesManager::process_input_message_content(
    DialogId dialog_id, tl_object_ptr<td_api::InputMessageContent> &&input_message_content) {
  if (input_message_content == nullptr) {
    return Status::Error(400, "Can't send message without content");
  }

  if (input_message_content->get_id() == td_api::inputMessageForwarded::ID) {
    // for sendMessageAlbum/editMessageMedia/addLocalMessage
    auto input_message = td_api::move_object_as<td_api::inputMessageForwarded>(input_message_content);
    TRY_RESULT(copy_options, process_message_copy_options(dialog_id, std::move(input_message->copy_options_)));
    if (!copy_options.send_copy) {
      return Status::Error(400, "Can't use forwarded message");
    }

    DialogId from_dialog_id(input_message->from_chat_id_);
    Dialog *from_dialog = get_dialog_force(from_dialog_id, "send_message copy");
    if (from_dialog == nullptr) {
      return Status::Error(400, "Chat to copy message from not found");
    }
    if (!have_input_peer(from_dialog_id, AccessRights::Read)) {
      return Status::Error(400, "Can't access the chat to copy message from");
    }
    if (from_dialog_id.get_type() == DialogType::SecretChat) {
      return Status::Error(400, "Can't copy message from secret chats");
    }
    MessageId message_id = get_persistent_message_id(from_dialog, MessageId(input_message->message_id_));

    const Message *copied_message = get_message_force(from_dialog, message_id, "process_input_message_content");
    if (copied_message == nullptr) {
      return Status::Error(400, "Can't find message to copy");
    }
    if (!can_forward_message(from_dialog_id, copied_message)) {
      return Status::Error(400, "Can't copy message");
    }
    if (!can_save_message(from_dialog_id, copied_message) && !td_->auth_manager_->is_bot()) {
      return Status::Error(400, "Message copying is restricted");
    }

    unique_ptr<MessageContent> content = dup_message_content(td_, dialog_id, copied_message->content.get(),
                                                             MessageContentDupType::Copy, std::move(copy_options));
    if (content == nullptr) {
      return Status::Error(400, "Can't copy message content");
    }

    return InputMessageContent(std::move(content), get_message_disable_web_page_preview(copied_message), false, 0,
                               UserId(), copied_message->send_emoji);
  }

  TRY_RESULT(content, get_input_message_content(dialog_id, std::move(input_message_content), td_));

  if (content.ttl < 0 || content.ttl > MAX_PRIVATE_MESSAGE_TTL) {
    return Status::Error(400, "Invalid message content TTL specified");
  }
  if (content.ttl > 0 && dialog_id.get_type() != DialogType::User) {
    return Status::Error(400, "Message content TTL can be specified only in private chats");
  }

  if (dialog_id != DialogId()) {
    TRY_STATUS(can_send_message_content(dialog_id, content.content.get(), false, td_));
  }

  return std::move(content);
}

void MessagesManager::update_list_last_dialog_date(DialogList &list) {
  CHECK(!td_->auth_manager_->is_bot());
  auto old_dialog_total_count = get_dialog_total_count(list);
  auto old_last_dialog_date = list.list_last_dialog_date_;
  if (!do_update_list_last_dialog_date(list, get_dialog_list_folder_ids(list))) {
    LOG(INFO) << "Don't need to update last dialog date in " << list.dialog_list_id;
    return;
  }

  for (auto it = std::upper_bound(list.pinned_dialogs_.begin(), list.pinned_dialogs_.end(), old_last_dialog_date);
       it != list.pinned_dialogs_.end() && *it <= list.list_last_dialog_date_; ++it) {
    auto dialog_id = it->get_dialog_id();
    auto d = get_dialog(dialog_id);
    CHECK(d != nullptr);
    send_update_chat_position(list.dialog_list_id, d, "update_list_last_dialog_date");
  }

  bool is_list_further_loaded = list.list_last_dialog_date_ == MAX_DIALOG_DATE;
  for (auto folder_id : get_dialog_list_folder_ids(list)) {
    const auto &folder = *get_dialog_folder(folder_id);
    for (auto it = folder.ordered_dialogs_.upper_bound(old_last_dialog_date);
         it != folder.ordered_dialogs_.end() && *it <= folder.folder_last_dialog_date_; ++it) {
      if (it->get_order() == DEFAULT_ORDER) {
        break;
      }
      auto dialog_id = it->get_dialog_id();
      if (get_dialog_pinned_order(&list, dialog_id) == DEFAULT_ORDER) {
        auto d = get_dialog(dialog_id);
        CHECK(d != nullptr);
        if (is_dialog_in_list(d, list.dialog_list_id)) {
          send_update_chat_position(list.dialog_list_id, d, "update_list_last_dialog_date 2");
          is_list_further_loaded = true;
        }
      }
    }
  }

  if (list.list_last_dialog_date_ == MAX_DIALOG_DATE) {
    recalc_unread_count(list.dialog_list_id, old_dialog_total_count, true);
  }

  LOG(INFO) << "After updating last dialog date in " << list.dialog_list_id << " to " << list.list_last_dialog_date_
            << " have is_list_further_loaded == " << is_list_further_loaded << " and " << list.load_list_queries_.size()
            << " pending load list queries";
  if (is_list_further_loaded && !list.load_list_queries_.empty()) {
    auto promises = std::move(list.load_list_queries_);
    list.load_list_queries_.clear();
    for (auto &promise : promises) {
      promise.set_value(Unit());
    }
  }
}

void MessagesManager::update_dialog_unmute_timeout(Dialog *d, bool &old_use_default, int32 &old_mute_until,
                                                   bool new_use_default, int32 new_mute_until) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  if (old_use_default == new_use_default && old_mute_until == new_mute_until) {
    return;
  }
  CHECK(d != nullptr);
  CHECK(old_mute_until >= 0);

  schedule_dialog_unmute(d->dialog_id, new_use_default, new_mute_until);

  bool was_muted = (old_use_default ? get_scope_mute_until(d->dialog_id) : old_mute_until) != 0;
  bool is_muted = (new_use_default ? get_scope_mute_until(d->dialog_id) : new_mute_until) != 0;
  if (was_muted != is_muted && need_unread_counter(d->order)) {
    auto unread_count = d->server_unread_count + d->local_unread_count;
    if (unread_count != 0 || d->is_marked_as_unread) {
      for (auto &list : get_dialog_lists(d)) {
        if (unread_count != 0 && list.is_message_unread_count_inited_) {
          int32 delta = was_muted ? -unread_count : unread_count;
          list.unread_message_muted_count_ += delta;
          send_update_unread_message_count(list, d->dialog_id, true, "update_dialog_unmute_timeout");
        }
        if (list.is_dialog_unread_count_inited_) {
          int32 delta = was_muted ? -1 : 1;
          list.unread_dialog_muted_count_ += delta;
          if (unread_count == 0 && d->is_marked_as_unread) {
            list.unread_dialog_muted_marked_count_ += delta;
          }
          send_update_unread_chat_count(list, d->dialog_id, true, "update_dialog_unmute_timeout");
        }
      }
    }
  }

  old_use_default = new_use_default;
  old_mute_until = new_mute_until;

  if (was_muted != is_muted && !dialog_filters_.empty()) {
    update_dialog_lists(d, get_dialog_positions(d), true, false, "update_dialog_unmute_timeout");
  }
}

void MessagesManager::update_message_interaction_info(FullMessageId full_message_id, int32 view_count,
                                                      int32 forward_count, bool has_reply_info,
                                                      tl_object_ptr<telegram_api::messageReplies> &&reply_info) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "update_message_interaction_info");
  if (d == nullptr) {
    return;
  }
  auto message_id = full_message_id.get_message_id();
  Message *m = get_message_force(d, message_id, "update_message_interaction_info");
  if (m == nullptr) {
    LOG(INFO) << "Ignore message interaction info about unknown " << full_message_id;
    if (!message_id.is_scheduled() && message_id > d->last_new_message_id &&
        dialog_id.get_type() == DialogType::Channel) {
      get_channel_difference(dialog_id, d->pts, true, "update_message_interaction_info");
    }
    return;
  }

  if (view_count < 0) {
    view_count = m->view_count;
  }
  if (forward_count < 0) {
    forward_count = m->forward_count;
  }
  bool is_empty_reply_info = reply_info == nullptr;
  MessageReplyInfo new_reply_info(td_, std::move(reply_info), td_->auth_manager_->is_bot());
  if (new_reply_info.is_empty() && !is_empty_reply_info) {
    has_reply_info = false;
  }

  if (update_message_interaction_info(dialog_id, m, view_count, forward_count, has_reply_info,
                                      std::move(new_reply_info), "update_message_interaction_info")) {
    on_message_changed(d, m, true, "update_message_interaction_info");
  }
}

Result<vector<MessagesDbDialogMessage>> MessagesManager::do_get_message_notifications_from_database_force(
    Dialog *d, bool from_mentions, NotificationId from_notification_id, MessageId from_message_id, int32 limit) {
  CHECK(G()->parameters().use_message_db);
  CHECK(!from_message_id.is_scheduled());

  auto *db = G()->td_db()->get_messages_db_sync();
  if (!from_mentions) {
    CHECK(from_message_id > d->last_read_inbox_message_id);
    VLOG(notifications) << "Trying to load " << limit << " messages with notifications in "
                        << d->message_notification_group.group_id << '/' << d->dialog_id << " from "
                        << from_notification_id;
    return db->get_messages_from_notification_id(d->dialog_id, from_notification_id, limit);
  } else {
    VLOG(notifications) << "Trying to load " << limit << " messages with unread mentions in "
                        << d->mention_notification_group.group_id << '/' << d->dialog_id << " from " << from_message_id;

    // ignore first_db_message_id, notifications can be nonconsecutive
    MessagesDbMessagesQuery db_query;
    db_query.dialog_id = d->dialog_id;
    db_query.filter = MessageSearchFilter::UnreadMention;
    db_query.from_message_id = from_message_id;
    db_query.offset = 0;
    db_query.limit = limit;
    return db->get_messages(db_query);
  }
}

Status MessagesManager::can_get_media_timestamp_link(DialogId dialog_id, const Message *m) {
  if (m == nullptr) {
    return Status::Error(400, "Message not found");
  }

  if (dialog_id.get_type() != DialogType::Channel) {
    auto forward_info = m->forward_info.get();
    if (!can_message_content_have_media_timestamp(m->content.get()) || forward_info == nullptr ||
        forward_info->is_imported || is_forward_info_sender_hidden(forward_info) ||
        !forward_info->message_id.is_valid() || !m->forward_info->message_id.is_server() ||
        !forward_info->sender_dialog_id.is_valid() ||
        forward_info->sender_dialog_id.get_type() != DialogType::Channel) {
      return Status::Error(400, "Message links are available only for messages in supergroups and channel chats");
    }
    return Status::OK();
  }

  if (m->message_id.is_yet_unsent()) {
    return Status::Error(400, "Message is not sent yet");
  }
  if (m->message_id.is_scheduled()) {
    return Status::Error(400, "Message is scheduled");
  }
  if (!m->message_id.is_server()) {
    return Status::Error(400, "Message is local");
  }
  return Status::OK();
}

void MessagesManager::reorder_dialog_filters(vector<DialogFilterId> dialog_filter_ids, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());

  for (auto dialog_filter_id : dialog_filter_ids) {
    auto dialog_filter = get_dialog_filter(dialog_filter_id);
    if (dialog_filter == nullptr) {
      return promise.set_error(Status::Error(400, "Chat filter not found"));
    }
  }
  std::unordered_set<DialogFilterId, DialogFilterIdHash> new_dialog_filter_ids_set(dialog_filter_ids.begin(),
                                                                                   dialog_filter_ids.end());
  if (new_dialog_filter_ids_set.size() != dialog_filter_ids.size()) {
    return promise.set_error(Status::Error(400, "Duplicate chat filters in the new list"));
  }

  if (set_dialog_filters_order(dialog_filters_, dialog_filter_ids)) {
    save_dialog_filters();
    send_update_chat_filters();

    synchronize_dialog_filters();
  }
  promise.set_value(Unit());
}

bool MessagesManager::is_message_auto_read(DialogId dialog_id, bool is_outgoing) const {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      auto user_id = dialog_id.get_user_id();
      if (user_id == td_->contacts_manager_->get_my_id()) {
        return true;
      }
      if (is_outgoing && td_->contacts_manager_->is_user_bot(user_id) &&
          !td_->contacts_manager_->is_user_support(user_id)) {
        return true;
      }
      return false;
    }
    case DialogType::Chat:
      // TODO auto_read message content and messages sent to group with bots only
      return false;
    case DialogType::Channel:
      return is_outgoing && is_broadcast_channel(dialog_id);
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
      return false;
    default:
      UNREACHABLE();
      return false;
  }
}

void MessagesManager::send_update_secret_chats_with_user_theme(const Dialog *d) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (d->dialog_id.get_type() != DialogType::User) {
    return;
  }

  td_->contacts_manager_->for_each_secret_chat_with_user(
      d->dialog_id.get_user_id(), [this, user_d = d](SecretChatId secret_chat_id) {
        DialogId dialog_id(secret_chat_id);
        auto secret_chat_d = get_dialog(dialog_id);  // must not create the dialog
        if (secret_chat_d != nullptr && secret_chat_d->is_update_new_chat_sent) {
          send_closure(G()->td(), &Td::send_update,
                       td_api::make_object<td_api::updateChatTheme>(dialog_id.get(), user_d->theme_name));
        }
      });
}

void MessagesManager::try_restore_dialog_reply_markup(Dialog *d, const Message *m) {
  if (!d->need_restore_reply_markup || td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(!m->message_id.is_scheduled());
  if (m->had_reply_markup) {
    LOG(INFO) << "Restore deleted reply markup in " << d->dialog_id;
    set_dialog_reply_markup(d, MessageId());
  } else if (m->reply_markup != nullptr && m->reply_markup->type != ReplyMarkup::Type::InlineKeyboard &&
             m->reply_markup->is_personal) {
    LOG(INFO) << "Restore reply markup in " << d->dialog_id << " to " << m->message_id;
    set_dialog_reply_markup(d, m->message_id);
  }
}

void MessagesManager::on_update_dialog_is_marked_as_unread(DialogId dialog_id, bool is_marked_as_unread) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive marking as unread of invalid " << dialog_id;
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_is_marked_as_unread");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (is_marked_as_unread == d->is_marked_as_unread) {
    return;
  }

  set_dialog_is_marked_as_unread(d, is_marked_as_unread);
}

void MessagesManager::do_read_history_on_server(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  for (auto top_thread_message_id : d->updated_read_history_message_ids) {
    if (!top_thread_message_id.is_valid()) {
      read_history_on_server_impl(d, MessageId());
    } else {
      read_message_thread_history_on_server_impl(d, top_thread_message_id, MessageId());
    }
  }
  reset_to_empty(d->updated_read_history_message_ids);
}

void MessagesManager::on_reorder_dialog_filters(vector<DialogFilterId> dialog_filter_ids, Status result) {
  CHECK(!td_->auth_manager_->is_bot());
  if (result.is_error()) {
    // TODO rollback dialog_filters_ changes if error isn't 429
  } else {
    if (set_dialog_filters_order(server_dialog_filters_, std::move(dialog_filter_ids))) {
      save_dialog_filters();
    }
  }

  are_dialog_filters_being_synchronized_ = false;
  synchronize_dialog_filters();
}

tl_object_ptr<td_api::message> MessagesManager::get_dialog_message_by_date_object(int64 random_id) {
  auto it = get_dialog_message_by_date_results_.find(random_id);
  CHECK(it != get_dialog_message_by_date_results_.end());
  auto full_message_id = std::move(it->second);
  get_dialog_message_by_date_results_.erase(it);
  return get_message_object(full_message_id, "get_dialog_message_by_date_object");
}

InputDialogId MessagesManager::get_input_dialog_id(DialogId dialog_id) const {
  auto input_peer = get_input_peer(dialog_id, AccessRights::Read);
  if (input_peer == nullptr || input_peer->get_id() == telegram_api::inputPeerSelf::ID) {
    return InputDialogId(dialog_id);
  } else {
    return InputDialogId(input_peer);
  }
}

DialogId MessagesManager::get_dialog_message_sender(FullMessageId full_message_id) {
  const auto *m = get_message_force(full_message_id, "get_dialog_message_sender");
  if (m == nullptr) {
    return DialogId();
  }
  return get_message_sender(m);
}

void MessagesManager::remove_dialog_pinned_message_notification(Dialog *d, const char *source) {
  set_dialog_pinned_message_notification(d, MessageId(), source);
}
}  // namespace td
