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

class SearchMessagesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  string query_;
  DialogId sender_dialog_id_;
  MessageId from_message_id_;
  int32 offset_;
  int32 limit_;
  MessageSearchFilter filter_;
  MessageId top_thread_message_id_;
  int64 random_id_;
  bool handle_errors_ = true;

 public:
  explicit SearchMessagesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &query, DialogId sender_dialog_id, MessageId from_message_id, int32 offset,
            int32 limit, MessageSearchFilter filter, MessageId top_thread_message_id, int64 random_id) {
    auto input_peer = dialog_id.is_valid() ? td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read)
                                           : make_tl_object<telegram_api::inputPeerEmpty>();
    CHECK(input_peer != nullptr);

    dialog_id_ = dialog_id;
    query_ = query;
    sender_dialog_id_ = sender_dialog_id;
    from_message_id_ = from_message_id;
    offset_ = offset;
    limit_ = limit;
    filter_ = filter;
    top_thread_message_id_ = top_thread_message_id;
    random_id_ = random_id;

    if (filter == MessageSearchFilter::UnreadMention) {
      send_query(G()->net_query_creator().create(
          telegram_api::messages_getUnreadMentions(std::move(input_peer), from_message_id.get_server_message_id().get(),
                                                   offset, limit, std::numeric_limits<int32>::max(), 0)));
    } else if (top_thread_message_id.is_valid() && query.empty() && !sender_dialog_id.is_valid() &&
               filter == MessageSearchFilter::Empty) {
      handle_errors_ = dialog_id.get_type() != DialogType::Channel ||
                       td_->contacts_manager_->get_channel_type(dialog_id.get_channel_id()) !=
                           ContactsManager::ChannelType::Broadcast;
      send_query(G()->net_query_creator().create(telegram_api::messages_getReplies(
          std::move(input_peer), top_thread_message_id.get_server_message_id().get(),
          from_message_id.get_server_message_id().get(), 0, offset, limit, std::numeric_limits<int32>::max(), 0, 0)));
    } else {
      int32 flags = 0;
      tl_object_ptr<telegram_api::InputPeer> sender_input_peer;
      if (sender_dialog_id.is_valid()) {
        flags |= telegram_api::messages_search::FROM_ID_MASK;
        sender_input_peer = td_->messages_manager_->get_input_peer(sender_dialog_id, AccessRights::Know);
        CHECK(sender_input_peer != nullptr);
      }
      if (top_thread_message_id.is_valid()) {
        flags |= telegram_api::messages_search::TOP_MSG_ID_MASK;
      }

      send_query(G()->net_query_creator().create(telegram_api::messages_search(
          flags, std::move(input_peer), query, std::move(sender_input_peer),
          top_thread_message_id.get_server_message_id().get(), get_input_messages_filter(filter), 0,
          std::numeric_limits<int32>::max(), from_message_id.get_server_message_id().get(), offset, limit,
          std::numeric_limits<int32>::max(), 0, 0)));
    }
  }

  void on_result(BufferSlice packet) final {
    static_assert(std::is_same<telegram_api::messages_getUnreadMentions::ReturnType,
                               telegram_api::messages_search::ReturnType>::value,
                  "");
    static_assert(
        std::is_same<telegram_api::messages_getReplies::ReturnType, telegram_api::messages_search::ReturnType>::value,
        "");
    auto result_ptr = fetch_result<telegram_api::messages_search>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "SearchMessagesQuery");
    td_->messages_manager_->get_channel_difference_if_needed(
        dialog_id_, std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(), dialog_id = dialog_id_,
                                query = std::move(query_), sender_dialog_id = sender_dialog_id_,
                                from_message_id = from_message_id_, offset = offset_, limit = limit_, filter = filter_,
                                top_thread_message_id = top_thread_message_id_, random_id = random_id_,
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_dialog_messages_search_result, dialog_id, query,
                         sender_dialog_id, from_message_id, offset, limit, filter, top_thread_message_id, random_id,
                         info.total_count, std::move(info.messages), std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    if (handle_errors_) {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SearchMessagesQuery");
    }
    td_->messages_manager_->on_failed_dialog_messages_search(dialog_id_, random_id_);
    promise_.set_error(std::move(status));
  }
};

template <class T, class It>
vector<MessageId> MessagesManager::get_message_history_slice(const T &begin, It it, const T &end,
                                                             MessageId from_message_id, int32 offset, int32 limit) {
  int32 left_offset = -offset;
  int32 left_limit = limit + offset;
  while (left_offset > 0 && it != end) {
    ++it;
    left_offset--;
    left_limit++;
  }

  vector<MessageId> message_ids;
  while (left_limit > 0 && it != begin) {
    --it;
    left_limit--;
    message_ids.push_back(*it);
  }
  return message_ids;
}

std::pair<DialogId, vector<MessageId>> MessagesManager::get_message_thread_history(
    DialogId dialog_id, MessageId message_id, MessageId from_message_id, int32 offset, int32 limit, int64 &random_id,
    Promise<Unit> &&promise) {
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }
  if (limit > MAX_GET_HISTORY) {
    limit = MAX_GET_HISTORY;
  }
  if (offset > 0) {
    promise.set_error(Status::Error(400, "Parameter offset must be non-positive"));
    return {};
  }
  if (offset <= -MAX_GET_HISTORY) {
    promise.set_error(Status::Error(400, "Parameter offset must be greater than -100"));
    return {};
  }
  if (offset < -limit) {
    promise.set_error(Status::Error(400, "Parameter offset must be greater than or equal to -limit"));
    return {};
  }
  bool is_limit_increased = false;
  if (limit == -offset) {
    limit++;
    is_limit_increased = true;
  }
  CHECK(0 < limit && limit <= MAX_GET_HISTORY);
  CHECK(-limit < offset && offset <= 0);

  Dialog *d = get_dialog_force(dialog_id, "get_message_thread_history");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return {};
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return {};
  }
  if (dialog_id.get_type() != DialogType::Channel) {
    promise.set_error(Status::Error(400, "Can't get message thread history in the chat"));
    return {};
  }

  if (from_message_id == MessageId() || from_message_id.get() > MessageId::max().get()) {
    from_message_id = MessageId::max();
  }
  if (!from_message_id.is_valid()) {
    promise.set_error(Status::Error(400, "Parameter from_message_id must be identifier of a chat message or 0"));
    return {};
  }

  FullMessageId top_thread_full_message_id;
  {
    Message *m = get_message_force(d, message_id, "get_message_thread_history 1");
    if (m == nullptr) {
      promise.set_error(Status::Error(400, "Message not found"));
      return {};
    }

    auto r_top_thread_full_message_id = get_top_thread_full_message_id(dialog_id, m);
    if (r_top_thread_full_message_id.is_error()) {
      promise.set_error(r_top_thread_full_message_id.move_as_error());
      return {};
    }
    top_thread_full_message_id = r_top_thread_full_message_id.move_as_ok();

    if (!top_thread_full_message_id.get_message_id().is_valid()) {
      CHECK(m->reply_info.is_comment);
      get_message_thread(
          dialog_id, message_id,
          PromiseCreator::lambda([promise = std::move(promise)](Result<MessageThreadInfo> &&result) mutable {
            if (result.is_error()) {
              promise.set_error(result.move_as_error());
            } else {
              promise.set_value(Unit());
            }
          }));
      return {};
    }
  }

  if (random_id != 0) {
    // request has already been sent before
    auto it = found_dialog_messages_.find(random_id);
    CHECK(it != found_dialog_messages_.end());
    auto result = std::move(it->second.second);
    found_dialog_messages_.erase(it);

    auto dialog_id_it = found_dialog_messages_dialog_id_.find(random_id);
    if (dialog_id_it != found_dialog_messages_dialog_id_.end()) {
      dialog_id = dialog_id_it->second;
      found_dialog_messages_dialog_id_.erase(dialog_id_it);

      d = get_dialog(dialog_id);
      CHECK(d != nullptr);
    }
    if (dialog_id != top_thread_full_message_id.get_dialog_id()) {
      promise.set_error(Status::Error(500, "Receive messages in an unexpected chat"));
      return {};
    }

    auto yet_unsent_it = d->yet_unsent_thread_message_ids.find(top_thread_full_message_id.get_message_id());
    if (yet_unsent_it != d->yet_unsent_thread_message_ids.end()) {
      const std::set<MessageId> &message_ids = yet_unsent_it->second;
      auto merge_message_ids = get_message_history_slice(message_ids.begin(), message_ids.lower_bound(from_message_id),
                                                         message_ids.end(), from_message_id, offset, limit);
      vector<MessageId> new_result(result.size() + merge_message_ids.size());
      std::merge(result.begin(), result.end(), merge_message_ids.begin(), merge_message_ids.end(), new_result.begin(),
                 std::greater<>());
      result = std::move(new_result);
    }

    Message *top_m = get_message_force(d, top_thread_full_message_id.get_message_id(), "get_message_thread_history 2");
    if (top_m != nullptr && !top_m->local_thread_message_ids.empty()) {
      vector<MessageId> &message_ids = top_m->local_thread_message_ids;
      vector<MessageId> merge_message_ids;
      while (true) {
        merge_message_ids = get_message_history_slice(
            message_ids.begin(), std::lower_bound(message_ids.begin(), message_ids.end(), from_message_id),
            message_ids.end(), from_message_id, offset, limit);
        bool found_deleted = false;
        for (auto local_message_id : merge_message_ids) {
          Message *local_m = get_message_force(d, local_message_id, "get_message_thread_history 3");
          if (local_m == nullptr) {
            auto local_it = std::lower_bound(message_ids.begin(), message_ids.end(), local_message_id);
            CHECK(local_it != message_ids.end() && *local_it == local_message_id);
            message_ids.erase(local_it);
            found_deleted = true;
          }
        }
        if (!found_deleted) {
          break;
        }
        on_message_changed(d, top_m, false, "get_message_thread_history");
      }
      vector<MessageId> new_result(result.size() + merge_message_ids.size());
      std::merge(result.begin(), result.end(), merge_message_ids.begin(), merge_message_ids.end(), new_result.begin(),
                 std::greater<>());
      result = std::move(new_result);
    }

    if (is_limit_increased) {
      limit--;
    }

    std::reverse(result.begin(), result.end());
    result = get_message_history_slice(result.begin(), std::lower_bound(result.begin(), result.end(), from_message_id),
                                       result.end(), from_message_id, offset, limit);

    LOG(INFO) << "Return " << result.size() << " messages in result to getMessageThreadHistory";

    promise.set_value(Unit());
    return {dialog_id, std::move(result)};
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || found_dialog_messages_.find(random_id) != found_dialog_messages_.end());
  found_dialog_messages_[random_id];  // reserve place for result

  td_->create_handler<SearchMessagesQuery>(std::move(promise))
      ->send(dialog_id, string(), DialogId(), from_message_id.get_next_server_message_id(), offset, limit,
             MessageSearchFilter::Empty, message_id, random_id);
  return {};
}

std::pair<int32, vector<MessageId>> MessagesManager::search_dialog_messages(
    DialogId dialog_id, const string &query, const td_api::object_ptr<td_api::MessageSender> &sender,
    MessageId from_message_id, int32 offset, int32 limit, MessageSearchFilter filter, MessageId top_thread_message_id,
    int64 &random_id, bool use_db, Promise<Unit> &&promise) {
  if (random_id != 0) {
    // request has already been sent before
    auto it = found_dialog_messages_.find(random_id);
    if (it != found_dialog_messages_.end()) {
      CHECK(found_dialog_messages_dialog_id_.count(random_id) == 0);
      auto result = std::move(it->second);
      found_dialog_messages_.erase(it);
      promise.set_value(Unit());
      return result;
    }
    random_id = 0;
  }
  LOG(INFO) << "Search messages with query \"" << query << "\" in " << dialog_id << " sent by "
            << oneline(to_string(sender)) << " in thread of " << top_thread_message_id << " filtered by " << filter
            << " from " << from_message_id << " with offset " << offset << " and limit " << limit;

  std::pair<int32, vector<MessageId>> result;
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return result;
  }
  if (limit > MAX_SEARCH_MESSAGES) {
    limit = MAX_SEARCH_MESSAGES;
  }
  if (limit <= -offset) {
    promise.set_error(Status::Error(400, "Parameter limit must be greater than -offset"));
    return result;
  }
  if (offset > 0) {
    promise.set_error(Status::Error(400, "Parameter offset must be non-positive"));
    return result;
  }

  if (from_message_id.get() > MessageId::max().get()) {
    from_message_id = MessageId::max();
  }

  if (!from_message_id.is_valid() && from_message_id != MessageId()) {
    promise.set_error(Status::Error(400, "Parameter from_message_id must be identifier of a chat message or 0"));
    return result;
  }
  from_message_id = from_message_id.get_next_server_message_id();

  const Dialog *d = get_dialog_force(dialog_id, "search_dialog_messages");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return result;
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return result;
  }

  auto r_sender_dialog_id = get_message_sender_dialog_id(td_, sender, true, true);
  if (r_sender_dialog_id.is_error()) {
    promise.set_error(r_sender_dialog_id.move_as_error());
    return result;
  }
  auto sender_dialog_id = r_sender_dialog_id.move_as_ok();
  if (sender_dialog_id != DialogId() && !have_input_peer(sender_dialog_id, AccessRights::Know)) {
    promise.set_error(Status::Error(400, "Invalid message sender specified"));
    return result;
  }
  if (sender_dialog_id == dialog_id && is_broadcast_channel(dialog_id)) {
    sender_dialog_id = DialogId();
  }

  if (filter == MessageSearchFilter::FailedToSend && sender_dialog_id.is_valid()) {
    if (sender_dialog_id != get_my_dialog_id()) {
      promise.set_value(Unit());
      return result;
    }
    sender_dialog_id = DialogId();
  }

  if (top_thread_message_id != MessageId()) {
    if (!top_thread_message_id.is_valid() || !top_thread_message_id.is_server()) {
      promise.set_error(Status::Error(400, "Invalid message thread ID specified"));
      return result;
    }
    if (dialog_id.get_type() != DialogType::Channel || is_broadcast_channel(dialog_id)) {
      promise.set_error(Status::Error(400, "Can't filter by message thread ID in the chat"));
      return result;
    }
  }

  if (sender_dialog_id.get_type() == DialogType::SecretChat) {
    promise.set_value(Unit());
    return result;
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || found_dialog_messages_.find(random_id) != found_dialog_messages_.end());
  found_dialog_messages_[random_id];  // reserve place for result

  if (filter == MessageSearchFilter::UnreadMention) {
    if (!query.empty()) {
      promise.set_error(Status::Error(400, "Non-empty query is unsupported with the specified filter"));
      return result;
    }
    if (sender_dialog_id.is_valid()) {
      promise.set_error(Status::Error(400, "Filtering by sender is unsupported with the specified filter"));
      return result;
    }
    if (top_thread_message_id != MessageId()) {
      promise.set_error(Status::Error(400, "Filtering by message thread is unsupported with the specified filter"));
      return result;
    }
  }

  // Trying to use database
  if (use_db && query.empty() && G()->parameters().use_message_db && filter != MessageSearchFilter::Empty &&
      !sender_dialog_id.is_valid() && top_thread_message_id == MessageId()) {
    MessageId first_db_message_id = get_first_database_message_id_by_index(d, filter);
    int32 message_count = d->message_count_by_index[message_search_filter_index(filter)];
    auto fixed_from_message_id = from_message_id;
    if (fixed_from_message_id == MessageId()) {
      fixed_from_message_id = MessageId::max();
    }
    LOG(INFO) << "Search messages in " << dialog_id << " from " << fixed_from_message_id << ", have up to "
              << first_db_message_id << ", message_count = " << message_count;
    if ((first_db_message_id < fixed_from_message_id || (first_db_message_id == fixed_from_message_id && offset < 0)) &&
        message_count != -1) {
      LOG(INFO) << "Search messages in database in " << dialog_id << " from " << fixed_from_message_id
                << " and with limit " << limit;
      auto new_promise = PromiseCreator::lambda(
          [random_id, dialog_id, fixed_from_message_id, first_db_message_id, filter, offset, limit,
           promise = std::move(promise)](Result<vector<MessagesDbDialogMessage>> r_messages) mutable {
            send_closure(G()->messages_manager(), &MessagesManager::on_search_dialog_messages_db_result, random_id,
                         dialog_id, fixed_from_message_id, first_db_message_id, filter, offset, limit,
                         std::move(r_messages), std::move(promise));
          });
      MessagesDbMessagesQuery db_query;
      db_query.dialog_id = dialog_id;
      db_query.filter = filter;
      db_query.from_message_id = fixed_from_message_id;
      db_query.offset = offset;
      db_query.limit = limit;
      G()->td_db()->get_messages_db_async()->get_messages(db_query, std::move(new_promise));
      return result;
    }
  }
  if (filter == MessageSearchFilter::FailedToSend) {
    promise.set_value(Unit());
    return result;
  }

  LOG(DEBUG) << "Search messages on server in " << dialog_id << " with query \"" << query << "\" from "
             << sender_dialog_id << " in thread of " << top_thread_message_id << " from " << from_message_id
             << " and with limit " << limit;

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      td_->create_handler<SearchMessagesQuery>(std::move(promise))
          ->send(dialog_id, query, sender_dialog_id, from_message_id, offset, limit, filter, top_thread_message_id,
                 random_id);
      break;
    case DialogType::SecretChat:
      if (filter == MessageSearchFilter::UnreadMention || filter == MessageSearchFilter::Pinned) {
        promise.set_value(Unit());
      } else {
        promise.set_error(Status::Error(500, "Search messages in secret chats is not supported"));
      }
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
      promise.set_error(Status::Error(500, "Search messages is not supported"));
  }
  return result;
}

std::pair<int32, vector<FullMessageId>> MessagesManager::search_call_messages(MessageId from_message_id, int32 limit,
                                                                              bool only_missed, int64 &random_id,
                                                                              bool use_db, Promise<Unit> &&promise) {
  if (random_id != 0) {
    // request has already been sent before
    auto it = found_call_messages_.find(random_id);
    if (it != found_call_messages_.end()) {
      auto result = std::move(it->second);
      found_call_messages_.erase(it);
      promise.set_value(Unit());
      return result;
    }
    random_id = 0;
  }
  LOG(INFO) << "Search call messages from " << from_message_id << " with limit " << limit;

  std::pair<int32, vector<FullMessageId>> result;
  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return result;
  }
  if (limit > MAX_SEARCH_MESSAGES) {
    limit = MAX_SEARCH_MESSAGES;
  }

  if (from_message_id.get() > MessageId::max().get()) {
    from_message_id = MessageId::max();
  }

  if (!from_message_id.is_valid() && from_message_id != MessageId()) {
    promise.set_error(Status::Error(400, "Parameter from_message_id must be identifier of a chat message or 0"));
    return result;
  }
  from_message_id = from_message_id.get_next_server_message_id();

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || found_call_messages_.find(random_id) != found_call_messages_.end());
  found_call_messages_[random_id];  // reserve place for result

  auto filter = only_missed ? MessageSearchFilter::MissedCall : MessageSearchFilter::Call;

  if (use_db && G()->parameters().use_message_db) {
    // try to use database
    MessageId first_db_message_id =
        calls_db_state_.first_calls_database_message_id_by_index[call_message_search_filter_index(filter)];
    int32 message_count = calls_db_state_.message_count_by_index[call_message_search_filter_index(filter)];
    auto fixed_from_message_id = from_message_id;
    if (fixed_from_message_id == MessageId()) {
      fixed_from_message_id = MessageId::max();
    }
    CHECK(fixed_from_message_id.is_valid() && fixed_from_message_id.is_server());
    LOG(INFO) << "Search call messages from " << fixed_from_message_id << ", have up to " << first_db_message_id
              << ", message_count = " << message_count;
    if (first_db_message_id < fixed_from_message_id && message_count != -1) {
      LOG(INFO) << "Search messages in database from " << fixed_from_message_id << " and with limit " << limit;

      MessagesDbCallsQuery db_query;
      db_query.filter = filter;
      db_query.from_unique_message_id = fixed_from_message_id.get_server_message_id().get();
      db_query.limit = limit;
      G()->td_db()->get_messages_db_async()->get_calls(
          db_query, PromiseCreator::lambda([random_id, first_db_message_id, filter, promise = std::move(promise)](
                                               Result<MessagesDbCallsResult> calls_result) mutable {
            send_closure(G()->messages_manager(), &MessagesManager::on_messages_db_calls_result,
                         std::move(calls_result), random_id, first_db_message_id, filter, std::move(promise));
          }));
      return result;
    }
  }

  LOG(DEBUG) << "Search call messages on server from " << from_message_id << " and with limit " << limit;
  td_->create_handler<SearchMessagesQuery>(std::move(promise))
      ->send(DialogId(), "", DialogId(), from_message_id, 0, limit, filter, MessageId(), random_id);
  return result;
}

// must not call get_dialog_filter
bool MessagesManager::do_update_list_last_pinned_dialog_date(DialogList &list) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (list.last_pinned_dialog_date_ == MAX_DIALOG_DATE) {
    return false;
  }
  if (!list.are_pinned_dialogs_inited_) {
    return false;
  }

  DialogDate max_dialog_date = MIN_DIALOG_DATE;
  for (auto &pinned_dialog : list.pinned_dialogs_) {
    if (!have_dialog(pinned_dialog.get_dialog_id())) {
      break;
    }

    max_dialog_date = pinned_dialog;
  }
  if (list.pinned_dialogs_.empty() || max_dialog_date == list.pinned_dialogs_.back()) {
    max_dialog_date = MAX_DIALOG_DATE;
  }
  if (list.last_pinned_dialog_date_ < max_dialog_date) {
    LOG(INFO) << "Update last pinned dialog date in " << list.dialog_list_id << " from "
              << list.last_pinned_dialog_date_ << " to " << max_dialog_date;
    list.last_pinned_dialog_date_ = max_dialog_date;
    return true;
  }
  return false;
}

int32 MessagesManager::get_dialog_total_count(const DialogList &list) const {
  int32 sponsored_dialog_count = 0;
  if (sponsored_dialog_id_.is_valid() && list.dialog_list_id == DialogListId(FolderId::main())) {
    auto d = get_dialog(sponsored_dialog_id_);
    CHECK(d != nullptr);
    if (is_dialog_sponsored(d)) {
      sponsored_dialog_count = 1;
    }
  }
  if (list.server_dialog_total_count_ != -1 && list.secret_chat_total_count_ != -1) {
    return std::max(list.server_dialog_total_count_ + list.secret_chat_total_count_,
                    list.in_memory_dialog_total_count_) +
           sponsored_dialog_count;
  }
  if (list.list_last_dialog_date_ == MAX_DIALOG_DATE) {
    return list.in_memory_dialog_total_count_ + sponsored_dialog_count;
  }
  return list.in_memory_dialog_total_count_ + sponsored_dialog_count + 1;
}

void MessagesManager::preload_older_messages(const Dialog *d, MessageId min_message_id) {
  CHECK(d != nullptr);
  CHECK(min_message_id.is_valid());
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  /*
    if (d->first_remote_message_id == -1) {
      // nothing left to preload from server
      return;
    }
  */
  MessagesConstIterator p(d, min_message_id);
  int32 limit = MAX_GET_HISTORY * 3 / 10 + 1;
  while (*p != nullptr && limit-- > 0) {
    min_message_id = (*p)->message_id;
    --p;
  }
  if (limit > 0) {
    // need to preload some old messages
    LOG(INFO) << "Preloading older before " << min_message_id;
    load_messages_impl(d, min_message_id, 0, MAX_GET_HISTORY / 2, 3, false, Promise<Unit>());
  }
}

string MessagesManager::get_dialog_title(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->get_user_title(dialog_id.get_user_id());
    case DialogType::Chat:
      return td_->contacts_manager_->get_chat_title(dialog_id.get_chat_id());
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_title(dialog_id.get_channel_id());
    case DialogType::SecretChat:
      return td_->contacts_manager_->get_secret_chat_title(dialog_id.get_secret_chat_id());
    case DialogType::None:
    default:
      UNREACHABLE();
      return string();
  }
}

void MessagesManager::remove_message_file_sources(DialogId dialog_id, const Message *m) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto file_ids = get_message_content_file_ids(m->content.get(), td_);
  if (file_ids.empty()) {
    return;
  }

  // do not create file_source_id for messages without file_ids
  auto file_source_id = get_message_file_source_id(FullMessageId(dialog_id, m->message_id));
  if (file_source_id.is_valid()) {
    for (auto file_id : file_ids) {
      td_->file_manager_->remove_file_source(file_id, file_source_id);
    }
  }
}

void MessagesManager::on_save_dialog_to_database(DialogId dialog_id, bool can_reuse_notification_group, bool success) {
  LOG(INFO) << "Successfully saved " << dialog_id << " to database";

  if (success && can_reuse_notification_group && !G()->close_flag()) {
    auto d = get_dialog(dialog_id);
    CHECK(d != nullptr);
    try_reuse_notification_group(d->message_notification_group);
    try_reuse_notification_group(d->mention_notification_group);
  }

  // TODO erase some events from binlog
}

void MessagesManager::on_update_dialog_theme_name(DialogId dialog_id, string theme_name) {
  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive theme in invalid " << dialog_id;
    return;
  }
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_theme_name");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  set_dialog_theme_name(d, std::move(theme_name));
}

tl_object_ptr<td_api::chats> MessagesManager::get_chats_object(int32 total_count, const vector<DialogId> &dialog_ids) {
  if (total_count == -1) {
    total_count = narrow_cast<int32>(dialog_ids.size());
  }
  return td_api::make_object<td_api::chats>(total_count,
                                            transform(dialog_ids, [](DialogId dialog_id) { return dialog_id.get(); }));
}

const MessagesManager::DialogFolder *MessagesManager::get_dialog_folder(FolderId folder_id) const {
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

void MessagesManager::on_updated_dialog_folder_id(DialogId dialog_id, uint64 generation) {
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  delete_log_event(d->set_folder_id_log_event_id, generation, "set chat folder");
}

void MessagesManager::on_get_dialog_message_by_date_fail(int64 random_id) {
  auto erased_count = get_dialog_message_by_date_results_.erase(random_id);
  CHECK(erased_count > 0);
}
}  // namespace td
