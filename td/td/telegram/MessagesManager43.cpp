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

#include <algorithm>

#include <limits>



#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

class SearchMessagesGlobalQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  string query_;
  int32 offset_date_;
  DialogId offset_dialog_id_;
  MessageId offset_message_id_;
  int32 limit_;
  MessageSearchFilter filter_;
  int32 min_date_;
  int32 max_date_;
  int64 random_id_;

 public:
  explicit SearchMessagesGlobalQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FolderId folder_id, bool ignore_folder_id, const string &query, int32 offset_date,
            DialogId offset_dialog_id, MessageId offset_message_id, int32 limit, MessageSearchFilter filter,
            int32 min_date, int32 max_date, int64 random_id) {
    query_ = query;
    offset_date_ = offset_date;
    offset_dialog_id_ = offset_dialog_id;
    offset_message_id_ = offset_message_id;
    limit_ = limit;
    random_id_ = random_id;
    filter_ = filter;
    min_date_ = min_date;
    max_date_ = max_date;

    auto input_peer = MessagesManager::get_input_peer_force(offset_dialog_id);
    CHECK(input_peer != nullptr);

    int32 flags = 0;
    if (!ignore_folder_id) {
      flags |= telegram_api::messages_searchGlobal::FOLDER_ID_MASK;
    }
    send_query(G()->net_query_creator().create(telegram_api::messages_searchGlobal(
        flags, folder_id.get(), query, get_input_messages_filter(filter), min_date_, max_date_, offset_date_,
        std::move(input_peer), offset_message_id.get_server_message_id().get(), limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_searchGlobal>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "SearchMessagesGlobalQuery");
    td_->messages_manager_->get_channel_differences_if_needed(
        std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(), query = std::move(query_),
                                offset_date = offset_date_, offset_dialog_id = offset_dialog_id_,
                                offset_message_id = offset_message_id_, limit = limit_, filter = std::move(filter_),
                                min_date = min_date_, max_date = max_date_, random_id = random_id_,
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_messages_search_result, query, offset_date,
                         offset_dialog_id, offset_message_id, limit, filter, min_date, max_date, random_id,
                         info.total_count, std::move(info.messages), std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_failed_messages_search(random_id_);
    promise_.set_error(std::move(status));
  }
};

std::pair<int32, vector<FullMessageId>> MessagesManager::search_messages(
    FolderId folder_id, bool ignore_folder_id, const string &query, int32 offset_date, DialogId offset_dialog_id,
    MessageId offset_message_id, int32 limit, MessageSearchFilter filter, int32 min_date, int32 max_date,
    int64 &random_id, Promise<Unit> &&promise) {
  if (random_id != 0) {
    // request has already been sent before
    auto it = found_messages_.find(random_id);
    CHECK(it != found_messages_.end());
    auto result = std::move(it->second);
    found_messages_.erase(it);
    promise.set_value(Unit());
    return result;
  }

  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }
  if (limit > MAX_SEARCH_MESSAGES) {
    limit = MAX_SEARCH_MESSAGES;
  }

  if (offset_date <= 0) {
    offset_date = std::numeric_limits<int32>::max();
  }
  if (!offset_message_id.is_valid()) {
    if (offset_message_id.is_valid_scheduled()) {
      promise.set_error(Status::Error(400, "Parameter offset_message_id can't be a scheduled message identifier"));
      return {};
    }
    offset_message_id = MessageId();
  }
  if (offset_message_id != MessageId() && !offset_message_id.is_server()) {
    promise.set_error(
        Status::Error(400, "Parameter offset_message_id must be identifier of the last found message or 0"));
    return {};
  }

  CHECK(filter != MessageSearchFilter::Call && filter != MessageSearchFilter::MissedCall);
  if (filter == MessageSearchFilter::Mention || filter == MessageSearchFilter::UnreadMention ||
      filter == MessageSearchFilter::FailedToSend || filter == MessageSearchFilter::Pinned) {
    promise.set_error(Status::Error(400, "The filter is not supported"));
    return {};
  }

  if (query.empty() && filter == MessageSearchFilter::Empty) {
    promise.set_value(Unit());
    return {};
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || found_messages_.find(random_id) != found_messages_.end());
  found_messages_[random_id];  // reserve place for result

  LOG(DEBUG) << "Search all messages filtered by " << filter << " with query = \"" << query << "\" from date "
             << offset_date << ", " << offset_dialog_id << ", " << offset_message_id << " and limit " << limit;

  td_->create_handler<SearchMessagesGlobalQuery>(std::move(promise))
      ->send(folder_id, ignore_folder_id, query, offset_date, offset_dialog_id, offset_message_id, limit, filter,
             min_date, max_date, random_id);
  return {};
}

Status MessagesManager::set_pinned_dialogs(DialogListId dialog_list_id, vector<DialogId> dialog_ids) {
  if (td_->auth_manager_->is_bot()) {
    return Status::Error(400, "Bots can't reorder pinned chats");
  }

  int32 dialog_count = 0;
  int32 secret_dialog_count = 0;
  auto dialog_count_limit = get_pinned_dialogs_limit(dialog_list_id);
  for (auto dialog_id : dialog_ids) {
    Dialog *d = get_dialog_force(dialog_id, "set_pinned_dialogs");
    if (d == nullptr) {
      return Status::Error(400, "Chat not found");
    }
    if (!have_input_peer(dialog_id, AccessRights::Read)) {
      return Status::Error(400, "Can't access the chat");
    }
    if (d->order == DEFAULT_ORDER) {
      return Status::Error(400, "The chat can't be pinned");
    }
    if (dialog_list_id.is_folder() && d->folder_id != dialog_list_id.get_folder_id()) {
      return Status::Error(400, "Chat not in the list");
    }
    if (dialog_id.get_type() == DialogType::SecretChat) {
      secret_dialog_count++;
    } else {
      dialog_count++;
    }

    if (dialog_count > dialog_count_limit || secret_dialog_count > dialog_count_limit) {
      return Status::Error(400, "The maximum number of pinned chats exceeded");
    }
  }
  std::unordered_set<DialogId, DialogIdHash> new_pinned_dialog_ids(dialog_ids.begin(), dialog_ids.end());
  if (new_pinned_dialog_ids.size() != dialog_ids.size()) {
    return Status::Error(400, "Duplicate chats in the list of pinned chats");
  }

  auto *list = get_dialog_list(dialog_list_id);
  if (list == nullptr) {
    return Status::Error(400, "Chat list not found");
  }
  if (!list->are_pinned_dialogs_inited_) {
    return Status::Error(400, "Pinned chats must be loaded first");
  }

  auto pinned_dialog_ids = get_pinned_dialog_ids(dialog_list_id);
  if (pinned_dialog_ids == dialog_ids) {
    return Status::OK();
  }
  LOG(INFO) << "Reorder pinned chats in " << dialog_list_id << " from " << pinned_dialog_ids << " to " << dialog_ids;

  auto server_old_dialog_ids = remove_secret_chat_dialog_ids(pinned_dialog_ids);
  auto server_new_dialog_ids = remove_secret_chat_dialog_ids(dialog_ids);

  if (dialog_list_id.is_filter()) {
    CHECK(is_update_chat_filters_sent_);
    auto dialog_filter_id = dialog_list_id.get_filter_id();
    auto old_dialog_filter = get_dialog_filter(dialog_filter_id);
    CHECK(old_dialog_filter != nullptr);
    auto new_dialog_filter = make_unique<DialogFilter>(*old_dialog_filter);
    auto old_pinned_dialog_ids = std::move(new_dialog_filter->pinned_dialog_ids);
    new_dialog_filter->pinned_dialog_ids =
        transform(dialog_ids, [this](DialogId dialog_id) { return get_input_dialog_id(dialog_id); });
    auto is_new_pinned = [&new_pinned_dialog_ids](InputDialogId input_dialog_id) {
      return new_pinned_dialog_ids.count(input_dialog_id.get_dialog_id()) > 0;
    };
    td::remove_if(old_pinned_dialog_ids, is_new_pinned);
    td::remove_if(new_dialog_filter->included_dialog_ids, is_new_pinned);
    td::remove_if(new_dialog_filter->excluded_dialog_ids, is_new_pinned);
    append(new_dialog_filter->included_dialog_ids, old_pinned_dialog_ids);

    TRY_STATUS(new_dialog_filter->check_limits());
    sort_dialog_filter_input_dialog_ids(new_dialog_filter.get(), "set_pinned_dialogs");

    edit_dialog_filter(std::move(new_dialog_filter), "set_pinned_dialogs");
    save_dialog_filters();
    send_update_chat_filters();

    if (server_old_dialog_ids != server_new_dialog_ids) {
      synchronize_dialog_filters();
    }

    return Status::OK();
  }

  CHECK(dialog_list_id.is_folder());

  std::reverse(pinned_dialog_ids.begin(), pinned_dialog_ids.end());
  std::reverse(dialog_ids.begin(), dialog_ids.end());

  std::unordered_set<DialogId, DialogIdHash> old_pinned_dialog_ids(pinned_dialog_ids.begin(), pinned_dialog_ids.end());
  auto old_it = pinned_dialog_ids.begin();
  for (auto dialog_id : dialog_ids) {
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
    set_dialog_is_pinned(dialog_id, true);
  }
  for (auto dialog_id : old_pinned_dialog_ids) {
    Dialog *d = get_dialog_force(dialog_id, "set_pinned_dialogs 2");
    if (d == nullptr) {
      LOG(ERROR) << "Failed to find " << dialog_id << " to unpin in " << dialog_list_id;
      force_create_dialog(dialog_id, "set_pinned_dialogs", true);
      d = get_dialog_force(dialog_id, "set_pinned_dialogs 3");
    }
    if (d != nullptr) {
      set_dialog_is_pinned(dialog_list_id, d, false);
    }
  }

  if (server_old_dialog_ids != server_new_dialog_ids) {
    reorder_pinned_dialogs_on_server(dialog_list_id.get_folder_id(), server_new_dialog_ids, 0);
  }
  return Status::OK();
}

class EditChatDefaultBannedRightsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit EditChatDefaultBannedRightsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, RestrictedRights permissions) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::messages_editChatDefaultBannedRights(
        std::move(input_peer), permissions.get_chat_banned_rights())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editChatDefaultBannedRights>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditChatDefaultBannedRightsQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditChatDefaultBannedRightsQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::set_dialog_permissions(DialogId dialog_id,
                                             const td_api::object_ptr<td_api::chatPermissions> &permissions,
                                             Promise<Unit> &&promise) {
  if (!have_dialog_force(dialog_id, "set_dialog_permissions")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Write)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  if (permissions == nullptr) {
    return promise.set_error(Status::Error(400, "New permissions must be non-empty"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't change private chat permissions"));
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto status = td_->contacts_manager_->get_chat_permissions(chat_id);
      if (!status.can_restrict_members()) {
        return promise.set_error(Status::Error(400, "Not enough rights to change chat permissions"));
      }
      break;
    }
    case DialogType::Channel: {
      if (is_broadcast_channel(dialog_id)) {
        return promise.set_error(Status::Error(400, "Can't change channel chat permissions"));
      }
      auto status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      if (!status.can_restrict_members()) {
        return promise.set_error(Status::Error(400, "Not enough rights to change chat permissions"));
      }
      break;
    }
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't change secret chat permissions"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  auto new_permissions = get_restricted_rights(permissions);

  // TODO this can be wrong if there were previous change permissions requests
  if (get_dialog_default_permissions(dialog_id) == new_permissions) {
    return promise.set_value(Unit());
  }

  // TODO invoke after
  td_->create_handler<EditChatDefaultBannedRightsQuery>(std::move(promise))->send(dialog_id, new_permissions);
}

class GetBlockedDialogsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::messageSenders>> promise_;
  int32 offset_;
  int32 limit_;

 public:
  explicit GetBlockedDialogsQuery(Promise<td_api::object_ptr<td_api::messageSenders>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(int32 offset, int32 limit) {
    offset_ = offset;
    limit_ = limit;

    send_query(G()->net_query_creator().create(telegram_api::contacts_getBlocked(offset, limit)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_getBlocked>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetBlockedDialogsQuery: " << to_string(ptr);

    switch (ptr->get_id()) {
      case telegram_api::contacts_blocked::ID: {
        auto blocked_peers = move_tl_object_as<telegram_api::contacts_blocked>(ptr);

        td_->contacts_manager_->on_get_users(std::move(blocked_peers->users_), "GetBlockedDialogsQuery");
        td_->contacts_manager_->on_get_chats(std::move(blocked_peers->chats_), "GetBlockedDialogsQuery");
        td_->messages_manager_->on_get_blocked_dialogs(offset_, limit_,
                                                       narrow_cast<int32>(blocked_peers->blocked_.size()),
                                                       std::move(blocked_peers->blocked_), std::move(promise_));
        break;
      }
      case telegram_api::contacts_blockedSlice::ID: {
        auto blocked_peers = move_tl_object_as<telegram_api::contacts_blockedSlice>(ptr);

        td_->contacts_manager_->on_get_users(std::move(blocked_peers->users_), "GetBlockedDialogsQuery");
        td_->contacts_manager_->on_get_chats(std::move(blocked_peers->chats_), "GetBlockedDialogsQuery");
        td_->messages_manager_->on_get_blocked_dialogs(offset_, limit_, blocked_peers->count_,
                                                       std::move(blocked_peers->blocked_), std::move(promise_));
        break;
      }
      default:
        UNREACHABLE();
    }
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_blocked_dialogs(int32 offset, int32 limit,
                                          Promise<td_api::object_ptr<td_api::messageSenders>> &&promise) {
  if (offset < 0) {
    return promise.set_error(Status::Error(400, "Parameter offset must be non-negative"));
  }

  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }

  td_->create_handler<GetBlockedDialogsQuery>(std::move(promise))->send(offset, limit);
}

class ToggleDialogPinQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  bool is_pinned_;

 public:
  explicit ToggleDialogPinQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, bool is_pinned) {
    dialog_id_ = dialog_id;
    is_pinned_ = is_pinned;

    auto input_peer = td_->messages_manager_->get_input_dialog_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    int32 flags = 0;
    if (is_pinned) {
      flags |= telegram_api::messages_toggleDialogPin::PINNED_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_toggleDialogPin(flags, false /*ignored*/, std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_toggleDialogPin>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      on_error(Status::Error(400, "Toggle dialog pin failed"));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ToggleDialogPinQuery")) {
      LOG(ERROR) << "Receive error for ToggleDialogPinQuery: " << status;
    }
    td_->messages_manager_->on_update_pinned_dialogs(FolderId::main());
    td_->messages_manager_->on_update_pinned_dialogs(FolderId::archive());
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::toggle_dialog_is_pinned_on_server(DialogId dialog_id, bool is_pinned, uint64 log_event_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (log_event_id == 0 && dialog_id.get_type() == DialogType::SecretChat) {
    // don't even create new binlog events
    return;
  }

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_toggle_dialog_is_pinned_on_server_log_event(dialog_id, is_pinned);
  }

  td_->create_handler<ToggleDialogPinQuery>(get_erase_log_event_promise(log_event_id))->send(dialog_id, is_pinned);
}

void MessagesManager::remove_message_notifications_by_message_ids(DialogId dialog_id,
                                                                  const vector<MessageId> &message_ids) {
  VLOG(notifications) << "Trying to remove notification about " << message_ids << " in " << dialog_id;
  Dialog *d = get_dialog_force(dialog_id, "remove_message_notifications_by_message_ids");
  if (d == nullptr) {
    return;
  }

  bool need_update_dialog_pos = false;
  vector<int64> deleted_message_ids;
  for (auto message_id : message_ids) {
    CHECK(!message_id.is_scheduled());
    // can't remove just notification_id, because total_count will stay wrong after restart
    // delete whole message
    auto message =
        delete_message(d, message_id, true, &need_update_dialog_pos, "remove_message_notifications_by_message_ids");
    if (message == nullptr) {
      LOG(INFO) << "Can't delete " << message_id << " because it is not found";
      // call synchronously to remove them before ProcessPush returns
      td_->notification_manager_->remove_temporary_notification_by_message_id(
          d->message_notification_group.group_id, message_id, true, "remove_message_notifications_by_message_ids");
      td_->notification_manager_->remove_temporary_notification_by_message_id(
          d->mention_notification_group.group_id, message_id, true, "remove_message_notifications_by_message_ids");
      continue;
    }
    deleted_message_ids.push_back(message->message_id.get());
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "remove_message_notifications_by_message_ids");
  }
  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);
}

void MessagesManager::do_send_media(DialogId dialog_id, Message *m, FileId file_id, FileId thumbnail_file_id,
                                    tl_object_ptr<telegram_api::InputFile> input_file,
                                    tl_object_ptr<telegram_api::InputFile> input_thumbnail) {
  CHECK(m != nullptr);

  bool have_input_file = input_file != nullptr;
  bool have_input_thumbnail = input_thumbnail != nullptr;
  LOG(INFO) << "Do send media file " << file_id << " with thumbnail " << thumbnail_file_id
            << ", have_input_file = " << have_input_file << ", have_input_thumbnail = " << have_input_thumbnail
            << ", TTL = " << m->ttl;

  MessageContent *content = nullptr;
  if (m->message_id.is_any_server()) {
    content = m->edited_content.get();
    if (content == nullptr) {
      LOG(ERROR) << "Message has no edited content";
      return;
    }
  } else {
    content = m->content.get();
  }

  auto input_media = get_input_media(content, td_, std::move(input_file), std::move(input_thumbnail), file_id,
                                     thumbnail_file_id, m->ttl, true);
  LOG_CHECK(input_media != nullptr) << to_string(get_message_object(dialog_id, m, "do_send_media")) << ' '
                                    << have_input_file << ' ' << have_input_thumbnail << ' ' << file_id << ' '
                                    << thumbnail_file_id << ' ' << m->ttl;

  on_message_media_uploaded(dialog_id, m, std::move(input_media), file_id, thumbnail_file_id);
}

Result<unique_ptr<ReplyMarkup>> MessagesManager::get_dialog_reply_markup(
    DialogId dialog_id, tl_object_ptr<td_api::ReplyMarkup> &&reply_markup_ptr) const {
  if (reply_markup_ptr == nullptr) {
    return nullptr;
  }

  auto dialog_type = dialog_id.get_type();
  bool is_anonymous = is_anonymous_administrator(dialog_id, nullptr);

  bool only_inline_keyboard = is_anonymous;
  bool request_buttons_allowed = dialog_type == DialogType::User;
  bool switch_inline_buttons_allowed = !is_anonymous;

  TRY_RESULT(reply_markup,
             get_reply_markup(std::move(reply_markup_ptr), td_->auth_manager_->is_bot(), only_inline_keyboard,
                              request_buttons_allowed, switch_inline_buttons_allowed));
  if (reply_markup == nullptr) {
    return nullptr;
  }

  switch (dialog_type) {
    case DialogType::User:
      if (reply_markup->type != ReplyMarkup::Type::InlineKeyboard) {
        reply_markup->is_personal = false;
      }
      break;
    case DialogType::Channel:
    case DialogType::Chat:
    case DialogType::SecretChat:
    case DialogType::None:
      // nothing special
      break;
    default:
      UNREACHABLE();
  }

  return std::move(reply_markup);
}

bool MessagesManager::is_dialog_action_unneeded(DialogId dialog_id) const {
  if (is_anonymous_administrator(dialog_id, nullptr)) {
    return true;
  }

  auto dialog_type = dialog_id.get_type();
  if (dialog_type == DialogType::User || dialog_type == DialogType::SecretChat) {
    UserId user_id = dialog_type == DialogType::User
                         ? dialog_id.get_user_id()
                         : td_->contacts_manager_->get_secret_chat_user_id(dialog_id.get_secret_chat_id());
    if (td_->contacts_manager_->is_user_deleted(user_id)) {
      return true;
    }
    if (td_->contacts_manager_->is_user_bot(user_id) && !td_->contacts_manager_->is_user_support(user_id)) {
      return true;
    }
    if (user_id == td_->contacts_manager_->get_my_id()) {
      return true;
    }

    if (!td_->auth_manager_->is_bot()) {
      if (td_->contacts_manager_->is_user_status_exact(user_id)) {
        if (!td_->contacts_manager_->is_user_online(user_id, 30)) {
          return true;
        }
      } else {
        // return true;
      }
    }
  }
  return false;
}

void MessagesManager::add_sponsored_dialog(const Dialog *d, DialogSource source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(!sponsored_dialog_id_.is_valid());
  sponsored_dialog_id_ = d->dialog_id;
  sponsored_dialog_source_ = std::move(source);

  // update last_pinned_dialog_date in any case, because all chats before SPONSORED_DIALOG_ORDER are known
  auto dialog_list_id = DialogListId(FolderId::main());
  auto *list = get_dialog_list(dialog_list_id);
  CHECK(list != nullptr);
  DialogDate max_dialog_date(SPONSORED_DIALOG_ORDER, d->dialog_id);
  if (list->last_pinned_dialog_date_ < max_dialog_date) {
    list->last_pinned_dialog_date_ = max_dialog_date;
    update_list_last_dialog_date(*list);
  }

  if (is_dialog_sponsored(d)) {
    send_update_chat_position(dialog_list_id, d, "add_sponsored_dialog");
    // the sponsored dialog must not be saved there
  }
}

void MessagesManager::on_update_some_live_location_viewed(Promise<Unit> &&promise) {
  LOG(DEBUG) << "Some live location was viewed";
  if (!are_active_live_location_messages_loaded_) {
    get_active_live_location_messages(
        PromiseCreator::lambda([actor_id = actor_id(this), promise = std::move(promise)](Unit result) mutable {
          send_closure(actor_id, &MessagesManager::on_update_some_live_location_viewed, std::move(promise));
        }));
    return;
  }

  // update all live locations, because it is unknown, which exactly was viewed
  auto active_live_location_message_ids = get_active_live_location_messages(Auto());
  for (const auto &full_message_id : active_live_location_message_ids) {
    send_update_message_live_location_viewed(full_message_id);
  }

  promise.set_value(Unit());
}

void MessagesManager::attach_message_to_next(Dialog *d, MessageId message_id, const char *source) {
  CHECK(d != nullptr);
  CHECK(message_id.is_valid());
  MessagesIterator it(d, message_id);
  Message *m = *it;
  CHECK(m != nullptr);
  CHECK(m->message_id == message_id);
  LOG_CHECK(m->have_next) << d->dialog_id << " " << message_id << " " << source;
  ++it;
  LOG_CHECK(*it != nullptr) << d->dialog_id << " " << message_id << " " << source;
  LOG(INFO) << "Attach " << message_id << " to the next " << (*it)->message_id << " in " << d->dialog_id;
  if ((*it)->have_previous) {
    m->have_previous = true;
  } else {
    (*it)->have_previous = true;
  }
}

const ScopeNotificationSettings *MessagesManager::get_scope_notification_settings(NotificationSettingsScope scope,
                                                                                  Promise<Unit> &&promise) {
  const ScopeNotificationSettings *notification_settings = get_scope_notification_settings(scope);
  CHECK(notification_settings != nullptr);
  if (!notification_settings->is_synchronized && !td_->auth_manager_->is_bot()) {
    send_get_scope_notification_settings_query(scope, std::move(promise));
    return nullptr;
  }

  promise.set_value(Unit());
  return notification_settings;
}

void MessagesManager::on_send_message_get_quick_ack(int64 random_id) {
  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    LOG(ERROR) << "Receive quick ack about unknown message with random_id = " << random_id;
    return;
  }

  auto dialog_id = it->second.get_dialog_id();
  auto message_id = it->second.get_message_id();

  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateMessageSendAcknowledged>(dialog_id.get(), message_id.get()));
}

void MessagesManager::on_external_update_message_content(FullMessageId full_message_id) {
  Dialog *d = get_dialog(full_message_id.get_dialog_id());
  CHECK(d != nullptr);
  Message *m = get_message(d, full_message_id.get_message_id());
  CHECK(m != nullptr);
  send_update_message_content(d, m, true, "on_external_update_message_content");
  if (m->message_id == d->last_message_id) {
    send_update_chat_last_message_impl(d, "on_external_update_message_content");
  }
}

void MessagesManager::on_pending_unload_dialog_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::unload_dialog,
                     DialogId(dialog_id_int));
}

std::pair<bool, int32> MessagesManager::get_dialog_mute_until(DialogId dialog_id, const Dialog *d) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (d == nullptr || !d->notification_settings.is_synchronized) {
    return {false, get_scope_mute_until(dialog_id)};
  }

  return {d->notification_settings.is_use_default_fixed, get_dialog_mute_until(d)};
}

void MessagesManager::ttl_db_loop_start(double server_now) {
  ttl_db_expires_from_ = 0;
  ttl_db_expires_till_ = static_cast<int32>(server_now) + 15 /* 15 seconds */;
  ttl_db_has_query_ = false;

  ttl_db_loop(server_now);
}

void MessagesManager::set_message_id(unique_ptr<Message> &message, MessageId message_id) {
  message->message_id = message_id;
  message->random_y = get_random_y(message_id);
}
}  // namespace td
