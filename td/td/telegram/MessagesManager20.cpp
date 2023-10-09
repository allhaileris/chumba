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



#include <type_traits>


#include <utility>

namespace td {
}  // namespace td
namespace td {

class ExportChannelMessageLinkQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  MessageId message_id_;
  bool for_group_ = false;
  bool ignore_result_ = false;

 public:
  explicit ExportChannelMessageLinkQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, MessageId message_id, bool for_group, bool ignore_result) {
    channel_id_ = channel_id;
    message_id_ = message_id;
    for_group_ = for_group;
    ignore_result_ = ignore_result;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }
    int32 flags = 0;
    if (for_group) {
      flags |= telegram_api::channels_exportMessageLink::GROUPED_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::channels_exportMessageLink(flags, false /*ignored*/, false /*ignored*/, std::move(input_channel),
                                                 message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_exportMessageLink>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for ExportChannelMessageLinkQuery: " << to_string(ptr);
    if (!ignore_result_) {
      td_->messages_manager_->on_get_public_message_link({DialogId(channel_id_), message_id_}, for_group_,
                                                         std::move(ptr->link_), std::move(ptr->html_));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!ignore_result_) {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ExportChannelMessageLinkQuery");
    }
    promise_.set_error(std::move(status));
  }
};

Result<std::pair<string, bool>> MessagesManager::get_message_link(FullMessageId full_message_id, int32 media_timestamp,
                                                                  bool for_group, bool for_comment) {
  auto dialog_id = full_message_id.get_dialog_id();
  auto d = get_dialog_force(dialog_id, "get_message_link");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }

  auto *m = get_message_force(d, full_message_id.get_message_id(), "get_message_link");
  TRY_STATUS(can_get_media_timestamp_link(dialog_id, m));

  if (media_timestamp <= 0 || !can_message_content_have_media_timestamp(m->content.get())) {
    media_timestamp = 0;
  }
  if (media_timestamp != 0) {
    for_group = false;
    auto duration = get_message_content_media_duration(m->content.get(), td_);
    if (duration != 0 && media_timestamp > duration) {
      media_timestamp = 0;
    }
  }

  auto message_id = m->message_id;
  if (dialog_id.get_type() != DialogType::Channel) {
    CHECK(m->forward_info != nullptr);
    CHECK(m->forward_info->sender_dialog_id.get_type() == DialogType::Channel);

    dialog_id = m->forward_info->sender_dialog_id;
    message_id = m->forward_info->message_id;
    for_group = false;
    for_comment = false;
    auto channel_message = get_message({dialog_id, message_id});
    if (channel_message != nullptr && channel_message->media_album_id == 0) {
      for_group = true;  // default is true
    }
  } else {
    if (m->media_album_id == 0) {
      for_group = true;  // default is true
    }
  }

  if (!m->top_thread_message_id.is_valid() || !m->top_thread_message_id.is_server()) {
    for_comment = false;
  }
  if (d->deleted_message_ids.count(m->top_thread_message_id) != 0) {
    for_comment = false;
  }
  if (for_comment && is_broadcast_channel(dialog_id)) {
    for_comment = false;
  }

  if (!td_->auth_manager_->is_bot()) {
    td_->create_handler<ExportChannelMessageLinkQuery>(Promise<Unit>())
        ->send(dialog_id.get_channel_id(), message_id, for_group, true);
  }

  SliceBuilder sb;
  sb << G()->shared_config().get_option_string("t_me_url", "https://t.me/");

  if (for_comment) {
    auto *top_m = get_message_force(d, m->top_thread_message_id, "get_public_message_link");
    if (is_discussion_message(dialog_id, top_m) && is_active_message_reply_info(dialog_id, top_m->reply_info)) {
      auto linked_dialog_id = top_m->forward_info->from_dialog_id;
      auto linked_message_id = top_m->forward_info->from_message_id;
      auto linked_d = get_dialog(linked_dialog_id);
      CHECK(linked_d != nullptr);
      CHECK(linked_dialog_id.get_type() == DialogType::Channel);
      auto *linked_m = get_message_force(linked_d, linked_message_id, "get_public_message_link");
      auto channel_username = td_->contacts_manager_->get_channel_username(linked_dialog_id.get_channel_id());
      if (linked_m != nullptr && is_active_message_reply_info(linked_dialog_id, linked_m->reply_info) &&
          linked_message_id.is_server() && have_input_peer(linked_dialog_id, AccessRights::Read) &&
          !channel_username.empty()) {
        sb << channel_username << '/' << linked_message_id.get_server_message_id().get()
           << "?comment=" << message_id.get_server_message_id().get();
        if (!for_group) {
          sb << "&single";
        }
        if (media_timestamp > 0) {
          sb << "&t=" << media_timestamp;
        }
        return std::make_pair(sb.as_cslice().str(), true);
      }
    }
  }

  auto dialog_username = td_->contacts_manager_->get_channel_username(dialog_id.get_channel_id());
  bool is_public = !dialog_username.empty();
  if (m->content->get_type() == MessageContentType::VideoNote && is_broadcast_channel(dialog_id) && is_public) {
    return std::make_pair(
        PSTRING() << "https://telesco.pe/" << dialog_username << '/' << message_id.get_server_message_id().get(), true);
  }

  if (is_public) {
    sb << dialog_username;
  } else {
    sb << "c/" << dialog_id.get_channel_id().get();
  }
  sb << '/' << message_id.get_server_message_id().get();

  char separator = '?';
  if (for_comment) {
    sb << separator << "thread=" << m->top_thread_message_id.get_server_message_id().get();
    separator = '&';
  }
  if (!for_group) {
    sb << separator << "single";
    separator = '&';
  }
  if (media_timestamp > 0) {
    sb << separator << "t=" << media_timestamp;
    separator = '&';
  }

  return std::make_pair(sb.as_cslice().str(), is_public);
}

string MessagesManager::get_message_embedding_code(FullMessageId full_message_id, bool for_group,
                                                   Promise<Unit> &&promise) {
  auto dialog_id = full_message_id.get_dialog_id();
  auto d = get_dialog_force(dialog_id, "get_message_embedding_code");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return {};
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return {};
  }
  if (dialog_id.get_type() != DialogType::Channel ||
      td_->contacts_manager_->get_channel_username(dialog_id.get_channel_id()).empty()) {
    promise.set_error(Status::Error(
        400, "Message embedding code is available only for messages in public supergroups and channel chats"));
    return {};
  }

  auto *m = get_message_force(d, full_message_id.get_message_id(), "get_message_embedding_code");
  if (m == nullptr) {
    promise.set_error(Status::Error(400, "Message not found"));
    return {};
  }
  if (m->message_id.is_yet_unsent()) {
    promise.set_error(Status::Error(400, "Message is not sent yet"));
    return {};
  }
  if (m->message_id.is_scheduled()) {
    promise.set_error(Status::Error(400, "Message is scheduled"));
    return {};
  }
  if (!m->message_id.is_server()) {
    promise.set_error(Status::Error(400, "Message is local"));
    return {};
  }

  if (m->media_album_id == 0) {
    for_group = true;  // default is true
  }

  auto &links = message_embedding_codes_[for_group][dialog_id].embedding_codes_;
  auto it = links.find(m->message_id);
  if (it == links.end()) {
    td_->create_handler<ExportChannelMessageLinkQuery>(std::move(promise))
        ->send(dialog_id.get_channel_id(), m->message_id, for_group, false);
    return {};
  }

  promise.set_value(Unit());
  return it->second;
}

class EditDialogTitleQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit EditDialogTitleQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &title) {
    dialog_id_ = dialog_id;
    switch (dialog_id.get_type()) {
      case DialogType::Chat:
        send_query(G()->net_query_creator().create(
            telegram_api::messages_editChatTitle(dialog_id.get_chat_id().get(), title)));
        break;
      case DialogType::Channel: {
        auto channel_id = dialog_id.get_channel_id();
        auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
        CHECK(input_channel != nullptr);
        send_query(G()->net_query_creator().create(telegram_api::channels_editTitle(std::move(input_channel), title)));
        break;
      }
      default:
        UNREACHABLE();
    }
  }

  void on_result(BufferSlice packet) final {
    static_assert(std::is_same<telegram_api::messages_editChatTitle::ReturnType,
                               telegram_api::channels_editTitle::ReturnType>::value,
                  "");
    auto result_ptr = fetch_result<telegram_api::messages_editChatTitle>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditDialogTitleQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->updates_manager_->get_difference("EditDialogTitleQuery");

    if (status.message() == "CHAT_NOT_MODIFIED") {
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditDialogTitleQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::set_dialog_title(DialogId dialog_id, const string &title, Promise<Unit> &&promise) {
  LOG(INFO) << "Receive setChatTitle request to change title of " << dialog_id << " to \"" << title << '"';

  if (!have_dialog_force(dialog_id, "set_dialog_title")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  auto new_title = clean_name(title, MAX_TITLE_LENGTH);
  if (new_title.empty()) {
    return promise.set_error(Status::Error(400, "Title can't be empty"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't change private chat title"));
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto status = td_->contacts_manager_->get_chat_permissions(chat_id);
      if (!status.can_change_info_and_settings() ||
          (td_->auth_manager_->is_bot() && !td_->contacts_manager_->is_appointed_chat_administrator(chat_id))) {
        return promise.set_error(Status::Error(400, "Not enough rights to change chat title"));
      }
      break;
    }
    case DialogType::Channel: {
      auto status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      if (!status.can_change_info_and_settings()) {
        return promise.set_error(Status::Error(400, "Not enough rights to change chat title"));
      }
      break;
    }
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't change secret chat title"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  // TODO this can be wrong if there were previous change title requests
  if (get_dialog_title(dialog_id) == new_title) {
    return promise.set_value(Unit());
  }

  // TODO invoke after
  td_->create_handler<EditDialogTitleQuery>(std::move(promise))->send(dialog_id, new_title);
}

class CreateChatQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  int64 random_id_;

 public:
  explicit CreateChatQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<tl_object_ptr<telegram_api::InputUser>> &&input_users, const string &title, int64 random_id) {
    random_id_ = random_id;
    send_query(G()->net_query_creator().create(telegram_api::messages_createChat(std::move(input_users), title)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_createChat>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for CreateChatQuery: " << to_string(ptr);
    td_->messages_manager_->on_create_new_dialog_success(random_id_, std::move(ptr), DialogType::Chat,
                                                         std::move(promise_));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_create_new_dialog_fail(random_id_, std::move(status), std::move(promise_));
  }
};

DialogId MessagesManager::create_new_group_chat(const vector<UserId> &user_ids, const string &title, int64 &random_id,
                                                Promise<Unit> &&promise) {
  LOG(INFO) << "Trying to create group chat \"" << title << "\" with members " << format::as_array(user_ids);

  if (random_id != 0) {
    // request has already been sent before
    auto it = created_dialogs_.find(random_id);
    CHECK(it != created_dialogs_.end());
    auto dialog_id = it->second;
    CHECK(dialog_id.get_type() == DialogType::Chat);
    CHECK(have_dialog(dialog_id));

    created_dialogs_.erase(it);

    // set default notification settings to newly created chat
    on_update_dialog_notify_settings(dialog_id, make_tl_object<telegram_api::peerNotifySettings>(),
                                     "create_new_group_chat");

    promise.set_value(Unit());
    return dialog_id;
  }

  if (user_ids.empty()) {
    promise.set_error(Status::Error(400, "Too few users to create basic group chat"));
    return DialogId();
  }

  auto new_title = clean_name(title, MAX_TITLE_LENGTH);
  if (new_title.empty()) {
    promise.set_error(Status::Error(400, "Title can't be empty"));
    return DialogId();
  }

  vector<tl_object_ptr<telegram_api::InputUser>> input_users;
  for (auto user_id : user_ids) {
    auto r_input_user = td_->contacts_manager_->get_input_user(user_id);
    if (r_input_user.is_error()) {
      promise.set_error(r_input_user.move_as_error());
      return DialogId();
    }
    input_users.push_back(r_input_user.move_as_ok());
  }

  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || created_dialogs_.find(random_id) != created_dialogs_.end());
  created_dialogs_[random_id];  // reserve place for result

  td_->create_handler<CreateChatQuery>(std::move(promise))->send(std::move(input_users), new_title, random_id);
  return DialogId();
}

class ToggleDialogUnreadMarkQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  bool is_marked_as_unread_;

 public:
  explicit ToggleDialogUnreadMarkQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, bool is_marked_as_unread) {
    dialog_id_ = dialog_id;
    is_marked_as_unread_ = is_marked_as_unread;

    auto input_peer = td_->messages_manager_->get_input_dialog_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    int32 flags = 0;
    if (is_marked_as_unread) {
      flags |= telegram_api::messages_markDialogUnread::UNREAD_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_markDialogUnread(flags, false /*ignored*/, std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_markDialogUnread>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      on_error(Status::Error(400, "Toggle dialog mark failed"));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ToggleDialogUnreadMarkQuery")) {
      LOG(ERROR) << "Receive error for ToggleDialogUnreadMarkQuery: " << status;
    }
    if (!G()->close_flag()) {
      td_->messages_manager_->on_update_dialog_is_marked_as_unread(dialog_id_, !is_marked_as_unread_);
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::toggle_dialog_is_marked_as_unread_on_server(DialogId dialog_id, bool is_marked_as_unread,
                                                                  uint64 log_event_id) {
  if (log_event_id == 0 && dialog_id.get_type() == DialogType::SecretChat) {
    // don't even create new binlog events
    return;
  }

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_toggle_dialog_is_marked_as_unread_on_server_log_event(dialog_id, is_marked_as_unread);
  }

  td_->create_handler<ToggleDialogUnreadMarkQuery>(get_erase_log_event_promise(log_event_id))
      ->send(dialog_id, is_marked_as_unread);
}

void MessagesManager::add_dialog_filter(unique_ptr<DialogFilter> dialog_filter, bool at_beginning, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  CHECK(dialog_filter != nullptr);
  auto dialog_filter_id = dialog_filter->dialog_filter_id;
  LOG(INFO) << "Add " << dialog_filter_id << " from " << source;
  CHECK(get_dialog_filter(dialog_filter_id) == nullptr);
  if (at_beginning) {
    dialog_filters_.insert(dialog_filters_.begin(), std::move(dialog_filter));
  } else {
    dialog_filters_.push_back(std::move(dialog_filter));
  }

  auto dialog_list_id = DialogListId(dialog_filter_id);
  CHECK(dialog_lists_.find(dialog_list_id) == dialog_lists_.end());

  auto &list = add_dialog_list(dialog_list_id);
  auto folder_ids = get_dialog_list_folder_ids(list);
  CHECK(!folder_ids.empty());

  for (auto folder_id : folder_ids) {
    auto *folder = get_dialog_folder(folder_id);
    CHECK(folder != nullptr);
    for (const auto &dialog_date : folder->ordered_dialogs_) {
      if (dialog_date.get_order() == DEFAULT_ORDER) {
        break;
      }

      auto dialog_id = dialog_date.get_dialog_id();
      Dialog *d = get_dialog(dialog_id);
      CHECK(d != nullptr);

      if (need_dialog_in_list(d, list)) {
        list.in_memory_dialog_total_count_++;

        add_dialog_to_list(d, dialog_list_id);
      }
    }
  }

  for (const auto &input_dialog_id : reversed(dialog_filters_.back()->pinned_dialog_ids)) {
    auto dialog_id = input_dialog_id.get_dialog_id();
    auto order = get_next_pinned_dialog_order();
    list.pinned_dialogs_.emplace_back(order, dialog_id);
    list.pinned_dialog_id_orders_.emplace(dialog_id, order);
  }
  std::reverse(list.pinned_dialogs_.begin(), list.pinned_dialogs_.end());
  list.are_pinned_dialogs_inited_ = true;

  update_list_last_pinned_dialog_date(list);
  update_list_last_dialog_date(list);
}

void MessagesManager::delete_all_dialog_messages_from_database(Dialog *d, MessageId max_message_id,
                                                               const char *source) {
  CHECK(d != nullptr);
  CHECK(max_message_id.is_valid());
  if (d->new_secret_chat_notification_id.is_valid()) {
    remove_new_secret_chat_notification(d, true);
  }
  if (d->pinned_message_notification_message_id.is_valid() &&
      d->pinned_message_notification_message_id <= max_message_id) {
    remove_dialog_pinned_message_notification(d, source);
  }
  remove_message_dialog_notifications(d, max_message_id, false, source);
  remove_message_dialog_notifications(d, max_message_id, true, source);

  if (!G()->parameters().use_message_db) {
    return;
  }

  auto dialog_id = d->dialog_id;
  LOG(INFO) << "Delete all messages in " << dialog_id << " from database up to " << max_message_id << " from "
            << source;
  /*
  if (dialog_id.get_type() == DialogType::User && max_message_id.is_server()) {
    bool need_save = false;
    int i = 0;
    for (auto &first_message_id : calls_db_state_.first_calls_database_message_id_by_index) {
      if (first_message_id <= max_message_id) {
        first_message_id = max_message_id.get_next_server_message_id();
        calls_db_state_.message_count_by_index[i] = -1;
        need_save = true;
      }
      i++;
    }
    if (need_save) {
      save_calls_db_state();
    }
  }
  */
  G()->td_db()->get_messages_db_async()->delete_all_dialog_messages(dialog_id, max_message_id,
                                                                    Auto());  // TODO Promise
}

void MessagesManager::do_fix_dialog_last_notification_id(DialogId dialog_id, bool from_mentions,
                                                         NotificationId prev_last_notification_id,
                                                         Result<vector<Notification>> result) {
  if (result.is_error()) {
    return;
  }

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  VLOG(notifications) << "Receive " << result.ok().size() << " message notifications in " << group_info.group_id << '/'
                      << dialog_id << " from " << prev_last_notification_id;
  if (group_info.last_notification_id != prev_last_notification_id) {
    // last_notification_id was changed
    return;
  }

  auto notifications = result.move_as_ok();
  CHECK(notifications.size() <= 1);

  int32 last_notification_date = 0;
  NotificationId last_notification_id;
  if (!notifications.empty()) {
    last_notification_date = notifications[0].date;
    last_notification_id = notifications[0].notification_id;
  }

  bool is_fixed = set_dialog_last_notification(dialog_id, group_info, last_notification_date, last_notification_id,
                                               "do_fix_dialog_last_notification_id");
  CHECK(is_fixed);
}

void MessagesManager::do_set_dialog_folder_id(Dialog *d, FolderId folder_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (d->folder_id == folder_id && d->is_folder_id_inited) {
    return;
  }

  d->folder_id = folder_id;
  d->is_folder_id_inited = true;

  if (d->dialog_id.get_type() == DialogType::SecretChat) {
    // need to change action bar only for the secret chat and keep unarchive for the main chat
    auto user_id = td_->contacts_manager_->get_secret_chat_user_id(d->dialog_id.get_secret_chat_id());
    if (d->is_update_new_chat_sent && user_id.is_valid()) {
      const Dialog *user_d = get_dialog(DialogId(user_id));
      if (user_d != nullptr && user_d->action_bar != nullptr && user_d->action_bar->can_unarchive()) {
        send_closure(
            G()->td(), &Td::send_update,
            td_api::make_object<td_api::updateChatActionBar>(d->dialog_id.get(), get_chat_action_bar_object(d)));
      }
    }
  } else if (folder_id != FolderId::archive() && d->action_bar != nullptr && d->action_bar->on_dialog_unarchived()) {
    send_update_chat_action_bar(d);
  }

  on_dialog_updated(d->dialog_id, "do_set_dialog_folder_id");
}

bool MessagesManager::read_message_content(Dialog *d, Message *m, bool is_local_read, const char *source) {
  LOG_CHECK(m != nullptr) << source;
  CHECK(!m->message_id.is_scheduled());
  bool is_mention_read = update_message_contains_unread_mention(d, m, false, "read_message_content");
  bool is_content_read = update_opened_message_content(m->content.get());
  if (ttl_on_open(d, m, Time::now(), is_local_read)) {
    is_content_read = true;
  }

  LOG(INFO) << "Read message content of " << m->message_id << " in " << d->dialog_id
            << ": is_mention_read = " << is_mention_read << ", is_content_read = " << is_content_read;
  if (is_mention_read || is_content_read) {
    on_message_changed(d, m, true, "read_message_content");
    if (is_content_read) {
      send_closure(G()->td(), &Td::send_update,
                   make_tl_object<td_api::updateMessageContentOpened>(d->dialog_id.get(), m->message_id.get()));
    }
    return true;
  }
  return false;
}

std::pair<int32, vector<DialogId>> MessagesManager::search_dialogs(const string &query, int32 limit,
                                                                   Promise<Unit> &&promise) {
  LOG(INFO) << "Search chats with query \"" << query << "\" and limit " << limit;
  CHECK(!td_->auth_manager_->is_bot());

  if (limit < 0) {
    promise.set_error(Status::Error(400, "Limit must be non-negative"));
    return {};
  }
  if (query.empty()) {
    return recently_found_dialogs_.get_dialogs(limit, std::move(promise));
  }

  auto result = dialogs_hints_.search(query, limit);
  vector<DialogId> dialog_ids;
  dialog_ids.reserve(result.second.size());
  for (auto key : result.second) {
    dialog_ids.push_back(DialogId(-key));
  }

  promise.set_value(Unit());
  return {narrow_cast<int32>(result.first), std::move(dialog_ids)};
}

void MessagesManager::on_update_read_channel_messages_contents(
    tl_object_ptr<telegram_api::updateChannelReadMessagesContents> &&update) {
  ChannelId channel_id(update->channel_id_);
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id << " in updateChannelReadMessagesContents";
    return;
  }

  DialogId dialog_id = DialogId(channel_id);

  Dialog *d = get_dialog_force(dialog_id, "on_update_read_channel_messages_contents");
  if (d == nullptr) {
    LOG(INFO) << "Receive read channel messages contents update in unknown " << dialog_id;
    return;
  }

  for (auto &server_message_id : update->messages_) {
    read_channel_message_content_from_updates(d, MessageId(ServerMessageId(server_message_id)));
  }
}

void MessagesManager::on_update_dialog_pending_join_requests(DialogId dialog_id, int32 pending_join_request_count,
                                                             vector<int64> pending_requesters) {
  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive pending join request count in invalid " << dialog_id;
    return;
  }
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_pending_join_request_count");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  set_dialog_pending_join_requests(d, pending_join_request_count, UserId::get_user_ids(pending_requesters));
}

void MessagesManager::on_get_dialog_notification_settings_query_finished(DialogId dialog_id, Status &&status) {
  CHECK(!td_->auth_manager_->is_bot());
  auto it = get_dialog_notification_settings_queries_.find(dialog_id);
  CHECK(it != get_dialog_notification_settings_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  get_dialog_notification_settings_queries_.erase(it);

  for (auto &promise : promises) {
    if (status.is_ok()) {
      promise.set_value(Unit());
    } else {
      promise.set_error(status.clone());
    }
  }
}

void MessagesManager::on_get_messages(vector<tl_object_ptr<telegram_api::Message>> &&messages, bool is_channel_message,
                                      bool is_scheduled, Promise<Unit> &&promise, const char *source) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(DEBUG) << "Receive " << messages.size() << " messages";
  for (auto &message : messages) {
    on_get_message(std::move(message), false, is_channel_message, is_scheduled, false, false, source);
  }
  promise.set_value(Unit());
}

const ScopeNotificationSettings *MessagesManager::get_scope_notification_settings(
    NotificationSettingsScope scope) const {
  switch (scope) {
    case NotificationSettingsScope::Private:
      return &users_notification_settings_;
    case NotificationSettingsScope::Group:
      return &chats_notification_settings_;
    case NotificationSettingsScope::Channel:
      return &channels_notification_settings_;
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void MessagesManager::try_add_bot_command_message_id(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || !is_group_dialog(dialog_id) || m->message_id.is_scheduled() ||
      !has_bot_commands(get_message_content_text(m->content.get()))) {
    return;
  }

  dialog_bot_command_message_ids_[dialog_id].message_ids.insert(m->message_id);
}

void MessagesManager::cancel_upload_file(FileId file_id) {
  // send the request later so they doesn't interfere with other actions
  // for example merge, supposed to happen soon, can auto-cancel the upload
  LOG(INFO) << "Cancel upload of file " << file_id;
  send_closure_later(G()->file_manager(), &FileManager::cancel_upload, file_id);
}

int64 MessagesManager::generate_new_random_id() {
  int64 random_id;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 || being_sent_messages_.find(random_id) != being_sent_messages_.end());
  return random_id;
}

string MessagesManager::get_message_search_text(const Message *m) const {
  if (m->is_content_secret) {
    return string();
  }
  return get_message_content_search_text(td_, m->content.get());
}
}  // namespace td
