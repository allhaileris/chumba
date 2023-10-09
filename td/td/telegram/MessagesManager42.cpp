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




#include <tuple>



#include <utility>

namespace td {
}  // namespace td
namespace td {

bool MessagesManager::can_edit_message(DialogId dialog_id, const Message *m, bool is_editing,
                                       bool only_reply_markup) const {
  if (m == nullptr) {
    return false;
  }
  if (m->message_id.is_yet_unsent()) {
    return false;
  }
  if (m->message_id.is_local()) {
    return false;
  }
  if (m->forward_info != nullptr || m->had_forward_info) {
    return false;
  }

  if (m->had_reply_markup) {
    return false;
  }
  if (m->reply_markup != nullptr && m->reply_markup->type != ReplyMarkup::Type::InlineKeyboard) {
    return false;
  }

  auto my_id = td_->contacts_manager_->get_my_id();
  if (m->via_bot_user_id.is_valid() && (m->via_bot_user_id != my_id || m->message_id.is_scheduled())) {
    return false;
  }

  bool is_bot = td_->auth_manager_->is_bot();
  auto content_type = m->content->get_type();
  DialogId my_dialog_id(my_id);
  bool has_edit_time_limit = !(is_bot && m->is_outgoing) && dialog_id != my_dialog_id &&
                             content_type != MessageContentType::Poll &&
                             content_type != MessageContentType::LiveLocation && !m->message_id.is_scheduled();
  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (!m->is_outgoing && dialog_id != my_dialog_id && !m->via_bot_user_id.is_valid()) {
        return false;
      }
      break;
    case DialogType::Chat:
      if (!m->is_outgoing && !m->via_bot_user_id.is_valid()) {
        return false;
      }
      break;
    case DialogType::Channel: {
      if (m->via_bot_user_id.is_valid()) {
        // outgoing via_bot messages can always be edited
        break;
      }

      auto channel_id = dialog_id.get_channel_id();
      auto channel_status = td_->contacts_manager_->get_channel_permissions(channel_id);
      if (m->is_channel_post) {
        if (m->message_id.is_scheduled()) {
          if (!channel_status.can_post_messages()) {
            return false;
          }
        } else {
          if (!channel_status.can_edit_messages() && !(channel_status.can_post_messages() && m->is_outgoing)) {
            return false;
          }
          if (channel_status.can_edit_messages()) {
            has_edit_time_limit = false;
          }
        }
        if (is_bot && only_reply_markup) {
          has_edit_time_limit = false;
        }
      } else {
        if (!m->is_outgoing) {
          return false;
        }
        if (channel_status.can_pin_messages()) {
          has_edit_time_limit = false;
        }
      }
      break;
    }
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }

  if (has_edit_time_limit) {
    const int32 DEFAULT_EDIT_TIME_LIMIT = 2 * 86400;
    int64 edit_time_limit = G()->shared_config().get_option_integer("edit_time_limit", DEFAULT_EDIT_TIME_LIMIT);
    if (G()->unix_time_cached() - m->date - (is_editing ? 300 : 0) >= edit_time_limit) {
      return false;
    }
  }

  switch (content_type) {
    case MessageContentType::Animation:
    case MessageContentType::Audio:
    case MessageContentType::Document:
    case MessageContentType::Game:
    case MessageContentType::Photo:
    case MessageContentType::Text:
    case MessageContentType::Video:
    case MessageContentType::VoiceNote:
      return true;
    case MessageContentType::LiveLocation: {
      if (is_bot && only_reply_markup) {
        // there is no caption to edit, but bot can edit inline reply_markup
        return true;
      }
      return G()->unix_time_cached() - m->date < get_message_content_live_location_period(m->content.get());
    }
    case MessageContentType::Poll: {
      if (is_bot && only_reply_markup) {
        // there is no caption to edit, but bot can edit inline reply_markup
        return true;
      }
      if (m->message_id.is_scheduled()) {
        return false;
      }
      return !get_message_content_poll_is_closed(td_, m->content.get());
    }
    case MessageContentType::Contact:
    case MessageContentType::Dice:
    case MessageContentType::Location:
    case MessageContentType::Sticker:
    case MessageContentType::Venue:
    case MessageContentType::VideoNote:
      // there is no caption to edit, but bot can edit inline reply_markup
      return is_bot && only_reply_markup;
    case MessageContentType::Invoice:
    case MessageContentType::Unsupported:
    case MessageContentType::ChatCreate:
    case MessageContentType::ChatChangeTitle:
    case MessageContentType::ChatChangePhoto:
    case MessageContentType::ChatDeletePhoto:
    case MessageContentType::ChatDeleteHistory:
    case MessageContentType::ChatAddUsers:
    case MessageContentType::ChatJoinedByLink:
    case MessageContentType::ChatDeleteUser:
    case MessageContentType::ChatMigrateTo:
    case MessageContentType::ChannelCreate:
    case MessageContentType::ChannelMigrateFrom:
    case MessageContentType::PinMessage:
    case MessageContentType::GameScore:
    case MessageContentType::ScreenshotTaken:
    case MessageContentType::ChatSetTtl:
    case MessageContentType::Call:
    case MessageContentType::PaymentSuccessful:
    case MessageContentType::ContactRegistered:
    case MessageContentType::ExpiredPhoto:
    case MessageContentType::ExpiredVideo:
    case MessageContentType::CustomServiceAction:
    case MessageContentType::WebsiteConnected:
    case MessageContentType::PassportDataSent:
    case MessageContentType::PassportDataReceived:
    case MessageContentType::ProximityAlertTriggered:
    case MessageContentType::GroupCall:
    case MessageContentType::InviteToGroupCall:
    case MessageContentType::ChatSetTheme:
      return false;
    default:
      UNREACHABLE();
  }

  return false;
}

class DeleteHistoryQuery final : public Td::ResultHandler {
  Promise<AffectedHistory> promise_;
  DialogId dialog_id_;

 public:
  explicit DeleteHistoryQuery(Promise<AffectedHistory> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId max_message_id, bool remove_from_dialog_list, bool revoke) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id_, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Chat is not accessible"));
    }

    int32 flags = 0;
    if (!remove_from_dialog_list) {
      flags |= telegram_api::messages_deleteHistory::JUST_CLEAR_MASK;
    }
    if (revoke) {
      flags |= telegram_api::messages_deleteHistory::REVOKE_MASK;
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_deleteHistory(flags, false /*ignored*/, false /*ignored*/, std::move(input_peer),
                                             max_message_id.get_server_message_id().get(), 0, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(AffectedHistory(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "DeleteHistoryQuery");
    promise_.set_error(std::move(status));
  }
};

class DeleteChannelHistoryQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  MessageId max_message_id_;
  bool allow_error_;

 public:
  explicit DeleteChannelHistoryQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, MessageId max_message_id, bool allow_error) {
    channel_id_ = channel_id;
    max_message_id_ = max_message_id;
    allow_error_ = allow_error;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    send_query(G()->net_query_creator().create(
        telegram_api::channels_deleteHistory(std::move(input_channel), max_message_id.get_server_message_id().get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_deleteHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG_IF(ERROR, !allow_error_ && !result)
        << "Delete history in " << channel_id_ << " up to " << max_message_id_ << " failed";

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->contacts_manager_->on_get_channel_error(channel_id_, status, "DeleteChannelHistoryQuery")) {
      LOG(ERROR) << "Receive error for DeleteChannelHistoryQuery: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::delete_dialog_history_on_server(DialogId dialog_id, MessageId max_message_id,
                                                      bool remove_from_dialog_list, bool revoke, bool allow_error,
                                                      uint64 log_event_id, Promise<Unit> &&promise) {
  LOG(INFO) << "Delete history in " << dialog_id << " up to " << max_message_id << " from server";

  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id =
        save_delete_dialog_history_on_server_log_event(dialog_id, max_message_id, remove_from_dialog_list, revoke);
  }

  auto new_promise = get_erase_log_event_promise(log_event_id, std::move(promise));
  promise = std::move(new_promise);  // to prevent self-move

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat: {
      AffectedHistoryQuery query = [td = td_, max_message_id, remove_from_dialog_list, revoke](
                                       DialogId dialog_id, Promise<AffectedHistory> &&query_promise) {
        td->create_handler<DeleteHistoryQuery>(std::move(query_promise))
            ->send(dialog_id, max_message_id, remove_from_dialog_list, revoke);
      };
      run_affected_history_query_until_complete(dialog_id, std::move(query), false, std::move(promise));
      break;
    }
    case DialogType::Channel:
      td_->create_handler<DeleteChannelHistoryQuery>(std::move(promise))
          ->send(dialog_id.get_channel_id(), max_message_id, allow_error);
      break;
    case DialogType::SecretChat:
      send_closure(G()->secret_chats_manager(), &SecretChatsManager::delete_all_messages,
                   dialog_id.get_secret_chat_id(), std::move(promise));
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
      break;
  }
}

void MessagesManager::on_message_media_edited(DialogId dialog_id, MessageId message_id, FileId file_id,
                                              FileId thumbnail_file_id, bool was_uploaded, bool was_thumbnail_uploaded,
                                              string file_reference, int32 schedule_date, uint64 generation,
                                              Result<int32> &&result) {
  // must not run getDifference

  CHECK(message_id.is_any_server());
  auto m = get_message({dialog_id, message_id});
  if (m == nullptr || m->edit_generation != generation) {
    // message is already deleted or was edited again
    return;
  }

  CHECK(m->edited_content != nullptr);
  if (result.is_ok()) {
    // message content has already been replaced from updateEdit{Channel,}Message
    // need only merge files from edited_content with their uploaded counterparts
    // updateMessageContent was already sent and needs to be sent again,
    // only if 'i' and 't' sizes from edited_content was added to the photo
    auto pts = result.ok();
    LOG(INFO) << "Successfully edited " << message_id << " in " << dialog_id << " with pts = " << pts
              << " and last edit pts = " << m->last_edit_pts;
    std::swap(m->content, m->edited_content);
    bool need_send_update_message_content = m->edited_content->get_type() == MessageContentType::Photo &&
                                            m->content->get_type() == MessageContentType::Photo;
    bool need_merge_files = pts != 0 && pts == m->last_edit_pts;
    update_message_content(dialog_id, m, std::move(m->edited_content), need_send_update_message_content,
                           need_merge_files, true);
  } else {
    LOG(INFO) << "Failed to edit " << message_id << " in " << dialog_id << ": " << result.error();
    if (was_uploaded) {
      if (was_thumbnail_uploaded) {
        CHECK(thumbnail_file_id.is_valid());
        // always delete partial remote location for the thumbnail, because it can't be reused anyway
        td_->file_manager_->delete_partial_remote_location(thumbnail_file_id);
      }
      CHECK(file_id.is_valid());
      auto error_message = result.error().message();
      if (begins_with(error_message, "FILE_PART_") && ends_with(error_message, "_MISSING")) {
        do_send_message(dialog_id, m, {to_integer<int32>(error_message.substr(10))});
        return;
      }

      if (result.error().code() != 429 && result.error().code() < 500 && !G()->close_flag()) {
        td_->file_manager_->delete_partial_remote_location(file_id);
      }
    } else if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(result.error())) {
      if (file_id.is_valid()) {
        VLOG(file_references) << "Receive " << result.error() << " for " << file_id;
        td_->file_manager_->delete_file_reference(file_id, file_reference);
        do_send_message(dialog_id, m, {-1});
        return;
      } else {
        LOG(ERROR) << "Receive file reference error, but have no file_id";
      }
    }

    cancel_upload_message_content_files(m->edited_content.get());

    if (dialog_id.get_type() != DialogType::SecretChat) {
      get_message_from_server({dialog_id, m->message_id}, Auto(), "on_message_media_edited");
    }
  }

  if (m->edited_schedule_date == schedule_date) {
    m->edited_schedule_date = 0;
  }
  m->edited_content = nullptr;
  m->edited_reply_markup = nullptr;
  m->edit_generation = 0;
  if (result.is_ok()) {
    m->edit_promise.set_value(Unit());
  } else {
    m->edit_promise.set_error(result.move_as_error());
  }
}

void MessagesManager::add_dialog_to_list(DialogId dialog_id, DialogListId dialog_list_id, Promise<Unit> &&promise) {
  LOG(INFO) << "Receive addChatToList request to add " << dialog_id << " to " << dialog_list_id;
  CHECK(!td_->auth_manager_->is_bot());

  Dialog *d = get_dialog_force(dialog_id, "add_dialog_to_list");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  if (d->order == DEFAULT_ORDER) {
    return promise.set_error(Status::Error(400, "Chat is not in a chat list"));
  }

  if (get_dialog_list(dialog_list_id) == nullptr) {
    return promise.set_error(Status::Error(400, "Chat list not found"));
  }

  if (dialog_list_id.is_filter()) {
    CHECK(is_update_chat_filters_sent_);
    auto dialog_filter_id = dialog_list_id.get_filter_id();
    auto old_dialog_filter = get_dialog_filter(dialog_filter_id);
    CHECK(old_dialog_filter != nullptr);
    if (InputDialogId::contains(old_dialog_filter->included_dialog_ids, dialog_id) ||
        InputDialogId::contains(old_dialog_filter->pinned_dialog_ids, dialog_id)) {
      return promise.set_value(Unit());
    }

    auto new_dialog_filter = make_unique<DialogFilter>(*old_dialog_filter);
    new_dialog_filter->included_dialog_ids.push_back(get_input_dialog_id(dialog_id));
    td::remove_if(new_dialog_filter->excluded_dialog_ids,
                  [dialog_id](InputDialogId input_dialog_id) { return dialog_id == input_dialog_id.get_dialog_id(); });

    auto status = new_dialog_filter->check_limits();
    if (status.is_error()) {
      return promise.set_error(std::move(status));
    }
    sort_dialog_filter_input_dialog_ids(new_dialog_filter.get(), "add_dialog_to_list");

    edit_dialog_filter(std::move(new_dialog_filter), "add_dialog_to_list");
    save_dialog_filters();
    send_update_chat_filters();

    if (dialog_id.get_type() != DialogType::SecretChat) {
      synchronize_dialog_filters();
    }

    return promise.set_value(Unit());
  }

  CHECK(dialog_list_id.is_folder());
  auto folder_id = dialog_list_id.get_folder_id();
  if (d->folder_id == folder_id) {
    return promise.set_value(Unit());
  }

  if (folder_id == FolderId::archive() &&
      (dialog_id == get_my_dialog_id() ||
       dialog_id == DialogId(ContactsManager::get_service_notifications_user_id()))) {
    return promise.set_error(Status::Error(400, "Chat can't be archived"));
  }

  set_dialog_folder_id(d, folder_id);

  if (dialog_id.get_type() != DialogType::SecretChat) {
    set_dialog_folder_id_on_server(dialog_id, false);
  }
  promise.set_value(Unit());
}

void MessagesManager::get_message_notifications_from_database(DialogId dialog_id, NotificationGroupId group_id,
                                                              NotificationId from_notification_id,
                                                              MessageId from_message_id, int32 limit,
                                                              Promise<vector<Notification>> promise) {
  if (!G()->parameters().use_message_db) {
    return promise.set_error(Status::Error(500, "There is no message database"));
  }
  if (td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(500, "Bots have no notifications"));
  }

  CHECK(dialog_id.is_valid());
  CHECK(group_id.is_valid());
  CHECK(!from_message_id.is_scheduled());
  CHECK(limit > 0);

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  if (d->message_notification_group.group_id != group_id && d->mention_notification_group.group_id != group_id) {
    return promise.set_value(vector<Notification>());
  }

  VLOG(notifications) << "Get " << limit << " message notifications from database in " << group_id << " from "
                      << dialog_id << " from " << from_notification_id << "/" << from_message_id;
  bool from_mentions = d->mention_notification_group.group_id == group_id;
  if (d->new_secret_chat_notification_id.is_valid()) {
    CHECK(dialog_id.get_type() == DialogType::SecretChat);
    vector<Notification> notifications;
    if (!from_mentions && d->new_secret_chat_notification_id.get() < from_notification_id.get()) {
      auto date = td_->contacts_manager_->get_secret_chat_date(dialog_id.get_secret_chat_id());
      if (date <= 0) {
        remove_new_secret_chat_notification(d, true);
      } else {
        notifications.emplace_back(d->new_secret_chat_notification_id, date, false,
                                   create_new_secret_chat_notification());
      }
    }
    return promise.set_value(std::move(notifications));
  }

  do_get_message_notifications_from_database(d, from_mentions, from_notification_id, from_notification_id,
                                             from_message_id, limit, std::move(promise));
}

vector<DialogListId> MessagesManager::get_dialog_lists_to_add_dialog(DialogId dialog_id) {
  vector<DialogListId> result;
  const Dialog *d = get_dialog_force(dialog_id, "get_dialog_lists_to_add_dialog");
  if (d == nullptr || d->order == DEFAULT_ORDER || !have_input_peer(dialog_id, AccessRights::Read)) {
    return result;
  }

  if (dialog_id != get_my_dialog_id() && dialog_id != DialogId(ContactsManager::get_service_notifications_user_id())) {
    result.push_back(DialogListId(d->folder_id == FolderId::archive() ? FolderId::main() : FolderId::archive()));
  }

  for (const auto &dialog_filter : dialog_filters_) {
    auto dialog_filter_id = dialog_filter->dialog_filter_id;
    if (!InputDialogId::contains(dialog_filter->included_dialog_ids, dialog_id) &&
        !InputDialogId::contains(dialog_filter->pinned_dialog_ids, dialog_id)) {
      // the dialog isn't added yet to the dialog list
      // check that it can be actually added
      if (dialog_filter->included_dialog_ids.size() + dialog_filter->pinned_dialog_ids.size() <
          DialogFilter::MAX_INCLUDED_FILTER_DIALOGS) {
        // fast path
        result.push_back(DialogListId(dialog_filter_id));
        continue;
      }

      auto new_dialog_filter = make_unique<DialogFilter>(*dialog_filter);
      new_dialog_filter->included_dialog_ids.push_back(get_input_dialog_id(dialog_id));
      td::remove_if(new_dialog_filter->excluded_dialog_ids, [dialog_id](InputDialogId input_dialog_id) {
        return dialog_id == input_dialog_id.get_dialog_id();
      });

      if (new_dialog_filter->check_limits().is_ok()) {
        result.push_back(DialogListId(dialog_filter_id));
      }
    }
  }
  return result;
}

class GetDialogFiltersQuery final : public Td::ResultHandler {
  Promise<vector<tl_object_ptr<telegram_api::dialogFilter>>> promise_;

 public:
  explicit GetDialogFiltersQuery(Promise<vector<tl_object_ptr<telegram_api::dialogFilter>>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::messages_getDialogFilters()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getDialogFilters>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::reload_dialog_filters() {
  if (G()->close_flag()) {
    return;
  }
  CHECK(!td_->auth_manager_->is_bot());
  if (are_dialog_filters_being_synchronized_ || are_dialog_filters_being_reloaded_) {
    need_dialog_filters_reload_ = true;
    return;
  }
  LOG(INFO) << "Reload chat filters from server";
  are_dialog_filters_being_reloaded_ = true;
  need_dialog_filters_reload_ = false;
  auto promise = PromiseCreator::lambda(
      [actor_id = actor_id(this)](Result<vector<tl_object_ptr<telegram_api::dialogFilter>>> r_filters) {
        send_closure(actor_id, &MessagesManager::on_get_dialog_filters, std::move(r_filters), false);
      });
  td_->create_handler<GetDialogFiltersQuery>(std::move(promise))->send();
}

bool MessagesManager::on_update_message_id(int64 random_id, MessageId new_message_id, const string &source) {
  if (!new_message_id.is_valid() || !new_message_id.is_server()) {
    LOG(ERROR) << "Receive " << new_message_id << " in updateMessageId with random_id " << random_id << " from "
               << source;
    return false;
  }

  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    // update about a new message sent from other device or a service message
    LOG(INFO) << "Receive not send outgoing " << new_message_id << " with random_id = " << random_id;
    return true;
  }

  auto dialog_id = it->second.get_dialog_id();
  auto old_message_id = it->second.get_message_id();

  being_sent_messages_.erase(it);

  if (!have_message_force({dialog_id, old_message_id}, "on_update_message_id")) {
    delete_sent_message_on_server(dialog_id, new_message_id);
    return true;
  }

  LOG(INFO) << "Save correspondence from " << new_message_id << " in " << dialog_id << " to " << old_message_id;
  CHECK(old_message_id.is_yet_unsent());
  update_message_ids_[FullMessageId(dialog_id, new_message_id)] = old_message_id;
  return true;
}

void MessagesManager::load_folder_dialog_list_from_database(FolderId folder_id, int32 limit, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  auto &folder = *get_dialog_folder(folder_id);
  LOG(INFO) << "Load " << limit << " chats in " << folder_id << " from database from "
            << folder.last_loaded_database_dialog_date_
            << ", last database server dialog date = " << folder.last_database_server_dialog_date_;

  CHECK(folder.load_dialog_list_limit_max_ == 0);
  folder.load_dialog_list_limit_max_ = limit;
  G()->td_db()->get_dialog_db_async()->get_dialogs(
      folder_id, folder.last_loaded_database_dialog_date_.get_order(),
      folder.last_loaded_database_dialog_date_.get_dialog_id(), limit,
      PromiseCreator::lambda([actor_id = actor_id(this), folder_id, limit,
                              promise = std::move(promise)](DialogDbGetDialogsResult result) mutable {
        send_closure(actor_id, &MessagesManager::on_get_dialogs_from_database, folder_id, limit, std::move(result),
                     std::move(promise));
      }));
}

void MessagesManager::speculatively_update_active_group_call_id(Dialog *d, const Message *m) {
  CHECK(m != nullptr);
  if (!m->message_id.is_any_server() || m->content->get_type() != MessageContentType::GroupCall) {
    return;
  }

  InputGroupCallId input_group_call_id;
  bool is_ended;
  std::tie(input_group_call_id, is_ended) = get_message_content_group_call_info(m->content.get());
  d->has_expected_active_group_call_id = true;
  if (is_ended) {
    d->expected_active_group_call_id = InputGroupCallId();
    if (d->active_group_call_id == input_group_call_id) {
      on_update_dialog_group_call_id(d->dialog_id, InputGroupCallId());
    }
  } else {
    d->expected_active_group_call_id = input_group_call_id;
    if (d->active_group_call_id != input_group_call_id && !td_->auth_manager_->is_bot()) {
      repair_dialog_active_group_call_id(d->dialog_id);
    }
  }
}

void MessagesManager::create_dialog(DialogId dialog_id, bool force, Promise<Unit> &&promise) {
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    if (!have_dialog_info_force(dialog_id)) {
      return promise.set_error(Status::Error(400, "Chat info not found"));
    }
    if (!have_input_peer(dialog_id, AccessRights::Read)) {
      return promise.set_error(Status::Error(400, "Can't access the chat"));
    }
  }

  if (force || td_->auth_manager_->is_bot() || dialog_id.get_type() == DialogType::SecretChat) {
    force_create_dialog(dialog_id, "create dialog");
  } else {
    const Dialog *d = get_dialog_force(dialog_id, "create_dialog");
    if (!is_dialog_inited(d)) {
      return send_get_dialog_query(dialog_id, std::move(promise), 0, "create_dialog");
    }
  }

  promise.set_value(Unit());
}

bool MessagesManager::has_dialogs_from_folder(const DialogList &list, const DialogFolder &folder) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (list.dialog_list_id.is_folder()) {
    return list.dialog_list_id.get_folder_id() == folder.folder_id;
  }
  if (list.dialog_list_id.is_filter()) {
    auto dialog_filter_id = list.dialog_list_id.get_filter_id();
    auto *filter = get_dialog_filter(dialog_filter_id);
    CHECK(filter != nullptr);
    if (filter->exclude_archived && filter->pinned_dialog_ids.empty() && filter->included_dialog_ids.empty()) {
      return folder.folder_id == FolderId::main();
    }
    return true;
  }
  UNREACHABLE();
  return false;
}

void MessagesManager::add_active_live_location(FullMessageId full_message_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (!active_live_location_full_message_ids_.insert(full_message_id).second) {
    return;
  }

  // TODO add timer for live location expiration

  if (!G()->parameters().use_message_db) {
    return;
  }

  if (are_active_live_location_messages_loaded_) {
    save_active_live_locations();
  } else if (load_active_live_location_messages_queries_.empty()) {
    // load active live locations and save after that
    get_active_live_location_messages(Auto());
  }
}

void MessagesManager::on_message_edited(FullMessageId full_message_id, int32 pts) {
  if (full_message_id == FullMessageId()) {
    return;
  }

  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog(dialog_id);
  Message *m = get_message(d, full_message_id.get_message_id());
  CHECK(m != nullptr);
  m->last_edit_pts = pts;
  if (td_->auth_manager_->is_bot()) {
    d->last_edited_message_id = m->message_id;
    send_update_message_edited(dialog_id, m);
  }
  update_used_hashtags(dialog_id, m);
}

bool MessagesManager::is_dialog_pinned_message_notifications_disabled(const Dialog *d) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(d != nullptr);
  if (d->notification_settings.use_default_disable_pinned_message_notifications) {
    auto scope = get_dialog_notification_setting_scope(d->dialog_id);
    return get_scope_notification_settings(scope)->disable_pinned_message_notifications;
  }

  return d->notification_settings.disable_pinned_message_notifications;
}

void MessagesManager::find_old_messages(const Message *m, MessageId max_message_id, vector<MessageId> &message_ids) {
  if (m == nullptr) {
    return;
  }

  find_old_messages(m->left.get(), max_message_id, message_ids);

  if (m->message_id <= max_message_id) {
    message_ids.push_back(m->message_id);

    find_old_messages(m->right.get(), max_message_id, message_ids);
  }
}

Status MessagesManager::set_dialog_client_data(DialogId dialog_id, string &&client_data) {
  Dialog *d = get_dialog_force(dialog_id, "set_dialog_client_data");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  d->client_data = std::move(client_data);
  on_dialog_updated(d->dialog_id, "set_dialog_client_data");
  return Status::OK();
}

void MessagesManager::update_list_last_pinned_dialog_date(DialogList &list) {
  CHECK(!td_->auth_manager_->is_bot());
  if (do_update_list_last_pinned_dialog_date(list)) {
    update_list_last_dialog_date(list);
  }
}

void MessagesManager::drop_common_dialogs_cache(UserId user_id) {
  auto it = found_common_dialogs_.find(user_id);
  if (it != found_common_dialogs_.end()) {
    it->second.is_outdated = true;
  }
}
}  // namespace td
