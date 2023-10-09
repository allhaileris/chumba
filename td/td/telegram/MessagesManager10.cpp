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








#include <utility>

namespace td {
}  // namespace td
namespace td {

std::pair<DialogId, unique_ptr<MessagesManager::Message>> MessagesManager::create_message(MessageInfo &&message_info,
                                                                                          bool is_channel_message) {
  DialogId dialog_id = message_info.dialog_id;
  MessageId message_id = message_info.message_id;
  if ((!message_id.is_valid() && !message_id.is_valid_scheduled()) || !dialog_id.is_valid()) {
    if (message_id != MessageId() || dialog_id != DialogId()) {
      LOG(ERROR) << "Receive " << message_id << " in " << dialog_id;
    }
    return {DialogId(), nullptr};
  }
  if (message_id.is_yet_unsent() || message_id.is_local()) {
    LOG(ERROR) << "Receive " << message_id;
    return {DialogId(), nullptr};
  }

  CHECK(message_info.content != nullptr);

  auto dialog_type = dialog_id.get_type();
  UserId sender_user_id = message_info.sender_user_id;
  DialogId sender_dialog_id = message_info.sender_dialog_id;
  if (!sender_user_id.is_valid()) {
    if (sender_user_id != UserId()) {
      LOG(ERROR) << "Receive invalid " << sender_user_id;
      sender_user_id = UserId();
    }
    if (!is_broadcast_channel(dialog_id) && td_->auth_manager_->is_bot()) {
      if (dialog_id == sender_dialog_id) {
        td_->contacts_manager_->add_anonymous_bot_user();
      } else {
        td_->contacts_manager_->add_service_notifications_user();
        td_->contacts_manager_->add_channel_bot_user();
      }
    }
  }
  if (sender_dialog_id.is_valid()) {
    if (dialog_type == DialogType::User || dialog_type == DialogType::SecretChat) {
      LOG(ERROR) << "Receive " << message_id << " sent by " << sender_dialog_id << " in " << dialog_id;
      return {DialogId(), nullptr};
    }
  } else if (sender_dialog_id != DialogId()) {
    LOG(ERROR) << "Receive invalid " << sender_dialog_id;
    sender_dialog_id = DialogId();
  }
  if (message_id.is_scheduled()) {
    is_channel_message = (dialog_type == DialogType::Channel);
  }

  int32 flags = message_info.flags;
  bool is_outgoing = (flags & MESSAGE_FLAG_IS_OUT) != 0;
  bool is_silent = (flags & MESSAGE_FLAG_IS_SILENT) != 0;
  bool is_channel_post = (flags & MESSAGE_FLAG_IS_POST) != 0;
  bool is_legacy = (flags & MESSAGE_FLAG_IS_LEGACY) != 0;
  bool hide_edit_date = (flags & MESSAGE_FLAG_HIDE_EDIT_DATE) != 0;
  bool is_from_scheduled = (flags & MESSAGE_FLAG_IS_FROM_SCHEDULED) != 0;
  bool is_pinned = (flags & MESSAGE_FLAG_IS_PINNED) != 0;
  bool noforwards = (flags & MESSAGE_FLAG_NOFORWARDS) != 0;

  LOG_IF(ERROR, is_channel_message != (dialog_type == DialogType::Channel))
      << "Receive wrong is_channel_message for " << message_id << " in " << dialog_id;
  if (is_channel_post && !is_broadcast_channel(dialog_id)) {
    LOG(ERROR) << "Receive is_channel_post for " << message_id << " in " << dialog_id;
    is_channel_post = false;
  }

  UserId my_id = td_->contacts_manager_->get_my_id();
  DialogId my_dialog_id = DialogId(my_id);
  if (dialog_id == my_dialog_id && (sender_user_id != my_id || sender_dialog_id.is_valid())) {
    LOG(ERROR) << "Receive " << sender_user_id << "/" << sender_dialog_id << " as a sender of " << message_id
               << " instead of self";
    sender_user_id = my_id;
    sender_dialog_id = DialogId();
  }

  bool supposed_to_be_outgoing = sender_user_id == my_id && !(dialog_id == my_dialog_id && !message_id.is_scheduled());
  if (sender_user_id.is_valid() && supposed_to_be_outgoing != is_outgoing) {
    LOG(ERROR) << "Receive wrong message out flag: me is " << my_id << ", message is from " << sender_user_id
               << ", flags = " << flags << " for " << message_id << " in " << dialog_id;
    is_outgoing = supposed_to_be_outgoing;

    /*
    // it is useless to call getChannelsDifference, because the channel pts will be increased already
    if (dialog_type == DialogType::Channel && !running_get_difference_ && !running_get_channel_difference(dialog_id) &&
        get_channel_difference_to_log_event_id_.count(dialog_id) == 0) {
      // it is safer to completely ignore the message and re-get it through getChannelsDifference
      Dialog *d = get_dialog(dialog_id);
      if (d != nullptr) {
        channel_get_difference_retry_timeout_.add_timeout_in(dialog_id.get(), 0.001);
        return {DialogId(), nullptr};
      }
    }
    */
  }

  MessageId reply_to_message_id = message_info.reply_to_message_id;  // for secret messages
  DialogId reply_in_dialog_id;
  MessageId top_thread_message_id;
  if (message_info.reply_header != nullptr) {
    reply_to_message_id = MessageId(ServerMessageId(message_info.reply_header->reply_to_msg_id_));
    auto reply_to_peer_id = std::move(message_info.reply_header->reply_to_peer_id_);
    if (reply_to_peer_id != nullptr) {
      reply_in_dialog_id = DialogId(reply_to_peer_id);
      if (!reply_in_dialog_id.is_valid()) {
        LOG(ERROR) << "Receive reply in invalid " << to_string(reply_to_peer_id);
        reply_to_message_id = MessageId();
        reply_in_dialog_id = DialogId();
      }
      if (reply_in_dialog_id == dialog_id) {
        reply_in_dialog_id = DialogId();  // just in case
      }
    }
    if (reply_to_message_id.is_valid() && !td_->auth_manager_->is_bot() && !message_id.is_scheduled() &&
        !reply_in_dialog_id.is_valid()) {
      if ((message_info.reply_header->flags_ & telegram_api::messageReplyHeader::REPLY_TO_TOP_ID_MASK) != 0) {
        top_thread_message_id = MessageId(ServerMessageId(message_info.reply_header->reply_to_top_id_));
      } else if (!is_broadcast_channel(dialog_id)) {
        top_thread_message_id = reply_to_message_id;
      }
    }
  }
  fix_server_reply_to_message_id(dialog_id, message_id, reply_in_dialog_id, reply_to_message_id);
  fix_server_reply_to_message_id(dialog_id, message_id, reply_in_dialog_id, top_thread_message_id);

  UserId via_bot_user_id = message_info.via_bot_user_id;
  if (!via_bot_user_id.is_valid()) {
    via_bot_user_id = UserId();
  }

  int32 date = message_info.date;
  if (date <= 0) {
    LOG(ERROR) << "Wrong date = " << date << " received in " << message_id << " in " << dialog_id;
    date = 1;
  }

  int32 edit_date = message_info.edit_date;
  if (edit_date < 0) {
    LOG(ERROR) << "Wrong edit_date = " << edit_date << " received in " << message_id << " in " << dialog_id;
    edit_date = 0;
  }

  auto content_type = message_info.content->get_type();
  if (hide_edit_date && td_->auth_manager_->is_bot()) {
    hide_edit_date = false;
  }
  if (hide_edit_date && content_type == MessageContentType::LiveLocation) {
    hide_edit_date = false;
  }

  int32 ttl_period = message_info.ttl_period;
  if (ttl_period < 0 || (message_id.is_scheduled() && ttl_period != 0)) {
    LOG(ERROR) << "Wrong TTL period = " << ttl_period << " received in " << message_id << " in " << dialog_id;
    ttl_period = 0;
  }

  int32 ttl = message_info.ttl;
  bool is_content_secret = is_secret_message_content(ttl, content_type);  // must be calculated before TTL is adjusted
  if (ttl < 0 || (message_id.is_scheduled() && ttl != 0)) {
    LOG(ERROR) << "Wrong TTL = " << ttl << " received in " << message_id << " in " << dialog_id;
    ttl = 0;
  } else if (ttl > 0) {
    ttl = max(ttl, get_message_content_duration(message_info.content.get(), td_) + 1);
  }

  int32 view_count = message_info.view_count;
  if (view_count < 0) {
    LOG(ERROR) << "Wrong view_count = " << view_count << " received in " << message_id << " in " << dialog_id;
    view_count = 0;
  }
  int32 forward_count = message_info.forward_count;
  if (forward_count < 0) {
    LOG(ERROR) << "Wrong forward_count = " << forward_count << " received in " << message_id << " in " << dialog_id;
    forward_count = 0;
  }
  MessageReplyInfo reply_info(td_, std::move(message_info.reply_info), td_->auth_manager_->is_bot());
  if (!top_thread_message_id.is_valid() && !is_broadcast_channel(dialog_id) &&
      is_active_message_reply_info(dialog_id, reply_info) && !message_id.is_scheduled()) {
    top_thread_message_id = message_id;
  }
  if (top_thread_message_id.is_valid() && dialog_type != DialogType::Channel) {
    top_thread_message_id = MessageId();
  }

  bool has_forward_info = message_info.forward_header != nullptr;

  if (sender_dialog_id.is_valid() && sender_dialog_id != dialog_id && have_dialog_info_force(sender_dialog_id)) {
    force_create_dialog(sender_dialog_id, "create_message", sender_dialog_id.get_type() != DialogType::User);
  }

  LOG(INFO) << "Receive " << message_id << " in " << dialog_id << " from " << sender_user_id << "/" << sender_dialog_id;

  auto message = make_unique<Message>();
  set_message_id(message, message_id);
  message->sender_user_id = sender_user_id;
  message->sender_dialog_id = sender_dialog_id;
  message->date = date;
  message->ttl_period = ttl_period;
  message->ttl = ttl;
  message->disable_web_page_preview = message_info.disable_web_page_preview;
  message->edit_date = edit_date;
  message->random_id = message_info.random_id;
  message->forward_info = get_message_forward_info(std::move(message_info.forward_header));
  message->reply_to_message_id = reply_to_message_id;
  message->reply_in_dialog_id = reply_in_dialog_id;
  message->top_thread_message_id = top_thread_message_id;
  message->via_bot_user_id = via_bot_user_id;
  message->restriction_reasons = std::move(message_info.restriction_reasons);
  message->author_signature = std::move(message_info.author_signature);
  message->is_outgoing = is_outgoing;
  message->is_channel_post = is_channel_post;
  message->contains_mention =
      !is_outgoing && dialog_type != DialogType::User &&
      ((flags & MESSAGE_FLAG_HAS_MENTION) != 0 || content_type == MessageContentType::PinMessage);
  message->contains_unread_mention =
      !message_id.is_scheduled() && message_id.is_server() && message->contains_mention &&
      (flags & MESSAGE_FLAG_HAS_UNREAD_CONTENT) != 0 &&
      (dialog_type == DialogType::Chat || (dialog_type == DialogType::Channel && !is_broadcast_channel(dialog_id)));
  message->disable_notification = is_silent;
  message->is_content_secret = is_content_secret;
  message->hide_edit_date = hide_edit_date;
  message->is_from_scheduled = is_from_scheduled;
  message->is_pinned = is_pinned;
  message->noforwards = noforwards;
  message->view_count = view_count;
  message->forward_count = forward_count;
  message->reply_info = std::move(reply_info);
  message->legacy_layer = (is_legacy ? MTPROTO_LAYER : 0);
  message->content = std::move(message_info.content);
  message->reply_markup = get_reply_markup(std::move(message_info.reply_markup), td_->auth_manager_->is_bot(), false,
                                           message->contains_mention || dialog_type == DialogType::User);

  if (content_type == MessageContentType::ExpiredPhoto || content_type == MessageContentType::ExpiredVideo) {
    CHECK(message->ttl == 0);  // TTL is ignored/set to 0 if the message has already been expired
    if (message->reply_markup != nullptr) {
      if (message->reply_markup->type != ReplyMarkup::Type::InlineKeyboard) {
        message->had_reply_markup = true;
      }
      message->reply_markup = nullptr;
    }
    message->reply_to_message_id = MessageId();
    message->reply_in_dialog_id = DialogId();
    message->top_thread_message_id = MessageId();
    message->linked_top_thread_message_id = MessageId();
  }

  if (message_info.media_album_id != 0) {
    if (!is_allowed_media_group_content(content_type)) {
      if (content_type != MessageContentType::Unsupported) {
        LOG(ERROR) << "Receive media group identifier " << message_info.media_album_id << " in " << message_id
                   << " from " << dialog_id << " with content "
                   << oneline(to_string(get_message_content_object(message->content.get(), td_, dialog_id,
                                                                   message->date, is_content_secret, false, -1)));
      }
    } else {
      message->media_album_id = message_info.media_album_id;
    }
  }

  if (message->forward_info == nullptr && has_forward_info) {
    message->had_forward_info = true;
  }

  return {dialog_id, std::move(message)};
}

void MessagesManager::send_update_unread_chat_count(DialogList &list, DialogId dialog_id, bool force,
                                                    const char *source, bool from_database) {
  if (td_->auth_manager_->is_bot() || !G()->parameters().use_message_db) {
    return;
  }

  auto dialog_list_id = list.dialog_list_id;
  CHECK(list.is_dialog_unread_count_inited_);
  if (list.unread_dialog_muted_marked_count_ < 0 ||
      list.unread_dialog_marked_count_ < list.unread_dialog_muted_marked_count_ ||
      list.unread_dialog_muted_count_ < list.unread_dialog_muted_marked_count_ ||
      list.unread_dialog_total_count_ + list.unread_dialog_muted_marked_count_ <
          list.unread_dialog_muted_count_ + list.unread_dialog_marked_count_) {
    LOG(ERROR) << "Unread chat count became invalid in " << dialog_list_id << ": " << list.unread_dialog_total_count_
               << '/' << list.unread_dialog_total_count_ - list.unread_dialog_muted_count_ << '/'
               << list.unread_dialog_marked_count_ << '/'
               << list.unread_dialog_marked_count_ - list.unread_dialog_muted_marked_count_ << " from " << source
               << " and " << dialog_id;
    if (list.unread_dialog_muted_marked_count_ < 0) {
      list.unread_dialog_muted_marked_count_ = 0;
    }
    if (list.unread_dialog_marked_count_ < list.unread_dialog_muted_marked_count_) {
      list.unread_dialog_marked_count_ = list.unread_dialog_muted_marked_count_;
    }
    if (list.unread_dialog_muted_count_ < list.unread_dialog_muted_marked_count_) {
      list.unread_dialog_muted_count_ = list.unread_dialog_muted_marked_count_;
    }
    if (list.unread_dialog_total_count_ + list.unread_dialog_muted_marked_count_ <
        list.unread_dialog_muted_count_ + list.unread_dialog_marked_count_) {
      list.unread_dialog_total_count_ =
          list.unread_dialog_muted_count_ + list.unread_dialog_marked_count_ - list.unread_dialog_muted_marked_count_;
    }
  }

  if (!from_database) {
    save_unread_chat_count(list);
  }

  bool need_postpone = !force && running_get_difference_;
  int32 unread_unmuted_count = list.unread_dialog_total_count_ - list.unread_dialog_muted_count_;
  int32 unread_unmuted_marked_count = list.unread_dialog_marked_count_ - list.unread_dialog_muted_marked_count_;
  LOG(INFO) << (need_postpone ? "Postpone" : "Send") << " updateUnreadChatCount in " << dialog_list_id << " to "
            << list.in_memory_dialog_total_count_ << '/' << list.server_dialog_total_count_ << '+'
            << list.secret_chat_total_count_ << '/' << list.unread_dialog_total_count_ << '/' << unread_unmuted_count
            << '/' << list.unread_dialog_marked_count_ << '/' << unread_unmuted_marked_count << " from " << source
            << " and " << dialog_id;
  if (need_postpone) {
    postponed_unread_chat_count_updates_.insert(dialog_list_id);
  } else {
    postponed_unread_chat_count_updates_.erase(dialog_list_id);
    send_closure(G()->td(), &Td::send_update, get_update_unread_chat_count_object(list));
  }
}

class GetOnlinesQuery final : public Td::ResultHandler {
  DialogId dialog_id_;

 public:
  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;
    CHECK(dialog_id.get_type() == DialogType::Channel);
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_getOnlines(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getOnlines>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    td_->messages_manager_->on_update_dialog_online_member_count(dialog_id_, result->onlines_, true);
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetOnlinesQuery");
    td_->messages_manager_->on_update_dialog_online_member_count(dialog_id_, 0, true);
  }
};

void MessagesManager::on_update_dialog_online_member_count_timeout(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Expired timeout for number of online members in " << dialog_id;
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  if (!d->is_opened) {
    send_update_chat_online_member_count(dialog_id, 0);
    return;
  }

  if (dialog_id.get_type() == DialogType::Channel && !is_broadcast_channel(dialog_id)) {
    auto participant_count = td_->contacts_manager_->get_channel_participant_count(dialog_id.get_channel_id());
    if (participant_count == 0 || participant_count >= 195) {
      td_->create_handler<GetOnlinesQuery>()->send(dialog_id);
    } else {
      td_->contacts_manager_->get_channel_participants(dialog_id.get_channel_id(),
                                                       td_api::make_object<td_api::supergroupMembersFilterRecent>(),
                                                       string(), 0, 200, 200, Auto());
    }
    return;
  }
  if (dialog_id.get_type() == DialogType::Chat) {
    // we need actual online status state, so we need to reget chat participants
    td_->contacts_manager_->repair_chat_participants(dialog_id.get_chat_id());
    return;
  }
}

void MessagesManager::read_history_outbox(DialogId dialog_id, MessageId max_message_id, int32 read_date) {
  CHECK(!max_message_id.is_scheduled());

  if (td_->auth_manager_->is_bot()) {
    return;
  }

  Dialog *d = get_dialog_force(dialog_id, "read_history_outbox");
  if (d != nullptr) {
    if (!max_message_id.is_valid()) {
      LOG(ERROR) << "Receive read outbox update in " << dialog_id << " with " << max_message_id;
      return;
    }
    if (max_message_id <= d->last_read_outbox_message_id) {
      LOG(INFO) << "Receive read outbox update up to " << max_message_id
                << ", but all messages have already been read up to " << d->last_read_outbox_message_id;
      return;
    }

    if (max_message_id.is_yet_unsent()) {
      LOG(ERROR) << "Tried to update last read outbox message with " << max_message_id;
      return;
    }

    // it is impossible for just sent outgoing messages because updates are ordered by pts
    LOG_IF(INFO, d->last_new_message_id.is_valid() && max_message_id > d->last_new_message_id &&
                     dialog_id.get_type() != DialogType::Channel)
        << "Receive read outbox update about unknown " << max_message_id << " in " << dialog_id << " with last new "
        << d->last_new_message_id << ". Possible only for deleted outgoing message";

    if (dialog_id.get_type() == DialogType::SecretChat) {
      double server_time = Time::now();
      double read_time = server_time;
      if (read_date <= 0) {
        LOG(ERROR) << "Receive wrong read date " << read_date << " in " << dialog_id;
      } else if (read_date < server_time) {
        read_time = read_date;
      }
      ttl_read_history(d, true, max_message_id, d->last_read_outbox_message_id, read_time);
    }

    set_dialog_last_read_outbox_message_id(d, max_message_id);
  } else {
    LOG(INFO) << "Receive read outbox update about unknown " << dialog_id;
  }
}

void MessagesManager::update_last_dialog_date(FolderId folder_id) {
  CHECK(!td_->auth_manager_->is_bot());
  auto *folder = get_dialog_folder(folder_id);
  CHECK(folder != nullptr);
  auto old_last_dialog_date = folder->folder_last_dialog_date_;
  folder->folder_last_dialog_date_ = folder->last_server_dialog_date_;
  CHECK(old_last_dialog_date <= folder->folder_last_dialog_date_);

  LOG(INFO) << "Update last dialog date in " << folder_id << " from " << old_last_dialog_date << " to "
            << folder->folder_last_dialog_date_;
  LOG(INFO) << "Know about " << folder->ordered_dialogs_.size() << " chats";

  if (old_last_dialog_date != folder->folder_last_dialog_date_) {
    for (auto &dialog_list : dialog_lists_) {
      update_list_last_pinned_dialog_date(dialog_list.second);
      update_list_last_dialog_date(dialog_list.second);
    }
  }

  if (G()->parameters().use_message_db &&
      folder->last_database_server_dialog_date_ < folder->last_server_dialog_date_) {
    auto last_server_dialog_date_string = PSTRING() << folder->last_server_dialog_date_.get_order() << ' '
                                                    << folder->last_server_dialog_date_.get_dialog_id().get();
    G()->td_db()->get_binlog_pmc()->set(PSTRING() << "last_server_dialog_date" << folder_id.get(),
                                        last_server_dialog_date_string);
    LOG(INFO) << "Save last server dialog date " << folder->last_server_dialog_date_;
    folder->last_database_server_dialog_date_ = folder->last_server_dialog_date_;
    folder->last_loaded_database_dialog_date_ = folder->last_server_dialog_date_;
  }
}

int32 MessagesManager::calc_new_unread_count_from_the_end(Dialog *d, MessageId max_message_id, MessageType type,
                                                          int32 hint_unread_count) const {
  CHECK(!max_message_id.is_scheduled());
  int32 unread_count = 0;
  MessagesConstIterator it(d, MessageId::max());
  while (*it != nullptr && (*it)->message_id > max_message_id) {
    if (has_incoming_notification(d->dialog_id, *it) && (*it)->message_id.get_type() == type) {
      unread_count++;
    }
    --it;
  }

  bool is_count_exact = d->last_message_id.is_valid() && *it != nullptr;
  if (hint_unread_count >= 0) {
    if (is_count_exact) {
      if (hint_unread_count == unread_count) {
        return hint_unread_count;
      }
    } else {
      if (hint_unread_count >= unread_count) {
        return hint_unread_count;
      }
    }

    // hint_unread_count is definitely wrong, ignore it

    if (need_unread_counter(d->order)) {
      LOG(ERROR) << "Receive hint_unread_count = " << hint_unread_count << ", but found " << unread_count
                 << " unread messages in " << d->dialog_id;
    }
  }

  if (!is_count_exact) {
    // unread count is likely to be calculated wrong, so ignore it
    return -1;
  }

  LOG(INFO) << "Found " << unread_count << " unread messages in " << d->dialog_id << " from the end";
  return unread_count;
}

NotificationId MessagesManager::get_next_notification_id(Dialog *d, NotificationGroupId notification_group_id,
                                                         MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(!message_id.is_scheduled());
  NotificationId notification_id;
  do {
    notification_id = td_->notification_manager_->get_next_notification_id();
    if (!notification_id.is_valid()) {
      return NotificationId();
    }
  } while (d->notification_id_to_message_id.count(notification_id) != 0 ||
           d->new_secret_chat_notification_id == notification_id ||
           notification_id.get() <= d->message_notification_group.last_notification_id.get() ||
           notification_id.get() <= d->message_notification_group.max_removed_notification_id.get() ||
           notification_id.get() <= d->mention_notification_group.last_notification_id.get() ||
           notification_id.get() <= d->mention_notification_group.max_removed_notification_id.get());  // just in case
  if (message_id.is_valid()) {
    add_notification_id_to_message_id_correspondence(d, notification_id, message_id);
  }
  return notification_id;
}

const MessagesManager::Message *MessagesManager::get_message(const Dialog *d, MessageId message_id) {
  if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
    return nullptr;
  }

  CHECK(d != nullptr);
  bool is_scheduled = message_id.is_scheduled();
  if (is_scheduled && message_id.is_scheduled_server()) {
    auto server_message_id = message_id.get_scheduled_server_message_id();
    auto it = d->scheduled_message_date.find(server_message_id);
    if (it != d->scheduled_message_date.end()) {
      int32 date = it->second;
      message_id = MessageId(server_message_id, date);
      CHECK(message_id.is_scheduled_server());
    }
  }
  auto result = treap_find_message(is_scheduled ? &d->scheduled_messages : &d->messages, message_id)->get();
  if (result != nullptr && !is_scheduled) {
    result->last_access_date = G()->unix_time_cached();
  }
  LOG(INFO) << "Search for " << message_id << " in " << d->dialog_id << " found " << result;
  return result;
}

void MessagesManager::reload_dialog_info_full(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      send_closure_later(td_->contacts_manager_actor_, &ContactsManager::reload_user_full, dialog_id.get_user_id());
      return;
    case DialogType::Chat:
      send_closure_later(td_->contacts_manager_actor_, &ContactsManager::reload_chat_full, dialog_id.get_chat_id(),
                         Promise<Unit>());
      return;
    case DialogType::Channel:
      send_closure_later(td_->contacts_manager_actor_, &ContactsManager::reload_channel_full,
                         dialog_id.get_channel_id(), Promise<Unit>(), "reload_dialog_info_full");
      return;
    case DialogType::SecretChat:
      return;
    case DialogType::None:
    default:
      UNREACHABLE();
      return;
  }
}

void MessagesManager::do_remove_message_notification(DialogId dialog_id, bool from_mentions,
                                                     NotificationId notification_id,
                                                     vector<MessagesDbDialogMessage> result) {
  if (result.empty() || G()->close_flag()) {
    return;
  }
  CHECK(result.size() == 1);

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  auto m = on_get_message_from_database(d, result[0], false, "do_remove_message_notification");
  if (m != nullptr && m->notification_id == notification_id &&
      is_from_mention_notification_group(d, m) == from_mentions && is_message_notification_active(d, m)) {
    remove_message_notification_id(d, m, false, false);
  }
}

void MessagesManager::on_update_read_channel_inbox(tl_object_ptr<telegram_api::updateReadChannelInbox> &&update) {
  ChannelId channel_id(update->channel_id_);
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id << " in updateReadChannelInbox";
    return;
  }

  FolderId folder_id;
  if ((update->flags_ & telegram_api::updateReadChannelInbox::FOLDER_ID_MASK) != 0) {
    folder_id = FolderId(update->folder_id_);
  }
  on_update_dialog_folder_id(DialogId(channel_id), folder_id);
  on_read_channel_inbox(channel_id, MessageId(ServerMessageId(update->max_id_)), update->still_unread_count_,
                        update->pts_, "updateReadChannelInbox");
}

string MessagesManager::get_dialog_username(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->get_user_username(dialog_id.get_user_id());
    case DialogType::Chat:
      return string();
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_username(dialog_id.get_channel_id());
    case DialogType::SecretChat:
      return td_->contacts_manager_->get_secret_chat_username(dialog_id.get_secret_chat_id());
    case DialogType::None:
    default:
      UNREACHABLE();
      return string();
  }
}

void MessagesManager::on_update_dialog_message_ttl(DialogId dialog_id, MessageTtl message_ttl) {
  auto d = get_dialog_force(dialog_id, "on_update_dialog_message_ttl");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (d->message_ttl != message_ttl) {
    d->message_ttl = message_ttl;
    d->is_message_ttl_inited = true;
    send_update_chat_message_ttl(d);
  }
  if (!d->is_message_ttl_inited) {
    d->is_message_ttl_inited = true;
    on_dialog_updated(dialog_id, "on_update_dialog_message_ttl");
  }
}

void MessagesManager::on_message_changed(const Dialog *d, const Message *m, bool need_send_update, const char *source) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  if (need_send_update && m->message_id == d->last_message_id) {
    send_update_chat_last_message_impl(d, source);
  }

  if (m->message_id == d->last_database_message_id) {
    on_dialog_updated(d->dialog_id, source);
  }

  if (!m->message_id.is_yet_unsent()) {
    add_message_to_database(d, m, source);
  }
}

tl_object_ptr<telegram_api::InputNotifyPeer> MessagesManager::get_input_notify_peer(DialogId dialog_id) const {
  if (get_dialog(dialog_id) == nullptr) {
    return nullptr;
  }
  auto input_peer = get_input_peer(dialog_id, AccessRights::Read);
  if (input_peer == nullptr) {
    return nullptr;
  }
  return make_tl_object<telegram_api::inputNotifyPeer>(std::move(input_peer));
}

void MessagesManager::get_message(FullMessageId full_message_id, Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(full_message_id.get_dialog_id(), "get_message");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  get_message_force_from_server(d, full_message_id.get_message_id(), std::move(promise));
}

void MessagesManager::before_get_difference() {
  running_get_difference_ = true;

  // scheduled messages are not returned in getDifference, so we must always reget them after it
  scheduled_messages_sync_generation_++;
}

void MessagesManager::on_dialog_info_full_invalidated(DialogId dialog_id) {
  Dialog *d = get_dialog(dialog_id);
  if (d != nullptr && d->is_opened) {
    reload_dialog_info_full(dialog_id);
  }
}
}  // namespace td
