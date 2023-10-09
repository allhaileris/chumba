//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChatId.h"


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

#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"

#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"

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










namespace td {
}  // namespace td
namespace td {

bool MessagesManager::update_message(Dialog *d, Message *old_message, unique_ptr<Message> new_message,
                                     bool *need_update_dialog_pos, bool is_message_in_dialog) {
  CHECK(d != nullptr);
  CHECK(old_message != nullptr);
  CHECK(new_message != nullptr);
  LOG_CHECK(old_message->message_id == new_message->message_id)
      << d->dialog_id << ' ' << old_message->message_id << ' ' << new_message->message_id << ' '
      << is_message_in_dialog;
  CHECK(old_message->random_y == new_message->random_y);
  CHECK(need_update_dialog_pos != nullptr);

  DialogId dialog_id = d->dialog_id;
  MessageId message_id = old_message->message_id;
  auto old_content_type = old_message->content->get_type();
  auto new_content_type = new_message->content->get_type();
  bool is_scheduled = message_id.is_scheduled();
  bool need_send_update = false;
  bool is_new_available = new_content_type != MessageContentType::ChatDeleteHistory;
  bool replace_legacy = (old_message->legacy_layer != 0 &&
                         (new_message->legacy_layer == 0 || old_message->legacy_layer < new_message->legacy_layer)) ||
                        old_content_type == MessageContentType::Unsupported;
  bool was_visible_message_reply_info = is_visible_message_reply_info(dialog_id, old_message);
  if (old_message->date != new_message->date) {
    if (new_message->date > 0) {
      if (!(is_scheduled || message_id.is_yet_unsent() ||
            (message_id.is_server() && message_id.get_server_message_id().get() == 1) ||
            old_content_type == MessageContentType::ChannelMigrateFrom ||
            old_content_type == MessageContentType::ChannelCreate)) {
        LOG(ERROR) << "Date has changed for " << message_id << " in " << dialog_id << " from " << old_message->date
                   << " to " << new_message->date << ", message content type is " << old_content_type << '/'
                   << new_content_type;
      }
      CHECK(old_message->date > 0);
      LOG(DEBUG) << "Message date has changed from " << old_message->date << " to " << new_message->date;
      old_message->date = new_message->date;
      if (!is_scheduled && d->last_message_id == message_id) {
        *need_update_dialog_pos = true;
      }
      need_send_update = true;
    } else {
      LOG(ERROR) << "Receive " << message_id << " in " << dialog_id << " with wrong date " << new_message->date
                 << ", message content type is " << old_content_type << '/' << new_content_type;
    }
  }
  if (old_message->date == old_message->edited_schedule_date) {
    old_message->edited_schedule_date = 0;
  }
  bool is_edited = false;
  int32 old_shown_edit_date = old_message->hide_edit_date ? 0 : old_message->edit_date;
  if (old_message->edit_date != new_message->edit_date) {
    if (new_message->edit_date > 0) {
      if (new_message->edit_date > old_message->edit_date) {
        LOG(DEBUG) << "Message edit date has changed from " << old_message->edit_date << " to "
                   << new_message->edit_date;

        old_message->edit_date = new_message->edit_date;
        need_send_update = true;
      }
    } else {
      LOG(ERROR) << "Receive " << message_id << " in " << dialog_id << " of type " << old_content_type << "/"
                 << new_content_type << " with wrong edit date " << new_message->edit_date
                 << ", old edit date = " << old_message->edit_date;
    }
  }

  if (old_message->author_signature != new_message->author_signature) {
    LOG(DEBUG) << "Author signature has changed for " << message_id << " in " << dialog_id << " sent by "
               << old_message->sender_user_id << "/" << new_message->sender_user_id << " or "
               << old_message->sender_dialog_id << "/" << new_message->sender_dialog_id << " from "
               << old_message->author_signature << " to " << new_message->author_signature;
    old_message->author_signature = std::move(new_message->author_signature);
    need_send_update = true;
  }
  if (old_message->sender_user_id != new_message->sender_user_id) {
    // there can be race for sent signed posts or changed anonymous flag
    LOG_IF(ERROR, old_message->sender_user_id != UserId() && new_message->sender_user_id != UserId())
        << message_id << " in " << dialog_id << " has changed sender from " << old_message->sender_user_id << " to "
        << new_message->sender_user_id << ", message content type is " << old_content_type << '/' << new_content_type;

    LOG_IF(WARNING, (new_message->sender_user_id.is_valid() || old_message->author_signature.empty()) &&
                        !old_message->sender_dialog_id.is_valid() && !new_message->sender_dialog_id.is_valid())
        << "Update message sender from " << old_message->sender_user_id << " to " << new_message->sender_user_id
        << " in " << dialog_id;
    LOG(DEBUG) << "Change message sender";
    old_message->sender_user_id = new_message->sender_user_id;
    need_send_update = true;
  }
  if (old_message->sender_dialog_id != new_message->sender_dialog_id) {
    // there can be race for changed anonymous flag
    LOG_IF(ERROR, old_message->sender_dialog_id != DialogId() && new_message->sender_dialog_id != DialogId())
        << message_id << " in " << dialog_id << " has changed sender from " << old_message->sender_dialog_id << " to "
        << new_message->sender_dialog_id << ", message content type is " << old_content_type << '/' << new_content_type;

    LOG(DEBUG) << "Change message sender";
    old_message->sender_dialog_id = new_message->sender_dialog_id;
    need_send_update = true;
  }
  if (old_message->forward_info == nullptr) {
    if (new_message->forward_info != nullptr) {
      if (!replace_legacy) {
        LOG(ERROR) << message_id << " in " << dialog_id << " has received forward info " << *new_message->forward_info
                   << ", really forwarded from " << old_message->real_forward_from_message_id << " in "
                   << old_message->real_forward_from_dialog_id << ", message content type is " << old_content_type
                   << '/' << new_content_type;
      }
      old_message->forward_info = std::move(new_message->forward_info);
      need_send_update = true;
    }
  } else {
    if (new_message->forward_info != nullptr) {
      if (old_message->forward_info->author_signature != new_message->forward_info->author_signature) {
        old_message->forward_info->author_signature = new_message->forward_info->author_signature;
        LOG(DEBUG) << "Change message signature";
        need_send_update = true;
      }
      if (*old_message->forward_info != *new_message->forward_info) {
        bool need_warning = [&] {
          if (replace_legacy) {
            return false;
          }
          if (!is_scheduled && !message_id.is_yet_unsent()) {
            return true;
          }
          return !is_forward_info_sender_hidden(new_message->forward_info.get()) &&
                 !is_forward_info_sender_hidden(old_message->forward_info.get());
        }();
        if (need_warning) {
          LOG(ERROR) << message_id << " in " << dialog_id << " has changed forward info from "
                     << *old_message->forward_info << " to " << *new_message->forward_info << ", really forwarded from "
                     << old_message->real_forward_from_message_id << " in " << old_message->real_forward_from_dialog_id
                     << ", message content type is " << old_content_type << '/' << new_content_type;
        }
        old_message->forward_info = std::move(new_message->forward_info);
        need_send_update = true;
      }
    } else if (is_new_available) {
      LOG(ERROR) << message_id << " in " << dialog_id << " sent by " << old_message->sender_user_id << "/"
                 << old_message->sender_dialog_id << " has lost forward info " << *old_message->forward_info
                 << ", really forwarded from " << old_message->real_forward_from_message_id << " in "
                 << old_message->real_forward_from_dialog_id << ", message content type is " << old_content_type << '/'
                 << new_content_type;
      old_message->forward_info = nullptr;
      need_send_update = true;
    }
  }
  if (old_message->had_forward_info != new_message->had_forward_info) {
    old_message->had_forward_info = new_message->had_forward_info;
    need_send_update = true;
  }
  if (old_message->notification_id != new_message->notification_id) {
    CHECK(!is_scheduled);
    if (old_message->notification_id.is_valid()) {
      if (new_message->notification_id.is_valid()) {
        LOG(ERROR) << "Notification identifier for " << message_id << " in " << dialog_id
                   << " has tried to change from " << old_message->notification_id << " to "
                   << new_message->notification_id << ", message content type is " << old_content_type << '/'
                   << new_content_type;
      }
    } else {
      CHECK(new_message->notification_id.is_valid());
      add_notification_id_to_message_id_correspondence(d, new_message->notification_id, message_id);
      old_message->notification_id = new_message->notification_id;
    }
  }
  if (new_message->is_mention_notification_disabled) {
    old_message->is_mention_notification_disabled = true;
  }
  if (!new_message->from_database) {
    old_message->from_database = false;
  }

  if (old_message->ttl_period != new_message->ttl_period) {
    if (old_message->ttl_period != 0 || !message_id.is_yet_unsent()) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed TTL period from " << old_message->ttl_period
                 << " to " << new_message->ttl_period;
    } else {
      LOG(DEBUG) << "Change message TTL period";
      old_message->ttl_period = new_message->ttl_period;
      need_send_update = true;
    }
  }

  if (old_message->reply_to_message_id != new_message->reply_to_message_id) {
    // Can't check "&& get_message_force(d, old_message->reply_to_message_id, "update_message") == nullptr", because it
    // can change message tree and invalidate reference to old_message
    if (new_message->reply_to_message_id == MessageId() || replace_legacy) {
      LOG(DEBUG) << "Drop message reply_to_message_id";
      unregister_message_reply(d, old_message);
      old_message->reply_to_message_id = MessageId();
      update_message_max_reply_media_timestamp(d, old_message, true);
      need_send_update = true;
    } else if (is_new_available) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed message it is reply to from "
                 << old_message->reply_to_message_id << " to " << new_message->reply_to_message_id
                 << ", message content type is " << old_content_type << '/' << new_content_type;
    }
  }
  if (old_message->reply_in_dialog_id != new_message->reply_in_dialog_id) {
    if (new_message->reply_in_dialog_id == DialogId() || replace_legacy) {
      LOG(DEBUG) << "Drop message reply_in_dialog_id";
      old_message->reply_in_dialog_id = DialogId();
      need_send_update = true;
    } else if (is_new_available && old_message->reply_in_dialog_id.is_valid()) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed dialog it is reply in from "
                 << old_message->reply_in_dialog_id << " to " << new_message->reply_in_dialog_id
                 << ", message content type is " << old_content_type << '/' << new_content_type;
    }
  }
  if (old_message->top_thread_message_id != new_message->top_thread_message_id) {
    if (new_message->top_thread_message_id == MessageId() || old_message->top_thread_message_id == MessageId()) {
      LOG(DEBUG) << "Change message thread from " << old_message->top_thread_message_id << " to "
                 << new_message->top_thread_message_id;
      old_message->top_thread_message_id = new_message->top_thread_message_id;
      need_send_update = true;
    } else if (is_new_available) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed message thread from "
                 << old_message->top_thread_message_id << " to " << new_message->top_thread_message_id
                 << ", message content type is " << old_content_type << '/' << new_content_type;
    }
  }
  if (old_message->via_bot_user_id != new_message->via_bot_user_id) {
    if ((!message_id.is_yet_unsent() || old_message->via_bot_user_id.is_valid()) && is_new_available &&
        !replace_legacy) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed bot via it is sent from "
                 << old_message->via_bot_user_id << " to " << new_message->via_bot_user_id
                 << ", message content type is " << old_content_type << '/' << new_content_type;
    }
    LOG(DEBUG) << "Change message via_bot from " << old_message->via_bot_user_id << " to "
               << new_message->via_bot_user_id;
    old_message->via_bot_user_id = new_message->via_bot_user_id;
    need_send_update = true;

    if (old_message->hide_via_bot && old_message->via_bot_user_id.is_valid()) {
      // wrongly set hide_via_bot
      old_message->hide_via_bot = false;
    }
  }
  if (old_message->is_outgoing != new_message->is_outgoing && is_new_available) {
    if (!replace_legacy && !(message_id.is_scheduled() && dialog_id == get_my_dialog_id())) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed is_outgoing from " << old_message->is_outgoing
                 << " to " << new_message->is_outgoing << ", message content type is " << old_content_type << '/'
                 << new_content_type;
    }
    old_message->is_outgoing = new_message->is_outgoing;
    need_send_update = true;
  }
  LOG_IF(ERROR, old_message->is_channel_post != new_message->is_channel_post)
      << message_id << " in " << dialog_id << " has changed is_channel_post from " << old_message->is_channel_post
      << " to " << new_message->is_channel_post << ", message content type is " << old_content_type << '/'
      << new_content_type;
  if (old_message->contains_mention != new_message->contains_mention) {
    if (old_message->edit_date == 0 && is_new_available && old_content_type != MessageContentType::PinMessage &&
        old_content_type != MessageContentType::ExpiredPhoto && old_content_type != MessageContentType::ExpiredVideo &&
        !replace_legacy) {
      LOG(ERROR) << message_id << " in " << dialog_id << " has changed contains_mention from "
                 << old_message->contains_mention << " to " << new_message->contains_mention
                 << ", is_outgoing = " << old_message->is_outgoing << ", message content type is " << old_content_type
                 << '/' << new_content_type;
    }
    // contains_mention flag shouldn't be changed, because the message will not be added to unread mention list
    // and we are unable to show/hide message notification
    // old_message->contains_mention = new_message->contains_mention;
    // need_send_update = true;
  }
  if (old_message->disable_notification != new_message->disable_notification) {
    LOG_IF(ERROR, old_message->edit_date == 0 && is_new_available && !replace_legacy)
        << "Disable_notification has changed from " << old_message->disable_notification << " to "
        << new_message->disable_notification
        << ". Old message: " << to_string(get_message_object(dialog_id, old_message, "update_message"))
        << ". New message: " << to_string(get_message_object(dialog_id, new_message.get(), "update_message"));
    // disable_notification flag shouldn't be changed, because we are unable to show/hide message notification
    // old_message->disable_notification = new_message->disable_notification;
    // need_send_update = true;
  }
  if (old_message->disable_web_page_preview != new_message->disable_web_page_preview) {
    old_message->disable_web_page_preview = new_message->disable_web_page_preview;
  }

  if (!is_scheduled &&
      update_message_contains_unread_mention(d, old_message, new_message->contains_unread_mention, "update_message")) {
    need_send_update = true;
  }
  if (update_message_interaction_info(dialog_id, old_message, new_message->view_count, new_message->forward_count, true,
                                      std::move(new_message->reply_info), "update_message")) {
    need_send_update = true;
  }
  if (old_message->noforwards != new_message->noforwards) {
    old_message->noforwards = new_message->noforwards;
    need_send_update = true;
  }
  if (old_message->restriction_reasons != new_message->restriction_reasons) {
    old_message->restriction_reasons = std::move(new_message->restriction_reasons);
    need_send_update = true;
  }
  if (old_message->legacy_layer != new_message->legacy_layer) {
    old_message->legacy_layer = new_message->legacy_layer;
  }
  if ((old_message->media_album_id == 0 || td_->auth_manager_->is_bot()) && new_message->media_album_id != 0) {
    old_message->media_album_id = new_message->media_album_id;
    LOG(DEBUG) << "Update message media_album_id";
    need_send_update = true;
  }
  if (old_message->hide_edit_date != new_message->hide_edit_date) {
    old_message->hide_edit_date = new_message->hide_edit_date;
    if (old_message->edit_date > 0) {
      need_send_update = true;
    }
  }
  int32 new_shown_edit_date = old_message->hide_edit_date ? 0 : old_message->edit_date;
  if (new_shown_edit_date != old_shown_edit_date) {
    is_edited = true;
  }

  if (old_message->is_from_scheduled != new_message->is_from_scheduled) {
    // is_from_scheduled flag shouldn't be changed, because we are unable to show/hide message notification
    // old_message->is_from_scheduled = new_message->is_from_scheduled;
  }

  if (!is_scheduled && update_message_is_pinned(d, old_message, new_message->is_pinned, "update_message")) {
    need_send_update = true;
  }

  if (old_message->edit_date > 0) {
    // inline keyboard can be edited
    bool reply_markup_changed =
        ((old_message->reply_markup == nullptr) != (new_message->reply_markup == nullptr)) ||
        (old_message->reply_markup != nullptr && *old_message->reply_markup != *new_message->reply_markup);
    if (reply_markup_changed) {
      if (d->reply_markup_message_id == message_id && !td_->auth_manager_->is_bot() &&
          new_message->reply_markup == nullptr) {
        set_dialog_reply_markup(d, MessageId());
      }
      LOG(DEBUG) << "Update message reply keyboard";
      old_message->reply_markup = std::move(new_message->reply_markup);
      is_edited = true;
      need_send_update = true;
    }
    old_message->had_reply_markup = false;
  } else {
    if (old_message->reply_markup == nullptr) {
      if (new_message->reply_markup != nullptr) {
        // MessageGame and MessageInvoice reply markup can be generated server side
        // some forwards retain their reply markup
        if (old_content_type != MessageContentType::Game && old_content_type != MessageContentType::Invoice &&
            need_message_changed_warning(old_message) && !replace_legacy) {
          LOG(ERROR) << message_id << " in " << dialog_id << " has received reply markup " << *new_message->reply_markup
                     << ", message content type is " << old_content_type << '/' << new_content_type;
        } else {
          LOG(DEBUG) << "Add message reply keyboard";
        }

        old_message->had_reply_markup = false;
        old_message->reply_markup = std::move(new_message->reply_markup);
        need_send_update = true;
      }
    } else {
      if (new_message->reply_markup != nullptr) {
        if (replace_legacy ||
            (message_id.is_yet_unsent() && old_message->reply_markup->type == ReplyMarkup::Type::InlineKeyboard &&
             new_message->reply_markup->type == ReplyMarkup::Type::InlineKeyboard)) {
          // allow the server to update inline keyboard for sent messages
          // this is needed to get correct button_id for UrlAuth buttons
          old_message->had_reply_markup = false;
          old_message->reply_markup = std::move(new_message->reply_markup);
          need_send_update = true;
        } else if (need_message_changed_warning(old_message)) {
          LOG_IF(WARNING, *old_message->reply_markup != *new_message->reply_markup)
              << message_id << " in " << dialog_id << " has changed reply_markup from " << *old_message->reply_markup
              << " to " << *new_message->reply_markup;
        }
      } else {
        // if the message is not accessible anymore, then we don't need a warning
        if (need_message_changed_warning(old_message) && is_new_available) {
          LOG(ERROR) << message_id << " in " << dialog_id << " sent by " << old_message->sender_user_id << "/"
                     << old_message->sender_dialog_id << " has lost reply markup " << *old_message->reply_markup
                     << ". Old message: " << to_string(get_message_object(dialog_id, old_message, "update_message"))
                     << ". New message: "
                     << to_string(get_message_object(dialog_id, new_message.get(), "update_message"));
        }
      }
    }
  }

  if (old_message->last_access_date < new_message->last_access_date) {
    old_message->last_access_date = new_message->last_access_date;
  }
  if (new_message->is_update_sent) {
    old_message->is_update_sent = true;
  }

  if (!is_scheduled) {
    CHECK(!new_message->have_previous || !new_message->have_next);
    if (new_message->have_previous && !old_message->have_previous) {
      old_message->have_previous = true;
      attach_message_to_previous(d, message_id, "update_message");
    } else if (new_message->have_next && !old_message->have_next) {
      old_message->have_next = true;
      attach_message_to_next(d, message_id, "update_message");
    }
  }

  if (update_message_content(dialog_id, old_message, std::move(new_message->content), true,
                             message_id.is_yet_unsent() && new_message->edit_date == 0, is_message_in_dialog)) {
    need_send_update = true;
  }

  if (was_visible_message_reply_info && !is_visible_message_reply_info(dialog_id, old_message)) {
    send_update_message_interaction_info(dialog_id, old_message);
  }

  if (is_edited && !td_->auth_manager_->is_bot()) {
    d->last_edited_message_id = message_id;
    send_update_message_edited(dialog_id, old_message);
  }

  if (!was_visible_message_reply_info && is_visible_message_reply_info(dialog_id, old_message)) {
    send_update_message_interaction_info(dialog_id, old_message);
  }

  on_message_changed(d, old_message, need_send_update, "update_message");
  return need_send_update;
}

void MessagesManager::set_dialog_folder_id(Dialog *d, FolderId folder_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);

  if (d->folder_id == folder_id) {
    if (!d->is_folder_id_inited) {
      LOG(INFO) << "Folder of " << d->dialog_id << " is still " << folder_id;
      do_set_dialog_folder_id(d, folder_id);
    }
    return;
  }

  LOG(INFO) << "Change " << d->dialog_id << " folder from " << d->folder_id << " to " << folder_id;

  auto dialog_positions = get_dialog_positions(d);

  if (get_dialog_pinned_order(DialogListId(d->folder_id), d->dialog_id) != DEFAULT_ORDER) {
    set_dialog_is_pinned(DialogListId(d->folder_id), d, false, false);
  }

  DialogDate dialog_date(d->order, d->dialog_id);
  if (get_dialog_folder(d->folder_id)->ordered_dialogs_.erase(dialog_date) == 0) {
    LOG_IF(ERROR, d->order != DEFAULT_ORDER) << d->dialog_id << " not found in the chat list";
  }

  do_set_dialog_folder_id(d, folder_id);

  get_dialog_folder(d->folder_id)->ordered_dialogs_.insert(dialog_date);

  update_dialog_lists(d, std::move(dialog_positions), true, false, "set_dialog_folder_id");
}

void MessagesManager::on_dialog_username_updated(DialogId dialog_id, const string &old_username,
                                                 const string &new_username) {
  auto d = get_dialog(dialog_id);
  if (d != nullptr) {
    update_dialogs_hints(d);
  }
  if (old_username != new_username) {
    message_embedding_codes_[0].erase(dialog_id);
    message_embedding_codes_[1].erase(dialog_id);
  }
  if (!old_username.empty() && old_username != new_username) {
    resolved_usernames_.erase(clean_username(old_username));
    inaccessible_resolved_usernames_.erase(clean_username(old_username));
  }
  if (!new_username.empty()) {
    auto cache_time = is_update_about_username_change_received(dialog_id) ? USERNAME_CACHE_EXPIRE_TIME
                                                                          : USERNAME_CACHE_EXPIRE_TIME_SHORT;
    resolved_usernames_[clean_username(new_username)] = ResolvedUsername{dialog_id, Time::now() + cache_time};
  }
}

void MessagesManager::open_secret_message(SecretChatId secret_chat_id, int64 random_id, Promise<> promise) {
  promise.set_value(Unit());  // TODO: set after event is saved
  DialogId dialog_id(secret_chat_id);
  Dialog *d = get_dialog_force(dialog_id, "open_secret_message");
  if (d == nullptr) {
    LOG(ERROR) << "Ignore opening secret chat message in unknown " << dialog_id;
    return;
  }

  auto message_id = get_message_id_by_random_id(d, random_id, "open_secret_message");
  if (!message_id.is_valid()) {
    return;
  }
  Message *m = get_message(d, message_id);
  CHECK(m != nullptr);
  if (m->message_id.is_yet_unsent() || m->is_failed_to_send || !m->is_outgoing) {
    LOG(ERROR) << "Peer has opened wrong " << message_id << " in " << dialog_id;
    return;
  }

  read_message_content(d, m, false, "open_secret_message");
}

bool MessagesManager::is_message_notification_active(const Dialog *d, const Message *m) {
  CHECK(!m->message_id.is_scheduled());
  if (is_from_mention_notification_group(d, m)) {
    return m->notification_id.get() > d->mention_notification_group.max_removed_notification_id.get() &&
           m->message_id > d->mention_notification_group.max_removed_message_id &&
           (m->contains_unread_mention || m->message_id == d->pinned_message_notification_message_id);
  } else {
    return m->notification_id.get() > d->message_notification_group.max_removed_notification_id.get() &&
           m->message_id > d->message_notification_group.max_removed_message_id &&
           m->message_id > d->last_read_inbox_message_id;
  }
}

void MessagesManager::on_upload_imported_message_attachment_error(FileId file_id, Status status) {
  if (G()->close_flag()) {
    // do not fail upload if closing
    return;
  }

  LOG(INFO) << "File " << file_id << " has upload error " << status;
  CHECK(status.is_error());

  auto it = being_uploaded_imported_message_attachments_.find(file_id);
  if (it == being_uploaded_imported_message_attachments_.end()) {
    // just in case, as in on_upload_media_error
    return;
  }

  Promise<Unit> promise = std::move(it->second->promise);

  being_uploaded_imported_message_attachments_.erase(it);

  promise.set_error(std::move(status));
}

void MessagesManager::on_update_created_public_broadcasts(vector<ChannelId> channel_ids) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  if (created_public_broadcasts_inited_ && created_public_broadcasts_ == channel_ids) {
    return;
  }

  LOG(INFO) << "Update create public channels to " << channel_ids;
  for (auto channel_id : channel_ids) {
    force_create_dialog(DialogId(channel_id), "on_update_created_public_broadcasts");
  }

  created_public_broadcasts_inited_ = true;
  created_public_broadcasts_ = std::move(channel_ids);
}

FullMessageId MessagesManager::on_get_message(tl_object_ptr<telegram_api::Message> message_ptr, bool from_update,
                                              bool is_channel_message, bool is_scheduled, bool have_previous,
                                              bool have_next, const char *source) {
  return on_get_message(parse_telegram_api_message(std::move(message_ptr), is_scheduled, source), from_update,
                        is_channel_message, have_previous, have_next, source);
}

void MessagesManager::update_dialogs_hints_rating(const Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (d->order == DEFAULT_ORDER) {
    LOG(INFO) << "Remove " << d->dialog_id << " from chats search";
    dialogs_hints_.remove(-d->dialog_id.get());
  } else {
    LOG(INFO) << "Change position of " << d->dialog_id << " in chats search";
    dialogs_hints_.set_rating(-d->dialog_id.get(), -get_dialog_base_order(d));
  }
}

void MessagesManager::send_update_message_live_location_viewed(FullMessageId full_message_id) {
  CHECK(get_message(full_message_id) != nullptr);
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateMessageLiveLocationViewed>(full_message_id.get_dialog_id().get(),
                                                                            full_message_id.get_message_id().get()));
}

void MessagesManager::on_dialog_updated(DialogId dialog_id, const char *source) {
  if (G()->parameters().use_message_db) {
    LOG(INFO) << "Update " << dialog_id << " from " << source;
    pending_updated_dialog_timeout_.add_timeout_in(dialog_id.get(), MAX_SAVE_DIALOG_DELAY);
  }
}

bool MessagesManager::get_message_disable_web_page_preview(const Message *m) {
  if (m->content->get_type() != MessageContentType::Text) {
    return false;
  }
  if (has_message_content_web_page(m->content.get())) {
    return false;
  }
  return m->disable_web_page_preview;
}

vector<FileId> MessagesManager::get_message_file_ids(const Message *m) const {
  CHECK(m != nullptr);
  return get_message_content_file_ids(m->content.get(), td_);
}
}  // namespace td
