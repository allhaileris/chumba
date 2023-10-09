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






#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

Result<td_api::object_ptr<td_api::messages>> MessagesManager::forward_messages(
    DialogId to_dialog_id, DialogId from_dialog_id, vector<MessageId> message_ids,
    tl_object_ptr<td_api::messageSendOptions> &&options, bool in_game_share, vector<MessageCopyOptions> &&copy_options,
    bool only_preview) {
  TRY_RESULT(forwarded_messages_info,
             get_forwarded_messages(to_dialog_id, from_dialog_id, message_ids, std::move(options), in_game_share,
                                    std::move(copy_options)));
  auto from_dialog = forwarded_messages_info.from_dialog;
  auto to_dialog = forwarded_messages_info.to_dialog;
  auto message_send_options = forwarded_messages_info.message_send_options;
  auto &copied_messages = forwarded_messages_info.copied_messages;
  auto &forwarded_message_contents = forwarded_messages_info.forwarded_message_contents;

  vector<td_api::object_ptr<td_api::message>> result(message_ids.size());
  vector<Message *> forwarded_messages;
  vector<MessageId> forwarded_message_ids;
  bool need_update_dialog_pos = false;
  for (size_t j = 0; j < forwarded_message_contents.size(); j++) {
    MessageId message_id = get_persistent_message_id(from_dialog, message_ids[forwarded_message_contents[j].index]);
    const Message *forwarded_message = get_message(from_dialog, message_id);
    CHECK(forwarded_message != nullptr);

    auto content = std::move(forwarded_message_contents[j].content);
    auto forward_info = create_message_forward_info(from_dialog_id, to_dialog_id, forwarded_message);
    if (forward_info != nullptr && !forward_info->is_imported && !is_forward_info_sender_hidden(forward_info.get()) &&
        !forward_info->message_id.is_valid() && !forward_info->sender_dialog_id.is_valid() &&
        forward_info->sender_user_id.is_valid()) {
      auto private_forward_name = td_->contacts_manager_->get_user_private_forward_name(forward_info->sender_user_id);
      if (!private_forward_name.empty()) {
        forward_info->sender_user_id = UserId();
        forward_info->sender_name = std::move(private_forward_name);
      }
    }

    unique_ptr<Message> message;
    Message *m;
    if (only_preview) {
      message = create_message_to_send(to_dialog, MessageId(), MessageId(), message_send_options, std::move(content),
                                       j + 1 != forwarded_message_contents.size(), std::move(forward_info), false,
                                       DialogId());
      MessageId new_message_id =
          message_send_options.schedule_date != 0
              ? get_next_yet_unsent_scheduled_message_id(to_dialog, message_send_options.schedule_date)
              : get_next_yet_unsent_message_id(to_dialog);
      set_message_id(message, new_message_id);
      m = message.get();
    } else {
      m = get_message_to_send(to_dialog, MessageId(), MessageId(), message_send_options, std::move(content),
                              &need_update_dialog_pos, j + 1 != forwarded_message_contents.size(),
                              std::move(forward_info));
    }
    fix_forwarded_message(m, to_dialog_id, forwarded_message, forwarded_message_contents[j].media_album_id);
    m->in_game_share = in_game_share;
    m->real_forward_from_dialog_id = from_dialog_id;
    m->real_forward_from_message_id = message_id;

    if (!only_preview) {
      send_update_new_message(to_dialog, m);
      forwarded_messages.push_back(m);
      forwarded_message_ids.push_back(message_id);
    }

    result[forwarded_message_contents[j].index] = get_message_object(to_dialog_id, m, "forward_messages");
  }

  if (!forwarded_messages.empty()) {
    CHECK(!only_preview);
    do_forward_messages(to_dialog_id, from_dialog_id, forwarded_messages, forwarded_message_ids, 0);
  }

  for (auto &copied_message : copied_messages) {
    unique_ptr<Message> message;
    Message *m;
    if (only_preview) {
      message = create_message_to_send(to_dialog, copied_message.top_thread_message_id,
                                       copied_message.reply_to_message_id, message_send_options,
                                       std::move(copied_message.content), false, nullptr, true, DialogId());
      MessageId new_message_id =
          message_send_options.schedule_date != 0
              ? get_next_yet_unsent_scheduled_message_id(to_dialog, message_send_options.schedule_date)
              : get_next_yet_unsent_message_id(to_dialog);
      set_message_id(message, new_message_id);
      m = message.get();
    } else {
      m = get_message_to_send(to_dialog, copied_message.top_thread_message_id, copied_message.reply_to_message_id,
                              message_send_options, std::move(copied_message.content), &need_update_dialog_pos, false,
                              nullptr, true);
    }
    m->disable_web_page_preview = copied_message.disable_web_page_preview;
    m->media_album_id = copied_message.media_album_id;
    m->reply_markup = std::move(copied_message.reply_markup);

    if (!only_preview) {
      save_send_message_log_event(to_dialog_id, m);
      do_send_message(to_dialog_id, m);
      send_update_new_message(to_dialog, m);
    }

    result[copied_message.index] = get_message_object(to_dialog_id, m, "forward_messages");
  }

  if (need_update_dialog_pos) {
    CHECK(!only_preview);
    send_update_chat_last_message(to_dialog, "forward_messages");
  }

  return get_messages_object(-1, std::move(result), false);
}

void MessagesManager::update_dialog_lists(
    Dialog *d, std::unordered_map<DialogListId, DialogPositionInList, DialogListIdHash> &&old_positions,
    bool need_send_update, bool is_loaded_from_database, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  auto dialog_id = d->dialog_id;
  if (being_added_dialog_id_ == dialog_id) {
    // do not try to update dialog lists, while the dialog isn't inited
    return;
  }

  LOG(INFO) << "Update lists of " << dialog_id << " from " << source;

  if (d->order == DEFAULT_ORDER) {
    for (auto &old_position : old_positions) {
      if (old_position.second.is_pinned) {
        set_dialog_is_pinned(old_position.first, d, false, false);
      }
    }

    if (d->folder_id != FolderId::main()) {
      LOG(INFO) << "Change folder of " << dialog_id << " to " << FolderId::main();
      DialogDate dialog_date(d->order, dialog_id);
      get_dialog_folder(d->folder_id)->ordered_dialogs_.erase(dialog_date);
      do_set_dialog_folder_id(d, FolderId::main());
      get_dialog_folder(d->folder_id)->ordered_dialogs_.insert(dialog_date);
    }
  }

  for (auto &dialog_list : dialog_lists_) {
    auto dialog_list_id = dialog_list.first;
    auto &list = dialog_list.second;

    const DialogPositionInList &old_position = old_positions[dialog_list_id];
    const DialogPositionInList new_position = get_dialog_position_in_list(&list, d, true);

    // sponsored chat is never "in list"
    bool was_in_list = old_position.order != DEFAULT_ORDER && old_position.private_order != 0;
    bool is_in_list = new_position.order != DEFAULT_ORDER && new_position.private_order != 0;
    CHECK(was_in_list == is_dialog_in_list(d, dialog_list_id));

    LOG(DEBUG) << "Update position of " << dialog_id << " in " << dialog_list_id << " from " << old_position << " to "
               << new_position;

    bool need_update_unread_chat_count = false;
    if (was_in_list != is_in_list) {
      const int32 delta = was_in_list ? -1 : 1;
      list.in_memory_dialog_total_count_ += delta;
      if (!is_loaded_from_database) {
        int32 &total_count = dialog_id.get_type() == DialogType::SecretChat ? list.secret_chat_total_count_
                                                                            : list.server_dialog_total_count_;
        if (total_count != -1) {
          total_count += delta;
          if (total_count < 0) {
            LOG(ERROR) << "Total chat count became negative after leaving " << dialog_id;
            total_count = 0;
          }
        }
      }

      if (!is_loaded_from_database) {
        need_update_unread_chat_count =
            list.is_dialog_unread_count_inited_ && old_position.total_dialog_count != get_dialog_total_count(list);
        auto unread_count = d->server_unread_count + d->local_unread_count;
        const char *change_source = was_in_list ? "on_dialog_leave" : "on_dialog_join";
        if (unread_count != 0 && list.is_message_unread_count_inited_) {
          unread_count *= delta;

          list.unread_message_total_count_ += unread_count;
          if (is_dialog_muted(d)) {
            list.unread_message_muted_count_ += unread_count;
          }
          send_update_unread_message_count(list, dialog_id, true, change_source);
        }
        if ((unread_count != 0 || d->is_marked_as_unread) && list.is_dialog_unread_count_inited_) {
          list.unread_dialog_total_count_ += delta;
          if (unread_count == 0 && d->is_marked_as_unread) {
            list.unread_dialog_marked_count_ += delta;
          }
          if (is_dialog_muted(d)) {
            list.unread_dialog_muted_count_ += delta;
            if (unread_count == 0 && d->is_marked_as_unread) {
              list.unread_dialog_muted_marked_count_ += delta;
            }
          }
          need_update_unread_chat_count = true;
        }
        if (need_update_unread_chat_count) {
          send_update_unread_chat_count(list, dialog_id, true, change_source);
        }
      }

      if (was_in_list) {
        remove_dialog_from_list(d, dialog_list_id);
      } else {
        add_dialog_to_list(d, dialog_list_id);
      }
    }
    if (!need_update_unread_chat_count && list.is_dialog_unread_count_inited_ &&
        old_position.total_dialog_count != get_dialog_total_count(list)) {
      send_update_unread_chat_count(list, dialog_id, true, "changed total count");
    }

    if (need_send_update && need_send_update_chat_position(old_position, new_position)) {
      send_update_chat_position(dialog_list_id, d, source);
    }

    if (!is_loaded_from_database && !old_position.is_sponsored && new_position.is_sponsored) {
      // a chat is sponsored only if user isn't a chat member
      remove_all_dialog_notifications(d, false, "update_dialog_lists 3");
      remove_all_dialog_notifications(d, true, "update_dialog_lists 4");
    }
  }
}

td_api::object_ptr<td_api::chat> MessagesManager::get_chat_object(const Dialog *d) const {
  CHECK(d != nullptr);

  auto chat_source = is_dialog_sponsored(d) ? sponsored_dialog_source_.get_chat_source_object() : nullptr;

  bool can_delete_for_self = false;
  bool can_delete_for_all_users = false;
  if (chat_source != nullptr) {
    switch (chat_source->get_id()) {
      case td_api::chatSourcePublicServiceAnnouncement::ID:
        // can delete for self (but only while removing from dialog list)
        can_delete_for_self = true;
        break;
      default:
        // can't delete
        break;
    }
  } else if (!td_->auth_manager_->is_bot() && have_input_peer(d->dialog_id, AccessRights::Read)) {
    switch (d->dialog_id.get_type()) {
      case DialogType::User:
        can_delete_for_self = true;
        can_delete_for_all_users = G()->shared_config().get_option_boolean("revoke_pm_inbox", true);
        if (d->dialog_id == get_my_dialog_id() || td_->contacts_manager_->is_user_deleted(d->dialog_id.get_user_id()) ||
            td_->contacts_manager_->is_user_bot(d->dialog_id.get_user_id())) {
          can_delete_for_all_users = false;
        }
        break;
      case DialogType::Chat:
        // chats can be deleted only for self with deleteChatHistory
        can_delete_for_self = true;
        break;
      case DialogType::Channel:
        if (is_broadcast_channel(d->dialog_id) ||
            td_->contacts_manager_->is_channel_public(d->dialog_id.get_channel_id())) {
          // deleteChatHistory can't be used in channels and public supergroups
        } else {
          // private supergroups can be deleted for self
          can_delete_for_self = true;
        }
        break;
      case DialogType::SecretChat:
        if (td_->contacts_manager_->get_secret_chat_state(d->dialog_id.get_secret_chat_id()) ==
            SecretChatState::Closed) {
          // in a closed secret chats there is no way to delete messages for both users
          can_delete_for_self = true;
        } else {
          // active secret chats can be deleted only for both users
          can_delete_for_all_users = true;
        }
        break;
      case DialogType::None:
      default:
        UNREACHABLE();
    }
  }

  // TODO hide/show draft message when can_send_message(dialog_id) changes
  auto draft_message = can_send_message(d->dialog_id).is_ok() ? get_draft_message_object(d->draft_message) : nullptr;

  return make_tl_object<td_api::chat>(
      d->dialog_id.get(), get_chat_type_object(d->dialog_id), get_dialog_title(d->dialog_id),
      get_chat_photo_info_object(td_->file_manager_.get(), get_dialog_photo(d->dialog_id)),
      get_dialog_default_permissions(d->dialog_id).get_chat_permissions_object(),
      get_message_object(d->dialog_id, get_message(d, d->last_message_id), "get_chat_object"),
      get_chat_positions_object(d), get_default_message_sender_object(d),
      get_dialog_has_protected_content(d->dialog_id), d->is_marked_as_unread, d->is_blocked,
      get_dialog_has_scheduled_messages(d), can_delete_for_self, can_delete_for_all_users,
      can_report_dialog(d->dialog_id), d->notification_settings.silent_send_message,
      d->server_unread_count + d->local_unread_count, d->last_read_inbox_message_id.get(),
      d->last_read_outbox_message_id.get(), d->unread_mention_count,
      get_chat_notification_settings_object(&d->notification_settings), d->message_ttl.get_message_ttl_object(),
      get_dialog_theme_name(d), get_chat_action_bar_object(d), get_video_chat_object(d),
      get_chat_join_requests_info_object(d), d->reply_markup_message_id.get(), std::move(draft_message),
      d->client_data);
}

Result<unique_ptr<DialogFilter>> MessagesManager::create_dialog_filter(DialogFilterId dialog_filter_id,
                                                                       td_api::object_ptr<td_api::chatFilter> filter) {
  CHECK(filter != nullptr);
  for (auto chat_ids : {&filter->pinned_chat_ids_, &filter->excluded_chat_ids_, &filter->included_chat_ids_}) {
    for (const auto &chat_id : *chat_ids) {
      DialogId dialog_id(chat_id);
      if (!dialog_id.is_valid()) {
        return Status::Error(400, "Invalid chat identifier specified");
      }
      const Dialog *d = get_dialog_force(dialog_id, "create_dialog_filter");
      if (d == nullptr) {
        return Status::Error(400, "Chat not found");
      }
      if (!have_input_peer(dialog_id, AccessRights::Read)) {
        return Status::Error(400, "Can't access the chat");
      }
      if (d->order == DEFAULT_ORDER) {
        return Status::Error(400, "Chat is not in the chat list");
      }
    }
  }

  auto dialog_filter = make_unique<DialogFilter>();
  dialog_filter->dialog_filter_id = dialog_filter_id;

  std::unordered_set<int64> added_dialog_ids;
  auto add_chats = [this, &added_dialog_ids](vector<InputDialogId> &input_dialog_ids, const vector<int64> &chat_ids) {
    for (const auto &chat_id : chat_ids) {
      if (!added_dialog_ids.insert(chat_id).second) {
        // do not allow duplicate chat_ids
        continue;
      }

      input_dialog_ids.push_back(get_input_dialog_id(DialogId(chat_id)));
    }
  };
  add_chats(dialog_filter->pinned_dialog_ids, filter->pinned_chat_ids_);
  add_chats(dialog_filter->included_dialog_ids, filter->included_chat_ids_);
  add_chats(dialog_filter->excluded_dialog_ids, filter->excluded_chat_ids_);

  dialog_filter->title = clean_name(std::move(filter->title_), MAX_DIALOG_FILTER_TITLE_LENGTH);
  if (dialog_filter->title.empty()) {
    return Status::Error(400, "Title must be non-empty");
  }
  dialog_filter->emoji = DialogFilter::get_emoji_by_icon_name(filter->icon_name_);
  if (dialog_filter->emoji.empty() && !filter->icon_name_.empty()) {
    return Status::Error(400, "Invalid icon name specified");
  }
  dialog_filter->exclude_muted = filter->exclude_muted_;
  dialog_filter->exclude_read = filter->exclude_read_;
  dialog_filter->exclude_archived = filter->exclude_archived_;
  dialog_filter->include_contacts = filter->include_contacts_;
  dialog_filter->include_non_contacts = filter->include_non_contacts_;
  dialog_filter->include_bots = filter->include_bots_;
  dialog_filter->include_groups = filter->include_groups_;
  dialog_filter->include_channels = filter->include_channels_;

  TRY_STATUS(dialog_filter->check_limits());
  sort_dialog_filter_input_dialog_ids(dialog_filter.get(), "create_dialog_filter");

  return std::move(dialog_filter);
}

MessagesManager::Message *MessagesManager::continue_send_message(DialogId dialog_id, unique_ptr<Message> &&m,
                                                                 uint64 log_event_id) {
  CHECK(log_event_id != 0);
  CHECK(m != nullptr);
  CHECK(m->content != nullptr);

  Dialog *d = get_dialog_force(dialog_id, "continue_send_message");
  if (d == nullptr) {
    LOG(ERROR) << "Can't find " << dialog_id << " to continue send a message";
    binlog_erase(G()->td_db()->get_binlog(), log_event_id);
    return nullptr;
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    binlog_erase(G()->td_db()->get_binlog(), log_event_id);
    return nullptr;
  }

  LOG(INFO) << "Continue to send " << m->message_id << " to " << dialog_id << " initially sent at " << m->send_date
            << " from binlog";

  d->was_opened = true;

  auto now = G()->unix_time();
  if (m->message_id.is_scheduled()) {
    set_message_id(m, get_next_yet_unsent_scheduled_message_id(d, m->date));
  } else {
    set_message_id(m, get_next_yet_unsent_message_id(d));
    m->date = now;
  }
  m->have_previous = true;
  m->have_next = true;

  bool need_update = false;
  bool need_update_dialog_pos = false;
  auto result_message =
      add_message_to_dialog(d, std::move(m), true, &need_update, &need_update_dialog_pos, "continue_send_message");
  CHECK(result_message != nullptr);

  if (result_message->message_id.is_scheduled()) {
    send_update_chat_has_scheduled_messages(d, false);
  }

  send_update_new_message(d, result_message);
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "continue_send_message");
  }

  auto can_send_status = can_send_message(dialog_id);
  if (can_send_status.is_ok() && result_message->send_date < now - MAX_RESEND_DELAY &&
      dialog_id != get_my_dialog_id()) {
    can_send_status = Status::Error(400, "Message is too old to be re-sent automatically");
  }
  if (can_send_status.is_error()) {
    LOG(INFO) << "Can't continue to send a message to " << dialog_id << ": " << can_send_status.error();

    fail_send_message({dialog_id, result_message->message_id}, can_send_status.move_as_error());
    return nullptr;
  }

  return result_message;
}

void MessagesManager::on_load_secret_thumbnail(FileId thumbnail_file_id, BufferSlice thumbnail) {
  if (G()->close_flag()) {
    // do not send secret media if closing, thumbnail may be wrong
    return;
  }

  LOG(INFO) << "SecretThumbnail " << thumbnail_file_id << " has been loaded with size " << thumbnail.size();

  auto it = being_loaded_secret_thumbnails_.find(thumbnail_file_id);
  if (it == being_loaded_secret_thumbnails_.end()) {
    // just in case, as in on_upload_thumbnail
    return;
  }

  auto full_message_id = it->second.full_message_id;
  auto file_id = it->second.file_id;
  auto input_file = std::move(it->second.input_file);

  being_loaded_secret_thumbnails_.erase(it);

  Message *m = get_message(full_message_id);
  if (m == nullptr) {
    // message has already been deleted by the user, do not need to send it
    // cancel file upload of the main file to allow next upload with the same file to succeed
    LOG(INFO) << "Message with a media has already been deleted";
    cancel_upload_file(file_id);
    return;
  }
  CHECK(m->message_id.is_yet_unsent());

  if (thumbnail.empty()) {
    delete_message_content_thumbnail(m->content.get(), td_);
  }

  auto dialog_id = full_message_id.get_dialog_id();
  auto can_send_status = can_send_message(dialog_id);
  if (can_send_status.is_error()) {
    // secret chat was closed during load of the file
    LOG(INFO) << "Can't send a message to " << dialog_id << ": " << can_send_status.error();

    fail_send_message(full_message_id, can_send_status.move_as_error());
    return;
  }

  do_send_secret_media(dialog_id, m, file_id, thumbnail_file_id, std::move(input_file), std::move(thumbnail));
}

void MessagesManager::on_get_message_link_dialog(MessageLinkInfo &&info, Promise<MessageLinkInfo> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  DialogId dialog_id;
  if (info.username.empty()) {
    if (!td_->contacts_manager_->have_channel(info.channel_id)) {
      return promise.set_error(Status::Error(500, "Chat info not found"));
    }

    dialog_id = DialogId(info.channel_id);
    force_create_dialog(dialog_id, "on_get_message_link_dialog");
  } else {
    dialog_id = resolve_dialog_username(info.username);
    if (dialog_id.is_valid()) {
      force_create_dialog(dialog_id, "on_get_message_link_dialog", true);
    }
  }
  Dialog *d = get_dialog_force(dialog_id, "on_get_message_link_dialog");
  if (d == nullptr) {
    return promise.set_error(Status::Error(500, "Chat not found"));
  }

  auto message_id = info.message_id;
  get_message_force_from_server(d, message_id,
                                PromiseCreator::lambda([actor_id = actor_id(this), info = std::move(info), dialog_id,
                                                        promise = std::move(promise)](Result<Unit> &&result) mutable {
                                  if (result.is_error()) {
                                    return promise.set_value(std::move(info));
                                  }
                                  send_closure(actor_id, &MessagesManager::on_get_message_link_message, std::move(info),
                                               dialog_id, std::move(promise));
                                }));
}

int32 MessagesManager::get_message_index_mask(DialogId dialog_id, const Message *m) const {
  CHECK(m != nullptr);
  if (m->message_id.is_scheduled() || m->message_id.is_yet_unsent()) {
    return 0;
  }
  if (m->is_failed_to_send) {
    return message_search_filter_index_mask(MessageSearchFilter::FailedToSend);
  }
  bool is_secret = dialog_id.get_type() == DialogType::SecretChat;
  if (!m->message_id.is_server() && !is_secret) {
    return 0;
  }

  int32 index_mask = 0;
  if (m->is_pinned) {
    index_mask |= message_search_filter_index_mask(MessageSearchFilter::Pinned);
  }
  // retain second condition just in case
  if (m->is_content_secret || (m->ttl > 0 && !is_secret)) {
    return index_mask;
  }
  index_mask |= get_message_content_index_mask(m->content.get(), td_, m->is_outgoing);
  if (m->contains_mention) {
    index_mask |= message_search_filter_index_mask(MessageSearchFilter::Mention);
    if (m->contains_unread_mention) {
      index_mask |= message_search_filter_index_mask(MessageSearchFilter::UnreadMention);
    }
  }
  LOG(INFO) << "Have index mask " << index_mask << " for " << m->message_id << " in " << dialog_id;
  return index_mask;
}

void MessagesManager::get_poll_voters(FullMessageId full_message_id, int32 option_id, int32 offset, int32 limit,
                                      Promise<std::pair<int32, vector<UserId>>> &&promise) {
  auto m = get_message_force(full_message_id, "get_poll_voters");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (!have_input_peer(full_message_id.get_dialog_id(), AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }
  if (m->content->get_type() != MessageContentType::Poll) {
    return promise.set_error(Status::Error(400, "Message is not a poll"));
  }
  if (m->message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Can't get poll results from scheduled messages"));
  }
  if (!m->message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Poll results can't be received"));
  }

  get_message_content_poll_voters(td_, m->content.get(), full_message_id, option_id, offset, limit, std::move(promise));
}

void MessagesManager::add_notification_id_to_message_id_correspondence(Dialog *d, NotificationId notification_id,
                                                                       MessageId message_id) {
  CHECK(d != nullptr);
  CHECK(notification_id.is_valid());
  CHECK(message_id.is_valid());
  auto it = d->notification_id_to_message_id.find(notification_id);
  if (it == d->notification_id_to_message_id.end()) {
    VLOG(notifications) << "Add correspondence from " << notification_id << " to " << message_id << " in "
                        << d->dialog_id;
    d->notification_id_to_message_id.emplace(notification_id, message_id);
  } else if (it->second != message_id) {
    LOG(ERROR) << "Have duplicated " << notification_id << " in " << d->dialog_id << " in " << message_id << " and "
               << it->second;
    if (it->second < message_id) {
      it->second = message_id;
    }
  }
}

void MessagesManager::repair_channel_server_unread_count(Dialog *d) {
  CHECK(d != nullptr);
  CHECK(d->dialog_id.get_type() == DialogType::Channel);

  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (d->last_read_inbox_message_id >= d->last_new_message_id) {
    // all messages are already read
    return;
  }
  if (!need_unread_counter(d->order)) {
    // there are no unread counters in left channels
    return;
  }
  if (!d->need_repair_channel_server_unread_count) {
    d->need_repair_channel_server_unread_count = true;
    on_dialog_updated(d->dialog_id, "repair_channel_server_unread_count");
  }

  LOG(INFO) << "Reload ChannelFull for " << d->dialog_id << " to repair unread message counts";
  get_dialog_info_full(d->dialog_id, Auto(), "repair_channel_server_unread_count");
}

tl_object_ptr<td_api::messages> MessagesManager::get_messages_object(int32 total_count, DialogId dialog_id,
                                                                     const vector<MessageId> &message_ids,
                                                                     bool skip_not_found, const char *source) {
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  auto message_objects = transform(message_ids, [this, dialog_id, d, source](MessageId message_id) {
    return get_message_object(dialog_id, get_message_force(d, message_id, source), source);
  });
  return get_messages_object(total_count, std::move(message_objects), skip_not_found);
}

void MessagesManager::on_upload_imported_messages_error(FileId file_id, Status status) {
  if (G()->close_flag()) {
    // do not fail upload if closing
    return;
  }

  LOG(INFO) << "File " << file_id << " has upload error " << status;
  CHECK(status.is_error());

  auto it = being_uploaded_imported_messages_.find(file_id);
  if (it == being_uploaded_imported_messages_.end()) {
    // just in case, as in on_upload_media_error
    return;
  }

  Promise<Unit> promise = std::move(it->second->promise);

  being_uploaded_imported_messages_.erase(it);

  promise.set_error(std::move(status));
}

Status MessagesManager::can_use_message_send_options(const MessageSendOptions &options,
                                                     const unique_ptr<MessageContent> &content, int32 ttl) {
  if (options.schedule_date != 0) {
    if (ttl > 0) {
      return Status::Error(400, "Can't send scheduled self-destructing messages");
    }
    if (content->get_type() == MessageContentType::LiveLocation) {
      return Status::Error(400, "Can't send scheduled live location messages");
    }
  }

  return Status::OK();
}

bool MessagesManager::is_allowed_useless_update(const tl_object_ptr<telegram_api::Update> &update) {
  auto constructor_id = update->get_id();
  if (constructor_id == dummyUpdate::ID) {
    // allow dummyUpdate just in case
    return true;
  }
  if (constructor_id == telegram_api::updateNewMessage::ID ||
      constructor_id == telegram_api::updateNewChannelMessage::ID) {
    // new outgoing messages are received again if random_id coincide
    return true;
  }

  return false;
}

void MessagesManager::suffix_load_till_message_id(Dialog *d, MessageId message_id, Promise<> promise) {
  LOG(INFO) << "Load suffix of " << d->dialog_id << " till " << message_id;
  auto condition = [message_id](const Message *m) {
    return m != nullptr && m->message_id < message_id;
  };
  suffix_load_add_query(d, std::make_pair(std::move(promise), std::move(condition)));
}

int64 MessagesManager::get_dialog_pinned_order(const DialogList *list, DialogId dialog_id) {
  if (list != nullptr && !list->pinned_dialog_id_orders_.empty()) {
    auto it = list->pinned_dialog_id_orders_.find(dialog_id);
    if (it != list->pinned_dialog_id_orders_.end()) {
      return it->second;
    }
  }
  return DEFAULT_ORDER;
}

void MessagesManager::on_get_empty_messages(DialogId dialog_id, const vector<MessageId> &empty_message_ids) {
  if (!empty_message_ids.empty()) {
    delete_dialog_messages(dialog_id, empty_message_ids, true, true, "on_get_empty_messages");
  }
}

void MessagesManager::on_update_dialog_filters() {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  schedule_dialog_filters_reload(0.0);
}
}  // namespace td
