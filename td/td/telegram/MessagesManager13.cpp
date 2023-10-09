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










namespace td {
}  // namespace td
namespace td {

// DO NOT FORGET TO ADD ALL CHANGES OF THIS FUNCTION AS WELL TO do_delete_all_dialog_messages
unique_ptr<MessagesManager::Message> MessagesManager::do_delete_message(Dialog *d, MessageId message_id,
                                                                        bool is_permanently_deleted,
                                                                        bool only_from_memory,
                                                                        bool *need_update_dialog_pos,
                                                                        const char *source) {
  CHECK(d != nullptr);
  if (!message_id.is_valid()) {
    if (message_id.is_valid_scheduled()) {
      return do_delete_scheduled_message(d, message_id, is_permanently_deleted, source);
    }

    LOG(ERROR) << "Trying to delete " << message_id << " in " << d->dialog_id << " from " << source;
    return nullptr;
  }

  FullMessageId full_message_id(d->dialog_id, message_id);
  unique_ptr<Message> *v = treap_find_message(&d->messages, message_id);
  if (*v == nullptr) {
    LOG(INFO) << message_id << " is not found in " << d->dialog_id << " to be deleted from " << source;
    if (only_from_memory) {
      return nullptr;
    }

    if (get_message_force(d, message_id, "do_delete_message") == nullptr) {
      // currently there may be a race between add_message_to_database and get_message_force,
      // so delete a message from database just in case
      delete_message_from_database(d, message_id, nullptr, is_permanently_deleted);

      if (is_permanently_deleted && d->last_clear_history_message_id == message_id) {
        set_dialog_last_clear_history_date(d, 0, MessageId(), "do_delete_message");
        *need_update_dialog_pos = true;
      }

      /*
      can't do this because the message may be never received in the dialog, unread count will became negative
      // if last_read_inbox_message_id is not known, we can't be sure whether unread_count should be decreased or not
      if (message_id.is_valid() && !message_id.is_yet_unsent() && d->is_last_read_inbox_message_id_inited &&
          message_id > d->last_read_inbox_message_id && !td_->auth_manager_->is_bot()) {
        int32 server_unread_count = d->server_unread_count;
        int32 local_unread_count = d->local_unread_count;
        int32 &unread_count = message_id.is_server() ? server_unread_count : local_unread_count;
        if (unread_count == 0) {
          LOG(ERROR) << "Unread count became negative in " << d->dialog_id << " after deletion of " << message_id
                     << ". Last read is " << d->last_read_inbox_message_id;
          dump_debug_message_op(d, 3);
        } else {
          unread_count--;
          set_dialog_last_read_inbox_message_id(d, MessageId::min(), server_unread_count, local_unread_count, false,
                                                source);
        }
      }
      */
      return nullptr;
    }
    v = treap_find_message(&d->messages, message_id);
    CHECK(*v != nullptr);
  }

  const Message *m = v->get();
  CHECK(m->message_id == message_id);

  if (only_from_memory && !can_unload_message(d, m)) {
    return nullptr;
  }

  LOG_CHECK(!d->being_deleted_message_id.is_valid())
      << d->being_deleted_message_id << " " << message_id << " " << source;
  d->being_deleted_message_id = message_id;

  if (is_debug_message_op_enabled()) {
    d->debug_message_op.emplace_back(Dialog::MessageOp::Delete, m->message_id, m->content->get_type(), false,
                                     m->have_previous, m->have_next, source);
  }

  bool need_get_history = false;
  if (!only_from_memory) {
    LOG(INFO) << "Deleting " << full_message_id << " with have_previous = " << m->have_previous
              << " and have_next = " << m->have_next << " from " << source;

    delete_message_from_database(d, message_id, m, is_permanently_deleted);

    delete_active_live_location(d->dialog_id, m);
    remove_message_file_sources(d->dialog_id, m);

    if (message_id == d->last_message_id) {
      MessagesConstIterator it(d, message_id);
      CHECK(*it == m);
      if ((*it)->have_previous) {
        --it;
        if (*it != nullptr) {
          set_dialog_last_message_id(d, (*it)->message_id, "do_delete_message");
        } else {
          LOG(ERROR) << "Have have_previous is true, but there is no previous for " << full_message_id << " from "
                     << source;
          dump_debug_message_op(d);
          set_dialog_last_message_id(d, MessageId(), "do_delete_message");
        }
      } else {
        need_get_history = true;
        set_dialog_last_message_id(d, MessageId(), "do_delete_message");
        d->delete_last_message_date = m->date;
        d->deleted_last_message_id = message_id;
        d->is_last_message_deleted_locally = Slice(source) == Slice(DELETE_MESSAGE_USER_REQUEST_SOURCE);
        on_dialog_updated(d->dialog_id, "do delete last message");
      }
      *need_update_dialog_pos = true;
    }

    if (message_id == d->last_database_message_id) {
      MessagesConstIterator it(d, message_id);
      CHECK(*it == m);
      while ((*it)->have_previous) {
        --it;
        if (*it == nullptr || !(*it)->message_id.is_yet_unsent()) {
          break;
        }
      }

      if (*it != nullptr) {
        if (!(*it)->message_id.is_yet_unsent() && (*it)->message_id != d->last_database_message_id) {
          if ((*it)->message_id < d->first_database_message_id && d->dialog_id.get_type() == DialogType::Channel) {
            // possible if messages was deleted from database, but not from memory after updateChannelTooLong
            set_dialog_last_database_message_id(d, MessageId(), "do_delete_message");
          } else {
            set_dialog_last_database_message_id(d, (*it)->message_id, "do_delete_message");
            if (d->last_database_message_id < d->first_database_message_id) {
              LOG(ERROR) << "Last database " << d->last_database_message_id << " became less than first database "
                         << d->first_database_message_id << " after deletion of " << full_message_id;
              set_dialog_first_database_message_id(d, d->last_database_message_id, "do_delete_message 2");
            }
          }
        } else {
          need_get_history = true;
        }
      } else {
        LOG(ERROR) << "Have have_previous is true, but there is no previous";
        dump_debug_message_op(d);
      }
    }
    if (d->last_database_message_id.is_valid()) {
      CHECK(d->first_database_message_id.is_valid());
    } else {
      set_dialog_first_database_message_id(d, MessageId(), "do_delete_message");
    }

    if (message_id == d->suffix_load_first_message_id_) {
      MessagesConstIterator it(d, message_id);
      CHECK(*it == m);
      if ((*it)->have_previous) {
        --it;
        if (*it != nullptr) {
          d->suffix_load_first_message_id_ = (*it)->message_id;
        } else {
          LOG(ERROR) << "Have have_previous is true, but there is no previous for " << full_message_id << " from "
                     << source;
          dump_debug_message_op(d);
          d->suffix_load_first_message_id_ = MessageId();
          d->suffix_load_done_ = false;
        }
      } else {
        d->suffix_load_first_message_id_ = MessageId();
        d->suffix_load_done_ = false;
      }
    }
  }
  if (only_from_memory && message_id >= d->suffix_load_first_message_id_) {
    d->suffix_load_first_message_id_ = MessageId();
    d->suffix_load_done_ = false;
  }

  if (m->have_previous && (only_from_memory || !m->have_next)) {
    MessagesIterator it(d, message_id);
    CHECK(*it == m);
    --it;
    Message *prev_m = *it;
    if (prev_m != nullptr) {
      prev_m->have_next = false;
    } else {
      LOG(ERROR) << "Have have_previous is true, but there is no previous for " << full_message_id << " from "
                 << source;
      dump_debug_message_op(d);
    }
  }
  if ((*v)->have_next && (only_from_memory || !(*v)->have_previous)) {
    MessagesIterator it(d, message_id);
    CHECK(*it == m);
    ++it;
    Message *next_m = *it;
    if (next_m != nullptr) {
      next_m->have_previous = false;
    } else {
      LOG(ERROR) << "Have have_next is true, but there is no next for " << full_message_id << " from " << source;
      dump_debug_message_op(d);
    }
  }

  auto result = treap_delete_message(v);

  d->being_deleted_message_id = MessageId();

  if (!only_from_memory) {
    if (need_get_history && !td_->auth_manager_->is_bot() && have_input_peer(d->dialog_id, AccessRights::Read)) {
      send_closure_later(actor_id(this), &MessagesManager::get_history_from_the_end, d->dialog_id, true, false,
                         Promise<Unit>());
    }

    if (d->reply_markup_message_id == message_id) {
      set_dialog_reply_markup(d, MessageId());
    }
    // if last_read_inbox_message_id is not known, we can't be sure whether unread_count should be decreased or not
    if (has_incoming_notification(d->dialog_id, result.get()) && message_id > d->last_read_inbox_message_id &&
        d->is_last_read_inbox_message_id_inited && !td_->auth_manager_->is_bot()) {
      int32 server_unread_count = d->server_unread_count;
      int32 local_unread_count = d->local_unread_count;
      int32 &unread_count = message_id.is_server() ? server_unread_count : local_unread_count;
      if (unread_count == 0) {
        if (need_unread_counter(d->order)) {
          LOG(ERROR) << "Unread count became negative in " << d->dialog_id << " after deletion of " << message_id
                     << ". Last read is " << d->last_read_inbox_message_id;
          dump_debug_message_op(d, 3);
        }
      } else {
        unread_count--;
        set_dialog_last_read_inbox_message_id(d, MessageId::min(), server_unread_count, local_unread_count, false,
                                              source);
      }
    }
    if (result->contains_unread_mention) {
      if (d->unread_mention_count == 0) {
        if (is_dialog_inited(d)) {
          LOG(ERROR) << "Unread mention count became negative in " << d->dialog_id << " after deletion of "
                     << message_id;
        }
      } else {
        set_dialog_unread_mention_count(d, d->unread_mention_count - 1);
        send_update_chat_unread_mention_count(d);
      }
    }

    update_message_count_by_index(d, -1, result.get());
    update_reply_count_by_message(d, -1, result.get());
  }

  on_message_deleted(d, result.get(), is_permanently_deleted, source);

  return result;
}

Result<td_api::object_ptr<td_api::message>> MessagesManager::send_message(
    DialogId dialog_id, MessageId top_thread_message_id, MessageId reply_to_message_id,
    tl_object_ptr<td_api::messageSendOptions> &&options, tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
    tl_object_ptr<td_api::InputMessageContent> &&input_message_content) {
  if (input_message_content == nullptr) {
    return Status::Error(400, "Can't send message without content");
  }

  Dialog *d = get_dialog_force(dialog_id, "send_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  LOG(INFO) << "Begin to send message to " << dialog_id << " in reply to " << reply_to_message_id;

  reply_to_message_id = get_reply_to_message_id(d, top_thread_message_id, reply_to_message_id, false);

  if (input_message_content->get_id() == td_api::inputMessageForwarded::ID) {
    auto input_message = td_api::move_object_as<td_api::inputMessageForwarded>(input_message_content);
    TRY_RESULT(copy_options, process_message_copy_options(dialog_id, std::move(input_message->copy_options_)));
    copy_options.top_thread_message_id = top_thread_message_id;
    copy_options.reply_to_message_id = reply_to_message_id;
    TRY_RESULT_ASSIGN(copy_options.reply_markup, get_dialog_reply_markup(dialog_id, std::move(reply_markup)));
    return forward_message(dialog_id, DialogId(input_message->from_chat_id_), MessageId(input_message->message_id_),
                           std::move(options), input_message->in_game_share_, std::move(copy_options));
  }

  TRY_STATUS(can_send_message(dialog_id));
  TRY_RESULT(message_reply_markup, get_dialog_reply_markup(dialog_id, std::move(reply_markup)));
  TRY_RESULT(message_content, process_input_message_content(dialog_id, std::move(input_message_content)));
  TRY_RESULT(message_send_options, process_message_send_options(dialog_id, std::move(options)));
  TRY_STATUS(can_use_message_send_options(message_send_options, message_content));
  TRY_STATUS(can_use_top_thread_message_id(d, top_thread_message_id, reply_to_message_id));

  // there must be no errors after get_message_to_send call

  bool need_update_dialog_pos = false;
  Message *m = get_message_to_send(d, top_thread_message_id, reply_to_message_id, message_send_options,
                                   dup_message_content(td_, dialog_id, message_content.content.get(),
                                                       MessageContentDupType::Send, MessageCopyOptions()),
                                   &need_update_dialog_pos, false, nullptr, message_content.via_bot_user_id.is_valid());
  m->reply_markup = std::move(message_reply_markup);
  m->via_bot_user_id = message_content.via_bot_user_id;
  m->disable_web_page_preview = message_content.disable_web_page_preview;
  m->clear_draft = message_content.clear_draft;
  if (message_content.ttl > 0) {
    m->ttl = message_content.ttl;
    m->is_content_secret = is_secret_message_content(m->ttl, m->content->get_type());
  }
  m->send_emoji = std::move(message_content.emoji);

  if (message_content.clear_draft) {
    if (top_thread_message_id.is_valid()) {
      set_dialog_draft_message(dialog_id, top_thread_message_id, nullptr).ignore();
    } else {
      update_dialog_draft_message(d, nullptr, false, !need_update_dialog_pos);
    }
  }

  save_send_message_log_event(dialog_id, m);
  do_send_message(dialog_id, m);

  send_update_new_message(d, m);
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "send_message");
  }

  return get_message_object(dialog_id, m, "send_message");
}

void MessagesManager::on_upload_dialog_photo(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) {
  LOG(INFO) << "File " << file_id << " has been uploaded";

  auto it = being_uploaded_dialog_photos_.find(file_id);
  if (it == being_uploaded_dialog_photos_.end()) {
    // just in case, as in on_upload_media
    return;
  }

  DialogId dialog_id = it->second.dialog_id;
  double main_frame_timestamp = it->second.main_frame_timestamp;
  bool is_animation = it->second.is_animation;
  bool is_reupload = it->second.is_reupload;
  Promise<Unit> promise = std::move(it->second.promise);

  being_uploaded_dialog_photos_.erase(it);

  FileView file_view = td_->file_manager_->get_file_view(file_id);
  CHECK(!file_view.is_encrypted());
  if (input_file == nullptr && file_view.has_remote_location()) {
    if (file_view.main_remote_location().is_web()) {
      return promise.set_error(Status::Error(400, "Can't use web photo as profile photo"));
    }
    if (is_reupload) {
      return promise.set_error(Status::Error(400, "Failed to reupload the file"));
    }

    if (is_animation) {
      CHECK(file_view.get_type() == FileType::Animation);
      // delete file reference and forcely reupload the file
      auto file_reference = FileManager::extract_file_reference(file_view.main_remote_location().as_input_document());
      td_->file_manager_->delete_file_reference(file_id, file_reference);
      upload_dialog_photo(dialog_id, file_id, is_animation, main_frame_timestamp, true, std::move(promise), {-1});
    } else {
      CHECK(file_view.get_type() == FileType::Photo);
      auto input_photo = file_view.main_remote_location().as_input_photo();
      auto input_chat_photo = make_tl_object<telegram_api::inputChatPhoto>(std::move(input_photo));
      send_edit_dialog_photo_query(dialog_id, file_id, std::move(input_chat_photo), std::move(promise));
    }
    return;
  }
  CHECK(input_file != nullptr);

  int32 flags = 0;
  tl_object_ptr<telegram_api::InputFile> photo_input_file;
  tl_object_ptr<telegram_api::InputFile> video_input_file;
  if (is_animation) {
    flags |= telegram_api::inputChatUploadedPhoto::VIDEO_MASK;
    video_input_file = std::move(input_file);

    if (main_frame_timestamp != 0.0) {
      flags |= telegram_api::inputChatUploadedPhoto::VIDEO_START_TS_MASK;
    }
  } else {
    flags |= telegram_api::inputChatUploadedPhoto::FILE_MASK;
    photo_input_file = std::move(input_file);
  }

  auto input_chat_photo = make_tl_object<telegram_api::inputChatUploadedPhoto>(
      flags, std::move(photo_input_file), std::move(video_input_file), main_frame_timestamp);
  send_edit_dialog_photo_query(dialog_id, file_id, std::move(input_chat_photo), std::move(promise));
}

void MessagesManager::send_update_unread_message_count(DialogList &list, DialogId dialog_id, bool force,
                                                       const char *source, bool from_database) {
  if (td_->auth_manager_->is_bot() || !G()->parameters().use_message_db) {
    return;
  }

  auto dialog_list_id = list.dialog_list_id;
  CHECK(list.is_message_unread_count_inited_);
  if (list.unread_message_muted_count_ < 0 || list.unread_message_muted_count_ > list.unread_message_total_count_) {
    LOG(ERROR) << "Unread message count became invalid in " << dialog_list_id << ": "
               << list.unread_message_total_count_ << '/'
               << list.unread_message_total_count_ - list.unread_message_muted_count_ << " from " << source << " and "
               << dialog_id;
    if (list.unread_message_muted_count_ < 0) {
      list.unread_message_muted_count_ = 0;
    }
    if (list.unread_message_muted_count_ > list.unread_message_total_count_) {
      list.unread_message_total_count_ = list.unread_message_muted_count_;
    }
  }

  if (!from_database) {
    LOG(INFO) << "Save unread message count in " << dialog_list_id;
    G()->td_db()->get_binlog_pmc()->set(
        PSTRING() << "unread_message_count" << dialog_list_id.get(),
        PSTRING() << list.unread_message_total_count_ << ' ' << list.unread_message_muted_count_);
  }

  int32 unread_unmuted_count = list.unread_message_total_count_ - list.unread_message_muted_count_;
  if (!force && running_get_difference_) {
    LOG(INFO) << "Postpone updateUnreadMessageCount in " << dialog_list_id << " to " << list.unread_message_total_count_
              << '/' << unread_unmuted_count << " from " << source << " and " << dialog_id;
    postponed_unread_message_count_updates_.insert(dialog_list_id);
  } else {
    postponed_unread_message_count_updates_.erase(dialog_list_id);
    LOG(INFO) << "Send updateUnreadMessageCount in " << dialog_list_id << " to " << list.unread_message_total_count_
              << '/' << unread_unmuted_count << " from " << source << " and " << dialog_id;
    send_closure(G()->td(), &Td::send_update, get_update_unread_message_count_object(list));
  }
}

void MessagesManager::on_get_messages_search_result(const string &query, int32 offset_date, DialogId offset_dialog_id,
                                                    MessageId offset_message_id, int32 limit,
                                                    MessageSearchFilter filter, int32 min_date, int32 max_date,
                                                    int64 random_id, int32 total_count,
                                                    vector<tl_object_ptr<telegram_api::Message>> &&messages,
                                                    Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Receive " << messages.size() << " found messages";
  auto it = found_messages_.find(random_id);
  CHECK(it != found_messages_.end());

  auto &result = it->second.second;
  CHECK(result.empty());
  for (auto &message : messages) {
    auto dialog_id = get_message_dialog_id(message);
    auto new_full_message_id = on_get_message(std::move(message), false, dialog_id.get_type() == DialogType::Channel,
                                              false, false, false, "search messages");
    if (new_full_message_id != FullMessageId()) {
      CHECK(dialog_id == new_full_message_id.get_dialog_id());
      result.push_back(new_full_message_id);
    } else {
      total_count--;
    }
  }
  if (total_count < static_cast<int32>(result.size())) {
    LOG(ERROR) << "Receive " << result.size() << " valid messages out of " << total_count << " in " << messages.size()
               << " messages";
    total_count = static_cast<int32>(result.size());
  }
  it->second.first = total_count;
  promise.set_value(Unit());
}

void MessagesManager::get_message_force_from_server(Dialog *d, MessageId message_id, Promise<Unit> &&promise,
                                                    tl_object_ptr<telegram_api::InputMessage> input_message) {
  LOG(INFO) << "Get " << message_id << " in " << d->dialog_id << " using " << to_string(input_message);
  auto dialog_type = d->dialog_id.get_type();
  auto m = get_message_force(d, message_id, "get_message_force_from_server");
  if (m == nullptr) {
    if (message_id.is_valid() && message_id.is_server()) {
      if (d->last_new_message_id != MessageId() && message_id > d->last_new_message_id &&
          dialog_type != DialogType::Channel) {
        // message will not be added to the dialog anyway
        return promise.set_value(Unit());
      }

      if (d->deleted_message_ids.count(message_id) == 0 && dialog_type != DialogType::SecretChat) {
        return get_message_from_server({d->dialog_id, message_id}, std::move(promise), "get_message_force_from_server",
                                       std::move(input_message));
      }
    } else if (message_id.is_valid_scheduled() && message_id.is_scheduled_server()) {
      if (d->deleted_scheduled_server_message_ids.count(message_id.get_scheduled_server_message_id()) == 0 &&
          dialog_type != DialogType::SecretChat && input_message == nullptr) {
        return get_message_from_server({d->dialog_id, message_id}, std::move(promise), "get_message_force_from_server");
      }
    }
  }

  promise.set_value(Unit());
}

void MessagesManager::on_message_ttl_expired_impl(Dialog *d, Message *m) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());
  CHECK(m->ttl > 0);
  CHECK(d->dialog_id.get_type() != DialogType::SecretChat);
  delete_message_files(d->dialog_id, m);
  update_expired_message_content(m->content);
  m->ttl = 0;
  m->ttl_expires_at = 0;
  if (m->reply_markup != nullptr) {
    if (m->reply_markup->type != ReplyMarkup::Type::InlineKeyboard) {
      if (!td_->auth_manager_->is_bot()) {
        if (d->reply_markup_message_id == m->message_id) {
          set_dialog_reply_markup(d, MessageId());
        }
      }
      m->had_reply_markup = true;
    }
    m->reply_markup = nullptr;
  }
  remove_message_notification_id(d, m, true, true);
  update_message_contains_unread_mention(d, m, false, "on_message_ttl_expired_impl");
  unregister_message_reply(d, m);
  m->noforwards = false;
  m->contains_mention = false;
  m->reply_to_message_id = MessageId();
  m->max_reply_media_timestamp = -1;
  m->reply_in_dialog_id = DialogId();
  m->top_thread_message_id = MessageId();
  m->linked_top_thread_message_id = MessageId();
  m->is_content_secret = false;
}

void MessagesManager::on_send_secret_message_error(int64 random_id, Status error, Promise<> promise) {
  promise.set_value(Unit());  // TODO: set after error is saved

  auto it = being_sent_messages_.find(random_id);
  if (it != being_sent_messages_.end()) {
    auto full_message_id = it->second;
    auto *m = get_message(full_message_id);
    if (m != nullptr) {
      auto file_id = get_message_content_upload_file_id(m->content.get());
      if (file_id.is_valid()) {
        if (G()->close_flag() && G()->parameters().use_message_db) {
          // do not send error, message will be re-sent
          return;
        }
        if (begins_with(error.message(), "FILE_PART_") && ends_with(error.message(), "_MISSING")) {
          on_send_message_file_part_missing(random_id, to_integer<int32>(error.message().substr(10)));
          return;
        }

        if (error.code() != 429 && error.code() < 500 && !G()->close_flag()) {
          td_->file_manager_->delete_partial_remote_location(file_id);
        }
      }
    }
  }

  on_send_message_fail(random_id, std::move(error));
}

bool MessagesManager::on_get_dialog_error(DialogId dialog_id, const Status &status, const string &source) {
  if (status.message() == CSlice("BOT_METHOD_INVALID")) {
    LOG(ERROR) << "Receive BOT_METHOD_INVALID from " << source;
    return true;
  }
  if (G()->is_expected_error(status)) {
    return true;
  }
  if (status.message() == CSlice("SEND_AS_PEER_INVALID")) {
    reload_dialog_info_full(dialog_id);
    return true;
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::SecretChat:
      // to be implemented if necessary
      break;
    case DialogType::Channel:
      return td_->contacts_manager_->on_get_channel_error(dialog_id.get_channel_id(), status, source);
    case DialogType::None:
      // to be implemented if necessary
      break;
    default:
      UNREACHABLE();
  }
  return false;
}

tl_object_ptr<telegram_api::InputPeer> MessagesManager::get_input_peer(DialogId dialog_id,
                                                                       AccessRights access_rights) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->get_input_peer_user(dialog_id.get_user_id(), access_rights);
    case DialogType::Chat:
      return td_->contacts_manager_->get_input_peer_chat(dialog_id.get_chat_id(), access_rights);
    case DialogType::Channel:
      return td_->contacts_manager_->get_input_peer_channel(dialog_id.get_channel_id(), access_rights);
    case DialogType::SecretChat:
      return nullptr;
    case DialogType::None:
      return make_tl_object<telegram_api::inputPeerEmpty>();
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void MessagesManager::invalidate_message_indexes(Dialog *d) {
  CHECK(d != nullptr);
  bool is_secret = d->dialog_id.get_type() == DialogType::SecretChat;
  for (size_t i = 0; i < d->message_count_by_index.size(); i++) {
    if (is_secret || i == static_cast<size_t>(message_search_filter_index(MessageSearchFilter::FailedToSend))) {
      // always know all messages
      d->first_database_message_id_by_index[i] = MessageId::min();
      // keep the count
    } else {
      // some messages are unknown; drop first_database_message_id and count
      d->first_database_message_id_by_index[i] = MessageId();
      d->message_count_by_index[i] = -1;
    }
  }
}

vector<td_api::object_ptr<td_api::chatPosition>> MessagesManager::get_chat_positions_object(const Dialog *d) const {
  vector<td_api::object_ptr<td_api::chatPosition>> positions;
  if (!td_->auth_manager_->is_bot()) {
    for (auto dialog_list_id : get_dialog_list_ids(d)) {
      auto position = get_chat_position_object(dialog_list_id, d);
      if (position != nullptr) {
        positions.push_back(std::move(position));
      }
    }
    if (is_dialog_sponsored(d)) {
      CHECK(positions.empty());
      positions.push_back(get_chat_position_object(DialogListId(FolderId::main()), d));
    }
  }
  return positions;
}

void MessagesManager::on_failed_public_dialogs_search(const string &query, Status &&error) {
  auto it = search_public_dialogs_queries_.find(query);
  CHECK(it != search_public_dialogs_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  search_public_dialogs_queries_.erase(it);

  found_public_dialogs_[query];     // negative cache
  found_on_server_dialogs_[query];  // negative cache

  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

FullMessageId MessagesManager::get_replied_message_id(DialogId dialog_id, const Message *m) {
  auto full_message_id = get_message_content_replied_message_id(dialog_id, m->content.get());
  if (full_message_id.get_message_id().is_valid()) {
    CHECK(!m->reply_to_message_id.is_valid());
    return full_message_id;
  }
  if (!m->reply_to_message_id.is_valid()) {
    return {};
  }
  return {m->reply_in_dialog_id.is_valid() ? m->reply_in_dialog_id : dialog_id, m->reply_to_message_id};
}

void MessagesManager::on_add_secret_message_ready(int64 token) {
  if (G()->close_flag()) {
    return;
  }

  pending_secret_messages_.finish(
      token, [actor_id = actor_id(this)](unique_ptr<PendingSecretMessage> pending_secret_message) {
        send_closure_later(actor_id, &MessagesManager::finish_add_secret_message, std::move(pending_secret_message));
      });
}

td_api::object_ptr<td_api::updateChatFilters> MessagesManager::get_update_chat_filters_object() const {
  CHECK(!td_->auth_manager_->is_bot());
  auto update = td_api::make_object<td_api::updateChatFilters>();
  for (const auto &filter : dialog_filters_) {
    update->chat_filters_.push_back(filter->get_chat_filter_info_object());
  }
  return update;
}

void MessagesManager::on_saved_dialog_draft_message(DialogId dialog_id, uint64 generation) {
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  delete_log_event(d->save_draft_message_log_event_id, generation, "draft");
}

MessagesManager::Dialog *MessagesManager::get_dialog(DialogId dialog_id) {
  auto it = dialogs_.find(dialog_id);
  return it == dialogs_.end() ? nullptr : it->second.get();
}
}  // namespace td
