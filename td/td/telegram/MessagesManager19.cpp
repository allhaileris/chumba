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

void MessagesManager::edit_dialog_filter(unique_ptr<DialogFilter> new_dialog_filter, const char *source) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  CHECK(new_dialog_filter != nullptr);
  LOG(INFO) << "Edit " << new_dialog_filter->dialog_filter_id << " from " << source;
  for (auto &old_dialog_filter : dialog_filters_) {
    if (old_dialog_filter->dialog_filter_id == new_dialog_filter->dialog_filter_id) {
      CHECK(*old_dialog_filter != *new_dialog_filter);

      auto dialog_list_id = DialogListId(old_dialog_filter->dialog_filter_id);
      auto *old_list_ptr = get_dialog_list(dialog_list_id);
      CHECK(old_list_ptr != nullptr);
      auto &old_list = *old_list_ptr;

      disable_get_dialog_filter_ = true;  // to ensure crash if get_dialog_filter is called

      auto folder_ids = get_dialog_filter_folder_ids(old_dialog_filter.get());
      CHECK(!folder_ids.empty());
      for (auto folder_id : get_dialog_filter_folder_ids(new_dialog_filter.get())) {
        if (!td::contains(folder_ids, folder_id)) {
          folder_ids.push_back(folder_id);
        }
      }

      DialogList new_list;
      new_list.dialog_list_id = dialog_list_id;

      auto old_it = old_list.pinned_dialogs_.rbegin();
      for (const auto &input_dialog_id : reversed(new_dialog_filter->pinned_dialog_ids)) {
        auto dialog_id = input_dialog_id.get_dialog_id();
        while (old_it < old_list.pinned_dialogs_.rend()) {
          if (old_it->get_dialog_id() == dialog_id) {
            break;
          }
          ++old_it;
        }

        int64 order;
        if (old_it < old_list.pinned_dialogs_.rend()) {
          order = old_it->get_order();
          ++old_it;
        } else {
          order = get_next_pinned_dialog_order();
        }
        new_list.pinned_dialogs_.emplace_back(order, dialog_id);
        new_list.pinned_dialog_id_orders_.emplace(dialog_id, order);
      }
      std::reverse(new_list.pinned_dialogs_.begin(), new_list.pinned_dialogs_.end());
      new_list.are_pinned_dialogs_inited_ = true;

      do_update_list_last_pinned_dialog_date(new_list);
      do_update_list_last_dialog_date(new_list, get_dialog_filter_folder_ids(new_dialog_filter.get()));

      new_list.server_dialog_total_count_ = 0;
      new_list.secret_chat_total_count_ = 0;

      std::map<DialogDate, const Dialog *> updated_position_dialogs;
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

          const DialogPositionInList old_position = get_dialog_position_in_list(old_list_ptr, d);
          // can't use get_dialog_position_in_list, because need_dialog_in_list calls get_dialog_filter
          DialogPositionInList new_position;
          if (need_dialog_in_filter(d, new_dialog_filter.get())) {
            new_position.private_order = get_dialog_private_order(&new_list, d);
            if (new_position.private_order != 0) {
              new_position.public_order =
                  DialogDate(new_position.private_order, dialog_id) <= new_list.list_last_dialog_date_
                      ? new_position.private_order
                      : 0;
              new_position.is_pinned = get_dialog_pinned_order(&new_list, dialog_id) != DEFAULT_ORDER;
              new_position.is_sponsored = is_dialog_sponsored(d);
            }
          }

          if (need_send_update_chat_position(old_position, new_position)) {
            updated_position_dialogs.emplace(DialogDate(new_position.public_order, dialog_id), d);
          }

          bool was_in_list = old_position.private_order != 0;
          bool is_in_list = new_position.private_order != 0;
          if (is_in_list) {
            if (!was_in_list) {
              add_dialog_to_list(d, dialog_list_id);
            }

            new_list.in_memory_dialog_total_count_++;
            if (dialog_id.get_type() == DialogType::SecretChat) {
              new_list.secret_chat_total_count_++;
            } else {
              new_list.server_dialog_total_count_++;
            }

            auto unread_count = d->server_unread_count + d->local_unread_count;
            if (unread_count != 0) {
              new_list.unread_message_total_count_ += unread_count;
              if (is_dialog_muted(d)) {
                new_list.unread_message_muted_count_ += unread_count;
              }
            }
            if (unread_count != 0 || d->is_marked_as_unread) {
              new_list.unread_dialog_total_count_++;
              if (unread_count == 0 && d->is_marked_as_unread) {
                new_list.unread_dialog_marked_count_++;
              }
              if (is_dialog_muted(d)) {
                new_list.unread_dialog_muted_count_++;
                if (unread_count == 0 && d->is_marked_as_unread) {
                  new_list.unread_dialog_muted_marked_count_++;
                }
              }
            }
          } else {
            if (was_in_list) {
              remove_dialog_from_list(d, dialog_list_id);
            }
          }
        }
      }

      if (new_list.list_last_dialog_date_ == MAX_DIALOG_DATE) {
        new_list.is_message_unread_count_inited_ = true;
        new_list.is_dialog_unread_count_inited_ = true;
        new_list.need_unread_count_recalc_ = false;
      } else {
        if (old_list.is_message_unread_count_inited_) {  // can't stop sending updates
          new_list.is_message_unread_count_inited_ = true;
        }
        if (old_list.is_dialog_unread_count_inited_) {  // can't stop sending updates
          new_list.is_dialog_unread_count_inited_ = true;
        }
        new_list.server_dialog_total_count_ = -1;
        new_list.secret_chat_total_count_ = -1;
      }

      bool need_update_unread_message_count =
          new_list.is_message_unread_count_inited_ &&
          (old_list.unread_message_total_count_ != new_list.unread_message_total_count_ ||
           old_list.unread_message_muted_count_ != new_list.unread_message_muted_count_ ||
           !old_list.is_message_unread_count_inited_);
      bool need_update_unread_chat_count =
          new_list.is_dialog_unread_count_inited_ &&
          (old_list.unread_dialog_total_count_ != new_list.unread_dialog_total_count_ ||
           old_list.unread_dialog_muted_count_ != new_list.unread_dialog_muted_count_ ||
           old_list.unread_dialog_marked_count_ != new_list.unread_dialog_marked_count_ ||
           old_list.unread_dialog_muted_marked_count_ != new_list.unread_dialog_muted_marked_count_ ||
           get_dialog_total_count(old_list) != get_dialog_total_count(new_list) ||
           !old_list.is_dialog_unread_count_inited_);
      bool need_save_unread_chat_count = new_list.is_dialog_unread_count_inited_ &&
                                         (old_list.server_dialog_total_count_ != new_list.server_dialog_total_count_ ||
                                          old_list.secret_chat_total_count_ != new_list.secret_chat_total_count_);

      auto load_list_promises = std::move(old_list.load_list_queries_);

      disable_get_dialog_filter_ = false;

      old_list = std::move(new_list);
      old_dialog_filter = std::move(new_dialog_filter);

      if (need_update_unread_message_count) {
        send_update_unread_message_count(old_list, DialogId(), true, source);
      }
      if (need_update_unread_chat_count) {
        send_update_unread_chat_count(old_list, DialogId(), true, source);
      } else if (need_save_unread_chat_count) {
        save_unread_chat_count(old_list);
      }

      for (const auto &it : updated_position_dialogs) {
        send_update_chat_position(dialog_list_id, it.second, source);
      }

      if (old_list.need_unread_count_recalc_) {
        // repair unread count
        get_dialogs_from_list(dialog_list_id, static_cast<int32>(old_list.pinned_dialogs_.size() + 2), Auto());
      }

      if (!load_list_promises.empty()) {
        LOG(INFO) << "Retry loading of chats in " << dialog_list_id;
        for (auto &promise : load_list_promises) {
          promise.set_value(Unit());  // try again
        }
      }
      return;
    }
  }
  UNREACHABLE();
}

class UploadImportedMediaQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  int64 import_id_;
  FileId file_id_;

 public:
  explicit UploadImportedMediaQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int64 import_id, const string &file_name, FileId file_id,
            tl_object_ptr<telegram_api::InputMedia> &&input_media) {
    CHECK(input_media != nullptr);
    dialog_id_ = dialog_id;
    import_id_ = import_id;
    file_id_ = file_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_uploadImportedMedia(
        std::move(input_peer), import_id, file_name, std::move(input_media))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_uploadImportedMedia>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->file_manager_->delete_partial_remote_location(file_id_);

    // ignore response

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (FileReferenceManager::is_file_reference_error(status)) {
      LOG(ERROR) << "Receive file reference error " << status;
    }
    if (begins_with(status.message(), "FILE_PART_") && ends_with(status.message(), "_MISSING")) {
      // TODO support FILE_PART_*_MISSING
    }

    td_->file_manager_->delete_partial_remote_location(file_id_);
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "UploadImportedMediaQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::on_upload_imported_message_attachment(FileId file_id,
                                                            tl_object_ptr<telegram_api::InputFile> input_file) {
  LOG(INFO) << "File " << file_id << " has been uploaded";

  auto it = being_uploaded_imported_message_attachments_.find(file_id);
  if (it == being_uploaded_imported_message_attachments_.end()) {
    // just in case, as in on_upload_media
    return;
  }

  CHECK(it->second != nullptr);
  DialogId dialog_id = it->second->dialog_id;
  int64 import_id = it->second->import_id;
  bool is_reupload = it->second->is_reupload;
  Promise<Unit> promise = std::move(it->second->promise);

  being_uploaded_imported_message_attachments_.erase(it);

  FileView file_view = td_->file_manager_->get_file_view(file_id);
  CHECK(!file_view.is_encrypted());
  if (input_file == nullptr && file_view.has_remote_location()) {
    if (file_view.main_remote_location().is_web()) {
      return promise.set_error(Status::Error(400, "Can't use web file"));
    }
    if (is_reupload) {
      return promise.set_error(Status::Error(400, "Failed to reupload the file"));
    }

    // delete file reference and forcely reupload the file
    auto file_reference =
        file_view.get_type() == FileType::Photo
            ? FileManager::extract_file_reference(file_view.main_remote_location().as_input_photo())
            : FileManager::extract_file_reference(file_view.main_remote_location().as_input_document());
    td_->file_manager_->delete_file_reference(file_id, file_reference);
    upload_imported_message_attachment(dialog_id, import_id, file_id, true, std::move(promise), {-1});
    return;
  }
  CHECK(input_file != nullptr);

  auto suggested_path = file_view.suggested_path();
  const PathView path_view(suggested_path);
  td_->create_handler<UploadImportedMediaQuery>(std::move(promise))
      ->send(dialog_id, import_id, path_view.file_name().str(), file_id,
             get_fake_input_media(td_, std::move(input_file), file_id));
}

void MessagesManager::on_update_sent_text_message(int64 random_id,
                                                  tl_object_ptr<telegram_api::MessageMedia> message_media,
                                                  vector<tl_object_ptr<telegram_api::MessageEntity>> &&entities) {
  int32 message_media_id = message_media == nullptr ? telegram_api::messageMediaEmpty::ID : message_media->get_id();
  LOG_IF(ERROR, message_media_id != telegram_api::messageMediaWebPage::ID &&
                    message_media_id != telegram_api::messageMediaEmpty::ID)
      << "Receive non web-page media for text message: " << oneline(to_string(message_media));

  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    // result of sending message has already been received through getDifference
    return;
  }

  auto full_message_id = it->second;
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog(dialog_id);
  auto m = get_message_force(d, full_message_id.get_message_id(), "on_update_sent_text_message");
  if (m == nullptr) {
    // message has already been deleted
    return;
  }
  CHECK(m->message_id.is_yet_unsent());
  full_message_id = FullMessageId(dialog_id, m->message_id);

  if (m->content->get_type() != MessageContentType::Text) {
    LOG(ERROR) << "Text message content has been already changed to " << m->content->get_type();
    return;
  }

  const FormattedText *old_message_text = get_message_content_text(m->content.get());
  CHECK(old_message_text != nullptr);
  FormattedText new_message_text = get_message_text(
      td_->contacts_manager_.get(), old_message_text->text, std::move(entities), true, td_->auth_manager_->is_bot(),
      m->forward_info ? m->forward_info->date : m->date, m->media_album_id != 0, "on_update_sent_text_message");
  auto new_content =
      get_message_content(td_, std::move(new_message_text), std::move(message_media), dialog_id,
                          true /*likely ignored*/, UserId() /*likely ignored*/, nullptr /*ignored*/, nullptr);
  if (new_content->get_type() != MessageContentType::Text) {
    LOG(ERROR) << "Text message content has changed to " << new_content->get_type();
    return;
  }

  bool need_update = false;
  bool is_content_changed = false;
  merge_message_contents(td_, m->content.get(), new_content.get(), need_message_changed_warning(m), dialog_id, false,
                         is_content_changed, need_update);

  if (is_content_changed || need_update) {
    reregister_message_content(td_, m->content.get(), new_content.get(), full_message_id,
                               "on_update_sent_text_message");
    m->content = std::move(new_content);
    m->is_content_secret = is_secret_message_content(m->ttl, MessageContentType::Text);
  }
  if (need_update) {
    send_update_message_content(dialog_id, m, true, "on_update_sent_text_message");
    if (m->message_id == d->last_message_id) {
      send_update_chat_last_message_impl(d, "on_update_sent_text_message");
    }
  }
}

void MessagesManager::process_discussion_message_impl(
    telegram_api::object_ptr<telegram_api::messages_discussionMessage> &&result, DialogId dialog_id,
    MessageId message_id, DialogId expected_dialog_id, MessageId expected_message_id,
    Promise<MessageThreadInfo> promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  MessageId max_message_id;
  MessageId last_read_inbox_message_id;
  MessageId last_read_outbox_message_id;
  if ((result->flags_ & telegram_api::messages_discussionMessage::MAX_ID_MASK) != 0) {
    max_message_id = MessageId(ServerMessageId(result->max_id_));
  }
  if ((result->flags_ & telegram_api::messages_discussionMessage::READ_INBOX_MAX_ID_MASK) != 0) {
    last_read_inbox_message_id = MessageId(ServerMessageId(result->read_inbox_max_id_));
  }
  if ((result->flags_ & telegram_api::messages_discussionMessage::READ_OUTBOX_MAX_ID_MASK) != 0) {
    last_read_outbox_message_id = MessageId(ServerMessageId(result->read_outbox_max_id_));
  }

  MessageThreadInfo message_thread_info;
  message_thread_info.dialog_id = expected_dialog_id;
  message_thread_info.unread_message_count = max(0, result->unread_count_);
  MessageId top_message_id;
  for (auto &message : result->messages_) {
    auto full_message_id =
        on_get_message(std::move(message), false, true, false, false, false, "process_discussion_message_impl");
    if (full_message_id.get_message_id().is_valid()) {
      CHECK(full_message_id.get_dialog_id() == expected_dialog_id);
      message_thread_info.message_ids.push_back(full_message_id.get_message_id());
      if (full_message_id.get_message_id() == expected_message_id) {
        top_message_id = expected_message_id;
      }
    }
  }
  if (!message_thread_info.message_ids.empty() && !top_message_id.is_valid()) {
    top_message_id = message_thread_info.message_ids.back();
  }
  if (top_message_id.is_valid()) {
    on_update_read_message_comments(expected_dialog_id, top_message_id, max_message_id, last_read_inbox_message_id,
                                    last_read_outbox_message_id);
  }
  if (expected_dialog_id != dialog_id) {
    on_update_read_message_comments(dialog_id, message_id, max_message_id, last_read_inbox_message_id,
                                    last_read_outbox_message_id);
  }
  promise.set_value(std::move(message_thread_info));
}

int64 MessagesManager::get_dialog_message_by_date(DialogId dialog_id, int32 date, Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "get_dialog_message_by_date");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return 0;
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    promise.set_error(Status::Error(400, "Can't access the chat"));
    return 0;
  }

  if (date <= 0) {
    date = 1;
  }

  int64 random_id = 0;
  do {
    random_id = Random::secure_int64();
  } while (random_id == 0 ||
           get_dialog_message_by_date_results_.find(random_id) != get_dialog_message_by_date_results_.end());
  get_dialog_message_by_date_results_[random_id];  // reserve place for result

  auto message_id = find_message_by_date(d->messages.get(), date);
  if (message_id.is_valid() && (message_id == d->last_message_id || get_message(d, message_id)->have_next)) {
    get_dialog_message_by_date_results_[random_id] = {dialog_id, message_id};
    promise.set_value(Unit());
    return random_id;
  }

  if (G()->parameters().use_message_db && d->last_database_message_id != MessageId()) {
    CHECK(d->first_database_message_id != MessageId());
    G()->td_db()->get_messages_db_async()->get_dialog_message_by_date(
        dialog_id, d->first_database_message_id, d->last_database_message_id, date,
        PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, date, random_id,
                                promise = std::move(promise)](Result<MessagesDbDialogMessage> result) mutable {
          send_closure(actor_id, &MessagesManager::on_get_dialog_message_by_date_from_database, dialog_id, date,
                       random_id, std::move(result), std::move(promise));
        }));
  } else {
    get_dialog_message_by_date_from_server(d, date, random_id, false, std::move(promise));
  }
  return random_id;
}

td_api::object_ptr<td_api::messageLinkInfo> MessagesManager::get_message_link_info_object(
    const MessageLinkInfo &info) const {
  CHECK(info.username.empty() == info.channel_id.is_valid());

  bool is_public = !info.username.empty();
  DialogId dialog_id = info.comment_dialog_id.is_valid()
                           ? info.comment_dialog_id
                           : (is_public ? resolve_dialog_username(info.username) : DialogId(info.channel_id));
  MessageId message_id = info.comment_dialog_id.is_valid() ? info.comment_message_id : info.message_id;
  td_api::object_ptr<td_api::message> message;
  int32 media_timestamp = 0;
  bool for_album = false;
  bool for_comment = false;

  const Dialog *d = get_dialog(dialog_id);
  if (d == nullptr) {
    dialog_id = DialogId();
  } else {
    const Message *m = get_message(d, message_id);
    if (m != nullptr) {
      message = get_message_object(dialog_id, m, "get_message_link_info_object");
      for_album = !info.is_single && m->media_album_id != 0;
      for_comment = (info.comment_dialog_id.is_valid() || info.for_comment) && m->top_thread_message_id.is_valid();
      if (can_message_content_have_media_timestamp(m->content.get())) {
        auto duration = get_message_content_media_duration(m->content.get(), td_);
        if (duration == 0 || info.media_timestamp <= duration) {
          media_timestamp = info.media_timestamp;
        }
      }
    }
  }

  return td_api::make_object<td_api::messageLinkInfo>(is_public, dialog_id.get(), std::move(message), media_timestamp,
                                                      for_album, for_comment);
}

void MessagesManager::save_dialog_to_database(DialogId dialog_id) {
  CHECK(G()->parameters().use_message_db);
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  LOG(INFO) << "Save " << dialog_id << " to database";
  vector<NotificationGroupKey> changed_group_keys;
  bool can_reuse_notification_group = false;
  auto add_group_key = [&](auto &group_info) {
    if (group_info.is_changed) {
      can_reuse_notification_group |= group_info.try_reuse;
      changed_group_keys.emplace_back(group_info.group_id, group_info.try_reuse ? DialogId() : dialog_id,
                                      group_info.last_notification_date);
      group_info.is_changed = false;
    }
  };
  add_group_key(d->message_notification_group);
  add_group_key(d->mention_notification_group);
  auto fixed_folder_id = d->folder_id == FolderId::archive() ? FolderId::archive() : FolderId::main();
  G()->td_db()->get_dialog_db_async()->add_dialog(
      dialog_id, fixed_folder_id, d->is_folder_id_inited ? d->order : 0, get_dialog_database_value(d),
      std::move(changed_group_keys), PromiseCreator::lambda([dialog_id, can_reuse_notification_group](Result<> result) {
        send_closure(G()->messages_manager(), &MessagesManager::on_save_dialog_to_database, dialog_id,
                     can_reuse_notification_group, result.is_ok());
      }));
}

MessageId MessagesManager::get_next_message_id(Dialog *d, MessageType type) {
  CHECK(d != nullptr);
  MessageId last_message_id =
      std::max({d->last_message_id, d->last_new_message_id, d->last_database_message_id, d->last_assigned_message_id,
                d->last_clear_history_message_id, d->deleted_last_message_id, d->max_unavailable_message_id,
                d->max_added_message_id});
  if (last_message_id < d->last_read_inbox_message_id &&
      d->last_read_inbox_message_id < d->last_new_message_id.get_next_server_message_id()) {
    last_message_id = d->last_read_inbox_message_id;
  }
  if (last_message_id < d->last_read_outbox_message_id &&
      d->last_read_outbox_message_id < d->last_new_message_id.get_next_server_message_id()) {
    last_message_id = d->last_read_outbox_message_id;
  }

  d->last_assigned_message_id = last_message_id.get_next_message_id(type);
  if (d->last_assigned_message_id > MessageId::max()) {
    LOG(FATAL) << "Force restart because of message_id overflow: " << d->last_assigned_message_id;
  }
  CHECK(d->last_assigned_message_id.is_valid());
  return d->last_assigned_message_id;
}

MessagesManager::DialogPositionInList MessagesManager::get_dialog_position_in_list(const DialogList *list,
                                                                                   const Dialog *d, bool actual) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(list != nullptr);
  CHECK(d != nullptr);

  DialogPositionInList position;
  position.order = d->order;
  if (is_dialog_sponsored(d) || (actual ? need_dialog_in_list(d, *list) : is_dialog_in_list(d, list->dialog_list_id))) {
    position.private_order = get_dialog_private_order(list, d);
  }
  if (position.private_order != 0) {
    position.public_order =
        DialogDate(position.private_order, d->dialog_id) <= list->list_last_dialog_date_ ? position.private_order : 0;
    position.is_pinned = get_dialog_pinned_order(list, d->dialog_id) != DEFAULT_ORDER;
    position.is_sponsored = is_dialog_sponsored(d);
  }
  position.total_dialog_count = get_dialog_total_count(*list);
  return position;
}

bool MessagesManager::update_dialog_silent_send_message(Dialog *d, bool silent_send_message) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return false;
  }

  CHECK(d != nullptr);
  LOG_IF(WARNING, !d->notification_settings.is_synchronized)
      << "Have unknown notification settings in " << d->dialog_id;
  if (d->notification_settings.silent_send_message == silent_send_message) {
    return false;
  }

  LOG(INFO) << "Update silent send message in " << d->dialog_id << " to " << silent_send_message;
  d->notification_settings.silent_send_message = silent_send_message;

  on_dialog_updated(d->dialog_id, "update_dialog_silent_send_message");

  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatDefaultDisableNotification>(d->dialog_id.get(), silent_send_message));
  return true;
}

void MessagesManager::on_dialog_deleted(DialogId dialog_id, Promise<Unit> &&promise) {
  LOG(INFO) << "Delete " << dialog_id;
  Dialog *d = get_dialog_force(dialog_id, "on_dialog_deleted");
  if (d == nullptr) {
    return promise.set_value(Unit());
  }

  delete_all_dialog_messages(d, true, false);
  if (dialog_id.get_type() != DialogType::SecretChat) {
    d->have_full_history = false;
    d->is_empty = false;
    d->need_restore_reply_markup = true;
  }
  recently_found_dialogs_.remove_dialog(dialog_id);
  recently_opened_dialogs_.remove_dialog(dialog_id);
  if (dialog_id.get_type() == DialogType::Channel) {
    G()->td_db()->get_binlog_pmc()->erase(get_channel_pts_key(dialog_id));
  }

  close_dialog(d);
  promise.set_value(Unit());
}

void MessagesManager::try_add_active_live_location(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);

  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (m->content->get_type() != MessageContentType::LiveLocation || m->message_id.is_scheduled() ||
      m->message_id.is_local() || m->via_bot_user_id.is_valid() || m->forward_info != nullptr) {
    return;
  }

  auto live_period = get_message_content_live_location_period(m->content.get());
  if (live_period <= G()->unix_time() - m->date + 1) {  // bool is_expired flag?
    // live location is expired
    return;
  }
  add_active_live_location({dialog_id, m->message_id});
}

int32 MessagesManager::get_message_date(const tl_object_ptr<telegram_api::Message> &message_ptr) {
  switch (message_ptr->get_id()) {
    case telegram_api::messageEmpty::ID:
      return 0;
    case telegram_api::message::ID: {
      auto message = static_cast<const telegram_api::message *>(message_ptr.get());
      return message->date_;
    }
    case telegram_api::messageService::ID: {
      auto message = static_cast<const telegram_api::messageService *>(message_ptr.get());
      return message->date_;
    }
    default:
      UNREACHABLE();
      return 0;
  }
}

void MessagesManager::drop_username(const string &username) {
  inaccessible_resolved_usernames_.erase(clean_username(username));

  auto it = resolved_usernames_.find(clean_username(username));
  if (it == resolved_usernames_.end()) {
    return;
  }

  auto dialog_id = it->second.dialog_id;
  if (have_input_peer(dialog_id, AccessRights::Read)) {
    CHECK(dialog_id.get_type() != DialogType::SecretChat);
    send_get_dialog_query(dialog_id, Auto(), 0, "drop_username");
  }

  resolved_usernames_.erase(it);
}

ScopeNotificationSettings *MessagesManager::get_scope_notification_settings(NotificationSettingsScope scope) {
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

void MessagesManager::on_dialog_title_updated(DialogId dialog_id) {
  auto d = get_dialog(dialog_id);  // called from update_user, must not create the dialog
  if (d != nullptr) {
    update_dialogs_hints(d);
    if (d->is_update_new_chat_sent) {
      send_closure(G()->td(), &Td::send_update,
                   make_tl_object<td_api::updateChatTitle>(dialog_id.get(), get_dialog_title(dialog_id)));
    }
  }
}

Result<DialogDate> MessagesManager::get_dialog_list_last_date(DialogListId dialog_list_id) {
  CHECK(!td_->auth_manager_->is_bot());

  auto *list_ptr = get_dialog_list(dialog_list_id);
  if (list_ptr == nullptr) {
    return Status::Error(400, "Chat list not found");
  }
  return list_ptr->list_last_dialog_date_;
}

Status MessagesManager::add_recently_found_dialog(DialogId dialog_id) {
  if (!have_dialog_force(dialog_id, "add_recently_found_dialog")) {
    return Status::Error(400, "Chat not found");
  }
  recently_found_dialogs_.add_dialog(dialog_id);
  return Status::OK();
}

bool MessagesManager::is_message_unload_enabled() const {
  return G()->parameters().use_message_db || td_->auth_manager_->is_bot();
}

void MessagesManager::start_up() {
  init();
}
}  // namespace td
