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

class MessagesManager::UploadMediaCallback final : public FileManager::UploadCallback {
 public:
  void on_progress(FileId file_id) final {
  }
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_media, file_id, std::move(input_file),
                       nullptr);
  }
  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_media, file_id, nullptr,
                       std::move(input_file));
  }
  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_media_error, file_id, std::move(error));
  }
};

class MessagesManager::UploadThumbnailCallback final : public FileManager::UploadCallback {
 public:
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_thumbnail, file_id, std::move(input_file));
  }
  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_thumbnail, file_id, nullptr);
  }
};

class MessagesManager::UploadDialogPhotoCallback final : public FileManager::UploadCallback {
 public:
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_dialog_photo, file_id,
                       std::move(input_file));
  }
  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_dialog_photo_error, file_id,
                       std::move(error));
  }
};

class MessagesManager::UploadImportedMessagesCallback final : public FileManager::UploadCallback {
 public:
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_imported_messages, file_id,
                       std::move(input_file));
  }
  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_imported_messages_error, file_id,
                       std::move(error));
  }
};

class MessagesManager::UploadImportedMessageAttachmentCallback final : public FileManager::UploadCallback {
 public:
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_imported_message_attachment, file_id,
                       std::move(input_file));
  }
  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_upload_imported_message_attachment_error, file_id,
                       std::move(error));
  }
};

MessagesManager::MessagesManager(Td *td, ActorShared<> parent)
    : recently_found_dialogs_{td, "recently_found", MAX_RECENT_DIALOGS}
    , recently_opened_dialogs_{td, "recently_opened", MAX_RECENT_DIALOGS}
    , td_(td)
    , parent_(std::move(parent)) {
  upload_media_callback_ = std::make_shared<UploadMediaCallback>();
  upload_thumbnail_callback_ = std::make_shared<UploadThumbnailCallback>();
  upload_dialog_photo_callback_ = std::make_shared<UploadDialogPhotoCallback>();
  upload_imported_messages_callback_ = std::make_shared<UploadImportedMessagesCallback>();
  upload_imported_message_attachment_callback_ = std::make_shared<UploadImportedMessageAttachmentCallback>();

  channel_get_difference_timeout_.set_callback(on_channel_get_difference_timeout_callback);
  channel_get_difference_timeout_.set_callback_data(static_cast<void *>(this));

  channel_get_difference_retry_timeout_.set_callback(on_channel_get_difference_timeout_callback);
  channel_get_difference_retry_timeout_.set_callback_data(static_cast<void *>(this));

  pending_message_views_timeout_.set_callback(on_pending_message_views_timeout_callback);
  pending_message_views_timeout_.set_callback_data(static_cast<void *>(this));

  pending_message_live_location_view_timeout_.set_callback(on_pending_message_live_location_view_timeout_callback);
  pending_message_live_location_view_timeout_.set_callback_data(static_cast<void *>(this));

  pending_draft_message_timeout_.set_callback(on_pending_draft_message_timeout_callback);
  pending_draft_message_timeout_.set_callback_data(static_cast<void *>(this));

  pending_read_history_timeout_.set_callback(on_pending_read_history_timeout_callback);
  pending_read_history_timeout_.set_callback_data(static_cast<void *>(this));

  pending_updated_dialog_timeout_.set_callback(on_pending_updated_dialog_timeout_callback);
  pending_updated_dialog_timeout_.set_callback_data(static_cast<void *>(this));

  pending_unload_dialog_timeout_.set_callback(on_pending_unload_dialog_timeout_callback);
  pending_unload_dialog_timeout_.set_callback_data(static_cast<void *>(this));

  dialog_unmute_timeout_.set_callback(on_dialog_unmute_timeout_callback);
  dialog_unmute_timeout_.set_callback_data(static_cast<void *>(this));

  pending_send_dialog_action_timeout_.set_callback(on_pending_send_dialog_action_timeout_callback);
  pending_send_dialog_action_timeout_.set_callback_data(static_cast<void *>(this));

  active_dialog_action_timeout_.set_callback(on_active_dialog_action_timeout_callback);
  active_dialog_action_timeout_.set_callback_data(static_cast<void *>(this));

  update_dialog_online_member_count_timeout_.set_callback(on_update_dialog_online_member_count_timeout_callback);
  update_dialog_online_member_count_timeout_.set_callback_data(static_cast<void *>(this));

  preload_folder_dialog_list_timeout_.set_callback(on_preload_folder_dialog_list_timeout_callback);
  preload_folder_dialog_list_timeout_.set_callback_data(static_cast<void *>(this));

  sequence_dispatcher_ = create_actor<MultiSequenceDispatcher>("multi sequence dispatcher");
}

void MessagesManager::load_secret_thumbnail(FileId thumbnail_file_id) {
  class Callback final : public FileManager::DownloadCallback {
   public:
    explicit Callback(Promise<> download_promise) : download_promise_(std::move(download_promise)) {
    }

    void on_download_ok(FileId file_id) final {
      download_promise_.set_value(Unit());
    }
    void on_download_error(FileId file_id, Status error) final {
      download_promise_.set_error(std::move(error));
    }

   private:
    Promise<> download_promise_;
  };

  auto thumbnail_promise = PromiseCreator::lambda([actor_id = actor_id(this),
                                                   thumbnail_file_id](Result<BufferSlice> r_thumbnail) {
    BufferSlice thumbnail_slice;
    if (r_thumbnail.is_ok()) {
      thumbnail_slice = r_thumbnail.move_as_ok();
    }
    send_closure(actor_id, &MessagesManager::on_load_secret_thumbnail, thumbnail_file_id, std::move(thumbnail_slice));
  });

  auto download_promise = PromiseCreator::lambda(
      [thumbnail_file_id, thumbnail_promise = std::move(thumbnail_promise)](Result<Unit> r_download) mutable {
        if (r_download.is_error()) {
          thumbnail_promise.set_error(r_download.move_as_error());
          return;
        }
        send_closure(G()->file_manager(), &FileManager::get_content, thumbnail_file_id, std::move(thumbnail_promise));
      });

  send_closure(G()->file_manager(), &FileManager::download, thumbnail_file_id,
               std::make_shared<Callback>(std::move(download_promise)), 1, -1, -1);
}

void MessagesManager::on_upload_media(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file,
                                      tl_object_ptr<telegram_api::InputEncryptedFile> input_encrypted_file) {
  LOG(INFO) << "File " << file_id << " has been uploaded";

  auto it = being_uploaded_files_.find(file_id);
  if (it == being_uploaded_files_.end()) {
    // callback may be called just before the file upload was canceled
    return;
  }

  auto full_message_id = it->second.first;
  auto thumbnail_file_id = it->second.second;

  being_uploaded_files_.erase(it);

  Message *m = get_message(full_message_id);
  if (m == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel, do not need to send or edit it
    // file upload should be already canceled in cancel_send_message_query, it shouldn't happen
    LOG(ERROR) << "Message with a media has already been deleted";
    return;
  }

  bool is_edit = m->message_id.is_any_server();
  auto dialog_id = full_message_id.get_dialog_id();
  auto can_send_status = can_send_message(dialog_id);
  if (!is_edit && can_send_status.is_error()) {
    // user has left the chat during upload of the file or lost their privileges
    LOG(INFO) << "Can't send a message to " << dialog_id << ": " << can_send_status.error();

    fail_send_message(full_message_id, can_send_status.move_as_error());
    return;
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
      if (input_file && thumbnail_file_id.is_valid()) {
        // TODO: download thumbnail if needed (like in secret chats)
        LOG(INFO) << "Ask to upload thumbnail " << thumbnail_file_id;
        CHECK(being_uploaded_thumbnails_.find(thumbnail_file_id) == being_uploaded_thumbnails_.end());
        being_uploaded_thumbnails_[thumbnail_file_id] = {full_message_id, file_id, std::move(input_file)};
        td_->file_manager_->upload(thumbnail_file_id, upload_thumbnail_callback_, 32, m->message_id.get());
      } else {
        do_send_media(dialog_id, m, file_id, thumbnail_file_id, std::move(input_file), nullptr);
      }
      break;
    case DialogType::SecretChat:
      if (thumbnail_file_id.is_valid()) {
        LOG(INFO) << "Ask to load thumbnail " << thumbnail_file_id;
        CHECK(being_loaded_secret_thumbnails_.find(thumbnail_file_id) == being_loaded_secret_thumbnails_.end());
        being_loaded_secret_thumbnails_[thumbnail_file_id] = {full_message_id, file_id,
                                                              std::move(input_encrypted_file)};

        load_secret_thumbnail(thumbnail_file_id);
      } else {
        do_send_secret_media(dialog_id, m, file_id, thumbnail_file_id, std::move(input_encrypted_file), BufferSlice());
      }
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
      break;
  }
}

void MessagesManager::do_send_message(DialogId dialog_id, const Message *m, vector<int> bad_parts) {
  bool is_edit = m->message_id.is_any_server();
  LOG(INFO) << "Do " << (is_edit ? "edit" : "send") << ' ' << FullMessageId(dialog_id, m->message_id);
  bool is_secret = dialog_id.get_type() == DialogType::SecretChat;

  if (m->media_album_id != 0 && bad_parts.empty() && !is_secret && !is_edit) {
    auto &request = pending_message_group_sends_[m->media_album_id];
    request.dialog_id = dialog_id;
    request.message_ids.push_back(m->message_id);
    request.is_finished.push_back(false);

    request.results.push_back(Status::OK());
  }

  auto content = is_edit ? m->edited_content.get() : m->content.get();
  CHECK(content != nullptr);
  auto content_type = content->get_type();
  if (content_type == MessageContentType::Text) {
    CHECK(!is_edit);
    send_closure_later(actor_id(this), &MessagesManager::on_text_message_ready_to_send, dialog_id, m->message_id);
    return;
  }

  FileId file_id = get_message_content_any_file_id(content);  // any_file_id, because it could be a photo sent by ID
  FileView file_view = td_->file_manager_->get_file_view(file_id);
  FileId thumbnail_file_id = get_message_content_thumbnail_file_id(content, td_);
  LOG(DEBUG) << "Need to send file " << file_id << " with thumbnail " << thumbnail_file_id;
  if (is_secret) {
    CHECK(!is_edit);
    auto secret_input_media = get_secret_input_media(content, td_, nullptr, BufferSlice());
    if (secret_input_media.empty()) {
      LOG(INFO) << "Ask to upload encrypted file " << file_id;
      CHECK(file_view.is_encrypted_secret());
      CHECK(file_id.is_valid());
      CHECK(being_uploaded_files_.find(file_id) == being_uploaded_files_.end());
      being_uploaded_files_[file_id] = {FullMessageId(dialog_id, m->message_id), thumbnail_file_id};
      // need to call resume_upload synchronously to make upload process consistent with being_uploaded_files_
      td_->file_manager_->resume_upload(file_id, std::move(bad_parts), upload_media_callback_, 1, m->message_id.get());
    } else {
      on_secret_message_media_uploaded(dialog_id, m, std::move(secret_input_media), file_id, thumbnail_file_id);
    }
  } else {
    auto input_media =
        get_input_media(content, td_, m->ttl, m->send_emoji, td_->auth_manager_->is_bot() && bad_parts.empty());
    if (input_media == nullptr) {
      if (content_type == MessageContentType::Game || content_type == MessageContentType::Poll) {
        return;
      }
      if (content_type == MessageContentType::Photo) {
        thumbnail_file_id = FileId();
      }

      LOG(INFO) << "Ask to upload file " << file_id << " with bad parts " << bad_parts;
      CHECK(file_id.is_valid());
      CHECK(being_uploaded_files_.find(file_id) == being_uploaded_files_.end());
      being_uploaded_files_[file_id] = {FullMessageId(dialog_id, m->message_id), thumbnail_file_id};
      // need to call resume_upload synchronously to make upload process consistent with being_uploaded_files_
      td_->file_manager_->resume_upload(file_id, std::move(bad_parts), upload_media_callback_, 1, m->message_id.get());
    } else {
      on_message_media_uploaded(dialog_id, m, std::move(input_media), file_id, thumbnail_file_id);
    }
  }
}

void MessagesManager::upload_imported_messages(DialogId dialog_id, FileId file_id, vector<FileId> attached_file_ids,
                                               bool is_reupload, Promise<Unit> &&promise, vector<int> bad_parts) {
  CHECK(file_id.is_valid());
  LOG(INFO) << "Ask to upload imported messages file " << file_id;
  CHECK(being_uploaded_imported_messages_.find(file_id) == being_uploaded_imported_messages_.end());
  being_uploaded_imported_messages_.emplace(
      file_id, td::make_unique<UploadedImportedMessagesInfo>(dialog_id, std::move(attached_file_ids), is_reupload,
                                                             std::move(promise)));
  // TODO use force_reupload if is_reupload
  td_->file_manager_->resume_upload(file_id, std::move(bad_parts), upload_imported_messages_callback_, 1, 0, false,
                                    true);
}

void MessagesManager::upload_imported_message_attachment(DialogId dialog_id, int64 import_id, FileId file_id,
                                                         bool is_reupload, Promise<Unit> &&promise,
                                                         vector<int> bad_parts) {
  CHECK(file_id.is_valid());
  LOG(INFO) << "Ask to upload improted message attached file " << file_id;
  CHECK(being_uploaded_imported_message_attachments_.find(file_id) ==
        being_uploaded_imported_message_attachments_.end());
  being_uploaded_imported_message_attachments_.emplace(
      file_id,
      td::make_unique<UploadedImportedMessageAttachmentInfo>(dialog_id, import_id, is_reupload, std::move(promise)));
  // TODO use force_reupload if is_reupload
  td_->file_manager_->resume_upload(file_id, std::move(bad_parts), upload_imported_message_attachment_callback_, 1, 0,
                                    false, true);
}

void MessagesManager::upload_dialog_photo(DialogId dialog_id, FileId file_id, bool is_animation,
                                          double main_frame_timestamp, bool is_reupload, Promise<Unit> &&promise,
                                          vector<int> bad_parts) {
  CHECK(file_id.is_valid());
  LOG(INFO) << "Ask to upload chat photo " << file_id;
  CHECK(being_uploaded_dialog_photos_.find(file_id) == being_uploaded_dialog_photos_.end());
  being_uploaded_dialog_photos_.emplace(
      file_id, UploadedDialogPhotoInfo{dialog_id, main_frame_timestamp, is_animation, is_reupload, std::move(promise)});
  // TODO use force_reupload if is_reupload
  td_->file_manager_->resume_upload(file_id, std::move(bad_parts), upload_dialog_photo_callback_, 32, 0);
}

class UnpinAllMessagesQuery final : public Td::ResultHandler {
  Promise<AffectedHistory> promise_;
  DialogId dialog_id_;

 public:
  explicit UnpinAllMessagesQuery(Promise<AffectedHistory> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id_, AccessRights::Write);
    if (input_peer == nullptr) {
      LOG(INFO) << "Can't unpin all messages in " << dialog_id_;
      return on_error(Status::Error(400, "Can't unpin all messages"));
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_unpinAllMessages(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_unpinAllMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(AffectedHistory(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "UnpinAllMessagesQuery");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::unpin_all_dialog_messages_on_server(DialogId dialog_id, uint64 log_event_id,
                                                          Promise<Unit> &&promise) {
  if (log_event_id == 0 && G()->parameters().use_message_db) {
    log_event_id = save_unpin_all_dialog_messages_on_server_log_event(dialog_id);
  }

  AffectedHistoryQuery query = [td = td_](DialogId dialog_id, Promise<AffectedHistory> &&query_promise) {
    td->create_handler<UnpinAllMessagesQuery>(std::move(query_promise))->send(dialog_id);
  };
  run_affected_history_query_until_complete(dialog_id, std::move(query), true,
                                            get_erase_log_event_promise(log_event_id, std::move(promise)));
}

void MessagesManager::delete_dialog_messages(DialogId dialog_id, const vector<MessageId> &message_ids,
                                             bool from_updates, bool skip_update_for_not_found_messages,
                                             const char *source) {
  Dialog *d = get_dialog_force(dialog_id, "delete_dialog_messages");
  if (d == nullptr) {
    LOG(INFO) << "Ignore deleteChannelMessages for unknown " << dialog_id << " from " << source;
    CHECK(from_updates);
    CHECK(dialog_id.get_type() == DialogType::Channel);
    return;
  }

  vector<int64> deleted_message_ids;
  bool need_update_dialog_pos = false;
  for (auto message_id : message_ids) {
    CHECK(!message_id.is_scheduled());
    if (from_updates) {
      if (!message_id.is_valid() || (!message_id.is_server() && dialog_id.get_type() != DialogType::SecretChat)) {
        LOG(ERROR) << "Tried to delete " << message_id << " in " << dialog_id << " from " << source;
        continue;
      }
    } else {
      CHECK(message_id.is_valid());
    }

    bool was_already_deleted = d->deleted_message_ids.count(message_id) != 0;
    auto message = delete_message(d, message_id, true, &need_update_dialog_pos, source);
    if (message == nullptr) {
      if (!skip_update_for_not_found_messages && !was_already_deleted) {
        deleted_message_ids.push_back(message_id.get());
      }
    } else {
      deleted_message_ids.push_back(message->message_id.get());
    }
  }
  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, source);
  }
  send_update_delete_messages(dialog_id, std::move(deleted_message_ids), true, false);
}

// must not call get_dialog_filter
bool MessagesManager::do_update_list_last_dialog_date(DialogList &list, const vector<FolderId> &folder_ids) const {
  CHECK(!td_->auth_manager_->is_bot());
  auto new_last_dialog_date = list.last_pinned_dialog_date_;
  for (auto folder_id : folder_ids) {
    const auto &folder = *get_dialog_folder(folder_id);
    if (folder.folder_last_dialog_date_ < new_last_dialog_date) {
      new_last_dialog_date = folder.folder_last_dialog_date_;
    }
  }

  if (list.list_last_dialog_date_ != new_last_dialog_date) {
    auto old_last_dialog_date = list.list_last_dialog_date_;
    LOG(INFO) << "Update last dialog date in " << list.dialog_list_id << " from " << old_last_dialog_date << " to "
              << new_last_dialog_date;
    LOG_CHECK(old_last_dialog_date < new_last_dialog_date)
        << list.dialog_list_id << " " << old_last_dialog_date << " " << new_last_dialog_date << " "
        << get_dialog_list_folder_ids(list) << " " << list.last_pinned_dialog_date_ << " "
        << get_dialog_folder(FolderId::main())->folder_last_dialog_date_ << " "
        << get_dialog_folder(FolderId::archive())->folder_last_dialog_date_ << " " << list.load_list_queries_.size()
        << " " << list.pinned_dialogs_;
    list.list_last_dialog_date_ = new_last_dialog_date;
    return true;
  }
  return false;
}

Status MessagesManager::can_import_messages(DialogId dialog_id) {
  if (!have_dialog_force(dialog_id, "can_import_messages")) {
    return Status::Error(400, "Chat not found");
  }

  TRY_STATUS(can_send_message(dialog_id));

  switch (dialog_id.get_type()) {
    case DialogType::User:
      if (!td_->contacts_manager_->is_user_contact(dialog_id.get_user_id(), true)) {
        return Status::Error(400, "User must be a mutual contact");
      }
      break;
    case DialogType::Chat:
      return Status::Error(400, "Basic groups must be updagraded to supergroups first");
    case DialogType::Channel:
      if (is_broadcast_channel(dialog_id)) {
        return Status::Error(400, "Can't import messages to channels");
      }
      if (!td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id()).can_change_info_and_settings()) {
        return Status::Error(400, "Not enough rights to import messages");
      }
      break;
    case DialogType::SecretChat:
      return Status::Error(400, "Can't import messages to secret chats");
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  return Status::OK();
}

bool MessagesManager::need_synchronize_dialog_filters() const {
  CHECK(!td_->auth_manager_->is_bot());
  size_t server_dialog_filter_count = 0;
  vector<DialogFilterId> dialog_filter_ids;
  for (const auto &dialog_filter : dialog_filters_) {
    if (dialog_filter->is_empty(true)) {
      continue;
    }

    server_dialog_filter_count++;
    auto server_dialog_filter = get_server_dialog_filter(dialog_filter->dialog_filter_id);
    if (server_dialog_filter == nullptr || !DialogFilter::are_equivalent(*server_dialog_filter, *dialog_filter)) {
      // need update dialog filter on server
      return true;
    }
    dialog_filter_ids.push_back(dialog_filter->dialog_filter_id);
  }
  if (server_dialog_filter_count != server_dialog_filters_.size()) {
    // need delete dialog filter on server
    return true;
  }
  if (dialog_filter_ids != get_dialog_filter_ids(server_dialog_filters_)) {
    // need reorder dialog filters on server
    return true;
  }
  return false;
}

void MessagesManager::on_get_dialog_query_finished(DialogId dialog_id, Status &&status) {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Finished getting " << dialog_id << " with result " << status;
  auto it = get_dialog_queries_.find(dialog_id);
  CHECK(it != get_dialog_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  get_dialog_queries_.erase(it);

  auto log_event_it = get_dialog_query_log_event_id_.find(dialog_id);
  if (log_event_it != get_dialog_query_log_event_id_.end()) {
    if (!G()->close_flag()) {
      binlog_erase(G()->td_db()->get_binlog(), log_event_it->second);
    }
    get_dialog_query_log_event_id_.erase(log_event_it);
  }

  for (auto &promise : promises) {
    if (status.is_ok()) {
      promise.set_value(Unit());
    } else {
      promise.set_error(status.clone());
    }
  }
}

int32 MessagesManager::get_dialog_pending_notification_count(const Dialog *d, bool from_mentions) const {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(d != nullptr);
  if (from_mentions) {
    bool has_pinned_message = d->pinned_message_notification_message_id.is_valid() &&
                              d->pinned_message_notification_message_id <= d->last_new_message_id;
    return d->unread_mention_count + static_cast<int32>(has_pinned_message);
  } else {
    if (d->new_secret_chat_notification_id.is_valid()) {
      return 1;
    }
    if (is_dialog_muted(d)) {
      return narrow_cast<int32>(d->pending_new_message_notifications.size());  // usually 0
    }

    return d->server_unread_count + d->local_unread_count;
  }
}

void MessagesManager::on_update_pinned_dialogs(FolderId folder_id) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  // TODO log event + delete_log_event_promise

  auto *list = get_dialog_list(DialogListId(folder_id));
  if (list == nullptr || !list->are_pinned_dialogs_inited_) {
    return;
  }
  // preload all pinned dialogs
  int32 limit = narrow_cast<int32>(list->pinned_dialogs_.size()) +
                (folder_id == FolderId::main() && sponsored_dialog_id_.is_valid() ? 1 : 0);
  get_dialogs_from_list(DialogListId(folder_id), limit, Auto());
  reload_pinned_dialogs(DialogListId(folder_id), Auto());
}

void MessagesManager::read_secret_chat_outbox_inner(DialogId dialog_id, int32 up_to_date, int32 read_date) {
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  auto end = MessagesConstIterator(d, MessageId::max());
  while (*end && (*end)->date > up_to_date) {
    --end;
  }
  if (!*end) {
    LOG(INFO) << "Ignore read_secret_chat_outbox in " << dialog_id << " at " << up_to_date
              << ": no messages with such date are known";
    return;
  }
  auto max_message_id = (*end)->message_id;
  read_history_outbox(dialog_id, max_message_id, read_date);
}

bool MessagesManager::need_dialog_in_list(const Dialog *d, const DialogList &list) const {
  CHECK(!td_->auth_manager_->is_bot());
  if (d->order == DEFAULT_ORDER) {
    return false;
  }
  if (list.dialog_list_id.is_folder()) {
    return d->folder_id == list.dialog_list_id.get_folder_id();
  }
  if (list.dialog_list_id.is_filter()) {
    auto dialog_filter_id = list.dialog_list_id.get_filter_id();
    return need_dialog_in_filter(d, get_dialog_filter(dialog_filter_id));
  }
  UNREACHABLE();
  return false;
}

bool MessagesManager::is_deleted_secret_chat(const Dialog *d) const {
  if (d == nullptr) {
    return true;
  }
  if (d->dialog_id.get_type() != DialogType::SecretChat) {
    return false;
  }

  if (d->order != DEFAULT_ORDER || d->messages != nullptr) {
    return false;
  }

  auto state = td_->contacts_manager_->get_secret_chat_state(d->dialog_id.get_secret_chat_id());
  if (state != SecretChatState::Closed) {
    return false;
  }

  return true;
}

MessagesManager::Message *MessagesManager::on_get_message_from_database(Dialog *d,
                                                                        const MessagesDbDialogMessage &message,
                                                                        bool is_scheduled, const char *source) {
  return on_get_message_from_database(d, d->dialog_id, message.message_id, message.data, is_scheduled, source);
}

void MessagesManager::on_updated_dialog_notification_settings(DialogId dialog_id, uint64 generation) {
  CHECK(!td_->auth_manager_->is_bot());
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  delete_log_event(d->save_notification_settings_log_event_id, generation, "notification settings");
}

DialogFilter *MessagesManager::get_dialog_filter(DialogFilterId dialog_filter_id) {
  CHECK(!disable_get_dialog_filter_);
  for (auto &filter : dialog_filters_) {
    if (filter->dialog_filter_id == dialog_filter_id) {
      return filter.get();
    }
  }
  return nullptr;
}

MessageId MessagesManager::get_next_local_message_id(Dialog *d) {
  return get_next_message_id(d, MessageType::Local);
}

void MessagesManager::remove_sponsored_dialog() {
  set_sponsored_dialog(DialogId(), DialogSource());
}
}  // namespace td
