//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/Td.h"

#include "td/telegram/Account.h"


#include "td/telegram/AuthManager.h"
#include "td/telegram/AutoDownloadSettings.h"
#include "td/telegram/BackgroundId.h"

#include "td/telegram/BackgroundType.h"
#include "td/telegram/BotCommand.h"
#include "td/telegram/CallbackQueriesManager.h"
#include "td/telegram/CallId.h"
#include "td/telegram/CallManager.h"
#include "td/telegram/ChannelId.h"
#include "td/telegram/ChatId.h"
#include "td/telegram/ConfigManager.h"



#include "td/telegram/DeviceTokenManager.h"
#include "td/telegram/DialogAction.h"
#include "td/telegram/DialogEventLog.h"
#include "td/telegram/DialogFilter.h"
#include "td/telegram/DialogFilterId.h"
#include "td/telegram/DialogId.h"
#include "td/telegram/DialogListId.h"
#include "td/telegram/DialogLocation.h"
#include "td/telegram/DialogParticipant.h"
#include "td/telegram/DialogSource.h"

#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileGcParameters.h"
#include "td/telegram/files/FileId.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"

#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"



#include "td/telegram/JsonValue.h"


#include "td/telegram/Location.h"
#include "td/telegram/Logging.h"

#include "td/telegram/MessageEntity.h"
#include "td/telegram/MessageId.h"
#include "td/telegram/MessageLinkInfo.h"
#include "td/telegram/MessageSearchFilter.h"
#include "td/telegram/MessageSender.h"

#include "td/telegram/MessageThreadInfo.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/ConnectionCreator.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/net/MtprotoHeader.h"
#include "td/telegram/net/NetQuery.h"
#include "td/telegram/net/NetQueryDelayer.h"
#include "td/telegram/net/NetQueryDispatcher.h"
#include "td/telegram/net/NetStatsManager.h"
#include "td/telegram/net/NetType.h"
#include "td/telegram/net/Proxy.h"
#include "td/telegram/net/PublicRsaKeyShared.h"
#include "td/telegram/net/TempAuthKeyWatchdog.h"
#include "td/telegram/NotificationGroupId.h"
#include "td/telegram/NotificationId.h"

#include "td/telegram/NotificationSettings.h"

#include "td/telegram/PasswordManager.h"
#include "td/telegram/Payments.h"

#include "td/telegram/Photo.h"
#include "td/telegram/PhotoSizeSource.h"

#include "td/telegram/PrivacyManager.h"

#include "td/telegram/ReportReason.h"
#include "td/telegram/RequestActor.h"
#include "td/telegram/SecretChatId.h"

#include "td/telegram/SecureManager.h"
#include "td/telegram/SecureValue.h"

#include "td/telegram/StateManager.h"
#include "td/telegram/StickerSetId.h"

#include "td/telegram/StorageManager.h"
#include "td/telegram/SuggestedAction.h"
#include "td/telegram/td_api.hpp"

#include "td/telegram/telegram_api.hpp"




#include "td/telegram/Version.h"
#include "td/telegram/VideoNotesManager.h"
#include "td/telegram/VideosManager.h"
#include "td/telegram/VoiceNotesManager.h"



#include "td/db/binlog/BinlogEvent.h"

#include "td/mtproto/DhCallback.h"
#include "td/mtproto/Handshake.h"
#include "td/mtproto/HandshakeActor.h"
#include "td/mtproto/RawConnection.h"
#include "td/mtproto/RSA.h"
#include "td/mtproto/TransportType.h"

#include "td/actor/actor.h"
#include "td/actor/PromiseFuture.h"

#include "td/utils/algorithm.h"
#include "td/utils/buffer.h"
#include "td/utils/filesystem.h"
#include "td/utils/format.h"
#include "td/utils/MimeType.h"
#include "td/utils/misc.h"
#include "td/utils/PathView.h"
#include "td/utils/port/Clocks.h"
#include "td/utils/port/IPAddress.h"
#include "td/utils/port/path.h"
#include "td/utils/port/SocketFd.h"
#include "td/utils/port/uname.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/Status.h"
#include "td/utils/Timer.h"
#include "td/utils/tl_parsers.h"
#include "td/utils/utf8.h"



#include <type_traits>

namespace td {

#define CLEAN_INPUT_STRING(field_name)                                  \
  if (!clean_input_string(field_name)) {                                \
    return send_error_raw(id, 400, "Strings must be encoded in UTF-8"); \
  }
#define CHECK_IS_BOT()                                              \
  if (!auth_manager_->is_bot()) {                                   \
    return send_error_raw(id, 400, "Only bots can use the method"); \
  }
#define CHECK_IS_USER()                                                     \
  if (auth_manager_->is_bot()) {                                            \
    return send_error_raw(id, 400, "The method is not available for bots"); \
  }

#define CREATE_NO_ARGS_REQUEST(name)                                       \
  auto slot_id = request_actors_.create(ActorOwn<>(), RequestActorIdType); \
  inc_request_actor_refcnt();                                              \
  *request_actors_.get(slot_id) = create_actor<name>(#name, actor_shared(this, slot_id), id);
#define CREATE_REQUEST(name, ...)                                          \
  auto slot_id = request_actors_.create(ActorOwn<>(), RequestActorIdType); \
  inc_request_actor_refcnt();                                              \
  *request_actors_.get(slot_id) = create_actor<name>(#name, actor_shared(this, slot_id), id, __VA_ARGS__);
#define CREATE_REQUEST_PROMISE() auto promise = create_request_promise<std::decay_t<decltype(request)>::ReturnType>(id)
#define CREATE_OK_REQUEST_PROMISE()                                                                                    \
  static_assert(std::is_same<std::decay_t<decltype(request)>::ReturnType, td_api::object_ptr<td_api::ok>>::value, ""); \
  auto promise = create_ok_request_promise(id)

}  // namespace td
namespace td {

class Td::DownloadFileCallback final : public FileManager::DownloadCallback {
 public:
  void on_progress(FileId file_id) final {
  }

  void on_download_ok(FileId file_id) final {
    send_closure(G()->td(), &Td::on_file_download_finished, file_id);
  }

  void on_download_error(FileId file_id, Status error) final {
    send_closure(G()->td(), &Td::on_file_download_finished, file_id);
  }
};

class Td::UploadFileCallback final : public FileManager::UploadCallback {
 public:
  void on_progress(FileId file_id) final {
  }

  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    // cancel file upload of the file to allow next upload with the same file to succeed
    send_closure(G()->file_manager(), &FileManager::cancel_upload, file_id);
  }

  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    // cancel file upload of the file to allow next upload with the same file to succeed
    send_closure(G()->file_manager(), &FileManager::cancel_upload, file_id);
  }

  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    // cancel file upload of the file to allow next upload with the same file to succeed
    send_closure(G()->file_manager(), &FileManager::cancel_upload, file_id);
  }

  void on_upload_error(FileId file_id, Status error) final {
  }
};

void Td::init_file_manager() {
  VLOG(td_init) << "Create FileManager";
  download_file_callback_ = std::make_shared<DownloadFileCallback>();
  upload_file_callback_ = std::make_shared<UploadFileCallback>();

  class FileManagerContext final : public FileManager::Context {
   public:
    explicit FileManagerContext(Td *td) : td_(td) {
    }

    void on_new_file(int64 size, int64 real_size, int32 cnt) final {
      send_closure(G()->storage_manager(), &StorageManager::on_new_file, size, real_size, cnt);
    }

    void on_file_updated(FileId file_id) final {
      send_closure(G()->td(), &Td::send_update,
                   make_tl_object<td_api::updateFile>(td_->file_manager_->get_file_object(file_id)));
    }

    bool add_file_source(FileId file_id, FileSourceId file_source_id) final {
      return td_->file_reference_manager_->add_file_source(file_id, file_source_id);
    }

    bool remove_file_source(FileId file_id, FileSourceId file_source_id) final {
      return td_->file_reference_manager_->remove_file_source(file_id, file_source_id);
    }

    void on_merge_files(FileId to_file_id, FileId from_file_id) final {
      td_->file_reference_manager_->merge(to_file_id, from_file_id);
    }

    vector<FileSourceId> get_some_file_sources(FileId file_id) final {
      return td_->file_reference_manager_->get_some_file_sources(file_id);
    }

    void repair_file_reference(FileId file_id, Promise<Unit> promise) final {
      send_closure(G()->file_reference_manager(), &FileReferenceManager::repair_file_reference, file_id,
                   std::move(promise));
    }

    void reload_photo(PhotoSizeSource source, Promise<Unit> promise) final {
      FileReferenceManager::reload_photo(std::move(source), std::move(promise));
    }

    ActorShared<> create_reference() final {
      return td_->create_reference();
    }

   private:
    Td *td_;
  };

  file_manager_ = make_unique<FileManager>(make_unique<FileManagerContext>(this));
  file_manager_actor_ = register_actor("FileManager", file_manager_.get());
  file_manager_->init_actor();
  G()->set_file_manager(file_manager_actor_.get());

  file_reference_manager_ = make_unique<FileReferenceManager>();
  file_reference_manager_actor_ = register_actor("FileReferenceManager", file_reference_manager_.get());
  G()->set_file_reference_manager(file_reference_manager_actor_.get());
}

void Td::on_request(uint64 id, const td_api::downloadFile &request) {
  auto priority = request.priority_;
  if (!(1 <= priority && priority <= 32)) {
    return send_error_raw(id, 400, "Download priority must be in [1;32] range");
  }
  auto offset = request.offset_;
  if (offset < 0) {
    return send_error_raw(id, 400, "Download offset must be non-negative");
  }
  auto limit = request.limit_;
  if (limit < 0) {
    return send_error_raw(id, 400, "Download limit must be non-negative");
  }

  FileId file_id(request.file_id_, 0);
  auto file_view = file_manager_->get_file_view(file_id);
  if (file_view.empty()) {
    return send_error_raw(id, 400, "Invalid file identifier");
  }

  auto info_it = pending_file_downloads_.find(file_id);
  DownloadInfo *info = info_it == pending_file_downloads_.end() ? nullptr : &info_it->second;
  if (info != nullptr && (offset != info->offset || limit != info->limit)) {
    // we can't have two pending requests with different offset and limit, so cancel all previous requests
    for (auto request_id : info->request_ids) {
      send_closure(actor_id(this), &Td::send_error, request_id,
                   Status::Error(200, "Canceled by another downloadFile request"));
    }
    info->request_ids.clear();
  }
  if (request.synchronous_) {
    if (info == nullptr) {
      info = &pending_file_downloads_[file_id];
    }
    info->offset = offset;
    info->limit = limit;
    info->request_ids.push_back(id);
  }
  file_manager_->download(file_id, download_file_callback_, priority, offset, limit);
  if (!request.synchronous_) {
    send_closure(actor_id(this), &Td::send_result, id, file_manager_->get_file_object(file_id, false));
  }
}

void Td::on_request(uint64 id, td_api::uploadFile &request) {
  auto priority = request.priority_;
  if (!(1 <= priority && priority <= 32)) {
    return send_error_raw(id, 400, "Upload priority must be in [1;32] range");
  }

  auto file_type = request.file_type_ == nullptr ? FileType::Temp : get_file_type(*request.file_type_);
  bool is_secret = file_type == FileType::Encrypted || file_type == FileType::EncryptedThumbnail;
  bool is_secure = file_type == FileType::Secure;
  auto r_file_id = file_manager_->get_input_file_id(file_type, request.file_, DialogId(), false, is_secret,
                                                    !is_secure && !is_secret, is_secure);
  if (r_file_id.is_error()) {
    return send_error_raw(id, 400, r_file_id.error().message());
  }
  auto file_id = r_file_id.ok();
  auto upload_file_id = file_manager_->dup_file_id(file_id);

  file_manager_->upload(upload_file_id, upload_file_callback_, priority, 0);

  send_closure(actor_id(this), &Td::send_result, id, file_manager_->get_file_object(upload_file_id, false));
}
}  // namespace td
