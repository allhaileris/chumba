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

#include "td/telegram/ContactsManager.h"

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
#include "td/telegram/MessagesManager.h"
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
#include "td/telegram/StickersManager.h"
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
Status Td::fix_parameters(TdParameters &parameters) {
  if (parameters.database_directory.empty()) {
    VLOG(td_init) << "Fix database_directory";
    parameters.database_directory = ".";
  }
  if (parameters.files_directory.empty()) {
    VLOG(td_init) << "Fix files_directory";
    parameters.files_directory = parameters.database_directory;
  }
  if (parameters.use_message_db && !parameters.use_chat_info_db) {
    VLOG(td_init) << "Fix use_chat_info_db";
    parameters.use_chat_info_db = true;
  }
  if (parameters.use_chat_info_db && !parameters.use_file_db) {
    VLOG(td_init) << "Fix use_file_db";
    parameters.use_file_db = true;
  }
  if (parameters.api_id <= 0) {
    VLOG(td_init) << "Invalid api_id";
    return Status::Error(400, "Valid api_id must be provided. Can be obtained at https://my.telegram.org");
  }
  if (parameters.api_hash.empty()) {
    VLOG(td_init) << "Invalid api_hash";
    return Status::Error(400, "Valid api_hash must be provided. Can be obtained at https://my.telegram.org");
  }

  auto prepare_dir = [](string dir) -> Result<string> {
    CHECK(!dir.empty());
    if (dir.back() != TD_DIR_SLASH) {
      dir += TD_DIR_SLASH;
    }
    TRY_STATUS(mkpath(dir, 0750));
    TRY_RESULT(real_dir, realpath(dir, true));
    if (dir.back() != TD_DIR_SLASH) {
      dir += TD_DIR_SLASH;
    }
    return real_dir;
  };

  auto r_database_directory = prepare_dir(parameters.database_directory);
  if (r_database_directory.is_error()) {
    VLOG(td_init) << "Invalid database_directory";
    return Status::Error(400, PSLICE() << "Can't init database in the directory \"" << parameters.database_directory
                                       << "\": " << r_database_directory.error());
  }
  parameters.database_directory = r_database_directory.move_as_ok();
  auto r_files_directory = prepare_dir(parameters.files_directory);
  if (r_files_directory.is_error()) {
    VLOG(td_init) << "Invalid files_directory";
    return Status::Error(400, PSLICE() << "Can't init files directory \"" << parameters.files_directory
                                       << "\": " << r_files_directory.error());
  }
  parameters.files_directory = r_files_directory.move_as_ok();

  return Status::OK();
}

class GetMessagesRequest final : public RequestOnceActor {
  DialogId dialog_id_;
  vector<MessageId> message_ids_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->get_messages(dialog_id_, message_ids_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_messages_object(-1, dialog_id_, message_ids_, false, "GetMessagesRequest"));
  }

 public:
  GetMessagesRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, const vector<int64> &message_ids)
      : RequestOnceActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , message_ids_(MessagesManager::get_message_ids(message_ids)) {
  }
};

void Td::on_request(uint64 id, const td_api::getMessages &request) {
  CREATE_REQUEST(GetMessagesRequest, request.chat_id_, request.message_ids_);
}

void Td::on_request(uint64 id, const td_api::getMapThumbnailFile &request) {
  DialogId dialog_id(request.chat_id_);
  if (!messages_manager_->have_dialog_force(dialog_id, "getMapThumbnailFile")) {
    dialog_id = DialogId();
  }

  auto r_file_id = file_manager_->get_map_thumbnail_file_id(Location(request.location_), request.zoom_, request.width_,
                                                            request.height_, request.scale_, dialog_id);
  if (r_file_id.is_error()) {
    send_closure(actor_id(this), &Td::send_error, id, r_file_id.move_as_error());
  } else {
    send_closure(actor_id(this), &Td::send_result, id, file_manager_->get_file_object(r_file_id.ok()));
  }
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getChatFilterDefaultIconName &request) {
  if (request.filter_ == nullptr) {
    return make_error(400, "Chat filter must be non-empty");
  }
  if (!check_utf8(request.filter_->title_)) {
    return make_error(400, "Chat filter title must be encoded in UTF-8");
  }
  if (!check_utf8(request.filter_->icon_name_)) {
    return make_error(400, "Chat filter icon name must be encoded in UTF-8");
  }
  return td_api::make_object<td_api::text>(DialogFilter::get_default_icon_name(request.filter_.get()));
}

DbKey Td::as_db_key(string key) {
  // Database will still be effectively not encrypted, but
  // 1. SQLite database will be protected from corruption, because that's how sqlcipher works
  // 2. security through obscurity
  // 3. no need for reencryption of SQLite database
  if (key.empty()) {
    return DbKey::raw_key("cucumber");
  }
  return DbKey::raw_key(std::move(key));
}

void Td::hangup_shared() {
  auto token = get_link_token();
  auto type = Container<int>::type_from_id(token);

  if (type == RequestActorIdType) {
    request_actors_.erase(token);
    dec_request_actor_refcnt();
  } else if (type == ActorIdType) {
    dec_actor_refcnt();
  } else {
    LOG(FATAL) << "Unknown hangup_shared of type " << type;
  }
}

void Td::on_request(uint64 id, td_api::reorderInstalledStickerSets &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  stickers_manager_->reorder_installed_sticker_sets(
      request.is_masks_, StickersManager::convert_sticker_set_ids(request.sticker_set_ids_), std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getLogStream &request) {
  auto result = Logging::get_current_stream();
  if (result.is_ok()) {
    return result.move_as_ok();
  } else {
    return make_error(400, result.error().message());
  }
}

void Td::on_request(uint64 id, const td_api::getChatInviteLinkCounts &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_dialog_invite_link_counts(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::enableProxy &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->connection_creator(), &ConnectionCreator::enable_proxy, request.proxy_id_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getChatFilterDefaultIconName &request) {
  UNREACHABLE();
}

void Td::on_request(uint64 id, const td_api::getFileMimeType &request) {
  UNREACHABLE();
}
}  // namespace td
