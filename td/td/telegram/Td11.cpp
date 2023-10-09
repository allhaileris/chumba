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
#include "td/telegram/CountryInfoManager.h"
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

#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"


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
#include "td/telegram/NotificationManager.h"
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
#include "td/telegram/TdDb.h"
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

Status Td::set_parameters(td_api::object_ptr<td_api::tdlibParameters> parameters) {
  VLOG(td_init) << "Begin to set TDLib parameters";
  if (parameters == nullptr) {
    VLOG(td_init) << "Empty parameters";
    return Status::Error(400, "Parameters aren't specified");
  }

  if (!clean_input_string(parameters->api_hash_) && !clean_input_string(parameters->system_language_code_) &&
      !clean_input_string(parameters->device_model_) && !clean_input_string(parameters->system_version_) &&
      !clean_input_string(parameters->application_version_)) {
    VLOG(td_init) << "Wrong string encoding";
    return Status::Error(400, "Strings must be encoded in UTF-8");
  }

  parameters_.use_test_dc = parameters->use_test_dc_;
  parameters_.database_directory = parameters->database_directory_;
  parameters_.files_directory = parameters->files_directory_;
  parameters_.api_id = parameters->api_id_;
  parameters_.api_hash = parameters->api_hash_;
  parameters_.use_file_db = parameters->use_file_database_;
  parameters_.enable_storage_optimizer = parameters->enable_storage_optimizer_;
  parameters_.ignore_file_names = parameters->ignore_file_names_;
  parameters_.use_secret_chats = parameters->use_secret_chats_;
  parameters_.use_chat_info_db = parameters->use_chat_info_database_;
  parameters_.use_message_db = parameters->use_message_database_;

  VLOG(td_init) << "Fix parameters...";
  TRY_STATUS(fix_parameters(parameters_));
  VLOG(td_init) << "Check binlog encryption...";
  TRY_RESULT(encryption_info, TdDb::check_encryption(parameters_));
  is_database_encrypted_ = encryption_info.is_encrypted;

  VLOG(td_init) << "Create MtprotoHeader::Options";
  options_.api_id = parameters->api_id_;
  options_.system_language_code = trim(parameters->system_language_code_);
  options_.device_model = trim(parameters->device_model_);
  options_.system_version = trim(parameters->system_version_);
  options_.application_version = trim(parameters->application_version_);
  if (options_.system_language_code.empty()) {
    return Status::Error(400, "System language code must be non-empty");
  }
  if (options_.device_model.empty()) {
    return Status::Error(400, "Device model must be non-empty");
  }
  if (options_.system_version.empty()) {
    options_.system_version = get_operating_system_version().str();
    VLOG(td_init) << "Set system version to " << options_.system_version;
  }
  if (options_.application_version.empty()) {
    return Status::Error(400, "Application version must be non-empty");
  }
  if (options_.api_id != 21724) {
    options_.application_version += ", TDLib ";
    options_.application_version += TDLIB_VERSION;
  }
  options_.language_pack = string();
  options_.language_code = string();
  options_.parameters = string();
  options_.is_emulator = false;
  options_.proxy = Proxy();

  state_ = State::Decrypt;
  VLOG(td_init) << "Send authorizationStateWaitEncryptionKey";
  send_closure(actor_id(this), &Td::send_update,
               td_api::make_object<td_api::updateAuthorizationState>(
                   td_api::make_object<td_api::authorizationStateWaitEncryptionKey>(is_database_encrypted_)));
  VLOG(td_init) << "Finish set parameters";
  return Status::OK();
}

void Td::on_result(NetQueryPtr query) {
  query->debug("Td: received from DcManager");
  VLOG(net_query) << "Receive result of " << query;
  if (close_flag_ > 1) {
    return;
  }

  auto handler = extract_handler(query->id());
  if (handler != nullptr) {
    CHECK(query->is_ready());
    if (query->is_ok()) {
      handler->on_result(std::move(query->ok()));
    } else {
      handler->on_error(std::move(query->error()));
    }
  } else if (!query->is_ok() || query->ok_tl_constructor() != telegram_api::upload_file::ID) {
    LOG(WARNING) << query << " is ignored: no handlers found";
  }
  query->clear();
}

void Td::on_request(uint64 id, const td_api::getCountryCode &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::text>(result.move_as_ok()));
    }
  });
  country_info_manager_->get_current_country_code(std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::removeNotificationGroup &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  notification_manager_->remove_notification_group(NotificationGroupId(request.notification_group_id_),
                                                   NotificationId(request.max_notification_id_), MessageId(), -1, true,
                                                   std::move(promise));
}

void Td::on_request(uint64 id, td_api::getStatisticalGraph &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.token_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->load_statistics_graph(DialogId(request.chat_id_), std::move(request.token_), request.x_,
                                           std::move(promise));
}

void Td::on_request(uint64 id, const td_api::addChatMember &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->add_dialog_participant(DialogId(request.chat_id_), UserId(request.user_id_),
                                            request.forward_limit_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setCommands &request) {
  CHECK_IS_BOT();
  CREATE_OK_REQUEST_PROMISE();
  set_commands(this, std::move(request.scope_), std::move(request.language_code_), std::move(request.commands_),
               std::move(promise));
}

void Td::on_request(uint64 id, td_api::setStickerPositionInSet &request) {
  CHECK_IS_BOT();
  CREATE_OK_REQUEST_PROMISE();
  stickers_manager_->set_sticker_position_in_set(request.sticker_, request.position_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteProfilePhoto &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->delete_profile_photo(request.profile_photo_id_, std::move(promise));
}

void Td::add_handler(uint64 id, std::shared_ptr<ResultHandler> handler) {
  result_handlers_[id] = std::move(handler);
}

void Td::on_request(uint64 id, const td_api::parseMarkdown &request) {
  UNREACHABLE();
}
}  // namespace td
