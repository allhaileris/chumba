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

class CreateNewSupergroupChatRequest final : public RequestActor<> {
  string title_;
  bool is_megagroup_;
  string description_;
  DialogLocation location_;
  bool for_import_;
  int64 random_id_;

  DialogId dialog_id_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_id_ = td_->messages_manager_->create_new_channel_chat(title_, is_megagroup_, description_, location_,
                                                                 for_import_, random_id_, std::move(promise));
  }

  void do_send_result() final {
    CHECK(dialog_id_.is_valid());
    send_result(td_->messages_manager_->get_chat_object(dialog_id_));
  }

 public:
  CreateNewSupergroupChatRequest(ActorShared<Td> td, uint64 request_id, string title, bool is_megagroup,
                                 string description, td_api::object_ptr<td_api::chatLocation> &&location,
                                 bool for_import)
      : RequestActor(std::move(td), request_id)
      , title_(std::move(title))
      , is_megagroup_(is_megagroup)
      , description_(std::move(description))
      , location_(std::move(location))
      , for_import_(for_import)
      , random_id_(0) {
  }
};

void Td::on_request(uint64 id, td_api::createNewSupergroupChat &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.title_);
  CLEAN_INPUT_STRING(request.description_);
  CREATE_REQUEST(CreateNewSupergroupChatRequest, std::move(request.title_), !request.is_channel_,
                 std::move(request.description_), std::move(request.location_), request.for_import_);
}

bool Td::is_authentication_request(int32 id) {
  switch (id) {
    case td_api::setTdlibParameters::ID:
    case td_api::checkDatabaseEncryptionKey::ID:
    case td_api::setDatabaseEncryptionKey::ID:
    case td_api::getAuthorizationState::ID:
    case td_api::setAuthenticationPhoneNumber::ID:
    case td_api::resendAuthenticationCode::ID:
    case td_api::checkAuthenticationCode::ID:
    case td_api::registerUser::ID:
    case td_api::requestQrCodeAuthentication::ID:
    case td_api::checkAuthenticationPassword::ID:
    case td_api::requestAuthenticationPasswordRecovery::ID:
    case td_api::checkAuthenticationPasswordRecoveryCode::ID:
    case td_api::recoverAuthenticationPassword::ID:
    case td_api::deleteAccount::ID:
    case td_api::logOut::ID:
    case td_api::close::ID:
    case td_api::destroy::ID:
    case td_api::checkAuthenticationBotToken::ID:
      return true;
    default:
      return false;
  }
}

class GetMessageRequest final : public RequestOnceActor {
  FullMessageId full_message_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->get_message(full_message_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(full_message_id_, "GetMessageRequest"));
  }

 public:
  GetMessageRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id)
      : RequestOnceActor(std::move(td), request_id), full_message_id_(DialogId(dialog_id), MessageId(message_id)) {
  }
};

void Td::on_request(uint64 id, const td_api::getMessage &request) {
  CREATE_REQUEST(GetMessageRequest, request.chat_id_, request.message_id_);
}

void Td::on_get_terms_of_service(Result<std::pair<int32, TermsOfService>> result, bool dummy) {
  int32 expires_in = 0;
  if (result.is_error()) {
    expires_in = Random::fast(10, 60);
  } else {
    auto terms = result.move_as_ok();
    pending_terms_of_service_ = std::move(terms.second);
    auto update = get_update_terms_of_service_object();
    if (update == nullptr) {
      expires_in = min(max(terms.first, G()->unix_time() + 3600) - G()->unix_time(), 86400);
    } else {
      send_update(std::move(update));
    }
  }
  if (expires_in > 0) {
    schedule_get_terms_of_service(expires_in);
  }
}

void Td::on_request(uint64 id, td_api::sendPassportAuthorizationForm &request) {
  CHECK_IS_USER();
  for (auto &type : request.types_) {
    if (type == nullptr) {
      return send_error_raw(id, 400, "Type must be non-empty");
    }
  }

  CREATE_OK_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::send_passport_authorization_form, request.autorization_form_id_,
               get_secure_value_types_td_api(request.types_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getChatInviteLinkMembers &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_dialog_invite_link_users(DialogId(request.chat_id_), request.invite_link_,
                                                  std::move(request.offset_member_), request.limit_,
                                                  std::move(promise));
}

void Td::on_request(uint64 id, td_api::processPushNotification &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.payload_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->notification_manager(), &NotificationManager::process_push_notification,
               std::move(request.payload_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteMessages &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->delete_messages(DialogId(request.chat_id_), MessagesManager::get_message_ids(request.message_ids_),
                                     request.revoke_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setName &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.first_name_);
  CLEAN_INPUT_STRING(request.last_name_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_name(request.first_name_, request.last_name_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setDatabaseEncryptionKey &request) {
  CREATE_OK_REQUEST_PROMISE();
  G()->td_db()->get_binlog()->change_key(as_db_key(std::move(request.new_encryption_key_)), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getPasswordState &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::get_state, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::terminateSession &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  terminate_session(this, request.session_id_, std::move(promise));
}
}  // namespace td
