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
#include "td/telegram/BackgroundManager.h"
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


#include "td/telegram/GroupCallId.h"
#include "td/telegram/GroupCallManager.h"


#include "td/telegram/JsonValue.h"

#include "td/telegram/LinkManager.h"
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
#include "td/telegram/PublicDialogType.h"
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

class SetBackgroundRequest final : public RequestActor<> {
  td_api::object_ptr<td_api::InputBackground> input_background_;
  td_api::object_ptr<td_api::BackgroundType> background_type_;
  bool for_dark_theme_ = false;

  BackgroundId background_id_;

  void do_run(Promise<Unit> &&promise) final {
    background_id_ = td_->background_manager_->set_background(input_background_.get(), background_type_.get(),
                                                              for_dark_theme_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->background_manager_->get_background_object(background_id_, for_dark_theme_, nullptr));
  }

 public:
  SetBackgroundRequest(ActorShared<Td> td, uint64 request_id,
                       td_api::object_ptr<td_api::InputBackground> &&input_background,
                       td_api::object_ptr<td_api::BackgroundType> background_type, bool for_dark_theme)
      : RequestActor(std::move(td), request_id)
      , input_background_(std::move(input_background))
      , background_type_(std::move(background_type))
      , for_dark_theme_(for_dark_theme) {
  }
};

void Td::on_request(uint64 id, td_api::setBackground &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(SetBackgroundRequest, std::move(request.background_), std::move(request.type_),
                 request.for_dark_theme_);
}

bool Td::is_synchronous_request(int32 id) {
  switch (id) {
    case td_api::getTextEntities::ID:
    case td_api::parseTextEntities::ID:
    case td_api::parseMarkdown::ID:
    case td_api::getMarkdownText::ID:
    case td_api::getFileMimeType::ID:
    case td_api::getFileExtension::ID:
    case td_api::cleanFileName::ID:
    case td_api::getLanguagePackString::ID:
    case td_api::getPhoneNumberInfoSync::ID:
    case td_api::getChatFilterDefaultIconName::ID:
    case td_api::getJsonValue::ID:
    case td_api::getJsonString::ID:
    case td_api::getPushReceiverId::ID:
    case td_api::setLogStream::ID:
    case td_api::getLogStream::ID:
    case td_api::setLogVerbosityLevel::ID:
    case td_api::getLogVerbosityLevel::ID:
    case td_api::getLogTags::ID:
    case td_api::setLogTagVerbosityLevel::ID:
    case td_api::getLogTagVerbosityLevel::ID:
    case td_api::addLogMessage::ID:
    case td_api::testReturnError::ID:
      return true;
    default:
      return false;
  }
}

class GetStickerEmojisRequest final : public RequestActor<> {
  tl_object_ptr<td_api::InputFile> input_file_;

  vector<string> emojis_;

  void do_run(Promise<Unit> &&promise) final {
    emojis_ = td_->stickers_manager_->get_sticker_emojis(input_file_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_api::make_object<td_api::emojis>(std::move(emojis_)));
  }

 public:
  GetStickerEmojisRequest(ActorShared<Td> td, uint64 request_id, tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestActor(std::move(td), request_id), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::getStickerEmojis &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetStickerEmojisRequest, std::move(request.sticker_));
}

class GetGroupFullInfoRequest final : public RequestActor<> {
  ChatId chat_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->load_chat_full(chat_id_, get_tries() < 2, std::move(promise), "getBasicGroupFullInfo");
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_basic_group_full_info_object(chat_id_));
  }

 public:
  GetGroupFullInfoRequest(ActorShared<Td> td, uint64 request_id, int64 chat_id)
      : RequestActor(std::move(td), request_id), chat_id_(chat_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getBasicGroupFullInfo &request) {
  CREATE_REQUEST(GetGroupFullInfoRequest, request.basic_group_id_);
}

void Td::on_request(uint64 id, const td_api::setChatMemberStatus &request) {
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, participant_dialog_id,
                     get_message_sender_dialog_id(this, request.member_id_, false, false));
  contacts_manager_->set_dialog_participant_status(DialogId(request.chat_id_), participant_dialog_id,
                                                   get_dialog_participant_status(request.status_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getSuggestedFileName &request) {
  Result<string> r_file_name = file_manager_->get_suggested_file_name(FileId(request.file_id_, 0), request.directory_);
  if (r_file_name.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_file_name.move_as_error());
  }
  send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::text>(r_file_name.ok()));
}

void Td::on_request(uint64 id, td_api::checkEmailAddressVerificationCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.code_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::check_email_address_verification_code, std::move(request.code_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::loadGroupCallParticipants &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->load_group_call_participants(GroupCallId(request.group_call_id_), request.limit_,
                                                    std::move(promise));
}

void Td::on_request(uint64 id, const td_api::checkCreatedPublicChatsLimit &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->check_created_public_dialogs_limit(get_public_dialog_type(request.type_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getExternalLinkInfo &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.link_);
  CREATE_REQUEST_PROMISE();
  link_manager_->get_external_link_info(std::move(request.link_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::setBio &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.bio_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_bio(request.bio_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getActiveSessions &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  get_active_sessions(this, std::move(promise));
}

void Td::close() {
  close_impl(false);
}
}  // namespace td
