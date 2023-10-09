//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/Td.h"

#include "td/telegram/Account.h"
#include "td/telegram/AnimationsManager.h"

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

#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"

#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"



#include "td/telegram/JsonValue.h"
#include "td/telegram/LanguagePackManager.h"
#include "td/telegram/LinkManager.h"
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

class CreateChatRequest final : public RequestActor<> {
  DialogId dialog_id_;
  bool force_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->create_dialog(dialog_id_, force_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_chat_object(dialog_id_));
  }

 public:
  CreateChatRequest(ActorShared<Td> td, uint64 request_id, DialogId dialog_id, bool force)
      : RequestActor<>(std::move(td), request_id), dialog_id_(dialog_id), force_(force) {
  }
};

void Td::on_request(uint64 id, const td_api::createPrivateChat &request) {
  CREATE_REQUEST(CreateChatRequest, DialogId(UserId(request.user_id_)), request.force_);
}

void Td::on_request(uint64 id, const td_api::createBasicGroupChat &request) {
  CREATE_REQUEST(CreateChatRequest, DialogId(ChatId(request.basic_group_id_)), request.force_);
}

void Td::on_request(uint64 id, const td_api::createSupergroupChat &request) {
  CREATE_REQUEST(CreateChatRequest, DialogId(ChannelId(request.supergroup_id_)), request.force_);
}

void Td::on_request(uint64 id, td_api::createSecretChat &request) {
  CREATE_REQUEST(CreateChatRequest, DialogId(SecretChatId(request.secret_chat_id_)), true);
}

class SetBotUpdatesStatusQuery final : public Td::ResultHandler {
 public:
  void send(int32 pending_update_count, const string &error_message) {
    send_query(
        G()->net_query_creator().create(telegram_api::help_setBotUpdatesStatus(pending_update_count, error_message)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_setBotUpdatesStatus>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG_IF(WARNING, !result) << "Set bot updates status has failed";
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(WARNING) << "Receive error for SetBotUpdatesStatusQuery: " << status;
    }
    status.ignore();
  }
};

void Td::on_request(uint64 id, td_api::setBotUpdatesStatus &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.error_message_);
  create_handler<SetBotUpdatesStatusQuery>()->send(request.pending_update_count_, request.error_message_);
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

class GetSupergroupFullInfoRequest final : public RequestActor<> {
  ChannelId channel_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->load_channel_full(channel_id_, get_tries() < 2, std::move(promise),
                                              "GetSupergroupFullInfoRequest");
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_supergroup_full_info_object(channel_id_));
  }

 public:
  GetSupergroupFullInfoRequest(ActorShared<Td> td, uint64 request_id, int64 channel_id)
      : RequestActor(std::move(td), request_id), channel_id_(channel_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getSupergroupFullInfo &request) {
  CREATE_REQUEST(GetSupergroupFullInfoRequest, request.supergroup_id_);
}

class AddSavedAnimationRequest final : public RequestOnceActor {
  tl_object_ptr<td_api::InputFile> input_file_;

  void do_run(Promise<Unit> &&promise) final {
    td_->animations_manager_->add_saved_animation(input_file_, std::move(promise));
  }

 public:
  AddSavedAnimationRequest(ActorShared<Td> td, uint64 request_id, tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestOnceActor(std::move(td), request_id), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::addSavedAnimation &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(AddSavedAnimationRequest, std::move(request.animation_));
}

void Td::send_result(uint64 id, tl_object_ptr<td_api::Object> object) {
  if (id == 0) {
    LOG(ERROR) << "Sending " << to_string(object) << " through send_result";
    return;
  }

  auto it = request_set_.find(id);
  if (it != request_set_.end()) {
    request_set_.erase(it);
    VLOG(td_requests) << "Sending result for request " << id << ": " << to_string(object);
    if (object == nullptr) {
      object = make_tl_object<td_api::error>(404, "Not Found");
    }
    callback_->on_result(id, std::move(object));
  }
}

void Td::on_request(uint64 id, td_api::recoverAuthenticationPassword &request) {
  CLEAN_INPUT_STRING(request.recovery_code_);
  CLEAN_INPUT_STRING(request.new_password_);
  CLEAN_INPUT_STRING(request.new_hint_);
  send_closure(auth_manager_actor_, &AuthManager::recover_password, id, std::move(request.recovery_code_),
               std::move(request.new_password_), std::move(request.new_hint_));
}

void Td::on_request(uint64 id, td_api::sendEmailAddressVerificationCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.email_address_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::send_email_address_verification_code,
               std::move(request.email_address_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::processChatJoinRequest &request) {
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->process_dialog_join_request(DialogId(request.chat_id_), UserId(request.user_id_), request.approve_,
                                                 std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getLanguagePackString &request) {
  return LanguagePackManager::get_language_pack_string(
      request.language_pack_database_path_, request.localization_target_, request.language_pack_id_, request.key_);
}

void Td::on_request(uint64 id, const td_api::removeBackground &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  background_manager_->remove_background(BackgroundId(request.background_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getDeepLinkInfo &request) {
  CLEAN_INPUT_STRING(request.link_);
  CREATE_REQUEST_PROMISE();
  link_manager_->get_deep_link_info(request.link_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteSavedOrderInfo &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  delete_saved_order_info(this, std::move(promise));
}
}  // namespace td
