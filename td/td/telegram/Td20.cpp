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
#include "td/telegram/ConfigShared.h"
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
#include "td/telegram/GroupCallManager.h"


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

class UpdateStatusQuery final : public Td::ResultHandler {
  bool is_offline_;

 public:
  NetQueryRef send(bool is_offline) {
    is_offline_ = is_offline;
    auto net_query = G()->net_query_creator().create(telegram_api::account_updateStatus(is_offline));
    auto result = net_query.get_weak();
    send_query(std::move(net_query));
    return result;
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_updateStatus>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG(INFO) << "Receive result for UpdateStatusQuery: " << result;
    td_->on_update_status_success(!is_offline_);
  }

  void on_error(Status status) final {
    if (status.code() != NetQuery::Canceled && !G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for UpdateStatusQuery: " << status;
    }
    status.ignore();
  }
};

void Td::on_online_updated(bool force, bool send_update) {
  if (close_flag_ >= 2 || !auth_manager_->is_authorized() || auth_manager_->is_bot()) {
    return;
  }
  if (force || is_online_) {
    contacts_manager_->set_my_online_status(is_online_, send_update, true);
    if (!update_status_query_.empty()) {
      LOG(INFO) << "Cancel previous update status query";
      cancel_query(update_status_query_);
    }
    update_status_query_ = create_handler<UpdateStatusQuery>()->send(!is_online_);
  }
  if (is_online_) {
    alarm_timeout_.set_timeout_in(
        ONLINE_ALARM_ID,
        static_cast<double>(G()->shared_config().get_option_integer("online_update_period_ms", 210000)) * 1e-3);
  } else {
    alarm_timeout_.cancel_timeout(ONLINE_ALARM_ID);
  }
}

void Td::on_request(uint64 id, td_api::sendInlineQueryResultMessage &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.result_id_);

  DialogId dialog_id(request.chat_id_);
  auto r_new_message_id = messages_manager_->send_inline_query_result_message(
      dialog_id, MessageId(request.message_thread_id_), MessageId(request.reply_to_message_id_),
      std::move(request.options_), request.query_id_, request.result_id_, request.hide_via_bot_);
  if (r_new_message_id.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_new_message_id.move_as_error());
  }

  CHECK(r_new_message_id.ok().is_valid() || r_new_message_id.ok().is_valid_scheduled());
  send_closure(
      actor_id(this), &Td::send_result, id,
      messages_manager_->get_message_object({dialog_id, r_new_message_id.ok()}, "sendInlineQueryResultMessage"));
}

void Td::on_request(uint64 id, td_api::getPassportAuthorizationForm &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.public_key_);
  CLEAN_INPUT_STRING(request.scope_);
  CLEAN_INPUT_STRING(request.nonce_);
  UserId bot_user_id(request.bot_user_id_);
  if (!bot_user_id.is_valid()) {
    return send_error_raw(id, 400, "Bot user identifier invalid");
  }
  if (request.nonce_.empty()) {
    return send_error_raw(id, 400, "Nonce must be non-empty");
  }
  CREATE_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::get_passport_authorization_form, bot_user_id, std::move(request.scope_),
               std::move(request.public_key_), std::move(request.nonce_), std::move(promise));
}

bool Td::is_preinitialization_request(int32 id) {
  switch (id) {
    case td_api::getCurrentState::ID:
    case td_api::setAlarm::ID:
    case td_api::testUseUpdate::ID:
    case td_api::testCallEmpty::ID:
    case td_api::testSquareInt::ID:
    case td_api::testCallString::ID:
    case td_api::testCallBytes::ID:
    case td_api::testCallVectorInt::ID:
    case td_api::testCallVectorIntObject::ID:
    case td_api::testCallVectorString::ID:
    case td_api::testCallVectorStringObject::ID:
    case td_api::testProxy::ID:
      return true;
    default:
      return false;
  }
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallParticipantIsMuted &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, participant_dialog_id,
                     get_message_sender_dialog_id(this, request.participant_id_, true, false));
  group_call_manager_->toggle_group_call_participant_is_muted(
      GroupCallId(request.group_call_id_), participant_dialog_id, request.is_muted_, std::move(promise));
}

void Td::send_error_impl(uint64 id, tl_object_ptr<td_api::error> error) {
  CHECK(id != 0);
  CHECK(error != nullptr);
  auto it = request_set_.find(id);
  if (it != request_set_.end()) {
    request_set_.erase(it);
    VLOG(td_requests) << "Sending error for request " << id << ": " << oneline(to_string(error));
    callback_->on_error(id, std::move(error));
  }
}

void Td::on_request(uint64 id, td_api::sendCallRating &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.comment_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->call_manager(), &CallManager::rate_call, CallId(request.call_id_), request.rating_,
               std::move(request.comment_), std::move(request.problems_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::writeGeneratedFilePart &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(file_manager_actor_, &FileManager::external_file_generate_write_part, request.generation_id_,
               request.offset_, std::move(request.data_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::replacePrimaryChatInviteLink &request) {
  CREATE_REQUEST_PROMISE();
  contacts_manager_->export_dialog_invite_link(DialogId(request.chat_id_), string(), 0, 0, false, true,
                                               std::move(promise));
}

void Td::on_request(uint64 id, td_api::checkAuthenticationPassword &request) {
  CLEAN_INPUT_STRING(request.password_);
  send_closure(auth_manager_actor_, &AuthManager::check_password, id, std::move(request.password_));
}

void Td::on_request(uint64 id, const td_api::disableProxy &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->connection_creator(), &ConnectionCreator::disable_proxy, std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getJsonString &request) {
  return td_api::make_object<td_api::text>(get_json_string(request.json_value_.get()));
}
}  // namespace td
