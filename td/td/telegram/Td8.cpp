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

void Td::request(uint64 id, tl_object_ptr<td_api::Function> function) {
  if (id == 0) {
    LOG(ERROR) << "Ignore request with ID == 0: " << to_string(function);
    return;
  }

  request_set_.insert(id);
  if (function == nullptr) {
    return send_error_impl(id, make_error(400, "Request is empty"));
  }

  VLOG(td_requests) << "Receive request " << id << ": " << to_string(function);
  int32 function_id = function->get_id();
  if (is_synchronous_request(function_id)) {
    // send response synchronously
    return send_result(id, static_request(std::move(function)));
  }
  if (state_ != State::Run) {
    switch (function_id) {
      case td_api::getAuthorizationState::ID:
        // send response synchronously to prevent "Request aborted"
        return send_result(id, get_fake_authorization_state_object());
      case td_api::getCurrentState::ID: {
        vector<td_api::object_ptr<td_api::Update>> updates;
        updates.push_back(td_api::make_object<td_api::updateOption>(
            "version", td_api::make_object<td_api::optionValueString>(TDLIB_VERSION)));
        updates.push_back(td_api::make_object<td_api::updateAuthorizationState>(get_fake_authorization_state_object()));
        // send response synchronously to prevent "Request aborted"
        return send_result(id, td_api::make_object<td_api::updates>(std::move(updates)));
      }
      case td_api::close::ID:
        // need to send response before actual closing
        send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::ok>());
        send_closure(actor_id(this), &Td::close);
        return;
      default:
        break;
    }
  }
  switch (state_) {
    case State::WaitParameters: {
      switch (function_id) {
        case td_api::setTdlibParameters::ID:
          return answer_ok_query(
              id, set_parameters(std::move(move_tl_object_as<td_api::setTdlibParameters>(function)->parameters_)));
        default:
          if (is_preinitialization_request(function_id)) {
            break;
          }
          if (is_preauthentication_request(function_id)) {
            pending_preauthentication_requests_.emplace_back(id, std::move(function));
            return;
          }
          return send_error_impl(
              id, make_error(400, "Initialization parameters are needed: call setTdlibParameters first"));
      }
      break;
    }
    case State::Decrypt: {
      switch (function_id) {
        case td_api::checkDatabaseEncryptionKey::ID: {
          auto check_key = move_tl_object_as<td_api::checkDatabaseEncryptionKey>(function);
          return answer_ok_query(id, init(as_db_key(std::move(check_key->encryption_key_))));
        }
        case td_api::setDatabaseEncryptionKey::ID: {
          auto set_key = move_tl_object_as<td_api::setDatabaseEncryptionKey>(function);
          return answer_ok_query(id, init(as_db_key(std::move(set_key->new_encryption_key_))));
        }
        case td_api::destroy::ID:
          // need to send response synchronously before actual destroying
          send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::ok>());
          send_closure(actor_id(this), &Td::destroy);
          return;
        default:
          if (is_preinitialization_request(function_id)) {
            break;
          }
          if (is_preauthentication_request(function_id)) {
            pending_preauthentication_requests_.emplace_back(id, std::move(function));
            return;
          }
          return send_error_impl(
              id, make_error(400, "Database encryption key is needed: call checkDatabaseEncryptionKey first"));
      }
      break;
    }
    case State::Close:
      if (destroy_flag_) {
        return send_error_impl(id, make_error(401, "Unauthorized"));
      } else {
        return send_error_impl(id, make_error(500, "Request aborted"));
      }
    case State::Run:
      break;
  }

  if ((auth_manager_ == nullptr || !auth_manager_->is_authorized()) && !is_preauthentication_request(function_id) &&
      !is_preinitialization_request(function_id) && !is_authentication_request(function_id)) {
    return send_error_impl(id, make_error(401, "Unauthorized"));
  }
  downcast_call(*function, [this, id](auto &request) { this->on_request(id, request); });
}

void Td::on_request(uint64 id, td_api::registerDevice &request) {
  CHECK_IS_USER();
  if (request.device_token_ == nullptr) {
    return send_error_raw(id, 400, "Device token must be non-empty");
  }
  CREATE_REQUEST_PROMISE();
  send_closure(device_token_manager_, &DeviceTokenManager::register_device, std::move(request.device_token_),
               UserId::get_user_ids(request.other_user_ids_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::setGroupCallTitle &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.title_);
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->set_group_call_title(GroupCallId(request.group_call_id_), std::move(request.title_),
                                            std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleSupergroupSignMessages &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->toggle_channel_sign_messages(ChannelId(request.supergroup_id_), request.sign_messages_,
                                                  std::move(promise));
}

void Td::on_request(uint64 id, td_api::getAnimatedEmoji &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.emoji_);
  CREATE_REQUEST_PROMISE();
  stickers_manager_->get_animated_emoji(std::move(request.emoji_), false, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::endGroupCallScreenSharing &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->end_group_call_screen_sharing(GroupCallId(request.group_call_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getRecommendedChatFilters &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_recommended_dialog_filters(std::move(promise));
}

void Td::on_request(uint64 id, td_api::testCallVectorString &request) {
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::testVectorString>(std::move(request.x_)));
}
}  // namespace td
