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


#include "td/telegram/GroupCallId.h"
#include "td/telegram/GroupCallManager.h"


#include "td/telegram/JsonValue.h"
#include "td/telegram/LanguagePackManager.h"

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

void Td::on_request(uint64 id, td_api::optimizeStorage &request) {
  std::vector<FileType> file_types;
  for (auto &file_type : request.file_types_) {
    if (file_type == nullptr) {
      return send_error_raw(id, 400, "File type must be non-empty");
    }

    file_types.push_back(get_file_type(*file_type));
  }
  std::vector<DialogId> owner_dialog_ids;
  for (auto chat_id : request.chat_ids_) {
    DialogId dialog_id(chat_id);
    if (!dialog_id.is_valid() && dialog_id != DialogId()) {
      return send_error_raw(id, 400, "Wrong chat identifier");
    }
    owner_dialog_ids.push_back(dialog_id);
  }
  std::vector<DialogId> exclude_owner_dialog_ids;
  for (auto chat_id : request.exclude_chat_ids_) {
    DialogId dialog_id(chat_id);
    if (!dialog_id.is_valid() && dialog_id != DialogId()) {
      return send_error_raw(id, 400, "Wrong chat identifier");
    }
    exclude_owner_dialog_ids.push_back(dialog_id);
  }
  FileGcParameters parameters(request.size_, request.ttl_, request.count_, request.immunity_delay_,
                              std::move(file_types), std::move(owner_dialog_ids), std::move(exclude_owner_dialog_ids),
                              request.chat_limit_);

  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<FileStats> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(result.ok().get_storage_statistics_object());
    }
  });
  send_closure(storage_manager_, &StorageManager::run_gc, std::move(parameters),
               request.return_deleted_file_statistics_, std::move(query_promise));
}

class GetChatPinnedMessageRequest final : public RequestOnceActor {
  DialogId dialog_id_;

  MessageId pinned_message_id_;

  void do_run(Promise<Unit> &&promise) final {
    pinned_message_id_ = td_->messages_manager_->get_dialog_pinned_message(dialog_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(
        td_->messages_manager_->get_message_object({dialog_id_, pinned_message_id_}, "GetChatPinnedMessageRequest"));
  }

 public:
  GetChatPinnedMessageRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id)
      : RequestOnceActor(std::move(td), request_id), dialog_id_(dialog_id) {
    set_tries(3);  // 1 to get pinned_message_id, 1 to get the message and 1 for result
  }
};

void Td::on_request(uint64 id, const td_api::getChatPinnedMessage &request) {
  CREATE_REQUEST(GetChatPinnedMessageRequest, request.chat_id_);
}

void Td::on_request(uint64 id, td_api::getPollVoters &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda(
      [promise = std::move(promise), td = this](Result<std::pair<int32, vector<UserId>>> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(td->contacts_manager_->get_users_object(result.ok().first, result.ok().second));
        }
      });
  messages_manager_->get_poll_voters({DialogId(request.chat_id_), MessageId(request.message_id_)}, request.option_id_,
                                     request.offset_, request.limit_, std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::setPassword &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.old_password_);
  CLEAN_INPUT_STRING(request.new_password_);
  CLEAN_INPUT_STRING(request.new_hint_);
  CLEAN_INPUT_STRING(request.new_recovery_email_address_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::set_password, std::move(request.old_password_),
               std::move(request.new_password_), std::move(request.new_hint_), request.set_recovery_email_address_,
               std::move(request.new_recovery_email_address_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setGroupCallParticipantVolumeLevel &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, participant_dialog_id,
                     get_message_sender_dialog_id(this, request.participant_id_, true, false));
  group_call_manager_->set_group_call_participant_volume_level(
      GroupCallId(request.group_call_id_), participant_dialog_id, request.volume_level_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setCustomLanguagePackString &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_pack_id_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::set_custom_language_string,
               std::move(request.language_pack_id_), std::move(request.new_string_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::editMessageSchedulingState &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->edit_message_scheduling_state({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                                   std::move(request.scheduling_state_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setSupergroupStickerSet &request) {
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_channel_sticker_set(ChannelId(request.supergroup_id_), StickerSetId(request.sticker_set_id_),
                                             std::move(promise));
}

void Td::on_request(uint64 id, td_api::setChatTheme &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.theme_name_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_dialog_theme(DialogId(request.chat_id_), request.theme_name_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::requestQrCodeAuthentication &request) {
  send_closure(auth_manager_actor_, &AuthManager::request_qr_code_authentication, id,
               UserId::get_user_ids(request.other_user_ids_));
}

void Td::on_request(uint64 id, const td_api::cancelUploadFile &request) {
  file_manager_->cancel_upload(FileId(request.file_id_, 0));

  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::on_request(uint64 id, const td_api::setLogVerbosityLevel &request) {
  UNREACHABLE();
}

void Td::on_request(uint64 id, const td_api::parseTextEntities &request) {
  UNREACHABLE();
}
}  // namespace td
