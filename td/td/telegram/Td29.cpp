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
#include "td/telegram/OptionManager.h"
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

void Td::on_request(uint64 id, const td_api::getCurrentState &request) {
  vector<td_api::object_ptr<td_api::Update>> updates;

  option_manager_->get_current_state(updates);

  auto state = auth_manager_->get_current_authorization_state_object();
  if (state != nullptr) {
    updates.push_back(td_api::make_object<td_api::updateAuthorizationState>(std::move(state)));
  }

  updates.push_back(get_update_connection_state_object(connection_state_));

  if (auth_manager_->is_authorized()) {
    contacts_manager_->get_current_state(updates);

    background_manager_->get_current_state(updates);

    animations_manager_->get_current_state(updates);

    stickers_manager_->get_current_state(updates);

    messages_manager_->get_current_state(updates);

    notification_manager_->get_current_state(updates);

    config_manager_->get_actor_unsafe()->get_current_state(updates);

    // TODO updateFileGenerationStart generation_id:int64 original_path:string destination_path:string conversion:string = Update;
    // TODO updateCall call:call = Update;
    // TODO updateGroupCall call:groupCall = Update;
  }

  auto update_terms_of_service = get_update_terms_of_service_object();
  if (update_terms_of_service != nullptr) {
    updates.push_back(std::move(update_terms_of_service));
  }

  // send response synchronously to prevent "Request aborted" or other changes of the current state
  send_result(id, td_api::make_object<td_api::updates>(std::move(updates)));
}

class GetRepliedMessageRequest final : public RequestOnceActor {
  DialogId dialog_id_;
  MessageId message_id_;

  FullMessageId replied_message_id_;

  void do_run(Promise<Unit> &&promise) final {
    replied_message_id_ =
        td_->messages_manager_->get_replied_message(dialog_id_, message_id_, get_tries() < 3, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(replied_message_id_, "GetRepliedMessageRequest"));
  }

 public:
  GetRepliedMessageRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id)
      : RequestOnceActor(std::move(td), request_id), dialog_id_(dialog_id), message_id_(message_id) {
    set_tries(3);  // 1 to get initial message, 1 to get the reply and 1 for result
  }
};

void Td::on_request(uint64 id, const td_api::getRepliedMessage &request) {
  CREATE_REQUEST(GetRepliedMessageRequest, request.chat_id_, request.message_id_);
}

class GetAttachedStickerSetsRequest final : public RequestActor<> {
  FileId file_id_;

  vector<StickerSetId> sticker_set_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_ids_ = td_->stickers_manager_->get_attached_sticker_sets(file_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_sets_object(-1, sticker_set_ids_, 5));
  }

 public:
  GetAttachedStickerSetsRequest(ActorShared<Td> td, uint64 request_id, int32 file_id)
      : RequestActor(std::move(td), request_id), file_id_(file_id, 0) {
  }
};

void Td::on_request(uint64 id, const td_api::getAttachedStickerSets &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetAttachedStickerSetsRequest, request.file_id_);
}

void Td::on_request(uint64 id, td_api::acceptTermsOfService &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.terms_of_service_id_);
  auto promise = PromiseCreator::lambda([id = id, actor_id = actor_id(this)](Result<> result) {
    if (result.is_error()) {
      send_closure(actor_id, &Td::send_error, id, result.move_as_error());
    } else {
      send_closure(actor_id, &Td::send_result, id, td_api::make_object<td_api::ok>());
      send_closure(actor_id, &Td::schedule_get_terms_of_service, 0);
    }
  });
  accept_terms_of_service(this, std::move(request.terms_of_service_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::editChatFilter &request) {
  CHECK_IS_USER();
  if (request.filter_ == nullptr) {
    return send_error_raw(id, 400, "Chat filter must be non-empty");
  }
  CLEAN_INPUT_STRING(request.filter_->title_);
  CLEAN_INPUT_STRING(request.filter_->icon_name_);
  CREATE_REQUEST_PROMISE();
  messages_manager_->edit_dialog_filter(DialogFilterId(request.chat_filter_id_), std::move(request.filter_),
                                        std::move(promise));
}

void Td::on_request(uint64 id, td_api::setRecoveryEmailAddress &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  CLEAN_INPUT_STRING(request.new_recovery_email_address_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::set_recovery_email_address, std::move(request.password_),
               std::move(request.new_recovery_email_address_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteAllRevokedChatInviteLinks &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->delete_all_revoked_dialog_invite_links(DialogId(request.chat_id_),
                                                            UserId(request.creator_user_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getAllPassportElements &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  CREATE_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::get_all_secure_values, std::move(request.password_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::addChatToList &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->add_dialog_to_list(DialogId(request.chat_id_), DialogListId(request.chat_list_),
                                        std::move(promise));
}

void Td::on_request(uint64 id, const td_api::removeChatActionBar &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->remove_dialog_action_bar(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getAutoDownloadSettingsPresets &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  get_auto_download_settings_presets(this, std::move(promise));
}

void Td::on_request(uint64 id, td_api::getOption &request) {
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST_PROMISE();
  option_manager_->get_option(request.name_, std::move(promise));
}
}  // namespace td
