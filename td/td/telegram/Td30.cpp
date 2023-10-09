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


#include "td/telegram/files/FileGcParameters.h"
#include "td/telegram/files/FileId.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"
#include "td/telegram/GameManager.h"
#include "td/telegram/Global.h"
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

void Td::on_get_promo_data(Result<telegram_api::object_ptr<telegram_api::help_PromoData>> r_promo_data, bool dummy) {
  if (G()->close_flag()) {
    return;
  }

  if (r_promo_data.is_error()) {
    LOG(ERROR) << "Receive error for GetPromoData: " << r_promo_data.error();
    return schedule_get_promo_data(60);
  }

  auto promo_data_ptr = r_promo_data.move_as_ok();
  CHECK(promo_data_ptr != nullptr);
  LOG(DEBUG) << "Receive " << to_string(promo_data_ptr);
  int32 expires_at = 0;
  switch (promo_data_ptr->get_id()) {
    case telegram_api::help_promoDataEmpty::ID: {
      auto promo = telegram_api::move_object_as<telegram_api::help_promoDataEmpty>(promo_data_ptr);
      expires_at = promo->expires_;
      messages_manager_->remove_sponsored_dialog();
      break;
    }
    case telegram_api::help_promoData::ID: {
      auto promo = telegram_api::move_object_as<telegram_api::help_promoData>(promo_data_ptr);
      expires_at = promo->expires_;
      bool is_proxy = promo->proxy_;
      messages_manager_->on_get_sponsored_dialog(
          std::move(promo->peer_),
          is_proxy ? DialogSource::mtproto_proxy()
                   : DialogSource::public_service_announcement(promo->psa_type_, promo->psa_message_),
          std::move(promo->users_), std::move(promo->chats_));
      break;
    }
    default:
      UNREACHABLE();
  }
  schedule_get_promo_data(expires_at == 0 ? 0 : expires_at - G()->unix_time());
}

void Td::on_request(uint64 id, td_api::joinGroupCall &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_hash_);
  CLEAN_INPUT_STRING(request.payload_);
  CREATE_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, join_as_dialog_id,
                     get_message_sender_dialog_id(this, request.participant_id_, true, true));
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::text>(result.move_as_ok()));
    }
  });
  group_call_manager_->join_group_call(GroupCallId(request.group_call_id_), join_as_dialog_id, request.audio_source_id_,
                                       std::move(request.payload_), request.is_muted_, request.is_my_video_enabled_,
                                       request.invite_hash_, std::move(query_promise));
}

class AddRecentStickerRequest final : public RequestActor<> {
  bool is_attached_;
  tl_object_ptr<td_api::InputFile> input_file_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->add_recent_sticker(is_attached_, input_file_, std::move(promise));
  }

 public:
  AddRecentStickerRequest(ActorShared<Td> td, uint64 request_id, bool is_attached,
                          tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestActor(std::move(td), request_id), is_attached_(is_attached), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::addRecentSticker &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(AddRecentStickerRequest, request.is_attached_, std::move(request.sticker_));
}

void Td::on_request(uint64 id, const td_api::getChatMessageCount &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<int32> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::count>(result.move_as_ok()));
    }
  });
  messages_manager_->get_dialog_message_count(DialogId(request.chat_id_), get_message_search_filter(request.filter_),
                                              request.return_local_, std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::resendMessages &request) {
  DialogId dialog_id(request.chat_id_);
  auto r_message_ids =
      messages_manager_->resend_messages(dialog_id, MessagesManager::get_message_ids(request.message_ids_));
  if (r_message_ids.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_message_ids.move_as_error());
  }

  send_closure(actor_id(this), &Td::send_result, id,
               messages_manager_->get_messages_object(-1, dialog_id, r_message_ids.ok(), false, "resendMessages"));
}

void Td::on_request(uint64 id, td_api::setInlineGameScore &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_OK_REQUEST_PROMISE();
  game_manager_->set_inline_game_score(request.inline_message_id_, request.edit_message_, UserId(request.user_id_),
                                       request.score_, request.force_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallIsMyVideoEnabled &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_is_my_video_enabled(GroupCallId(request.group_call_id_),
                                                             request.is_my_video_enabled_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setCustomLanguagePack &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::set_custom_language, std::move(request.info_),
               std::move(request.strings_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::readFilePart &request) {
  CREATE_REQUEST_PROMISE();
  send_closure(file_manager_actor_, &FileManager::read_file_part, FileId(request.file_id_, 0), request.offset_,
               request.count_, 2, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteChatFilter &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->delete_dialog_filter(DialogFilterId(request.chat_filter_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::testCallVectorStringObject &request) {
  send_closure(actor_id(this), &Td::send_result, id,
               make_tl_object<td_api::testVectorStringObject>(std::move(request.x_)));
}

void Td::on_request(uint64 id, const td_api::getAuthorizationState &request) {
  send_closure(auth_manager_actor_, &AuthManager::get_state, id);
}

void Td::on_request(uint64 id, const td_api::getLogStream &request) {
  UNREACHABLE();
}
}  // namespace td
