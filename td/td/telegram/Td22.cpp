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

class CreateNewStickerSetRequest final : public RequestOnceActor {
  UserId user_id_;
  string title_;
  string name_;
  bool is_masks_;
  vector<tl_object_ptr<td_api::InputSticker>> stickers_;
  string software_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->create_new_sticker_set(user_id_, title_, name_, is_masks_, std::move(stickers_),
                                                   std::move(software_), std::move(promise));
  }

  void do_send_result() final {
    auto set_id = td_->stickers_manager_->search_sticker_set(name_, Auto());
    if (!set_id.is_valid()) {
      return send_error(Status::Error(500, "Created sticker set not found"));
    }
    send_result(td_->stickers_manager_->get_sticker_set_object(set_id));
  }

 public:
  CreateNewStickerSetRequest(ActorShared<Td> td, uint64 request_id, int64 user_id, string &&title, string &&name,
                             bool is_masks, vector<tl_object_ptr<td_api::InputSticker>> &&stickers, string &&software)
      : RequestOnceActor(std::move(td), request_id)
      , user_id_(user_id)
      , title_(std::move(title))
      , name_(std::move(name))
      , is_masks_(is_masks)
      , stickers_(std::move(stickers))
      , software_(std::move(software)) {
  }
};

void Td::on_request(uint64 id, td_api::createNewStickerSet &request) {
  CLEAN_INPUT_STRING(request.title_);
  CLEAN_INPUT_STRING(request.name_);
  CLEAN_INPUT_STRING(request.source_);
  CREATE_REQUEST(CreateNewStickerSetRequest, request.user_id_, std::move(request.title_), std::move(request.name_),
                 request.is_masks_, std::move(request.stickers_), std::move(request.source_));
}

class GetTrendingStickerSetsRequest final : public RequestActor<> {
  std::pair<int32, vector<StickerSetId>> sticker_set_ids_;
  int32 offset_;
  int32 limit_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_ids_ = td_->stickers_manager_->get_featured_sticker_sets(offset_, limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_sets_object(sticker_set_ids_.first, sticker_set_ids_.second, 5));
  }

 public:
  GetTrendingStickerSetsRequest(ActorShared<Td> td, uint64 request_id, int32 offset, int32 limit)
      : RequestActor(std::move(td), request_id), offset_(offset), limit_(limit) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getTrendingStickerSets &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetTrendingStickerSetsRequest, request.offset_, request.limit_);
}

class GetRecentlyOpenedChatsRequest final : public RequestActor<> {
  int32 limit_;

  std::pair<int32, vector<DialogId>> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->messages_manager_->get_recently_opened_dialogs(limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(dialog_ids_));
  }

 public:
  GetRecentlyOpenedChatsRequest(ActorShared<Td> td, uint64 request_id, int32 limit)
      : RequestActor(std::move(td), request_id), limit_(limit) {
  }
};

void Td::on_request(uint64 id, const td_api::getRecentlyOpenedChats &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetRecentlyOpenedChatsRequest, request.limit_);
}

void Td::on_request(uint64 id, td_api::checkStickerSetName &request) {
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda(
      [promise = std::move(promise)](Result<StickersManager::CheckStickerSetNameResult> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(StickersManager::get_check_sticker_set_name_result_object(result.ok()));
        }
      });
  stickers_manager_->check_sticker_set_name(request.name_, std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::banChatMember &request) {
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, participant_dialog_id,
                     get_message_sender_dialog_id(this, request.member_id_, false, false));
  contacts_manager_->ban_dialog_participant(DialogId(request.chat_id_), participant_dialog_id,
                                            request.banned_until_date_, request.revoke_messages_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::editInlineMessageCaption &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->edit_inline_message_caption(request.inline_message_id_, std::move(request.reply_markup_),
                                                 std::move(request.caption_), std::move(promise));
}

void Td::schedule_get_promo_data(int32 expires_in) {
  expires_in = expires_in <= 0 ? 0 : clamp(expires_in, 60, 86400);
  if (!close_flag_ && auth_manager_->is_authorized() && !auth_manager_->is_bot()) {
    LOG(INFO) << "Schedule getPromoData in " << expires_in;
    alarm_timeout_.set_timeout_in(PROMO_DATA_ALARM_ID, expires_in);
  }
}

void Td::on_request(uint64 id, td_api::sendChatAction &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->send_dialog_action(DialogId(request.chat_id_), MessageId(request.message_thread_id_),
                                        DialogAction(std::move(request.action_)), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getPaymentForm &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  get_payment_form(this, {DialogId(request.chat_id_), MessageId(request.message_id_)}, request.theme_,
                   std::move(promise));
}

void Td::on_request(uint64 id, const td_api::openMessageContent &request) {
  CHECK_IS_USER();
  answer_ok_query(
      id, messages_manager_->open_message_content({DialogId(request.chat_id_), MessageId(request.message_id_)}));
}

void Td::on_request(uint64 id, td_api::confirmQrCodeAuthentication &request) {
  CLEAN_INPUT_STRING(request.link_);
  CREATE_REQUEST_PROMISE();
  confirm_qr_code_authentication(this, request.link_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setLogTagVerbosityLevel &request) {
  UNREACHABLE();
}

void Td::on_request(uint64 id, const td_api::getTextEntities &request) {
  UNREACHABLE();
}
}  // namespace td
