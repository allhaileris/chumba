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

#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"

#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"



#include "td/telegram/JsonValue.h"

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

class SetStickerSetThumbnailRequest final : public RequestOnceActor {
  UserId user_id_;
  string name_;
  tl_object_ptr<td_api::InputFile> thumbnail_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->set_sticker_set_thumbnail(user_id_, name_, std::move(thumbnail_), std::move(promise));
  }

  void do_send_result() final {
    auto set_id = td_->stickers_manager_->search_sticker_set(name_, Auto());
    if (!set_id.is_valid()) {
      return send_error(Status::Error(500, "Sticker set not found"));
    }
    send_result(td_->stickers_manager_->get_sticker_set_object(set_id));
  }

 public:
  SetStickerSetThumbnailRequest(ActorShared<Td> td, uint64 request_id, int64 user_id, string &&name,
                                tl_object_ptr<td_api::InputFile> &&thumbnail)
      : RequestOnceActor(std::move(td), request_id)
      , user_id_(user_id)
      , name_(std::move(name))
      , thumbnail_(std::move(thumbnail)) {
  }
};

void Td::on_request(uint64 id, td_api::setStickerSetThumbnail &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST(SetStickerSetThumbnailRequest, request.user_id_, std::move(request.name_),
                 std::move(request.thumbnail_));
}

void Td::on_request(uint64 id, const td_api::createCall &request) {
  CHECK_IS_USER();

  if (request.protocol_ == nullptr) {
    return send_error_raw(id, 400, "Call protocol must be non-empty");
  }

  UserId user_id(request.user_id_);
  auto r_input_user = contacts_manager_->get_input_user(user_id);
  if (r_input_user.is_error()) {
    return send_error_raw(id, r_input_user.error().code(), r_input_user.error().message());
  }

  if (!G()->shared_config().get_option_boolean("calls_enabled")) {
    return send_error_raw(id, 400, "Calls are not enabled for the current user");
  }

  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<CallId> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(result.ok().get_call_id_object());
    }
  });
  send_closure(G()->call_manager(), &CallManager::create_call, user_id, r_input_user.move_as_ok(),
               CallProtocol(*request.protocol_), request.is_video_, std::move(query_promise));
}

class GetStickersRequest final : public RequestActor<> {
  string emoji_;
  int32 limit_;

  vector<FileId> sticker_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_ids_ = td_->stickers_manager_->get_stickers(emoji_, limit_, get_tries() < 2, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_stickers_object(sticker_ids_));
  }

 public:
  GetStickersRequest(ActorShared<Td> td, uint64 request_id, string &&emoji, int32 limit)
      : RequestActor(std::move(td), request_id), emoji_(std::move(emoji)), limit_(limit) {
    set_tries(5);
  }
};

void Td::on_request(uint64 id, td_api::getStickers &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.emoji_);
  CREATE_REQUEST(GetStickersRequest, std::move(request.emoji_), request.limit_);
}

class GetSupergroupRequest final : public RequestActor<> {
  ChannelId channel_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->get_channel(channel_id_, get_tries(), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_supergroup_object(channel_id_));
  }

 public:
  GetSupergroupRequest(ActorShared<Td> td, uint64 request_id, int64 channel_id)
      : RequestActor(std::move(td), request_id), channel_id_(channel_id) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getSupergroup &request) {
  CREATE_REQUEST(GetSupergroupRequest, request.supergroup_id_);
}

void Td::on_request(uint64 id, td_api::editInlineMessageLiveLocation &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->edit_inline_message_live_location(request.inline_message_id_, std::move(request.reply_markup_),
                                                       std::move(request.location_), request.heading_,
                                                       request.proximity_alert_radius_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::getChatEventLog &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST_PROMISE();
  get_dialog_event_log(this, DialogId(request.chat_id_), std::move(request.query_), request.from_event_id_,
                       request.limit_, std::move(request.filters_), UserId::get_user_ids(request.user_ids_),
                       std::move(promise));
}

void Td::schedule_get_terms_of_service(int32 expires_in) {
  if (expires_in == 0) {
    // drop pending Terms of Service after successful accept
    pending_terms_of_service_ = TermsOfService();
  }
  if (!close_flag_ && !auth_manager_->is_bot()) {
    alarm_timeout_.set_timeout_in(TERMS_OF_SERVICE_ALARM_ID, expires_in);
  }
}

void Td::on_request(uint64 id, const td_api::clickAnimatedEmojiMessage &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->click_animated_emoji_message({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                                  std::move(promise));
}

void Td::on_request(uint64 id, td_api::getExternalLink &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.link_);
  CREATE_REQUEST_PROMISE();
  link_manager_->get_link_login_url(request.link_, request.allow_write_access_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::getUserPrivacySettingRules &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(privacy_manager_, &PrivacyManager::get_privacy, std::move(request.setting_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::searchChatsNearby &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  contacts_manager_->search_dialogs_nearby(Location(request.location_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setTdlibParameters &request) {
  send_error_raw(id, 400, "Unexpected setTdlibParameters");
}

void Td::on_request(uint64 id, const td_api::getJsonString &request) {
  UNREACHABLE();
}
}  // namespace td
