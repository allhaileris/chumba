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
#include "td/telegram/WebPageId.h"
#include "td/telegram/WebPagesManager.h"

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

class LoadChatsRequest final : public RequestActor<> {
  DialogListId dialog_list_id_;
  DialogDate offset_;
  int32 limit_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->get_dialogs(dialog_list_id_, offset_, limit_, false, get_tries() < 2, std::move(promise));
  }

 public:
  LoadChatsRequest(ActorShared<Td> td, uint64 request_id, DialogListId dialog_list_id, DialogDate offset, int32 limit)
      : RequestActor(std::move(td), request_id), dialog_list_id_(dialog_list_id), offset_(offset), limit_(limit) {
    // 1 for database + 1 for server request + 1 for server request at the end + 1 for return + 1 just in case
    set_tries(5);

    if (limit_ > 100) {
      limit_ = 100;
    }
  }
};

void Td::on_request(uint64 id, const td_api::loadChats &request) {
  CHECK_IS_USER();

  DialogListId dialog_list_id(request.chat_list_);
  auto r_offset = messages_manager_->get_dialog_list_last_date(dialog_list_id);
  if (r_offset.is_error()) {
    return send_error_raw(id, 400, r_offset.error().message());
  }
  auto offset = r_offset.move_as_ok();
  if (offset == MAX_DIALOG_DATE) {
    return send_closure(actor_id(this), &Td::send_result, id, nullptr);
  }

  CREATE_REQUEST(LoadChatsRequest, dialog_list_id, offset, request.limit_);
}

class GetWebPageInstantViewRequest final : public RequestActor<WebPageId> {
  string url_;
  bool force_full_;

  WebPageId web_page_id_;

  void do_run(Promise<WebPageId> &&promise) final {
    if (get_tries() < 2) {
      promise.set_value(std::move(web_page_id_));
      return;
    }
    td_->web_pages_manager_->get_web_page_instant_view(url_, force_full_, std::move(promise));
  }

  void do_set_result(WebPageId &&result) final {
    web_page_id_ = result;
  }

  void do_send_result() final {
    send_result(td_->web_pages_manager_->get_web_page_instant_view_object(web_page_id_));
  }

 public:
  GetWebPageInstantViewRequest(ActorShared<Td> td, uint64 request_id, string url, bool force_full)
      : RequestActor(std::move(td), request_id), url_(std::move(url)), force_full_(force_full) {
  }
};

void Td::on_request(uint64 id, td_api::getWebPageInstantView &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.url_);
  CREATE_REQUEST(GetWebPageInstantViewRequest, std::move(request.url_), request.force_full_);
}

class SearchPublicChatRequest final : public RequestActor<> {
  string username_;

  DialogId dialog_id_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_id_ = td_->messages_manager_->search_public_dialog(username_, get_tries() < 3, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_chat_object(dialog_id_));
  }

 public:
  SearchPublicChatRequest(ActorShared<Td> td, uint64 request_id, string username)
      : RequestActor(std::move(td), request_id), username_(std::move(username)) {
    set_tries(4);  // 1 for server request + 1 for reload voice chat + 1 for reload dialog + 1 for result
  }
};

void Td::on_request(uint64 id, td_api::searchPublicChat &request) {
  CLEAN_INPUT_STRING(request.username_);
  CREATE_REQUEST(SearchPublicChatRequest, request.username_);
}

class GetFavoriteStickersRequest final : public RequestActor<> {
  vector<FileId> sticker_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_ids_ = td_->stickers_manager_->get_favorite_stickers(std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_stickers_object(sticker_ids_));
  }

 public:
  GetFavoriteStickersRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getFavoriteStickers &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetFavoriteStickersRequest);
}

void Td::on_request(uint64 id, td_api::reportChat &request) {
  CHECK_IS_USER();
  auto r_report_reason = ReportReason::get_report_reason(std::move(request.reason_), std::move(request.text_));
  if (r_report_reason.is_error()) {
    return send_error_raw(id, r_report_reason.error().code(), r_report_reason.error().message());
  }
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->report_dialog(DialogId(request.chat_id_), MessagesManager::get_message_ids(request.message_ids_),
                                   r_report_reason.move_as_ok(), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::pinChatMessage &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->pin_dialog_message(DialogId(request.chat_id_), MessageId(request.message_id_),
                                        request.disable_notification_, request.only_for_self_, false,
                                        std::move(promise));
}

void Td::on_request(uint64 id, td_api::deleteRevokedChatInviteLink &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->delete_revoked_dialog_invite_link(DialogId(request.chat_id_), request.invite_link_,
                                                       std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getLoginUrlInfo &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  link_manager_->get_login_url_info({DialogId(request.chat_id_), MessageId(request.message_id_)}, request.button_id_,
                                    std::move(promise));
}

void Td::dec_request_actor_refcnt() {
  request_actor_refcnt_--;
  LOG(DEBUG) << "Decrease request actor count to " << request_actor_refcnt_;
  if (request_actor_refcnt_ == 0) {
    LOG(INFO) << "Have no request actors";
    clear();
    dec_actor_refcnt();  // remove guard
  }
}

void Td::on_request(uint64 id, td_api::requestPasswordRecovery &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::request_password_recovery, std::move(promise));
}

void Td::on_request(uint64 id, td_api::removeStickerFromSet &request) {
  CHECK_IS_BOT();
  CREATE_OK_REQUEST_PROMISE();
  stickers_manager_->remove_sticker_from_set(request.sticker_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::requestAuthenticationPasswordRecovery &request) {
  send_closure(auth_manager_actor_, &AuthManager::request_password_recovery, id);
}

}  // namespace td
