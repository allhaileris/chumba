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

#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"



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
#include "td/telegram/PhoneNumberManager.h"
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
#include "td/telegram/TdDb.h"
#include "td/telegram/telegram_api.hpp"



#include "td/telegram/UpdatesManager.h"
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

#include <limits>

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

void Td::on_file_download_finished(FileId file_id) {
  auto it = pending_file_downloads_.find(file_id);
  if (it == pending_file_downloads_.end()) {
    return;
  }
  for (auto id : it->second.request_ids) {
    // there was send_closure to call this function
    auto file_object = file_manager_->get_file_object(file_id, false);
    CHECK(file_object != nullptr);
    auto download_offset = file_object->local_->download_offset_;
    auto downloaded_size = file_object->local_->downloaded_prefix_size_;
    auto file_size = file_object->size_;
    auto limit = it->second.limit;
    if (limit == 0) {
      limit = std::numeric_limits<int32>::max();
    }
    if (file_object->local_->is_downloading_completed_ ||
        (download_offset <= it->second.offset && download_offset + downloaded_size >= it->second.offset &&
         ((file_size != 0 && download_offset + downloaded_size == file_size) ||
          download_offset + downloaded_size - it->second.offset >= limit))) {
      send_result(id, std::move(file_object));
    } else {
      send_error_impl(id, td_api::make_object<td_api::error>(400, "File download has failed or was canceled"));
    }
  }
  pending_file_downloads_.erase(it);
}

void Td::close_impl(bool destroy_flag) {
  destroy_flag_ |= destroy_flag;
  if (close_flag_) {
    return;
  }

  LOG(WARNING) << (destroy_flag ? "Destroy" : "Close") << " Td in state " << static_cast<int32>(state_);
  if (state_ == State::WaitParameters || state_ == State::Decrypt) {
    clear_requests();
    if (destroy_flag && state_ == State::Decrypt) {
      TdDb::destroy(parameters_).ignore();
    }
    state_ = State::Close;
    close_flag_ = 4;
    G()->set_close_flag();
    send_update(td_api::make_object<td_api::updateAuthorizationState>(
        td_api::make_object<td_api::authorizationStateClosing>()));

    request_actors_.clear();
    return send_closure_later(actor_id(this), &Td::dec_request_actor_refcnt);  // remove guard
  }

  state_ = State::Close;
  close_flag_ = 1;
  G()->set_close_flag();
  send_closure(auth_manager_actor_, &AuthManager::on_closing, destroy_flag);
  updates_manager_->timeout_expired();  // save pts and qts

  // wait till all request_actors will stop
  request_actors_.clear();
  G()->td_db()->flush_all();
  send_closure_later(actor_id(this), &Td::dec_request_actor_refcnt);  // remove guard
}

class RemoveRecentStickerRequest final : public RequestActor<> {
  bool is_attached_;
  tl_object_ptr<td_api::InputFile> input_file_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->remove_recent_sticker(is_attached_, input_file_, std::move(promise));
  }

 public:
  RemoveRecentStickerRequest(ActorShared<Td> td, uint64 request_id, bool is_attached,
                             tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestActor(std::move(td), request_id), is_attached_(is_attached), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::removeRecentSticker &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(RemoveRecentStickerRequest, request.is_attached_, std::move(request.sticker_));
}

class GetSuitableDiscussionChatsRequest final : public RequestActor<> {
  vector<DialogId> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->contacts_manager_->get_dialogs_for_discussion(std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(-1, dialog_ids_));
  }

 public:
  GetSuitableDiscussionChatsRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getSuitableDiscussionChats &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetSuitableDiscussionChatsRequest);
}

void Td::on_request(uint64 id, const td_api::getProxyLink &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::httpUrl>(result.move_as_ok()));
    }
  });
  send_closure(G()->connection_creator(), &ConnectionCreator::get_proxy_link, request.proxy_id_,
               std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::editInlineMessageMedia &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->edit_inline_message_media(request.inline_message_id_, std::move(request.reply_markup_),
                                               std::move(request.input_message_content_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::addCustomServerLanguagePack &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_pack_id_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::add_custom_server_language,
               std::move(request.language_pack_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setChatSlowModeDelay &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_channel_slow_mode_delay(DialogId(request.chat_id_), request.slow_mode_delay_,
                                                 std::move(promise));
}

void Td::on_request(uint64 id, td_api::getChatInviteLink &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_dialog_invite_link(DialogId(request.chat_id_), request.invite_link_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setInactiveSessionTtl &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  set_inactive_session_ttl_days(this, request.inactive_session_ttl_days_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::resendPhoneNumberVerificationCode &request) {
  CHECK_IS_USER();
  send_closure(verify_phone_number_manager_, &PhoneNumberManager::resend_authentication_code, id);
}

void Td::on_request(uint64 id, const td_api::resendAuthenticationCode &request) {
  send_closure(auth_manager_actor_, &AuthManager::resend_authentication_code, id);
}

bool Td::is_online() const {
  return is_online_;
}
}  // namespace td
