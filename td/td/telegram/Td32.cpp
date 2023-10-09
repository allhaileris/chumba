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
#include "td/telegram/CountryInfoManager.h"
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
#include "td/telegram/LanguagePackManager.h"

#include "td/telegram/Location.h"
#include "td/telegram/Logging.h"
#include "td/telegram/MessageCopyOptions.h"
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

class EditMessageMediaRequest final : public RequestOnceActor {
  FullMessageId full_message_id_;
  tl_object_ptr<td_api::ReplyMarkup> reply_markup_;
  tl_object_ptr<td_api::InputMessageContent> input_message_content_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->edit_message_media(full_message_id_, std::move(reply_markup_),
                                               std::move(input_message_content_), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(full_message_id_, "EditMessageMediaRequest"));
  }

 public:
  EditMessageMediaRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                          tl_object_ptr<td_api::ReplyMarkup> reply_markup,
                          tl_object_ptr<td_api::InputMessageContent> input_message_content)
      : RequestOnceActor(std::move(td), request_id)
      , full_message_id_(DialogId(dialog_id), MessageId(message_id))
      , reply_markup_(std::move(reply_markup))
      , input_message_content_(std::move(input_message_content)) {
  }
};

void Td::on_request(uint64 id, td_api::editMessageMedia &request) {
  CREATE_REQUEST(EditMessageMediaRequest, request.chat_id_, request.message_id_, std::move(request.reply_markup_),
                 std::move(request.input_message_content_));
}

void Td::on_request(uint64 id, td_api::forwardMessages &request) {
  auto input_message_ids = MessagesManager::get_message_ids(request.message_ids_);
  auto message_copy_options =
      transform(input_message_ids, [send_copy = request.send_copy_, remove_caption = request.remove_caption_](
                                       MessageId) { return MessageCopyOptions(send_copy, remove_caption); });
  auto r_messages = messages_manager_->forward_messages(DialogId(request.chat_id_), DialogId(request.from_chat_id_),
                                                        std::move(input_message_ids), std::move(request.options_),
                                                        false, std::move(message_copy_options), request.only_preview_);
  if (r_messages.is_error()) {
    send_closure(actor_id(this), &Td::send_error, id, r_messages.move_as_error());
  } else {
    send_closure(actor_id(this), &Td::send_result, id, r_messages.move_as_ok());
  }
}

class GetChatMessageByDateRequest final : public RequestOnceActor {
  DialogId dialog_id_;
  int32 date_;

  int64 random_id_;

  void do_run(Promise<Unit> &&promise) final {
    random_id_ = td_->messages_manager_->get_dialog_message_by_date(dialog_id_, date_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_dialog_message_by_date_object(random_id_));
  }

 public:
  GetChatMessageByDateRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int32 date)
      : RequestOnceActor(std::move(td), request_id), dialog_id_(dialog_id), date_(date), random_id_(0) {
  }
};

void Td::on_request(uint64 id, const td_api::getChatMessageByDate &request) {
  CREATE_REQUEST(GetChatMessageByDateRequest, request.chat_id_, request.date_);
}

class RemoveFavoriteStickerRequest final : public RequestOnceActor {
  tl_object_ptr<td_api::InputFile> input_file_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->remove_favorite_sticker(input_file_, std::move(promise));
  }

 public:
  RemoveFavoriteStickerRequest(ActorShared<Td> td, uint64 request_id, tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestOnceActor(std::move(td), request_id), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::removeFavoriteSticker &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(RemoveFavoriteStickerRequest, std::move(request.sticker_));
}

void Td::on_request(uint64 id, td_api::getStorageStatisticsFast &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<FileStatsFast> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(result.ok().get_storage_statistics_fast_object());
    }
  });
  send_closure(storage_manager_, &StorageManager::get_storage_stats_fast, std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::createChatFilter &request) {
  CHECK_IS_USER();
  if (request.filter_ == nullptr) {
    return send_error_raw(id, 400, "Chat filter must be non-empty");
  }
  CLEAN_INPUT_STRING(request.filter_->title_);
  CLEAN_INPUT_STRING(request.filter_->icon_name_);
  CREATE_REQUEST_PROMISE();
  messages_manager_->create_dialog_filter(std::move(request.filter_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::synchronizeLanguagePack &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_pack_id_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::synchronize_language_pack,
               std::move(request.language_pack_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleSessionCanAcceptSecretChats &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  toggle_session_can_accept_secret_chats(this, request.session_id_, request.can_accept_secret_chats_,
                                         std::move(promise));
}

void Td::on_request(uint64 id, td_api::revokeChatInviteLink &request) {
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->revoke_dialog_invite_link(DialogId(request.chat_id_), request.invite_link_, std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::testReturnError &request) {
  if (request.error_ == nullptr) {
    return td_api::make_object<td_api::error>(404, "Not Found");
  }

  return std::move(request.error_);
}

void Td::on_request(uint64 id, const td_api::sharePhoneNumber &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->share_phone_number(UserId(request.user_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getCountries &request) {
  CREATE_REQUEST_PROMISE();
  country_info_manager_->get_countries(std::move(promise));
}

void Td::inc_request_actor_refcnt() {
  request_actor_refcnt_++;
}
}  // namespace td
