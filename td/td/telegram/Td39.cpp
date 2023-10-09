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
#include "td/telegram/PhoneNumberManager.h"
#include "td/telegram/Photo.h"
#include "td/telegram/PhotoSizeSource.h"

#include "td/telegram/PrivacyManager.h"
#include "td/telegram/PublicDialogType.h"
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

class SearchEmojisRequest final : public RequestActor<> {
  string text_;
  bool exact_match_;
  vector<string> input_language_codes_;

  vector<string> emojis_;

  void do_run(Promise<Unit> &&promise) final {
    emojis_ = td_->stickers_manager_->search_emojis(text_, exact_match_, input_language_codes_, get_tries() < 2,
                                                    std::move(promise));
  }

  void do_send_result() final {
    send_result(td_api::make_object<td_api::emojis>(std::move(emojis_)));
  }

 public:
  SearchEmojisRequest(ActorShared<Td> td, uint64 request_id, string &&text, bool exact_match,
                      vector<string> &&input_language_codes)
      : RequestActor(std::move(td), request_id)
      , text_(std::move(text))
      , exact_match_(exact_match)
      , input_language_codes_(std::move(input_language_codes)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::searchEmojis &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.text_);
  for (auto &input_language_code : request.input_language_codes_) {
    CLEAN_INPUT_STRING(input_language_code);
  }
  CREATE_REQUEST(SearchEmojisRequest, std::move(request.text_), request.exact_match_,
                 std::move(request.input_language_codes_));
}

class GetCallbackQueryMessageRequest final : public RequestOnceActor {
  DialogId dialog_id_;
  MessageId message_id_;
  int64 callback_query_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->get_callback_query_message(dialog_id_, message_id_, callback_query_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(
        td_->messages_manager_->get_message_object({dialog_id_, message_id_}, "GetCallbackQueryMessageRequest"));
  }

 public:
  GetCallbackQueryMessageRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                                 int64 callback_query_id)
      : RequestOnceActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , message_id_(message_id)
      , callback_query_id_(callback_query_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getCallbackQueryMessage &request) {
  CHECK_IS_BOT();
  CREATE_REQUEST(GetCallbackQueryMessageRequest, request.chat_id_, request.message_id_, request.callback_query_id_);
}

class CheckChatInviteLinkRequest final : public RequestActor<> {
  string invite_link_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->check_dialog_invite_link(invite_link_, std::move(promise));
  }

  void do_send_result() final {
    auto result = td_->contacts_manager_->get_chat_invite_link_info_object(invite_link_);
    CHECK(result != nullptr);
    send_result(std::move(result));
  }

 public:
  CheckChatInviteLinkRequest(ActorShared<Td> td, uint64 request_id, string invite_link)
      : RequestActor(std::move(td), request_id), invite_link_(std::move(invite_link)) {
  }
};

void Td::on_request(uint64 id, td_api::checkChatInviteLink &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_REQUEST(CheckChatInviteLinkRequest, request.invite_link_);
}

class AddFavoriteStickerRequest final : public RequestOnceActor {
  tl_object_ptr<td_api::InputFile> input_file_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->add_favorite_sticker(input_file_, std::move(promise));
  }

 public:
  AddFavoriteStickerRequest(ActorShared<Td> td, uint64 request_id, tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestOnceActor(std::move(td), request_id), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::addFavoriteSticker &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(AddFavoriteStickerRequest, std::move(request.sticker_));
}

void Td::on_request(uint64 id, td_api::getRemoteFile &request) {
  CLEAN_INPUT_STRING(request.remote_file_id_);
  auto file_type = request.file_type_ == nullptr ? FileType::Temp : get_file_type(*request.file_type_);
  auto r_file_id = file_manager_->from_persistent_id(request.remote_file_id_, file_type);
  if (r_file_id.is_error()) {
    send_closure(actor_id(this), &Td::send_error, id, r_file_id.move_as_error());
  } else {
    send_closure(actor_id(this), &Td::send_result, id, file_manager_->get_file_object(r_file_id.ok()));
  }
}

void Td::on_request(uint64 id, td_api::editInlineMessageText &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->edit_inline_message_text(request.inline_message_id_, std::move(request.reply_markup_),
                                              std::move(request.input_message_content_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::answerShippingQuery &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.error_message_);
  CREATE_OK_REQUEST_PROMISE();
  answer_shipping_query(this, request.shipping_query_id_, std::move(request.shipping_options_), request.error_message_,
                        std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getMessageLocally &request) {
  FullMessageId full_message_id(DialogId(request.chat_id_), MessageId(request.message_id_));
  send_closure(actor_id(this), &Td::send_result, id,
               messages_manager_->get_message_object(full_message_id, "getMessageLocally"));
}

void Td::on_request(uint64 id, td_api::checkPhoneNumberConfirmationCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.code_);
  send_closure(confirm_phone_number_manager_, &PhoneNumberManager::check_code, id, std::move(request.code_));
}

void Td::on_request(uint64 id, const td_api::getCreatedPublicChats &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_created_public_dialogs(get_public_dialog_type(request.type_), std::move(promise), false);
}

void Td::on_request(uint64 id, const td_api::hideSuggestedAction &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  dismiss_suggested_action(SuggestedAction(request.action_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::closeChat &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->close_dialog(DialogId(request.chat_id_)));
}

void Td::destroy() {
  close_impl(true);
}
}  // namespace td
