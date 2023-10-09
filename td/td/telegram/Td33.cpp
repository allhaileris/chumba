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

#include "td/telegram/Global.h"
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

class EditMessageTextRequest final : public RequestOnceActor {
  FullMessageId full_message_id_;
  tl_object_ptr<td_api::ReplyMarkup> reply_markup_;
  tl_object_ptr<td_api::InputMessageContent> input_message_content_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->edit_message_text(full_message_id_, std::move(reply_markup_),
                                              std::move(input_message_content_), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(full_message_id_, "EditMessageTextRequest"));
  }

 public:
  EditMessageTextRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                         tl_object_ptr<td_api::ReplyMarkup> reply_markup,
                         tl_object_ptr<td_api::InputMessageContent> input_message_content)
      : RequestOnceActor(std::move(td), request_id)
      , full_message_id_(DialogId(dialog_id), MessageId(message_id))
      , reply_markup_(std::move(reply_markup))
      , input_message_content_(std::move(input_message_content)) {
  }
};

void Td::on_request(uint64 id, td_api::editMessageText &request) {
  CREATE_REQUEST(EditMessageTextRequest, request.chat_id_, request.message_id_, std::move(request.reply_markup_),
                 std::move(request.input_message_content_));
}

class GetGroupsInCommonRequest final : public RequestActor<> {
  UserId user_id_;
  DialogId offset_dialog_id_;
  int32 limit_;

  std::pair<int32, vector<DialogId>> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->messages_manager_->get_common_dialogs(user_id_, offset_dialog_id_, limit_, get_tries() < 2,
                                                             std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(dialog_ids_));
  }

 public:
  GetGroupsInCommonRequest(ActorShared<Td> td, uint64 request_id, int64 user_id, int64 offset_dialog_id, int32 limit)
      : RequestActor(std::move(td), request_id), user_id_(user_id), offset_dialog_id_(offset_dialog_id), limit_(limit) {
  }
};

void Td::on_request(uint64 id, const td_api::getGroupsInCommon &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetGroupsInCommonRequest, request.user_id_, request.offset_chat_id_, request.limit_);
}

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::getMarkdownText &request) {
  if (request.text_ == nullptr) {
    return make_error(400, "Text must be non-empty");
  }

  auto r_entities = get_message_entities(nullptr, std::move(request.text_->entities_));
  if (r_entities.is_error()) {
    return make_error(400, r_entities.error().message());
  }
  auto entities = r_entities.move_as_ok();
  auto status = fix_formatted_text(request.text_->text_, entities, true, true, true, true, true);
  if (status.is_error()) {
    return make_error(400, status.error().message());
  }

  return get_formatted_text_object(get_markdown_v3({std::move(request.text_->text_), std::move(entities)}), false,
                                   std::numeric_limits<int32>::max());
}

class RemoveSavedAnimationRequest final : public RequestOnceActor {
  tl_object_ptr<td_api::InputFile> input_file_;

  void do_run(Promise<Unit> &&promise) final {
    td_->animations_manager_->remove_saved_animation(input_file_, std::move(promise));
  }

 public:
  RemoveSavedAnimationRequest(ActorShared<Td> td, uint64 request_id, tl_object_ptr<td_api::InputFile> &&input_file)
      : RequestOnceActor(std::move(td), request_id), input_file_(std::move(input_file)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::removeSavedAnimation &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(RemoveSavedAnimationRequest, std::move(request.animation_));
}

void Td::on_request(uint64 id, td_api::editChatInviteLink &request) {
  CLEAN_INPUT_STRING(request.name_);
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->edit_dialog_invite_link(DialogId(request.chat_id_), request.invite_link_, std::move(request.name_),
                                             request.expiration_date_, request.member_limit_,
                                             request.creates_join_request_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::getChatJoinRequests &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_dialog_join_requests(DialogId(request.chat_id_), request.invite_link_, request.query_,
                                              std::move(request.offset_request_), request.limit_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::checkPasswordRecoveryCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.recovery_code_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::check_password_recovery_code, std::move(request.recovery_code_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleChatIsPinned &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->toggle_dialog_is_pinned(DialogListId(request.chat_list_),
                                                                 DialogId(request.chat_id_), request.is_pinned_));
}

void Td::on_request(uint64 id, td_api::checkAuthenticationPasswordRecoveryCode &request) {
  CLEAN_INPUT_STRING(request.recovery_code_);
  send_closure(auth_manager_actor_, &AuthManager::check_password_recovery_code, id, std::move(request.recovery_code_));
}

void Td::on_request(uint64 id, const td_api::deleteChatReplyMarkup &request) {
  CHECK_IS_USER();
  answer_ok_query(
      id, messages_manager_->delete_dialog_reply_markup(DialogId(request.chat_id_), MessageId(request.message_id_)));
}

void Td::on_request(uint64 id, const td_api::removeProxy &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->connection_creator(), &ConnectionCreator::remove_proxy, request.proxy_id_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getLogTagVerbosityLevel &request) {
  UNREACHABLE();
}

void Td::on_request(uint64 id, const td_api::getMarkdownText &request) {
  UNREACHABLE();
}
}  // namespace td
