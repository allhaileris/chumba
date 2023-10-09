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
#include "td/telegram/GroupCallManager.h"

#include "td/telegram/InlineQueriesManager.h"
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

class CreateNewSecretChatRequest final : public RequestActor<SecretChatId> {
  UserId user_id_;
  SecretChatId secret_chat_id_;

  void do_run(Promise<SecretChatId> &&promise) final {
    if (get_tries() < 2) {
      promise.set_value(std::move(secret_chat_id_));
      return;
    }
    td_->messages_manager_->create_new_secret_chat(user_id_, std::move(promise));
  }

  void do_set_result(SecretChatId &&result) final {
    secret_chat_id_ = result;
    LOG(INFO) << "New " << secret_chat_id_ << " created";
  }

  void do_send_result() final {
    CHECK(secret_chat_id_.is_valid());
    // SecretChatActor will send this update by itself
    // But since the update may still be on its way, we will update essential fields here.
    td_->contacts_manager_->on_update_secret_chat(
        secret_chat_id_, 0 /* no access_hash */, user_id_, SecretChatState::Unknown, true /* it is outbound chat */,
        -1 /* unknown TTL */, 0 /* unknown creation date */, "" /* no key_hash */, 0, FolderId());
    DialogId dialog_id(secret_chat_id_);
    td_->messages_manager_->force_create_dialog(dialog_id, "create new secret chat", true);
    send_result(td_->messages_manager_->get_chat_object(dialog_id));
  }

 public:
  CreateNewSecretChatRequest(ActorShared<Td> td, uint64 request_id, int64 user_id)
      : RequestActor(std::move(td), request_id), user_id_(user_id) {
  }
};
void Td::on_request(uint64 id, td_api::createNewSecretChat &request) {
  CREATE_REQUEST(CreateNewSecretChatRequest, request.user_id_);
}

class GetMessageEmbeddingCodeRequest final : public RequestActor<> {
  FullMessageId full_message_id_;
  bool for_group_;

  string html_;

  void do_run(Promise<Unit> &&promise) final {
    html_ = td_->messages_manager_->get_message_embedding_code(full_message_id_, for_group_, std::move(promise));
  }

  void do_send_result() final {
    send_result(make_tl_object<td_api::text>(html_));
  }

 public:
  GetMessageEmbeddingCodeRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                                 bool for_group)
      : RequestActor(std::move(td), request_id)
      , full_message_id_(DialogId(dialog_id), MessageId(message_id))
      , for_group_(for_group) {
  }
};

void Td::on_request(uint64 id, const td_api::getMessageEmbeddingCode &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetMessageEmbeddingCodeRequest, request.chat_id_, request.message_id_, request.for_album_);
}

class GetChatFilterRequest final : public RequestActor<> {
  DialogFilterId dialog_filter_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->load_dialog_filter(dialog_filter_id_, get_tries() < 2, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_chat_filter_object(dialog_filter_id_));
  }

 public:
  GetChatFilterRequest(ActorShared<Td> td, uint64 request_id, int32 dialog_filter_id)
      : RequestActor(std::move(td), request_id), dialog_filter_id_(dialog_filter_id) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getChatFilter &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetChatFilterRequest, request.chat_filter_id_);
}

class GetRecentInlineBotsRequest final : public RequestActor<> {
  vector<UserId> user_ids_;

  void do_run(Promise<Unit> &&promise) final {
    user_ids_ = td_->inline_queries_manager_->get_recent_inline_bots(std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_users_object(-1, user_ids_));
  }

 public:
  GetRecentInlineBotsRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getRecentInlineBots &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetRecentInlineBotsRequest);
}

void Td::on_request(uint64 id, const td_api::setVideoChatDefaultParticipant &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, default_join_as_dialog_id,
                     get_message_sender_dialog_id(this, request.default_participant_id_, true, false));
  group_call_manager_->set_group_call_default_join_as(DialogId(request.chat_id_), default_join_as_dialog_id,
                                                      std::move(promise));
}

void Td::on_request(uint64 id, td_api::getPassportElement &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  if (request.type_ == nullptr) {
    return send_error_raw(id, 400, "Type must be non-empty");
  }
  CREATE_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::get_secure_value, std::move(request.password_),
               get_secure_value_type_td_api(request.type_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::createTemporaryPassword &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::create_temp_password, std::move(request.password_),
               request.valid_for_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteChatHistory &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->delete_dialog_history(DialogId(request.chat_id_), request.remove_from_chat_list_, request.revoke_,
                                           std::move(promise));
}

void Td::on_request(uint64 id, td_api::getMessageFileType &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.message_file_head_);
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_message_file_type(request.message_file_head_, std::move(promise));
}

void Td::set_is_online(bool is_online) {
  if (is_online == is_online_) {
    return;
  }

  is_online_ = is_online;
  if (auth_manager_ != nullptr) {  // postpone if there is no AuthManager yet
    on_online_updated(true, true);
  }
}

void Td::on_request(uint64 id, td_api::testCallVectorIntObject &request) {
  send_closure(actor_id(this), &Td::send_result, id,
               make_tl_object<td_api::testVectorIntObject>(std::move(request.x_)));
}

void Td::send_error(uint64 id, Status error) {
  send_error_impl(id, make_tl_object<td_api::error>(error.code(), error.message().str()));
  error.ignore();
}

void Td::tear_down() {
  LOG_CHECK(close_flag_ == 5) << close_flag_;
}
}  // namespace td
