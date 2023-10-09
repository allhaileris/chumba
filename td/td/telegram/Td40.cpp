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

class SendCustomRequestQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::customRequestResult>> promise_;

 public:
  explicit SendCustomRequestQuery(Promise<td_api::object_ptr<td_api::customRequestResult>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const string &method, const string &parameters) {
    send_query(G()->net_query_creator().create(
        telegram_api::bots_sendCustomRequest(method, make_tl_object<telegram_api::dataJSON>(parameters))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::bots_sendCustomRequest>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    promise_.set_value(td_api::make_object<td_api::customRequestResult>(result->data_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void Td::on_request(uint64 id, td_api::sendCustomRequest &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.method_);
  CLEAN_INPUT_STRING(request.parameters_);
  CREATE_REQUEST_PROMISE();
  create_handler<SendCustomRequestQuery>(std::move(promise))->send(request.method_, request.parameters_);
}

class GetMessageThreadRequest final : public RequestActor<MessageThreadInfo> {
  DialogId dialog_id_;
  MessageId message_id_;

  MessageThreadInfo message_thread_info_;

  void do_run(Promise<MessageThreadInfo> &&promise) final {
    if (get_tries() < 2) {
      promise.set_value(std::move(message_thread_info_));
      return;
    }
    td_->messages_manager_->get_message_thread(dialog_id_, message_id_, std::move(promise));
  }

  void do_set_result(MessageThreadInfo &&result) final {
    message_thread_info_ = std::move(result);
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_thread_info_object(message_thread_info_));
  }

 public:
  GetMessageThreadRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id)
      : RequestActor(std::move(td), request_id), dialog_id_(dialog_id), message_id_(message_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getMessageThread &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetMessageThreadRequest, request.chat_id_, request.message_id_);
}

class SearchBackgroundRequest final : public RequestActor<> {
  string name_;

  std::pair<BackgroundId, BackgroundType> background_;

  void do_run(Promise<Unit> &&promise) final {
    background_ = td_->background_manager_->search_background(name_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->background_manager_->get_background_object(background_.first, false, &background_.second));
  }

 public:
  SearchBackgroundRequest(ActorShared<Td> td, uint64 request_id, string &&name)
      : RequestActor(std::move(td), request_id), name_(std::move(name)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::searchBackground &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST(SearchBackgroundRequest, std::move(request.name_));
}

void Td::on_request(uint64 id, td_api::createVideoChat &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.title_);
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<GroupCallId> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(td_api::make_object<td_api::groupCallId>(result.ok().get()));
    }
  });
  group_call_manager_->create_voice_chat(DialogId(request.chat_id_), std::move(request.title_), request.start_date_,
                                         std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::reportChatPhoto &request) {
  CHECK_IS_USER();
  auto r_report_reason = ReportReason::get_report_reason(std::move(request.reason_), std::move(request.text_));
  if (r_report_reason.is_error()) {
    return send_error_raw(id, r_report_reason.error().code(), r_report_reason.error().message());
  }
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->report_dialog_photo(DialogId(request.chat_id_), FileId(request.file_id_, 0),
                                         r_report_reason.move_as_ok(), std::move(promise));
}

Promise<Unit> Td::create_ok_request_promise(uint64 id) {
  return PromiseCreator::lambda([id = id, actor_id = actor_id(this)](Result<Unit> result) {
    if (result.is_error()) {
      send_closure(actor_id, &Td::send_error, id, result.move_as_error());
    } else {
      send_closure(actor_id, &Td::send_result, id, td_api::make_object<td_api::ok>());
    }
  });
}

void Td::on_request(uint64 id, td_api::transferChatOwnership &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->transfer_dialog_ownership(DialogId(request.chat_id_), UserId(request.user_id_), request.password_,
                                               std::move(promise));
}

void Td::on_request(uint64 id, td_api::registerUser &request) {
  CLEAN_INPUT_STRING(request.first_name_);
  CLEAN_INPUT_STRING(request.last_name_);
  send_closure(auth_manager_actor_, &AuthManager::register_user, id, std::move(request.first_name_),
               std::move(request.last_name_));
}

void Td::on_request(uint64 id, td_api::editCustomLanguagePackInfo &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::edit_custom_language_info, std::move(request.info_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::cancelPasswordReset &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::cancel_password_reset, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setChatPhoto &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_dialog_photo(DialogId(request.chat_id_), request.photo_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::testSquareInt &request) {
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::testInt>(request.x_ * request.x_));
}
}  // namespace td
