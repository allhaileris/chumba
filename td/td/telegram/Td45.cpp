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

#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"
#include "td/telegram/GroupCallManager.h"


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
#include "td/telegram/SecretChatsManager.h"
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

class SearchCallMessagesRequest final : public RequestActor<> {
  MessageId from_message_id_;
  int32 limit_;
  bool only_missed_;
  int64 random_id_;

  std::pair<int32, vector<FullMessageId>> messages_;

  void do_run(Promise<Unit> &&promise) final {
    messages_ = td_->messages_manager_->search_call_messages(from_message_id_, limit_, only_missed_, random_id_,
                                                             get_tries() == 3, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_messages_object(messages_.first, messages_.second, true,
                                                            "SearchCallMessagesRequest"));
  }

 public:
  SearchCallMessagesRequest(ActorShared<Td> td, uint64 request_id, int64 from_message_id, int32 limit, bool only_missed)
      : RequestActor(std::move(td), request_id)
      , from_message_id_(from_message_id)
      , limit_(limit)
      , only_missed_(only_missed)
      , random_id_(0) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::searchCallMessages &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(SearchCallMessagesRequest, request.from_message_id_, request.limit_, request.only_missed_);
}

void Td::start_up() {
  always_wait_for_mailbox();

  uint64 check_endianness = 0x0706050403020100;
  auto check_endianness_raw = reinterpret_cast<const unsigned char *>(&check_endianness);
  for (unsigned char c = 0; c < 8; c++) {
    auto symbol = check_endianness_raw[static_cast<size_t>(c)];
    LOG_IF(FATAL, symbol != c) << "TDLib requires little-endian platform";
  }

  VLOG(td_init) << "Create Global";
  old_context_ = set_context(std::make_shared<Global>());
  G()->set_net_query_stats(td_options_.net_query_stats);
  inc_request_actor_refcnt();  // guard
  inc_actor_refcnt();          // guard

  alarm_timeout_.set_callback(on_alarm_timeout_callback);
  alarm_timeout_.set_callback_data(static_cast<void *>(this));

  CHECK(state_ == State::WaitParameters);
  send_update(td_api::make_object<td_api::updateOption>("version",
                                                        td_api::make_object<td_api::optionValueString>(TDLIB_VERSION)));
  send_update(td_api::make_object<td_api::updateAuthorizationState>(
      td_api::make_object<td_api::authorizationStateWaitTdlibParameters>()));
}

class SearchStickersRequest final : public RequestActor<> {
  string emoji_;
  int32 limit_;

  vector<FileId> sticker_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_ids_ = td_->stickers_manager_->search_stickers(emoji_, limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_stickers_object(sticker_ids_));
  }

 public:
  SearchStickersRequest(ActorShared<Td> td, uint64 request_id, string &&emoji, int32 limit)
      : RequestActor(std::move(td), request_id), emoji_(std::move(emoji)), limit_(limit) {
  }
};

void Td::on_request(uint64 id, td_api::searchStickers &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.emoji_);
  CREATE_REQUEST(SearchStickersRequest, std::move(request.emoji_), request.limit_);
}

class GetImportedContactCountRequest final : public RequestActor<> {
  int32 imported_contact_count_ = 0;

  void do_run(Promise<Unit> &&promise) final {
    imported_contact_count_ = td_->contacts_manager_->get_imported_contact_count(std::move(promise));
  }

  void do_send_result() final {
    send_result(td_api::make_object<td_api::count>(imported_contact_count_));
  }

 public:
  GetImportedContactCountRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getImportedContactCount &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetImportedContactCountRequest);
}

void Td::on_request(uint64 id, td_api::setPassportElementErrors &request) {
  CHECK_IS_BOT();
  auto r_input_user = contacts_manager_->get_input_user(UserId(request.user_id_));
  if (r_input_user.is_error()) {
    return send_error_raw(id, r_input_user.error().code(), r_input_user.error().message());
  }
  CREATE_OK_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::set_secure_value_errors, this, r_input_user.move_as_ok(),
               std::move(request.errors_), std::move(promise));
}

void Td::on_connection_state_changed(ConnectionState new_state) {
  if (new_state == connection_state_) {
    LOG(ERROR) << "State manager sends update about unchanged state " << static_cast<int32>(new_state);
    return;
  }
  if (G()->close_flag()) {
    return;
  }
  connection_state_ = new_state;

  send_closure(actor_id(this), &Td::send_update, get_update_connection_state_object(connection_state_));
}

void Td::on_request(uint64 id, const td_api::toggleChatDefaultDisableNotification &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->toggle_dialog_silent_send_message(DialogId(request.chat_id_),
                                                                           request.default_disable_notification_));
}

void Td::on_request(uint64 id, const td_api::setGroupCallParticipantIsSpeaking &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->set_group_call_participant_is_speaking(
      GroupCallId(request.group_call_id_), request.audio_source_, request.is_speaking_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::closeSecretChat &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(secret_chats_manager_, &SecretChatsManager::cancel_chat, SecretChatId(request.secret_chat_id_), false,
               std::move(promise));
}

void Td::on_request(uint64 id, td_api::getBankCardInfo &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.bank_card_number_);
  CREATE_REQUEST_PROMISE();
  get_bank_card_info(this, request.bank_card_number_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteAllCallMessages &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->delete_all_call_messages(request.revoke_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::testCallEmpty &request) {
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::ResultHandler::set_td(Td *td) {
  CHECK(td_ == nullptr);
  td_ = td;
}
}  // namespace td
