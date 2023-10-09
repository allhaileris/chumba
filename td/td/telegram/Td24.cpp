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
#include "td/telegram/GameManager.h"

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
#include "td/telegram/NotificationManager.h"
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

class EditMessageLiveLocationRequest final : public RequestOnceActor {
  FullMessageId full_message_id_;
  tl_object_ptr<td_api::ReplyMarkup> reply_markup_;
  tl_object_ptr<td_api::location> location_;
  int32 heading_;
  int32 proximity_alert_radius_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->edit_message_live_location(full_message_id_, std::move(reply_markup_), std::move(location_),
                                                       heading_, proximity_alert_radius_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(full_message_id_, "EditMessageLiveLocationRequest"));
  }

 public:
  EditMessageLiveLocationRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                                 tl_object_ptr<td_api::ReplyMarkup> reply_markup,
                                 tl_object_ptr<td_api::location> location, int32 heading, int32 proximity_alert_radius)
      : RequestOnceActor(std::move(td), request_id)
      , full_message_id_(DialogId(dialog_id), MessageId(message_id))
      , reply_markup_(std::move(reply_markup))
      , location_(std::move(location))
      , heading_(heading)
      , proximity_alert_radius_(proximity_alert_radius) {
  }
};

void Td::on_request(uint64 id, td_api::editMessageLiveLocation &request) {
  CREATE_REQUEST(EditMessageLiveLocationRequest, request.chat_id_, request.message_id_,
                 std::move(request.reply_markup_), std::move(request.location_), request.heading_,
                 request.proximity_alert_radius_);
}

class GetChatScheduledMessagesRequest final : public RequestActor<> {
  DialogId dialog_id_;

  vector<MessageId> message_ids_;

  void do_run(Promise<Unit> &&promise) final {
    message_ids_ =
        td_->messages_manager_->get_dialog_scheduled_messages(dialog_id_, get_tries() < 2, false, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_messages_object(-1, dialog_id_, message_ids_, true,
                                                            "GetChatScheduledMessagesRequest"));
  }

 public:
  GetChatScheduledMessagesRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id)
      : RequestActor(std::move(td), request_id), dialog_id_(dialog_id) {
    set_tries(4);
  }
};

void Td::on_request(uint64 id, const td_api::getChatScheduledMessages &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetChatScheduledMessagesRequest, request.chat_id_);
}

class SearchPublicChatsRequest final : public RequestActor<> {
  string query_;

  vector<DialogId> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->messages_manager_->search_public_dialogs(query_, std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(-1, dialog_ids_));
  }

 public:
  SearchPublicChatsRequest(ActorShared<Td> td, uint64 request_id, string query)
      : RequestActor(std::move(td), request_id), query_(std::move(query)) {
  }
};

void Td::on_request(uint64 id, td_api::searchPublicChats &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchPublicChatsRequest, request.query_);
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getPushReceiverId &request) {
  // don't check push payload UTF-8 correctness
  auto r_push_receiver_id = NotificationManager::get_push_receiver_id(request.payload_);
  if (r_push_receiver_id.is_error()) {
    VLOG(notifications) << "Failed to get push notification receiver from \"" << format::escaped(request.payload_)
                        << '"';
    return make_error(r_push_receiver_id.error().code(), r_push_receiver_id.error().message());
  }
  return td_api::make_object<td_api::pushReceiverId>(r_push_receiver_id.ok());
}

void Td::clear_requests() {
  while (!pending_alarms_.empty()) {
    auto it = pending_alarms_.begin();
    auto alarm_id = it->first;
    pending_alarms_.erase(it);
    alarm_timeout_.cancel_timeout(alarm_id);
  }
  while (!request_set_.empty()) {
    uint64 id = *request_set_.begin();
    if (destroy_flag_) {
      send_error_impl(id, make_error(401, "Unauthorized"));
    } else {
      send_error_impl(id, make_error(500, "Request aborted"));
    }
  }
}

void Td::on_request(uint64 id, td_api::getPassportAuthorizationFormAvailableElements &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  CREATE_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::get_passport_authorization_form_available_elements,
               request.autorization_form_id_, std::move(request.password_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::inviteGroupCallParticipants &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->invite_group_call_participants(GroupCallId(request.group_call_id_),
                                                      UserId::get_user_ids(request.user_ids_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getGameHighScores &request) {
  CHECK_IS_BOT();
  CREATE_REQUEST_PROMISE();
  game_manager_->get_game_high_scores({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                      UserId(request.user_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::reorderChatFilters &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->reorder_dialog_filters(
      transform(request.chat_filter_ids_, [](int32 id) { return DialogFilterId(id); }), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getTemporaryPasswordState &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::get_temp_password_state, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getChatAdministrators &request) {
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_dialog_administrators(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::testCallVectorInt &request) {
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::testVectorInt>(std::move(request.x_)));
}
}  // namespace td
