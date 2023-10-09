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

td_api::object_ptr<td_api::Object> Td::static_request(td_api::object_ptr<td_api::Function> function) {
  if (function == nullptr) {
    return td_api::make_object<td_api::error>(400, "Request is empty");
  }

  auto function_id = function->get_id();
  bool need_logging = [function_id] {
    switch (function_id) {
      case td_api::parseTextEntities::ID:
      case td_api::parseMarkdown::ID:
      case td_api::getMarkdownText::ID:
      case td_api::getFileMimeType::ID:
      case td_api::getFileExtension::ID:
      case td_api::cleanFileName::ID:
      case td_api::getChatFilterDefaultIconName::ID:
      case td_api::getJsonValue::ID:
      case td_api::getJsonString::ID:
      case td_api::testReturnError::ID:
        return true;
      default:
        return false;
    }
  }();

  if (need_logging) {
    VLOG(td_requests) << "Receive static request: " << to_string(function);
  }

  td_api::object_ptr<td_api::Object> response;
  downcast_call(*function, [&response](auto &request) { response = Td::do_static_request(request); });
  LOG_CHECK(response != nullptr) << function_id;

  if (need_logging) {
    VLOG(td_requests) << "Sending result for static request: " << to_string(response);
  }
  return response;
}

class EditMessageReplyMarkupRequest final : public RequestOnceActor {
  FullMessageId full_message_id_;
  tl_object_ptr<td_api::ReplyMarkup> reply_markup_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->edit_message_reply_markup(full_message_id_, std::move(reply_markup_), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(full_message_id_, "EditMessageReplyMarkupRequest"));
  }

 public:
  EditMessageReplyMarkupRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                                tl_object_ptr<td_api::ReplyMarkup> reply_markup)
      : RequestOnceActor(std::move(td), request_id)
      , full_message_id_(DialogId(dialog_id), MessageId(message_id))
      , reply_markup_(std::move(reply_markup)) {
  }
};

void Td::on_request(uint64 id, td_api::editMessageReplyMarkup &request) {
  CHECK_IS_BOT();
  CREATE_REQUEST(EditMessageReplyMarkupRequest, request.chat_id_, request.message_id_,
                 std::move(request.reply_markup_));
}

class GetActiveLiveLocationMessagesRequest final : public RequestActor<> {
  vector<FullMessageId> full_message_ids_;

  void do_run(Promise<Unit> &&promise) final {
    full_message_ids_ = td_->messages_manager_->get_active_live_location_messages(std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_messages_object(-1, full_message_ids_, true,
                                                            "GetActiveLiveLocationMessagesRequest"));
  }

 public:
  GetActiveLiveLocationMessagesRequest(ActorShared<Td> td, uint64 request_id)
      : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getActiveLiveLocationMessages &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetActiveLiveLocationMessagesRequest);
}

void Td::on_request(uint64 id, td_api::getSupergroupMembers &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise =
      PromiseCreator::lambda([promise = std::move(promise), td = this](Result<DialogParticipants> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(result.ok().get_chat_members_object(td));
        }
      });
  contacts_manager_->get_channel_participants(ChannelId(request.supergroup_id_), std::move(request.filter_), string(),
                                              request.offset_, request.limit_, -1, std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::answerInlineQuery &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.next_offset_);
  CLEAN_INPUT_STRING(request.switch_pm_text_);
  CLEAN_INPUT_STRING(request.switch_pm_parameter_);
  CREATE_OK_REQUEST_PROMISE();
  inline_queries_manager_->answer_inline_query(
      request.inline_query_id_, request.is_personal_, std::move(request.results_), request.cache_time_,
      request.next_offset_, request.switch_pm_text_, request.switch_pm_parameter_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setScopeNotificationSettings &request) {
  CHECK_IS_USER();
  if (request.scope_ == nullptr) {
    return send_error_raw(id, 400, "Scope must be non-empty");
  }
  answer_ok_query(id, messages_manager_->set_scope_notification_settings(
                          get_notification_settings_scope(request.scope_), std::move(request.notification_settings_)));
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallIsMyVideoPaused &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_is_my_video_paused(GroupCallId(request.group_call_id_),
                                                            request.is_my_video_paused_, std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::setLogStream &request) {
  auto result = Logging::set_current_stream(std::move(request.log_stream_));
  if (result.is_ok()) {
    return td_api::make_object<td_api::ok>();
  } else {
    return make_error(400, result.message());
  }
}

void Td::on_request(uint64 id, td_api::setSupergroupUsername &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.username_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_channel_username(ChannelId(request.supergroup_id_), request.username_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setChatPermissions &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_dialog_permissions(DialogId(request.chat_id_), request.permissions_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getPhoneNumberInfo &request) {
  CREATE_REQUEST_PROMISE();
  country_info_manager_->get_phone_number_info(request.phone_number_prefix_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::terminateAllOtherSessions &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  terminate_all_other_sessions(this, std::move(promise));
}
}  // namespace td
