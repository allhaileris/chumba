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

class EditMessageCaptionRequest final : public RequestOnceActor {
  FullMessageId full_message_id_;
  tl_object_ptr<td_api::ReplyMarkup> reply_markup_;
  tl_object_ptr<td_api::formattedText> caption_;

  void do_run(Promise<Unit> &&promise) final {
    td_->messages_manager_->edit_message_caption(full_message_id_, std::move(reply_markup_), std::move(caption_),
                                                 std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_object(full_message_id_, "EditMessageCaptionRequest"));
  }

 public:
  EditMessageCaptionRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                            tl_object_ptr<td_api::ReplyMarkup> reply_markup,
                            tl_object_ptr<td_api::formattedText> caption)
      : RequestOnceActor(std::move(td), request_id)
      , full_message_id_(DialogId(dialog_id), MessageId(message_id))
      , reply_markup_(std::move(reply_markup))
      , caption_(std::move(caption)) {
  }
};

void Td::on_request(uint64 id, td_api::editMessageCaption &request) {
  CREATE_REQUEST(EditMessageCaptionRequest, request.chat_id_, request.message_id_, std::move(request.reply_markup_),
                 std::move(request.caption_));
}

class GetScopeNotificationSettingsRequest final : public RequestActor<> {
  NotificationSettingsScope scope_;

  const ScopeNotificationSettings *notification_settings_ = nullptr;

  void do_run(Promise<Unit> &&promise) final {
    notification_settings_ = td_->messages_manager_->get_scope_notification_settings(scope_, std::move(promise));
  }

  void do_send_result() final {
    CHECK(notification_settings_ != nullptr);
    send_result(get_scope_notification_settings_object(notification_settings_));
  }

 public:
  GetScopeNotificationSettingsRequest(ActorShared<Td> td, uint64 request_id, NotificationSettingsScope scope)
      : RequestActor(std::move(td), request_id), scope_(scope) {
  }
};

void Td::on_request(uint64 id, const td_api::getScopeNotificationSettings &request) {
  CHECK_IS_USER();
  if (request.scope_ == nullptr) {
    return send_error_raw(id, 400, "Scope must be non-empty");
  }
  CREATE_REQUEST(GetScopeNotificationSettingsRequest, get_notification_settings_scope(request.scope_));
}

void Td::on_request(uint64 id, const td_api::leaveChat &request) {
  CREATE_OK_REQUEST_PROMISE();
  DialogId dialog_id(request.chat_id_);
  auto new_status = DialogParticipantStatus::Left();
  if (dialog_id.get_type() == DialogType::Channel && messages_manager_->have_dialog_force(dialog_id, "leaveChat")) {
    auto status = contacts_manager_->get_channel_status(dialog_id.get_channel_id());
    if (status.is_creator()) {
      if (!status.is_member()) {
        return promise.set_value(Unit());
      }

      new_status = DialogParticipantStatus::Creator(false, status.is_anonymous(), status.get_rank());
    }
  }
  contacts_manager_->set_dialog_participant_status(dialog_id, DialogId(contacts_manager_->get_my_id()),
                                                   std::move(new_status), std::move(promise));
}

class GetUserFullInfoRequest final : public RequestActor<> {
  UserId user_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->load_user_full(user_id_, get_tries() < 2, std::move(promise), "GetUserFullInfoRequest");
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_user_full_info_object(user_id_));
  }

 public:
  GetUserFullInfoRequest(ActorShared<Td> td, uint64 request_id, int64 user_id)
      : RequestActor(std::move(td), request_id), user_id_(user_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getUserFullInfo &request) {
  CREATE_REQUEST(GetUserFullInfoRequest, request.user_id_);
}

class GetMeRequest final : public RequestActor<> {
  UserId user_id_;

  void do_run(Promise<Unit> &&promise) final {
    user_id_ = td_->contacts_manager_->get_me(std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_user_object(user_id_));
  }

 public:
  GetMeRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getMe &request) {
  CREATE_NO_ARGS_REQUEST(GetMeRequest);
}

void Td::on_request(uint64 id, td_api::addContact &request) {
  CHECK_IS_USER();
  auto r_contact = get_contact(std::move(request.contact_));
  if (r_contact.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_contact.move_as_error());
  }
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->add_contact(r_contact.move_as_ok(), request.share_phone_number_, std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getLogTagVerbosityLevel &request) {
  auto result = Logging::get_tag_verbosity_level(request.tag_);
  if (result.is_ok()) {
    return td_api::make_object<td_api::logVerbosityLevel>(result.ok());
  } else {
    return make_error(400, result.error().message());
  }
}

void Td::on_request(uint64 id, const td_api::addChatMembers &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->add_dialog_participants(DialogId(request.chat_id_), UserId::get_user_ids(request.user_ids_),
                                             std::move(promise));
}

void Td::on_request(uint64 id, td_api::setUserPrivacySettingRules &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  send_closure(privacy_manager_, &PrivacyManager::set_privacy, std::move(request.setting_), std::move(request.rules_),
               std::move(promise));
}

void Td::on_request(uint64 id, td_api::deleteCommands &request) {
  CHECK_IS_BOT();
  CREATE_OK_REQUEST_PROMISE();
  delete_commands(this, std::move(request.scope_), std::move(request.language_code_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::removeRecentlyFoundChat &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->remove_recently_found_dialog(DialogId(request.chat_id_)));
}

void Td::on_request(uint64 id, const td_api::getProxies &request) {
  CREATE_REQUEST_PROMISE();
  send_closure(G()->connection_creator(), &ConnectionCreator::get_proxies, std::move(promise));
}
}  // namespace td
