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

#include "td/telegram/LinkManager.h"
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

class GetInviteTextQuery final : public Td::ResultHandler {
  Promise<string> promise_;

 public:
  explicit GetInviteTextQuery(Promise<string> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::help_getInviteText()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_getInviteText>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    promise_.set_value(std::move(result->message_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void Td::on_request(uint64 id, const td_api::getApplicationDownloadLink &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::httpUrl>(result.move_as_ok()));
    }
  });
  create_handler<GetInviteTextQuery>(std::move(query_promise))->send();
}

class GetUserProfilePhotosRequest final : public RequestActor<> {
  UserId user_id_;
  int32 offset_;
  int32 limit_;
  std::pair<int32, vector<const Photo *>> photos;

  void do_run(Promise<Unit> &&promise) final {
    photos = td_->contacts_manager_->get_user_profile_photos(user_id_, offset_, limit_, std::move(promise));
  }

  void do_send_result() final {
    // TODO create function get_user_profile_photos_object
    auto result = transform(photos.second, [file_manager = td_->file_manager_.get()](const Photo *photo) {
      CHECK(photo != nullptr);
      CHECK(!photo->is_empty());
      return get_chat_photo_object(file_manager, *photo);
    });

    send_result(make_tl_object<td_api::chatPhotos>(photos.first, std::move(result)));
  }

 public:
  GetUserProfilePhotosRequest(ActorShared<Td> td, uint64 request_id, int64 user_id, int32 offset, int32 limit)
      : RequestActor(std::move(td), request_id), user_id_(user_id), offset_(offset), limit_(limit) {
  }
};

void Td::on_request(uint64 id, const td_api::getUserProfilePhotos &request) {
  CREATE_REQUEST(GetUserProfilePhotosRequest, request.user_id_, request.offset_, request.limit_);
}

class GetChatRequest final : public RequestActor<> {
  DialogId dialog_id_;

  bool dialog_found_ = false;

  void do_run(Promise<Unit> &&promise) final {
    dialog_found_ = td_->messages_manager_->load_dialog(dialog_id_, get_tries(), std::move(promise));
  }

  void do_send_result() final {
    if (!dialog_found_) {
      send_error(Status::Error(400, "Chat is not accessible"));
    } else {
      send_result(td_->messages_manager_->get_chat_object(dialog_id_));
    }
  }

 public:
  GetChatRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id)
      : RequestActor(std::move(td), request_id), dialog_id_(dialog_id) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getChat &request) {
  CREATE_REQUEST(GetChatRequest, request.chat_id_);
}

void Td::on_request(uint64 id, td_api::sendMessageAlbum &request) {
  DialogId dialog_id(request.chat_id_);
  auto r_message_ids = messages_manager_->send_message_group(
      dialog_id, MessageId(request.message_thread_id_), MessageId(request.reply_to_message_id_),
      std::move(request.options_), std::move(request.input_message_contents_));
  if (r_message_ids.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_message_ids.move_as_error());
  }

  send_closure(actor_id(this), &Td::send_result, id,
               messages_manager_->get_messages_object(-1, dialog_id, r_message_ids.ok(), false, "sendMessageAlbum"));
}

void Td::on_request(uint64 id, td_api::setPassportElement &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  auto r_secure_value = get_secure_value(file_manager_.get(), std::move(request.element_));
  if (r_secure_value.is_error()) {
    return send_error_raw(id, 400, r_secure_value.error().message());
  }
  CREATE_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::set_secure_value, std::move(request.password_),
               r_secure_value.move_as_ok(), std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getTextEntities &request) {
  if (!check_utf8(request.text_)) {
    return make_error(400, "Text must be encoded in UTF-8");
  }
  auto text_entities = find_entities(request.text_, false, false);
  return make_tl_object<td_api::textEntities>(
      get_text_entities_object(text_entities, false, std::numeric_limits<int32>::max()));
}

void Td::on_request(uint64 id, const td_api::setAlarm &request) {
  if (request.seconds_ < 0 || request.seconds_ > 3e9) {
    return send_error_raw(id, 400, "Wrong parameter seconds specified");
  }

  int64 alarm_id = alarm_id_++;
  pending_alarms_.emplace(alarm_id, id);
  alarm_timeout_.set_timeout_in(alarm_id, request.seconds_);
}

void Td::on_request(uint64 id, const td_api::getLoginUrl &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  link_manager_->get_login_url({DialogId(request.chat_id_), MessageId(request.message_id_)}, request.button_id_,
                               request.allow_write_access_, std::move(promise));
}

void Td::answer_ok_query(uint64 id, Status status) {
  if (status.is_error()) {
    send_closure(actor_id(this), &Td::send_error, id, std::move(status));
  } else {
    send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
  }
}

void Td::on_request(uint64 id, td_api::setChatTitle &request) {
  CLEAN_INPUT_STRING(request.title_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_dialog_title(DialogId(request.chat_id_), request.title_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::checkAuthenticationBotToken &request) {
  CLEAN_INPUT_STRING(request.token_);
  send_closure(auth_manager_actor_, &AuthManager::check_bot_token, id, std::move(request.token_));
}

void Td::on_request(uint64 id, const td_api::testUseUpdate &request) {
  send_closure(actor_id(this), &Td::send_result, id, nullptr);
}

void Td::on_request(uint64 id, const td_api::cleanFileName &request) {
  UNREACHABLE();
}
}  // namespace td
