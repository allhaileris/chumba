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
#include "td/telegram/PhoneNumberManager.h"
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

class GetMessageThreadHistoryRequest final : public RequestActor<> {
  DialogId dialog_id_;
  MessageId message_id_;
  MessageId from_message_id_;
  int32 offset_;
  int32 limit_;
  int64 random_id_;

  std::pair<DialogId, vector<MessageId>> messages_;

  void do_run(Promise<Unit> &&promise) final {
    messages_ = td_->messages_manager_->get_message_thread_history(dialog_id_, message_id_, from_message_id_, offset_,
                                                                   limit_, random_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_messages_object(-1, messages_.first, messages_.second, true,
                                                            "GetMessageThreadHistoryRequest"));
  }

 public:
  GetMessageThreadHistoryRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 message_id,
                                 int64 from_message_id, int32 offset, int32 limit)
      : RequestActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , message_id_(message_id)
      , from_message_id_(from_message_id)
      , offset_(offset)
      , limit_(limit)
      , random_id_(0) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getMessageThreadHistory &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetMessageThreadHistoryRequest, request.chat_id_, request.message_id_, request.from_message_id_,
                 request.offset_, request.limit_);
}

class GetMessageLinkInfoRequest final : public RequestActor<MessageLinkInfo> {
  string url_;

  MessageLinkInfo message_link_info_;

  void do_run(Promise<MessageLinkInfo> &&promise) final {
    if (get_tries() < 2) {
      promise.set_value(std::move(message_link_info_));
      return;
    }
    td_->messages_manager_->get_message_link_info(url_, std::move(promise));
  }

  void do_set_result(MessageLinkInfo &&result) final {
    message_link_info_ = std::move(result);
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_message_link_info_object(message_link_info_));
  }

 public:
  GetMessageLinkInfoRequest(ActorShared<Td> td, uint64 request_id, string url)
      : RequestActor(std::move(td), request_id), url_(std::move(url)) {
  }
};

void Td::on_request(uint64 id, td_api::getMessageLinkInfo &request) {
  CLEAN_INPUT_STRING(request.url_);
  CREATE_REQUEST(GetMessageLinkInfoRequest, std::move(request.url_));
}

class SearchStickerSetRequest final : public RequestActor<> {
  string name_;

  StickerSetId sticker_set_id_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_id_ = td_->stickers_manager_->search_sticker_set(name_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_set_object(sticker_set_id_));
  }

 public:
  SearchStickerSetRequest(ActorShared<Td> td, uint64 request_id, string &&name)
      : RequestActor(std::move(td), request_id), name_(std::move(name)) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::searchStickerSet &request) {
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST(SearchStickerSetRequest, std::move(request.name_));
}

void Td::on_request(uint64 id, const td_api::getMessageLink &request) {
  auto r_message_link =
      messages_manager_->get_message_link({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                          request.media_timestamp_, request.for_album_, request.for_comment_);
  if (r_message_link.is_error()) {
    send_closure(actor_id(this), &Td::send_error, id, r_message_link.move_as_error());
  } else {
    send_closure(actor_id(this), &Td::send_result, id,
                 td_api::make_object<td_api::messageLink>(r_message_link.ok().first, r_message_link.ok().second));
  }
}

void Td::on_request(uint64 id, const td_api::pingProxy &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<double> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::seconds>(result.move_as_ok()));
    }
  });
  send_closure(G()->connection_creator(), &ConnectionCreator::ping_proxy, request.proxy_id_, std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::setAutoDownloadSettings &request) {
  CHECK_IS_USER();
  if (request.settings_ == nullptr) {
    return send_error_raw(id, 400, "New settings must be non-empty");
  }
  CREATE_OK_REQUEST_PROMISE();
  set_auto_download_settings(this, get_net_type(request.type_), get_auto_download_settings(request.settings_),
                             std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::getPhoneNumberInfoSync &request) {
  // don't check language_code/phone number UTF-8 correctness
  return CountryInfoManager::get_phone_number_info_sync(request.language_code_,
                                                        std::move(request.phone_number_prefix_));
}

void Td::on_request(uint64 id, td_api::changePhoneNumber &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.phone_number_);
  send_closure(change_phone_number_manager_, &PhoneNumberManager::set_phone_number, id,
               std::move(request.phone_number_), std::move(request.settings_));
}

void Td::on_request(uint64 id, td_api::answerPreCheckoutQuery &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.error_message_);
  CREATE_OK_REQUEST_PROMISE();
  answer_pre_checkout_query(this, request.pre_checkout_query_id_, request.error_message_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::readAllChatMentions &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->read_all_dialog_mentions(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setLocation &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_location(Location(request.location_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteSavedCredentials &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  delete_saved_credentials(this, std::move(promise));
}
}  // namespace td
