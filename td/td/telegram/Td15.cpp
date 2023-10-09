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

class SearchChatMessagesRequest final : public RequestActor<> {
  DialogId dialog_id_;
  string query_;
  td_api::object_ptr<td_api::MessageSender> sender_id_;
  MessageId from_message_id_;
  int32 offset_;
  int32 limit_;
  MessageSearchFilter filter_;
  MessageId top_thread_message_id_;
  int64 random_id_;

  std::pair<int32, vector<MessageId>> messages_;

  void do_run(Promise<Unit> &&promise) final {
    messages_ = td_->messages_manager_->search_dialog_messages(dialog_id_, query_, sender_id_, from_message_id_,
                                                               offset_, limit_, filter_, top_thread_message_id_,
                                                               random_id_, get_tries() == 3, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_messages_object(messages_.first, dialog_id_, messages_.second, true,
                                                            "SearchChatMessagesRequest"));
  }

  void do_send_error(Status &&status) final {
    if (status.message() == "SEARCH_QUERY_EMPTY") {
      messages_.first = 0;
      messages_.second.clear();
      return do_send_result();
    }
    send_error(std::move(status));
  }

 public:
  SearchChatMessagesRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, string query,
                            td_api::object_ptr<td_api::MessageSender> sender_id, int64 from_message_id, int32 offset,
                            int32 limit, tl_object_ptr<td_api::SearchMessagesFilter> filter, int64 message_thread_id)
      : RequestActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , query_(std::move(query))
      , sender_id_(std::move(sender_id))
      , from_message_id_(from_message_id)
      , offset_(offset)
      , limit_(limit)
      , filter_(get_message_search_filter(filter))
      , top_thread_message_id_(message_thread_id)
      , random_id_(0) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::searchChatMessages &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchChatMessagesRequest, request.chat_id_, std::move(request.query_), std::move(request.sender_id_),
                 request.from_message_id_, request.offset_, request.limit_, std::move(request.filter_),
                 request.message_thread_id_);
}

class GetSavedAnimationsRequest final : public RequestActor<> {
  vector<FileId> animation_ids_;

  void do_run(Promise<Unit> &&promise) final {
    animation_ids_ = td_->animations_manager_->get_saved_animations(std::move(promise));
  }

  void do_send_result() final {
    send_result(
        make_tl_object<td_api::animations>(transform(std::move(animation_ids_), [td_ = td_](FileId animation_id) {
          return td_->animations_manager_->get_animation_object(animation_id);
        })));
  }

 public:
  GetSavedAnimationsRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getSavedAnimations &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetSavedAnimationsRequest);
}

void Td::on_request(uint64 id, td_api::checkChatUsername &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.username_);
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda(
      [promise = std::move(promise)](Result<ContactsManager::CheckDialogUsernameResult> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(ContactsManager::get_check_chat_username_result_object(result.ok()));
        }
      });
  contacts_manager_->check_dialog_username(DialogId(request.chat_id_), request.username_, std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::getMessageImportConfirmationText &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::text>(result.move_as_ok()));
    }
  });
  messages_manager_->get_message_import_confirmation_text(DialogId(request.chat_id_), std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::acceptCall &request) {
  CHECK_IS_USER();
  if (request.protocol_ == nullptr) {
    return send_error_raw(id, 400, "Call protocol must be non-empty");
  }
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->call_manager(), &CallManager::accept_call, CallId(request.call_id_),
               CallProtocol(*request.protocol_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getMessageStatistics &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_channel_message_statistics({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                                    request.is_dark_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleChatIsMarkedAsUnread &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->toggle_dialog_is_marked_as_unread(DialogId(request.chat_id_),
                                                                           request.is_marked_as_unread_));
}

void Td::on_request(uint64 id, const td_api::resendEmailAddressVerificationCode &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::resend_email_address_verification_code, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::clearRecentlyFoundChats &request) {
  CHECK_IS_USER();
  messages_manager_->clear_recently_found_dialogs();
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::on_request(uint64 id, const td_api::getApplicationConfig &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(G()->config_manager(), &ConfigManager::get_app_config, std::move(promise));
}

ActorShared<Td> Td::create_reference() {
  inc_actor_refcnt();
  return actor_shared(this, ActorIdType);
}

void Td::on_request(uint64 id, const td_api::testReturnError &request) {
  UNREACHABLE();
}
}  // namespace td
