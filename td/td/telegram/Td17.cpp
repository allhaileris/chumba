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
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"


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

class ChangeImportedContactsRequest final : public RequestActor<> {
  vector<Contact> contacts_;
  size_t contacts_size_;
  int64 random_id_;

  std::pair<vector<UserId>, vector<int32>> imported_contacts_;

  void do_run(Promise<Unit> &&promise) final {
    imported_contacts_ = td_->contacts_manager_->change_imported_contacts(contacts_, random_id_, std::move(promise));
  }

  void do_send_result() final {
    CHECK(imported_contacts_.first.size() == contacts_size_);
    CHECK(imported_contacts_.second.size() == contacts_size_);
    send_result(make_tl_object<td_api::importedContacts>(transform(imported_contacts_.first,
                                                                   [this](UserId user_id) {
                                                                     return td_->contacts_manager_->get_user_id_object(
                                                                         user_id, "ChangeImportedContactsRequest");
                                                                   }),
                                                         std::move(imported_contacts_.second)));
  }

 public:
  ChangeImportedContactsRequest(ActorShared<Td> td, uint64 request_id, vector<Contact> &&contacts)
      : RequestActor(std::move(td), request_id)
      , contacts_(std::move(contacts))
      , contacts_size_(contacts_.size())
      , random_id_(0) {
    set_tries(4);  // load_contacts + load_local_contacts + (import_contacts + delete_contacts)
  }
};

void Td::on_request(uint64 id, td_api::changeImportedContacts &request) {
  CHECK_IS_USER();
  vector<Contact> contacts;
  contacts.reserve(request.contacts_.size());
  for (auto &contact : request.contacts_) {
    auto r_contact = get_contact(std::move(contact));
    if (r_contact.is_error()) {
      return send_closure(actor_id(this), &Td::send_error, id, r_contact.move_as_error());
    }
    contacts.push_back(r_contact.move_as_ok());
  }
  CREATE_REQUEST(ChangeImportedContactsRequest, std::move(contacts));
}

class GetEmojiSuggestionsUrlRequest final : public RequestOnceActor {
  string language_code_;

  int64 random_id_;

  void do_run(Promise<Unit> &&promise) final {
    random_id_ = td_->stickers_manager_->get_emoji_suggestions_url(language_code_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_emoji_suggestions_url_result(random_id_));
  }

 public:
  GetEmojiSuggestionsUrlRequest(ActorShared<Td> td, uint64 request_id, string &&language_code)
      : RequestOnceActor(std::move(td), request_id), language_code_(std::move(language_code)), random_id_(0) {
  }
};

void Td::on_request(uint64 id, td_api::getEmojiSuggestionsUrl &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_code_);
  CREATE_REQUEST(GetEmojiSuggestionsUrlRequest, std::move(request.language_code_));
}

void Td::on_request(uint64 id, td_api::sendBotStartMessage &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.parameter_);

  DialogId dialog_id(request.chat_id_);
  auto r_new_message_id =
      messages_manager_->send_bot_start_message(UserId(request.bot_user_id_), dialog_id, request.parameter_);
  if (r_new_message_id.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_new_message_id.move_as_error());
  }

  CHECK(r_new_message_id.ok().is_valid() || r_new_message_id.ok().is_valid_scheduled());
  send_closure(actor_id(this), &Td::send_result, id,
               messages_manager_->get_message_object({dialog_id, r_new_message_id.ok()}, "sendBotStartMessage"));
}

void Td::on_request(uint64 id, const td_api::getFileDownloadedPrefixSize &request) {
  if (request.offset_ < 0) {
    return send_error_raw(id, 400, "Parameter offset must be non-negative");
  }
  auto file_view = file_manager_->get_file_view(FileId(request.file_id_, 0));
  if (file_view.empty()) {
    return send_closure(actor_id(this), &Td::send_error, id, Status::Error(400, "Unknown file ID"));
  }
  send_closure(actor_id(this), &Td::send_result, id,
               td_api::make_object<td_api::count>(narrow_cast<int32>(file_view.downloaded_prefix(request.offset_))));
}

void Td::on_request(uint64 id, td_api::finishFileGeneration &request) {
  Status status;
  if (request.error_ != nullptr) {
    CLEAN_INPUT_STRING(request.error_->message_);
    status = Status::Error(request.error_->code_, request.error_->message_);
  }
  CREATE_OK_REQUEST_PROMISE();
  send_closure(file_manager_actor_, &FileManager::external_file_generate_finish, request.generation_id_,
               std::move(status), std::move(promise));
}

void Td::on_request(uint64 id, td_api::processChatJoinRequests &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->process_dialog_join_requests(DialogId(request.chat_id_), request.invite_link_, request.approve_,
                                                  std::move(promise));
}

void Td::on_request(uint64 id, td_api::getPreferredCountryLanguage &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.country_code_);
  CREATE_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::get_preferred_country_language, std::move(request.country_code_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getMessageViewers &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_message_viewers({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                         std::move(promise));
}

void Td::on_request(uint64 id, const td_api::resendRecoveryEmailAddressCode &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::resend_recovery_email_address_code, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::leaveGroupCall &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->leave_group_call(GroupCallId(request.group_call_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::disconnectWebsite &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  disconnect_website(this, request.website_id_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::addLogMessage &request) {
  UNREACHABLE();
}
}  // namespace td
