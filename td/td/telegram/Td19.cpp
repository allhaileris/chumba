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

#include "td/telegram/TopDialogCategory.h"
#include "td/telegram/TopDialogManager.h"

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

class ImportContactsRequest final : public RequestActor<> {
  vector<Contact> contacts_;
  int64 random_id_;

  std::pair<vector<UserId>, vector<int32>> imported_contacts_;

  void do_run(Promise<Unit> &&promise) final {
    imported_contacts_ = td_->contacts_manager_->import_contacts(contacts_, random_id_, std::move(promise));
  }

  void do_send_result() final {
    CHECK(imported_contacts_.first.size() == contacts_.size());
    CHECK(imported_contacts_.second.size() == contacts_.size());
    send_result(make_tl_object<td_api::importedContacts>(transform(imported_contacts_.first,
                                                                   [this](UserId user_id) {
                                                                     return td_->contacts_manager_->get_user_id_object(
                                                                         user_id, "ImportContactsRequest");
                                                                   }),
                                                         std::move(imported_contacts_.second)));
  }

 public:
  ImportContactsRequest(ActorShared<Td> td, uint64 request_id, vector<Contact> &&contacts)
      : RequestActor(std::move(td), request_id), contacts_(std::move(contacts)), random_id_(0) {
    set_tries(3);  // load_contacts + import_contacts
  }
};

void Td::on_request(uint64 id, td_api::importContacts &request) {
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
  CREATE_REQUEST(ImportContactsRequest, std::move(contacts));
}

class UploadStickerFileRequest final : public RequestOnceActor {
  UserId user_id_;
  tl_object_ptr<td_api::InputSticker> sticker_;

  FileId file_id;

  void do_run(Promise<Unit> &&promise) final {
    file_id = td_->stickers_manager_->upload_sticker_file(user_id_, std::move(sticker_), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->file_manager_->get_file_object(file_id));
  }

 public:
  UploadStickerFileRequest(ActorShared<Td> td, uint64 request_id, int64 user_id,
                           tl_object_ptr<td_api::InputSticker> &&sticker)
      : RequestOnceActor(std::move(td), request_id), user_id_(user_id), sticker_(std::move(sticker)) {
  }
};

void Td::on_request(uint64 id, td_api::uploadStickerFile &request) {
  CREATE_REQUEST(UploadStickerFileRequest, request.user_id_, std::move(request.sticker_));
}

td_api::object_ptr<td_api::AuthorizationState> Td::get_fake_authorization_state_object() const {
  switch (state_) {
    case State::WaitParameters:
      return td_api::make_object<td_api::authorizationStateWaitTdlibParameters>();
    case State::Decrypt:
      return td_api::make_object<td_api::authorizationStateWaitEncryptionKey>(is_database_encrypted_);
    case State::Run:
      UNREACHABLE();
      return nullptr;
    case State::Close:
      if (close_flag_ == 5) {
        return td_api::make_object<td_api::authorizationStateClosed>();
      } else {
        return td_api::make_object<td_api::authorizationStateClosing>();
      }
    default:
      UNREACHABLE();
      return nullptr;
  }
}

class ClearRecentStickersRequest final : public RequestActor<> {
  bool is_attached_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->clear_recent_stickers(is_attached_, std::move(promise));
  }

 public:
  ClearRecentStickersRequest(ActorShared<Td> td, uint64 request_id, bool is_attached)
      : RequestActor(std::move(td), request_id), is_attached_(is_attached) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::clearRecentStickers &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(ClearRecentStickersRequest, request.is_attached_);
}

void Td::on_request(uint64 id, td_api::getBackgroundUrl &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.name_);
  Result<string> r_url = background_manager_->get_background_url(request.name_, std::move(request.type_));
  if (r_url.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_url.move_as_error());
  }

  send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::httpUrl>(r_url.ok()));
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallEnabledStartNotification &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_start_subscribed(GroupCallId(request.group_call_id_),
                                                          request.enabled_start_notification_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setChatDraftMessage &request) {
  CHECK_IS_USER();
  answer_ok_query(
      id, messages_manager_->set_dialog_draft_message(DialogId(request.chat_id_), MessageId(request.message_thread_id_),
                                                      std::move(request.draft_message_)));
}

void Td::on_request(uint64 id, const td_api::removeTopChat &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  top_dialog_manager_->remove_dialog(get_top_dialog_category(request.category_), DialogId(request.chat_id_),
                                     std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleSupergroupIsBroadcastGroup &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->convert_channel_to_gigagroup(ChannelId(request.supergroup_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setNetworkType &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(state_manager_, &StateManager::on_network, get_net_type(request.type_));
  promise.set_value(Unit());
}

void Td::on_request(uint64 id, const td_api::clearImportedContacts &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->clear_imported_contacts(std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getLogTags &request) {
  return td_api::make_object<td_api::logTags>(Logging::get_tags());
}
int VERBOSITY_NAME(td_requests) = VERBOSITY_NAME(INFO);
}  // namespace td
