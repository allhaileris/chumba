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
#include "td/telegram/SponsoredMessageManager.h"
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

#include "td/telegram/WebPagesManager.h"

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

class GetChatNotificationSettingsExceptionsRequest final : public RequestActor<> {
  NotificationSettingsScope scope_;
  bool filter_scope_;
  bool compare_sound_;

  vector<DialogId> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->messages_manager_->get_dialog_notification_settings_exceptions(
        scope_, filter_scope_, compare_sound_, get_tries() < 3, std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(-1, dialog_ids_));
  }

 public:
  GetChatNotificationSettingsExceptionsRequest(ActorShared<Td> td, uint64 request_id, NotificationSettingsScope scope,
                                               bool filter_scope, bool compare_sound)
      : RequestActor(std::move(td), request_id)
      , scope_(scope)
      , filter_scope_(filter_scope)
      , compare_sound_(compare_sound) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getChatNotificationSettingsExceptions &request) {
  CHECK_IS_USER();
  bool filter_scope = false;
  NotificationSettingsScope scope = NotificationSettingsScope::Private;
  if (request.scope_ != nullptr) {
    filter_scope = true;
    scope = get_notification_settings_scope(request.scope_);
  }
  CREATE_REQUEST(GetChatNotificationSettingsExceptionsRequest, scope, filter_scope, request.compare_sound_);
}

class CreateNewGroupChatRequest final : public RequestActor<> {
  vector<UserId> user_ids_;
  string title_;
  int64 random_id_;

  DialogId dialog_id_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_id_ = td_->messages_manager_->create_new_group_chat(user_ids_, title_, random_id_, std::move(promise));
  }

  void do_send_result() final {
    CHECK(dialog_id_.is_valid());
    send_result(td_->messages_manager_->get_chat_object(dialog_id_));
  }

 public:
  CreateNewGroupChatRequest(ActorShared<Td> td, uint64 request_id, vector<UserId> user_ids, string title)
      : RequestActor(std::move(td), request_id)
      , user_ids_(std::move(user_ids))
      , title_(std::move(title))
      , random_id_(0) {
  }
};

void Td::on_request(uint64 id, td_api::createNewBasicGroupChat &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.title_);
  CREATE_REQUEST(CreateNewGroupChatRequest, UserId::get_user_ids(request.user_ids_), std::move(request.title_));
}

class GetWebPagePreviewRequest final : public RequestOnceActor {
  td_api::object_ptr<td_api::formattedText> text_;

  int64 request_id_ = 0;

  void do_run(Promise<Unit> &&promise) final {
    request_id_ = td_->web_pages_manager_->get_web_page_preview(std::move(text_), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->web_pages_manager_->get_web_page_preview_result(request_id_));
  }

 public:
  GetWebPagePreviewRequest(ActorShared<Td> td, uint64 request_id, td_api::object_ptr<td_api::formattedText> text)
      : RequestOnceActor(std::move(td), request_id), text_(std::move(text)) {
  }
};

void Td::on_request(uint64 id, td_api::getWebPagePreview &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetWebPagePreviewRequest, std::move(request.text_));
}

class GetStickerSetRequest final : public RequestActor<> {
  StickerSetId set_id_;

  StickerSetId sticker_set_id_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_id_ = td_->stickers_manager_->get_sticker_set(set_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_set_object(sticker_set_id_));
  }

 public:
  GetStickerSetRequest(ActorShared<Td> td, uint64 request_id, int64 set_id)
      : RequestActor(std::move(td), request_id), set_id_(set_id) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getStickerSet &request) {
  CREATE_REQUEST(GetStickerSetRequest, request.set_id_);
}

void Td::on_request(uint64 id, td_api::getChatInviteLinks &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.offset_invite_link_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_dialog_invite_links(DialogId(request.chat_id_), UserId(request.creator_user_id_),
                                             request.is_revoked_, request.offset_date_, request.offset_invite_link_,
                                             request.limit_, std::move(promise));
}

td_api::object_ptr<td_api::updateTermsOfService> Td::get_update_terms_of_service_object() const {
  auto terms_of_service = pending_terms_of_service_.get_terms_of_service_object();
  if (terms_of_service == nullptr) {
    return nullptr;
  }
  return td_api::make_object<td_api::updateTermsOfService>(pending_terms_of_service_.get_id().str(),
                                                           std::move(terms_of_service));
}

void Td::on_request(uint64 id, td_api::addProxy &request) {
  CLEAN_INPUT_STRING(request.server_);
  CREATE_REQUEST_PROMISE();
  send_closure(G()->connection_creator(), &ConnectionCreator::add_proxy, -1, std::move(request.server_), request.port_,
               request.enable_, std::move(request.type_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setFileGenerationProgress &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(file_manager_actor_, &FileManager::external_file_generate_progress, request.generation_id_,
               request.expected_size_, request.local_prefix_size_, std::move(promise));
}

std::shared_ptr<Td::ResultHandler> Td::extract_handler(uint64 id) {
  auto it = result_handlers_.find(id);
  if (it == result_handlers_.end()) {
    return nullptr;
  }
  auto result = std::move(it->second);
  result_handlers_.erase(it);
  return result;
}

void Td::on_request(uint64 id, const td_api::getChatSponsoredMessage &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  sponsored_message_manager_->get_dialog_sponsored_message(DialogId(request.chat_id_), std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::cleanFileName &request) {
  // don't check file name UTF-8 correctness
  return make_tl_object<td_api::text>(clean_filename(request.file_name_));
}

void Td::on_request(uint64 id, const td_api::getPhoneNumberInfoSync &request) {
  UNREACHABLE();
}

void Td::on_request(uint64 id, const td_api::getFileExtension &request) {
  UNREACHABLE();
}
}  // namespace td
