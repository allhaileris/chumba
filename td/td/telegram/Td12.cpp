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

class SaveAppLogQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit SaveAppLogQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &type, int64 peer_id, tl_object_ptr<telegram_api::JSONValue> &&data) {
    CHECK(data != nullptr);
    vector<telegram_api::object_ptr<telegram_api::inputAppEvent>> input_app_events;
    input_app_events.push_back(
        make_tl_object<telegram_api::inputAppEvent>(G()->server_time_cached(), type, peer_id, std::move(data)));
    send_query(G()->net_query_creator().create_unauth(telegram_api::help_saveAppLog(std::move(input_app_events))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_saveAppLog>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG_IF(ERROR, !result) << "Receive false from help.saveAppLog";
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

bool Td::is_preauthentication_request(int32 id) {
  switch (id) {
    case td_api::getInternalLinkType::ID:
    case td_api::getLocalizationTargetInfo::ID:
    case td_api::getLanguagePackInfo::ID:
    case td_api::getLanguagePackStrings::ID:
    case td_api::synchronizeLanguagePack::ID:
    case td_api::addCustomServerLanguagePack::ID:
    case td_api::setCustomLanguagePack::ID:
    case td_api::editCustomLanguagePackInfo::ID:
    case td_api::setCustomLanguagePackString::ID:
    case td_api::deleteLanguagePack::ID:
    case td_api::processPushNotification::ID:
    case td_api::getOption::ID:
    case td_api::setOption::ID:
    case td_api::getStorageStatistics::ID:
    case td_api::getStorageStatisticsFast::ID:
    case td_api::getDatabaseStatistics::ID:
    case td_api::setNetworkType::ID:
    case td_api::getNetworkStatistics::ID:
    case td_api::addNetworkStatistics::ID:
    case td_api::resetNetworkStatistics::ID:
    case td_api::getCountries::ID:
    case td_api::getCountryCode::ID:
    case td_api::getPhoneNumberInfo::ID:
    case td_api::getDeepLinkInfo::ID:
    case td_api::getApplicationConfig::ID:
    case td_api::saveApplicationLogEvent::ID:
    case td_api::addProxy::ID:
    case td_api::editProxy::ID:
    case td_api::enableProxy::ID:
    case td_api::disableProxy::ID:
    case td_api::removeProxy::ID:
    case td_api::getProxies::ID:
    case td_api::getProxyLink::ID:
    case td_api::pingProxy::ID:
    case td_api::testNetwork::ID:
      return true;
    default:
      return false;
  }
}

void Td::on_request(uint64 id, td_api::saveApplicationLogEvent &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.type_);
  auto result = convert_json_value(std::move(request.data_));
  CREATE_OK_REQUEST_PROMISE();
  create_handler<SaveAppLogQuery>(std::move(promise))->send(request.type_, request.chat_id_, std::move(result));
}

void Td::on_request(uint64 id, td_api::searchChatMembers &request) {
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST_PROMISE();
  auto query_promise =
      PromiseCreator::lambda([promise = std::move(promise), td = this](Result<DialogParticipants> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(result.ok().get_chat_members_object(td));
        }
      });
  contacts_manager_->search_dialog_participants(DialogId(request.chat_id_), request.query_, request.limit_,
                                                DialogParticipantsFilter(request.filter_), std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::canTransferOwnership &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda(
      [promise = std::move(promise)](Result<ContactsManager::CanTransferOwnershipResult> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(ContactsManager::get_can_transfer_ownership_result_object(result.ok()));
        }
      });
  contacts_manager_->can_transfer_ownership(std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::editProxy &request) {
  if (request.proxy_id_ < 0) {
    return send_error_raw(id, 400, "Proxy identifier invalid");
  }
  CLEAN_INPUT_STRING(request.server_);
  CREATE_REQUEST_PROMISE();
  send_closure(G()->connection_creator(), &ConnectionCreator::add_proxy, request.proxy_id_, std::move(request.server_),
               request.port_, request.enable_, std::move(request.type_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallScreenSharingIsPaused &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_is_my_presentation_paused(GroupCallId(request.group_call_id_),
                                                                   request.is_paused_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setChatNotificationSettings &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->set_dialog_notification_settings(DialogId(request.chat_id_),
                                                                          std::move(request.notification_settings_)));
}

void Td::on_request(uint64 id, const td_api::deleteFile &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(file_manager_actor_, &FileManager::delete_file, FileId(request.file_id_, 0), std::move(promise),
               "td_api::deleteFile");
}

void Td::on_request(uint64 id, const td_api::revokeGroupCallInviteLink &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->revoke_group_call_invite_link(GroupCallId(request.group_call_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::setChatClientData &request) {
  answer_ok_query(
      id, messages_manager_->set_dialog_client_data(DialogId(request.chat_id_), std::move(request.client_data_)));
}

void Td::on_request(uint64 id, const td_api::resetBackgrounds &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  background_manager_->reset_backgrounds(std::move(promise));
}
}  // namespace td
