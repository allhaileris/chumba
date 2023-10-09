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


#include "td/telegram/GroupCallId.h"
#include "td/telegram/GroupCallManager.h"


#include "td/telegram/JsonValue.h"
#include "td/telegram/LanguagePackManager.h"

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

void Td::on_request(uint64 id, td_api::addNetworkStatistics &request) {
  if (request.entry_ == nullptr) {
    return send_error_raw(id, 400, "Network statistics entry must be non-empty");
  }

  NetworkStatsEntry entry;
  switch (request.entry_->get_id()) {
    case td_api::networkStatisticsEntryFile::ID: {
      auto file_entry = move_tl_object_as<td_api::networkStatisticsEntryFile>(request.entry_);
      entry.is_call = false;
      if (file_entry->file_type_ != nullptr) {
        entry.file_type = get_file_type(*file_entry->file_type_);
      }
      entry.net_type = get_net_type(file_entry->network_type_);
      entry.rx = file_entry->received_bytes_;
      entry.tx = file_entry->sent_bytes_;
      break;
    }
    case td_api::networkStatisticsEntryCall::ID: {
      auto call_entry = move_tl_object_as<td_api::networkStatisticsEntryCall>(request.entry_);
      entry.is_call = true;
      entry.net_type = get_net_type(call_entry->network_type_);
      entry.rx = call_entry->received_bytes_;
      entry.tx = call_entry->sent_bytes_;
      entry.duration = call_entry->duration_;
      break;
    }
    default:
      UNREACHABLE();
  }

  if (entry.net_type == NetType::None) {
    return send_error_raw(id, 400, "Network statistics entry can't be increased for NetworkTypeNone");
  }
  if (entry.rx > (1ll << 40) || entry.rx < 0) {
    return send_error_raw(id, 400, "Wrong received bytes value");
  }
  if (entry.tx > (1ll << 40) || entry.tx < 0) {
    return send_error_raw(id, 400, "Wrong sent bytes value");
  }
  if (entry.count > (1 << 30) || entry.count < 0) {
    return send_error_raw(id, 400, "Wrong count value");
  }
  if (entry.duration > (1 << 30) || entry.duration < 0) {
    return send_error_raw(id, 400, "Wrong duration value");
  }

  send_closure(net_stats_manager_, &NetStatsManager::add_network_stats, entry);
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

class UpgradeGroupChatToSupergroupChatRequest final : public RequestActor<> {
  string title_;
  DialogId dialog_id_;

  DialogId result_dialog_id_;

  void do_run(Promise<Unit> &&promise) final {
    result_dialog_id_ = td_->messages_manager_->migrate_dialog_to_megagroup(dialog_id_, std::move(promise));
  }

  void do_send_result() final {
    CHECK(result_dialog_id_.is_valid());
    send_result(td_->messages_manager_->get_chat_object(result_dialog_id_));
  }

 public:
  UpgradeGroupChatToSupergroupChatRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id)
      : RequestActor(std::move(td), request_id), dialog_id_(dialog_id) {
  }
};

void Td::on_request(uint64 id, const td_api::upgradeBasicGroupChatToSupergroupChat &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(UpgradeGroupChatToSupergroupChatRequest, request.chat_id_);
}

void Td::on_request(uint64 id, td_api::addLocalMessage &request) {
  CHECK_IS_USER();

  DialogId dialog_id(request.chat_id_);
  auto r_new_message_id = messages_manager_->add_local_message(
      dialog_id, std::move(request.sender_id_), MessageId(request.reply_to_message_id_), request.disable_notification_,
      std::move(request.input_message_content_));
  if (r_new_message_id.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_new_message_id.move_as_error());
  }

  CHECK(r_new_message_id.ok().is_valid());
  send_closure(actor_id(this), &Td::send_result, id,
               messages_manager_->get_message_object({dialog_id, r_new_message_id.ok()}, "addLocalMessage"));
}

class GetSupportUserRequest final : public RequestActor<> {
  UserId user_id_;

  void do_run(Promise<Unit> &&promise) final {
    user_id_ = td_->contacts_manager_->get_support_user(std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_user_object(user_id_));
  }

 public:
  GetSupportUserRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getSupportUser &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetSupportUserRequest);
}

void Td::on_request(uint64 id, const td_api::getChatListsToAddChat &request) {
  CHECK_IS_USER();
  auto dialog_lists = messages_manager_->get_dialog_lists_to_add_dialog(DialogId(request.chat_id_));
  auto chat_lists =
      transform(dialog_lists, [](DialogListId dialog_list_id) { return dialog_list_id.get_chat_list_object(); });
  send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::chatLists>(std::move(chat_lists)));
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallMuteNewParticipants &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_mute_new_participants(GroupCallId(request.group_call_id_),
                                                               request.mute_new_participants_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::deleteLanguagePack &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_pack_id_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::delete_language, std::move(request.language_pack_id_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::joinChat &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->add_dialog_participant(DialogId(request.chat_id_), contacts_manager_->get_my_id(), 0,
                                            std::move(promise));
}

void Td::on_request(uint64 id, const td_api::resetAllNotificationSettings &request) {
  CHECK_IS_USER();
  messages_manager_->reset_all_notification_settings();
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::on_request(uint64 id, td_api::sendChatScreenshotTakenNotification &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->send_screenshot_taken_notification_message(DialogId(request.chat_id_)));
}

void Td::on_closed() {
  close_flag_ = 5;
  send_update(
      td_api::make_object<td_api::updateAuthorizationState>(td_api::make_object<td_api::authorizationStateClosed>()));
  dec_stop_cnt();
}

void Td::on_request(uint64 id, const td_api::getLogVerbosityLevel &request) {
  UNREACHABLE();
}
}  // namespace td
