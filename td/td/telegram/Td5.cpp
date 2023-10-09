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

#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileGcParameters.h"
#include "td/telegram/files/FileId.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"
#include "td/telegram/GameManager.h"
#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"
#include "td/telegram/GroupCallManager.h"
#include "td/telegram/HashtagHints.h"
#include "td/telegram/InlineQueriesManager.h"
#include "td/telegram/JsonValue.h"
#include "td/telegram/LanguagePackManager.h"
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
#include "td/telegram/NotificationManager.h"
#include "td/telegram/NotificationSettings.h"
#include "td/telegram/OptionManager.h"
#include "td/telegram/PasswordManager.h"
#include "td/telegram/Payments.h"
#include "td/telegram/PhoneNumberManager.h"
#include "td/telegram/Photo.h"
#include "td/telegram/PhotoSizeSource.h"
#include "td/telegram/PollManager.h"
#include "td/telegram/PrivacyManager.h"

#include "td/telegram/ReportReason.h"
#include "td/telegram/RequestActor.h"
#include "td/telegram/SecretChatId.h"
#include "td/telegram/SecretChatsManager.h"
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
#include "td/telegram/ThemeManager.h"

#include "td/telegram/TopDialogManager.h"
#include "td/telegram/UpdatesManager.h"
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

void Td::clear() {
  if (close_flag_ >= 2) {
    return;
  }

  LOG(INFO) << "Clear Td";
  close_flag_ = 2;

  Timer timer;
  if (destroy_flag_) {
    OptionManager::clear_options();
    if (!auth_manager_->is_bot()) {
      notification_manager_->destroy_all_notifications();
    }
  } else {
    if (!auth_manager_->is_bot()) {
      notification_manager_->flush_all_notifications();
    }
  }

  G()->net_query_creator().stop_check();
  result_handlers_.clear();
  LOG(DEBUG) << "Handlers were cleared" << timer;
  G()->net_query_dispatcher().stop();
  LOG(DEBUG) << "NetQueryDispatcher was stopped" << timer;
  state_manager_.reset();
  LOG(DEBUG) << "StateManager was cleared" << timer;
  clear_requests();
  if (is_online_) {
    is_online_ = false;
    alarm_timeout_.cancel_timeout(ONLINE_ALARM_ID);
  }
  alarm_timeout_.cancel_timeout(PING_SERVER_ALARM_ID);
  alarm_timeout_.cancel_timeout(TERMS_OF_SERVICE_ALARM_ID);
  alarm_timeout_.cancel_timeout(PROMO_DATA_ALARM_ID);
  LOG(DEBUG) << "Requests were answered" << timer;

  // close all pure actors
  call_manager_.reset();
  LOG(DEBUG) << "CallManager was cleared" << timer;
  change_phone_number_manager_.reset();
  LOG(DEBUG) << "ChangePhoneNumberManager was cleared" << timer;
  config_manager_.reset();
  LOG(DEBUG) << "ConfigManager was cleared" << timer;
  confirm_phone_number_manager_.reset();
  LOG(DEBUG) << "ConfirmPhoneNumberManager was cleared" << timer;
  device_token_manager_.reset();
  LOG(DEBUG) << "DeviceTokenManager was cleared" << timer;
  hashtag_hints_.reset();
  LOG(DEBUG) << "HashtagHints was cleared" << timer;
  language_pack_manager_.reset();
  LOG(DEBUG) << "LanguagePackManager was cleared" << timer;
  net_stats_manager_.reset();
  LOG(DEBUG) << "NetStatsManager was cleared" << timer;
  password_manager_.reset();
  LOG(DEBUG) << "PasswordManager was cleared" << timer;
  privacy_manager_.reset();
  LOG(DEBUG) << "PrivacyManager was cleared" << timer;
  secure_manager_.reset();
  LOG(DEBUG) << "SecureManager was cleared" << timer;
  secret_chats_manager_.reset();
  LOG(DEBUG) << "SecretChatsManager was cleared" << timer;
  storage_manager_.reset();
  LOG(DEBUG) << "StorageManager was cleared" << timer;
  verify_phone_number_manager_.reset();
  LOG(DEBUG) << "VerifyPhoneNumberManager was cleared" << timer;

  G()->set_connection_creator(ActorOwn<ConnectionCreator>());
  LOG(DEBUG) << "ConnectionCreator was cleared" << timer;
  G()->set_temp_auth_key_watchdog(ActorOwn<TempAuthKeyWatchdog>());
  LOG(DEBUG) << "TempAuthKeyWatchdog was cleared" << timer;

  // clear actors which are unique pointers
  animations_manager_actor_.reset();
  LOG(DEBUG) << "AnimationsManager actor was cleared" << timer;
  auth_manager_actor_.reset();
  LOG(DEBUG) << "AuthManager actor was cleared" << timer;
  background_manager_actor_.reset();
  LOG(DEBUG) << "BackgroundManager actor was cleared" << timer;
  contacts_manager_actor_.reset();
  LOG(DEBUG) << "ContactsManager actor was cleared" << timer;
  country_info_manager_actor_.reset();
  LOG(DEBUG) << "CountryInfoManager actor was cleared" << timer;
  file_manager_actor_.reset();
  LOG(DEBUG) << "FileManager actor was cleared" << timer;
  file_reference_manager_actor_.reset();
  LOG(DEBUG) << "FileReferenceManager actor was cleared" << timer;
  game_manager_actor_.reset();
  LOG(DEBUG) << "GameManager actor was cleared" << timer;
  group_call_manager_actor_.reset();
  LOG(DEBUG) << "GroupCallManager actor was cleared" << timer;
  inline_queries_manager_actor_.reset();
  LOG(DEBUG) << "InlineQueriesManager actor was cleared" << timer;
  link_manager_actor_.reset();
  LOG(DEBUG) << "LinkManager actor was cleared" << timer;
  messages_manager_actor_.reset();  // TODO: Stop silent
  LOG(DEBUG) << "MessagesManager actor was cleared" << timer;
  notification_manager_actor_.reset();
  LOG(DEBUG) << "NotificationManager actor was cleared" << timer;
  option_manager_actor_.reset();
  LOG(DEBUG) << "OptionManager actor was cleared" << timer;
  poll_manager_actor_.reset();
  LOG(DEBUG) << "PollManager actor was cleared" << timer;
  sponsored_message_manager_actor_.reset();
  LOG(DEBUG) << "SponsoredMessageManager actor was cleared" << timer;
  stickers_manager_actor_.reset();
  LOG(DEBUG) << "StickersManager actor was cleared" << timer;
  theme_manager_actor_.reset();
  LOG(DEBUG) << "ThemeManager actor was cleared" << timer;
  top_dialog_manager_actor_.reset();
  LOG(DEBUG) << "TopDialogManager actor was cleared" << timer;
  updates_manager_actor_.reset();
  LOG(DEBUG) << "UpdatesManager actor was cleared" << timer;
  web_pages_manager_actor_.reset();
  LOG(DEBUG) << "WebPagesManager actor was cleared" << timer;
}

void Td::on_request(uint64 id, td_api::sendCallDebugInformation &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.debug_information_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->call_manager(), &CallManager::send_call_debug_information, CallId(request.call_id_),
               std::move(request.debug_information_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setPinnedChats &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->set_pinned_dialogs(
                          DialogListId(request.chat_list_),
                          transform(request.chat_ids_, [](int64 chat_id) { return DialogId(chat_id); })));
}

void Td::on_request(uint64 id, const td_api::unpinChatMessage &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->pin_dialog_message(DialogId(request.chat_id_), MessageId(request.message_id_), false, false, true,
                                        std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleMessageSenderIsBlocked &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->toggle_message_sender_is_blocked(request.sender_id_, request.is_blocked_));
}

void Td::on_alarm_timeout_callback(void *td_ptr, int64 alarm_id) {
  auto td = static_cast<Td *>(td_ptr);
  auto td_id = td->actor_id(td);
  send_closure_later(td_id, &Td::on_alarm_timeout, alarm_id);
}

void Td::on_request(uint64 id, td_api::setProfilePhoto &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_profile_photo(request.photo_, std::move(promise));
}
}  // namespace td
