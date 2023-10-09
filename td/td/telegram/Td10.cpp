//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/Td.h"

#include "td/telegram/Account.h"
#include "td/telegram/AnimationsManager.h"
#include "td/telegram/AudiosManager.h"
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
#include "td/telegram/ConfigShared.h"
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
#include "td/telegram/DocumentsManager.h"
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

#include "td/telegram/InlineQueriesManager.h"
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
#include "td/telegram/TopDialogCategory.h"
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

void Td::dec_actor_refcnt() {
  actor_refcnt_--;
  if (actor_refcnt_ < 3) {
    LOG(DEBUG) << "Decrease reference count to " << actor_refcnt_;
  }
  if (actor_refcnt_ == 0) {
    if (close_flag_ == 2) {
      create_reference();
      close_flag_ = 3;
    } else if (close_flag_ == 3) {
      LOG(INFO) << "All actors were closed";
      Timer timer;
      animations_manager_.reset();
      LOG(DEBUG) << "AnimationsManager was cleared" << timer;
      audios_manager_.reset();
      LOG(DEBUG) << "AudiosManager was cleared" << timer;
      auth_manager_.reset();
      LOG(DEBUG) << "AuthManager was cleared" << timer;
      background_manager_.reset();
      LOG(DEBUG) << "BackgroundManager was cleared" << timer;
      callback_queries_manager_.reset();
      LOG(DEBUG) << "CallbackQueriesManager was cleared" << timer;
      contacts_manager_.reset();
      LOG(DEBUG) << "ContactsManager was cleared" << timer;
      country_info_manager_.reset();
      LOG(DEBUG) << "CountryInfoManager was cleared" << timer;
      documents_manager_.reset();
      LOG(DEBUG) << "DocumentsManager was cleared" << timer;
      file_manager_.reset();
      LOG(DEBUG) << "FileManager was cleared" << timer;
      file_reference_manager_.reset();
      LOG(DEBUG) << "FileReferenceManager was cleared" << timer;
      game_manager_.reset();
      LOG(DEBUG) << "GameManager was cleared" << timer;
      group_call_manager_.reset();
      LOG(DEBUG) << "GroupCallManager was cleared" << timer;
      inline_queries_manager_.reset();
      LOG(DEBUG) << "InlineQueriesManager was cleared" << timer;
      link_manager_.reset();
      LOG(DEBUG) << "LinkManager was cleared" << timer;
      messages_manager_.reset();
      LOG(DEBUG) << "MessagesManager was cleared" << timer;
      notification_manager_.reset();
      LOG(DEBUG) << "NotificationManager was cleared" << timer;
      option_manager_.reset();
      LOG(DEBUG) << "OptionManager was cleared" << timer;
      poll_manager_.reset();
      LOG(DEBUG) << "PollManager was cleared" << timer;
      sponsored_message_manager_.reset();
      LOG(DEBUG) << "SponsoredMessageManager was cleared" << timer;
      stickers_manager_.reset();
      LOG(DEBUG) << "StickersManager was cleared" << timer;
      theme_manager_.reset();
      LOG(DEBUG) << "ThemeManager was cleared" << timer;
      top_dialog_manager_.reset();
      LOG(DEBUG) << "TopDialogManager was cleared" << timer;
      updates_manager_.reset();
      LOG(DEBUG) << "UpdatesManager was cleared" << timer;
      video_notes_manager_.reset();
      LOG(DEBUG) << "VideoNotesManager was cleared" << timer;
      videos_manager_.reset();
      LOG(DEBUG) << "VideosManager was cleared" << timer;
      voice_notes_manager_.reset();
      LOG(DEBUG) << "VoiceNotesManager was cleared" << timer;
      web_pages_manager_.reset();
      LOG(DEBUG) << "WebPagesManager was cleared" << timer;
      Promise<> promise = PromiseCreator::lambda([actor_id = create_reference()](Unit) mutable { actor_id.reset(); });

      G()->set_shared_config(nullptr);
      if (destroy_flag_) {
        G()->close_and_destroy_all(std::move(promise));
      } else {
        G()->close_all(std::move(promise));
      }
      // NetQueryDispatcher will be closed automatically
      close_flag_ = 4;
    } else if (close_flag_ == 4) {
      on_closed();
    } else {
      UNREACHABLE();
    }
  }
}

void Td::on_request(uint64 id, const td_api::getTopChats &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<vector<DialogId>> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(MessagesManager::get_chats_object(-1, result.ok()));
    }
  });
  top_dialog_manager_->get_top_dialogs(get_top_dialog_category(request.category_), request.limit_,
                                       std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::recoverPassword &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.recovery_code_);
  CLEAN_INPUT_STRING(request.new_password_);
  CLEAN_INPUT_STRING(request.new_hint_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::recover_password, std::move(request.recovery_code_),
               std::move(request.new_password_), std::move(request.new_hint_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getChatMember &request) {
  CREATE_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, participant_dialog_id,
                     get_message_sender_dialog_id(this, request.member_id_, false, false));
  contacts_manager_->get_dialog_participant(DialogId(request.chat_id_), participant_dialog_id, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::reportSupergroupSpam &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->report_channel_spam(ChannelId(request.supergroup_id_),
                                         MessagesManager::get_message_ids(request.message_ids_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::getInlineGameHighScores &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_REQUEST_PROMISE();
  game_manager_->get_inline_game_high_scores(request.inline_message_id_, UserId(request.user_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::destroy &request) {
  // send response before actually destroying
  send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::ok>());
  send_closure(actor_id(this), &Td::destroy);
}

void Td::on_request(uint64 id, td_api::setOption &request) {
  CLEAN_INPUT_STRING(request.name_);
  CREATE_OK_REQUEST_PROMISE();
  option_manager_->set_option(request.name_, std::move(request.value_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::resendChangePhoneNumberCode &request) {
  CHECK_IS_USER();
  send_closure(change_phone_number_manager_, &PhoneNumberManager::resend_authentication_code, id);
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getLogVerbosityLevel &request) {
  return td_api::make_object<td_api::logVerbosityLevel>(Logging::get_verbosity_level());
}
}  // namespace td
