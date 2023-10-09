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
#include "td/telegram/ConfigShared.h"
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

class GetChatHistoryRequest final : public RequestActor<> {
  DialogId dialog_id_;
  MessageId from_message_id_;
  int32 offset_;
  int32 limit_;
  bool only_local_;

  tl_object_ptr<td_api::messages> messages_;

  void do_run(Promise<Unit> &&promise) final {
    messages_ = td_->messages_manager_->get_dialog_history(dialog_id_, from_message_id_, offset_, limit_,
                                                           get_tries() - 1, only_local_, std::move(promise));
  }

  void do_send_result() final {
    send_result(std::move(messages_));
  }

 public:
  GetChatHistoryRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 from_message_id, int32 offset,
                        int32 limit, bool only_local)
      : RequestActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , from_message_id_(from_message_id)
      , offset_(offset)
      , limit_(limit)
      , only_local_(only_local) {
    if (!only_local_) {
      set_tries(4);
    }
  }
};

void Td::on_request(uint64 id, const td_api::getChatHistory &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetChatHistoryRequest, request.chat_id_, request.from_message_id_, request.offset_, request.limit_,
                 request.only_local_);
}

class JoinChatByInviteLinkRequest final : public RequestActor<DialogId> {
  string invite_link_;

  DialogId dialog_id_;

  void do_run(Promise<DialogId> &&promise) final {
    if (get_tries() < 2) {
      promise.set_value(std::move(dialog_id_));
      return;
    }
    td_->contacts_manager_->import_dialog_invite_link(invite_link_, std::move(promise));
  }

  void do_set_result(DialogId &&result) final {
    dialog_id_ = result;
  }

  void do_send_result() final {
    CHECK(dialog_id_.is_valid());
    td_->messages_manager_->force_create_dialog(dialog_id_, "join chat via an invite link");
    send_result(td_->messages_manager_->get_chat_object(dialog_id_));
  }

 public:
  JoinChatByInviteLinkRequest(ActorShared<Td> td, uint64 request_id, string invite_link)
      : RequestActor(std::move(td), request_id), invite_link_(std::move(invite_link)) {
  }
};

void Td::on_request(uint64 id, td_api::joinChatByInviteLink &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.invite_link_);
  CREATE_REQUEST(JoinChatByInviteLinkRequest, request.invite_link_);
}

void Td::on_request(uint64 id, td_api::getGroupCallStreamSegment &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      auto file_part = td_api::make_object<td_api::filePart>();
      file_part->data_ = result.move_as_ok();
      promise.set_value(std::move(file_part));
    }
  });
  group_call_manager_->get_group_call_stream_segment(GroupCallId(request.group_call_id_), request.time_offset_,
                                                     request.scale_, request.channel_id_,
                                                     std::move(request.video_quality_), std::move(query_promise));
}

class GetSecretChatRequest final : public RequestActor<> {
  SecretChatId secret_chat_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->get_secret_chat(secret_chat_id_, get_tries() < 2, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_secret_chat_object(secret_chat_id_));
  }

 public:
  GetSecretChatRequest(ActorShared<Td> td, uint64 request_id, int32 secret_chat_id)
      : RequestActor(std::move(td), request_id), secret_chat_id_(secret_chat_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getSecretChat &request) {
  CREATE_REQUEST(GetSecretChatRequest, request.secret_chat_id_);
}

void Td::on_request(uint64 id, const td_api::toggleGroupCallParticipantIsHandRaised &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, participant_dialog_id,
                     get_message_sender_dialog_id(this, request.participant_id_, true, false));
  group_call_manager_->toggle_group_call_participant_is_hand_raised(
      GroupCallId(request.group_call_id_), participant_dialog_id, request.is_hand_raised_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::blockMessageSenderFromReplies &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->block_message_sender_from_replies(MessageId(request.message_id_), request.delete_message_,
                                                       request.delete_all_messages_, request.report_spam_,
                                                       std::move(promise));
}

void Td::set_is_bot_online(bool is_bot_online) {
  if (G()->shared_config().get_option_integer("session_count") > 1) {
    is_bot_online = false;
  }

  if (is_bot_online == is_bot_online_) {
    return;
  }

  is_bot_online_ = is_bot_online;
  send_closure(G()->state_manager(), &StateManager::on_online, is_bot_online_);
}

void Td::on_request(uint64 id, const td_api::setChatDiscussionGroup &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_channel_discussion_group(DialogId(request.chat_id_), DialogId(request.discussion_chat_id_),
                                                  std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getLocalizationTargetInfo &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::get_languages, request.only_local_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::startScheduledGroupCall &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->start_scheduled_group_call(GroupCallId(request.group_call_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::resendPhoneNumberConfirmationCode &request) {
  CHECK_IS_USER();
  send_closure(confirm_phone_number_manager_, &PhoneNumberManager::resend_authentication_code, id);
}

void Td::on_request(uint64 id, const td_api::openChat &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->open_dialog(DialogId(request.chat_id_)));
}

void Td::inc_actor_refcnt() {
  actor_refcnt_++;
}
}  // namespace td
