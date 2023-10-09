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

#include <limits>

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

void Td::send_update(tl_object_ptr<td_api::Update> &&object) {
  CHECK(object != nullptr);
  auto object_id = object->get_id();
  if (close_flag_ >= 5 && object_id != td_api::updateAuthorizationState::ID) {
    // just in case
    return;
  }

  switch (object_id) {
    case td_api::updateChatThemes::ID:
    case td_api::updateFavoriteStickers::ID:
    case td_api::updateInstalledStickerSets::ID:
    case td_api::updateRecentStickers::ID:
    case td_api::updateSavedAnimations::ID:
    case td_api::updateUserStatus::ID:
      VLOG(td_requests) << "Sending update: " << oneline(to_string(object));
      break;
    case td_api::updateTrendingStickerSets::ID: {
      auto sticker_sets = static_cast<const td_api::updateTrendingStickerSets *>(object.get())->sticker_sets_.get();
      VLOG(td_requests) << "Sending update: updateTrendingStickerSets { total_count = " << sticker_sets->total_count_
                        << ", count = " << sticker_sets->sets_.size() << " }";
      break;
    }
    case td_api::updateAnimatedEmojiMessageClicked::ID / 2:
    case td_api::updateOption::ID / 2:
    case td_api::updateChatReadInbox::ID / 2:
    case td_api::updateUnreadMessageCount::ID / 2:
    case td_api::updateUnreadChatCount::ID / 2:
    case td_api::updateChatOnlineMemberCount::ID / 2:
    case td_api::updateChatAction::ID / 2:
    case td_api::updateChatFilters::ID / 2:
    case td_api::updateChatPosition::ID / 2:
      LOG(ERROR) << "Sending update: " << oneline(to_string(object));
      break;
    default:
      VLOG(td_requests) << "Sending update: " << to_string(object);
  }

  callback_->on_result(0, std::move(object));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::parseMarkdown &request) {
  if (request.text_ == nullptr) {
    return make_error(400, "Text must be non-empty");
  }

  auto r_entities = get_message_entities(nullptr, std::move(request.text_->entities_), true);
  if (r_entities.is_error()) {
    return make_error(400, r_entities.error().message());
  }
  auto entities = r_entities.move_as_ok();
  auto status = fix_formatted_text(request.text_->text_, entities, true, true, true, true, true);
  if (status.is_error()) {
    return make_error(400, status.error().message());
  }

  auto parsed_text = parse_markdown_v3({std::move(request.text_->text_), std::move(entities)});
  fix_formatted_text(parsed_text.text, parsed_text.entities, true, true, true, true, true).ensure();
  return get_formatted_text_object(parsed_text, false, std::numeric_limits<int32>::max());
}

void Td::on_request(uint64 id, td_api::getNetworkStatistics &request) {
  if (!request.only_current_ && G()->shared_config().get_option_boolean("disable_persistent_network_statistics")) {
    return send_error_raw(id, 400, "Persistent network statistics is disabled");
  }
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<NetworkStats> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(result.ok().get_network_statistics_object());
    }
  });
  send_closure(net_stats_manager_, &NetStatsManager::get_network_stats, request.only_current_,
               std::move(query_promise));
}

class GetUserRequest final : public RequestActor<> {
  UserId user_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->get_user(user_id_, get_tries(), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_user_object(user_id_));
  }

 public:
  GetUserRequest(ActorShared<Td> td, uint64 request_id, int64 user_id)
      : RequestActor(std::move(td), request_id), user_id_(user_id) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getUser &request) {
  CREATE_REQUEST(GetUserRequest, request.user_id_);
}

void Td::on_request(uint64 id, const td_api::getAccountTtl &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<int32> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(td_api::make_object<td_api::accountTtl>(result.ok()));
    }
  });
  get_account_ttl(this, std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::sendPhoneNumberConfirmationCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.phone_number_);
  CLEAN_INPUT_STRING(request.hash_);
  send_closure(confirm_phone_number_manager_, &PhoneNumberManager::set_phone_number_and_hash, id,
               std::move(request.hash_), std::move(request.phone_number_), std::move(request.settings_));
}

void Td::on_request(uint64 id, td_api::getCallbackQueryAnswer &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  callback_queries_manager_->send_callback_query({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                                 std::move(request.payload_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::sendPhoneNumberVerificationCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.phone_number_);
  send_closure(verify_phone_number_manager_, &PhoneNumberManager::set_phone_number, id,
               std::move(request.phone_number_), std::move(request.settings_));
}

void Td::on_request(uint64 id, td_api::checkPhoneNumberVerificationCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.code_);
  send_closure(verify_phone_number_manager_, &PhoneNumberManager::check_code, id, std::move(request.code_));
}

void Td::on_request(uint64 id, td_api::checkChangePhoneNumberCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.code_);
  send_closure(change_phone_number_manager_, &PhoneNumberManager::check_code, id, std::move(request.code_));
}

void Td::on_request(uint64 id, td_api::checkAuthenticationCode &request) {
  CLEAN_INPUT_STRING(request.code_);
  send_closure(auth_manager_actor_, &AuthManager::check_code, id, std::move(request.code_));
}

void Td::on_request(uint64 id, const td_api::getFile &request) {
  send_closure(actor_id(this), &Td::send_result, id, file_manager_->get_file_object(FileId(request.file_id_, 0)));
}
}  // namespace td
