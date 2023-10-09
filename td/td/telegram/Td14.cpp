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



#include "td/telegram/UpdatesManager.h"
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

class GetPromoDataQuery final : public Td::ResultHandler {
  Promise<telegram_api::object_ptr<telegram_api::help_PromoData>> promise_;

 public:
  explicit GetPromoDataQuery(Promise<telegram_api::object_ptr<telegram_api::help_PromoData>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send() {
    // we don't poll promo data before authorization
    send_query(G()->net_query_creator().create(telegram_api::help_getPromoData()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_getPromoData>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void Td::on_alarm_timeout(int64 alarm_id) {
  if (alarm_id == ONLINE_ALARM_ID) {
    on_online_updated(false, true);
    return;
  }
  if (alarm_id == PING_SERVER_ALARM_ID) {
    if (!close_flag_ && updates_manager_ != nullptr && auth_manager_->is_authorized()) {
      updates_manager_->ping_server();
      alarm_timeout_.set_timeout_in(PING_SERVER_ALARM_ID,
                                    PING_SERVER_TIMEOUT + Random::fast(0, PING_SERVER_TIMEOUT / 5));
      set_is_bot_online(false);
    }
    return;
  }
  if (alarm_id == TERMS_OF_SERVICE_ALARM_ID) {
    if (!close_flag_ && !auth_manager_->is_bot()) {
      get_terms_of_service(
          this, PromiseCreator::lambda([actor_id = actor_id(this)](Result<std::pair<int32, TermsOfService>> result) {
            send_closure(actor_id, &Td::on_get_terms_of_service, std::move(result), false);
          }));
    }
    return;
  }
  if (alarm_id == PROMO_DATA_ALARM_ID) {
    if (!close_flag_ && !auth_manager_->is_bot()) {
      auto promise = PromiseCreator::lambda(
          [actor_id = actor_id(this)](Result<telegram_api::object_ptr<telegram_api::help_PromoData>> result) {
            send_closure(actor_id, &Td::on_get_promo_data, std::move(result), false);
          });
      create_handler<GetPromoDataQuery>(std::move(promise))->send();
    }
    return;
  }
  if (close_flag_ >= 2) {
    // pending_alarms_ was already cleared
    return;
  }

  auto it = pending_alarms_.find(alarm_id);
  CHECK(it != pending_alarms_.end());
  uint64 request_id = it->second;
  pending_alarms_.erase(alarm_id);
  send_result(request_id, make_tl_object<td_api::ok>());
}

class SearchStickerSetsRequest final : public RequestActor<> {
  string query_;

  vector<StickerSetId> sticker_set_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_ids_ = td_->stickers_manager_->search_sticker_sets(query_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_sets_object(-1, sticker_set_ids_, 5));
  }

 public:
  SearchStickerSetsRequest(ActorShared<Td> td, uint64 request_id, string &&query)
      : RequestActor(std::move(td), request_id), query_(std::move(query)) {
  }
};

void Td::on_request(uint64 id, td_api::searchStickerSets &request) {
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchStickerSetsRequest, std::move(request.query_));
}

void Td::on_request(uint64 id, const td_api::getGroupCallInviteLink &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(td_api::make_object<td_api::httpUrl>(result.move_as_ok()));
    }
  });
  group_call_manager_->get_group_call_invite_link(GroupCallId(request.group_call_id_), request.can_self_unmute_,
                                                  std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::setChatMessageSender &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, message_sender_dialog_id,
                     get_message_sender_dialog_id(this, request.message_sender_id_, true, false));
  messages_manager_->set_dialog_default_send_message_as_dialog_id(DialogId(request.chat_id_), message_sender_dialog_id,
                                                                  std::move(promise));
}

void Td::on_request(uint64 id, td_api::getLanguagePackStrings &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_pack_id_);
  for (auto &key : request.keys_) {
    CLEAN_INPUT_STRING(key);
  }
  CREATE_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::get_language_pack_strings,
               std::move(request.language_pack_id_), std::move(request.keys_), std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::setLogVerbosityLevel &request) {
  auto result = Logging::set_verbosity_level(static_cast<int>(request.new_verbosity_level_));
  if (result.is_ok()) {
    return td_api::make_object<td_api::ok>();
  } else {
    return make_error(400, result.message());
  }
}

void Td::on_request(uint64 id, td_api::getRecoveryEmailAddress &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.password_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::get_recovery_email_address, std::move(request.password_),
               std::move(promise));
}

void Td::on_request(uint64 id, td_api::setChatDescription &request) {
  CLEAN_INPUT_STRING(request.description_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_dialog_description(DialogId(request.chat_id_), request.description_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getPaymentReceipt &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  get_payment_receipt(this, {DialogId(request.chat_id_), MessageId(request.message_id_)}, std::move(promise));
}

void Td::on_request(uint64 id, td_api::getCommands &request) {
  CHECK_IS_BOT();
  CREATE_REQUEST_PROMISE();
  get_commands(this, std::move(request.scope_), std::move(request.language_code_), std::move(promise));
}

void Td::send_error_raw(uint64 id, int32 code, CSlice error) {
  send_closure(actor_id(this), &Td::send_error_impl, id, make_error(code, error));
}

void Td::on_request(uint64 id, const td_api::setLogStream &request) {
  UNREACHABLE();
}
}  // namespace td
