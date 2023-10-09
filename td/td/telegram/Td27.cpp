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

#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"



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
#include "td/telegram/NotificationManager.h"
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

class SearchSecretMessagesRequest final : public RequestActor<> {
  DialogId dialog_id_;
  string query_;
  string offset_;
  int32 limit_;
  MessageSearchFilter filter_;
  int64 random_id_;

  MessagesManager::FoundMessages found_messages_;

  void do_run(Promise<Unit> &&promise) final {
    found_messages_ = td_->messages_manager_->offline_search_messages(dialog_id_, query_, offset_, limit_, filter_,
                                                                      random_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->messages_manager_->get_found_messages_object(found_messages_, "SearchSecretMessagesRequest"));
  }

 public:
  SearchSecretMessagesRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, string query, string offset,
                              int32 limit, tl_object_ptr<td_api::SearchMessagesFilter> filter)
      : RequestActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , query_(std::move(query))
      , offset_(std::move(offset))
      , limit_(limit)
      , filter_(get_message_search_filter(filter))
      , random_id_(0) {
  }
};

void Td::on_request(uint64 id, td_api::searchSecretMessages &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CLEAN_INPUT_STRING(request.offset_);
  CREATE_REQUEST(SearchSecretMessagesRequest, request.chat_id_, std::move(request.query_), std::move(request.offset_),
                 request.limit_, std::move(request.filter_));
}

class TestNetworkQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit TestNetworkQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create_unauth(telegram_api::help_getConfig()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_getConfig>(packet);
    if (result_ptr.is_error()) {
      return on_error(Status::Error(500, "Fetch failed"));
    }

    LOG(DEBUG) << "TestNetwork OK: " << to_string(result_ptr.ok());
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(ERROR) << "Test query failed: " << status;
    promise_.set_error(std::move(status));
  }
};

// test
void Td::on_request(uint64 id, const td_api::testNetwork &request) {
  CREATE_OK_REQUEST_PROMISE();
  create_handler<TestNetworkQuery>(std::move(promise))->send();
}

void Td::on_update(BufferSlice &&update) {
  if (close_flag_ > 1) {
    return;
  }

  TlBufferParser parser(&update);
  auto ptr = telegram_api::Updates::fetch(parser);
  parser.fetch_end();
  if (parser.get_error()) {
    LOG(ERROR) << "Failed to fetch update: " << parser.get_error() << format::as_hex_dump<4>(update.as_slice());
    updates_manager_->schedule_get_difference("failed to fetch update");
  } else {
    updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
    if (auth_manager_->is_bot() && auth_manager_->is_authorized()) {
      alarm_timeout_.set_timeout_in(PING_SERVER_ALARM_ID,
                                    PING_SERVER_TIMEOUT + Random::fast(0, PING_SERVER_TIMEOUT / 5));
      set_is_bot_online(true);
    }
  }
}

class GetGroupRequest final : public RequestActor<> {
  ChatId chat_id_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->get_chat(chat_id_, get_tries(), std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_basic_group_object(chat_id_));
  }

 public:
  GetGroupRequest(ActorShared<Td> td, uint64 request_id, int64 chat_id)
      : RequestActor(std::move(td), request_id), chat_id_(chat_id) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, const td_api::getBasicGroup &request) {
  CREATE_REQUEST(GetGroupRequest, request.basic_group_id_);
}
void Td::on_request(uint64 id, td_api::getDatabaseStatistics &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<DatabaseStats> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(result.ok().get_database_statistics_object());
    }
  });
  send_closure(storage_manager_, &StorageManager::get_database_stats, std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::removeNotification &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  notification_manager_->remove_notification(NotificationGroupId(request.notification_group_id_),
                                             NotificationId(request.notification_id_), false, true, std::move(promise),
                                             "td_api::removeNotification");
}

void Td::on_request(uint64 id, const td_api::discardCall &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->call_manager(), &CallManager::discard_call, CallId(request.call_id_), request.is_disconnected_,
               request.duration_, request.is_video_, request.connection_id_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::getLanguagePackInfo &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.language_pack_id_);
  CREATE_REQUEST_PROMISE();
  send_closure(language_pack_manager_, &LanguagePackManager::search_language_info, request.language_pack_id_,
               std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getFileMimeType &request) {
  // don't check file name UTF-8 correctness
  return make_tl_object<td_api::text>(MimeType::from_extension(PathView(request.file_name_).extension()));
}

void Td::on_request(uint64 id, const td_api::close &request) {
  // send response before actually closing
  send_closure(actor_id(this), &Td::send_result, id, td_api::make_object<td_api::ok>());
  send_closure(actor_id(this), &Td::close);
}

void Td::on_request(uint64 id, td_api::deleteAccount &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.reason_);
  send_closure(auth_manager_actor_, &AuthManager::delete_account, id, request.reason_);
}

void Td::on_request(uint64 id, const td_api::logOut &request) {
  // will call Td::destroy later
  send_closure(auth_manager_actor_, &AuthManager::log_out, id);
}

int VERBOSITY_NAME(td_init) = VERBOSITY_NAME(DEBUG) + 3;
}  // namespace td
