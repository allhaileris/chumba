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

#include "td/telegram/HashtagHints.h"

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

class AddStickerToSetRequest final : public RequestOnceActor {
  UserId user_id_;
  string name_;
  tl_object_ptr<td_api::InputSticker> sticker_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->add_sticker_to_set(user_id_, name_, std::move(sticker_), std::move(promise));
  }

  void do_send_result() final {
    auto set_id = td_->stickers_manager_->search_sticker_set(name_, Auto());
    if (!set_id.is_valid()) {
      return send_error(Status::Error(500, "Sticker set not found"));
    }
    send_result(td_->stickers_manager_->get_sticker_set_object(set_id));
  }

 public:
  AddStickerToSetRequest(ActorShared<Td> td, uint64 request_id, int64 user_id, string &&name,
                         tl_object_ptr<td_api::InputSticker> &&sticker)
      : RequestOnceActor(std::move(td), request_id)
      , user_id_(user_id)
      , name_(std::move(name))
      , sticker_(std::move(sticker)) {
  }
};

void Td::on_request(uint64 id, td_api::addStickerToSet &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST(AddStickerToSetRequest, request.user_id_, std::move(request.name_), std::move(request.sticker_));
}

class AnswerCustomQueryQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit AnswerCustomQueryQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(int64 custom_query_id, const string &data) {
    send_query(G()->net_query_creator().create(
        telegram_api::bots_answerWebhookJSONQuery(custom_query_id, make_tl_object<telegram_api::dataJSON>(data))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::bots_answerWebhookJSONQuery>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      LOG(INFO) << "Sending answer to a custom query has failed";
    }
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void Td::on_request(uint64 id, td_api::answerCustomQuery &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.data_);
  CREATE_OK_REQUEST_PROMISE();
  create_handler<AnswerCustomQueryQuery>(std::move(promise))->send(request.custom_query_id_, request.data_);
}

class SearchChatsOnServerRequest final : public RequestActor<> {
  string query_;
  int32 limit_;

  vector<DialogId> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->messages_manager_->search_dialogs_on_server(query_, limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(-1, dialog_ids_));
  }

 public:
  SearchChatsOnServerRequest(ActorShared<Td> td, uint64 request_id, string query, int32 limit)
      : RequestActor(std::move(td), request_id), query_(std::move(query)), limit_(limit) {
  }
};

void Td::on_request(uint64 id, td_api::searchChatsOnServer &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchChatsOnServerRequest, request.query_, request.limit_);
}

class GetInactiveSupergroupChatsRequest final : public RequestActor<> {
  vector<DialogId> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->contacts_manager_->get_inactive_channels(std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(-1, dialog_ids_));
  }

 public:
  GetInactiveSupergroupChatsRequest(ActorShared<Td> td, uint64 request_id) : RequestActor(std::move(td), request_id) {
  }
};

void Td::on_request(uint64 id, const td_api::getInactiveSupergroupChats &request) {
  CHECK_IS_USER();
  CREATE_NO_ARGS_REQUEST(GetInactiveSupergroupChatsRequest);
}

void Td::on_request(uint64 id, td_api::getStorageStatistics &request) {
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<FileStats> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(result.ok().get_storage_statistics_object());
    }
  });
  send_closure(storage_manager_, &StorageManager::get_storage_stats, false /*need_all_files*/, request.chat_limit_,
               std::move(query_promise));
}

void Td::on_request(uint64 id, const td_api::deleteChatMessagesBySender &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  TRY_RESULT_PROMISE(promise, sender_dialog_id, get_message_sender_dialog_id(this, request.sender_id_, false, false));
  messages_manager_->delete_dialog_messages_by_sender(DialogId(request.chat_id_), sender_dialog_id, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getChatSparseMessagePositions &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_dialog_sparse_message_positions(
      DialogId(request.chat_id_), get_message_search_filter(request.filter_), MessageId(request.from_message_id_),
      request.limit_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setAccountTtl &request) {
  CHECK_IS_USER();
  if (request.ttl_ == nullptr) {
    return send_error_raw(id, 400, "New account TTL must be non-empty");
  }
  CREATE_OK_REQUEST_PROMISE();
  set_account_ttl(this, request.ttl_->days_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::removeRecentHashtag &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.hashtag_);
  CREATE_OK_REQUEST_PROMISE();
  send_closure(hashtag_hints_, &HashtagHints::remove_hashtag, std::move(request.hashtag_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getChats &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_dialogs_from_list(DialogListId(request.chat_list_), request.limit_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::resetPassword &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::reset_password, std::move(promise));
}

void Td::on_request(uint64 id, td_api::testCallBytes &request) {
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::testBytes>(std::move(request.x_)));
}
}  // namespace td
