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

#include "td/telegram/HashtagHints.h"
#include "td/telegram/InlineQueriesManager.h"
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

class GetInlineQueryResultsRequest final : public RequestOnceActor {
  UserId bot_user_id_;
  DialogId dialog_id_;
  Location user_location_;
  string query_;
  string offset_;

  uint64 query_hash_;

  void do_run(Promise<Unit> &&promise) final {
    query_hash_ = td_->inline_queries_manager_->send_inline_query(bot_user_id_, dialog_id_, user_location_, query_,
                                                                  offset_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->inline_queries_manager_->get_inline_query_results_object(query_hash_));
  }

 public:
  GetInlineQueryResultsRequest(ActorShared<Td> td, uint64 request_id, int64 bot_user_id, int64 dialog_id,
                               const tl_object_ptr<td_api::location> &user_location, string query, string offset)
      : RequestOnceActor(std::move(td), request_id)
      , bot_user_id_(bot_user_id)
      , dialog_id_(dialog_id)
      , user_location_(user_location)
      , query_(std::move(query))
      , offset_(std::move(offset))
      , query_hash_(0) {
  }
};

void Td::on_request(uint64 id, td_api::getInlineQueryResults &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CLEAN_INPUT_STRING(request.offset_);
  CREATE_REQUEST(GetInlineQueryResultsRequest, request.bot_user_id_, request.chat_id_, request.user_location_,
                 std::move(request.query_), std::move(request.offset_));
}

class SearchContactsRequest final : public RequestActor<> {
  string query_;
  int32 limit_;

  std::pair<int32, vector<UserId>> user_ids_;

  void do_run(Promise<Unit> &&promise) final {
    user_ids_ = td_->contacts_manager_->search_contacts(query_, limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->contacts_manager_->get_users_object(user_ids_.first, user_ids_.second));
  }

 public:
  SearchContactsRequest(ActorShared<Td> td, uint64 request_id, string query, int32 limit)
      : RequestActor(std::move(td), request_id), query_(std::move(query)), limit_(limit) {
  }
};

void Td::on_request(uint64 id, const td_api::getContacts &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(SearchContactsRequest, string(), 1000000);
}

void Td::on_request(uint64 id, td_api::searchContacts &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchContactsRequest, request.query_, request.limit_);
}

class GetInstalledStickerSetsRequest final : public RequestActor<> {
  bool is_masks_;

  vector<StickerSetId> sticker_set_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_ids_ = td_->stickers_manager_->get_installed_sticker_sets(is_masks_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_sets_object(-1, sticker_set_ids_, 1));
  }

 public:
  GetInstalledStickerSetsRequest(ActorShared<Td> td, uint64 request_id, bool is_masks)
      : RequestActor(std::move(td), request_id), is_masks_(is_masks) {
  }
};

void Td::on_request(uint64 id, const td_api::getInstalledStickerSets &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetInstalledStickerSetsRequest, request.is_masks_);
}

void Td::on_request(uint64 id, td_api::searchHashtags &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.prefix_);
  CREATE_REQUEST_PROMISE();
  auto query_promise =
      PromiseCreator::lambda([promise = std::move(promise)](Result<std::vector<string>> result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          promise.set_value(make_tl_object<td_api::hashtags>(result.move_as_ok()));
        }
      });
  send_closure(hashtag_hints_, &HashtagHints::query, std::move(request.prefix_), request.limit_,
               std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::getSuggestedStickerSetName &request) {
  CLEAN_INPUT_STRING(request.title_);
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::text>(result.move_as_ok()));
    }
  });
  stickers_manager_->get_suggested_sticker_set_name(std::move(request.title_), std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::getMessagePublicForwards &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.offset_);
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_message_public_forwards({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                                 std::move(request.offset_), request.limit_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleChatHasProtectedContent &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->toggle_dialog_has_protected_content(DialogId(request.chat_id_), request.has_protected_content_,
                                                         std::move(promise));
}

void Td::on_request(uint64 id, td_api::checkRecoveryEmailAddressCode &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.code_);
  CREATE_REQUEST_PROMISE();
  send_closure(password_manager_, &PasswordManager::check_recovery_email_address_code, std::move(request.code_),
               std::move(promise));
}

void Td::on_update_status_success(bool is_online) {
  if (is_online == is_online_) {
    if (!update_status_query_.empty()) {
      update_status_query_ = NetQueryRef();
    }
    contacts_manager_->set_my_online_status(is_online_, true, false);
  }
}

void Td::on_request(uint64 id, const td_api::getChatAvailableMessageSenders &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_dialog_send_message_as_dialog_ids(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::unpinAllChatMessages &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->unpin_all_dialog_messages(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, td_api::testCallString &request) {
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::testString>(std::move(request.x_)));
}
}  // namespace td
