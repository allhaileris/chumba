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

class GetRecentMeUrlsQuery final : public Td::ResultHandler {
  Promise<tl_object_ptr<td_api::tMeUrls>> promise_;

 public:
  explicit GetRecentMeUrlsQuery(Promise<tl_object_ptr<td_api::tMeUrls>> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &referrer) {
    send_query(G()->net_query_creator().create(telegram_api::help_getRecentMeUrls(referrer)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_getRecentMeUrls>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto urls_full = result_ptr.move_as_ok();
    td_->contacts_manager_->on_get_users(std::move(urls_full->users_), "GetRecentMeUrlsQuery");
    td_->contacts_manager_->on_get_chats(std::move(urls_full->chats_), "GetRecentMeUrlsQuery");

    auto urls = std::move(urls_full->urls_);
    auto results = make_tl_object<td_api::tMeUrls>();
    results->urls_.reserve(urls.size());
    for (auto &url_ptr : urls) {
      CHECK(url_ptr != nullptr);
      tl_object_ptr<td_api::tMeUrl> result = make_tl_object<td_api::tMeUrl>();
      switch (url_ptr->get_id()) {
        case telegram_api::recentMeUrlUser::ID: {
          auto url = move_tl_object_as<telegram_api::recentMeUrlUser>(url_ptr);
          result->url_ = std::move(url->url_);
          UserId user_id(url->user_id_);
          if (!user_id.is_valid()) {
            LOG(ERROR) << "Receive invalid " << user_id;
            result = nullptr;
            break;
          }
          result->type_ = make_tl_object<td_api::tMeUrlTypeUser>(
              td_->contacts_manager_->get_user_id_object(user_id, "tMeUrlTypeUser"));
          break;
        }
        case telegram_api::recentMeUrlChat::ID: {
          auto url = move_tl_object_as<telegram_api::recentMeUrlChat>(url_ptr);
          result->url_ = std::move(url->url_);
          ChannelId channel_id(url->chat_id_);
          if (!channel_id.is_valid()) {
            LOG(ERROR) << "Receive invalid " << channel_id;
            result = nullptr;
            break;
          }
          result->type_ = make_tl_object<td_api::tMeUrlTypeSupergroup>(
              td_->contacts_manager_->get_supergroup_id_object(channel_id, "tMeUrlTypeSupergroup"));
          break;
        }
        case telegram_api::recentMeUrlChatInvite::ID: {
          auto url = move_tl_object_as<telegram_api::recentMeUrlChatInvite>(url_ptr);
          result->url_ = std::move(url->url_);
          td_->contacts_manager_->on_get_dialog_invite_link_info(result->url_, std::move(url->chat_invite_),
                                                                 Promise<Unit>());
          auto info_object = td_->contacts_manager_->get_chat_invite_link_info_object(result->url_);
          if (info_object == nullptr) {
            result = nullptr;
            break;
          }
          result->type_ = make_tl_object<td_api::tMeUrlTypeChatInvite>(std::move(info_object));
          break;
        }
        case telegram_api::recentMeUrlStickerSet::ID: {
          auto url = move_tl_object_as<telegram_api::recentMeUrlStickerSet>(url_ptr);
          result->url_ = std::move(url->url_);
          auto sticker_set_id =
              td_->stickers_manager_->on_get_sticker_set_covered(std::move(url->set_), false, "recentMeUrlStickerSet");
          if (!sticker_set_id.is_valid()) {
            LOG(ERROR) << "Receive invalid sticker set";
            result = nullptr;
            break;
          }
          result->type_ = make_tl_object<td_api::tMeUrlTypeStickerSet>(sticker_set_id.get());
          break;
        }
        case telegram_api::recentMeUrlUnknown::ID:
          // skip
          result = nullptr;
          break;
        default:
          UNREACHABLE();
      }
      if (result != nullptr) {
        results->urls_.push_back(std::move(result));
      }
    }
    promise_.set_value(std::move(results));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void Td::on_request(uint64 id, td_api::getRecentlyVisitedTMeUrls &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.referrer_);
  CREATE_REQUEST_PROMISE();
  create_handler<GetRecentMeUrlsQuery>(std::move(promise))->send(request.referrer_);
}

void Td::on_request(uint64 id, td_api::createChatInviteLink &request) {
  CLEAN_INPUT_STRING(request.name_);
  CREATE_REQUEST_PROMISE();
  contacts_manager_->export_dialog_invite_link(DialogId(request.chat_id_), std::move(request.name_),
                                               request.expiration_date_, request.member_limit_,
                                               request.creates_join_request_, false, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deleteChatMessagesByDate &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->delete_dialog_messages_by_date(DialogId(request.chat_id_), request.min_date_, request.max_date_,
                                                    request.revoke_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::validateOrderInfo &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  validate_order_info(this, {DialogId(request.chat_id_), MessageId(request.message_id_)},
                      std::move(request.order_info_), request.allow_save_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::stopPoll &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->stop_poll({DialogId(request.chat_id_), MessageId(request.message_id_)},
                               std::move(request.reply_markup_), std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::getFileExtension &request) {
  // don't check MIME type UTF-8 correctness
  return make_tl_object<td_api::text>(MimeType::to_extension(request.mime_type_));
}

void Td::on_request(uint64 id, td_api::resetNetworkStatistics &request) {
  CREATE_OK_REQUEST_PROMISE();
  send_closure(net_stats_manager_, &NetStatsManager::reset_network_stats);
  promise.set_value(Unit());
}

void Td::on_request(uint64 id, const td_api::getSavedOrderInfo &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  get_saved_order_info(this, std::move(promise));
}


constexpr const char *Td::TDLIB_VERSION;
}  // namespace td
