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
#include "td/telegram/GameManager.h"
#include "td/telegram/Global.h"
#include "td/telegram/GroupCallId.h"
#include "td/telegram/GroupCallManager.h"


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

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::parseTextEntities &request) {
  if (!check_utf8(request.text_)) {
    return make_error(400, "Text must be encoded in UTF-8");
  }
  if (request.parse_mode_ == nullptr) {
    return make_error(400, "Parse mode must be non-empty");
  }

  auto r_entities = [&]() -> Result<vector<MessageEntity>> {
    switch (request.parse_mode_->get_id()) {
      case td_api::textParseModeHTML::ID:
        return parse_html(request.text_);
      case td_api::textParseModeMarkdown::ID: {
        auto version = static_cast<const td_api::textParseModeMarkdown *>(request.parse_mode_.get())->version_;
        if (version == 0 || version == 1) {
          return parse_markdown(request.text_);
        }
        if (version == 2) {
          return parse_markdown_v2(request.text_);
        }
        return Status::Error("Wrong Markdown version specified");
      }
      default:
        UNREACHABLE();
        return Status::Error(500, "Unknown parse mode");
    }
  }();
  if (r_entities.is_error()) {
    return make_error(400, PSLICE() << "Can't parse entities: " << r_entities.error().message());
  }

  return make_tl_object<td_api::formattedText>(std::move(request.text_),
                                               get_text_entities_object(r_entities.ok(), false, -1));
}

class SearchInstalledStickerSetsRequest final : public RequestActor<> {
  bool is_masks_;
  string query_;
  int32 limit_;

  std::pair<int32, vector<StickerSetId>> sticker_set_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_set_ids_ =
        td_->stickers_manager_->search_installed_sticker_sets(is_masks_, query_, limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_sets_object(sticker_set_ids_.first, sticker_set_ids_.second, 5));
  }

 public:
  SearchInstalledStickerSetsRequest(ActorShared<Td> td, uint64 request_id, bool is_masks, string &&query, int32 limit)
      : RequestActor(std::move(td), request_id), is_masks_(is_masks), query_(std::move(query)), limit_(limit) {
  }
};

void Td::on_request(uint64 id, td_api::searchInstalledStickerSets &request) {
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchInstalledStickerSetsRequest, request.is_masks_, std::move(request.query_), request.limit_);
}

class ChangeStickerSetRequest final : public RequestOnceActor {
  StickerSetId set_id_;
  bool is_installed_;
  bool is_archived_;

  void do_run(Promise<Unit> &&promise) final {
    td_->stickers_manager_->change_sticker_set(set_id_, is_installed_, is_archived_, std::move(promise));
  }

 public:
  ChangeStickerSetRequest(ActorShared<Td> td, uint64 request_id, int64 set_id, bool is_installed, bool is_archived)
      : RequestOnceActor(std::move(td), request_id)
      , set_id_(set_id)
      , is_installed_(is_installed)
      , is_archived_(is_archived) {
    set_tries(4);
  }
};

void Td::on_request(uint64 id, const td_api::changeStickerSet &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(ChangeStickerSetRequest, request.set_id_, request.is_installed_, request.is_archived_);
}

void Td::on_request(uint64 id, td_api::startGroupCallScreenSharing &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.payload_);
  CREATE_REQUEST_PROMISE();
  auto query_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<string> result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      promise.set_value(make_tl_object<td_api::text>(result.move_as_ok()));
    }
  });
  group_call_manager_->start_group_call_screen_sharing(GroupCallId(request.group_call_id_), request.audio_source_id_,
                                                       std::move(request.payload_), std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::sendPaymentForm &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.order_info_id_);
  CLEAN_INPUT_STRING(request.shipping_option_id_);
  CREATE_REQUEST_PROMISE();
  send_payment_form(this, {DialogId(request.chat_id_), MessageId(request.message_id_)}, request.payment_form_id_,
                    request.order_info_id_, request.shipping_option_id_, request.credentials_, request.tip_amount_,
                    std::move(promise));
}

void Td::on_request(uint64 id, td_api::answerCallbackQuery &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.text_);
  CLEAN_INPUT_STRING(request.url_);
  CREATE_OK_REQUEST_PROMISE();
  callback_queries_manager_->answer_callback_query(request.callback_query_id_, request.text_, request.show_alert_,
                                                   request.url_, request.cache_time_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setGameScore &request) {
  CHECK_IS_BOT();
  CREATE_REQUEST_PROMISE();
  game_manager_->set_game_score({DialogId(request.chat_id_), MessageId(request.message_id_)}, request.edit_message_,
                                UserId(request.user_id_), request.score_, request.force_, std::move(promise));
}

void Td::on_request(uint64 id, td_api::setPollAnswer &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_poll_answer({DialogId(request.chat_id_), MessageId(request.message_id_)},
                                     std::move(request.option_ids_), std::move(promise));
}

void Td::ResultHandler::send_query(NetQueryPtr query) {
  CHECK(!is_query_sent_)
  is_query_sent_ = true;
  td_->add_handler(query->id(), shared_from_this());
  query->debug("Send to NetQueryDispatcher");
  G()->net_query_dispatcher().dispatch(std::move(query));
}

void Td::on_request(uint64 id, const td_api::getVideoChatAvailableParticipants &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  group_call_manager_->get_group_call_join_as(DialogId(request.chat_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::testGetDifference &request) {
  updates_manager_->get_difference("testGetDifference");
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::on_request(uint64 id, const td_api::getLanguagePackString &request) {
  UNREACHABLE();
}

void Td::on_request(uint64 id, const td_api::getPushReceiverId &request) {
  UNREACHABLE();
}
}  // namespace td
