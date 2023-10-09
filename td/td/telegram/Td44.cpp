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


#include <tuple>
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

class GetChatMessageCalendarRequest final : public RequestActor<> {
  DialogId dialog_id_;
  MessageId from_message_id_;
  MessageSearchFilter filter_;
  int64 random_id_;

  td_api::object_ptr<td_api::messageCalendar> calendar_;

  void do_run(Promise<Unit> &&promise) final {
    calendar_ = td_->messages_manager_->get_dialog_message_calendar(dialog_id_, from_message_id_, filter_, random_id_,
                                                                    get_tries() == 3, std::move(promise));
  }

  void do_send_result() final {
    send_result(std::move(calendar_));
  }

 public:
  GetChatMessageCalendarRequest(ActorShared<Td> td, uint64 request_id, int64 dialog_id, int64 from_message_id,
                                tl_object_ptr<td_api::SearchMessagesFilter> filter)
      : RequestActor(std::move(td), request_id)
      , dialog_id_(dialog_id)
      , from_message_id_(from_message_id)
      , filter_(get_message_search_filter(filter))
      , random_id_(0) {
    set_tries(3);
  }
};

void Td::on_request(uint64 id, td_api::getChatMessageCalendar &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetChatMessageCalendarRequest, request.chat_id_, request.from_message_id_, std::move(request.filter_));
}

class GetArchivedStickerSetsRequest final : public RequestActor<> {
  bool is_masks_;
  StickerSetId offset_sticker_set_id_;
  int32 limit_;

  int32 total_count_ = -1;
  vector<StickerSetId> sticker_set_ids_;

  void do_run(Promise<Unit> &&promise) final {
    std::tie(total_count_, sticker_set_ids_) = td_->stickers_manager_->get_archived_sticker_sets(
        is_masks_, offset_sticker_set_id_, limit_, get_tries() < 2, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_sticker_sets_object(total_count_, sticker_set_ids_, 1));
  }

 public:
  GetArchivedStickerSetsRequest(ActorShared<Td> td, uint64 request_id, bool is_masks, int64 offset_sticker_set_id,
                                int32 limit)
      : RequestActor(std::move(td), request_id)
      , is_masks_(is_masks)
      , offset_sticker_set_id_(offset_sticker_set_id)
      , limit_(limit) {
  }
};

void Td::on_request(uint64 id, const td_api::getArchivedStickerSets &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetArchivedStickerSetsRequest, request.is_masks_, request.offset_sticker_set_id_, request.limit_);
}

class SearchChatsRequest final : public RequestActor<> {
  string query_;
  int32 limit_;

  std::pair<int32, vector<DialogId>> dialog_ids_;

  void do_run(Promise<Unit> &&promise) final {
    dialog_ids_ = td_->messages_manager_->search_dialogs(query_, limit_, std::move(promise));
  }

  void do_send_result() final {
    send_result(MessagesManager::get_chats_object(dialog_ids_));
  }

 public:
  SearchChatsRequest(ActorShared<Td> td, uint64 request_id, string query, int32 limit)
      : RequestActor(std::move(td), request_id), query_(std::move(query)), limit_(limit) {
  }
};

void Td::on_request(uint64 id, td_api::searchChats &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  CREATE_REQUEST(SearchChatsRequest, request.query_, request.limit_);
}

class RemoveContactsRequest final : public RequestActor<> {
  vector<UserId> user_ids_;

  void do_run(Promise<Unit> &&promise) final {
    td_->contacts_manager_->remove_contacts(user_ids_, std::move(promise));
  }

 public:
  RemoveContactsRequest(ActorShared<Td> td, uint64 request_id, vector<UserId> &&user_ids)
      : RequestActor(std::move(td), request_id), user_ids_(std::move(user_ids)) {
    set_tries(3);  // load_contacts + delete_contacts
  }
};

void Td::on_request(uint64 id, td_api::removeContacts &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(RemoveContactsRequest, UserId::get_user_ids(request.user_ids_));
}

void Td::on_request(uint64 id, td_api::sendMessage &request) {
  auto r_sent_message = messages_manager_->send_message(
      DialogId(request.chat_id_), MessageId(request.message_thread_id_), MessageId(request.reply_to_message_id_),
      std::move(request.options_), std::move(request.reply_markup_), std::move(request.input_message_content_));
  if (r_sent_message.is_error()) {
    send_closure(actor_id(this), &Td::send_error, id, r_sent_message.move_as_error());
  } else {
    send_closure(actor_id(this), &Td::send_result, id, r_sent_message.move_as_ok());
  }
}

void Td::on_request(uint64 id, td_api::editInlineMessageReplyMarkup &request) {
  CHECK_IS_BOT();
  CLEAN_INPUT_STRING(request.inline_message_id_);
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->edit_inline_message_reply_markup(request.inline_message_id_, std::move(request.reply_markup_),
                                                      std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::setLogTagVerbosityLevel &request) {
  auto result = Logging::set_tag_verbosity_level(request.tag_, static_cast<int>(request.new_verbosity_level_));
  if (result.is_ok()) {
    return td_api::make_object<td_api::ok>();
  } else {
    return make_error(400, result.message());
  }
}

void Td::on_request(uint64 id, td_api::setChatLocation &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_channel_location(DialogId(request.chat_id_), DialogLocation(std::move(request.location_)),
                                          std::move(promise));
}

void Td::on_request(uint64 id, td_api::setAuthenticationPhoneNumber &request) {
  CLEAN_INPUT_STRING(request.phone_number_);
  send_closure(auth_manager_actor_, &AuthManager::set_phone_number, id, std::move(request.phone_number_),
               std::move(request.settings_));
}

void Td::on_request(uint64 id, const td_api::getBlockedMessageSenders &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->get_blocked_dialogs(request.offset_, request.limit_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getBackgrounds &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  background_manager_->get_backgrounds(request.for_dark_theme_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::disconnectAllWebsites &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  disconnect_all_websites(this, std::move(promise));
}
}  // namespace td
