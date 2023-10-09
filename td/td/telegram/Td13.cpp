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
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileSourceId.h"
#include "td/telegram/files/FileStats.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/FullMessageId.h"


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

class SearchMessagesRequest final : public RequestActor<> {
  FolderId folder_id_;
  bool ignore_folder_id_;
  string query_;
  int32 offset_date_;
  DialogId offset_dialog_id_;
  MessageId offset_message_id_;
  int32 limit_;
  MessageSearchFilter filter_;
  int32 min_date_;
  int32 max_date_;
  int64 random_id_;

  std::pair<int32, vector<FullMessageId>> messages_;

  void do_run(Promise<Unit> &&promise) final {
    messages_ = td_->messages_manager_->search_messages(folder_id_, ignore_folder_id_, query_, offset_date_,
                                                        offset_dialog_id_, offset_message_id_, limit_, filter_,
                                                        min_date_, max_date_, random_id_, std::move(promise));
  }

  void do_send_result() final {
    send_result(
        td_->messages_manager_->get_messages_object(messages_.first, messages_.second, true, "SearchMessagesRequest"));
  }

  void do_send_error(Status &&status) final {
    if (status.message() == "SEARCH_QUERY_EMPTY") {
      messages_.first = 0;
      messages_.second.clear();
      return do_send_result();
    }
    send_error(std::move(status));
  }

 public:
  SearchMessagesRequest(ActorShared<Td> td, uint64 request_id, FolderId folder_id, bool ignore_folder_id, string query,
                        int32 offset_date, int64 offset_dialog_id, int64 offset_message_id, int32 limit,
                        tl_object_ptr<td_api::SearchMessagesFilter> &&filter, int32 min_date, int32 max_date)
      : RequestActor(std::move(td), request_id)
      , folder_id_(folder_id)
      , ignore_folder_id_(ignore_folder_id)
      , query_(std::move(query))
      , offset_date_(offset_date)
      , offset_dialog_id_(offset_dialog_id)
      , offset_message_id_(offset_message_id)
      , limit_(limit)
      , filter_(get_message_search_filter(filter))
      , min_date_(min_date)
      , max_date_(max_date)
      , random_id_(0) {
  }
};

void Td::on_request(uint64 id, td_api::searchMessages &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.query_);
  DialogListId dialog_list_id(request.chat_list_);
  if (!dialog_list_id.is_folder()) {
    return send_error_raw(id, 400, "Wrong chat list specified");
  }
  CREATE_REQUEST(SearchMessagesRequest, dialog_list_id.get_folder_id(), request.chat_list_ == nullptr,
                 std::move(request.query_), request.offset_date_, request.offset_chat_id_, request.offset_message_id_,
                 request.limit_, std::move(request.filter_), request.min_date_, request.max_date_);
}

class GetRecentStickersRequest final : public RequestActor<> {
  bool is_attached_;

  vector<FileId> sticker_ids_;

  void do_run(Promise<Unit> &&promise) final {
    sticker_ids_ = td_->stickers_manager_->get_recent_stickers(is_attached_, std::move(promise));
  }

  void do_send_result() final {
    send_result(td_->stickers_manager_->get_stickers_object(sticker_ids_));
  }

 public:
  GetRecentStickersRequest(ActorShared<Td> td, uint64 request_id, bool is_attached)
      : RequestActor(std::move(td), request_id), is_attached_(is_attached) {
  }
};

void Td::on_request(uint64 id, const td_api::getRecentStickers &request) {
  CHECK_IS_USER();
  CREATE_REQUEST(GetRecentStickersRequest, request.is_attached_);
}

void Td::on_request(uint64 id, const td_api::deleteChat &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  DialogId dialog_id(request.chat_id_);
  auto query_promise = [actor_id = messages_manager_actor_.get(), dialog_id,
                        promise = std::move(promise)](Result<Unit> &&result) mutable {
    if (result.is_error()) {
      promise.set_error(result.move_as_error());
    } else {
      send_closure(actor_id, &MessagesManager::on_dialog_deleted, dialog_id, std::move(promise));
    }
  };
  contacts_manager_->delete_dialog(dialog_id, std::move(query_promise));
}

void Td::on_request(uint64 id, td_api::startGroupCallRecording &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.title_);
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_recording(GroupCallId(request.group_call_id_), true, std::move(request.title_),
                                                   request.record_video_, request.use_portrait_orientation_,
                                                   std::move(promise));
}

void Td::on_request(uint64 id, const td_api::deletePassportElement &request) {
  CHECK_IS_USER();
  if (request.type_ == nullptr) {
    return send_error_raw(id, 400, "Type must be non-empty");
  }
  CREATE_OK_REQUEST_PROMISE();
  send_closure(secure_manager_, &SecureManager::delete_secure_value, get_secure_value_type_td_api(request.type_),
               std::move(promise));
}

void Td::on_request(uint64 id, const td_api::viewMessages &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->view_messages(
                          DialogId(request.chat_id_), MessageId(request.message_thread_id_),
                          MessagesManager::get_message_ids(request.message_ids_), request.force_read_));
}

void Td::on_request(uint64 id, const td_api::viewTrendingStickerSets &request) {
  CHECK_IS_USER();
  stickers_manager_->view_featured_sticker_sets(StickersManager::convert_sticker_set_ids(request.sticker_set_ids_));
  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::on_request(uint64 id, const td_api::cancelDownloadFile &request) {
  file_manager_->download(FileId(request.file_id_, 0), nullptr, request.only_if_pending_ ? -1 : 0, -1, -1);

  send_closure(actor_id(this), &Td::send_result, id, make_tl_object<td_api::ok>());
}

void Td::on_request(uint64 id, td_api::setUsername &request) {
  CHECK_IS_USER();
  CLEAN_INPUT_STRING(request.username_);
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->set_username(request.username_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::setChatMessageTtl &request) {
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->set_dialog_message_ttl(DialogId(request.chat_id_), request.ttl_, std::move(promise));
}

void Td::dec_stop_cnt() {
  stop_cnt_--;
  if (stop_cnt_ == 0) {
    LOG(INFO) << "Stop Td";
    set_context(std::move(old_context_));
    stop();
  }
}

void Td::hangup() {
  LOG(INFO) << "Receive Td::hangup";
  close();
  dec_stop_cnt();
}
}  // namespace td
