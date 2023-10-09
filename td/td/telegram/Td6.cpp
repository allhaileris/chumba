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

class TestProxyRequest final : public RequestOnceActor {
  Proxy proxy_;
  int16 dc_id_;
  double timeout_;
  ActorOwn<> child_;
  Promise<> promise_;

  auto get_transport() {
    return mtproto::TransportType{mtproto::TransportType::ObfuscatedTcp, dc_id_, proxy_.secret()};
  }

  void do_run(Promise<Unit> &&promise) final {
    set_timeout_in(timeout_);

    promise_ = std::move(promise);
    IPAddress ip_address;
    auto status = ip_address.init_host_port(proxy_.server(), proxy_.port());
    if (status.is_error()) {
      return promise_.set_error(Status::Error(400, status.public_message()));
    }
    auto r_socket_fd = SocketFd::open(ip_address);
    if (r_socket_fd.is_error()) {
      return promise_.set_error(Status::Error(400, r_socket_fd.error().public_message()));
    }

    auto dc_options = ConnectionCreator::get_default_dc_options(false);
    IPAddress mtproto_ip_address;
    for (auto &dc_option : dc_options.dc_options) {
      if (dc_option.get_dc_id().get_raw_id() == dc_id_) {
        mtproto_ip_address = dc_option.get_ip_address();
        break;
      }
    }

    auto connection_promise =
        PromiseCreator::lambda([actor_id = actor_id(this)](Result<ConnectionCreator::ConnectionData> r_data) mutable {
          send_closure(actor_id, &TestProxyRequest::on_connection_data, std::move(r_data));
        });

    child_ = ConnectionCreator::prepare_connection(ip_address, r_socket_fd.move_as_ok(), proxy_, mtproto_ip_address,
                                                   get_transport(), "Test", "TestPingDC2", nullptr, {}, false,
                                                   std::move(connection_promise));
  }

  void on_connection_data(Result<ConnectionCreator::ConnectionData> r_data) {
    if (r_data.is_error()) {
      return promise_.set_error(r_data.move_as_error());
    }
    class HandshakeContext final : public mtproto::AuthKeyHandshakeContext {
     public:
      mtproto::DhCallback *get_dh_callback() final {
        return nullptr;
      }
      mtproto::PublicRsaKeyInterface *get_public_rsa_key_interface() final {
        return &public_rsa_key;
      }

     private:
      PublicRsaKeyShared public_rsa_key{DcId::empty(), false};
    };
    auto handshake = make_unique<mtproto::AuthKeyHandshake>(dc_id_, 3600);
    auto data = r_data.move_as_ok();
    auto raw_connection =
        mtproto::RawConnection::create(data.ip_address, std::move(data.buffered_socket_fd), get_transport(), nullptr);
    child_ = create_actor<mtproto::HandshakeActor>(
        "HandshakeActor", std::move(handshake), std::move(raw_connection), make_unique<HandshakeContext>(), 10.0,
        PromiseCreator::lambda([actor_id = actor_id(this)](Result<unique_ptr<mtproto::RawConnection>> raw_connection) {
          send_closure(actor_id, &TestProxyRequest::on_handshake_connection, std::move(raw_connection));
        }),
        PromiseCreator::lambda(
            [actor_id = actor_id(this)](Result<unique_ptr<mtproto::AuthKeyHandshake>> handshake) mutable {
              send_closure(actor_id, &TestProxyRequest::on_handshake, std::move(handshake));
            }));
  }
  void on_handshake_connection(Result<unique_ptr<mtproto::RawConnection>> r_raw_connection) {
    if (r_raw_connection.is_error()) {
      return promise_.set_error(Status::Error(400, r_raw_connection.move_as_error().public_message()));
    }
  }
  void on_handshake(Result<unique_ptr<mtproto::AuthKeyHandshake>> r_handshake) {
    if (!promise_) {
      return;
    }
    if (r_handshake.is_error()) {
      return promise_.set_error(Status::Error(400, r_handshake.move_as_error().public_message()));
    }

    auto handshake = r_handshake.move_as_ok();
    if (!handshake->is_ready_for_finish()) {
      promise_.set_error(Status::Error(400, "Handshake is not ready"));
    }
    promise_.set_value(Unit());
  }

  void timeout_expired() final {
    send_error(Status::Error(400, "Timeout expired"));
    stop();
  }

 public:
  TestProxyRequest(ActorShared<Td> td, uint64 request_id, Proxy proxy, int32 dc_id, double timeout)
      : RequestOnceActor(std::move(td), request_id)
      , proxy_(std::move(proxy))
      , dc_id_(static_cast<int16>(dc_id))
      , timeout_(timeout) {
  }
};

void Td::on_request(uint64 id, td_api::testProxy &request) {
  auto r_proxy = Proxy::create_proxy(std::move(request.server_), request.port_, request.type_.get());
  if (r_proxy.is_error()) {
    return send_closure(actor_id(this), &Td::send_error, id, r_proxy.move_as_error());
  }
  CREATE_REQUEST(TestProxyRequest, r_proxy.move_as_ok(), request.dc_id_, request.timeout_);
}

void Td::on_request(uint64 id, const td_api::toggleSupergroupIsAllHistoryAvailable &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  contacts_manager_->toggle_channel_is_all_history_available(ChannelId(request.supergroup_id_),
                                                             request.is_all_history_available_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::endGroupCallRecording &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->toggle_group_call_recording(GroupCallId(request.group_call_id_), false, string(), false, false,
                                                   std::move(promise));
}

void Td::on_request(uint64 id, td_api::sendCallSignalingData &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  send_closure(G()->call_manager(), &CallManager::send_call_signaling_data, CallId(request.call_id_),
               std::move(request.data_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::toggleSessionCanAcceptCalls &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  toggle_session_can_accept_calls(this, request.session_id_, request.can_accept_calls_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::endGroupCall &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  group_call_manager_->discard_group_call(GroupCallId(request.group_call_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::addRecentlyFoundChat &request) {
  CHECK_IS_USER();
  answer_ok_query(id, messages_manager_->add_recently_found_dialog(DialogId(request.chat_id_)));
}

void Td::on_request(uint64 id, const td_api::getJsonValue &request) {
  UNREACHABLE();
}
}  // namespace td
