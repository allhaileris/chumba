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
#include "td/telegram/CountryInfoManager.h"
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
#include "td/telegram/PollManager.h"
#include "td/telegram/PrivacyManager.h"

#include "td/telegram/ReportReason.h"
#include "td/telegram/RequestActor.h"
#include "td/telegram/SecretChatId.h"
#include "td/telegram/SecretChatsManager.h"
#include "td/telegram/SecureManager.h"
#include "td/telegram/SecureValue.h"

#include "td/telegram/StateManager.h"
#include "td/telegram/StickerSetId.h"

#include "td/telegram/StorageManager.h"
#include "td/telegram/SuggestedAction.h"
#include "td/telegram/td_api.hpp"
#include "td/telegram/TdDb.h"
#include "td/telegram/telegram_api.hpp"



#include "td/telegram/UpdatesManager.h"
#include "td/telegram/Version.h"
#include "td/telegram/VideoNotesManager.h"
#include "td/telegram/VideosManager.h"
#include "td/telegram/VoiceNotesManager.h"

#include "td/telegram/WebPagesManager.h"

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

template <class T>
void Td::complete_pending_preauthentication_requests(const T &func) {
  for (auto &request : pending_preauthentication_requests_) {
    if (request.second != nullptr && func(request.second->get_id())) {
      downcast_call(*request.second, [this, id = request.first](auto &request) { this->on_request(id, request); });
      request.second = nullptr;
    }
  }
}

Status Td::init(DbKey key) {
  auto current_scheduler_id = Scheduler::instance()->sched_id();
  auto scheduler_count = Scheduler::instance()->sched_count();

  VLOG(td_init) << "Begin to init database";
  TdDb::Events events;
  auto r_td_db = TdDb::open(min(current_scheduler_id + 1, scheduler_count - 1), parameters_, std::move(key), events);
  if (r_td_db.is_error()) {
    LOG(WARNING) << "Failed to open database: " << r_td_db.error();
    return Status::Error(400, r_td_db.error().message());
  }
  LOG(INFO) << "Successfully inited database in " << tag("database_directory", parameters_.database_directory)
            << " and " << tag("files_directory", parameters_.files_directory);
  VLOG(td_init) << "Successfully inited database";

  G()->init(parameters_, actor_id(this), r_td_db.move_as_ok()).ensure();

  init_options_and_network();

  complete_pending_preauthentication_requests([](int32 id) {
    switch (id) {
      case td_api::getOption::ID:
      case td_api::setOption::ID:
        return true;
      default:
        return false;
    }
  });

  options_.language_pack = G()->shared_config().get_option_string("localization_target");
  options_.language_code = G()->shared_config().get_option_string("language_pack_id");
  options_.parameters = G()->shared_config().get_option_string("connection_parameters");
  options_.tz_offset = static_cast<int32>(G()->shared_config().get_option_integer("utc_time_offset"));
  options_.is_emulator = G()->shared_config().get_option_boolean("is_emulator");
  // options_.proxy = Proxy();
  G()->set_mtproto_header(make_unique<MtprotoHeader>(options_));
  G()->set_store_all_files_in_files_directory(
      G()->shared_config().get_option_boolean("store_all_files_in_files_directory"));

  VLOG(td_init) << "Create NetQueryDispatcher";
  auto net_query_dispatcher = make_unique<NetQueryDispatcher>([&] { return create_reference(); });
  G()->set_net_query_dispatcher(std::move(net_query_dispatcher));

  complete_pending_preauthentication_requests([](int32 id) {
    // pingProxy uses NetQueryDispatcher to get main_dc_id, so must be called after NetQueryDispatcher is created
    return id == td_api::pingProxy::ID;
  });

  VLOG(td_init) << "Create AuthManager";
  auth_manager_ = td::make_unique<AuthManager>(parameters_.api_id, parameters_.api_hash, create_reference());
  auth_manager_actor_ = register_actor("AuthManager", auth_manager_.get());

  init_file_manager();

  init_managers();

  G()->set_my_id(G()->shared_config().get_option_integer("my_id"));

  storage_manager_ = create_actor<StorageManager>("StorageManager", create_reference(),
                                                  min(current_scheduler_id + 2, scheduler_count - 1));
  G()->set_storage_manager(storage_manager_.get());

  VLOG(td_init) << "Send binlog events";
  for (auto &event : events.user_events) {
    contacts_manager_->on_binlog_user_event(std::move(event));
  }

  for (auto &event : events.channel_events) {
    contacts_manager_->on_binlog_channel_event(std::move(event));
  }

  // chats may contain links to channels, so should be inited after
  for (auto &event : events.chat_events) {
    contacts_manager_->on_binlog_chat_event(std::move(event));
  }

  for (auto &event : events.secret_chat_events) {
    contacts_manager_->on_binlog_secret_chat_event(std::move(event));
  }

  for (auto &event : events.web_page_events) {
    web_pages_manager_->on_binlog_web_page_event(std::move(event));
  }

  if (is_online_) {
    on_online_updated(true, true);
  }
  if (auth_manager_->is_bot()) {
    set_is_bot_online(true);
  }

  // Send binlog events to managers
  //
  // 1. Actors must receive all binlog events before other queries.
  //
  // -- All actors have one "entry point". So there is only one way to send query to them. So all queries are ordered
  // for each Actor.
  //
  // 2. An actor must not make some decisions before all binlog events are processed.
  // For example, SecretChatActor must not send RequestKey, before it receives log event with RequestKey and understands
  // that RequestKey was already sent.
  //
  // 3. During replay of binlog some queries may be sent to other actors. They shouldn't process such events before all
  // their binlog events are processed. So actor may receive some old queries. It must be in its actual state in
  // orded to handle them properly.
  //
  // -- Use send_closure_later, so actors don't even start process binlog events, before all binlog events are sent

  for (auto &event : events.to_secret_chats_manager) {
    send_closure_later(secret_chats_manager_, &SecretChatsManager::replay_binlog_event, std::move(event));
  }

  send_closure_later(poll_manager_actor_, &PollManager::on_binlog_events, std::move(events.to_poll_manager));

  send_closure_later(messages_manager_actor_, &MessagesManager::on_binlog_events,
                     std::move(events.to_messages_manager));

  send_closure_later(notification_manager_actor_, &NotificationManager::on_binlog_events,
                     std::move(events.to_notification_manager));

  send_closure(secret_chats_manager_, &SecretChatsManager::binlog_replay_finish);

  VLOG(td_init) << "Ping datacenter";
  if (!auth_manager_->is_authorized()) {
    country_info_manager_->get_current_country_code(Promise<string>());
  } else {
    updates_manager_->get_difference("init");
    schedule_get_terms_of_service(0);
    schedule_get_promo_data(0);
  }

  complete_pending_preauthentication_requests([](int32 id) { return true; });

  VLOG(td_init) << "Finish initialization";

  state_ = State::Run;
  return Status::OK();
}

void Td::init_connection_creator() {
  VLOG(td_init) << "Create ConnectionCreator";
  auto connection_creator = create_actor<ConnectionCreator>("ConnectionCreator", create_reference());
  auto net_stats_manager = create_actor<NetStatsManager>("NetStatsManager", create_reference());

  // How else could I let two actor know about each other, without quite complex async logic?
  auto net_stats_manager_ptr = net_stats_manager->get_actor_unsafe();
  net_stats_manager_ptr->init();
  connection_creator->get_actor_unsafe()->set_net_stats_callback(net_stats_manager_ptr->get_common_stats_callback(),
                                                                 net_stats_manager_ptr->get_media_stats_callback());
  G()->set_net_stats_file_callbacks(net_stats_manager_ptr->get_file_stats_callbacks());

  G()->set_connection_creator(std::move(connection_creator));
  net_stats_manager_ = std::move(net_stats_manager);

  complete_pending_preauthentication_requests([](int32 id) {
    switch (id) {
      case td_api::setNetworkType::ID:
      case td_api::getNetworkStatistics::ID:
      case td_api::addNetworkStatistics::ID:
      case td_api::resetNetworkStatistics::ID:
      case td_api::addProxy::ID:
      case td_api::editProxy::ID:
      case td_api::enableProxy::ID:
      case td_api::disableProxy::ID:
      case td_api::removeProxy::ID:
      case td_api::getProxies::ID:
      case td_api::getProxyLink::ID:
        return true;
      default:
        return false;
    }
  });
}
}  // namespace td
