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

#include "td/telegram/LinkManager.h"
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
#include "td/telegram/OptionManager.h"
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
#include "td/telegram/TdDb.h"
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

void Td::init_options_and_network() {
  VLOG(td_init) << "Create StateManager";
  class StateManagerCallback final : public StateManager::Callback {
   public:
    explicit StateManagerCallback(ActorShared<Td> td) : td_(std::move(td)) {
    }
    bool on_state(ConnectionState state) final {
      send_closure(td_, &Td::on_connection_state_changed, state);
      return td_.is_alive();
    }

   private:
    ActorShared<Td> td_;
  };
  state_manager_ = create_actor<StateManager>("State manager", create_reference());
  send_closure(state_manager_, &StateManager::add_callback, make_unique<StateManagerCallback>(create_reference()));
  G()->set_state_manager(state_manager_.get());

  VLOG(td_init) << "Create ConfigShared";
  G()->set_shared_config(td::make_unique<ConfigShared>(G()->td_db()->get_config_pmc_shared()));

  if (G()->shared_config().have_option("language_database_path")) {
    G()->shared_config().set_option_string("language_pack_database_path",
                                           G()->shared_config().get_option_string("language_database_path"));
    G()->shared_config().set_option_empty("language_database_path");
  }
  if (G()->shared_config().have_option("language_pack")) {
    G()->shared_config().set_option_string("localization_target",
                                           G()->shared_config().get_option_string("language_pack"));
    G()->shared_config().set_option_empty("language_pack");
  }
  if (G()->shared_config().have_option("language_code")) {
    G()->shared_config().set_option_string("language_pack_id", G()->shared_config().get_option_string("language_code"));
    G()->shared_config().set_option_empty("language_code");
  }
  if (!G()->shared_config().have_option("message_text_length_max")) {
    G()->shared_config().set_option_integer("message_text_length_max", 4096);
  }
  if (!G()->shared_config().have_option("message_caption_length_max")) {
    G()->shared_config().set_option_integer("message_caption_length_max", 1024);
  }
  if (!G()->shared_config().have_option("suggested_video_note_length")) {
    G()->shared_config().set_option_integer("suggested_video_note_length", 384);
  }
  if (!G()->shared_config().have_option("suggested_video_note_video_bitrate")) {
    G()->shared_config().set_option_integer("suggested_video_note_video_bitrate", 1000);
  }
  if (!G()->shared_config().have_option("suggested_video_note_audio_bitrate")) {
    G()->shared_config().set_option_integer("suggested_video_note_audio_bitrate", 64);
  }
  G()->shared_config().set_option_integer("utc_time_offset", Clocks::tz_offset());

  init_connection_creator();

  VLOG(td_init) << "Create TempAuthKeyWatchdog";
  auto temp_auth_key_watchdog = create_actor<TempAuthKeyWatchdog>("TempAuthKeyWatchdog", create_reference());
  G()->set_temp_auth_key_watchdog(std::move(temp_auth_key_watchdog));

  VLOG(td_init) << "Create ConfigManager";
  config_manager_ = create_actor<ConfigManager>("ConfigManager", create_reference());
  G()->set_config_manager(config_manager_.get());

  VLOG(td_init) << "Create OptionManager";
  option_manager_ = make_unique<OptionManager>(this, create_reference());
  option_manager_actor_ = register_actor("OptionManager", option_manager_.get());
  G()->set_option_manager(option_manager_actor_.get());

  VLOG(td_init) << "Set ConfigShared callback";
  class ConfigSharedCallback final : public ConfigShared::Callback {
   public:
    void on_option_updated(const string &name, const string &value) const final {
      send_closure_later(G()->option_manager(), &OptionManager::on_option_updated, name);
    }
    ~ConfigSharedCallback() final {
      LOG(INFO) << "Destroy ConfigSharedCallback";
    }
  };
  // we need to set ConfigShared callback before td_api::getOption requests are processed for consistency
  // TODO currently they will be inconsistent anyway, because td_api::getOption returns current value,
  // but in td_api::updateOption there will be a newer value, obtained at the time of update creation
  // so, there can be even two succesive updateOption with the same value
  // we need to process td_api::getOption along with td_api::setOption for consistency
  // we need to process td_api::setOption before managers and MTProto header are created,
  // because their initialiation may be affected by the options
  G()->shared_config().set_callback(make_unique<ConfigSharedCallback>());
}

td_api::object_ptr<td_api::Object> Td::do_static_request(td_api::getJsonValue &request) {
  if (!check_utf8(request.json_)) {
    return make_error(400, "JSON has invalid encoding");
  }
  auto result = get_json_value(request.json_);
  if (result.is_error()) {
    return make_error(400, result.error().message());
  } else {
    return result.move_as_ok();
  }
}

void Td::on_request(uint64 id, const td_api::searchChatRecentLocationMessages &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  messages_manager_->search_dialog_recent_location_messages(DialogId(request.chat_id_), request.limit_,
                                                            std::move(promise));
}

void Td::on_request(uint64 id, const td_api::importMessages &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->import_messages(DialogId(request.chat_id_), request.message_file_, request.attached_files_,
                                     std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getInternalLinkType &request) {
  auto type = link_manager_->parse_internal_link(request.link_);
  send_closure(actor_id(this), &Td::send_result, id, type == nullptr ? nullptr : type->get_internal_link_type_object());
}

void Td::on_request(uint64 id, const td_api::getChatStatistics &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  contacts_manager_->get_channel_statistics(DialogId(request.chat_id_), request.is_dark_, std::move(promise));
}

void Td::on_request(uint64 id, const td_api::getGroupCall &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  group_call_manager_->get_group_call(GroupCallId(request.group_call_id_), std::move(promise));
}

void Td::on_request(uint64 id, const td_api::checkDatabaseEncryptionKey &request) {
  send_error_raw(id, 400, "Unexpected checkDatabaseEncryptionKey");
}

void Td::on_request(uint64 id, const td_api::getLogTags &request) {
  UNREACHABLE();
}
}  // namespace td
