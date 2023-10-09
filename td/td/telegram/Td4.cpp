//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/Td.h"

#include "td/telegram/Account.h"
#include "td/telegram/AnimationsManager.h"
#include "td/telegram/AudiosManager.h"
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
#include "td/telegram/DocumentsManager.h"

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
#include "td/telegram/HashtagHints.h"
#include "td/telegram/InlineQueriesManager.h"
#include "td/telegram/JsonValue.h"
#include "td/telegram/LanguagePackManager.h"
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
#include "td/telegram/NotificationManager.h"
#include "td/telegram/NotificationSettings.h"

#include "td/telegram/PasswordManager.h"
#include "td/telegram/Payments.h"
#include "td/telegram/PhoneNumberManager.h"
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
#include "td/telegram/SponsoredMessageManager.h"
#include "td/telegram/StateManager.h"
#include "td/telegram/StickerSetId.h"
#include "td/telegram/StickersManager.h"
#include "td/telegram/StorageManager.h"
#include "td/telegram/SuggestedAction.h"
#include "td/telegram/td_api.hpp"

#include "td/telegram/telegram_api.hpp"
#include "td/telegram/ThemeManager.h"

#include "td/telegram/TopDialogManager.h"
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

void Td::init_managers() {
  VLOG(td_init) << "Create Managers";
  audios_manager_ = make_unique<AudiosManager>(this);
  callback_queries_manager_ = make_unique<CallbackQueriesManager>(this);
  documents_manager_ = make_unique<DocumentsManager>(this);
  video_notes_manager_ = make_unique<VideoNotesManager>(this);
  videos_manager_ = make_unique<VideosManager>(this);
  voice_notes_manager_ = make_unique<VoiceNotesManager>(this);

  animations_manager_ = make_unique<AnimationsManager>(this, create_reference());
  animations_manager_actor_ = register_actor("AnimationsManager", animations_manager_.get());
  G()->set_animations_manager(animations_manager_actor_.get());
  background_manager_ = make_unique<BackgroundManager>(this, create_reference());
  background_manager_actor_ = register_actor("BackgroundManager", background_manager_.get());
  G()->set_background_manager(background_manager_actor_.get());
  contacts_manager_ = make_unique<ContactsManager>(this, create_reference());
  contacts_manager_actor_ = register_actor("ContactsManager", contacts_manager_.get());
  G()->set_contacts_manager(contacts_manager_actor_.get());
  country_info_manager_ = make_unique<CountryInfoManager>(this, create_reference());
  country_info_manager_actor_ = register_actor("CountryInfoManager", country_info_manager_.get());
  game_manager_ = make_unique<GameManager>(this, create_reference());
  game_manager_actor_ = register_actor("GameManager", game_manager_.get());
  G()->set_game_manager(game_manager_actor_.get());
  group_call_manager_ = make_unique<GroupCallManager>(this, create_reference());
  group_call_manager_actor_ = register_actor("GroupCallManager", group_call_manager_.get());
  G()->set_group_call_manager(group_call_manager_actor_.get());
  inline_queries_manager_ = make_unique<InlineQueriesManager>(this, create_reference());
  inline_queries_manager_actor_ = register_actor("InlineQueriesManager", inline_queries_manager_.get());
  link_manager_ = make_unique<LinkManager>(this, create_reference());
  link_manager_actor_ = register_actor("LinkManager", link_manager_.get());
  G()->set_link_manager(link_manager_actor_.get());
  messages_manager_ = make_unique<MessagesManager>(this, create_reference());
  messages_manager_actor_ = register_actor("MessagesManager", messages_manager_.get());
  G()->set_messages_manager(messages_manager_actor_.get());
  notification_manager_ = make_unique<NotificationManager>(this, create_reference());
  notification_manager_actor_ = register_actor("NotificationManager", notification_manager_.get());
  G()->set_notification_manager(notification_manager_actor_.get());
  poll_manager_ = make_unique<PollManager>(this, create_reference());
  poll_manager_actor_ = register_actor("PollManager", poll_manager_.get());
  sponsored_message_manager_ = make_unique<SponsoredMessageManager>(this, create_reference());
  sponsored_message_manager_actor_ = register_actor("SponsoredMessageManager", sponsored_message_manager_.get());
  G()->set_sponsored_message_manager(sponsored_message_manager_actor_.get());
  stickers_manager_ = make_unique<StickersManager>(this, create_reference());
  stickers_manager_actor_ = register_actor("StickersManager", stickers_manager_.get());
  G()->set_stickers_manager(stickers_manager_actor_.get());
  theme_manager_ = make_unique<ThemeManager>(this, create_reference());
  theme_manager_actor_ = register_actor("ThemeManager", theme_manager_.get());
  G()->set_theme_manager(theme_manager_actor_.get());
  top_dialog_manager_ = make_unique<TopDialogManager>(this, create_reference());
  top_dialog_manager_actor_ = register_actor("TopDialogManager", top_dialog_manager_.get());
  G()->set_top_dialog_manager(top_dialog_manager_actor_.get());
  updates_manager_ = make_unique<UpdatesManager>(this, create_reference());
  updates_manager_actor_ = register_actor("UpdatesManager", updates_manager_.get());
  G()->set_updates_manager(updates_manager_actor_.get());
  web_pages_manager_ = make_unique<WebPagesManager>(this, create_reference());
  web_pages_manager_actor_ = register_actor("WebPagesManager", web_pages_manager_.get());
  G()->set_web_pages_manager(web_pages_manager_actor_.get());

  call_manager_ = create_actor<CallManager>("CallManager", create_reference());
  G()->set_call_manager(call_manager_.get());
  change_phone_number_manager_ = create_actor<PhoneNumberManager>(
      "ChangePhoneNumberManager", PhoneNumberManager::Type::ChangePhone, create_reference());
  confirm_phone_number_manager_ = create_actor<PhoneNumberManager>(
      "ConfirmPhoneNumberManager", PhoneNumberManager::Type::ConfirmPhone, create_reference());
  device_token_manager_ = create_actor<DeviceTokenManager>("DeviceTokenManager", create_reference());
  hashtag_hints_ = create_actor<HashtagHints>("HashtagHints", "text", create_reference());
  language_pack_manager_ = create_actor<LanguagePackManager>("LanguagePackManager", create_reference());
  G()->set_language_pack_manager(language_pack_manager_.get());
  password_manager_ = create_actor<PasswordManager>("PasswordManager", create_reference());
  G()->set_password_manager(password_manager_.get());
  privacy_manager_ = create_actor<PrivacyManager>("PrivacyManager", create_reference());
  secret_chats_manager_ = create_actor<SecretChatsManager>("SecretChatsManager", create_reference());
  G()->set_secret_chats_manager(secret_chats_manager_.get());
  secure_manager_ = create_actor<SecureManager>("SecureManager", create_reference());
  verify_phone_number_manager_ = create_actor<PhoneNumberManager>(
      "VerifyPhoneNumberManager", PhoneNumberManager::Type::VerifyPhone, create_reference());
}

void Td::on_request(uint64 id, const td_api::clearAllDraftMessages &request) {
  CHECK_IS_USER();
  CREATE_OK_REQUEST_PROMISE();
  messages_manager_->clear_all_draft_messages(request.exclude_secret_chats_, std::move(promise));
}

td_api::object_ptr<td_api::Object> Td::do_static_request(const td_api::addLogMessage &request) {
  Logging::add_message(request.verbosity_level_, request.text_);
  return td_api::make_object<td_api::ok>();
}

void Td::on_request(uint64 id, const td_api::getConnectedWebsites &request) {
  CHECK_IS_USER();
  CREATE_REQUEST_PROMISE();
  get_connected_websites(this, std::move(promise));
}
}  // namespace td
