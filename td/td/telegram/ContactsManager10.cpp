//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/ContactsManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ConfigShared.h"
#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogInviteLink.h"
#include "td/telegram/DialogLocation.h"
#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/Global.h"


#include "td/telegram/InputGroupCallId.h"

#include "td/telegram/logevent/LogEvent.h"

#include "td/telegram/MessageSender.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/MessageTtl.h"
#include "td/telegram/MinChannel.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/NetQuery.h"

#include "td/telegram/PasswordManager.h"
#include "td/telegram/Photo.h"
#include "td/telegram/Photo.hpp"
#include "td/telegram/SecretChatLayer.h"

#include "td/telegram/ServerMessageId.h"
#include "td/telegram/StickerSetId.hpp"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.hpp"
#include "td/telegram/UpdatesManager.h"
#include "td/telegram/Version.h"

#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"
#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/buffer.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"

#include <algorithm>
#include <limits>
#include <tuple>
#include <utility>

namespace td {
}  // namespace td
namespace td {

class ContactsManager::UploadProfilePhotoCallback final : public FileManager::UploadCallback {
 public:
  void on_upload_ok(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) final {
    send_closure_later(G()->contacts_manager(), &ContactsManager::on_upload_profile_photo, file_id,
                       std::move(input_file));
  }
  void on_upload_encrypted_ok(FileId file_id, tl_object_ptr<telegram_api::InputEncryptedFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_secure_ok(FileId file_id, tl_object_ptr<telegram_api::InputSecureFile> input_file) final {
    UNREACHABLE();
  }
  void on_upload_error(FileId file_id, Status error) final {
    send_closure_later(G()->contacts_manager(), &ContactsManager::on_upload_profile_photo_error, file_id,
                       std::move(error));
  }
};

ContactsManager::ContactsManager(Td *td, ActorShared<> parent) : td_(td), parent_(std::move(parent)) {
  upload_profile_photo_callback_ = std::make_shared<UploadProfilePhotoCallback>();

  my_id_ = load_my_id();

  G()->shared_config().set_option_integer("telegram_service_notifications_chat_id",
                                          DialogId(get_service_notifications_user_id()).get());
  G()->shared_config().set_option_integer("replies_bot_chat_id", DialogId(get_replies_bot_user_id()).get());
  G()->shared_config().set_option_integer("group_anonymous_bot_user_id", get_anonymous_bot_user_id().get());
  G()->shared_config().set_option_integer("channel_bot_user_id", get_channel_bot_user_id().get());

  if (G()->parameters().use_chat_info_db) {
    auto next_contacts_sync_date_string = G()->td_db()->get_binlog_pmc()->get("next_contacts_sync_date");
    if (!next_contacts_sync_date_string.empty()) {
      next_contacts_sync_date_ = min(to_integer<int32>(next_contacts_sync_date_string), G()->unix_time() + 100000);
    }

    auto saved_contact_count_string = G()->td_db()->get_binlog_pmc()->get("saved_contact_count");
    if (!saved_contact_count_string.empty()) {
      saved_contact_count_ = to_integer<int32>(saved_contact_count_string);
    }
  } else {
    G()->td_db()->get_binlog_pmc()->erase("next_contacts_sync_date");
    G()->td_db()->get_binlog_pmc()->erase("saved_contact_count");
  }
  if (G()->parameters().use_file_db) {
    G()->td_db()->get_sqlite_pmc()->erase_by_prefix("us_bot_info", Auto());
  }

  was_online_local_ = to_integer<int32>(G()->td_db()->get_binlog_pmc()->get("my_was_online_local"));
  was_online_remote_ = to_integer<int32>(G()->td_db()->get_binlog_pmc()->get("my_was_online_remote"));
  if (was_online_local_ >= G()->unix_time_cached() && !td_->is_online()) {
    was_online_local_ = G()->unix_time_cached() - 1;
  }

  location_visibility_expire_date_ =
      to_integer<int32>(G()->td_db()->get_binlog_pmc()->get("location_visibility_expire_date"));
  if (location_visibility_expire_date_ != 0 && location_visibility_expire_date_ <= G()->unix_time()) {
    location_visibility_expire_date_ = 0;
    G()->td_db()->get_binlog_pmc()->erase("location_visibility_expire_date");
  }
  auto pending_location_visibility_expire_date_string =
      G()->td_db()->get_binlog_pmc()->get("pending_location_visibility_expire_date");
  if (!pending_location_visibility_expire_date_string.empty()) {
    pending_location_visibility_expire_date_ = to_integer<int32>(pending_location_visibility_expire_date_string);
    try_send_set_location_visibility_query();
  }
  update_is_location_visible();
  LOG(INFO) << "Loaded location_visibility_expire_date = " << location_visibility_expire_date_
            << " and pending_location_visibility_expire_date = " << pending_location_visibility_expire_date_;

  user_online_timeout_.set_callback(on_user_online_timeout_callback);
  user_online_timeout_.set_callback_data(static_cast<void *>(this));

  channel_unban_timeout_.set_callback(on_channel_unban_timeout_callback);
  channel_unban_timeout_.set_callback_data(static_cast<void *>(this));

  user_nearby_timeout_.set_callback(on_user_nearby_timeout_callback);
  user_nearby_timeout_.set_callback_data(static_cast<void *>(this));

  slow_mode_delay_timeout_.set_callback(on_slow_mode_delay_timeout_callback);
  slow_mode_delay_timeout_.set_callback_data(static_cast<void *>(this));

  invite_link_info_expire_timeout_.set_callback(on_invite_link_info_expire_timeout_callback);
  invite_link_info_expire_timeout_.set_callback_data(static_cast<void *>(this));

  channel_participant_cache_timeout_.set_callback(on_channel_participant_cache_timeout_callback);
  channel_participant_cache_timeout_.set_callback_data(static_cast<void *>(this));
}

void ContactsManager::upload_profile_photo(FileId file_id, bool is_animation, double main_frame_timestamp,
                                           Promise<Unit> &&promise, int reupload_count, vector<int> bad_parts) {
  CHECK(file_id.is_valid());
  CHECK(uploaded_profile_photos_.find(file_id) == uploaded_profile_photos_.end());
  uploaded_profile_photos_.emplace(
      file_id, UploadedProfilePhoto{main_frame_timestamp, is_animation, reupload_count, std::move(promise)});
  LOG(INFO) << "Ask to upload " << (is_animation ? "animated" : "static") << " profile photo " << file_id
            << " with bad parts " << bad_parts;
  // TODO use force_reupload if reupload_count >= 1, replace reupload_count with is_reupload
  td_->file_manager_->resume_upload(file_id, std::move(bad_parts), upload_profile_photo_callback_, 32, 0);
}

class GetChannelParticipantsQuery final : public Td::ResultHandler {
  Promise<tl_object_ptr<telegram_api::channels_channelParticipants>> promise_;
  ChannelId channel_id_;

 public:
  explicit GetChannelParticipantsQuery(Promise<tl_object_ptr<telegram_api::channels_channelParticipants>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, const ChannelParticipantsFilter &filter, int32 offset, int32 limit) {
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      return promise_.set_error(Status::Error(400, "Supergroup not found"));
    }

    channel_id_ = channel_id;
    send_query(G()->net_query_creator().create(telegram_api::channels_getParticipants(
        std::move(input_channel), filter.get_input_channel_participants_filter(), offset, limit, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getParticipants>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto participants_ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetChannelParticipantsQuery: " << to_string(participants_ptr);
    switch (participants_ptr->get_id()) {
      case telegram_api::channels_channelParticipants::ID: {
        promise_.set_value(telegram_api::move_object_as<telegram_api::channels_channelParticipants>(participants_ptr));
        break;
      }
      case telegram_api::channels_channelParticipantsNotModified::ID:
        LOG(ERROR) << "Receive channelParticipantsNotModified";
        return on_error(Status::Error(500, "Receive channelParticipantsNotModified"));
      default:
        UNREACHABLE();
    }
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetChannelParticipantsQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::get_channel_participants(ChannelId channel_id,
                                               tl_object_ptr<td_api::SupergroupMembersFilter> &&filter,
                                               string additional_query, int32 offset, int32 limit,
                                               int32 additional_limit, Promise<DialogParticipants> &&promise) {
  if (limit <= 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be positive"));
  }
  if (limit > MAX_GET_CHANNEL_PARTICIPANTS) {
    limit = MAX_GET_CHANNEL_PARTICIPANTS;
  }

  if (offset < 0) {
    return promise.set_error(Status::Error(400, "Parameter offset must be non-negative"));
  }

  auto channel_full = get_channel_full_force(channel_id, true, "get_channel_participants");
  if (channel_full != nullptr && !channel_full->is_expired() && !channel_full->can_get_participants) {
    return promise.set_error(Status::Error(400, "Member list is inaccessible"));
  }

  ChannelParticipantsFilter participants_filter(filter);
  auto get_channel_participants_promise = PromiseCreator::lambda(
      [actor_id = actor_id(this), channel_id, filter = participants_filter,
       additional_query = std::move(additional_query), offset, limit, additional_limit, promise = std::move(promise)](
          Result<tl_object_ptr<telegram_api::channels_channelParticipants>> &&result) mutable {
        if (result.is_error()) {
          promise.set_error(result.move_as_error());
        } else {
          send_closure(actor_id, &ContactsManager::on_get_channel_participants, channel_id, std::move(filter), offset,
                       limit, std::move(additional_query), additional_limit, result.move_as_ok(), std::move(promise));
        }
      });
  td_->create_handler<GetChannelParticipantsQuery>(std::move(get_channel_participants_promise))
      ->send(channel_id, participants_filter, offset, limit);
}

void ContactsManager::on_update_bot_commands(DialogId dialog_id, UserId bot_user_id,
                                             vector<tl_object_ptr<telegram_api::botCommand>> &&bot_commands) {
  if (!bot_user_id.is_valid()) {
    LOG(ERROR) << "Receive updateBotCOmmands about invalid " << bot_user_id;
    return;
  }
  if (!have_user(bot_user_id) || !is_user_bot(bot_user_id)) {
    return;
  }
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto is_from_bot = [bot_user_id](const BotCommands &commands) {
    return commands.get_bot_user_id() == bot_user_id;
  };

  switch (dialog_id.get_type()) {
    case DialogType::User: {
      UserId user_id(dialog_id.get_user_id());
      auto user_full = get_user_full(user_id);
      if (user_full != nullptr) {
        on_update_user_full_commands(user_full, user_id, std::move(bot_commands));
        update_user_full(user_full, user_id, "on_update_bot_commands");
      }
      break;
    }
    case DialogType::Chat: {
      ChatId chat_id(dialog_id.get_chat_id());
      auto chat_full = get_chat_full(chat_id);
      if (chat_full != nullptr) {
        if (bot_commands.empty()) {
          if (td::remove_if(chat_full->bot_commands, is_from_bot)) {
            chat_full->is_changed = true;
          }
        } else {
          BotCommands commands(bot_user_id, std::move(bot_commands));
          auto it = std::find_if(chat_full->bot_commands.begin(), chat_full->bot_commands.end(), is_from_bot);
          if (it != chat_full->bot_commands.end()) {
            if (*it != commands) {
              *it = std::move(commands);
              chat_full->is_changed = true;
            }
          } else {
            chat_full->bot_commands.push_back(std::move(commands));
            chat_full->is_changed = true;
          }
        }
        update_chat_full(chat_full, chat_id, "on_update_bot_commands");
      }
      break;
    }
    case DialogType::Channel: {
      ChannelId channel_id(dialog_id.get_channel_id());
      auto channel_full = get_channel_full(channel_id, true, "on_update_bot_commands");
      if (channel_full != nullptr) {
        if (bot_commands.empty()) {
          if (td::remove_if(channel_full->bot_commands, is_from_bot)) {
            channel_full->is_changed = true;
          }
        } else {
          BotCommands commands(bot_user_id, std::move(bot_commands));
          auto it = std::find_if(channel_full->bot_commands.begin(), channel_full->bot_commands.end(), is_from_bot);
          if (it != channel_full->bot_commands.end()) {
            if (*it != commands) {
              *it = std::move(commands);
              channel_full->is_changed = true;
            }
          } else {
            channel_full->bot_commands.push_back(std::move(commands));
            channel_full->is_changed = true;
          }
        }
        update_channel_full(channel_full, channel_id, "on_update_bot_commands");
      }
      break;
    }
    case DialogType::SecretChat:
    default:
      LOG(ERROR) << "Receive updateBotCommands in " << dialog_id;
      break;
  }
}

class GetChatsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetChatsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<int64> &&chat_ids) {
    send_query(G()->net_query_creator().create(telegram_api::messages_getChats(std::move(chat_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getChats>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto chats_ptr = result_ptr.move_as_ok();
    int32 constructor_id = chats_ptr->get_id();
    switch (constructor_id) {
      case telegram_api::messages_chats::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chats>(chats_ptr);
        td_->contacts_manager_->on_get_chats(std::move(chats->chats_), "GetChatsQuery");
        break;
      }
      case telegram_api::messages_chatsSlice::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chatsSlice>(chats_ptr);
        LOG(ERROR) << "Receive chatsSlice in result of GetChatsQuery";
        td_->contacts_manager_->on_get_chats(std::move(chats->chats_), "GetChatsQuery");
        break;
      }
      default:
        UNREACHABLE();
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

bool ContactsManager::get_chat(ChatId chat_id, int left_tries, Promise<Unit> &&promise) {
  if (!chat_id.is_valid()) {
    promise.set_error(Status::Error(400, "Invalid basic group identifier"));
    return false;
  }

  if (!have_chat(chat_id)) {
    if (left_tries > 2 && G()->parameters().use_chat_info_db) {
      send_closure_later(actor_id(this), &ContactsManager::load_chat_from_database, nullptr, chat_id,
                         std::move(promise));
      return false;
    }

    if (left_tries > 1) {
      td_->create_handler<GetChatsQuery>(std::move(promise))->send(vector<int64>{chat_id.get()});
      return false;
    }

    promise.set_error(Status::Error(400, "Group not found"));
    return false;
  }

  promise.set_value(Unit());
  return true;
}

void ContactsManager::reload_chat(ChatId chat_id, Promise<Unit> &&promise) {
  if (!chat_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid basic group identifier"));
  }

  // there is no much reason to combine different requests into one request
  td_->create_handler<GetChatsQuery>(std::move(promise))->send(vector<int64>{chat_id.get()});
}

class ConvertToGigagroupQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit ConvertToGigagroupQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id) {
    channel_id_ = channel_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::channels_convertToGigagroup(std::move(input_channel))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_convertToGigagroup>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ConvertToGigagroupQuery: " << to_string(ptr);

    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      promise_.set_value(Unit());
      return;
    } else {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ConvertToGigagroupQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::convert_channel_to_gigagroup(ChannelId channel_id, Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }
  if (!get_channel_permissions(c).is_creator()) {
    return promise.set_error(Status::Error(400, "Not enough rights to convert group to broadcast group"));
  }
  if (get_channel_type(c) != ChannelType::Megagroup) {
    return promise.set_error(Status::Error(400, "Chat must be a supergroup"));
  }

  remove_dialog_suggested_action(SuggestedAction{SuggestedAction::Type::ConvertToGigagroup, DialogId(channel_id)});

  td_->create_handler<ConvertToGigagroupQuery>(std::move(promise))->send(channel_id);
}

void ContactsManager::search_dialog_participants(DialogId dialog_id, const string &query, int32 limit,
                                                 DialogParticipantsFilter filter,
                                                 Promise<DialogParticipants> &&promise) {
  LOG(INFO) << "Receive searchChatMembers request to search for \"" << query << "\" in " << dialog_id << " with filter "
            << filter;
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "search_dialog_participants")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (limit < 0) {
    return promise.set_error(Status::Error(400, "Parameter limit must be non-negative"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      promise.set_value(search_private_chat_participants(get_my_id(), dialog_id.get_user_id(), query, limit, filter));
      return;
    case DialogType::Chat:
      return search_chat_participants(dialog_id.get_chat_id(), query, limit, filter, std::move(promise));
    case DialogType::Channel: {
      auto channel_id = dialog_id.get_channel_id();
      if (filter.has_query()) {
        return get_channel_participants(channel_id, filter.get_supergroup_members_filter_object(query), string(), 0,
                                        limit, 0, std::move(promise));
      } else {
        return get_channel_participants(channel_id, filter.get_supergroup_members_filter_object(string()), query, 0,
                                        100, limit, std::move(promise));
      }
    }
    case DialogType::SecretChat: {
      auto peer_user_id = get_secret_chat_user_id(dialog_id.get_secret_chat_id());
      promise.set_value(search_private_chat_participants(get_my_id(), peer_user_id, query, limit, filter));
      return;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
      promise.set_error(Status::Error(500, "Wrong chat type"));
  }
}

void ContactsManager::on_update_chat_full_photo(ChatFull *chat_full, ChatId chat_id, Photo photo) {
  CHECK(chat_full != nullptr);
  if (photo != chat_full->photo) {
    chat_full->photo = std::move(photo);
    chat_full->is_changed = true;
  }
  if (chat_full->photo.is_empty()) {
    drop_chat_photos(chat_id, true, false, "on_update_chat_full_photo");
  }

  auto photo_file_ids = photo_get_file_ids(chat_full->photo);
  if (chat_full->registered_photo_file_ids == photo_file_ids) {
    return;
  }

  auto &file_source_id = chat_full->file_source_id;
  if (!file_source_id.is_valid()) {
    auto it = chat_full_file_source_ids_.find(chat_id);
    if (it != chat_full_file_source_ids_.end()) {
      VLOG(file_references) << "Move " << it->second << " inside of " << chat_id;
      file_source_id = it->second;
      chat_full_file_source_ids_.erase(it);
    } else {
      VLOG(file_references) << "Need to create new file source for full " << chat_id;
      file_source_id = td_->file_reference_manager_->create_chat_full_file_source(chat_id);
    }
  }

  for (auto &file_id : chat_full->registered_photo_file_ids) {
    td_->file_manager_->remove_file_source(file_id, file_source_id);
  }
  chat_full->registered_photo_file_ids = std::move(photo_file_ids);
  for (auto &file_id : chat_full->registered_photo_file_ids) {
    td_->file_manager_->add_file_source(file_id, file_source_id);
  }
}

void ContactsManager::on_update_user_photo(User *u, UserId user_id,
                                           tl_object_ptr<telegram_api::UserProfilePhoto> &&photo, const char *source) {
  if (td_->auth_manager_->is_bot() && !G()->parameters().use_file_db && !u->is_photo_inited) {
    if (photo != nullptr && photo->get_id() == telegram_api::userProfilePhoto::ID) {
      auto *profile_photo = static_cast<telegram_api::userProfilePhoto *>(photo.get());
      if ((profile_photo->flags_ & telegram_api::userProfilePhoto::STRIPPED_THUMB_MASK) != 0) {
        profile_photo->flags_ -= telegram_api::userProfilePhoto::STRIPPED_THUMB_MASK;
        profile_photo->stripped_thumb_ = BufferSlice();
      }
    }
    auto &old_photo = pending_user_photos_[user_id];
    if (!LOG_IS_STRIPPED(ERROR) && to_string(old_photo) == to_string(photo)) {
      return;
    }

    bool is_empty = photo == nullptr || photo->get_id() == telegram_api::userProfilePhotoEmpty::ID;
    old_photo = std::move(photo);

    drop_user_photos(user_id, is_empty, true, "on_update_user_photo");
    return;
  }

  do_update_user_photo(u, user_id, std::move(photo), source);
}

tl_object_ptr<td_api::user> ContactsManager::get_user_object(UserId user_id, const User *u) const {
  if (u == nullptr) {
    return nullptr;
  }
  tl_object_ptr<td_api::UserType> type;
  if (u->is_deleted) {
    type = make_tl_object<td_api::userTypeDeleted>();
  } else if (u->is_bot) {
    type = make_tl_object<td_api::userTypeBot>(u->can_join_groups, u->can_read_all_group_messages, u->is_inline_bot,
                                               u->inline_query_placeholder, u->need_location_bot);
  } else {
    type = make_tl_object<td_api::userTypeRegular>();
  }

  return make_tl_object<td_api::user>(
      user_id.get(), u->first_name, u->last_name, u->username, u->phone_number, get_user_status_object(user_id, u),
      get_profile_photo_object(td_->file_manager_.get(), u->photo), u->is_contact, u->is_mutual_contact, u->is_verified,
      u->is_support, get_restriction_reason_description(u->restriction_reasons), u->is_scam, u->is_fake, u->is_received,
      std::move(type), u->language_code);
}

void ContactsManager::set_location_visibility() {
  bool is_location_visible = G()->shared_config().get_option_boolean("is_location_visible");
  auto pending_location_visibility_expire_date = is_location_visible ? std::numeric_limits<int32>::max() : 0;
  if (pending_location_visibility_expire_date_ == -1 &&
      pending_location_visibility_expire_date == location_visibility_expire_date_) {
    return;
  }
  if (pending_location_visibility_expire_date_ != pending_location_visibility_expire_date) {
    pending_location_visibility_expire_date_ = pending_location_visibility_expire_date;
    G()->td_db()->get_binlog_pmc()->set("pending_location_visibility_expire_date",
                                        to_string(pending_location_visibility_expire_date));
    update_is_location_visible();
  }
  try_send_set_location_visibility_query();
}

tl_object_ptr<td_api::basicGroupFullInfo> ContactsManager::get_basic_group_full_info_object(
    const ChatFull *chat_full) const {
  CHECK(chat_full != nullptr);
  auto bot_commands = transform(chat_full->bot_commands, [td = td_](const BotCommands &commands) {
    return commands.get_bot_commands_object(td);
  });
  return make_tl_object<td_api::basicGroupFullInfo>(
      get_chat_photo_object(td_->file_manager_.get(), chat_full->photo), chat_full->description,
      get_user_id_object(chat_full->creator_user_id, "basicGroupFullInfo"),
      transform(chat_full->participants,
                [this](const DialogParticipant &chat_participant) { return get_chat_member_object(chat_participant); }),
      chat_full->invite_link.get_chat_invite_link_object(this), std::move(bot_commands));
}

void ContactsManager::load_secret_chat_from_database_impl(SecretChatId secret_chat_id, Promise<Unit> promise) {
  LOG(INFO) << "Load " << secret_chat_id << " from database";
  auto &load_secret_chat_queries = load_secret_chat_from_database_queries_[secret_chat_id];
  load_secret_chat_queries.push_back(std::move(promise));
  if (load_secret_chat_queries.size() == 1u) {
    G()->td_db()->get_sqlite_pmc()->get(
        get_secret_chat_database_key(secret_chat_id), PromiseCreator::lambda([secret_chat_id](string value) {
          send_closure(G()->contacts_manager(), &ContactsManager::on_load_secret_chat_from_database, secret_chat_id,
                       std::move(value), false);
        }));
  }
}

void ContactsManager::save_channel_to_database_impl(Channel *c, ChannelId channel_id, string value) {
  CHECK(c != nullptr);
  CHECK(load_channel_from_database_queries_.count(channel_id) == 0);
  CHECK(!c->is_being_saved);
  c->is_being_saved = true;
  c->is_saved = true;
  LOG(INFO) << "Trying to save to database " << channel_id;
  G()->td_db()->get_sqlite_pmc()->set(
      get_channel_database_key(channel_id), std::move(value), PromiseCreator::lambda([channel_id](Result<> result) {
        send_closure(G()->contacts_manager(), &ContactsManager::on_save_channel_to_database, channel_id,
                     result.is_ok());
      }));
}

bool ContactsManager::is_dialog_info_received_from_server(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      auto u = get_user(dialog_id.get_user_id());
      return u != nullptr && u->is_received_from_server;
    }
    case DialogType::Chat: {
      auto c = get_chat(dialog_id.get_chat_id());
      return c != nullptr && c->is_received_from_server;
    }
    case DialogType::Channel: {
      auto c = get_channel(dialog_id.get_channel_id());
      return c != nullptr && c->is_received_from_server;
    }
    default:
      return false;
  }
}

void ContactsManager::on_reload_dialog_administrators(
    DialogId dialog_id, Promise<td_api::object_ptr<td_api::chatAdministrators>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto it = dialog_administrators_.find(dialog_id);
  if (it != dialog_administrators_.end()) {
    return promise.set_value(get_chat_administrators_object(it->second));
  }

  LOG(ERROR) << "Failed to load administrators in " << dialog_id;
  promise.set_error(Status::Error(500, "Failed to find chat administrators"));
}

void ContactsManager::save_secret_chat_to_database(SecretChat *c, SecretChatId secret_chat_id) {
  CHECK(c != nullptr);
  if (c->is_being_saved) {
    return;
  }
  if (loaded_from_database_secret_chats_.count(secret_chat_id)) {
    save_secret_chat_to_database_impl(c, secret_chat_id, get_secret_chat_database_value(c));
    return;
  }
  if (load_secret_chat_from_database_queries_.count(secret_chat_id) != 0) {
    return;
  }

  load_secret_chat_from_database_impl(secret_chat_id, Auto());
}

bool ContactsManager::have_input_peer_chat(const Chat *c, AccessRights access_rights) {
  if (c == nullptr) {
    return false;
  }
  if (access_rights == AccessRights::Know) {
    return true;
  }
  if (access_rights == AccessRights::Read) {
    return true;
  }
  if (c->status.is_left()) {
    return false;
  }
  if (access_rights == AccessRights::Write && !c->is_active) {
    return false;
  }
  return true;
}

void ContactsManager::after_get_difference() {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  get_user(get_my_id(), 3, Promise<Unit>());

  if (td_->is_online()) {
    reload_created_public_dialogs(PublicDialogType::HasUsername, Promise<td_api::object_ptr<td_api::chats>>());
    reload_created_public_dialogs(PublicDialogType::IsLocationBased, Promise<td_api::object_ptr<td_api::chats>>());
  }
}

bool ContactsManager::have_input_encrypted_peer(const SecretChat *secret_chat, AccessRights access_rights) {
  if (secret_chat == nullptr) {
    return false;
  }
  if (access_rights == AccessRights::Know) {
    return true;
  }
  if (access_rights == AccessRights::Read) {
    return true;
  }
  return secret_chat->state == SecretChatState::Active;
}

void ContactsManager::on_get_channel_full_failed(ChannelId channel_id) {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Failed to get full " << channel_id;
  auto channel_full = get_channel_full(channel_id, true, "on_get_channel_full");
  if (channel_full != nullptr) {
    channel_full->repair_request_version = 0;
  }
}

DialogParticipantStatus ContactsManager::get_chat_permissions(const Chat *c) const {
  if (!c->is_active) {
    return DialogParticipantStatus::Banned(0);
  }
  return c->status.apply_restrictions(c->default_permissions, td_->auth_manager_->is_bot());
}

const ContactsManager::SecretChat *ContactsManager::get_secret_chat(SecretChatId secret_chat_id) const {
  auto it = secret_chats_.find(secret_chat_id);
  if (it == secret_chats_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const ContactsManager::ChatFull *ContactsManager::get_chat_full(ChatId chat_id) const {
  auto p = chats_full_.find(chat_id);
  if (p == chats_full_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

bool ContactsManager::have_input_encrypted_peer(SecretChatId secret_chat_id, AccessRights access_rights) const {
  return have_input_encrypted_peer(get_secret_chat(secret_chat_id), access_rights);
}

tl_object_ptr<td_api::secretChat> ContactsManager::get_secret_chat_object(SecretChatId secret_chat_id) {
  return get_secret_chat_object(secret_chat_id, get_secret_chat(secret_chat_id));
}

bool ContactsManager::have_secret_chat_force(SecretChatId secret_chat_id) {
  return get_secret_chat_force(secret_chat_id) != nullptr;
}

bool ContactsManager::is_channel_public(const Channel *c) {
  return c != nullptr && (!c->username.empty() || c->has_location);
}
}  // namespace td
