//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/ContactsManager.h"

#include "td/telegram/AuthManager.h"

#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogInviteLink.h"
#include "td/telegram/DialogLocation.h"


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




#include <utility>

namespace td {
}  // namespace td
namespace td {

void ContactsManager::on_get_channel_participants(
    ChannelId channel_id, ChannelParticipantsFilter filter, int32 offset, int32 limit, string additional_query,
    int32 additional_limit, tl_object_ptr<telegram_api::channels_channelParticipants> &&channel_participants,
    Promise<DialogParticipants> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  on_get_users(std::move(channel_participants->users_), "on_get_channel_participants");
  on_get_chats(std::move(channel_participants->chats_), "on_get_channel_participants");
  int32 total_count = channel_participants->count_;
  auto participants = std::move(channel_participants->participants_);
  LOG(INFO) << "Receive " << participants.size() << " " << filter << " members in " << channel_id;

  bool is_full = offset == 0 && static_cast<int32>(participants.size()) < limit && total_count < limit;

  vector<DialogParticipant> result;
  for (auto &participant_ptr : participants) {
    auto debug_participant = to_string(participant_ptr);
    result.emplace_back(std::move(participant_ptr));
    const auto &participant = result.back();
    UserId participant_user_id;
    if (participant.dialog_id_.get_type() == DialogType::User) {
      participant_user_id = participant.dialog_id_.get_user_id();
    }
    if (!participant.is_valid() || (filter.is_bots() && !is_user_bot(participant_user_id)) ||
        (filter.is_administrators() && !participant.status_.is_administrator()) ||
        ((filter.is_recent() || filter.is_contacts() || filter.is_search()) && !participant.status_.is_member()) ||
        (filter.is_contacts() && !is_user_contact(participant_user_id)) ||
        (filter.is_restricted() && !participant.status_.is_restricted()) ||
        (filter.is_banned() && !participant.status_.is_banned())) {
      bool skip_error = ((filter.is_administrators() || filter.is_bots()) && is_user_deleted(participant_user_id)) ||
                        (filter.is_contacts() && participant_user_id == get_my_id());
      if (!skip_error) {
        LOG(ERROR) << "Receive " << participant << ", while searching for " << filter << " in " << channel_id
                   << " with offset " << offset << " and limit " << limit << ": " << oneline(debug_participant);
      }
      result.pop_back();
      total_count--;
    }
  }

  if (total_count < narrow_cast<int32>(result.size())) {
    LOG(ERROR) << "Receive total_count = " << total_count << ", but have at least " << result.size() << " " << filter
               << " members in " << channel_id;
    total_count = static_cast<int32>(result.size());
  } else if (is_full && total_count > static_cast<int32>(result.size())) {
    LOG(ERROR) << "Fix total number of " << filter << " members from " << total_count << " to " << result.size()
               << " in " << channel_id;
    total_count = static_cast<int32>(result.size());
  }

  const auto max_participant_count = get_channel_type(channel_id) == ChannelType::Megagroup ? 975 : 195;
  auto participant_count =
      filter.is_recent() && total_count != 0 && total_count < max_participant_count ? total_count : -1;
  int32 administrator_count = filter.is_administrators() ? total_count : -1;
  if (is_full && (filter.is_administrators() || filter.is_bots() || filter.is_recent())) {
    vector<DialogAdministrator> administrators;
    vector<UserId> bot_user_ids;
    {
      if (filter.is_recent()) {
        for (const auto &participant : result) {
          if (participant.dialog_id_.get_type() == DialogType::User) {
            auto participant_user_id = participant.dialog_id_.get_user_id();
            if (participant.status_.is_administrator()) {
              administrators.emplace_back(participant_user_id, participant.status_.get_rank(),
                                          participant.status_.is_creator());
            }
            if (is_user_bot(participant_user_id)) {
              bot_user_ids.push_back(participant_user_id);
            }
          }
        }
        administrator_count = narrow_cast<int32>(administrators.size());

        if (get_channel_type(channel_id) == ChannelType::Megagroup && !td_->auth_manager_->is_bot()) {
          cached_channel_participants_[channel_id] = result;
          update_channel_online_member_count(channel_id, true);
        }
      } else if (filter.is_administrators()) {
        for (const auto &participant : result) {
          if (participant.dialog_id_.get_type() == DialogType::User) {
            administrators.emplace_back(participant.dialog_id_.get_user_id(), participant.status_.get_rank(),
                                        participant.status_.is_creator());
          }
        }
      } else if (filter.is_bots()) {
        bot_user_ids = transform(result, [](const DialogParticipant &participant) {
          CHECK(participant.dialog_id_.get_type() == DialogType::User);
          return participant.dialog_id_.get_user_id();
        });
      }
    }
    if (filter.is_administrators() || filter.is_recent()) {
      on_update_dialog_administrators(DialogId(channel_id), std::move(administrators), true, false);
    }
    if (filter.is_bots() || filter.is_recent()) {
      on_update_channel_bot_user_ids(channel_id, std::move(bot_user_ids));
    }
  }
  if (have_channel_participant_cache(channel_id)) {
    for (const auto &participant : result) {
      add_channel_participant_to_cache(channel_id, participant, false);
    }
  }

  if (participant_count != -1 || administrator_count != -1) {
    auto channel_full = get_channel_full_force(channel_id, true, "on_get_channel_participants_success");
    if (channel_full != nullptr) {
      if (administrator_count == -1) {
        administrator_count = channel_full->administrator_count;
      }
      if (participant_count == -1) {
        participant_count = channel_full->participant_count;
      }
      if (participant_count < administrator_count) {
        participant_count = administrator_count;
      }
      if (channel_full->participant_count != participant_count) {
        channel_full->participant_count = participant_count;
        channel_full->is_changed = true;
      }
      if (channel_full->administrator_count != administrator_count) {
        channel_full->administrator_count = administrator_count;
        channel_full->is_changed = true;
      }
      update_channel_full(channel_full, channel_id, "on_get_channel_participants");
    }
    if (participant_count != -1) {
      auto c = get_channel(channel_id);
      if (c != nullptr && c->participant_count != participant_count) {
        c->participant_count = participant_count;
        c->is_changed = true;
        update_channel(c, channel_id);
      }
    }
  }

  if (!additional_query.empty()) {
    auto dialog_ids = transform(result, [](const DialogParticipant &participant) { return participant.dialog_id_; });
    std::pair<int32, vector<DialogId>> result_dialog_ids =
        search_among_dialogs(dialog_ids, additional_query, additional_limit);

    total_count = result_dialog_ids.first;
    std::unordered_set<DialogId, DialogIdHash> result_dialog_ids_set(result_dialog_ids.second.begin(),
                                                                     result_dialog_ids.second.end());
    auto all_participants = std::move(result);
    result.clear();
    for (auto &participant : all_participants) {
      if (result_dialog_ids_set.count(participant.dialog_id_)) {
        result_dialog_ids_set.erase(participant.dialog_id_);
        result.push_back(std::move(participant));
      }
    }
  }

  promise.set_value(DialogParticipants{total_count, std::move(result)});
}

void ContactsManager::on_get_user_photos(UserId user_id, int32 offset, int32 limit, int32 total_count,
                                         vector<tl_object_ptr<telegram_api::Photo>> photos) {
  auto photo_count = narrow_cast<int32>(photos.size());
  int32 min_total_count = (offset >= 0 && photo_count > 0 ? offset : 0) + photo_count;
  if (total_count < min_total_count) {
    LOG(ERROR) << "Receive wrong photos total_count " << total_count << " for user " << user_id << ": receive "
               << photo_count << " photos with offset " << offset;
    total_count = min_total_count;
  }
  LOG_IF(ERROR, limit < photo_count) << "Requested not more than " << limit << " photos, but " << photo_count
                                     << " received";

  User *u = get_user(user_id);
  if (u == nullptr) {
    LOG(ERROR) << "Can't find " << user_id;
    return;
  }

  if (offset == -1) {
    // from reload_user_profile_photo
    CHECK(limit == 1);
    for (auto &photo_ptr : photos) {
      if (photo_ptr->get_id() == telegram_api::photo::ID) {
        auto server_photo = telegram_api::move_object_as<telegram_api::photo>(photo_ptr);
        if (server_photo->id_ == u->photo.id) {
          auto profile_photo = convert_photo_to_profile_photo(server_photo);
          if (profile_photo) {
            LOG_IF(ERROR, u->access_hash == -1) << "Receive profile photo of " << user_id << " without access hash";
            get_profile_photo(td_->file_manager_.get(), user_id, u->access_hash, std::move(profile_photo));
          } else {
            LOG(ERROR) << "Failed to get profile photo from " << to_string(server_photo);
          }
        }

        auto photo = get_photo(td_->file_manager_.get(), std::move(server_photo), DialogId(user_id));
        register_user_photo(u, user_id, photo);
      }
    }
    return;
  }

  LOG(INFO) << "Receive " << photo_count << " photos of " << user_id << " out of " << total_count << " with offset "
            << offset << " and limit " << limit;
  UserPhotos *user_photos = &user_photos_[user_id];
  user_photos->count = total_count;
  CHECK(user_photos->getting_now);
  user_photos->getting_now = false;

  if (user_photos->offset == -1) {
    user_photos->offset = 0;
    CHECK(user_photos->photos.empty());
  }

  if (offset != narrow_cast<int32>(user_photos->photos.size()) + user_photos->offset) {
    LOG(INFO) << "Inappropriate offset to append " << user_id << " profile photos to cache: offset = " << offset
              << ", current_offset = " << user_photos->offset << ", photo_count = " << user_photos->photos.size();
    user_photos->photos.clear();
    user_photos->offset = offset;
  }

  for (auto &photo : photos) {
    auto user_photo = get_photo(td_->file_manager_.get(), std::move(photo), DialogId(user_id));
    if (user_photo.is_empty()) {
      LOG(ERROR) << "Receive empty profile photo in getUserPhotos request for " << user_id << " with offset " << offset
                 << " and limit " << limit << ". Receive " << photo_count << " photos out of " << total_count
                 << " photos";
      user_photos->count--;
      CHECK(user_photos->count >= 0);
      continue;
    }

    user_photos->photos.push_back(std::move(user_photo));
    register_user_photo(u, user_id, user_photos->photos.back());
  }
  if (user_photos->offset > user_photos->count) {
    user_photos->offset = user_photos->count;
    user_photos->photos.clear();
  }

  auto known_photo_count = narrow_cast<int32>(user_photos->photos.size());
  if (user_photos->offset + known_photo_count > user_photos->count) {
    user_photos->photos.resize(user_photos->count - user_photos->offset);
  }
}

void ContactsManager::set_chat_participant_status(ChatId chat_id, UserId user_id, DialogParticipantStatus status,
                                                  Promise<Unit> &&promise) {
  if (!status.is_member()) {
    return delete_chat_participant(chat_id, user_id, false, std::move(promise));
  }
  if (status.is_creator()) {
    return promise.set_error(Status::Error(400, "Can't change owner in basic group chats"));
  }
  if (status.is_restricted()) {
    return promise.set_error(Status::Error(400, "Can't restrict users in basic group chats"));
  }

  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!c->is_active) {
    return promise.set_error(Status::Error(400, "Chat is deactivated"));
  }

  auto chat_full = get_chat_full(chat_id);
  if (chat_full == nullptr) {
    auto load_chat_full_promise =
        PromiseCreator::lambda([actor_id = actor_id(this), chat_id, user_id, status = std::move(status),
                                promise = std::move(promise)](Result<Unit> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            send_closure(actor_id, &ContactsManager::set_chat_participant_status, chat_id, user_id, status,
                         std::move(promise));
          }
        });
    return load_chat_full(chat_id, false, std::move(load_chat_full_promise), "set_chat_participant_status");
  }

  auto participant = get_chat_full_participant(chat_full, DialogId(user_id));
  if (participant == nullptr && !status.is_administrator()) {
    // the user isn't a member, but needs to be added
    return add_chat_participant(chat_id, user_id, 0, std::move(promise));
  }

  if (!get_chat_permissions(c).can_promote_members()) {
    return promise.set_error(Status::Error(400, "Need owner rights in the group chat"));
  }

  if (user_id == get_my_id()) {
    return promise.set_error(Status::Error(400, "Can't promote or demote self"));
  }

  if (participant == nullptr) {
    // the user must be added first
    CHECK(status.is_administrator());
    auto add_chat_participant_promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), chat_id, user_id, promise = std::move(promise)](Result<Unit> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            send_closure(actor_id, &ContactsManager::send_edit_chat_admin_query, chat_id, user_id, true,
                         std::move(promise));
          }
        });
    return add_chat_participant(chat_id, user_id, 0, std::move(add_chat_participant_promise));
  }

  send_edit_chat_admin_query(chat_id, user_id, status.is_administrator(), std::move(promise));
}

class GetMessageStatsQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::messageStatistics>> promise_;
  ChannelId channel_id_;

 public:
  explicit GetMessageStatsQuery(Promise<td_api::object_ptr<td_api::messageStatistics>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, MessageId message_id, bool is_dark, DcId dc_id) {
    channel_id_ = channel_id;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    int32 flags = 0;
    if (is_dark) {
      flags |= telegram_api::stats_getMessageStats::DARK_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::stats_getMessageStats(flags, false /*ignored*/, std::move(input_channel),
                                            message_id.get_server_message_id().get()),
        dc_id));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::stats_getMessageStats>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(td_->contacts_manager_->convert_message_stats(result_ptr.move_as_ok()));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetMessageStatsQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_get_channel_message_stats_query(
    DcId dc_id, FullMessageId full_message_id, bool is_dark,
    Promise<td_api::object_ptr<td_api::messageStatistics>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto dialog_id = full_message_id.get_dialog_id();
  if (!td_->messages_manager_->have_message_force(full_message_id, "send_get_channel_message_stats_query")) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (!td_->messages_manager_->can_get_message_statistics(full_message_id)) {
    return promise.set_error(Status::Error(400, "Message statistics is inaccessible"));
  }
  CHECK(dialog_id.get_type() == DialogType::Channel);
  td_->create_handler<GetMessageStatsQuery>(std::move(promise))
      ->send(dialog_id.get_channel_id(), full_message_id.get_message_id(), is_dark, dc_id);
}

void ContactsManager::on_update_chat_default_permissions(ChatId chat_id, RestrictedRights default_permissions,
                                                         int32 version) {
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id;
    return;
  }
  auto c = get_chat_force(chat_id);
  if (c == nullptr) {
    LOG(INFO) << "Ignoring update about unknown " << chat_id;
    return;
  }

  LOG(INFO) << "Receive updateChatDefaultBannedRights in " << chat_id << " with " << default_permissions
            << " and version " << version << ". Current version is " << c->version;

  if (c->status.is_left()) {
    // possible if updates come out of order
    LOG(WARNING) << "Receive updateChatDefaultBannedRights for left " << chat_id << ". Couldn't apply it";

    repair_chat_participants(chat_id);  // just in case
    return;
  }
  if (version <= -1) {
    LOG(ERROR) << "Receive wrong version " << version << " for " << chat_id;
    return;
  }
  CHECK(c->version >= 0);

  if (version > c->version) {
    // this should be unreachable, because version and default permissions must be already updated from
    // the chat object in on_chat_update
    if (version != c->version + 1) {
      LOG(INFO) << "Default permissions of " << chat_id << " with version " << c->version
                << " has changed, but new version is " << version;
      repair_chat_participants(chat_id);
      return;
    }

    LOG_IF(ERROR, default_permissions == c->default_permissions)
        << "Receive updateChatDefaultBannedRights in " << chat_id << " with version " << version
        << " and default_permissions = " << default_permissions
        << ", but default_permissions are not changed. Current version is " << c->version;
    c->version = version;
    c->need_save_to_database = true;
    on_update_chat_default_permissions(c, chat_id, default_permissions, version);
    update_chat(c, chat_id);
  }
}

tl_object_ptr<td_api::supergroupFullInfo> ContactsManager::get_supergroup_full_info_object(
    const ChannelFull *channel_full, ChannelId channel_id) const {
  CHECK(channel_full != nullptr);
  double slow_mode_delay_expires_in = 0;
  if (channel_full->slow_mode_next_send_date != 0) {
    slow_mode_delay_expires_in = max(channel_full->slow_mode_next_send_date - G()->server_time(), 1e-3);
  }
  auto bot_commands = transform(channel_full->bot_commands, [td = td_](const BotCommands &commands) {
    return commands.get_bot_commands_object(td);
  });
  return td_api::make_object<td_api::supergroupFullInfo>(
      get_chat_photo_object(td_->file_manager_.get(), channel_full->photo), channel_full->description,
      channel_full->participant_count, channel_full->administrator_count, channel_full->restricted_count,
      channel_full->banned_count, DialogId(channel_full->linked_channel_id).get(), channel_full->slow_mode_delay,
      slow_mode_delay_expires_in, channel_full->can_get_participants, channel_full->can_set_username,
      channel_full->can_set_sticker_set, channel_full->can_set_location, channel_full->can_view_statistics,
      channel_full->is_all_history_available, channel_full->sticker_set_id.get(),
      channel_full->location.get_chat_location_object(), channel_full->invite_link.get_chat_invite_link_object(this),
      std::move(bot_commands),
      get_basic_group_id_object(channel_full->migrated_from_chat_id, "get_supergroup_full_info_object"),
      channel_full->migrated_from_max_message_id.get());
}

void ContactsManager::ban_dialog_participant(DialogId dialog_id, DialogId participant_dialog_id,
                                             int32 banned_until_date, bool revoke_messages, Promise<Unit> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "ban_dialog_participant")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't ban members in private chats"));
    case DialogType::Chat:
      if (participant_dialog_id.get_type() != DialogType::User) {
        return promise.set_error(Status::Error(400, "Can't ban chats in basic groups"));
      }
      return delete_chat_participant(dialog_id.get_chat_id(), participant_dialog_id.get_user_id(), revoke_messages,
                                     std::move(promise));
    case DialogType::Channel:
      return set_channel_participant_status(dialog_id.get_channel_id(), participant_dialog_id,
                                            DialogParticipantStatus::Banned(banned_until_date), std::move(promise));
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't ban members in secret chats"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void ContactsManager::load_imported_contacts(Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    are_imported_contacts_loaded_ = true;
  }
  if (are_imported_contacts_loaded_) {
    LOG(INFO) << "Imported contacts are already loaded";
    promise.set_value(Unit());
    return;
  }
  load_imported_contacts_queries_.push_back(std::move(promise));
  if (load_imported_contacts_queries_.size() == 1u) {
    if (G()->parameters().use_chat_info_db) {
      LOG(INFO) << "Load imported contacts from database";
      G()->td_db()->get_sqlite_pmc()->get(
          "user_imported_contacts", PromiseCreator::lambda([](string value) {
            send_closure_later(G()->contacts_manager(), &ContactsManager::on_load_imported_contacts_from_database,
                               std::move(value));
          }));
    } else {
      LOG(INFO) << "Have no previously imported contacts";
      send_closure_later(G()->contacts_manager(), &ContactsManager::on_load_imported_contacts_from_database, string());
    }
  } else {
    LOG(INFO) << "Load imported contacts request has already been sent";
  }
}

tl_object_ptr<td_api::userFullInfo> ContactsManager::get_user_full_info_object(UserId user_id,
                                                                               const UserFull *user_full) const {
  CHECK(user_full != nullptr);
  bool is_bot = is_user_bot(user_id);
  auto commands = transform(user_full->commands, [](const auto &command) { return command.get_bot_command_object(); });
  return make_tl_object<td_api::userFullInfo>(
      get_chat_photo_object(td_->file_manager_.get(), user_full->photo), user_full->is_blocked,
      user_full->can_be_called, user_full->supports_video_calls, user_full->has_private_calls,
      !user_full->private_forward_name.empty(), user_full->need_phone_number_privacy_exception,
      is_bot ? string() : user_full->about, is_bot ? user_full->about : string(),
      is_bot ? user_full->description : string(), user_full->common_chat_count, std::move(commands));
}

bool ContactsManager::is_valid_username(const string &username) {
  if (username.size() < 5 || username.size() > 32) {
    return false;
  }
  if (!is_alpha(username[0])) {
    return false;
  }
  for (auto c : username) {
    if (!is_alpha(c) && !is_digit(c) && c != '_') {
      return false;
    }
  }
  if (username.back() == '_') {
    return false;
  }
  for (size_t i = 1; i < username.size(); i++) {
    if (username[i - 1] == '_' && username[i] == '_') {
      return false;
    }
  }
  if (username.find("admin") == 0 || username.find("telegram") == 0 || username.find("support") == 0 ||
      username.find("security") == 0 || username.find("settings") == 0 || username.find("contacts") == 0 ||
      username.find("service") == 0 || username.find("telegraph") == 0) {
    return false;
  }
  return true;
}

void ContactsManager::on_save_channel_to_database(ChannelId channel_id, bool success) {
  if (G()->close_flag()) {
    return;
  }

  Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);
  CHECK(c->is_being_saved);
  CHECK(load_channel_from_database_queries_.count(channel_id) == 0);
  c->is_being_saved = false;

  if (!success) {
    LOG(ERROR) << "Failed to save " << channel_id << " to database";
    c->is_saved = false;
  } else {
    LOG(INFO) << "Successfully saved " << channel_id << " to database";
  }
  if (c->is_saved) {
    if (c->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), c->log_event_id);
      c->log_event_id = 0;
    }
  } else {
    save_channel(c, channel_id, c->log_event_id != 0);
  }
}

void ContactsManager::load_channel_full(ChannelId channel_id, bool force, Promise<Unit> &&promise, const char *source) {
  auto channel_full = get_channel_full_force(channel_id, true, source);
  if (channel_full == nullptr) {
    return send_get_channel_full_query(channel_full, channel_id, std::move(promise), source);
  }
  if (channel_full->is_expired()) {
    if (td_->auth_manager_->is_bot() && !force) {
      return send_get_channel_full_query(channel_full, channel_id, std::move(promise), "load expired channel_full");
    }

    send_get_channel_full_query(channel_full, channel_id, Auto(), "load expired channel_full");
  }

  promise.set_value(Unit());
}

tl_object_ptr<telegram_api::InputPeer> ContactsManager::get_input_peer_channel(ChannelId channel_id,
                                                                               AccessRights access_rights) const {
  const Channel *c = get_channel(channel_id);
  if (!have_input_peer_channel(c, channel_id, access_rights)) {
    if (c == nullptr && td_->auth_manager_->is_bot() && channel_id.is_valid()) {
      return make_tl_object<telegram_api::inputPeerChannel>(channel_id.get(), 0);
    }
    return nullptr;
  }

  return make_tl_object<telegram_api::inputPeerChannel>(channel_id.get(), c->access_hash);
}

void ContactsManager::on_update_channel_default_permissions(Channel *c, ChannelId channel_id,
                                                            RestrictedRights default_permissions) {
  if (c->default_permissions != default_permissions) {
    LOG(INFO) << "Update " << channel_id << " default permissions from " << c->default_permissions << " to "
              << default_permissions;
    c->default_permissions = default_permissions;
    c->is_default_permissions_changed = true;
    c->need_save_to_database = true;
  }
}

void ContactsManager::on_update_user_photo(UserId user_id, tl_object_ptr<telegram_api::UserProfilePhoto> &&photo_ptr) {
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  User *u = get_user_force(user_id);
  if (u != nullptr) {
    on_update_user_photo(u, user_id, std::move(photo_ptr), "on_update_user_photo");
    update_user(u, user_id);
  } else {
    LOG(INFO) << "Ignore update user photo about unknown " << user_id;
  }
}

UserId ContactsManager::get_user_id(const tl_object_ptr<telegram_api::User> &user) {
  CHECK(user != nullptr);
  switch (user->get_id()) {
    case telegram_api::userEmpty::ID:
      return UserId(static_cast<const telegram_api::userEmpty *>(user.get())->id_);
    case telegram_api::user::ID:
      return UserId(static_cast<const telegram_api::user *>(user.get())->id_);
    default:
      UNREACHABLE();
      return UserId();
  }
}

td_api::object_ptr<td_api::updateUser> ContactsManager::get_update_unknown_user_object(UserId user_id) {
  return td_api::make_object<td_api::updateUser>(td_api::make_object<td_api::user>(
      user_id.get(), "", "", "", "", td_api::make_object<td_api::userStatusEmpty>(), nullptr, false, false, false,
      false, "", false, false, false, td_api::make_object<td_api::userTypeUnknown>(), ""));
}

void ContactsManager::on_update_channel_has_location(Channel *c, ChannelId channel_id, bool has_location) {
  if (c->has_location != has_location) {
    LOG(INFO) << "Update " << channel_id << " has_location from " << c->has_location << " to " << has_location;
    c->has_location = has_location;
    c->is_has_location_changed = true;
    c->is_changed = true;
  }
}

const DialogPhoto *ContactsManager::get_channel_dialog_photo(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    auto min_channel = get_min_channel(channel_id);
    if (min_channel != nullptr) {
      return &min_channel->photo_;
    }
    return nullptr;
  }
  return &c->photo;
}

double ContactsManager::get_percentage_value(double part, double total) {
  if (total < 1e-6 && total > -1e-6) {
    if (part < 1e-6 && part > -1e-6) {
      return 0.0;
    }
    return 100.0;
  }
  if (part > 1e20) {
    return 100.0;
  }
  return part / total * 100;
}

DialogParticipantStatus ContactsManager::get_channel_status(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return DialogParticipantStatus::Banned(0);
  }
  return get_channel_status(c);
}

tl_object_ptr<td_api::dateRange> ContactsManager::convert_date_range(
    const tl_object_ptr<telegram_api::statsDateRangeDays> &obj) {
  return make_tl_object<td_api::dateRange>(obj->min_date_, obj->max_date_);
}

FileId ContactsManager::get_profile_photo_file_id(int64 photo_id) const {
  auto it = my_photo_file_id_.find(photo_id);
  if (it == my_photo_file_id_.end()) {
    return FileId();
  }
  return it->second;
}

bool ContactsManager::get_chat_has_protected_content(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return false;
  }
  return c->noforwards;
}

string ContactsManager::get_chat_title(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return string();
  }
  return c->title;
}

UserId ContactsManager::get_service_notifications_user_id() {
  return UserId(static_cast<int64>(777000));
}

bool ContactsManager::have_chat(ChatId chat_id) const {
  return chats_.count(chat_id) > 0;
}
}  // namespace td
