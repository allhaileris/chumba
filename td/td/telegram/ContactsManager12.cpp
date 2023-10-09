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
#include "td/telegram/GroupCallManager.h"

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




#include <utility>

namespace td {
}  // namespace td
namespace td {

void ContactsManager::speculative_add_channel_user(ChannelId channel_id, UserId user_id,
                                                   const DialogParticipantStatus &new_status,
                                                   const DialogParticipantStatus &old_status) {
  auto c = get_channel_force(channel_id);
  // channel full must be loaded before c->participant_count is updated, because on_load_channel_full_from_database
  // must copy the initial c->participant_count before it is speculatibely updated
  auto channel_full = get_channel_full_force(channel_id, true, "speculative_add_channel_user");
  int32 min_count = 0;
  if (channel_full != nullptr) {
    channel_full->is_changed |= speculative_add_count(channel_full->administrator_count,
                                                      new_status.is_administrator() - old_status.is_administrator());
    min_count = channel_full->administrator_count;
  }

  if (c != nullptr && c->participant_count != 0 &&
      speculative_add_count(c->participant_count, new_status.is_member() - old_status.is_member(), min_count)) {
    c->is_changed = true;
    update_channel(c, channel_id);
  }

  if (new_status.is_administrator() != old_status.is_administrator() ||
      new_status.get_rank() != old_status.get_rank()) {
    DialogId dialog_id(channel_id);
    auto administrators_it = dialog_administrators_.find(dialog_id);
    if (administrators_it != dialog_administrators_.end()) {
      auto administrators = administrators_it->second;
      if (new_status.is_administrator()) {
        bool is_found = false;
        for (auto &administrator : administrators) {
          if (administrator.get_user_id() == user_id) {
            is_found = true;
            if (administrator.get_rank() != new_status.get_rank() ||
                administrator.is_creator() != new_status.is_creator()) {
              administrator = DialogAdministrator(user_id, new_status.get_rank(), new_status.is_creator());
              on_update_dialog_administrators(dialog_id, std::move(administrators), true, false);
            }
            break;
          }
        }
        if (!is_found) {
          administrators.emplace_back(user_id, new_status.get_rank(), new_status.is_creator());
          on_update_dialog_administrators(dialog_id, std::move(administrators), true, false);
        }
      } else {
        size_t i = 0;
        while (i != administrators.size() && administrators[i].get_user_id() != user_id) {
          i++;
        }
        if (i != administrators.size()) {
          administrators.erase(administrators.begin() + i);
          on_update_dialog_administrators(dialog_id, std::move(administrators), true, false);
        }
      }
    }
  }

  auto it = cached_channel_participants_.find(channel_id);
  if (it != cached_channel_participants_.end()) {
    auto &participants = it->second;
    bool is_found = false;
    for (size_t i = 0; i < participants.size(); i++) {
      if (participants[i].dialog_id_ == DialogId(user_id)) {
        if (!new_status.is_member()) {
          participants.erase(participants.begin() + i);
          update_channel_online_member_count(channel_id, false);
        } else {
          participants[i].status_ = new_status;
        }
        is_found = true;
        break;
      }
    }
    if (!is_found && new_status.is_member()) {
      participants.emplace_back(DialogId(user_id), get_my_id(), G()->unix_time(), new_status);
      update_channel_online_member_count(channel_id, false);
    }
  }

  if (channel_full == nullptr) {
    return;
  }

  channel_full->is_changed |= speculative_add_count(channel_full->participant_count,
                                                    new_status.is_member() - old_status.is_member(), min_count);
  channel_full->is_changed |=
      speculative_add_count(channel_full->restricted_count, new_status.is_restricted() - old_status.is_restricted());
  channel_full->is_changed |=
      speculative_add_count(channel_full->banned_count, new_status.is_banned() - old_status.is_banned());

  if (channel_full->is_changed) {
    channel_full->speculative_version++;
  }

  if (new_status.is_member() != old_status.is_member() && is_user_bot(user_id)) {
    if (new_status.is_member()) {
      if (!td::contains(channel_full->bot_user_ids, user_id)) {
        channel_full->bot_user_ids.push_back(user_id);
        channel_full->need_save_to_database = true;
        reload_channel_full(channel_id, Promise<Unit>(), "speculative_add_channel_user");

        send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                           channel_full->bot_user_ids, false);
      }
    } else {
      if (td::remove(channel_full->bot_user_ids, user_id)) {
        channel_full->need_save_to_database = true;

        send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                           channel_full->bot_user_ids, false);
      }
    }
  }

  update_channel_full(channel_full, channel_id, "speculative_add_channel_user");
}

class SearchDialogsNearbyQuery final : public Td::ResultHandler {
  Promise<tl_object_ptr<telegram_api::Updates>> promise_;

 public:
  explicit SearchDialogsNearbyQuery(Promise<tl_object_ptr<telegram_api::Updates>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const Location &location, bool from_background, int32 expire_date) {
    int32 flags = 0;
    if (from_background) {
      flags |= telegram_api::contacts_getLocated::BACKGROUND_MASK;
    }
    if (expire_date != -1) {
      flags |= telegram_api::contacts_getLocated::SELF_EXPIRES_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::contacts_getLocated(flags, false /*ignored*/, location.get_input_geo_point(), expire_date)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_getLocated>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::search_dialogs_nearby(const Location &location,
                                            Promise<td_api::object_ptr<td_api::chatsNearby>> &&promise) {
  if (location.empty()) {
    return promise.set_error(Status::Error(400, "Invalid location specified"));
  }
  last_user_location_ = location;
  try_send_set_location_visibility_query();

  auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), promise = std::move(promise)](
                                                  Result<tl_object_ptr<telegram_api::Updates>> result) mutable {
    send_closure(actor_id, &ContactsManager::on_get_dialogs_nearby, std::move(result), std::move(promise));
  });
  td_->create_handler<SearchDialogsNearbyQuery>(std::move(query_promise))->send(location, false, -1);
}

void ContactsManager::set_location(const Location &location, Promise<Unit> &&promise) {
  if (location.empty()) {
    return promise.set_error(Status::Error(400, "Invalid location specified"));
  }
  last_user_location_ = location;
  try_send_set_location_visibility_query();

  auto query_promise = PromiseCreator::lambda(
      [promise = std::move(promise)](Result<tl_object_ptr<telegram_api::Updates>> result) mutable {
        promise.set_value(Unit());
      });
  td_->create_handler<SearchDialogsNearbyQuery>(std::move(query_promise))->send(location, true, -1);
}

void ContactsManager::try_send_set_location_visibility_query() {
  if (G()->close_flag()) {
    return;
  }
  if (pending_location_visibility_expire_date_ == -1) {
    return;
  }

  if (is_set_location_visibility_request_sent_) {
    return;
  }
  if (pending_location_visibility_expire_date_ != 0 && last_user_location_.empty()) {
    return;
  }

  is_set_location_visibility_request_sent_ = true;
  auto query_promise =
      PromiseCreator::lambda([actor_id = actor_id(this), set_expire_date = pending_location_visibility_expire_date_](
                                 Result<tl_object_ptr<telegram_api::Updates>> result) {
        send_closure(actor_id, &ContactsManager::on_set_location_visibility_expire_date, set_expire_date,
                     result.is_ok() ? 0 : result.error().code());
      });
  td_->create_handler<SearchDialogsNearbyQuery>(std::move(query_promise))
      ->send(last_user_location_, true, pending_location_visibility_expire_date_);
}

void ContactsManager::get_is_location_visible(Promise<Unit> &&promise) {
  auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), promise = std::move(promise)](
                                                  Result<tl_object_ptr<telegram_api::Updates>> result) mutable {
    send_closure(actor_id, &ContactsManager::on_get_is_location_visible, std::move(result), std::move(promise));
  });
  td_->create_handler<SearchDialogsNearbyQuery>(std::move(query_promise))->send(Location(), true, -1);
}

ContactsManager::User *ContactsManager::get_user_force(UserId user_id) {
  auto u = get_user_force_impl(user_id);
  if ((u == nullptr || !u->is_received) &&
      (user_id == get_service_notifications_user_id() || user_id == get_replies_bot_user_id() ||
       user_id == get_anonymous_bot_user_id() || user_id == get_channel_bot_user_id())) {
    int32 flags = USER_FLAG_HAS_ACCESS_HASH | USER_FLAG_HAS_FIRST_NAME | USER_FLAG_NEED_APPLY_MIN_PHOTO;
    int64 profile_photo_id = 0;
    int32 profile_photo_dc_id = 1;
    string first_name;
    string last_name;
    string username;
    string phone_number;
    int32 bot_info_version = 0;

    if (user_id == get_service_notifications_user_id()) {
      flags |= USER_FLAG_HAS_PHONE_NUMBER | USER_FLAG_IS_VERIFIED | USER_FLAG_IS_SUPPORT;
      first_name = "Telegram";
      if (G()->is_test_dc()) {
        flags |= USER_FLAG_HAS_LAST_NAME;
        last_name = "Notifications";
      }
      phone_number = "42777";
      profile_photo_id = 3337190045231023;
    } else if (user_id == get_replies_bot_user_id()) {
      flags |= USER_FLAG_HAS_USERNAME | USER_FLAG_IS_BOT;
      if (!G()->is_test_dc()) {
        flags |= USER_FLAG_IS_PRIVATE_BOT;
      }
      first_name = "Replies";
      username = "replies";
      bot_info_version = G()->is_test_dc() ? 1 : 3;
    } else if (user_id == get_anonymous_bot_user_id()) {
      flags |= USER_FLAG_HAS_USERNAME | USER_FLAG_IS_BOT;
      if (!G()->is_test_dc()) {
        flags |= USER_FLAG_IS_PRIVATE_BOT;
      }
      first_name = "Group";
      username = G()->is_test_dc() ? "izgroupbot" : "GroupAnonymousBot";
      bot_info_version = G()->is_test_dc() ? 1 : 3;
      profile_photo_id = 5159307831025969322;
    } else if (user_id == get_channel_bot_user_id()) {
      flags |= USER_FLAG_HAS_USERNAME | USER_FLAG_IS_BOT;
      if (!G()->is_test_dc()) {
        flags |= USER_FLAG_IS_PRIVATE_BOT;
      }
      first_name = G()->is_test_dc() ? "Channels" : "Channel";
      username = G()->is_test_dc() ? "channelsbot" : "Channel_Bot";
      bot_info_version = G()->is_test_dc() ? 1 : 4;
      profile_photo_id = 587627495930570665;
    }

    telegram_api::object_ptr<telegram_api::userProfilePhoto> profile_photo;
    if (!G()->is_test_dc() && profile_photo_id != 0) {
      profile_photo = telegram_api::make_object<telegram_api::userProfilePhoto>(0, false /*ignored*/, profile_photo_id,
                                                                                BufferSlice(), profile_photo_dc_id);
    }

    auto user = telegram_api::make_object<telegram_api::user>(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, user_id.get(), 1,
        first_name, string(), username, phone_number, std::move(profile_photo), nullptr, bot_info_version, Auto(),
        string(), string());
    on_get_user(std::move(user), "get_user_force");
    u = get_user(user_id);
    CHECK(u != nullptr && u->is_received);
  }
  return u;
}

bool ContactsManager::on_get_channel_error(ChannelId channel_id, const Status &status, const string &source) {
  LOG(INFO) << "Receive " << status << " in " << channel_id << " from " << source;
  if (status.message() == CSlice("BOT_METHOD_INVALID")) {
    LOG(ERROR) << "Receive BOT_METHOD_INVALID from " << source;
    return true;
  }
  if (G()->is_expected_error(status)) {
    return true;
  }
  if (status.message() == "CHANNEL_PRIVATE" || status.message() == "CHANNEL_PUBLIC_GROUP_NA") {
    if (!channel_id.is_valid()) {
      LOG(ERROR) << "Receive " << status.message() << " in invalid " << channel_id << " from " << source;
      return false;
    }

    auto c = get_channel(channel_id);
    if (c == nullptr) {
      if (source == "GetChannelDifferenceQuery" || (td_->auth_manager_->is_bot() && source == "GetChannelsQuery")) {
        // get channel difference after restart
        // get channel from server by its identifier
        return true;
      }
      LOG(ERROR) << "Receive " << status.message() << " in not found " << channel_id << " from " << source;
      return false;
    }

    auto debug_channel_object = oneline(to_string(get_supergroup_object(channel_id, c)));
    if (c->status.is_member()) {
      LOG(INFO) << "Emulate leaving " << channel_id;
      // TODO we also may try to write to a public channel
      int32 flags = 0;
      if (c->is_megagroup) {
        flags |= CHANNEL_FLAG_IS_MEGAGROUP;
      } else {
        flags |= CHANNEL_FLAG_IS_BROADCAST;
      }
      telegram_api::channelForbidden update(flags, false /*ignored*/, false /*ignored*/, channel_id.get(),
                                            c->access_hash, c->title, 0);
      on_chat_update(update, "CHANNEL_PRIVATE");
    } else if (!c->status.is_banned()) {
      if (!c->username.empty()) {
        LOG(INFO) << "Drop username of " << channel_id;
        on_update_channel_username(c, channel_id, "");
      }

      on_update_channel_has_location(c, channel_id, false);

      on_update_channel_linked_channel_id(channel_id, ChannelId());

      update_channel(c, channel_id);

      remove_dialog_access_by_invite_link(DialogId(channel_id));
    }
    invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
    LOG_IF(ERROR, have_input_peer_channel(c, channel_id, AccessRights::Read))
        << "Have read access to channel after receiving CHANNEL_PRIVATE. Channel state: "
        << oneline(to_string(get_supergroup_object(channel_id, c)))
        << ". Previous channel state: " << debug_channel_object;

    return true;
  }
  return false;
}

void ContactsManager::update_chat(Chat *c, ChatId chat_id, bool from_binlog, bool from_database) {
  CHECK(c != nullptr);
  if (c->is_photo_changed) {
    td_->messages_manager_->on_dialog_photo_updated(DialogId(chat_id));
    drop_chat_photos(chat_id, !c->photo.small_file_id.is_valid(), true, "update_chat");
    c->is_photo_changed = false;
  }
  if (c->is_title_changed) {
    td_->messages_manager_->on_dialog_title_updated(DialogId(chat_id));
    c->is_title_changed = false;
  }
  if (c->is_default_permissions_changed) {
    td_->messages_manager_->on_dialog_default_permissions_updated(DialogId(chat_id));
    c->is_default_permissions_changed = false;
  }
  if (c->is_is_active_changed) {
    update_dialogs_for_discussion(DialogId(chat_id), c->is_active && c->status.is_creator());
    c->is_is_active_changed = false;
  }
  if (c->is_status_changed) {
    if (!c->status.can_manage_invite_links()) {
      td_->messages_manager_->drop_dialog_pending_join_requests(DialogId(chat_id));
    }
    c->is_status_changed = false;
  }
  if (c->is_noforwards_changed) {
    td_->messages_manager_->on_dialog_has_protected_content_updated(DialogId(chat_id));
    c->is_noforwards_changed = false;
  }

  LOG(DEBUG) << "Update " << chat_id << ": need_save_to_database = " << c->need_save_to_database
             << ", is_changed = " << c->is_changed;
  c->need_save_to_database |= c->is_changed;
  if (c->need_save_to_database) {
    if (!from_database) {
      c->is_saved = false;
    }
    c->need_save_to_database = false;
  }
  if (c->is_changed) {
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateBasicGroup>(get_basic_group_object(chat_id, c)));
    c->is_changed = false;
    c->is_update_basic_group_sent = true;
  }

  if (!from_database) {
    save_chat(c, chat_id, from_binlog);
  }

  if (c->cache_version != Chat::CACHE_VERSION && !c->is_repaired && have_input_peer_chat(c, AccessRights::Read) &&
      !G()->close_flag()) {
    c->is_repaired = true;

    LOG(INFO) << "Repairing cache of " << chat_id;
    reload_chat(chat_id, Promise<Unit>());
  }
}

class AcceptContactQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  UserId user_id_;

 public:
  explicit AcceptContactQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(UserId user_id, tl_object_ptr<telegram_api::InputUser> &&input_user) {
    user_id_ = user_id;
    send_query(G()->net_query_creator().create(telegram_api::contacts_acceptContact(std::move(input_user))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_acceptContact>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for AcceptContactQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->contacts_manager_->reload_contacts(true);
    td_->messages_manager_->reget_dialog_action_bar(DialogId(user_id_), "AcceptContactQuery");
  }
};

void ContactsManager::share_phone_number(UserId user_id, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (!are_contacts_loaded_) {
    load_contacts(PromiseCreator::lambda(
        [actor_id = actor_id(this), user_id, promise = std::move(promise)](Result<Unit> &&) mutable {
          send_closure(actor_id, &ContactsManager::share_phone_number, user_id, std::move(promise));
        }));
    return;
  }

  LOG(INFO) << "Share phone number with " << user_id;
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  td_->messages_manager_->hide_dialog_action_bar(DialogId(user_id));

  td_->create_handler<AcceptContactQuery>(std::move(promise))->send(user_id, r_input_user.move_as_ok());
}

class DeleteChatQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit DeleteChatQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChatId chat_id) {
    send_query(G()->net_query_creator().create(telegram_api::messages_deleteChat(chat_id.get())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteChat>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG(INFO) << "Receive result for DeleteChatQuery: " << result_ptr.ok();
    td_->updates_manager_->get_difference("DeleteChatQuery");
    td_->updates_manager_->on_get_updates(make_tl_object<telegram_api::updates>(), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::delete_chat(ChatId chat_id, Promise<Unit> &&promise) {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!get_chat_status(c).is_creator()) {
    return promise.set_error(Status::Error(400, "Not enough rights to delete the chat"));
  }
  if (!c->is_active) {
    return promise.set_error(Status::Error(400, "Chat is already deactivated"));
  }

  td_->create_handler<DeleteChatQuery>(std::move(promise))->send(chat_id);
}

void ContactsManager::on_update_profile_success(int32 flags, const string &first_name, const string &last_name,
                                                const string &about) {
  CHECK(flags != 0);

  auto my_user_id = get_my_id();
  const User *u = get_user(my_user_id);
  if (u == nullptr) {
    LOG(ERROR) << "Doesn't receive info about me during update profile";
    return;
  }
  LOG_IF(ERROR, (flags & ACCOUNT_UPDATE_FIRST_NAME) != 0 && u->first_name != first_name)
      << "Wrong first name \"" << u->first_name << "\", expected \"" << first_name << '"';
  LOG_IF(ERROR, (flags & ACCOUNT_UPDATE_LAST_NAME) != 0 && u->last_name != last_name)
      << "Wrong last name \"" << u->last_name << "\", expected \"" << last_name << '"';

  if ((flags & ACCOUNT_UPDATE_ABOUT) != 0) {
    UserFull *user_full = get_user_full_force(my_user_id);
    if (user_full != nullptr) {
      user_full->about = about;
      user_full->is_changed = true;
      update_user_full(user_full, my_user_id, "on_update_profile_success");
      td_->group_call_manager_->on_update_dialog_about(DialogId(my_user_id), user_full->about, true);
    }
  }
}

DialogParticipants ContactsManager::search_private_chat_participants(UserId my_user_id, UserId peer_user_id,
                                                                     const string &query, int32 limit,
                                                                     DialogParticipantsFilter filter) const {
  vector<DialogId> dialog_ids;
  if (filter.is_dialog_participant_suitable(td_, DialogParticipant::private_member(my_user_id, peer_user_id))) {
    dialog_ids.push_back(DialogId(my_user_id));
  }
  if (peer_user_id.is_valid() && peer_user_id != my_user_id &&
      filter.is_dialog_participant_suitable(td_, DialogParticipant::private_member(peer_user_id, my_user_id))) {
    dialog_ids.push_back(DialogId(peer_user_id));
  }

  auto result = search_among_dialogs(dialog_ids, query, limit);
  return {result.first, transform(result.second, [&](DialogId dialog_id) {
            auto user_id = dialog_id.get_user_id();
            return DialogParticipant::private_member(user_id, user_id == my_user_id ? peer_user_id : my_user_id);
          })};
}

void ContactsManager::on_update_bot_stopped(UserId user_id, int32 date, bool is_stopped) {
  if (!td_->auth_manager_->is_bot()) {
    LOG(ERROR) << "Receive updateBotStopped by non-bot";
    return;
  }
  if (date <= 0 || !have_user_force(user_id)) {
    LOG(ERROR) << "Receive invalid updateBotStopped by " << user_id << " at " << date;
    return;
  }

  DialogParticipant old_dialog_participant(DialogId(get_my_id()), user_id, date, DialogParticipantStatus::Banned(0));
  DialogParticipant new_dialog_participant(DialogId(get_my_id()), user_id, date, DialogParticipantStatus::Member());
  if (is_stopped) {
    std::swap(old_dialog_participant.status_, new_dialog_participant.status_);
  }

  send_update_chat_member(DialogId(user_id), user_id, date, DialogInviteLink(), old_dialog_participant,
                          new_dialog_participant);
}

void ContactsManager::on_update_channel_is_all_history_available(ChannelId channel_id, bool is_all_history_available,
                                                                 Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  CHECK(channel_id.is_valid());
  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_is_all_history_available");
  if (channel_full != nullptr && channel_full->is_all_history_available != is_all_history_available) {
    channel_full->is_all_history_available = is_all_history_available;
    channel_full->is_changed = true;
    update_channel_full(channel_full, channel_id, "on_update_channel_is_all_history_available");
  }
  promise.set_value(Unit());
}

tl_object_ptr<telegram_api::InputPeer> ContactsManager::get_input_peer_user(UserId user_id,
                                                                            AccessRights access_rights) const {
  if (user_id == get_my_id()) {
    return make_tl_object<telegram_api::inputPeerSelf>();
  }
  const User *u = get_user(user_id);
  if (!have_input_peer_user(u, access_rights)) {
    if ((u == nullptr || u->access_hash == -1 || u->is_min_access_hash) && td_->auth_manager_->is_bot() &&
        user_id.is_valid()) {
      return make_tl_object<telegram_api::inputPeerUser>(user_id.get(), 0);
    }
    return nullptr;
  }

  return make_tl_object<telegram_api::inputPeerUser>(user_id.get(), u->access_hash);
}

void ContactsManager::on_update_user_full_need_phone_number_privacy_exception(
    UserFull *user_full, UserId user_id, bool need_phone_number_privacy_exception) const {
  CHECK(user_full != nullptr);
  if (need_phone_number_privacy_exception) {
    const User *u = get_user(user_id);
    if (u == nullptr || u->is_contact || user_id == get_my_id()) {
      need_phone_number_privacy_exception = false;
    }
  }
  if (user_full->need_phone_number_privacy_exception != need_phone_number_privacy_exception) {
    user_full->need_phone_number_privacy_exception = need_phone_number_privacy_exception;
    user_full->is_changed = true;
  }
}

vector<ChannelId> ContactsManager::get_channel_ids(vector<tl_object_ptr<telegram_api::Chat>> &&chats,
                                                   const char *source) {
  vector<ChannelId> channel_ids;
  for (auto &chat : chats) {
    auto channel_id = get_channel_id(chat);
    if (!channel_id.is_valid()) {
      LOG(ERROR) << "Receive invalid " << channel_id << " from " << source << " in " << to_string(chat);
      continue;
    }
    on_get_chat(std::move(chat), source);
    if (have_channel(channel_id)) {
      channel_ids.push_back(channel_id);
    }
  }
  return channel_ids;
}

void ContactsManager::reload_dialog_info(DialogId dialog_id, Promise<Unit> &&promise) {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return reload_user(dialog_id.get_user_id(), std::move(promise));
    case DialogType::Chat:
      return reload_chat(dialog_id.get_chat_id(), std::move(promise));
    case DialogType::Channel:
      return reload_channel(dialog_id.get_channel_id(), std::move(promise));
    default:
      return promise.set_error(Status::Error("Invalid dialog ID to reload"));
  }
}

int32 ContactsManager::get_user_was_online(const User *u, UserId user_id) const {
  if (u == nullptr || u->is_deleted) {
    return 0;
  }

  int32 was_online = u->was_online;
  if (user_id == get_my_id()) {
    if (my_was_online_local_ != 0) {
      was_online = my_was_online_local_;
    }
  } else {
    if (u->local_was_online > 0 && u->local_was_online > was_online && u->local_was_online > G()->unix_time_cached()) {
      was_online = u->local_was_online;
    }
  }
  return was_online;
}

void ContactsManager::on_channel_participant_cache_timeout_callback(void *contacts_manager_ptr, int64 channel_id_long) {
  if (G()->close_flag()) {
    return;
  }

  auto contacts_manager = static_cast<ContactsManager *>(contacts_manager_ptr);
  send_closure_later(contacts_manager->actor_id(contacts_manager),
                     &ContactsManager::on_channel_participant_cache_timeout, ChannelId(channel_id_long));
}

void ContactsManager::on_slow_mode_delay_timeout_callback(void *contacts_manager_ptr, int64 channel_id_long) {
  if (G()->close_flag()) {
    return;
  }

  auto contacts_manager = static_cast<ContactsManager *>(contacts_manager_ptr);
  send_closure_later(contacts_manager->actor_id(contacts_manager), &ContactsManager::on_slow_mode_delay_timeout,
                     ChannelId(channel_id_long));
}

void ContactsManager::on_get_contacts_statuses(vector<tl_object_ptr<telegram_api::contactStatus>> &&statuses) {
  auto my_user_id = get_my_id();
  for (auto &status : statuses) {
    UserId user_id(status->user_id_);
    if (user_id != my_user_id) {
      on_update_user_online(user_id, std::move(status->status_));
    }
  }
  save_next_contacts_sync_date();
}

void ContactsManager::remove_dialog_suggested_action(SuggestedAction action) {
  auto it = dialog_suggested_actions_.find(action.dialog_id_);
  if (it == dialog_suggested_actions_.end()) {
    return;
  }
  remove_suggested_action(it->second, action);
  if (it->second.empty()) {
    dialog_suggested_actions_.erase(it);
  }
}

const DialogParticipant *ContactsManager::get_chat_participant(ChatId chat_id, UserId user_id) const {
  auto chat_full = get_chat_full(chat_id);
  if (chat_full == nullptr) {
    return nullptr;
  }
  return get_chat_full_participant(chat_full, DialogId(user_id));
}

UserId ContactsManager::add_service_notifications_user() {
  auto user_id = get_service_notifications_user_id();
  if (!have_user_force(user_id)) {
    LOG(FATAL) << "Failed to load service notification user";
  }
  return user_id;
}

SecretChatState ContactsManager::get_secret_chat_state(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return SecretChatState::Unknown;
  }
  return c->state;
}

bool ContactsManager::get_channel_sign_messages(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return false;
  }
  return get_channel_sign_messages(c);
}

int32 ContactsManager::get_secret_chat_date(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return 0;
  }
  return c->date;
}

bool ContactsManager::is_user_contact(UserId user_id, bool is_mutual) const {
  return is_user_contact(get_user(user_id), user_id, is_mutual);
}

string ContactsManager::get_channel_database_key(ChannelId channel_id) {
  return PSTRING() << "ch" << channel_id.get();
}

ContactsManager::~ContactsManager() = default;
}  // namespace td
