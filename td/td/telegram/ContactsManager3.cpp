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

void ContactsManager::on_get_user(tl_object_ptr<telegram_api::User> &&user_ptr, const char *source, bool is_me,
                                  bool expect_support) {
  LOG(DEBUG) << "Receive from " << source << ' ' << to_string(user_ptr);
  int32 constructor_id = user_ptr->get_id();
  if (constructor_id == telegram_api::userEmpty::ID) {
    auto user = move_tl_object_as<telegram_api::userEmpty>(user_ptr);
    UserId user_id(user->id_);
    if (!user_id.is_valid()) {
      LOG(ERROR) << "Receive invalid " << user_id << " from " << source;
      return;
    }
    LOG(INFO) << "Receive empty " << user_id << " from " << source;

    User *u = get_user_force(user_id);
    if (u == nullptr && Slice(source) != Slice("GetUsersQuery")) {
      // userEmpty should be received only through getUsers for unexisting users
      LOG(ERROR) << "Have no information about " << user_id << ", but received userEmpty from " << source;
    }
    return;
  }

  CHECK(constructor_id == telegram_api::user::ID);
  auto user = move_tl_object_as<telegram_api::user>(user_ptr);
  UserId user_id(user->id_);
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }
  int32 flags = user->flags_;
  LOG(INFO) << "Receive " << user_id << " with flags " << flags << " from " << source;
  if (is_me && (flags & USER_FLAG_IS_ME) == 0) {
    LOG(ERROR) << user_id << " doesn't have flag IS_ME, but must have it when received from " << source;
    flags |= USER_FLAG_IS_ME;
  }

  bool is_bot = (flags & USER_FLAG_IS_BOT) != 0;
  if (flags & USER_FLAG_IS_ME) {
    set_my_id(user_id);
    if (!is_bot) {
      G()->shared_config().set_option_string("my_phone_number", user->phone_);
    }
  }

  if (expect_support) {
    support_user_id_ = user_id;
  }

  bool have_access_hash = (flags & USER_FLAG_HAS_ACCESS_HASH) != 0;
  bool is_received = (flags & USER_FLAG_IS_INACCESSIBLE) == 0;

  if (!is_received && !have_user_force(user_id)) {
    // we must preload information about received inaccessible users from database in order to not save
    // the min-user to the database and to not override access_hash and another info
    LOG(INFO) << "Receive inaccessible " << user_id;
  }

  User *u = add_user(user_id, "on_get_user");
  if (have_access_hash) {  // access_hash must be updated before photo
    auto access_hash = user->access_hash_;
    bool is_min_access_hash = !is_received && !((flags & USER_FLAG_HAS_PHONE_NUMBER) != 0 && user->phone_.empty());
    if (u->access_hash != access_hash && (!is_min_access_hash || u->is_min_access_hash || u->access_hash == -1)) {
      LOG(DEBUG) << "Access hash has changed for " << user_id << " from " << u->access_hash << "/"
                 << u->is_min_access_hash << " to " << access_hash << "/" << is_min_access_hash;
      u->access_hash = access_hash;
      u->is_min_access_hash = is_min_access_hash;
      u->need_save_to_database = true;
    }
  }
  if (is_received || !user->phone_.empty()) {
    on_update_user_phone_number(u, user_id, std::move(user->phone_));
  }
  if (is_received || u->need_apply_min_photo || !u->is_received) {
    on_update_user_photo(u, user_id, std::move(user->photo_), source);
  }
  if (is_received) {
    on_update_user_online(u, user_id, std::move(user->status_));

    auto is_contact = (flags & USER_FLAG_IS_CONTACT) != 0;
    auto is_mutual_contact = (flags & USER_FLAG_IS_MUTUAL_CONTACT) != 0;
    on_update_user_is_contact(u, user_id, is_contact, is_mutual_contact);
  }

  if (is_received || !u->is_received) {
    on_update_user_name(u, user_id, std::move(user->first_name_), std::move(user->last_name_),
                        std::move(user->username_));
  }

  bool is_verified = (flags & USER_FLAG_IS_VERIFIED) != 0;
  bool is_support = (flags & USER_FLAG_IS_SUPPORT) != 0;
  bool is_deleted = (flags & USER_FLAG_IS_DELETED) != 0;
  bool can_join_groups = (flags & USER_FLAG_IS_PRIVATE_BOT) == 0;
  bool can_read_all_group_messages = (flags & USER_FLAG_IS_BOT_WITH_PRIVACY_DISABLED) != 0;
  auto restriction_reasons = get_restriction_reasons(std::move(user->restriction_reason_));
  bool is_scam = (flags & USER_FLAG_IS_SCAM) != 0;
  bool is_inline_bot = (flags & USER_FLAG_IS_INLINE_BOT) != 0;
  string inline_query_placeholder = user->bot_inline_placeholder_;
  bool need_location_bot = (flags & USER_FLAG_NEED_LOCATION_BOT) != 0;
  bool has_bot_info_version = (flags & USER_FLAG_HAS_BOT_INFO_VERSION) != 0;
  bool need_apply_min_photo = (flags & USER_FLAG_NEED_APPLY_MIN_PHOTO) != 0;
  bool is_fake = (flags & USER_FLAG_IS_FAKE) != 0;

  LOG_IF(ERROR, !is_support && expect_support) << "Receive non-support " << user_id << ", but expected a support user";
  LOG_IF(ERROR, !can_join_groups && !is_bot)
      << "Receive not bot " << user_id << " which can't join groups from " << source;
  LOG_IF(ERROR, can_read_all_group_messages && !is_bot)
      << "Receive not bot " << user_id << " which can read all group messages from " << source;
  LOG_IF(ERROR, is_inline_bot && !is_bot) << "Receive not bot " << user_id << " which is inline bot from " << source;
  LOG_IF(ERROR, need_location_bot && !is_inline_bot)
      << "Receive not inline bot " << user_id << " which needs user location from " << source;

  if (is_deleted) {
    // just in case
    is_verified = false;
    is_support = false;
    is_bot = false;
    can_join_groups = false;
    can_read_all_group_messages = false;
    is_inline_bot = false;
    inline_query_placeholder = string();
    need_location_bot = false;
    has_bot_info_version = false;
    need_apply_min_photo = false;
  }

  LOG_IF(ERROR, has_bot_info_version && !is_bot)
      << "Receive not bot " << user_id << " which has bot info version from " << source;

  int32 bot_info_version = has_bot_info_version ? user->bot_info_version_ : -1;
  if (is_verified != u->is_verified || is_support != u->is_support || is_bot != u->is_bot ||
      can_join_groups != u->can_join_groups || can_read_all_group_messages != u->can_read_all_group_messages ||
      restriction_reasons != u->restriction_reasons || is_scam != u->is_scam || is_fake != u->is_fake ||
      is_inline_bot != u->is_inline_bot || inline_query_placeholder != u->inline_query_placeholder ||
      need_location_bot != u->need_location_bot) {
    LOG_IF(ERROR, is_bot != u->is_bot && !is_deleted && !u->is_deleted && u->is_received)
        << "User.is_bot has changed for " << user_id << "/" << u->username << " from " << source << " from "
        << u->is_bot << " to " << is_bot;
    u->is_verified = is_verified;
    u->is_support = is_support;
    u->is_bot = is_bot;
    u->can_join_groups = can_join_groups;
    u->can_read_all_group_messages = can_read_all_group_messages;
    u->restriction_reasons = std::move(restriction_reasons);
    u->is_scam = is_scam;
    u->is_fake = is_fake;
    u->is_inline_bot = is_inline_bot;
    u->inline_query_placeholder = std::move(inline_query_placeholder);
    u->need_location_bot = need_location_bot;

    LOG(DEBUG) << "Info has changed for " << user_id;
    u->is_changed = true;
  }

  if (u->bot_info_version != bot_info_version) {
    u->bot_info_version = bot_info_version;
    LOG(DEBUG) << "Bot info version has changed for " << user_id;
    u->need_save_to_database = true;
  }
  if (is_received && u->need_apply_min_photo != need_apply_min_photo) {
    u->need_apply_min_photo = need_apply_min_photo;
    u->need_save_to_database = true;
  }

  if (is_received && !u->is_received) {
    u->is_received = true;

    LOG(DEBUG) << "Receive " << user_id;
    u->is_changed = true;
  }

  if (is_deleted != u->is_deleted) {
    u->is_deleted = is_deleted;

    LOG(DEBUG) << "User.is_deleted has changed for " << user_id << " to " << u->is_deleted;
    u->is_is_deleted_changed = true;
    u->is_changed = true;
  }

  bool has_language_code = (flags & USER_FLAG_HAS_LANGUAGE_CODE) != 0;
  LOG_IF(ERROR, has_language_code && !td_->auth_manager_->is_bot())
      << "Receive language code for " << user_id << " from " << source;
  if (u->language_code != user->lang_code_ && !user->lang_code_.empty()) {
    u->language_code = user->lang_code_;

    LOG(DEBUG) << "Language code has changed for " << user_id << " to " << u->language_code;
    u->is_changed = true;
  }

  if (u->cache_version != User::CACHE_VERSION && u->is_received) {
    u->cache_version = User::CACHE_VERSION;
    u->need_save_to_database = true;
  }
  u->is_received_from_server = true;
  update_user(u, user_id);
}

class DeleteChatUserQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit DeleteChatUserQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChatId chat_id, tl_object_ptr<telegram_api::InputUser> &&input_user, bool revoke_messages) {
    int32 flags = 0;
    if (revoke_messages) {
      flags |= telegram_api::messages_deleteChatUser::REVOKE_HISTORY_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_deleteChatUser(flags, false /*ignored*/, chat_id.get(), std::move(input_user))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteChatUser>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for DeleteChatUserQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("DeleteChatUserQuery");
  }
};

void ContactsManager::delete_chat_participant(ChatId chat_id, UserId user_id, bool revoke_messages,
                                              Promise<Unit> &&promise) {
  const Chat *c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!c->is_active) {
    return promise.set_error(Status::Error(400, "Chat is deactivated"));
  }
  auto my_id = get_my_id();
  if (c->status.is_left()) {
    if (user_id == my_id) {
      if (revoke_messages) {
        return td_->messages_manager_->delete_dialog_history(DialogId(chat_id), true, true, std::move(promise));
      }
      return promise.set_value(Unit());
    } else {
      return promise.set_error(Status::Error(400, "Not in the chat"));
    }
  }
  if (user_id != my_id) {
    auto my_status = get_chat_permissions(c);
    if (!my_status.is_creator()) {  // creator can delete anyone
      auto participant = get_chat_participant(chat_id, user_id);
      if (participant != nullptr) {  // if have no information about participant, just send request to the server
        /*
        TODO
        if (c->everyone_is_administrator) {
          // if all are administrators, only invited by me participants can be deleted
          if (participant->inviter_user_id_ != my_id) {
            return promise.set_error(Status::Error(400, "Need to be inviter of a user to kick it from a basic group"));
          }
        } else {
          // otherwise, only creator can kick administrators
          if (participant->status_.is_administrator()) {
            return promise.set_error(
                Status::Error(400, "Only the creator of a basic group can kick group administrators"));
          }
          // regular users can be kicked by administrators and their inviters
          if (!my_status.is_administrator() && participant->inviter_user_id_ != my_id) {
            return promise.set_error(Status::Error(400, "Need to be inviter of a user to kick it from a basic group"));
          }
        }
        */
      }
    }
  }
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  // TODO invoke after
  td_->create_handler<DeleteChatUserQuery>(std::move(promise))
      ->send(chat_id, r_input_user.move_as_ok(), revoke_messages);
}

void ContactsManager::on_update_channel_participant(ChannelId channel_id, UserId user_id, int32 date,
                                                    DialogInviteLink invite_link,
                                                    tl_object_ptr<telegram_api::ChannelParticipant> old_participant,
                                                    tl_object_ptr<telegram_api::ChannelParticipant> new_participant) {
  if (!td_->auth_manager_->is_bot()) {
    LOG(ERROR) << "Receive updateChannelParticipant by non-bot";
    return;
  }
  if (!channel_id.is_valid() || !user_id.is_valid() || date <= 0 ||
      (old_participant == nullptr && new_participant == nullptr)) {
    LOG(ERROR) << "Receive invalid updateChannelParticipant in " << channel_id << " by " << user_id << " at " << date
               << ": " << to_string(old_participant) << " -> " << to_string(new_participant);
    return;
  }

  DialogParticipant old_dialog_participant;
  DialogParticipant new_dialog_participant;
  if (old_participant != nullptr) {
    old_dialog_participant = DialogParticipant(std::move(old_participant));
    if (new_participant == nullptr) {
      new_dialog_participant = DialogParticipant::left(old_dialog_participant.dialog_id_);
    } else {
      new_dialog_participant = DialogParticipant(std::move(new_participant));
    }
  } else {
    new_dialog_participant = DialogParticipant(std::move(new_participant));
    old_dialog_participant = DialogParticipant::left(new_dialog_participant.dialog_id_);
  }
  if (old_dialog_participant.dialog_id_ != new_dialog_participant.dialog_id_ || !old_dialog_participant.is_valid() ||
      !new_dialog_participant.is_valid()) {
    LOG(ERROR) << "Receive wrong updateChannelParticipant: " << old_dialog_participant << " -> "
               << new_dialog_participant;
    return;
  }

  if (old_dialog_participant.dialog_id_ == DialogId(get_my_id()) && old_dialog_participant.status_.is_administrator() &&
      !new_dialog_participant.status_.is_administrator()) {
    channel_participants_.erase(channel_id);
  } else if (have_channel_participant_cache(channel_id)) {
    add_channel_participant_to_cache(channel_id, new_dialog_participant, true);
  }
  if (new_dialog_participant.dialog_id_ == DialogId(get_my_id()) &&
      new_dialog_participant.status_ != get_channel_status(channel_id) && false) {
    LOG(ERROR) << "Have status " << get_channel_status(channel_id) << " after receiving updateChannelParticipant in "
               << channel_id << " by " << user_id << " at " << date << " from " << old_dialog_participant << " to "
               << new_dialog_participant;
  }

  send_update_chat_member(DialogId(channel_id), user_id, date, invite_link, old_dialog_participant,
                          new_dialog_participant);
}

void ContactsManager::get_created_public_dialogs(PublicDialogType type,
                                                 Promise<td_api::object_ptr<td_api::chats>> &&promise,
                                                 bool from_binlog) {
  auto index = static_cast<int32>(type);
  if (created_public_channels_inited_[index]) {
    return return_created_public_dialogs(std::move(promise), created_public_channels_[index]);
  }

  if (get_created_public_channels_queries_[index].empty() && G()->parameters().use_chat_info_db) {
    auto pmc_key = PSTRING() << "public_channels" << index;
    auto str = G()->td_db()->get_binlog_pmc()->get(pmc_key);
    if (!str.empty()) {
      auto r_channel_ids = transform(full_split(Slice(str), ','), [](Slice str) -> Result<ChannelId> {
        TRY_RESULT(channel_id_int, to_integer_safe<int64>(str));
        ChannelId channel_id(channel_id_int);
        if (!channel_id.is_valid()) {
          return Status::Error("Have invalid channel ID");
        }
        return channel_id;
      });
      if (std::any_of(r_channel_ids.begin(), r_channel_ids.end(),
                      [](auto &r_channel_id) { return r_channel_id.is_error(); })) {
        LOG(ERROR) << "Can't parse " << str;
        G()->td_db()->get_binlog_pmc()->erase(pmc_key);
      } else {
        Dependencies dependencies;
        vector<ChannelId> channel_ids;
        for (auto &r_channel_id : r_channel_ids) {
          auto channel_id = r_channel_id.move_as_ok();
          add_dialog_and_dependencies(dependencies, DialogId(channel_id));
          channel_ids.push_back(channel_id);
        }
        if (!resolve_dependencies_force(td_, dependencies, "get_created_public_dialogs")) {
          G()->td_db()->get_binlog_pmc()->erase(pmc_key);
        } else {
          created_public_channels_[index] = std::move(channel_ids);
          created_public_channels_inited_[index] = true;

          if (type == PublicDialogType::HasUsername) {
            update_created_public_broadcasts();
          }

          if (from_binlog) {
            return return_created_public_dialogs(std::move(promise), created_public_channels_[index]);
          }
        }
      }
    }
  }

  reload_created_public_dialogs(type, std::move(promise));
}

void ContactsManager::on_update_contacts_reset() {
  /*
  UserId my_id = get_my_id();
  for (auto &p : users_) {
    UserId user_id = p.first;
    User u = &p.second;
    if (u->is_contact) {
      LOG(INFO) << "Drop contact with " << user_id;
      if (user_id != my_id) {
        CHECK(contacts_hints_.has_key(user_id.get()));
      }
      on_update_user_is_contact(u, user_id, false, false);
      CHECK(u->is_is_contact_changed);
      u->cache_version = 0;
      u->is_repaired = false;
      update_user(u, user_id);
      CHECK(!u->is_contact);
      if (user_id != my_id) {
        CHECK(!contacts_hints_.has_key(user_id.get()));
      }
    }
  }
  */

  saved_contact_count_ = 0;
  if (G()->parameters().use_chat_info_db) {
    G()->td_db()->get_binlog_pmc()->set("saved_contact_count", "0");
    G()->td_db()->get_sqlite_pmc()->erase("user_imported_contacts", Auto());
  }
  if (!are_imported_contacts_loaded_) {
    CHECK(all_imported_contacts_.empty());
    if (load_imported_contacts_queries_.empty()) {
      LOG(INFO) << "Imported contacts was never loaded, just clear them";
    } else {
      LOG(INFO) << "Imported contacts are being loaded, clear them also when they will be loaded";
      need_clear_imported_contacts_ = true;
    }
  } else {
    if (!are_imported_contacts_changing_) {
      LOG(INFO) << "Imported contacts was loaded, but aren't changing now, just clear them";
      all_imported_contacts_.clear();
    } else {
      LOG(INFO) << "Imported contacts are changing now, clear them also after they will be loaded";
      need_clear_imported_contacts_ = true;
    }
  }
  reload_contacts(true);
}

void ContactsManager::update_user_full(UserFull *user_full, UserId user_id, const char *source, bool from_database) {
  CHECK(user_full != nullptr);
  unavailable_user_fulls_.erase(user_id);  // don't needed anymore
  if (user_full->is_common_chat_count_changed) {
    td_->messages_manager_->drop_common_dialogs_cache(user_id);
    user_full->is_common_chat_count_changed = false;
  }

  user_full->need_send_update |= user_full->is_changed;
  user_full->need_save_to_database |= user_full->is_changed;
  user_full->is_changed = false;
  if (user_full->need_send_update || user_full->need_save_to_database) {
    LOG(INFO) << "Update full " << user_id << " from " << source;
  }
  if (user_full->need_send_update) {
    {
      auto u = get_user(user_id);
      CHECK(u == nullptr || u->is_update_user_sent);
    }
    if (!user_full->is_update_user_full_sent) {
      LOG(ERROR) << "Send partial updateUserFullInfo for " << user_id << " from " << source;
      user_full->is_update_user_full_sent = true;
    }
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateUserFullInfo>(get_user_id_object(user_id, "updateUserFullInfo"),
                                                            get_user_full_info_object(user_id, user_full)));
    user_full->need_send_update = false;
  }
  if (user_full->need_save_to_database) {
    if (!from_database) {
      save_user_full(user_full, user_id);
    }
    user_full->need_save_to_database = false;
  }
}

void ContactsManager::update_user_online_member_count(User *u) {
  if (u->online_member_dialogs.empty()) {
    return;
  }

  auto now = G()->unix_time_cached();
  vector<DialogId> expired_dialog_ids;
  for (auto &it : u->online_member_dialogs) {
    auto dialog_id = it.first;
    auto time = it.second;
    if (time < now - MessagesManager::ONLINE_MEMBER_COUNT_CACHE_EXPIRE_TIME) {
      expired_dialog_ids.push_back(dialog_id);
      continue;
    }

    switch (dialog_id.get_type()) {
      case DialogType::Chat: {
        auto chat_id = dialog_id.get_chat_id();
        auto chat_full = get_chat_full(chat_id);
        CHECK(chat_full != nullptr);
        update_chat_online_member_count(chat_full, chat_id, false);
        break;
      }
      case DialogType::Channel: {
        auto channel_id = dialog_id.get_channel_id();
        update_channel_online_member_count(channel_id, false);
        break;
      }
      case DialogType::User:
      case DialogType::SecretChat:
      case DialogType::None:
        UNREACHABLE();
        break;
    }
  }
  for (auto &dialog_id : expired_dialog_ids) {
    u->online_member_dialogs.erase(dialog_id);
    if (dialog_id.get_type() == DialogType::Channel) {
      cached_channel_participants_.erase(dialog_id.get_channel_id());
    }
  }
}

tl_object_ptr<td_api::StatisticalGraph> ContactsManager::convert_stats_graph(
    tl_object_ptr<telegram_api::StatsGraph> obj) {
  CHECK(obj != nullptr);

  switch (obj->get_id()) {
    case telegram_api::statsGraphAsync::ID: {
      auto graph = move_tl_object_as<telegram_api::statsGraphAsync>(obj);
      return make_tl_object<td_api::statisticalGraphAsync>(std::move(graph->token_));
    }
    case telegram_api::statsGraphError::ID: {
      auto graph = move_tl_object_as<telegram_api::statsGraphError>(obj);
      return make_tl_object<td_api::statisticalGraphError>(std::move(graph->error_));
    }
    case telegram_api::statsGraph::ID: {
      auto graph = move_tl_object_as<telegram_api::statsGraph>(obj);
      return make_tl_object<td_api::statisticalGraphData>(std::move(graph->json_->data_),
                                                          std::move(graph->zoom_token_));
    }
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void ContactsManager::send_update_chat_member(DialogId dialog_id, UserId agent_user_id, int32 date,
                                              const DialogInviteLink &invite_link,
                                              const DialogParticipant &old_dialog_participant,
                                              const DialogParticipant &new_dialog_participant) {
  CHECK(td_->auth_manager_->is_bot());
  td_->messages_manager_->force_create_dialog(dialog_id, "send_update_chat_member", true);
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateChatMember>(
                   dialog_id.get(), get_user_id_object(agent_user_id, "send_update_chat_member"), date,
                   invite_link.get_chat_invite_link_object(this), get_chat_member_object(old_dialog_participant),
                   get_chat_member_object(new_dialog_participant)));
}

FileSourceId ContactsManager::get_user_profile_photo_file_source_id(UserId user_id, int64 photo_id) {
  auto u = get_user(user_id);
  if (u != nullptr && u->photo_ids.count(photo_id) != 0) {
    VLOG(file_references) << "Don't need to create file source for photo " << photo_id << " of " << user_id;
    // photo was already added, source ID was registered and shouldn't be needed
    return FileSourceId();
  }

  auto &source_id = user_profile_photo_file_source_ids_[std::make_pair(user_id, photo_id)];
  if (!source_id.is_valid()) {
    source_id = td_->file_reference_manager_->create_user_photo_file_source(user_id, photo_id);
  }
  VLOG(file_references) << "Return " << source_id << " for photo " << photo_id << " of " << user_id;
  return source_id;
}

void ContactsManager::on_update_channel_bot_user_ids(ChannelId channel_id, vector<UserId> &&bot_user_ids) {
  CHECK(channel_id.is_valid());
  if (!have_channel(channel_id)) {
    LOG(ERROR) << channel_id << " not found";
    return;
  }

  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_bot_user_ids");
  if (channel_full == nullptr) {
    send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                       std::move(bot_user_ids), false);
    return;
  }
  on_update_channel_full_bot_user_ids(channel_full, channel_id, std::move(bot_user_ids));
  update_channel_full(channel_full, channel_id, "on_update_channel_bot_user_ids");
}

void ContactsManager::on_update_chat_description(ChatId chat_id, string &&description) {
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id;
    return;
  }

  auto chat_full = get_chat_full_force(chat_id, "on_update_chat_description");
  if (chat_full == nullptr) {
    return;
  }
  if (chat_full->description != description) {
    chat_full->description = std::move(description);
    chat_full->is_changed = true;
    update_chat_full(chat_full, chat_id, "on_update_chat_description");
    td_->group_call_manager_->on_update_dialog_about(DialogId(chat_id), chat_full->description, true);
  }
}

void ContactsManager::on_set_profile_photo(tl_object_ptr<telegram_api::photos_photo> &&photo, int64 old_photo_id) {
  LOG(INFO) << "Changed profile photo to " << to_string(photo);

  UserId my_user_id = get_my_id();

  if (old_photo_id != 0) {
    delete_profile_photo_from_cache(my_user_id, old_photo_id, false);
  }

  add_profile_photo_to_cache(my_user_id,
                             get_photo(td_->file_manager_.get(), std::move(photo->photo_), DialogId(my_user_id)));

  // if cache was correctly updated, this should produce no updates
  on_get_users(std::move(photo->users_), "on_set_profile_photo");
}

void ContactsManager::on_update_channel_sticker_set(ChannelId channel_id, StickerSetId sticker_set_id) {
  CHECK(channel_id.is_valid());
  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_sticker_set");
  if (channel_full == nullptr) {
    return;
  }
  if (channel_full->sticker_set_id != sticker_set_id) {
    channel_full->sticker_set_id = sticker_set_id;
    channel_full->is_changed = true;
    update_channel_full(channel_full, channel_id, "on_update_channel_sticker_set");
  }
}

void ContactsManager::return_created_public_dialogs(Promise<td_api::object_ptr<td_api::chats>> &&promise,
                                                    const vector<ChannelId> &channel_ids) {
  if (!promise) {
    return;
  }

  auto total_count = narrow_cast<int32>(channel_ids.size());
  promise.set_value(td_api::make_object<td_api::chats>(
      total_count, transform(channel_ids, [](ChannelId channel_id) { return DialogId(channel_id).get(); })));
}

void ContactsManager::do_update_user_photo(User *u, UserId user_id,
                                           tl_object_ptr<telegram_api::UserProfilePhoto> &&photo, const char *source) {
  ProfilePhoto new_photo = get_profile_photo(td_->file_manager_.get(), user_id, u->access_hash, std::move(photo));
  if (td_->auth_manager_->is_bot()) {
    new_photo.minithumbnail.clear();
  }
  do_update_user_photo(u, user_id, std::move(new_photo), true, source);
}

ChannelId ContactsManager::get_channel_linked_channel_id(ChannelId channel_id) {
  auto channel_full = get_channel_full_const(channel_id);
  if (channel_full == nullptr) {
    channel_full = get_channel_full_force(channel_id, false, "get_channel_linked_channel_id");
    if (channel_full == nullptr) {
      return ChannelId();
    }
  }
  return channel_full->linked_channel_id;
}

tl_object_ptr<td_api::secretChat> ContactsManager::get_secret_chat_object(SecretChatId secret_chat_id,
                                                                          const SecretChat *secret_chat) {
  if (secret_chat == nullptr) {
    return nullptr;
  }
  get_user_force(secret_chat->user_id);
  return get_secret_chat_object_const(secret_chat_id, secret_chat);
}

ContactsManager::SecretChat *ContactsManager::add_secret_chat(SecretChatId secret_chat_id) {
  CHECK(secret_chat_id.is_valid());
  auto &secret_chat_ptr = secret_chats_[secret_chat_id];
  if (secret_chat_ptr == nullptr) {
    secret_chat_ptr = make_unique<SecretChat>();
  }
  return secret_chat_ptr.get();
}

RestrictedRights ContactsManager::get_chat_default_permissions(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return RestrictedRights(false, false, false, false, false, false, false, false, false, false, false);
  }
  return c->default_permissions;
}

void ContactsManager::on_update_channel_title(Channel *c, ChannelId channel_id, string &&title) {
  if (c->title != title) {
    c->title = std::move(title);
    c->is_title_changed = true;
    c->need_save_to_database = true;
  }
}

DialogParticipantStatus ContactsManager::get_chat_status(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return DialogParticipantStatus::Banned(0);
  }
  return get_chat_status(c);
}

UserId ContactsManager::add_channel_bot_user() {
  auto user_id = get_channel_bot_user_id();
  if (!have_user_force(user_id)) {
    LOG(FATAL) << "Failed to load channel bot user";
  }
  return user_id;
}

void ContactsManager::on_slow_mode_delay_timeout(ChannelId channel_id) {
  if (G()->close_flag()) {
    return;
  }

  on_update_channel_slow_mode_next_send_date(channel_id, 0);
}

void ContactsManager::on_get_chat_full_failed(ChatId chat_id) {
  if (G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Failed to get full " << chat_id;
}

string ContactsManager::get_chat_database_key(ChatId chat_id) {
  return PSTRING() << "gr" << chat_id.get();
}
}  // namespace td
