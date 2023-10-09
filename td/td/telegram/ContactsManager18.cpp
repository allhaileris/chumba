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


#include <utility>

namespace td {
}  // namespace td
namespace td {

std::pair<vector<UserId>, vector<int32>> ContactsManager::change_imported_contacts(vector<Contact> &contacts,
                                                                                   int64 &random_id,
                                                                                   Promise<Unit> &&promise) {
  if (!are_contacts_loaded_) {
    load_contacts(std::move(promise));
    return {};
  }
  if (!are_imported_contacts_loaded_) {
    load_imported_contacts(std::move(promise));
    return {};
  }

  LOG(INFO) << "Asked to change imported contacts to a list of " << contacts.size()
            << " contacts with random_id = " << random_id;
  if (random_id != 0) {
    // request has already been sent before
    if (need_clear_imported_contacts_) {
      need_clear_imported_contacts_ = false;
      all_imported_contacts_.clear();
      if (G()->parameters().use_chat_info_db) {
        G()->td_db()->get_sqlite_pmc()->erase("user_imported_contacts", Auto());
      }
      reload_contacts(true);
    }

    CHECK(are_imported_contacts_changing_);
    are_imported_contacts_changing_ = false;

    auto unimported_contact_invites = std::move(unimported_contact_invites_);
    unimported_contact_invites_.clear();

    auto imported_contact_user_ids = std::move(imported_contact_user_ids_);
    imported_contact_user_ids_.clear();

    promise.set_value(Unit());
    return {std::move(imported_contact_user_ids), std::move(unimported_contact_invites)};
  }

  if (are_imported_contacts_changing_) {
    promise.set_error(Status::Error(400, "ChangeImportedContacts can be called only once at the same time"));
    return {};
  }

  vector<size_t> new_contacts_unique_id(contacts.size());
  vector<Contact> unique_new_contacts;
  unique_new_contacts.reserve(contacts.size());
  std::unordered_map<Contact, size_t, ContactHash, ContactEqual> different_new_contacts;
  std::unordered_set<string> different_new_phone_numbers;
  size_t unique_size = 0;
  for (size_t i = 0; i < contacts.size(); i++) {
    auto it_success = different_new_contacts.emplace(std::move(contacts[i]), unique_size);
    new_contacts_unique_id[i] = it_success.first->second;
    if (it_success.second) {
      unique_new_contacts.push_back(it_success.first->first);
      different_new_phone_numbers.insert(unique_new_contacts.back().get_phone_number());
      unique_size++;
    }
  }

  vector<string> to_delete;
  vector<UserId> to_delete_user_ids;
  for (auto &old_contact : all_imported_contacts_) {
    auto user_id = old_contact.get_user_id();
    auto it = different_new_contacts.find(old_contact);
    if (it == different_new_contacts.end()) {
      auto phone_number = old_contact.get_phone_number();
      if (different_new_phone_numbers.count(phone_number) == 0) {
        to_delete.push_back(std::move(phone_number));
        if (user_id.is_valid()) {
          to_delete_user_ids.push_back(user_id);
        }
      }
    } else {
      unique_new_contacts[it->second].set_user_id(user_id);
      different_new_contacts.erase(it);
    }
  }
  std::pair<vector<size_t>, vector<Contact>> to_add;
  for (auto &new_contact : different_new_contacts) {
    to_add.first.push_back(new_contact.second);
    to_add.second.push_back(new_contact.first);
  }

  if (to_add.first.empty() && to_delete.empty()) {
    for (size_t i = 0; i < contacts.size(); i++) {
      auto unique_id = new_contacts_unique_id[i];
      contacts[i].set_user_id(unique_new_contacts[unique_id].get_user_id());
    }

    promise.set_value(Unit());
    return {transform(contacts, [&](const Contact &contact) { return contact.get_user_id(); }),
            vector<int32>(contacts.size())};
  }

  are_imported_contacts_changing_ = true;
  random_id = 1;

  remove_contacts_by_phone_number(
      std::move(to_delete), std::move(to_delete_user_ids),
      PromiseCreator::lambda([new_contacts = std::move(unique_new_contacts),
                              new_contacts_unique_id = std::move(new_contacts_unique_id), to_add = std::move(to_add),
                              promise = std::move(promise)](Result<> result) mutable {
        if (result.is_ok()) {
          send_closure_later(G()->contacts_manager(), &ContactsManager::on_clear_imported_contacts,
                             std::move(new_contacts), std::move(new_contacts_unique_id), std::move(to_add),
                             std::move(promise));
        } else {
          promise.set_error(result.move_as_error());
        }
      }));
  return {};
}

void ContactsManager::on_get_user_full(tl_object_ptr<telegram_api::userFull> &&user) {
  LOG(INFO) << "Receive " << to_string(user);

  UserId user_id(user->id_);
  User *u = get_user(user_id);
  if (u == nullptr) {
    LOG(ERROR) << "Failed to find " << user_id;
    return;
  }

  td_->messages_manager_->on_update_dialog_notify_settings(DialogId(user_id), std::move(user->notify_settings_),
                                                           "on_get_user_full");

  td_->messages_manager_->on_update_dialog_theme_name(DialogId(user_id), std::move(user->theme_emoticon_));

  {
    MessageId pinned_message_id;
    if ((user->flags_ & USER_FULL_FLAG_HAS_PINNED_MESSAGE) != 0) {
      pinned_message_id = MessageId(ServerMessageId(user->pinned_msg_id_));
    }
    td_->messages_manager_->on_update_dialog_last_pinned_message_id(DialogId(user_id), pinned_message_id);
  }
  {
    FolderId folder_id;
    if ((user->flags_ & USER_FULL_FLAG_HAS_FOLDER_ID) != 0) {
      folder_id = FolderId(user->folder_id_);
    }
    td_->messages_manager_->on_update_dialog_folder_id(DialogId(user_id), folder_id);
  }
  td_->messages_manager_->on_update_dialog_has_scheduled_server_messages(
      DialogId(user_id), (user->flags_ & USER_FULL_FLAG_HAS_SCHEDULED_MESSAGES) != 0);
  {
    MessageTtl message_ttl;
    if ((user->flags_ & USER_FULL_FLAG_HAS_MESSAGE_TTL) != 0) {
      message_ttl = MessageTtl(user->ttl_period_);
    }
    td_->messages_manager_->on_update_dialog_message_ttl(DialogId(user_id), message_ttl);
  }

  UserFull *user_full = add_user_full(user_id);
  user_full->expires_at = Time::now() + USER_FULL_EXPIRE_TIME;

  {
    bool is_blocked = (user->flags_ & USER_FULL_FLAG_IS_BLOCKED) != 0;
    on_update_user_full_is_blocked(user_full, user_id, is_blocked);
    td_->messages_manager_->on_update_dialog_is_blocked(DialogId(user_id), is_blocked);
  }

  on_update_user_full_common_chat_count(user_full, user_id, user->common_chats_count_);
  on_update_user_full_need_phone_number_privacy_exception(user_full, user_id,
                                                          user->settings_->need_contacts_exception_);

  bool can_pin_messages = user->can_pin_message_;
  if (user_full->can_pin_messages != can_pin_messages) {
    user_full->can_pin_messages = can_pin_messages;
    user_full->is_changed = true;
  }

  bool can_be_called = user->phone_calls_available_ && !user->phone_calls_private_;
  bool supports_video_calls = user->video_calls_available_ && !user->phone_calls_private_;
  bool has_private_calls = user->phone_calls_private_;
  if (user_full->can_be_called != can_be_called || user_full->supports_video_calls != supports_video_calls ||
      user_full->has_private_calls != has_private_calls ||
      user_full->private_forward_name != user->private_forward_name_) {
    user_full->can_be_called = can_be_called;
    user_full->supports_video_calls = supports_video_calls;
    user_full->has_private_calls = has_private_calls;
    user_full->private_forward_name = std::move(user->private_forward_name_);

    user_full->is_changed = true;
  }
  if (user_full->about != user->about_) {
    user_full->about = std::move(user->about_);
    user_full->is_changed = true;
    td_->group_call_manager_->on_update_dialog_about(DialogId(user_id), user_full->about, true);
  }
  string description;
  if (user->bot_info_ != nullptr && !td_->auth_manager_->is_bot()) {
    description = std::move(user->bot_info_->description_);

    on_update_user_full_commands(user_full, user_id, std::move(user->bot_info_->commands_));
  }
  if (user_full->description != description) {
    user_full->description = std::move(description);
    user_full->is_changed = true;
  }

  auto photo = get_photo(td_->file_manager_.get(), std::move(user->profile_photo_), DialogId(user_id));
  if (photo != user_full->photo) {
    user_full->photo = std::move(photo);
    user_full->is_changed = true;
  }
  if (user_full->photo.is_empty()) {
    drop_user_photos(user_id, true, false, "on_get_user_full");
  } else {
    register_user_photo(u, user_id, user_full->photo);
  }

  user_full->is_update_user_full_sent = true;
  update_user_full(user_full, user_id, "on_get_user_full");

  // update peer settings after UserFull is created and updated to not update twice need_phone_number_privacy_exception
  td_->messages_manager_->on_get_peer_settings(DialogId(user_id), std::move(user->settings_));
}

class ToggleSlowModeQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  int32 slow_mode_delay_ = 0;

 public:
  explicit ToggleSlowModeQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, int32 slow_mode_delay) {
    channel_id_ = channel_id;
    slow_mode_delay_ = slow_mode_delay;

    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);

    send_query(G()->net_query_creator().create(
        telegram_api::channels_toggleSlowMode(std::move(input_channel), slow_mode_delay)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_toggleSlowMode>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ToggleSlowModeQuery: " << to_string(ptr);

    td_->updates_manager_->on_get_updates(
        std::move(ptr),
        PromiseCreator::lambda([actor_id = G()->contacts_manager(), promise = std::move(promise_),
                                channel_id = channel_id_, slow_mode_delay = slow_mode_delay_](Unit result) mutable {
          send_closure(actor_id, &ContactsManager::on_update_channel_slow_mode_delay, channel_id, slow_mode_delay,
                       std::move(promise));
        }));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      td_->contacts_manager_->on_update_channel_slow_mode_delay(channel_id_, slow_mode_delay_, Promise<Unit>());
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "ToggleSlowModeQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_channel_slow_mode_delay(DialogId dialog_id, int32 slow_mode_delay, Promise<Unit> &&promise) {
  vector<int32> allowed_slow_mode_delays{0, 10, 30, 60, 300, 900, 3600};
  if (!td::contains(allowed_slow_mode_delays, slow_mode_delay)) {
    return promise.set_error(Status::Error(400, "Invalid new value for slow mode delay"));
  }

  if (!dialog_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid chat identifier specified"));
  }
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "set_channel_slow_mode_delay")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (dialog_id.get_type() != DialogType::Channel) {
    return promise.set_error(Status::Error(400, "Chat is not a supergroup"));
  }

  auto channel_id = dialog_id.get_channel_id();
  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  if (!c->is_megagroup) {
    return promise.set_error(Status::Error(400, "Chat is not a supergroup"));
  }
  if (!get_channel_permissions(c).can_restrict_members()) {
    return promise.set_error(Status::Error(400, "Not enough rights in the supergroup"));
  }

  td_->create_handler<ToggleSlowModeQuery>(std::move(promise))->send(channel_id, slow_mode_delay);
}

tl_object_ptr<td_api::chatStatisticsSupergroup> ContactsManager::convert_megagroup_stats(
    tl_object_ptr<telegram_api::stats_megagroupStats> obj) {
  CHECK(obj != nullptr);

  on_get_users(std::move(obj->users_), "convert_megagroup_stats");

  // just in case
  td::remove_if(obj->top_posters_, [](auto &obj) {
    return !UserId(obj->user_id_).is_valid() || obj->messages_ < 0 || obj->avg_chars_ < 0;
  });
  td::remove_if(obj->top_admins_, [](auto &obj) {
    return !UserId(obj->user_id_).is_valid() || obj->deleted_ < 0 || obj->kicked_ < 0 || obj->banned_ < 0;
  });
  td::remove_if(obj->top_inviters_,
                [](auto &obj) { return !UserId(obj->user_id_).is_valid() || obj->invitations_ < 0; });

  auto top_senders =
      transform(std::move(obj->top_posters_), [this](tl_object_ptr<telegram_api::statsGroupTopPoster> &&top_poster) {
        return td_api::make_object<td_api::chatStatisticsMessageSenderInfo>(
            get_user_id_object(UserId(top_poster->user_id_), "get_top_senders"), top_poster->messages_,
            top_poster->avg_chars_);
      });
  auto top_administrators =
      transform(std::move(obj->top_admins_), [this](tl_object_ptr<telegram_api::statsGroupTopAdmin> &&top_admin) {
        return td_api::make_object<td_api::chatStatisticsAdministratorActionsInfo>(
            get_user_id_object(UserId(top_admin->user_id_), "get_top_administrators"), top_admin->deleted_,
            top_admin->kicked_, top_admin->banned_);
      });
  auto top_inviters =
      transform(std::move(obj->top_inviters_), [this](tl_object_ptr<telegram_api::statsGroupTopInviter> &&top_inviter) {
        return td_api::make_object<td_api::chatStatisticsInviterInfo>(
            get_user_id_object(UserId(top_inviter->user_id_), "get_top_inviters"), top_inviter->invitations_);
      });

  return make_tl_object<td_api::chatStatisticsSupergroup>(
      convert_date_range(obj->period_), convert_stats_absolute_value(obj->members_),
      convert_stats_absolute_value(obj->messages_), convert_stats_absolute_value(obj->viewers_),
      convert_stats_absolute_value(obj->posters_), convert_stats_graph(std::move(obj->growth_graph_)),
      convert_stats_graph(std::move(obj->members_graph_)),
      convert_stats_graph(std::move(obj->new_members_by_source_graph_)),
      convert_stats_graph(std::move(obj->languages_graph_)), convert_stats_graph(std::move(obj->messages_graph_)),
      convert_stats_graph(std::move(obj->actions_graph_)), convert_stats_graph(std::move(obj->top_hours_graph_)),
      convert_stats_graph(std::move(obj->weekdays_graph_)), std::move(top_senders), std::move(top_administrators),
      std::move(top_inviters));
}

void ContactsManager::on_get_dialogs_nearby(Result<tl_object_ptr<telegram_api::Updates>> result,
                                            Promise<td_api::object_ptr<td_api::chatsNearby>> &&promise) {
  if (result.is_error()) {
    return promise.set_error(result.move_as_error());
  }

  auto updates_ptr = result.move_as_ok();
  if (updates_ptr->get_id() != telegram_api::updates::ID) {
    LOG(ERROR) << "Receive " << oneline(to_string(*updates_ptr)) << " instead of updates";
    return promise.set_error(Status::Error(500, "Receive unsupported response from the server"));
  }

  auto update = telegram_api::move_object_as<telegram_api::updates>(updates_ptr);
  LOG(INFO) << "Receive chats nearby in " << to_string(update);

  on_get_users(std::move(update->users_), "on_get_dialogs_nearby");
  on_get_chats(std::move(update->chats_), "on_get_dialogs_nearby");

  for (auto &dialog_nearby : users_nearby_) {
    user_nearby_timeout_.cancel_timeout(dialog_nearby.dialog_id.get_user_id().get());
  }
  auto old_users_nearby = std::move(users_nearby_);
  users_nearby_.clear();
  channels_nearby_.clear();
  int32 location_visibility_expire_date = 0;
  for (auto &update_ptr : update->updates_) {
    if (update_ptr->get_id() != telegram_api::updatePeerLocated::ID) {
      LOG(ERROR) << "Receive unexpected " << to_string(update);
      continue;
    }

    auto expire_date = on_update_peer_located(
        std::move(static_cast<telegram_api::updatePeerLocated *>(update_ptr.get())->peers_), false);
    if (expire_date != -1) {
      location_visibility_expire_date = expire_date;
    }
  }
  if (location_visibility_expire_date != location_visibility_expire_date_) {
    set_location_visibility_expire_date(location_visibility_expire_date);
    update_is_location_visible();
  }

  std::sort(users_nearby_.begin(), users_nearby_.end());
  if (old_users_nearby != users_nearby_) {
    send_update_users_nearby();  // for other clients connected to the same TDLib instance
  }
  promise.set_value(td_api::make_object<td_api::chatsNearby>(get_chats_nearby_object(users_nearby_),
                                                             get_chats_nearby_object(channels_nearby_)));
}

class DeleteContactsByPhoneNumberQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  vector<UserId> user_ids_;

 public:
  explicit DeleteContactsByPhoneNumberQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<string> &&user_phone_numbers, vector<UserId> &&user_ids) {
    if (user_phone_numbers.empty()) {
      return promise_.set_value(Unit());
    }
    user_ids_ = std::move(user_ids);
    send_query(G()->net_query_creator().create(telegram_api::contacts_deleteByPhones(std::move(user_phone_numbers))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_deleteByPhones>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      return on_error(Status::Error(500, "Some contacts can't be deleted"));
    }

    td_->contacts_manager_->on_deleted_contacts(user_ids_);
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->contacts_manager_->reload_contacts(true);
  }
};

void ContactsManager::remove_contacts_by_phone_number(vector<string> user_phone_numbers, vector<UserId> user_ids,
                                                      Promise<Unit> &&promise) {
  LOG(INFO) << "Delete contacts by phone number: " << format::as_array(user_phone_numbers);
  if (!are_contacts_loaded_) {
    load_contacts(std::move(promise));
    return;
  }

  td_->create_handler<DeleteContactsByPhoneNumberQuery>(std::move(promise))
      ->send(std::move(user_phone_numbers), std::move(user_ids));
}

class GetInactiveChannelsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetInactiveChannelsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::channels_getInactiveChannels()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getInactiveChannels>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetInactiveChannelsQuery: " << to_string(result);
    // TODO use result->dates_
    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetInactiveChannelsQuery");
    td_->contacts_manager_->on_get_inactive_channels(std::move(result->chats_));

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

vector<DialogId> ContactsManager::get_inactive_channels(Promise<Unit> &&promise) {
  if (inactive_channels_inited_) {
    promise.set_value(Unit());
    return transform(inactive_channels_, [&](ChannelId channel_id) {
      DialogId dialog_id{channel_id};
      td_->messages_manager_->force_create_dialog(dialog_id, "get_inactive_channels");
      return dialog_id;
    });
  }

  td_->create_handler<GetInactiveChannelsQuery>(std::move(promise))->send();
  return {};
}

std::pair<int32, vector<DialogId>> ContactsManager::search_among_dialogs(const vector<DialogId> &dialog_ids,
                                                                         const string &query, int32 limit) const {
  Hints hints;  // TODO cache Hints

  for (auto dialog_id : dialog_ids) {
    int64 rating = 0;
    if (dialog_id.get_type() == DialogType::User) {
      auto user_id = dialog_id.get_user_id();
      auto u = get_user(user_id);
      if (u == nullptr) {
        continue;
      }
      if (query.empty()) {
        hints.add(dialog_id.get(), Slice(" "));
      } else {
        hints.add(dialog_id.get(), PSLICE() << u->first_name << ' ' << u->last_name << ' ' << u->username);
      }
      rating = -get_user_was_online(u, user_id);
    } else {
      if (!td_->messages_manager_->have_dialog_info(dialog_id)) {
        continue;
      }
      if (query.empty()) {
        hints.add(dialog_id.get(), Slice(" "));
      } else {
        hints.add(dialog_id.get(), td_->messages_manager_->get_dialog_title(dialog_id));
      }
    }
    hints.set_rating(dialog_id.get(), rating);
  }

  auto result = hints.search(query, limit, true);
  return {narrow_cast<int32>(result.first), transform(result.second, [](int64 key) { return DialogId(key); })};
}

void ContactsManager::add_dialog_participants(DialogId dialog_id, const vector<UserId> &user_ids,
                                              Promise<Unit> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "add_dialog_participants")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't add members to a private chat"));
    case DialogType::Chat:
      return promise.set_error(Status::Error(400, "Can't add many members at once to a basic group chat"));
    case DialogType::Channel:
      return add_channel_participants(dialog_id.get_channel_id(), user_ids, std::move(promise));
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't add members to a secret chat"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void ContactsManager::on_update_channel_full_slow_mode_delay(ChannelFull *channel_full, ChannelId channel_id,
                                                             int32 slow_mode_delay, int32 slow_mode_next_send_date) {
  if (slow_mode_delay < 0) {
    LOG(ERROR) << "Receive slow mode delay " << slow_mode_delay << " in " << channel_id;
    slow_mode_delay = 0;
  }

  if (channel_full->slow_mode_delay != slow_mode_delay) {
    channel_full->slow_mode_delay = slow_mode_delay;
    channel_full->is_changed = true;
  }
  on_update_channel_full_slow_mode_next_send_date(channel_full, slow_mode_next_send_date);

  Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);
  bool is_slow_mode_enabled = slow_mode_delay != 0;
  if (is_slow_mode_enabled != c->is_slow_mode_enabled) {
    c->is_slow_mode_enabled = is_slow_mode_enabled;
    c->is_changed = true;
    update_channel(c, channel_id);
  }
}

void ContactsManager::load_chat_full(ChatId chat_id, bool force, Promise<Unit> &&promise, const char *source) {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Group not found"));
  }

  auto chat_full = get_chat_full_force(chat_id, source);
  if (chat_full == nullptr) {
    LOG(INFO) << "Full " << chat_id << " not found";
    return send_get_chat_full_query(chat_id, std::move(promise), source);
  }

  if (is_chat_full_outdated(chat_full, c, chat_id)) {
    LOG(INFO) << "Have outdated full " << chat_id;
    if (td_->auth_manager_->is_bot() && !force) {
      return send_get_chat_full_query(chat_id, std::move(promise), source);
    }

    send_get_chat_full_query(chat_id, Auto(), source);
  }

  promise.set_value(Unit());
}

void ContactsManager::finish_get_dialog_participant(DialogParticipant &&dialog_participant,
                                                    Promise<td_api::object_ptr<td_api::chatMember>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto participant_dialog_id = dialog_participant.dialog_id_;
  bool is_user = participant_dialog_id.get_type() == DialogType::User;
  if ((is_user && !have_user(participant_dialog_id.get_user_id())) ||
      (!is_user && !td_->messages_manager_->have_dialog(participant_dialog_id))) {
    return promise.set_error(Status::Error(400, "Member not found"));
  }

  promise.set_value(get_chat_member_object(dialog_participant));
}

void ContactsManager::finish_get_created_public_dialogs(PublicDialogType type, Result<Unit> &&result) {
  auto index = static_cast<int32>(type);
  auto promises = std::move(get_created_public_channels_queries_[index]);
  reset_to_empty(get_created_public_channels_queries_[index]);
  if (G()->close_flag()) {
    result = G()->close_status();
  }
  if (result.is_error()) {
    for (auto &promise : promises) {
      promise.set_error(result.error().clone());
    }
    return;
  }

  CHECK(created_public_channels_inited_[index]);
  for (auto &promise : promises) {
    return_created_public_dialogs(std::move(promise), created_public_channels_[index]);
  }
}

void ContactsManager::on_dismiss_suggested_action(SuggestedAction action, Result<Unit> &&result) {
  auto it = dismiss_suggested_action_queries_.find(action.dialog_id_);
  CHECK(it != dismiss_suggested_action_queries_.end());
  auto promises = std::move(it->second);
  dismiss_suggested_action_queries_.erase(it);

  if (result.is_error()) {
    for (auto &promise : promises) {
      promise.set_error(result.error().clone());
    }
    return;
  }

  remove_dialog_suggested_action(action);

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

void ContactsManager::on_update_channel_photo(Channel *c, ChannelId channel_id,
                                              tl_object_ptr<telegram_api::ChatPhoto> &&chat_photo_ptr) {
  DialogPhoto new_chat_photo =
      get_dialog_photo(td_->file_manager_.get(), DialogId(channel_id), c->access_hash, std::move(chat_photo_ptr));
  if (td_->auth_manager_->is_bot()) {
    new_chat_photo.minithumbnail.clear();
  }

  if (new_chat_photo != c->photo) {
    c->photo = new_chat_photo;
    c->is_photo_changed = true;
    c->need_save_to_database = true;
  }
}

void ContactsManager::on_update_channel_username(ChannelId channel_id, string &&username) {
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id;
    return;
  }

  Channel *c = get_channel_force(channel_id);
  if (c != nullptr) {
    on_update_channel_username(c, channel_id, std::move(username));
    update_channel(c, channel_id);
  } else {
    LOG(INFO) << "Ignore update channel username about unknown " << channel_id;
  }
}

void ContactsManager::save_channel_to_database(Channel *c, ChannelId channel_id) {
  CHECK(c != nullptr);
  if (c->is_being_saved) {
    return;
  }
  if (loaded_from_database_channels_.count(channel_id)) {
    save_channel_to_database_impl(c, channel_id, get_channel_database_value(c));
    return;
  }
  if (load_channel_from_database_queries_.count(channel_id) != 0) {
    return;
  }

  load_channel_from_database_impl(channel_id, Auto());
}

void ContactsManager::on_channel_unban_timeout_callback(void *contacts_manager_ptr, int64 channel_id_long) {
  if (G()->close_flag()) {
    return;
  }

  auto contacts_manager = static_cast<ContactsManager *>(contacts_manager_ptr);
  send_closure_later(contacts_manager->actor_id(contacts_manager), &ContactsManager::on_channel_unban_timeout,
                     ChannelId(channel_id_long));
}

void ContactsManager::on_delete_profile_photo(int64 profile_photo_id, Promise<Unit> promise) {
  UserId my_user_id = get_my_id();

  bool need_reget_user = delete_profile_photo_from_cache(my_user_id, profile_photo_id, true);
  if (need_reget_user && !G()->close_flag()) {
    return reload_user(my_user_id, std::move(promise));
  }

  promise.set_value(Unit());
}

void ContactsManager::on_get_contacts_failed(Status error) {
  CHECK(error.is_error());
  next_contacts_sync_date_ = G()->unix_time() + Random::fast(5, 10);
  auto promises = std::move(load_contacts_queries_);
  load_contacts_queries_.clear();
  for (auto &promise : promises) {
    promise.set_error(error.clone());
  }
}

ContactsManager::Channel *ContactsManager::add_channel(ChannelId channel_id, const char *source) {
  CHECK(channel_id.is_valid());
  auto &channel_ptr = channels_[channel_id];
  if (channel_ptr == nullptr) {
    channel_ptr = make_unique<Channel>();
  }
  return channel_ptr.get();
}

const ContactsManager::Channel *ContactsManager::get_channel(ChannelId channel_id) const {
  auto p = channels_.find(channel_id);
  if (p == channels_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

const MinChannel *ContactsManager::get_min_channel(ChannelId channel_id) const {
  auto it = min_channels_.find(channel_id);
  if (it == min_channels_.end()) {
    return nullptr;
  }
  return it->second.get();
}

tl_object_ptr<td_api::supergroupFullInfo> ContactsManager::get_supergroup_full_info_object(ChannelId channel_id) const {
  return get_supergroup_full_info_object(get_channel_full(channel_id), channel_id);
}

bool ContactsManager::is_user_status_exact(UserId user_id) const {
  auto u = get_user(user_id);
  return u != nullptr && !u->is_deleted && !u->is_bot && u->was_online > 0;
}

bool ContactsManager::get_chat_is_active(ChatId chat_id) const {
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return false;
  }
  return c->is_active;
}

bool ContactsManager::get_channel_has_linked_channel(const Channel *c) {
  return c->has_linked_channel;
}

bool ContactsManager::get_channel_sign_messages(const Channel *c) {
  return c->sign_messages;
}
}  // namespace td
