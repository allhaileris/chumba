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
#include "td/telegram/StickersManager.h"
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


#include <limits>



namespace td {
}  // namespace td
namespace td {

void ContactsManager::on_update_channel_full_linked_channel_id(ChannelFull *channel_full, ChannelId channel_id,
                                                               ChannelId linked_channel_id) {
  auto old_linked_channel_id = get_linked_channel_id(channel_id);
  LOG(INFO) << "Uplate linked channel in " << channel_id << " from " << old_linked_channel_id << " to "
            << linked_channel_id;

  if (channel_full != nullptr && channel_full->linked_channel_id != linked_channel_id &&
      channel_full->linked_channel_id.is_valid()) {
    get_channel_force(channel_full->linked_channel_id);
    get_channel_full_force(channel_full->linked_channel_id, true, "on_update_channel_full_linked_channel_id 0");
  }
  auto old_linked_linked_channel_id = get_linked_channel_id(linked_channel_id);

  remove_linked_channel_id(channel_id);
  remove_linked_channel_id(linked_channel_id);
  if (channel_id.is_valid() && linked_channel_id.is_valid()) {
    linked_channel_ids_[channel_id] = linked_channel_id;
    linked_channel_ids_[linked_channel_id] = channel_id;
  }

  if (channel_full != nullptr && channel_full->linked_channel_id != linked_channel_id) {
    if (channel_full->linked_channel_id.is_valid()) {
      // remove link from a previously linked channel_full
      auto linked_channel = get_channel_force(channel_full->linked_channel_id);
      if (linked_channel != nullptr && linked_channel->has_linked_channel) {
        linked_channel->has_linked_channel = false;
        linked_channel->is_changed = true;
        update_channel(linked_channel, channel_full->linked_channel_id);
        reload_channel(channel_full->linked_channel_id, Auto());
      }
      auto linked_channel_full =
          get_channel_full_force(channel_full->linked_channel_id, true, "on_update_channel_full_linked_channel_id 1");
      if (linked_channel_full != nullptr && linked_channel_full->linked_channel_id == channel_id) {
        linked_channel_full->linked_channel_id = ChannelId();
        linked_channel_full->is_changed = true;
        update_channel_full(linked_channel_full, channel_full->linked_channel_id,
                            "on_update_channel_full_linked_channel_id 3");
      }
    }

    channel_full->linked_channel_id = linked_channel_id;
    channel_full->is_changed = true;

    if (channel_full->linked_channel_id.is_valid()) {
      // add link from a newly linked channel_full
      auto linked_channel = get_channel_force(channel_full->linked_channel_id);
      if (linked_channel != nullptr && !linked_channel->has_linked_channel) {
        linked_channel->has_linked_channel = true;
        linked_channel->is_changed = true;
        update_channel(linked_channel, channel_full->linked_channel_id);
        reload_channel(channel_full->linked_channel_id, Auto());
      }
      auto linked_channel_full =
          get_channel_full_force(channel_full->linked_channel_id, true, "on_update_channel_full_linked_channel_id 2");
      if (linked_channel_full != nullptr && linked_channel_full->linked_channel_id != channel_id) {
        linked_channel_full->linked_channel_id = channel_id;
        linked_channel_full->is_changed = true;
        update_channel_full(linked_channel_full, channel_full->linked_channel_id,
                            "on_update_channel_full_linked_channel_id 4");
      }
    }
  }

  Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);
  if (linked_channel_id.is_valid() != c->has_linked_channel) {
    c->has_linked_channel = linked_channel_id.is_valid();
    c->is_changed = true;
    update_channel(c, channel_id);
  }

  if (old_linked_channel_id != linked_channel_id) {
    // must be called after the linked channel is changed
    td_->messages_manager_->on_dialog_linked_channel_updated(DialogId(channel_id), old_linked_channel_id,
                                                             linked_channel_id);
  }

  if (linked_channel_id.is_valid()) {
    auto new_linked_linked_channel_id = get_linked_channel_id(linked_channel_id);
    LOG(INFO) << "Uplate linked channel in " << linked_channel_id << " from " << old_linked_linked_channel_id << " to "
              << new_linked_linked_channel_id;
    if (old_linked_linked_channel_id != new_linked_linked_channel_id) {
      // must be called after the linked channel is changed
      td_->messages_manager_->on_dialog_linked_channel_updated(
          DialogId(linked_channel_id), old_linked_linked_channel_id, new_linked_linked_channel_id);
    }
  }
}

class SetDiscussionGroupQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId broadcast_channel_id_;
  ChannelId group_channel_id_;

 public:
  explicit SetDiscussionGroupQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId broadcast_channel_id,
            telegram_api::object_ptr<telegram_api::InputChannel> broadcast_input_channel, ChannelId group_channel_id,
            telegram_api::object_ptr<telegram_api::InputChannel> group_input_channel) {
    broadcast_channel_id_ = broadcast_channel_id;
    group_channel_id_ = group_channel_id;
    send_query(G()->net_query_creator().create(
        telegram_api::channels_setDiscussionGroup(std::move(broadcast_input_channel), std::move(group_input_channel))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_setDiscussionGroup>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    LOG_IF(INFO, !result) << "Set discussion group has failed";

    td_->contacts_manager_->on_update_channel_linked_channel_id(broadcast_channel_id_, group_channel_id_);
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (status.message() == "LINK_NOT_MODIFIED") {
      return promise_.set_value(Unit());
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_channel_discussion_group(DialogId dialog_id, DialogId discussion_dialog_id,
                                                   Promise<Unit> &&promise) {
  if (!dialog_id.is_valid() && !discussion_dialog_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid chat identifiers specified"));
  }

  ChannelId broadcast_channel_id;
  telegram_api::object_ptr<telegram_api::InputChannel> broadcast_input_channel;
  if (dialog_id.is_valid()) {
    if (!td_->messages_manager_->have_dialog_force(dialog_id, "set_channel_discussion_group 1")) {
      return promise.set_error(Status::Error(400, "Chat not found"));
    }

    if (dialog_id.get_type() != DialogType::Channel) {
      return promise.set_error(Status::Error(400, "Chat is not a channel"));
    }

    broadcast_channel_id = dialog_id.get_channel_id();
    const Channel *c = get_channel(broadcast_channel_id);
    if (c == nullptr) {
      return promise.set_error(Status::Error(400, "Chat info not found"));
    }

    if (c->is_megagroup) {
      return promise.set_error(Status::Error(400, "Chat is not a channel"));
    }
    if (!c->status.is_administrator() || !c->status.can_change_info_and_settings()) {
      return promise.set_error(Status::Error(400, "Not enough rights in the channel"));
    }

    broadcast_input_channel = get_input_channel(broadcast_channel_id);
    CHECK(broadcast_input_channel != nullptr);
  } else {
    broadcast_input_channel = telegram_api::make_object<telegram_api::inputChannelEmpty>();
  }

  ChannelId group_channel_id;
  telegram_api::object_ptr<telegram_api::InputChannel> group_input_channel;
  if (discussion_dialog_id.is_valid()) {
    if (!td_->messages_manager_->have_dialog_force(discussion_dialog_id, "set_channel_discussion_group 2")) {
      return promise.set_error(Status::Error(400, "Discussion chat not found"));
    }
    if (discussion_dialog_id.get_type() != DialogType::Channel) {
      return promise.set_error(Status::Error(400, "Discussion chat is not a supergroup"));
    }

    group_channel_id = discussion_dialog_id.get_channel_id();
    const Channel *c = get_channel(group_channel_id);
    if (c == nullptr) {
      return promise.set_error(Status::Error(400, "Discussion chat info not found"));
    }

    if (!c->is_megagroup) {
      return promise.set_error(Status::Error(400, "Discussion chat is not a supergroup"));
    }
    if (!c->status.is_administrator() || !c->status.can_pin_messages()) {
      return promise.set_error(Status::Error(400, "Not enough rights in the supergroup"));
    }

    group_input_channel = get_input_channel(group_channel_id);
    CHECK(group_input_channel != nullptr);
  } else {
    group_input_channel = telegram_api::make_object<telegram_api::inputChannelEmpty>();
  }

  td_->create_handler<SetDiscussionGroupQuery>(std::move(promise))
      ->send(broadcast_channel_id, std::move(broadcast_input_channel), group_channel_id,
             std::move(group_input_channel));
}

class SetChannelStickerSetQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;
  StickerSetId sticker_set_id_;

 public:
  explicit SetChannelStickerSetQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, StickerSetId sticker_set_id,
            telegram_api::object_ptr<telegram_api::InputStickerSet> &&input_sticker_set) {
    channel_id_ = channel_id;
    sticker_set_id_ = sticker_set_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::channels_setStickers(std::move(input_channel), std::move(input_sticker_set))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_setStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    LOG(DEBUG) << "Receive result for SetChannelStickerSetQuery: " << result;
    if (!result) {
      return on_error(Status::Error(500, "Supergroup sticker set not updated"));
    }

    td_->contacts_manager_->on_update_channel_sticker_set(channel_id_, sticker_set_id_);
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      td_->contacts_manager_->on_update_channel_sticker_set(channel_id_, sticker_set_id_);
      if (!td_->auth_manager_->is_bot()) {
        promise_.set_value(Unit());
        return;
      }
    } else {
      td_->contacts_manager_->on_get_channel_error(channel_id_, status, "SetChannelStickerSetQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::set_channel_sticker_set(ChannelId channel_id, StickerSetId sticker_set_id,
                                              Promise<Unit> &&promise) {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Supergroup not found"));
  }
  if (!c->is_megagroup) {
    return promise.set_error(Status::Error(400, "Chat sticker set can be set only for supergroups"));
  }
  if (!get_channel_permissions(c).can_change_info_and_settings()) {
    return promise.set_error(Status::Error(400, "Not enough rights to change supergroup sticker set"));
  }

  telegram_api::object_ptr<telegram_api::InputStickerSet> input_sticker_set;
  if (!sticker_set_id.is_valid()) {
    input_sticker_set = telegram_api::make_object<telegram_api::inputStickerSetEmpty>();
  } else {
    input_sticker_set = td_->stickers_manager_->get_input_sticker_set(sticker_set_id);
    if (input_sticker_set == nullptr) {
      return promise.set_error(Status::Error(400, "Sticker set not found"));
    }
  }

  auto channel_full = get_channel_full(channel_id, false, "set_channel_sticker_set");
  if (channel_full != nullptr && !channel_full->can_set_sticker_set) {
    return promise.set_error(Status::Error(400, "Can't set supergroup sticker set"));
  }

  td_->create_handler<SetChannelStickerSetQuery>(std::move(promise))
      ->send(channel_id, sticker_set_id, std::move(input_sticker_set));
}

class GetContactsStatusesQuery final : public Td::ResultHandler {
 public:
  void send() {
    send_query(G()->net_query_creator().create(telegram_api::contacts_getStatuses()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_getStatuses>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->contacts_manager_->on_get_contacts_statuses(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for GetContactsStatusesQuery: " << status;
    }
  }
};

void ContactsManager::on_get_contacts(tl_object_ptr<telegram_api::contacts_Contacts> &&new_contacts) {
  next_contacts_sync_date_ = G()->unix_time() + Random::fast(70000, 100000);

  CHECK(new_contacts != nullptr);
  if (new_contacts->get_id() == telegram_api::contacts_contactsNotModified::ID) {
    if (saved_contact_count_ == -1) {
      saved_contact_count_ = 0;
    }
    on_get_contacts_finished(contacts_hints_.size());
    td_->create_handler<GetContactsStatusesQuery>()->send();
    return;
  }

  auto contacts = move_tl_object_as<telegram_api::contacts_contacts>(new_contacts);
  std::unordered_set<UserId, UserIdHash> contact_user_ids;
  for (auto &user : contacts->users_) {
    auto user_id = get_user_id(user);
    if (!user_id.is_valid()) {
      LOG(ERROR) << "Receive invalid " << user_id;
      continue;
    }
    contact_user_ids.insert(user_id);
  }
  on_get_users(std::move(contacts->users_), "on_get_contacts");

  UserId my_id = get_my_id();
  for (auto &p : users_) {
    UserId user_id = p.first;
    User *u = p.second.get();
    bool should_be_contact = contact_user_ids.count(user_id) == 1;
    if (u->is_contact != should_be_contact) {
      if (u->is_contact) {
        LOG(INFO) << "Drop contact with " << user_id;
        if (user_id != my_id) {
          LOG_CHECK(contacts_hints_.has_key(user_id.get()))
              << my_id << " " << user_id << " " << to_string(get_user_object(user_id, u));
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
      } else {
        LOG(ERROR) << "Receive non-contact " << user_id << " in the list of contacts";
      }
    }
  }

  saved_contact_count_ = contacts->saved_count_;
  on_get_contacts_finished(std::numeric_limits<size_t>::max());
}

void ContactsManager::on_update_online_status_privacy() {
  td_->create_handler<GetContactsStatusesQuery>()->send();
}

class EditChannelCreatorQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit EditChannelCreatorQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, UserId user_id,
            tl_object_ptr<telegram_api::InputCheckPasswordSRP> input_check_password) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      return promise_.set_error(Status::Error(400, "Have no access to the chat"));
    }
    auto r_input_user = td_->contacts_manager_->get_input_user(user_id);
    if (r_input_user.is_error()) {
      return promise_.set_error(r_input_user.move_as_error());
    }
    send_query(G()->net_query_creator().create(telegram_api::channels_editCreator(
        std::move(input_channel), r_input_user.move_as_ok(), std::move(input_check_password))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_editCreator>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditChannelCreatorQuery: " << to_string(ptr);
    td_->contacts_manager_->invalidate_channel_full(channel_id_, false);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "EditChannelCreatorQuery");
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("EditChannelCreatorQuery");
  }
};

void ContactsManager::transfer_channel_ownership(
    ChannelId channel_id, UserId user_id, tl_object_ptr<telegram_api::InputCheckPasswordSRP> input_check_password,
    Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  td_->create_handler<EditChannelCreatorQuery>(std::move(promise))
      ->send(channel_id, user_id, std::move(input_check_password));
}

void ContactsManager::on_get_is_location_visible(Result<tl_object_ptr<telegram_api::Updates>> &&result,
                                                 Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  if (result.is_error()) {
    if (result.error().message() == "GEO_POINT_INVALID" && pending_location_visibility_expire_date_ == -1 &&
        location_visibility_expire_date_ > 0) {
      set_location_visibility_expire_date(0);
      update_is_location_visible();
    }
    return promise.set_value(Unit());
  }

  auto updates_ptr = result.move_as_ok();
  if (updates_ptr->get_id() != telegram_api::updates::ID) {
    LOG(ERROR) << "Receive " << oneline(to_string(*updates_ptr)) << " instead of updates";
    return promise.set_value(Unit());
  }

  auto updates = std::move(telegram_api::move_object_as<telegram_api::updates>(updates_ptr)->updates_);
  if (updates.size() != 1 || updates[0]->get_id() != telegram_api::updatePeerLocated::ID) {
    LOG(ERROR) << "Receive unexpected " << to_string(updates);
    return promise.set_value(Unit());
  }

  auto peers = std::move(static_cast<telegram_api::updatePeerLocated *>(updates[0].get())->peers_);
  if (peers.size() != 1 || peers[0]->get_id() != telegram_api::peerSelfLocated::ID) {
    LOG(ERROR) << "Receive unexpected " << to_string(peers);
    return promise.set_value(Unit());
  }

  auto location_visibility_expire_date = static_cast<telegram_api::peerSelfLocated *>(peers[0].get())->expires_;
  if (location_visibility_expire_date != location_visibility_expire_date_) {
    set_location_visibility_expire_date(location_visibility_expire_date);
    update_is_location_visible();
  }

  promise.set_value(Unit());
}

void ContactsManager::update_secret_chat(SecretChat *c, SecretChatId secret_chat_id, bool from_binlog,
                                         bool from_database) {
  CHECK(c != nullptr);
  LOG(DEBUG) << "Update " << secret_chat_id << ": need_save_to_database = " << c->need_save_to_database
             << ", is_changed = " << c->is_changed;
  c->need_save_to_database |= c->is_changed;
  if (c->need_save_to_database) {
    if (!from_database) {
      c->is_saved = false;
    }
    c->need_save_to_database = false;

    DialogId dialog_id(secret_chat_id);
    send_closure_later(G()->messages_manager(), &MessagesManager::force_create_dialog, dialog_id, "update secret chat",
                       true, true);
    if (c->is_state_changed) {
      send_closure_later(G()->messages_manager(), &MessagesManager::on_update_secret_chat_state, secret_chat_id,
                         c->state);
      c->is_state_changed = false;
    }
    if (c->is_ttl_changed) {
      send_closure_later(G()->messages_manager(), &MessagesManager::on_update_dialog_message_ttl,
                         DialogId(secret_chat_id), MessageTtl(c->ttl));
      c->is_ttl_changed = false;
    }
  }
  if (c->is_changed) {
    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateSecretChat>(get_secret_chat_object(secret_chat_id, c)));
    c->is_changed = false;
  }

  if (!from_database) {
    save_secret_chat(c, secret_chat_id, from_binlog);
  }
}

void ContactsManager::drop_user_photos(UserId user_id, bool is_empty, bool drop_user_full_photo, const char *source) {
  auto it = user_photos_.find(user_id);
  if (it != user_photos_.end()) {
    auto user_photos = &it->second;
    int32 new_count = is_empty ? 0 : -1;
    if (user_photos->count == new_count) {
      CHECK(user_photos->photos.empty());
      CHECK(user_photos->offset == user_photos->count);
      return;
    }

    LOG(INFO) << "Drop photos of " << user_id << " to " << (is_empty ? "empty" : "unknown") << " from " << source;
    user_photos->photos.clear();
    user_photos->count = new_count;
    user_photos->offset = user_photos->count;
  }

  if (drop_user_full_photo) {
    auto user_full = get_user_full(user_id);  // must not load UserFull
    if (user_full == nullptr) {
      return;
    }

    if (!user_full->photo.is_empty()) {
      user_full->photo = Photo();
      user_full->is_changed = true;
    }
    if (!is_empty) {
      if (user_full->expires_at > 0.0) {
        user_full->expires_at = 0.0;
        user_full->need_save_to_database = true;
      }
      load_user_full(user_id, true, Auto(), "drop_user_photos");
    }
    update_user_full(user_full, user_id, "drop_user_photos");
  }
}

void ContactsManager::add_dialog_participant(DialogId dialog_id, UserId user_id, int32 forward_limit,
                                             Promise<Unit> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "add_dialog_participant")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't add members to a private chat"));
    case DialogType::Chat:
      return add_chat_participant(dialog_id.get_chat_id(), user_id, forward_limit, std::move(promise));
    case DialogType::Channel:
      return add_channel_participant(dialog_id.get_channel_id(), user_id, DialogParticipantStatus::Left(),
                                     std::move(promise));
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't add members to a secret chat"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void ContactsManager::on_set_location_visibility_expire_date(int32 set_expire_date, int32 error_code) {
  bool success = error_code == 0;
  is_set_location_visibility_request_sent_ = false;

  if (set_expire_date != pending_location_visibility_expire_date_) {
    try_send_set_location_visibility_query();
    return;
  }

  if (success) {
    set_location_visibility_expire_date(pending_location_visibility_expire_date_);
  } else {
    if (G()->close_flag()) {
      // request will be re-sent after restart
      return;
    }
    if (error_code != 406) {
      LOG(ERROR) << "Failed to set location visibility expire date to " << pending_location_visibility_expire_date_;
    }
  }
  G()->td_db()->get_binlog_pmc()->erase("pending_location_visibility_expire_date");
  pending_location_visibility_expire_date_ = -1;
  update_is_location_visible();
}

ContactsManager::SecretChat *ContactsManager::get_secret_chat_force(SecretChatId secret_chat_id) {
  if (!secret_chat_id.is_valid()) {
    return nullptr;
  }

  SecretChat *c = get_secret_chat(secret_chat_id);
  if (c != nullptr) {
    if (!have_user_force(c->user_id)) {
      LOG(ERROR) << "Can't find " << c->user_id << " from " << secret_chat_id;
    }
    return c;
  }
  if (!G()->parameters().use_chat_info_db) {
    return nullptr;
  }
  if (loaded_from_database_secret_chats_.count(secret_chat_id)) {
    return nullptr;
  }

  LOG(INFO) << "Trying to load " << secret_chat_id << " from database";
  on_load_secret_chat_from_database(
      secret_chat_id, G()->td_db()->get_sqlite_sync_pmc()->get(get_secret_chat_database_key(secret_chat_id)), true);
  return get_secret_chat(secret_chat_id);
}

void ContactsManager::get_dialog_participant(DialogId dialog_id, DialogId participant_dialog_id,
                                             Promise<td_api::object_ptr<td_api::chatMember>> &&promise) {
  auto new_promise = PromiseCreator::lambda(
      [actor_id = actor_id(this), promise = std::move(promise)](Result<DialogParticipant> &&result) mutable {
        TRY_RESULT_PROMISE(promise, dialog_participant, std::move(result));
        send_closure(actor_id, &ContactsManager::finish_get_dialog_participant, std::move(dialog_participant),
                     std::move(promise));
      });
  do_get_dialog_participant(dialog_id, participant_dialog_id, std::move(new_promise));
}

void ContactsManager::on_load_imported_contacts_finished() {
  LOG(INFO) << "Finished to load " << all_imported_contacts_.size() << " imported contacts";

  for (const auto &contact : all_imported_contacts_) {
    get_user_id_object(contact.get_user_id(), "on_load_imported_contacts_finished");  // to ensure updateUser
  }

  if (need_clear_imported_contacts_) {
    need_clear_imported_contacts_ = false;
    all_imported_contacts_.clear();
  }
  are_imported_contacts_loaded_ = true;
  auto promises = std::move(load_imported_contacts_queries_);
  load_imported_contacts_queries_.clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

void ContactsManager::update_dialogs_for_discussion(DialogId dialog_id, bool is_suitable) {
  if (!dialogs_for_discussion_inited_) {
    return;
  }

  if (is_suitable) {
    if (!td::contains(dialogs_for_discussion_, dialog_id)) {
      LOG(DEBUG) << "Add " << dialog_id << " to list of suitable discussion chats";
      dialogs_for_discussion_.insert(dialogs_for_discussion_.begin(), dialog_id);
    }
  } else {
    if (td::remove(dialogs_for_discussion_, dialog_id)) {
      LOG(DEBUG) << "Remove " << dialog_id << " from list of suitable discussion chats";
    }
  }
}

ChatId ContactsManager::get_chat_id(const tl_object_ptr<telegram_api::Chat> &chat) {
  CHECK(chat != nullptr);
  switch (chat->get_id()) {
    case telegram_api::chatEmpty::ID:
      return ChatId(static_cast<const telegram_api::chatEmpty *>(chat.get())->id_);
    case telegram_api::chat::ID:
      return ChatId(static_cast<const telegram_api::chat *>(chat.get())->id_);
    case telegram_api::chatForbidden::ID:
      return ChatId(static_cast<const telegram_api::chatForbidden *>(chat.get())->id_);
    default:
      return ChatId();
  }
}

const DialogPhoto *ContactsManager::get_user_dialog_photo(UserId user_id) {
  auto u = get_user(user_id);
  if (u == nullptr) {
    return nullptr;
  }

  if (!u->is_photo_inited) {
    auto it = pending_user_photos_.find(user_id);
    if (it != pending_user_photos_.end()) {
      do_update_user_photo(u, user_id, std::move(it->second), "get_user_dialog_photo");
      pending_user_photos_.erase(it);
      update_user(u, user_id);
    }
  }
  return &u->photo;
}

ChannelId ContactsManager::get_channel_id(const tl_object_ptr<telegram_api::Chat> &chat) {
  CHECK(chat != nullptr);
  switch (chat->get_id()) {
    case telegram_api::channel::ID:
      return ChannelId(static_cast<const telegram_api::channel *>(chat.get())->id_);
    case telegram_api::channelForbidden::ID:
      return ChannelId(static_cast<const telegram_api::channelForbidden *>(chat.get())->id_);
    default:
      return ChannelId();
  }
}

RestrictedRights ContactsManager::get_user_default_permissions(UserId user_id) const {
  auto u = get_user(user_id);
  if (u == nullptr || user_id == get_replies_bot_user_id()) {
    return RestrictedRights(false, false, false, false, false, false, false, false, false, false, u != nullptr);
  }
  return RestrictedRights(true, true, true, true, true, true, true, true, false, false, true);
}

void ContactsManager::on_update_chat_full_invite_link(ChatFull *chat_full,
                                                      tl_object_ptr<telegram_api::chatInviteExported> &&invite_link) {
  CHECK(chat_full != nullptr);
  if (update_permanent_invite_link(chat_full->invite_link, DialogInviteLink(std::move(invite_link)))) {
    chat_full->is_changed = true;
  }
}

td_api::object_ptr<td_api::updateBasicGroup> ContactsManager::get_update_unknown_basic_group_object(ChatId chat_id) {
  return td_api::make_object<td_api::updateBasicGroup>(td_api::make_object<td_api::basicGroup>(
      chat_id.get(), 0, DialogParticipantStatus::Banned(0).get_chat_member_status_object(), true, 0));
}

void ContactsManager::on_update_user_phone_number(User *u, UserId user_id, string &&phone_number) {
  if (u->phone_number != phone_number) {
    u->phone_number = std::move(phone_number);
    LOG(DEBUG) << "Phone number has changed for " << user_id;
    u->is_changed = true;
  }
}

ContactsManager::Chat *ContactsManager::add_chat(ChatId chat_id) {
  CHECK(chat_id.is_valid());
  auto &chat_ptr = chats_[chat_id];
  if (chat_ptr == nullptr) {
    chat_ptr = make_unique<Chat>();
  }
  return chat_ptr.get();
}

ContactsManager::UserFull *ContactsManager::get_user_full(UserId user_id) {
  auto p = users_full_.find(user_id);
  if (p == users_full_.end()) {
    return nullptr;
  } else {
    return p->second.get();
  }
}

bool ContactsManager::get_channel_has_linked_channel(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return false;
  }
  return get_channel_has_linked_channel(c);
}

tl_object_ptr<td_api::supergroup> ContactsManager::get_supergroup_object(ChannelId channel_id) const {
  return get_supergroup_object(channel_id, get_channel(channel_id));
}

bool ContactsManager::have_input_peer_chat(ChatId chat_id, AccessRights access_rights) const {
  return have_input_peer_chat(get_chat(chat_id), access_rights);
}

string ContactsManager::get_user_database_key(UserId user_id) {
  return PSTRING() << "us" << user_id.get();
}

}  // namespace td
