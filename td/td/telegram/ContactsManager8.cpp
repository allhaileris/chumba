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

#include "td/telegram/files/FileManager.h"
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


#include <limits>



namespace td {
}  // namespace td
namespace td {

class EditChannelAdminQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit EditChannelAdminQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, tl_object_ptr<telegram_api::InputUser> &&input_user,
            const DialogParticipantStatus &status) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::channels_editAdmin(
        std::move(input_channel), std::move(input_user), status.get_chat_admin_rights(), status.get_rank())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_editAdmin>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditChannelAdminQuery: " << to_string(ptr);
    td_->contacts_manager_->invalidate_channel_full(channel_id_, false);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "EditChannelAdminQuery");
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("EditChannelAdminQuery");
  }
};

void ContactsManager::set_channel_participant_status_impl(ChannelId channel_id, DialogId participant_dialog_id,
                                                          DialogParticipantStatus status,
                                                          DialogParticipantStatus old_status, Promise<Unit> &&promise) {
  if (old_status == status && !old_status.is_creator()) {
    return promise.set_value(Unit());
  }

  LOG(INFO) << "Change status of " << participant_dialog_id << " in " << channel_id << " from " << old_status << " to "
            << status;
  bool need_add = false;
  bool need_promote = false;
  bool need_restrict = false;
  if (status.is_creator() || old_status.is_creator()) {
    if (!old_status.is_creator()) {
      return promise.set_error(Status::Error(400, "Can't add another owner to the chat"));
    }
    if (!status.is_creator()) {
      return promise.set_error(Status::Error(400, "Can't remove chat owner"));
    }
    if (participant_dialog_id != DialogId(get_my_id())) {
      return promise.set_error(Status::Error(400, "Not enough rights to edit chat owner rights"));
    }
    if (status.is_member() == old_status.is_member()) {
      // change rank and is_anonymous
      auto r_input_user = get_input_user(get_my_id());
      CHECK(r_input_user.is_ok());
      td_->create_handler<EditChannelAdminQuery>(std::move(promise))
          ->send(channel_id, r_input_user.move_as_ok(), status);
      return;
    }
    if (status.is_member()) {
      // creator not member -> creator member
      need_add = true;
    } else {
      // creator member -> creator not member
      need_restrict = true;
    }
  } else if (status.is_administrator()) {
    need_promote = true;
  } else if (!status.is_member() || status.is_restricted()) {
    if (status.is_member() && !old_status.is_member()) {
      // TODO there is no way in server API to invite someone and change restrictions
      // we need to first add user and change restrictions again after that
      // but if restrictions aren't changed, then adding is enough
      auto copy_old_status = old_status;
      copy_old_status.set_is_member(true);
      if (copy_old_status == status) {
        need_add = true;
      } else {
        need_restrict = true;
      }
    } else {
      need_restrict = true;
    }
  } else {
    // regular member
    if (old_status.is_administrator()) {
      need_promote = true;
    } else if (old_status.is_restricted() || old_status.is_banned()) {
      need_restrict = true;
    } else {
      CHECK(!old_status.is_member());
      need_add = true;
    }
  }

  if (need_promote) {
    if (participant_dialog_id.get_type() != DialogType::User) {
      return promise.set_error(Status::Error(400, "Can't promote chats to chat administrators"));
    }
    return promote_channel_participant(channel_id, participant_dialog_id.get_user_id(), status, old_status,
                                       std::move(promise));
  } else if (need_restrict) {
    return restrict_channel_participant(channel_id, participant_dialog_id, std::move(status), std::move(old_status),
                                        std::move(promise));
  } else {
    CHECK(need_add);
    if (participant_dialog_id.get_type() != DialogType::User) {
      return promise.set_error(Status::Error(400, "Can't add chats as chat members"));
    }
    return add_channel_participant(channel_id, participant_dialog_id.get_user_id(), old_status, std::move(promise));
  }
}

void ContactsManager::promote_channel_participant(ChannelId channel_id, UserId user_id,
                                                  const DialogParticipantStatus &status,
                                                  const DialogParticipantStatus &old_status, Promise<Unit> &&promise) {
  LOG(INFO) << "Promote " << user_id << " in " << channel_id << " from " << old_status << " to " << status;
  const Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);

  if (user_id == get_my_id()) {
    if (status.is_administrator()) {
      return promise.set_error(Status::Error(400, "Can't promote self"));
    }
    CHECK(status.is_member());
    // allow to demote self. TODO is it allowed server-side?
  } else {
    if (!get_channel_permissions(c).can_promote_members()) {
      return promise.set_error(Status::Error(400, "Not enough rights"));
    }

    CHECK(!old_status.is_creator());
    CHECK(!status.is_creator());
  }

  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  speculative_add_channel_user(channel_id, user_id, status, old_status);
  td_->create_handler<EditChannelAdminQuery>(std::move(promise))->send(channel_id, r_input_user.move_as_ok(), status);
}

tl_object_ptr<td_api::chatInviteLinkInfo> ContactsManager::get_chat_invite_link_info_object(const string &invite_link) {
  auto it = invite_link_infos_.find(invite_link);
  if (it == invite_link_infos_.end()) {
    return nullptr;
  }

  auto invite_link_info = it->second.get();
  CHECK(invite_link_info != nullptr);

  DialogId dialog_id = invite_link_info->dialog_id;
  string title;
  const DialogPhoto *photo = nullptr;
  DialogPhoto invite_link_photo;
  string description;
  int32 participant_count = 0;
  vector<int64> member_user_ids;
  bool creates_join_request = false;
  bool is_public = false;
  bool is_member = false;
  td_api::object_ptr<td_api::ChatType> chat_type;

  if (dialog_id.is_valid()) {
    switch (dialog_id.get_type()) {
      case DialogType::Chat: {
        auto chat_id = dialog_id.get_chat_id();
        const Chat *c = get_chat(chat_id);

        if (c != nullptr) {
          title = c->title;
          photo = &c->photo;
          participant_count = c->participant_count;
          is_member = c->status.is_member();
        } else {
          LOG(ERROR) << "Have no information about " << chat_id;
        }
        chat_type = td_api::make_object<td_api::chatTypeBasicGroup>(
            get_basic_group_id_object(chat_id, "get_chat_invite_link_info_object"));
        break;
      }
      case DialogType::Channel: {
        auto channel_id = dialog_id.get_channel_id();
        const Channel *c = get_channel(channel_id);

        bool is_megagroup = false;
        if (c != nullptr) {
          title = c->title;
          photo = &c->photo;
          is_public = is_channel_public(c);
          is_megagroup = c->is_megagroup;
          participant_count = c->participant_count;
          is_member = c->status.is_member();
        } else {
          LOG(ERROR) << "Have no information about " << channel_id;
        }
        chat_type = td_api::make_object<td_api::chatTypeSupergroup>(
            get_supergroup_id_object(channel_id, "get_chat_invite_link_info_object"), !is_megagroup);
        break;
      }
      default:
        UNREACHABLE();
    }
    description = get_dialog_about(dialog_id);
  } else {
    title = invite_link_info->title;
    invite_link_photo = as_fake_dialog_photo(invite_link_info->photo, dialog_id);
    photo = &invite_link_photo;
    description = invite_link_info->description;
    participant_count = invite_link_info->participant_count;
    member_user_ids = get_user_ids_object(invite_link_info->participant_user_ids, "get_chat_invite_link_info_object");
    creates_join_request = invite_link_info->creates_join_request;
    is_public = invite_link_info->is_public;

    if (invite_link_info->is_chat) {
      chat_type = td_api::make_object<td_api::chatTypeBasicGroup>(0);
    } else {
      chat_type = td_api::make_object<td_api::chatTypeSupergroup>(0, !invite_link_info->is_megagroup);
    }
  }

  if (dialog_id.is_valid()) {
    td_->messages_manager_->force_create_dialog(dialog_id, "get_chat_invite_link_info_object");
  }
  int32 accessible_for = 0;
  if (dialog_id.is_valid() && !is_member) {
    auto access_it = dialog_access_by_invite_link_.find(dialog_id);
    if (access_it != dialog_access_by_invite_link_.end()) {
      accessible_for = td::max(1, access_it->second.accessible_before - G()->unix_time() - 1);
    }
  }

  return make_tl_object<td_api::chatInviteLinkInfo>(dialog_id.get(), accessible_for, std::move(chat_type), title,
                                                    get_chat_photo_info_object(td_->file_manager_.get(), photo),
                                                    description, participant_count, std::move(member_user_ids),
                                                    creates_join_request, is_public);
}

class GetUsersQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetUsersQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<tl_object_ptr<telegram_api::InputUser>> &&input_users) {
    send_query(G()->net_query_creator().create(telegram_api::users_getUsers(std::move(input_users))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::users_getUsers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->contacts_manager_->on_get_users(result_ptr.move_as_ok(), "GetUsersQuery");

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_get_me_query(Td *td, Promise<Unit> &&promise) {
  vector<tl_object_ptr<telegram_api::InputUser>> users;
  users.push_back(make_tl_object<telegram_api::inputUserSelf>());
  td->create_handler<GetUsersQuery>(std::move(promise))->send(std::move(users));
}

bool ContactsManager::get_user(UserId user_id, int left_tries, Promise<Unit> &&promise) {
  if (!user_id.is_valid()) {
    promise.set_error(Status::Error(400, "Invalid user identifier"));
    return false;
  }

  if (user_id == get_service_notifications_user_id() || user_id == get_replies_bot_user_id() ||
      user_id == get_anonymous_bot_user_id() || user_id == get_channel_bot_user_id()) {
    get_user_force(user_id);
  }

  // TODO support loading user from database and merging it with min-user in memory
  if (!have_min_user(user_id)) {
    // TODO UserLoader
    if (left_tries > 2 && G()->parameters().use_chat_info_db) {
      send_closure_later(actor_id(this), &ContactsManager::load_user_from_database, nullptr, user_id,
                         std::move(promise));
      return false;
    }
    auto r_input_user = get_input_user(user_id);
    if (left_tries == 1 || r_input_user.is_error()) {
      promise.set_error(r_input_user.move_as_error());
      return false;
    }

    vector<tl_object_ptr<telegram_api::InputUser>> users;
    users.push_back(r_input_user.move_as_ok());
    td_->create_handler<GetUsersQuery>(std::move(promise))->send(std::move(users));
    return false;
  }

  promise.set_value(Unit());
  return true;
}

void ContactsManager::reload_user(UserId user_id, Promise<Unit> &&promise) {
  if (!user_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid user identifier"));
  }

  have_user_force(user_id);
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  // there is no much reason to combine different requests into one request
  vector<tl_object_ptr<telegram_api::InputUser>> users;
  users.push_back(r_input_user.move_as_ok());
  td_->create_handler<GetUsersQuery>(std::move(promise))->send(std::move(users));
}

void ContactsManager::set_profile_photo(const td_api::object_ptr<td_api::InputChatPhoto> &input_photo,
                                        Promise<Unit> &&promise) {
  if (input_photo == nullptr) {
    return promise.set_error(Status::Error(400, "New profile photo must be non-empty"));
  }

  const td_api::object_ptr<td_api::InputFile> *input_file = nullptr;
  double main_frame_timestamp = 0.0;
  bool is_animation = false;
  switch (input_photo->get_id()) {
    case td_api::inputChatPhotoPrevious::ID: {
      auto photo = static_cast<const td_api::inputChatPhotoPrevious *>(input_photo.get());
      auto photo_id = photo->chat_photo_id_;
      auto *u = get_user(get_my_id());
      if (u != nullptr && u->photo.id > 0 && photo_id == u->photo.id) {
        return promise.set_value(Unit());
      }

      auto file_id = get_profile_photo_file_id(photo_id);
      if (!file_id.is_valid()) {
        return promise.set_error(Status::Error(400, "Unknown profile photo ID specified"));
      }
      return send_update_profile_photo_query(td_->file_manager_->dup_file_id(file_id), photo_id, std::move(promise));
    }
    case td_api::inputChatPhotoStatic::ID: {
      auto photo = static_cast<const td_api::inputChatPhotoStatic *>(input_photo.get());
      input_file = &photo->photo_;
      break;
    }
    case td_api::inputChatPhotoAnimation::ID: {
      auto photo = static_cast<const td_api::inputChatPhotoAnimation *>(input_photo.get());
      input_file = &photo->animation_;
      main_frame_timestamp = photo->main_frame_timestamp_;
      is_animation = true;
      break;
    }
    default:
      UNREACHABLE();
      break;
  }

  const double MAX_ANIMATION_DURATION = 10.0;
  if (main_frame_timestamp < 0.0 || main_frame_timestamp > MAX_ANIMATION_DURATION) {
    return promise.set_error(Status::Error(400, "Wrong main frame timestamp specified"));
  }

  auto file_type = is_animation ? FileType::Animation : FileType::Photo;
  auto r_file_id = td_->file_manager_->get_input_file_id(file_type, *input_file, DialogId(get_my_id()), false, false);
  if (r_file_id.is_error()) {
    // TODO promise.set_error(std::move(status));
    return promise.set_error(Status::Error(400, r_file_id.error().message()));
  }
  FileId file_id = r_file_id.ok();
  CHECK(file_id.is_valid());

  upload_profile_photo(td_->file_manager_->dup_file_id(file_id), is_animation, main_frame_timestamp,
                       std::move(promise));
}

class GetGroupsForDiscussionQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetGroupsForDiscussionQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::channels_getGroupsForDiscussion()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getGroupsForDiscussion>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto chats_ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetGroupsForDiscussionQuery: " << to_string(chats_ptr);
    int32 constructor_id = chats_ptr->get_id();
    switch (constructor_id) {
      case telegram_api::messages_chats::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chats>(chats_ptr);
        td_->contacts_manager_->on_get_dialogs_for_discussion(std::move(chats->chats_));
        break;
      }
      case telegram_api::messages_chatsSlice::ID: {
        auto chats = move_tl_object_as<telegram_api::messages_chatsSlice>(chats_ptr);
        LOG(ERROR) << "Receive chatsSlice in result of GetGroupsForDiscussionQuery";
        td_->contacts_manager_->on_get_dialogs_for_discussion(std::move(chats->chats_));
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

vector<DialogId> ContactsManager::get_dialogs_for_discussion(Promise<Unit> &&promise) {
  if (dialogs_for_discussion_inited_) {
    promise.set_value(Unit());
    return transform(dialogs_for_discussion_, [&](DialogId dialog_id) {
      td_->messages_manager_->force_create_dialog(dialog_id, "get_dialogs_for_discussion");
      return dialog_id;
    });
  }

  td_->create_handler<GetGroupsForDiscussionQuery>(std::move(promise))->send();
  return {};
}

void ContactsManager::get_channel_statistics_dc_id(DialogId dialog_id, bool for_full_statistics,
                                                   Promise<DcId> &&promise) {
  if (!dialog_id.is_valid()) {
    return promise.set_error(Status::Error(400, "Invalid chat identifier specified"));
  }
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "get_channel_statistics_dc_id")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (dialog_id.get_type() != DialogType::Channel) {
    return promise.set_error(Status::Error(400, "Chat is not a channel"));
  }

  auto channel_id = dialog_id.get_channel_id();
  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }

  auto channel_full = get_channel_full_force(channel_id, false, "get_channel_statistics_dc_id");
  if (channel_full == nullptr || !channel_full->stats_dc_id.is_exact() ||
      (for_full_statistics && !channel_full->can_view_statistics)) {
    auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), channel_id, for_full_statistics,
                                                 promise = std::move(promise)](Result<Unit> result) mutable {
      send_closure(actor_id, &ContactsManager::get_channel_statistics_dc_id_impl, channel_id, for_full_statistics,
                   std::move(promise));
    });
    send_get_channel_full_query(channel_full, channel_id, std::move(query_promise), "get_channel_statistics_dc_id");
    return;
  }

  promise.set_value(DcId(channel_full->stats_dc_id));
}

void ContactsManager::update_created_public_channels(Channel *c, ChannelId channel_id) {
  if (created_public_channels_inited_[0]) {
    bool was_changed = false;
    if (c->username.empty() || !c->status.is_creator()) {
      was_changed = td::remove(created_public_channels_[0], channel_id);
    } else {
      if (!td::contains(created_public_channels_[0], channel_id)) {
        created_public_channels_[0].push_back(channel_id);
        was_changed = true;
      }
    }
    if (was_changed) {
      if (!c->is_megagroup) {
        update_created_public_broadcasts();
      }

      save_created_public_channels(PublicDialogType::HasUsername);

      reload_created_public_dialogs(PublicDialogType::HasUsername, Promise<td_api::object_ptr<td_api::chats>>());
    }
  }
  if (created_public_channels_inited_[1]) {
    bool was_changed = false;
    if (!c->has_location || !c->status.is_creator()) {
      was_changed = td::remove(created_public_channels_[1], channel_id);
    } else {
      if (!td::contains(created_public_channels_[1], channel_id)) {
        created_public_channels_[1].push_back(channel_id);
        was_changed = true;
      }
    }
    if (was_changed) {
      save_created_public_channels(PublicDialogType::IsLocationBased);

      reload_created_public_dialogs(PublicDialogType::IsLocationBased, Promise<td_api::object_ptr<td_api::chats>>());
    }
  }
}

class GetContactsQuery final : public Td::ResultHandler {
 public:
  void send(int64 hash) {
    send_query(G()->net_query_creator().create(telegram_api::contacts_getContacts(hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_getContacts>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetContactsQuery: " << to_string(ptr);
    td_->contacts_manager_->on_get_contacts(std::move(ptr));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_contacts_failed(std::move(status));
    td_->updates_manager_->get_difference("GetContactsQuery");
  }
};

void ContactsManager::reload_contacts(bool force) {
  if (!td_->auth_manager_->is_bot() && next_contacts_sync_date_ != std::numeric_limits<int32>::max() &&
      (next_contacts_sync_date_ < G()->unix_time() || force)) {
    next_contacts_sync_date_ = std::numeric_limits<int32>::max();
    td_->create_handler<GetContactsQuery>()->send(get_contacts_hash());
  }
}

void ContactsManager::on_update_chat_invite_requester(DialogId dialog_id, UserId user_id, string about, int32 date,
                                                      DialogInviteLink invite_link) {
  if (!td_->auth_manager_->is_bot() || date <= 0 || !have_user_force(user_id) ||
      !td_->messages_manager_->have_dialog_info_force(dialog_id)) {
    LOG(ERROR) << "Receive invalid updateBotChatInviteRequester by " << user_id << " in " << dialog_id << " at "
               << date;
    return;
  }
  td_->messages_manager_->force_create_dialog(dialog_id, "on_update_chat_invite_requester", true);

  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateNewChatJoinRequest>(
                   dialog_id.get(),
                   td_api::make_object<td_api::chatJoinRequest>(
                       get_user_id_object(user_id, "on_update_chat_invite_requester"), date, about),
                   invite_link.get_chat_invite_link_object(this)));
}

void ContactsManager::drop_user_full(UserId user_id) {
  auto user_full = get_user_full_force(user_id);

  drop_user_photos(user_id, false, false, "drop_user_full");

  if (user_full == nullptr) {
    return;
  }

  user_full->expires_at = 0.0;

  user_full->photo = Photo();
  user_full->is_blocked = false;
  user_full->can_be_called = false;
  user_full->supports_video_calls = false;
  user_full->has_private_calls = false;
  user_full->need_phone_number_privacy_exception = false;
  user_full->about = string();
  user_full->description = string();
  user_full->commands.clear();
  user_full->common_chat_count = 0;
  user_full->private_forward_name.clear();
  user_full->is_changed = true;

  update_user_full(user_full, user_id, "drop_user_full");
  td_->group_call_manager_->on_update_dialog_about(DialogId(user_id), user_full->about, true);
}

void ContactsManager::on_update_user_name(User *u, UserId user_id, string &&first_name, string &&last_name,
                                          string &&username) {
  if (first_name.empty() && last_name.empty()) {
    first_name = u->phone_number;
  }
  if (u->first_name != first_name || u->last_name != last_name) {
    u->first_name = std::move(first_name);
    u->last_name = std::move(last_name);
    u->is_name_changed = true;
    LOG(DEBUG) << "Name has changed for " << user_id;
    u->is_changed = true;
  }
  td_->messages_manager_->on_dialog_username_updated(DialogId(user_id), u->username, username);
  if (u->username != username) {
    u->username = std::move(username);
    u->is_username_changed = true;
    LOG(DEBUG) << "Username has changed for " << user_id;
    u->is_changed = true;
  }
}

void ContactsManager::do_update_user_photo(User *u, UserId user_id, ProfilePhoto &&new_photo,
                                           bool invalidate_photo_cache, const char *source) {
  u->is_photo_inited = true;
  if (new_photo != u->photo) {
    LOG_IF(ERROR, u->access_hash == -1 && new_photo.small_file_id.is_valid())
        << "Update profile photo of " << user_id << " without access hash from " << source;
    u->photo = new_photo;
    u->is_photo_changed = true;
    LOG(DEBUG) << "Photo has changed for " << user_id;
    u->is_changed = true;

    if (invalidate_photo_cache) {
      drop_user_photos(user_id, u->photo.id <= 0, true, "do_update_user_photo");
    }
  }
}

Result<tl_object_ptr<telegram_api::InputUser>> ContactsManager::get_input_user(UserId user_id) const {
  if (user_id == get_my_id()) {
    return make_tl_object<telegram_api::inputUserSelf>();
  }

  const User *u = get_user(user_id);
  if (u == nullptr) {
    return Status::Error(400, "User not found");
  }
  if (u->access_hash == -1 || u->is_min_access_hash) {
    if (td_->auth_manager_->is_bot() && user_id.is_valid()) {
      return make_tl_object<telegram_api::inputUser>(user_id.get(), 0);
    }
    return Status::Error(400, "Have no access to the user");
  }

  return make_tl_object<telegram_api::inputUser>(user_id.get(), u->access_hash);
}

void ContactsManager::on_update_channel_slow_mode_delay(ChannelId channel_id, int32 slow_mode_delay,
                                                        Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_slow_mode_delay");
  if (channel_full != nullptr) {
    on_update_channel_full_slow_mode_delay(channel_full, channel_id, slow_mode_delay, 0);
    update_channel_full(channel_full, channel_id, "on_update_channel_slow_mode_delay");
  }
  promise.set_value(Unit());
}

void ContactsManager::on_update_user_common_chat_count(UserId user_id, int32 common_chat_count) {
  LOG(INFO) << "Receive " << common_chat_count << " common chat count with " << user_id;
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  UserFull *user_full = get_user_full_force(user_id);
  if (user_full == nullptr) {
    return;
  }
  on_update_user_full_common_chat_count(user_full, user_id, common_chat_count);
  update_user_full(user_full, user_id, "on_update_user_common_chat_count");
}

void ContactsManager::on_user_online_timeout(UserId user_id) {
  if (G()->close_flag()) {
    return;
  }

  auto u = get_user(user_id);
  CHECK(u != nullptr);
  CHECK(u->is_update_user_sent);

  LOG(INFO) << "Update " << user_id << " online status to offline";
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateUserStatus>(user_id.get(), get_user_status_object(user_id, u)));

  update_user_online_member_count(u);
}

void ContactsManager::remove_dialog_access_by_invite_link(DialogId dialog_id) {
  auto access_it = dialog_access_by_invite_link_.find(dialog_id);
  if (access_it == dialog_access_by_invite_link_.end()) {
    return;
  }

  for (auto &invite_link : access_it->second.invite_links) {
    invalidate_invite_link_info(invite_link);
  }
  dialog_access_by_invite_link_.erase(access_it);

  invite_link_info_expire_timeout_.cancel_timeout(dialog_id.get());
}

void ContactsManager::on_user_nearby_timeout_callback(void *contacts_manager_ptr, int64 user_id_long) {
  if (G()->close_flag()) {
    return;
  }

  auto contacts_manager = static_cast<ContactsManager *>(contacts_manager_ptr);
  send_closure_later(contacts_manager->actor_id(contacts_manager), &ContactsManager::on_user_nearby_timeout,
                     UserId(user_id_long));
}

ContactsManager::MyOnlineStatusInfo ContactsManager::get_my_online_status() const {
  MyOnlineStatusInfo status_info;
  status_info.is_online_local = td_->is_online();
  status_info.is_online_remote = was_online_remote_ > G()->unix_time_cached();
  status_info.was_online_local = was_online_local_;
  status_info.was_online_remote = was_online_remote_;

  return status_info;
}

void ContactsManager::load_user_from_database(User *u, UserId user_id, Promise<Unit> promise) {
  if (loaded_from_database_users_.count(user_id)) {
    promise.set_value(Unit());
    return;
  }

  CHECK(u == nullptr || !u->is_being_saved);
  load_user_from_database_impl(user_id, std::move(promise));
}

RestrictedRights ContactsManager::get_channel_default_permissions(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return RestrictedRights(false, false, false, false, false, false, false, false, false, false, false);
  }
  return c->default_permissions;
}

void ContactsManager::on_get_inactive_channels(vector<tl_object_ptr<telegram_api::Chat>> &&chats) {
  inactive_channels_inited_ = true;
  inactive_channels_ = get_channel_ids(std::move(chats), "on_get_inactive_channels");
}

UserId ContactsManager::add_anonymous_bot_user() {
  auto user_id = get_anonymous_bot_user_id();
  if (!have_user_force(user_id)) {
    LOG(FATAL) << "Failed to load anonymous bot user";
  }
  return user_id;
}

bool ContactsManager::can_report_user(UserId user_id) const {
  auto u = get_user(user_id);
  return u != nullptr && !u->is_deleted && !u->is_support && (u->is_bot || all_users_nearby_.count(user_id) != 0);
}

tl_object_ptr<td_api::userFullInfo> ContactsManager::get_user_full_info_object(UserId user_id) const {
  return get_user_full_info_object(user_id, get_user_full(user_id));
}

int32 ContactsManager::get_channel_date(ChannelId channel_id) const {
  auto c = get_channel(channel_id);
  if (c == nullptr) {
    return 0;
  }
  return c->date;
}

bool ContactsManager::have_user_force(UserId user_id) {
  return get_user_force(user_id) != nullptr;
}

bool ContactsManager::have_chat_force(ChatId chat_id) {
  return get_chat_force(chat_id) != nullptr;
}
}  // namespace td
