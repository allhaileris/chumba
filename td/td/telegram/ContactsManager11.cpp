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
#include "td/telegram/SecretChatsManager.h"
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




#include <utility>

namespace td {
}  // namespace td
namespace td {

class JoinChannelQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit JoinChannelQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::channels_joinChannel(std::move(input_channel))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_joinChannel>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for JoinChannelQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "JoinChannelQuery");
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("JoinChannelQuery");
  }
};

class InviteToChannelQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit InviteToChannelQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, vector<tl_object_ptr<telegram_api::InputUser>> &&input_users) {
    channel_id_ = channel_id;
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    CHECK(input_channel != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::channels_inviteToChannel(std::move(input_channel), std::move(input_users))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_inviteToChannel>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for InviteToChannelQuery: " << to_string(ptr);
    td_->contacts_manager_->invalidate_channel_full(channel_id_, false);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "InviteToChannelQuery");
    promise_.set_error(std::move(status));
    td_->updates_manager_->get_difference("InviteToChannelQuery");
  }
};

void ContactsManager::add_channel_participant(ChannelId channel_id, UserId user_id,
                                              const DialogParticipantStatus &old_status, Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Bots can't add new chat members"));
  }

  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }
  auto r_input_user = get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }

  if (user_id == get_my_id()) {
    // join the channel
    if (get_channel_status(c).is_banned()) {
      return promise.set_error(Status::Error(400, "Can't return to kicked from chat"));
    }

    speculative_add_channel_user(channel_id, user_id, DialogParticipantStatus::Member(), c->status);
    td_->create_handler<JoinChannelQuery>(std::move(promise))->send(channel_id);
    return;
  }

  if (!get_channel_permissions(c).can_invite_users()) {
    return promise.set_error(Status::Error(400, "Not enough rights to invite members to the supergroup chat"));
  }

  speculative_add_channel_user(channel_id, user_id, DialogParticipantStatus::Member(), old_status);
  vector<tl_object_ptr<telegram_api::InputUser>> input_users;
  input_users.push_back(r_input_user.move_as_ok());
  td_->create_handler<InviteToChannelQuery>(std::move(promise))->send(channel_id, std::move(input_users));
}

void ContactsManager::add_channel_participants(ChannelId channel_id, const vector<UserId> &user_ids,
                                               Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Bots can't add new chat members"));
  }

  const Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }

  if (!get_channel_permissions(c).can_invite_users()) {
    return promise.set_error(Status::Error(400, "Not enough rights to invite members to the supergroup chat"));
  }

  vector<tl_object_ptr<telegram_api::InputUser>> input_users;
  for (auto user_id : user_ids) {
    auto r_input_user = get_input_user(user_id);
    if (r_input_user.is_error()) {
      return promise.set_error(r_input_user.move_as_error());
    }

    if (user_id == get_my_id()) {
      // can't invite self
      continue;
    }
    input_users.push_back(r_input_user.move_as_ok());
  }

  if (input_users.empty()) {
    return promise.set_value(Unit());
  }

  td_->create_handler<InviteToChannelQuery>(std::move(promise))->send(channel_id, std::move(input_users));
}

class UploadProfilePhotoQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;

 public:
  explicit UploadProfilePhotoQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FileId file_id, tl_object_ptr<telegram_api::InputFile> &&input_file, bool is_animation,
            double main_frame_timestamp) {
    CHECK(input_file != nullptr);
    CHECK(file_id.is_valid());

    file_id_ = file_id;

    int32 flags = 0;
    tl_object_ptr<telegram_api::InputFile> photo_input_file;
    tl_object_ptr<telegram_api::InputFile> video_input_file;
    if (is_animation) {
      flags |= telegram_api::photos_uploadProfilePhoto::VIDEO_MASK;
      video_input_file = std::move(input_file);

      if (main_frame_timestamp != 0.0) {
        flags |= telegram_api::photos_uploadProfilePhoto::VIDEO_START_TS_MASK;
      }
    } else {
      flags |= telegram_api::photos_uploadProfilePhoto::FILE_MASK;
      photo_input_file = std::move(input_file);
    }
    send_query(G()->net_query_creator().create(telegram_api::photos_uploadProfilePhoto(
        flags, std::move(photo_input_file), std::move(video_input_file), main_frame_timestamp)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::photos_uploadProfilePhoto>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->contacts_manager_->on_set_profile_photo(result_ptr.move_as_ok(), 0);

    td_->file_manager_->delete_partial_remote_location(file_id_);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->file_manager_->delete_partial_remote_location(file_id_);
    td_->updates_manager_->get_difference("UploadProfilePhotoQuery");
  }
};

void ContactsManager::on_upload_profile_photo(FileId file_id, tl_object_ptr<telegram_api::InputFile> input_file) {
  auto it = uploaded_profile_photos_.find(file_id);
  CHECK(it != uploaded_profile_photos_.end());

  double main_frame_timestamp = it->second.main_frame_timestamp;
  bool is_animation = it->second.is_animation;
  int32 reupload_count = it->second.reupload_count;
  auto promise = std::move(it->second.promise);

  uploaded_profile_photos_.erase(it);

  LOG(INFO) << "Uploaded " << (is_animation ? "animated" : "static") << " profile photo " << file_id
            << " with reupload_count = " << reupload_count;
  FileView file_view = td_->file_manager_->get_file_view(file_id);
  if (file_view.has_remote_location() && input_file == nullptr) {
    if (file_view.main_remote_location().is_web()) {
      return promise.set_error(Status::Error(400, "Can't use web photo as profile photo"));
    }
    if (reupload_count == 3) {  // upload, ForceReupload repair file reference, reupload
      return promise.set_error(Status::Error(400, "Failed to reupload the file"));
    }

    // delete file reference and forcely reupload the file
    if (is_animation) {
      CHECK(file_view.get_type() == FileType::Animation);
      LOG_CHECK(file_view.main_remote_location().is_common()) << file_view.main_remote_location();
    } else {
      CHECK(file_view.get_type() == FileType::Photo);
      LOG_CHECK(file_view.main_remote_location().is_photo()) << file_view.main_remote_location();
    }
    auto file_reference =
        is_animation ? FileManager::extract_file_reference(file_view.main_remote_location().as_input_document())
                     : FileManager::extract_file_reference(file_view.main_remote_location().as_input_photo());
    td_->file_manager_->delete_file_reference(file_id, file_reference);
    upload_profile_photo(file_id, is_animation, main_frame_timestamp, std::move(promise), reupload_count + 1, {-1});
    return;
  }
  CHECK(input_file != nullptr);

  td_->create_handler<UploadProfilePhotoQuery>(std::move(promise))
      ->send(file_id, std::move(input_file), is_animation, main_frame_timestamp);
}

void ContactsManager::on_update_chat_add_user(ChatId chat_id, UserId inviter_user_id, UserId user_id, int32 date,
                                              int32 version) {
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id;
    return;
  }
  if (!have_user(user_id)) {
    LOG(ERROR) << "Can't find " << user_id;
    return;
  }
  if (!have_user(inviter_user_id)) {
    LOG(ERROR) << "Can't find " << inviter_user_id;
    return;
  }
  LOG(INFO) << "Receive updateChatParticipantAdd to " << chat_id << " with " << user_id << " invited by "
            << inviter_user_id << " at " << date << " with version " << version;

  ChatFull *chat_full = get_chat_full_force(chat_id, "on_update_chat_add_user");
  if (chat_full == nullptr) {
    LOG(INFO) << "Ignoring update about members of " << chat_id;
    return;
  }
  auto c = get_chat(chat_id);
  if (c == nullptr) {
    LOG(ERROR) << "Receive updateChatParticipantAdd for unknown " << chat_id << ". Couldn't apply it";
    repair_chat_participants(chat_id);
    return;
  }
  if (c->status.is_left()) {
    // possible if updates come out of order
    LOG(WARNING) << "Receive updateChatParticipantAdd for left " << chat_id << ". Couldn't apply it";

    repair_chat_participants(chat_id);  // just in case
    return;
  }
  if (on_update_chat_full_participants_short(chat_full, chat_id, version)) {
    for (auto &participant : chat_full->participants) {
      if (participant.dialog_id_ == DialogId(user_id)) {
        if (participant.inviter_user_id_ != inviter_user_id) {
          LOG(ERROR) << user_id << " was readded to " << chat_id << " by " << inviter_user_id
                     << ", previously invited by " << participant.inviter_user_id_;
          participant.inviter_user_id_ = inviter_user_id;
          participant.joined_date_ = date;
          repair_chat_participants(chat_id);
        } else {
          // Possible if update comes twice
          LOG(INFO) << user_id << " was readded to " << chat_id;
        }
        return;
      }
    }
    chat_full->participants.push_back(DialogParticipant{DialogId(user_id), inviter_user_id, date,
                                                        user_id == chat_full->creator_user_id
                                                            ? DialogParticipantStatus::Creator(true, false, string())
                                                            : DialogParticipantStatus::Member()});
    update_chat_online_member_count(chat_full, chat_id, false);
    chat_full->is_changed = true;
    update_chat_full(chat_full, chat_id, "on_update_chat_add_user");

    // Chat is already updated
    if (chat_full->version == c->version &&
        narrow_cast<int32>(chat_full->participants.size()) != c->participant_count) {
      LOG(ERROR) << "Number of members in " << chat_id << " with version " << c->version << " is "
                 << c->participant_count << " but there are " << chat_full->participants.size()
                 << " members in the ChatFull";
      repair_chat_participants(chat_id);
    }
  }
}

void ContactsManager::on_update_chat_edit_administrator(ChatId chat_id, UserId user_id, bool is_administrator,
                                                        int32 version) {
  if (!chat_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << chat_id;
    return;
  }
  if (!have_user(user_id)) {
    LOG(ERROR) << "Can't find " << user_id;
    return;
  }
  LOG(INFO) << "Receive updateChatParticipantAdmin in " << chat_id << " with " << user_id << ", administrator rights "
            << (is_administrator ? "enabled" : "disabled") << " with version " << version;

  auto c = get_chat_force(chat_id);
  if (c == nullptr) {
    LOG(INFO) << "Ignoring update about members of unknown " << chat_id;
    return;
  }

  if (c->status.is_left()) {
    // possible if updates come out of order
    LOG(WARNING) << "Receive updateChatParticipantAdmin for left " << chat_id << ". Couldn't apply it";

    repair_chat_participants(chat_id);  // just in case
    return;
  }
  if (version <= -1) {
    LOG(ERROR) << "Receive wrong version " << version << " for " << chat_id;
    return;
  }
  CHECK(c->version >= 0);

  auto status = is_administrator ? DialogParticipantStatus::GroupAdministrator(c->status.is_creator())
                                 : DialogParticipantStatus::Member();
  if (version > c->version) {
    if (version != c->version + 1) {
      LOG(INFO) << "Administrators of " << chat_id << " with version " << c->version
                << " has changed, but new version is " << version;
      repair_chat_participants(chat_id);
      return;
    }

    c->version = version;
    c->need_save_to_database = true;
    if (user_id == get_my_id() && !c->status.is_creator()) {
      // if chat with version was already received, then the update is already processed
      // so we need to call on_update_chat_status only if version > c->version
      on_update_chat_status(c, chat_id, status);
    }
    update_chat(c, chat_id);
  }

  ChatFull *chat_full = get_chat_full_force(chat_id, "on_update_chat_edit_administrator");
  if (chat_full != nullptr) {
    if (chat_full->version + 1 == version) {
      for (auto &participant : chat_full->participants) {
        if (participant.dialog_id_ == DialogId(user_id)) {
          participant.status_ = std::move(status);
          chat_full->is_changed = true;
          update_chat_full(chat_full, chat_id, "on_update_chat_edit_administrator");
          return;
        }
      }
    }

    // can't find chat member or version have increased too much
    repair_chat_participants(chat_id);
  }
}

void ContactsManager::get_current_state(vector<td_api::object_ptr<td_api::Update>> &updates) const {
  for (auto user_id : unknown_users_) {
    if (!have_min_user(user_id)) {
      updates.push_back(get_update_unknown_user_object(user_id));
    }
  }
  for (auto chat_id : unknown_chats_) {
    if (!have_chat(chat_id)) {
      updates.push_back(get_update_unknown_basic_group_object(chat_id));
    }
  }
  for (auto channel_id : unknown_channels_) {
    if (!have_channel(channel_id)) {
      updates.push_back(get_update_unknown_supergroup_object(channel_id));
    }
  }
  for (auto secret_chat_id : unknown_secret_chats_) {
    if (!have_secret_chat(secret_chat_id)) {
      updates.push_back(get_update_unknown_secret_chat_object(secret_chat_id));
    }
  }

  for (auto &it : users_) {
    updates.push_back(td_api::make_object<td_api::updateUser>(get_user_object(it.first, it.second.get())));
  }
  for (auto &it : channels_) {
    updates.push_back(td_api::make_object<td_api::updateSupergroup>(get_supergroup_object(it.first, it.second.get())));
  }
  for (auto &it : chats_) {  // chat object can contain channel_id, so it must be sent after channels
    updates.push_back(
        td_api::make_object<td_api::updateBasicGroup>(get_basic_group_object_const(it.first, it.second.get())));
  }
  for (auto &it : secret_chats_) {  // secret chat object contains user_id, so it must be sent after users
    updates.push_back(
        td_api::make_object<td_api::updateSecretChat>(get_secret_chat_object_const(it.first, it.second.get())));
  }

  for (auto &it : users_full_) {
    updates.push_back(td_api::make_object<td_api::updateUserFullInfo>(
        it.first.get(), get_user_full_info_object(it.first, it.second.get())));
  }
  for (auto &it : channels_full_) {
    updates.push_back(td_api::make_object<td_api::updateSupergroupFullInfo>(
        it.first.get(), get_supergroup_full_info_object(it.second.get(), it.first)));
  }
  for (auto &it : chats_full_) {
    updates.push_back(td_api::make_object<td_api::updateBasicGroupFullInfo>(
        it.first.get(), get_basic_group_full_info_object(it.second.get())));
  }
}

class DeleteRevokedExportedChatInvitesQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit DeleteRevokedExportedChatInvitesQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, UserId creator_user_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    auto r_input_user = td_->contacts_manager_->get_input_user(creator_user_id);
    CHECK(r_input_user.is_ok());

    send_query(G()->net_query_creator().create(
        telegram_api::messages_deleteRevokedExportedChatInvites(std::move(input_peer), r_input_user.move_as_ok())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deleteRevokedExportedChatInvites>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "DeleteRevokedExportedChatInvitesQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::delete_all_revoked_dialog_invite_links(DialogId dialog_id, UserId creator_user_id,
                                                             Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id, creator_user_id != get_my_id()));

  if (!have_input_user(creator_user_id)) {
    return promise.set_error(Status::Error(400, "Administrator user not found"));
  }

  td_->create_handler<DeleteRevokedExportedChatInvitesQuery>(std::move(promise))->send(dialog_id, creator_user_id);
}

void ContactsManager::register_user_photo(User *u, UserId user_id, const Photo &photo) {
  auto photo_file_ids = photo_get_file_ids(photo);
  if (photo.is_empty() || photo_file_ids.empty()) {
    return;
  }
  auto first_file_id = photo_file_ids[0];
  auto file_type = td_->file_manager_->get_file_view(first_file_id).get_type();
  if (file_type == FileType::ProfilePhoto) {
    return;
  }
  CHECK(file_type == FileType::Photo);
  CHECK(u != nullptr);
  auto photo_id = photo.id.get();
  if (u->photo_ids.emplace(photo_id).second) {
    VLOG(file_references) << "Register photo " << photo_id << " of " << user_id;
    if (user_id == get_my_id()) {
      my_photo_file_id_[photo_id] = first_file_id;
    }
    FileSourceId file_source_id;
    auto it = user_profile_photo_file_source_ids_.find(std::make_pair(user_id, photo_id));
    if (it != user_profile_photo_file_source_ids_.end()) {
      VLOG(file_references) << "Move " << it->second << " inside of " << user_id;
      file_source_id = it->second;
      user_profile_photo_file_source_ids_.erase(it);
    } else {
      VLOG(file_references) << "Need to create new file source for photo " << photo_id << " of " << user_id;
      file_source_id = td_->file_reference_manager_->create_user_photo_file_source(user_id, photo_id);
    }
    for (auto &file_id : photo_file_ids) {
      td_->file_manager_->add_file_source(file_id, file_source_id);
    }
  }
}

void ContactsManager::get_chat_participant(ChatId chat_id, UserId user_id, Promise<DialogParticipant> &&promise) {
  LOG(INFO) << "Trying to get " << user_id << " as member of " << chat_id;

  auto c = get_chat(chat_id);
  if (c == nullptr) {
    return promise.set_error(Status::Error(400, "Group not found"));
  }

  auto chat_full = get_chat_full_force(chat_id, "get_chat_participant");
  if (chat_full == nullptr || (td_->auth_manager_->is_bot() && is_chat_full_outdated(chat_full, c, chat_id))) {
    auto query_promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), chat_id, user_id, promise = std::move(promise)](Result<Unit> &&result) mutable {
          TRY_STATUS_PROMISE(promise, std::move(result));
          send_closure(actor_id, &ContactsManager::finish_get_chat_participant, chat_id, user_id, std::move(promise));
        });
    send_get_chat_full_query(chat_id, std::move(query_promise), "get_chat_participant");
    return;
  }

  if (is_chat_full_outdated(chat_full, c, chat_id)) {
    send_get_chat_full_query(chat_id, Auto(), "get_chat_participant lazy");
  }

  finish_get_chat_participant(chat_id, user_id, std::move(promise));
}

string ContactsManager::get_dialog_about(DialogId dialog_id) {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      auto user_full = get_user_full_force(dialog_id.get_user_id());
      if (user_full != nullptr) {
        return user_full->about;
      }
      break;
    }
    case DialogType::Chat: {
      auto chat_full = get_chat_full_force(dialog_id.get_chat_id(), "get_dialog_about");
      if (chat_full != nullptr) {
        return chat_full->description;
      }
      break;
    }
    case DialogType::Channel: {
      auto channel_full = get_channel_full_force(dialog_id.get_channel_id(), false, "get_dialog_about");
      if (channel_full != nullptr) {
        return channel_full->description;
      }
      break;
    }
    case DialogType::SecretChat: {
      auto user_full = get_user_full_force(get_secret_chat_user_id(dialog_id.get_secret_chat_id()));
      if (user_full != nullptr) {
        return user_full->about;
      }
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  return string();
}

void ContactsManager::delete_dialog(DialogId dialog_id, Promise<Unit> &&promise) {
  if (!td_->messages_manager_->have_dialog_force(dialog_id, "delete_dialog")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->messages_manager_->delete_dialog_history(dialog_id, true, true, std::move(promise));
    case DialogType::Chat:
      return delete_chat(dialog_id.get_chat_id(), std::move(promise));
    case DialogType::Channel:
      return delete_channel(dialog_id.get_channel_id(), std::move(promise));
    case DialogType::SecretChat:
      send_closure(td_->secret_chats_manager_, &SecretChatsManager::cancel_chat, dialog_id.get_secret_chat_id(), true,
                   std::move(promise));
      return;
    default:
      UNREACHABLE();
  }
}

void ContactsManager::on_update_user_need_phone_number_privacy_exception(UserId user_id,
                                                                         bool need_phone_number_privacy_exception) {
  LOG(INFO) << "Receive " << need_phone_number_privacy_exception << " need phone number privacy exception with "
            << user_id;
  if (!user_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << user_id;
    return;
  }

  UserFull *user_full = get_user_full_force(user_id);
  if (user_full == nullptr) {
    return;
  }
  on_update_user_full_need_phone_number_privacy_exception(user_full, user_id, need_phone_number_privacy_exception);
  update_user_full(user_full, user_id, "on_update_user_need_phone_number_privacy_exception");
}

void ContactsManager::on_channel_unban_timeout(ChannelId channel_id) {
  if (G()->close_flag()) {
    return;
  }

  auto c = get_channel(channel_id);
  CHECK(c != nullptr);

  auto old_status = c->status;
  c->status.update_restrictions();
  if (c->status == old_status) {
    LOG_IF(ERROR, c->status.is_restricted() || c->status.is_banned())
        << "Status of " << channel_id << " wasn't updated: " << c->status;
  } else {
    c->is_changed = true;
  }

  LOG(INFO) << "Update " << channel_id << " status";
  c->is_status_changed = true;
  invalidate_channel_full(channel_id, !c->is_slow_mode_enabled);
  update_channel(c, channel_id);  // always call, because in case of failure we need to reactivate timeout
}

FileSourceId ContactsManager::get_channel_full_file_source_id(ChannelId channel_id) {
  if (get_channel_full(channel_id) != nullptr) {
    VLOG(file_references) << "Don't need to create file source for full " << channel_id;
    // channel full was already added, source ID was registered and shouldn't be needed
    return FileSourceId();
  }

  auto &source_id = channel_full_file_source_ids_[channel_id];
  if (!source_id.is_valid()) {
    source_id = td_->file_reference_manager_->create_channel_full_file_source(channel_id);
  }
  VLOG(file_references) << "Return " << source_id << " for full " << channel_id;
  return source_id;
}

void ContactsManager::set_my_id(UserId my_id) {
  UserId my_old_id = my_id_;
  if (my_old_id.is_valid() && my_old_id != my_id) {
    LOG(ERROR) << "Already know that me is " << my_old_id << " but received userSelf with " << my_id;
  }
  if (!my_id.is_valid()) {
    LOG(ERROR) << "Receive invalid my ID " << my_id;
    return;
  }
  if (my_old_id != my_id) {
    my_id_ = my_id;
    G()->td_db()->get_binlog_pmc()->set("my_id", to_string(my_id.get()));
    G()->shared_config().set_option_integer("my_id", my_id_.get());
    G()->td_db()->get_binlog_pmc()->force_sync(Promise<Unit>());
  }
}

void ContactsManager::on_update_user_full_commands(UserFull *user_full, UserId user_id,
                                                   vector<tl_object_ptr<telegram_api::botCommand>> &&bot_commands) {
  CHECK(user_full != nullptr);
  auto commands = transform(std::move(bot_commands), [](tl_object_ptr<telegram_api::botCommand> &&bot_command) {
    return BotCommand(std::move(bot_command));
  });
  if (user_full->commands != commands) {
    user_full->commands = std::move(commands);
    user_full->is_changed = true;
  }
}

void ContactsManager::on_user_nearby_timeout(UserId user_id) {
  if (G()->close_flag()) {
    return;
  }

  auto u = get_user(user_id);
  CHECK(u != nullptr);

  LOG(INFO) << "Remove " << user_id << " from nearby list";
  DialogId dialog_id(user_id);
  for (size_t i = 0; i < users_nearby_.size(); i++) {
    if (users_nearby_[i].dialog_id == dialog_id) {
      users_nearby_.erase(users_nearby_.begin() + i);
      send_update_users_nearby();
      return;
    }
  }
}

td_api::object_ptr<td_api::chatAdministrators> ContactsManager::get_chat_administrators_object(
    const vector<DialogAdministrator> &dialog_administrators) {
  auto administrator_objects = transform(dialog_administrators, [this](const DialogAdministrator &administrator) {
    return administrator.get_chat_administrator_object(this);
  });
  return td_api::make_object<td_api::chatAdministrators>(std::move(administrator_objects));
}

bool ContactsManager::have_input_peer_user(const User *u, AccessRights access_rights) {
  if (u == nullptr) {
    return false;
  }
  if (u->access_hash == -1 || u->is_min_access_hash) {
    return false;
  }
  if (access_rights == AccessRights::Know) {
    return true;
  }
  if (access_rights == AccessRights::Read) {
    return true;
  }
  if (u->is_deleted) {
    return false;
  }
  return true;
}

tl_object_ptr<td_api::basicGroup> ContactsManager::get_basic_group_object_const(ChatId chat_id, const Chat *c) const {
  return make_tl_object<td_api::basicGroup>(
      chat_id.get(), c->participant_count, get_chat_status(c).get_chat_member_status_object(), c->is_active,
      get_supergroup_id_object(c->migrated_to_channel_id, "get_basic_group_object"));
}

void ContactsManager::load_channel_from_database(Channel *c, ChannelId channel_id, Promise<Unit> promise) {
  if (loaded_from_database_channels_.count(channel_id)) {
    promise.set_value(Unit());
    return;
  }

  CHECK(c == nullptr || !c->is_being_saved);
  load_channel_from_database_impl(channel_id, std::move(promise));
}

void ContactsManager::on_get_chat(tl_object_ptr<telegram_api::Chat> &&chat, const char *source) {
  LOG(DEBUG) << "Receive from " << source << ' ' << to_string(chat);
  downcast_call(*chat, [this, source](auto &c) { this->on_chat_update(c, source); });
}

void ContactsManager::on_get_dialogs_for_discussion(vector<tl_object_ptr<telegram_api::Chat>> &&chats) {
  dialogs_for_discussion_inited_ = true;
  dialogs_for_discussion_ = get_dialog_ids(std::move(chats), "on_get_dialogs_for_discussion");
}

vector<int64> ContactsManager::get_user_ids_object(const vector<UserId> &user_ids, const char *source) const {
  return transform(user_ids, [this, source](UserId user_id) { return get_user_id_object(user_id, source); });
}

UserId ContactsManager::get_secret_chat_user_id(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return UserId();
  }
  return c->user_id;
}

int32 ContactsManager::get_secret_chat_layer(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return 0;
  }
  return c->layer;
}

UserId ContactsManager::get_my_id() const {
  LOG_IF(ERROR, !my_id_.is_valid()) << "Wrong or unknown my ID returned";
  return my_id_;
}

bool ContactsManager::is_user_deleted(UserId user_id) const {
  auto u = get_user(user_id);
  return u == nullptr || u->is_deleted;
}
}  // namespace td
