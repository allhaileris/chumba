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

class GetChannelAdministratorsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  ChannelId channel_id_;

 public:
  explicit GetChannelAdministratorsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(ChannelId channel_id, int64 hash) {
    auto input_channel = td_->contacts_manager_->get_input_channel(channel_id);
    if (input_channel == nullptr) {
      return promise_.set_error(Status::Error(400, "Supergroup not found"));
    }

    hash = 0;  // to load even only ranks or creator changed

    channel_id_ = channel_id;
    send_query(G()->net_query_creator().create(telegram_api::channels_getParticipants(
        std::move(input_channel), telegram_api::make_object<telegram_api::channelParticipantsAdmins>(), 0,
        std::numeric_limits<int32>::max(), hash)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_getParticipants>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto participants_ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetChannelAdministratorsQuery: " << to_string(participants_ptr);
    switch (participants_ptr->get_id()) {
      case telegram_api::channels_channelParticipants::ID: {
        auto participants = telegram_api::move_object_as<telegram_api::channels_channelParticipants>(participants_ptr);
        td_->contacts_manager_->on_get_users(std::move(participants->users_), "GetChannelAdministratorsQuery");
        td_->contacts_manager_->on_get_chats(std::move(participants->chats_), "GetChannelAdministratorsQuery");
        vector<DialogAdministrator> administrators;
        administrators.reserve(participants->participants_.size());
        for (auto &participant : participants->participants_) {
          DialogParticipant dialog_participant(std::move(participant));
          if (!dialog_participant.is_valid() || !dialog_participant.status_.is_administrator() ||
              dialog_participant.dialog_id_.get_type() != DialogType::User) {
            LOG(ERROR) << "Receive " << dialog_participant << " as an administrator of " << channel_id_;
            continue;
          }
          administrators.emplace_back(dialog_participant.dialog_id_.get_user_id(),
                                      dialog_participant.status_.get_rank(), dialog_participant.status_.is_creator());
        }

        td_->contacts_manager_->on_update_channel_administrator_count(channel_id_,
                                                                      narrow_cast<int32>(administrators.size()));
        td_->contacts_manager_->on_update_dialog_administrators(DialogId(channel_id_), std::move(administrators), true,
                                                                false);

        break;
      }
      case telegram_api::channels_channelParticipantsNotModified::ID:
        break;
      default:
        UNREACHABLE();
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    td_->contacts_manager_->on_get_channel_error(channel_id_, status, "GetChannelAdministratorsQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::reload_dialog_administrators(DialogId dialog_id,
                                                   const vector<DialogAdministrator> &dialog_administrators,
                                                   Promise<td_api::object_ptr<td_api::chatAdministrators>> &&promise) {
  auto query_promise = PromiseCreator::lambda(
      [actor_id = actor_id(this), dialog_id, promise = std::move(promise)](Result<Unit> &&result) mutable {
        if (promise) {
          if (result.is_ok()) {
            send_closure(actor_id, &ContactsManager::on_reload_dialog_administrators, dialog_id, std::move(promise));
          } else {
            promise.set_error(result.move_as_error());
          }
        }
      });
  switch (dialog_id.get_type()) {
    case DialogType::Chat:
      load_chat_full(dialog_id.get_chat_id(), false, std::move(query_promise), "reload_dialog_administrators");
      break;
    case DialogType::Channel: {
      auto hash = get_vector_hash(transform(dialog_administrators, [](const DialogAdministrator &administrator) {
        return static_cast<uint64>(administrator.get_user_id().get());
      }));
      td_->create_handler<GetChannelAdministratorsQuery>(std::move(query_promise))
          ->send(dialog_id.get_channel_id(), hash);
      break;
    }
    default:
      UNREACHABLE();
  }
}

class UpdateProfilePhotoQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  FileId file_id_;
  int64 old_photo_id_;
  string file_reference_;

 public:
  explicit UpdateProfilePhotoQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FileId file_id, int64 old_photo_id, tl_object_ptr<telegram_api::InputPhoto> &&input_photo) {
    CHECK(input_photo != nullptr);
    file_id_ = file_id;
    old_photo_id_ = old_photo_id;
    file_reference_ = FileManager::extract_file_reference(input_photo);
    send_query(G()->net_query_creator().create(telegram_api::photos_updateProfilePhoto(std::move(input_photo))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::photos_updateProfilePhoto>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    td_->contacts_manager_->on_set_profile_photo(result_ptr.move_as_ok(), old_photo_id_);

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      if (file_id_.is_valid()) {
        VLOG(file_references) << "Receive " << status << " for " << file_id_;
        td_->file_manager_->delete_file_reference(file_id_, file_reference_);
        td_->file_reference_manager_->repair_file_reference(
            file_id_, PromiseCreator::lambda([file_id = file_id_, old_photo_id = old_photo_id_,
                                              promise = std::move(promise_)](Result<Unit> result) mutable {
              if (result.is_error()) {
                return promise.set_error(Status::Error(400, "Can't find the photo"));
              }

              send_closure(G()->contacts_manager(), &ContactsManager::send_update_profile_photo_query, file_id,
                           old_photo_id, std::move(promise));
            }));
        return;
      } else {
        LOG(ERROR) << "Receive file reference error, but file_id = " << file_id_;
      }
    }

    promise_.set_error(std::move(status));
  }
};

class DeleteProfilePhotoQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  int64 profile_photo_id_;

 public:
  explicit DeleteProfilePhotoQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(int64 profile_photo_id) {
    profile_photo_id_ = profile_photo_id;
    vector<tl_object_ptr<telegram_api::InputPhoto>> input_photo_ids;
    input_photo_ids.push_back(make_tl_object<telegram_api::inputPhoto>(profile_photo_id, 0, BufferSlice()));
    send_query(G()->net_query_creator().create(telegram_api::photos_deletePhotos(std::move(input_photo_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::photos_deletePhotos>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for DeleteProfilePhotoQuery: " << format::as_array(result);
    if (result.size() != 1u) {
      LOG(WARNING) << "Photo can't be deleted";
      return on_error(Status::Error(400, "Photo can't be deleted"));
    }

    td_->contacts_manager_->on_delete_profile_photo(profile_photo_id_, std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::send_update_profile_photo_query(FileId file_id, int64 old_photo_id, Promise<Unit> &&promise) {
  FileView file_view = td_->file_manager_->get_file_view(file_id);
  td_->create_handler<UpdateProfilePhotoQuery>(std::move(promise))
      ->send(file_id, old_photo_id, file_view.main_remote_location().as_input_photo());
}

void ContactsManager::delete_profile_photo(int64 profile_photo_id, Promise<Unit> &&promise) {
  const User *u = get_user(get_my_id());
  if (u != nullptr && u->photo.id == profile_photo_id) {
    td_->create_handler<UpdateProfilePhotoQuery>(std::move(promise))
        ->send(FileId(), profile_photo_id, make_tl_object<telegram_api::inputPhotoEmpty>());
    return;
  }

  td_->create_handler<DeleteProfilePhotoQuery>(std::move(promise))->send(profile_photo_id);
}

class ExportChatInviteQuery final : public Td::ResultHandler {
  Promise<td_api::object_ptr<td_api::chatInviteLink>> promise_;
  DialogId dialog_id_;

 public:
  explicit ExportChatInviteQuery(Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const string &title, int32 expire_date, int32 usage_limit, bool creates_join_request,
            bool is_permanent) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Can't access the chat"));
    }

    int32 flags = 0;
    if (expire_date > 0) {
      flags |= telegram_api::messages_exportChatInvite::EXPIRE_DATE_MASK;
    }
    if (usage_limit > 0) {
      flags |= telegram_api::messages_exportChatInvite::USAGE_LIMIT_MASK;
    }
    if (creates_join_request) {
      flags |= telegram_api::messages_exportChatInvite::REQUEST_NEEDED_MASK;
    }
    if (is_permanent) {
      flags |= telegram_api::messages_exportChatInvite::LEGACY_REVOKE_PERMANENT_MASK;
    }
    if (!title.empty()) {
      flags |= telegram_api::messages_exportChatInvite::TITLE_MASK;
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_exportChatInvite(
        flags, false /*ignored*/, false /*ignored*/, std::move(input_peer), expire_date, usage_limit, title)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_exportChatInvite>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ExportChatInviteQuery: " << to_string(ptr);

    DialogInviteLink invite_link(std::move(ptr));
    if (!invite_link.is_valid()) {
      return on_error(Status::Error(500, "Receive invalid invite link"));
    }
    if (invite_link.get_creator_user_id() != td_->contacts_manager_->get_my_id()) {
      return on_error(Status::Error(500, "Receive invalid invite link creator"));
    }
    if (invite_link.is_permanent()) {
      td_->contacts_manager_->on_get_permanent_dialog_invite_link(dialog_id_, invite_link);
    }
    promise_.set_value(invite_link.get_chat_invite_link_object(td_->contacts_manager_.get()));
  }

  void on_error(Status status) final {
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ExportChatInviteQuery");
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::export_dialog_invite_link_impl(DialogId dialog_id, string title, int32 expire_date,
                                                     int32 usage_limit, bool creates_join_request, bool is_permanent,
                                                     Promise<td_api::object_ptr<td_api::chatInviteLink>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  TRY_STATUS_PROMISE(promise, can_manage_dialog_invite_links(dialog_id));
  if (creates_join_request && usage_limit > 0) {
    return promise.set_error(
        Status::Error(400, "Member limit can't be specified for links requiring administrator approval"));
  }

  auto new_title = clean_name(std::move(title), MAX_INVITE_LINK_TITLE_LENGTH);
  td_->create_handler<ExportChatInviteQuery>(std::move(promise))
      ->send(dialog_id, new_title, expire_date, usage_limit, creates_join_request, is_permanent);
}

class CanEditChannelCreatorQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit CanEditChannelCreatorQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    auto r_input_user = td_->contacts_manager_->get_input_user(td_->contacts_manager_->get_my_id());
    CHECK(r_input_user.is_ok());
    send_query(G()->net_query_creator().create(telegram_api::channels_editCreator(
        telegram_api::make_object<telegram_api::inputChannelEmpty>(), r_input_user.move_as_ok(),
        make_tl_object<telegram_api::inputCheckPasswordEmpty>())));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::channels_editCreator>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(ERROR) << "Receive result for CanEditChannelCreatorQuery: " << to_string(ptr);
    promise_.set_error(Status::Error(500, "Server doesn't returned error"));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void ContactsManager::can_transfer_ownership(Promise<CanTransferOwnershipResult> &&promise) {
  auto request_promise = PromiseCreator::lambda([promise = std::move(promise)](Result<Unit> r_result) mutable {
    CHECK(r_result.is_error());

    auto error = r_result.move_as_error();
    CanTransferOwnershipResult result;
    if (error.message() == "PASSWORD_HASH_INVALID") {
      return promise.set_value(std::move(result));
    }
    if (error.message() == "PASSWORD_MISSING") {
      result.type = CanTransferOwnershipResult::Type::PasswordNeeded;
      return promise.set_value(std::move(result));
    }
    if (begins_with(error.message(), "PASSWORD_TOO_FRESH_")) {
      result.type = CanTransferOwnershipResult::Type::PasswordTooFresh;
      result.retry_after = to_integer<int32>(error.message().substr(Slice("PASSWORD_TOO_FRESH_").size()));
      if (result.retry_after < 0) {
        result.retry_after = 0;
      }
      return promise.set_value(std::move(result));
    }
    if (begins_with(error.message(), "SESSION_TOO_FRESH_")) {
      result.type = CanTransferOwnershipResult::Type::SessionTooFresh;
      result.retry_after = to_integer<int32>(error.message().substr(Slice("SESSION_TOO_FRESH_").size()));
      if (result.retry_after < 0) {
        result.retry_after = 0;
      }
      return promise.set_value(std::move(result));
    }
    promise.set_error(std::move(error));
  });

  td_->create_handler<CanEditChannelCreatorQuery>(std::move(request_promise))->send();
}

void ContactsManager::on_update_user_online(User *u, UserId user_id, tl_object_ptr<telegram_api::UserStatus> &&status) {
  int32 id = status == nullptr ? telegram_api::userStatusEmpty::ID : status->get_id();
  int32 new_online;
  bool is_offline = false;
  if (id == telegram_api::userStatusOnline::ID) {
    int32 now = G()->unix_time();

    auto st = move_tl_object_as<telegram_api::userStatusOnline>(status);
    new_online = st->expires_;
    LOG_IF(ERROR, new_online < now - 86400)
        << "Receive userStatusOnline expired more than one day in past " << new_online;
  } else if (id == telegram_api::userStatusOffline::ID) {
    int32 now = G()->unix_time();

    auto st = move_tl_object_as<telegram_api::userStatusOffline>(status);
    new_online = st->was_online_;
    if (new_online >= now) {
      LOG_IF(ERROR, new_online > now + 10)
          << "Receive userStatusOffline but was online points to future time " << new_online << ", now is " << now;
      new_online = now - 1;
    }
    is_offline = true;
  } else if (id == telegram_api::userStatusRecently::ID) {
    new_online = -1;
  } else if (id == telegram_api::userStatusLastWeek::ID) {
    new_online = -2;
    is_offline = true;
  } else if (id == telegram_api::userStatusLastMonth::ID) {
    new_online = -3;
    is_offline = true;
  } else {
    CHECK(id == telegram_api::userStatusEmpty::ID);
    new_online = 0;
  }

  if (new_online != u->was_online) {
    LOG(DEBUG) << "Update " << user_id << " online from " << u->was_online << " to " << new_online;
    bool old_is_online = u->was_online > G()->unix_time_cached();
    bool new_is_online = new_online > G()->unix_time_cached();
    u->was_online = new_online;
    u->is_status_changed = true;
    if (u->was_online > 0) {
      u->local_was_online = 0;
    }

    if (user_id == get_my_id()) {
      if (my_was_online_local_ != 0 || old_is_online != new_is_online) {
        my_was_online_local_ = 0;
        u->is_online_status_changed = true;
      }
      if (is_offline) {
        td_->on_online_updated(false, false);
      }
    } else if (old_is_online != new_is_online) {
      u->is_online_status_changed = true;
    }
  }
}

class DeleteContactsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit DeleteContactsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<tl_object_ptr<telegram_api::InputUser>> &&input_users) {
    send_query(G()->net_query_creator().create(telegram_api::contacts_deleteContacts(std::move(input_users))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_deleteContacts>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for DeleteContactsQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->contacts_manager_->reload_contacts(true);
  }
};

void ContactsManager::remove_contacts(const vector<UserId> &user_ids, Promise<Unit> &&promise) {
  LOG(INFO) << "Delete contacts: " << format::as_array(user_ids);
  if (!are_contacts_loaded_) {
    load_contacts(std::move(promise));
    return;
  }

  vector<UserId> to_delete_user_ids;
  vector<tl_object_ptr<telegram_api::InputUser>> input_users;
  for (auto &user_id : user_ids) {
    const User *u = get_user(user_id);
    if (u != nullptr && u->is_contact) {
      auto r_input_user = get_input_user(user_id);
      if (r_input_user.is_ok()) {
        to_delete_user_ids.push_back(user_id);
        input_users.push_back(r_input_user.move_as_ok());
      }
    }
  }

  if (input_users.empty()) {
    return promise.set_value(Unit());
  }

  td_->create_handler<DeleteContactsQuery>(std::move(promise))->send(std::move(input_users));
}

void ContactsManager::add_profile_photo_to_cache(UserId user_id, Photo &&photo) {
  if (photo.is_empty()) {
    return;
  }

  // we have subsequence of user photos in user_photos_
  // ProfilePhoto in User and Photo in UserFull

  User *u = get_user_force(user_id);
  if (u == nullptr) {
    return;
  }

  // update photo list
  auto it = user_photos_.find(user_id);
  if (it != user_photos_.end() && it->second.count != -1) {
    auto user_photos = &it->second;
    if (user_photos->offset == 0) {
      if (user_photos->photos.empty() || user_photos->photos[0].id.get() != photo.id.get()) {
        user_photos->photos.insert(user_photos->photos.begin(), photo);
        user_photos->count++;
        register_user_photo(u, user_id, user_photos->photos[0]);
      }
    } else {
      user_photos->count++;
      user_photos->offset++;
    }
  }

  // update Photo in UserFull
  auto user_full = get_user_full_force(user_id);
  if (user_full != nullptr) {
    if (user_full->photo != photo) {
      user_full->photo = photo;
      user_full->is_changed = true;
      register_user_photo(u, user_id, photo);
    }
    update_user_full(user_full, user_id, "add_profile_photo_to_cache");
  }

  // update ProfilePhoto in User
  do_update_user_photo(u, user_id, as_profile_photo(td_->file_manager_.get(), user_id, u->access_hash, photo), false,
                       "add_profile_photo_to_cache");
  update_user(u, user_id);
}

class ResetContactsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit ResetContactsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::contacts_resetSaved()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_resetSaved>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      LOG(ERROR) << "Failed to delete imported contacts";
      td_->contacts_manager_->reload_contacts(true);
    } else {
      td_->contacts_manager_->on_update_contacts_reset();
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
    td_->contacts_manager_->reload_contacts(true);
  }
};

void ContactsManager::clear_imported_contacts(Promise<Unit> &&promise) {
  LOG(INFO) << "Delete imported contacts";

  if (saved_contact_count_ == 0) {
    promise.set_value(Unit());
    return;
  }

  td_->create_handler<ResetContactsQuery>(std::move(promise))->send();
}

void ContactsManager::load_user_full(UserId user_id, bool force, Promise<Unit> &&promise, const char *source) {
  auto u = get_user(user_id);
  if (u == nullptr) {
    return promise.set_error(Status::Error(400, "User not found"));
  }

  auto user_full = get_user_full_force(user_id);
  if (user_full == nullptr) {
    auto r_input_user = get_input_user(user_id);
    if (r_input_user.is_error()) {
      return promise.set_error(r_input_user.move_as_error());
    }

    return send_get_user_full_query(user_id, r_input_user.move_as_ok(), std::move(promise), source);
  }
  if (user_full->is_expired()) {
    auto r_input_user = get_input_user(user_id);
    CHECK(r_input_user.is_ok());
    if (td_->auth_manager_->is_bot() && !force) {
      return send_get_user_full_query(user_id, r_input_user.move_as_ok(), std::move(promise), "load expired user_full");
    }

    send_get_user_full_query(user_id, r_input_user.move_as_ok(), Auto(), "load expired user_full");
  }

  promise.set_value(Unit());
}

void ContactsManager::on_update_channel_linked_channel_id(ChannelId channel_id, ChannelId group_channel_id) {
  if (channel_id.is_valid()) {
    auto channel_full = get_channel_full_force(channel_id, true, "on_update_channel_linked_channel_id 1");
    on_update_channel_full_linked_channel_id(channel_full, channel_id, group_channel_id);
    if (channel_full != nullptr) {
      update_channel_full(channel_full, channel_id, "on_update_channel_linked_channel_id 3");
    }
  }
  if (group_channel_id.is_valid()) {
    auto channel_full = get_channel_full_force(group_channel_id, true, "on_update_channel_linked_channel_id 2");
    on_update_channel_full_linked_channel_id(channel_full, group_channel_id, channel_id);
    if (channel_full != nullptr) {
      update_channel_full(channel_full, group_channel_id, "on_update_channel_linked_channel_id 4");
    }
  }
}

void ContactsManager::on_save_secret_chat_to_database(SecretChatId secret_chat_id, bool success) {
  if (G()->close_flag()) {
    return;
  }

  SecretChat *c = get_secret_chat(secret_chat_id);
  CHECK(c != nullptr);
  CHECK(c->is_being_saved);
  CHECK(load_secret_chat_from_database_queries_.count(secret_chat_id) == 0);
  c->is_being_saved = false;

  if (!success) {
    LOG(ERROR) << "Failed to save " << secret_chat_id << " to database";
    c->is_saved = false;
  } else {
    LOG(INFO) << "Successfully saved " << secret_chat_id << " to database";
  }
  if (c->is_saved) {
    if (c->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), c->log_event_id);
      c->log_event_id = 0;
    }
  } else {
    save_secret_chat(c, secret_chat_id, c->log_event_id != 0);
  }
}

void ContactsManager::drop_chat_full(ChatId chat_id) {
  ChatFull *chat_full = get_chat_full_force(chat_id, "drop_chat_full");
  if (chat_full == nullptr) {
    drop_chat_photos(chat_id, false, false, "drop_chat_full");
    return;
  }

  LOG(INFO) << "Drop basicGroupFullInfo of " << chat_id;
  on_update_chat_full_photo(chat_full, chat_id, Photo());
  // chat_full->creator_user_id = UserId();
  chat_full->participants.clear();
  chat_full->bot_commands.clear();
  chat_full->version = -1;
  on_update_chat_full_invite_link(chat_full, nullptr);
  update_chat_online_member_count(chat_full, chat_id, true);
  chat_full->is_changed = true;
  update_chat_full(chat_full, chat_id, "drop_chat_full");
}

int64 ContactsManager::get_contacts_hash() {
  if (!are_contacts_loaded_) {
    return 0;
  }

  vector<int64> user_ids = contacts_hints_.search_empty(100000).second;
  CHECK(std::is_sorted(user_ids.begin(), user_ids.end()));
  auto my_id = get_my_id();
  const User *u = get_user_force(my_id);
  if (u != nullptr && u->is_contact) {
    user_ids.insert(std::upper_bound(user_ids.begin(), user_ids.end(), my_id.get()), my_id.get());
  }

  vector<uint64> numbers;
  numbers.reserve(user_ids.size() + 1);
  numbers.push_back(saved_contact_count_);
  for (auto user_id : user_ids) {
    numbers.push_back(user_id);
  }
  return get_vector_hash(numbers);
}

void ContactsManager::on_update_user_full_common_chat_count(UserFull *user_full, UserId user_id,
                                                            int32 common_chat_count) {
  CHECK(user_full != nullptr);
  if (common_chat_count < 0) {
    LOG(ERROR) << "Receive " << common_chat_count << " as common group count with " << user_id;
    common_chat_count = 0;
  }
  if (user_full->common_chat_count != common_chat_count) {
    user_full->common_chat_count = common_chat_count;
    user_full->is_common_chat_count_changed = true;
    user_full->is_changed = true;
  }
}

void ContactsManager::invalidate_channel_full(ChannelId channel_id, bool need_drop_slow_mode_delay) {
  LOG(INFO) << "Invalidate supergroup full for " << channel_id;
  auto channel_full = get_channel_full(channel_id, true, "invalidate_channel_full");  // must not load ChannelFull
  if (channel_full != nullptr) {
    do_invalidate_channel_full(channel_full, channel_id, need_drop_slow_mode_delay);
    update_channel_full(channel_full, channel_id, "invalidate_channel_full");
  } else {
    invalidated_channels_full_.insert(channel_id);
  }
}

void ContactsManager::save_created_public_channels(PublicDialogType type) {
  auto index = static_cast<int32>(type);
  CHECK(created_public_channels_inited_[index]);
  if (G()->parameters().use_chat_info_db) {
    G()->td_db()->get_binlog_pmc()->set(
        PSTRING() << "public_channels" << index,
        implode(
            transform(created_public_channels_[index], [](auto channel_id) { return PSTRING() << channel_id.get(); }),
            ','));
  }
}

bool ContactsManager::update_permanent_invite_link(DialogInviteLink &invite_link, DialogInviteLink new_invite_link) {
  if (new_invite_link != invite_link) {
    if (invite_link.is_valid() && invite_link.get_invite_link() != new_invite_link.get_invite_link()) {
      // old link was invalidated
      invite_link_infos_.erase(invite_link.get_invite_link());
    }

    invite_link = std::move(new_invite_link);
    return true;
  }
  return false;
}

tl_object_ptr<telegram_api::inputEncryptedChat> ContactsManager::get_input_encrypted_chat(
    SecretChatId secret_chat_id, AccessRights access_rights) const {
  auto sc = get_secret_chat(secret_chat_id);
  if (!have_input_encrypted_peer(sc, access_rights)) {
    return nullptr;
  }

  return make_tl_object<telegram_api::inputEncryptedChat>(secret_chat_id.get(), sc->access_hash);
}

void ContactsManager::on_update_user_full_is_blocked(UserFull *user_full, UserId user_id, bool is_blocked) {
  CHECK(user_full != nullptr);
  if (user_full->is_blocked != is_blocked) {
    LOG(INFO) << "Receive update user full is blocked with " << user_id << " and is_blocked = " << is_blocked;
    user_full->is_blocked = is_blocked;
    user_full->is_changed = true;
  }
}

string ContactsManager::get_user_title(UserId user_id) const {
  auto u = get_user(user_id);
  if (u == nullptr) {
    return string();
  }
  if (u->last_name.empty()) {
    return u->first_name;
  }
  if (u->first_name.empty()) {
    return u->last_name;
  }
  return PSTRING() << u->first_name << ' ' << u->last_name;
}

ContactsManager::ChatFull *ContactsManager::add_chat_full(ChatId chat_id) {
  CHECK(chat_id.is_valid());
  auto &chat_full_ptr = chats_full_[chat_id];
  if (chat_full_ptr == nullptr) {
    chat_full_ptr = make_unique<ChatFull>();
  }
  return chat_full_ptr.get();
}

bool ContactsManager::have_channel_participant_cache(ChannelId channel_id) const {
  if (!td_->auth_manager_->is_bot()) {
    return false;
  }
  auto c = get_channel(channel_id);
  return c != nullptr && c->status.is_administrator();
}

string ContactsManager::get_secret_chat_username(SecretChatId secret_chat_id) const {
  auto c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    return string();
  }
  return get_user_username(c->user_id);
}

bool ContactsManager::is_user_contact(const User *u, UserId user_id, bool is_mutual) const {
  return u != nullptr && (is_mutual ? u->is_mutual_contact : u->is_contact) && user_id != get_my_id();
}

void ContactsManager::invalidate_invite_link_info(const string &invite_link) {
  LOG(INFO) << "Invalidate info about invite link " << invite_link;
  invite_link_infos_.erase(invite_link);
}

DialogParticipantStatus ContactsManager::get_channel_status(const Channel *c) {
  c->status.update_restrictions();
  return c->status;
}

bool ContactsManager::have_user(UserId user_id) const {
  auto u = get_user(user_id);
  return u != nullptr && u->is_received;
}
}  // namespace td
