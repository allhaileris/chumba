//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChatId.h"

#include "td/telegram/ContactsManager.h"
#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogActionBar.h"
#include "td/telegram/DialogDb.h"
#include "td/telegram/DialogFilter.h"
#include "td/telegram/DialogFilter.hpp"
#include "td/telegram/DialogLocation.h"
#include "td/telegram/DraftMessage.h"
#include "td/telegram/DraftMessage.hpp"

#include "td/telegram/files/FileId.hpp"
#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"


#include "td/telegram/InputMessageText.h"

#include "td/telegram/Location.h"
#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/MessageContent.h"
#include "td/telegram/MessageEntity.h"
#include "td/telegram/MessageEntity.hpp"
#include "td/telegram/MessageReplyInfo.hpp"
#include "td/telegram/MessagesDb.h"
#include "td/telegram/MessageSender.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/net/NetActor.h"
#include "td/telegram/net/NetQuery.h"
#include "td/telegram/NotificationGroupType.h"

#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"

#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"

#include "td/telegram/UpdatesManager.h"
#include "td/telegram/Version.h"


#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"
#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/format.h"
#include "td/utils/misc.h"
#include "td/utils/PathView.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"










namespace td {
}  // namespace td
namespace td {

class GetAllDraftsQuery final : public Td::ResultHandler {
 public:
  void send() {
    send_query(G()->net_query_creator().create(telegram_api::messages_getAllDrafts()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getAllDrafts>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetAllDraftsQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for GetAllDraftsQuery: " << status;
    }
    status.ignore();
  }
};

class GetDialogListActor final : public NetActorOnce {
  FolderId folder_id_;
  Promise<Unit> promise_;

 public:
  explicit GetDialogListActor(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(FolderId folder_id, int32 offset_date, ServerMessageId offset_message_id, DialogId offset_dialog_id,
            int32 limit, uint64 sequence_id) {
    folder_id_ = folder_id;
    auto input_peer = MessagesManager::get_input_peer_force(offset_dialog_id);
    CHECK(input_peer != nullptr);

    int32 flags =
        telegram_api::messages_getDialogs::EXCLUDE_PINNED_MASK | telegram_api::messages_getDialogs::FOLDER_ID_MASK;
    auto query = G()->net_query_creator().create(
        telegram_api::messages_getDialogs(flags, false /*ignored*/, folder_id.get(), offset_date,
                                          offset_message_id.get(), std::move(input_peer), limit, 0));
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getDialogs>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive chats from chat list of " << folder_id_ << ": " << to_string(ptr);
    switch (ptr->get_id()) {
      case telegram_api::messages_dialogs::ID: {
        auto dialogs = move_tl_object_as<telegram_api::messages_dialogs>(ptr);
        td_->contacts_manager_->on_get_users(std::move(dialogs->users_), "GetDialogListActor");
        td_->contacts_manager_->on_get_chats(std::move(dialogs->chats_), "GetDialogListActor");
        td_->messages_manager_->on_get_dialogs(folder_id_, std::move(dialogs->dialogs_),
                                               narrow_cast<int32>(dialogs->dialogs_.size()),
                                               std::move(dialogs->messages_), std::move(promise_));
        break;
      }
      case telegram_api::messages_dialogsSlice::ID: {
        auto dialogs = move_tl_object_as<telegram_api::messages_dialogsSlice>(ptr);
        td_->contacts_manager_->on_get_users(std::move(dialogs->users_), "GetDialogListActor");
        td_->contacts_manager_->on_get_chats(std::move(dialogs->chats_), "GetDialogListActor");
        td_->messages_manager_->on_get_dialogs(folder_id_, std::move(dialogs->dialogs_), max(dialogs->count_, 0),
                                               std::move(dialogs->messages_), std::move(promise_));
        break;
      }
      case telegram_api::messages_dialogsNotModified::ID:
        LOG(ERROR) << "Receive " << to_string(ptr);
        return on_error(Status::Error(500, "Receive wrong server response messages.dialogsNotModified"));
      default:
        UNREACHABLE();
    }
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::repair_server_dialog_total_count(DialogListId dialog_list_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  if (!dialog_list_id.is_folder()) {
    // can repair total count only in folders
    return;
  }

  LOG(INFO) << "Repair total chat count in " << dialog_list_id;
  send_closure(td_->create_net_actor<GetDialogListActor>(Promise<Unit>()), &GetDialogListActor::send,
               dialog_list_id.get_folder_id(), 2147483647, ServerMessageId(), DialogId(), 1,
               get_sequence_dispatcher_id(DialogId(), MessageContentType::None));
}

void MessagesManager::load_folder_dialog_list(FolderId folder_id, int32 limit, bool only_local) {
  if (G()->close_flag()) {
    return;
  }

  CHECK(!td_->auth_manager_->is_bot());
  auto &folder = *get_dialog_folder(folder_id);
  if (folder.folder_last_dialog_date_ == MAX_DIALOG_DATE) {
    return;
  }

  bool use_database = G()->parameters().use_message_db &&
                      folder.last_loaded_database_dialog_date_ < folder.last_database_server_dialog_date_;
  if (only_local && !use_database) {
    return;
  }

  auto &multipromise = folder.load_folder_dialog_list_multipromise_;
  if (multipromise.promise_count() != 0) {
    // queries have already been sent, just wait for the result
    LOG(INFO) << "Skip loading of dialog list in " << folder_id << " with limit " << limit
              << ", because it is already being loaded";
    if (use_database && folder.load_dialog_list_limit_max_ != 0) {
      folder.load_dialog_list_limit_max_ = max(folder.load_dialog_list_limit_max_, limit);
    }
    return;
  }
  LOG(INFO) << "Load chat list in " << folder_id << " with limit " << limit;
  multipromise.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), folder_id](Result<Unit> result) {
    send_closure_later(actor_id, &MessagesManager::on_load_folder_dialog_list, folder_id, std::move(result));
  }));

  bool is_query_sent = false;
  if (use_database) {
    load_folder_dialog_list_from_database(folder_id, limit, multipromise.get_promise());
    is_query_sent = true;
  } else {
    LOG(INFO) << "Get chats from " << folder.last_server_dialog_date_;
    multipromise.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), folder_id](Result<Unit> result) {
      if (result.is_ok()) {
        send_closure(actor_id, &MessagesManager::recalc_unread_count, DialogListId(folder_id), -1, true);
      }
    }));
    auto lock = multipromise.get_promise();
    reload_pinned_dialogs(DialogListId(folder_id), multipromise.get_promise());
    if (folder.folder_last_dialog_date_ == folder.last_server_dialog_date_) {
      send_closure(
          td_->create_net_actor<GetDialogListActor>(multipromise.get_promise()), &GetDialogListActor::send, folder_id,
          folder.last_server_dialog_date_.get_date(),
          folder.last_server_dialog_date_.get_message_id().get_next_server_message_id().get_server_message_id(),
          folder.last_server_dialog_date_.get_dialog_id(), int32{MAX_GET_DIALOGS},
          get_sequence_dispatcher_id(DialogId(), MessageContentType::None));
      is_query_sent = true;
    }
    if (folder_id == FolderId::main() && folder.last_server_dialog_date_ == MIN_DIALOG_DATE) {
      // do not pass promise to not wait for drafts before showing chat list
      td_->create_handler<GetAllDraftsQuery>()->send();
    }
    lock.set_value(Unit());
  }
  CHECK(is_query_sent);
}

void MessagesManager::set_dialog_photo(DialogId dialog_id, const tl_object_ptr<td_api::InputChatPhoto> &input_photo,
                                       Promise<Unit> &&promise) {
  LOG(INFO) << "Receive setChatPhoto request to change photo of " << dialog_id;

  if (!have_dialog_force(dialog_id, "set_dialog_photo")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
      return promise.set_error(Status::Error(400, "Can't change private chat photo"));
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto status = td_->contacts_manager_->get_chat_permissions(chat_id);
      if (!status.can_change_info_and_settings() ||
          (td_->auth_manager_->is_bot() && !td_->contacts_manager_->is_appointed_chat_administrator(chat_id))) {
        return promise.set_error(Status::Error(400, "Not enough rights to change chat photo"));
      }
      break;
    }
    case DialogType::Channel: {
      auto status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      if (!status.can_change_info_and_settings()) {
        return promise.set_error(Status::Error(400, "Not enough rights to change chat photo"));
      }
      break;
    }
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't change secret chat photo"));
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  const td_api::object_ptr<td_api::InputFile> *input_file = nullptr;
  double main_frame_timestamp = 0.0;
  bool is_animation = false;
  if (input_photo != nullptr) {
    switch (input_photo->get_id()) {
      case td_api::inputChatPhotoPrevious::ID: {
        auto photo = static_cast<const td_api::inputChatPhotoPrevious *>(input_photo.get());
        auto file_id = td_->contacts_manager_->get_profile_photo_file_id(photo->chat_photo_id_);
        if (!file_id.is_valid()) {
          return promise.set_error(Status::Error(400, "Unknown profile photo ID specified"));
        }

        auto file_view = td_->file_manager_->get_file_view(file_id);
        auto input_chat_photo =
            make_tl_object<telegram_api::inputChatPhoto>(file_view.main_remote_location().as_input_photo());
        send_edit_dialog_photo_query(dialog_id, file_id, std::move(input_chat_photo), std::move(promise));
        return;
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
  }
  if (input_file == nullptr) {
    send_edit_dialog_photo_query(dialog_id, FileId(), make_tl_object<telegram_api::inputChatPhotoEmpty>(),
                                 std::move(promise));
    return;
  }

  const double MAX_ANIMATION_DURATION = 10.0;
  if (main_frame_timestamp < 0.0 || main_frame_timestamp > MAX_ANIMATION_DURATION) {
    return promise.set_error(Status::Error(400, "Wrong main frame timestamp specified"));
  }

  auto file_type = is_animation ? FileType::Animation : FileType::Photo;
  auto r_file_id = td_->file_manager_->get_input_file_id(file_type, *input_file, dialog_id, true, false);
  if (r_file_id.is_error()) {
    // TODO promise.set_error(std::move(status));
    return promise.set_error(Status::Error(400, r_file_id.error().message()));
  }
  FileId file_id = r_file_id.ok();
  if (!file_id.is_valid()) {
    send_edit_dialog_photo_query(dialog_id, FileId(), make_tl_object<telegram_api::inputChatPhotoEmpty>(),
                                 std::move(promise));
    return;
  }

  upload_dialog_photo(dialog_id, td_->file_manager_->dup_file_id(file_id), is_animation, main_frame_timestamp, false,
                      std::move(promise));
}

class GetDialogMessageByDateQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  int32 date_;
  int64 random_id_;

 public:
  explicit GetDialogMessageByDateQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, int32 date, int64 random_id) {
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise_.set_error(Status::Error(400, "Can't access the chat"));
    }

    dialog_id_ = dialog_id;
    date_ = date;
    random_id_ = random_id;

    send_query(G()->net_query_creator().create(
        telegram_api::messages_getHistory(std::move(input_peer), 0, date, -3, 5, 0, 0, 0)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto info = td_->messages_manager_->get_messages_info(result_ptr.move_as_ok(), "GetDialogMessageByDateQuery");
    td_->messages_manager_->get_channel_difference_if_needed(
        dialog_id_, std::move(info),
        PromiseCreator::lambda([actor_id = td_->messages_manager_actor_.get(), dialog_id = dialog_id_, date = date_,
                                random_id = random_id_,
                                promise = std::move(promise_)](Result<MessagesManager::MessagesInfo> &&result) mutable {
          if (result.is_error()) {
            promise.set_error(result.move_as_error());
          } else {
            auto info = result.move_as_ok();
            send_closure(actor_id, &MessagesManager::on_get_dialog_message_by_date_success, dialog_id, date, random_id,
                         std::move(info.messages), std::move(promise));
          }
        }));
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetDialogMessageByDateQuery")) {
      LOG(ERROR) << "Receive error for GetDialogMessageByDateQuery in " << dialog_id_ << ": " << status;
    }
    promise_.set_error(std::move(status));
    td_->messages_manager_->on_get_dialog_message_by_date_fail(random_id_);
  }
};

void MessagesManager::get_dialog_message_by_date_from_server(const Dialog *d, int32 date, int64 random_id,
                                                             bool after_database_search, Promise<Unit> &&promise) {
  CHECK(d != nullptr);
  if (d->have_full_history) {
    // request can be always done locally/in memory. There is no need to send request to the server
    if (after_database_search) {
      return promise.set_value(Unit());
    }

    auto message_id = find_message_by_date(d->messages.get(), date);
    if (message_id.is_valid()) {
      get_dialog_message_by_date_results_[random_id] = {d->dialog_id, message_id};
    }
    promise.set_value(Unit());
    return;
  }
  if (d->dialog_id.get_type() == DialogType::SecretChat) {
    // there is no way to send request to the server
    return promise.set_value(Unit());
  }

  td_->create_handler<GetDialogMessageByDateQuery>(std::move(promise))->send(d->dialog_id, date, random_id);
}

class UpdateScopeNotifySettingsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  NotificationSettingsScope scope_;

 public:
  explicit UpdateScopeNotifySettingsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(NotificationSettingsScope scope, const ScopeNotificationSettings &new_settings) {
    auto input_notify_peer = get_input_notify_peer(scope);
    CHECK(input_notify_peer != nullptr);
    int32 flags = telegram_api::inputPeerNotifySettings::MUTE_UNTIL_MASK |
                  telegram_api::inputPeerNotifySettings::SOUND_MASK |
                  telegram_api::inputPeerNotifySettings::SHOW_PREVIEWS_MASK;
    send_query(G()->net_query_creator().create(telegram_api::account_updateNotifySettings(
        std::move(input_notify_peer),
        make_tl_object<telegram_api::inputPeerNotifySettings>(flags, new_settings.show_preview, false,
                                                              new_settings.mute_until, new_settings.sound))));
    scope_ = scope;
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::account_updateNotifySettings>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      return on_error(Status::Error(400, "Receive false as result"));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for set notification settings: " << status;

    if (!td_->auth_manager_->is_bot()) {
      // trying to repair notification settings for this scope
      td_->messages_manager_->send_get_scope_notification_settings_query(scope_, Promise<>());
    }

    promise_.set_error(std::move(status));
  }
};

void MessagesManager::update_scope_notification_settings_on_server(NotificationSettingsScope scope,
                                                                   uint64 log_event_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (log_event_id == 0) {
    log_event_id = save_update_scope_notification_settings_on_server_log_event(scope);
  }

  LOG(INFO) << "Update " << scope << " notification settings on server with log_event " << log_event_id;
  td_->create_handler<UpdateScopeNotifySettingsQuery>(get_erase_log_event_promise(log_event_id))
      ->send(scope, *get_scope_notification_settings(scope));
}

bool MessagesManager::load_dialog(DialogId dialog_id, int left_tries, Promise<Unit> &&promise) {
  if (!dialog_id.is_valid()) {
    promise.set_error(Status::Error(400, "Invalid chat identifier specified"));
    return false;
  }

  if (!have_dialog_force(dialog_id, "load_dialog")) {  // TODO remove _force
    if (G()->parameters().use_message_db) {
      //      TODO load dialog from database, DialogLoader
      //      send_closure_later(actor_id(this), &MessagesManager::load_dialog_from_database, dialog_id,
      //      std::move(promise));
      //      return false;
    }
    if (td_->auth_manager_->is_bot()) {
      switch (dialog_id.get_type()) {
        case DialogType::User: {
          auto user_id = dialog_id.get_user_id();
          auto have_user = td_->contacts_manager_->get_user(user_id, left_tries, std::move(promise));
          if (!have_user) {
            return false;
          }
          break;
        }
        case DialogType::Chat: {
          auto have_chat = td_->contacts_manager_->get_chat(dialog_id.get_chat_id(), left_tries, std::move(promise));
          if (!have_chat) {
            return false;
          }
          break;
        }
        case DialogType::Channel: {
          auto have_channel =
              td_->contacts_manager_->get_channel(dialog_id.get_channel_id(), left_tries, std::move(promise));
          if (!have_channel) {
            return false;
          }
          break;
        }
        case DialogType::SecretChat:
          promise.set_error(Status::Error(400, "Chat not found"));
          return false;
        case DialogType::None:
        default:
          UNREACHABLE();
      }
      if (!have_input_peer(dialog_id, AccessRights::Read)) {
        return false;
      }

      add_dialog(dialog_id, "load_dialog");
      return true;
    }

    promise.set_error(Status::Error(400, "Chat not found"));
    return false;
  }

  promise.set_value(Unit());
  return true;
}

class DeletePhoneCallHistoryQuery final : public Td::ResultHandler {
  Promise<AffectedHistory> promise_;

 public:
  explicit DeletePhoneCallHistoryQuery(Promise<AffectedHistory> &&promise) : promise_(std::move(promise)) {
  }

  void send(bool revoke) {
    int32 flags = 0;
    if (revoke) {
      flags |= telegram_api::messages_deletePhoneCallHistory::REVOKE_MASK;
    }
    send_query(
        G()->net_query_creator().create(telegram_api::messages_deletePhoneCallHistory(flags, false /*ignored*/)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_deletePhoneCallHistory>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto affected_messages = result_ptr.move_as_ok();
    if (!affected_messages->messages_.empty()) {
      td_->messages_manager_->process_pts_update(
          make_tl_object<telegram_api::updateDeleteMessages>(std::move(affected_messages->messages_), 0, 0));
    }
    promise_.set_value(AffectedHistory(std::move(affected_messages)));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::delete_all_call_messages_on_server(bool revoke, uint64 log_event_id, Promise<Unit> &&promise) {
  if (log_event_id == 0) {
    log_event_id = save_delete_all_call_messages_on_server_log_event(revoke);
  }

  AffectedHistoryQuery query = [td = td_, revoke](DialogId /*dialog_id*/, Promise<AffectedHistory> &&query_promise) {
    td->create_handler<DeletePhoneCallHistoryQuery>(std::move(query_promise))->send(revoke);
  };
  run_affected_history_query_until_complete(DialogId(), std::move(query), false,
                                            get_erase_log_event_promise(log_event_id, std::move(promise)));
}

void MessagesManager::repair_secret_chat_total_count(DialogListId dialog_list_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (G()->parameters().use_message_db && dialog_list_id.is_folder()) {
    // race-prone
    G()->td_db()->get_dialog_db_async()->get_secret_chat_count(
        dialog_list_id.get_folder_id(),
        PromiseCreator::lambda([actor_id = actor_id(this), dialog_list_id](Result<int32> result) {
          if (result.is_error()) {
            return;
          }
          send_closure(actor_id, &MessagesManager::on_get_secret_chat_total_count, dialog_list_id, result.move_as_ok());
        }));
  } else {
    int32 total_count = 0;
    auto *list = get_dialog_list(dialog_list_id);
    CHECK(list != nullptr);
    for (auto &folder_id : get_dialog_list_folder_ids(*list)) {
      const auto *folder_list = get_dialog_list(DialogListId(folder_id));
      CHECK(folder_list != nullptr);
      if (folder_list->need_unread_count_recalc_) {
        // can't repair total secret chat count yet
        return;
      }

      const auto *folder = get_dialog_folder(folder_id);
      CHECK(folder != nullptr);
      for (const auto &dialog_date : folder->ordered_dialogs_) {
        auto dialog_id = dialog_date.get_dialog_id();
        if (dialog_id.get_type() == DialogType::SecretChat && dialog_date.get_order() != DEFAULT_ORDER) {
          total_count++;
        }
      }
    }
    on_get_secret_chat_total_count(dialog_list_id, total_count);
  }
}

MessageId MessagesManager::get_dialog_pinned_message(DialogId dialog_id, Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "get_dialog_pinned_message");
  if (d == nullptr) {
    promise.set_error(Status::Error(400, "Chat not found"));
    return MessageId();
  }

  LOG(INFO) << "Get pinned message in " << dialog_id << " with "
            << (d->is_last_pinned_message_id_inited ? "inited" : "unknown") << " pinned " << d->last_pinned_message_id;

  if (!d->is_last_pinned_message_id_inited) {
    // must call get_dialog_info_full as expected in fix_new_dialog
    get_dialog_info_full(dialog_id, std::move(promise), "get_dialog_pinned_message 1");
    return MessageId();
  }

  get_dialog_info_full(dialog_id, Auto(), "get_dialog_pinned_message 2");

  if (d->last_pinned_message_id.is_valid()) {
    tl_object_ptr<telegram_api::InputMessage> input_message;
    if (dialog_id.get_type() == DialogType::Channel) {
      input_message = make_tl_object<telegram_api::inputMessagePinned>();
    }
    get_message_force_from_server(d, d->last_pinned_message_id, std::move(promise), std::move(input_message));
  } else {
    promise.set_value(Unit());
  }

  return d->last_pinned_message_id;
}

void MessagesManager::on_get_dialogs_from_list(int64 task_id, Result<Unit> &&result) {
  auto task_it = get_dialogs_tasks_.find(task_id);
  if (task_it == get_dialogs_tasks_.end()) {
    // the task has already been completed
    LOG(INFO) << "Chat list load task " << task_id << " has already been completed";
    return;
  }
  auto &task = task_it->second;
  if (result.is_error()) {
    LOG(INFO) << "Chat list load task " << task_id << " failed with the error " << result.error();
    auto task_promise = std::move(task.promise);
    get_dialogs_tasks_.erase(task_it);
    return task_promise.set_error(result.move_as_error());
  }

  auto list_ptr = get_dialog_list(task.dialog_list_id);
  CHECK(list_ptr != nullptr);
  auto &list = *list_ptr;
  if (task.last_dialog_date == list.list_last_dialog_date_) {
    // no new chats were loaded
    task.retry_count--;
  } else {
    CHECK(task.last_dialog_date < list.list_last_dialog_date_);
    task.last_dialog_date = list.list_last_dialog_date_;
    task.retry_count = 5;
  }
  get_dialogs_from_list_impl(task_id);
}

void MessagesManager::load_dialog_list(DialogList &list, int32 limit, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  if (limit > MAX_GET_DIALOGS + 2) {
    limit = MAX_GET_DIALOGS + 2;
  }
  bool is_request_sent = false;
  for (auto folder_id : get_dialog_list_folder_ids(list)) {
    const auto &folder = *get_dialog_folder(folder_id);
    if (folder.folder_last_dialog_date_ != MAX_DIALOG_DATE) {
      load_folder_dialog_list(folder_id, limit, false);
      is_request_sent = true;
    }
  }
  if (is_request_sent) {
    LOG(INFO) << "Wait for loading of " << limit << " chats in " << list.dialog_list_id;
    list.load_list_queries_.push_back(std::move(promise));
  } else {
    LOG(ERROR) << "There is nothing to load for " << list.dialog_list_id << " with folders "
               << get_dialog_list_folder_ids(list);
    promise.set_value(Unit());
  }
}

DialogId MessagesManager::get_message_dialog_id(const tl_object_ptr<telegram_api::Message> &message_ptr) {
  CHECK(message_ptr != nullptr);
  switch (message_ptr->get_id()) {
    case telegram_api::messageEmpty::ID: {
      auto message = static_cast<const telegram_api::messageEmpty *>(message_ptr.get());
      return message->peer_id_ == nullptr ? DialogId() : DialogId(message->peer_id_);
    }
    case telegram_api::message::ID: {
      auto message = static_cast<const telegram_api::message *>(message_ptr.get());
      return DialogId(message->peer_id_);
    }
    case telegram_api::messageService::ID: {
      auto message = static_cast<const telegram_api::messageService *>(message_ptr.get());
      return DialogId(message->peer_id_);
    }
    default:
      UNREACHABLE();
      return DialogId();
  }
}

Result<ServerMessageId> MessagesManager::get_payment_successful_message_id(FullMessageId full_message_id) {
  auto m = get_message_force(full_message_id, "get_payment_successful_message_id");
  if (m == nullptr) {
    return Status::Error(400, "Message not found");
  }
  if (m->content->get_type() != MessageContentType::PaymentSuccessful) {
    return Status::Error(400, "Message has wrong type");
  }
  if (m->message_id.is_scheduled()) {
    return Status::Error(400, "Wrong scheduled message identifier");
  }
  if (!m->message_id.is_server()) {
    return Status::Error(400, "Wrong message identifier");
  }

  return m->message_id.get_server_message_id();
}

vector<tl_object_ptr<telegram_api::InputPeer>> MessagesManager::get_input_peers(const vector<DialogId> &dialog_ids,
                                                                                AccessRights access_rights) const {
  vector<tl_object_ptr<telegram_api::InputPeer>> input_peers;
  input_peers.reserve(dialog_ids.size());
  for (auto &dialog_id : dialog_ids) {
    auto input_peer = get_input_peer(dialog_id, access_rights);
    if (input_peer == nullptr) {
      LOG(ERROR) << "Have no access to " << dialog_id;
      continue;
    }
    input_peers.push_back(std::move(input_peer));
  }
  return input_peers;
}

void MessagesManager::cancel_upload_message_content_files(const MessageContent *content) {
  auto file_id = get_message_content_upload_file_id(content);
  // always cancel file upload, it should be a no-op in the worst case
  if (being_uploaded_files_.erase(file_id) || file_id.is_valid()) {
    cancel_upload_file(file_id);
  }
  file_id = get_message_content_thumbnail_file_id(content, td_);
  if (being_uploaded_thumbnails_.erase(file_id) || file_id.is_valid()) {
    cancel_upload_file(file_id);
  }
}

void MessagesManager::on_pending_message_live_location_view_timeout_callback(void *messages_manager_ptr,
                                                                             int64 task_id) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager),
                     &MessagesManager::view_message_live_location_on_server, task_id);
}

void MessagesManager::cancel_edit_message_media(DialogId dialog_id, Message *m, Slice error_message) {
  if (m->edited_content == nullptr) {
    return;
  }

  cancel_upload_message_content_files(m->edited_content.get());

  m->edited_content = nullptr;
  m->edited_reply_markup = nullptr;
  m->edit_generation = 0;
  m->edit_promise.set_error(Status::Error(400, error_message));
}

void MessagesManager::update_message_count_by_index(Dialog *d, int diff, const Message *m) {
  auto index_mask = get_message_index_mask(d->dialog_id, m);
  index_mask &= ~message_search_filter_index_mask(
      MessageSearchFilter::UnreadMention);  // unread mention count has been already manually updated

  update_message_count_by_index(d, diff, index_mask);
}

string MessagesManager::get_channel_pts_key(DialogId dialog_id) {
  CHECK(dialog_id.get_type() == DialogType::Channel);
  auto channel_id = dialog_id.get_channel_id();
  return PSTRING() << "ch.p" << channel_id.get();
}

int64 MessagesManager::get_dialog_pinned_order(DialogListId dialog_list_id, DialogId dialog_id) const {
  return get_dialog_pinned_order(get_dialog_list(dialog_list_id), dialog_id);
}
}  // namespace td
