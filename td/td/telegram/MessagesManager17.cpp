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
#include "td/telegram/NotificationManager.h"
#include "td/telegram/NotificationSettings.hpp"
#include "td/telegram/NotificationType.h"

#include "td/telegram/ReplyMarkup.h"
#include "td/telegram/ReplyMarkup.hpp"

#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"


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

#include <algorithm>
#include <cstring>
#include <limits>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

class GetDialogsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit GetDialogsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(vector<InputDialogId> input_dialog_ids) {
    CHECK(!input_dialog_ids.empty());
    CHECK(input_dialog_ids.size() <= 100);
    auto input_dialog_peers = InputDialogId::get_input_dialog_peers(input_dialog_ids);
    CHECK(input_dialog_peers.size() == input_dialog_ids.size());
    send_query(G()->net_query_creator().create(telegram_api::messages_getPeerDialogs(std::move(input_dialog_peers))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getPeerDialogs>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for GetDialogsQuery: " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetDialogsQuery");
    td_->contacts_manager_->on_get_chats(std::move(result->chats_), "GetDialogsQuery");
    td_->messages_manager_->on_get_dialogs(FolderId(), std::move(result->dialogs_), -1, std::move(result->messages_),
                                           std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::load_dialog_filter(const DialogFilter *filter, bool force, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  vector<InputDialogId> needed_dialog_ids;
  for (auto input_dialog_ids :
       {&filter->pinned_dialog_ids, &filter->excluded_dialog_ids, &filter->included_dialog_ids}) {
    for (const auto &input_dialog_id : *input_dialog_ids) {
      if (!have_dialog(input_dialog_id.get_dialog_id())) {
        needed_dialog_ids.push_back(input_dialog_id);
      }
    }
  }

  vector<InputDialogId> input_dialog_ids;
  for (const auto &input_dialog_id : needed_dialog_ids) {
    auto dialog_id = input_dialog_id.get_dialog_id();
    // TODO load dialogs asynchronously
    if (!have_dialog_force(dialog_id, "load_dialog_filter")) {
      if (dialog_id.get_type() == DialogType::SecretChat) {
        if (have_dialog_info_force(dialog_id)) {
          force_create_dialog(dialog_id, "load_dialog_filter");
        }
      } else {
        input_dialog_ids.push_back(input_dialog_id);
      }
    }
  }

  if (!input_dialog_ids.empty() && !force) {
    const size_t MAX_SLICE_SIZE = 100;  // server side limit
    MultiPromiseActorSafe mpas{"GetFilterDialogsOnServerMultiPromiseActor"};
    mpas.add_promise(std::move(promise));
    mpas.set_ignore_errors(true);
    auto lock = mpas.get_promise();

    for (size_t i = 0; i < input_dialog_ids.size(); i += MAX_SLICE_SIZE) {
      auto end_i = i + MAX_SLICE_SIZE;
      auto end = end_i < input_dialog_ids.size() ? input_dialog_ids.begin() + end_i : input_dialog_ids.end();
      td_->create_handler<GetDialogsQuery>(mpas.get_promise())->send({input_dialog_ids.begin() + i, end});
    }

    lock.set_value(Unit());
    return;
  }

  promise.set_value(Unit());
}

vector<DialogId> MessagesManager::get_dialogs(DialogListId dialog_list_id, DialogDate offset, int32 limit,
                                              bool exact_limit, bool force, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());

  auto *list_ptr = get_dialog_list(dialog_list_id);
  if (list_ptr == nullptr) {
    promise.set_error(Status::Error(400, "Chat list not found"));
    return {};
  }
  auto &list = *list_ptr;

  LOG(INFO) << "Get chats in " << dialog_list_id << " with offset " << offset << " and limit " << limit
            << ". last_dialog_date = " << list.list_last_dialog_date_
            << ", last_pinned_dialog_date_ = " << list.last_pinned_dialog_date_
            << ", are_pinned_dialogs_inited_ = " << list.are_pinned_dialogs_inited_;

  if (limit <= 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be positive"));
    return {};
  }

  vector<DialogId> result;
  if (dialog_list_id == DialogListId(FolderId::main()) && sponsored_dialog_id_.is_valid()) {
    auto d = get_dialog(sponsored_dialog_id_);
    CHECK(d != nullptr);
    if (is_dialog_sponsored(d)) {
      DialogDate date(get_dialog_private_order(&list, d), d->dialog_id);
      if (offset < date) {
        result.push_back(sponsored_dialog_id_);
        offset = date;
        limit--;
      }
    }
  }

  if (!list.are_pinned_dialogs_inited_) {
    if (limit == 0 || force) {
      promise.set_value(Unit());
      return result;
    } else {
      if (dialog_list_id.is_folder()) {
        auto &folder = *get_dialog_folder(dialog_list_id.get_folder_id());
        if (folder.last_loaded_database_dialog_date_ == folder.last_database_server_dialog_date_ &&
            folder.folder_last_dialog_date_ != MAX_DIALOG_DATE) {
          load_dialog_list(list, limit, std::move(promise));
          return {};
        }
      }
      reload_pinned_dialogs(dialog_list_id, std::move(promise));
      return {};
    }
  }
  if (dialog_list_id.is_filter()) {
    auto *filter = get_dialog_filter(dialog_list_id.get_filter_id());
    CHECK(filter != nullptr);
    vector<InputDialogId> input_dialog_ids;
    for (const auto &input_dialog_id : filter->pinned_dialog_ids) {
      auto dialog_id = input_dialog_id.get_dialog_id();
      if (!have_dialog_force(dialog_id, "get_dialogs")) {
        if (dialog_id.get_type() == DialogType::SecretChat) {
          if (have_dialog_info_force(dialog_id)) {
            force_create_dialog(dialog_id, "get_dialogs");
          }
        } else {
          input_dialog_ids.push_back(input_dialog_id);
        }
      }
    }

    if (!input_dialog_ids.empty()) {
      if (limit == 0 || force) {
        promise.set_value(Unit());
        return result;
      } else {
        td_->create_handler<GetDialogsQuery>(std::move(promise))->send(std::move(input_dialog_ids));
        return {};
      }
    }
  }

  bool need_reload_pinned_dialogs = false;
  if (!list.pinned_dialogs_.empty() && offset < list.pinned_dialogs_.back() && limit > 0) {
    for (auto &pinned_dialog : list.pinned_dialogs_) {
      if (offset < pinned_dialog) {
        auto dialog_id = pinned_dialog.get_dialog_id();
        auto d = get_dialog_force(dialog_id, "get_dialogs");
        if (d == nullptr) {
          LOG(ERROR) << "Failed to load pinned " << dialog_id << " from " << dialog_list_id;
          if (dialog_id.get_type() != DialogType::SecretChat) {
            need_reload_pinned_dialogs = true;
          }
          continue;
        }
        if (d->order == DEFAULT_ORDER) {
          LOG(INFO) << "Loaded pinned " << dialog_id << " with default order in " << dialog_list_id;
          continue;
        }
        result.push_back(dialog_id);
        offset = pinned_dialog;
        limit--;
        if (limit == 0) {
          break;
        }
      }
    }
  }
  if (need_reload_pinned_dialogs) {
    reload_pinned_dialogs(dialog_list_id, Auto());
  }
  update_list_last_pinned_dialog_date(list);

  vector<const DialogFolder *> folders;
  vector<std::set<DialogDate>::const_iterator> folder_iterators;
  for (auto folder_id : get_dialog_list_folder_ids(list)) {
    folders.push_back(get_dialog_folder(folder_id));
    folder_iterators.push_back(folders.back()->ordered_dialogs_.upper_bound(offset));
  }
  while (limit > 0) {
    size_t best_pos = 0;
    DialogDate best_dialog_date = MAX_DIALOG_DATE;
    for (size_t i = 0; i < folders.size(); i++) {
      while (folder_iterators[i] != folders[i]->ordered_dialogs_.end() &&
             *folder_iterators[i] <= list.list_last_dialog_date_ &&
             (!is_dialog_in_list(get_dialog(folder_iterators[i]->get_dialog_id()), dialog_list_id) ||
              get_dialog_pinned_order(&list, folder_iterators[i]->get_dialog_id()) != DEFAULT_ORDER)) {
        ++folder_iterators[i];
      }
      if (folder_iterators[i] != folders[i]->ordered_dialogs_.end() &&
          *folder_iterators[i] <= list.list_last_dialog_date_ && *folder_iterators[i] < best_dialog_date) {
        best_pos = i;
        best_dialog_date = *folder_iterators[i];
      }
    }
    if (best_dialog_date == MAX_DIALOG_DATE || best_dialog_date.get_order() == DEFAULT_ORDER) {
      break;
    }

    limit--;
    result.push_back(folder_iterators[best_pos]->get_dialog_id());
    ++folder_iterators[best_pos];
  }

  if ((!result.empty() && (!exact_limit || limit == 0)) || force || list.list_last_dialog_date_ == MAX_DIALOG_DATE) {
    if (limit > 0 && list.list_last_dialog_date_ != MAX_DIALOG_DATE) {
      load_dialog_list(list, limit, Promise<Unit>());
    }

    promise.set_value(Unit());
    return result;
  } else {
    if (!result.empty()) {
      LOG(INFO) << "Have only " << result.size() << " chats, but " << limit << " chats more are needed";
    }
    load_dialog_list(list, limit, std::move(promise));
    return {};
  }
}

void MessagesManager::update_scope_unmute_timeout(NotificationSettingsScope scope, int32 &old_mute_until,
                                                  int32 new_mute_until) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  LOG(INFO) << "Update " << scope << " unmute timeout from " << old_mute_until << " to " << new_mute_until;
  if (old_mute_until == new_mute_until) {
    return;
  }
  CHECK(old_mute_until >= 0);

  schedule_scope_unmute(scope, new_mute_until);

  auto was_muted = old_mute_until != 0;
  auto is_muted = new_mute_until != 0;
  if (was_muted != is_muted) {
    if (G()->parameters().use_message_db) {
      std::unordered_map<DialogListId, int32, DialogListIdHash> delta;
      std::unordered_map<DialogListId, int32, DialogListIdHash> total_count;
      std::unordered_map<DialogListId, int32, DialogListIdHash> marked_count;
      std::unordered_set<DialogListId, DialogListIdHash> dialog_list_ids;
      for (auto &dialog : dialogs_) {
        Dialog *d = dialog.second.get();
        if (need_unread_counter(d->order) && d->notification_settings.use_default_mute_until &&
            get_dialog_notification_setting_scope(d->dialog_id) == scope) {
          int32 unread_count = d->server_unread_count + d->local_unread_count;
          if (unread_count != 0) {
            for (auto dialog_list_id : get_dialog_list_ids(d)) {
              delta[dialog_list_id] += unread_count;
              total_count[dialog_list_id]++;
              dialog_list_ids.insert(dialog_list_id);
            }
          } else if (d->is_marked_as_unread) {
            for (auto dialog_list_id : get_dialog_list_ids(d)) {
              total_count[dialog_list_id]++;
              marked_count[dialog_list_id]++;
              dialog_list_ids.insert(dialog_list_id);
            }
          }
        }
      }
      for (auto dialog_list_id : dialog_list_ids) {
        auto *list = get_dialog_list(dialog_list_id);
        CHECK(list != nullptr);
        if (delta[dialog_list_id] != 0 && list->is_message_unread_count_inited_) {
          if (was_muted) {
            list->unread_message_muted_count_ -= delta[dialog_list_id];
          } else {
            list->unread_message_muted_count_ += delta[dialog_list_id];
          }
          send_update_unread_message_count(*list, DialogId(), true, "update_scope_unmute_timeout");
        }
        if (total_count[dialog_list_id] != 0 && list->is_dialog_unread_count_inited_) {
          if (was_muted) {
            list->unread_dialog_muted_count_ -= total_count[dialog_list_id];
            list->unread_dialog_muted_marked_count_ -= marked_count[dialog_list_id];
          } else {
            list->unread_dialog_muted_count_ += total_count[dialog_list_id];
            list->unread_dialog_muted_marked_count_ += marked_count[dialog_list_id];
          }
          send_update_unread_chat_count(*list, DialogId(), true, "update_scope_unmute_timeout");
        }
      }
    }
  }

  old_mute_until = new_mute_until;

  if (was_muted != is_muted && !dialog_filters_.empty()) {
    for (auto &dialog : dialogs_) {
      Dialog *d = dialog.second.get();
      if (d->order != DEFAULT_ORDER && d->notification_settings.use_default_mute_until &&
          get_dialog_notification_setting_scope(d->dialog_id) == scope) {
        update_dialog_lists(d, get_dialog_positions(d), true, false, "update_scope_unmute_timeout");
      }
    }
  }
  if (!was_muted && is_muted) {
    for (auto &dialog : dialogs_) {
      Dialog *d = dialog.second.get();
      if (d->order != DEFAULT_ORDER && d->notification_settings.use_default_mute_until &&
          get_dialog_notification_setting_scope(d->dialog_id) == scope) {
        remove_all_dialog_notifications(d, false, "update_scope_unmute_timeout");
      }
    }
  }
}

bool MessagesManager::update_scope_notification_settings(NotificationSettingsScope scope,
                                                         ScopeNotificationSettings *current_settings,
                                                         const ScopeNotificationSettings &new_settings) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return false;
  }

  bool need_update_server = current_settings->mute_until != new_settings.mute_until ||
                            current_settings->sound != new_settings.sound ||
                            current_settings->show_preview != new_settings.show_preview;
  bool need_update_local =
      current_settings->disable_pinned_message_notifications != new_settings.disable_pinned_message_notifications ||
      current_settings->disable_mention_notifications != new_settings.disable_mention_notifications;
  bool was_inited = current_settings->is_synchronized;
  bool is_inited = new_settings.is_synchronized;
  if (was_inited && !is_inited) {
    return false;  // just in case
  }
  bool is_changed = need_update_server || need_update_local || was_inited != is_inited;
  if (is_changed) {
    save_scope_notification_settings(scope, new_settings);

    VLOG(notifications) << "Update notification settings in " << scope << " from " << *current_settings << " to "
                        << new_settings;

    update_scope_unmute_timeout(scope, current_settings->mute_until, new_settings.mute_until);

    if (!current_settings->disable_pinned_message_notifications && new_settings.disable_pinned_message_notifications) {
      VLOG(notifications) << "Remove pinned message notifications in " << scope;
      for (auto &dialog : dialogs_) {
        Dialog *d = dialog.second.get();
        if (d->notification_settings.use_default_disable_pinned_message_notifications &&
            d->mention_notification_group.group_id.is_valid() && d->pinned_message_notification_message_id.is_valid() &&
            get_dialog_notification_setting_scope(d->dialog_id) == scope) {
          remove_dialog_pinned_message_notification(d, "update_scope_notification_settings");
        }
      }
    }
    if (current_settings->disable_mention_notifications != new_settings.disable_mention_notifications) {
      VLOG(notifications) << "Remove mention notifications in " << scope;
      for (auto &dialog : dialogs_) {
        Dialog *d = dialog.second.get();
        if (d->notification_settings.use_default_disable_mention_notifications &&
            get_dialog_notification_setting_scope(d->dialog_id) == scope) {
          if (current_settings->disable_mention_notifications) {
            update_dialog_mention_notification_count(d);
          } else {
            remove_dialog_mention_notifications(d);
          }
        }
      }
    }

    *current_settings = new_settings;

    send_closure(G()->td(), &Td::send_update, get_update_scope_notification_settings_object(scope));
  }
  return need_update_server;
}

void MessagesManager::on_get_discussion_message(DialogId dialog_id, MessageId message_id,
                                                MessageThreadInfo &&message_thread_info,
                                                Promise<MessageThreadInfo> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  Dialog *d = get_dialog_force(dialog_id, "on_get_discussion_message");
  CHECK(d != nullptr);

  auto m = get_message_force(d, message_id, "on_get_discussion_message");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }
  if (message_thread_info.message_ids.empty()) {
    return promise.set_error(Status::Error(400, "Message has no thread"));
  }

  DialogId expected_dialog_id;
  if (m->reply_info.is_comment) {
    if (!is_active_message_reply_info(dialog_id, m->reply_info)) {
      return promise.set_error(Status::Error(400, "Message has no comments"));
    }
    expected_dialog_id = DialogId(m->reply_info.channel_id);
  } else {
    if (!m->top_thread_message_id.is_valid()) {
      return promise.set_error(Status::Error(400, "Message has no thread"));
    }
    expected_dialog_id = dialog_id;
  }

  if (expected_dialog_id != dialog_id && m->reply_info.is_comment &&
      m->linked_top_thread_message_id != message_thread_info.message_ids.back()) {
    auto linked_d = get_dialog_force(expected_dialog_id, "on_get_discussion_message 2");
    CHECK(linked_d != nullptr);
    auto linked_message_id = message_thread_info.message_ids.back();
    Message *linked_m = get_message_force(linked_d, linked_message_id, "on_get_discussion_message 3");
    CHECK(linked_m != nullptr && linked_m->message_id.is_server());
    if (linked_m->top_thread_message_id == linked_m->message_id &&
        is_active_message_reply_info(expected_dialog_id, linked_m->reply_info)) {
      if (m->linked_top_thread_message_id.is_valid()) {
        LOG(ERROR) << "Comment message identifier for " << message_id << " in " << dialog_id << " changed from "
                   << m->linked_top_thread_message_id << " to " << linked_message_id;
      }
      m->linked_top_thread_message_id = linked_message_id;
      on_dialog_updated(dialog_id, "on_get_discussion_message");
    }
  }
  promise.set_value(std::move(message_thread_info));
}

td_api::object_ptr<td_api::messageThreadInfo> MessagesManager::get_message_thread_info_object(
    const MessageThreadInfo &info) {
  if (info.message_ids.empty()) {
    return nullptr;
  }

  Dialog *d = get_dialog(info.dialog_id);
  CHECK(d != nullptr);
  td_api::object_ptr<td_api::messageReplyInfo> reply_info;
  vector<td_api::object_ptr<td_api::message>> messages;
  messages.reserve(info.message_ids.size());
  for (auto message_id : info.message_ids) {
    const Message *m = get_message_force(d, message_id, "get_message_thread_info_object");
    auto message = get_message_object(d->dialog_id, m, "get_message_thread_info_object");
    if (message != nullptr) {
      if (message->interaction_info_ != nullptr && message->interaction_info_->reply_info_ != nullptr) {
        reply_info = m->reply_info.get_message_reply_info_object(td_);
        CHECK(reply_info != nullptr);
      }
      messages.push_back(std::move(message));
    }
  }
  if (reply_info == nullptr) {
    return nullptr;
  }

  MessageId top_thread_message_id;
  td_api::object_ptr<td_api::draftMessage> draft_message;
  if (!info.message_ids.empty()) {
    top_thread_message_id = info.message_ids.back();
    if (can_send_message(d->dialog_id).is_ok()) {
      const Message *m = get_message_force(d, top_thread_message_id, "get_message_thread_info_object 2");
      if (m != nullptr && !m->reply_info.is_comment && is_active_message_reply_info(d->dialog_id, m->reply_info)) {
        draft_message = get_draft_message_object(m->thread_draft_message);
      }
    }
  }
  return td_api::make_object<td_api::messageThreadInfo>(d->dialog_id.get(), top_thread_message_id.get(),
                                                        std::move(reply_info), info.unread_message_count,
                                                        std::move(messages), std::move(draft_message));
}

class CheckHistoryImportQuery final : public Td::ResultHandler {
  Promise<tl_object_ptr<td_api::MessageFileType>> promise_;

 public:
  explicit CheckHistoryImportQuery(Promise<tl_object_ptr<td_api::MessageFileType>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send(const string &message_file_head) {
    send_query(G()->net_query_creator().create(telegram_api::messages_checkHistoryImport(message_file_head)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_checkHistoryImport>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for CheckHistoryImportQuery: " << to_string(ptr);
    auto file_type = [&]() -> td_api::object_ptr<td_api::MessageFileType> {
      if (ptr->pm_) {
        return td_api::make_object<td_api::messageFileTypePrivate>(ptr->title_);
      } else if (ptr->group_) {
        return td_api::make_object<td_api::messageFileTypeGroup>(ptr->title_);
      } else {
        return td_api::make_object<td_api::messageFileTypeUnknown>();
      }
    }();
    promise_.set_value(std::move(file_type));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_message_file_type(const string &message_file_head,
                                            Promise<td_api::object_ptr<td_api::MessageFileType>> &&promise) {
  td_->create_handler<CheckHistoryImportQuery>(std::move(promise))->send(message_file_head);
}

void MessagesManager::add_message_dependencies(Dependencies &dependencies, const Message *m) {
  dependencies.user_ids.insert(m->sender_user_id);
  add_dialog_and_dependencies(dependencies, m->sender_dialog_id);
  add_dialog_and_dependencies(dependencies, m->reply_in_dialog_id);
  add_dialog_and_dependencies(dependencies, m->real_forward_from_dialog_id);
  dependencies.user_ids.insert(m->via_bot_user_id);
  if (m->forward_info != nullptr) {
    dependencies.user_ids.insert(m->forward_info->sender_user_id);
    add_dialog_and_dependencies(dependencies, m->forward_info->sender_dialog_id);
    add_dialog_and_dependencies(dependencies, m->forward_info->from_dialog_id);
  }
  for (const auto &replier_min_channel : m->reply_info.replier_min_channels) {
    LOG(INFO) << "Add min " << replier_min_channel.first;
    td_->contacts_manager_->add_min_channel(replier_min_channel.first, replier_min_channel.second);
  }
  for (auto recent_replier_dialog_id : m->reply_info.recent_replier_dialog_ids) {
    // don't load the dialog itself
    // it will be created in get_message_reply_info_object if needed
    add_dialog_dependencies(dependencies, recent_replier_dialog_id);
  }
  add_message_content_dependencies(dependencies, m->content.get());
  add_reply_markup_dependencies(dependencies, m->reply_markup.get());
}

MessagesManager::Dialog *MessagesManager::get_dialog_by_message_id(MessageId message_id) {
  CHECK(message_id.is_valid() && message_id.is_server());
  auto it = message_id_to_dialog_id_.find(message_id);
  if (it == message_id_to_dialog_id_.end()) {
    if (G()->parameters().use_message_db) {
      auto r_value =
          G()->td_db()->get_messages_db_sync()->get_message_by_unique_message_id(message_id.get_server_message_id());
      if (r_value.is_ok()) {
        Message *m = on_get_message_from_database(r_value.ok(), false, "get_dialog_by_message_id");
        if (m != nullptr) {
          auto dialog_id = r_value.ok().dialog_id;
          CHECK(m->message_id == message_id);
          LOG_CHECK(message_id_to_dialog_id_[message_id] == dialog_id)
              << message_id << ' ' << dialog_id << ' ' << message_id_to_dialog_id_[message_id] << ' '
              << m->debug_source;
          Dialog *d = get_dialog(dialog_id);
          CHECK(d != nullptr);
          return d;
        }
      }
    }

    LOG(INFO) << "Can't find the chat by " << message_id;
    return nullptr;
  }

  return get_dialog(it->second);
}

bool MessagesManager::have_input_peer(DialogId dialog_id, AccessRights access_rights) const {
  switch (dialog_id.get_type()) {
    case DialogType::User: {
      UserId user_id = dialog_id.get_user_id();
      return td_->contacts_manager_->have_input_peer_user(user_id, access_rights);
    }
    case DialogType::Chat: {
      ChatId chat_id = dialog_id.get_chat_id();
      return td_->contacts_manager_->have_input_peer_chat(chat_id, access_rights);
    }
    case DialogType::Channel: {
      ChannelId channel_id = dialog_id.get_channel_id();
      return td_->contacts_manager_->have_input_peer_channel(channel_id, access_rights);
    }
    case DialogType::SecretChat: {
      SecretChatId secret_chat_id = dialog_id.get_secret_chat_id();
      return td_->contacts_manager_->have_input_encrypted_peer(secret_chat_id, access_rights);
    }
    case DialogType::None:
      return false;
    default:
      UNREACHABLE();
      return false;
  }
}

void MessagesManager::on_get_message_link_discussion_message(MessageLinkInfo &&info, DialogId comment_dialog_id,
                                                             Promise<MessageLinkInfo> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  CHECK(comment_dialog_id.is_valid());
  info.comment_dialog_id = comment_dialog_id;

  Dialog *d = get_dialog_force(comment_dialog_id, "on_get_message_link_discussion_message");
  if (d == nullptr) {
    return promise.set_error(Status::Error(500, "Chat not found"));
  }

  auto comment_message_id = info.comment_message_id;
  get_message_force_from_server(
      d, comment_message_id,
      PromiseCreator::lambda([info = std::move(info), promise = std::move(promise)](Result<Unit> &&result) mutable {
        return promise.set_value(std::move(info));
      }));
}

void MessagesManager::load_dialogs(vector<DialogId> dialog_ids, Promise<vector<DialogId>> &&promise) {
  LOG(INFO) << "Load chats " << format::as_array(dialog_ids);

  Dependencies dependencies;
  for (auto dialog_id : dialog_ids) {
    if (dialog_id.is_valid() && !have_dialog(dialog_id)) {
      add_dialog_dependencies(dependencies, dialog_id);
    }
  }
  resolve_dependencies_force(td_, dependencies, "load_dialogs");

  td::remove_if(dialog_ids, [this](DialogId dialog_id) { return !have_dialog_info(dialog_id); });

  for (auto dialog_id : dialog_ids) {
    force_create_dialog(dialog_id, "load_dialogs");
  }

  LOG(INFO) << "Loaded chats " << format::as_array(dialog_ids);
  promise.set_value(std::move(dialog_ids));
}

void MessagesManager::fail_edit_message_media(FullMessageId full_message_id, Status &&error) {
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  MessageId message_id = full_message_id.get_message_id();
  CHECK(message_id.is_any_server());

  auto m = get_message(d, message_id);
  if (m == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel
    return;
  }
  CHECK(m->edited_content != nullptr);
  m->edit_promise.set_error(std::move(error));
  cancel_edit_message_media(dialog_id, m, "Failed to edit message. MUST BE IGNORED");
}

bool MessagesManager::is_update_about_username_change_received(DialogId dialog_id) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return td_->contacts_manager_->is_update_about_username_change_received(dialog_id.get_user_id());
    case DialogType::Chat:
      return true;
    case DialogType::Channel:
      return td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id()).is_member();
    case DialogType::SecretChat:
      return true;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }
}

std::unordered_map<DialogListId, MessagesManager::DialogPositionInList, DialogListIdHash>
MessagesManager::get_dialog_positions(const Dialog *d) const {
  CHECK(d != nullptr);
  std::unordered_map<DialogListId, MessagesManager::DialogPositionInList, DialogListIdHash> positions;
  if (!td_->auth_manager_->is_bot()) {
    for (const auto &dialog_list : dialog_lists_) {
      positions.emplace(dialog_list.first, get_dialog_position_in_list(&dialog_list.second, d));
    }
  }
  return positions;
}

unique_ptr<MessagesManager::Message> MessagesManager::delete_message(Dialog *d, MessageId message_id,
                                                                     bool is_permanently_deleted,
                                                                     bool *need_update_dialog_pos, const char *source) {
  return do_delete_message(d, message_id, is_permanently_deleted, false, need_update_dialog_pos, source);
}

DialogNotificationSettings *MessagesManager::get_dialog_notification_settings(DialogId dialog_id, bool force) {
  Dialog *d = get_dialog_force(dialog_id, "get_dialog_notification_settings");
  if (d == nullptr) {
    return nullptr;
  }
  if (!force && !have_input_peer(dialog_id, AccessRights::Read)) {
    return nullptr;
  }
  return &d->notification_settings;
}

void MessagesManager::ttl_update_timeout(double now) {
  if (ttl_heap_.empty()) {
    if (!ttl_slot_.empty()) {
      ttl_slot_.cancel_timeout();
    }
    return;
  }
  ttl_slot_.set_event(EventCreator::yield(actor_id()));
  ttl_slot_.set_timeout_in(ttl_heap_.top_key() - now);
}

bool MessagesManager::delete_active_live_location(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  return active_live_location_full_message_ids_.erase(FullMessageId{dialog_id, m->message_id}) != 0;
}

vector<DialogListId> MessagesManager::get_dialog_list_ids(const Dialog *d) {
  return d->dialog_list_ids;
}

bool MessagesManager::have_dialog(DialogId dialog_id) const {
  return dialogs_.count(dialog_id) > 0;
}
}  // namespace td
