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
#include "td/telegram/GroupCallManager.h"

#include "td/telegram/InputMessageText.h"
#include "td/telegram/LinkManager.h"
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










namespace td {
}  // namespace td
namespace td {

class ResolveUsernameQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  string username_;

 public:
  explicit ResolveUsernameQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(const string &username) {
    username_ = username;
    send_query(G()->net_query_creator().create(telegram_api::contacts_resolveUsername(username)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::contacts_resolveUsername>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for ResolveUsernameQuery: " << to_string(ptr);
    td_->contacts_manager_->on_get_users(std::move(ptr->users_), "ResolveUsernameQuery");
    td_->contacts_manager_->on_get_chats(std::move(ptr->chats_), "ResolveUsernameQuery");

    td_->messages_manager_->on_resolved_username(username_, DialogId(ptr->peer_));

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (status.message() == Slice("USERNAME_NOT_OCCUPIED")) {
      td_->messages_manager_->drop_username(username_);
    }
    promise_.set_error(std::move(status));
  }
};

vector<DialogId> MessagesManager::search_public_dialogs(const string &query, Promise<Unit> &&promise) {
  LOG(INFO) << "Search public chats with query = \"" << query << '"';

  if (utf8_length(query) < MIN_SEARCH_PUBLIC_DIALOG_PREFIX_LEN) {
    string username = clean_username(query);
    if (username[0] == '@') {
      username = username.substr(1);
    }

    for (auto &short_username : get_valid_short_usernames()) {
      if (2 * username.size() > short_username.size() && begins_with(short_username, username)) {
        username = short_username.str();
        auto it = resolved_usernames_.find(username);
        if (it == resolved_usernames_.end()) {
          td_->create_handler<ResolveUsernameQuery>(std::move(promise))->send(username);
          return {};
        }

        if (it->second.expires_at < Time::now()) {
          td_->create_handler<ResolveUsernameQuery>(Promise<>())->send(username);
        }

        auto dialog_id = it->second.dialog_id;
        force_create_dialog(dialog_id, "public dialogs search");

        auto d = get_dialog(dialog_id);
        if (d == nullptr || d->order != DEFAULT_ORDER ||
            (dialog_id.get_type() == DialogType::User &&
             td_->contacts_manager_->is_user_contact(dialog_id.get_user_id()))) {
          continue;
        }

        promise.set_value(Unit());
        return {dialog_id};
      }
    }
    promise.set_value(Unit());
    return {};
  }

  auto it = found_public_dialogs_.find(query);
  if (it != found_public_dialogs_.end()) {
    promise.set_value(Unit());
    return it->second;
  }

  send_search_public_dialogs_query(query, std::move(promise));
  return {};
}

void MessagesManager::get_message_link_info(Slice url, Promise<MessageLinkInfo> &&promise) {
  auto r_message_link_info = LinkManager::get_message_link_info(url);
  if (r_message_link_info.is_error()) {
    return promise.set_error(Status::Error(400, r_message_link_info.error().message()));
  }

  auto info = r_message_link_info.move_as_ok();
  CHECK(info.username.empty() == info.channel_id.is_valid());

  bool have_dialog = info.username.empty() ? td_->contacts_manager_->have_channel_force(info.channel_id)
                                           : resolve_dialog_username(info.username).is_valid();
  if (!have_dialog) {
    auto query_promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), info, promise = std::move(promise)](Result<Unit> &&result) mutable {
          if (result.is_error()) {
            return promise.set_value(std::move(info));
          }
          send_closure(actor_id, &MessagesManager::on_get_message_link_dialog, std::move(info), std::move(promise));
        });
    if (info.username.empty()) {
      td_->contacts_manager_->reload_channel(info.channel_id, std::move(query_promise));
    } else {
      td_->create_handler<ResolveUsernameQuery>(std::move(query_promise))->send(info.username);
    }
    return;
  }

  return on_get_message_link_dialog(std::move(info), std::move(promise));
}

DialogId MessagesManager::search_public_dialog(const string &username_to_search, bool force, Promise<Unit> &&promise) {
  string username = clean_username(username_to_search);
  if (username[0] == '@') {
    username = username.substr(1);
  }
  if (username.empty()) {
    promise.set_error(Status::Error(200, "Username is invalid"));
    return DialogId();
  }

  DialogId dialog_id;
  auto it = resolved_usernames_.find(username);
  if (it != resolved_usernames_.end()) {
    if (it->second.expires_at < Time::now()) {
      td_->create_handler<ResolveUsernameQuery>(Promise<>())->send(username);
    }
    dialog_id = it->second.dialog_id;
  } else {
    auto it2 = inaccessible_resolved_usernames_.find(username);
    if (it2 != inaccessible_resolved_usernames_.end()) {
      dialog_id = it2->second;
    }
  }

  if (dialog_id.is_valid()) {
    if (have_input_peer(dialog_id, AccessRights::Read)) {
      if (!force && reload_voice_chat_on_search_usernames_.count(username)) {
        reload_voice_chat_on_search_usernames_.erase(username);
        if (dialog_id.get_type() == DialogType::Channel) {
          td_->contacts_manager_->reload_channel_full(dialog_id.get_channel_id(), std::move(promise),
                                                      "search_public_dialog");
          return DialogId();
        }
      }

      if (td_->auth_manager_->is_bot()) {
        force_create_dialog(dialog_id, "search_public_dialog", true);
      } else {
        const Dialog *d = get_dialog_force(dialog_id, "search_public_dialog");
        if (!is_dialog_inited(d)) {
          send_get_dialog_query(dialog_id, std::move(promise), 0, "search_public_dialog");
          return DialogId();
        }
      }

      promise.set_value(Unit());
      return dialog_id;
    } else {
      // bot username maybe known despite there is no access_hash
      if (force || dialog_id.get_type() != DialogType::User) {
        force_create_dialog(dialog_id, "search_public_dialog", true);
        promise.set_value(Unit());
        return dialog_id;
      }
    }
  }

  td_->create_handler<ResolveUsernameQuery>(std::move(promise))->send(username);
  return DialogId();
}

bool MessagesManager::update_message_content(DialogId dialog_id, Message *old_message,
                                             unique_ptr<MessageContent> new_content,
                                             bool need_send_update_message_content, bool need_merge_files,
                                             bool is_message_in_dialog) {
  bool is_content_changed = false;
  bool need_update = false;
  unique_ptr<MessageContent> &old_content = old_message->content;
  MessageContentType old_content_type = old_content->get_type();
  MessageContentType new_content_type = new_content->get_type();

  auto old_file_id = get_message_content_any_file_id(old_content.get());
  bool need_finish_upload = old_file_id.is_valid() && need_merge_files;
  if (old_content_type != new_content_type) {
    if (old_message->ttl > 0 && old_message->ttl_expires_at > 0 &&
        ((new_content_type == MessageContentType::ExpiredPhoto && old_content_type == MessageContentType::Photo) ||
         (new_content_type == MessageContentType::ExpiredVideo && old_content_type == MessageContentType::Video))) {
      LOG(INFO) << "Do not apply expired message content early";
    } else {
      need_update = true;
      LOG(INFO) << "Message content has changed its type from " << old_content_type << " to " << new_content_type;

      old_message->is_content_secret = is_secret_message_content(old_message->ttl, new_content->get_type());
    }

    if (need_merge_files && old_file_id.is_valid()) {
      auto new_file_id = get_message_content_any_file_id(new_content.get());
      if (new_file_id.is_valid()) {
        FileView old_file_view = td_->file_manager_->get_file_view(old_file_id);
        FileView new_file_view = td_->file_manager_->get_file_view(new_file_id);
        // if file type has changed, but file size remains the same, we are trying to update local location of the new
        // file with the old local location
        if (old_file_view.has_local_location() && !new_file_view.has_local_location() && old_file_view.size() != 0 &&
            old_file_view.size() == new_file_view.size()) {
          auto old_file_type = old_file_view.get_type();
          auto new_file_type = new_file_view.get_type();
          auto is_document_file_type = [](FileType file_type) {
            switch (file_type) {
              case FileType::Animation:
              case FileType::Audio:
              case FileType::Document:
              case FileType::DocumentAsFile:
              case FileType::Sticker:
              case FileType::Video:
              case FileType::VideoNote:
              case FileType::VoiceNote:
                return true;
              default:
                return false;
            }
          };

          if (is_document_file_type(old_file_type) && is_document_file_type(new_file_type)) {
            auto &old_location = old_file_view.local_location();
            auto r_file_id = td_->file_manager_->register_local(
                FullLocalFileLocation(new_file_type, old_location.path_, old_location.mtime_nsec_), dialog_id,
                old_file_view.size());
            if (r_file_id.is_ok()) {
              LOG_STATUS(td_->file_manager_->merge(new_file_id, r_file_id.ok()));
            }
          }
        }
      }
    }
  } else {
    merge_message_contents(td_, old_content.get(), new_content.get(), need_message_changed_warning(old_message),
                           dialog_id, need_merge_files, is_content_changed, need_update);
  }
  if (need_finish_upload) {
    // the file is likely to be already merged with a server file, but if not we need to
    // cancel file upload of the main file to allow next upload with the same file to succeed
    cancel_upload_file(old_file_id);
  }

  if (is_content_changed || need_update) {
    if (is_message_in_dialog) {
      reregister_message_content(td_, old_content.get(), new_content.get(), {dialog_id, old_message->message_id},
                                 "update_message_content");
    }
    old_content = std::move(new_content);
    old_message->last_edit_pts = 0;
    update_message_content_file_id_remote(old_content.get(), old_file_id);
  } else {
    update_message_content_file_id_remote(old_content.get(), get_message_content_any_file_id(new_content.get()));
  }
  if (is_content_changed && !need_update) {
    LOG(INFO) << "Content of " << old_message->message_id << " in " << dialog_id << " has changed";
  }

  if (need_update && need_send_update_message_content) {
    send_update_message_content(dialog_id, old_message, is_message_in_dialog, "update_message_content");
  }
  return need_update;
}

void MessagesManager::on_update_service_notification(tl_object_ptr<telegram_api::updateServiceNotification> &&update,
                                                     bool skip_new_entities, Promise<Unit> &&promise) {
  bool has_date = (update->flags_ & telegram_api::updateServiceNotification::INBOX_DATE_MASK) != 0;
  auto date = has_date ? update->inbox_date_ : G()->unix_time();
  if (date <= 0) {
    LOG(ERROR) << "Receive message date " << date << " in " << to_string(update);
    return;
  }
  bool is_auth_notification = begins_with(update->type_, "auth");
  if (is_auth_notification) {
    auto &old_date = auth_notification_id_date_[update->type_.substr(4)];
    if (date <= old_date) {
      LOG(INFO) << "Skip already applied " << to_string(update);
      return;
    }
    old_date = date;
  }

  bool is_authorized = td_->auth_manager_->is_authorized();
  bool is_user = is_authorized && !td_->auth_manager_->is_bot();
  auto contacts_manager = is_authorized ? td_->contacts_manager_.get() : nullptr;
  auto message_text = get_message_text(contacts_manager, std::move(update->message_), std::move(update->entities_),
                                       skip_new_entities, !is_user, date, false, "on_update_service_notification");
  DialogId owner_dialog_id = is_user ? get_service_notifications_dialog()->dialog_id : DialogId();
  int32 ttl = 0;
  bool disable_web_page_preview = false;
  auto content = get_message_content(td_, std::move(message_text), std::move(update->media_), owner_dialog_id, false,
                                     UserId(), &ttl, &disable_web_page_preview);
  bool is_content_secret = is_secret_message_content(ttl, content->get_type());

  if (update->popup_) {
    send_closure(G()->td(), &Td::send_update,
                 td_api::make_object<td_api::updateServiceNotification>(
                     update->type_, get_message_content_object(content.get(), td_, owner_dialog_id, date,
                                                               is_content_secret, true, -1)));
  }
  if (has_date && is_user) {
    Dialog *d = get_service_notifications_dialog();
    CHECK(d != nullptr);
    auto dialog_id = d->dialog_id;
    CHECK(dialog_id.get_type() == DialogType::User);

    auto new_message = make_unique<Message>();
    set_message_id(new_message, get_next_local_message_id(d));
    new_message->sender_user_id = dialog_id.get_user_id();
    new_message->date = date;
    new_message->ttl = ttl;
    new_message->disable_web_page_preview = disable_web_page_preview;
    new_message->is_content_secret = is_content_secret;
    new_message->content = std::move(content);
    new_message->have_previous = true;
    new_message->have_next = true;

    bool need_update = true;
    bool need_update_dialog_pos = false;

    const Message *m = add_message_to_dialog(d, std::move(new_message), true, &need_update, &need_update_dialog_pos,
                                             "on_update_service_notification");
    if (m != nullptr && need_update) {
      send_update_new_message(d, m);
    }
    register_new_local_message_id(d, m);

    if (need_update_dialog_pos) {
      send_update_chat_last_message(d, "on_update_service_notification");
    }
  }
  promise.set_value(Unit());

  if (is_auth_notification) {
    save_auth_notification_ids();
  }
}

bool MessagesManager::need_dialog_in_filter(const Dialog *d, const DialogFilter *filter) const {
  CHECK(d != nullptr);
  CHECK(filter != nullptr);
  CHECK(d->order != DEFAULT_ORDER);

  if (InputDialogId::contains(filter->pinned_dialog_ids, d->dialog_id)) {
    return true;
  }
  if (InputDialogId::contains(filter->included_dialog_ids, d->dialog_id)) {
    return true;
  }
  if (InputDialogId::contains(filter->excluded_dialog_ids, d->dialog_id)) {
    return false;
  }
  if (d->dialog_id.get_type() == DialogType::SecretChat) {
    auto user_id = td_->contacts_manager_->get_secret_chat_user_id(d->dialog_id.get_secret_chat_id());
    if (user_id.is_valid()) {
      auto dialog_id = DialogId(user_id);
      if (InputDialogId::contains(filter->pinned_dialog_ids, dialog_id)) {
        return true;
      }
      if (InputDialogId::contains(filter->included_dialog_ids, dialog_id)) {
        return true;
      }
      if (InputDialogId::contains(filter->excluded_dialog_ids, dialog_id)) {
        return false;
      }
    }
  }
  if (d->unread_mention_count == 0 || is_dialog_mention_notifications_disabled(d)) {
    if (filter->exclude_muted && is_dialog_muted(d)) {
      return false;
    }
    if (filter->exclude_read && d->server_unread_count + d->local_unread_count == 0 && !d->is_marked_as_unread) {
      return false;
    }
  }
  if (filter->exclude_archived && d->folder_id == FolderId::archive()) {
    return false;
  }
  switch (d->dialog_id.get_type()) {
    case DialogType::User: {
      auto user_id = d->dialog_id.get_user_id();
      if (td_->contacts_manager_->is_user_bot(user_id)) {
        return filter->include_bots;
      }
      if (user_id == td_->contacts_manager_->get_my_id() || td_->contacts_manager_->is_user_contact(user_id)) {
        return filter->include_contacts;
      }
      return filter->include_non_contacts;
    }
    case DialogType::Chat:
      return filter->include_groups;
    case DialogType::Channel:
      return is_broadcast_channel(d->dialog_id) ? filter->include_channels : filter->include_groups;
    case DialogType::SecretChat: {
      auto user_id = td_->contacts_manager_->get_secret_chat_user_id(d->dialog_id.get_secret_chat_id());
      if (td_->contacts_manager_->is_user_bot(user_id)) {
        return filter->include_bots;
      }
      if (td_->contacts_manager_->is_user_contact(user_id)) {
        return filter->include_contacts;
      }
      return filter->include_non_contacts;
    }
    default:
      UNREACHABLE();
      return false;
  }
}

class GetPeerSettingsQuery final : public Td::ResultHandler {
  DialogId dialog_id_;

 public:
  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    send_query(G()->net_query_creator().create(telegram_api::messages_getPeerSettings(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getPeerSettings>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    td_->contacts_manager_->on_get_users(std::move(ptr->users_), "GetPeerSettingsQuery");
    td_->contacts_manager_->on_get_chats(std::move(ptr->chats_), "GetPeerSettingsQuery");
    td_->messages_manager_->on_get_peer_settings(dialog_id_, std::move(ptr->settings_));
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for get peer settings: " << status;
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "GetPeerSettingsQuery");
  }
};

void MessagesManager::reget_dialog_action_bar(DialogId dialog_id, const char *source, bool is_repair) {
  if (G()->close_flag() || !dialog_id.is_valid() || td_->auth_manager_->is_bot()) {
    return;
  }

  Dialog *d = get_dialog_force(dialog_id, source);
  if (d == nullptr) {
    return;
  }

  if (is_repair && !d->need_repair_action_bar) {
    d->need_repair_action_bar = true;
    on_dialog_updated(dialog_id, source);
  }

  LOG(INFO) << "Reget action bar in " << dialog_id << " from " << source;
  switch (dialog_id.get_type()) {
    case DialogType::User:
      td_->contacts_manager_->reload_user_full(dialog_id.get_user_id());
      return;
    case DialogType::Chat:
    case DialogType::Channel:
      if (!have_input_peer(dialog_id, AccessRights::Read)) {
        return;
      }

      return td_->create_handler<GetPeerSettingsQuery>()->send(dialog_id);
    case DialogType::SecretChat:
    case DialogType::None:
    default:
      UNREACHABLE();
  }
}

void MessagesManager::create_dialog_filter(td_api::object_ptr<td_api::chatFilter> filter,
                                           Promise<td_api::object_ptr<td_api::chatFilterInfo>> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  if (dialog_filters_.size() >= MAX_DIALOG_FILTERS) {
    return promise.set_error(Status::Error(400, "The maximum number of chat folders exceeded"));
  }
  if (!is_update_chat_filters_sent_) {
    return promise.set_error(Status::Error(400, "Chat folders are not synchronized yet"));
  }

  DialogFilterId dialog_filter_id;
  do {
    auto min_id = static_cast<int>(DialogFilterId::min().get());
    auto max_id = static_cast<int>(DialogFilterId::max().get());
    dialog_filter_id = DialogFilterId(static_cast<int32>(Random::fast(min_id, max_id)));
  } while (get_dialog_filter(dialog_filter_id) != nullptr || get_server_dialog_filter(dialog_filter_id) != nullptr);

  auto r_dialog_filter = create_dialog_filter(dialog_filter_id, std::move(filter));
  if (r_dialog_filter.is_error()) {
    return promise.set_error(r_dialog_filter.move_as_error());
  }
  auto dialog_filter = r_dialog_filter.move_as_ok();
  CHECK(dialog_filter != nullptr);
  auto chat_filter_info = dialog_filter->get_chat_filter_info_object();

  bool at_beginning = false;
  for (const auto &recommended_dialog_filter : recommended_dialog_filters_) {
    if (DialogFilter::are_similar(*recommended_dialog_filter.dialog_filter, *dialog_filter)) {
      at_beginning = true;
    }
  }

  add_dialog_filter(std::move(dialog_filter), at_beginning, "create_dialog_filter");
  save_dialog_filters();
  send_update_chat_filters();

  synchronize_dialog_filters();
  promise.set_value(std::move(chat_filter_info));
}

void MessagesManager::get_dialogs_from_list_impl(int64 task_id) {
  auto task_it = get_dialogs_tasks_.find(task_id);
  CHECK(task_it != get_dialogs_tasks_.end());
  auto &task = task_it->second;
  auto promise = PromiseCreator::lambda([actor_id = actor_id(this), task_id](Result<Unit> &&result) {
    // on_get_dialogs_from_list can delete get_dialogs_tasks_[task_id], so it must be called later
    send_closure_later(actor_id, &MessagesManager::on_get_dialogs_from_list, task_id, std::move(result));
  });
  auto dialog_ids = get_dialogs(task.dialog_list_id, MIN_DIALOG_DATE, task.limit, true, false, std::move(promise));
  auto &list = *get_dialog_list(task.dialog_list_id);
  auto total_count = get_dialog_total_count(list);
  LOG(INFO) << "Receive " << dialog_ids.size() << " chats instead of " << task.limit << " out of " << total_count
            << " in " << task.dialog_list_id;
  CHECK(dialog_ids.size() <= static_cast<size_t>(total_count));
  CHECK(dialog_ids.size() <= static_cast<size_t>(task.limit));
  if (dialog_ids.size() == static_cast<size_t>(min(total_count, task.limit)) ||
      list.list_last_dialog_date_ == MAX_DIALOG_DATE || task.retry_count == 0) {
    auto task_promise = std::move(task.promise);
    get_dialogs_tasks_.erase(task_it);
    if (!task_promise) {
      dialog_ids.clear();
    }
    return task_promise.set_value(get_chats_object(total_count, dialog_ids));
  }
  // nor the limit, nor the end of the list were reached; wait for the promise
}

void MessagesManager::on_media_message_ready_to_send(DialogId dialog_id, MessageId message_id,
                                                     Promise<Message *> &&promise) {
  LOG(INFO) << "Ready to send " << message_id << " to " << dialog_id;
  CHECK(promise);
  if (!G()->parameters().use_file_db || message_id.is_scheduled()) {  // ResourceManager::Mode::Greedy
    auto m = get_message({dialog_id, message_id});
    if (m != nullptr) {
      promise.set_value(std::move(m));
    }
    return;
  }

  auto queue_id = get_sequence_dispatcher_id(dialog_id, MessageContentType::Photo);
  CHECK(queue_id & 1);
  auto &queue = yet_unsent_media_queues_[queue_id];
  auto it = queue.find(message_id);
  if (it == queue.end()) {
    if (queue.empty()) {
      yet_unsent_media_queues_.erase(queue_id);
    }

    LOG(INFO) << "Can't find " << message_id << " in the queue of " << dialog_id;
    auto m = get_message({dialog_id, message_id});
    if (m != nullptr) {
      promise.set_value(std::move(m));
    }
    return;
  }
  if (it->second) {
    promise.set_error(Status::Error(500, "Duplicate promise"));
    return;
  }
  it->second = std::move(promise);

  on_yet_unsent_media_queue_updated(dialog_id);
}

void MessagesManager::on_upload_message_media_file_part_missing(DialogId dialog_id, MessageId message_id,
                                                                int bad_part) {
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  Message *m = get_message(d, message_id);
  if (m == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel
    // don't need to send error to the user, because the message has already been deleted
    // and there is nothing to be deleted from the server
    LOG(INFO) << "Fail to send already deleted by the user or sent to inaccessible chat "
              << FullMessageId{dialog_id, message_id};
    return;
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // LOG(ERROR) << "Found " << m->message_id << " in inaccessible " << dialog_id;
    // dump_debug_message_op(get_dialog(dialog_id), 5);
    return;  // the message should be deleted soon
  }

  CHECK(dialog_id.get_type() != DialogType::SecretChat);

  do_send_message(dialog_id, m, {bad_part});
}

bool MessagesManager::need_channel_difference_to_add_message(DialogId dialog_id,
                                                             const tl_object_ptr<telegram_api::Message> &message_ptr) {
  if (dialog_id.get_type() != DialogType::Channel || !have_input_peer(dialog_id, AccessRights::Read) ||
      dialog_id == debug_channel_difference_dialog_) {
    return false;
  }
  if (message_ptr == nullptr) {
    return true;
  }
  if (get_message_dialog_id(message_ptr) != dialog_id) {
    return false;
  }

  Dialog *d = get_dialog_force(dialog_id, "need_channel_difference_to_add_message");
  if (d == nullptr) {
    return load_channel_pts(dialog_id) > 0 && !is_channel_difference_finished_.count(dialog_id);
  }
  if (d->last_new_message_id == MessageId()) {
    return d->pts > 0 && !d->is_channel_difference_finished;
  }

  return get_message_id(message_ptr, false) > d->last_new_message_id;
}

bool MessagesManager::is_discussion_message(DialogId dialog_id, const Message *m) const {
  if (m == nullptr || m->forward_info == nullptr) {
    return false;
  }
  if (m->sender_user_id.is_valid()) {
    if (!td_->auth_manager_->is_bot() || m->sender_user_id != ContactsManager::get_service_notifications_user_id()) {
      return false;
    }
  }
  if (!m->forward_info->from_dialog_id.is_valid() || !m->forward_info->from_message_id.is_valid()) {
    return false;
  }
  if (dialog_id.get_type() != DialogType::Channel || is_broadcast_channel(dialog_id)) {
    return false;
  }
  if (m->forward_info->from_dialog_id == dialog_id) {
    return false;
  }
  if (m->forward_info->from_dialog_id.get_type() != DialogType::Channel) {
    return false;
  }
  return true;
}

td_api::object_ptr<td_api::videoChat> MessagesManager::get_video_chat_object(const Dialog *d) const {
  auto active_group_call_id = td_->group_call_manager_->get_group_call_id(d->active_group_call_id, d->dialog_id);
  auto default_participant_alias =
      d->default_join_group_call_as_dialog_id.is_valid()
          ? get_message_sender_object_const(td_, d->default_join_group_call_as_dialog_id, "get_video_chat_object")
          : nullptr;
  return make_tl_object<td_api::videoChat>(active_group_call_id.get(),
                                           active_group_call_id.is_valid() ? !d->is_group_call_empty : false,
                                           std::move(default_participant_alias));
}

Status MessagesManager::toggle_dialog_silent_send_message(DialogId dialog_id, bool silent_send_message) {
  CHECK(!td_->auth_manager_->is_bot());

  Dialog *d = get_dialog_force(dialog_id, "toggle_dialog_silent_send_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return Status::Error(400, "Can't access the chat");
  }

  if (update_dialog_silent_send_message(d, silent_send_message)) {
    update_dialog_notification_settings_on_server(dialog_id, false);
  }

  return Status::OK();
}

void MessagesManager::unregister_message_reply(const Dialog *d, const Message *m) {
  auto it = replied_by_media_timestamp_messages_.find({d->dialog_id, m->reply_to_message_id});
  if (it == replied_by_media_timestamp_messages_.end()) {
    return;
  }

  auto is_deleted = it->second.erase(m->message_id) > 0;
  if (is_deleted) {
    LOG(INFO) << "Unregister " << m->message_id << " in " << d->dialog_id << " as reply to " << m->reply_to_message_id;
    if (it->second.empty()) {
      replied_by_media_timestamp_messages_.erase(it);
    }
  }
}

DialogId MessagesManager::resolve_dialog_username(const string &username) const {
  auto cleaned_username = clean_username(username);
  auto it = resolved_usernames_.find(cleaned_username);
  if (it != resolved_usernames_.end()) {
    return it->second.dialog_id;
  }

  auto it2 = inaccessible_resolved_usernames_.find(cleaned_username);
  if (it2 != inaccessible_resolved_usernames_.end()) {
    return it2->second;
  }

  return DialogId();
}

void MessagesManager::ttl_on_view(const Dialog *d, Message *m, double view_date, double now) {
  if (m->ttl > 0 && m->ttl_expires_at == 0 && !m->message_id.is_scheduled() && !m->message_id.is_yet_unsent() &&
      !m->is_failed_to_send && !m->is_content_secret) {
    m->ttl_expires_at = m->ttl + view_date;
    ttl_register_message(d->dialog_id, m, now);
    on_message_changed(d, m, true, "ttl_on_view");
  }
}

void MessagesManager::on_reload_dialog_filters_timeout(void *messages_manager_ptr) {
  if (G()->close_flag()) {
    return;
  }
  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::reload_dialog_filters);
}

void MessagesManager::on_update_dialog_folder_id(DialogId dialog_id, FolderId folder_id) {
  auto d = get_dialog_force(dialog_id, "on_update_dialog_folder_id");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  set_dialog_folder_id(d, folder_id);
}

bool MessagesManager::running_get_channel_difference(DialogId dialog_id) const {
  return active_get_channel_differencies_.count(dialog_id) > 0;
}
}  // namespace td
