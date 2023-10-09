//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/MessagesManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChatId.h"
#include "td/telegram/ConfigShared.h"
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

#include <limits>


#include <unordered_map>
#include <unordered_set>


namespace td {
}  // namespace td
namespace td {

void MessagesManager::on_get_dialog_filters(Result<vector<tl_object_ptr<telegram_api::dialogFilter>>> r_filters,
                                            bool dummy) {
  are_dialog_filters_being_reloaded_ = false;
  if (G()->close_flag()) {
    return;
  }
  CHECK(!td_->auth_manager_->is_bot());
  auto promises = std::move(dialog_filter_reload_queries_);
  dialog_filter_reload_queries_.clear();
  if (r_filters.is_error()) {
    for (auto &promise : promises) {
      promise.set_error(r_filters.error().clone());
    }
    LOG(WARNING) << "Receive error " << r_filters.error() << " for GetDialogFiltersQuery";
    need_dialog_filters_reload_ = false;
    schedule_dialog_filters_reload(Random::fast(60, 5 * 60));
    return;
  }

  auto filters = r_filters.move_as_ok();
  vector<unique_ptr<DialogFilter>> new_server_dialog_filters;
  LOG(INFO) << "Receive " << filters.size() << " chat filters from server";
  std::unordered_set<DialogFilterId, DialogFilterIdHash> new_dialog_filter_ids;
  for (auto &filter : filters) {
    auto dialog_filter = DialogFilter::get_dialog_filter(std::move(filter), true);
    if (dialog_filter == nullptr) {
      continue;
    }
    if (!new_dialog_filter_ids.insert(dialog_filter->dialog_filter_id).second) {
      LOG(ERROR) << "Receive duplicate " << dialog_filter->dialog_filter_id;
      continue;
    }

    sort_dialog_filter_input_dialog_ids(dialog_filter.get(), "on_get_dialog_filters 1");
    new_server_dialog_filters.push_back(std::move(dialog_filter));
  }

  bool is_changed = false;
  dialog_filters_updated_date_ = G()->unix_time();
  if (server_dialog_filters_ != new_server_dialog_filters) {
    LOG(INFO) << "Change server chat filters from " << get_dialog_filter_ids(server_dialog_filters_) << " to "
              << get_dialog_filter_ids(new_server_dialog_filters);
    std::unordered_map<DialogFilterId, const DialogFilter *, DialogFilterIdHash> old_server_dialog_filters;
    for (const auto &filter : server_dialog_filters_) {
      old_server_dialog_filters.emplace(filter->dialog_filter_id, filter.get());
    }
    for (const auto &new_server_filter : new_server_dialog_filters) {
      auto dialog_filter_id = new_server_filter->dialog_filter_id;
      auto old_filter = get_dialog_filter(dialog_filter_id);
      auto it = old_server_dialog_filters.find(dialog_filter_id);
      if (it != old_server_dialog_filters.end()) {
        auto old_server_filter = it->second;
        if (*new_server_filter != *old_server_filter) {
          if (old_filter == nullptr) {
            // the filter was deleted, don't need to edit it
          } else {
            if (DialogFilter::are_equivalent(*old_filter, *new_server_filter)) {  // fast path
              // the filter was edited from this client, nothing to do
            } else {
              auto new_filter =
                  DialogFilter::merge_dialog_filter_changes(old_filter, old_server_filter, new_server_filter.get());
              LOG(INFO) << "Old  local filter: " << *old_filter;
              LOG(INFO) << "Old server filter: " << *old_server_filter;
              LOG(INFO) << "New server filter: " << *new_server_filter;
              LOG(INFO) << "New  local filter: " << *new_filter;
              sort_dialog_filter_input_dialog_ids(new_filter.get(), "on_get_dialog_filters 2");
              if (*new_filter != *old_filter) {
                is_changed = true;
                edit_dialog_filter(std::move(new_filter), "on_get_dialog_filters");
              }
            }
          }
        }
        old_server_dialog_filters.erase(it);
      } else {
        if (old_filter == nullptr) {
          // the filter was added from another client
          is_changed = true;
          add_dialog_filter(make_unique<DialogFilter>(*new_server_filter), false, "on_get_dialog_filters");
        } else {
          // the filter was added from this client
          // after that it could be added from another client, or edited from this client, or edited from another client
          // prefer local value, so do nothing
          // effectively, ignore edits from other clients, if didn't receive UpdateDialogFilterQuery response
        }
      }
    }
    vector<DialogFilterId> left_old_server_dialog_filter_ids;
    for (const auto &filter : server_dialog_filters_) {
      if (old_server_dialog_filters.count(filter->dialog_filter_id) == 0) {
        left_old_server_dialog_filter_ids.push_back(filter->dialog_filter_id);
      }
    }
    LOG(INFO) << "Still existing server chat filters: " << left_old_server_dialog_filter_ids;
    for (auto &old_server_filter : old_server_dialog_filters) {
      auto dialog_filter_id = old_server_filter.first;
      // deleted filter
      auto old_filter = get_dialog_filter(dialog_filter_id);
      if (old_filter == nullptr) {
        // the filter was deleted from this client, nothing to do
      } else {
        // the filter was deleted from another client
        // ignore edits done from the current client and just delete the filter
        is_changed = true;
        delete_dialog_filter(dialog_filter_id, "on_get_dialog_filters");
      }
    }
    bool is_order_changed = [&] {
      vector<DialogFilterId> new_server_dialog_filter_ids = get_dialog_filter_ids(new_server_dialog_filters);
      CHECK(new_server_dialog_filter_ids.size() >= left_old_server_dialog_filter_ids.size());
      new_server_dialog_filter_ids.resize(left_old_server_dialog_filter_ids.size());
      return new_server_dialog_filter_ids != left_old_server_dialog_filter_ids;
    }();
    if (is_order_changed) {  // if order is changed from this and other clients, prefer order from another client
      vector<DialogFilterId> new_dialog_filter_order;
      for (const auto &new_server_filter : new_server_dialog_filters) {
        auto dialog_filter_id = new_server_filter->dialog_filter_id;
        if (get_dialog_filter(dialog_filter_id) != nullptr) {
          new_dialog_filter_order.push_back(dialog_filter_id);
        }
      }
      is_changed = true;
      set_dialog_filters_order(dialog_filters_, new_dialog_filter_order);
    }

    server_dialog_filters_ = std::move(new_server_dialog_filters);
  }
  if (is_changed || !is_update_chat_filters_sent_) {
    send_update_chat_filters();
  }
  schedule_dialog_filters_reload(get_dialog_filters_cache_time());
  save_dialog_filters();

  if (need_synchronize_dialog_filters()) {
    synchronize_dialog_filters();
  }
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

unique_ptr<MessagesManager::Message> MessagesManager::create_message_to_send(
    Dialog *d, MessageId top_thread_message_id, MessageId reply_to_message_id, const MessageSendOptions &options,
    unique_ptr<MessageContent> &&content, bool suppress_reply_info, unique_ptr<MessageForwardInfo> forward_info,
    bool is_copy, DialogId send_as_dialog_id) const {
  CHECK(d != nullptr);
  CHECK(!reply_to_message_id.is_scheduled());
  CHECK(content != nullptr);

  bool is_scheduled = options.schedule_date != 0;
  DialogId dialog_id = d->dialog_id;

  auto dialog_type = dialog_id.get_type();
  auto my_id = td_->contacts_manager_->get_my_id();

  auto m = make_unique<Message>();
  bool is_channel_post = is_broadcast_channel(dialog_id);
  if (is_channel_post) {
    // sender of the post can be hidden
    if (!is_scheduled && td_->contacts_manager_->get_channel_sign_messages(dialog_id.get_channel_id())) {
      m->author_signature = td_->contacts_manager_->get_user_title(my_id);
    }
    m->sender_dialog_id = dialog_id;
  } else {
    if (send_as_dialog_id.is_valid()) {
      if (send_as_dialog_id.get_type() == DialogType::User) {
        m->sender_user_id = send_as_dialog_id.get_user_id();
      } else {
        m->sender_dialog_id = send_as_dialog_id;
      }
    } else if (d->default_send_message_as_dialog_id.is_valid()) {
      if (d->default_send_message_as_dialog_id.get_type() == DialogType::User) {
        m->sender_user_id = my_id;
      } else {
        m->sender_dialog_id = d->default_send_message_as_dialog_id;
      }
      m->has_explicit_sender = true;
    } else {
      if (is_anonymous_administrator(dialog_id, &m->author_signature)) {
        m->sender_dialog_id = dialog_id;
      } else {
        m->sender_user_id = my_id;
      }
    }
  }
  m->send_date = G()->unix_time();
  m->date = is_scheduled ? options.schedule_date : m->send_date;
  m->reply_to_message_id = reply_to_message_id;
  if (!is_scheduled) {
    m->top_thread_message_id = top_thread_message_id;
    if (reply_to_message_id.is_valid()) {
      const Message *reply_m = get_message(d, reply_to_message_id);
      if (reply_m != nullptr && reply_m->top_thread_message_id.is_valid()) {
        m->top_thread_message_id = reply_m->top_thread_message_id;
      }
    }
  }
  m->is_channel_post = is_channel_post;
  m->is_outgoing = is_scheduled || dialog_id != DialogId(my_id);
  m->from_background = options.from_background;
  m->view_count = is_channel_post && !is_scheduled ? 1 : 0;
  m->forward_count = 0;
  if ([&] {
        if (suppress_reply_info) {
          return false;
        }
        if (is_scheduled) {
          return false;
        }
        if (dialog_type != DialogType::Channel) {
          return false;
        }
        if (td_->auth_manager_->is_bot()) {
          return false;
        }
        if (is_channel_post) {
          return td_->contacts_manager_->get_channel_has_linked_channel(dialog_id.get_channel_id());
        }
        return !reply_to_message_id.is_valid();
      }()) {
    m->reply_info.reply_count = 0;
    if (is_channel_post) {
      auto linked_channel_id = td_->contacts_manager_->get_channel_linked_channel_id(dialog_id.get_channel_id());
      if (linked_channel_id.is_valid()) {
        m->reply_info.is_comment = true;
        m->reply_info.channel_id = linked_channel_id;
      }
    }
  }
  m->content = std::move(content);
  m->forward_info = std::move(forward_info);
  m->is_copy = is_copy || m->forward_info != nullptr;

  if (td_->auth_manager_->is_bot() || options.disable_notification ||
      G()->shared_config().get_option_boolean("ignore_default_disable_notification")) {
    m->disable_notification = options.disable_notification;
  } else {
    m->disable_notification = d->notification_settings.silent_send_message;
  }

  if (dialog_type == DialogType::SecretChat) {
    CHECK(!is_scheduled);
    m->ttl = td_->contacts_manager_->get_secret_chat_ttl(dialog_id.get_secret_chat_id());
    if (is_service_message_content(m->content->get_type())) {
      m->ttl = 0;
    }
    m->is_content_secret = is_secret_message_content(m->ttl, m->content->get_type());
    if (reply_to_message_id.is_valid()) {
      // the message was forcely preloaded in get_reply_to_message_id
      auto *reply_to_message = get_message(d, reply_to_message_id);
      if (reply_to_message != nullptr) {
        m->reply_to_random_id = reply_to_message->random_id;
      } else {
        m->reply_to_message_id = MessageId();
      }
    }
  }
  return m;
}

void MessagesManager::on_get_common_dialogs(UserId user_id, int64 offset_chat_id,
                                            vector<tl_object_ptr<telegram_api::Chat>> &&chats, int32 total_count) {
  td_->contacts_manager_->on_update_user_common_chat_count(user_id, total_count);

  auto &common_dialogs = found_common_dialogs_[user_id];
  if (common_dialogs.is_outdated && offset_chat_id == 0 &&
      common_dialogs.dialog_ids.size() < static_cast<size_t>(MAX_GET_DIALOGS)) {
    // drop outdated cache if possible
    common_dialogs = CommonDialogs();
  }
  if (common_dialogs.received_date == 0) {
    common_dialogs.received_date = Time::now();
  }
  common_dialogs.is_outdated = false;
  auto &result = common_dialogs.dialog_ids;
  if (!result.empty() && result.back() == DialogId()) {
    return;
  }
  bool is_last = chats.empty() && offset_chat_id == 0;
  for (auto &chat : chats) {
    DialogId dialog_id;
    switch (chat->get_id()) {
      case telegram_api::chatEmpty::ID: {
        auto c = static_cast<const telegram_api::chatEmpty *>(chat.get());
        ChatId chat_id(c->id_);
        if (!chat_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << chat_id;
          continue;
        }
        dialog_id = DialogId(chat_id);
        break;
      }
      case telegram_api::chat::ID: {
        auto c = static_cast<const telegram_api::chat *>(chat.get());
        ChatId chat_id(c->id_);
        if (!chat_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << chat_id;
          continue;
        }
        dialog_id = DialogId(chat_id);
        break;
      }
      case telegram_api::chatForbidden::ID: {
        auto c = static_cast<const telegram_api::chatForbidden *>(chat.get());
        ChatId chat_id(c->id_);
        if (!chat_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << chat_id;
          continue;
        }
        dialog_id = DialogId(chat_id);
        break;
      }
      case telegram_api::channel::ID: {
        auto c = static_cast<const telegram_api::channel *>(chat.get());
        ChannelId channel_id(c->id_);
        if (!channel_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << channel_id;
          continue;
        }
        dialog_id = DialogId(channel_id);
        break;
      }
      case telegram_api::channelForbidden::ID: {
        auto c = static_cast<const telegram_api::channelForbidden *>(chat.get());
        ChannelId channel_id(c->id_);
        if (!channel_id.is_valid()) {
          LOG(ERROR) << "Receive invalid " << channel_id;
          continue;
        }
        dialog_id = DialogId(channel_id);
        break;
      }
      default:
        UNREACHABLE();
    }
    CHECK(dialog_id.is_valid());
    td_->contacts_manager_->on_get_chat(std::move(chat), "on_get_common_dialogs");

    if (!td::contains(result, dialog_id)) {
      force_create_dialog(dialog_id, "get common dialogs");
      result.push_back(dialog_id);
    }
  }
  if (result.size() >= static_cast<size_t>(total_count) || is_last) {
    if (result.size() != static_cast<size_t>(total_count)) {
      LOG(ERROR) << "Fix total count of common groups with " << user_id << " from " << total_count << " to "
                 << result.size();
      total_count = narrow_cast<int32>(result.size());
      td_->contacts_manager_->on_update_user_common_chat_count(user_id, total_count);
    }

    result.emplace_back();
  }
  common_dialogs.total_count = total_count;
}

// only removes the Dialog from the dialog list, but changes nothing in the corresponding DialogFilter
bool MessagesManager::set_dialog_is_pinned(DialogListId dialog_list_id, Dialog *d, bool is_pinned,
                                           bool need_update_dialog_lists) {
  if (td_->auth_manager_->is_bot()) {
    return false;
  }

  CHECK(d != nullptr);
  if (d->order == DEFAULT_ORDER && is_pinned) {
    // the chat can't be pinned
    return false;
  }

  auto positions = get_dialog_positions(d);
  auto *list = get_dialog_list(dialog_list_id);
  if (list == nullptr) {
    return false;
  }
  if (!list->are_pinned_dialogs_inited_) {
    return false;
  }
  bool was_pinned = false;
  for (size_t pos = 0; pos < list->pinned_dialogs_.size(); pos++) {
    auto &pinned_dialog = list->pinned_dialogs_[pos];
    if (pinned_dialog.get_dialog_id() == d->dialog_id) {
      // the dialog was already pinned
      if (is_pinned) {
        if (pos == 0) {
          return false;
        }
        auto order = get_next_pinned_dialog_order();
        pinned_dialog = DialogDate(order, d->dialog_id);
        std::rotate(list->pinned_dialogs_.begin(), list->pinned_dialogs_.begin() + pos,
                    list->pinned_dialogs_.begin() + pos + 1);
        list->pinned_dialog_id_orders_[d->dialog_id] = order;
      } else {
        list->pinned_dialogs_.erase(list->pinned_dialogs_.begin() + pos);
        list->pinned_dialog_id_orders_.erase(d->dialog_id);
      }
      was_pinned = true;
      break;
    }
  }
  if (!was_pinned) {
    if (!is_pinned) {
      return false;
    }
    auto order = get_next_pinned_dialog_order();
    list->pinned_dialogs_.insert(list->pinned_dialogs_.begin(), {order, d->dialog_id});
    list->pinned_dialog_id_orders_.emplace(d->dialog_id, order);
  }

  LOG(INFO) << "Set " << d->dialog_id << " is pinned in " << dialog_list_id << " to " << is_pinned;
  if (dialog_list_id.is_folder() && G()->parameters().use_message_db) {
    G()->td_db()->get_binlog_pmc()->set(
        PSTRING() << "pinned_dialog_ids" << dialog_list_id.get_folder_id().get(),
        implode(transform(list->pinned_dialogs_,
                          [](auto &pinned_dialog) { return PSTRING() << pinned_dialog.get_dialog_id().get(); }),
                ','));
  }

  if (need_update_dialog_lists) {
    update_dialog_lists(d, std::move(positions), true, false, "set_dialog_is_pinned");
  }
  return true;
}

void MessagesManager::remove_message_notifications(DialogId dialog_id, NotificationGroupId group_id,
                                                   NotificationId max_notification_id, MessageId max_message_id) {
  Dialog *d = get_dialog_force(dialog_id, "remove_message_notifications");
  if (d == nullptr) {
    LOG(ERROR) << "Can't find " << dialog_id;
    return;
  }
  if (d->message_notification_group.group_id != group_id && d->mention_notification_group.group_id != group_id) {
    LOG(ERROR) << "There is no " << group_id << " in " << dialog_id;
    return;
  }
  if (!max_notification_id.is_valid()) {
    return;
  }
  CHECK(!max_message_id.is_scheduled());

  bool from_mentions = d->mention_notification_group.group_id == group_id;
  if (d->new_secret_chat_notification_id.is_valid()) {
    if (!from_mentions && d->new_secret_chat_notification_id.get() <= max_notification_id.get()) {
      return remove_new_secret_chat_notification(d, false);
    }
    return;
  }
  auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;
  if (max_notification_id.get() <= group_info.max_removed_notification_id.get()) {
    return;
  }
  if (max_message_id > group_info.max_removed_message_id) {
    VLOG(notifications) << "Set max_removed_message_id in " << group_info.group_id << '/' << dialog_id << " to "
                        << max_message_id;
    group_info.max_removed_message_id = max_message_id.get_prev_server_message_id();
  }

  VLOG(notifications) << "Set max_removed_notification_id in " << group_info.group_id << '/' << dialog_id << " to "
                      << max_notification_id;
  group_info.max_removed_notification_id = max_notification_id;
  on_dialog_updated(dialog_id, "remove_message_notifications");

  if (group_info.last_notification_id.is_valid() &&
      max_notification_id.get() >= group_info.last_notification_id.get()) {
    bool is_changed =
        set_dialog_last_notification(dialog_id, group_info, 0, NotificationId(), "remove_message_notifications");
    CHECK(is_changed);
  }
}

void MessagesManager::on_get_dialog_message_by_date_success(DialogId dialog_id, int32 date, int64 random_id,
                                                            vector<tl_object_ptr<telegram_api::Message>> &&messages,
                                                            Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  auto it = get_dialog_message_by_date_results_.find(random_id);
  CHECK(it != get_dialog_message_by_date_results_.end());
  auto &result = it->second;
  CHECK(result == FullMessageId());

  for (auto &message : messages) {
    auto message_date = get_message_date(message);
    auto message_dialog_id = get_message_dialog_id(message);
    if (message_dialog_id != dialog_id) {
      LOG(ERROR) << "Receive message in wrong " << message_dialog_id << " instead of " << dialog_id;
      continue;
    }
    if (message_date != 0 && message_date <= date) {
      result = on_get_message(std::move(message), false, dialog_id.get_type() == DialogType::Channel, false, false,
                              false, "on_get_dialog_message_by_date_success");
      if (result != FullMessageId()) {
        const Dialog *d = get_dialog(dialog_id);
        CHECK(d != nullptr);
        auto message_id = find_message_by_date(d->messages.get(), date);
        if (!message_id.is_valid()) {
          LOG(ERROR) << "Failed to find " << result.get_message_id() << " in " << dialog_id << " by date " << date;
          message_id = result.get_message_id();
        }
        get_dialog_message_by_date_results_[random_id] = {dialog_id, message_id};
        // TODO result must be adjusted by local messages
        promise.set_value(Unit());
        return;
      }
    }
  }
  promise.set_value(Unit());
}

void MessagesManager::on_get_recommended_dialog_filters(
    Result<vector<tl_object_ptr<telegram_api::dialogFilterSuggested>>> result,
    Promise<td_api::object_ptr<td_api::recommendedChatFilters>> &&promise) {
  if (result.is_error()) {
    return promise.set_error(result.move_as_error());
  }
  CHECK(!td_->auth_manager_->is_bot());
  auto suggested_filters = result.move_as_ok();

  MultiPromiseActorSafe mpas{"LoadRecommendedFiltersMultiPromiseActor"};
  mpas.add_promise(Promise<Unit>());
  auto lock = mpas.get_promise();

  vector<RecommendedDialogFilter> filters;
  for (auto &suggested_filter : suggested_filters) {
    RecommendedDialogFilter filter;
    filter.dialog_filter = DialogFilter::get_dialog_filter(std::move(suggested_filter->filter_), false);
    CHECK(filter.dialog_filter != nullptr);
    filter.dialog_filter->dialog_filter_id = DialogFilterId();  // just in case
    load_dialog_filter(filter.dialog_filter.get(), false, mpas.get_promise());

    filter.description = std::move(suggested_filter->description_);
    filters.push_back(std::move(filter));
  }

  mpas.add_promise(PromiseCreator::lambda([actor_id = actor_id(this), filters = std::move(filters),
                                           promise = std::move(promise)](Result<Unit> &&result) mutable {
    send_closure(actor_id, &MessagesManager::on_load_recommended_dialog_filters, std::move(result), std::move(filters),
                 std::move(promise));
  }));
  lock.set_value(Unit());
}

bool MessagesManager::on_update_scheduled_message_id(int64 random_id, ScheduledServerMessageId new_message_id,
                                                     const string &source) {
  if (!new_message_id.is_valid()) {
    LOG(ERROR) << "Receive " << new_message_id << " in updateMessageId with random_id " << random_id << " from "
               << source;
    return false;
  }

  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    LOG(ERROR) << "Receive not send outgoing " << new_message_id << " with random_id = " << random_id;
    return false;
  }

  auto dialog_id = it->second.get_dialog_id();
  auto old_message_id = it->second.get_message_id();

  being_sent_messages_.erase(it);

  if (!have_message_force({dialog_id, old_message_id}, "on_update_scheduled_message_id")) {
    delete_sent_message_on_server(dialog_id, MessageId(new_message_id, std::numeric_limits<int32>::max()));
    return true;
  }

  LOG(INFO) << "Save correspondence from " << new_message_id << " in " << dialog_id << " to " << old_message_id;
  CHECK(old_message_id.is_yet_unsent());
  update_scheduled_message_ids_[dialog_id][new_message_id] = old_message_id;
  return true;
}

void MessagesManager::finish_delete_secret_messages(DialogId dialog_id, std::vector<int64> random_ids,
                                                    Promise<> promise) {
  LOG(INFO) << "Delete messages with random_ids " << random_ids << " in " << dialog_id;
  promise.set_value(Unit());  // TODO: set after event is saved

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  vector<MessageId> to_delete_message_ids;
  for (auto &random_id : random_ids) {
    auto message_id = get_message_id_by_random_id(d, random_id, "delete_secret_messages");
    if (!message_id.is_valid()) {
      LOG(INFO) << "Can't find message with random_id " << random_id;
      continue;
    }
    const Message *m = get_message(d, message_id);
    CHECK(m != nullptr);
    if (!is_service_message_content(m->content->get_type())) {
      to_delete_message_ids.push_back(message_id);
    } else {
      LOG(INFO) << "Skip deletion of service " << message_id;
    }
  }
  delete_dialog_messages(dialog_id, to_delete_message_ids, true, false, "finish_delete_secret_messages");
}

void MessagesManager::on_update_dialog_online_member_count(DialogId dialog_id, int32 online_member_count,
                                                           bool is_from_server) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive number of online members in invalid " << dialog_id;
    return;
  }

  if (is_broadcast_channel(dialog_id)) {
    LOG_IF(ERROR, online_member_count != 0)
        << "Receive " << online_member_count << " as a number of online members in a channel " << dialog_id;
    return;
  }

  if (online_member_count < 0) {
    LOG(ERROR) << "Receive " << online_member_count << " as a number of online members in a " << dialog_id;
    return;
  }

  set_dialog_online_member_count(dialog_id, online_member_count, is_from_server,
                                 "on_update_channel_online_member_count");
}

void MessagesManager::on_resolve_secret_chat_message_via_bot_username(const string &via_bot_username,
                                                                      MessageInfo *message_info_ptr,
                                                                      Promise<Unit> &&promise) {
  if (!G()->close_flag()) {
    auto dialog_id = resolve_dialog_username(via_bot_username);
    if (dialog_id.is_valid() && dialog_id.get_type() == DialogType::User) {
      auto user_id = dialog_id.get_user_id();
      auto r_bot_data = td_->contacts_manager_->get_bot_data(user_id);
      if (r_bot_data.is_ok() && r_bot_data.ok().is_inline) {
        message_info_ptr->flags |= MESSAGE_FLAG_IS_SENT_VIA_BOT;
        message_info_ptr->via_bot_user_id = user_id;
      }
    }
  }
  promise.set_value(Unit());
}

MessageId MessagesManager::get_first_database_message_id_by_index(const Dialog *d, MessageSearchFilter filter) {
  CHECK(d != nullptr);
  auto message_id = filter == MessageSearchFilter::Empty
                        ? d->first_database_message_id
                        : d->first_database_message_id_by_index[message_search_filter_index(filter)];
  CHECK(!message_id.is_scheduled());
  if (!message_id.is_valid()) {
    if (d->dialog_id.get_type() == DialogType::SecretChat) {
      LOG(ERROR) << "Invalid first_database_message_id_by_index in " << d->dialog_id;
      return MessageId::min();
    }
    return MessageId::max();
  }
  return message_id;
}

void MessagesManager::repair_dialog_action_bar(Dialog *d, const char *source) {
  CHECK(d != nullptr);
  auto dialog_id = d->dialog_id;
  d->need_repair_action_bar = true;
  if (have_input_peer(dialog_id, AccessRights::Read)) {
    create_actor<SleepActor>(
        "RepairChatActionBarActor", 1.0,
        PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, source](Result<Unit> result) {
          send_closure(actor_id, &MessagesManager::reget_dialog_action_bar, dialog_id, source, true);
        }))
        .release();
  }
  // there is no need to change action bar
  on_dialog_updated(dialog_id, source);
}

void MessagesManager::send_update_delete_messages(DialogId dialog_id, vector<int64> &&message_ids, bool is_permanent,
                                                  bool from_cache) const {
  if (message_ids.empty()) {
    return;
  }

  LOG_CHECK(have_dialog(dialog_id)) << "Wrong " << dialog_id << " in send_update_delete_messages";
  send_closure(
      G()->td(), &Td::send_update,
      make_tl_object<td_api::updateDeleteMessages>(dialog_id.get(), std::move(message_ids), is_permanent, from_cache));
}

void MessagesManager::ttl_period_register_message(DialogId dialog_id, const Message *m, double server_time) {
  CHECK(m != nullptr);
  CHECK(m->ttl_period != 0);
  CHECK(!m->message_id.is_scheduled());

  auto it_flag = ttl_nodes_.emplace(dialog_id, m->message_id, true);
  CHECK(it_flag.second);
  auto it = it_flag.first;

  auto now = Time::now();
  ttl_heap_.insert(now + (m->date + m->ttl_period - server_time), it->as_heap_node());
  ttl_update_timeout(now);
}

void MessagesManager::ttl_register_message(DialogId dialog_id, const Message *m, double now) {
  CHECK(m != nullptr);
  CHECK(m->ttl_expires_at != 0);
  CHECK(!m->message_id.is_scheduled());

  auto it_flag = ttl_nodes_.emplace(dialog_id, m->message_id, false);
  CHECK(it_flag.second);
  auto it = it_flag.first;

  ttl_heap_.insert(m->ttl_expires_at, it->as_heap_node());
  ttl_update_timeout(now);
}

void MessagesManager::load_messages(DialogId dialog_id, MessageId from_message_id, int32 offset, int32 limit,
                                    int left_tries, bool only_local, Promise<Unit> &&promise) {
  load_messages_impl(get_dialog(dialog_id), from_message_id, offset, limit, left_tries, only_local, std::move(promise));
}

void MessagesManager::remove_dialog_from_list(Dialog *d, DialogListId dialog_list_id) {
  LOG(INFO) << "Remove " << d->dialog_id << " from " << dialog_list_id;
  bool is_removed = td::remove(d->dialog_list_ids, dialog_list_id);
  CHECK(is_removed);
}

bool MessagesManager::have_message_force(FullMessageId full_message_id, const char *source) {
  return get_message_force(full_message_id, source) != nullptr;
}
}  // namespace td
