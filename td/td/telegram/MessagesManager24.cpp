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

void MessagesManager::on_get_dialog_messages_search_result(
    DialogId dialog_id, const string &query, DialogId sender_dialog_id, MessageId from_message_id, int32 offset,
    int32 limit, MessageSearchFilter filter, MessageId top_thread_message_id, int64 random_id, int32 total_count,
    vector<tl_object_ptr<telegram_api::Message>> &&messages, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  LOG(INFO) << "Receive " << messages.size() << " found messages in " << dialog_id;
  if (!dialog_id.is_valid()) {
    CHECK(query.empty());
    CHECK(!sender_dialog_id.is_valid());
    CHECK(!top_thread_message_id.is_valid());
    auto it = found_call_messages_.find(random_id);
    CHECK(it != found_call_messages_.end());

    MessageId first_added_message_id;
    if (messages.empty()) {
      // messages may be empty because there are no more messages or they can't be found due to global limit
      // anyway pretend that there are no more messages
      first_added_message_id = MessageId::min();
    }

    auto &result = it->second.second;
    CHECK(result.empty());
    int32 added_message_count = 0;
    for (auto &message : messages) {
      auto new_full_message_id =
          on_get_message(std::move(message), false, false, false, false, false, "search call messages");
      if (new_full_message_id != FullMessageId()) {
        result.push_back(new_full_message_id);
        added_message_count++;
      }

      auto message_id = new_full_message_id.get_message_id();
      if (message_id < first_added_message_id || !first_added_message_id.is_valid()) {
        first_added_message_id = message_id;
      }
    }
    if (total_count < added_message_count) {
      LOG(ERROR) << "Receive total_count = " << total_count << ", but added " << added_message_count
                 << " messages out of " << messages.size();
      total_count = added_message_count;
    }
    if (G()->parameters().use_message_db) {
      bool update_state = false;

      auto &old_message_count = calls_db_state_.message_count_by_index[call_message_search_filter_index(filter)];
      if (old_message_count != total_count) {
        LOG(INFO) << "Update calls database message count to " << total_count;
        old_message_count = total_count;
        update_state = true;
      }

      auto &old_first_db_message_id =
          calls_db_state_.first_calls_database_message_id_by_index[call_message_search_filter_index(filter)];
      bool from_the_end = !from_message_id.is_valid() || from_message_id >= MessageId::max();
      LOG(INFO) << "Have from_the_end = " << from_the_end << ", old_first_db_message_id = " << old_first_db_message_id
                << ", first_added_message_id = " << first_added_message_id << ", from_message_id = " << from_message_id;
      if ((from_the_end || (old_first_db_message_id.is_valid() && old_first_db_message_id <= from_message_id)) &&
          (!old_first_db_message_id.is_valid() || first_added_message_id < old_first_db_message_id)) {
        LOG(INFO) << "Update calls database first message to " << first_added_message_id;
        old_first_db_message_id = first_added_message_id;
        update_state = true;
      }
      if (update_state) {
        save_calls_db_state();
      }
    }
    it->second.first = total_count;
    promise.set_value(Unit());
    return;
  }

  auto it = found_dialog_messages_.find(random_id);
  CHECK(it != found_dialog_messages_.end());

  auto &result = it->second.second;
  CHECK(result.empty());
  MessageId first_added_message_id;
  if (messages.empty()) {
    // messages may be empty because there are no more messages or they can't be found due to global limit
    // anyway pretend that there are no more messages
    first_added_message_id = MessageId::min();
  }
  bool can_be_in_different_dialog = top_thread_message_id.is_valid() && is_broadcast_channel(dialog_id);
  DialogId real_dialog_id;
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  for (auto &message : messages) {
    auto new_full_message_id = on_get_message(std::move(message), false, dialog_id.get_type() == DialogType::Channel,
                                              false, false, false, "on_get_dialog_messages_search_result");
    if (new_full_message_id == FullMessageId()) {
      total_count--;
      continue;
    }

    if (new_full_message_id.get_dialog_id() != dialog_id) {
      if (!can_be_in_different_dialog) {
        LOG(ERROR) << "Receive " << new_full_message_id << " instead of a message in " << dialog_id;
        total_count--;
        continue;
      } else {
        if (!real_dialog_id.is_valid()) {
          real_dialog_id = new_full_message_id.get_dialog_id();
          found_dialog_messages_dialog_id_[random_id] = real_dialog_id;
        } else if (new_full_message_id.get_dialog_id() != real_dialog_id) {
          LOG(ERROR) << "Receive " << new_full_message_id << " instead of a message in " << real_dialog_id << " or "
                     << dialog_id;
          total_count--;
          continue;
        }
      }
    }

    auto message_id = new_full_message_id.get_message_id();
    if (filter == MessageSearchFilter::UnreadMention && message_id <= d->last_read_all_mentions_message_id &&
        !real_dialog_id.is_valid()) {
      total_count--;
      continue;
    }

    // TODO check that messages are returned in decreasing message_id order
    if (message_id < first_added_message_id || !first_added_message_id.is_valid()) {
      first_added_message_id = message_id;
    }
    result.push_back(message_id);
  }
  if (total_count < static_cast<int32>(result.size())) {
    LOG(ERROR) << "Receive " << result.size() << " valid messages out of " << total_count << " in " << messages.size()
               << " messages";
    total_count = static_cast<int32>(result.size());
  }
  if (query.empty() && !sender_dialog_id.is_valid() && filter != MessageSearchFilter::Empty &&
      !top_thread_message_id.is_valid()) {
    bool from_the_end = !from_message_id.is_valid() ||
                        (d->last_message_id != MessageId() && from_message_id > d->last_message_id) ||
                        from_message_id >= MessageId::max();
    bool update_dialog = false;

    auto &old_message_count = d->message_count_by_index[message_search_filter_index(filter)];
    if (old_message_count != total_count) {
      old_message_count = total_count;
      if (filter == MessageSearchFilter::UnreadMention) {
        d->unread_mention_count = old_message_count;
        update_dialog_mention_notification_count(d);
        send_update_chat_unread_mention_count(d);
      }
      update_dialog = true;
    }

    auto &old_first_database_message_id = d->first_database_message_id_by_index[message_search_filter_index(filter)];
    if ((from_the_end ||
         (old_first_database_message_id.is_valid() && old_first_database_message_id <= from_message_id)) &&
        (!old_first_database_message_id.is_valid() || first_added_message_id < old_first_database_message_id)) {
      old_first_database_message_id = first_added_message_id;
      update_dialog = true;
    }
    if (update_dialog) {
      on_dialog_updated(dialog_id, "search results");
    }

    if (from_the_end && filter == MessageSearchFilter::Pinned) {
      set_dialog_last_pinned_message_id(d, result.empty() ? MessageId() : result[0]);
    }
  }

  it->second.first = total_count;
  promise.set_value(Unit());
}

void MessagesManager::edit_message_media(FullMessageId full_message_id,
                                         tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                         tl_object_ptr<td_api::InputMessageContent> &&input_message_content,
                                         Promise<Unit> &&promise) {
  if (input_message_content == nullptr) {
    return promise.set_error(Status::Error(400, "Can't edit message without new content"));
  }
  int32 new_message_content_type = input_message_content->get_id();
  if (new_message_content_type != td_api::inputMessageAnimation::ID &&
      new_message_content_type != td_api::inputMessageAudio::ID &&
      new_message_content_type != td_api::inputMessageDocument::ID &&
      new_message_content_type != td_api::inputMessagePhoto::ID &&
      new_message_content_type != td_api::inputMessageVideo::ID) {
    return promise.set_error(Status::Error(400, "Unsupported input message content type"));
  }

  LOG(INFO) << "Begin to edit media of " << full_message_id;
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "edit_message_media");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Edit)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  Message *m = get_message_force(d, full_message_id.get_message_id(), "edit_message_media");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (!can_edit_message(dialog_id, m, true)) {
    return promise.set_error(Status::Error(400, "Message can't be edited"));
  }
  CHECK(m->message_id.is_any_server());

  MessageContentType old_message_content_type = m->content->get_type();
  if (old_message_content_type != MessageContentType::Animation &&
      old_message_content_type != MessageContentType::Audio &&
      old_message_content_type != MessageContentType::Document &&
      old_message_content_type != MessageContentType::Photo && old_message_content_type != MessageContentType::Video) {
    return promise.set_error(Status::Error(400, "There is no media in the message to edit"));
  }
  if (m->ttl > 0) {
    return promise.set_error(Status::Error(400, "Can't edit media in self-destructing message"));
  }

  auto r_input_message_content = process_input_message_content(dialog_id, std::move(input_message_content));
  if (r_input_message_content.is_error()) {
    return promise.set_error(r_input_message_content.move_as_error());
  }
  InputMessageContent content = r_input_message_content.move_as_ok();
  if (content.ttl > 0) {
    return promise.set_error(Status::Error(400, "Can't enable self-destruction for media"));
  }

  if (m->media_album_id != 0) {
    auto new_content_type = content.content->get_type();
    if (old_message_content_type != new_content_type) {
      if (!is_allowed_media_group_content(new_content_type)) {
        return promise.set_error(Status::Error(400, "Message content type can't be used in an album"));
      }
      if (is_homogenous_media_group_content(old_message_content_type) ||
          is_homogenous_media_group_content(new_content_type)) {
        return promise.set_error(Status::Error(400, "Can't change media type in the album"));
      }
    }
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false,
                                             has_message_sender_user_id(dialog_id, m));
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  cancel_edit_message_media(dialog_id, m, "Canceled by new editMessageMedia request");

  m->edited_content =
      dup_message_content(td_, dialog_id, content.content.get(), MessageContentDupType::Send, MessageCopyOptions());
  CHECK(m->edited_content != nullptr);
  m->edited_reply_markup = r_new_reply_markup.move_as_ok();
  m->edit_generation = ++current_message_edit_generation_;
  m->edit_promise = std::move(promise);

  do_send_message(dialog_id, m);
}

void MessagesManager::process_channel_update(tl_object_ptr<telegram_api::Update> &&update) {
  switch (update->get_id()) {
    case dummyUpdate::ID:
      LOG(INFO) << "Process dummyUpdate";
      break;
    case updateSentMessage::ID: {
      auto update_sent_message = move_tl_object_as<updateSentMessage>(update);
      LOG(INFO) << "Process updateSentMessage " << update_sent_message->random_id_;
      on_send_message_success(update_sent_message->random_id_, update_sent_message->message_id_,
                              update_sent_message->date_, update_sent_message->ttl_period_, FileId(),
                              "process updateSentChannelMessage");
      break;
    }
    case telegram_api::updateNewChannelMessage::ID: {
      auto update_new_channel_message = move_tl_object_as<telegram_api::updateNewChannelMessage>(update);
      LOG(INFO) << "Process updateNewChannelMessage";
      on_get_message(std::move(update_new_channel_message->message_), true, true, false, true, true,
                     "updateNewChannelMessage");
      break;
    }
    case telegram_api::updateDeleteChannelMessages::ID: {
      auto delete_channel_messages_update = move_tl_object_as<telegram_api::updateDeleteChannelMessages>(update);
      LOG(INFO) << "Process updateDeleteChannelMessages";
      ChannelId channel_id(delete_channel_messages_update->channel_id_);
      if (!channel_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << channel_id;
        break;
      }

      vector<MessageId> message_ids;
      for (auto &message : delete_channel_messages_update->messages_) {
        message_ids.push_back(MessageId(ServerMessageId(message)));
      }

      auto dialog_id = DialogId(channel_id);
      delete_dialog_messages(dialog_id, message_ids, true, false, "updateDeleteChannelMessages");
      break;
    }
    case telegram_api::updateEditChannelMessage::ID: {
      auto update_edit_channel_message = move_tl_object_as<telegram_api::updateEditChannelMessage>(update);
      LOG(INFO) << "Process updateEditChannelMessage";
      auto full_message_id = on_get_message(std::move(update_edit_channel_message->message_), false, true, false, false,
                                            false, "updateEditChannelMessage");
      on_message_edited(full_message_id, update_edit_channel_message->pts_);
      break;
    }
    case telegram_api::updatePinnedChannelMessages::ID: {
      auto pinned_channel_messages_update = move_tl_object_as<telegram_api::updatePinnedChannelMessages>(update);
      LOG(INFO) << "Process updatePinnedChannelMessages";
      ChannelId channel_id(pinned_channel_messages_update->channel_id_);
      if (!channel_id.is_valid()) {
        LOG(ERROR) << "Receive invalid " << channel_id;
        break;
      }

      vector<MessageId> message_ids;
      for (auto &message : pinned_channel_messages_update->messages_) {
        message_ids.push_back(MessageId(ServerMessageId(message)));
      }

      update_dialog_pinned_messages_from_updates(DialogId(channel_id), message_ids,
                                                 pinned_channel_messages_update->pinned_);
      break;
    }
    default:
      UNREACHABLE();
  }
}

void MessagesManager::on_update_dialog_default_send_message_as_dialog_id(DialogId dialog_id,
                                                                         DialogId default_send_as_dialog_id,
                                                                         bool force) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }
  auto dialog_type = dialog_id.get_type();
  if (dialog_type != DialogType::Channel || is_broadcast_channel(dialog_id)) {
    if (default_send_as_dialog_id != DialogId()) {
      LOG(ERROR) << "Receive message sender " << default_send_as_dialog_id << " in " << dialog_id;
    }
    return;
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_default_send_message_as_dialog_id");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (!force) {
    // TODO ignore update if have being sent messages
  }

  if (default_send_as_dialog_id.is_valid()) {
    if (default_send_as_dialog_id.get_type() != DialogType::User) {
      force_create_dialog(default_send_as_dialog_id, "on_update_dialog_default_send_message_as_dialog_id");
    } else if (!td_->contacts_manager_->have_user_force(default_send_as_dialog_id.get_user_id()) ||
               default_send_as_dialog_id != get_my_dialog_id()) {
      default_send_as_dialog_id = DialogId();
    }
  }

  if (d->default_send_message_as_dialog_id != default_send_as_dialog_id) {
    if (force || default_send_as_dialog_id.is_valid() ||
        (created_public_broadcasts_inited_ && !created_public_broadcasts_.empty())) {
      LOG(INFO) << "Set message sender in " << dialog_id << " to " << default_send_as_dialog_id;
      d->need_drop_default_send_message_as_dialog_id = false;
      d->default_send_message_as_dialog_id = default_send_as_dialog_id;
      send_update_chat_message_sender(d);
    } else {
      LOG(INFO) << "Postpone removal of message sender in " << dialog_id;
      d->need_drop_default_send_message_as_dialog_id = true;
    }
    on_dialog_updated(d->dialog_id, "on_update_dialog_default_send_message_as_dialog_id");
  } else if (default_send_as_dialog_id.is_valid() && d->need_drop_default_send_message_as_dialog_id) {
    LOG(INFO) << "Don't remove message sender in " << dialog_id;
    d->need_drop_default_send_message_as_dialog_id = false;
    on_dialog_updated(d->dialog_id, "on_update_dialog_default_send_message_as_dialog_id");
  }
}

unique_ptr<MessagesManager::Message> MessagesManager::do_delete_scheduled_message(Dialog *d, MessageId message_id,
                                                                                  bool is_permanently_deleted,
                                                                                  const char *source) {
  CHECK(d != nullptr);
  LOG_CHECK(message_id.is_valid_scheduled()) << d->dialog_id << ' ' << message_id << ' ' << source;

  unique_ptr<Message> *v = treap_find_message(&d->scheduled_messages, message_id);
  if (*v == nullptr) {
    LOG(INFO) << message_id << " is not found in " << d->dialog_id << " to be deleted from " << source;
    auto message = get_message_force(d, message_id, "do_delete_scheduled_message");
    if (message == nullptr) {
      // currently there may be a race between add_message_to_database and get_message_force,
      // so delete a message from database just in case
      delete_message_from_database(d, message_id, nullptr, is_permanently_deleted);
      return nullptr;
    }

    message_id = message->message_id;
    v = treap_find_message(&d->scheduled_messages, message_id);
    CHECK(*v != nullptr);
  }

  const Message *m = v->get();
  CHECK(m->message_id == message_id);

  LOG(INFO) << "Deleting " << FullMessageId{d->dialog_id, message_id} << " from " << source;

  delete_message_from_database(d, message_id, m, is_permanently_deleted);

  remove_message_file_sources(d->dialog_id, m);

  auto result = treap_delete_message(v);

  if (message_id.is_scheduled_server()) {
    size_t erased_count = d->scheduled_message_date.erase(message_id.get_scheduled_server_message_id());
    CHECK(erased_count != 0);
  }

  cancel_send_deleted_message(d->dialog_id, result.get(), is_permanently_deleted);

  unregister_message_content(td_, result->content.get(), {d->dialog_id, message_id}, "do_delete_scheduled_message");
  unregister_message_reply(d, m);

  return result;
}

void MessagesManager::synchronize_dialog_filters() {
  if (G()->close_flag()) {
    return;
  }
  CHECK(!td_->auth_manager_->is_bot());
  if (are_dialog_filters_being_synchronized_ || are_dialog_filters_being_reloaded_) {
    return;
  }
  if (need_dialog_filters_reload_) {
    return reload_dialog_filters();
  }
  if (!need_synchronize_dialog_filters()) {
    // reload filters to repair their order if the server added new filter to the beginning of the list
    return reload_dialog_filters();
  }

  LOG(INFO) << "Synchronize chat filter changes with server having local " << get_dialog_filter_ids(dialog_filters_)
            << " and server " << get_dialog_filter_ids(server_dialog_filters_);
  for (const auto &server_dialog_filter : server_dialog_filters_) {
    if (get_dialog_filter(server_dialog_filter->dialog_filter_id) == nullptr) {
      return delete_dialog_filter_on_server(server_dialog_filter->dialog_filter_id);
    }
  }

  vector<DialogFilterId> dialog_filter_ids;
  for (const auto &dialog_filter : dialog_filters_) {
    if (dialog_filter->is_empty(true)) {
      continue;
    }

    auto server_dialog_filter = get_server_dialog_filter(dialog_filter->dialog_filter_id);
    if (server_dialog_filter == nullptr || !DialogFilter::are_equivalent(*server_dialog_filter, *dialog_filter)) {
      return update_dialog_filter_on_server(make_unique<DialogFilter>(*dialog_filter));
    }
    dialog_filter_ids.push_back(dialog_filter->dialog_filter_id);
  }

  if (dialog_filter_ids != get_dialog_filter_ids(server_dialog_filters_)) {
    return reorder_dialog_filters_on_server(std::move(dialog_filter_ids));
  }

  UNREACHABLE();
}

class GetSuggestedDialogFiltersQuery final : public Td::ResultHandler {
  Promise<vector<tl_object_ptr<telegram_api::dialogFilterSuggested>>> promise_;

 public:
  explicit GetSuggestedDialogFiltersQuery(Promise<vector<tl_object_ptr<telegram_api::dialogFilterSuggested>>> &&promise)
      : promise_(std::move(promise)) {
  }

  void send() {
    send_query(G()->net_query_creator().create(telegram_api::messages_getSuggestedDialogFilters()));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getSuggestedDialogFilters>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    promise_.set_value(result_ptr.move_as_ok());
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::get_recommended_dialog_filters(
    Promise<td_api::object_ptr<td_api::recommendedChatFilters>> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  auto query_promise =
      PromiseCreator::lambda([actor_id = actor_id(this), promise = std::move(promise)](
                                 Result<vector<tl_object_ptr<telegram_api::dialogFilterSuggested>>> result) mutable {
        send_closure(actor_id, &MessagesManager::on_get_recommended_dialog_filters, std::move(result),
                     std::move(promise));
      });
  td_->create_handler<GetSuggestedDialogFiltersQuery>(std::move(query_promise))->send();
}

void MessagesManager::on_dialog_user_is_contact_updated(DialogId dialog_id, bool is_contact) {
  CHECK(dialog_id.get_type() == DialogType::User);
  auto d = get_dialog(dialog_id);  // called from update_user, must not create the dialog
  if (d != nullptr && d->is_update_new_chat_sent) {
    if (d->know_action_bar) {
      if (is_contact) {
        if (d->action_bar != nullptr && d->action_bar->on_user_contact_added()) {
          send_update_chat_action_bar(d);
        }
      } else {
        repair_dialog_action_bar(d, "on_dialog_user_is_contact_updated");
      }
    }

    if (!dialog_filters_.empty() && d->order != DEFAULT_ORDER) {
      update_dialog_lists(d, get_dialog_positions(d), true, false, "on_dialog_user_is_contact_updated");
      td_->contacts_manager_->for_each_secret_chat_with_user(
          d->dialog_id.get_user_id(), [this](SecretChatId secret_chat_id) {
            DialogId dialog_id(secret_chat_id);
            auto d = get_dialog(dialog_id);  // must not create the dialog
            if (d != nullptr && d->is_update_new_chat_sent && d->order != DEFAULT_ORDER) {
              update_dialog_lists(d, get_dialog_positions(d), true, false, "on_dialog_user_is_contact_updated");
            }
          });
    }
  }
}

void MessagesManager::remove_new_secret_chat_notification(Dialog *d, bool is_permanent) {
  CHECK(d != nullptr);
  auto notification_id = d->new_secret_chat_notification_id;
  CHECK(notification_id.is_valid());
  VLOG(notifications) << "Remove " << notification_id << " about new secret " << d->dialog_id << " from "
                      << d->message_notification_group.group_id;
  d->new_secret_chat_notification_id = NotificationId();
  bool is_fixed = set_dialog_last_notification(d->dialog_id, d->message_notification_group, 0, NotificationId(),
                                               "remove_new_secret_chat_notification");
  CHECK(is_fixed);
  if (is_permanent) {
    CHECK(d->message_notification_group.group_id.is_valid());
    send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification,
                       d->message_notification_group.group_id, notification_id, true, true, Promise<Unit>(),
                       "remove_new_secret_chat_notification");
  }
}

void MessagesManager::update_message_max_reply_media_timestamp_in_replied_messages(DialogId dialog_id,
                                                                                   MessageId reply_to_message_id) {
  if (reply_to_message_id.is_scheduled()) {
    return;
  }
  CHECK(reply_to_message_id.is_valid());

  FullMessageId full_message_id{dialog_id, reply_to_message_id};
  auto it = replied_by_media_timestamp_messages_.find(full_message_id);
  if (it == replied_by_media_timestamp_messages_.end()) {
    return;
  }

  LOG(INFO) << "Update max_reply_media_timestamp for replies of " << reply_to_message_id << " in " << dialog_id;

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  for (auto message_id : it->second) {
    auto m = get_message(d, message_id);
    CHECK(m != nullptr);
    CHECK(m->reply_to_message_id == reply_to_message_id);
    update_message_max_reply_media_timestamp(d, m, true);
  }
}

void MessagesManager::on_update_dialog_is_blocked(DialogId dialog_id, bool is_blocked) {
  if (!dialog_id.is_valid()) {
    LOG(ERROR) << "Receive pinned message in invalid " << dialog_id;
    return;
  }
  if (dialog_id.get_type() == DialogType::User) {
    td_->contacts_manager_->on_update_user_is_blocked(dialog_id.get_user_id(), is_blocked);
  }

  auto d = get_dialog_force(dialog_id, "on_update_dialog_is_blocked");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (d->is_blocked == is_blocked) {
    if (!d->is_is_blocked_inited) {
      CHECK(is_blocked == false);
      d->is_is_blocked_inited = true;
      on_dialog_updated(dialog_id, "on_update_dialog_is_blocked");
    }
    return;
  }

  set_dialog_is_blocked(d, is_blocked);
}

void MessagesManager::send_update_chat_action(DialogId dialog_id, MessageId top_thread_message_id,
                                              DialogId typing_dialog_id, const DialogAction &action) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  LOG(DEBUG) << "Send " << action << " of " << typing_dialog_id << " in thread of " << top_thread_message_id << " in "
             << dialog_id;
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatAction>(
                   dialog_id.get(), top_thread_message_id.get(),
                   get_message_sender_object(td_, typing_dialog_id, "send_update_chat_action"),
                   action.get_chat_action_object()));
}

void MessagesManager::on_delete_dialog_filter(DialogFilterId dialog_filter_id, Status result) {
  CHECK(!td_->auth_manager_->is_bot());
  if (result.is_error()) {
    // TODO rollback dialog_filters_ changes if error isn't 429
  } else {
    for (auto it = server_dialog_filters_.begin(); it != server_dialog_filters_.end(); ++it) {
      if ((*it)->dialog_filter_id == dialog_filter_id) {
        server_dialog_filters_.erase(it);
        save_dialog_filters();
        break;
      }
    }
  }

  are_dialog_filters_being_synchronized_ = false;
  synchronize_dialog_filters();
}

void MessagesManager::save_unread_chat_count(const DialogList &list) {
  LOG(INFO) << "Save unread chat count in " << list.dialog_list_id;
  G()->td_db()->get_binlog_pmc()->set(
      PSTRING() << "unread_dialog_count" << list.dialog_list_id.get(),
      PSTRING() << list.unread_dialog_total_count_ << ' ' << list.unread_dialog_muted_count_ << ' '
                << list.unread_dialog_marked_count_ << ' ' << list.unread_dialog_muted_marked_count_ << ' '
                << list.server_dialog_total_count_ << ' ' << list.secret_chat_total_count_);
}

void MessagesManager::cancel_dialog_action(DialogId dialog_id, const Message *m) {
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || m->forward_info != nullptr || m->had_forward_info ||
      m->via_bot_user_id.is_valid() || m->hide_via_bot || m->is_channel_post || m->message_id.is_scheduled()) {
    return;
  }

  on_dialog_action(dialog_id, MessageId(), get_message_sender(m), DialogAction(), m->date, m->content->get_type());
}

MessagesManager::DialogList *MessagesManager::get_dialog_list(DialogListId dialog_list_id) {
  CHECK(!td_->auth_manager_->is_bot());
  if (dialog_list_id.is_folder() && dialog_list_id.get_folder_id() != FolderId::archive()) {
    dialog_list_id = DialogListId(FolderId::main());
  }
  auto it = dialog_lists_.find(dialog_list_id);
  if (it == dialog_lists_.end()) {
    return nullptr;
  }
  return &it->second;
}

vector<FolderId> MessagesManager::get_dialog_filter_folder_ids(const DialogFilter *filter) {
  CHECK(filter != nullptr);
  if (filter->exclude_archived && filter->pinned_dialog_ids.empty() && filter->included_dialog_ids.empty()) {
    return {FolderId::main()};
  }
  return {FolderId::main(), FolderId::archive()};
}

void MessagesManager::on_read_channel_outbox(ChannelId channel_id, MessageId max_message_id) {
  DialogId dialog_id(channel_id);
  CHECK(!max_message_id.is_scheduled());
  if (max_message_id.is_valid()) {
    read_history_outbox(dialog_id, max_message_id);
  }
}

int32 MessagesManager::get_random_y(MessageId message_id) {
  return static_cast<int32>(static_cast<uint32>(message_id.get() * 2101234567u));
}
}  // namespace td
