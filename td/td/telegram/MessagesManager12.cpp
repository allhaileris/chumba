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
#include "td/telegram/SecretChatsManager.h"
#include "td/telegram/SequenceDispatcher.h"

#include "td/telegram/Td.h"

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




#include <tuple>



#include <utility>

namespace td {
}  // namespace td
namespace td {

class SendSecretMessageActor final : public NetActor {
  int64 random_id_;

 public:
  void send(DialogId dialog_id, int64 reply_to_random_id, int32 ttl, const string &text, SecretInputMedia media,
            vector<tl_object_ptr<secret_api::MessageEntity>> &&entities, UserId via_bot_user_id, int64 media_album_id,
            bool disable_notification, int64 random_id) {
    if (false && !media.empty()) {
      td_->messages_manager_->on_send_secret_message_error(random_id, Status::Error(400, "FILE_PART_1_MISSING"),
                                                           Auto());
      stop();
      return;
    }

    CHECK(dialog_id.get_type() == DialogType::SecretChat);
    random_id_ = random_id;

    int32 flags = 0;
    if (reply_to_random_id != 0) {
      flags |= secret_api::decryptedMessage::REPLY_TO_RANDOM_ID_MASK;
    }
    if (via_bot_user_id.is_valid()) {
      flags |= secret_api::decryptedMessage::VIA_BOT_NAME_MASK;
    }
    if (!media.empty()) {
      flags |= secret_api::decryptedMessage::MEDIA_MASK;
    }
    if (!entities.empty()) {
      flags |= secret_api::decryptedMessage::ENTITIES_MASK;
    }
    if (media_album_id != 0) {
      CHECK(media_album_id < 0);
      flags |= secret_api::decryptedMessage::GROUPED_ID_MASK;
    }
    if (disable_notification) {
      flags |= secret_api::decryptedMessage::SILENT_MASK;
    }

    send_closure(
        G()->secret_chats_manager(), &SecretChatsManager::send_message, dialog_id.get_secret_chat_id(),
        make_tl_object<secret_api::decryptedMessage>(
            flags, false /*ignored*/, random_id, ttl, text, std::move(media.decrypted_media_), std::move(entities),
            td_->contacts_manager_->get_user_username(via_bot_user_id), reply_to_random_id, -media_album_id),
        std::move(media.input_file_), PromiseCreator::event(self_closure(this, &SendSecretMessageActor::done)));
  }

  void done() {
    stop();
  }
};

class SendMessageActor final : public NetActorOnce {
  int64 random_id_;
  DialogId dialog_id_;

 public:
  void send(int32 flags, DialogId dialog_id, tl_object_ptr<telegram_api::InputPeer> as_input_peer,
            MessageId reply_to_message_id, int32 schedule_date, tl_object_ptr<telegram_api::ReplyMarkup> &&reply_markup,
            vector<tl_object_ptr<telegram_api::MessageEntity>> &&entities, const string &text, int64 random_id,
            NetQueryRef *send_query_ref, uint64 sequence_dispatcher_id) {
    random_id_ = random_id;
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      on_error(Status::Error(400, "Have no write access to the chat"));
      stop();
      return;
    }

    if (!entities.empty()) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_ENTITIES;
    }
    if (as_input_peer != nullptr) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_SEND_AS;
    }

    auto query = G()->net_query_creator().create(telegram_api::messages_sendMessage(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, std::move(input_peer),
        reply_to_message_id.get_server_message_id().get(), text, random_id, std::move(reply_markup),
        std::move(entities), schedule_date, std::move(as_input_peer)));
    if (G()->shared_config().get_option_boolean("use_quick_ack")) {
      query->quick_ack_promise_ = PromiseCreator::lambda(
          [random_id](Unit) {
            send_closure(G()->messages_manager(), &MessagesManager::on_send_message_get_quick_ack, random_id);
          },
          PromiseCreator::Ignore());
    }
    *send_query_ref = query.get_weak();
    query->debug("send to MessagesManager::MultiSequenceDispatcher");
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_sendMessage>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SendMessage for " << random_id_ << ": " << to_string(ptr);

    auto constructor_id = ptr->get_id();
    if (constructor_id != telegram_api::updateShortSentMessage::ID) {
      td_->messages_manager_->check_send_message_result(random_id_, dialog_id_, ptr.get(), "SendMessage");
      return td_->updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
    }
    auto sent_message = move_tl_object_as<telegram_api::updateShortSentMessage>(ptr);
    td_->messages_manager_->on_update_sent_text_message(random_id_, std::move(sent_message->media_),
                                                        std::move(sent_message->entities_));

    auto message_id = MessageId(ServerMessageId(sent_message->id_));
    auto ttl_period = (sent_message->flags_ & telegram_api::updateShortSentMessage::TTL_PERIOD_MASK) != 0
                          ? sent_message->ttl_period_
                          : 0;
    auto update = make_tl_object<updateSentMessage>(random_id_, message_id, sent_message->date_, ttl_period);
    if (dialog_id_.get_type() == DialogType::Channel) {
      td_->messages_manager_->add_pending_channel_update(dialog_id_, std::move(update), sent_message->pts_,
                                                         sent_message->pts_count_, Promise<Unit>(),
                                                         "send message actor");
      return;
    }

    td_->updates_manager_->add_pending_pts_update(std::move(update), sent_message->pts_, sent_message->pts_count_,
                                                  Time::now(), Promise<Unit>(), "send message actor");
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for SendMessage: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, message will be re-sent
      return;
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendMessageActor");
    td_->messages_manager_->on_send_message_fail(random_id_, std::move(status));
  }
};

void MessagesManager::on_secret_message_media_uploaded(DialogId dialog_id, const Message *m,
                                                       SecretInputMedia &&secret_input_media, FileId file_id,
                                                       FileId thumbnail_file_id) {
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());
  CHECK(!secret_input_media.empty());
  /*
  if (m->media_album_id != 0) {
    switch (secret_input_media->input_file_->get_id()) {
      case telegram_api::inputEncryptedFileUploaded::ID:
      case telegram_api::inputEncryptedFileBigUploaded::ID:
        LOG(INFO) << "Upload media from " << m->message_id << " in " << dialog_id;
        return td_->create_handler<UploadEncryptedMediaQuery>()->send(dialog_id, m->message_id, std::move(secret_input_media));
      case telegram_api::inputEncryptedFile::ID:
        return send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id,
                           dialog_id, m->message_id, Status::OK());
      default:
        LOG(ERROR) << "Have wrong secret input media " << to_string(secret_input_media->input_file_);
        return send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id,
                           dialog_id, m->message_id, Status::Error(400, "Invalid input media"));
  }
  */
  // TODO use file_id, thumbnail_file_id, was_uploaded, was_thumbnail_uploaded,
  // invalidate partial remote location for file_id in case of failed upload even message has already been deleted
  send_closure_later(
      actor_id(this), &MessagesManager::on_media_message_ready_to_send, dialog_id, m->message_id,
      PromiseCreator::lambda(
          [this, dialog_id, secret_input_media = std::move(secret_input_media)](Result<Message *> result) mutable {
            if (result.is_error() || G()->close_flag()) {
              return;
            }

            auto m = result.move_as_ok();
            CHECK(m != nullptr);
            CHECK(!secret_input_media.empty());
            LOG(INFO) << "Send secret media from " << m->message_id << " in " << dialog_id << " in reply to "
                      << m->reply_to_message_id;
            int64 random_id = begin_send_message(dialog_id, m);
            auto layer = td_->contacts_manager_->get_secret_chat_layer(dialog_id.get_secret_chat_id());
            auto caption = get_message_content_caption(m->content.get());
            vector<tl_object_ptr<secret_api::MessageEntity>> entities;
            if (caption != nullptr && !caption->entities.empty()) {
              entities = get_input_secret_message_entities(caption->entities, layer);
            }
            send_closure(td_->create_net_actor<SendSecretMessageActor>(), &SendSecretMessageActor::send, dialog_id,
                         m->reply_to_random_id, m->ttl, "", std::move(secret_input_media), std::move(entities),
                         m->via_bot_user_id, m->media_album_id, m->disable_notification, random_id);
          }));
}

void MessagesManager::on_text_message_ready_to_send(DialogId dialog_id, MessageId message_id) {
  LOG(INFO) << "Ready to send " << message_id << " to " << dialog_id;

  auto m = get_message({dialog_id, message_id});
  if (m == nullptr) {
    return;
  }

  CHECK(message_id.is_yet_unsent());

  auto content = m->content.get();
  CHECK(content != nullptr);
  auto content_type = content->get_type();

  const FormattedText *message_text = get_message_content_text(content);
  CHECK(message_text != nullptr);

  int64 random_id = begin_send_message(dialog_id, m);
  if (dialog_id.get_type() == DialogType::SecretChat) {
    CHECK(!message_id.is_scheduled());
    auto layer = td_->contacts_manager_->get_secret_chat_layer(dialog_id.get_secret_chat_id());
    send_closure(td_->create_net_actor<SendSecretMessageActor>(), &SendSecretMessageActor::send, dialog_id,
                 m->reply_to_random_id, m->ttl, message_text->text,
                 get_secret_input_media(content, td_, nullptr, BufferSlice()),
                 get_input_secret_message_entities(message_text->entities, layer), m->via_bot_user_id,
                 m->media_album_id, m->disable_notification, random_id);
  } else {
    send_closure(td_->create_net_actor<SendMessageActor>(), &SendMessageActor::send, get_message_flags(m), dialog_id,
                 get_send_message_as_input_peer(m), m->reply_to_message_id, get_message_schedule_date(m),
                 get_input_reply_markup(m->reply_markup),
                 get_input_message_entities(td_->contacts_manager_.get(), message_text->entities, "do_send_message"),
                 message_text->text, random_id, &m->send_query_ref,
                 get_sequence_dispatcher_id(dialog_id, content_type));
  }
}

class SaveDraftMessageQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit SaveDraftMessageQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, const unique_ptr<DraftMessage> &draft_message) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      LOG(INFO) << "Can't update draft message because have no write access to " << dialog_id;
      return on_error(Status::Error(400, "Can't save draft message"));
    }

    int32 flags = 0;
    ServerMessageId reply_to_message_id;
    if (draft_message != nullptr) {
      if (draft_message->reply_to_message_id.is_valid() && draft_message->reply_to_message_id.is_server()) {
        reply_to_message_id = draft_message->reply_to_message_id.get_server_message_id();
        flags |= MessagesManager::SEND_MESSAGE_FLAG_IS_REPLY;
      }
      if (draft_message->input_message_text.disable_web_page_preview) {
        flags |= MessagesManager::SEND_MESSAGE_FLAG_DISABLE_WEB_PAGE_PREVIEW;
      }
      if (!draft_message->input_message_text.text.entities.empty()) {
        flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_ENTITIES;
      }
    }

    send_query(G()->net_query_creator().create(telegram_api::messages_saveDraft(
        flags, false /*ignored*/, reply_to_message_id.get(), std::move(input_peer),
        draft_message == nullptr ? "" : draft_message->input_message_text.text.text,
        draft_message == nullptr
            ? vector<tl_object_ptr<telegram_api::MessageEntity>>()
            : get_input_message_entities(td_->contacts_manager_.get(), draft_message->input_message_text.text.entities,
                                         "SaveDraftMessageQuery"))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_saveDraft>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.ok();
    if (!result) {
      on_error(Status::Error(400, "Save draft failed"));
    }

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SaveDraftMessageQuery")) {
      LOG(ERROR) << "Receive error for SaveDraftMessageQuery: " << status;
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::save_dialog_draft_message_on_server(DialogId dialog_id) {
  if (G()->close_flag()) {
    return;
  }

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  Promise<> promise;
  if (d->save_draft_message_log_event_id.log_event_id != 0) {
    d->save_draft_message_log_event_id.generation++;
    promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id,
                                      generation = d->save_draft_message_log_event_id.generation](Result<Unit> result) {
      if (!G()->close_flag()) {
        send_closure(actor_id, &MessagesManager::on_saved_dialog_draft_message, dialog_id, generation);
      }
    });
  }

  // TODO do not send two queries simultaneously or use SequenceDispatcher
  td_->create_handler<SaveDraftMessageQuery>(std::move(promise))->send(dialog_id, d->draft_message);
}

void MessagesManager::on_get_message_calendar_from_database(int64 random_id, DialogId dialog_id,
                                                            MessageId from_message_id, MessageId first_db_message_id,
                                                            MessageSearchFilter filter,
                                                            Result<MessagesDbCalendar> r_calendar,
                                                            Promise<Unit> promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (r_calendar.is_error()) {
    LOG(ERROR) << "Failed to get message calendar from the database: " << r_calendar.error();
    if (first_db_message_id != MessageId::min() && dialog_id.get_type() != DialogType::SecretChat &&
        filter != MessageSearchFilter::FailedToSend) {
      found_dialog_message_calendars_.erase(random_id);
    }
    return promise.set_value(Unit());
  }
  CHECK(!from_message_id.is_scheduled());
  CHECK(!first_db_message_id.is_scheduled());

  auto calendar = r_calendar.move_as_ok();

  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  auto it = found_dialog_message_calendars_.find(random_id);
  CHECK(it != found_dialog_message_calendars_.end());
  CHECK(it->second == nullptr);

  vector<std::pair<MessageId, int32>> periods;
  periods.reserve(calendar.messages.size());
  for (size_t i = 0; i < calendar.messages.size(); i++) {
    auto m = on_get_message_from_database(d, calendar.messages[i], false, "on_get_message_calendar_from_database");
    if (m != nullptr && first_db_message_id <= m->message_id) {
      CHECK(!m->message_id.is_scheduled());
      periods.emplace_back(m->message_id, calendar.total_counts[i]);
    }
  }

  if (periods.empty() && first_db_message_id != MessageId::min() && dialog_id.get_type() != DialogType::SecretChat) {
    LOG(INFO) << "No messages found in database";
    found_dialog_message_calendars_.erase(it);
  } else {
    auto total_count = d->message_count_by_index[message_search_filter_index(filter)];
    vector<td_api::object_ptr<td_api::messageCalendarDay>> days;
    for (auto &period : periods) {
      const auto *m = get_message(d, period.first);
      CHECK(m != nullptr);
      days.push_back(td_api::make_object<td_api::messageCalendarDay>(
          period.second, get_message_object(dialog_id, m, "on_get_message_calendar_from_database")));
    }
    it->second = td_api::make_object<td_api::messageCalendar>(total_count, std::move(days));
  }
  promise.set_value(Unit());
}

class GetPinnedDialogsActor final : public NetActorOnce {
  FolderId folder_id_;
  Promise<Unit> promise_;

 public:
  explicit GetPinnedDialogsActor(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  NetQueryRef send(FolderId folder_id, uint64 sequence_id) {
    folder_id_ = folder_id;
    auto query = G()->net_query_creator().create(telegram_api::messages_getPinnedDialogs(folder_id.get()));
    auto result = query.get_weak();
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_id);
    return result;
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_getPinnedDialogs>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto result = result_ptr.move_as_ok();
    LOG(INFO) << "Receive pinned chats in " << folder_id_ << ": " << to_string(result);

    td_->contacts_manager_->on_get_users(std::move(result->users_), "GetPinnedDialogsActor");
    td_->contacts_manager_->on_get_chats(std::move(result->chats_), "GetPinnedDialogsActor");
    td_->messages_manager_->on_get_dialogs(folder_id_, std::move(result->dialogs_), -2, std::move(result->messages_),
                                           std::move(promise_));
  }

  void on_error(Status status) final {
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::reload_pinned_dialogs(DialogListId dialog_list_id, Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());
  CHECK(!td_->auth_manager_->is_bot());

  if (dialog_list_id.is_folder()) {
    send_closure(td_->create_net_actor<GetPinnedDialogsActor>(std::move(promise)), &GetPinnedDialogsActor::send,
                 dialog_list_id.get_folder_id(), get_sequence_dispatcher_id(DialogId(), MessageContentType::None));
  } else if (dialog_list_id.is_filter()) {
    schedule_dialog_filters_reload(0.0);
    dialog_filter_reload_queries_.push_back(std::move(promise));
  }
}

void MessagesManager::import_messages(DialogId dialog_id, const td_api::object_ptr<td_api::InputFile> &message_file,
                                      const vector<td_api::object_ptr<td_api::InputFile>> &attached_files,
                                      Promise<Unit> &&promise) {
  TRY_STATUS_PROMISE(promise, can_import_messages(dialog_id));

  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Document, message_file, dialog_id, false, false);
  if (r_file_id.is_error()) {
    // TODO TRY_RESULT_PROMISE(promise, ...);
    return promise.set_error(Status::Error(400, r_file_id.error().message()));
  }
  FileId file_id = r_file_id.ok();

  vector<FileId> attached_file_ids;
  attached_file_ids.reserve(attached_files.size());
  for (auto &attached_file : attached_files) {
    auto file_type = td_->file_manager_->guess_file_type(attached_file);
    if (file_type != FileType::Animation && file_type != FileType::Audio && file_type != FileType::Document &&
        file_type != FileType::Photo && file_type != FileType::Sticker && file_type != FileType::Video &&
        file_type != FileType::VoiceNote) {
      LOG(INFO) << "Skip attached file of type " << file_type;
      continue;
    }
    auto r_attached_file_id = td_->file_manager_->get_input_file_id(file_type, attached_file, dialog_id, false, false);
    if (r_attached_file_id.is_error()) {
      // TODO TRY_RESULT_PROMISE(promise, ...);
      return promise.set_error(Status::Error(400, r_attached_file_id.error().message()));
    }
    attached_file_ids.push_back(r_attached_file_id.ok());
  }

  upload_imported_messages(dialog_id, td_->file_manager_->dup_file_id(file_id), std::move(attached_file_ids), false,
                           std::move(promise));
}

void MessagesManager::set_dialog_is_marked_as_unread(Dialog *d, bool is_marked_as_unread) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  CHECK(d != nullptr);
  CHECK(d->is_marked_as_unread != is_marked_as_unread);
  d->is_marked_as_unread = is_marked_as_unread;
  on_dialog_updated(d->dialog_id, "set_dialog_is_marked_as_unread");

  LOG(INFO) << "Set " << d->dialog_id << " is marked as unread to " << is_marked_as_unread;
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in set_dialog_is_marked_as_unread";
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatIsMarkedAsUnread>(d->dialog_id.get(), is_marked_as_unread));

  if (d->server_unread_count + d->local_unread_count == 0 && need_unread_counter(d->order)) {
    int32 delta = d->is_marked_as_unread ? 1 : -1;
    for (auto &list : get_dialog_lists(d)) {
      if (list.is_dialog_unread_count_inited_) {
        list.unread_dialog_total_count_ += delta;
        list.unread_dialog_marked_count_ += delta;
        if (is_dialog_muted(d)) {
          list.unread_dialog_muted_count_ += delta;
          list.unread_dialog_muted_marked_count_ += delta;
        }
        send_update_unread_chat_count(list, d->dialog_id, true, "set_dialog_is_marked_as_unread");
      }
    }

    if (!dialog_filters_.empty()) {
      update_dialog_lists(d, get_dialog_positions(d), true, false, "set_dialog_is_marked_as_unread");
    }
  }
}

void MessagesManager::on_update_channel_too_long(tl_object_ptr<telegram_api::updateChannelTooLong> &&update,
                                                 bool force_apply) {
  ChannelId channel_id(update->channel_id_);
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id << " in updateChannelTooLong";
    return;
  }

  DialogId dialog_id = DialogId(channel_id);
  auto d = get_dialog_force(dialog_id, "on_update_channel_too_long 4");
  if (d == nullptr) {
    auto pts = load_channel_pts(dialog_id);
    if (pts > 0) {
      d = add_dialog(dialog_id, "on_update_channel_too_long 5");
      CHECK(d != nullptr);
      CHECK(d->pts == pts);
      update_dialog_pos(d, "on_update_channel_too_long 6");
    }
  }

  int32 update_pts = (update->flags_ & UPDATE_CHANNEL_TO_LONG_FLAG_HAS_PTS) ? update->pts_ : 0;

  if (d != nullptr) {
    if (update_pts == 0 || update_pts > d->pts) {
      get_channel_difference(dialog_id, d->pts, true, "on_update_channel_too_long 1");
    }
  } else {
    if (force_apply) {
      get_channel_difference(dialog_id, -1, true, "on_update_channel_too_long 2");
    } else {
      td_->updates_manager_->schedule_get_difference("on_update_channel_too_long 3");
    }
  }
}

void MessagesManager::on_yet_unsent_media_queue_updated(DialogId dialog_id) {
  auto queue_id = get_sequence_dispatcher_id(dialog_id, MessageContentType::Photo);
  CHECK(queue_id & 1);
  while (true) {
    auto it = yet_unsent_media_queues_.find(queue_id);
    if (it == yet_unsent_media_queues_.end()) {
      return;
    }
    auto &queue = it->second;
    if (queue.empty()) {
      yet_unsent_media_queues_.erase(it);
      return;
    }
    auto first_it = queue.begin();
    if (!first_it->second) {
      return;
    }

    auto m = get_message({dialog_id, first_it->first});
    auto promise = std::move(first_it->second);
    queue.erase(first_it);
    LOG(INFO) << "Queue for " << dialog_id << " now has size " << queue.size();

    // don't use it/queue/first_it after promise is called
    if (m != nullptr) {
      LOG(INFO) << "Can send " << FullMessageId{dialog_id, m->message_id};
      promise.set_value(std::move(m));
    } else {
      promise.set_error(Status::Error(400, "Message not found"));
    }
  }
}

void MessagesManager::get_message_public_forwards(FullMessageId full_message_id, string offset, int32 limit,
                                                  Promise<td_api::object_ptr<td_api::foundMessages>> &&promise) {
  auto dc_id_promise = PromiseCreator::lambda([actor_id = actor_id(this), full_message_id, offset = std::move(offset),
                                               limit, promise = std::move(promise)](Result<DcId> r_dc_id) mutable {
    if (r_dc_id.is_error()) {
      return promise.set_error(r_dc_id.move_as_error());
    }
    send_closure(actor_id, &MessagesManager::send_get_message_public_forwards_query, r_dc_id.move_as_ok(),
                 full_message_id, std::move(offset), limit, std::move(promise));
  });
  td_->contacts_manager_->get_channel_statistics_dc_id(full_message_id.get_dialog_id(), false,
                                                       std::move(dc_id_promise));
}

tl_object_ptr<td_api::messages> MessagesManager::get_messages_object(int32 total_count,
                                                                     vector<tl_object_ptr<td_api::message>> &&messages,
                                                                     bool skip_not_found) {
  auto message_count = narrow_cast<int32>(messages.size());
  if (total_count < message_count) {
    if (total_count != -1) {
      LOG(ERROR) << "Have wrong total_count = " << total_count << ", while having " << message_count << " messages";
    }
    total_count = message_count;
  }
  if (skip_not_found && td::remove(messages, nullptr)) {
    total_count -= message_count - static_cast<int32>(messages.size());
  }
  return td_api::make_object<td_api::messages>(total_count, std::move(messages));
}

void MessagesManager::create_new_secret_chat(UserId user_id, Promise<SecretChatId> &&promise) {
  auto r_input_user = td_->contacts_manager_->get_input_user(user_id);
  if (r_input_user.is_error()) {
    return promise.set_error(r_input_user.move_as_error());
  }
  if (r_input_user.ok()->get_id() != telegram_api::inputUser::ID) {
    return promise.set_error(Status::Error(400, "Can't create secret chat with self"));
  }
  auto user = static_cast<const telegram_api::inputUser *>(r_input_user.ok().get());

  send_closure(G()->secret_chats_manager(), &SecretChatsManager::create_chat, UserId(user->user_id_),
               user->access_hash_, std::move(promise));
}

void MessagesManager::update_message_max_own_media_timestamp(const Dialog *d, Message *m) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto new_max_own_media_timestamp = get_message_own_max_media_timestamp(m);
  if (m->max_own_media_timestamp == new_max_own_media_timestamp) {
    return;
  }

  LOG(INFO) << "Set max_own_media_timestamp in " << m->message_id << " in " << d->dialog_id << " to "
            << new_max_own_media_timestamp;
  m->max_own_media_timestamp = new_max_own_media_timestamp;

  update_message_max_reply_media_timestamp_in_replied_messages(d->dialog_id, m->message_id);
}

void MessagesManager::update_reply_count_by_message(Dialog *d, int diff, const Message *m) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  if (td_->auth_manager_->is_bot() || !m->top_thread_message_id.is_valid() ||
      m->top_thread_message_id == m->message_id || !m->message_id.is_valid() || !m->message_id.is_server()) {
    return;
  }

  update_message_reply_count(d, m->top_thread_message_id, get_message_sender(m), m->message_id,
                             diff < 0 ? G()->unix_time() : m->date, diff);
}

bool MessagesManager::may_need_message_notification(const Dialog *d, const Message *m) const {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());

  if (is_message_notification_disabled(d, m)) {
    return false;
  }

  if (is_from_mention_notification_group(d, m)) {
    return true;
  }

  bool have_settings;
  int32 mute_until;
  std::tie(have_settings, mute_until) = get_dialog_mute_until(d->dialog_id, d);
  return !have_settings || mute_until <= m->date;
}

void MessagesManager::add_postponed_channel_update(DialogId dialog_id, tl_object_ptr<telegram_api::Update> &&update,
                                                   int32 new_pts, int32 pts_count, Promise<Unit> &&promise) {
  postponed_channel_updates_[dialog_id].emplace(
      new_pts, PendingPtsUpdate(std::move(update), new_pts, pts_count, std::move(promise)));
}

void MessagesManager::get_history(DialogId dialog_id, MessageId from_message_id, int32 offset, int32 limit,
                                  bool from_database, bool only_local, Promise<Unit> &&promise) {
  get_history_impl(get_dialog(dialog_id), from_message_id, offset, limit, from_database, only_local,
                   std::move(promise));
}

vector<DialogFilterId> MessagesManager::get_dialog_filter_ids(const vector<unique_ptr<DialogFilter>> &dialog_filters) {
  return transform(dialog_filters, [](const auto &dialog_filter) { return dialog_filter->dialog_filter_id; });
}

MessagesManager::Message *MessagesManager::get_message(Dialog *d, MessageId message_id) {
  return const_cast<Message *>(get_message(static_cast<const Dialog *>(d), message_id));
}
}  // namespace td
