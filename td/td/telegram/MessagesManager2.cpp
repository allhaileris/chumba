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

#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogActionBar.h"
#include "td/telegram/DialogDb.h"
#include "td/telegram/DialogFilter.h"
#include "td/telegram/DialogFilter.hpp"
#include "td/telegram/DialogLocation.h"
#include "td/telegram/DraftMessage.h"
#include "td/telegram/DraftMessage.hpp"
#include "td/telegram/FileReferenceManager.h"
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

class SendMediaActor final : public NetActorOnce {
  int64 random_id_ = 0;
  FileId file_id_;
  FileId thumbnail_file_id_;
  DialogId dialog_id_;
  string file_reference_;
  bool was_uploaded_ = false;
  bool was_thumbnail_uploaded_ = false;

 public:
  void send(FileId file_id, FileId thumbnail_file_id, int32 flags, DialogId dialog_id,
            tl_object_ptr<telegram_api::InputPeer> as_input_peer, MessageId reply_to_message_id, int32 schedule_date,
            tl_object_ptr<telegram_api::ReplyMarkup> &&reply_markup,
            vector<tl_object_ptr<telegram_api::MessageEntity>> &&entities, const string &text,
            tl_object_ptr<telegram_api::InputMedia> &&input_media, int64 random_id, NetQueryRef *send_query_ref,
            uint64 sequence_dispatcher_id) {
    random_id_ = random_id;
    file_id_ = file_id;
    thumbnail_file_id_ = thumbnail_file_id;
    dialog_id_ = dialog_id;
    file_reference_ = FileManager::extract_file_reference(input_media);
    was_uploaded_ = FileManager::extract_was_uploaded(input_media);
    was_thumbnail_uploaded_ = FileManager::extract_was_thumbnail_uploaded(input_media);

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      on_error(Status::Error(400, "Have no write access to the chat"));
      stop();
      return;
    }

    if (!entities.empty()) {
      flags |= telegram_api::messages_sendMedia::ENTITIES_MASK;
    }
    if (as_input_peer != nullptr) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_SEND_AS;
    }

    auto query = G()->net_query_creator().create(telegram_api::messages_sendMedia(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, std::move(input_peer),
        reply_to_message_id.get_server_message_id().get(), std::move(input_media), text, random_id,
        std::move(reply_markup), std::move(entities), schedule_date, std::move(as_input_peer)));
    if (G()->shared_config().get_option_boolean("use_quick_ack") && was_uploaded_) {
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
    auto result_ptr = fetch_result<telegram_api::messages_sendMedia>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    if (was_thumbnail_uploaded_) {
      CHECK(thumbnail_file_id_.is_valid());
      // always delete partial remote location for the thumbnail, because it can't be reused anyway
      // TODO delete it only in the case it can't be merged with file thumbnail
      td_->file_manager_->delete_partial_remote_location(thumbnail_file_id_);
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SendMedia for " << random_id_ << ": " << to_string(ptr);
    td_->messages_manager_->check_send_message_result(random_id_, dialog_id_, ptr.get(), "SendMedia");
    td_->updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for SendMedia: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, message will be re-sent
      return;
    }
    if (was_uploaded_) {
      if (was_thumbnail_uploaded_) {
        CHECK(thumbnail_file_id_.is_valid());
        // always delete partial remote location for the thumbnail, because it can't be reused anyway
        td_->file_manager_->delete_partial_remote_location(thumbnail_file_id_);
      }

      CHECK(file_id_.is_valid());
      if (begins_with(status.message(), "FILE_PART_") && ends_with(status.message(), "_MISSING")) {
        td_->messages_manager_->on_send_message_file_part_missing(random_id_,
                                                                  to_integer<int32>(status.message().substr(10)));
        return;
      } else {
        if (status.code() != 429 && status.code() < 500 && !G()->close_flag()) {
          td_->file_manager_->delete_partial_remote_location(file_id_);
        }
      }
    } else if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      if (file_id_.is_valid() && !was_uploaded_) {
        VLOG(file_references) << "Receive " << status << " for " << file_id_;
        td_->file_manager_->delete_file_reference(file_id_, file_reference_);
        td_->messages_manager_->on_send_message_file_reference_error(random_id_);
        return;
      } else {
        LOG(ERROR) << "Receive file reference error, but file_id = " << file_id_
                   << ", was_uploaded = " << was_uploaded_;
      }
    }

    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendMediaActor");
    td_->messages_manager_->on_send_message_fail(random_id_, std::move(status));
  }
};

class UploadMediaQuery final : public Td::ResultHandler {
  DialogId dialog_id_;
  MessageId message_id_;
  FileId file_id_;
  FileId thumbnail_file_id_;
  string file_reference_;
  bool was_uploaded_ = false;
  bool was_thumbnail_uploaded_ = false;

 public:
  void send(DialogId dialog_id, MessageId message_id, FileId file_id, FileId thumbnail_file_id,
            tl_object_ptr<telegram_api::InputMedia> &&input_media) {
    CHECK(input_media != nullptr);
    dialog_id_ = dialog_id;
    message_id_ = message_id;
    file_id_ = file_id;
    thumbnail_file_id_ = thumbnail_file_id;
    file_reference_ = FileManager::extract_file_reference(input_media);
    was_uploaded_ = FileManager::extract_was_uploaded(input_media);
    was_thumbnail_uploaded_ = FileManager::extract_was_thumbnail_uploaded(input_media);

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      return on_error(Status::Error(400, "Have no write access to the chat"));
    }

    send_query(G()->net_query_creator().create(
        telegram_api::messages_uploadMedia(std::move(input_peer), std::move(input_media))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_uploadMedia>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    if (was_thumbnail_uploaded_) {
      CHECK(thumbnail_file_id_.is_valid());
      // always delete partial remote location for the thumbnail, because it can't be reused anyway
      td_->file_manager_->delete_partial_remote_location(thumbnail_file_id_);
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for UploadMediaQuery for " << message_id_ << " in " << dialog_id_ << ": "
              << to_string(ptr);
    td_->messages_manager_->on_upload_message_media_success(dialog_id_, message_id_, std::move(ptr));
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for UploadMediaQuery for " << message_id_ << " in " << dialog_id_ << ": " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, message will be re-sent
      return;
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "UploadMediaQuery");
    if (was_uploaded_) {
      if (was_thumbnail_uploaded_) {
        CHECK(thumbnail_file_id_.is_valid());
        // always delete partial remote location for the thumbnail, because it can't be reused anyway
        td_->file_manager_->delete_partial_remote_location(thumbnail_file_id_);
      }

      CHECK(file_id_.is_valid());
      if (begins_with(status.message(), "FILE_PART_") && ends_with(status.message(), "_MISSING")) {
        td_->messages_manager_->on_upload_message_media_file_part_missing(
            dialog_id_, message_id_, to_integer<int32>(status.message().substr(10)));
        return;
      } else {
        if (status.code() != 429 && status.code() < 500 && !G()->close_flag()) {
          td_->file_manager_->delete_partial_remote_location(file_id_);
        }
      }
    } else if (FileReferenceManager::is_file_reference_error(status)) {
      LOG(ERROR) << "Receive file reference error for UploadMediaQuery";
    }
    td_->messages_manager_->on_upload_message_media_fail(dialog_id_, message_id_, std::move(status));
  }
};

class SendScheduledMessageActor final : public NetActorOnce {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit SendScheduledMessageActor(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, MessageId message_id, uint64 sequence_dispatcher_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Edit);
    if (input_peer == nullptr) {
      on_error(Status::Error(400, "Can't access the chat"));
      stop();
      return;
    }

    int32 server_message_id = message_id.get_scheduled_server_message_id().get();
    auto query = G()->net_query_creator().create(
        telegram_api::messages_sendScheduledMessages(std::move(input_peer), {server_message_id}));

    query->debug("send to MessagesManager::MultiSequenceDispatcher");
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_sendScheduledMessages>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SendScheduledMessageActor: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for SendScheduledMessageActor: " << status;
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendScheduledMessageActor");
    promise_.set_error(std::move(status));
  }
};

class EditMessageActor final : public NetActorOnce {
  Promise<int32> promise_;
  DialogId dialog_id_;

 public:
  explicit EditMessageActor(Promise<Unit> &&promise) {
    promise_ = PromiseCreator::lambda([promise = std::move(promise)](Result<int32> result) mutable {
      if (result.is_error()) {
        promise.set_error(result.move_as_error());
      } else {
        promise.set_value(Unit());
      }
    });
  }
  explicit EditMessageActor(Promise<int32> &&promise) : promise_(std::move(promise)) {
  }

  void send(int32 flags, DialogId dialog_id, MessageId message_id, const string &text,
            vector<tl_object_ptr<telegram_api::MessageEntity>> &&entities,
            tl_object_ptr<telegram_api::InputMedia> &&input_media,
            tl_object_ptr<telegram_api::ReplyMarkup> &&reply_markup, int32 schedule_date,
            uint64 sequence_dispatcher_id) {
    dialog_id_ = dialog_id;

    if (input_media != nullptr && false) {
      on_error(Status::Error(400, "FILE_PART_1_MISSING"));
      stop();
      return;
    }

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Edit);
    if (input_peer == nullptr) {
      on_error(Status::Error(400, "Can't access the chat"));
      stop();
      return;
    }

    if (reply_markup != nullptr) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_REPLY_MARKUP;
    }
    if (!entities.empty()) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_ENTITIES;
    }
    if (!text.empty()) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_MESSAGE;
    }
    if (input_media != nullptr) {
      flags |= telegram_api::messages_editMessage::MEDIA_MASK;
    }
    if (schedule_date != 0) {
      flags |= telegram_api::messages_editMessage::SCHEDULE_DATE_MASK;
    }

    int32 server_message_id = schedule_date != 0 ? message_id.get_scheduled_server_message_id().get()
                                                 : message_id.get_server_message_id().get();
    auto query = G()->net_query_creator().create(telegram_api::messages_editMessage(
        flags, false /*ignored*/, std::move(input_peer), server_message_id, text, std::move(input_media),
        std::move(reply_markup), std::move(entities), schedule_date));

    query->debug("send to MessagesManager::MultiSequenceDispatcher");
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editMessage>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditMessageActor: " << to_string(ptr);
    auto pts = td_->updates_manager_->get_update_edit_message_pts(ptr.get());
    auto promise = PromiseCreator::lambda(
        [promise = std::move(promise_), pts](Result<Unit> result) mutable { promise.set_value(std::move(pts)); });
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise));
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for EditMessage: " << status;
    if (!td_->auth_manager_->is_bot() && status.message() == "MESSAGE_NOT_MODIFIED") {
      return promise_.set_value(0);
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditMessageActor");
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::on_message_media_uploaded(DialogId dialog_id, const Message *m,
                                                tl_object_ptr<telegram_api::InputMedia> &&input_media, FileId file_id,
                                                FileId thumbnail_file_id) {
  CHECK(m != nullptr);
  CHECK(input_media != nullptr);

  auto message_id = m->message_id;
  if (message_id.is_any_server()) {
    const FormattedText *caption = get_message_content_caption(m->edited_content.get());
    auto input_reply_markup = get_input_reply_markup(m->edited_reply_markup);
    bool was_uploaded = FileManager::extract_was_uploaded(input_media);
    bool was_thumbnail_uploaded = FileManager::extract_was_thumbnail_uploaded(input_media);

    LOG(INFO) << "Edit media from " << message_id << " in " << dialog_id;
    auto schedule_date = get_message_schedule_date(m);
    auto promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), dialog_id, message_id, file_id, thumbnail_file_id, schedule_date,
         generation = m->edit_generation, was_uploaded, was_thumbnail_uploaded,
         file_reference = FileManager::extract_file_reference(input_media)](Result<int32> result) mutable {
          send_closure(actor_id, &MessagesManager::on_message_media_edited, dialog_id, message_id, file_id,
                       thumbnail_file_id, was_uploaded, was_thumbnail_uploaded, std::move(file_reference),
                       schedule_date, generation, std::move(result));
        });
    send_closure(td_->create_net_actor<EditMessageActor>(std::move(promise)), &EditMessageActor::send, 1 << 11,
                 dialog_id, message_id, caption == nullptr ? "" : caption->text,
                 get_input_message_entities(td_->contacts_manager_.get(), caption, "edit_message_media"),
                 std::move(input_media), std::move(input_reply_markup), schedule_date,
                 get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
    return;
  }

  if (m->media_album_id == 0) {
    send_closure_later(
        actor_id(this), &MessagesManager::on_media_message_ready_to_send, dialog_id, message_id,
        PromiseCreator::lambda([this, dialog_id, input_media = std::move(input_media), file_id,
                                thumbnail_file_id](Result<Message *> result) mutable {
          if (result.is_error() || G()->close_flag()) {
            return;
          }

          auto m = result.move_as_ok();
          CHECK(m != nullptr);
          CHECK(input_media != nullptr);

          const FormattedText *caption = get_message_content_caption(m->content.get());
          LOG(INFO) << "Send media from " << m->message_id << " in " << dialog_id << " in reply to "
                    << m->reply_to_message_id;
          int64 random_id = begin_send_message(dialog_id, m);
          send_closure(
              td_->create_net_actor<SendMediaActor>(), &SendMediaActor::send, file_id, thumbnail_file_id,
              get_message_flags(m), dialog_id, get_send_message_as_input_peer(m), m->reply_to_message_id,
              get_message_schedule_date(m), get_input_reply_markup(m->reply_markup),
              get_input_message_entities(td_->contacts_manager_.get(), caption, "on_message_media_uploaded"),
              caption == nullptr ? "" : caption->text, std::move(input_media), random_id, &m->send_query_ref,
              get_sequence_dispatcher_id(dialog_id, m->is_copy ? MessageContentType::None : m->content->get_type()));
        }));
  } else {
    switch (input_media->get_id()) {
      case telegram_api::inputMediaUploadedDocument::ID:
        static_cast<telegram_api::inputMediaUploadedDocument *>(input_media.get())->flags_ |=
            telegram_api::inputMediaUploadedDocument::NOSOUND_VIDEO_MASK;
      // fallthrough
      case telegram_api::inputMediaUploadedPhoto::ID:
      case telegram_api::inputMediaDocumentExternal::ID:
      case telegram_api::inputMediaPhotoExternal::ID:
        LOG(INFO) << "Upload media from " << message_id << " in " << dialog_id;
        td_->create_handler<UploadMediaQuery>()->send(dialog_id, message_id, file_id, thumbnail_file_id,
                                                      std::move(input_media));
        break;
      case telegram_api::inputMediaDocument::ID:
      case telegram_api::inputMediaPhoto::ID:
        send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id,
                           dialog_id, message_id, Status::OK());
        break;
      default:
        LOG(ERROR) << "Have wrong input media " << to_string(input_media);
        send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id,
                           dialog_id, message_id, Status::Error(400, "Invalid input media"));
    }
  }
}

void MessagesManager::edit_message_text(FullMessageId full_message_id,
                                        tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                        tl_object_ptr<td_api::InputMessageContent> &&input_message_content,
                                        Promise<Unit> &&promise) {
  if (input_message_content == nullptr) {
    return promise.set_error(Status::Error(400, "Can't edit message without new content"));
  }
  int32 new_message_content_type = input_message_content->get_id();
  if (new_message_content_type != td_api::inputMessageText::ID) {
    return promise.set_error(Status::Error(400, "Input message content type must be InputMessageText"));
  }

  LOG(INFO) << "Begin to edit text of " << full_message_id;
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "edit_message_text");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Edit)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  const Message *m = get_message_force(d, full_message_id.get_message_id(), "edit_message_text");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (!can_edit_message(dialog_id, m, true)) {
    return promise.set_error(Status::Error(400, "Message can't be edited"));
  }

  MessageContentType old_message_content_type = m->content->get_type();
  if (old_message_content_type != MessageContentType::Text && old_message_content_type != MessageContentType::Game) {
    return promise.set_error(Status::Error(400, "There is no text in the message to edit"));
  }

  auto r_input_message_text = process_input_message_text(
      td_->contacts_manager_.get(), dialog_id, std::move(input_message_content), td_->auth_manager_->is_bot());
  if (r_input_message_text.is_error()) {
    return promise.set_error(r_input_message_text.move_as_error());
  }
  InputMessageText input_message_text = r_input_message_text.move_as_ok();

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false,
                                             has_message_sender_user_id(dialog_id, m));
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }
  auto input_reply_markup = get_input_reply_markup(r_new_reply_markup.ok());
  int32 flags = 0;
  if (input_message_text.disable_web_page_preview) {
    flags |= SEND_MESSAGE_FLAG_DISABLE_WEB_PAGE_PREVIEW;
  }

  send_closure(
      td_->create_net_actor<EditMessageActor>(std::move(promise)), &EditMessageActor::send, flags, dialog_id,
      m->message_id, input_message_text.text.text,
      get_input_message_entities(td_->contacts_manager_.get(), input_message_text.text.entities, "edit_message_text"),
      nullptr, std::move(input_reply_markup), get_message_schedule_date(m),
      get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
}

void MessagesManager::edit_message_live_location(FullMessageId full_message_id,
                                                 tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                                 tl_object_ptr<td_api::location> &&input_location, int32 heading,
                                                 int32 proximity_alert_radius, Promise<Unit> &&promise) {
  LOG(INFO) << "Begin to edit live location of " << full_message_id;
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "edit_message_live_location");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Edit)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  const Message *m = get_message_force(d, full_message_id.get_message_id(), "edit_message_live_location");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (!can_edit_message(dialog_id, m, true)) {
    return promise.set_error(Status::Error(400, "Message can't be edited"));
  }

  MessageContentType old_message_content_type = m->content->get_type();
  if (old_message_content_type != MessageContentType::LiveLocation) {
    return promise.set_error(Status::Error(400, "There is no live location in the message to edit"));
  }
  if (m->message_id.is_scheduled()) {
    LOG(ERROR) << "Have " << full_message_id << " with live location";
    return promise.set_error(Status::Error(400, "Can't edit live location in scheduled message"));
  }

  Location location(input_location);
  if (location.empty() && input_location != nullptr) {
    return promise.set_error(Status::Error(400, "Invalid location specified"));
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false,
                                             has_message_sender_user_id(dialog_id, m));
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }
  auto input_reply_markup = get_input_reply_markup(r_new_reply_markup.ok());

  int32 flags = 0;
  if (location.empty()) {
    flags |= telegram_api::inputMediaGeoLive::STOPPED_MASK;
  }
  if (heading != 0) {
    flags |= telegram_api::inputMediaGeoLive::HEADING_MASK;
  }
  flags |= telegram_api::inputMediaGeoLive::PROXIMITY_NOTIFICATION_RADIUS_MASK;
  auto input_media = telegram_api::make_object<telegram_api::inputMediaGeoLive>(
      flags, false /*ignored*/, location.get_input_geo_point(), heading, 0, proximity_alert_radius);
  send_closure(td_->create_net_actor<EditMessageActor>(std::move(promise)), &EditMessageActor::send, 0, dialog_id,
               m->message_id, string(), vector<tl_object_ptr<telegram_api::MessageEntity>>(), std::move(input_media),
               std::move(input_reply_markup), get_message_schedule_date(m),
               get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
}

void MessagesManager::edit_message_caption(FullMessageId full_message_id,
                                           tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                           tl_object_ptr<td_api::formattedText> &&input_caption,
                                           Promise<Unit> &&promise) {
  LOG(INFO) << "Begin to edit caption of " << full_message_id;

  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "edit_message_caption");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Edit)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  const Message *m = get_message_force(d, full_message_id.get_message_id(), "edit_message_caption");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (!can_edit_message(dialog_id, m, true)) {
    return promise.set_error(Status::Error(400, "Message can't be edited"));
  }

  if (!can_have_message_content_caption(m->content->get_type())) {
    return promise.set_error(Status::Error(400, "There is no caption in the message to edit"));
  }

  auto r_caption = process_input_caption(td_->contacts_manager_.get(), dialog_id, std::move(input_caption),
                                         td_->auth_manager_->is_bot());
  if (r_caption.is_error()) {
    return promise.set_error(r_caption.move_as_error());
  }
  auto caption = r_caption.move_as_ok();

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false,
                                             has_message_sender_user_id(dialog_id, m));
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }
  auto input_reply_markup = get_input_reply_markup(r_new_reply_markup.ok());

  send_closure(td_->create_net_actor<EditMessageActor>(std::move(promise)), &EditMessageActor::send, 1 << 11, dialog_id,
               m->message_id, caption.text,
               get_input_message_entities(td_->contacts_manager_.get(), caption.entities, "edit_message_caption"),
               nullptr, std::move(input_reply_markup), get_message_schedule_date(m),
               get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
}

void MessagesManager::edit_message_reply_markup(FullMessageId full_message_id,
                                                tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                                Promise<Unit> &&promise) {
  if (!td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Method is available only for bots"));
  }

  LOG(INFO) << "Begin to edit reply markup of " << full_message_id;
  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "edit_message_reply_markup");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Edit)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  const Message *m = get_message_force(d, full_message_id.get_message_id(), "edit_message_reply_markup");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (!can_edit_message(dialog_id, m, true, true)) {
    return promise.set_error(Status::Error(400, "Message can't be edited"));
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false,
                                             has_message_sender_user_id(dialog_id, m));
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }
  auto input_reply_markup = get_input_reply_markup(r_new_reply_markup.ok());
  send_closure(td_->create_net_actor<EditMessageActor>(std::move(promise)), &EditMessageActor::send, 0, dialog_id,
               m->message_id, string(), vector<tl_object_ptr<telegram_api::MessageEntity>>(), nullptr,
               std::move(input_reply_markup), get_message_schedule_date(m),
               get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
}

void MessagesManager::edit_message_scheduling_state(
    FullMessageId full_message_id, td_api::object_ptr<td_api::MessageSchedulingState> &&scheduling_state,
    Promise<Unit> &&promise) {
  auto r_schedule_date = get_message_schedule_date(std::move(scheduling_state));
  if (r_schedule_date.is_error()) {
    return promise.set_error(r_schedule_date.move_as_error());
  }
  auto schedule_date = r_schedule_date.move_as_ok();

  LOG(INFO) << "Begin to reschedule " << full_message_id << " to " << schedule_date;

  auto dialog_id = full_message_id.get_dialog_id();
  Dialog *d = get_dialog_force(dialog_id, "edit_message_scheduling_state");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Edit)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  Message *m = get_message_force(d, full_message_id.get_message_id(), "edit_message_scheduling_state");
  if (m == nullptr) {
    return promise.set_error(Status::Error(400, "Message not found"));
  }

  if (!m->message_id.is_scheduled()) {
    return promise.set_error(Status::Error(400, "Message is not scheduled"));
  }
  if (!m->message_id.is_scheduled_server()) {
    return promise.set_error(Status::Error(400, "Can't reschedule the message"));
  }

  if (get_message_schedule_date(m) == schedule_date) {
    return promise.set_value(Unit());
  }
  m->edited_schedule_date = schedule_date;

  if (schedule_date > 0) {
    send_closure(td_->create_net_actor<EditMessageActor>(std::move(promise)), &EditMessageActor::send, 0, dialog_id,
                 m->message_id, string(), vector<tl_object_ptr<telegram_api::MessageEntity>>(), nullptr, nullptr,
                 schedule_date, get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
  } else {
    send_closure(td_->create_net_actor<SendScheduledMessageActor>(std::move(promise)), &SendScheduledMessageActor::send,
                 dialog_id, m->message_id, get_sequence_dispatcher_id(dialog_id, MessageContentType::None));
  }
}
}  // namespace td
