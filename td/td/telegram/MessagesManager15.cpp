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
#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileId.hpp"
#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"
#include "td/telegram/GroupCallManager.h"

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



#include <limits>






namespace td {
}  // namespace td
namespace td {

class SendMultiMediaActor final : public NetActorOnce {
  vector<FileId> file_ids_;
  vector<string> file_references_;
  vector<int64> random_ids_;
  DialogId dialog_id_;

 public:
  void send(int32 flags, DialogId dialog_id, tl_object_ptr<telegram_api::InputPeer> as_input_peer,
            MessageId reply_to_message_id, int32 schedule_date, vector<FileId> &&file_ids,
            vector<tl_object_ptr<telegram_api::inputSingleMedia>> &&input_single_media, uint64 sequence_dispatcher_id) {
    for (auto &single_media : input_single_media) {
      random_ids_.push_back(single_media->random_id_);
      CHECK(FileManager::extract_was_uploaded(single_media->media_) == false);
      file_references_.push_back(FileManager::extract_file_reference(single_media->media_));
    }
    dialog_id_ = dialog_id;
    file_ids_ = std::move(file_ids);
    CHECK(file_ids_.size() == random_ids_.size());

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    if (input_peer == nullptr) {
      on_error(Status::Error(400, "Have no write access to the chat"));
      stop();
      return;
    }

    if (as_input_peer != nullptr) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_SEND_AS;
    }

    auto query = G()->net_query_creator().create(
        telegram_api::messages_sendMultiMedia(flags, false /*ignored*/, false /*ignored*/, false /*ignored*/,
                                              std::move(input_peer), reply_to_message_id.get_server_message_id().get(),
                                              std::move(input_single_media), schedule_date, std::move(as_input_peer)));
    // no quick ack, because file reference errors are very likely to happen
    query->debug("send to MessagesManager::MultiSequenceDispatcher");
    send_closure(td_->messages_manager_->sequence_dispatcher_, &MultiSequenceDispatcher::send_with_callback,
                 std::move(query), actor_shared(this), sequence_dispatcher_id);
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_sendMultiMedia>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SendMultiMedia for " << format::as_array(random_ids_) << ": " << to_string(ptr);

    auto sent_random_ids = UpdatesManager::get_sent_messages_random_ids(ptr.get());
    bool is_result_wrong = false;
    auto sent_random_ids_size = sent_random_ids.size();
    for (auto &random_id : random_ids_) {
      auto it = sent_random_ids.find(random_id);
      if (it == sent_random_ids.end()) {
        if (random_ids_.size() == 1) {
          is_result_wrong = true;
        }
        td_->messages_manager_->on_send_message_fail(random_id, Status::Error(400, "Message was not sent"));
      } else {
        sent_random_ids.erase(it);
      }
    }
    if (!sent_random_ids.empty()) {
      is_result_wrong = true;
    }
    if (!is_result_wrong) {
      auto sent_messages = UpdatesManager::get_new_messages(ptr.get());
      if (sent_random_ids_size != sent_messages.size()) {
        is_result_wrong = true;
      }
      for (auto &sent_message : sent_messages) {
        if (MessagesManager::get_message_dialog_id(*sent_message) != dialog_id_) {
          is_result_wrong = true;
        }
      }
    }
    if (is_result_wrong) {
      LOG(ERROR) << "Receive wrong result for SendMultiMedia with random_ids " << format::as_array(random_ids_)
                 << " to " << dialog_id_ << ": " << oneline(to_string(ptr));
      td_->updates_manager_->schedule_get_difference("Wrong sendMultiMedia result");
    }

    td_->updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for SendMultiMedia: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, message will be re-sent
      return;
    }
    if (!td_->auth_manager_->is_bot() && FileReferenceManager::is_file_reference_error(status)) {
      auto pos = FileReferenceManager::get_file_reference_error_pos(status);
      if (1 <= pos && pos <= file_ids_.size() && file_ids_[pos - 1].is_valid()) {
        VLOG(file_references) << "Receive " << status << " for " << file_ids_[pos - 1];
        td_->file_manager_->delete_file_reference(file_ids_[pos - 1], file_references_[pos - 1]);
        td_->messages_manager_->on_send_media_group_file_reference_error(dialog_id_, std::move(random_ids_));
        return;
      } else {
        LOG(ERROR) << "Receive file reference error " << status << ", but file_ids = " << file_ids_
                   << ", message_count = " << file_ids_.size();
      }
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendMultiMediaActor");
    for (auto &random_id : random_ids_) {
      td_->messages_manager_->on_send_message_fail(random_id, status.clone());
    }
  }
};

void MessagesManager::do_send_message_group(int64 media_album_id) {
  CHECK(media_album_id < 0);
  auto it = pending_message_group_sends_.find(media_album_id);
  if (it == pending_message_group_sends_.end()) {
    // the group may be already sent or failed to be sent
    return;
  }
  auto &request = it->second;

  auto dialog_id = request.dialog_id;
  Dialog *d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  auto default_status = can_send_message(dialog_id);
  bool success = default_status.is_ok();
  vector<FileId> file_ids;
  vector<int64> random_ids;
  vector<tl_object_ptr<telegram_api::inputSingleMedia>> input_single_media;
  tl_object_ptr<telegram_api::InputPeer> as_input_peer;
  MessageId reply_to_message_id;
  int32 flags = 0;
  int32 schedule_date = 0;
  bool is_copy = false;
  for (size_t i = 0; i < request.message_ids.size(); i++) {
    auto *m = get_message(d, request.message_ids[i]);
    if (m == nullptr) {
      // skip deleted messages
      random_ids.push_back(0);
      continue;
    }

    reply_to_message_id = m->reply_to_message_id;
    flags = get_message_flags(m);
    schedule_date = get_message_schedule_date(m);
    is_copy = m->is_copy;
    as_input_peer = get_send_message_as_input_peer(m);

    file_ids.push_back(get_message_content_any_file_id(m->content.get()));
    random_ids.push_back(begin_send_message(dialog_id, m));

    LOG(INFO) << "Have file " << file_ids.back() << " in " << m->message_id << " with result " << request.results[i]
              << " and is_finished = " << static_cast<bool>(request.is_finished[i]);

    if (request.results[i].is_error() || !request.is_finished[i]) {
      success = false;
      continue;
    }

    const FormattedText *caption = get_message_content_caption(m->content.get());
    auto input_media = get_input_media(m->content.get(), td_, m->ttl, m->send_emoji, true);
    if (input_media == nullptr) {
      // TODO return CHECK
      auto file_id = get_message_content_any_file_id(m->content.get());
      auto file_view = td_->file_manager_->get_file_view(file_id);
      bool has_remote = file_view.has_remote_location();
      bool is_web = has_remote ? file_view.remote_location().is_web() : false;
      LOG(FATAL) << request.dialog_id << " " << request.finished_count << " " << i << " " << request.message_ids << " "
                 << request.is_finished << " " << request.results << " " << m->ttl << " " << has_remote << " "
                 << file_view.has_alive_remote_location() << " " << file_view.has_active_upload_remote_location() << " "
                 << file_view.has_active_download_remote_location() << " " << file_view.is_encrypted() << " " << is_web
                 << " " << file_view.has_url() << " "
                 << to_string(get_message_content_object(m->content.get(), td_, dialog_id, m->date,
                                                         m->is_content_secret, false, -1));
    }
    auto entities = get_input_message_entities(td_->contacts_manager_.get(), caption, "do_send_message_group");
    int32 input_single_media_flags = 0;
    if (!entities.empty()) {
      input_single_media_flags |= telegram_api::inputSingleMedia::ENTITIES_MASK;
    }

    input_single_media.push_back(make_tl_object<telegram_api::inputSingleMedia>(
        input_single_media_flags, std::move(input_media), random_ids.back(), caption == nullptr ? "" : caption->text,
        std::move(entities)));
  }

  if (!success) {
    if (default_status.is_ok()) {
      default_status = Status::Error(400, "Group send failed");
    }
    for (size_t i = 0; i < random_ids.size(); i++) {
      if (random_ids[i] != 0) {
        on_send_message_fail(random_ids[i],
                             request.results[i].is_error() ? std::move(request.results[i]) : default_status.clone());
      }
    }
    pending_message_group_sends_.erase(it);
    return;
  }
  LOG_CHECK(request.finished_count == request.message_ids.size())
      << request.finished_count << " " << request.message_ids.size();
  pending_message_group_sends_.erase(it);

  LOG(INFO) << "Begin to send media group " << media_album_id << " to " << dialog_id;

  if (input_single_media.empty()) {
    LOG(INFO) << "Media group " << media_album_id << " from " << dialog_id << " is empty";
  }
  send_closure(td_->create_net_actor<SendMultiMediaActor>(), &SendMultiMediaActor::send, flags, dialog_id,
               std::move(as_input_peer), reply_to_message_id, schedule_date, std::move(file_ids),
               std::move(input_single_media),
               get_sequence_dispatcher_id(dialog_id, is_copy ? MessageContentType::None : MessageContentType::Photo));
}

class SetTypingQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;
  int32 generation_ = 0;

 public:
  explicit SetTypingQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  NetQueryRef send(DialogId dialog_id, tl_object_ptr<telegram_api::InputPeer> &&input_peer, MessageId message_id,
                   tl_object_ptr<telegram_api::SendMessageAction> &&action) {
    dialog_id_ = dialog_id;
    CHECK(input_peer != nullptr);

    int32 flags = 0;
    if (message_id.is_valid()) {
      flags |= telegram_api::messages_setTyping::TOP_MSG_ID_MASK;
    }
    auto net_query = G()->net_query_creator().create(telegram_api::messages_setTyping(
        flags, std::move(input_peer), message_id.get_server_message_id().get(), std::move(action)));
    auto result = net_query.get_weak();
    generation_ = result.generation();
    send_query(std::move(net_query));
    return result;
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_setTyping>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    // ignore result
    promise_.set_value(Unit());

    send_closure_later(G()->messages_manager(), &MessagesManager::after_set_typing_query, dialog_id_, generation_);
  }

  void on_error(Status status) final {
    if (status.code() == NetQuery::Canceled) {
      return promise_.set_value(Unit());
    }

    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SetTypingQuery")) {
      LOG(INFO) << "Receive error for set typing: " << status;
    }
    promise_.set_error(std::move(status));

    send_closure_later(G()->messages_manager(), &MessagesManager::after_set_typing_query, dialog_id_, generation_);
  }
};

void MessagesManager::send_dialog_action(DialogId dialog_id, MessageId top_thread_message_id, DialogAction action,
                                         Promise<Unit> &&promise) {
  if (!have_dialog_force(dialog_id, "send_dialog_action")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (top_thread_message_id != MessageId() &&
      (!top_thread_message_id.is_valid() || !top_thread_message_id.is_server())) {
    return promise.set_error(Status::Error(400, "Invalid message thread specified"));
  }

  tl_object_ptr<telegram_api::InputPeer> input_peer;
  if (action == DialogAction::get_speaking_action()) {
    input_peer = get_input_peer(dialog_id, AccessRights::Read);
    if (input_peer == nullptr) {
      return promise.set_error(Status::Error(400, "Have no access to the chat"));
    }
  } else {
    auto can_send_status = can_send_message(dialog_id);
    if (can_send_status.is_error()) {
      if (td_->auth_manager_->is_bot()) {
        return promise.set_error(can_send_status.move_as_error());
      }
      return promise.set_value(Unit());
    }

    if (is_dialog_action_unneeded(dialog_id)) {
      return promise.set_value(Unit());
    }

    input_peer = get_input_peer(dialog_id, AccessRights::Write);
  }

  if (dialog_id.get_type() == DialogType::SecretChat) {
    send_closure(G()->secret_chats_manager(), &SecretChatsManager::send_message_action, dialog_id.get_secret_chat_id(),
                 action.get_secret_input_send_message_action());
    promise.set_value(Unit());
    return;
  }

  auto &query_ref = set_typing_query_[dialog_id];
  if (!query_ref.empty() && !td_->auth_manager_->is_bot()) {
    LOG(INFO) << "Cancel previous send chat action query";
    cancel_query(query_ref);
  }
  query_ref =
      td_->create_handler<SetTypingQuery>(std::move(promise))
          ->send(dialog_id, std::move(input_peer), top_thread_message_id, action.get_input_send_message_action());
}

class SendInlineBotResultQuery final : public Td::ResultHandler {
  int64 random_id_;
  DialogId dialog_id_;

 public:
  NetQueryRef send(int32 flags, DialogId dialog_id, tl_object_ptr<telegram_api::InputPeer> as_input_peer,
                   MessageId reply_to_message_id, int32 schedule_date, int64 random_id, int64 query_id,
                   const string &result_id) {
    random_id_ = random_id;
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Write);
    CHECK(input_peer != nullptr);

    if (as_input_peer != nullptr) {
      flags |= MessagesManager::SEND_MESSAGE_FLAG_HAS_SEND_AS;
    }

    auto query = G()->net_query_creator().create(telegram_api::messages_sendInlineBotResult(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, std::move(input_peer),
        reply_to_message_id.get_server_message_id().get(), random_id, query_id, result_id, schedule_date,
        std::move(as_input_peer)));
    auto send_query_ref = query.get_weak();
    send_query(std::move(query));
    return send_query_ref;
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_sendInlineBotResult>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for SendInlineBotResultQuery for " << random_id_ << ": " << to_string(ptr);
    td_->messages_manager_->check_send_message_result(random_id_, dialog_id_, ptr.get(), "SendInlineBotResult");
    td_->updates_manager_->on_get_updates(std::move(ptr), Promise<Unit>());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for SendInlineBotResultQuery: " << status;
    if (G()->close_flag() && G()->parameters().use_message_db) {
      // do not send error, message will be re-sent
      return;
    }
    td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "SendInlineBotResultQuery");
    td_->messages_manager_->on_send_message_fail(random_id_, std::move(status));
  }
};

void MessagesManager::do_send_inline_query_result_message(DialogId dialog_id, const Message *m, int64 query_id,
                                                          const string &result_id) {
  LOG(INFO) << "Do send inline query result " << FullMessageId(dialog_id, m->message_id);

  int64 random_id = begin_send_message(dialog_id, m);
  auto flags = get_message_flags(m);
  if (!m->via_bot_user_id.is_valid() || m->hide_via_bot) {
    flags |= telegram_api::messages_sendInlineBotResult::HIDE_VIA_MASK;
  }
  m->send_query_ref = td_->create_handler<SendInlineBotResultQuery>()->send(
      flags, dialog_id, get_send_message_as_input_peer(m), m->reply_to_message_id, get_message_schedule_date(m),
      random_id, query_id, result_id);
}

void MessagesManager::set_channel_pts(Dialog *d, int32 new_pts, const char *source) {
  CHECK(d != nullptr);
  CHECK(d->dialog_id.get_type() == DialogType::Channel);

  LOG_IF(ERROR, running_get_channel_difference(d->dialog_id))
      << "Set pts of " << d->dialog_id << " to " << new_pts << " from " << source
      << " while running getChannelDifference";

  if (is_debug_message_op_enabled()) {
    d->debug_message_op.emplace_back(Dialog::MessageOp::SetPts, new_pts, source);
  }

  // TODO delete_first_messages support in channels
  if (new_pts == std::numeric_limits<int32>::max()) {
    LOG(ERROR) << "Update " << d->dialog_id << " pts to -1 from " << source;
    G()->td_db()->get_binlog_pmc()->erase(get_channel_pts_key(d->dialog_id));
    d->pts = std::numeric_limits<int32>::max();
    if (d->pending_read_channel_inbox_pts != 0) {
      d->pending_read_channel_inbox_pts = 0;
    }
    return;
  }
  if (new_pts > d->pts || (0 < new_pts && new_pts < d->pts - 99999)) {  // pts can only go up or drop cardinally
    if (new_pts < d->pts - 99999) {
      LOG(WARNING) << "Pts of " << d->dialog_id << " decreases from " << d->pts << " to " << new_pts << " from "
                   << source;
    } else {
      LOG(INFO) << "Update " << d->dialog_id << " pts to " << new_pts << " from " << source;
    }

    d->pts = new_pts;
    if (d->pending_read_channel_inbox_pts != 0 && d->pending_read_channel_inbox_pts <= d->pts) {
      auto pts = d->pending_read_channel_inbox_pts;
      d->pending_read_channel_inbox_pts = 0;
      on_dialog_updated(d->dialog_id, "set_channel_pts");
      if (d->pts == pts) {
        read_history_inbox(d->dialog_id, d->pending_read_channel_inbox_max_message_id,
                           d->pending_read_channel_inbox_server_unread_count, "set_channel_pts");
      } else if (d->pts > pts) {
        repair_channel_server_unread_count(d);
      }
    }
    if (!G()->ignore_background_updates() && have_input_peer(d->dialog_id, AccessRights::Read)) {
      G()->td_db()->get_binlog_pmc()->set(get_channel_pts_key(d->dialog_id), to_string(new_pts));
    }
  } else if (new_pts < d->pts) {
    LOG(ERROR) << "Receive wrong pts " << new_pts << " in " << d->dialog_id << " from " << source << ". Current pts is "
               << d->pts;
  }
}

void MessagesManager::on_upload_thumbnail(FileId thumbnail_file_id,
                                          tl_object_ptr<telegram_api::InputFile> thumbnail_input_file) {
  if (G()->close_flag()) {
    // do not fail upload if closing
    return;
  }

  LOG(INFO) << "Thumbnail " << thumbnail_file_id << " has been uploaded as " << to_string(thumbnail_input_file);

  auto it = being_uploaded_thumbnails_.find(thumbnail_file_id);
  if (it == being_uploaded_thumbnails_.end()) {
    // callback may be called just before the thumbnail upload was canceled
    return;
  }

  auto full_message_id = it->second.full_message_id;
  auto file_id = it->second.file_id;
  auto input_file = std::move(it->second.input_file);

  being_uploaded_thumbnails_.erase(it);

  Message *m = get_message(full_message_id);
  if (m == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel, do not need to send or edit it
    // thumbnail file upload should be already canceled in cancel_send_message_query
    LOG(ERROR) << "Message with a media has already been deleted";
    return;
  }

  bool is_edit = m->message_id.is_any_server();

  if (thumbnail_input_file == nullptr) {
    delete_message_content_thumbnail(is_edit ? m->edited_content.get() : m->content.get(), td_);
  }

  auto dialog_id = full_message_id.get_dialog_id();
  auto can_send_status = can_send_message(dialog_id);
  if (!is_edit && can_send_status.is_error()) {
    // user has left the chat during upload of the thumbnail or lost their privileges
    LOG(INFO) << "Can't send a message to " << dialog_id << ": " << can_send_status.error();

    fail_send_message(full_message_id, can_send_status.move_as_error());
    return;
  }

  do_send_media(dialog_id, m, file_id, thumbnail_file_id, std::move(input_file), std::move(thumbnail_input_file));
}

bool MessagesManager::update_dialog_draft_message(Dialog *d, unique_ptr<DraftMessage> &&draft_message, bool from_update,
                                                  bool need_update_dialog_pos) {
  CHECK(d != nullptr);
  if (from_update && d->is_opened && d->draft_message != nullptr) {
    // send the update anyway, despite it shouldn't be applied client-side
    // return false;
  }
  if (draft_message == nullptr) {
    if (d->draft_message != nullptr) {
      d->draft_message = nullptr;
      if (need_update_dialog_pos) {
        update_dialog_pos(d, "update_dialog_draft_message", false);
      }
      send_update_chat_draft_message(d);
      return true;
    }
  } else {
    if (d->draft_message != nullptr && d->draft_message->reply_to_message_id == draft_message->reply_to_message_id &&
        d->draft_message->input_message_text == draft_message->input_message_text) {
      if (d->draft_message->date < draft_message->date) {
        d->draft_message->date = draft_message->date;
        if (need_update_dialog_pos) {
          update_dialog_pos(d, "update_dialog_draft_message 2", false);
        }
        send_update_chat_draft_message(d);
        return true;
      }
    } else {
      if (!from_update || d->draft_message == nullptr || d->draft_message->date <= draft_message->date) {
        d->draft_message = std::move(draft_message);
        if (need_update_dialog_pos) {
          update_dialog_pos(d, "update_dialog_draft_message 3", false);
        }
        send_update_chat_draft_message(d);
        return true;
      }
    }
  }
  return false;
}

void MessagesManager::on_messages_db_calls_result(Result<MessagesDbCallsResult> result, int64 random_id,
                                                  MessageId first_db_message_id, MessageSearchFilter filter,
                                                  Promise<> &&promise) {
  CHECK(!first_db_message_id.is_scheduled());
  if (G()->close_flag()) {
    result = Global::request_aborted_error();
  }
  if (result.is_error()) {
    found_call_messages_.erase(random_id);
    return promise.set_error(result.move_as_error());
  }
  auto calls_result = result.move_as_ok();

  auto it = found_call_messages_.find(random_id);
  CHECK(it != found_call_messages_.end());
  auto &res = it->second.second;

  res.reserve(calls_result.messages.size());
  for (auto &message : calls_result.messages) {
    auto m = on_get_message_from_database(message, false, "on_messages_db_calls_result");

    if (m != nullptr && first_db_message_id <= m->message_id) {
      res.emplace_back(message.dialog_id, m->message_id);
    }
  }
  it->second.first = calls_db_state_.message_count_by_index[call_message_search_filter_index(filter)];

  if (res.empty() && first_db_message_id != MessageId::min()) {
    LOG(INFO) << "No messages found in database";
    found_call_messages_.erase(it);
  }

  promise.set_value(Unit());
}

void MessagesManager::on_upload_message_media_fail(DialogId dialog_id, MessageId message_id, Status error) {
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

  send_closure_later(actor_id(this), &MessagesManager::on_upload_message_media_finished, m->media_album_id, dialog_id,
                     m->message_id, std::move(error));
}

void MessagesManager::get_callback_query_message(DialogId dialog_id, MessageId message_id, int64 callback_query_id,
                                                 Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "get_callback_query_message");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!message_id.is_valid() || !message_id.is_server()) {
    return promise.set_error(Status::Error(400, "Invalid message identifier specified"));
  }

  LOG(INFO) << "Get callback query " << message_id << " in " << dialog_id << " for query " << callback_query_id;

  auto input_message = make_tl_object<telegram_api::inputMessageCallbackQuery>(message_id.get_server_message_id().get(),
                                                                               callback_query_id);
  get_message_force_from_server(d, message_id, std::move(promise), std::move(input_message));
}

void MessagesManager::ttl_read_history(Dialog *d, bool is_outgoing, MessageId from_message_id,
                                       MessageId till_message_id, double view_date) {
  CHECK(!from_message_id.is_scheduled());
  CHECK(!till_message_id.is_scheduled());

  // TODO: protect with log event
  suffix_load_till_message_id(d, till_message_id,
                              PromiseCreator::lambda([actor_id = actor_id(this), dialog_id = d->dialog_id, is_outgoing,
                                                      from_message_id, till_message_id, view_date](Result<Unit>) {
                                send_closure(actor_id, &MessagesManager::ttl_read_history_impl, dialog_id, is_outgoing,
                                             from_message_id, till_message_id, view_date);
                              }));
}

bool MessagesManager::can_delete_message(DialogId dialog_id, const Message *m) const {
  if (m == nullptr) {
    return true;
  }
  if (m->message_id.is_local() || m->message_id.is_yet_unsent()) {
    return true;
  }
  switch (dialog_id.get_type()) {
    case DialogType::User:
      return true;
    case DialogType::Chat:
      return true;
    case DialogType::Channel: {
      auto dialog_status = td_->contacts_manager_->get_channel_permissions(dialog_id.get_channel_id());
      return can_delete_channel_message(dialog_status, m, td_->auth_manager_->is_bot());
    }
    case DialogType::SecretChat:
      return true;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }
}

bool MessagesManager::is_anonymous_administrator(DialogId dialog_id, string *author_signature) const {
  CHECK(dialog_id.is_valid());

  if (is_broadcast_channel(dialog_id)) {
    return true;
  }

  if (td_->auth_manager_->is_bot()) {
    return false;
  }

  if (dialog_id.get_type() != DialogType::Channel) {
    return false;
  }

  auto status = td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id());
  if (!status.is_anonymous()) {
    return false;
  }

  if (author_signature != nullptr) {
    *author_signature = status.get_rank();
  }
  return true;
}

void MessagesManager::send_update_chat_theme(const Dialog *d) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  CHECK(d->dialog_id.get_type() != DialogType::SecretChat);
  LOG_CHECK(d->is_update_new_chat_sent) << "Wrong " << d->dialog_id << " in send_update_chat_theme";
  on_dialog_updated(d->dialog_id, "send_update_chat_theme");
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateChatTheme>(d->dialog_id.get(), d->theme_name));

  send_update_secret_chats_with_user_theme(d);
}

bool MessagesManager::ttl_on_open(Dialog *d, Message *m, double now, bool is_local_read) {
  CHECK(!m->message_id.is_scheduled());
  if (m->ttl > 0 && m->ttl_expires_at == 0) {
    if (!is_local_read && d->dialog_id.get_type() != DialogType::SecretChat) {
      on_message_ttl_expired(d, m);
    } else {
      m->ttl_expires_at = m->ttl + now;
      ttl_register_message(d->dialog_id, m, now);
    }
    return true;
  }
  return false;
}

void MessagesManager::on_update_read_channel_outbox(tl_object_ptr<telegram_api::updateReadChannelOutbox> &&update) {
  ChannelId channel_id(update->channel_id_);
  if (!channel_id.is_valid()) {
    LOG(ERROR) << "Receive invalid " << channel_id << " in updateReadChannelOutbox";
    return;
  }

  DialogId dialog_id = DialogId(channel_id);
  read_history_outbox(dialog_id, MessageId(ServerMessageId(update->max_id_)));
}

void MessagesManager::on_update_dialog_group_call_rights(DialogId dialog_id) {
  auto d = get_dialog(dialog_id);
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (d->active_group_call_id.is_valid()) {
    td_->group_call_manager_->on_update_group_call_rights(d->active_group_call_id);
  }
}

FullMessageId MessagesManager::get_full_message_id(const tl_object_ptr<telegram_api::Message> &message_ptr,
                                                   bool is_scheduled) {
  return {get_message_dialog_id(message_ptr), get_message_id(message_ptr, is_scheduled)};
}

tl_object_ptr<td_api::chat> MessagesManager::get_chat_object(DialogId dialog_id) const {
  return get_chat_object(get_dialog(dialog_id));
}

}  // namespace td
