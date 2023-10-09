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

#include "td/telegram/InlineQueriesManager.h"
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

#include <algorithm>








namespace td {
}  // namespace td
namespace td {

class EditInlineMessageQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit EditInlineMessageQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(int32 flags, tl_object_ptr<telegram_api::InputBotInlineMessageID> input_bot_inline_message_id,
            const string &text, vector<tl_object_ptr<telegram_api::MessageEntity>> &&entities,
            tl_object_ptr<telegram_api::InputMedia> &&input_media,
            tl_object_ptr<telegram_api::ReplyMarkup> &&reply_markup) {
    CHECK(input_bot_inline_message_id != nullptr);

    // file in an inline message can't be uploaded to another datacenter,
    // so only previously uploaded files or URLs can be used in the InputMedia
    CHECK(!FileManager::extract_was_uploaded(input_media));

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
      flags |= telegram_api::messages_editInlineBotMessage::MEDIA_MASK;
    }

    auto dc_id = DcId::internal(InlineQueriesManager::get_inline_message_dc_id(input_bot_inline_message_id));
    send_query(G()->net_query_creator().create(
        telegram_api::messages_editInlineBotMessage(flags, false /*ignored*/, std::move(input_bot_inline_message_id),
                                                    text, std::move(input_media), std::move(reply_markup),
                                                    std::move(entities)),
        dc_id));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_editInlineBotMessage>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG_IF(ERROR, !result_ptr.ok()) << "Receive false in result of editInlineMessage";

    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(INFO) << "Receive error for EditInlineMessageQuery: " << status;
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::edit_inline_message_text(const string &inline_message_id,
                                               tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                               tl_object_ptr<td_api::InputMessageContent> &&input_message_content,
                                               Promise<Unit> &&promise) {
  if (!td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Method is available only for bots"));
  }

  if (input_message_content == nullptr) {
    return promise.set_error(Status::Error(400, "Can't edit message without new content"));
  }
  int32 new_message_content_type = input_message_content->get_id();
  if (new_message_content_type != td_api::inputMessageText::ID) {
    return promise.set_error(Status::Error(400, "Input message content type must be InputMessageText"));
  }

  auto r_input_message_text = process_input_message_text(
      td_->contacts_manager_.get(), DialogId(), std::move(input_message_content), td_->auth_manager_->is_bot());
  if (r_input_message_text.is_error()) {
    return promise.set_error(r_input_message_text.move_as_error());
  }
  InputMessageText input_message_text = r_input_message_text.move_as_ok();

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false, true);
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  auto input_bot_inline_message_id = td_->inline_queries_manager_->get_input_bot_inline_message_id(inline_message_id);
  if (input_bot_inline_message_id == nullptr) {
    return promise.set_error(Status::Error(400, "Invalid inline message identifier specified"));
  }

  int32 flags = 0;
  if (input_message_text.disable_web_page_preview) {
    flags |= SEND_MESSAGE_FLAG_DISABLE_WEB_PAGE_PREVIEW;
  }
  td_->create_handler<EditInlineMessageQuery>(std::move(promise))
      ->send(flags, std::move(input_bot_inline_message_id), input_message_text.text.text,
             get_input_message_entities(td_->contacts_manager_.get(), input_message_text.text.entities,
                                        "edit_inline_message_text"),
             nullptr, get_input_reply_markup(r_new_reply_markup.ok()));
}

void MessagesManager::edit_inline_message_live_location(const string &inline_message_id,
                                                        tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                                        tl_object_ptr<td_api::location> &&input_location, int32 heading,
                                                        int32 proximity_alert_radius, Promise<Unit> &&promise) {
  if (!td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Method is available only for bots"));
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false, true);
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  auto input_bot_inline_message_id = td_->inline_queries_manager_->get_input_bot_inline_message_id(inline_message_id);
  if (input_bot_inline_message_id == nullptr) {
    return promise.set_error(Status::Error(400, "Invalid inline message identifier specified"));
  }

  Location location(input_location);
  if (location.empty() && input_location != nullptr) {
    return promise.set_error(Status::Error(400, "Invalid location specified"));
  }

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
  td_->create_handler<EditInlineMessageQuery>(std::move(promise))
      ->send(0, std::move(input_bot_inline_message_id), "", vector<tl_object_ptr<telegram_api::MessageEntity>>(),
             std::move(input_media), get_input_reply_markup(r_new_reply_markup.ok()));
}

void MessagesManager::edit_inline_message_media(const string &inline_message_id,
                                                tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                                tl_object_ptr<td_api::InputMessageContent> &&input_message_content,
                                                Promise<Unit> &&promise) {
  if (!td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Method is available only for bots"));
  }

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

  auto r_input_message_content = process_input_message_content(DialogId(), std::move(input_message_content));
  if (r_input_message_content.is_error()) {
    return promise.set_error(r_input_message_content.move_as_error());
  }
  InputMessageContent content = r_input_message_content.move_as_ok();
  if (content.ttl > 0) {
    LOG(ERROR) << "Have message content with TTL " << content.ttl;
    return promise.set_error(Status::Error(400, "Can't enable self-destruction for media"));
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false, true);
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  auto input_bot_inline_message_id = td_->inline_queries_manager_->get_input_bot_inline_message_id(inline_message_id);
  if (input_bot_inline_message_id == nullptr) {
    return promise.set_error(Status::Error(400, "Invalid inline message identifier specified"));
  }

  auto input_media = get_input_media(content.content.get(), td_, 0, string(), true);
  if (input_media == nullptr) {
    return promise.set_error(Status::Error(400, "Invalid message content specified"));
  }

  const FormattedText *caption = get_message_content_caption(content.content.get());
  td_->create_handler<EditInlineMessageQuery>(std::move(promise))
      ->send(1 << 11, std::move(input_bot_inline_message_id), caption == nullptr ? "" : caption->text,
             get_input_message_entities(td_->contacts_manager_.get(), caption, "edit_inline_message_media"),
             std::move(input_media), get_input_reply_markup(r_new_reply_markup.ok()));
}

void MessagesManager::edit_inline_message_caption(const string &inline_message_id,
                                                  tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                                  tl_object_ptr<td_api::formattedText> &&input_caption,
                                                  Promise<Unit> &&promise) {
  if (!td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Method is available only for bots"));
  }

  auto r_caption = process_input_caption(td_->contacts_manager_.get(), DialogId(), std::move(input_caption),
                                         td_->auth_manager_->is_bot());
  if (r_caption.is_error()) {
    return promise.set_error(r_caption.move_as_error());
  }
  auto caption = r_caption.move_as_ok();

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false, true);
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  auto input_bot_inline_message_id = td_->inline_queries_manager_->get_input_bot_inline_message_id(inline_message_id);
  if (input_bot_inline_message_id == nullptr) {
    return promise.set_error(Status::Error(400, "Invalid inline message identifier specified"));
  }

  td_->create_handler<EditInlineMessageQuery>(std::move(promise))
      ->send(1 << 11, std::move(input_bot_inline_message_id), caption.text,
             get_input_message_entities(td_->contacts_manager_.get(), caption.entities, "edit_inline_message_caption"),
             nullptr, get_input_reply_markup(r_new_reply_markup.ok()));
}

void MessagesManager::edit_inline_message_reply_markup(const string &inline_message_id,
                                                       tl_object_ptr<td_api::ReplyMarkup> &&reply_markup,
                                                       Promise<Unit> &&promise) {
  if (!td_->auth_manager_->is_bot()) {
    return promise.set_error(Status::Error(400, "Method is available only for bots"));
  }

  auto r_new_reply_markup = get_reply_markup(std::move(reply_markup), td_->auth_manager_->is_bot(), true, false, true);
  if (r_new_reply_markup.is_error()) {
    return promise.set_error(r_new_reply_markup.move_as_error());
  }

  auto input_bot_inline_message_id = td_->inline_queries_manager_->get_input_bot_inline_message_id(inline_message_id);
  if (input_bot_inline_message_id == nullptr) {
    return promise.set_error(Status::Error(400, "Invalid inline message identifier specified"));
  }

  td_->create_handler<EditInlineMessageQuery>(std::move(promise))
      ->send(0, std::move(input_bot_inline_message_id), string(), vector<tl_object_ptr<telegram_api::MessageEntity>>(),
             nullptr, get_input_reply_markup(r_new_reply_markup.ok()));
}

void MessagesManager::on_upload_message_media_finished(int64 media_album_id, DialogId dialog_id, MessageId message_id,
                                                       Status result) {
  CHECK(media_album_id < 0);
  auto it = pending_message_group_sends_.find(media_album_id);
  if (it == pending_message_group_sends_.end()) {
    // the group may be already sent or failed to be sent
    return;
  }
  auto &request = it->second;
  CHECK(request.dialog_id == dialog_id);
  auto message_it = std::find(request.message_ids.begin(), request.message_ids.end(), message_id);
  if (message_it == request.message_ids.end()) {
    // the message may be already deleted and the album is recreated without it
    CHECK(message_id.is_yet_unsent());
    LOG_CHECK(get_message({dialog_id, message_id}) == nullptr)
        << dialog_id << ' ' << request.message_ids << ' ' << message_id << ' ' << request.finished_count << ' '
        << request.is_finished << ' ' << request.results;
    return;
  }
  auto pos = static_cast<size_t>(message_it - request.message_ids.begin());

  if (request.is_finished[pos]) {
    LOG(INFO) << "Upload media of " << message_id << " in " << dialog_id << " from group " << media_album_id
              << " at pos " << pos << " was already finished";
    return;
  }
  LOG(INFO) << "Finish to upload media of " << message_id << " in " << dialog_id << " from group " << media_album_id
            << " at pos " << pos << " with result " << result
            << " and previous finished_count = " << request.finished_count;

  request.results[pos] = std::move(result);
  request.is_finished[pos] = true;
  request.finished_count++;

  if (request.finished_count == request.message_ids.size() || request.results[pos].is_error()) {
    // must use send_closure_later if some messages may be being deleted now
    // but this function is called only through send_closure_later, so there should be no being deleted messages
    // we must use synchronous calls to keep the correct message order during copying of multiple messages
    // but "request" iterator can be invalidated by do_send_message_group, so it must not be used below
    auto message_ids = request.message_ids;
    for (auto request_message_id : message_ids) {
      LOG(INFO) << "Send on_media_message_ready_to_send for " << request_message_id << " in " << dialog_id;
      auto promise = PromiseCreator::lambda([this, media_album_id](Result<Message *> result) {
        if (result.is_error() || G()->close_flag()) {
          return;
        }

        auto m = result.move_as_ok();
        CHECK(m != nullptr);
        CHECK(m->media_album_id == media_album_id);
        do_send_message_group(media_album_id);
        // send_closure_later(actor_id, &MessagesManager::do_send_message_group, media_album_id);
      });
      // send_closure_later(actor_id(this), &MessagesManager::on_media_message_ready_to_send, dialog_id,
      //                   request_message_id, std::move(promise));
      on_media_message_ready_to_send(dialog_id, request_message_id, std::move(promise));
    }
  }
}

void MessagesManager::skip_old_pending_pts_update(tl_object_ptr<telegram_api::Update> &&update, int32 new_pts,
                                                  int32 old_pts, int32 pts_count, const char *source) {
  if (update->get_id() == telegram_api::updateNewMessage::ID) {
    auto update_new_message = static_cast<telegram_api::updateNewMessage *>(update.get());
    auto full_message_id = get_full_message_id(update_new_message->message_, false);
    if (update_message_ids_.find(full_message_id) != update_message_ids_.end()) {
      if (new_pts == old_pts) {  // otherwise message can be already deleted
        // apply sent message anyway
        on_get_message(std::move(update_new_message->message_), true, false, false, true, true,
                       "updateNewMessage with an awaited message");
        return;
      } else {
        LOG(ERROR) << "Receive awaited sent " << full_message_id << " from " << source << " with pts " << new_pts
                   << " and pts_count " << pts_count << ", but current pts is " << old_pts;
        dump_debug_message_op(get_dialog(full_message_id.get_dialog_id()), 3);
      }
    }
  }
  if (update->get_id() == updateSentMessage::ID) {
    auto update_sent_message = static_cast<updateSentMessage *>(update.get());
    if (being_sent_messages_.count(update_sent_message->random_id_) > 0) {
      if (new_pts == old_pts) {  // otherwise message can be already deleted
        // apply sent message anyway
        on_send_message_success(update_sent_message->random_id_, update_sent_message->message_id_,
                                update_sent_message->date_, update_sent_message->ttl_period_, FileId(),
                                "process old updateSentMessage");
        return;
      } else {
        LOG(ERROR) << "Receive awaited sent " << update_sent_message->message_id_ << " from " << source << " with pts "
                   << new_pts << " and pts_count " << pts_count << ", but current pts is " << old_pts;
        dump_debug_message_op(get_dialog(being_sent_messages_[update_sent_message->random_id_].get_dialog_id()), 3);
      }
    }
    return;
  }

  // very old or unuseful update
  LOG_IF(WARNING, new_pts == old_pts && pts_count == 0 && !is_allowed_useless_update(update))
      << "Receive useless update " << oneline(to_string(update)) << " from " << source;
}

void MessagesManager::set_sponsored_dialog(DialogId dialog_id, DialogSource source) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }
  LOG(INFO) << "Change sponsored chat from " << sponsored_dialog_id_ << " to " << dialog_id;
  if (removed_sponsored_dialog_id_.is_valid() && dialog_id == removed_sponsored_dialog_id_) {
    return;
  }

  if (sponsored_dialog_id_ == dialog_id) {
    if (sponsored_dialog_source_ != source) {
      CHECK(sponsored_dialog_id_.is_valid());
      sponsored_dialog_source_ = std::move(source);
      const Dialog *d = get_dialog(sponsored_dialog_id_);
      CHECK(d != nullptr);
      send_update_chat_position(DialogListId(FolderId::main()), d, "set_sponsored_dialog");
      save_sponsored_dialog();
    }
    return;
  }

  bool need_update_total_chat_count = false;
  if (sponsored_dialog_id_.is_valid()) {
    const Dialog *d = get_dialog(sponsored_dialog_id_);
    CHECK(d != nullptr);
    bool was_sponsored = is_dialog_sponsored(d);
    sponsored_dialog_id_ = DialogId();
    sponsored_dialog_source_ = DialogSource();
    if (was_sponsored) {
      send_update_chat_position(DialogListId(FolderId::main()), d, "set_sponsored_dialog 2");
      need_update_total_chat_count = true;
    }
  }

  if (dialog_id.is_valid()) {
    force_create_dialog(dialog_id, "set_sponsored_dialog_id");
    const Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);
    add_sponsored_dialog(d, std::move(source));
    if (is_dialog_sponsored(d)) {
      need_update_total_chat_count = !need_update_total_chat_count;
    }
  }

  if (need_update_total_chat_count) {
    auto dialog_list_id = DialogListId(FolderId::main());
    auto *list = get_dialog_list(dialog_list_id);
    CHECK(list != nullptr);
    if (list->is_dialog_unread_count_inited_) {
      send_update_unread_chat_count(*list, DialogId(), true, "set_sponsored_dialog_id");
    }
  }

  save_sponsored_dialog();
}

NotificationGroupId MessagesManager::get_dialog_notification_group_id(DialogId dialog_id,
                                                                      NotificationGroupInfo &group_info) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return NotificationGroupId();
  }
  if (!group_info.group_id.is_valid()) {
    NotificationGroupId next_notification_group_id;
    do {
      next_notification_group_id = td_->notification_manager_->get_next_notification_group_id();
      if (!next_notification_group_id.is_valid()) {
        return NotificationGroupId();
      }
    } while (get_message_notification_group_force(next_notification_group_id).dialog_id.is_valid());
    group_info.group_id = next_notification_group_id;
    group_info.is_changed = true;
    VLOG(notifications) << "Assign " << next_notification_group_id << " to " << dialog_id;
    on_dialog_updated(dialog_id, "get_dialog_notification_group_id");

    notification_group_id_to_dialog_id_.emplace(next_notification_group_id, dialog_id);

    if (running_get_channel_difference(dialog_id) || get_channel_difference_to_log_event_id_.count(dialog_id) != 0) {
      send_closure_later(G()->notification_manager(), &NotificationManager::before_get_chat_difference,
                         next_notification_group_id);
    }
  }

  CHECK(group_info.group_id.is_valid());

  // notification group must be preloaded to guarantee that there is no race between
  // get_message_notifications_from_database_force and new notifications added right now
  td_->notification_manager_->load_group_force(group_info.group_id);

  return group_info.group_id;
}

void MessagesManager::dump_debug_message_op(const Dialog *d, int priority) {
  if (!is_debug_message_op_enabled()) {
    return;
  }
  if (d == nullptr) {
    LOG(ERROR) << "Chat not found";
    return;
  }
  static int last_dumped_priority = -1;
  if (priority <= last_dumped_priority) {
    LOG(ERROR) << "Skip dump " << d->dialog_id;
    return;
  }
  last_dumped_priority = priority;

  for (auto &op : d->debug_message_op) {
    switch (op.type) {
      case Dialog::MessageOp::Add: {
        LOG(ERROR) << "MessageOpAdd at " << op.date << " " << op.message_id << " " << op.content_type << " "
                   << op.from_update << " " << op.have_previous << " " << op.have_next << " " << op.source;
        break;
      }
      case Dialog::MessageOp::SetPts: {
        LOG(ERROR) << "MessageOpSetPts at " << op.date << " " << op.pts << " " << op.source;
        break;
      }
      case Dialog::MessageOp::Delete: {
        LOG(ERROR) << "MessageOpDelete at " << op.date << " " << op.message_id << " " << op.content_type << " "
                   << op.from_update << " " << op.have_previous << " " << op.have_next << " " << op.source;
        break;
      }
      case Dialog::MessageOp::DeleteAll: {
        LOG(ERROR) << "MessageOpDeleteAll at " << op.date << " " << op.from_update;
        break;
      }
      default:
        UNREACHABLE();
    }
  }
}

void MessagesManager::check_send_message_result(int64 random_id, DialogId dialog_id,
                                                const telegram_api::Updates *updates_ptr, const char *source) {
  CHECK(updates_ptr != nullptr);
  CHECK(source != nullptr);
  auto sent_messages = UpdatesManager::get_new_messages(updates_ptr);
  auto sent_messages_random_ids = UpdatesManager::get_sent_messages_random_ids(updates_ptr);
  if (sent_messages.size() != 1u || sent_messages_random_ids.size() != 1u ||
      *sent_messages_random_ids.begin() != random_id || get_message_dialog_id(*sent_messages[0]) != dialog_id) {
    LOG(ERROR) << "Receive wrong result for sending message with random_id " << random_id << " from " << source
               << " to " << dialog_id << ": " << oneline(to_string(*updates_ptr));
    Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);
    if (dialog_id.get_type() == DialogType::Channel) {
      get_channel_difference(dialog_id, d->pts, true, "check_send_message_result");
    } else {
      td_->updates_manager_->schedule_get_difference("check_send_message_result");
    }
    repair_dialog_scheduled_messages(d);
  }
}

void MessagesManager::repair_dialog_scheduled_messages(Dialog *d) {
  if (td_->auth_manager_->is_bot() || d->dialog_id.get_type() == DialogType::SecretChat) {
    return;
  }

  if (d->last_repair_scheduled_messages_generation == scheduled_messages_sync_generation_) {
    return;
  }
  d->last_repair_scheduled_messages_generation = scheduled_messages_sync_generation_;

  // TODO create log event
  auto dialog_id = d->dialog_id;
  LOG(INFO) << "Repair scheduled messages in " << dialog_id << " with generation "
            << d->last_repair_scheduled_messages_generation;
  get_dialog_scheduled_messages(dialog_id, false, true,
                                PromiseCreator::lambda([actor_id = actor_id(this), dialog_id](Unit) {
                                  send_closure(G()->messages_manager(), &MessagesManager::get_dialog_scheduled_messages,
                                               dialog_id, true, true, Promise<Unit>());
                                }));
}

td_api::object_ptr<td_api::messageInteractionInfo> MessagesManager::get_message_interaction_info_object(
    DialogId dialog_id, const Message *m) const {
  bool is_visible_reply_info = is_visible_message_reply_info(dialog_id, m);
  if (m->view_count == 0 && m->forward_count == 0 && !is_visible_reply_info) {
    return nullptr;
  }
  if (m->message_id.is_scheduled() && (m->forward_info == nullptr || is_broadcast_channel(dialog_id))) {
    return nullptr;
  }
  if (m->message_id.is_local() && m->forward_info == nullptr) {
    return nullptr;
  }

  td_api::object_ptr<td_api::messageReplyInfo> reply_info;
  if (is_visible_reply_info) {
    reply_info = m->reply_info.get_message_reply_info_object(td_);
    CHECK(reply_info != nullptr);
  }

  return td_api::make_object<td_api::messageInteractionInfo>(m->view_count, m->forward_count, std::move(reply_info));
}

td_api::object_ptr<td_api::message> MessagesManager::get_dialog_event_log_message_object(
    DialogId dialog_id, tl_object_ptr<telegram_api::Message> &&message, DialogId &sender_dialog_id) {
  auto dialog_message = create_message(parse_telegram_api_message(std::move(message), false, "dialog_event_log"),
                                       dialog_id.get_type() == DialogType::Channel);
  if (dialog_message.second == nullptr || dialog_message.first != dialog_id) {
    LOG(ERROR) << "Failed to create event log message in " << dialog_id;
    return nullptr;
  }
  sender_dialog_id = get_message_sender(dialog_message.second.get());
  return get_message_object(dialog_id, dialog_message.second.get(), "get_dialog_event_log_message_object", true);
}

void MessagesManager::on_get_dialog_sparse_message_positions(
    DialogId dialog_id, MessageSearchFilter filter,
    telegram_api::object_ptr<telegram_api::messages_searchResultsPositions> positions,
    Promise<td_api::object_ptr<td_api::messagePositions>> &&promise) {
  auto message_positions = transform(
      positions->positions_, [](const telegram_api::object_ptr<telegram_api::searchResultPosition> &position) {
        return td_api::make_object<td_api::messagePosition>(
            position->offset_, MessageId(ServerMessageId(position->msg_id_)).get(), position->date_);
      });
  promise.set_value(td_api::make_object<td_api::messagePositions>(positions->count_, std::move(message_positions)));
}

void MessagesManager::reget_message_from_server_if_needed(DialogId dialog_id, const Message *m) {
  if (!m->message_id.is_any_server() || dialog_id.get_type() == DialogType::SecretChat) {
    return;
  }

  if (need_reget_message_content(m->content.get()) || (m->legacy_layer != 0 && m->legacy_layer < MTPROTO_LAYER) ||
      m->reply_info.need_reget(td_)) {
    FullMessageId full_message_id{dialog_id, m->message_id};
    LOG(INFO) << "Reget from server " << full_message_id;
    get_message_from_server(full_message_id, Auto(), "reget_message_from_server_if_needed");
  }
}

tl_object_ptr<telegram_api::inputEncryptedChat> MessagesManager::get_input_encrypted_chat(
    DialogId dialog_id, AccessRights access_rights) const {
  switch (dialog_id.get_type()) {
    case DialogType::SecretChat: {
      SecretChatId secret_chat_id = dialog_id.get_secret_chat_id();
      return td_->contacts_manager_->get_input_encrypted_chat(secret_chat_id, access_rights);
    }
    case DialogType::User:
    case DialogType::Chat:
    case DialogType::Channel:
    case DialogType::None:
    default:
      UNREACHABLE();
      return nullptr;
  }
}

string MessagesManager::get_dialog_theme_name(const Dialog *d) const {
  CHECK(d != nullptr);
  if (d->dialog_id.get_type() == DialogType::SecretChat) {
    auto user_id = td_->contacts_manager_->get_secret_chat_user_id(d->dialog_id.get_secret_chat_id());
    if (!user_id.is_valid()) {
      return string();
    }
    d = get_dialog(DialogId(user_id));
    if (d == nullptr) {
      return string();
    }
  }
  return d->theme_name;
}

void MessagesManager::on_pending_draft_message_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager),
                     &MessagesManager::save_dialog_draft_message_on_server, DialogId(dialog_id_int));
}

void MessagesManager::send_update_chat_online_member_count(DialogId dialog_id, int32 online_member_count) const {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatOnlineMemberCount>(dialog_id.get(), online_member_count));
}

bool MessagesManager::can_get_message_statistics(FullMessageId full_message_id) {
  return can_get_message_statistics(full_message_id.get_dialog_id(),
                                    get_message_force(full_message_id, "can_get_message_statistics"));
}

void MessagesManager::delete_all_call_messages(bool revoke, Promise<Unit> &&promise) {
  delete_all_call_messages_on_server(revoke, 0, std::move(promise));
}
}  // namespace td
