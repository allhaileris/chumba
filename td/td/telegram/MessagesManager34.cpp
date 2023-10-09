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







#include <unordered_set>
#include <utility>

namespace td {
}  // namespace td
namespace td {

void MessagesManager::on_send_message_fail(int64 random_id, Status error) {
  CHECK(error.is_error());

  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    // we can't receive fail more than once
    // but message can be successfully sent before
    if (error.code() != NetQuery::Canceled) {
      LOG(ERROR) << "Receive error " << error << " about successfully sent message with random_id = " << random_id;
    }
    return;
  }

  auto full_message_id = it->second;

  being_sent_messages_.erase(it);

  Message *m = get_message(full_message_id);
  if (m == nullptr) {
    // message has already been deleted by the user or sent to inaccessible channel
    // don't need to send error to the user, because the message has already been deleted
    // and there is nothing to be deleted from the server
    LOG(INFO) << "Fail to send already deleted by the user or sent to inaccessible chat " << full_message_id;
    return;
  }
  LOG_IF(ERROR, error.code() == NetQuery::Canceled)
      << "Receive error " << error << " about sent message with random_id = " << random_id;

  auto dialog_id = full_message_id.get_dialog_id();
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // LOG(ERROR) << "Found " << m->message_id << " in inaccessible " << dialog_id;
    // dump_debug_message_op(get_dialog(dialog_id), 5);
  }

  int error_code = error.code();
  string error_message = error.message().str();
  switch (error_code) {
    case 420:
      error_code = 429;
      LOG(ERROR) << "Receive error 420: " << error_message;
      break;
    case 429:
      // nothing special, error description has already been changed
      LOG_IF(ERROR, !begins_with(error_message, "Too Many Requests: retry after "))
          << "Wrong error message: " << error_message;
      break;
    case 400:
      if (error.message() == "MESSAGE_TOO_LONG") {
        error_message = "Message is too long";
        // TODO move check to send_message
      } else if (error.message() == "INPUT_USER_DEACTIVATED") {
        error_code = 403;
        error_message = "User is deactivated";
      } else if (error.message() == "USER_IS_BLOCKED") {
        error_code = 403;
        if (td_->auth_manager_->is_bot()) {
          switch (dialog_id.get_type()) {
            case DialogType::User:
            case DialogType::SecretChat:
              error_message = "Bot was blocked by the user";
              break;
            case DialogType::Chat:
            case DialogType::Channel:
              error_message = "Bot was kicked from the chat";
              break;
            case DialogType::None:
            default:
              UNREACHABLE();
          }
        } else {
          switch (dialog_id.get_type()) {
            case DialogType::User:
            case DialogType::SecretChat:
              error_message = "User was blocked by the other user";
              break;
            case DialogType::Chat:
            case DialogType::Channel:
              error_message = "User is not in the chat";
              break;
            case DialogType::None:
            default:
              UNREACHABLE();
          }
        }
        // TODO add check to send_message
      } else if (error.message() == "USER_IS_BOT") {
        if (td_->auth_manager_->is_bot() &&
            (dialog_id.get_type() == DialogType::User || dialog_id.get_type() == DialogType::SecretChat)) {
          error_code = 403;
          if (td_->contacts_manager_->is_user_bot(dialog_id.get_user_id())) {
            error_message = "Bot can't send messages to bots";
          } else {
            error_message = "Bot can't send messages to the user";
          }
          // TODO move check to send_message
        }
      } else if (error.message() == "PEER_ID_INVALID") {
        error_code = 403;
        if (td_->auth_manager_->is_bot() &&
            (dialog_id.get_type() == DialogType::User || dialog_id.get_type() == DialogType::SecretChat)) {
          error_message = "Bot can't initiate conversation with a user";
        }
      } else if (error.message() == "WC_CONVERT_URL_INVALID" || error.message() == "EXTERNAL_URL_INVALID") {
        error_message = "Wrong HTTP URL specified";
      } else if (error.message() == "WEBPAGE_CURL_FAILED") {
        error_message = "Failed to get HTTP URL content";
      } else if (error.message() == "WEBPAGE_MEDIA_EMPTY") {
        error_message = "Wrong type of the web page content";
      } else if (error.message() == "MEDIA_EMPTY") {
        auto content_type = m->content->get_type();
        if (content_type == MessageContentType::Game) {
          error_message = "Wrong game short name specified";
        } else if (content_type == MessageContentType::Invoice) {
          error_message = "Wrong invoice information specified";
        } else if (content_type == MessageContentType::Poll) {
          error_message = "Wrong poll data specified";
        } else if (content_type == MessageContentType::Contact) {
          error_message = "Wrong phone number specified";
        } else {
          error_message = "Wrong file identifier/HTTP URL specified";
        }
      } else if (error.message() == "PHOTO_EXT_INVALID") {
        error_message = "Photo has unsupported extension. Use one of .jpg, .jpeg, .gif, .png, .tif or .bmp";
      }
      break;
    case 403:
      if (error.message() == "MESSAGE_DELETE_FORBIDDEN") {
        error_code = 400;
        error_message = "Message can't be deleted";
      } else if (error.message() != "CHANNEL_PUBLIC_GROUP_NA" && error.message() != "USER_IS_BLOCKED" &&
                 error.message() != "USER_BOT_INVALID" && error.message() != "USER_DELETED") {
        error_code = 400;
      }
      break;
    // TODO other codes
    default:
      break;
  }
  if (error.message() == "REPLY_MARKUP_INVALID") {
    if (m->reply_markup == nullptr) {
      LOG(ERROR) << "Receive " << error.message() << " for "
                 << oneline(to_string(get_message_object(dialog_id, m, "on_send_message_fail")));
    } else {
      LOG(ERROR) << "Receive " << error.message() << " for " << full_message_id << " with keyboard "
                 << *m->reply_markup;
    }
  }
  if (error_code != 403 && !(error_code == 500 && G()->close_flag())) {
    LOG(WARNING) << "Fail to send " << full_message_id << " with the error " << error;
  }
  if (error_code <= 0) {
    error_code = 500;
  }
  fail_send_message(full_message_id, error_code, error_message);
}

void MessagesManager::force_create_dialog(DialogId dialog_id, const char *source, bool expect_no_access,
                                          bool force_update_dialog_pos) {
  LOG_CHECK(dialog_id.is_valid()) << source;
  LOG_CHECK(is_inited_) << dialog_id << ' ' << source << ' ' << expect_no_access << ' ' << force_update_dialog_pos;
  Dialog *d = get_dialog_force(dialog_id, source);
  if (d == nullptr) {
    LOG(INFO) << "Force create " << dialog_id << " from " << source;
    if (loaded_dialogs_.count(dialog_id) > 0) {
      LOG(INFO) << "Skip creation of " << dialog_id << ", because it is being loaded now";
      return;
    }

    d = add_dialog(dialog_id, "force_create_dialog");
    update_dialog_pos(d, "force_create_dialog");

    if (dialog_id.get_type() == DialogType::SecretChat && !d->notification_settings.is_synchronized &&
        td_->contacts_manager_->get_secret_chat_state(dialog_id.get_secret_chat_id()) != SecretChatState::Closed) {
      // secret chat is being created
      // let's copy notification settings from main chat if available
      VLOG(notifications) << "Create new secret " << dialog_id << " from " << source;
      auto secret_chat_id = dialog_id.get_secret_chat_id();
      {
        auto user_id = td_->contacts_manager_->get_secret_chat_user_id(secret_chat_id);
        Dialog *user_d = get_dialog_force(DialogId(user_id), source);
        if (user_d != nullptr && user_d->notification_settings.is_synchronized) {
          VLOG(notifications) << "Copy notification settings from " << user_d->dialog_id << " to " << dialog_id;
          auto new_notification_settings = user_d->notification_settings;
          new_notification_settings.use_default_show_preview = true;
          new_notification_settings.show_preview = false;
          new_notification_settings.is_secret_chat_show_preview_fixed = true;
          update_dialog_notification_settings(dialog_id, &d->notification_settings, new_notification_settings);
        } else {
          d->notification_settings.is_synchronized = true;
        }
      }

      if (G()->parameters().use_message_db && !td_->auth_manager_->is_bot() &&
          !td_->contacts_manager_->get_secret_chat_is_outbound(secret_chat_id)) {
        auto notification_group_id = get_dialog_notification_group_id(dialog_id, d->message_notification_group);
        if (notification_group_id.is_valid()) {
          if (d->new_secret_chat_notification_id.is_valid()) {
            LOG(ERROR) << "Found previously created " << d->new_secret_chat_notification_id << " in " << d->dialog_id
                       << ", when creating it from " << source;
          } else {
            d->new_secret_chat_notification_id = get_next_notification_id(d, notification_group_id, MessageId());
            if (d->new_secret_chat_notification_id.is_valid()) {
              auto date = td_->contacts_manager_->get_secret_chat_date(secret_chat_id);
              bool is_changed = set_dialog_last_notification(dialog_id, d->message_notification_group, date,
                                                             d->new_secret_chat_notification_id, "add_new_secret_chat");
              CHECK(is_changed);
              VLOG(notifications) << "Create " << d->new_secret_chat_notification_id << " with " << secret_chat_id;
              send_closure_later(G()->notification_manager(), &NotificationManager::add_notification,
                                 notification_group_id, NotificationGroupType::SecretChat, dialog_id, date, dialog_id,
                                 false, false, 0, d->new_secret_chat_notification_id,
                                 create_new_secret_chat_notification(), "add_new_secret_chat_notification");
            }
          }
        }
      }
    }
    if (!have_input_peer(dialog_id, AccessRights::Read)) {
      if (!have_dialog_info(dialog_id)) {
        if (expect_no_access && dialog_id.get_type() == DialogType::Channel &&
            td_->contacts_manager_->have_min_channel(dialog_id.get_channel_id())) {
          LOG(INFO) << "Created " << dialog_id << " for min-channel from " << source;
        } else {
          LOG(ERROR) << "Have no info about " << dialog_id << " received from " << source
                     << ", but forced to create it";
        }
      } else if (!expect_no_access) {
        LOG(ERROR) << "Have no access to " << dialog_id << " received from " << source << ", but forced to create it";
      }
    }
  } else if (force_update_dialog_pos) {
    update_dialog_pos(d, "force update dialog pos");
  }
}

Result<vector<MessageId>> MessagesManager::send_message_group(
    DialogId dialog_id, MessageId top_thread_message_id, MessageId reply_to_message_id,
    tl_object_ptr<td_api::messageSendOptions> &&options,
    vector<tl_object_ptr<td_api::InputMessageContent>> &&input_message_contents) {
  if (input_message_contents.size() > MAX_GROUPED_MESSAGES) {
    return Status::Error(400, "Too much messages to send as an album");
  }
  if (input_message_contents.empty()) {
    return Status::Error(400, "There are no messages to send");
  }

  Dialog *d = get_dialog_force(dialog_id, "send_message_group");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }

  TRY_STATUS(can_send_message(dialog_id));
  TRY_RESULT(message_send_options, process_message_send_options(dialog_id, std::move(options)));

  vector<std::pair<unique_ptr<MessageContent>, int32>> message_contents;
  std::unordered_set<MessageContentType, MessageContentTypeHash> message_content_types;
  for (auto &input_message_content : input_message_contents) {
    TRY_RESULT(message_content, process_input_message_content(dialog_id, std::move(input_message_content)));
    TRY_STATUS(can_use_message_send_options(message_send_options, message_content));
    auto message_content_type = message_content.content->get_type();
    if (!is_allowed_media_group_content(message_content_type)) {
      return Status::Error(400, "Invalid message content type");
    }
    message_content_types.insert(message_content_type);

    message_contents.emplace_back(std::move(message_content.content), message_content.ttl);
  }
  if (message_content_types.size() > 1) {
    for (auto message_content_type : message_content_types) {
      if (is_homogenous_media_group_content(message_content_type)) {
        return Status::Error(400, PSLICE() << message_content_type << " can't be mixed with other media types");
      }
    }
  }

  reply_to_message_id = get_reply_to_message_id(d, top_thread_message_id, reply_to_message_id, false);
  TRY_STATUS(can_use_top_thread_message_id(d, top_thread_message_id, reply_to_message_id));

  int64 media_album_id = 0;
  if (message_contents.size() > 1) {
    media_album_id = generate_new_media_album_id();
  }

  // there must be no errors after get_message_to_send calls

  vector<MessageId> result;
  bool need_update_dialog_pos = false;
  for (size_t i = 0; i < message_contents.size(); i++) {
    auto &message_content = message_contents[i];
    Message *m = get_message_to_send(d, top_thread_message_id, reply_to_message_id, message_send_options,
                                     dup_message_content(td_, dialog_id, message_content.first.get(),
                                                         MessageContentDupType::Send, MessageCopyOptions()),
                                     &need_update_dialog_pos, i != 0);
    result.push_back(m->message_id);
    auto ttl = message_content.second;
    if (ttl > 0) {
      m->ttl = ttl;
      m->is_content_secret = is_secret_message_content(m->ttl, m->content->get_type());
    }
    m->media_album_id = media_album_id;

    save_send_message_log_event(dialog_id, m);
    do_send_message(dialog_id, m);

    send_update_new_message(d, m);
  }

  if (need_update_dialog_pos) {
    send_update_chat_last_message(d, "send_message_group");
  }

  return result;
}

class UpdateDialogFilterQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;

 public:
  explicit UpdateDialogFilterQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogFilterId dialog_filter_id, tl_object_ptr<telegram_api::dialogFilter> filter) {
    int32 flags = 0;
    if (filter != nullptr) {
      flags |= telegram_api::messages_updateDialogFilter::FILTER_MASK;
    }
    send_query(G()->net_query_creator().create(
        telegram_api::messages_updateDialogFilter(flags, dialog_filter_id.get(), std::move(filter))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_updateDialogFilter>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    LOG(INFO) << "Receive result for UpdateDialogFilterQuery: " << result_ptr.ok();
    promise_.set_value(Unit());
  }

  void on_error(Status status) final {
    LOG(ERROR) << "Receive error for UpdateDialogFilterQuery: " << status;
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::update_dialog_filter_on_server(unique_ptr<DialogFilter> &&dialog_filter) {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(dialog_filter != nullptr);
  are_dialog_filters_being_synchronized_ = true;
  dialog_filter->remove_secret_chat_dialog_ids();
  auto dialog_filter_id = dialog_filter->dialog_filter_id;
  auto input_dialog_filter = dialog_filter->get_input_dialog_filter();

  auto promise = PromiseCreator::lambda(
      [actor_id = actor_id(this), dialog_filter = std::move(dialog_filter)](Result<Unit> result) mutable {
        send_closure(actor_id, &MessagesManager::on_update_dialog_filter, std::move(dialog_filter),
                     result.is_error() ? result.move_as_error() : Status::OK());
      });
  td_->create_handler<UpdateDialogFilterQuery>(std::move(promise))
      ->send(dialog_filter_id, std::move(input_dialog_filter));
}

void MessagesManager::delete_dialog_filter_on_server(DialogFilterId dialog_filter_id) {
  CHECK(!td_->auth_manager_->is_bot());
  are_dialog_filters_being_synchronized_ = true;
  auto promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_filter_id](Result<Unit> result) {
    send_closure(actor_id, &MessagesManager::on_delete_dialog_filter, dialog_filter_id,
                 result.is_error() ? result.move_as_error() : Status::OK());
  });
  td_->create_handler<UpdateDialogFilterQuery>(std::move(promise))->send(dialog_filter_id, nullptr);
}

void MessagesManager::add_dialog_last_database_message(Dialog *d, unique_ptr<Message> &&last_database_message) {
  CHECK(d != nullptr);
  CHECK(last_database_message != nullptr);
  CHECK(last_database_message->left == nullptr);
  CHECK(last_database_message->right == nullptr);

  auto dialog_id = d->dialog_id;
  auto message_id = last_database_message->message_id;
  CHECK(message_id.is_valid());
  LOG_CHECK(d->last_database_message_id == message_id)
      << message_id << " " << d->last_database_message_id << " " << d->debug_set_dialog_last_database_message_id;

  bool need_update_dialog_pos = false;
  const Message *m = nullptr;
  if (have_input_peer(dialog_id, AccessRights::Read)) {
    bool need_update = false;
    last_database_message->have_previous = false;
    last_database_message->have_next = false;
    last_database_message->from_database = true;
    m = add_message_to_dialog(d, std::move(last_database_message), false, &need_update, &need_update_dialog_pos,
                              "add_dialog_last_database_message 1");
    if (need_update_dialog_pos) {
      LOG(ERROR) << "Need to update pos in " << dialog_id;
    }
  }
  if (m != nullptr) {
    set_dialog_last_message_id(d, m->message_id, "add_dialog_last_database_message 2");
    send_update_chat_last_message(d, "add_dialog_last_database_message 3");
  } else {
    if (d->pending_last_message_date != 0) {
      d->pending_last_message_date = 0;
      d->pending_last_message_id = MessageId();
      need_update_dialog_pos = true;
    }
    on_dialog_updated(dialog_id, "add_dialog_last_database_message 4");  // resave without last database message

    if (!td_->auth_manager_->is_bot() && dialog_id != being_added_dialog_id_ &&
        dialog_id != being_added_by_new_message_dialog_id_ && have_input_peer(dialog_id, AccessRights::Read) &&
        (d->order != DEFAULT_ORDER || is_dialog_sponsored(d))) {
      get_history_from_the_end_impl(d, true, false, Auto());
    }
  }

  if (need_update_dialog_pos) {
    update_dialog_pos(d, "add_dialog_last_database_message 5");
  }
}

void MessagesManager::update_message_max_reply_media_timestamp(const Dialog *d, Message *m,
                                                               bool need_send_update_message_content) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  auto new_max_reply_media_timestamp = -1;
  if (m->reply_to_message_id.is_valid()) {
    auto replied_m = get_message(d, m->reply_to_message_id);
    if (replied_m != nullptr) {
      new_max_reply_media_timestamp = get_message_own_max_media_timestamp(replied_m);
    } else if (!d->deleted_message_ids.count(m->reply_to_message_id) &&
               m->reply_to_message_id > d->last_clear_history_message_id &&
               m->reply_to_message_id > d->max_unavailable_message_id) {
      // replied message isn't deleted and isn't loaded yet
      return;
    }
  }

  if (m->max_reply_media_timestamp == new_max_reply_media_timestamp) {
    return;
  }

  LOG(INFO) << "Set max_reply_media_timestamp in " << m->message_id << " in " << d->dialog_id << " to "
            << new_max_reply_media_timestamp;
  auto old_max_media_timestamp = get_message_max_media_timestamp(m);
  m->max_reply_media_timestamp = new_max_reply_media_timestamp;
  auto new_max_media_timestamp = get_message_max_media_timestamp(m);
  if (need_send_update_message_content && old_max_media_timestamp != new_max_media_timestamp) {
    if (old_max_media_timestamp > new_max_media_timestamp) {
      std::swap(old_max_media_timestamp, new_max_media_timestamp);
    }

    if (has_media_timestamps(get_message_content_text(m->content.get()), old_max_media_timestamp + 1,
                             new_max_media_timestamp)) {
      send_update_message_content_impl(d->dialog_id, m, "update_message_max_reply_media_timestamp");
    }
  }
}

Status MessagesManager::delete_dialog_reply_markup(DialogId dialog_id, MessageId message_id) {
  if (td_->auth_manager_->is_bot()) {
    return Status::Error(400, "Bots can't delete chat reply markup");
  }
  if (message_id.is_scheduled()) {
    return Status::Error(400, "Wrong message identifier specified");
  }
  if (!message_id.is_valid()) {
    return Status::Error(400, "Invalid message identifier specified");
  }

  Dialog *d = get_dialog_force(dialog_id, "delete_dialog_reply_markup");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  if (d->reply_markup_message_id != message_id) {
    return Status::OK();
  }

  Message *m = get_message_force(d, message_id, "delete_dialog_reply_markup");
  CHECK(m != nullptr);
  CHECK(m->reply_markup != nullptr);

  if (m->reply_markup->type == ReplyMarkup::Type::ForceReply) {
    set_dialog_reply_markup(d, MessageId());
  } else if (m->reply_markup->type == ReplyMarkup::Type::ShowKeyboard) {
    if (!m->reply_markup->is_one_time_keyboard) {
      return Status::Error(400, "Do not need to delete non one-time keyboard");
    }
    if (m->reply_markup->is_personal) {
      m->reply_markup->is_personal = false;
      set_dialog_reply_markup(d, message_id);

      on_message_changed(d, m, true, "delete_dialog_reply_markup");
    }
  } else {
    // non-bots can't have messages with RemoveKeyboard
    UNREACHABLE();
  }
  return Status::OK();
}

td_api::object_ptr<td_api::chatFilter> MessagesManager::get_chat_filter_object(const DialogFilter *filter) const {
  auto get_chat_ids = [this,
                       dialog_filter_id = filter->dialog_filter_id](const vector<InputDialogId> &input_dialog_ids) {
    vector<int64> chat_ids;
    chat_ids.reserve(input_dialog_ids.size());
    for (auto &input_dialog_id : input_dialog_ids) {
      auto dialog_id = input_dialog_id.get_dialog_id();
      const Dialog *d = get_dialog(dialog_id);
      if (d != nullptr) {
        if (d->order != DEFAULT_ORDER) {
          chat_ids.push_back(dialog_id.get());
        } else {
          LOG(INFO) << "Skip nonjoined " << dialog_id << " from " << dialog_filter_id;
        }
      } else {
        LOG(ERROR) << "Can't find " << dialog_id << " from " << dialog_filter_id;
      }
    }
    return chat_ids;
  };
  return td_api::make_object<td_api::chatFilter>(
      filter->title, filter->get_icon_name(), get_chat_ids(filter->pinned_dialog_ids),
      get_chat_ids(filter->included_dialog_ids), get_chat_ids(filter->excluded_dialog_ids), filter->exclude_muted,
      filter->exclude_read, filter->exclude_archived, filter->include_contacts, filter->include_non_contacts,
      filter->include_bots, filter->include_groups, filter->include_channels);
}

MessagesManager::Dialog *MessagesManager::get_dialog_force(DialogId dialog_id, const char *source) {
  init();

  auto it = dialogs_.find(dialog_id);
  if (it != dialogs_.end()) {
    Dialog *d = it->second.get();
    LOG_CHECK(d->dialog_id == dialog_id) << d->dialog_id << ' ' << dialog_id;
    return d;
  }

  if (!dialog_id.is_valid() || !G()->parameters().use_message_db || loaded_dialogs_.count(dialog_id) > 0) {
    return nullptr;
  }

  auto r_value = G()->td_db()->get_dialog_db_sync()->get_dialog(dialog_id);
  if (r_value.is_ok()) {
    LOG(INFO) << "Loaded " << dialog_id << " from database from " << source;
    auto d = on_load_dialog_from_database(dialog_id, r_value.move_as_ok(), source);
    LOG_CHECK(d == nullptr || d->dialog_id == dialog_id) << d->dialog_id << ' ' << dialog_id;
    return d;
  } else {
    LOG(INFO) << "Failed to load " << dialog_id << " from database from " << source << ": "
              << r_value.error().message();
    return nullptr;
  }
}

bool MessagesManager::is_dialog_message_notification_disabled(DialogId dialog_id, int32 message_date) const {
  switch (dialog_id.get_type()) {
    case DialogType::User:
      break;
    case DialogType::Chat:
      if (!td_->contacts_manager_->get_chat_is_active(dialog_id.get_chat_id())) {
        return true;
      }
      break;
    case DialogType::Channel:
      if (!td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id()).is_member() ||
          message_date < td_->contacts_manager_->get_channel_date(dialog_id.get_channel_id())) {
        return true;
      }
      break;
    case DialogType::SecretChat:
      if (td_->contacts_manager_->get_secret_chat_state(dialog_id.get_secret_chat_id()) == SecretChatState::Closed) {
        return true;
      }
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }
  if (message_date < authorization_date_) {
    return true;
  }

  return false;
}

void MessagesManager::run_affected_history_query_until_complete(DialogId dialog_id, AffectedHistoryQuery query,
                                                                bool get_affected_messages, Promise<Unit> &&promise) {
  CHECK(!G()->close_flag());
  auto query_promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, query, get_affected_messages,
                                               promise = std::move(promise)](Result<AffectedHistory> &&result) mutable {
    if (result.is_error()) {
      return promise.set_error(result.move_as_error());
    }

    send_closure(actor_id, &MessagesManager::on_get_affected_history, dialog_id, query, get_affected_messages,
                 result.move_as_ok(), std::move(promise));
  });
  query(dialog_id, std::move(query_promise));
}

td_api::object_ptr<td_api::foundMessages> MessagesManager::get_found_messages_object(
    const FoundMessages &found_messages, const char *source) {
  vector<tl_object_ptr<td_api::message>> result;
  result.reserve(found_messages.full_message_ids.size());
  for (const auto &full_message_id : found_messages.full_message_ids) {
    auto message = get_message_object(full_message_id, source);
    if (message != nullptr) {
      result.push_back(std::move(message));
    }
  }

  return td_api::make_object<td_api::foundMessages>(found_messages.total_count, std::move(result),
                                                    found_messages.next_offset);
}

void MessagesManager::schedule_dialog_filters_reload(double timeout) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }
  if (timeout <= 0) {
    timeout = 0.0;
    if (dialog_filters_updated_date_ != 0) {
      dialog_filters_updated_date_ = 0;
      save_dialog_filters();
    }
  }
  LOG(INFO) << "Schedule reload of chat filters in " << timeout;
  reload_dialog_filters_timeout_.set_callback(std::move(MessagesManager::on_reload_dialog_filters_timeout));
  reload_dialog_filters_timeout_.set_callback_data(static_cast<void *>(this));
  reload_dialog_filters_timeout_.set_timeout_in(timeout);
}

int64 MessagesManager::get_dialog_private_order(const DialogList *list, const Dialog *d) const {
  if (list == nullptr || td_->auth_manager_->is_bot()) {
    return 0;
  }

  if (is_dialog_sponsored(d) && list->dialog_list_id == DialogListId(FolderId::main())) {
    return SPONSORED_DIALOG_ORDER;
  }
  if (d->order == DEFAULT_ORDER) {
    return 0;
  }
  auto pinned_order = get_dialog_pinned_order(list, d->dialog_id);
  if (pinned_order != DEFAULT_ORDER) {
    return pinned_order;
  }
  return d->order;
}

void MessagesManager::delete_dialog_filter(DialogFilterId dialog_filter_id, Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  auto dialog_filter = get_dialog_filter(dialog_filter_id);
  if (dialog_filter == nullptr) {
    return promise.set_value(Unit());
  }

  delete_dialog_filter(dialog_filter_id, "delete_dialog_filter");
  save_dialog_filters();
  send_update_chat_filters();

  synchronize_dialog_filters();
  promise.set_value(Unit());
}

void MessagesManager::on_active_dialog_action_timeout_callback(void *messages_manager_ptr, int64 dialog_id_int) {
  if (G()->close_flag()) {
    return;
  }

  auto messages_manager = static_cast<MessagesManager *>(messages_manager_ptr);
  send_closure_later(messages_manager->actor_id(messages_manager), &MessagesManager::on_active_dialog_action_timeout,
                     DialogId(dialog_id_int));
}

void MessagesManager::delete_message_files(DialogId dialog_id, const Message *m) const {
  for (auto file_id : get_message_file_ids(m)) {
    if (need_delete_file({dialog_id, m->message_id}, file_id)) {
      send_closure(G()->file_manager(), &FileManager::delete_file, file_id, Promise<>(), "delete_message_files");
    }
  }
}

void MessagesManager::add_dialog_to_list(Dialog *d, DialogListId dialog_list_id) {
  LOG(INFO) << "Add " << d->dialog_id << " to " << dialog_list_id;
  CHECK(!is_dialog_in_list(d, dialog_list_id));
  d->dialog_list_ids.push_back(dialog_list_id);
}

bool MessagesManager::can_set_game_score(FullMessageId full_message_id) const {
  return can_set_game_score(full_message_id.get_dialog_id(), get_message(full_message_id));
}
}  // namespace td
