//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/NotificationManager.h"

#include "td/telegram/AuthManager.h"
#include "td/telegram/ChannelId.h"
#include "td/telegram/ChatId.h"

#include "td/telegram/ContactsManager.h"
#include "td/telegram/DeviceTokenManager.h"
#include "td/telegram/Document.h"
#include "td/telegram/Document.hpp"
#include "td/telegram/DocumentsManager.h"

#include "td/telegram/Global.h"
#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/ConnectionCreator.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/Photo.h"
#include "td/telegram/Photo.hpp"
#include "td/telegram/SecretChatId.h"
#include "td/telegram/ServerMessageId.h"
#include "td/telegram/StateManager.h"
#include "td/telegram/Td.h"

#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.h"

#include "td/mtproto/AuthKey.h"
#include "td/mtproto/mtproto_api.h"
#include "td/mtproto/PacketInfo.h"
#include "td/mtproto/Transport.h"

#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"

#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/as.h"
#include "td/utils/base64.h"
#include "td/utils/buffer.h"
#include "td/utils/format.h"
#include "td/utils/Gzip.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/tl_parsers.h"
#include "td/utils/utf8.h"







namespace td {
}  // namespace td
namespace td {

Status NotificationManager::process_push_notification_payload(string payload, bool was_encrypted,
                                                              Promise<Unit> &promise) {
  VLOG(notifications) << "Process push notification payload " << payload;
  auto r_json_value = json_decode(payload);
  if (r_json_value.is_error()) {
    return Status::Error("Failed to parse payload as JSON object");
  }

  auto json_value = r_json_value.move_as_ok();
  if (json_value.type() != JsonValue::Type::Object) {
    return Status::Error("Expected a JSON object as push payload");
  }

  auto data = std::move(json_value.get_object());
  int32 sent_date = G()->unix_time();
  if (has_json_object_field(data, "data")) {
    TRY_RESULT(date, get_json_object_int_field(data, "date", true, sent_date));
    if (sent_date - 28 * 86400 <= date && date <= sent_date + 5) {
      sent_date = date;
    }
    TRY_RESULT(data_data, get_json_object_field(data, "data", JsonValue::Type::Object, false));
    data = std::move(data_data.get_object());
  }

  string loc_key;
  JsonObject custom;
  string announcement_message_text;
  vector<string> loc_args;
  string sender_name;
  for (auto &field_value : data) {
    if (field_value.first == "loc_key") {
      if (field_value.second.type() != JsonValue::Type::String) {
        return Status::Error("Expected loc_key as a String");
      }
      loc_key = field_value.second.get_string().str();
    } else if (field_value.first == "loc_args") {
      if (field_value.second.type() != JsonValue::Type::Array) {
        return Status::Error("Expected loc_args as an Array");
      }
      loc_args.reserve(field_value.second.get_array().size());
      for (auto &arg : field_value.second.get_array()) {
        if (arg.type() != JsonValue::Type::String) {
          return Status::Error("Expected loc_arg as a String");
        }
        loc_args.push_back(arg.get_string().str());
      }
    } else if (field_value.first == "custom") {
      if (field_value.second.type() != JsonValue::Type::Object) {
        return Status::Error("Expected custom as an Object");
      }
      custom = std::move(field_value.second.get_object());
    } else if (field_value.first == "message") {
      if (field_value.second.type() != JsonValue::Type::String) {
        return Status::Error("Expected announcement message text as a String");
      }
      announcement_message_text = field_value.second.get_string().str();
    } else if (field_value.first == "google.sent_time") {
      TRY_RESULT(google_sent_time, get_json_object_long_field(data, "google.sent_time"));
      google_sent_time /= 1000;
      if (sent_date - 28 * 86400 <= google_sent_time && google_sent_time <= sent_date + 5) {
        sent_date = narrow_cast<int32>(google_sent_time);
      }
    }
  }

  if (!clean_input_string(loc_key)) {
    return Status::Error(PSLICE() << "Receive invalid loc_key " << format::escaped(loc_key));
  }
  if (loc_key.empty()) {
    return Status::Error("Receive empty loc_key");
  }
  for (auto &loc_arg : loc_args) {
    if (!clean_input_string(loc_arg)) {
      return Status::Error(PSLICE() << "Receive invalid loc_arg " << format::escaped(loc_arg));
    }
  }

  if (loc_key == "MESSAGE_ANNOUNCEMENT") {
    if (announcement_message_text.empty()) {
      return Status::Error("Have empty announcement message text");
    }
    TRY_RESULT(announcement_id, get_json_object_int_field(custom, "announcement"));
    auto &date = announcement_id_date_[announcement_id];
    auto now = G()->unix_time();
    if (date >= now - ANNOUNCEMENT_ID_CACHE_TIME) {
      VLOG(notifications) << "Ignore duplicate announcement " << announcement_id;
      return Status::Error(200, "Immediate success");
    }
    date = now;

    auto update = telegram_api::make_object<telegram_api::updateServiceNotification>(
        telegram_api::updateServiceNotification::INBOX_DATE_MASK, false, G()->unix_time(), string(),
        announcement_message_text, nullptr, vector<telegram_api::object_ptr<telegram_api::MessageEntity>>());
    send_closure(G()->messages_manager(), &MessagesManager::on_update_service_notification, std::move(update), false,
                 std::move(promise));
    save_announcement_ids();
    return Status::OK();
  }
  if (!announcement_message_text.empty()) {
    LOG(ERROR) << "Have non-empty announcement message text with loc_key = " << loc_key;
  }

  if (loc_key == "DC_UPDATE") {
    TRY_RESULT(dc_id, get_json_object_int_field(custom, "dc", false));
    TRY_RESULT(addr, get_json_object_string_field(custom, "addr", false));
    if (!DcId::is_valid(dc_id)) {
      return Status::Error("Invalid datacenter ID");
    }
    if (!clean_input_string(addr)) {
      return Status::Error(PSLICE() << "Receive invalid addr " << format::escaped(addr));
    }
    send_closure(G()->connection_creator(), &ConnectionCreator::on_dc_update, DcId::internal(dc_id), std::move(addr),
                 std::move(promise));
    return Status::OK();
  }

  if (loc_key == "SESSION_REVOKE") {
    if (was_encrypted) {
      send_closure(td_->auth_manager_actor_, &AuthManager::on_authorization_lost, "SESSION_REVOKE");
    } else {
      LOG(ERROR) << "Receive unencrypted SESSION_REVOKE push notification";
    }
    promise.set_value(Unit());
    return Status::OK();
  }

  if (loc_key == "LOCKED_MESSAGE") {
    return Status::Error(200, "Immediate success");
  }

  if (loc_key == "GEO_LIVE_PENDING") {
    td_->messages_manager_->on_update_some_live_location_viewed(std::move(promise));
    return Status::OK();
  }

  if (loc_key == "AUTH_REGION" || loc_key == "AUTH_UNKNOWN") {
    // TODO
    return Status::Error(200, "Immediate success");
  }

  DialogId dialog_id;
  if (has_json_object_field(custom, "from_id")) {
    TRY_RESULT(user_id_int, get_json_object_long_field(custom, "from_id"));
    UserId user_id(user_id_int);
    if (!user_id.is_valid()) {
      return Status::Error("Receive invalid user_id");
    }
    dialog_id = DialogId(user_id);
  }
  if (has_json_object_field(custom, "chat_id")) {
    TRY_RESULT(chat_id_int, get_json_object_long_field(custom, "chat_id"));
    ChatId chat_id(chat_id_int);
    if (!chat_id.is_valid()) {
      return Status::Error("Receive invalid chat_id");
    }
    dialog_id = DialogId(chat_id);
  }
  if (has_json_object_field(custom, "channel_id")) {
    TRY_RESULT(channel_id_int, get_json_object_long_field(custom, "channel_id"));
    ChannelId channel_id(channel_id_int);
    if (!channel_id.is_valid()) {
      return Status::Error("Receive invalid channel_id");
    }
    dialog_id = DialogId(channel_id);
  }
  if (has_json_object_field(custom, "encryption_id")) {
    TRY_RESULT(secret_chat_id_int, get_json_object_int_field(custom, "encryption_id"));
    SecretChatId secret_chat_id(secret_chat_id_int);
    if (!secret_chat_id.is_valid()) {
      return Status::Error("Receive invalid secret_chat_id");
    }
    dialog_id = DialogId(secret_chat_id);
  }
  if (!dialog_id.is_valid()) {
    if (loc_key == "ENCRYPTED_MESSAGE" || loc_key == "MESSAGE_MUTED") {
      return Status::Error(406, "Force loading data from the server");
    }
    return Status::Error("Can't find dialog_id");
  }

  if (loc_key == "READ_HISTORY") {
    if (dialog_id.get_type() == DialogType::SecretChat) {
      return Status::Error("Receive read history in a secret chat");
    }

    TRY_RESULT(max_id, get_json_object_int_field(custom, "max_id"));
    ServerMessageId max_server_message_id(max_id);
    if (!max_server_message_id.is_valid()) {
      return Status::Error("Receive invalid max_id");
    }

    td_->messages_manager_->read_history_inbox(dialog_id, MessageId(max_server_message_id), -1,
                                               "process_push_notification_payload");
    promise.set_value(Unit());
    return Status::OK();
  }

  if (loc_key == "MESSAGE_DELETED") {
    if (dialog_id.get_type() == DialogType::SecretChat) {
      return Status::Error("Receive MESSAGE_DELETED in a secret chat");
    }
    TRY_RESULT(server_message_ids_str, get_json_object_string_field(custom, "messages", false));
    auto server_message_ids = full_split(server_message_ids_str, ',');
    vector<MessageId> message_ids;
    for (const auto &server_message_id_str : server_message_ids) {
      TRY_RESULT(server_message_id_int, to_integer_safe<int32>(server_message_id_str));
      ServerMessageId server_message_id(server_message_id_int);
      if (!server_message_id.is_valid()) {
        return Status::Error("Receive invalid message_id");
      }
      message_ids.push_back(MessageId(server_message_id));
    }
    td_->messages_manager_->remove_message_notifications_by_message_ids(dialog_id, message_ids);
    promise.set_value(Unit());
    return Status::OK();
  }

  if (loc_key == "MESSAGE_MUTED") {
    return Status::Error(406, "Notifications about muted messages force loading data from the server");
  }

  TRY_RESULT(msg_id, get_json_object_int_field(custom, "msg_id"));
  ServerMessageId server_message_id(msg_id);
  if (server_message_id != ServerMessageId() && !server_message_id.is_valid()) {
    return Status::Error("Receive invalid msg_id");
  }

  TRY_RESULT(random_id, get_json_object_long_field(custom, "random_id"));

  UserId sender_user_id;
  DialogId sender_dialog_id;
  if (has_json_object_field(custom, "chat_from_broadcast_id")) {
    TRY_RESULT(sender_channel_id_int, get_json_object_long_field(custom, "chat_from_broadcast_id"));
    sender_dialog_id = DialogId(ChannelId(sender_channel_id_int));
    if (!sender_dialog_id.is_valid()) {
      return Status::Error("Receive invalid chat_from_broadcast_id");
    }
  } else if (has_json_object_field(custom, "chat_from_group_id")) {
    TRY_RESULT(sender_channel_id_int, get_json_object_long_field(custom, "chat_from_group_id"));
    sender_dialog_id = DialogId(ChannelId(sender_channel_id_int));
    if (!sender_dialog_id.is_valid()) {
      return Status::Error("Receive invalid chat_from_group_id");
    }
  } else if (has_json_object_field(custom, "chat_from_id")) {
    TRY_RESULT(sender_user_id_int, get_json_object_long_field(custom, "chat_from_id"));
    sender_user_id = UserId(sender_user_id_int);
    if (!sender_user_id.is_valid()) {
      return Status::Error("Receive invalid chat_from_id");
    }
  } else if (dialog_id.get_type() == DialogType::User) {
    sender_user_id = dialog_id.get_user_id();
  } else if (dialog_id.get_type() == DialogType::Channel) {
    sender_dialog_id = dialog_id;
  }

  TRY_RESULT(contains_mention_int, get_json_object_int_field(custom, "mention"));
  bool contains_mention = contains_mention_int != 0;

  if (begins_with(loc_key, "CHANNEL_MESSAGE") || loc_key == "CHANNEL_ALBUM") {
    if (dialog_id.get_type() != DialogType::Channel) {
      return Status::Error("Receive wrong chat type");
    }
    loc_key = loc_key.substr(8);
  }
  if (begins_with(loc_key, "CHAT_")) {
    auto dialog_type = dialog_id.get_type();
    if (dialog_type != DialogType::Chat && dialog_type != DialogType::Channel) {
      return Status::Error("Receive wrong chat type");
    }

    if (begins_with(loc_key, "CHAT_MESSAGE") || loc_key == "CHAT_ALBUM") {
      loc_key = loc_key.substr(5);
    }
    if (loc_args.empty()) {
      return Status::Error("Expect sender name as first argument");
    }
    sender_name = std::move(loc_args[0]);
    loc_args.erase(loc_args.begin());
  }
  if (begins_with(loc_key, "MESSAGE") && !server_message_id.is_valid()) {
    return Status::Error("Receive no message ID");
  }
  if (begins_with(loc_key, "ENCRYPT") || random_id != 0) {
    if (dialog_id.get_type() != DialogType::SecretChat) {
      return Status::Error("Receive wrong chat type");
    }
  }
  if (server_message_id.is_valid() && dialog_id.get_type() == DialogType::SecretChat) {
    return Status::Error("Receive message ID in secret chat push");
  }

  if (begins_with(loc_key, "ENCRYPTION_")) {
    // TODO ENCRYPTION_REQUEST/ENCRYPTION_ACCEPT notifications
    return Status::Error(406, "New secret chat notification is not supported");
  }

  if (begins_with(loc_key, "PHONE_CALL_") || begins_with(loc_key, "VIDEO_CALL_")) {
    // TODO PHONE_CALL_REQUEST/PHONE_CALL_DECLINE/PHONE_CALL_MISSED/VIDEO_CALL_REQUEST/VIDEO_CALL_MISSED notifications
    return Status::Error(406, "Phone call notification is not supported");
  }

  loc_key = convert_loc_key(loc_key);
  if (loc_key.empty()) {
    return Status::Error("Push type is unknown");
  }

  if (loc_args.empty()) {
    return Status::Error("Expected chat name as next argument");
  }
  if (dialog_id.get_type() == DialogType::User) {
    sender_name = std::move(loc_args[0]);
  } else if ((sender_user_id.is_valid() || sender_dialog_id.is_valid()) && begins_with(loc_key, "PINNED_")) {
    if (loc_args.size() < 2) {
      return Status::Error("Expected chat title as the last argument");
    }
    loc_args.pop_back();
  }
  // chat title for CHAT_*, CHANNEL_* and ENCRYPTED_MESSAGE, sender name for MESSAGE_* and CONTACT_JOINED
  // chat title or sender name for PINNED_*
  loc_args.erase(loc_args.begin());

  string arg;
  if (loc_key == "MESSAGE_GAME_SCORE") {
    if (loc_args.size() != 2) {
      return Status::Error("Expected 2 arguments for MESSAGE_GAME_SCORE");
    }
    TRY_RESULT(score, to_integer_safe<int32>(loc_args[1]));
    if (score < 0) {
      return Status::Error("Expected score to be non-negative");
    }
    arg = PSTRING() << loc_args[1] << ' ' << loc_args[0];
    loc_args.clear();
  }
  if (loc_args.size() > 1) {
    return Status::Error("Receive too much arguments");
  }

  if (loc_args.size() == 1) {
    arg = std::move(loc_args[0]);
  }

  if (sender_user_id.is_valid() && !td_->contacts_manager_->have_user_force(sender_user_id)) {
    int64 sender_access_hash = -1;
    telegram_api::object_ptr<telegram_api::UserProfilePhoto> sender_photo;
    TRY_RESULT(mtpeer, get_json_object_field(custom, "mtpeer", JsonValue::Type::Object));
    if (mtpeer.type() != JsonValue::Type::Null) {
      TRY_RESULT(ah, get_json_object_string_field(mtpeer.get_object(), "ah"));
      if (!ah.empty()) {
        TRY_RESULT_ASSIGN(sender_access_hash, to_integer_safe<int64>(ah));
      }
      TRY_RESULT(ph, get_json_object_field(mtpeer.get_object(), "ph", JsonValue::Type::Object));
      if (ph.type() != JsonValue::Type::Null) {
        // TODO parse sender photo
      }
    }

    int32 flags = USER_FLAG_IS_INACCESSIBLE;
    if (sender_access_hash != -1) {
      // set phone number flag to show that this is a full access hash
      flags |= USER_FLAG_HAS_ACCESS_HASH | USER_FLAG_HAS_PHONE_NUMBER;
    }
    auto user_name = sender_user_id.get() == 136817688 ? "Channel" : sender_name;
    auto user = telegram_api::make_object<telegram_api::user>(
        flags, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/, false /*ignored*/,
        sender_user_id.get(), sender_access_hash, user_name, string(), string(), string(), std::move(sender_photo),
        nullptr, 0, Auto(), string(), string());
    td_->contacts_manager_->on_get_user(std::move(user), "process_push_notification_payload");
  }

  Photo attached_photo;
  Document attached_document;
  if (has_json_object_field(custom, "attachb64")) {
    TRY_RESULT(attachb64, get_json_object_string_field(custom, "attachb64", false));
    TRY_RESULT(attach, base64url_decode(attachb64));

    TlParser gzip_parser(attach);
    int32 id = gzip_parser.fetch_int();
    if (gzip_parser.get_error()) {
      return Status::Error(PSLICE() << "Failed to parse attach: " << gzip_parser.get_error());
    }
    BufferSlice buffer;
    if (id == mtproto_api::gzip_packed::ID) {
      mtproto_api::gzip_packed gzip(gzip_parser);
      gzip_parser.fetch_end();
      if (gzip_parser.get_error()) {
        return Status::Error(PSLICE() << "Failed to parse mtproto_api::gzip_packed in attach: "
                                      << gzip_parser.get_error());
      }
      buffer = gzdecode(gzip.packed_data_);
      if (buffer.empty()) {
        return Status::Error("Failed to uncompress attach");
      }
    } else {
      buffer = BufferSlice(attach);
    }

    TlBufferParser parser(&buffer);
    auto result = telegram_api::Object::fetch(parser);
    parser.fetch_end();
    const char *error = parser.get_error();
    if (error != nullptr) {
      LOG(ERROR) << "Can't parse attach: " << Slice(error) << " at " << parser.get_error_pos() << ": "
                 << format::as_hex_dump<4>(Slice(attach));
    } else {
      switch (result->get_id()) {
        case telegram_api::photo::ID:
          if (ends_with(loc_key, "MESSAGE_PHOTO") || ends_with(loc_key, "MESSAGE_TEXT")) {
            VLOG(notifications) << "Have attached photo";
            loc_key.resize(loc_key.rfind('_') + 1);
            loc_key += "PHOTO";
            attached_photo = get_photo(td_->file_manager_.get(),
                                       telegram_api::move_object_as<telegram_api::photo>(result), dialog_id);
          } else {
            LOG(ERROR) << "Receive attached photo for " << loc_key;
          }
          break;
        case telegram_api::document::ID: {
          if (ends_with(loc_key, "MESSAGE_ANIMATION") || ends_with(loc_key, "MESSAGE_AUDIO") ||
              ends_with(loc_key, "MESSAGE_DOCUMENT") || ends_with(loc_key, "MESSAGE_STICKER") ||
              ends_with(loc_key, "MESSAGE_VIDEO") || ends_with(loc_key, "MESSAGE_VIDEO_NOTE") ||
              ends_with(loc_key, "MESSAGE_VOICE_NOTE") || ends_with(loc_key, "MESSAGE_TEXT")) {
            VLOG(notifications) << "Have attached document";
            attached_document = td_->documents_manager_->on_get_document(
                telegram_api::move_object_as<telegram_api::document>(result), dialog_id);
            if (!attached_document.empty()) {
              if (ends_with(loc_key, "_NOTE")) {
                loc_key.resize(loc_key.rfind('_'));
              }
              loc_key.resize(loc_key.rfind('_') + 1);

              auto type = [attached_document] {
                switch (attached_document.type) {
                  case Document::Type::Animation:
                    return "ANIMATION";
                  case Document::Type::Audio:
                    return "AUDIO";
                  case Document::Type::General:
                    return "DOCUMENT";
                  case Document::Type::Sticker:
                    return "STICKER";
                  case Document::Type::Video:
                    return "VIDEO";
                  case Document::Type::VideoNote:
                    return "VIDEO_NOTE";
                  case Document::Type::VoiceNote:
                    return "VOICE_NOTE";
                  case Document::Type::Unknown:
                  default:
                    UNREACHABLE();
                    return "UNREACHABLE";
                }
              }();

              loc_key += type;
            }
          } else {
            LOG(ERROR) << "Receive attached document for " << loc_key;
          }
          break;
        }
        default:
          LOG(ERROR) << "Receive unexpected attached " << to_string(result);
      }
    }
  }
  if (!arg.empty()) {
    uint32 emoji = [&] {
      if (ends_with(loc_key, "PHOTO")) {
        return 0x1F5BC;
      }
      if (ends_with(loc_key, "ANIMATION")) {
        return 0x1F3AC;
      }
      if (ends_with(loc_key, "DOCUMENT")) {
        return 0x1F4CE;
      }
      if (ends_with(loc_key, "VIDEO")) {
        return 0x1F4F9;
      }
      return 0;
    }();
    if (emoji != 0) {
      string prefix;
      append_utf8_character(prefix, emoji);
      prefix += ' ';
      if (begins_with(arg, prefix)) {
        arg = arg.substr(prefix.size());
      }
    }
  }

  if (has_json_object_field(custom, "edit_date")) {
    if (random_id != 0) {
      return Status::Error("Receive edit of secret message");
    }
    TRY_RESULT(edit_date, get_json_object_int_field(custom, "edit_date"));
    if (edit_date <= 0) {
      return Status::Error("Receive wrong edit date");
    }
    edit_message_push_notification(dialog_id, MessageId(server_message_id), edit_date, std::move(loc_key),
                                   std::move(arg), std::move(attached_photo), std::move(attached_document), 0,
                                   std::move(promise));
  } else {
    bool is_from_scheduled = has_json_object_field(custom, "schedule");
    bool is_silent = has_json_object_field(custom, "silent");
    add_message_push_notification(dialog_id, MessageId(server_message_id), random_id, sender_user_id, sender_dialog_id,
                                  std::move(sender_name), sent_date, is_from_scheduled, contains_mention, is_silent,
                                  is_silent, std::move(loc_key), std::move(arg), std::move(attached_photo),
                                  std::move(attached_document), NotificationId(), 0, std::move(promise));
  }
  return Status::OK();
}
}  // namespace td
