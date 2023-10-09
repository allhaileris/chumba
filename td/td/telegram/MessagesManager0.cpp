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

class EditPeerFoldersQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit EditPeerFoldersQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, FolderId folder_id) {
    dialog_id_ = dialog_id;

    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);

    vector<telegram_api::object_ptr<telegram_api::inputFolderPeer>> input_folder_peers;
    input_folder_peers.push_back(
        telegram_api::make_object<telegram_api::inputFolderPeer>(std::move(input_peer), folder_id.get()));
    send_query(G()->net_query_creator().create(telegram_api::folders_editPeerFolders(std::move(input_folder_peers))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::folders_editPeerFolders>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for EditPeerFoldersQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "EditPeerFoldersQuery")) {
      LOG(INFO) << "Receive error for EditPeerFoldersQuery: " << status;
    }

    // trying to repair folder ID for this dialog
    td_->messages_manager_->get_dialog_info_full(dialog_id_, Auto(), "EditPeerFoldersQuery");

    promise_.set_error(std::move(status));
  }
};

template <class StorerT>
void MessagesManager::Message::store(StorerT &storer) const {
  using td::store;
  bool has_sender = sender_user_id.is_valid();
  bool has_edit_date = edit_date > 0;
  bool has_random_id = random_id != 0;
  bool is_forwarded = forward_info != nullptr;
  bool is_reply = reply_to_message_id.is_valid();
  bool is_reply_to_random_id = reply_to_random_id != 0;
  bool is_via_bot = via_bot_user_id.is_valid();
  bool has_view_count = view_count > 0;
  bool has_reply_markup = reply_markup != nullptr;
  bool has_ttl = ttl != 0;
  bool has_author_signature = !author_signature.empty();
  bool has_forward_author_signature = is_forwarded && !forward_info->author_signature.empty();
  bool has_media_album_id = media_album_id != 0;
  bool has_forward_from =
      is_forwarded && (forward_info->from_dialog_id.is_valid() || forward_info->from_message_id.is_valid());
  bool has_send_date = message_id.is_yet_unsent() && send_date != 0;
  bool has_flags2 = true;
  bool has_notification_id = notification_id.is_valid();
  bool has_forward_sender_name = is_forwarded && !forward_info->sender_name.empty();
  bool has_send_error_code = send_error_code != 0;
  bool has_real_forward_from = real_forward_from_dialog_id.is_valid() && real_forward_from_message_id.is_valid();
  bool has_legacy_layer = legacy_layer != 0;
  bool has_restriction_reasons = !restriction_reasons.empty();
  bool has_forward_psa_type = is_forwarded && !forward_info->psa_type.empty();
  bool has_forward_count = forward_count > 0;
  bool has_reply_info = !reply_info.is_empty();
  bool has_sender_dialog_id = sender_dialog_id.is_valid();
  bool has_reply_in_dialog_id = is_reply && reply_in_dialog_id.is_valid();
  bool has_top_thread_message_id = top_thread_message_id.is_valid();
  bool has_thread_draft_message = thread_draft_message != nullptr;
  bool has_local_thread_message_ids = !local_thread_message_ids.empty();
  bool has_linked_top_thread_message_id = linked_top_thread_message_id.is_valid();
  bool has_interaction_info_update_date = interaction_info_update_date != 0;
  bool has_send_emoji = !send_emoji.empty();
  bool is_imported = is_forwarded && forward_info->is_imported;
  bool has_ttl_period = ttl_period != 0;
  bool has_max_reply_media_timestamp = max_reply_media_timestamp >= 0;
  bool are_message_media_timestamp_entities_found = true;
  bool has_flags3 = true;
  BEGIN_STORE_FLAGS();
  STORE_FLAG(is_channel_post);
  STORE_FLAG(is_outgoing);
  STORE_FLAG(is_failed_to_send);
  STORE_FLAG(disable_notification);
  STORE_FLAG(contains_mention);
  STORE_FLAG(from_background);
  STORE_FLAG(disable_web_page_preview);
  STORE_FLAG(clear_draft);
  STORE_FLAG(have_previous);
  STORE_FLAG(have_next);
  STORE_FLAG(has_sender);
  STORE_FLAG(has_edit_date);
  STORE_FLAG(has_random_id);
  STORE_FLAG(is_forwarded);
  STORE_FLAG(is_reply);
  STORE_FLAG(is_reply_to_random_id);
  STORE_FLAG(is_via_bot);
  STORE_FLAG(has_view_count);
  STORE_FLAG(has_reply_markup);
  STORE_FLAG(has_ttl);
  STORE_FLAG(has_author_signature);
  STORE_FLAG(has_forward_author_signature);
  STORE_FLAG(had_reply_markup);
  STORE_FLAG(contains_unread_mention);
  STORE_FLAG(has_media_album_id);
  STORE_FLAG(has_forward_from);
  STORE_FLAG(in_game_share);
  STORE_FLAG(is_content_secret);
  STORE_FLAG(has_send_date);
  STORE_FLAG(has_flags2);
  END_STORE_FLAGS();
  if (has_flags2) {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(has_notification_id);
    STORE_FLAG(is_mention_notification_disabled);
    STORE_FLAG(had_forward_info);
    STORE_FLAG(has_forward_sender_name);
    STORE_FLAG(has_send_error_code);
    STORE_FLAG(hide_via_bot);
    STORE_FLAG(is_bot_start_message);
    STORE_FLAG(has_real_forward_from);
    STORE_FLAG(has_legacy_layer);
    STORE_FLAG(hide_edit_date);
    STORE_FLAG(has_restriction_reasons);
    STORE_FLAG(is_from_scheduled);
    STORE_FLAG(is_copy);
    STORE_FLAG(has_forward_psa_type);
    STORE_FLAG(has_forward_count);
    STORE_FLAG(has_reply_info);
    STORE_FLAG(has_sender_dialog_id);
    STORE_FLAG(has_reply_in_dialog_id);
    STORE_FLAG(has_top_thread_message_id);
    STORE_FLAG(has_thread_draft_message);
    STORE_FLAG(has_local_thread_message_ids);
    STORE_FLAG(has_linked_top_thread_message_id);
    STORE_FLAG(is_pinned);
    STORE_FLAG(has_interaction_info_update_date);
    STORE_FLAG(has_send_emoji);
    STORE_FLAG(is_imported);
    STORE_FLAG(has_ttl_period);
    STORE_FLAG(has_max_reply_media_timestamp);
    STORE_FLAG(are_message_media_timestamp_entities_found);
    STORE_FLAG(has_flags3);
    END_STORE_FLAGS();
  }
  if (has_flags3) {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(noforwards);
    STORE_FLAG(has_explicit_sender);
    END_STORE_FLAGS();
  }

  store(message_id, storer);
  if (has_sender) {
    store(sender_user_id, storer);
  }
  store(date, storer);
  if (has_edit_date) {
    store(edit_date, storer);
  }
  if (has_send_date) {
    store(send_date, storer);
  }
  if (has_random_id) {
    store(random_id, storer);
  }
  if (is_forwarded) {
    store(forward_info->sender_user_id, storer);
    store(forward_info->date, storer);
    store(forward_info->sender_dialog_id, storer);
    store(forward_info->message_id, storer);
    if (has_forward_author_signature) {
      store(forward_info->author_signature, storer);
    }
    if (has_forward_sender_name) {
      store(forward_info->sender_name, storer);
    }
    if (has_forward_from) {
      store(forward_info->from_dialog_id, storer);
      store(forward_info->from_message_id, storer);
    }
    if (has_forward_psa_type) {
      store(forward_info->psa_type, storer);
    }
  }
  if (has_real_forward_from) {
    store(real_forward_from_dialog_id, storer);
    store(real_forward_from_message_id, storer);
  }
  if (is_reply) {
    store(reply_to_message_id, storer);
  }
  if (is_reply_to_random_id) {
    store(reply_to_random_id, storer);
  }
  if (is_via_bot) {
    store(via_bot_user_id, storer);
  }
  if (has_view_count) {
    store(view_count, storer);
  }
  if (has_forward_count) {
    store(forward_count, storer);
  }
  if (has_reply_info) {
    store(reply_info, storer);
  }
  if (has_ttl) {
    store(ttl, storer);
    store_time(ttl_expires_at, storer);
  }
  if (has_send_error_code) {
    store(send_error_code, storer);
    store(send_error_message, storer);
    if (send_error_code == 429) {
      store_time(try_resend_at, storer);
    }
  }
  if (has_author_signature) {
    store(author_signature, storer);
  }
  if (has_media_album_id) {
    store(media_album_id, storer);
  }
  if (has_notification_id) {
    store(notification_id, storer);
  }
  if (has_legacy_layer) {
    store(legacy_layer, storer);
  }
  if (has_restriction_reasons) {
    store(restriction_reasons, storer);
  }
  if (has_sender_dialog_id) {
    store(sender_dialog_id, storer);
  }
  if (has_reply_in_dialog_id) {
    store(reply_in_dialog_id, storer);
  }
  if (has_top_thread_message_id) {
    store(top_thread_message_id, storer);
  }
  if (has_thread_draft_message) {
    store(thread_draft_message, storer);
  }
  if (has_local_thread_message_ids) {
    store(local_thread_message_ids, storer);
  }
  if (has_linked_top_thread_message_id) {
    store(linked_top_thread_message_id, storer);
  }
  if (has_interaction_info_update_date) {
    store(interaction_info_update_date, storer);
  }
  if (has_send_emoji) {
    store(send_emoji, storer);
  }
  store_message_content(content.get(), storer);
  if (has_reply_markup) {
    store(reply_markup, storer);
  }
  if (has_ttl_period) {
    store(ttl_period, storer);
  }
  if (has_max_reply_media_timestamp) {
    store(max_reply_media_timestamp, storer);
  }
}

// do not forget to resolve message dependencies
template <class ParserT>
void MessagesManager::Message::parse(ParserT &parser) {
  using td::parse;
  bool has_sender;
  bool has_edit_date;
  bool has_random_id;
  bool is_forwarded;
  bool is_reply;
  bool is_reply_to_random_id;
  bool is_via_bot;
  bool has_view_count;
  bool has_reply_markup;
  bool has_ttl;
  bool has_author_signature;
  bool has_forward_author_signature;
  bool has_media_album_id;
  bool has_forward_from;
  bool has_send_date;
  bool has_flags2;
  bool has_notification_id = false;
  bool has_forward_sender_name = false;
  bool has_send_error_code = false;
  bool has_real_forward_from = false;
  bool has_legacy_layer = false;
  bool has_restriction_reasons = false;
  bool has_forward_psa_type = false;
  bool has_forward_count = false;
  bool has_reply_info = false;
  bool has_sender_dialog_id = false;
  bool has_reply_in_dialog_id = false;
  bool has_top_thread_message_id = false;
  bool has_thread_draft_message = false;
  bool has_local_thread_message_ids = false;
  bool has_linked_top_thread_message_id = false;
  bool has_interaction_info_update_date = false;
  bool has_send_emoji = false;
  bool is_imported = false;
  bool has_ttl_period = false;
  bool has_max_reply_media_timestamp = false;
  bool has_flags3 = false;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(is_channel_post);
  PARSE_FLAG(is_outgoing);
  PARSE_FLAG(is_failed_to_send);
  PARSE_FLAG(disable_notification);
  PARSE_FLAG(contains_mention);
  PARSE_FLAG(from_background);
  PARSE_FLAG(disable_web_page_preview);
  PARSE_FLAG(clear_draft);
  PARSE_FLAG(have_previous);
  PARSE_FLAG(have_next);
  PARSE_FLAG(has_sender);
  PARSE_FLAG(has_edit_date);
  PARSE_FLAG(has_random_id);
  PARSE_FLAG(is_forwarded);
  PARSE_FLAG(is_reply);
  PARSE_FLAG(is_reply_to_random_id);
  PARSE_FLAG(is_via_bot);
  PARSE_FLAG(has_view_count);
  PARSE_FLAG(has_reply_markup);
  PARSE_FLAG(has_ttl);
  PARSE_FLAG(has_author_signature);
  PARSE_FLAG(has_forward_author_signature);
  PARSE_FLAG(had_reply_markup);
  PARSE_FLAG(contains_unread_mention);
  PARSE_FLAG(has_media_album_id);
  PARSE_FLAG(has_forward_from);
  PARSE_FLAG(in_game_share);
  PARSE_FLAG(is_content_secret);
  PARSE_FLAG(has_send_date);
  PARSE_FLAG(has_flags2);
  END_PARSE_FLAGS();
  if (has_flags2) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(has_notification_id);
    PARSE_FLAG(is_mention_notification_disabled);
    PARSE_FLAG(had_forward_info);
    PARSE_FLAG(has_forward_sender_name);
    PARSE_FLAG(has_send_error_code);
    PARSE_FLAG(hide_via_bot);
    PARSE_FLAG(is_bot_start_message);
    PARSE_FLAG(has_real_forward_from);
    PARSE_FLAG(has_legacy_layer);
    PARSE_FLAG(hide_edit_date);
    PARSE_FLAG(has_restriction_reasons);
    PARSE_FLAG(is_from_scheduled);
    PARSE_FLAG(is_copy);
    PARSE_FLAG(has_forward_psa_type);
    PARSE_FLAG(has_forward_count);
    PARSE_FLAG(has_reply_info);
    PARSE_FLAG(has_sender_dialog_id);
    PARSE_FLAG(has_reply_in_dialog_id);
    PARSE_FLAG(has_top_thread_message_id);
    PARSE_FLAG(has_thread_draft_message);
    PARSE_FLAG(has_local_thread_message_ids);
    PARSE_FLAG(has_linked_top_thread_message_id);
    PARSE_FLAG(is_pinned);
    PARSE_FLAG(has_interaction_info_update_date);
    PARSE_FLAG(has_send_emoji);
    PARSE_FLAG(is_imported);
    PARSE_FLAG(has_ttl_period);
    PARSE_FLAG(has_max_reply_media_timestamp);
    PARSE_FLAG(are_media_timestamp_entities_found);
    PARSE_FLAG(has_flags3);
    END_PARSE_FLAGS();
  }
  if (has_flags3) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(noforwards);
    PARSE_FLAG(has_explicit_sender);
    END_PARSE_FLAGS();
  }

  parse(message_id, parser);
  random_y = get_random_y(message_id);
  if (has_sender) {
    parse(sender_user_id, parser);
  }
  parse(date, parser);
  if (has_edit_date) {
    parse(edit_date, parser);
  }
  if (has_send_date) {
    CHECK(message_id.is_valid() || message_id.is_valid_scheduled());
    CHECK(message_id.is_yet_unsent());
    parse(send_date, parser);
  } else if (message_id.is_valid() && message_id.is_yet_unsent()) {
    send_date = date;  // for backward compatibility
  }
  if (has_random_id) {
    parse(random_id, parser);
  }
  if (is_forwarded) {
    forward_info = make_unique<MessageForwardInfo>();
    parse(forward_info->sender_user_id, parser);
    parse(forward_info->date, parser);
    parse(forward_info->sender_dialog_id, parser);
    parse(forward_info->message_id, parser);
    if (has_forward_author_signature) {
      parse(forward_info->author_signature, parser);
    }
    if (has_forward_sender_name) {
      parse(forward_info->sender_name, parser);
    }
    if (has_forward_from) {
      parse(forward_info->from_dialog_id, parser);
      parse(forward_info->from_message_id, parser);
    }
    if (has_forward_psa_type) {
      parse(forward_info->psa_type, parser);
    }
    forward_info->is_imported = is_imported;
  }
  if (has_real_forward_from) {
    parse(real_forward_from_dialog_id, parser);
    parse(real_forward_from_message_id, parser);
  }
  if (is_reply) {
    parse(reply_to_message_id, parser);
  }
  if (is_reply_to_random_id) {
    parse(reply_to_random_id, parser);
  }
  if (is_via_bot) {
    parse(via_bot_user_id, parser);
  }
  if (has_view_count) {
    parse(view_count, parser);
  }
  if (has_forward_count) {
    parse(forward_count, parser);
  }
  if (has_reply_info) {
    parse(reply_info, parser);
  }
  if (has_ttl) {
    parse(ttl, parser);
    parse_time(ttl_expires_at, parser);
  }
  if (has_send_error_code) {
    parse(send_error_code, parser);
    parse(send_error_message, parser);
    if (send_error_code == 429) {
      parse_time(try_resend_at, parser);
    }
  }
  if (has_author_signature) {
    parse(author_signature, parser);
  }
  if (has_media_album_id) {
    parse(media_album_id, parser);
  }
  if (has_notification_id) {
    parse(notification_id, parser);
  }
  if (has_legacy_layer) {
    parse(legacy_layer, parser);
  }
  if (has_restriction_reasons) {
    parse(restriction_reasons, parser);
  }
  if (has_sender_dialog_id) {
    parse(sender_dialog_id, parser);
  }
  if (has_reply_in_dialog_id) {
    parse(reply_in_dialog_id, parser);
  }
  if (has_top_thread_message_id) {
    parse(top_thread_message_id, parser);
  }
  if (has_thread_draft_message) {
    parse(thread_draft_message, parser);
  }
  if (has_local_thread_message_ids) {
    parse(local_thread_message_ids, parser);
  }
  if (has_linked_top_thread_message_id) {
    parse(linked_top_thread_message_id, parser);
  }
  if (has_interaction_info_update_date) {
    parse(interaction_info_update_date, parser);
  }
  if (has_send_emoji) {
    parse(send_emoji, parser);
  }
  parse_message_content(content, parser);
  if (has_reply_markup) {
    parse(reply_markup, parser);
  }
  if (has_ttl_period) {
    parse(ttl_period, parser);
  }
  if (has_max_reply_media_timestamp) {
    parse(max_reply_media_timestamp, parser);
  }

  CHECK(content != nullptr);
  is_content_secret |=
      is_secret_message_content(ttl, content->get_type());  // repair is_content_secret for old messages
  if (hide_edit_date && content->get_type() == MessageContentType::LiveLocation) {
    hide_edit_date = false;
  }
}

template <class StorerT>
void MessagesManager::NotificationGroupInfo::store(StorerT &storer) const {
  using td::store;
  store(group_id, storer);
  store(last_notification_date, storer);
  store(last_notification_id, storer);
  store(max_removed_notification_id, storer);
  store(max_removed_message_id, storer);
}

template <class ParserT>
void MessagesManager::NotificationGroupInfo::parse(ParserT &parser) {
  using td::parse;
  parse(group_id, parser);
  parse(last_notification_date, parser);
  parse(last_notification_id, parser);
  parse(max_removed_notification_id, parser);
  if (parser.version() >= static_cast<int32>(Version::AddNotificationGroupInfoMaxRemovedMessageId)) {
    parse(max_removed_message_id, parser);
  }
}

template <class StorerT>
void MessagesManager::Dialog::store(StorerT &storer) const {
  using td::store;
  const Message *last_database_message = nullptr;
  if (last_database_message_id.is_valid()) {
    last_database_message = get_message(this, last_database_message_id);
  }

  auto dialog_type = dialog_id.get_type();
  bool has_draft_message = draft_message != nullptr;
  bool has_last_database_message = last_database_message != nullptr;
  bool has_first_database_message_id = first_database_message_id.is_valid();
  bool has_first_database_message_id_by_index = true;
  bool has_message_count_by_index = true;
  bool has_client_data = !client_data.empty();
  bool has_last_read_all_mentions_message_id = last_read_all_mentions_message_id.is_valid();
  bool has_max_unavailable_message_id = max_unavailable_message_id.is_valid();
  bool has_local_unread_count = local_unread_count != 0;
  bool has_deleted_last_message = delete_last_message_date > 0;
  bool has_last_clear_history_message_id = last_clear_history_message_id.is_valid();
  bool has_last_database_message_id = !has_last_database_message && last_database_message_id.is_valid();
  bool has_message_notification_group =
      message_notification_group.group_id.is_valid() && !message_notification_group.try_reuse;
  bool has_mention_notification_group =
      mention_notification_group.group_id.is_valid() && !mention_notification_group.try_reuse;
  bool has_new_secret_chat_notification_id = new_secret_chat_notification_id.is_valid();
  bool has_pinned_message_notification = pinned_message_notification_message_id.is_valid();
  bool has_last_pinned_message_id = last_pinned_message_id.is_valid();
  bool has_flags2 = true;
  bool has_max_notification_message_id =
      max_notification_message_id.is_valid() && max_notification_message_id > last_new_message_id;
  bool has_folder_id = folder_id != FolderId();
  bool has_pending_read_channel_inbox = pending_read_channel_inbox_pts != 0;
  bool has_last_yet_unsent_message = last_message_id.is_valid() && last_message_id.is_yet_unsent();
  bool has_active_group_call_id = active_group_call_id.is_valid();
  bool has_message_ttl = !message_ttl.is_empty();
  bool has_default_join_group_call_as_dialog_id = default_join_group_call_as_dialog_id.is_valid();
  bool store_has_bots = dialog_type == DialogType::Chat || dialog_type == DialogType::Channel;
  bool has_theme_name = !theme_name.empty();
  bool has_flags3 = true;
  bool has_pending_join_requests = pending_join_request_count != 0;
  bool has_action_bar = action_bar != nullptr;
  bool has_default_send_message_as_dialog_id = default_send_message_as_dialog_id.is_valid();
  BEGIN_STORE_FLAGS();
  STORE_FLAG(has_draft_message);
  STORE_FLAG(has_last_database_message);
  STORE_FLAG(false);  // legacy_know_can_report_spam
  STORE_FLAG(false);  // action_bar->can_report_spam
  STORE_FLAG(has_first_database_message_id);
  STORE_FLAG(false);  // legacy_is_pinned
  STORE_FLAG(has_first_database_message_id_by_index);
  STORE_FLAG(has_message_count_by_index);
  STORE_FLAG(has_client_data);
  STORE_FLAG(need_restore_reply_markup);
  STORE_FLAG(have_full_history);
  STORE_FLAG(has_last_read_all_mentions_message_id);
  STORE_FLAG(has_max_unavailable_message_id);
  STORE_FLAG(is_last_read_inbox_message_id_inited);
  STORE_FLAG(is_last_read_outbox_message_id_inited);
  STORE_FLAG(has_local_unread_count);
  STORE_FLAG(has_deleted_last_message);
  STORE_FLAG(has_last_clear_history_message_id);
  STORE_FLAG(is_last_message_deleted_locally);
  STORE_FLAG(has_contact_registered_message);
  STORE_FLAG(has_last_database_message_id);
  STORE_FLAG(need_repair_server_unread_count);
  STORE_FLAG(is_marked_as_unread);
  STORE_FLAG(has_message_notification_group);
  STORE_FLAG(has_mention_notification_group);
  STORE_FLAG(has_new_secret_chat_notification_id);
  STORE_FLAG(has_pinned_message_notification);
  STORE_FLAG(has_last_pinned_message_id);
  STORE_FLAG(is_last_pinned_message_id_inited);
  STORE_FLAG(has_flags2);
  END_STORE_FLAGS();

  store(dialog_id, storer);  // must be stored at offset 4

  if (has_flags2) {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(has_max_notification_message_id);
    STORE_FLAG(has_folder_id);
    STORE_FLAG(is_folder_id_inited);
    STORE_FLAG(has_pending_read_channel_inbox);
    STORE_FLAG(know_action_bar);
    STORE_FLAG(false);  // action_bar->can_add_contact
    STORE_FLAG(false);  // action_bar->can_block_user
    STORE_FLAG(false);  // action_bar->can_share_phone_number
    STORE_FLAG(false);  // action_bar->can_report_location
    STORE_FLAG(has_scheduled_server_messages);
    STORE_FLAG(has_scheduled_database_messages);
    STORE_FLAG(need_repair_channel_server_unread_count);
    STORE_FLAG(false);  // action_bar->can_unarchive
    STORE_FLAG(false);  // action_bar_has_distance
    STORE_FLAG(has_outgoing_messages);
    STORE_FLAG(has_last_yet_unsent_message);
    STORE_FLAG(is_blocked);
    STORE_FLAG(is_is_blocked_inited);
    STORE_FLAG(has_active_group_call);
    STORE_FLAG(is_group_call_empty);
    STORE_FLAG(has_active_group_call_id);
    STORE_FLAG(false);  // action_bar->can_invite_members
    STORE_FLAG(has_message_ttl);
    STORE_FLAG(is_message_ttl_inited);
    STORE_FLAG(has_default_join_group_call_as_dialog_id);
    STORE_FLAG(store_has_bots ? has_bots : false);
    STORE_FLAG(store_has_bots ? is_has_bots_inited : false);
    STORE_FLAG(is_theme_name_inited);
    STORE_FLAG(has_theme_name);
    STORE_FLAG(has_flags3);
    END_STORE_FLAGS();
  }
  if (has_flags3) {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(has_pending_join_requests);
    STORE_FLAG(need_repair_action_bar);
    STORE_FLAG(has_action_bar);
    STORE_FLAG(has_default_send_message_as_dialog_id);
    STORE_FLAG(need_drop_default_send_message_as_dialog_id);
    END_STORE_FLAGS();
  }

  store(last_new_message_id, storer);
  store(server_unread_count, storer);
  if (has_local_unread_count) {
    store(local_unread_count, storer);
  }
  store(last_read_inbox_message_id, storer);
  store(last_read_outbox_message_id, storer);
  store(reply_markup_message_id, storer);
  store(notification_settings, storer);
  if (has_draft_message) {
    store(draft_message, storer);
  }
  store(last_clear_history_date, storer);
  store(order, storer);
  if (has_last_database_message) {
    store(*last_database_message, storer);
  }
  if (has_first_database_message_id) {
    store(first_database_message_id, storer);
  }
  if (has_deleted_last_message) {
    store(delete_last_message_date, storer);
    store(deleted_last_message_id, storer);
  }
  if (has_last_clear_history_message_id) {
    store(last_clear_history_message_id, storer);
  }

  if (has_first_database_message_id_by_index) {
    store(static_cast<int32>(first_database_message_id_by_index.size()), storer);
    for (auto first_message_id : first_database_message_id_by_index) {
      store(first_message_id, storer);
    }
  }
  if (has_message_count_by_index) {
    store(static_cast<int32>(message_count_by_index.size()), storer);
    for (auto message_count : message_count_by_index) {
      store(message_count, storer);
    }
  }
  if (has_client_data) {
    store(client_data, storer);
  }
  if (has_last_read_all_mentions_message_id) {
    store(last_read_all_mentions_message_id, storer);
  }
  if (has_max_unavailable_message_id) {
    store(max_unavailable_message_id, storer);
  }
  if (has_last_database_message_id) {
    store(last_database_message_id, storer);
  }
  if (has_message_notification_group) {
    store(message_notification_group, storer);
  }
  if (has_mention_notification_group) {
    store(mention_notification_group, storer);
  }
  if (has_new_secret_chat_notification_id) {
    store(new_secret_chat_notification_id, storer);
  }
  if (has_pinned_message_notification) {
    store(pinned_message_notification_message_id, storer);
  }
  if (has_last_pinned_message_id) {
    store(last_pinned_message_id, storer);
  }
  if (has_max_notification_message_id) {
    store(max_notification_message_id, storer);
  }
  if (has_folder_id) {
    store(folder_id, storer);
  }
  if (has_pending_read_channel_inbox) {
    store(pending_read_channel_inbox_pts, storer);
    store(pending_read_channel_inbox_max_message_id, storer);
    store(pending_read_channel_inbox_server_unread_count, storer);
  }
  if (has_active_group_call_id) {
    store(active_group_call_id, storer);
  }
  if (has_message_ttl) {
    store(message_ttl, storer);
  }
  if (has_default_join_group_call_as_dialog_id) {
    store(default_join_group_call_as_dialog_id, storer);
  }
  if (has_theme_name) {
    store(theme_name, storer);
  }
  if (has_pending_join_requests) {
    store(pending_join_request_count, storer);
    store(pending_join_request_user_ids, storer);
  }
  if (has_action_bar) {
    store(action_bar, storer);
  }
  if (has_default_send_message_as_dialog_id) {
    store(default_send_message_as_dialog_id, storer);
  }
}

// do not forget to resolve dialog dependencies including dependencies of last_message
template <class ParserT>
void MessagesManager::Dialog::parse(ParserT &parser) {
  using td::parse;
  bool has_draft_message;
  bool has_last_database_message;
  bool legacy_know_can_report_spam;
  bool has_first_database_message_id;
  bool legacy_is_pinned;
  bool has_first_database_message_id_by_index;
  bool has_message_count_by_index;
  bool has_client_data;
  bool has_last_read_all_mentions_message_id;
  bool has_max_unavailable_message_id;
  bool has_local_unread_count;
  bool has_deleted_last_message;
  bool has_last_clear_history_message_id;
  bool has_last_database_message_id;
  bool has_message_notification_group;
  bool has_mention_notification_group;
  bool has_new_secret_chat_notification_id;
  bool has_pinned_message_notification;
  bool has_last_pinned_message_id;
  bool has_flags2;
  bool has_max_notification_message_id = false;
  bool has_folder_id = false;
  bool has_pending_read_channel_inbox = false;
  bool has_active_group_call_id = false;
  bool has_message_ttl = false;
  bool has_default_join_group_call_as_dialog_id = false;
  bool has_theme_name = false;
  bool has_flags3 = false;
  bool has_pending_join_requests = false;
  bool action_bar_can_report_spam = false;
  bool action_bar_can_add_contact = false;
  bool action_bar_can_block_user = false;
  bool action_bar_can_share_phone_number = false;
  bool action_bar_can_report_location = false;
  bool action_bar_can_unarchive = false;
  bool action_bar_has_distance = false;
  bool action_bar_can_invite_members = false;
  bool has_action_bar = false;
  bool has_default_send_message_as_dialog_id = false;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(has_draft_message);
  PARSE_FLAG(has_last_database_message);
  PARSE_FLAG(legacy_know_can_report_spam);
  PARSE_FLAG(action_bar_can_report_spam);
  PARSE_FLAG(has_first_database_message_id);
  PARSE_FLAG(legacy_is_pinned);
  PARSE_FLAG(has_first_database_message_id_by_index);
  PARSE_FLAG(has_message_count_by_index);
  PARSE_FLAG(has_client_data);
  PARSE_FLAG(need_restore_reply_markup);
  PARSE_FLAG(have_full_history);
  PARSE_FLAG(has_last_read_all_mentions_message_id);
  PARSE_FLAG(has_max_unavailable_message_id);
  PARSE_FLAG(is_last_read_inbox_message_id_inited);
  PARSE_FLAG(is_last_read_outbox_message_id_inited);
  PARSE_FLAG(has_local_unread_count);
  PARSE_FLAG(has_deleted_last_message);
  PARSE_FLAG(has_last_clear_history_message_id);
  PARSE_FLAG(is_last_message_deleted_locally);
  PARSE_FLAG(has_contact_registered_message);
  PARSE_FLAG(has_last_database_message_id);
  PARSE_FLAG(need_repair_server_unread_count);
  PARSE_FLAG(is_marked_as_unread);
  PARSE_FLAG(has_message_notification_group);
  PARSE_FLAG(has_mention_notification_group);
  PARSE_FLAG(has_new_secret_chat_notification_id);
  PARSE_FLAG(has_pinned_message_notification);
  PARSE_FLAG(has_last_pinned_message_id);
  PARSE_FLAG(is_last_pinned_message_id_inited);
  PARSE_FLAG(has_flags2);
  END_PARSE_FLAGS();

  parse(dialog_id, parser);  // must be stored at offset 4

  if (has_flags2) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(has_max_notification_message_id);
    PARSE_FLAG(has_folder_id);
    PARSE_FLAG(is_folder_id_inited);
    PARSE_FLAG(has_pending_read_channel_inbox);
    PARSE_FLAG(know_action_bar);
    PARSE_FLAG(action_bar_can_add_contact);
    PARSE_FLAG(action_bar_can_block_user);
    PARSE_FLAG(action_bar_can_share_phone_number);
    PARSE_FLAG(action_bar_can_report_location);
    PARSE_FLAG(has_scheduled_server_messages);
    PARSE_FLAG(has_scheduled_database_messages);
    PARSE_FLAG(need_repair_channel_server_unread_count);
    PARSE_FLAG(action_bar_can_unarchive);
    PARSE_FLAG(action_bar_has_distance);
    PARSE_FLAG(has_outgoing_messages);
    PARSE_FLAG(had_last_yet_unsent_message);
    PARSE_FLAG(is_blocked);
    PARSE_FLAG(is_is_blocked_inited);
    PARSE_FLAG(has_active_group_call);
    PARSE_FLAG(is_group_call_empty);
    PARSE_FLAG(has_active_group_call_id);
    PARSE_FLAG(action_bar_can_invite_members);
    PARSE_FLAG(has_message_ttl);
    PARSE_FLAG(is_message_ttl_inited);
    PARSE_FLAG(has_default_join_group_call_as_dialog_id);
    PARSE_FLAG(has_bots);
    PARSE_FLAG(is_has_bots_inited);
    PARSE_FLAG(is_theme_name_inited);
    PARSE_FLAG(has_theme_name);
    PARSE_FLAG(has_flags3);
    END_PARSE_FLAGS();
  } else {
    is_folder_id_inited = false;
    has_scheduled_server_messages = false;
    has_scheduled_database_messages = false;
    need_repair_channel_server_unread_count = false;
    has_outgoing_messages = false;
    had_last_yet_unsent_message = false;
    is_blocked = false;
    is_is_blocked_inited = false;
    has_active_group_call = false;
    is_group_call_empty = false;
    is_message_ttl_inited = false;
    has_bots = false;
    is_has_bots_inited = false;
    is_theme_name_inited = false;
  }
  if (has_flags3) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(has_pending_join_requests);
    PARSE_FLAG(need_repair_action_bar);
    PARSE_FLAG(has_action_bar);
    PARSE_FLAG(has_default_send_message_as_dialog_id);
    PARSE_FLAG(need_drop_default_send_message_as_dialog_id);
    END_PARSE_FLAGS();
  } else {
    need_repair_action_bar = false;
  }

  parse(last_new_message_id, parser);
  parse(server_unread_count, parser);
  if (has_local_unread_count) {
    parse(local_unread_count, parser);
  }
  parse(last_read_inbox_message_id, parser);
  if (last_read_inbox_message_id.is_valid()) {
    is_last_read_inbox_message_id_inited = true;
  }
  parse(last_read_outbox_message_id, parser);
  if (last_read_outbox_message_id.is_valid()) {
    is_last_read_outbox_message_id_inited = true;
  }
  parse(reply_markup_message_id, parser);
  parse(notification_settings, parser);
  if (has_draft_message) {
    parse(draft_message, parser);
  }
  parse(last_clear_history_date, parser);
  parse(order, parser);
  if (has_last_database_message) {
    parse(messages, parser);
  }
  if (has_first_database_message_id) {
    parse(first_database_message_id, parser);
  }
  if (legacy_is_pinned) {
    int64 legacy_pinned_order;
    parse(legacy_pinned_order, parser);
  }
  if (has_deleted_last_message) {
    parse(delete_last_message_date, parser);
    parse(deleted_last_message_id, parser);
  }
  if (has_last_clear_history_message_id) {
    parse(last_clear_history_message_id, parser);
  }

  if (has_first_database_message_id_by_index) {
    int32 size;
    parse(size, parser);
    if (size < 0) {
      // the log event is broken
      // it should be impossible, but has happenned at least once
      parser.set_error("Wrong first_database_message_id_by_index table size");
      return;
    }
    LOG_CHECK(static_cast<size_t>(size) <= first_database_message_id_by_index.size())
        << size << " " << first_database_message_id_by_index.size();
    for (int32 i = 0; i < size; i++) {
      parse(first_database_message_id_by_index[i], parser);
    }
  }
  if (has_message_count_by_index) {
    int32 size;
    parse(size, parser);
    if (size < 0) {
      // the log event is broken
      // it should be impossible, but has happenned at least once
      parser.set_error("Wrong message_count_by_index table size");
      return;
    }
    LOG_CHECK(static_cast<size_t>(size) <= message_count_by_index.size())
        << size << " " << message_count_by_index.size();
    for (int32 i = 0; i < size; i++) {
      parse(message_count_by_index[i], parser);
    }
  }
  unread_mention_count = message_count_by_index[message_search_filter_index(MessageSearchFilter::UnreadMention)];
  LOG(INFO) << "Set unread mention message count in " << dialog_id << " to " << unread_mention_count;
  if (unread_mention_count < 0) {
    unread_mention_count = 0;
  }
  if (has_client_data) {
    parse(client_data, parser);
  }
  if (has_last_read_all_mentions_message_id) {
    parse(last_read_all_mentions_message_id, parser);
  }
  if (has_max_unavailable_message_id) {
    parse(max_unavailable_message_id, parser);
  }
  if (has_last_database_message_id) {
    parse(last_database_message_id, parser);
  }
  if (has_message_notification_group) {
    parse(message_notification_group, parser);
  }
  if (has_mention_notification_group) {
    parse(mention_notification_group, parser);
  }
  if (has_new_secret_chat_notification_id) {
    parse(new_secret_chat_notification_id, parser);
  }
  if (has_pinned_message_notification) {
    parse(pinned_message_notification_message_id, parser);
  }
  if (has_last_pinned_message_id) {
    parse(last_pinned_message_id, parser);
  }
  if (has_max_notification_message_id) {
    parse(max_notification_message_id, parser);
  }
  if (has_folder_id) {
    parse(folder_id, parser);
  }
  if (has_pending_read_channel_inbox) {
    parse(pending_read_channel_inbox_pts, parser);
    parse(pending_read_channel_inbox_max_message_id, parser);
    parse(pending_read_channel_inbox_server_unread_count, parser);
  }
  int32 action_bar_distance = -1;
  if (action_bar_has_distance) {
    parse(action_bar_distance, parser);
  }
  if (has_active_group_call_id) {
    parse(active_group_call_id, parser);
  }
  if (has_message_ttl) {
    parse(message_ttl, parser);
  }
  if (has_default_join_group_call_as_dialog_id) {
    parse(default_join_group_call_as_dialog_id, parser);
  }
  if (has_theme_name) {
    parse(theme_name, parser);
  }
  if (has_pending_join_requests) {
    parse(pending_join_request_count, parser);
    parse(pending_join_request_user_ids, parser);
  }
  if (has_action_bar) {
    parse(action_bar, parser);
  }
  if (has_default_send_message_as_dialog_id) {
    parse(default_send_message_as_dialog_id, parser);
  }

  (void)legacy_know_can_report_spam;
  if (know_action_bar && !has_action_bar) {
    action_bar = DialogActionBar::create(
        action_bar_can_report_spam, action_bar_can_add_contact, action_bar_can_block_user,
        action_bar_can_share_phone_number, action_bar_can_report_location, action_bar_can_unarchive,
        has_outgoing_messages ? -1 : action_bar_distance, action_bar_can_invite_members, string(), false, 0);
  }
}

template <class StorerT>
void MessagesManager::CallsDbState::store(StorerT &storer) const {
  using td::store;
  store(static_cast<int32>(first_calls_database_message_id_by_index.size()), storer);
  for (auto first_message_id : first_calls_database_message_id_by_index) {
    store(first_message_id, storer);
  }
  store(static_cast<int32>(message_count_by_index.size()), storer);
  for (auto message_count : message_count_by_index) {
    store(message_count, storer);
  }
}

template <class ParserT>
void MessagesManager::CallsDbState::parse(ParserT &parser) {
  using td::parse;
  int32 size;
  parse(size, parser);
  LOG_CHECK(static_cast<size_t>(size) <= first_calls_database_message_id_by_index.size())
      << size << " " << first_calls_database_message_id_by_index.size();
  for (int32 i = 0; i < size; i++) {
    parse(first_calls_database_message_id_by_index[i], parser);
  }
  parse(size, parser);
  LOG_CHECK(static_cast<size_t>(size) <= message_count_by_index.size()) << size << " " << message_count_by_index.size();
  for (int32 i = 0; i < size; i++) {
    parse(message_count_by_index[i], parser);
  }
}

void MessagesManager::load_calls_db_state() {
  if (!G()->parameters().use_message_db) {
    return;
  }
  std::fill(calls_db_state_.message_count_by_index.begin(), calls_db_state_.message_count_by_index.end(), -1);
  auto value = G()->td_db()->get_sqlite_sync_pmc()->get("calls_db_state");
  if (value.empty()) {
    return;
  }
  log_event_parse(calls_db_state_, value).ensure();
  LOG(INFO) << "Save calls database state " << calls_db_state_.first_calls_database_message_id_by_index[0] << " ("
            << calls_db_state_.message_count_by_index[0] << ") "
            << calls_db_state_.first_calls_database_message_id_by_index[1] << " ("
            << calls_db_state_.message_count_by_index[1] << ")";
}

void MessagesManager::save_calls_db_state() {
  if (!G()->parameters().use_message_db) {
    return;
  }

  LOG(INFO) << "Save calls database state " << calls_db_state_.first_calls_database_message_id_by_index[0] << " ("
            << calls_db_state_.message_count_by_index[0] << ") "
            << calls_db_state_.first_calls_database_message_id_by_index[1] << " ("
            << calls_db_state_.message_count_by_index[1] << ")";
  G()->td_db()->get_sqlite_pmc()->set("calls_db_state", log_event_store(calls_db_state_).as_slice().str(), Auto());
}

BufferSlice MessagesManager::get_dialog_database_value(const Dialog *d) {
  // can't use log_event_store, because it tries to parse stored Dialog
  LogEventStorerCalcLength storer_calc_length;
  store(*d, storer_calc_length);

  BufferSlice value_buffer{storer_calc_length.get_length()};
  auto value = value_buffer.as_slice();

  LogEventStorerUnsafe storer_unsafe(value.ubegin());
  store(*d, storer_unsafe);
  return value_buffer;
}

void MessagesManager::save_scope_notification_settings(NotificationSettingsScope scope,
                                                       const ScopeNotificationSettings &new_settings) {
  string key = get_notification_settings_scope_database_key(scope);
  G()->td_db()->get_binlog_pmc()->set(key, log_event_store(new_settings).as_slice().str());
}

class MessagesManager::ToggleDialogReportSpamStateOnServerLogEvent {
 public:
  DialogId dialog_id_;
  bool is_spam_dialog_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(is_spam_dialog_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(is_spam_dialog_, parser);
  }
};

uint64 MessagesManager::save_toggle_dialog_report_spam_state_on_server_log_event(DialogId dialog_id,
                                                                                 bool is_spam_dialog) {
  ToggleDialogReportSpamStateOnServerLogEvent log_event{dialog_id, is_spam_dialog};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ToggleDialogReportSpamStateOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DeleteMessagesOnServerLogEvent {
 public:
  DialogId dialog_id_;
  vector<MessageId> message_ids_;
  bool revoke_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(revoke_);
    END_STORE_FLAGS();

    td::store(dialog_id_, storer);
    td::store(message_ids_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(revoke_);
    END_PARSE_FLAGS();

    td::parse(dialog_id_, parser);
    td::parse(message_ids_, parser);
  }
};

uint64 MessagesManager::save_delete_messages_on_server_log_event(DialogId dialog_id,
                                                                 const vector<MessageId> &message_ids, bool revoke) {
  DeleteMessagesOnServerLogEvent log_event{dialog_id, message_ids, revoke};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteMessagesOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DeleteScheduledMessagesOnServerLogEvent {
 public:
  DialogId dialog_id_;
  vector<MessageId> message_ids_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(message_ids_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(message_ids_, parser);
  }
};

uint64 MessagesManager::save_delete_scheduled_messages_on_server_log_event(DialogId dialog_id,
                                                                           const vector<MessageId> &message_ids) {
  DeleteScheduledMessagesOnServerLogEvent log_event{dialog_id, message_ids};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteScheduledMessagesOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DeleteDialogHistoryOnServerLogEvent {
 public:
  DialogId dialog_id_;
  MessageId max_message_id_;
  bool remove_from_dialog_list_;
  bool revoke_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(remove_from_dialog_list_);
    STORE_FLAG(revoke_);
    END_STORE_FLAGS();

    td::store(dialog_id_, storer);
    td::store(max_message_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(remove_from_dialog_list_);
    PARSE_FLAG(revoke_);
    END_PARSE_FLAGS();

    td::parse(dialog_id_, parser);
    td::parse(max_message_id_, parser);
  }
};

uint64 MessagesManager::save_delete_dialog_history_on_server_log_event(DialogId dialog_id, MessageId max_message_id,
                                                                       bool remove_from_dialog_list, bool revoke) {
  DeleteDialogHistoryOnServerLogEvent log_event{dialog_id, max_message_id, remove_from_dialog_list, revoke};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteDialogHistoryOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DeleteAllCallMessagesOnServerLogEvent {
 public:
  bool revoke_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(revoke_);
    END_STORE_FLAGS();
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(revoke_);
    END_PARSE_FLAGS();
  }
};

uint64 MessagesManager::save_delete_all_call_messages_on_server_log_event(bool revoke) {
  DeleteAllCallMessagesOnServerLogEvent log_event{revoke};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteAllCallMessagesOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DeleteAllChannelMessagesFromSenderOnServerLogEvent {
 public:
  ChannelId channel_id_;
  DialogId sender_dialog_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(channel_id_, storer);
    td::store(sender_dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(channel_id_, parser);
    if (parser.version() >= static_cast<int32>(Version::AddKeyboardButtonFlags)) {
      td::parse(sender_dialog_id_, parser);
    } else {
      UserId user_id;
      td::parse(user_id, parser);
      sender_dialog_id_ = DialogId(user_id);
    }
  }
};

uint64 MessagesManager::save_delete_all_channel_messages_by_sender_on_server_log_event(ChannelId channel_id,
                                                                                       DialogId sender_dialog_id) {
  DeleteAllChannelMessagesFromSenderOnServerLogEvent log_event{channel_id, sender_dialog_id};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteAllChannelMessagesFromSenderOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DeleteDialogMessagesByDateOnServerLogEvent {
 public:
  DialogId dialog_id_;
  int32 min_date_;
  int32 max_date_;
  bool revoke_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(revoke_);
    END_STORE_FLAGS();
    td::store(dialog_id_, storer);
    td::store(min_date_, storer);
    td::store(max_date_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(revoke_);
    END_PARSE_FLAGS();
    td::parse(dialog_id_, parser);
    td::parse(min_date_, parser);
    td::parse(max_date_, parser);
  }
};

uint64 MessagesManager::save_delete_dialog_messages_by_date_on_server_log_event(DialogId dialog_id, int32 min_date,
                                                                                int32 max_date, bool revoke) {
  DeleteDialogMessagesByDateOnServerLogEvent log_event{dialog_id, min_date, max_date, revoke};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteDialogMessagesByDateOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::ReadAllDialogMentionsOnServerLogEvent {
 public:
  DialogId dialog_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
  }
};

uint64 MessagesManager::save_read_all_dialog_mentions_on_server_log_event(DialogId dialog_id) {
  ReadAllDialogMentionsOnServerLogEvent log_event{dialog_id};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ReadAllDialogMentionsOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::DialogFiltersLogEvent {
 public:
  int32 updated_date = 0;
  const vector<unique_ptr<DialogFilter>> *server_dialog_filters_in;
  const vector<unique_ptr<DialogFilter>> *dialog_filters_in;
  vector<unique_ptr<DialogFilter>> server_dialog_filters_out;
  vector<unique_ptr<DialogFilter>> dialog_filters_out;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(updated_date, storer);
    td::store(*server_dialog_filters_in, storer);
    td::store(*dialog_filters_in, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(updated_date, parser);
    td::parse(server_dialog_filters_out, parser);
    td::parse(dialog_filters_out, parser);
  }
};

void MessagesManager::init() {
  if (is_inited_) {
    return;
  }
  is_inited_ = true;

  always_wait_for_mailbox();

  start_time_ = Time::now();
  last_channel_pts_jump_warning_time_ = start_time_ - 3600;

  bool is_authorized = td_->auth_manager_->is_authorized();
  bool was_authorized_user = td_->auth_manager_->was_authorized() && !td_->auth_manager_->is_bot();
  if (was_authorized_user) {
    create_folders();  // ensure that Main and Archive dialog lists are created
  }
  if (is_authorized && td_->auth_manager_->is_bot()) {
    disable_get_dialog_filter_ = true;
  }
  authorization_date_ = G()->shared_config().get_option_integer("authorization_date");

  if (was_authorized_user) {
    vector<NotificationSettingsScope> scopes{NotificationSettingsScope::Private, NotificationSettingsScope::Group,
                                             NotificationSettingsScope::Channel};
    for (auto scope : scopes) {
      auto notification_settings_string =
          G()->td_db()->get_binlog_pmc()->get(get_notification_settings_scope_database_key(scope));
      if (!notification_settings_string.empty()) {
        auto current_settings = get_scope_notification_settings(scope);
        CHECK(current_settings != nullptr);
        log_event_parse(*current_settings, notification_settings_string).ensure();

        VLOG(notifications) << "Loaded notification settings in " << scope << ": " << *current_settings;

        schedule_scope_unmute(scope, current_settings->mute_until);

        send_closure(G()->td(), &Td::send_update, get_update_scope_notification_settings_object(scope));
      }
    }
    if (!channels_notification_settings_.is_synchronized && is_authorized) {
      channels_notification_settings_ = chats_notification_settings_;
      channels_notification_settings_.disable_pinned_message_notifications = false;
      channels_notification_settings_.disable_mention_notifications = false;
      channels_notification_settings_.is_synchronized = false;
      send_get_scope_notification_settings_query(NotificationSettingsScope::Channel, Promise<>());
    }
  }
  G()->td_db()->get_binlog_pmc()->erase("nsfac");

  if (was_authorized_user) {
    auto dialog_filters = G()->td_db()->get_binlog_pmc()->get("dialog_filters");
    if (!dialog_filters.empty()) {
      DialogFiltersLogEvent log_event;
      if (log_event_parse(log_event, dialog_filters).is_ok()) {
        dialog_filters_updated_date_ = G()->ignore_background_updates() ? 0 : log_event.updated_date;
        std::unordered_set<DialogFilterId, DialogFilterIdHash> server_dialog_filter_ids;
        for (auto &dialog_filter : log_event.server_dialog_filters_out) {
          if (server_dialog_filter_ids.insert(dialog_filter->dialog_filter_id).second) {
            server_dialog_filters_.push_back(std::move(dialog_filter));
          }
        }
        for (auto &dialog_filter : log_event.dialog_filters_out) {
          add_dialog_filter(std::move(dialog_filter), false, "binlog");
        }
        LOG(INFO) << "Loaded server chat filters " << get_dialog_filter_ids(server_dialog_filters_)
                  << " and local chat filters " << get_dialog_filter_ids(dialog_filters_);
      } else {
        LOG(ERROR) << "Failed to parse chat filters from binlog";
      }
    }
    send_update_chat_filters();  // always send updateChatFilters
  }

  if (G()->parameters().use_message_db && was_authorized_user) {
    // erase old keys
    G()->td_db()->get_binlog_pmc()->erase("last_server_dialog_date");
    G()->td_db()->get_binlog_pmc()->erase("unread_message_count");
    G()->td_db()->get_binlog_pmc()->erase("unread_dialog_count");

    auto last_database_server_dialog_dates = G()->td_db()->get_binlog_pmc()->prefix_get("last_server_dialog_date");
    for (auto &it : last_database_server_dialog_dates) {
      auto r_folder_id = to_integer_safe<int32>(it.first);
      if (r_folder_id.is_error()) {
        LOG(ERROR) << "Can't parse folder ID from " << it.first;
        continue;
      }

      string order_str;
      string dialog_id_str;
      std::tie(order_str, dialog_id_str) = split(it.second);

      auto r_order = to_integer_safe<int64>(order_str);
      auto r_dialog_id = to_integer_safe<int64>(dialog_id_str);
      if (r_order.is_error() || r_dialog_id.is_error()) {
        LOG(ERROR) << "Can't parse " << it.second;
      } else {
        FolderId folder_id(r_folder_id.ok());
        auto *folder = get_dialog_folder(folder_id);
        CHECK(folder != nullptr);
        DialogDate dialog_date(r_order.ok(), DialogId(r_dialog_id.ok()));
        if (dialog_date.get_date() == 0 && dialog_date != MAX_DIALOG_DATE) {
          LOG(ERROR) << "Ignore incorrect last database server dialog date " << dialog_date << " in " << folder_id;
        } else {
          if (folder->last_database_server_dialog_date_ < dialog_date) {
            folder->last_database_server_dialog_date_ = dialog_date;
          }
          LOG(INFO) << "Loaded last_database_server_dialog_date_ " << folder->last_database_server_dialog_date_
                    << " in " << folder_id;
        }
      }
    }

    auto sponsored_dialog_id_string = G()->td_db()->get_binlog_pmc()->get("sponsored_dialog_id");
    if (!sponsored_dialog_id_string.empty()) {
      auto dialog_id_source = split(Slice(sponsored_dialog_id_string));
      auto r_dialog_id = to_integer_safe<int64>(dialog_id_source.first);
      auto r_source = DialogSource::unserialize(dialog_id_source.second);
      if (r_dialog_id.is_error() || r_source.is_error()) {
        LOG(ERROR) << "Can't parse " << sponsored_dialog_id_string;
      } else {
        DialogId dialog_id(r_dialog_id.ok());

        const Dialog *d = get_dialog_force(dialog_id, "init");
        if (d != nullptr) {
          LOG(INFO) << "Loaded sponsored " << dialog_id;
          add_sponsored_dialog(d, r_source.move_as_ok());
        } else {
          LOG(ERROR) << "Can't load " << dialog_id;
        }
      }
    }

    auto pinned_dialog_ids = G()->td_db()->get_binlog_pmc()->prefix_get("pinned_dialog_ids");
    for (auto &it : pinned_dialog_ids) {
      auto r_folder_id = to_integer_safe<int32>(it.first);
      if (r_folder_id.is_error()) {
        LOG(ERROR) << "Can't parse folder ID from " << it.first;
        continue;
      }
      FolderId folder_id(r_folder_id.ok());

      auto r_dialog_ids = transform(full_split(Slice(it.second), ','), [](Slice str) -> Result<DialogId> {
        TRY_RESULT(dialog_id_int, to_integer_safe<int64>(str));
        DialogId dialog_id(dialog_id_int);
        if (!dialog_id.is_valid()) {
          return Status::Error("Have invalid dialog ID");
        }
        return dialog_id;
      });
      if (std::any_of(r_dialog_ids.begin(), r_dialog_ids.end(),
                      [](auto &r_dialog_id) { return r_dialog_id.is_error(); })) {
        LOG(ERROR) << "Can't parse " << it.second;
        reload_pinned_dialogs(DialogListId(folder_id), Auto());
      } else {
        auto *list = get_dialog_list(DialogListId(folder_id));
        CHECK(list != nullptr);
        CHECK(list->pinned_dialogs_.empty());
        for (auto &r_dialog_id : reversed(r_dialog_ids)) {
          auto dialog_id = r_dialog_id.move_as_ok();
          auto order = get_next_pinned_dialog_order();
          list->pinned_dialogs_.emplace_back(order, dialog_id);
          list->pinned_dialog_id_orders_.emplace(dialog_id, order);
        }
        std::reverse(list->pinned_dialogs_.begin(), list->pinned_dialogs_.end());
        list->are_pinned_dialogs_inited_ = true;
        update_list_last_pinned_dialog_date(*list);

        LOG(INFO) << "Loaded pinned chats " << list->pinned_dialogs_ << " in " << folder_id;
      }
    }

    auto unread_message_counts = G()->td_db()->get_binlog_pmc()->prefix_get("unread_message_count");
    for (auto &it : unread_message_counts) {
      auto r_dialog_list_id = to_integer_safe<int64>(it.first);
      if (r_dialog_list_id.is_error()) {
        LOG(ERROR) << "Can't parse dialog list ID from " << it.first;
        continue;
      }
      string total_count;
      string muted_count;
      std::tie(total_count, muted_count) = split(it.second);

      auto r_total_count = to_integer_safe<int32>(total_count);
      auto r_muted_count = to_integer_safe<int32>(muted_count);
      if (r_total_count.is_error() || r_muted_count.is_error()) {
        LOG(ERROR) << "Can't parse " << it.second;
      } else {
        DialogListId dialog_list_id(r_dialog_list_id.ok());
        auto *list = get_dialog_list(dialog_list_id);
        if (list != nullptr) {
          list->unread_message_total_count_ = r_total_count.ok();
          list->unread_message_muted_count_ = r_muted_count.ok();
          list->is_message_unread_count_inited_ = true;
          send_update_unread_message_count(*list, DialogId(), true, "load unread_message_count", true);
        } else {
          G()->td_db()->get_binlog_pmc()->erase("unread_message_count" + it.first);
        }
      }
    }

    auto unread_dialog_counts = G()->td_db()->get_binlog_pmc()->prefix_get("unread_dialog_count");
    for (auto &it : unread_dialog_counts) {
      auto r_dialog_list_id = to_integer_safe<int64>(it.first);
      if (r_dialog_list_id.is_error()) {
        LOG(ERROR) << "Can't parse dialog list ID from " << it.first;
        continue;
      }

      auto counts = transform(full_split(Slice(it.second)), [](Slice str) { return to_integer_safe<int32>(str); });
      if ((counts.size() != 4 && counts.size() != 6) ||
          std::any_of(counts.begin(), counts.end(), [](auto &c) { return c.is_error(); })) {
        LOG(ERROR) << "Can't parse " << it.second;
      } else {
        DialogListId dialog_list_id(r_dialog_list_id.ok());
        auto *list = get_dialog_list(dialog_list_id);
        if (list != nullptr) {
          list->unread_dialog_total_count_ = counts[0].ok();
          list->unread_dialog_muted_count_ = counts[1].ok();
          list->unread_dialog_marked_count_ = counts[2].ok();
          list->unread_dialog_muted_marked_count_ = counts[3].ok();
          if (counts.size() == 6) {
            list->server_dialog_total_count_ = counts[4].ok();
            list->secret_chat_total_count_ = counts[5].ok();
          }
          if (list->server_dialog_total_count_ == -1) {
            repair_server_dialog_total_count(dialog_list_id);
          }
          if (list->secret_chat_total_count_ == -1) {
            repair_secret_chat_total_count(dialog_list_id);
          }
          list->is_dialog_unread_count_inited_ = true;
          send_update_unread_chat_count(*list, DialogId(), true, "load unread_dialog_count", true);
        } else {
          G()->td_db()->get_binlog_pmc()->erase("unread_dialog_count" + it.first);
        }
      }
    }
  } else {
    G()->td_db()->get_binlog_pmc()->erase_by_prefix("pinned_dialog_ids");
    G()->td_db()->get_binlog_pmc()->erase_by_prefix("last_server_dialog_date");
    G()->td_db()->get_binlog_pmc()->erase_by_prefix("unread_message_count");
    G()->td_db()->get_binlog_pmc()->erase_by_prefix("unread_dialog_count");
    G()->td_db()->get_binlog_pmc()->erase("sponsored_dialog_id");
  }
  G()->td_db()->get_binlog_pmc()->erase("dialog_pinned_current_order");

  if (G()->parameters().use_message_db) {
    ttl_db_loop_start(G()->server_time());
  }

  load_calls_db_state();

  if (was_authorized_user && is_authorized) {
    if (need_synchronize_dialog_filters()) {
      reload_dialog_filters();
    } else {
      auto cache_time = get_dialog_filters_cache_time();
      schedule_dialog_filters_reload(cache_time - max(0, G()->unix_time() - dialog_filters_updated_date_));
    }
  }

  auto auth_notification_ids_string = G()->td_db()->get_binlog_pmc()->get("auth_notification_ids");
  if (!auth_notification_ids_string.empty()) {
    VLOG(notifications) << "Loaded auth_notification_ids = " << auth_notification_ids_string;
    auto ids = full_split(auth_notification_ids_string, ',');
    CHECK(ids.size() % 2 == 0);
    bool is_changed = false;
    auto min_date = G()->unix_time() - AUTH_NOTIFICATION_ID_CACHE_TIME;
    for (size_t i = 0; i < ids.size(); i += 2) {
      auto date = to_integer_safe<int32>(ids[i + 1]).ok();
      if (date < min_date) {
        is_changed = true;
        continue;
      }
      auth_notification_id_date_.emplace(std::move(ids[i]), date);
    }
    if (is_changed) {
      save_auth_notification_ids();
    }
  }

  /*
  FI LE *f = std::f open("error.txt", "r");
  if (f != nullptr) {
    DialogId dialog_id(ChannelId(123456));
    force_create_dialog(dialog_id, "test");
    Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);

    delete_all_dialog_messages(d, true, false);

    d->last_new_message_id = MessageId();
    d->last_read_inbox_message_id = MessageId();
    d->last_read_outbox_message_id = MessageId();
    d->is_last_read_inbox_message_id_inited = false;
    d->is_last_read_outbox_message_id_inited = false;

    struct MessageBasicInfo {
      MessageId message_id;
      bool have_previous;
      bool have_next;
    };
    vector<MessageBasicInfo> messages_info;
    std::function<void(Message *m)> get_messages_info = [&](Message *m) {
      if (m == nullptr) {
        return;
      }
      get_messages_info(m->left.get());
      messages_info.push_back(MessageBasicInfo{m->message_id, m->have_previous, m->have_next});
      get_messages_info(m->right.get());
    };

    char buf[1280];
    while (std::f gets(buf, sizeof(buf), f) != nullptr) {
      Slice log_string(buf, std::strlen(buf));
      Slice op = log_string.substr(0, log_string.find(' '));
      if (op != "MessageOpAdd" && op != "MessageOpDelete") {
        LOG(ERROR) << "Unsupported op " << op;
        continue;
      }
      log_string.remove_prefix(log_string.find(' ') + 1);

      if (!begins_with(log_string, "at ")) {
        LOG(ERROR) << "Date expected, found " << log_string;
        continue;
      }
      log_string.remove_prefix(3);
      auto date_slice = log_string.substr(0, log_string.find(' '));
      log_string.remove_prefix(date_slice.size());

      bool is_server = false;
      if (begins_with(log_string, " server message ")) {
        log_string.remove_prefix(16);
        is_server = true;
      } else if (begins_with(log_string, " yet unsent message ")) {
        log_string.remove_prefix(20);
      } else if (begins_with(log_string, " local message ")) {
        log_string.remove_prefix(15);
      } else {
        LOG(ERROR) << "Message identifier expected, found " << log_string;
        continue;
      }

      auto server_message_id = to_integer<int32>(log_string);
      auto add = 0;
      if (!is_server) {
        log_string.remove_prefix(log_string.find('.') + 1);
        add = to_integer<int32>(log_string);
      }
      log_string.remove_prefix(log_string.find(' ') + 1);

      auto message_id = MessageId(MessageId(ServerMessageId(server_message_id)).get() + add);

      auto content_type = log_string.substr(0, log_string.find(' '));
      log_string.remove_prefix(log_string.find(' ') + 1);

      auto read_bool = [](Slice &str) {
        if (begins_with(str, "true ")) {
          str.remove_prefix(5);
          return true;
        }
        if (begins_with(str, "false ")) {
          str.remove_prefix(6);
          return false;
        }
        LOG(ERROR) << "Bool expected, found " << str;
        return false;
      };

      bool from_update = read_bool(log_string);
      bool have_previous = read_bool(log_string);
      bool have_next = read_bool(log_string);

      if (op == "MessageOpAdd") {
        auto m = make_unique<Message>();
        set_message_id(m, message_id);
        m->date = G()->unix_time();
        m->content = create_text_message_content("text", {}, {});

        m->have_previous = have_previous;
        m->have_next = have_next;

        bool need_update = from_update;
        bool need_update_dialog_pos = false;
        if (add_message_to_dialog(dialog_id, std::move(m), from_update, &need_update, &need_update_dialog_pos,
                                  "Unknown source") == nullptr) {
          LOG(ERROR) << "Can't add message " << message_id;
        }
      } else {
        bool need_update_dialog_pos = false;
        auto m = delete_message(d, message_id, true, &need_update_dialog_pos, "Unknown source");
        CHECK(m != nullptr);
      }

      messages_info.clear();
      get_messages_info(d->messages.get());

      for (size_t i = 0; i + 1 < messages_info.size(); i++) {
        if (messages_info[i].have_next != messages_info[i + 1].have_previous) {
          LOG(ERROR) << messages_info[i].message_id << " has have_next = " << messages_info[i].have_next << ", but "
                     << messages_info[i + 1].message_id
                     << " has have_previous = " << messages_info[i + 1].have_previous;
        }
      }
      if (!messages_info.empty()) {
        if (messages_info.back().have_next != false) {
          LOG(ERROR) << messages_info.back().message_id << " has have_next = true, but there is no next message";
        }
        if (messages_info[0].have_previous != false) {
          LOG(ERROR) << messages_info[0].message_id << " has have_previous = true, but there is no previous message";
        }
      }
    }

    messages_info.clear();
    get_messages_info(d->messages.get());
    for (auto &info : messages_info) {
      bool need_update_dialog_pos = false;
      auto m = delete_message(d, info.message_id, true, &need_update_dialog_pos, "Unknown source");
      CHECK(m != nullptr);
    }

    std::f close(f);
  }
  */
}

class MessagesManager::BlockMessageSenderFromRepliesOnServerLogEvent {
 public:
  MessageId message_id_;
  bool delete_message_;
  bool delete_all_messages_;
  bool report_spam_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(delete_message_);
    STORE_FLAG(delete_all_messages_);
    STORE_FLAG(report_spam_);
    END_STORE_FLAGS();

    td::store(message_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(delete_message_);
    PARSE_FLAG(delete_all_messages_);
    PARSE_FLAG(report_spam_);
    END_PARSE_FLAGS();

    td::parse(message_id_, parser);
  }
};

uint64 MessagesManager::save_block_message_sender_from_replies_on_server_log_event(MessageId message_id,
                                                                                   bool need_delete_message,
                                                                                   bool need_delete_all_messages,
                                                                                   bool report_spam) {
  BlockMessageSenderFromRepliesOnServerLogEvent log_event{message_id, need_delete_message, need_delete_all_messages,
                                                          report_spam};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::BlockMessageSenderFromRepliesOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::SaveDialogDraftMessageOnServerLogEvent {
 public:
  DialogId dialog_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
  }
};

Status MessagesManager::set_dialog_draft_message(DialogId dialog_id, MessageId top_thread_message_id,
                                                 tl_object_ptr<td_api::draftMessage> &&draft_message) {
  if (td_->auth_manager_->is_bot()) {
    return Status::Error(400, "Bots can't change chat draft message");
  }

  Dialog *d = get_dialog_force(dialog_id, "set_dialog_draft_message");
  if (d == nullptr) {
    return Status::Error(400, "Chat not found");
  }
  TRY_STATUS(can_send_message(dialog_id));

  TRY_RESULT(new_draft_message, get_draft_message(td_->contacts_manager_.get(), dialog_id, std::move(draft_message)));
  if (new_draft_message != nullptr) {
    new_draft_message->reply_to_message_id =
        get_reply_to_message_id(d, top_thread_message_id, new_draft_message->reply_to_message_id, true);

    if (!new_draft_message->reply_to_message_id.is_valid() && new_draft_message->input_message_text.text.text.empty()) {
      new_draft_message = nullptr;
    }
  }

  if (top_thread_message_id != MessageId()) {
    if (!top_thread_message_id.is_valid()) {
      return Status::Error(400, "Invalid message thread specified");
    }

    auto m = get_message_force(d, top_thread_message_id, "set_dialog_draft_message");
    if (m == nullptr || m->message_id.is_scheduled() || m->reply_info.is_comment ||
        !is_active_message_reply_info(dialog_id, m->reply_info)) {
      return Status::OK();
    }

    auto &old_draft_message = m->thread_draft_message;
    if (((new_draft_message == nullptr) != (old_draft_message == nullptr)) ||
        (new_draft_message != nullptr &&
         (old_draft_message->reply_to_message_id != new_draft_message->reply_to_message_id ||
          old_draft_message->input_message_text != new_draft_message->input_message_text))) {
      old_draft_message = std::move(new_draft_message);
      on_message_changed(d, m, false, "set_dialog_draft_message");
    }
    return Status::OK();
  }

  if (update_dialog_draft_message(d, std::move(new_draft_message), false, true)) {
    if (dialog_id.get_type() != DialogType::SecretChat) {
      if (G()->parameters().use_message_db) {
        SaveDialogDraftMessageOnServerLogEvent log_event;
        log_event.dialog_id_ = dialog_id;
        add_log_event(d->save_draft_message_log_event_id, get_log_event_storer(log_event),
                      LogEvent::HandlerType::SaveDialogDraftMessageOnServer, "draft");
      }

      pending_draft_message_timeout_.set_timeout_in(dialog_id.get(), d->is_opened ? MIN_SAVE_DRAFT_DELAY : 0);
    }
  }
  return Status::OK();
}

class MessagesManager::ToggleDialogIsPinnedOnServerLogEvent {
 public:
  DialogId dialog_id_;
  bool is_pinned_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(is_pinned_);
    END_STORE_FLAGS();

    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(is_pinned_);
    END_PARSE_FLAGS();

    td::parse(dialog_id_, parser);
  }
};

uint64 MessagesManager::save_toggle_dialog_is_pinned_on_server_log_event(DialogId dialog_id, bool is_pinned) {
  ToggleDialogIsPinnedOnServerLogEvent log_event{dialog_id, is_pinned};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ToggleDialogIsPinnedOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::ReorderPinnedDialogsOnServerLogEvent {
 public:
  FolderId folder_id_;
  vector<DialogId> dialog_ids_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(folder_id_, storer);
    td::store(dialog_ids_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    if (parser.version() >= static_cast<int32>(Version::AddFolders)) {
      td::parse(folder_id_, parser);
    } else {
      folder_id_ = FolderId();
    }
    td::parse(dialog_ids_, parser);
  }
};

uint64 MessagesManager::save_reorder_pinned_dialogs_on_server_log_event(FolderId folder_id,
                                                                        const vector<DialogId> &dialog_ids) {
  ReorderPinnedDialogsOnServerLogEvent log_event{folder_id, dialog_ids};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ReorderPinnedDialogsOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::ToggleDialogIsMarkedAsUnreadOnServerLogEvent {
 public:
  DialogId dialog_id_;
  bool is_marked_as_unread_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(is_marked_as_unread_);
    END_STORE_FLAGS();

    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(is_marked_as_unread_);
    END_PARSE_FLAGS();

    td::parse(dialog_id_, parser);
  }
};

uint64 MessagesManager::save_toggle_dialog_is_marked_as_unread_on_server_log_event(DialogId dialog_id,
                                                                                   bool is_marked_as_unread) {
  ToggleDialogIsMarkedAsUnreadOnServerLogEvent log_event{dialog_id, is_marked_as_unread};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ToggleDialogIsMarkedAsUnreadOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::ToggleDialogIsBlockedOnServerLogEvent {
 public:
  DialogId dialog_id_;
  bool is_blocked_;

  template <class StorerT>
  void store(StorerT &storer) const {
    BEGIN_STORE_FLAGS();
    STORE_FLAG(is_blocked_);
    END_STORE_FLAGS();

    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(is_blocked_);
    END_PARSE_FLAGS();

    td::parse(dialog_id_, parser);
  }
};

uint64 MessagesManager::save_toggle_dialog_is_blocked_on_server_log_event(DialogId dialog_id, bool is_blocked) {
  ToggleDialogIsBlockedOnServerLogEvent log_event{dialog_id, is_blocked};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ToggleDialogIsBlockedOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::UpdateDialogNotificationSettingsOnServerLogEvent {
 public:
  DialogId dialog_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
  }
};

void MessagesManager::update_dialog_notification_settings_on_server(DialogId dialog_id, bool from_binlog) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  if (!from_binlog && get_input_notify_peer(dialog_id) == nullptr) {
    // don't even create new binlog events
    return;
  }

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  if (!from_binlog && G()->parameters().use_message_db) {
    UpdateDialogNotificationSettingsOnServerLogEvent log_event;
    log_event.dialog_id_ = dialog_id;
    add_log_event(d->save_notification_settings_log_event_id, get_log_event_storer(log_event),
                  LogEvent::HandlerType::UpdateDialogNotificationSettingsOnServer, "notification settings");
  }

  Promise<> promise;
  if (d->save_notification_settings_log_event_id.log_event_id != 0) {
    d->save_notification_settings_log_event_id.generation++;
    promise = PromiseCreator::lambda(
        [actor_id = actor_id(this), dialog_id,
         generation = d->save_notification_settings_log_event_id.generation](Result<Unit> result) {
          if (!G()->close_flag()) {
            send_closure(actor_id, &MessagesManager::on_updated_dialog_notification_settings, dialog_id, generation);
          }
        });
  }

  send_update_dialog_notification_settings_query(d, std::move(promise));
}

class MessagesManager::ReadMessageContentsOnServerLogEvent {
 public:
  DialogId dialog_id_;
  vector<MessageId> message_ids_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(message_ids_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(message_ids_, parser);
  }
};

uint64 MessagesManager::save_read_message_contents_on_server_log_event(DialogId dialog_id,
                                                                       const vector<MessageId> &message_ids) {
  ReadMessageContentsOnServerLogEvent log_event{dialog_id, message_ids};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ReadMessageContentsOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::UpdateScopeNotificationSettingsOnServerLogEvent {
 public:
  NotificationSettingsScope scope_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(scope_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(scope_, parser);
  }
};

uint64 MessagesManager::save_update_scope_notification_settings_on_server_log_event(NotificationSettingsScope scope) {
  UpdateScopeNotificationSettingsOnServerLogEvent log_event{scope};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::UpdateScopeNotificationSettingsOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::ResetAllNotificationSettingsOnServerLogEvent {
 public:
  template <class StorerT>
  void store(StorerT &storer) const {
  }

  template <class ParserT>
  void parse(ParserT &parser) {
  }
};

uint64 MessagesManager::save_reset_all_notification_settings_on_server_log_event() {
  ResetAllNotificationSettingsOnServerLogEvent log_event;
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ResetAllNotificationSettingsOnServer,
                    get_log_event_storer(log_event));
}

class MessagesManager::ReadHistoryOnServerLogEvent {
 public:
  DialogId dialog_id_;
  MessageId max_message_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(max_message_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(max_message_id_, parser);
  }
};

class MessagesManager::ReadHistoryInSecretChatLogEvent {
 public:
  DialogId dialog_id_;
  int32 max_date_ = 0;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(max_date_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(max_date_, parser);
  }
};

class MessagesManager::ReadMessageThreadHistoryOnServerLogEvent {
 public:
  DialogId dialog_id_;
  MessageId top_thread_message_id_;
  MessageId max_message_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(top_thread_message_id_, storer);
    td::store(max_message_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(top_thread_message_id_, parser);
    td::parse(max_message_id_, parser);
  }
};

void MessagesManager::read_history_on_server(Dialog *d, MessageId max_message_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  CHECK(!max_message_id.is_scheduled());

  auto dialog_id = d->dialog_id;
  LOG(INFO) << "Read history in " << dialog_id << " on server up to " << max_message_id;

  bool is_secret = dialog_id.get_type() == DialogType::SecretChat;
  if (is_secret) {
    auto *m = get_message_force(d, max_message_id, "read_history_on_server");
    if (m == nullptr) {
      LOG(ERROR) << "Failed to read history in " << dialog_id << " up to " << max_message_id;
      return;
    }

    ReadHistoryInSecretChatLogEvent log_event;
    log_event.dialog_id_ = dialog_id;
    log_event.max_date_ = m->date;
    add_log_event(d->read_history_log_event_ids[0], get_log_event_storer(log_event),
                  LogEvent::HandlerType::ReadHistoryInSecretChat, "read history");

    d->last_read_inbox_message_date = m->date;
  } else if (G()->parameters().use_message_db) {
    ReadHistoryOnServerLogEvent log_event;
    log_event.dialog_id_ = dialog_id;
    log_event.max_message_id_ = max_message_id;
    add_log_event(d->read_history_log_event_ids[0], get_log_event_storer(log_event),
                  LogEvent::HandlerType::ReadHistoryOnServer, "read history");
  }

  d->updated_read_history_message_ids.insert(MessageId());

  bool need_delay = d->is_opened && !is_secret &&
                    (d->server_unread_count > 0 || (!need_unread_counter(d->order) && d->last_message_id.is_valid() &&
                                                    max_message_id < d->last_message_id));
  pending_read_history_timeout_.set_timeout_in(dialog_id.get(), need_delay ? MIN_READ_HISTORY_DELAY : 0);
}

void MessagesManager::read_message_thread_history_on_server(Dialog *d, MessageId top_thread_message_id,
                                                            MessageId max_message_id, MessageId last_message_id) {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  CHECK(d != nullptr);
  CHECK(top_thread_message_id.is_valid());
  CHECK(top_thread_message_id.is_server());
  CHECK(max_message_id.is_server());

  auto dialog_id = d->dialog_id;
  LOG(INFO) << "Read history in thread of " << top_thread_message_id << " in " << dialog_id << " on server up to "
            << max_message_id;

  if (G()->parameters().use_message_db) {
    ReadMessageThreadHistoryOnServerLogEvent log_event;
    log_event.dialog_id_ = dialog_id;
    log_event.top_thread_message_id_ = top_thread_message_id;
    log_event.max_message_id_ = max_message_id;
    add_log_event(d->read_history_log_event_ids[top_thread_message_id.get()], get_log_event_storer(log_event),
                  LogEvent::HandlerType::ReadMessageThreadHistoryOnServer, "read history");
  }

  d->updated_read_history_message_ids.insert(top_thread_message_id);

  bool need_delay = d->is_opened && last_message_id.is_valid() && max_message_id < last_message_id;
  pending_read_history_timeout_.set_timeout_in(dialog_id.get(), need_delay ? MIN_READ_HISTORY_DELAY : 0);
}

void MessagesManager::on_load_active_live_location_full_message_ids_from_database(string value) {
  if (G()->close_flag()) {
    return;
  }
  if (value.empty()) {
    LOG(INFO) << "Active live location messages aren't found in the database";
    on_load_active_live_location_messages_finished();

    if (!active_live_location_full_message_ids_.empty()) {
      save_active_live_locations();
    }
    return;
  }

  LOG(INFO) << "Successfully loaded active live location messages list of size " << value.size() << " from database";

  auto new_full_message_ids = std::move(active_live_location_full_message_ids_);
  vector<FullMessageId> old_full_message_ids;
  log_event_parse(old_full_message_ids, value).ensure();

  // TODO asynchronously load messages from database
  active_live_location_full_message_ids_.clear();
  for (const auto &full_message_id : old_full_message_ids) {
    Message *m = get_message_force(full_message_id, "on_load_active_live_location_full_message_ids_from_database");
    if (m != nullptr) {
      try_add_active_live_location(full_message_id.get_dialog_id(), m);
    }
  }

  for (const auto &full_message_id : new_full_message_ids) {
    add_active_live_location(full_message_id);
  }

  on_load_active_live_location_messages_finished();

  if (!new_full_message_ids.empty() || old_full_message_ids.size() != active_live_location_full_message_ids_.size()) {
    save_active_live_locations();
  }
}

void MessagesManager::save_active_live_locations() {
  CHECK(are_active_live_location_messages_loaded_);
  LOG(INFO) << "Save active live locations of size " << active_live_location_full_message_ids_.size() << " to database";
  if (G()->parameters().use_message_db) {
    G()->td_db()->get_sqlite_pmc()->set("di_active_live_location_messages",
                                        log_event_store(active_live_location_full_message_ids_).as_slice().str(),
                                        Auto());
  }
}

unique_ptr<MessagesManager::Message> MessagesManager::parse_message(DialogId dialog_id, MessageId expected_message_id,
                                                                    const BufferSlice &value, bool is_scheduled) {
  auto m = make_unique<Message>();

  auto status = log_event_parse(*m, value.as_slice());
  bool is_message_id_valid = [&] {
    if (is_scheduled) {
      if (!expected_message_id.is_valid_scheduled()) {
        return false;
      }
      if (m->message_id == expected_message_id) {
        return true;
      }
      return m->message_id.is_valid_scheduled() && expected_message_id.is_scheduled_server() &&
             m->message_id.is_scheduled_server() &&
             m->message_id.get_scheduled_server_message_id() == expected_message_id.get_scheduled_server_message_id();
    } else {
      if (!expected_message_id.is_valid()) {
        return false;
      }
      return m->message_id == expected_message_id;
    }
  }();
  if (status.is_error() || !is_message_id_valid) {
    // can't happen unless the database is broken, but has been seen in the wild
    LOG(ERROR) << "Receive invalid message from database: " << expected_message_id << ' ' << m->message_id << ' '
               << status << ' ' << format::as_hex_dump<4>(value.as_slice());
    if (!is_scheduled && dialog_id.get_type() != DialogType::SecretChat) {
      // trying to repair the message
      if (expected_message_id.is_valid() && expected_message_id.is_server()) {
        get_message_from_server({dialog_id, expected_message_id}, Auto(), "parse_message");
      }
      if (m->message_id.is_valid() && m->message_id.is_server()) {
        get_message_from_server({dialog_id, m->message_id}, Auto(), "parse_message");
      }
    }
    return nullptr;
  }

  LOG(INFO) << "Loaded " << m->message_id << " in " << dialog_id << " of size " << value.size() << " from database";
  return m;
}

class MessagesManager::SendMessageLogEvent {
 public:
  DialogId dialog_id;
  const Message *m_in;
  unique_ptr<Message> m_out;

  SendMessageLogEvent() : dialog_id(), m_in(nullptr) {
  }

  SendMessageLogEvent(DialogId dialog_id, const Message *m) : dialog_id(dialog_id), m_in(m) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id, storer);
    td::store(*m_in, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id, parser);
    td::parse(m_out, parser);
  }
};

void MessagesManager::save_send_message_log_event(DialogId dialog_id, const Message *m) {
  if (!G()->parameters().use_message_db) {
    return;
  }

  CHECK(m != nullptr);
  LOG(INFO) << "Save " << FullMessageId(dialog_id, m->message_id) << " to binlog";
  auto log_event = SendMessageLogEvent(dialog_id, m);
  CHECK(m->send_message_log_event_id == 0);
  m->send_message_log_event_id =
      binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::SendMessage, get_log_event_storer(log_event));
}

class MessagesManager::SendBotStartMessageLogEvent {
 public:
  UserId bot_user_id;
  DialogId dialog_id;
  string parameter;
  const Message *m_in = nullptr;
  unique_ptr<Message> m_out;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(bot_user_id, storer);
    td::store(dialog_id, storer);
    td::store(parameter, storer);
    td::store(*m_in, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(bot_user_id, parser);
    td::parse(dialog_id, parser);
    td::parse(parameter, parser);
    td::parse(m_out, parser);
  }
};

void MessagesManager::save_send_bot_start_message_log_event(UserId bot_user_id, DialogId dialog_id,
                                                            const string &parameter, const Message *m) {
  if (!G()->parameters().use_message_db) {
    return;
  }

  CHECK(m != nullptr);
  LOG(INFO) << "Save " << FullMessageId(dialog_id, m->message_id) << " to binlog";
  SendBotStartMessageLogEvent log_event;
  log_event.bot_user_id = bot_user_id;
  log_event.dialog_id = dialog_id;
  log_event.parameter = parameter;
  log_event.m_in = m;
  CHECK(m->send_message_log_event_id == 0);
  m->send_message_log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::SendBotStartMessage,
                                            get_log_event_storer(log_event));
}

class MessagesManager::SendInlineQueryResultMessageLogEvent {
 public:
  DialogId dialog_id;
  int64 query_id;
  string result_id;
  const Message *m_in = nullptr;
  unique_ptr<Message> m_out;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id, storer);
    td::store(query_id, storer);
    td::store(result_id, storer);
    td::store(*m_in, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id, parser);
    td::parse(query_id, parser);
    td::parse(result_id, parser);
    td::parse(m_out, parser);
  }
};

void MessagesManager::save_send_inline_query_result_message_log_event(DialogId dialog_id, const Message *m,
                                                                      int64 query_id, const string &result_id) {
  if (!G()->parameters().use_message_db) {
    return;
  }

  CHECK(m != nullptr);
  LOG(INFO) << "Save " << FullMessageId(dialog_id, m->message_id) << " to binlog";
  SendInlineQueryResultMessageLogEvent log_event;
  log_event.dialog_id = dialog_id;
  log_event.query_id = query_id;
  log_event.result_id = result_id;
  log_event.m_in = m;
  CHECK(m->send_message_log_event_id == 0);
  m->send_message_log_event_id = binlog_add(
      G()->td_db()->get_binlog(), LogEvent::HandlerType::SendInlineQueryResultMessage, get_log_event_storer(log_event));
}

class MessagesManager::ForwardMessagesLogEvent {
 public:
  DialogId to_dialog_id;
  DialogId from_dialog_id;
  vector<MessageId> message_ids;
  vector<Message *> messages_in;
  vector<unique_ptr<Message>> messages_out;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(to_dialog_id, storer);
    td::store(from_dialog_id, storer);
    td::store(message_ids, storer);
    td::store(messages_in, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(to_dialog_id, parser);
    td::parse(from_dialog_id, parser);
    td::parse(message_ids, parser);
    td::parse(messages_out, parser);
  }
};

uint64 MessagesManager::save_forward_messages_log_event(DialogId to_dialog_id, DialogId from_dialog_id,
                                                        const vector<Message *> &messages,
                                                        const vector<MessageId> &message_ids) {
  ForwardMessagesLogEvent log_event{to_dialog_id, from_dialog_id, message_ids, messages, Auto()};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::ForwardMessages,
                    get_log_event_storer(log_event));
}

class MessagesManager::SendScreenshotTakenNotificationMessageLogEvent {
 public:
  DialogId dialog_id;
  const Message *m_in = nullptr;
  unique_ptr<Message> m_out;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id, storer);
    td::store(*m_in, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id, parser);
    td::parse(m_out, parser);
  }
};

uint64 MessagesManager::save_send_screenshot_taken_notification_message_log_event(DialogId dialog_id,
                                                                                  const Message *m) {
  if (!G()->parameters().use_message_db) {
    return 0;
  }

  CHECK(m != nullptr);
  LOG(INFO) << "Save " << FullMessageId(dialog_id, m->message_id) << " to binlog";
  SendScreenshotTakenNotificationMessageLogEvent log_event;
  log_event.dialog_id = dialog_id;
  log_event.m_in = m;
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::SendScreenshotTakenNotificationMessage,
                    get_log_event_storer(log_event));
}

void MessagesManager::save_dialog_filters() {
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  DialogFiltersLogEvent log_event;
  log_event.updated_date = dialog_filters_updated_date_;
  log_event.server_dialog_filters_in = &server_dialog_filters_;
  log_event.dialog_filters_in = &dialog_filters_;

  LOG(INFO) << "Save server chat filters " << get_dialog_filter_ids(server_dialog_filters_)
            << " and local chat filters " << get_dialog_filter_ids(dialog_filters_);

  G()->td_db()->get_binlog_pmc()->set("dialog_filters", log_event_store(log_event).as_slice().str());
}

void MessagesManager::on_send_message_file_part_missing(int64 random_id, int bad_part) {
  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    // we can't receive fail more than once
    // but message can be successfully sent before
    LOG(WARNING) << "Receive FILE_PART_" << bad_part
                 << "_MISSING about successfully sent message with random_id = " << random_id;
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

  auto dialog_id = full_message_id.get_dialog_id();
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // LOG(ERROR) << "Found " << m->message_id << " in inaccessible " << dialog_id;
    // dump_debug_message_op(get_dialog(dialog_id), 5);
  }

  if (dialog_id.get_type() == DialogType::SecretChat) {
    CHECK(!m->message_id.is_scheduled());
    Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);

    // need to change message random_id before resending
    m->random_id = generate_new_random_id();

    delete_random_id_to_message_id_correspondence(d, random_id, m->message_id);
    add_random_id_to_message_id_correspondence(d, m->random_id, m->message_id);

    auto log_event = SendMessageLogEvent(dialog_id, m);
    CHECK(m->send_message_log_event_id != 0);
    binlog_rewrite(G()->td_db()->get_binlog(), m->send_message_log_event_id, LogEvent::HandlerType::SendMessage,
                   get_log_event_storer(log_event));
  }

  do_send_message(dialog_id, m, {bad_part});
}

void MessagesManager::on_send_message_file_reference_error(int64 random_id) {
  auto it = being_sent_messages_.find(random_id);
  if (it == being_sent_messages_.end()) {
    // we can't receive fail more than once
    // but message can be successfully sent before
    LOG(WARNING) << "Receive file reference invalid error about successfully sent message with random_id = "
                 << random_id;
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

  auto dialog_id = full_message_id.get_dialog_id();
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    // LOG(ERROR) << "Found " << m->message_id << " in inaccessible " << dialog_id;
    // dump_debug_message_op(get_dialog(dialog_id), 5);
  }

  if (dialog_id.get_type() == DialogType::SecretChat) {
    CHECK(!m->message_id.is_scheduled());
    Dialog *d = get_dialog(dialog_id);
    CHECK(d != nullptr);

    // need to change message random_id before resending
    m->random_id = generate_new_random_id();

    delete_random_id_to_message_id_correspondence(d, random_id, m->message_id);
    add_random_id_to_message_id_correspondence(d, m->random_id, m->message_id);

    auto log_event = SendMessageLogEvent(dialog_id, m);
    CHECK(m->send_message_log_event_id != 0);
    binlog_rewrite(G()->td_db()->get_binlog(), m->send_message_log_event_id, LogEvent::HandlerType::SendMessage,
                   get_log_event_storer(log_event));
  }

  do_send_message(dialog_id, m, {-1});
}

class MessagesManager::RegetDialogLogEvent {
 public:
  DialogId dialog_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
  }
};

uint64 MessagesManager::save_reget_dialog_log_event(DialogId dialog_id) {
  RegetDialogLogEvent log_event{dialog_id};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::RegetDialog, get_log_event_storer(log_event));
}

class MessagesManager::SetDialogFolderIdOnServerLogEvent {
 public:
  DialogId dialog_id_;
  FolderId folder_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
    td::store(folder_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
    td::parse(folder_id_, parser);
  }
};

void MessagesManager::set_dialog_folder_id_on_server(DialogId dialog_id, bool from_binlog) {
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  if (!from_binlog && G()->parameters().use_message_db) {
    SetDialogFolderIdOnServerLogEvent log_event;
    log_event.dialog_id_ = dialog_id;
    log_event.folder_id_ = d->folder_id;
    add_log_event(d->set_folder_id_log_event_id, get_log_event_storer(log_event),
                  LogEvent::HandlerType::SetDialogFolderIdOnServer, "set chat folder");
  }

  Promise<> promise;
  if (d->set_folder_id_log_event_id.log_event_id != 0) {
    d->set_folder_id_log_event_id.generation++;
    promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id,
                                      generation = d->set_folder_id_log_event_id.generation](Result<Unit> result) {
      if (!G()->close_flag()) {
        send_closure(actor_id, &MessagesManager::on_updated_dialog_folder_id, dialog_id, generation);
      }
    });
  }

  // TODO do not send two queries simultaneously or use SequenceDispatcher
  td_->create_handler<EditPeerFoldersQuery>(std::move(promise))->send(dialog_id, d->folder_id);
}

class MessagesManager::UnpinAllDialogMessagesOnServerLogEvent {
 public:
  DialogId dialog_id_;

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(dialog_id_, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(dialog_id_, parser);
  }
};

uint64 MessagesManager::save_unpin_all_dialog_messages_on_server_log_event(DialogId dialog_id) {
  UnpinAllDialogMessagesOnServerLogEvent log_event{dialog_id};
  return binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::UnpinAllDialogMessagesOnServer,
                    get_log_event_storer(log_event));
}

void MessagesManager::add_message_to_database(const Dialog *d, const Message *m, const char *source) {
  if (!G()->parameters().use_message_db) {
    return;
  }

  CHECK(d != nullptr);
  CHECK(m != nullptr);
  MessageId message_id = m->message_id;

  LOG(INFO) << "Add " << FullMessageId(d->dialog_id, message_id) << " to database from " << source;

  if (message_id.is_scheduled()) {
    set_dialog_has_scheduled_database_messages(d->dialog_id, true);
    G()->td_db()->get_messages_db_async()->add_scheduled_message({d->dialog_id, message_id}, log_event_store(*m),
                                                                 Auto());  // TODO Promise
    return;
  }
  LOG_CHECK(message_id.is_server() || message_id.is_local()) << source;

  ServerMessageId unique_message_id;
  int64 random_id = 0;
  int64 search_id = 0;
  string text;
  switch (d->dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
      if (message_id.is_server()) {
        unique_message_id = message_id.get_server_message_id();
      }
      // FOR DEBUG
      // text = get_message_search_text(m);
      // if (!text.empty()) {
      //   search_id = (static_cast<int64>(m->date) << 32) | static_cast<uint32>(Random::secure_int32());
      // }
      break;
    case DialogType::Channel:
      break;
    case DialogType::SecretChat:
      random_id = m->random_id;
      text = get_message_search_text(m);
      if (!text.empty()) {
        search_id = (static_cast<int64>(m->date) << 32) | static_cast<uint32>(m->random_id);
      }
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  int32 ttl_expires_at = 0;
  if (m->ttl_expires_at != 0) {
    ttl_expires_at = static_cast<int32>(m->ttl_expires_at - Time::now() + G()->server_time()) + 1;
  }
  if (m->ttl_period != 0 && (ttl_expires_at == 0 || m->date + m->ttl_period < ttl_expires_at)) {
    ttl_expires_at = m->date + m->ttl_period;
  }
  G()->td_db()->get_messages_db_async()->add_message({d->dialog_id, message_id}, unique_message_id,
                                                     get_message_sender(m), random_id, ttl_expires_at,
                                                     get_message_index_mask(d->dialog_id, m), search_id, text,
                                                     m->notification_id, m->top_thread_message_id, log_event_store(*m),
                                                     Auto());  // TODO Promise
}

class MessagesManager::DeleteMessageLogEvent {
 public:
  LogEvent::Id id_{0};
  FullMessageId full_message_id_;
  std::vector<FileId> file_ids_;

  template <class StorerT>
  void store(StorerT &storer) const {
    bool has_file_ids = !file_ids_.empty();
    BEGIN_STORE_FLAGS();
    STORE_FLAG(has_file_ids);
    END_STORE_FLAGS();

    td::store(full_message_id_, storer);
    if (has_file_ids) {
      td::store(file_ids_, storer);
    }
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    bool has_file_ids;
    BEGIN_PARSE_FLAGS();
    PARSE_FLAG(has_file_ids);
    END_PARSE_FLAGS();

    td::parse(full_message_id_, parser);
    if (has_file_ids) {
      td::parse(file_ids_, parser);
    }
  }
};

void MessagesManager::delete_message_from_database(Dialog *d, MessageId message_id, const Message *m,
                                                   bool is_permanently_deleted) {
  CHECK(d != nullptr);
  if (!message_id.is_valid() && !message_id.is_valid_scheduled()) {
    return;
  }

  if (message_id.is_yet_unsent()) {
    return;
  }

  if (m != nullptr && !m->message_id.is_scheduled() && m->message_id.is_local() &&
      m->top_thread_message_id.is_valid() && m->top_thread_message_id != m->message_id) {
    // must not load the message from the database
    Message *top_m = get_message(d, m->top_thread_message_id);
    if (top_m != nullptr && top_m->top_thread_message_id == top_m->message_id) {
      auto it = std::lower_bound(top_m->local_thread_message_ids.begin(), top_m->local_thread_message_ids.end(),
                                 m->message_id);
      if (it != top_m->local_thread_message_ids.end() && *it == m->message_id) {
        top_m->local_thread_message_ids.erase(it);
        on_message_changed(d, top_m, false, "delete_message_from_database");
      }
    }
  }

  if (is_permanently_deleted) {
    if (message_id.is_scheduled() && message_id.is_scheduled_server()) {
      d->deleted_scheduled_server_message_ids.insert(message_id.get_scheduled_server_message_id());
    } else {
      // don't store failed to send message identifiers for bots to reduce memory usage
      if (m == nullptr || !td_->auth_manager_->is_bot() || !m->is_failed_to_send) {
        d->deleted_message_ids.insert(message_id);
        send_closure_later(actor_id(this),
                           &MessagesManager::update_message_max_reply_media_timestamp_in_replied_messages, d->dialog_id,
                           message_id);
      }
    }

    if (message_id.is_any_server()) {
      auto old_message_id = find_old_message_id(d->dialog_id, message_id);
      if (old_message_id.is_valid()) {
        bool have_old_message = get_message(d, old_message_id) != nullptr;
        LOG(WARNING) << "Sent " << FullMessageId{d->dialog_id, message_id}
                     << " was deleted before it was received. Have old " << old_message_id << " = " << have_old_message;
        send_closure_later(actor_id(this), &MessagesManager::delete_messages, d->dialog_id,
                           vector<MessageId>{old_message_id}, false, Promise<Unit>());
        delete_update_message_id(d->dialog_id, message_id);
      }
    }
  }

  if (m != nullptr && m->notification_id.is_valid()) {
    CHECK(!message_id.is_scheduled());
    auto from_mentions = is_from_mention_notification_group(d, m);
    auto &group_info = from_mentions ? d->mention_notification_group : d->message_notification_group;

    if (group_info.group_id.is_valid()) {
      if (group_info.last_notification_id == m->notification_id) {
        // last notification is deleted, need to find new last notification
        fix_dialog_last_notification_id(d, from_mentions, m->message_id);
      }
      if (is_message_notification_active(d, m)) {
        send_closure_later(G()->notification_manager(), &NotificationManager::remove_notification, group_info.group_id,
                           m->notification_id, true, false, Promise<Unit>(), "delete_message_from_database");
      }
    }
  } else if (!message_id.is_scheduled() && message_id > d->last_new_message_id) {
    send_closure_later(G()->notification_manager(), &NotificationManager::remove_temporary_notification_by_message_id,
                       d->message_notification_group.group_id, message_id, false, "delete_message_from_database");
    send_closure_later(G()->notification_manager(), &NotificationManager::remove_temporary_notification_by_message_id,
                       d->mention_notification_group.group_id, message_id, false, "delete_message_from_database");
  }

  auto need_delete_files = need_delete_message_files(d->dialog_id, m);
  if (need_delete_files) {
    delete_message_files(d->dialog_id, m);
  }

  if (!G()->parameters().use_message_db) {
    return;
  }

  DeleteMessageLogEvent log_event;

  log_event.full_message_id_ = {d->dialog_id, message_id};

  if (need_delete_files) {
    log_event.file_ids_ = get_message_file_ids(m);
  }

  do_delete_message_log_event(log_event);
}

void MessagesManager::do_delete_message_log_event(const DeleteMessageLogEvent &log_event) const {
  CHECK(G()->parameters().use_message_db);

  Promise<Unit> db_promise;
  if (!log_event.file_ids_.empty()) {
    auto log_event_id = log_event.id_;
    if (log_event_id == 0) {
      log_event_id =
          binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::DeleteMessage, get_log_event_storer(log_event));
    }

    MultiPromiseActorSafe mpas{"DeleteMessageMultiPromiseActor"};
    mpas.add_promise(
        PromiseCreator::lambda([log_event_id, context_weak_ptr = get_context_weak_ptr()](Result<Unit> result) {
          auto context = context_weak_ptr.lock();
          if (result.is_error() || context == nullptr) {
            return;
          }
          CHECK(context->get_id() == Global::ID);
          auto global = static_cast<Global *>(context.get());
          if (global->close_flag()) {
            return;
          }

          binlog_erase(global->td_db()->get_binlog(), log_event_id);
        }));

    auto lock = mpas.get_promise();
    for (auto file_id : log_event.file_ids_) {
      if (need_delete_file(log_event.full_message_id_, file_id)) {
        send_closure(G()->file_manager(), &FileManager::delete_file, file_id, mpas.get_promise(),
                     "do_delete_message_log_event");
      }
    }
    db_promise = mpas.get_promise();
    lock.set_value(Unit());
  }

  // message may not exist in the dialog
  LOG(INFO) << "Delete " << log_event.full_message_id_ << " from database";
  G()->td_db()->get_messages_db_async()->delete_message(log_event.full_message_id_, std::move(db_promise));
}

unique_ptr<MessagesManager::Dialog> MessagesManager::parse_dialog(DialogId dialog_id, const BufferSlice &value,
                                                                  const char *source) {
  LOG(INFO) << "Loaded " << dialog_id << " of size " << value.size() << " from database from " << source;
  auto d = make_unique<Dialog>();
  d->dialog_id = dialog_id;
  invalidate_message_indexes(d.get());  // must initialize indexes, because some of them could be not parsed

  loaded_dialogs_.insert(dialog_id);

  auto status = log_event_parse(*d, value.as_slice());
  if (status.is_error() || !d->dialog_id.is_valid() || d->dialog_id != dialog_id) {
    // can't happen unless database is broken, but has been seen in the wild
    // if dialog_id is invalid, we can't repair the dialog
    LOG_CHECK(dialog_id.is_valid()) << "Can't repair " << dialog_id << ' ' << d->dialog_id << ' ' << status << ' '
                                    << source << ' ' << format::as_hex_dump<4>(value.as_slice());

    LOG(ERROR) << "Repair broken " << dialog_id << ' ' << format::as_hex_dump<4>(value.as_slice());

    // just clean all known data about the dialog
    d = make_unique<Dialog>();
    d->dialog_id = dialog_id;
    invalidate_message_indexes(d.get());

    // and try to reget it from the server if possible
    have_dialog_info_force(dialog_id);
    if (have_input_peer(dialog_id, AccessRights::Read)) {
      if (dialog_id.get_type() != DialogType::SecretChat) {
        send_get_dialog_query(dialog_id, Auto(), 0, source);
      }
    } else {
      LOG(ERROR) << "Have no info about " << dialog_id << " from " << source << " to repair it";
    }
  }
  CHECK(dialog_id == d->dialog_id);

  Dependencies dependencies;
  add_dialog_dependencies(dependencies, dialog_id);
  if (d->default_join_group_call_as_dialog_id != dialog_id) {
    add_message_sender_dependencies(dependencies, d->default_join_group_call_as_dialog_id);
  }
  if (d->default_send_message_as_dialog_id != dialog_id) {
    add_message_sender_dependencies(dependencies, d->default_send_message_as_dialog_id);
  }
  if (d->messages != nullptr) {
    add_message_dependencies(dependencies, d->messages.get());
  }
  if (d->draft_message != nullptr) {
    add_formatted_text_dependencies(dependencies, &d->draft_message->input_message_text.text);
  }
  for (auto user_id : d->pending_join_request_user_ids) {
    dependencies.user_ids.insert(user_id);
  }
  if (!resolve_dependencies_force(td_, dependencies, source)) {
    send_get_dialog_query(dialog_id, Auto(), 0, source);
  }

  return d;
}

MessagesManager::Dialog *MessagesManager::on_load_dialog_from_database(DialogId dialog_id, BufferSlice &&value,
                                                                       const char *source) {
  CHECK(G()->parameters().use_message_db);

  if (!dialog_id.is_valid()) {
    // hack
    LogEventParser dialog_id_parser(value.as_slice());
    int32 flags;
    parse(flags, dialog_id_parser);
    parse(dialog_id, dialog_id_parser);

    if (!dialog_id.is_valid()) {
      LOG(ERROR) << "Failed to parse dialog_id from blob. Database is broken";
      return nullptr;
    }
  }

  auto old_d = get_dialog(dialog_id);
  if (old_d != nullptr) {
    return old_d;
  }

  LOG(INFO) << "Add new " << dialog_id << " from database from " << source;
  return add_new_dialog(parse_dialog(dialog_id, value, source), true, source);
}

class MessagesManager::GetChannelDifferenceLogEvent {
 public:
  ChannelId channel_id;
  int64 access_hash;

  GetChannelDifferenceLogEvent() : channel_id(), access_hash() {
  }

  GetChannelDifferenceLogEvent(ChannelId channel_id, int64 access_hash)
      : channel_id(channel_id), access_hash(access_hash) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(channel_id, storer);
    td::store(access_hash, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(channel_id, parser);
    td::parse(access_hash, parser);
  }
};

void MessagesManager::get_channel_difference(DialogId dialog_id, int32 pts, bool force, const char *source) {
  if (channel_get_difference_retry_timeout_.has_timeout(dialog_id.get())) {
    LOG(INFO) << "Skip running channels.getDifference for " << dialog_id << " from " << source
              << " because it is scheduled for later time";
    return;
  }
  LOG_CHECK(dialog_id.get_type() == DialogType::Channel) << dialog_id << " " << source;

  if (active_get_channel_differencies_.count(dialog_id)) {
    LOG(INFO) << "Skip running channels.getDifference for " << dialog_id << " from " << source
              << " because it has already been run";
    return;
  }
  auto input_channel = td_->contacts_manager_->get_input_channel(dialog_id.get_channel_id());
  if (input_channel == nullptr) {
    LOG(ERROR) << "Skip running channels.getDifference for " << dialog_id << " from " << source
               << " because have no info about the chat";
    after_get_channel_difference(dialog_id, false);
    return;
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    LOG(INFO) << "Skip running channels.getDifference for " << dialog_id << " from " << source
              << " because have no read access to it";
    after_get_channel_difference(dialog_id, false);
    return;
  }

  if (force && get_channel_difference_to_log_event_id_.count(dialog_id) == 0 && !G()->ignore_background_updates()) {
    auto channel_id = dialog_id.get_channel_id();
    CHECK(input_channel->get_id() == telegram_api::inputChannel::ID);
    auto access_hash = static_cast<const telegram_api::inputChannel &>(*input_channel).access_hash_;
    auto log_event = GetChannelDifferenceLogEvent(channel_id, access_hash);
    auto log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::GetChannelDifference,
                                   get_log_event_storer(log_event));

    get_channel_difference_to_log_event_id_.emplace(dialog_id, log_event_id);
  }

  return do_get_channel_difference(dialog_id, pts, force, std::move(input_channel), source);
}

void MessagesManager::on_binlog_events(vector<BinlogEvent> &&events) {
  for (auto &event : events) {
    CHECK(event.id_ != 0);
    switch (event.type_) {
      case LogEvent::HandlerType::SendMessage: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        SendMessageLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id;
        auto m = std::move(log_event.m_out);
        m->send_message_log_event_id = event.id_;

        if (m->content->get_type() == MessageContentType::Unsupported) {
          LOG(ERROR) << "Message content is invalid: " << format::as_hex_dump<4>(Slice(event.data_));
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }

        Dependencies dependencies;
        add_dialog_dependencies(dependencies, dialog_id);
        add_message_dependencies(dependencies, m.get());
        resolve_dependencies_force(td_, dependencies, "SendMessageLogEvent");

        m->content =
            dup_message_content(td_, dialog_id, m->content.get(), MessageContentDupType::Send, MessageCopyOptions());

        auto result_message = continue_send_message(dialog_id, std::move(m), event.id_);
        if (result_message != nullptr) {
          do_send_message(dialog_id, result_message);
        }

        break;
      }
      case LogEvent::HandlerType::SendBotStartMessage: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        SendBotStartMessageLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id;
        auto m = std::move(log_event.m_out);
        m->send_message_log_event_id = event.id_;

        CHECK(m->content->get_type() == MessageContentType::Text);

        Dependencies dependencies;
        add_dialog_dependencies(dependencies, dialog_id);
        add_message_dependencies(dependencies, m.get());
        resolve_dependencies_force(td_, dependencies, "SendBotStartMessageLogEvent");

        auto bot_user_id = log_event.bot_user_id;
        if (!td_->contacts_manager_->have_user_force(bot_user_id)) {
          LOG(ERROR) << "Can't find bot " << bot_user_id;
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }

        auto result_message = continue_send_message(dialog_id, std::move(m), event.id_);
        if (result_message != nullptr) {
          do_send_bot_start_message(bot_user_id, dialog_id, log_event.parameter, result_message);
        }
        break;
      }
      case LogEvent::HandlerType::SendInlineQueryResultMessage: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        SendInlineQueryResultMessageLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id;
        auto m = std::move(log_event.m_out);
        m->send_message_log_event_id = event.id_;

        if (m->content->get_type() == MessageContentType::Unsupported) {
          LOG(ERROR) << "Message content is invalid: " << format::as_hex_dump<4>(Slice(event.data_));
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }

        Dependencies dependencies;
        add_dialog_dependencies(dependencies, dialog_id);
        add_message_dependencies(dependencies, m.get());
        resolve_dependencies_force(td_, dependencies, "SendInlineQueryResultMessageLogEvent");

        m->content = dup_message_content(td_, dialog_id, m->content.get(), MessageContentDupType::SendViaBot,
                                         MessageCopyOptions());

        auto result_message = continue_send_message(dialog_id, std::move(m), event.id_);
        if (result_message != nullptr) {
          do_send_inline_query_result_message(dialog_id, result_message, log_event.query_id, log_event.result_id);
        }
        break;
      }
      case LogEvent::HandlerType::SendScreenshotTakenNotificationMessage: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        SendScreenshotTakenNotificationMessageLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id;
        auto m = std::move(log_event.m_out);
        m->send_message_log_event_id = 0;  // to not allow event deletion by message deletion

        CHECK(m->content->get_type() == MessageContentType::ScreenshotTaken);

        Dependencies dependencies;
        add_dialog_dependencies(dependencies, dialog_id);
        add_message_dependencies(dependencies, m.get());
        resolve_dependencies_force(td_, dependencies, "SendScreenshotTakenNotificationMessageLogEvent");

        auto result_message = continue_send_message(dialog_id, std::move(m), event.id_);
        if (result_message != nullptr) {
          do_send_screenshot_taken_notification_message(dialog_id, result_message, event.id_);
        }
        break;
      }
      case LogEvent::HandlerType::ForwardMessages: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }

        ForwardMessagesLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto to_dialog_id = log_event.to_dialog_id;
        auto from_dialog_id = log_event.from_dialog_id;
        auto messages = std::move(log_event.messages_out);

        Dependencies dependencies;
        add_dialog_dependencies(dependencies, to_dialog_id);
        add_dialog_dependencies(dependencies, from_dialog_id);
        for (auto &m : messages) {
          add_message_dependencies(dependencies, m.get());
        }
        resolve_dependencies_force(td_, dependencies, "ForwardMessagesLogEvent");

        Dialog *to_dialog = get_dialog_force(to_dialog_id, "ForwardMessagesLogEvent to");
        if (to_dialog == nullptr) {
          LOG(ERROR) << "Can't find " << to_dialog_id << " to forward messages to";
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }
        Dialog *from_dialog = get_dialog_force(from_dialog_id, "ForwardMessagesLogEvent from");
        if (from_dialog == nullptr) {
          LOG(ERROR) << "Can't find " << from_dialog_id << " to forward messages from";
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }

        to_dialog->was_opened = true;

        auto now = G()->unix_time();
        if (!have_input_peer(from_dialog_id, AccessRights::Read) || can_send_message(to_dialog_id).is_error() ||
            messages.empty() ||
            (messages[0]->send_date < now - MAX_RESEND_DELAY && to_dialog_id != get_my_dialog_id())) {
          LOG(WARNING) << "Can't continue forwarding " << messages.size() << " message(s) to " << to_dialog_id;
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          continue;
        }

        LOG(INFO) << "Continue to forward " << messages.size() << " message(s) to " << to_dialog_id << " from binlog";

        bool need_update = false;
        bool need_update_dialog_pos = false;
        vector<Message *> forwarded_messages;
        for (auto &m : messages) {
          if (m->message_id.is_scheduled()) {
            set_message_id(m, get_next_yet_unsent_scheduled_message_id(to_dialog, m->date));
          } else {
            set_message_id(m, get_next_yet_unsent_message_id(to_dialog));
            m->date = now;
          }
          m->content = dup_message_content(td_, to_dialog_id, m->content.get(), MessageContentDupType::Forward,
                                           MessageCopyOptions());
          CHECK(m->content != nullptr);
          m->have_previous = true;
          m->have_next = true;

          forwarded_messages.push_back(add_message_to_dialog(to_dialog, std::move(m), true, &need_update,
                                                             &need_update_dialog_pos, "forward message again"));
          send_update_new_message(to_dialog, forwarded_messages.back());
        }

        send_update_chat_has_scheduled_messages(to_dialog, false);

        if (need_update_dialog_pos) {
          send_update_chat_last_message(to_dialog, "on_reforward_message");
        }

        do_forward_messages(to_dialog_id, from_dialog_id, forwarded_messages, log_event.message_ids, event.id_);
        break;
      }
      case LogEvent::HandlerType::DeleteMessage: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        DeleteMessageLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();
        log_event.id_ = event.id_;

        Dialog *d = get_dialog_force(log_event.full_message_id_.get_dialog_id(), "DeleteMessageLogEvent");
        if (d != nullptr) {
          auto message_id = log_event.full_message_id_.get_message_id();
          if (message_id.is_valid_scheduled() && message_id.is_scheduled_server()) {
            d->deleted_scheduled_server_message_ids.insert(message_id.get_scheduled_server_message_id());
          } else {
            d->deleted_message_ids.insert(message_id);
          }
        }

        do_delete_message_log_event(log_event);
        break;
      }
      case LogEvent::HandlerType::DeleteMessagesOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        DeleteMessagesOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "DeleteMessagesOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        d->deleted_message_ids.insert(log_event.message_ids_.begin(), log_event.message_ids_.end());

        delete_messages_on_server(dialog_id, std::move(log_event.message_ids_), log_event.revoke_, event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::DeleteScheduledMessagesOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        DeleteScheduledMessagesOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "DeleteScheduledMessagesOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        for (auto message_id : log_event.message_ids_) {
          CHECK(message_id.is_scheduled_server());
          d->deleted_scheduled_server_message_ids.insert(message_id.get_scheduled_server_message_id());
        }

        delete_scheduled_messages_on_server(dialog_id, std::move(log_event.message_ids_), event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::DeleteDialogHistoryOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        DeleteDialogHistoryOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "DeleteDialogHistoryOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        delete_dialog_history_on_server(dialog_id, log_event.max_message_id_, log_event.remove_from_dialog_list_,
                                        log_event.revoke_, true, event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::DeleteAllCallMessagesOnServer: {
        DeleteAllCallMessagesOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        delete_all_call_messages_on_server(log_event.revoke_, event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::BlockMessageSenderFromRepliesOnServer: {
        BlockMessageSenderFromRepliesOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        block_message_sender_from_replies_on_server(log_event.message_id_, log_event.delete_message_,
                                                    log_event.delete_all_messages_, log_event.report_spam_, event.id_,
                                                    Auto());
        break;
      }
      case LogEvent::HandlerType::DeleteAllChannelMessagesFromSenderOnServer: {
        if (!G()->parameters().use_chat_info_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        DeleteAllChannelMessagesFromSenderOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto channel_id = log_event.channel_id_;
        if (!td_->contacts_manager_->have_channel_force(channel_id)) {
          LOG(ERROR) << "Can't find " << channel_id;
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        auto sender_dialog_id = log_event.sender_dialog_id_;
        if (!have_dialog_info_force(sender_dialog_id) || !have_input_peer(sender_dialog_id, AccessRights::Know)) {
          LOG(ERROR) << "Can't find " << sender_dialog_id;
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        delete_all_channel_messages_by_sender_on_server(channel_id, sender_dialog_id, event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::DeleteDialogMessagesByDateOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        DeleteDialogMessagesByDateOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "DeleteDialogMessagesByDateOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        delete_dialog_messages_by_date_on_server(dialog_id, log_event.min_date_, log_event.max_date_, log_event.revoke_,
                                                 event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::ReadHistoryOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ReadHistoryOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "ReadHistoryOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        if (d->read_history_log_event_ids[0].log_event_id != 0) {
          // we need only latest read history event
          binlog_erase(G()->td_db()->get_binlog(), d->read_history_log_event_ids[0].log_event_id);
        }
        d->read_history_log_event_ids[0].log_event_id = event.id_;

        read_history_on_server_impl(d, log_event.max_message_id_);
        break;
      }
      case LogEvent::HandlerType::ReadHistoryInSecretChat: {
        ReadHistoryInSecretChatLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        CHECK(dialog_id.get_type() == DialogType::SecretChat);
        if (!td_->contacts_manager_->have_secret_chat_force(dialog_id.get_secret_chat_id())) {
          LOG(ERROR) << "Have no info about " << dialog_id;
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        force_create_dialog(dialog_id, "ReadHistoryInSecretChatLogEvent");
        Dialog *d = get_dialog(dialog_id);
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        if (d->read_history_log_event_ids[0].log_event_id != 0) {
          // we need only latest read history event
          binlog_erase(G()->td_db()->get_binlog(), d->read_history_log_event_ids[0].log_event_id);
        }
        d->read_history_log_event_ids[0].log_event_id = event.id_;
        d->last_read_inbox_message_date = log_event.max_date_;

        read_history_on_server_impl(d, MessageId());
        break;
      }
      case LogEvent::HandlerType::ReadMessageThreadHistoryOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ReadMessageThreadHistoryOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "ReadMessageThreadHistoryOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        auto top_thread_message_id = log_event.top_thread_message_id_;
        if (d->read_history_log_event_ids[top_thread_message_id.get()].log_event_id != 0) {
          // we need only latest read history event
          binlog_erase(G()->td_db()->get_binlog(),
                       d->read_history_log_event_ids[top_thread_message_id.get()].log_event_id);
        }
        d->read_history_log_event_ids[top_thread_message_id.get()].log_event_id = event.id_;

        read_message_thread_history_on_server_impl(d, top_thread_message_id, log_event.max_message_id_);
        break;
      }
      case LogEvent::HandlerType::ReadMessageContentsOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ReadMessageContentsOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "ReadMessageContentsOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        read_message_contents_on_server(dialog_id, std::move(log_event.message_ids_), event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::ReadAllDialogMentionsOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ReadAllDialogMentionsOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "ReadAllDialogMentionsOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        read_all_dialog_mentions_on_server(dialog_id, event.id_, Promise<Unit>());
        break;
      }
      case LogEvent::HandlerType::ToggleDialogIsPinnedOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ToggleDialogIsPinnedOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "ToggleDialogIsPinnedOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        toggle_dialog_is_pinned_on_server(dialog_id, log_event.is_pinned_, event.id_);
        break;
      }
      case LogEvent::HandlerType::ReorderPinnedDialogsOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ReorderPinnedDialogsOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        vector<DialogId> dialog_ids;
        for (auto &dialog_id : log_event.dialog_ids_) {
          Dialog *d = get_dialog_force(dialog_id, "ReorderPinnedDialogsOnServerLogEvent");
          if (d != nullptr && have_input_peer(dialog_id, AccessRights::Read)) {
            dialog_ids.push_back(dialog_id);
          }
        }
        if (dialog_ids.empty()) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        reorder_pinned_dialogs_on_server(log_event.folder_id_, dialog_ids, event.id_);
        break;
      }
      case LogEvent::HandlerType::ToggleDialogIsMarkedAsUnreadOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ToggleDialogIsMarkedAsUnreadOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        bool have_info = dialog_id.get_type() == DialogType::User
                             ? td_->contacts_manager_->have_user_force(dialog_id.get_user_id())
                             : have_dialog_force(dialog_id, "ToggleDialogIsMarkedAsUnreadOnServerLogEvent");
        if (!have_info || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        toggle_dialog_is_marked_as_unread_on_server(dialog_id, log_event.is_marked_as_unread_, event.id_);
        break;
      }
      case LogEvent::HandlerType::ToggleDialogIsBlockedOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ToggleDialogIsBlockedOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        if (dialog_id.get_type() == DialogType::SecretChat || !have_dialog_info_force(dialog_id) ||
            !have_input_peer(dialog_id, AccessRights::Know)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        toggle_dialog_is_blocked_on_server(dialog_id, log_event.is_blocked_, event.id_);
        break;
      }
      case LogEvent::HandlerType::SaveDialogDraftMessageOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        SaveDialogDraftMessageOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "SaveDialogDraftMessageOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Write)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        d->save_draft_message_log_event_id.log_event_id = event.id_;

        save_dialog_draft_message_on_server(dialog_id);
        break;
      }
      case LogEvent::HandlerType::UpdateDialogNotificationSettingsOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        UpdateDialogNotificationSettingsOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "UpdateDialogNotificationSettingsOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        d->save_notification_settings_log_event_id.log_event_id = event.id_;

        update_dialog_notification_settings_on_server(dialog_id, true);
        break;
      }
      case LogEvent::HandlerType::UpdateScopeNotificationSettingsOnServer: {
        UpdateScopeNotificationSettingsOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        update_scope_notification_settings_on_server(log_event.scope_, event.id_);
        break;
      }
      case LogEvent::HandlerType::ResetAllNotificationSettingsOnServer: {
        ResetAllNotificationSettingsOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        reset_all_notification_settings_on_server(event.id_);
        break;
      }
      case LogEvent::HandlerType::ToggleDialogReportSpamStateOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        ToggleDialogReportSpamStateOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "ToggleDialogReportSpamStateOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        toggle_dialog_report_spam_state_on_server(dialog_id, log_event.is_spam_dialog_, event.id_, Promise<Unit>());
        break;
      }
      case LogEvent::HandlerType::SetDialogFolderIdOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        SetDialogFolderIdOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dialog *d = get_dialog_force(dialog_id, "SetDialogFolderIdOnServerLogEvent");
        if (d == nullptr || !have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }
        d->set_folder_id_log_event_id.log_event_id = event.id_;

        set_dialog_folder_id(d, log_event.folder_id_);

        set_dialog_folder_id_on_server(dialog_id, true);
        break;
      }
      case LogEvent::HandlerType::RegetDialog: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        RegetDialogLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        auto dialog_id = log_event.dialog_id_;
        Dependencies dependencies;
        add_dialog_dependencies(dependencies, dialog_id);
        resolve_dependencies_force(td_, dependencies, "RegetDialogLogEvent");

        get_dialog_force(dialog_id, "RegetDialogLogEvent");  // load it if exists

        if (!have_input_peer(dialog_id, AccessRights::Read)) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        send_get_dialog_query(dialog_id, Auto(), event.id_, "RegetDialogLogEvent");
        break;
      }
      case LogEvent::HandlerType::UnpinAllDialogMessagesOnServer: {
        if (!G()->parameters().use_message_db) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        UnpinAllDialogMessagesOnServerLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        unpin_all_dialog_messages_on_server(log_event.dialog_id_, event.id_, Auto());
        break;
      }
      case LogEvent::HandlerType::GetChannelDifference: {
        if (G()->ignore_background_updates()) {
          binlog_erase(G()->td_db()->get_binlog(), event.id_);
          break;
        }

        GetChannelDifferenceLogEvent log_event;
        log_event_parse(log_event, event.data_).ensure();

        DialogId dialog_id(log_event.channel_id);
        LOG(INFO) << "Continue to run getChannelDifference in " << dialog_id;
        get_channel_difference_to_log_event_id_.emplace(dialog_id, event.id_);
        do_get_channel_difference(
            dialog_id, load_channel_pts(dialog_id), true,
            telegram_api::make_object<telegram_api::inputChannel>(log_event.channel_id.get(), log_event.access_hash),
            "LogEvent::HandlerType::GetChannelDifference");
        break;
      }
      default:
        LOG(FATAL) << "Unsupported log event type " << event.type_;
    }
  }
}
}  // namespace td
