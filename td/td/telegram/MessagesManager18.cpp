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




#include <tuple>





namespace td {
}  // namespace td
namespace td {

bool MessagesManager::add_new_message_notification(Dialog *d, Message *m, bool force) {
  CHECK(d != nullptr);
  CHECK(m != nullptr);
  CHECK(m->message_id.is_valid());

  if (!force) {
    if (d->message_notification_group.group_id.is_valid()) {
      send_closure_later(G()->notification_manager(), &NotificationManager::remove_temporary_notifications,
                         d->message_notification_group.group_id, "add_new_message_notification 1");
    }
    if (d->mention_notification_group.group_id.is_valid()) {
      send_closure_later(G()->notification_manager(), &NotificationManager::remove_temporary_notifications,
                         d->mention_notification_group.group_id, "add_new_message_notification 2");
    }
  }

  CHECK(!m->notification_id.is_valid());
  if (is_message_notification_disabled(d, m)) {
    return false;
  }

  auto from_mentions = is_from_mention_notification_group(d, m);
  bool is_pinned = m->content->get_type() == MessageContentType::PinMessage;
  bool is_active =
      from_mentions ? m->contains_unread_mention || is_pinned : m->message_id > d->last_read_inbox_message_id;
  if (is_active) {
    auto &group = from_mentions ? d->mention_notification_group : d->message_notification_group;
    if (group.max_removed_message_id >= m->message_id) {
      is_active = false;
    }
  }
  if (!is_active) {
    VLOG(notifications) << "Disable inactive notification for " << m->message_id << " in " << d->dialog_id;
    if (is_pinned) {
      remove_dialog_pinned_message_notification(d, "add_new_message_notification");
    }
    return false;
  }

  VLOG(notifications) << "Trying to " << (force ? "forcely " : "") << "add new message notification for "
                      << m->message_id << " in " << d->dialog_id
                      << (m->disable_notification ? " silently" : " with sound");

  DialogId settings_dialog_id = d->dialog_id;
  Dialog *settings_dialog = d;
  if (m->contains_mention && !m->is_mention_notification_disabled) {
    // have a mention, so use notification settings from the dialog with the sender
    auto sender_dialog_id = get_message_sender(m);
    if (sender_dialog_id.is_valid()) {
      settings_dialog_id = sender_dialog_id;
      settings_dialog = get_dialog_force(settings_dialog_id, "add_new_message_notification");
    }
  }

  bool have_settings;
  int32 mute_until;
  std::tie(have_settings, mute_until) = get_dialog_mute_until(settings_dialog_id, settings_dialog);
  if (mute_until > m->date && (have_settings || force)) {
    VLOG(notifications) << "Disable notification, because " << settings_dialog_id << " is muted";
    if (is_pinned) {
      remove_dialog_pinned_message_notification(d, "add_new_message_notification");
    }
    return false;
  }

  MessageId missing_pinned_message_id;
  if (is_pinned) {
    auto message_id = get_message_content_pinned_message_id(m->content.get());
    if (message_id.is_valid() &&
        !have_message_force(d, message_id,
                            force ? "add_new_message_notification force" : "add_new_message_notification not force")) {
      missing_pinned_message_id = message_id;
    }
  }

  auto &pending_notifications =
      from_mentions ? d->pending_new_mention_notifications : d->pending_new_message_notifications;
  if (!force && (!have_settings || !pending_notifications.empty() || missing_pinned_message_id.is_valid())) {
    VLOG(notifications) << "Delay new message notification for " << m->message_id << " in " << d->dialog_id << " with "
                        << pending_notifications.size() << " already waiting messages";
    if (pending_notifications.empty()) {
      VLOG(notifications) << "Create FlushPendingNewMessageNotificationsSleepActor for " << d->dialog_id;
      create_actor<SleepActor>("FlushPendingNewMessageNotificationsSleepActor", 5.0,
                               PromiseCreator::lambda([actor_id = actor_id(this), dialog_id = d->dialog_id,
                                                       from_mentions](Result<Unit> result) {
                                 VLOG(notifications)
                                     << "Pending notifications timeout in " << dialog_id << " has expired";
                                 send_closure(actor_id, &MessagesManager::flush_pending_new_message_notifications,
                                              dialog_id, from_mentions, DialogId());
                               }))
          .release();
    }
    auto last_settings_dialog_id = (pending_notifications.empty() ? DialogId() : pending_notifications.back().first);
    pending_notifications.emplace_back((have_settings ? DialogId() : settings_dialog_id), m->message_id);
    if (!have_settings && last_settings_dialog_id != settings_dialog_id) {
      VLOG(notifications) << "Fetch notification settings for " << settings_dialog_id;
      auto promise = PromiseCreator::lambda([actor_id = actor_id(this), dialog_id = d->dialog_id, from_mentions,
                                             settings_dialog_id](Result<Unit> result) {
        send_closure(actor_id, &MessagesManager::flush_pending_new_message_notifications, dialog_id, from_mentions,
                     settings_dialog_id);
      });
      if (settings_dialog == nullptr && have_input_peer(settings_dialog_id, AccessRights::Read)) {
        force_create_dialog(settings_dialog_id, "add_new_message_notification 2");
        settings_dialog = get_dialog(settings_dialog_id);
      }
      if (settings_dialog != nullptr) {
        send_get_dialog_notification_settings_query(settings_dialog_id, std::move(promise));
      } else {
        send_get_dialog_query(settings_dialog_id, std::move(promise), 0, "add_new_message_notification");
      }
    }
    if (missing_pinned_message_id.is_valid()) {
      VLOG(notifications) << "Fetch pinned " << missing_pinned_message_id;
      auto promise = PromiseCreator::lambda(
          [actor_id = actor_id(this), dialog_id = d->dialog_id, from_mentions](Result<Unit> result) {
            send_closure(actor_id, &MessagesManager::flush_pending_new_message_notifications, dialog_id, from_mentions,
                         dialog_id);
          });
      get_message_from_server({d->dialog_id, missing_pinned_message_id}, std::move(promise),
                              "add_new_message_notification");
    }
    return false;
  }

  LOG_IF(WARNING, !have_settings) << "Have no notification settings for " << settings_dialog_id
                                  << ", but forced to send notification about " << m->message_id << " in "
                                  << d->dialog_id;
  auto &group_info = get_notification_group_info(d, m);
  auto notification_group_id = get_dialog_notification_group_id(d->dialog_id, group_info);
  if (!notification_group_id.is_valid()) {
    return false;
  }
  // if !force, then add_message_to_dialog will add the correspondence
  m->notification_id = get_next_notification_id(d, notification_group_id, force ? m->message_id : MessageId());
  if (!m->notification_id.is_valid()) {
    return false;
  }
  bool is_changed = set_dialog_last_notification(d->dialog_id, group_info, m->date, m->notification_id,
                                                 "add_new_message_notification 3");
  CHECK(is_changed);
  if (is_pinned) {
    set_dialog_pinned_message_notification(d, from_mentions ? m->message_id : MessageId(),
                                           "add_new_message_notification");
  }
  if (!m->notification_id.is_valid()) {
    // protection from accidental notification_id removal in set_dialog_pinned_message_notification
    return false;
  }
  VLOG(notifications) << "Create " << m->notification_id << " with " << m->message_id << " in " << group_info.group_id
                      << '/' << d->dialog_id;
  int32 min_delay_ms = 0;
  if (need_delay_message_content_notification(m->content.get(), td_->contacts_manager_->get_my_id())) {
    min_delay_ms = 3000;  // 3 seconds
  } else if (td_->is_online() && d->is_opened) {
    min_delay_ms = 1000;  // 1 second
  }
  bool is_silent = m->disable_notification || m->message_id <= d->max_notification_message_id;
  send_closure_later(G()->notification_manager(), &NotificationManager::add_notification, notification_group_id,
                     from_mentions ? NotificationGroupType::Mentions : NotificationGroupType::Messages, d->dialog_id,
                     m->date, settings_dialog_id, m->disable_notification, is_silent, min_delay_ms, m->notification_id,
                     create_new_message_notification(m->message_id), "add_new_message_notification");
  return true;
}

class HidePromoDataQuery final : public Td::ResultHandler {
  DialogId dialog_id_;

 public:
  void send(DialogId dialog_id) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(telegram_api::help_hidePromoData(std::move(input_peer))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::help_hidePromoData>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    // we are not interested in the result
  }

  void on_error(Status status) final {
    if (!td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "HidePromoDataQuery")) {
      LOG(ERROR) << "Receive error for sponsored chat hiding: " << status;
    }
  }
};

void MessagesManager::delete_dialog_history(DialogId dialog_id, bool remove_from_dialog_list, bool revoke,
                                            Promise<Unit> &&promise) {
  LOG(INFO) << "Receive deleteChatHistory request to delete all messages in " << dialog_id
            << ", remove_from_chat_list is " << remove_from_dialog_list << ", revoke is " << revoke;

  Dialog *d = get_dialog_force(dialog_id, "delete_dialog_history");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Chat info not found"));
  }

  if (is_dialog_sponsored(d)) {
    auto chat_source = sponsored_dialog_source_.get_chat_source_object();
    if (chat_source == nullptr || chat_source->get_id() != td_api::chatSourcePublicServiceAnnouncement::ID) {
      return promise.set_error(Status::Error(400, "Can't delete the chat"));
    }
    if (!remove_from_dialog_list) {
      return promise.set_error(
          Status::Error(400, "Can't delete only chat history without removing the chat from the chat list"));
    }

    removed_sponsored_dialog_id_ = dialog_id;
    remove_sponsored_dialog();

    td_->create_handler<HidePromoDataQuery>()->send(dialog_id);
    promise.set_value(Unit());
    return;
  }

  auto dialog_type = dialog_id.get_type();
  switch (dialog_type) {
    case DialogType::User:
    case DialogType::Chat:
      // ok
      break;
    case DialogType::Channel:
      if (is_broadcast_channel(dialog_id)) {
        return promise.set_error(Status::Error(400, "Can't delete chat history in a channel"));
      }
      if (td_->contacts_manager_->is_channel_public(dialog_id.get_channel_id())) {
        return promise.set_error(Status::Error(400, "Can't delete chat history in a public supergroup"));
      }
      break;
    case DialogType::SecretChat:
      // ok
      break;
    case DialogType::None:
    default:
      UNREACHABLE();
      break;
  }

  auto last_new_message_id = d->last_new_message_id;
  if (dialog_type != DialogType::SecretChat && last_new_message_id == MessageId()) {
    // TODO get dialog from the server and delete history from last message identifier
  }

  bool allow_error = d->messages == nullptr;
  auto old_order = d->order;

  delete_all_dialog_messages(d, remove_from_dialog_list, true);

  if (last_new_message_id.is_valid() && last_new_message_id == d->max_unavailable_message_id && !revoke &&
      !(old_order != DEFAULT_ORDER && remove_from_dialog_list)) {
    // history has already been cleared, nothing to do
    promise.set_value(Unit());
    return;
  }

  set_dialog_max_unavailable_message_id(dialog_id, last_new_message_id, false, "delete_dialog_history");

  delete_dialog_history_on_server(dialog_id, last_new_message_id, remove_from_dialog_list, revoke, allow_error, 0,
                                  std::move(promise));
}

class ToggleNoForwardsQuery final : public Td::ResultHandler {
  Promise<Unit> promise_;
  DialogId dialog_id_;

 public:
  explicit ToggleNoForwardsQuery(Promise<Unit> &&promise) : promise_(std::move(promise)) {
  }

  void send(DialogId dialog_id, bool has_protected_content) {
    dialog_id_ = dialog_id;
    auto input_peer = td_->messages_manager_->get_input_peer(dialog_id, AccessRights::Read);
    CHECK(input_peer != nullptr);
    send_query(G()->net_query_creator().create(
        telegram_api::messages_toggleNoForwards(std::move(input_peer), has_protected_content)));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_toggleNoForwards>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(INFO) << "Receive result for ToggleNoForwardsQuery: " << to_string(ptr);
    td_->updates_manager_->on_get_updates(std::move(ptr), std::move(promise_));
  }

  void on_error(Status status) final {
    if (status.message() == "CHAT_NOT_MODIFIED") {
      promise_.set_value(Unit());
      return;
    } else {
      td_->messages_manager_->on_get_dialog_error(dialog_id_, status, "ToggleNoForwardsQuery");
    }
    promise_.set_error(std::move(status));
  }
};

void MessagesManager::toggle_dialog_has_protected_content(DialogId dialog_id, bool has_protected_content,
                                                          Promise<Unit> &&promise) {
  if (!have_dialog_force(dialog_id, "toggle_dialog_has_protected_content")) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Can't access the chat"));
  }

  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::SecretChat:
      return promise.set_error(Status::Error(400, "Can't restrict saving content in the chat"));
    case DialogType::Chat: {
      auto chat_id = dialog_id.get_chat_id();
      auto status = td_->contacts_manager_->get_chat_status(chat_id);
      if (!status.is_creator()) {
        return promise.set_error(Status::Error(400, "Only owner can restrict saving content"));
      }
      break;
    }
    case DialogType::Channel: {
      auto status = td_->contacts_manager_->get_channel_status(dialog_id.get_channel_id());
      if (!status.is_creator()) {
        return promise.set_error(Status::Error(400, "Only owner can restrict saving content"));
      }
      break;
    }
    case DialogType::None:
    default:
      UNREACHABLE();
  }

  // TODO this can be wrong if there were previous toggle_dialog_has_protected_content requests
  if (get_dialog_has_protected_content(dialog_id) == has_protected_content) {
    return promise.set_value(Unit());
  }

  // TODO invoke after
  td_->create_handler<ToggleNoForwardsQuery>(std::move(promise))->send(dialog_id, has_protected_content);
}

void MessagesManager::read_all_dialog_mentions(DialogId dialog_id, Promise<Unit> &&promise) {
  Dialog *d = get_dialog_force(dialog_id, "read_all_dialog_mentions");
  if (d == nullptr) {
    return promise.set_error(Status::Error(400, "Chat not found"));
  }

  LOG(INFO) << "Receive readAllChatMentions request in " << dialog_id << " with " << d->unread_mention_count
            << " unread mentions";
  if (!have_input_peer(dialog_id, AccessRights::Read)) {
    return promise.set_error(Status::Error(400, "Chat is not accessible"));
  }
  if (dialog_id.get_type() == DialogType::SecretChat) {
    CHECK(d->unread_mention_count == 0);
    return promise.set_value(Unit());
  }

  if (d->last_new_message_id > d->last_read_all_mentions_message_id) {
    d->last_read_all_mentions_message_id = d->last_new_message_id;
    on_dialog_updated(dialog_id, "read_all_dialog_mentions");
  }

  vector<MessageId> message_ids;
  find_messages(d->messages.get(), message_ids, [](const Message *m) { return m->contains_unread_mention; });

  LOG(INFO) << "Found " << message_ids.size() << " messages with unread mentions in memory";
  bool is_update_sent = false;
  for (auto message_id : message_ids) {
    auto m = get_message(d, message_id);
    CHECK(m != nullptr);
    CHECK(m->contains_unread_mention);
    CHECK(m->message_id == message_id);
    CHECK(m->message_id.is_valid());
    remove_message_notification_id(d, m, true, false);  // should be called before contains_unread_mention is updated
    m->contains_unread_mention = false;

    send_closure(G()->td(), &Td::send_update,
                 make_tl_object<td_api::updateMessageMentionRead>(dialog_id.get(), m->message_id.get(), 0));
    is_update_sent = true;
    on_message_changed(d, m, true, "read_all_dialog_mentions");
  }

  if (d->unread_mention_count != 0) {
    set_dialog_unread_mention_count(d, 0);
    if (!is_update_sent) {
      send_update_chat_unread_mention_count(d);
    } else {
      LOG(INFO) << "Update unread mention message count in " << dialog_id << " to " << d->unread_mention_count;
      on_dialog_updated(dialog_id, "read_all_dialog_mentions");
    }
  }
  remove_message_dialog_notifications(d, MessageId::max(), true, "read_all_dialog_mentions");

  read_all_dialog_mentions_on_server(dialog_id, 0, std::move(promise));
}

void MessagesManager::on_get_peer_settings(DialogId dialog_id,
                                           tl_object_ptr<telegram_api::peerSettings> &&peer_settings,
                                           bool ignore_privacy_exception) {
  CHECK(peer_settings != nullptr);
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  if (dialog_id.get_type() == DialogType::User && !ignore_privacy_exception) {
    td_->contacts_manager_->on_update_user_need_phone_number_privacy_exception(dialog_id.get_user_id(),
                                                                               peer_settings->need_contacts_exception_);
  }

  Dialog *d = get_dialog_force(dialog_id, "on_get_peer_settings");
  if (d == nullptr) {
    return;
  }

  auto distance =
      (peer_settings->flags_ & telegram_api::peerSettings::GEO_DISTANCE_MASK) != 0 ? peer_settings->geo_distance_ : -1;
  if (distance < -1 || d->has_outgoing_messages) {
    distance = -1;
  }
  auto action_bar =
      DialogActionBar::create(peer_settings->report_spam_, peer_settings->add_contact_, peer_settings->block_contact_,
                              peer_settings->share_contact_, peer_settings->report_geo_, peer_settings->autoarchived_,
                              distance, peer_settings->invite_members_, peer_settings->request_chat_title_,
                              peer_settings->request_chat_broadcast_, peer_settings->request_chat_date_);

  fix_dialog_action_bar(d, action_bar.get());

  if (d->action_bar == action_bar) {
    if (!d->know_action_bar || d->need_repair_action_bar) {
      d->know_action_bar = true;
      d->need_repair_action_bar = false;
      on_dialog_updated(d->dialog_id, "on_get_peer_settings");
    }
    return;
  }

  d->know_action_bar = true;
  d->need_repair_action_bar = false;
  d->action_bar = std::move(action_bar);

  send_update_chat_action_bar(d);
}

void MessagesManager::on_secret_chat_ttl_changed(SecretChatId secret_chat_id, UserId user_id, MessageId message_id,
                                                 int32 date, int32 ttl, int64 random_id, Promise<> promise) {
  LOG(DEBUG) << "On TTL set in " << secret_chat_id << " to " << ttl;
  CHECK(secret_chat_id.is_valid());
  CHECK(user_id.is_valid());
  CHECK(message_id.is_valid());
  CHECK(date > 0);
  if (ttl < 0) {
    LOG(WARNING) << "Receive wrong TTL = " << ttl;
    promise.set_value(Unit());
    return;
  }

  auto pending_secret_message = make_unique<PendingSecretMessage>();
  pending_secret_message->success_promise = std::move(promise);
  MessageInfo &message_info = pending_secret_message->message_info;
  message_info.dialog_id = DialogId(secret_chat_id);
  message_info.message_id = message_id;
  message_info.sender_user_id = user_id;
  message_info.date = date;
  message_info.random_id = random_id;
  message_info.flags = MESSAGE_FLAG_HAS_FROM_ID;
  message_info.content = create_chat_set_ttl_message_content(ttl);

  Dialog *d = get_dialog_force(message_info.dialog_id, "on_secret_chat_ttl_changed");
  if (d == nullptr && have_dialog_info_force(message_info.dialog_id)) {
    force_create_dialog(message_info.dialog_id, "on_get_secret_message", true, true);
    d = get_dialog(message_info.dialog_id);
  }
  if (d == nullptr) {
    LOG(ERROR) << "Ignore secret message in unknown " << message_info.dialog_id;
    pending_secret_message->success_promise.set_error(Status::Error(500, "Chat not found"));
    return;
  }

  add_secret_message(std::move(pending_secret_message));
}

void MessagesManager::on_update_dialog_default_join_group_call_as_dialog_id(DialogId dialog_id,
                                                                            DialogId default_join_as_dialog_id,
                                                                            bool force) {
  auto d = get_dialog_force(dialog_id, "on_update_dialog_default_join_group_call_as_dialog_id");
  if (d == nullptr) {
    // nothing to do
    return;
  }

  if (!force && d->active_group_call_id.is_valid() &&
      td_->group_call_manager_->is_group_call_being_joined(d->active_group_call_id)) {
    LOG(INFO) << "Ignore default_join_as_dialog_id update in a being joined group call";
    return;
  }

  if (default_join_as_dialog_id.is_valid()) {
    if (default_join_as_dialog_id.get_type() != DialogType::User) {
      force_create_dialog(default_join_as_dialog_id, "on_update_dialog_default_join_group_call_as_dialog_id");
    } else if (!td_->contacts_manager_->have_user_force(default_join_as_dialog_id.get_user_id()) ||
               default_join_as_dialog_id != get_my_dialog_id()) {
      default_join_as_dialog_id = DialogId();
    }
  }

  if (d->default_join_group_call_as_dialog_id != default_join_as_dialog_id) {
    d->default_join_group_call_as_dialog_id = default_join_as_dialog_id;
    send_update_chat_video_chat(d);
  }
}

void MessagesManager::on_dialog_unmute(DialogId dialog_id) {
  if (td_->auth_manager_->is_bot()) {
    // just in case
    return;
  }

  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);

  if (d->notification_settings.use_default_mute_until) {
    return;
  }
  if (d->notification_settings.mute_until == 0) {
    return;
  }

  auto now = G()->unix_time();
  if (d->notification_settings.mute_until > now) {
    LOG(ERROR) << "Failed to unmute " << dialog_id << " in " << now << ", will be unmuted in "
               << d->notification_settings.mute_until;
    schedule_dialog_unmute(dialog_id, false, d->notification_settings.mute_until);
    return;
  }

  LOG(INFO) << "Unmute " << dialog_id;
  update_dialog_unmute_timeout(d, d->notification_settings.use_default_mute_until, d->notification_settings.mute_until,
                               false, 0);
  send_closure(G()->td(), &Td::send_update,
               make_tl_object<td_api::updateChatNotificationSettings>(
                   dialog_id.get(), get_chat_notification_settings_object(&d->notification_settings)));
  on_dialog_updated(dialog_id, "on_dialog_unmute");
}

void MessagesManager::send_update_message_content_impl(DialogId dialog_id, const Message *m, const char *source) const {
  CHECK(m != nullptr);
  if (!m->is_update_sent) {
    LOG(INFO) << "Skip updateMessageContent for " << m->message_id << " in " << dialog_id << " from " << source;
    return;
  }
  LOG(INFO) << "Send updateMessageContent for " << m->message_id << " in " << dialog_id << " from " << source;
  auto content_object = get_message_content_object(m->content.get(), td_, dialog_id, m->is_failed_to_send ? 0 : m->date,
                                                   m->is_content_secret, need_skip_bot_commands(dialog_id, m),
                                                   get_message_max_media_timestamp(m));
  send_closure(G()->td(), &Td::send_update,
               td_api::make_object<td_api::updateMessageContent>(dialog_id.get(), m->message_id.get(),
                                                                 std::move(content_object)));
}

void MessagesManager::repair_server_unread_count(DialogId dialog_id, int32 unread_count) {
  if (td_->auth_manager_->is_bot() || !have_input_peer(dialog_id, AccessRights::Read)) {
    return;
  }
  if (pending_read_history_timeout_.has_timeout(dialog_id.get())) {
    return;  // postpone until read history request is sent
  }

  LOG(INFO) << "Repair server unread count in " << dialog_id << " from " << unread_count;
  create_actor<SleepActor>("RepairServerUnreadCountSleepActor", 0.2,
                           PromiseCreator::lambda([actor_id = actor_id(this), dialog_id](Result<Unit> result) {
                             send_closure(actor_id, &MessagesManager::send_get_dialog_query, dialog_id, Promise<Unit>(),
                                          0, "repair_server_unread_count");
                           }))
      .release();
}

void MessagesManager::on_get_public_dialogs_search_result(const string &query,
                                                          vector<tl_object_ptr<telegram_api::Peer>> &&my_peers,
                                                          vector<tl_object_ptr<telegram_api::Peer>> &&peers) {
  auto it = search_public_dialogs_queries_.find(query);
  CHECK(it != search_public_dialogs_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  search_public_dialogs_queries_.erase(it);

  found_public_dialogs_[query] = get_peers_dialog_ids(std::move(peers));
  found_on_server_dialogs_[query] = get_peers_dialog_ids(std::move(my_peers));

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

void MessagesManager::send_update_new_chat(Dialog *d) {
  CHECK(d != nullptr);
  CHECK(d->messages == nullptr);
  auto chat_object = get_chat_object(d);
  bool has_action_bar = chat_object->action_bar_ != nullptr;
  bool has_theme = !chat_object->theme_name_.empty();
  d->last_sent_has_scheduled_messages = chat_object->has_scheduled_messages_;
  send_closure(G()->td(), &Td::send_update, make_tl_object<td_api::updateNewChat>(std::move(chat_object)));
  d->is_update_new_chat_sent = true;

  if (has_action_bar) {
    send_update_secret_chats_with_user_action_bar(d);
  }
  if (has_theme) {
    send_update_secret_chats_with_user_theme(d);
  }
}

int32 MessagesManager::get_pinned_dialogs_limit(DialogListId dialog_list_id) {
  if (dialog_list_id.is_filter()) {
    return DialogFilter::MAX_INCLUDED_FILTER_DIALOGS;
  }

  Slice key{"pinned_chat_count_max"};
  int32 default_limit = 5;
  if (!dialog_list_id.is_folder() || dialog_list_id.get_folder_id() != FolderId::main()) {
    key = Slice("pinned_archived_chat_count_max");
    default_limit = 100;
  }
  int32 limit = clamp(narrow_cast<int32>(G()->shared_config().get_option_integer(key)), 0, 1000);
  if (limit <= 0) {
    return default_limit;
  }
  return limit;
}

void MessagesManager::clear_active_dialog_actions(DialogId dialog_id) {
  LOG(DEBUG) << "Clear active dialog actions in " << dialog_id;
  auto actions_it = active_dialog_actions_.find(dialog_id);
  while (actions_it != active_dialog_actions_.end()) {
    CHECK(!actions_it->second.empty());
    on_dialog_action(dialog_id, actions_it->second[0].top_thread_message_id, actions_it->second[0].typing_dialog_id,
                     DialogAction(), 0);
    actions_it = active_dialog_actions_.find(dialog_id);
  }
}

void MessagesManager::on_read_history_finished(DialogId dialog_id, MessageId top_thread_message_id, uint64 generation) {
  auto d = get_dialog(dialog_id);
  CHECK(d != nullptr);
  auto it = d->read_history_log_event_ids.find(top_thread_message_id.get());
  if (it == d->read_history_log_event_ids.end()) {
    return;
  }
  delete_log_event(it->second, generation, "read history");
  if (it->second.log_event_id == 0) {
    d->read_history_log_event_ids.erase(it);
  }
}

bool MessagesManager::has_qts_messages(DialogId dialog_id) {
  switch (dialog_id.get_type()) {
    case DialogType::User:
    case DialogType::Chat:
      return G()->shared_config().get_option_integer("session_count") > 1;
    case DialogType::Channel:
    case DialogType::SecretChat:
      return false;
    case DialogType::None:
    default:
      UNREACHABLE();
      return false;
  }
}

void MessagesManager::on_update_message_view_count(FullMessageId full_message_id, int32 view_count) {
  if (view_count < 0) {
    LOG(ERROR) << "Receive " << view_count << " views in updateChannelMessageViews for " << full_message_id;
    return;
  }
  update_message_interaction_info(full_message_id, view_count, -1, false, nullptr);
}

int32 MessagesManager::get_message_schedule_date(const Message *m) {
  CHECK(m != nullptr);
  if (!m->message_id.is_scheduled()) {
    return 0;
  }
  if (m->edited_schedule_date != 0) {
    return m->edited_schedule_date;
  }
  return m->date;
}

bool MessagesManager::is_from_mention_notification_group(const Dialog *d, const Message *m) {
  return m->contains_mention && !m->is_mention_notification_disabled;
}
}  // namespace td
