//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/ContactsManager.h"



#include "td/telegram/Dependencies.h"
#include "td/telegram/DialogInviteLink.h"
#include "td/telegram/DialogLocation.h"

#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/FolderId.h"
#include "td/telegram/Global.h"
#include "td/telegram/GroupCallManager.h"

#include "td/telegram/InputGroupCallId.h"

#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/logevent/LogEventHelper.h"
#include "td/telegram/MessageSender.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/MessageTtl.h"
#include "td/telegram/MinChannel.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/NetQuery.h"

#include "td/telegram/PasswordManager.h"
#include "td/telegram/Photo.h"
#include "td/telegram/Photo.hpp"
#include "td/telegram/SecretChatLayer.h"

#include "td/telegram/ServerMessageId.h"
#include "td/telegram/StickerSetId.hpp"

#include "td/telegram/Td.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.hpp"

#include "td/telegram/Version.h"

#include "td/db/binlog/BinlogEvent.h"
#include "td/db/binlog/BinlogHelper.h"
#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/buffer.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/misc.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"

#include <algorithm>




namespace td {
}  // namespace td
namespace td {

template <class StorerT>
void ContactsManager::User::store(StorerT &storer) const {
  using td::store;
  bool has_last_name = !last_name.empty();
  bool has_username = !username.empty();
  bool has_photo = photo.small_file_id.is_valid();
  bool has_language_code = !language_code.empty();
  bool have_access_hash = access_hash != -1;
  bool has_cache_version = cache_version != 0;
  bool has_is_contact = true;
  bool has_restriction_reasons = !restriction_reasons.empty();
  BEGIN_STORE_FLAGS();
  STORE_FLAG(is_received);
  STORE_FLAG(is_verified);
  STORE_FLAG(is_deleted);
  STORE_FLAG(is_bot);
  STORE_FLAG(can_join_groups);
  STORE_FLAG(can_read_all_group_messages);
  STORE_FLAG(is_inline_bot);
  STORE_FLAG(need_location_bot);
  STORE_FLAG(has_last_name);
  STORE_FLAG(has_username);
  STORE_FLAG(has_photo);
  STORE_FLAG(false);  // legacy is_restricted
  STORE_FLAG(has_language_code);
  STORE_FLAG(have_access_hash);
  STORE_FLAG(is_support);
  STORE_FLAG(is_min_access_hash);
  STORE_FLAG(is_scam);
  STORE_FLAG(has_cache_version);
  STORE_FLAG(has_is_contact);
  STORE_FLAG(is_contact);
  STORE_FLAG(is_mutual_contact);
  STORE_FLAG(has_restriction_reasons);
  STORE_FLAG(need_apply_min_photo);
  STORE_FLAG(is_fake);
  END_STORE_FLAGS();
  store(first_name, storer);
  if (has_last_name) {
    store(last_name, storer);
  }
  if (has_username) {
    store(username, storer);
  }
  store(phone_number, storer);
  if (have_access_hash) {
    store(access_hash, storer);
  }
  if (has_photo) {
    store(photo, storer);
  }
  store(was_online, storer);
  if (has_restriction_reasons) {
    store(restriction_reasons, storer);
  }
  if (is_inline_bot) {
    store(inline_query_placeholder, storer);
  }
  if (is_bot) {
    store(bot_info_version, storer);
  }
  if (has_language_code) {
    store(language_code, storer);
  }
  if (has_cache_version) {
    store(cache_version, storer);
  }
}

template <class ParserT>
void ContactsManager::User::parse(ParserT &parser) {
  using td::parse;
  bool has_last_name;
  bool has_username;
  bool has_photo;
  bool legacy_is_restricted;
  bool has_language_code;
  bool have_access_hash;
  bool has_cache_version;
  bool has_is_contact;
  bool has_restriction_reasons;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(is_received);
  PARSE_FLAG(is_verified);
  PARSE_FLAG(is_deleted);
  PARSE_FLAG(is_bot);
  PARSE_FLAG(can_join_groups);
  PARSE_FLAG(can_read_all_group_messages);
  PARSE_FLAG(is_inline_bot);
  PARSE_FLAG(need_location_bot);
  PARSE_FLAG(has_last_name);
  PARSE_FLAG(has_username);
  PARSE_FLAG(has_photo);
  PARSE_FLAG(legacy_is_restricted);
  PARSE_FLAG(has_language_code);
  PARSE_FLAG(have_access_hash);
  PARSE_FLAG(is_support);
  PARSE_FLAG(is_min_access_hash);
  PARSE_FLAG(is_scam);
  PARSE_FLAG(has_cache_version);
  PARSE_FLAG(has_is_contact);
  PARSE_FLAG(is_contact);
  PARSE_FLAG(is_mutual_contact);
  PARSE_FLAG(has_restriction_reasons);
  PARSE_FLAG(need_apply_min_photo);
  PARSE_FLAG(is_fake);
  END_PARSE_FLAGS();
  parse(first_name, parser);
  if (has_last_name) {
    parse(last_name, parser);
  }
  if (has_username) {
    parse(username, parser);
  }
  parse(phone_number, parser);
  if (parser.version() < static_cast<int32>(Version::FixMinUsers)) {
    have_access_hash = is_received;
  }
  if (have_access_hash) {
    parse(access_hash, parser);
  } else {
    is_min_access_hash = true;
  }
  if (has_photo) {
    parse(photo, parser);
  }
  if (!has_is_contact) {
    // enum class LinkState : uint8 { Unknown, None, KnowsPhoneNumber, Contact };

    uint32 link_state_inbound;
    uint32 link_state_outbound;
    parse(link_state_inbound, parser);
    parse(link_state_outbound, parser);

    is_contact = link_state_outbound == 3;
    is_mutual_contact = is_contact && link_state_inbound == 3;
  }
  parse(was_online, parser);
  if (legacy_is_restricted) {
    string restriction_reason;
    parse(restriction_reason, parser);
    restriction_reasons = get_restriction_reasons(restriction_reason);
  } else if (has_restriction_reasons) {
    parse(restriction_reasons, parser);
  }
  if (is_inline_bot) {
    parse(inline_query_placeholder, parser);
  }
  if (is_bot) {
    parse(bot_info_version, parser);
  }
  if (has_language_code) {
    parse(language_code, parser);
  }
  if (has_cache_version) {
    parse(cache_version, parser);
  }

  if (!check_utf8(first_name)) {
    LOG(ERROR) << "Have invalid first name \"" << first_name << '"';
    first_name.clear();
    cache_version = 0;
  }
  if (!check_utf8(last_name)) {
    LOG(ERROR) << "Have invalid last name \"" << last_name << '"';
    last_name.clear();
    cache_version = 0;
  }
  if (!check_utf8(username)) {
    LOG(ERROR) << "Have invalid username \"" << username << '"';
    username.clear();
    cache_version = 0;
  }

  if (first_name.empty() && last_name.empty()) {
    first_name = phone_number;
  }
  if (!is_contact && is_mutual_contact) {
    LOG(ERROR) << "Have invalid flag is_mutual_contact";
    is_mutual_contact = false;
    cache_version = 0;
  }
}

template <class StorerT>
void ContactsManager::UserFull::store(StorerT &storer) const {
  using td::store;
  bool has_about = !about.empty();
  bool has_photo = !photo.is_empty();
  bool has_description = !description.empty();
  bool has_commands = !commands.empty();
  bool has_private_forward_name = !private_forward_name.empty();
  BEGIN_STORE_FLAGS();
  STORE_FLAG(has_about);
  STORE_FLAG(is_blocked);
  STORE_FLAG(can_be_called);
  STORE_FLAG(has_private_calls);
  STORE_FLAG(can_pin_messages);
  STORE_FLAG(need_phone_number_privacy_exception);
  STORE_FLAG(has_photo);
  STORE_FLAG(supports_video_calls);
  STORE_FLAG(has_description);
  STORE_FLAG(has_commands);
  STORE_FLAG(has_private_forward_name);
  END_STORE_FLAGS();
  if (has_about) {
    store(about, storer);
  }
  store(common_chat_count, storer);
  store_time(expires_at, storer);
  if (has_photo) {
    store(photo, storer);
  }
  if (has_description) {
    store(description, storer);
  }
  if (has_commands) {
    store(commands, storer);
  }
  if (has_private_forward_name) {
    store(private_forward_name, storer);
  }
}

template <class ParserT>
void ContactsManager::UserFull::parse(ParserT &parser) {
  using td::parse;
  bool has_about;
  bool has_photo;
  bool has_description;
  bool has_commands;
  bool has_private_forward_name;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(has_about);
  PARSE_FLAG(is_blocked);
  PARSE_FLAG(can_be_called);
  PARSE_FLAG(has_private_calls);
  PARSE_FLAG(can_pin_messages);
  PARSE_FLAG(need_phone_number_privacy_exception);
  PARSE_FLAG(has_photo);
  PARSE_FLAG(supports_video_calls);
  PARSE_FLAG(has_description);
  PARSE_FLAG(has_commands);
  PARSE_FLAG(has_private_forward_name);
  END_PARSE_FLAGS();
  if (has_about) {
    parse(about, parser);
  }
  parse(common_chat_count, parser);
  parse_time(expires_at, parser);
  if (has_photo) {
    parse(photo, parser);
  }
  if (has_description) {
    parse(description, parser);
  }
  if (has_commands) {
    parse(commands, parser);
  }
  if (has_private_forward_name) {
    parse(private_forward_name, parser);
  }
}

template <class StorerT>
void ContactsManager::Chat::store(StorerT &storer) const {
  using td::store;
  bool has_photo = photo.small_file_id.is_valid();
  bool use_new_rights = true;
  bool has_default_permissions_version = default_permissions_version != -1;
  bool has_pinned_message_version = pinned_message_version != -1;
  bool has_cache_version = cache_version != 0;
  BEGIN_STORE_FLAGS();
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(is_active);
  STORE_FLAG(has_photo);
  STORE_FLAG(use_new_rights);
  STORE_FLAG(has_default_permissions_version);
  STORE_FLAG(has_pinned_message_version);
  STORE_FLAG(has_cache_version);
  STORE_FLAG(noforwards);
  END_STORE_FLAGS();

  store(title, storer);
  if (has_photo) {
    store(photo, storer);
  }
  store(participant_count, storer);
  store(date, storer);
  store(migrated_to_channel_id, storer);
  store(version, storer);
  store(status, storer);
  store(default_permissions, storer);
  if (has_default_permissions_version) {
    store(default_permissions_version, storer);
  }
  if (has_pinned_message_version) {
    store(pinned_message_version, storer);
  }
  if (has_cache_version) {
    store(cache_version, storer);
  }
}

template <class ParserT>
void ContactsManager::Chat::parse(ParserT &parser) {
  using td::parse;
  bool has_photo;
  bool left;
  bool kicked;
  bool is_creator;
  bool is_administrator;
  bool everyone_is_administrator;
  bool can_edit;
  bool use_new_rights;
  bool has_default_permissions_version;
  bool has_pinned_message_version;
  bool has_cache_version;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(left);
  PARSE_FLAG(kicked);
  PARSE_FLAG(is_creator);
  PARSE_FLAG(is_administrator);
  PARSE_FLAG(everyone_is_administrator);
  PARSE_FLAG(can_edit);
  PARSE_FLAG(is_active);
  PARSE_FLAG(has_photo);
  PARSE_FLAG(use_new_rights);
  PARSE_FLAG(has_default_permissions_version);
  PARSE_FLAG(has_pinned_message_version);
  PARSE_FLAG(has_cache_version);
  PARSE_FLAG(noforwards);
  END_PARSE_FLAGS();

  parse(title, parser);
  if (has_photo) {
    parse(photo, parser);
  }
  parse(participant_count, parser);
  parse(date, parser);
  parse(migrated_to_channel_id, parser);
  parse(version, parser);
  if (use_new_rights) {
    parse(status, parser);
    parse(default_permissions, parser);
  } else {
    if (can_edit != (is_creator || is_administrator || everyone_is_administrator)) {
      LOG(ERROR) << "Have wrong can_edit flag";
    }

    if (kicked || !is_active) {
      status = DialogParticipantStatus::Banned(0);
    } else if (left) {
      status = DialogParticipantStatus::Left();
    } else if (is_creator) {
      status = DialogParticipantStatus::Creator(true, false, string());
    } else if (is_administrator && !everyone_is_administrator) {
      status = DialogParticipantStatus::GroupAdministrator(false);
    } else {
      status = DialogParticipantStatus::Member();
    }
    default_permissions = RestrictedRights(true, true, true, true, true, true, true, true, everyone_is_administrator,
                                           everyone_is_administrator, everyone_is_administrator);
  }
  if (has_default_permissions_version) {
    parse(default_permissions_version, parser);
  }
  if (has_pinned_message_version) {
    parse(pinned_message_version, parser);
  }
  if (has_cache_version) {
    parse(cache_version, parser);
  }

  if (!check_utf8(title)) {
    LOG(ERROR) << "Have invalid title \"" << title << '"';
    title.clear();
    cache_version = 0;
  }

  if (status.is_administrator() && !status.is_creator()) {
    status = DialogParticipantStatus::GroupAdministrator(false);
  }
}

template <class StorerT>
void ContactsManager::ChatFull::store(StorerT &storer) const {
  using td::store;
  bool has_description = !description.empty();
  bool has_legacy_invite_link = false;
  bool has_photo = !photo.is_empty();
  bool has_invite_link = invite_link.is_valid();
  bool has_bot_commands = !bot_commands.empty();
  BEGIN_STORE_FLAGS();
  STORE_FLAG(has_description);
  STORE_FLAG(has_legacy_invite_link);
  STORE_FLAG(can_set_username);
  STORE_FLAG(has_photo);
  STORE_FLAG(has_invite_link);
  STORE_FLAG(has_bot_commands);
  END_STORE_FLAGS();
  store(version, storer);
  store(creator_user_id, storer);
  store(participants, storer);
  if (has_description) {
    store(description, storer);
  }
  if (has_photo) {
    store(photo, storer);
  }
  if (has_invite_link) {
    store(invite_link, storer);
  }
  if (has_bot_commands) {
    store(bot_commands, storer);
  }
}

template <class ParserT>
void ContactsManager::ChatFull::parse(ParserT &parser) {
  using td::parse;
  bool has_description;
  bool legacy_has_invite_link;
  bool has_photo;
  bool has_invite_link;
  bool has_bot_commands;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(has_description);
  PARSE_FLAG(legacy_has_invite_link);
  PARSE_FLAG(can_set_username);
  PARSE_FLAG(has_photo);
  PARSE_FLAG(has_invite_link);
  PARSE_FLAG(has_bot_commands);
  END_PARSE_FLAGS();
  parse(version, parser);
  parse(creator_user_id, parser);
  parse(participants, parser);
  if (has_description) {
    parse(description, parser);
  }
  if (legacy_has_invite_link) {
    string legacy_invite_link;
    parse(legacy_invite_link, parser);
  }
  if (has_photo) {
    parse(photo, parser);
  }
  if (has_invite_link) {
    parse(invite_link, parser);
  }
  if (has_bot_commands) {
    parse(bot_commands, parser);
  }
}

template <class StorerT>
void ContactsManager::Channel::store(StorerT &storer) const {
  using td::store;
  bool has_photo = photo.small_file_id.is_valid();
  bool has_username = !username.empty();
  bool use_new_rights = true;
  bool has_participant_count = participant_count != 0;
  bool have_default_permissions = true;
  bool has_cache_version = cache_version != 0;
  bool has_restriction_reasons = !restriction_reasons.empty();
  bool legacy_has_active_group_call = false;
  BEGIN_STORE_FLAGS();
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(false);
  STORE_FLAG(sign_messages);
  STORE_FLAG(false);
  STORE_FLAG(false);  // 5
  STORE_FLAG(false);
  STORE_FLAG(is_megagroup);
  STORE_FLAG(is_verified);
  STORE_FLAG(has_photo);
  STORE_FLAG(has_username);  // 10
  STORE_FLAG(false);
  STORE_FLAG(use_new_rights);
  STORE_FLAG(has_participant_count);
  STORE_FLAG(have_default_permissions);
  STORE_FLAG(is_scam);  // 15
  STORE_FLAG(has_cache_version);
  STORE_FLAG(has_linked_channel);
  STORE_FLAG(has_location);
  STORE_FLAG(is_slow_mode_enabled);
  STORE_FLAG(has_restriction_reasons);  // 20
  STORE_FLAG(legacy_has_active_group_call);
  STORE_FLAG(is_fake);
  STORE_FLAG(is_gigagroup);
  STORE_FLAG(noforwards);
  END_STORE_FLAGS();

  store(status, storer);
  store(access_hash, storer);
  store(title, storer);
  if (has_photo) {
    store(photo, storer);
  }
  if (has_username) {
    store(username, storer);
  }
  store(date, storer);
  if (has_restriction_reasons) {
    store(restriction_reasons, storer);
  }
  if (has_participant_count) {
    store(participant_count, storer);
  }
  if (is_megagroup) {
    store(default_permissions, storer);
  }
  if (has_cache_version) {
    store(cache_version, storer);
  }
}

template <class ParserT>
void ContactsManager::Channel::parse(ParserT &parser) {
  using td::parse;
  bool has_photo;
  bool has_username;
  bool legacy_is_restricted;
  bool left;
  bool kicked;
  bool is_creator;
  bool can_edit;
  bool can_moderate;
  bool anyone_can_invite;
  bool use_new_rights;
  bool has_participant_count;
  bool have_default_permissions;
  bool has_cache_version;
  bool has_restriction_reasons;
  bool legacy_has_active_group_call;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(left);
  PARSE_FLAG(kicked);
  PARSE_FLAG(anyone_can_invite);
  PARSE_FLAG(sign_messages);
  PARSE_FLAG(is_creator);
  PARSE_FLAG(can_edit);
  PARSE_FLAG(can_moderate);
  PARSE_FLAG(is_megagroup);
  PARSE_FLAG(is_verified);
  PARSE_FLAG(has_photo);
  PARSE_FLAG(has_username);
  PARSE_FLAG(legacy_is_restricted);
  PARSE_FLAG(use_new_rights);
  PARSE_FLAG(has_participant_count);
  PARSE_FLAG(have_default_permissions);
  PARSE_FLAG(is_scam);
  PARSE_FLAG(has_cache_version);
  PARSE_FLAG(has_linked_channel);
  PARSE_FLAG(has_location);
  PARSE_FLAG(is_slow_mode_enabled);
  PARSE_FLAG(has_restriction_reasons);
  PARSE_FLAG(legacy_has_active_group_call);
  PARSE_FLAG(is_fake);
  PARSE_FLAG(is_gigagroup);
  PARSE_FLAG(noforwards);
  END_PARSE_FLAGS();

  if (use_new_rights) {
    parse(status, parser);
  } else {
    if (kicked) {
      status = DialogParticipantStatus::Banned(0);
    } else if (left) {
      status = DialogParticipantStatus::Left();
    } else if (is_creator) {
      status = DialogParticipantStatus::Creator(true, false, string());
    } else if (can_edit || can_moderate) {
      status = DialogParticipantStatus::ChannelAdministrator(false, is_megagroup);
    } else {
      status = DialogParticipantStatus::Member();
    }
  }
  parse(access_hash, parser);
  parse(title, parser);
  if (has_photo) {
    parse(photo, parser);
  }
  if (has_username) {
    parse(username, parser);
  }
  parse(date, parser);
  if (legacy_is_restricted) {
    string restriction_reason;
    parse(restriction_reason, parser);
    restriction_reasons = get_restriction_reasons(restriction_reason);
  } else if (has_restriction_reasons) {
    parse(restriction_reasons, parser);
  }
  if (has_participant_count) {
    parse(participant_count, parser);
  }
  if (is_megagroup) {
    if (have_default_permissions) {
      parse(default_permissions, parser);
    } else {
      default_permissions =
          RestrictedRights(true, true, true, true, true, true, true, true, false, anyone_can_invite, false);
    }
  }
  if (has_cache_version) {
    parse(cache_version, parser);
  }

  if (!check_utf8(title)) {
    LOG(ERROR) << "Have invalid title \"" << title << '"';
    title.clear();
    cache_version = 0;
  }
  if (!check_utf8(username)) {
    LOG(ERROR) << "Have invalid username \"" << username << '"';
    username.clear();
    cache_version = 0;
  }
  if (legacy_has_active_group_call) {
    cache_version = 0;
  }
}

template <class StorerT>
void ContactsManager::ChannelFull::store(StorerT &storer) const {
  using td::store;
  bool has_description = !description.empty();
  bool has_administrator_count = administrator_count != 0;
  bool has_restricted_count = restricted_count != 0;
  bool has_banned_count = banned_count != 0;
  bool legacy_has_invite_link = false;
  bool has_sticker_set = sticker_set_id.is_valid();
  bool has_linked_channel_id = linked_channel_id.is_valid();
  bool has_migrated_from_max_message_id = migrated_from_max_message_id.is_valid();
  bool has_migrated_from_chat_id = migrated_from_chat_id.is_valid();
  bool has_location = !location.empty();
  bool has_bot_user_ids = !bot_user_ids.empty();
  bool is_slow_mode_enabled = slow_mode_delay != 0;
  bool is_slow_mode_delay_active = slow_mode_next_send_date != 0;
  bool has_stats_dc_id = stats_dc_id.is_exact();
  bool has_photo = !photo.is_empty();
  bool legacy_has_active_group_call_id = false;
  bool has_invite_link = invite_link.is_valid();
  bool has_bot_commands = !bot_commands.empty();
  BEGIN_STORE_FLAGS();
  STORE_FLAG(has_description);
  STORE_FLAG(has_administrator_count);
  STORE_FLAG(has_restricted_count);
  STORE_FLAG(has_banned_count);
  STORE_FLAG(legacy_has_invite_link);
  STORE_FLAG(has_sticker_set);
  STORE_FLAG(has_linked_channel_id);
  STORE_FLAG(has_migrated_from_max_message_id);
  STORE_FLAG(has_migrated_from_chat_id);
  STORE_FLAG(can_get_participants);
  STORE_FLAG(can_set_username);
  STORE_FLAG(can_set_sticker_set);
  STORE_FLAG(false);  // legacy_can_view_statistics
  STORE_FLAG(is_all_history_available);
  STORE_FLAG(can_set_location);
  STORE_FLAG(has_location);
  STORE_FLAG(has_bot_user_ids);
  STORE_FLAG(is_slow_mode_enabled);
  STORE_FLAG(is_slow_mode_delay_active);
  STORE_FLAG(has_stats_dc_id);
  STORE_FLAG(has_photo);
  STORE_FLAG(is_can_view_statistics_inited);
  STORE_FLAG(can_view_statistics);
  STORE_FLAG(legacy_has_active_group_call_id);
  STORE_FLAG(has_invite_link);
  STORE_FLAG(has_bot_commands);
  END_STORE_FLAGS();
  if (has_description) {
    store(description, storer);
  }
  store(participant_count, storer);
  if (has_administrator_count) {
    store(administrator_count, storer);
  }
  if (has_restricted_count) {
    store(restricted_count, storer);
  }
  if (has_banned_count) {
    store(banned_count, storer);
  }
  if (has_sticker_set) {
    store(sticker_set_id, storer);
  }
  if (has_linked_channel_id) {
    store(linked_channel_id, storer);
  }
  if (has_location) {
    store(location, storer);
  }
  if (has_bot_user_ids) {
    store(bot_user_ids, storer);
  }
  if (has_migrated_from_max_message_id) {
    store(migrated_from_max_message_id, storer);
  }
  if (has_migrated_from_chat_id) {
    store(migrated_from_chat_id, storer);
  }
  if (is_slow_mode_enabled) {
    store(slow_mode_delay, storer);
  }
  if (is_slow_mode_delay_active) {
    store(slow_mode_next_send_date, storer);
  }
  store_time(expires_at, storer);
  if (has_stats_dc_id) {
    store(stats_dc_id.get_raw_id(), storer);
  }
  if (has_photo) {
    store(photo, storer);
  }
  if (has_invite_link) {
    store(invite_link, storer);
  }
  if (has_bot_commands) {
    store(bot_commands, storer);
  }
}

template <class ParserT>
void ContactsManager::ChannelFull::parse(ParserT &parser) {
  using td::parse;
  bool has_description;
  bool has_administrator_count;
  bool has_restricted_count;
  bool has_banned_count;
  bool legacy_has_invite_link;
  bool has_sticker_set;
  bool has_linked_channel_id;
  bool has_migrated_from_max_message_id;
  bool has_migrated_from_chat_id;
  bool legacy_can_view_statistics;
  bool has_location;
  bool has_bot_user_ids;
  bool is_slow_mode_enabled;
  bool is_slow_mode_delay_active;
  bool has_stats_dc_id;
  bool has_photo;
  bool legacy_has_active_group_call_id;
  bool has_invite_link;
  bool has_bot_commands;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(has_description);
  PARSE_FLAG(has_administrator_count);
  PARSE_FLAG(has_restricted_count);
  PARSE_FLAG(has_banned_count);
  PARSE_FLAG(legacy_has_invite_link);
  PARSE_FLAG(has_sticker_set);
  PARSE_FLAG(has_linked_channel_id);
  PARSE_FLAG(has_migrated_from_max_message_id);
  PARSE_FLAG(has_migrated_from_chat_id);
  PARSE_FLAG(can_get_participants);
  PARSE_FLAG(can_set_username);
  PARSE_FLAG(can_set_sticker_set);
  PARSE_FLAG(legacy_can_view_statistics);
  PARSE_FLAG(is_all_history_available);
  PARSE_FLAG(can_set_location);
  PARSE_FLAG(has_location);
  PARSE_FLAG(has_bot_user_ids);
  PARSE_FLAG(is_slow_mode_enabled);
  PARSE_FLAG(is_slow_mode_delay_active);
  PARSE_FLAG(has_stats_dc_id);
  PARSE_FLAG(has_photo);
  PARSE_FLAG(is_can_view_statistics_inited);
  PARSE_FLAG(can_view_statistics);
  PARSE_FLAG(legacy_has_active_group_call_id);
  PARSE_FLAG(has_invite_link);
  PARSE_FLAG(has_bot_commands);
  END_PARSE_FLAGS();
  if (has_description) {
    parse(description, parser);
  }
  parse(participant_count, parser);
  if (has_administrator_count) {
    parse(administrator_count, parser);
  }
  if (has_restricted_count) {
    parse(restricted_count, parser);
  }
  if (has_banned_count) {
    parse(banned_count, parser);
  }
  if (legacy_has_invite_link) {
    string legacy_invite_link;
    parse(legacy_invite_link, parser);
  }
  if (has_sticker_set) {
    parse(sticker_set_id, parser);
  }
  if (has_linked_channel_id) {
    parse(linked_channel_id, parser);
  }
  if (has_location) {
    parse(location, parser);
  }
  if (has_bot_user_ids) {
    parse(bot_user_ids, parser);
  }
  if (has_migrated_from_max_message_id) {
    parse(migrated_from_max_message_id, parser);
  }
  if (has_migrated_from_chat_id) {
    parse(migrated_from_chat_id, parser);
  }
  if (is_slow_mode_enabled) {
    parse(slow_mode_delay, parser);
  }
  if (is_slow_mode_delay_active) {
    parse(slow_mode_next_send_date, parser);
  }
  parse_time(expires_at, parser);
  if (has_stats_dc_id) {
    stats_dc_id = DcId::create(parser.fetch_int());
  }
  if (has_photo) {
    parse(photo, parser);
  }
  if (legacy_has_active_group_call_id) {
    InputGroupCallId input_group_call_id;
    parse(input_group_call_id, parser);
  }
  if (has_invite_link) {
    parse(invite_link, parser);
  }
  if (has_bot_commands) {
    parse(bot_commands, parser);
  }

  if (legacy_can_view_statistics) {
    LOG(DEBUG) << "Ignore legacy can view statistics flag";
  }
  if (!is_can_view_statistics_inited) {
    can_view_statistics = stats_dc_id.is_exact();
  }
}

template <class StorerT>
void ContactsManager::SecretChat::store(StorerT &storer) const {
  using td::store;
  bool has_layer = layer > static_cast<int32>(SecretChatLayer::Default);
  bool has_initial_folder_id = initial_folder_id != FolderId();
  BEGIN_STORE_FLAGS();
  STORE_FLAG(is_outbound);
  STORE_FLAG(has_layer);
  STORE_FLAG(has_initial_folder_id);
  END_STORE_FLAGS();

  store(access_hash, storer);
  store(user_id, storer);
  store(state, storer);
  store(ttl, storer);
  store(date, storer);
  store(key_hash, storer);
  if (has_layer) {
    store(layer, storer);
  }
  if (has_initial_folder_id) {
    store(initial_folder_id, storer);
  }
}

template <class ParserT>
void ContactsManager::SecretChat::parse(ParserT &parser) {
  using td::parse;
  bool has_layer;
  bool has_initial_folder_id;
  BEGIN_PARSE_FLAGS();
  PARSE_FLAG(is_outbound);
  PARSE_FLAG(has_layer);
  PARSE_FLAG(has_initial_folder_id);
  END_PARSE_FLAGS();

  if (parser.version() >= static_cast<int32>(Version::AddAccessHashToSecretChat)) {
    parse(access_hash, parser);
  }
  parse(user_id, parser);
  parse(state, parser);
  parse(ttl, parser);
  parse(date, parser);
  if (parser.version() >= static_cast<int32>(Version::AddKeyHashToSecretChat)) {
    parse(key_hash, parser);
  }
  if (has_layer) {
    parse(layer, parser);
  } else {
    layer = static_cast<int32>(SecretChatLayer::Default);
  }
  if (has_initial_folder_id) {
    parse(initial_folder_id, parser);
  }
}

void ContactsManager::on_load_imported_contacts_from_database(string value) {
  if (G()->close_flag()) {
    return;
  }

  CHECK(!are_imported_contacts_loaded_);
  if (need_clear_imported_contacts_) {
    need_clear_imported_contacts_ = false;
    value.clear();
  }
  if (value.empty()) {
    CHECK(all_imported_contacts_.empty());
  } else {
    log_event_parse(all_imported_contacts_, value).ensure();
    LOG(INFO) << "Successfully loaded " << all_imported_contacts_.size() << " imported contacts from database";
  }

  load_imported_contact_users_multipromise_.add_promise(
      PromiseCreator::lambda([actor_id = actor_id(this)](Result<Unit> result) {
        if (result.is_ok()) {
          send_closure_later(actor_id, &ContactsManager::on_load_imported_contacts_finished);
        }
      }));

  auto lock_promise = load_imported_contact_users_multipromise_.get_promise();

  for (const auto &contact : all_imported_contacts_) {
    auto user_id = contact.get_user_id();
    if (user_id.is_valid()) {
      get_user(user_id, 3, load_imported_contact_users_multipromise_.get_promise());
    }
  }

  lock_promise.set_value(Unit());
}

void ContactsManager::on_import_contacts_finished(int64 random_id, vector<UserId> imported_contact_user_ids,
                                                  vector<int32> unimported_contact_invites) {
  LOG(INFO) << "Contacts import with random_id " << random_id
            << " has finished: " << format::as_array(imported_contact_user_ids);
  if (random_id == 0) {
    // import from change_imported_contacts
    all_imported_contacts_ = std::move(next_all_imported_contacts_);
    next_all_imported_contacts_.clear();

    auto result_size = imported_contacts_unique_id_.size();
    auto unique_size = all_imported_contacts_.size();
    auto add_size = imported_contacts_pos_.size();

    imported_contact_user_ids_.resize(result_size);
    unimported_contact_invites_.resize(result_size);

    CHECK(imported_contact_user_ids.size() == add_size);
    CHECK(unimported_contact_invites.size() == add_size);
    CHECK(imported_contacts_unique_id_.size() == result_size);

    std::unordered_map<size_t, int32> unique_id_to_unimported_contact_invites;
    for (size_t i = 0; i < add_size; i++) {
      auto unique_id = imported_contacts_pos_[i];
      get_user_id_object(imported_contact_user_ids[i], "on_import_contacts_finished");  // to ensure updateUser
      all_imported_contacts_[unique_id].set_user_id(imported_contact_user_ids[i]);
      unique_id_to_unimported_contact_invites[unique_id] = unimported_contact_invites[i];
    }

    if (G()->parameters().use_chat_info_db) {
      G()->td_db()->get_binlog()->force_sync(PromiseCreator::lambda(
          [log_event = log_event_store(all_imported_contacts_).as_slice().str()](Result<> result) mutable {
            if (result.is_ok()) {
              LOG(INFO) << "Save imported contacts to database";
              G()->td_db()->get_sqlite_pmc()->set("user_imported_contacts", std::move(log_event), Auto());
            }
          }));
    }

    for (size_t i = 0; i < result_size; i++) {
      auto unique_id = imported_contacts_unique_id_[i];
      CHECK(unique_id < unique_size);
      imported_contact_user_ids_[i] = all_imported_contacts_[unique_id].get_user_id();
      auto it = unique_id_to_unimported_contact_invites.find(unique_id);
      if (it == unique_id_to_unimported_contact_invites.end()) {
        unimported_contact_invites_[i] = 0;
      } else {
        unimported_contact_invites_[i] = it->second;
      }
    }
    return;
  }

  auto it = imported_contacts_.find(random_id);
  CHECK(it != imported_contacts_.end());
  CHECK(it->second.first.empty());
  CHECK(it->second.second.empty());
  imported_contacts_[random_id] = {std::move(imported_contact_user_ids), std::move(unimported_contact_invites)};
}

void ContactsManager::save_contacts_to_database() {
  if (!G()->parameters().use_chat_info_db || !are_contacts_loaded_) {
    return;
  }

  LOG(INFO) << "Schedule save contacts to database";
  vector<UserId> user_ids =
      transform(contacts_hints_.search_empty(100000).second, [](int64 key) { return UserId(key); });

  G()->td_db()->get_binlog_pmc()->set("saved_contact_count", to_string(saved_contact_count_));
  G()->td_db()->get_binlog()->force_sync(PromiseCreator::lambda([user_ids = std::move(user_ids)](Result<> result) {
    if (result.is_ok()) {
      LOG(INFO) << "Save contacts to database";
      G()->td_db()->get_sqlite_pmc()->set(
          "user_contacts", log_event_store(user_ids).as_slice().str(), PromiseCreator::lambda([](Result<> result) {
            if (result.is_ok()) {
              send_closure(G()->contacts_manager(), &ContactsManager::save_next_contacts_sync_date);
            }
          }));
    }
  }));
}

void ContactsManager::on_load_contacts_from_database(string value) {
  if (G()->close_flag()) {
    return;
  }
  if (value.empty()) {
    reload_contacts(true);
    return;
  }

  vector<UserId> user_ids;
  log_event_parse(user_ids, value).ensure();

  LOG(INFO) << "Successfully loaded " << user_ids.size() << " contacts from database";

  load_contact_users_multipromise_.add_promise(PromiseCreator::lambda(
      [actor_id = actor_id(this), expected_contact_count = user_ids.size()](Result<Unit> result) {
        if (result.is_ok()) {
          send_closure(actor_id, &ContactsManager::on_get_contacts_finished, expected_contact_count);
        }
      }));

  auto lock_promise = load_contact_users_multipromise_.get_promise();

  for (auto user_id : user_ids) {
    get_user(user_id, 3, load_contact_users_multipromise_.get_promise());
  }

  lock_promise.set_value(Unit());
}

class ContactsManager::UserLogEvent {
 public:
  UserId user_id;
  User u;

  UserLogEvent() = default;

  UserLogEvent(UserId user_id, const User &u) : user_id(user_id), u(u) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(user_id, storer);
    td::store(u, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(user_id, parser);
    td::parse(u, parser);
  }
};

void ContactsManager::save_user(User *u, UserId user_id, bool from_binlog) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }
  CHECK(u != nullptr);
  if (!u->is_saved || !u->is_status_saved) {  // TODO more effective handling of !u->is_status_saved
    if (!from_binlog) {
      auto log_event = UserLogEvent(user_id, *u);
      auto storer = get_log_event_storer(log_event);
      if (u->log_event_id == 0) {
        u->log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::Users, storer);
      } else {
        binlog_rewrite(G()->td_db()->get_binlog(), u->log_event_id, LogEvent::HandlerType::Users, storer);
      }
    }

    save_user_to_database(u, user_id);
  }
}

void ContactsManager::on_binlog_user_event(BinlogEvent &&event) {
  if (!G()->parameters().use_chat_info_db) {
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  UserLogEvent log_event;
  log_event_parse(log_event, event.data_).ensure();

  auto user_id = log_event.user_id;
  if (have_user(user_id)) {
    LOG(ERROR) << "Skip adding already added " << user_id;
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  LOG(INFO) << "Add " << user_id << " from binlog";
  User *u = add_user(user_id, "on_binlog_user_event");
  *u = std::move(log_event.u);  // users come from binlog before all other events, so just add them

  u->log_event_id = event.id_;

  update_user(u, user_id, true, false);
}

string ContactsManager::get_user_database_value(const User *u) {
  return log_event_store(*u).as_slice().str();
}

void ContactsManager::on_load_user_from_database(UserId user_id, string value, bool force) {
  if (G()->close_flag() && !force) {
    // the user is in Binlog and will be saved after restart
    return;
  }

  if (!loaded_from_database_users_.insert(user_id).second) {
    return;
  }

  auto it = load_user_from_database_queries_.find(user_id);
  vector<Promise<Unit>> promises;
  if (it != load_user_from_database_queries_.end()) {
    promises = std::move(it->second);
    CHECK(!promises.empty());
    load_user_from_database_queries_.erase(it);
  }

  LOG(INFO) << "Successfully loaded " << user_id << " of size " << value.size() << " from database";
  //  G()->td_db()->get_sqlite_pmc()->erase(get_user_database_key(user_id), Auto());
  //  return;

  User *u = get_user(user_id);
  if (u == nullptr) {
    if (!value.empty()) {
      u = add_user(user_id, "on_load_user_from_database");

      log_event_parse(*u, value).ensure();

      u->is_saved = true;
      u->is_status_saved = true;
      update_user(u, user_id, true, true);
    }
  } else {
    CHECK(!u->is_saved);  // user can't be saved before load completes
    CHECK(!u->is_being_saved);
    auto new_value = get_user_database_value(u);
    if (value != new_value) {
      save_user_to_database_impl(u, user_id, std::move(new_value));
    } else if (u->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), u->log_event_id);
      u->log_event_id = 0;
    }
  }

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

class ContactsManager::ChatLogEvent {
 public:
  ChatId chat_id;
  Chat c;

  ChatLogEvent() = default;

  ChatLogEvent(ChatId chat_id, const Chat &c) : chat_id(chat_id), c(c) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(chat_id, storer);
    td::store(c, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(chat_id, parser);
    td::parse(c, parser);
  }
};

void ContactsManager::save_chat(Chat *c, ChatId chat_id, bool from_binlog) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }
  CHECK(c != nullptr);
  if (!c->is_saved) {
    if (!from_binlog) {
      auto log_event = ChatLogEvent(chat_id, *c);
      auto storer = get_log_event_storer(log_event);
      if (c->log_event_id == 0) {
        c->log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::Chats, storer);
      } else {
        binlog_rewrite(G()->td_db()->get_binlog(), c->log_event_id, LogEvent::HandlerType::Chats, storer);
      }
    }

    save_chat_to_database(c, chat_id);
    return;
  }
}

void ContactsManager::on_binlog_chat_event(BinlogEvent &&event) {
  if (!G()->parameters().use_chat_info_db) {
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  ChatLogEvent log_event;
  log_event_parse(log_event, event.data_).ensure();

  auto chat_id = log_event.chat_id;
  if (have_chat(chat_id)) {
    LOG(ERROR) << "Skip adding already added " << chat_id;
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  LOG(INFO) << "Add " << chat_id << " from binlog";
  Chat *c = add_chat(chat_id);
  *c = std::move(log_event.c);  // chats come from binlog before all other events, so just add them

  c->log_event_id = event.id_;

  update_chat(c, chat_id, true, false);
}

string ContactsManager::get_chat_database_value(const Chat *c) {
  return log_event_store(*c).as_slice().str();
}

void ContactsManager::on_load_chat_from_database(ChatId chat_id, string value, bool force) {
  if (G()->close_flag() && !force) {
    // the chat is in Binlog and will be saved after restart
    return;
  }

  if (!loaded_from_database_chats_.insert(chat_id).second) {
    return;
  }

  auto it = load_chat_from_database_queries_.find(chat_id);
  vector<Promise<Unit>> promises;
  if (it != load_chat_from_database_queries_.end()) {
    promises = std::move(it->second);
    CHECK(!promises.empty());
    load_chat_from_database_queries_.erase(it);
  }

  LOG(INFO) << "Successfully loaded " << chat_id << " of size " << value.size() << " from database";
  //  G()->td_db()->get_sqlite_pmc()->erase(get_chat_database_key(chat_id), Auto());
  //  return;

  Chat *c = get_chat(chat_id);
  if (c == nullptr) {
    if (!value.empty()) {
      c = add_chat(chat_id);

      log_event_parse(*c, value).ensure();

      c->is_saved = true;
      update_chat(c, chat_id, true, true);
    }
  } else {
    CHECK(!c->is_saved);  // chat can't be saved before load completes
    CHECK(!c->is_being_saved);
    auto new_value = get_chat_database_value(c);
    if (value != new_value) {
      save_chat_to_database_impl(c, chat_id, std::move(new_value));
    } else if (c->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), c->log_event_id);
      c->log_event_id = 0;
    }
  }

  if (c != nullptr && c->migrated_to_channel_id.is_valid() && !have_channel_force(c->migrated_to_channel_id)) {
    LOG(ERROR) << "Can't find " << c->migrated_to_channel_id << " from " << chat_id;
  }

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

class ContactsManager::ChannelLogEvent {
 public:
  ChannelId channel_id;
  Channel c;

  ChannelLogEvent() = default;

  ChannelLogEvent(ChannelId channel_id, const Channel &c) : channel_id(channel_id), c(c) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(channel_id, storer);
    td::store(c, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(channel_id, parser);
    td::parse(c, parser);
  }
};

void ContactsManager::save_channel(Channel *c, ChannelId channel_id, bool from_binlog) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }
  CHECK(c != nullptr);
  if (!c->is_saved) {
    if (!from_binlog) {
      auto log_event = ChannelLogEvent(channel_id, *c);
      auto storer = get_log_event_storer(log_event);
      if (c->log_event_id == 0) {
        c->log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::Channels, storer);
      } else {
        binlog_rewrite(G()->td_db()->get_binlog(), c->log_event_id, LogEvent::HandlerType::Channels, storer);
      }
    }

    save_channel_to_database(c, channel_id);
    return;
  }
}

void ContactsManager::on_binlog_channel_event(BinlogEvent &&event) {
  if (!G()->parameters().use_chat_info_db) {
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  ChannelLogEvent log_event;
  log_event_parse(log_event, event.data_).ensure();

  auto channel_id = log_event.channel_id;
  if (have_channel(channel_id)) {
    LOG(ERROR) << "Skip adding already added " << channel_id;
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  LOG(INFO) << "Add " << channel_id << " from binlog";
  Channel *c = add_channel(channel_id, "on_binlog_channel_event");
  *c = std::move(log_event.c);  // channels come from binlog before all other events, so just add them

  c->log_event_id = event.id_;

  update_channel(c, channel_id, true, false);
}

string ContactsManager::get_channel_database_value(const Channel *c) {
  return log_event_store(*c).as_slice().str();
}

void ContactsManager::on_load_channel_from_database(ChannelId channel_id, string value, bool force) {
  if (G()->close_flag() && !force) {
    // the channel is in Binlog and will be saved after restart
    return;
  }

  if (!loaded_from_database_channels_.insert(channel_id).second) {
    return;
  }

  auto it = load_channel_from_database_queries_.find(channel_id);
  vector<Promise<Unit>> promises;
  if (it != load_channel_from_database_queries_.end()) {
    promises = std::move(it->second);
    CHECK(!promises.empty());
    load_channel_from_database_queries_.erase(it);
  }

  LOG(INFO) << "Successfully loaded " << channel_id << " of size " << value.size() << " from database";
  //  G()->td_db()->get_sqlite_pmc()->erase(get_channel_database_key(channel_id), Auto());
  //  return;

  Channel *c = get_channel(channel_id);
  if (c == nullptr) {
    if (!value.empty()) {
      c = add_channel(channel_id, "on_load_channel_from_database");

      log_event_parse(*c, value).ensure();

      c->is_saved = true;
      update_channel(c, channel_id, true, true);
    }
  } else {
    CHECK(!c->is_saved);  // channel can't be saved before load completes
    CHECK(!c->is_being_saved);
    if (!value.empty()) {
      Channel temp_c;
      log_event_parse(temp_c, value).ensure();
      if (c->participant_count == 0 && temp_c.participant_count != 0) {
        c->participant_count = temp_c.participant_count;
        CHECK(c->is_update_supergroup_sent);
        send_closure(G()->td(), &Td::send_update,
                     make_tl_object<td_api::updateSupergroup>(get_supergroup_object(channel_id, c)));
      }

      c->status.update_restrictions();
      temp_c.status.update_restrictions();
      if (temp_c.status != c->status) {
        on_channel_status_changed(c, channel_id, temp_c.status, c->status);
        CHECK(!c->is_being_saved);
      }

      if (temp_c.username != c->username) {
        on_channel_username_changed(c, channel_id, temp_c.username, c->username);
        CHECK(!c->is_being_saved);
      }
    }
    auto new_value = get_channel_database_value(c);
    if (value != new_value) {
      save_channel_to_database_impl(c, channel_id, std::move(new_value));
    } else if (c->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), c->log_event_id);
      c->log_event_id = 0;
    }
  }

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

class ContactsManager::SecretChatLogEvent {
 public:
  SecretChatId secret_chat_id;
  SecretChat c;

  SecretChatLogEvent() = default;

  SecretChatLogEvent(SecretChatId secret_chat_id, const SecretChat &c) : secret_chat_id(secret_chat_id), c(c) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(secret_chat_id, storer);
    td::store(c, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(secret_chat_id, parser);
    td::parse(c, parser);
  }
};

void ContactsManager::save_secret_chat(SecretChat *c, SecretChatId secret_chat_id, bool from_binlog) {
  if (!G()->parameters().use_chat_info_db) {
    return;
  }
  CHECK(c != nullptr);
  if (!c->is_saved) {
    if (!from_binlog) {
      auto log_event = SecretChatLogEvent(secret_chat_id, *c);
      auto storer = get_log_event_storer(log_event);
      if (c->log_event_id == 0) {
        c->log_event_id = binlog_add(G()->td_db()->get_binlog(), LogEvent::HandlerType::SecretChatInfos, storer);
      } else {
        binlog_rewrite(G()->td_db()->get_binlog(), c->log_event_id, LogEvent::HandlerType::SecretChatInfos, storer);
      }
    }

    save_secret_chat_to_database(c, secret_chat_id);
    return;
  }
}

void ContactsManager::on_binlog_secret_chat_event(BinlogEvent &&event) {
  if (!G()->parameters().use_chat_info_db) {
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  SecretChatLogEvent log_event;
  log_event_parse(log_event, event.data_).ensure();

  auto secret_chat_id = log_event.secret_chat_id;
  if (have_secret_chat(secret_chat_id)) {
    LOG(ERROR) << "Skip adding already added " << secret_chat_id;
    binlog_erase(G()->td_db()->get_binlog(), event.id_);
    return;
  }

  LOG(INFO) << "Add " << secret_chat_id << " from binlog";
  SecretChat *c = add_secret_chat(secret_chat_id);
  *c = std::move(log_event.c);  // secret chats come from binlog before all other events, so just add them

  c->log_event_id = event.id_;

  update_secret_chat(c, secret_chat_id, true, false);
}

string ContactsManager::get_secret_chat_database_value(const SecretChat *c) {
  return log_event_store(*c).as_slice().str();
}

void ContactsManager::on_load_secret_chat_from_database(SecretChatId secret_chat_id, string value, bool force) {
  if (G()->close_flag() && !force) {
    // the secret chat is in Binlog and will be saved after restart
    return;
  }

  if (!loaded_from_database_secret_chats_.insert(secret_chat_id).second) {
    return;
  }

  auto it = load_secret_chat_from_database_queries_.find(secret_chat_id);
  vector<Promise<Unit>> promises;
  if (it != load_secret_chat_from_database_queries_.end()) {
    promises = std::move(it->second);
    CHECK(!promises.empty());
    load_secret_chat_from_database_queries_.erase(it);
  }

  LOG(INFO) << "Successfully loaded " << secret_chat_id << " of size " << value.size() << " from database";
  //  G()->td_db()->get_sqlite_pmc()->erase(get_secret_chat_database_key(secret_chat_id), Auto());
  //  return;

  SecretChat *c = get_secret_chat(secret_chat_id);
  if (c == nullptr) {
    if (!value.empty()) {
      c = add_secret_chat(secret_chat_id);

      log_event_parse(*c, value).ensure();

      c->is_saved = true;
      update_secret_chat(c, secret_chat_id, true, true);
    }
  } else {
    CHECK(!c->is_saved);  // secret chat can't be saved before load completes
    CHECK(!c->is_being_saved);
    auto new_value = get_secret_chat_database_value(c);
    if (value != new_value) {
      save_secret_chat_to_database_impl(c, secret_chat_id, std::move(new_value));
    } else if (c->log_event_id != 0) {
      binlog_erase(G()->td_db()->get_binlog(), c->log_event_id);
      c->log_event_id = 0;
    }
  }

  // TODO load users asynchronously
  if (c != nullptr && !have_user_force(c->user_id)) {
    LOG(ERROR) << "Can't find " << c->user_id << " from " << secret_chat_id;
  }

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

string ContactsManager::get_user_full_database_value(const UserFull *user_full) {
  return log_event_store(*user_full).as_slice().str();
}

void ContactsManager::on_load_user_full_from_database(UserId user_id, string value) {
  LOG(INFO) << "Successfully loaded full " << user_id << " of size " << value.size() << " from database";
  //  G()->td_db()->get_sqlite_pmc()->erase(get_user_full_database_key(user_id), Auto());
  //  return;

  if (get_user_full(user_id) != nullptr || value.empty()) {
    return;
  }

  UserFull *user_full = add_user_full(user_id);
  auto status = log_event_parse(*user_full, value);
  if (status.is_error()) {
    // can't happen unless database is broken
    LOG(ERROR) << "Repair broken full " << user_id << ' ' << format::as_hex_dump<4>(Slice(value));

    // just clean all known data about the user and pretend that there was nothing in the database
    users_full_.erase(user_id);
    G()->td_db()->get_sqlite_pmc()->erase(get_user_full_database_key(user_id), Auto());
    return;
  }

  Dependencies dependencies;
  dependencies.user_ids.insert(user_id);
  if (!resolve_dependencies_force(td_, dependencies, "on_load_user_full_from_database")) {
    users_full_.erase(user_id);
    G()->td_db()->get_sqlite_pmc()->erase(get_user_full_database_key(user_id), Auto());
    return;
  }

  if (user_full->need_phone_number_privacy_exception && is_user_contact(user_id)) {
    user_full->need_phone_number_privacy_exception = false;
  }

  User *u = get_user(user_id);
  CHECK(u != nullptr);
  if (u->photo.id != user_full->photo.id.get()) {
    user_full->photo = Photo();
    if (u->photo.id > 0) {
      user_full->expires_at = 0.0;
    }
  }
  if (!user_full->photo.is_empty()) {
    register_user_photo(u, user_id, user_full->photo);
  }

  td_->group_call_manager_->on_update_dialog_about(DialogId(user_id), user_full->about, false);

  user_full->is_update_user_full_sent = true;
  update_user_full(user_full, user_id, "on_load_user_full_from_database", true);

  if (is_user_deleted(user_id)) {
    drop_user_full(user_id);
  } else if (user_full->expires_at == 0.0) {
    load_user_full(user_id, true, Auto(), "on_load_user_full_from_database");
  }
}

string ContactsManager::get_chat_full_database_value(const ChatFull *chat_full) {
  return log_event_store(*chat_full).as_slice().str();
}

void ContactsManager::on_load_chat_full_from_database(ChatId chat_id, string value) {
  LOG(INFO) << "Successfully loaded full " << chat_id << " of size " << value.size() << " from database";
  //  G()->td_db()->get_sqlite_pmc()->erase(get_chat_full_database_key(chat_id), Auto());
  //  return;

  if (get_chat_full(chat_id) != nullptr || value.empty()) {
    return;
  }

  ChatFull *chat_full = add_chat_full(chat_id);
  auto status = log_event_parse(*chat_full, value);
  if (status.is_error()) {
    // can't happen unless database is broken
    LOG(ERROR) << "Repair broken full " << chat_id << ' ' << format::as_hex_dump<4>(Slice(value));

    // just clean all known data about the chat and pretend that there was nothing in the database
    chats_full_.erase(chat_id);
    G()->td_db()->get_sqlite_pmc()->erase(get_chat_full_database_key(chat_id), Auto());
    return;
  }

  Dependencies dependencies;
  dependencies.chat_ids.insert(chat_id);
  dependencies.user_ids.insert(chat_full->creator_user_id);
  for (auto &participant : chat_full->participants) {
    add_message_sender_dependencies(dependencies, participant.dialog_id_);
    dependencies.user_ids.insert(participant.inviter_user_id_);
  }
  dependencies.user_ids.insert(chat_full->invite_link.get_creator_user_id());
  if (!resolve_dependencies_force(td_, dependencies, "on_load_chat_full_from_database")) {
    chats_full_.erase(chat_id);
    G()->td_db()->get_sqlite_pmc()->erase(get_chat_full_database_key(chat_id), Auto());
    return;
  }

  Chat *c = get_chat(chat_id);
  CHECK(c != nullptr);

  bool need_invite_link = c->is_active && c->status.can_manage_invite_links();
  bool have_invite_link = chat_full->invite_link.is_valid();
  if (need_invite_link != have_invite_link) {
    if (need_invite_link) {
      // ignore ChatFull without invite link
      chats_full_.erase(chat_id);
      return;
    } else {
      chat_full->invite_link = DialogInviteLink();
    }
  }

  if (td_->file_manager_->get_file_view(c->photo.small_file_id).get_unique_file_id() !=
      td_->file_manager_->get_file_view(as_fake_dialog_photo(chat_full->photo, DialogId(chat_id)).small_file_id)
          .get_unique_file_id()) {
    chat_full->photo = Photo();
    if (c->photo.small_file_id.is_valid()) {
      reload_chat_full(chat_id, Auto());
    }
  }

  td_->group_call_manager_->on_update_dialog_about(DialogId(chat_id), chat_full->description, false);

  on_update_chat_full_photo(chat_full, chat_id, std::move(chat_full->photo));

  chat_full->is_update_chat_full_sent = true;
  update_chat_full(chat_full, chat_id, "on_load_chat_full_from_database", true);
}

string ContactsManager::get_channel_full_database_value(const ChannelFull *channel_full) {
  return log_event_store(*channel_full).as_slice().str();
}

void ContactsManager::on_load_channel_full_from_database(ChannelId channel_id, string value, const char *source) {
  LOG(INFO) << "Successfully loaded full " << channel_id << " of size " << value.size() << " from database from "
            << source;
  //  G()->td_db()->get_sqlite_pmc()->erase(get_channel_full_database_key(channel_id), Auto());
  //  return;

  if (get_channel_full(channel_id, true, "on_load_channel_full_from_database") != nullptr || value.empty()) {
    return;
  }

  ChannelFull *channel_full = add_channel_full(channel_id);
  auto status = log_event_parse(*channel_full, value);
  if (status.is_error()) {
    // can't happen unless database is broken
    LOG(ERROR) << "Repair broken full " << channel_id << ' ' << format::as_hex_dump<4>(Slice(value));

    // just clean all known data about the channel and pretend that there was nothing in the database
    channels_full_.erase(channel_id);
    G()->td_db()->get_sqlite_pmc()->erase(get_channel_full_database_key(channel_id), Auto());
    return;
  }

  Dependencies dependencies;
  dependencies.channel_ids.insert(channel_id);
  // must not depend on the linked_dialog_id itself, because message database can be disabled
  // the Dialog will be forcely created in update_channel_full
  add_dialog_dependencies(dependencies, DialogId(channel_full->linked_channel_id));
  dependencies.chat_ids.insert(channel_full->migrated_from_chat_id);
  dependencies.user_ids.insert(channel_full->bot_user_ids.begin(), channel_full->bot_user_ids.end());
  dependencies.user_ids.insert(channel_full->invite_link.get_creator_user_id());
  if (!resolve_dependencies_force(td_, dependencies, source)) {
    channels_full_.erase(channel_id);
    G()->td_db()->get_sqlite_pmc()->erase(get_channel_full_database_key(channel_id), Auto());
    return;
  }

  Channel *c = get_channel(channel_id);
  CHECK(c != nullptr);

  bool need_invite_link = c->status.can_manage_invite_links();
  bool have_invite_link = channel_full->invite_link.is_valid();
  if (need_invite_link != have_invite_link) {
    if (need_invite_link) {
      // ignore ChannelFull without invite link
      channels_full_.erase(channel_id);
      return;
    } else {
      channel_full->invite_link = DialogInviteLink();
    }
  }

  if (td_->file_manager_->get_file_view(c->photo.small_file_id).get_unique_file_id() !=
      td_->file_manager_->get_file_view(as_fake_dialog_photo(channel_full->photo, DialogId(channel_id)).small_file_id)
          .get_unique_file_id()) {
    channel_full->photo = Photo();
    if (c->photo.small_file_id.is_valid()) {
      channel_full->expires_at = 0.0;
    }
  }
  auto photo = std::move(channel_full->photo);
  on_update_channel_full_photo(channel_full, channel_id, std::move(photo));

  if (channel_full->participant_count < channel_full->administrator_count) {
    channel_full->participant_count = channel_full->administrator_count;
  }
  if (c->participant_count != 0 && c->participant_count != channel_full->participant_count) {
    channel_full->participant_count = c->participant_count;

    if (channel_full->participant_count < channel_full->administrator_count) {
      channel_full->participant_count = channel_full->administrator_count;
      channel_full->expires_at = 0.0;

      c->participant_count = channel_full->participant_count;
      c->is_changed = true;
      update_channel(c, channel_id);
    }
  }

  if (invalidated_channels_full_.erase(channel_id) > 0 ||
      (!c->is_slow_mode_enabled && channel_full->slow_mode_delay != 0)) {
    do_invalidate_channel_full(channel_full, channel_id, !c->is_slow_mode_enabled);
  }

  td_->group_call_manager_->on_update_dialog_about(DialogId(channel_id), channel_full->description, false);

  send_closure_later(G()->messages_manager(), &MessagesManager::on_dialog_bots_updated, DialogId(channel_id),
                     channel_full->bot_user_ids, true);

  channel_full->is_update_channel_full_sent = true;
  update_channel_full(channel_full, channel_id, "on_load_channel_full_from_database", true);

  if (channel_full->expires_at == 0.0) {
    load_channel_full(channel_id, true, Auto(), "on_load_channel_full_from_database");
  }
}

void ContactsManager::on_load_dialog_administrators_from_database(
    DialogId dialog_id, string value, Promise<td_api::object_ptr<td_api::chatAdministrators>> &&promise) {
  TRY_STATUS_PROMISE(promise, G()->close_status());

  if (value.empty()) {
    return reload_dialog_administrators(dialog_id, {}, std::move(promise));
  }

  vector<DialogAdministrator> administrators;
  log_event_parse(administrators, value).ensure();

  LOG(INFO) << "Successfully loaded " << administrators.size() << " administrators in " << dialog_id
            << " from database";

  MultiPromiseActorSafe load_users_multipromise{"LoadUsersMultiPromiseActor"};
  load_users_multipromise.add_promise(
      PromiseCreator::lambda([actor_id = actor_id(this), dialog_id, administrators,
                              promise = std::move(promise)](Result<Unit> result) mutable {
        send_closure(actor_id, &ContactsManager::on_load_administrator_users_finished, dialog_id,
                     std::move(administrators), std::move(result), std::move(promise));
      }));

  auto lock_promise = load_users_multipromise.get_promise();

  for (auto &administrator : administrators) {
    get_user(administrator.get_user_id(), 3, load_users_multipromise.get_promise());
  }

  lock_promise.set_value(Unit());
}

void ContactsManager::on_update_dialog_administrators(DialogId dialog_id, vector<DialogAdministrator> &&administrators,
                                                      bool have_access, bool from_database) {
  LOG(INFO) << "Update administrators in " << dialog_id << " to " << format::as_array(administrators);
  if (have_access) {
    std::sort(administrators.begin(), administrators.end(),
              [](const DialogAdministrator &lhs, const DialogAdministrator &rhs) {
                return lhs.get_user_id().get() < rhs.get_user_id().get();
              });

    auto it = dialog_administrators_.find(dialog_id);
    if (it != dialog_administrators_.end()) {
      if (it->second == administrators) {
        return;
      }
      it->second = std::move(administrators);
    } else {
      it = dialog_administrators_.emplace(dialog_id, std::move(administrators)).first;
    }

    if (G()->parameters().use_chat_info_db && !from_database) {
      LOG(INFO) << "Save administrators of " << dialog_id << " to database";
      G()->td_db()->get_sqlite_pmc()->set(get_dialog_administrators_database_key(dialog_id),
                                          log_event_store(it->second).as_slice().str(), Auto());
    }
  } else {
    dialog_administrators_.erase(dialog_id);
    if (G()->parameters().use_chat_info_db) {
      G()->td_db()->get_sqlite_pmc()->erase(get_dialog_administrators_database_key(dialog_id), Auto());
    }
  }
}
}  // namespace td
