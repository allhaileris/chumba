//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/StickersManager.h"

#include "td/telegram/AccessRights.h"
#include "td/telegram/AuthManager.h"
#include "td/telegram/ConfigManager.h"
#include "td/telegram/ConfigShared.h"

#include "td/telegram/DialogId.h"
#include "td/telegram/Document.h"


#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"
#include "td/telegram/LanguagePackManager.h"
#include "td/telegram/logevent/LogEvent.h"
#include "td/telegram/MessagesManager.h"
#include "td/telegram/misc.h"
#include "td/telegram/net/DcId.h"
#include "td/telegram/net/MtprotoHeader.h"
#include "td/telegram/net/NetQueryDispatcher.h"
#include "td/telegram/PhotoSizeSource.h"
#include "td/telegram/secret_api.h"
#include "td/telegram/StickerSetId.hpp"
#include "td/telegram/StickersManager.hpp"
#include "td/telegram/Td.h"
#include "td/telegram/td_api.h"
#include "td/telegram/TdDb.h"
#include "td/telegram/TdParameters.h"
#include "td/telegram/telegram_api.h"

#include "td/db/SqliteKeyValue.h"
#include "td/db/SqliteKeyValueAsync.h"

#include "td/actor/MultiPromise.h"
#include "td/actor/PromiseFuture.h"
#include "td/actor/SleepActor.h"

#include "td/utils/algorithm.h"
#include "td/utils/base64.h"
#include "td/utils/emoji.h"
#include "td/utils/format.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"
#include "td/utils/MimeType.h"
#include "td/utils/misc.h"
#include "td/utils/PathView.h"
#include "td/utils/Random.h"
#include "td/utils/Slice.h"
#include "td/utils/SliceBuilder.h"
#include "td/utils/StackAllocator.h"
#include "td/utils/StringBuilder.h"
#include "td/utils/Time.h"
#include "td/utils/tl_helpers.h"
#include "td/utils/utf8.h"

#include <algorithm>


#include <type_traits>


namespace td {
}  // namespace td
namespace td {

vector<td_api::object_ptr<td_api::closedVectorPath>> StickersManager::get_sticker_minithumbnail(
    CSlice path, StickerSetId sticker_set_id, int64 document_id, double zoom) {
  if (path.empty()) {
    return {};
  }

  auto buf = StackAllocator::alloc(1 << 9);
  StringBuilder sb(buf.as_slice(), true);

  sb << 'M';
  for (unsigned char c : path) {
    if (c >= 128 + 64) {
      sb << "AACAAAAHAAALMAAAQASTAVAAAZaacaaaahaaalmaaaqastava.az0123456789-,"[c - 128 - 64];
    } else {
      if (c >= 128) {
        sb << ',';
      } else if (c >= 64) {
        sb << '-';
      }
      sb << (c & 63);
    }
  }
  sb << 'z';

  CHECK(!sb.is_error());
  path = sb.as_cslice();
  LOG(DEBUG) << "Transform SVG path " << path;

  size_t pos = 0;
  auto skip_commas = [&path, &pos] {
    while (path[pos] == ',') {
      pos++;
    }
  };
  auto get_number = [&] {
    skip_commas();
    int sign = 1;
    if (path[pos] == '-') {
      sign = -1;
      pos++;
    }
    double res = 0;
    while (is_digit(path[pos])) {
      res = res * 10 + path[pos++] - '0';
    }
    if (path[pos] == '.') {
      pos++;
      double mul = 0.1;
      while (is_digit(path[pos])) {
        res += (path[pos] - '0') * mul;
        mul *= 0.1;
        pos++;
      }
    }
    return sign * res;
  };
  auto make_point = [zoom](double x, double y) {
    return td_api::make_object<td_api::point>(x * zoom, y * zoom);
  };

  vector<td_api::object_ptr<td_api::closedVectorPath>> result;
  double x = 0;
  double y = 0;
  while (path[pos] != '\0') {
    skip_commas();
    if (path[pos] == '\0') {
      break;
    }

    while (path[pos] == 'm' || path[pos] == 'M') {
      auto command = path[pos++];
      do {
        if (command == 'm') {
          x += get_number();
          y += get_number();
        } else {
          x = get_number();
          y = get_number();
        }
        skip_commas();
      } while (path[pos] != '\0' && !is_alpha(path[pos]));
    }

    double start_x = x;
    double start_y = y;

    vector<td_api::object_ptr<td_api::VectorPathCommand>> commands;
    bool have_last_end_control_point = false;
    double last_end_control_point_x = 0;
    double last_end_control_point_y = 0;
    bool is_closed = false;
    char command = '-';
    while (!is_closed) {
      skip_commas();
      if (path[pos] == '\0') {
        LOG(ERROR) << "Receive unclosed path " << path << " in a sticker " << document_id << " from " << sticker_set_id;
        return {};
      }
      if (is_alpha(path[pos])) {
        command = path[pos++];
      }
      switch (command) {
        case 'l':
        case 'L':
        case 'h':
        case 'H':
        case 'v':
        case 'V':
          if (command == 'l' || command == 'h') {
            x += get_number();
          } else if (command == 'L' || command == 'H') {
            x = get_number();
          }
          if (command == 'l' || command == 'v') {
            y += get_number();
          } else if (command == 'L' || command == 'V') {
            y = get_number();
          }
          commands.push_back(td_api::make_object<td_api::vectorPathCommandLine>(make_point(x, y)));
          have_last_end_control_point = false;
          break;
        case 'C':
        case 'c':
        case 'S':
        case 's': {
          double start_control_point_x;
          double start_control_point_y;
          if (command == 'S' || command == 's') {
            if (have_last_end_control_point) {
              start_control_point_x = 2 * x - last_end_control_point_x;
              start_control_point_y = 2 * y - last_end_control_point_y;
            } else {
              start_control_point_x = x;
              start_control_point_y = y;
            }
          } else {
            start_control_point_x = get_number();
            start_control_point_y = get_number();
            if (command == 'c') {
              start_control_point_x += x;
              start_control_point_y += y;
            }
          }

          last_end_control_point_x = get_number();
          last_end_control_point_y = get_number();
          if (command == 'c' || command == 's') {
            last_end_control_point_x += x;
            last_end_control_point_y += y;
          }
          have_last_end_control_point = true;

          if (command == 'c' || command == 's') {
            x += get_number();
            y += get_number();
          } else {
            x = get_number();
            y = get_number();
          }

          commands.push_back(td_api::make_object<td_api::vectorPathCommandCubicBezierCurve>(
              make_point(start_control_point_x, start_control_point_y),
              make_point(last_end_control_point_x, last_end_control_point_y), make_point(x, y)));
          break;
        }
        case 'm':
        case 'M':
          pos--;
          // fallthrough
        case 'z':
        case 'Z':
          if (x != start_x || y != start_y) {
            x = start_x;
            y = start_y;
            commands.push_back(td_api::make_object<td_api::vectorPathCommandLine>(make_point(x, y)));
          }
          if (!commands.empty()) {
            result.push_back(td_api::make_object<td_api::closedVectorPath>(std::move(commands)));
            commands.clear();
          }
          is_closed = true;
          break;
        default:
          LOG(ERROR) << "Receive invalid command " << command << " at pos " << pos << " in a sticker " << document_id
                     << " from " << sticker_set_id << ": " << path;
          return {};
      }
    }
  }
  /*
  string svg;
  for (const auto &vector_path : result) {
    CHECK(!vector_path->commands_.empty());
    svg += 'M';
    auto add_point = [&](const td_api::object_ptr<td_api::point> &p) {
      svg += to_string(static_cast<int>(p->x_));
      svg += ',';
      svg += to_string(static_cast<int>(p->y_));
      svg += ',';
    };
    auto last_command = vector_path->commands_.back().get();
    switch (last_command->get_id()) {
      case td_api::vectorPathCommandLine::ID:
        add_point(static_cast<const td_api::vectorPathCommandLine *>(last_command)->end_point_);
        break;
      case td_api::vectorPathCommandCubicBezierCurve::ID:
        add_point(static_cast<const td_api::vectorPathCommandCubicBezierCurve *>(last_command)->end_point_);
        break;
      default:
        UNREACHABLE();
    }
    for (auto &command : vector_path->commands_) {
      switch (command->get_id()) {
        case td_api::vectorPathCommandLine::ID: {
          auto line = static_cast<const td_api::vectorPathCommandLine *>(command.get());
          svg += 'L';
          add_point(line->end_point_);
          break;
        }
        case td_api::vectorPathCommandCubicBezierCurve::ID: {
          auto curve = static_cast<const td_api::vectorPathCommandCubicBezierCurve *>(command.get());
          svg += 'C';
          add_point(curve->start_control_point_);
          add_point(curve->end_control_point_);
          add_point(curve->end_point_);
          break;
        }
        default:
          UNREACHABLE();
      }
    }
    svg += 'z';
  }
  */

  return result;
}

vector<string> StickersManager::get_emoji_language_codes(const vector<string> &input_language_codes, Slice text,
                                                         Promise<Unit> &promise) {
  vector<string> language_codes = td_->language_pack_manager_->get_actor_unsafe()->get_used_language_codes();
  auto system_language_code = G()->mtproto_header().get_system_language_code();
  if (system_language_code.size() >= 2 && system_language_code.find('$') == string::npos &&
      (system_language_code.size() == 2 || system_language_code[2] == '-')) {
    language_codes.push_back(system_language_code.substr(0, 2));
  }
  for (auto &input_language_code : input_language_codes) {
    if (input_language_code.size() >= 2 && input_language_code.find('$') == string::npos &&
        (input_language_code.size() == 2 || input_language_code[2] == '-')) {
      language_codes.push_back(input_language_code.substr(0, 2));
    }
  }
  if (!text.empty()) {
    uint32 code = 0;
    next_utf8_unsafe(text.ubegin(), &code, "get_emoji_language_codes");
    if ((0x410 <= code && code <= 0x44F) || code == 0x401 || code == 0x451) {
      // the first letter is cyrillic
      if (!td::contains(language_codes, "ru") && !td::contains(language_codes, "uk") &&
          !td::contains(language_codes, "bg") && !td::contains(language_codes, "be") &&
          !td::contains(language_codes, "mk") && !td::contains(language_codes, "sr") &&
          !td::contains(language_codes, "mn") && !td::contains(language_codes, "ky") &&
          !td::contains(language_codes, "kk") && !td::contains(language_codes, "uz") &&
          !td::contains(language_codes, "tk")) {
        language_codes.push_back("ru");
      }
    }
  }

  if (language_codes.empty()) {
    LOG(INFO) << "List of language codes is empty";
    language_codes.push_back("en");
  }
  td::unique(language_codes);

  LOG(DEBUG) << "Have language codes " << language_codes;
  auto key = get_emoji_language_codes_database_key(language_codes);
  auto it = emoji_language_codes_.find(key);
  if (it == emoji_language_codes_.end()) {
    it = emoji_language_codes_.emplace(key, full_split(G()->td_db()->get_sqlite_sync_pmc()->get(key), '$')).first;
  }
  if (it->second.empty()) {
    load_language_codes(std::move(language_codes), std::move(key), std::move(promise));
  } else {
    LOG(DEBUG) << "Have emoji language codes " << it->second;
    double now = Time::now_cached();
    for (auto &language_code : it->second) {
      double last_difference_time = get_emoji_language_code_last_difference_time(language_code);
      if (last_difference_time < now - EMOJI_KEYWORDS_UPDATE_DELAY &&
          get_emoji_language_code_version(language_code) != 0) {
        load_emoji_keywords_difference(language_code);
      }
    }
    if (reloaded_emoji_keywords_.insert(key).second) {
      load_language_codes(std::move(language_codes), std::move(key), Auto());
    }
  }
  return it->second;
}

FileId StickersManager::on_get_sticker(unique_ptr<Sticker> new_sticker, bool replace) {
  auto file_id = new_sticker->file_id;
  CHECK(file_id.is_valid());
  LOG(INFO) << "Receive sticker " << file_id;
  auto &s = stickers_[file_id];
  if (s == nullptr) {
    s = std::move(new_sticker);
  } else if (replace) {
    CHECK(s->file_id == file_id);
    if (s->dimensions != new_sticker->dimensions && new_sticker->dimensions.width != 0) {
      LOG(DEBUG) << "Sticker " << file_id << " dimensions have changed";
      s->dimensions = new_sticker->dimensions;
    }
    if (s->set_id != new_sticker->set_id && new_sticker->set_id.is_valid()) {
      LOG_IF(ERROR, s->set_id.is_valid()) << "Sticker " << file_id << " set_id has changed";
      s->set_id = new_sticker->set_id;
    }
    if (s->alt != new_sticker->alt && !new_sticker->alt.empty()) {
      LOG(DEBUG) << "Sticker " << file_id << " emoji has changed";
      s->alt = std::move(new_sticker->alt);
    }
    if (s->minithumbnail != new_sticker->minithumbnail) {
      LOG(DEBUG) << "Sticker " << file_id << " minithumbnail has changed";
      s->minithumbnail = std::move(new_sticker->minithumbnail);
    }
    if (s->s_thumbnail != new_sticker->s_thumbnail && new_sticker->s_thumbnail.file_id.is_valid()) {
      LOG_IF(INFO, s->s_thumbnail.file_id.is_valid()) << "Sticker " << file_id << " s thumbnail has changed from "
                                                      << s->s_thumbnail << " to " << new_sticker->s_thumbnail;
      s->s_thumbnail = std::move(new_sticker->s_thumbnail);
    }
    if (s->m_thumbnail != new_sticker->m_thumbnail && new_sticker->m_thumbnail.file_id.is_valid()) {
      LOG_IF(INFO, s->m_thumbnail.file_id.is_valid()) << "Sticker " << file_id << " m thumbnail has changed from "
                                                      << s->m_thumbnail << " to " << new_sticker->m_thumbnail;
      s->m_thumbnail = std::move(new_sticker->m_thumbnail);
    }
    if (s->is_animated != new_sticker->is_animated && new_sticker->is_animated) {
      s->is_animated = new_sticker->is_animated;
    }
    if (s->is_mask != new_sticker->is_mask && new_sticker->is_mask) {
      s->is_mask = new_sticker->is_mask;
    }
    if (s->point != new_sticker->point && new_sticker->point != -1) {
      s->point = new_sticker->point;
      s->x_shift = new_sticker->x_shift;
      s->y_shift = new_sticker->y_shift;
      s->scale = new_sticker->scale;
    }
  }

  return file_id;
}

std::pair<int32, vector<StickerSetId>> StickersManager::get_featured_sticker_sets(int32 offset, int32 limit,
                                                                                  Promise<Unit> &&promise) {
  if (offset < 0) {
    promise.set_error(Status::Error(400, "Parameter offset must be non-negative"));
    return {};
  }

  if (limit < 0) {
    promise.set_error(Status::Error(400, "Parameter limit must be non-negative"));
    return {};
  }
  if (limit == 0) {
    offset = 0;
  }

  if (!are_featured_sticker_sets_loaded_) {
    load_featured_sticker_sets(std::move(promise));
    return {};
  }
  reload_featured_sticker_sets(false);

  auto set_count = static_cast<int32>(featured_sticker_set_ids_.size());
  auto total_count = set_count + (old_featured_sticker_set_count_ == -1 ? 1 : old_featured_sticker_set_count_);
  if (offset < set_count) {
    if (limit > set_count - offset) {
      limit = set_count - offset;
    }
    promise.set_value(Unit());
    auto begin = featured_sticker_set_ids_.begin() + offset;
    return {total_count, {begin, begin + limit}};
  }

  if (offset == set_count && are_old_featured_sticker_sets_invalidated_) {
    invalidate_old_featured_sticker_sets();
  }

  if (offset < total_count || old_featured_sticker_set_count_ == -1) {
    offset -= set_count;
    set_count = static_cast<int32>(old_featured_sticker_set_ids_.size());
    if (offset < set_count) {
      if (limit > set_count - offset) {
        limit = set_count - offset;
      }
      promise.set_value(Unit());
      auto begin = old_featured_sticker_set_ids_.begin() + offset;
      return {total_count, {begin, begin + limit}};
    }
    if (offset > set_count) {
      promise.set_error(
          Status::Error(400, "Too big offset specified; trending sticker sets can be received only consequently"));
      return {};
    }

    load_old_featured_sticker_sets(std::move(promise));
    return {};
  }

  promise.set_value(Unit());
  return {total_count, vector<StickerSetId>()};
}

class GetAllStickersQuery final : public Td::ResultHandler {
  bool is_masks_;

 public:
  void send(bool is_masks, int64 hash) {
    is_masks_ = is_masks;
    if (is_masks) {
      send_query(G()->net_query_creator().create(telegram_api::messages_getMaskStickers(hash)));
    } else {
      send_query(G()->net_query_creator().create(telegram_api::messages_getAllStickers(hash)));
    }
  }

  void on_result(BufferSlice packet) final {
    static_assert(std::is_same<telegram_api::messages_getMaskStickers::ReturnType,
                               telegram_api::messages_getAllStickers::ReturnType>::value,
                  "");
    auto result_ptr = fetch_result<telegram_api::messages_getAllStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    auto ptr = result_ptr.move_as_ok();
    LOG(DEBUG) << "Receive result for get all " << (is_masks_ ? "masks" : "stickers") << ": " << to_string(ptr);
    td_->stickers_manager_->on_get_installed_sticker_sets(is_masks_, std::move(ptr));
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for get all stickers: " << status;
    }
    td_->stickers_manager_->on_get_installed_sticker_sets_failed(is_masks_, std::move(status));
  }
};

void StickersManager::reload_installed_sticker_sets(bool is_masks, bool force) {
  if (G()->close_flag()) {
    return;
  }

  auto &next_load_time = next_installed_sticker_sets_load_time_[is_masks];
  if (!td_->auth_manager_->is_bot() && next_load_time >= 0 && (next_load_time < Time::now() || force)) {
    LOG_IF(INFO, force) << "Reload sticker sets";
    next_load_time = -1;
    td_->create_handler<GetAllStickersQuery>()->send(is_masks, installed_sticker_sets_hash_[is_masks]);
  }
}

tl_object_ptr<telegram_api::inputStickerSetItem> StickersManager::get_input_sticker(td_api::InputSticker *sticker,
                                                                                    FileId file_id) const {
  CHECK(sticker != nullptr);
  FileView file_view = td_->file_manager_->get_file_view(file_id);
  CHECK(file_view.has_remote_location());
  auto input_document = file_view.main_remote_location().as_input_document();

  tl_object_ptr<telegram_api::maskCoords> mask_coords;
  if (sticker->get_id() == td_api::inputStickerStatic::ID) {
    auto mask_position = static_cast<td_api::inputStickerStatic *>(sticker)->mask_position_.get();
    if (mask_position != nullptr && mask_position->point_ != nullptr) {
      auto point = [mask_point = std::move(mask_position->point_)] {
        switch (mask_point->get_id()) {
          case td_api::maskPointForehead::ID:
            return 0;
          case td_api::maskPointEyes::ID:
            return 1;
          case td_api::maskPointMouth::ID:
            return 2;
          case td_api::maskPointChin::ID:
            return 3;
          default:
            UNREACHABLE();
            return -1;
        }
      }();
      mask_coords = make_tl_object<telegram_api::maskCoords>(point, mask_position->x_shift_, mask_position->y_shift_,
                                                             mask_position->scale_);
    }
  }

  int32 flags = 0;
  if (mask_coords != nullptr) {
    flags |= telegram_api::inputStickerSetItem::MASK_COORDS_MASK;
  }

  return make_tl_object<telegram_api::inputStickerSetItem>(flags, std::move(input_document),
                                                           get_input_sticker_emojis(sticker), std::move(mask_coords));
}

void StickersManager::register_dice(const string &emoji, int32 value, FullMessageId full_message_id,
                                    const char *source) {
  CHECK(!emoji.empty());
  if (td_->auth_manager_->is_bot()) {
    return;
  }

  LOG(INFO) << "Register dice " << emoji << " with value " << value << " from " << full_message_id << " from "
            << source;
  bool is_inserted = dice_messages_[emoji].insert(full_message_id).second;
  LOG_CHECK(is_inserted) << source << " " << emoji << " " << value << " " << full_message_id;

  if (!td::contains(dice_emojis_, emoji)) {
    if (full_message_id.get_message_id().is_any_server() &&
        full_message_id.get_dialog_id().get_type() != DialogType::SecretChat) {
      send_closure(G()->config_manager(), &ConfigManager::reget_app_config, Promise<Unit>());
    }
    return;
  }

  auto &special_sticker_set = add_special_sticker_set(SpecialStickerSetType::animated_dice(emoji));
  bool need_load = false;
  StickerSet *sticker_set = nullptr;
  if (!special_sticker_set.id_.is_valid()) {
    need_load = true;
  } else {
    sticker_set = get_sticker_set(special_sticker_set.id_);
    CHECK(sticker_set != nullptr);
    need_load = !sticker_set->was_loaded;
  }

  if (need_load) {
    LOG(INFO) << "Waiting for a dice sticker set needed in " << full_message_id;
    load_special_sticker_set(special_sticker_set);
  } else {
    // TODO reload once in a while
    // reload_special_sticker_set(special_sticker_set, sticker_set->is_loaded ? sticker_set->hash : 0);
  }
}

void StickersManager::on_find_sticker_sets_success(
    const string &query, tl_object_ptr<telegram_api::messages_FoundStickerSets> &&sticker_sets) {
  CHECK(sticker_sets != nullptr);
  switch (sticker_sets->get_id()) {
    case telegram_api::messages_foundStickerSetsNotModified::ID:
      return on_find_sticker_sets_fail(query, Status::Error(500, "Receive messages.foundStickerSetsNotModified"));
    case telegram_api::messages_foundStickerSets::ID: {
      auto found_stickers_sets = move_tl_object_as<telegram_api::messages_foundStickerSets>(sticker_sets);
      vector<StickerSetId> &sticker_set_ids = found_sticker_sets_[query];
      CHECK(sticker_set_ids.empty());

      for (auto &sticker_set : found_stickers_sets->sets_) {
        StickerSetId set_id = on_get_sticker_set_covered(std::move(sticker_set), true, "on_find_sticker_sets_success");
        if (!set_id.is_valid()) {
          continue;
        }

        update_sticker_set(get_sticker_set(set_id), "on_find_sticker_sets_success");
        sticker_set_ids.push_back(set_id);
      }

      send_update_installed_sticker_sets();
      break;
    }
    default:
      UNREACHABLE();
  }

  auto it = search_sticker_sets_queries_.find(query);
  CHECK(it != search_sticker_sets_queries_.end());
  CHECK(!it->second.empty());
  auto promises = std::move(it->second);
  search_sticker_sets_queries_.erase(it);

  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

class ReadFeaturedStickerSetsQuery final : public Td::ResultHandler {
 public:
  void send(const vector<StickerSetId> &sticker_set_ids) {
    send_query(G()->net_query_creator().create(
        telegram_api::messages_readFeaturedStickers(StickersManager::convert_sticker_set_ids(sticker_set_ids))));
  }

  void on_result(BufferSlice packet) final {
    auto result_ptr = fetch_result<telegram_api::messages_readFeaturedStickers>(packet);
    if (result_ptr.is_error()) {
      return on_error(result_ptr.move_as_error());
    }

    bool result = result_ptr.move_as_ok();
    (void)result;
  }

  void on_error(Status status) final {
    if (!G()->is_expected_error(status)) {
      LOG(ERROR) << "Receive error for ReadFeaturedStickerSetsQuery: " << status;
    }
    td_->stickers_manager_->reload_featured_sticker_sets(true);
  }
};

void StickersManager::read_featured_sticker_sets(void *td_void) {
  if (G()->close_flag()) {
    return;
  }

  CHECK(td_void != nullptr);
  auto td = static_cast<Td *>(td_void);

  auto &set_ids = td->stickers_manager_->pending_viewed_featured_sticker_set_ids_;
  td->create_handler<ReadFeaturedStickerSetsQuery>()->send(vector<StickerSetId>(set_ids.begin(), set_ids.end()));
  set_ids.clear();
}

void StickersManager::load_old_featured_sticker_sets(Promise<Unit> &&promise) {
  CHECK(!td_->auth_manager_->is_bot());
  CHECK(old_featured_sticker_set_ids_.size() % OLD_FEATURED_STICKER_SET_SLICE_SIZE == 0);
  load_old_featured_sticker_sets_queries_.push_back(std::move(promise));
  if (load_old_featured_sticker_sets_queries_.size() == 1u) {
    if (G()->parameters().use_file_db) {
      LOG(INFO) << "Trying to load old trending sticker sets from database with offset "
                << old_featured_sticker_set_ids_.size();
      G()->td_db()->get_sqlite_pmc()->get(
          PSTRING() << "sssoldfeatured" << old_featured_sticker_set_ids_.size(),
          PromiseCreator::lambda([generation = old_featured_sticker_set_generation_](string value) {
            send_closure(G()->stickers_manager(), &StickersManager::on_load_old_featured_sticker_sets_from_database,
                         generation, std::move(value));
          }));
    } else {
      LOG(INFO) << "Trying to load old trending sticker sets from server with offset "
                << old_featured_sticker_set_ids_.size();
      reload_old_featured_sticker_sets();
    }
  }
}

void StickersManager::on_update_dice_success_values() {
  if (G()->close_flag()) {
    return;
  }
  if (td_->auth_manager_->is_bot()) {
    G()->shared_config().set_option_empty("dice_success_values");
    return;
  }
  if (!is_inited_) {
    return;
  }

  auto dice_success_values_str =
      G()->shared_config().get_option_string("dice_success_values", "0,6:62,5:110,5:110,5:110,64:110,6:110");
  if (dice_success_values_str == dice_success_values_str_) {
    return;
  }

  LOG(INFO) << "Change dice success values to " << dice_success_values_str;
  dice_success_values_str_ = std::move(dice_success_values_str);
  dice_success_values_ = transform(full_split(dice_success_values_str_, ','), [](Slice value) {
    auto result = split(value, ':');
    return std::make_pair(to_integer<int32>(result.first), to_integer<int32>(result.second));
  });
}

void StickersManager::send_update_animated_emoji_clicked(FullMessageId full_message_id, FileId sticker_id) {
  if (G()->close_flag() || disable_animated_emojis_ || td_->auth_manager_->is_bot()) {
    return;
  }
  if (td_->messages_manager_->is_message_edited_recently(full_message_id, 2)) {
    // includes deleted full_message_id
    return;
  }
  auto dialog_id = full_message_id.get_dialog_id();
  if (!td_->messages_manager_->have_input_peer(dialog_id, AccessRights::Write)) {
    return;
  }

  send_closure(
      G()->td(), &Td::send_update,
      td_api::make_object<td_api::updateAnimatedEmojiMessageClicked>(
          dialog_id.get(), full_message_id.get_message_id().get(), get_sticker_object(sticker_id, false, true)));
}

void StickersManager::add_recent_sticker(bool is_attached, const tl_object_ptr<td_api::InputFile> &input_file,
                                         Promise<Unit> &&promise) {
  if (!are_recent_stickers_loaded_[is_attached]) {
    load_recent_stickers(is_attached, std::move(promise));
    return;
  }

  auto r_file_id = td_->file_manager_->get_input_file_id(FileType::Sticker, input_file, DialogId(), false, false);
  if (r_file_id.is_error()) {
    return promise.set_error(Status::Error(400, r_file_id.error().message()));  // TODO do not drop error code
  }

  add_recent_sticker_impl(is_attached, r_file_id.ok(), true, std::move(promise));
}

vector<FileId> StickersManager::get_animated_emoji_click_stickers(const StickerSet *sticker_set, Slice emoji) const {
  vector<FileId> result;
  for (auto sticker_id : sticker_set->sticker_ids) {
    auto s = get_sticker(sticker_id);
    CHECK(s != nullptr);
    if (remove_emoji_modifiers(s->alt) == emoji) {
      result.push_back(sticker_id);
    }
  }
  if (result.empty()) {
    const static vector<string> heart_emojis{"💛", "💙", "💚", "💜", "🧡", "🖤", "🤎", "🤍"};
    if (td::contains(heart_emojis, emoji)) {
      return get_animated_emoji_click_stickers(sticker_set, Slice("❤"));
    }
  }
  return result;
}

int64 StickersManager::get_featured_sticker_sets_hash() const {
  vector<uint64> numbers;
  numbers.reserve(featured_sticker_set_ids_.size() * 2);
  for (auto sticker_set_id : featured_sticker_set_ids_) {
    const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    CHECK(sticker_set->is_inited);

    numbers.push_back(sticker_set_id.get());

    if (!sticker_set->is_viewed) {
      numbers.push_back(1);
    }
  }
  return get_vector_hash(numbers);
}

vector<StickerSetId> StickersManager::get_attached_sticker_sets(FileId file_id, Promise<Unit> &&promise) {
  if (!file_id.is_valid()) {
    promise.set_error(Status::Error(400, "Wrong file_id specified"));
    return {};
  }

  auto it = attached_sticker_sets_.find(file_id);
  if (it != attached_sticker_sets_.end()) {
    promise.set_value(Unit());
    return it->second;
  }

  send_get_attached_stickers_query(file_id, std::move(promise));
  return {};
}

td_api::object_ptr<td_api::animatedEmoji> StickersManager::get_animated_emoji_object(const string &emoji) {
  auto it = emoji_messages_.find(emoji);
  if (it == emoji_messages_.end()) {
    return get_animated_emoji_object(get_animated_emoji_sticker(emoji), get_animated_emoji_sound_file_id(emoji));
  } else {
    return get_animated_emoji_object(it->second.animated_emoji_sticker, it->second.sound_file_id);
  }
}

void StickersManager::on_uninstall_sticker_set(StickerSetId set_id) {
  StickerSet *sticker_set = get_sticker_set(set_id);
  CHECK(sticker_set != nullptr);
  on_update_sticker_set(sticker_set, false, false, true);
  update_sticker_set(sticker_set, "on_uninstall_sticker_set");
  send_update_installed_sticker_sets();
}

int StickersManager::get_emoji_number(Slice emoji) {
  // '0'-'9' + U+20E3
  auto data = emoji.ubegin();
  if (emoji.size() != 4 || emoji[0] < '0' || emoji[0] > '9' || data[1] != 0xE2 || data[2] != 0x83 || data[3] != 0xA3) {
    return -1;
  }
  return emoji[0] - '0';
}

td_api::object_ptr<td_api::updateFavoriteStickers> StickersManager::get_update_favorite_stickers_object() const {
  return td_api::make_object<td_api::updateFavoriteStickers>(
      td_->file_manager_->get_file_ids_object(favorite_sticker_ids_));
}

string StickersManager::get_emoji_language_codes_database_key(const vector<string> &language_codes) {
  return PSTRING() << "emojilc$" << implode(language_codes, '$');
}
}  // namespace td
