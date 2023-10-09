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


#include "td/telegram/DialogId.h"
#include "td/telegram/Document.h"

#include "td/telegram/FileReferenceManager.h"
#include "td/telegram/files/FileLocation.h"
#include "td/telegram/files/FileManager.h"
#include "td/telegram/files/FileType.h"
#include "td/telegram/Global.h"

#include "td/telegram/logevent/LogEvent.h"

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



#include <unordered_set>

namespace td {
}  // namespace td
namespace td {

class StickersManager::StickerListLogEvent {
 public:
  vector<FileId> sticker_ids;

  StickerListLogEvent() = default;

  explicit StickerListLogEvent(vector<FileId> sticker_ids) : sticker_ids(std::move(sticker_ids)) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    StickersManager *stickers_manager = storer.context()->td().get_actor_unsafe()->stickers_manager_.get();
    td::store(narrow_cast<int32>(sticker_ids.size()), storer);
    for (auto sticker_id : sticker_ids) {
      stickers_manager->store_sticker(sticker_id, false, storer, "StickerListLogEvent");
    }
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    StickersManager *stickers_manager = parser.context()->td().get_actor_unsafe()->stickers_manager_.get();
    int32 size = parser.fetch_int();
    sticker_ids.resize(size);
    for (auto &sticker_id : sticker_ids) {
      sticker_id = stickers_manager->parse_sticker(false, parser);
    }
  }
};

class StickersManager::StickerSetListLogEvent {
 public:
  vector<StickerSetId> sticker_set_ids;

  StickerSetListLogEvent() = default;

  explicit StickerSetListLogEvent(vector<StickerSetId> sticker_set_ids) : sticker_set_ids(std::move(sticker_set_ids)) {
  }

  template <class StorerT>
  void store(StorerT &storer) const {
    td::store(sticker_set_ids, storer);
  }

  template <class ParserT>
  void parse(ParserT &parser) {
    td::parse(sticker_set_ids, parser);
  }
};

void StickersManager::on_load_installed_sticker_sets_from_database(bool is_masks, string value) {
  if (G()->close_flag()) {
    return;
  }
  if (value.empty()) {
    LOG(INFO) << "Installed " << (is_masks ? "mask " : "") << "sticker sets aren't found in database";
    reload_installed_sticker_sets(is_masks, true);
    return;
  }

  LOG(INFO) << "Successfully loaded installed " << (is_masks ? "mask " : "") << "sticker set list of size "
            << value.size() << " from database";

  StickerSetListLogEvent log_event;
  auto status = log_event_parse(log_event, value);
  if (status.is_error()) {
    // can't happen unless database is broken
    LOG(ERROR) << "Can't load installed sticker set list: " << status << ' ' << format::as_hex_dump<4>(Slice(value));
    return reload_installed_sticker_sets(is_masks, true);
  }

  vector<StickerSetId> sets_to_load;
  for (auto sticker_set_id : log_event.sticker_set_ids) {
    StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    if (!sticker_set->is_inited) {
      sets_to_load.push_back(sticker_set_id);
    }
  }
  std::reverse(sets_to_load.begin(), sets_to_load.end());  // load installed sticker sets in reverse order

  load_sticker_sets_without_stickers(
      std::move(sets_to_load),
      PromiseCreator::lambda(
          [is_masks, sticker_set_ids = std::move(log_event.sticker_set_ids)](Result<> result) mutable {
            if (result.is_ok()) {
              send_closure(G()->stickers_manager(), &StickersManager::on_load_installed_sticker_sets_finished, is_masks,
                           std::move(sticker_set_ids), true);
            } else {
              send_closure(G()->stickers_manager(), &StickersManager::reload_installed_sticker_sets, is_masks, true);
            }
          }));
}

string StickersManager::get_sticker_set_database_value(const StickerSet *s, bool with_stickers, const char *source) {
  LogEventStorerCalcLength storer_calc_length;
  store_sticker_set(s, with_stickers, storer_calc_length, source);

  BufferSlice value_buffer{storer_calc_length.get_length()};
  auto value = value_buffer.as_slice();

  LOG(DEBUG) << "Serialized size of " << s->id << " is " << value.size();

  LogEventStorerUnsafe storer_unsafe(value.ubegin());
  store_sticker_set(s, with_stickers, storer_unsafe, source);

  return value.str();
}

void StickersManager::on_load_sticker_set_from_database(StickerSetId sticker_set_id, bool with_stickers, string value) {
  if (G()->close_flag()) {
    return;
  }
  StickerSet *sticker_set = get_sticker_set(sticker_set_id);
  CHECK(sticker_set != nullptr);
  if (sticker_set->was_loaded) {
    LOG(INFO) << "Receive from database previously loaded " << sticker_set_id;
    return;
  }
  if (!with_stickers && sticker_set->is_inited) {
    LOG(INFO) << "Receive from database previously inited " << sticker_set_id;
    return;
  }

  // it is possible that a server reload_sticker_set request has failed and cleared requests list with an error
  if (with_stickers) {
    // CHECK(!sticker_set->load_requests.empty());
  } else {
    // CHECK(!sticker_set->load_without_stickers_requests.empty());
  }

  if (value.empty()) {
    LOG(INFO) << "Failed to find in the database " << sticker_set_id;
    return do_reload_sticker_set(sticker_set_id, get_input_sticker_set(sticker_set), 0, Auto());
  }

  LOG(INFO) << "Successfully loaded " << sticker_set_id << " with" << (with_stickers ? "" : "out")
            << " stickers of size " << value.size() << " from database";

  auto old_sticker_count = sticker_set->sticker_ids.size();

  {
    LOG_IF(ERROR, sticker_set->is_changed) << sticker_set_id << " with" << (with_stickers ? "" : "out")
                                           << " stickers was changed before it is loaded from database";
    LogEventParser parser(value);
    parse_sticker_set(sticker_set, parser);
    LOG_IF(ERROR, sticker_set->is_changed)
        << sticker_set_id << " with" << (with_stickers ? "" : "out") << " stickers is changed";
    parser.fetch_end();
    auto status = parser.get_status();
    if (status.is_error()) {
      G()->td_db()->get_sqlite_sync_pmc()->erase(with_stickers ? get_full_sticker_set_database_key(sticker_set_id)
                                                               : get_sticker_set_database_key(sticker_set_id));
      // need to crash, because the current StickerSet state is spoiled by parse_sticker_set
      LOG(FATAL) << "Failed to parse " << sticker_set_id << ": " << status << ' '
                 << format::as_hex_dump<4>(Slice(value));
    }
  }
  if (!sticker_set->is_thumbnail_reloaded || !sticker_set->are_legacy_sticker_thumbnails_reloaded) {
    do_reload_sticker_set(sticker_set_id, get_input_sticker_set(sticker_set), 0, Auto());
  }

  if (with_stickers && old_sticker_count < 5 && old_sticker_count < sticker_set->sticker_ids.size()) {
    sticker_set->need_save_to_database = true;
    update_sticker_set(sticker_set, "on_load_sticker_set_from_database");
  }

  update_load_requests(sticker_set, with_stickers, Status::OK());
}

void StickersManager::on_get_featured_sticker_sets(
    int32 offset, int32 limit, uint32 generation,
    tl_object_ptr<telegram_api::messages_FeaturedStickers> &&sticker_sets_ptr) {
  if (offset < 0) {
    next_featured_sticker_sets_load_time_ = Time::now_cached() + Random::fast(30 * 60, 50 * 60);
  }

  int32 constructor_id = sticker_sets_ptr->get_id();
  if (constructor_id == telegram_api::messages_featuredStickersNotModified::ID) {
    LOG(INFO) << "Trending sticker sets are not modified";
    auto *stickers = static_cast<const telegram_api::messages_featuredStickersNotModified *>(sticker_sets_ptr.get());
    if (offset >= 0 && generation == old_featured_sticker_set_generation_) {
      set_old_featured_sticker_set_count(stickers->count_);
      fix_old_featured_sticker_set_count();
    }
    send_update_featured_sticker_sets();
    return;
  }
  CHECK(constructor_id == telegram_api::messages_featuredStickers::ID);
  auto featured_stickers = move_tl_object_as<telegram_api::messages_featuredStickers>(sticker_sets_ptr);

  if (offset >= 0 && generation == old_featured_sticker_set_generation_) {
    set_old_featured_sticker_set_count(featured_stickers->count_);
    // the count will be fixed in on_load_old_featured_sticker_sets_finished
  }

  std::unordered_set<StickerSetId, StickerSetIdHash> unread_sticker_set_ids;
  for (auto &unread_sticker_set_id : featured_stickers->unread_) {
    unread_sticker_set_ids.insert(StickerSetId(unread_sticker_set_id));
  }

  vector<StickerSetId> featured_sticker_set_ids;
  for (auto &sticker_set : featured_stickers->sets_) {
    StickerSetId set_id = on_get_sticker_set_covered(std::move(sticker_set), true, "on_get_featured_sticker_sets");
    if (!set_id.is_valid()) {
      continue;
    }

    auto set = get_sticker_set(set_id);
    CHECK(set != nullptr);
    bool is_viewed = unread_sticker_set_ids.count(set_id) == 0;
    if (is_viewed != set->is_viewed) {
      set->is_viewed = is_viewed;
      set->is_changed = true;
    }

    update_sticker_set(set, "on_get_archived_sticker_sets 2");

    featured_sticker_set_ids.push_back(set_id);
  }

  send_update_installed_sticker_sets();

  if (offset >= 0) {
    if (generation == old_featured_sticker_set_generation_) {
      if (G()->parameters().use_file_db && !G()->close_flag()) {
        LOG(INFO) << "Save old trending sticker sets to database with offset " << old_featured_sticker_set_ids_.size();
        CHECK(old_featured_sticker_set_ids_.size() % OLD_FEATURED_STICKER_SET_SLICE_SIZE == 0);
        StickerSetListLogEvent log_event(featured_sticker_set_ids);
        G()->td_db()->get_sqlite_pmc()->set(PSTRING() << "sssoldfeatured" << old_featured_sticker_set_ids_.size(),
                                            log_event_store(log_event).as_slice().str(), Auto());
      }
      on_load_old_featured_sticker_sets_finished(generation, std::move(featured_sticker_set_ids));
    }

    send_update_featured_sticker_sets();  // because of changed count
    return;
  }

  on_load_featured_sticker_sets_finished(std::move(featured_sticker_set_ids));

  LOG_IF(ERROR, featured_sticker_sets_hash_ != featured_stickers->hash_) << "Trending sticker sets hash mismatch";

  if (!G()->parameters().use_file_db || G()->close_flag()) {
    return;
  }

  LOG(INFO) << "Save trending sticker sets to database";
  StickerSetListLogEvent log_event(featured_sticker_set_ids_);
  G()->td_db()->get_sqlite_pmc()->set("sssfeatured", log_event_store(log_event).as_slice().str(), Auto());
}

void StickersManager::on_load_featured_sticker_sets_from_database(string value) {
  if (G()->close_flag()) {
    return;
  }
  if (value.empty()) {
    LOG(INFO) << "Trending sticker sets aren't found in database";
    reload_featured_sticker_sets(true);
    return;
  }

  LOG(INFO) << "Successfully loaded trending sticker set list of size " << value.size() << " from database";

  StickerSetListLogEvent log_event;
  auto status = log_event_parse(log_event, value);
  if (status.is_error()) {
    // can't happen unless database is broken
    LOG(ERROR) << "Can't load trending sticker set list: " << status << ' ' << format::as_hex_dump<4>(Slice(value));
    return reload_featured_sticker_sets(true);
  }

  vector<StickerSetId> sets_to_load;
  for (auto sticker_set_id : log_event.sticker_set_ids) {
    StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    if (!sticker_set->is_inited) {
      sets_to_load.push_back(sticker_set_id);
    }
  }

  load_sticker_sets_without_stickers(
      std::move(sets_to_load),
      PromiseCreator::lambda([sticker_set_ids = std::move(log_event.sticker_set_ids)](Result<> result) mutable {
        if (result.is_ok()) {
          send_closure(G()->stickers_manager(), &StickersManager::on_load_featured_sticker_sets_finished,
                       std::move(sticker_set_ids));
        } else {
          send_closure(G()->stickers_manager(), &StickersManager::reload_featured_sticker_sets, true);
        }
      }));
}

void StickersManager::on_load_old_featured_sticker_sets_from_database(uint32 generation, string value) {
  if (G()->close_flag()) {
    return;
  }
  if (generation != old_featured_sticker_set_generation_) {
    return;
  }
  if (value.empty()) {
    LOG(INFO) << "Old trending sticker sets aren't found in database";
    return reload_old_featured_sticker_sets();
  }

  LOG(INFO) << "Successfully loaded old trending sticker set list of size " << value.size()
            << " from database with offset " << old_featured_sticker_set_ids_.size();

  StickerSetListLogEvent log_event;
  auto status = log_event_parse(log_event, value);
  if (status.is_error()) {
    // can't happen unless database is broken
    LOG(ERROR) << "Can't load old trending sticker set list: " << status << ' ' << format::as_hex_dump<4>(Slice(value));
    return reload_old_featured_sticker_sets();
  }

  vector<StickerSetId> sets_to_load;
  for (auto sticker_set_id : log_event.sticker_set_ids) {
    StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    if (!sticker_set->is_inited) {
      sets_to_load.push_back(sticker_set_id);
    }
  }

  load_sticker_sets_without_stickers(
      std::move(sets_to_load),
      PromiseCreator::lambda(
          [generation, sticker_set_ids = std::move(log_event.sticker_set_ids)](Result<> result) mutable {
            if (result.is_ok()) {
              send_closure(G()->stickers_manager(), &StickersManager::on_load_old_featured_sticker_sets_finished,
                           generation, std::move(sticker_set_ids));
            } else {
              send_closure(G()->stickers_manager(), &StickersManager::reload_old_featured_sticker_sets, generation);
            }
          }));
}

void StickersManager::send_update_installed_sticker_sets(bool from_database) {
  for (int is_masks = 0; is_masks < 2; is_masks++) {
    if (need_update_installed_sticker_sets_[is_masks]) {
      need_update_installed_sticker_sets_[is_masks] = false;
      if (are_installed_sticker_sets_loaded_[is_masks]) {
        installed_sticker_sets_hash_[is_masks] = get_sticker_sets_hash(installed_sticker_set_ids_[is_masks]);
        send_closure(G()->td(), &Td::send_update, get_update_installed_sticker_sets_object(is_masks));

        if (G()->parameters().use_file_db && !from_database && !G()->close_flag()) {
          LOG(INFO) << "Save installed " << (is_masks ? "mask " : "") << "sticker sets to database";
          StickerSetListLogEvent log_event(installed_sticker_set_ids_[is_masks]);
          G()->td_db()->get_sqlite_pmc()->set(is_masks ? "sss1" : "sss0", log_event_store(log_event).as_slice().str(),
                                              Auto());
        }
      }
    }
  }
}

void StickersManager::on_load_recent_stickers_from_database(bool is_attached, string value) {
  if (G()->close_flag()) {
    return;
  }
  if (value.empty()) {
    LOG(INFO) << "Recent " << (is_attached ? "attached " : "") << "stickers aren't found in database";
    reload_recent_stickers(is_attached, true);
    return;
  }

  LOG(INFO) << "Successfully loaded recent " << (is_attached ? "attached " : "") << "stickers list of size "
            << value.size() << " from database";

  StickerListLogEvent log_event;
  auto status = log_event_parse(log_event, value);
  if (status.is_error()) {
    // can't happen unless database is broken, but has been seen in the wild
    LOG(ERROR) << "Can't load recent stickers: " << status << ' ' << format::as_hex_dump<4>(Slice(value));
    return reload_recent_stickers(is_attached, true);
  }

  on_load_recent_stickers_finished(is_attached, std::move(log_event.sticker_ids), true);
}

void StickersManager::save_recent_stickers_to_database(bool is_attached) {
  if (G()->parameters().use_file_db && !G()->close_flag()) {
    LOG(INFO) << "Save recent " << (is_attached ? "attached " : "") << "stickers to database";
    StickerListLogEvent log_event(recent_sticker_ids_[is_attached]);
    G()->td_db()->get_sqlite_pmc()->set(is_attached ? "ssr1" : "ssr0", log_event_store(log_event).as_slice().str(),
                                        Auto());
  }
}

void StickersManager::on_load_favorite_stickers_from_database(const string &value) {
  if (G()->close_flag()) {
    return;
  }
  if (value.empty()) {
    LOG(INFO) << "Favorite stickers aren't found in database";
    reload_favorite_stickers(true);
    return;
  }

  LOG(INFO) << "Successfully loaded favorite stickers list of size " << value.size() << " from database";

  StickerListLogEvent log_event;
  auto status = log_event_parse(log_event, value);
  if (status.is_error()) {
    // can't happen unless database is broken, but has been seen in the wild
    LOG(ERROR) << "Can't load favorite stickers: " << status << ' ' << format::as_hex_dump<4>(Slice(value));
    return reload_favorite_stickers(true);
  }

  on_load_favorite_stickers_finished(std::move(log_event.sticker_ids), true);
}

void StickersManager::save_favorite_stickers_to_database() {
  if (G()->parameters().use_file_db && !G()->close_flag()) {
    LOG(INFO) << "Save favorite stickers to database";
    StickerListLogEvent log_event(favorite_sticker_ids_);
    G()->td_db()->get_sqlite_pmc()->set("ssfav", log_event_store(log_event).as_slice().str(), Auto());
  }
}

vector<string> StickersManager::search_emojis(const string &text, bool exact_match,
                                              const vector<string> &input_language_codes, bool force,
                                              Promise<Unit> &&promise) {
  if (text.empty() || !G()->parameters().use_file_db /* have SQLite PMC */) {
    promise.set_value(Unit());
    return {};
  }

  auto language_codes = get_emoji_language_codes(input_language_codes, text, promise);
  if (language_codes.empty()) {
    // promise was consumed
    return {};
  }

  vector<string> languages_to_load;
  for (auto &language_code : language_codes) {
    auto version = get_emoji_language_code_version(language_code);
    if (version == 0) {
      languages_to_load.push_back(language_code);
    } else {
      LOG(DEBUG) << "Found language " << language_code << " with version " << version;
    }
  }

  if (!languages_to_load.empty()) {
    if (!force) {
      MultiPromiseActorSafe mpas{"LoadEmojiLanguagesMultiPromiseActor"};
      mpas.add_promise(std::move(promise));

      auto lock = mpas.get_promise();
      for (auto &language_code : languages_to_load) {
        load_emoji_keywords(language_code, mpas.get_promise());
      }
      lock.set_value(Unit());
      return {};
    } else {
      LOG(ERROR) << "Have no " << languages_to_load << " emoji keywords";
    }
  }

  auto text_lowered = utf8_to_lower(text);
  vector<string> result;
  for (auto &language_code : language_codes) {
    combine(result, search_language_emojis(language_code, text_lowered, exact_match));
  }

  td::unique(result);

  promise.set_value(Unit());
  return result;
}

void StickersManager::load_sticker_sets(vector<StickerSetId> &&sticker_set_ids, Promise<Unit> &&promise) {
  if (sticker_set_ids.empty()) {
    promise.set_value(Unit());
    return;
  }

  auto load_request_id = current_sticker_set_load_request_++;
  StickerSetLoadRequest &load_request = sticker_set_load_requests_[load_request_id];
  load_request.promise = std::move(promise);
  load_request.left_queries = sticker_set_ids.size();

  for (auto sticker_set_id : sticker_set_ids) {
    StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    CHECK(!sticker_set->is_loaded);

    sticker_set->load_requests.push_back(load_request_id);
    if (sticker_set->load_requests.size() == 1u) {
      if (G()->parameters().use_file_db && !sticker_set->was_loaded) {
        LOG(INFO) << "Trying to load " << sticker_set_id << " with stickers from database";
        G()->td_db()->get_sqlite_pmc()->get(
            get_full_sticker_set_database_key(sticker_set_id), PromiseCreator::lambda([sticker_set_id](string value) {
              send_closure(G()->stickers_manager(), &StickersManager::on_load_sticker_set_from_database, sticker_set_id,
                           true, std::move(value));
            }));
      } else {
        LOG(INFO) << "Trying to load " << sticker_set_id << " with stickers from server";
        do_reload_sticker_set(sticker_set_id, get_input_sticker_set(sticker_set), 0, Auto());
      }
    }
  }
}

void StickersManager::on_get_special_sticker_set(const SpecialStickerSetType &type, StickerSetId sticker_set_id) {
  auto s = get_sticker_set(sticker_set_id);
  CHECK(s != nullptr);
  CHECK(s->is_inited);
  CHECK(s->is_loaded);

  LOG(INFO) << "Receive special sticker set " << type.type_ << ": " << sticker_set_id << ' ' << s->access_hash << ' '
            << s->short_name;
  auto &sticker_set = add_special_sticker_set(type);
  if (sticker_set_id == sticker_set.id_ && s->access_hash == sticker_set.access_hash_ &&
      s->short_name == sticker_set.short_name_ && !s->short_name.empty()) {
    on_load_special_sticker_set(type, Status::OK());
    return;
  }

  sticker_set.id_ = sticker_set_id;
  sticker_set.access_hash_ = s->access_hash;
  sticker_set.short_name_ = clean_username(s->short_name);
  sticker_set.type_ = type;

  G()->td_db()->get_binlog_pmc()->set(type.type_, PSTRING() << sticker_set.id_.get() << ' ' << sticker_set.access_hash_
                                                            << ' ' << sticker_set.short_name_);
  if (type == SpecialStickerSetType::animated_emoji()) {
    try_update_animated_emoji_messages();
  } else if (!type.get_dice_emoji().empty()) {
    sticker_set.is_being_loaded_ = true;
  }
  on_load_special_sticker_set(type, Status::OK());
}

StickerSetId StickersManager::get_sticker_set_id(const tl_object_ptr<telegram_api::InputStickerSet> &set_ptr) {
  CHECK(set_ptr != nullptr);
  switch (set_ptr->get_id()) {
    case telegram_api::inputStickerSetEmpty::ID:
      return StickerSetId();
    case telegram_api::inputStickerSetID::ID:
      return StickerSetId(static_cast<const telegram_api::inputStickerSetID *>(set_ptr.get())->id_);
    case telegram_api::inputStickerSetShortName::ID:
      LOG(ERROR) << "Receive sticker set by its short name";
      return search_sticker_set(static_cast<const telegram_api::inputStickerSetShortName *>(set_ptr.get())->short_name_,
                                Auto());
    case telegram_api::inputStickerSetAnimatedEmoji::ID:
    case telegram_api::inputStickerSetAnimatedEmojiAnimations::ID:
      LOG(ERROR) << "Receive special sticker set " << to_string(set_ptr);
      return add_special_sticker_set(SpecialStickerSetType(set_ptr)).id_;
    case telegram_api::inputStickerSetDice::ID:
      LOG(ERROR) << "Receive special sticker set " << to_string(set_ptr);
      return StickerSetId();
    default:
      UNREACHABLE();
      return StickerSetId();
  }
}

void StickersManager::load_favorite_stickers(Promise<Unit> &&promise) {
  if (td_->auth_manager_->is_bot()) {
    are_favorite_stickers_loaded_ = true;
  }
  if (are_favorite_stickers_loaded_) {
    promise.set_value(Unit());
    return;
  }
  load_favorite_stickers_queries_.push_back(std::move(promise));
  if (load_favorite_stickers_queries_.size() == 1u) {
    if (G()->parameters().use_file_db) {
      LOG(INFO) << "Trying to load favorite stickers from database";
      G()->td_db()->get_sqlite_pmc()->get("ssfav", PromiseCreator::lambda([](string value) {
                                            send_closure(G()->stickers_manager(),
                                                         &StickersManager::on_load_favorite_stickers_from_database,
                                                         std::move(value));
                                          }));
    } else {
      LOG(INFO) << "Trying to load favorite stickers from server";
      reload_favorite_stickers(true);
    }
  }
}

void StickersManager::send_update_favorite_stickers(bool from_database) {
  if (are_favorite_stickers_loaded_) {
    vector<FileId> new_favorite_sticker_file_ids;
    for (auto &sticker_id : favorite_sticker_ids_) {
      append(new_favorite_sticker_file_ids, get_sticker_file_ids(sticker_id));
    }
    std::sort(new_favorite_sticker_file_ids.begin(), new_favorite_sticker_file_ids.end());
    if (new_favorite_sticker_file_ids != favorite_sticker_file_ids_) {
      td_->file_manager_->change_files_source(get_favorite_stickers_file_source_id(), favorite_sticker_file_ids_,
                                              new_favorite_sticker_file_ids);
      favorite_sticker_file_ids_ = std::move(new_favorite_sticker_file_ids);
    }

    send_closure(G()->td(), &Td::send_update, get_update_favorite_stickers_object());

    if (!from_database) {
      save_favorite_stickers_to_database();
    }
  }
}

vector<string> StickersManager::search_language_emojis(const string &language_code, const string &text,
                                                       bool exact_match) {
  LOG(INFO) << "Search for \"" << text << "\" in language " << language_code;
  auto key = get_language_emojis_database_key(language_code, text);
  if (exact_match) {
    string emojis = G()->td_db()->get_sqlite_sync_pmc()->get(key);
    return full_split(emojis, '$');
  } else {
    vector<string> result;
    G()->td_db()->get_sqlite_sync_pmc()->get_by_prefix(key, [&result](Slice key, Slice value) {
      for (auto &emoji : full_split(value, '$')) {
        result.push_back(emoji.str());
      }
      return true;
    });
    return result;
  }
}

void StickersManager::on_update_favorite_stickers_limit(int32 favorite_stickers_limit) {
  if (favorite_stickers_limit != favorite_stickers_limit_) {
    if (favorite_stickers_limit > 0) {
      LOG(INFO) << "Update favorite stickers limit to " << favorite_stickers_limit;
      favorite_stickers_limit_ = favorite_stickers_limit;
      if (static_cast<int32>(favorite_sticker_ids_.size()) > favorite_stickers_limit) {
        favorite_sticker_ids_.resize(favorite_stickers_limit);
        send_update_favorite_stickers();
      }
    } else {
      LOG(ERROR) << "Receive wrong favorite stickers limit = " << favorite_stickers_limit;
    }
  }
}

td_api::object_ptr<td_api::CheckStickerSetNameResult> StickersManager::get_check_sticker_set_name_result_object(
    CheckStickerSetNameResult result) {
  switch (result) {
    case CheckStickerSetNameResult::Ok:
      return td_api::make_object<td_api::checkStickerSetNameResultOk>();
    case CheckStickerSetNameResult::Invalid:
      return td_api::make_object<td_api::checkStickerSetNameResultNameInvalid>();
    case CheckStickerSetNameResult::Occupied:
      return td_api::make_object<td_api::checkStickerSetNameResultNameOccupied>();
    default:
      UNREACHABLE();
      return nullptr;
  }
}

void StickersManager::on_load_favorite_stickers_finished(vector<FileId> &&favorite_sticker_ids, bool from_database) {
  if (static_cast<int32>(favorite_sticker_ids.size()) > favorite_stickers_limit_) {
    favorite_sticker_ids.resize(favorite_stickers_limit_);
  }
  favorite_sticker_ids_ = std::move(favorite_sticker_ids);
  are_favorite_stickers_loaded_ = true;
  send_update_favorite_stickers(from_database);
  auto promises = std::move(load_favorite_stickers_queries_);
  load_favorite_stickers_queries_.clear();
  for (auto &promise : promises) {
    promise.set_value(Unit());
  }
}

int64 StickersManager::get_sticker_sets_hash(const vector<StickerSetId> &sticker_set_ids) const {
  vector<uint64> numbers;
  numbers.reserve(sticker_set_ids.size());
  for (auto sticker_set_id : sticker_set_ids) {
    const StickerSet *sticker_set = get_sticker_set(sticker_set_id);
    CHECK(sticker_set != nullptr);
    CHECK(sticker_set->is_inited);
    numbers.push_back(sticker_set->hash);
  }
  return get_vector_hash(numbers);
}

FileSourceId StickersManager::get_recent_stickers_file_source_id(int is_attached) {
  if (!recent_stickers_file_source_id_[is_attached].is_valid()) {
    recent_stickers_file_source_id_[is_attached] =
        td_->file_reference_manager_->create_recent_stickers_file_source(is_attached != 0);
  }
  return recent_stickers_file_source_id_[is_attached];
}

td_api::object_ptr<td_api::httpUrl> StickersManager::get_emoji_suggestions_url_result(int64 random_id) {
  auto it = emoji_suggestions_urls_.find(random_id);
  CHECK(it != emoji_suggestions_urls_.end());
  auto result = td_api::make_object<td_api::httpUrl>(it->second);
  emoji_suggestions_urls_.erase(it);
  return result;
}

td_api::object_ptr<td_api::updateInstalledStickerSets> StickersManager::get_update_installed_sticker_sets_object(
    int is_masks) const {
  return td_api::make_object<td_api::updateInstalledStickerSets>(
      is_masks != 0, convert_sticker_set_ids(installed_sticker_set_ids_[is_masks]));
}

vector<StickerSetId> StickersManager::convert_sticker_set_ids(const vector<int64> &sticker_set_ids) {
  return transform(sticker_set_ids, [](int64 sticker_set_id) { return StickerSetId(sticker_set_id); });
}

FileId StickersManager::get_sticker_thumbnail_file_id(FileId file_id) const {
  auto sticker = get_sticker(file_id);
  CHECK(sticker != nullptr);
  return sticker->s_thumbnail.file_id;
}

string StickersManager::get_sticker_set_database_key(StickerSetId set_id) {
  return PSTRING() << "ss" << set_id.get();
}
}  // namespace td
