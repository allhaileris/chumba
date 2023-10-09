//
// Copyright Aliaksei Levin (levlam@telegram.org), Arseny Smirnov (arseny30@gmail.com) 2014-2021
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#include "td/telegram/Td.h"


#include "td/telegram/AnimationsManager.h"
#include "td/telegram/AudiosManager.h"
#include "td/telegram/AuthManager.h"


#include "td/telegram/BackgroundManager.h"


#include "td/telegram/CallbackQueriesManager.h"

#include "td/telegram/CallManager.h"


#include "td/telegram/ConfigManager.h"

#include "td/telegram/ContactsManager.h"
#include "td/telegram/CountryInfoManager.h"
#include "td/telegram/DeviceTokenManager.h"









#include "td/telegram/DocumentsManager.h"
#include "td/telegram/FileReferenceManager.h"


#include "td/telegram/files/FileManager.h"





#include "td/telegram/GameManager.h"


#include "td/telegram/GroupCallManager.h"
#include "td/telegram/HashtagHints.h"
#include "td/telegram/InlineQueriesManager.h"

#include "td/telegram/LanguagePackManager.h"
#include "td/telegram/LinkManager.h"








#include "td/telegram/MessagesManager.h"








#include "td/telegram/net/NetStatsManager.h"






#include "td/telegram/NotificationManager.h"

#include "td/telegram/OptionManager.h"
#include "td/telegram/PasswordManager.h"

#include "td/telegram/PhoneNumberManager.h"


#include "td/telegram/PollManager.h"
#include "td/telegram/PrivacyManager.h"




#include "td/telegram/SecretChatsManager.h"
#include "td/telegram/SecureManager.h"

#include "td/telegram/SponsoredMessageManager.h"
#include "td/telegram/StateManager.h"

#include "td/telegram/StickersManager.h"
#include "td/telegram/StorageManager.h"




#include "td/telegram/ThemeManager.h"

#include "td/telegram/TopDialogManager.h"
#include "td/telegram/UpdatesManager.h"

#include "td/telegram/VideoNotesManager.h"
#include "td/telegram/VideosManager.h"
#include "td/telegram/VoiceNotesManager.h"

#include "td/telegram/WebPagesManager.h"



































#include <type_traits>

namespace td {

#define CLEAN_INPUT_STRING(field_name)                                  \
  if (!clean_input_string(field_name)) {                                \
    return send_error_raw(id, 400, "Strings must be encoded in UTF-8"); \
  }
#define CHECK_IS_BOT()                                              \
  if (!auth_manager_->is_bot()) {                                   \
    return send_error_raw(id, 400, "Only bots can use the method"); \
  }
#define CHECK_IS_USER()                                                     \
  if (auth_manager_->is_bot()) {                                            \
    return send_error_raw(id, 400, "The method is not available for bots"); \
  }

#define CREATE_NO_ARGS_REQUEST(name)                                       \
  auto slot_id = request_actors_.create(ActorOwn<>(), RequestActorIdType); \
  inc_request_actor_refcnt();                                              \
  *request_actors_.get(slot_id) = create_actor<name>(#name, actor_shared(this, slot_id), id);
#define CREATE_REQUEST(name, ...)                                          \
  auto slot_id = request_actors_.create(ActorOwn<>(), RequestActorIdType); \
  inc_request_actor_refcnt();                                              \
  *request_actors_.get(slot_id) = create_actor<name>(#name, actor_shared(this, slot_id), id, __VA_ARGS__);
#define CREATE_REQUEST_PROMISE() auto promise = create_request_promise<std::decay_t<decltype(request)>::ReturnType>(id)
#define CREATE_OK_REQUEST_PROMISE()                                                                                    \
  static_assert(std::is_same<std::decay_t<decltype(request)>::ReturnType, td_api::object_ptr<td_api::ok>>::value, ""); \
  auto promise = create_ok_request_promise(id)

}  // namespace td
namespace td {

Td::~Td() = default;
}  // namespace td
