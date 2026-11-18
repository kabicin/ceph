#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <array>
#include <atomic>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <set>
#include <variant>
#include <shared_mutex>
#include <mutex>
#include <boost/lockfree/queue.hpp>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"
#include <tbb/spin_mutex.h>
#include <tbb/spin_rw_mutex.h>

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW with mutex protection
using BackgroundMapValue = std::variant<std::string, long long int, double, bool>;

class BackgroundMap {
public:
  enum class SetResult { ok, too_many_entries };

  std::optional<BackgroundMapValue> get(std::string_view key) const;
  SetResult set(std::string_view key, BackgroundMapValue value);
  void erase(std::string_view key);
  bool increment(std::string_view key, const BackgroundMapValue& by);
  size_t size() const { return entries.load(std::memory_order_relaxed); }
  std::optional<std::pair<std::string, BackgroundMapValue>> next(
      std::optional<std::string_view> previous) const;

private:
  struct Entry {
    mutable tbb::spin_mutex mtx;
    BackgroundMapValue value;
  };
  struct alignas(64) Shard {
    mutable tbb::spin_rw_mutex mtx;
    std::map<std::string, Entry, std::less<>> map;
    std::atomic<size_t> count = 0;
  };
  static constexpr size_t SHARDS = 256;

  static size_t shard_index(std::string_view key);
  std::array<Shard, SHARDS> shards;
  std::atomic<size_t> entries = 0;
};

struct RGWTable : EmptyMetaTable {

  static const char* INCREMENT;
  static const char* DECREMENT;

  static int increment_by(lua_State* L);

  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    size_t len;
    const char* index = luaL_checklstring(L, 2, &len);

    if (strcasecmp(index, INCREMENT) == 0 || strcasecmp(index, DECREMENT) == 0) {
      lua_pushlightuserdata(L, map);
      lua_pushboolean(L, strcasecmp(index, DECREMENT) == 0);
      lua_pushcclosure(L, increment_by, TWO_UPVALS);
      return ONE_RETURNVAL;
    }

    const auto value = map->get({index, len});
    if (!value) {
      lua_pushnil(L);
      return ONE_RETURNVAL;
    }
    std::visit([L](auto&& v) { pushvalue(L, v); }, *value);
    return ONE_RETURNVAL;
  }

  static int LenClosure(lua_State* L) {
    auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
    lua_pushinteger(L, map->size());
    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    size_t key_len;
    const char* index = luaL_checklstring(L, 2, &key_len);
    const std::string_view key(index, key_len);

    if (strcasecmp(index, INCREMENT) == 0 || strcasecmp(index, DECREMENT) == 0) {
        return luaL_error(L, "increment/decrement are reserved function names for RGW");
    }

    size_t len;
    BackgroundMapValue value;
    switch (lua_type(L, 3)) {
      case LUA_TNIL:
        // erase the element. since in lua: "t[index] = nil" is removing the entry at "t[index]"
        map->erase(key);
        return NO_RETURNVAL;
      case LUA_TBOOLEAN:
        value = static_cast<bool>(lua_toboolean(L, 3));
        len = sizeof(bool);
        break;
      case LUA_TNUMBER:
         if (lua_isinteger(L, 3)) {
          value = lua_tointeger(L, 3);
          len = sizeof(long long int);
         } else {
          value = lua_tonumber(L, 3);
          len = sizeof(double);
         }
         break;
      case LUA_TSTRING:
      {
        const auto str = lua_tolstring(L, 3, &len);
        value = std::string{str, len};
        break;
      }
      default:
        return luaL_error(L, "unsupported value type for RGW table");
    }

    if (len + key_len > MAX_LUA_VALUE_SIZE) {
      return luaL_error(L, "Lua maximum size of entry limit exceeded");
    }
    if (map->set(key, std::move(value)) == BackgroundMap::SetResult::too_many_entries) {
      return luaL_error(L, "Lua max number of entries limit exceeded");
    }
    return NO_RETURNVAL;
  }

  static int PairsClosure(lua_State* L);
  static int iterator_next(lua_State* L);
};

class Background : public RGWRealmReloader::Pauser {
public:
  static const BackgroundMapValue empty_table_value;

private:
  BackgroundMap rgw_map;
  bool stopped = false;
  bool started = false;
  bool paused = false;
  int execute_interval;
  const DoutPrefix dp;
  rgw::sal::LuaManager* lua_manager; 
  CephContext* const cct;
  std::thread runner;
  std::mutex cond_mutex;
  std::mutex pause_mutex;
  std::condition_variable cond;

  std::map<std::string, std::unique_ptr<std::vector<char>>> lua_bytecode_cache; // script-name -> bytecode
//  bool updating = false;
  std::shared_mutex updating_mutex;
  boost::lockfree::queue<std::string*> processing_q{16};

  void run();

  std::string rgw_script;
  int read_script();
  std::unique_ptr<lua_state_guard> initialize_lguard_state();

  void process_scripts();

 public:
  Background(CephContext* _cct,
             rgw::sal::LuaManager* _lua_manager,
             int _execute_interval = INIT_EXECUTE_INTERVAL);

  ~Background() override = default;
  void start();
  void shutdown();
  void create_background_metatable(lua_State* L);
  BackgroundMapValue get_table_value(const std::string& key) const;

  template<typename T>
  void put_table_value(const std::string& key, T value) {
    rgw_map.set(key, value);
  }

  // update the manager after 
  void set_manager(rgw::sal::LuaManager* _lua_manager);
  void pause() override;
  // Does not actually use `Driver` argument.
  void resume(rgw::sal::Driver*) override;

  // for lua bytecode caching
  void process_script_add(std::string script);
  int get_script_bytecode(std::string key, std::vector<char>& lua_bytecode);
};

} //namespace rgw::lua

