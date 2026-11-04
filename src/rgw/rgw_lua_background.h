#pragma once
#include "common/dout.h"
#include "rgw_common.h"
#include <array>
#include <atomic>
#include <optional>
#include <string>
#include <string_view>
#include <set>
#include <type_traits>
#include <variant>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <boost/lockfree/queue.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <tbb/spin_rw_mutex.h>
#include "rgw_lua_utils.h"
#include "rgw_realm_reloader.h"

namespace rgw::lua {

//Interval between each execution of the script is set to 5 seconds
constexpr const int INIT_EXECUTE_INTERVAL = 5;

//Writeable meta table named RGW
using BackgroundMapValue = std::variant<std::string, long long int, double, bool>;

class BackgroundMap {
public:
  static constexpr size_t SHARD_BITS = 8;
  static constexpr size_t NUM_SHARDS = size_t(1) << SHARD_BITS;
  using Entry = std::pair<std::string, BackgroundMapValue>;

  enum class SetResult { ok, too_many_entries };
  enum class IncrementResult { ok, not_numeric };

  std::optional<BackgroundMapValue> get(std::string_view key) const;
  SetResult set(std::string_view key, BackgroundMapValue value);
  void assign(std::string_view key, BackgroundMapValue value);
  void erase(std::string_view key);
  IncrementResult increment(std::string_view key, const BackgroundMapValue& inc_by);
  size_t size() const { return entries.load(std::memory_order_relaxed); }

  bool copy_next_shard(size_t& next_shard, size_t& shard, uint64_t& version,
                       std::vector<Entry>& out) const;
  uint64_t shard_version(size_t shard) const {
    return shards[shard].version.load(std::memory_order_acquire);
  }

private:
  struct HashedKey {
    std::string_view key;
    size_t hash;
  };

  struct KeyHash {
    using is_transparent = void;
    using is_avalanching = std::true_type;
    size_t operator()(std::string_view key) const noexcept {
      return boost::hash<std::string_view>{}(key);
    }
    size_t operator()(const std::string& key) const noexcept {
      return (*this)(std::string_view{key});
    }
    size_t operator()(const HashedKey& key) const noexcept {
      return key.hash;
    }
  };

  struct KeyEqual {
    using is_transparent = void;
    bool operator()(const std::string& a, const std::string& b) const noexcept { return a == b; }
    bool operator()(const std::string& a, const HashedKey& b) const noexcept { return a == b.key; }
    bool operator()(const HashedKey& a, const std::string& b) const noexcept { return a.key == b; }
  };

  struct alignas(64) Shard {
    mutable tbb::spin_rw_mutex mtx;
    boost::unordered_flat_map<std::string, BackgroundMapValue, KeyHash, KeyEqual> map;
    std::atomic<uint64_t> version{0};
    std::atomic<size_t> count{0};

    void changed() {
      version.store(version.load(std::memory_order_relaxed) + 1, std::memory_order_release);
    }
    void resized() {
      count.store(map.size(), std::memory_order_relaxed);
      changed();
    }
  };

  static constexpr size_t SHARD_SHIFT = 32;
  static_assert(sizeof(size_t) == 8, "the shard is taken from the middle of a 64-bit hash");

  static HashedKey hashed(std::string_view key) {
    return {key, KeyHash{}(key)};
  }
  Shard& shard_for(const HashedKey& key) {
    return shards[(key.hash >> SHARD_SHIFT) & (NUM_SHARDS - 1)];
  }
  const Shard& shard_for(const HashedKey& key) const {
    return shards[(key.hash >> SHARD_SHIFT) & (NUM_SHARDS - 1)];
  }

  std::array<Shard, NUM_SHARDS> shards;
  alignas(64) std::atomic<size_t> entries{0};
};

struct RGWTable : EmptyMetaTable {

  static const char* INCREMENT;
  static const char* DECREMENT;

  static int increment_by(lua_State* L);
  static void push_increment_closure(lua_State* L, BackgroundMap* map, bool decrement);

  static bool is_reserved_name(const char* index, size_t len, const char* name) {
    return len == strlen(name) && strcasecmp(index, name) == 0;
  }

  static int IndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    size_t index_len;
    const char* index = luaL_checklstring(L, 2, &index_len);

    if (is_reserved_name(index, index_len, INCREMENT)) {
      push_increment_closure(L, map, false);
      return ONE_RETURNVAL;
    }
    if (is_reserved_name(index, index_len, DECREMENT)) {
      push_increment_closure(L, map, true);
      return ONE_RETURNVAL;
    }

    const auto value = map->get({index, index_len});
    if (!value) {
      lua_pushnil(L);
      return ONE_RETURNVAL;
    }
    std::visit([L](auto&& value) { pushvalue(L, value); }, *value);
    return ONE_RETURNVAL;
  }

  static int LenClosure(lua_State* L) {
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));

    lua_pushinteger(L, map->size());

    return ONE_RETURNVAL;
  }

  static int NewIndexClosure(lua_State* L) {
    std::ignore = table_name_upvalue(L);
    const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
    size_t index_len;
    const char* index = luaL_checklstring(L, 2, &index_len);
    const std::string_view key{index, index_len};

    if (is_reserved_name(index, index_len, INCREMENT) || is_reserved_name(index, index_len, DECREMENT)) {
        return luaL_error(L, "increment/decrement are reserved function names for RGW");
    }

    size_t len;
    BackgroundMapValue value;
    const int value_type = lua_type(L, 3);

    switch (value_type) {
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

    if (len + index_len > MAX_LUA_VALUE_SIZE) {
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
    rgw_map.assign(key, BackgroundMapValue(std::move(value)));
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

