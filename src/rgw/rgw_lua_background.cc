#include "driver/rados/rgw_sal_rados.h"
#include "rgw_lua_background.h"
#include "rgw_lua.h"
#include "rgw_lua_utils.h"
#include "rgw_perf_counters.h"
#include "include/ceph_assert.h"
#include <lua.hpp>
#include <optional>

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

std::optional<BackgroundMapValue> BackgroundMap::get(std::string_view key) const {
  const auto hk = hashed(key);
  const auto& shard = shard_for(hk);
  std::shared_lock l(shard.mtx);
  if (const auto it = shard.map.find(hk); it != shard.map.end()) {
    return it->second;
  }
  return std::nullopt;
}

BackgroundMap::SetResult BackgroundMap::set(std::string_view key, BackgroundMapValue value) {
  const auto hk = hashed(key);
  auto& shard = shard_for(hk);
  std::lock_guard l(shard.mtx);
  if (const auto it = shard.map.find(hk); it != shard.map.end()) {
    it->second = std::move(value);
    shard.changed();
    return SetResult::ok;
  }
  if (entries.fetch_add(1, std::memory_order_relaxed) > MAX_LUA_KEY_ENTRIES) {
    entries.fetch_sub(1, std::memory_order_relaxed);
    return SetResult::too_many_entries;
  }
  shard.map.emplace(std::string{key}, std::move(value));
  shard.resized();
  return SetResult::ok;
}

void BackgroundMap::assign(std::string_view key, BackgroundMapValue value) {
  const auto hk = hashed(key);
  auto& shard = shard_for(hk);
  std::lock_guard l(shard.mtx);
  if (const auto it = shard.map.find(hk); it != shard.map.end()) {
    it->second = std::move(value);
    shard.changed();
    return;
  }
  shard.map.emplace(std::string{key}, std::move(value));
  entries.fetch_add(1, std::memory_order_relaxed);
  shard.resized();
}

void BackgroundMap::erase(std::string_view key) {
  const auto hk = hashed(key);
  auto& shard = shard_for(hk);
  std::lock_guard l(shard.mtx);
  if (const auto it = shard.map.find(hk); it != shard.map.end()) {
    shard.map.erase(it);
    entries.fetch_sub(1, std::memory_order_relaxed);
    shard.resized();
  }
}

BackgroundMap::IncrementResult BackgroundMap::increment(std::string_view key, const BackgroundMapValue& inc_by) {
  const auto hk = hashed(key);
  auto& shard = shard_for(hk);
  std::lock_guard l(shard.mtx);
  const auto it = shard.map.find(hk);
  if (it == shard.map.end()) {
    return IncrementResult::ok;
  }
  auto& value = it->second;
  if (std::holds_alternative<double>(value) && std::holds_alternative<double>(inc_by)) {
    value = std::get<double>(value) + std::get<double>(inc_by);
  } else if (std::holds_alternative<long long int>(value) && std::holds_alternative<long long int>(inc_by)) {
    value = std::get<long long int>(value) + std::get<long long int>(inc_by);
  } else if (std::holds_alternative<double>(value) && std::holds_alternative<long long int>(inc_by)) {
    value = std::get<double>(value) + static_cast<double>(std::get<long long int>(inc_by));
  } else if (std::holds_alternative<long long int>(value) && std::holds_alternative<double>(inc_by)) {
    value = static_cast<double>(std::get<long long int>(value)) + std::get<double>(inc_by);
  } else {
    return IncrementResult::not_numeric;
  }
  shard.changed();
  return IncrementResult::ok;
}

bool BackgroundMap::copy_next_shard(size_t& next_shard, size_t& shard, uint64_t& version,
                                    std::vector<Entry>& out) const {
  while (next_shard < NUM_SHARDS) {
    const auto index = next_shard++;
    const auto& s = shards[index];
    if (s.count.load(std::memory_order_relaxed) == 0) {
      continue;
    }
    std::shared_lock l(s.mtx);
    if (s.map.empty()) {
      continue;
    }
    version = s.version.load(std::memory_order_relaxed);
    out.assign(s.map.begin(), s.map.end());
    shard = index;
    return true;
  }
  return false;
}

struct RGWTableIterator {
  std::vector<BackgroundMap::Entry> buffer;
  size_t pos = 0;
  size_t shard = 0;
  size_t next_shard = 0;
  uint64_t version = 0;
};

int RGWTable::PairsClosure(lua_State *L) {
  std::ignore = table_name_upvalue(L);
  auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(SECOND_UPVAL)));
  if(luaL_newmetatable(L, "RGW.Iterator")) {
    lua_pushcfunction(L, [](lua_State* L) {
      static_cast<RGWTableIterator*>(lua_touserdata(L, 1))->~RGWTableIterator();
      return 0;
    });
    lua_setfield(L, -2, "__gc");
  }
  new (lua_newuserdata(L, sizeof(RGWTableIterator))) RGWTableIterator{};
  lua_insert(L, -2);
  lua_setmetatable(L, -2);

  lua_pushlightuserdata(L, map);
  lua_pushcclosure(L, iterator_next, ONE_UPVAL);
  lua_insert(L, -2);
  lua_pushnil(L);
  return THREE_RETURNVALS;
}

int RGWTable::iterator_next(lua_State* L) {
  auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
  auto iterator = static_cast<RGWTableIterator*>(luaL_checkudata(L, 1, "RGW.Iterator"));

  for (;;) {
    while (iterator->pos < iterator->buffer.size()) {
      auto& [key, value] = iterator->buffer[iterator->pos++];
      if (map->shard_version(iterator->shard) != iterator->version) {
        auto current = map->get(key);
        if (!current) {
          continue;
        }
        value = std::move(*current);
      }
      pushstring(L, key);
      std::visit([L](auto&& v) { pushvalue(L, v); }, value);
      return TWO_RETURNVALS;
    }
    iterator->buffer.clear();
    iterator->pos = 0;
    if (!map->copy_next_shard(iterator->next_shard, iterator->shard,
                              iterator->version, iterator->buffer)) {
      iterator->buffer.shrink_to_fit();
      lua_pushnil(L);
      return ONE_RETURNVAL;
    }
  }
}

const char* RGWTable::INCREMENT = "increment";
const char* RGWTable::DECREMENT = "decrement";

void RGWTable::push_increment_closure(lua_State* L, BackgroundMap* map, bool decrement) {
  const void* registry_key = decrement ? DECREMENT : INCREMENT;
  if (lua_rawgetp(L, LUA_REGISTRYINDEX, registry_key) != LUA_TNIL) {
    return;
  }
  lua_pop(L, 1);
  lua_pushlightuserdata(L, map);
  lua_pushboolean(L, decrement);
  lua_pushcclosure(L, increment_by, TWO_UPVALS);
  lua_pushvalue(L, -1);
  lua_rawsetp(L, LUA_REGISTRYINDEX, registry_key);
}

int RGWTable::increment_by(lua_State* L) {
  const auto map = reinterpret_cast<BackgroundMap*>(lua_touserdata(L, lua_upvalueindex(FIRST_UPVAL)));
  auto decrement = lua_toboolean(L, lua_upvalueindex(SECOND_UPVAL));

  const auto args = lua_gettop(L);
  size_t index_len;
  const char* index = luaL_checklstring(L, 1, &index_len);

  // by default we increment by 1/-1
  const long long int default_inc = (decrement ? -1 : 1);
  BackgroundMapValue inc_by = default_inc;
  if (args == 2) {
    if (lua_isinteger(L, 2)) {
      inc_by = lua_tointeger(L, 2)*default_inc;
    } else if (lua_isnumber(L, 2)){
      inc_by = lua_tonumber(L, 2)*static_cast<double>(default_inc);
    } else {
      return luaL_error(L, "can increment only by numeric values");
    }
  }

  if (map->increment({index, index_len}, inc_by) == BackgroundMap::IncrementResult::not_numeric) {
    return luaL_error(L, "can increment only numeric values");
  }

  return 0;
}

static int bytecode_writer (lua_State *L, const void* p, size_t sz, void* ud) {
  std::vector<char>* buffer = static_cast<std::vector<char>*>(ud);
  const char* bytes = static_cast<const char*>(p);
  buffer->insert(buffer->end(), bytes, bytes + sz);
  return 0;
}


Background::Background(
    CephContext* _cct,
    rgw::sal::LuaManager* _lua_manager,
    int _execute_interval) :
    execute_interval(_execute_interval)
    , dp(_cct, dout_subsys, "lua background: ")
    , lua_manager(_lua_manager)
    , cct(_cct)
{}

void Background::shutdown(){
  stopped = true;
  cond.notify_all();
  if (runner.joinable()) {
    runner.join();
  }
  started = false;
  stopped = false;
}

void Background::start() {
  if (started) {
    // start the thread only once
    return;
  }
  started = true;
  runner = std::thread(&Background::run, this);
}

void Background::pause() {
  {
    std::unique_lock cond_lock(pause_mutex);
    paused = true;
  }
  cond.notify_all();
}

void Background::resume(rgw::sal::Driver*) {
  paused = false;
  cond.notify_all();
}

int Background::read_script() {
  std::unique_lock cond_lock(pause_mutex);
  if (paused) {
    return -EAGAIN;
  }
  std::string tenant;
  return rgw::lua::read_script(&dp, lua_manager, tenant, null_yield, rgw::lua::context::background, rgw_script);
}

std::unique_ptr<lua_state_guard> Background::initialize_lguard_state() {
  auto lguard = std::make_unique<lua_state_guard>(
      cct->_conf->rgw_lua_max_memory_per_state,
      cct->_conf->rgw_lua_max_runtime_per_state, &dp);
  lua_State* L = lguard->get();
  if (!L) {
    ldpp_dout(&dp, 1) << "Failed to create state for Lua background thread"
                      << dendl;
    return nullptr;
  }
  try {
    open_standard_libs(L);
    set_package_path(L, lua_manager->luarocks_path());
    create_debug_action(L, cct);
    create_background_metatable(L);
  } catch (const std::runtime_error& e) {
    ldpp_dout(&dp, 1)
        << "Failed to create initial setup of Lua background thread. error "
        << e.what() << dendl;
    return nullptr;
  }
  return lguard;
}

const BackgroundMapValue Background::empty_table_value;

BackgroundMapValue Background::get_table_value(const std::string& key) const {
  return rgw_map.get(key).value_or(empty_table_value);
}

//(1) Loads the script from the object if not paused
//(2) Executes the script
//(3) Sleep (configurable)
void Background::run() {
  ceph_pthread_setname("lua_background");
  const DoutPrefixProvider* const dpp = &dp;

  while (!stopped) {
    if (paused) {
      ldpp_dout(dpp, 10) << "Lua background thread paused" << dendl;
      std::unique_lock cond_lock(cond_mutex);
      cond.wait(cond_lock, [this]{return !paused || stopped;}); 
      if (stopped) {
        ldpp_dout(dpp, 10) << "Lua background thread stopped" << dendl;
        return;
      }
      ldpp_dout(dpp, 10) << "Lua background thread resumed" << dendl;
    }
    std::unique_ptr<lua_state_guard> lguard = initialize_lguard_state();
    if (!lguard) {
      return;
    }
    const auto rc = read_script();
    if (rc == -ENOENT || rc == -EAGAIN) {
      // either no script or paused, nothing to do
    } else if (rc < 0) {
      ldpp_dout(dpp, 1) << "WARNING: failed to read background script. error " << rc << dendl;
    } else {
      auto failed = false;
      auto L = lguard->get();
      try {
        //execute the background lua script
        if (luaL_dostring(L, rgw_script.c_str()) != LUA_OK) {
          const std::string err(lua_tostring(L, -1));
          ldpp_dout(dpp, 1) << "Lua ERROR: " << err << dendl;
          failed = true;
        }
      } catch (const std::runtime_error& e) {
        ldpp_dout(dpp, 1) << "Lua ERROR: " << e.what() << dendl;
        failed = true;
      }
      if (perfcounter) {
        perfcounter->inc((failed ? l_rgw_lua_script_fail : l_rgw_lua_script_ok), 1);
      }
    }
    process_scripts();
    std::unique_lock cond_lock(cond_mutex);
    cond.wait_for(cond_lock, std::chrono::seconds(execute_interval), [this]{return stopped;}); 
  }
  ldpp_dout(dpp, 10) << "Lua background thread stopped" << dendl;
}

void Background::create_background_metatable(lua_State* L) {
  static const char* background_table_name = "RGW";
  create_metatable<RGWTable>(L, "", background_table_name, true, &rgw_map);
  lua_getglobal(L, background_table_name);
  ceph_assert(lua_istable(L, -1));
}

void Background::set_manager(rgw::sal::LuaManager* _lua_manager) {
  lua_manager = _lua_manager;
}

void Background::process_script_add(std::string script_oid) {
  auto script_ptr = make_unique<std::string>(std::move(script_oid));
  if (processing_q.push(script_ptr.get())) {
    script_ptr.release();
  }
}

void Background::process_scripts() {
  std::set<std::string> removed;
  std::set<std::string> updated_scripts;

  const auto count = processing_q.consume_all([&](std::string* s) {
             std::unique_ptr<std::string> sptr(s);
             updated_scripts.insert(*sptr);
          });

  if (updated_scripts.empty()) {
    return;
  }
  ldpp_dout(&dp, 20) << "INFO: Num scripts to process: " << count << dendl;
  //updating = true;
  std::unique_ptr<lua_state_guard> lguard = initialize_lguard_state();
  if (!lguard) {
    return;
  }
  std::string script;
  for (const auto& key: updated_scripts) {
    int r = lua_manager->get_script(&dp, null_yield, key, script);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(&dp, 10) << "ERROR: Failed to get script : " << key
                         << ". r = " << r << dendl;
      // Clear the cache
      removed.insert(key);
      std::unique_lock<std::shared_mutex> lock(updating_mutex);
      lua_bytecode_cache.erase(key);
      continue;
    }
    if (r == -ENOENT) {
      removed.insert(key);
      std::unique_lock<std::shared_mutex> lock(updating_mutex);
      lua_bytecode_cache.erase(key);
      continue;
    }
    ldpp_dout(&dp, 20) << "INFO: processing script: " << key << dendl;
    auto L = lguard->get();
    auto buffer = std::make_unique<std::vector<char>>();
    try {
      if (luaL_loadstring(L, script.c_str()) == LUA_OK) {
        lua_dump(L, bytecode_writer, buffer.get(), 0);
        {
          std::unique_lock<std::shared_mutex> lock(updating_mutex);
          lua_bytecode_cache.insert_or_assign(key, std::move(buffer));
        }
        lua_pop(L, 1); 
        removed.insert(key);
      } else {
        const std::string err(lua_tostring(L, -1));
        ldpp_dout(&dp, 1) << "Lua ERROR: failed to compile script : " << key
                          << ", error : " << err << dendl;
      }
    } catch (const std::runtime_error& e) {
      ldpp_dout(&dp, 1) << "Lua ERROR: failed to compile script : " << key
                        << ", error : " << e.what() << dendl;
    }
  }
  
  //updating = false;
  for (const auto& key: removed) {
    updated_scripts.erase(key);
  }
}

int Background::get_script_bytecode(std::string script, std::vector<char>& lua_bytecode) {
  lua_bytecode.clear();
  std::shared_lock<std::shared_mutex> lock(updating_mutex);
  auto itr = lua_bytecode_cache.find(script);
  if(itr != lua_bytecode_cache.end()) {
    lua_bytecode = *((itr->second).get());
    return 0;
  }

  ldpp_dout(&dp, 20) << "INFO: lua script bytecode not found : " << script << dendl;
  return -ENOENT;
}

} //namespace rgw::lua

