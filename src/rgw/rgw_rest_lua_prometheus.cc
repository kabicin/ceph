#include "rgw_rest_lua_prometheus.h"

#include <sstream>
#include <string>
#include <variant>

#include "rgw_lua_background.h"
#include "rgw_process_env.h"
#include "rgw_rest.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

namespace {

static std::string sanitize_metric_name(const std::string& key) {
  std::string out;
  out.reserve(key.size());
  for (size_t i = 0; i < key.size(); ++i) {
    char c = key[i];
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9' && i > 0) || c == '_' || c == ':') {
      out += c;
    } else {
      out += '_';
    }
  }
  return out;
}

static std::string escape_label_value(const std::string& v) {
  std::string out;
  out.reserve(v.size());
  for (char c : v) {
    if (c == '\\') {
      out += "\\\\";
    } else if (c == '"') {
      out += "\\\"";
    } else if (c == '\n') {
      out += "\\n";
    } else {
      out += c;
    }
  }
  return out;
}

class LuaPrometheusOp : public RGWOp {
  std::string body;
  int64_t body_length = 0;

 public:
  const char* name() const override { return "lua_prometheus_metrics"; }

  int verify_permission(optional_yield) override {
    return 0;
  }

  void execute(optional_yield) override {
    const rgw::lua::Background* bg = s->penv.lua.background;
    if (!bg) {
      op_ret = -ENOENT;
      return;
    }

    const BackgroundMap snapshot = bg->get_background_map_snapshot();

    std::ostringstream oss;
    for (const auto& [key, val] : snapshot) {
      const std::string safe_key = sanitize_metric_name(key);

      std::visit([&](auto&& v) {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, long long int>) {
          oss << "# HELP rgw_lua_" << safe_key << " Lua background table value\n";
          oss << "# TYPE rgw_lua_" << safe_key << " gauge\n";
          oss << "rgw_lua_" << safe_key << " " << v << "\n";
        } else if constexpr (std::is_same_v<T, double>) {
          oss << "# HELP rgw_lua_" << safe_key << " Lua background table value\n";
          oss << "# TYPE rgw_lua_" << safe_key << " gauge\n";
          oss << "rgw_lua_" << safe_key << " " << v << "\n";
        } else if constexpr (std::is_same_v<T, bool>) {
          oss << "# HELP rgw_lua_" << safe_key << " Lua background table value\n";
          oss << "# TYPE rgw_lua_" << safe_key << " gauge\n";
          oss << "rgw_lua_" << safe_key << " " << (v ? 1 : 0) << "\n";
        } else if constexpr (std::is_same_v<T, std::string>) {
          const std::string escaped = escape_label_value(v);
          oss << "# HELP rgw_lua_" << safe_key << " Lua background table value\n";
          oss << "# TYPE rgw_lua_" << safe_key << " untyped\n";
          oss << "rgw_lua_" << safe_key << "{value=\"" << escaped << "\"} 1\n";
        }
      }, val);
    }

    body = oss.str();
    body_length = static_cast<int64_t>(body.size());
  }

  void send_response() override {
    if (op_ret) {
      set_req_state_err(s, op_ret);
      dump_errno(s);
      end_header(s, this);
      return;
    }
    dump_errno(s);
    end_header(s, this, "text/plain; version=0.0.4; charset=utf-8",
               body_length);
    dump_body(s, body);
  }
};

class LuaPrometheusHandler : public RGWHandler_REST {
 public:
  int init_permissions(RGWOp*, optional_yield) override { return 0; }
  int read_permissions(RGWOp*, optional_yield) override { return 0; }
  int authorize(const DoutPrefixProvider*, optional_yield) override { return 0; }
  int postauth_init(optional_yield) override { return 0; }

  RGWOp* op_get() override { return new LuaPrometheusOp(); }
};

} // namespace

RGWHandler_REST*
RESTMgr_LuaPrometheus::get_handler(rgw::sal::Driver*,
                                    req_state*,
                                    const rgw::auth::StrategyRegistry&,
                                    const std::string&)
{
  return new LuaPrometheusHandler();
}

} // namespace rgw::lua
