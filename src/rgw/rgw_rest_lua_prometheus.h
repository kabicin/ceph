#pragma once

#include "rgw_rest.h"

namespace rgw::lua {

class RESTMgr_LuaPrometheus : public RGWRESTMgr {
 public:
  RGWHandler_REST* get_handler(rgw::sal::Driver* driver,
                               req_state* s,
                               const rgw::auth::StrategyRegistry& auth,
                               const std::string& prefix) override;
};

} // namespace rgw::lua
