#include "rgw_metrics.h"

RGWMetrics::RGWMetrics(prometheus::Registry& registry) :
    events(prometheus::BuildCounter().Name("rgw_events_total").Help("RGW event count").Register(registry)),
    durations(prometheus::BuildHistogram().Name("rgw_lua_duration_seconds").Help("Lua request duration").Register(registry)),
    luaPreRequests(RGWCounter{RGWLuaRequestType::PREREQUEST, events.Add({{"rgw_lua", "prerequest"}}), durations.Add({}, boundaries)}),
    luaPostRequests(RGWCounter{RGWLuaRequestType::POSTREQUEST, events.Add({{"rgw_lua", "postrequest"}}), durations.Add({}, boundaries)}),
    luaPostAuths(RGWCounter{RGWLuaRequestType::POSTAUTH, events.Add({{"rgw_lua", "postauth"}}), durations.Add({}, boundaries)}) {}

RGWMetricsService::RGWMetricsService()
  : registry(std::make_shared<prometheus::Registry>()),
    counters(*registry),
    exposer("0.0.0.0:9080")
{
  exposer.RegisterCollectable(registry);
}

RGWMetricsService& rgw_metrics_service()
{
  static RGWMetricsService service;
  return service;
}
