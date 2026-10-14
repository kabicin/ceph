#pragma once

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/histogram.h>

#include <memory>

enum RGWLuaRequestType {
  PREREQUEST,
  POSTREQUEST,
  POSTAUTH
};

struct RGWCounter {
  RGWLuaRequestType type;
  prometheus::Counter& count;
  prometheus::Histogram& duration;
};

struct RGWMetrics {
private:
  prometheus::Family<prometheus::Counter>& events;
  prometheus::Family<prometheus::Histogram>& durations;
  prometheus::Histogram::BucketBoundaries boundaries{0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500};
public:
  explicit RGWMetrics(prometheus::Registry& registry);
  RGWCounter luaPreRequests;
  RGWCounter luaPostRequests;
  RGWCounter luaPostAuths;
};

class RGWMetricsService {
  std::shared_ptr<prometheus::Registry> registry;
  RGWMetrics counters;
  prometheus::Exposer exposer;

public:
  RGWMetricsService();

  RGWMetrics& get_counters() {
    return counters;
  }

  const std::shared_ptr<prometheus::Registry>& get_registry() const {
    return registry;
  }
};

RGWMetricsService& rgw_metrics_service();
