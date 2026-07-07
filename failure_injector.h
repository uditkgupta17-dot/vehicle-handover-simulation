#ifndef FAILURE_INJECTOR_H
#define FAILURE_INJECTOR_H

#include <atomic>
#include <chrono>
#include <string>
#include <unordered_map>
#include "context_vector.h"
#include "scenario_config.h"

// Deterministically perturbs a ContextVector to simulate operational
// failures for repeatable experiments: bandwidth drop, packet loss spike,
// CPU overload, memory shortage, early vehicle exit, prediction failure,
// and peer-server unavailability.
//
// Each fault can be scheduled ahead of time via scenario.conf (fires once
// "at_s" seconds in), OR forced on/off live at any time via setOverride()
// - the Vehicle Controller's "fail <name> <on|off>" command calls this.
// A live override always wins over the scheduled timer.
//
// Applied AFTER ContextMonitor sampling and BEFORE StateAnalyzer /
// DecisionEngine see the vector, so neither of those modules - nor
// ContextMonitor itself - needs any awareness that failure injection exists.
// The Decision Engine reacts to the resulting cost changes exactly as it
// would to a real degradation, which is what causes it to pick another
// continuity strategy automatically.
class FailureInjector
{
public:
    explicit FailureInjector(const ScenarioConfig& cfg);

    ContextVector apply(ContextVector cv) const;

    // fault_name matches the scenario.conf key prefix, e.g. "bandwidth_drop".
    // Unknown names are ignored. Thread-safe; callable from the controller
    // listener thread while apply() runs concurrently on the decision tick.
    void setOverride(const std::string& fault_name, bool enabled);

private:
    enum class OverrideState { FOLLOW_CONFIG = 0, FORCE_ON = 1, FORCE_OFF = 2 };

    struct TimedFault
    {
        bool   enabled = false;
        double at_s    = -1.0;
        double value   = 0.0;
        std::atomic<int> live_override{static_cast<int>(OverrideState::FOLLOW_CONFIG)};
    };

    std::chrono::steady_clock::time_point start_time_;

    TimedFault bandwidth_drop_;
    TimedFault packet_loss_spike_;
    TimedFault cpu_overload_;
    TimedFault memory_shortage_;
    TimedFault vehicle_exit_early_;
    TimedFault prediction_failure_;
    TimedFault server_unavailable_;

    std::unordered_map<std::string, TimedFault*> faults_by_name_;

    double elapsedSeconds() const;
    static bool isActive(const TimedFault& f, double elapsed_s);
};

#endif
