#include "failure_injector.h"

FailureInjector::FailureInjector(const ScenarioConfig& cfg)
    : start_time_(std::chrono::steady_clock::now())
{
    bandwidth_drop_.enabled = cfg.getBool("failure.bandwidth_drop.enabled", false);
    bandwidth_drop_.at_s    = cfg.getDouble("failure.bandwidth_drop.at_s", 5.0);
    bandwidth_drop_.value   = cfg.getDouble("failure.bandwidth_drop.value_mbps", 1.0);

    packet_loss_spike_.enabled = cfg.getBool("failure.packet_loss_spike.enabled", false);
    packet_loss_spike_.at_s    = cfg.getDouble("failure.packet_loss_spike.at_s", 5.0);
    packet_loss_spike_.value   = cfg.getDouble("failure.packet_loss_spike.value_pct", 40.0);

    cpu_overload_.enabled = cfg.getBool("failure.cpu_overload.enabled", false);
    cpu_overload_.at_s    = cfg.getDouble("failure.cpu_overload.at_s", 5.0);
    cpu_overload_.value   = cfg.getDouble("failure.cpu_overload.value_pct", 98.0);

    memory_shortage_.enabled = cfg.getBool("failure.memory_shortage.enabled", false);
    memory_shortage_.at_s    = cfg.getDouble("failure.memory_shortage.at_s", 5.0);
    memory_shortage_.value   = cfg.getDouble("failure.memory_shortage.value_mb", 8.0);

    vehicle_exit_early_.enabled = cfg.getBool("failure.vehicle_exit_early.enabled", false);
    vehicle_exit_early_.at_s    = cfg.getDouble("failure.vehicle_exit_early.at_s", 5.0);

    prediction_failure_.enabled = cfg.getBool("failure.prediction_failure.enabled", false);
    prediction_failure_.at_s    = cfg.getDouble("failure.prediction_failure.at_s", 5.0);

    server_unavailable_.enabled = cfg.getBool("failure.server_unavailable.enabled", false);
    server_unavailable_.at_s    = cfg.getDouble("failure.server_unavailable.at_s", 5.0);

    faults_by_name_ = {
        {"bandwidth_drop",      &bandwidth_drop_},
        {"packet_loss_spike",   &packet_loss_spike_},
        {"cpu_overload",        &cpu_overload_},
        {"memory_shortage",     &memory_shortage_},
        {"vehicle_exit_early",  &vehicle_exit_early_},
        {"prediction_failure",  &prediction_failure_},
        {"server_unavailable",  &server_unavailable_},
    };
}

double FailureInjector::elapsedSeconds() const
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start_time_)
               .count() / 1000.0;
}

bool FailureInjector::isActive(const TimedFault& f, double elapsed_s)
{
    switch (static_cast<OverrideState>(f.live_override.load(std::memory_order_relaxed))) {
        case OverrideState::FORCE_ON:  return true;
        case OverrideState::FORCE_OFF: return false;
        case OverrideState::FOLLOW_CONFIG: default:
            return f.enabled && elapsed_s >= f.at_s;
    }
}

void FailureInjector::setOverride(const std::string& fault_name, bool enabled)
{
    auto it = faults_by_name_.find(fault_name);
    if (it == faults_by_name_.end()) return;
    it->second->live_override.store(
        static_cast<int>(enabled ? OverrideState::FORCE_ON : OverrideState::FORCE_OFF),
        std::memory_order_relaxed);
}

ContextVector FailureInjector::apply(ContextVector cv) const
{
    double elapsed_s = elapsedSeconds();

    if (isActive(bandwidth_drop_, elapsed_s))
        cv.bandwidth_mbps = bandwidth_drop_.value;

    if (isActive(packet_loss_spike_, elapsed_s))
        cv.packet_loss_pct = packet_loss_spike_.value;

    if (isActive(cpu_overload_, elapsed_s))
        cv.cpu_util_pct = cpu_overload_.value;

    if (isActive(memory_shortage_, elapsed_s))
        cv.memory_mb = memory_shortage_.value;

    if (isActive(vehicle_exit_early_, elapsed_s)) {
        // Vehicle is physically gone: no distance or dwell time left.
        cv.dist_to_exit_m    = 0.0;
        cv.remaining_dwell_s = 0.0;
    }

    if (isActive(prediction_failure_, elapsed_s)) {
        // The dwell/gap estimate was wrong, not the vehicle's position:
        // dist_to_exit_m is left untouched to reflect that mismatch.
        cv.remaining_dwell_s = 0.05;
    }

    if (isActive(server_unavailable_, elapsed_s))
        cv.peer_server_unavailable = true;

    return cv;
}
