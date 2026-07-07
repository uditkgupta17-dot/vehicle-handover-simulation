#ifndef CONTEXT_MONITOR_H
#define CONTEXT_MONITOR_H

#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include "context_vector.h"
#include "traffic_load.h"

// A base + amplitude*|sin(elapsed_s * frequency)| generator used for the
// simulated network/compute readings until real sensors are wired in.
// Values come from ScenarioConfig instead of being literals in this file.
struct SineProfile
{
    double base      = 0.0;
    double amplitude = 0.0;
    double frequency = 1.0;

    double sample(double elapsed_s) const
    {
        return base + amplitude * std::abs(std::sin(elapsed_s * frequency));
    }
};

// Everything ContextMonitor needs to produce a ContextVector for ONE server.
// Server A and Server B each build one of these from ScenarioConfig plus
// references to their own live counters; ContextMonitor itself has no
// knowledge of either server's internal data structures (queues, sockets,
// datasets), which is what lets one implementation serve both.
struct ContextMonitorInputs
{
    std::string rsu_id;
    double      rsu_radius_m = 200.0;
    double      rsu_gap_m    = 5.0;

    TaskType task_type    = TaskType::PACKET_STREAM;
    int      file_size_mb = 0;
    double   baseline_state_size_mb    = 1.0;
    double   state_growth_amplitude_mb = 3.0;

    SineProfile bandwidth_mbps{10.0, 10.0, 0.3};
    SineProfile latency_ms{0.05, 0.01, 1.1};
    SineProfile rssi_dbm{-55.0, -15.0, 0.2};
    SineProfile cpu_util_pct{40.0, 40.0, 0.5};
    SineProfile memory_mb{128.0, 32.0, 0.4};

    // Used only when loss_count/received_count below are left unset.
    double packet_loss_pct_fixed = 1.0;

    // Vehicle speed: either a constant (Server A knows it up front) or a
    // live atomic (Server B learns/updates it from received packets).
    double                      speed_kmh_constant = 0.0;
    const std::atomic<double>* speed_kmh_atomic    = nullptr;

    // Task progress: done/total. Both are LIVE atomics, read fresh on every
    // sample - required because the adaptive boundary can change the total
    // mid-run (e.g. Server A's local_ceiling shrinking, or Server B's
    // offload_count being updated by a boundary-sync message from Server A).
    // A denominator captured once at construction would go stale exactly
    // when the adaptive engine actually does something.
    const std::atomic<int>* progress_total = nullptr;
    const std::atomic<int>* progress_done  = nullptr;

    // Optional live loss accounting; if either is null, packet_loss_pct_fixed is used.
    const std::atomic<int>* loss_count     = nullptr;
    const std::atomic<int>* received_count = nullptr;
    // Loss % is meaningless (and wildly noisy) over a tiny sample - e.g. one
    // early reorder out of two packets reads as 50% loss. Hold at 0% until
    // at least this many packets have been accounted for.
    int min_loss_sample_count = 20;

    // Optional accessor for queue depth (needs a lock on the caller's side).
    std::function<int()> get_queue_length;

    // When set, cpu_util_pct/memory_mb/queue_length come from REAL measurements
    // (process CPU time, resident memory, a real congestion queue) instead of
    // the simulated SineProfile fields above - this is how live traffic
    // injection from the Vehicle Controller actually loads the server.
    TrafficLoad* traffic_load = nullptr;

    int interval_s = 1;
};

// ContextMonitor - periodically samples live counters + simulated
// network/compute signals into a ContextVector snapshot.
//
// Design rules (unchanged from the original per-server implementations):
//   - Does NOT make any decisions - monitoring only.
//   - Does NOT interfere with any existing logic; all inputs are read-only.
//   - Runs on its own thread; call stop() before destruction.
//   - The optional on_sample callback is the single hook point the rest of
//     the continuity pipeline (StateAnalyzer/DecisionEngine/ActionExecutor)
//     attaches through; ContextMonitor itself never references them.
class ContextMonitor
{
public:
    using SampleCallback = std::function<void(const ContextVector&)>;

    explicit ContextMonitor(ContextMonitorInputs inputs, SampleCallback on_sample = nullptr)
        : inputs_(std::move(inputs)), on_sample_(std::move(on_sample)), running_(false)
    {}

    void start()
    {
        running_.store(true, std::memory_order_release);
        worker_ = std::thread(&ContextMonitor::collectionLoop, this);
    }

    void stop()
    {
        running_.store(false, std::memory_order_release);
        wake_cv_.notify_all();
        if (worker_.joinable())
            worker_.join();
    }

    // Wakes the sampling loop immediately instead of waiting out the rest of
    // interval_s. Used by the controller listener thread so a live
    // speed/traffic/failure command is felt right away. The sample still
    // runs on ContextMonitor's own single worker thread (not the caller's),
    // so on_sample_ - which drives StateAnalyzer/DecisionEngine, both of
    // which hold non-thread-safe internal state - never runs concurrently
    // with itself.
    void triggerImmediateSample()
    {
        wake_cv_.notify_all();
    }

    ContextVector getCurrentContext()
    {
        std::lock_guard<std::mutex> lg(cv_mtx_);
        return latest_;
    }

private:
    ContextMonitorInputs inputs_;
    SampleCallback        on_sample_;

    std::atomic<bool>       running_;
    std::thread             worker_;
    std::mutex              cv_mtx_;
    ContextVector           latest_{};
    std::mutex              wake_mtx_;
    std::condition_variable wake_cv_;

    void collectionLoop()
    {
        auto start_time = std::chrono::high_resolution_clock::now();

        while (running_.load(std::memory_order_acquire))
        {
            {
                std::unique_lock<std::mutex> ul(wake_mtx_);
                wake_cv_.wait_for(ul, std::chrono::seconds(inputs_.interval_s));
            }
            if (!running_.load(std::memory_order_acquire)) break;

            ContextVector cv = sample(start_time);
            {
                std::lock_guard<std::mutex> lg(cv_mtx_);
                latest_ = cv;
            }
            printContextVector(cv);
            if (on_sample_) on_sample_(cv);
        }
    }

    ContextVector sample(const std::chrono::high_resolution_clock::time_point& start_time)
    {
        ContextVector cv{};
        double elapsed_s = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::high_resolution_clock::now() - start_time)
                                .count() / 1000.0;

        double speed_kmh = inputs_.speed_kmh_atomic
                                ? inputs_.speed_kmh_atomic->load(std::memory_order_relaxed)
                                : inputs_.speed_kmh_constant;
        double speed_ms = speed_kmh / 3.6;

        cv.vehicle_speed_kmh = speed_kmh;
        cv.current_rsu_id    = inputs_.rsu_id;

        double covered_m       = speed_ms * elapsed_s;
        cv.dist_to_exit_m      = std::max(0.0, inputs_.rsu_radius_m - covered_m);
        cv.dist_to_next_rsu_m  = cv.dist_to_exit_m + inputs_.rsu_gap_m;
        cv.remaining_dwell_s   = (speed_ms > 0.0) ? cv.dist_to_exit_m / speed_ms : 0.0;
        cv.predicted_gap_duration_s = (speed_ms > 0.0) ? inputs_.rsu_gap_m / speed_ms : 0.0;

        cv.bandwidth_mbps = inputs_.bandwidth_mbps.sample(elapsed_s);
        cv.avg_latency_ms = inputs_.latency_ms.sample(elapsed_s);
        cv.rssi_dbm       = inputs_.rssi_dbm.sample(elapsed_s);
        cv.peer_server_unavailable = false; // failure injection sets this downstream, not here

        if (inputs_.loss_count && inputs_.received_count) {
            int lost  = inputs_.loss_count->load(std::memory_order_relaxed);
            int recvd = inputs_.received_count->load(std::memory_order_relaxed);
            int total = lost + recvd;
            cv.packet_loss_pct = (total >= inputs_.min_loss_sample_count)
                                      ? (100.0 * lost / total)
                                      : 0.0;
        } else {
            cv.packet_loss_pct = inputs_.packet_loss_pct_fixed;
        }

        int base_queue_length = inputs_.get_queue_length ? inputs_.get_queue_length() : 0;
        if (inputs_.traffic_load) {
            // Real measurements: background traffic genuinely consumes CPU/
            // memory on this process and genuinely queues up, so these numbers
            // reflect actual degradation rather than a simulated formula.
            cv.cpu_util_pct = inputs_.traffic_load->sampleProcessCpuUtilPct();
            cv.memory_mb    = inputs_.traffic_load->sampleProcessMemoryMb();
            cv.queue_length = base_queue_length + inputs_.traffic_load->getFillerQueueLength();
        } else {
            cv.cpu_util_pct = inputs_.cpu_util_pct.sample(elapsed_s);
            cv.memory_mb    = inputs_.memory_mb.sample(elapsed_s);
            cv.queue_length = base_queue_length;
        }

        int done  = inputs_.progress_done  ? inputs_.progress_done->load(std::memory_order_relaxed)  : 0;
        int total = inputs_.progress_total ? inputs_.progress_total->load(std::memory_order_relaxed) : 0;
        cv.task_progress_pct = (total > 0)
                                    ? std::min(100.0, 100.0 * done / total)
                                    : 0.0;

        cv.baseline_execution_state_size_mb =
            inputs_.baseline_state_size_mb +
            inputs_.state_growth_amplitude_mb * (cv.task_progress_pct / 100.0);

        cv.task_type    = inputs_.task_type;
        cv.file_size_mb = inputs_.file_size_mb;

        return cv;
    }

    static void printContextVector(const ContextVector& cv)
    {
        std::cout << "\n============================\n"
                  << " Context Vector [" << cv.current_rsu_id << "]\n"
                  << "============================\n"
                  << std::fixed << std::setprecision(1)
                  << "Vehicle Speed      : " << cv.vehicle_speed_kmh   << " km/h\n"
                  << "Distance to Exit   : " << cv.dist_to_exit_m       << " m\n"
                  << "Distance to Next   : " << cv.dist_to_next_rsu_m   << " m\n"
                  << "Remaining Dwell    : " << cv.remaining_dwell_s    << " s\n"
                  << "Predicted Gap      : " << cv.predicted_gap_duration_s << " s\n"
                  << "Bandwidth          : " << cv.bandwidth_mbps       << " Mbps\n"
                  << "Latency            : " << cv.avg_latency_ms       << " ms\n"
                  << "Packet Loss        : " << cv.packet_loss_pct      << "%\n"
                  << "CPU Usage          : " << cv.cpu_util_pct         << "%\n"
                  << "Memory Usage       : " << cv.memory_mb            << " MB\n"
                  << "Queue Length       : " << cv.queue_length         << "\n"
                  << "Task Progress      : " << cv.task_progress_pct    << "%\n"
                  << "State Size         : " << cv.baseline_execution_state_size_mb << " MB\n"
                  << "RSSI               : " << cv.rssi_dbm             << " dBm\n"
                  << "Peer Unavailable   : " << (cv.peer_server_unavailable ? "yes" : "no") << "\n"
                  << "============================\n\n";
    }
};

#endif
