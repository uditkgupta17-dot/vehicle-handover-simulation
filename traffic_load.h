#ifndef TRAFFIC_LOAD_H
#define TRAFFIC_LOAD_H

#include <atomic>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// Models background vehicles competing for the SAME server's resources.
// Deliberately does NOT generate any application/network packets - it only
// consumes real CPU (busy-spin threads) and real memory (a touched heap
// buffer), and runs a small producer/consumer pair that models a
// congestion queue growing when arrival rate exceeds a fixed service rate.
// ContextMonitor reads the resulting REAL measurements (via getrusage) and
// the REAL queue depth - nothing here is a fabricated number.
//
// setLevel() is safe to call at any time, including while sampling is in
// progress on another thread; it reconfigures the load in place.
class TrafficLoad
{
public:
    TrafficLoad();
    ~TrafficLoad();

    // level is clamped to [0, 100]. 0 = no injected load.
    void setLevel(int level);
    int  getLevel() const;

    // Depth of the synthetic congestion queue (real producer/consumer counters,
    // not a formula). Callers add this on top of any real application queue.
    int getFillerQueueLength() const;

    // Real, measured CPU utilisation (%) and resident memory (MB) of THIS
    // process since the previous call, via POSIX getrusage(). Not simulated.
    double sampleProcessCpuUtilPct();
    double sampleProcessMemoryMb() const;

private:
    static constexpr double MB_PER_LEVEL = 2.0;      // memory ballast scaling
    static constexpr int    MS_PER_FILLER_POP = 50;  // fixed background "service rate"

    std::atomic<int> level_{0};

    // Busy-spin CPU consumers, one stop flag per thread so the pool can be
    // resized up or down without tearing down threads that should keep running.
    std::mutex threads_mtx_;
    std::vector<std::thread> busy_threads_;
    std::vector<std::shared_ptr<std::atomic<bool>>> busy_stop_flags_;

    // Real, touched memory ballast representing background memory pressure.
    mutable std::mutex ballast_mtx_;
    std::vector<char> memory_ballast_;

    // Congestion queue: producer rate scales with level, consumer rate is fixed,
    // so the queue only grows once injected traffic exceeds service capacity.
    std::atomic<bool>       filler_running_{true};
    std::thread             filler_producer_;
    std::thread             filler_consumer_;
    mutable std::mutex      filler_mtx_;
    std::deque<int>         filler_queue_;

    // getrusage bookkeeping (instance state, not global).
    std::mutex                                    cpu_sample_mtx_;
    bool                                           has_last_sample_ = false;
    long long                                      last_cpu_time_us_ = 0;
    std::chrono::high_resolution_clock::time_point last_wall_time_;

    static void busyWorker(std::shared_ptr<std::atomic<bool>> stop_flag);
    void fillerProducerLoop();
    void fillerConsumerLoop();
};

#endif
