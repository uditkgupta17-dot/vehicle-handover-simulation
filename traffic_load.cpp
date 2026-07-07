#include "traffic_load.h"
#include <algorithm>
#include <cmath>
#include <sys/resource.h>
#include <sys/time.h>

TrafficLoad::TrafficLoad()
{
    filler_producer_ = std::thread(&TrafficLoad::fillerProducerLoop, this);
    filler_consumer_ = std::thread(&TrafficLoad::fillerConsumerLoop, this);
}

TrafficLoad::~TrafficLoad()
{
    filler_running_.store(false, std::memory_order_release);
    if (filler_producer_.joinable()) filler_producer_.join();
    if (filler_consumer_.joinable()) filler_consumer_.join();

    std::lock_guard<std::mutex> lg(threads_mtx_);
    for (auto& flag : busy_stop_flags_) flag->store(true, std::memory_order_release);
    for (auto& t : busy_threads_) if (t.joinable()) t.join();
}

void TrafficLoad::busyWorker(std::shared_ptr<std::atomic<bool>> stop_flag)
{
    double v = 1.0;
    while (!stop_flag->load(std::memory_order_relaxed)) {
        for (int i = 0; i < 200000 && !stop_flag->load(std::memory_order_relaxed); ++i) {
            v = std::sqrt(v + 1.0001);
            if (v > 1e12) v = 1.0;
        }
    }
}

void TrafficLoad::setLevel(int level)
{
    level = std::clamp(level, 0, 100);
    level_.store(level, std::memory_order_relaxed);

    // Deliberately OVERSUBSCRIBES cores as level approaches 100 (cores+1,
    // not cores-1): the goal isn't just a high reported CPU%, it's genuine
    // scheduler contention with t_local's real processing thread, so
    // measured throughput actually drops and the adaptive ceiling has
    // something real to react to. Under-subscribing (leaving a core free)
    // lets t_local run unimpeded regardless of how "busy" other cores look.
    unsigned hw = std::thread::hardware_concurrency();
    int max_threads = std::max(1, static_cast<int>(hw) + 1);
    int target_threads = static_cast<int>(std::round(max_threads * (level / 100.0)));

    {
        std::lock_guard<std::mutex> lg(threads_mtx_);
        int current = static_cast<int>(busy_threads_.size());

        if (target_threads > current) {
            for (int i = current; i < target_threads; ++i) {
                auto flag = std::make_shared<std::atomic<bool>>(false);
                busy_stop_flags_.push_back(flag);
                busy_threads_.emplace_back(&TrafficLoad::busyWorker, flag);
            }
        } else if (target_threads < current) {
            for (int i = target_threads; i < current; ++i)
                busy_stop_flags_[i]->store(true, std::memory_order_release);
            for (int i = target_threads; i < current; ++i)
                if (busy_threads_[i].joinable()) busy_threads_[i].join();
            busy_threads_.resize(target_threads);
            busy_stop_flags_.resize(target_threads);
        }
    }

    {
        std::lock_guard<std::mutex> lg(ballast_mtx_);
        size_t target_bytes = static_cast<size_t>(level * MB_PER_LEVEL * 1024.0 * 1024.0);
        memory_ballast_.resize(target_bytes);
        // Touch every page so the OS actually backs it with real memory
        // (a resize() alone can leave pages unmapped/copy-on-write).
        for (size_t i = 0; i < memory_ballast_.size(); i += 4096)
            memory_ballast_[i] = static_cast<char>(i & 0xFF);
    }
}

int TrafficLoad::getLevel() const
{
    return level_.load(std::memory_order_relaxed);
}

int TrafficLoad::getFillerQueueLength() const
{
    std::lock_guard<std::mutex> lg(filler_mtx_);
    return static_cast<int>(filler_queue_.size());
}

void TrafficLoad::fillerProducerLoop()
{
    int seq = 0;
    while (filler_running_.load(std::memory_order_acquire)) {
        int level = level_.load(std::memory_order_relaxed);
        if (level <= 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            continue;
        }
        int interval_ms = std::max(5, 100 - level);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        {
            std::lock_guard<std::mutex> lg(filler_mtx_);
            filler_queue_.push_back(seq++);
        }
    }
}

void TrafficLoad::fillerConsumerLoop()
{
    while (filler_running_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(MS_PER_FILLER_POP));
        std::lock_guard<std::mutex> lg(filler_mtx_);
        if (!filler_queue_.empty()) filler_queue_.pop_front();
    }
}

double TrafficLoad::sampleProcessCpuUtilPct()
{
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);

    long long cpu_us = static_cast<long long>(usage.ru_utime.tv_sec) * 1'000'000 + usage.ru_utime.tv_usec +
                        static_cast<long long>(usage.ru_stime.tv_sec) * 1'000'000 + usage.ru_stime.tv_usec;
    auto now = std::chrono::high_resolution_clock::now();

    double pct = 0.0;
    {
        std::lock_guard<std::mutex> lg(cpu_sample_mtx_);
        if (has_last_sample_) {
            long long wall_us = std::chrono::duration_cast<std::chrono::microseconds>(now - last_wall_time_).count();
            unsigned hw = std::thread::hardware_concurrency();
            int cores = std::max(1, static_cast<int>(hw));
            if (wall_us > 0)
                pct = 100.0 * static_cast<double>(cpu_us - last_cpu_time_us_) / (static_cast<double>(wall_us) * cores);
        }
        last_cpu_time_us_ = cpu_us;
        last_wall_time_   = now;
        has_last_sample_  = true;
    }
    return std::clamp(pct, 0.0, 100.0);
}

double TrafficLoad::sampleProcessMemoryMb() const
{
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
#ifdef __APPLE__
    return static_cast<double>(usage.ru_maxrss) / (1024.0 * 1024.0); // macOS reports bytes
#else
    return static_cast<double>(usage.ru_maxrss) / 1024.0;            // Linux reports KB
#endif
}
