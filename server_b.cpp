#include <iostream>
#include <map>
#include <deque>
#include <vector>
#include <string>
#include <netinet/in.h>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <cmath>
#include <iomanip>
#include "custom_payload.pb.h"

// ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
#include "context_vector.h"
#include "context_monitor.h"
#include "scenario_config.h"
#include "state_analyzer.h"
#include "decision_engine.h"
#include "csv_logger.h"
#include "traffic_load.h"
// ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

// ===== METRICS DASHBOARD START =====
class MetricsDashboard {
private:
    std::mutex mtx;
    int frames_sent = 0;
    int frames_recv = 0;
    int missing_frames = 0;
    int duplicate_frames = 0;

    std::vector<double> network_latencies;
    std::vector<double> processing_times;
    std::vector<double> queue_wait_times;
    std::vector<int> queue_lengths;
    std::vector<double> scheduler_delays;

    int frames_processed_before_ho = 0;
    int frames_delivered_after_ho = 0;
    double actual_handover_downtime_ms = 0.0;
    double sentinel_latency_ms = 0.0;

    std::chrono::high_resolution_clock::time_point start_time;

    double getAvg(const std::vector<double>& v) {
        if (v.empty()) return 0.0;
        double sum = 0; for (double x : v) sum += x;
        return sum / v.size();
    }
    double getAvg(const std::vector<int>& v) {
        if (v.empty()) return 0.0;
        double sum = 0; for (int x : v) sum += x;
        return sum / v.size();
    }
    double getMin(const std::vector<double>& v) {
        if (v.empty()) return 0.0;
        double m = v[0]; for (double x : v) if (x < m) m = x;
        return m;
    }
    double getMax(const std::vector<double>& v) {
        if (v.empty()) return 0.0;
        double m = v[0]; for (double x : v) if (x > m) m = x;
        return m;
    }
    int getMax(const std::vector<int>& v) {
        if (v.empty()) return 0;
        int m = v[0]; for (int x : v) if (x > m) m = x;
        return m;
    }
    double getStdDev(const std::vector<double>& v, double avg) {
        if (v.empty()) return 0.0;
        double sum = 0; for (double x : v) sum += (x - avg) * (x - avg);
        return std::sqrt(sum / v.size());
    }

public:
    MetricsDashboard() {
        start_time = std::chrono::high_resolution_clock::now();
    }

    void recordPacketSent(int count) {
        std::lock_guard<std::mutex> lock(mtx);
        frames_sent = count;
    }
    void recordPacketReceived() {
        std::lock_guard<std::mutex> lock(mtx);
        frames_recv++;
    }
    void recordMissingPacket(int count) {
        std::lock_guard<std::mutex> lock(mtx);
        missing_frames += count;
    }
    void recordDuplicatePacket() {
        std::lock_guard<std::mutex> lock(mtx);
        duplicate_frames++;
    }
    void recordLatency(double latency_ms) {
        std::lock_guard<std::mutex> lock(mtx);
        network_latencies.push_back(latency_ms);
    }
    void recordProcessingTime(double proc_ms, double wait_ms) {
        std::lock_guard<std::mutex> lock(mtx);
        processing_times.push_back(proc_ms);
        queue_wait_times.push_back(wait_ms);
    }
    void recordQueueLength(int len) {
        std::lock_guard<std::mutex> lock(mtx);
        queue_lengths.push_back(len);
    }
    void recordSchedulerDelay(double delay_ms) {
        std::lock_guard<std::mutex> lock(mtx);
        scheduler_delays.push_back(delay_ms);
    }
    void recordHandoverMetrics(double downtime_ms, double sentinel_lat_ms) {
        std::lock_guard<std::mutex> lock(mtx);
        if (downtime_ms >= 0) actual_handover_downtime_ms = downtime_ms;
        if (sentinel_lat_ms > 0) sentinel_latency_ms = sentinel_lat_ms;
    }
    void setActualDowntime(double downtime_ms) {
        std::lock_guard<std::mutex> lock(mtx);
        actual_handover_downtime_ms = downtime_ms;
    }
    void recordFramesProcessedBeforeHO(int count) {
        std::lock_guard<std::mutex> lock(mtx);
        frames_processed_before_ho = count;
    }
    void recordFramesDeliveredAfterHO(int count) {
        std::lock_guard<std::mutex> lock(mtx);
        frames_delivered_after_ho += count;
    }

    void printFinalReport() {
        auto end_time = std::chrono::high_resolution_clock::now();
        double total_exec_time_s = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count() / 1000.0;

        std::lock_guard<std::mutex> lock(mtx);

        double transfer_success_rate = (frames_sent > 0) ? ((frames_recv * 100.0) / frames_sent) : 0.0;
        if (transfer_success_rate > 100.0) transfer_success_rate = 100.0;
        double transfer_miss_rate = (frames_sent > 0) ? ((missing_frames * 100.0) / frames_sent) : 0.0;
        if (transfer_miss_rate > 100.0) transfer_miss_rate = 100.0;

        double avg_lat = getAvg(network_latencies);
        double min_lat = getMin(network_latencies);
        double max_lat = getMax(network_latencies);
        double std_lat = getStdDev(network_latencies, avg_lat);

        double avg_proc = getAvg(processing_times);
        double min_proc = getMin(processing_times);
        double max_proc = getMax(processing_times);

        double avg_queue_wait = getAvg(queue_wait_times);
        double max_queue_wait = getMax(queue_wait_times);

        double avg_sched_delay = getAvg(scheduler_delays);

        int max_q_len = getMax(queue_lengths);
        double avg_q_len = getAvg(queue_lengths);
        double queue_occupancy = (max_q_len > 0) ? ((avg_q_len / max_q_len) * 100.0) : 0.0;

        int total_processed = processing_times.size();
        double proc_completion_rate = (frames_sent > 0) ? ((total_processed * 100.0) / frames_sent) : 0.0;
        if (proc_completion_rate > 100.0) proc_completion_rate = 100.0;
        double overall_eff = (transfer_success_rate + proc_completion_rate) / 2.0;

        std::string status = "FAILED";
        if (transfer_success_rate > 90.0 && proc_completion_rate > 90.0) status = "SUCCESS";
        else if (transfer_success_rate > 50.0 || proc_completion_rate > 50.0) status = "PARTIAL";

        std::cout << "\n\n";
        std::cout << "======================================\n";
        std::cout << "   EVALUATION METRICS DASHBOARD\n";
        std::cout << "======================================\n";

        std::cout << "\n--------------------------------------\n";
        std::cout << "TRANSFER METRICS\n";
        std::cout << "--------------------------------------\n";
        std::cout << "Total Frames Sent            : " << frames_sent << "\n";
        std::cout << "Total Frames Received        : " << frames_recv << "\n";
        std::cout << "Successfully Received        : " << frames_recv << "\n";
        std::cout << "Missing Frames               : " << missing_frames << "\n";
        std::cout << "Duplicate Frames             : " << duplicate_frames << "\n";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "Transfer Success Rate (%)    : " << transfer_success_rate << " %\n";
        std::cout << "Transfer Miss Rate (%)       : " << transfer_miss_rate << " %\n";

        std::cout << "\n--------------------------------------\n";
        std::cout << "NETWORK METRICS\n";
        std::cout << "--------------------------------------\n";
        std::cout << "Average Network Latency      : " << avg_lat << " ms\n";
        std::cout << "Minimum Network Latency      : " << min_lat << " ms\n";
        std::cout << "Maximum Network Latency      : " << max_lat << " ms\n";
        std::cout << "Standard Deviation of Latency: " << std_lat << " ms\n";

        std::cout << "\n--------------------------------------\n";
        std::cout << "PROCESSING METRICS\n";
        std::cout << "--------------------------------------\n";
        std::cout << "Average Processing Time      : " << avg_proc << " ms\n";
        std::cout << "Minimum Processing Time      : " << min_proc << " ms\n";
        std::cout << "Maximum Processing Time      : " << max_proc << " ms\n";
        std::cout << "Average Queue Waiting Time   : " << avg_queue_wait << " ms\n";
        std::cout << "Maximum Queue Waiting Time   : " << max_queue_wait << " ms\n";
        std::cout << "Average Scheduler Delivery Delay: " << avg_sched_delay << " ms\n";

        std::cout << "\n--------------------------------------\n";
        std::cout << "QUEUE METRICS\n";
        std::cout << "--------------------------------------\n";
        std::cout << "Maximum Queue Length         : " << max_q_len << "\n";
        std::cout << "Average Queue Length         : " << avg_q_len << "\n";
        std::cout << "Queue Occupancy              : " << queue_occupancy << " %\n";

        std::cout << "\n--------------------------------------\n";
        std::cout << "SYSTEM METRICS\n";
        std::cout << "--------------------------------------\n";
        std::cout << "Total Execution Time         : " << total_exec_time_s << " s\n";
        std::cout << "Total Frames Processed       : " << total_processed << "\n";
        std::cout << "Frames Processed Before Handover: " << frames_processed_before_ho << "\n";
        std::cout << "Frames Delivered After Handover : " << frames_delivered_after_ho << "\n";
        std::cout << "Actual Handover Downtime     : " << actual_handover_downtime_ms << " ms\n";
        std::cout << "Sentinel Latency             : " << sentinel_latency_ms << " ms\n";

        std::cout << "\n--------------------------------------\n";
        std::cout << "OVERALL RESULTS\n";
        std::cout << "--------------------------------------\n";
        std::cout << "Experiment Status            : " << status << "\n";
        std::cout << "Transfer Success Rate        : " << transfer_success_rate << " %\n";
        std::cout << "Processing Completion Rate   : " << proc_completion_rate << " %\n";
        std::cout << "Overall Migration Efficiency : " << overall_eff << " %\n";
        std::cout << "======================================\n\n";
    }
};

MetricsDashboard g_dashboard;
// ===== METRICS DASHBOARD END =====

using namespace std;
using namespace chrono;

static mutex g_print_mtx;
#define PRINT(x) { lock_guard<mutex> _lg(g_print_mtx); cout << x << "\n"; }

struct RawFrame {
    int    frame_id;
    string sensor_path;
    long long enqueue_us;
    long long net_latency_us;
};

struct ProcessedResult {
    atomic<bool> ready{false};
    double       proc_ms{0};
    double       queue_wait_ms{0};
    double       net_latency_ms{0};
    double       value{0};
};

// ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
// The receiving counterpart to Server A's Action Executor. Server B never
// runs its own Decision Engine (per the pipeline design, the decision is
// made once, on Server A); it only reacts to the strategy A already chose.
// A production deployment would branch into real container/prewarm APIs
// here - this simulator records the strategy that was actually applied.
static void handleControlPacket(const edgesim::VehicleMetadata& p)
{
    auto action = static_cast<ContinuityDecision>(p.continuity_action());
    const char* name = "UNKNOWN";
    switch (action) {
        case ContinuityDecision::NORMAL_PIPELINE:        name = "NORMAL_PIPELINE"; break;
        case ContinuityDecision::SPECULATIVE_PREWARM:    name = "SPECULATIVE_PREWARM"; break;
        case ContinuityDecision::MESSAGE_STATE_TRANSFER: name = "MESSAGE_STATE_TRANSFER"; break;
        case ContinuityDecision::POINTER_STATE_TRANSFER: name = "POINTER_STATE_TRANSFER"; break;
        case ContinuityDecision::CARRY_STATE:            name = "CARRY_STATE"; break;
        case ContinuityDecision::RESTART:                name = "RESTART"; break;
    }
    PRINT("[ACTION-EXECUTOR:B] Control packet received  action=" << name
          << "  payload=" << p.state_payload());
}
// ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

static double processPacket(int frame_id)
{
    const int N = 2'000'000;
    double v = static_cast<double>(frame_id + 1);
    for (int i = 0; i < N; ++i) {
        v += std::sqrt(v + static_cast<double>(i) * 0.0001);
        if (v > 1e15) v = static_cast<double>(frame_id + 1);
    }
    return v;
}

int main()
{
    int socketid = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketid < 0) { cout << "Socket didnt form\n"; return 1; }

    sockaddr_in saddr{};
    saddr.sin_family      = AF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY;
    saddr.sin_port        = htons(8080);

    if (::bind(socketid, (sockaddr*)&saddr, sizeof(saddr)) < 0) {
        cout << "Binding didnt happen\n"; return 1;
    }

    cout << "Server B is working\n"
         << "--- SERVER B (MULTI-METHOD WORKLOAD HANDLING) ---\n";

    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
    ScenarioConfig scenario = ScenarioConfig::load("scenario.conf");
    CsvLogger      csv_logger(scenario.getString("csv_log_path", "continuity_log.csv"));
    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

    map<int,string> downlink_cache;
    for (int i = 11; i <= 20; ++i)
        downlink_cache[i] = "Video_Chunk_" + to_string(i) + ".mp4";

    char       buf[4096];
    sockaddr_in veh{};
    socklen_t   addlen = sizeof(veh);

    cout << "Waiting for packets from Server A...\n";

    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
    // Server A's continuity engine can emit control packets (is_control_packet)
    // independently of data traffic. Those must never be mistaken for the
    // first real data/method packet used below to pick Method B vs Method C.
    edgesim::VehicleMetadata first_pkt;
    while (true) {
        int first_bytes = recvfrom(socketid, buf, sizeof(buf), 0,
                                    (sockaddr*)&veh, &addlen);
        if (first_bytes <= 0) continue;
        if (!first_pkt.ParseFromArray(buf, first_bytes)) continue;
        if (first_pkt.is_control_packet()) {
            handleControlPacket(first_pkt);
            continue;
        }
        break;
    }
    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

    if (first_pkt.task_type() == 1)
    {
        cout << "\n[METHOD B] Cache shift mode activated.\n";

        int total_expected = max(1, first_pkt.file_size_mb());
        int pac_process    = 0;
        int expected_seq   = first_pkt.sequence_number();
        int total_lost     = 0;

        auto process_mb = [&](const edgesim::VehicleMetadata& p)
        {
            auto now_us = duration_cast<microseconds>(
                              high_resolution_clock::now().time_since_epoch()).count();
            long long latency = now_us - p.timestamp_ns();

            int rseq = p.sequence_number();
            if (rseq > expected_seq) {
                total_lost += rseq - expected_seq;
                cout << "\n!!! [TELEMETRY ALERT] "
                     << rseq - expected_seq << " packet(s) lost!\n";
            } else if (rseq < expected_seq && pac_process != 0) {
                cout << "\n!!! [TELEMETRY ALERT] Stale/out-of-order packet!\n";
            }
            expected_seq = rseq + 1;

            if (p.is_handover_packet()) {
                cout << "\n==================================================\n"
                     << " [METRIC] HANDOVER TRIGGER DETECTED (Method B)\n"
                     << " [METRIC] Handover Downtime: " << latency/1000.0 << " ms\n"
                     << "==================================================\n";
            }

            cout << "[METHOD B] Received packet " << p.frame_id()
                 << "  latency=" << latency/1000.0 << " ms\n";
            downlink_cache[p.frame_id()] = p.sensor_path();
            pac_process++;
        };

        process_mb(first_pkt);

        while (pac_process < total_expected) {
            int b = recvfrom(socketid, buf, sizeof(buf), 0, (sockaddr*)&veh, &addlen);
            if (b <= 0) continue;
            edgesim::VehicleMetadata p;
            if (!p.ParseFromArray(buf, b)) continue;
            // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
            if (p.is_control_packet()) { handleControlPacket(p); continue; }
            // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====
            process_mb(p);
        }

        cout << "\nVehicle Crossed Boundary Zone\n"
             << "Total Lost Packets: " << total_lost << "\n\n"
             << "Connection switched to Server B. Downlink Cache Self-Healed.\n"
             << "Final Sorted Cache:\n";
        for (auto& [id, path] : downlink_cache)
            cout << "  -> Packet " << id << " | " << path << "\n";

        close(socketid);
        cout << "\n[SERVER B] Holding pod active for logs...\n";
        while (true) sleep(10);
        return 0;
    }

    cout << "\n[METHOD C] Speculative processing engine activated.\n";

    // BUG FIX: these were plain ints written under seq_mtx but read from
    // other threads (processor_thread, ContextMonitor) without any lock -
    // a pre-existing data race. Now atomic, they also double as the live
    // channel Server A's boundary-sync updates flow through, so Server B's
    // notion of "how much work is there in total" can never go stale after
    // an adaptive ceiling shrink (Server A is always the source of truth).
    atomic<int> offload_count{first_pkt.file_size_mb()};
    atomic<int> A_boundary{first_pkt.handover_status()};

    if (offload_count.load() <= 0) {
        cout << "Invalid offload_count from first packet.\n";
        close(socketid); return 1;
    }

    cout << "[METHOD C] offload_count=" << offload_count.load()
         << "  A_boundary=" << A_boundary.load() << "\n\n";

// ===== METRICS DASHBOARD START =====
    g_dashboard.recordPacketSent(offload_count.load());
// ===== METRICS DASHBOARD END =====

    const int STORE_SIZE = 16384;
    vector<ProcessedResult> processed_store(STORE_SIZE);

    deque<RawFrame>    incoming_queue;
    mutex              queue_mtx;
    condition_variable queue_cv;
    atomic<bool>       handover_triggered{false};
    atomic<bool>       receiver_exited{false};

    atomic<int>       speculative_done{0};
    atomic<int>       total_lost{0};
    atomic<long long> sentinel_recv_us{0};
    atomic<double>    vehicle_speed_kmh{first_pkt.vehicle_speed_kmh()};

    int expected_seq = first_pkt.sequence_number();
    mutex seq_mtx; // still guards expected_seq only

    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
    // Models background vehicles competing for Server B's resources once the
    // handover happens; never generates application packets (see traffic_load.h).
    TrafficLoad traffic_load;
    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

    // ===== CONTEXT MONITOR MODULE START =====
    ContextMonitorInputs cm_inputs;
    cm_inputs.rsu_id       = "RSU-B";
    cm_inputs.rsu_radius_m = scenario.getDouble("rsu_radius_m", 200.0);
    cm_inputs.rsu_gap_m    = scenario.getDouble("rsu_gap_m", 5.0);
    cm_inputs.task_type    = TaskType::PACKET_STREAM;
    cm_inputs.file_size_mb = offload_count.load();
    cm_inputs.baseline_state_size_mb    = scenario.getDouble("server_b.baseline_state_size_mb", 1.0);
    cm_inputs.state_growth_amplitude_mb = scenario.getDouble("server_b.state_growth_amplitude_mb", 3.5);
    cm_inputs.bandwidth_mbps = { scenario.getDouble("server_b.bandwidth_base_mbps", 12.0),
                                 scenario.getDouble("server_b.bandwidth_amplitude_mbps", 10.0),
                                 scenario.getDouble("server_b.bandwidth_frequency", 0.25) };
    cm_inputs.latency_ms     = { scenario.getDouble("server_b.latency_base_ms", 0.05),
                                 scenario.getDouble("server_b.latency_amplitude_ms", 0.02),
                                 scenario.getDouble("server_b.latency_frequency", 0.9) };
    cm_inputs.rssi_dbm       = { scenario.getDouble("server_b.rssi_base_dbm", -58.0),
                                 scenario.getDouble("server_b.rssi_amplitude_dbm", -14.0),
                                 scenario.getDouble("server_b.rssi_frequency", 0.18) };
    cm_inputs.cpu_util_pct   = { scenario.getDouble("server_b.cpu_base_pct", 45.0),
                                 scenario.getDouble("server_b.cpu_amplitude_pct", 40.0),
                                 scenario.getDouble("server_b.cpu_frequency", 0.55) };
    cm_inputs.memory_mb      = { scenario.getDouble("server_b.memory_base_mb", 130.0),
                                 scenario.getDouble("server_b.memory_amplitude_mb", 40.0),
                                 scenario.getDouble("server_b.memory_frequency", 0.38) };
    cm_inputs.speed_kmh_atomic = &vehicle_speed_kmh;
    cm_inputs.traffic_load     = &traffic_load;
    cm_inputs.progress_total   = &offload_count; // BUG FIX: live, so it tracks Server A's boundary updates
    cm_inputs.progress_done    = &speculative_done;
    cm_inputs.loss_count       = &total_lost;
    cm_inputs.received_count   = &speculative_done; // matches original recv=done+lost proxy
    cm_inputs.min_loss_sample_count = scenario.getInt("min_loss_sample_count", 20);
    cm_inputs.get_queue_length = [&]{
        lock_guard<mutex> lg(queue_mtx);
        return static_cast<int>(incoming_queue.size());
    };
    cm_inputs.interval_s = scenario.getInt("context_monitor_interval_s", 1);

    // Server B does not run its own Decision Engine - it only observes and
    // logs telemetry here; the continuity decision is made once, on Server A.
    ContextMonitor ctx_monitor(cm_inputs, [&](const ContextVector& cv)
    {
        StateAnalysis sa = StateAnalyzer::analyze(cv);
        Decision telemetry_only;
        telemetry_only.action        = ContinuityDecision::NORMAL_PIPELINE;
        telemetry_only.explanation   = "Server B telemetry only - Decision Engine runs on Server A";
        telemetry_only.confidence    = 0.0;
        telemetry_only.calculated_cost = 0.0;
        csv_logger.logRow("SERVER_B", cv, sa, telemetry_only, 0.0, 0.0, true);
    });
    ctx_monitor.start();

    // ===== EXECUTION CONTINUITY: CONTROLLER OWNERSHIP =====
    // BUG FIX: Server B previously also bound controller_port, racing
    // Server A for the same UDP port and violating "Server A is the single
    // source of truth for vehicle behaviour." The Vehicle Controller now
    // talks ONLY to Server A; Server B never binds the controller port and
    // never independently learns vehicle state. Whatever Server B needs to
    // know (the current adaptive partition) arrives over the existing data
    // socket as a boundary-sync update (handled in receiver_thread below).
    // traffic_load on Server B therefore stays at level 0 (real baseline
    // measurement only) until a future extension has Server A forward a
    // traffic level as part of that same boundary-sync message.
    // ===== CONTEXT MONITOR MODULE END =====

    {
        auto now_us = duration_cast<microseconds>(
                          high_resolution_clock::now().time_since_epoch()).count();
        RawFrame rf;
        rf.frame_id        = first_pkt.frame_id();
        rf.sensor_path     = first_pkt.sensor_path();
        rf.enqueue_us      = now_us;
        rf.net_latency_us  = now_us - first_pkt.timestamp_ns();

// ===== METRICS DASHBOARD START =====
        g_dashboard.recordPacketReceived();
        g_dashboard.recordLatency(rf.net_latency_us / 1000.0);
// ===== METRICS DASHBOARD END =====

        PRINT("[RECEIVER ] First frame " << rf.frame_id
              << "  enqueued  net_lat="
              << rf.net_latency_us/1000.0 << " ms");
        {
            lock_guard<mutex> lg(queue_mtx);
            incoming_queue.push_back(rf);

// ===== METRICS DASHBOARD START =====
            g_dashboard.recordQueueLength(incoming_queue.size());
// ===== METRICS DASHBOARD END =====
        }
        queue_cv.notify_one();
    }

    thread receiver_thread([&]()
    {
        int pkts = 1;

        while (true)
        {
            int bytes = recvfrom(socketid, buf, sizeof(buf), 0,
                                 (sockaddr*)&veh, &addlen);
            if (bytes <= 0) continue;

            edgesim::VehicleMetadata p;
            if (!p.ParseFromArray(buf, bytes)) continue;

            auto now_us = duration_cast<microseconds>(
                              high_resolution_clock::now().time_since_epoch()).count();
            long long net_lat = now_us - p.timestamp_ns();

            {
                lock_guard<mutex> sl(seq_mtx);
                int rseq = p.sequence_number();
                if (rseq > expected_seq) {
                    int lost = rseq - expected_seq;
                    total_lost.fetch_add(lost, memory_order_relaxed);
                    PRINT("!!! [TELEMETRY ALERT] " << lost
                          << " packet(s) lost!  (got seq=" << rseq
                          << "  expected=" << expected_seq << ")");

// ===== METRICS DASHBOARD START =====
                    g_dashboard.recordMissingPacket(lost);
// ===== METRICS DASHBOARD END =====

                } else if (rseq < expected_seq) {
                    PRINT("!!! [TELEMETRY ALERT] Stale packet  seq=" << rseq);

// ===== METRICS DASHBOARD START =====
                    g_dashboard.recordDuplicatePacket();
// ===== METRICS DASHBOARD END =====
                }
                expected_seq = p.sequence_number() + 1;
            }

            // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
            if (p.is_control_packet()) {
                handleControlPacket(p);
                continue;
            }

            // BUG FIX (synchronization): Server A sends this the instant its
            // adaptive ceiling shrinks, so Server B's offload_count/
            // A_boundary never drift from what Server A actually decided -
            // instead of only learning about it (with stale values) at the
            // final handover sentinel.
            if (p.is_boundary_update()) {
                int new_start = p.handover_status();
                int new_total = p.file_size_mb();
                A_boundary.store(new_start, memory_order_release);
                offload_count.store(new_total, memory_order_release);

// ===== METRICS DASHBOARD START =====
                g_dashboard.recordPacketSent(new_total);
// ===== METRICS DASHBOARD END =====

                PRINT("[BOUNDARY-SYNC] Updated partition from Server A: start_frame_id="
                      << new_start << "  total_count=" << new_total);
                continue;
            }
            // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

            if (p.is_handover_packet()) {
                sentinel_recv_us.store(now_us, memory_order_release);

                // Final values always win, even if an earlier boundary-sync
                // update already arrived - Server A computes both from the
                // same latest ceiling, so they agree; this just guarantees
                // Server B ends up with whatever Server A considered final.
                offload_count.store(p.file_size_mb(), memory_order_release);
                A_boundary.store(p.handover_status(), memory_order_release);
                vehicle_speed_kmh.store(p.vehicle_speed_kmh(), memory_order_relaxed);

                cout << "\n==================================================\n"
                     << " [METRIC] HANDOVER SENTINEL RECEIVED\n"
                     << " [METRIC] Network latency of sentinel : "
                     << net_lat/1000.0 << " ms\n"
                     << " [METRIC] Speculative frames done so far: "
                     << speculative_done.load() << " / " << offload_count.load() << "\n"
                     << " [METRIC] Phase-2 will start at frame_id: "
                     << A_boundary.load() << "\n"
                     << "==================================================\n\n";

// ===== METRICS DASHBOARD START =====
                g_dashboard.recordHandoverMetrics(-1.0, net_lat / 1000.0);
                g_dashboard.recordFramesProcessedBeforeHO(speculative_done.load());
// ===== METRICS DASHBOARD END =====

                handover_triggered.store(true, memory_order_release);
                queue_cv.notify_all();
                break;
            }

            pkts++;
            RawFrame rf;
            rf.frame_id       = p.frame_id();
            rf.sensor_path    = p.sensor_path();
            rf.enqueue_us     = now_us;
            rf.net_latency_us = net_lat;

// ===== METRICS DASHBOARD START =====
            g_dashboard.recordPacketReceived();
            g_dashboard.recordLatency(net_lat / 1000.0);
// ===== METRICS DASHBOARD END =====

            PRINT("[RECEIVER ] Frame " << rf.frame_id
                  << "  seq=" << p.sequence_number()
                  << "  net_lat=" << net_lat/1000.0 << " ms");

            {
                lock_guard<mutex> lg(queue_mtx);
                incoming_queue.push_back(rf);

// ===== METRICS DASHBOARD START =====
                g_dashboard.recordQueueLength(incoming_queue.size());
// ===== METRICS DASHBOARD END =====
            }
            queue_cv.notify_one();
        }

        receiver_exited.store(true, memory_order_release);
        queue_cv.notify_all();
    });

    thread processor_thread([&]()
    {
        PRINT("[PROCESSOR] Background processor started.");

        while (true)
        {
            RawFrame rf;
            bool got = false;

            {
                unique_lock<mutex> ul(queue_mtx);
                queue_cv.wait(ul, [&]{
                    return !incoming_queue.empty()
                        || handover_triggered.load(memory_order_acquire)
                        || receiver_exited.load(memory_order_acquire);
                });

                if (!incoming_queue.empty()) {
                    rf  = incoming_queue.front();
                    incoming_queue.pop_front();
                    got = true;
// ===== METRICS DASHBOARD START =====
                    g_dashboard.recordQueueLength(incoming_queue.size());
// ===== METRICS DASHBOARD END =====
                }
            }

            if (!got) {
                if (handover_triggered.load(memory_order_acquire)
                    || receiver_exited.load(memory_order_acquire))
                    break;
                continue;
            }

            auto proc_start = high_resolution_clock::now();
            long long start_us = duration_cast<microseconds>(
                                     proc_start.time_since_epoch()).count();
            double queue_wait_ms = (start_us - rf.enqueue_us) / 1000.0;

            double result = processPacket(rf.frame_id);

            auto proc_end = high_resolution_clock::now();
            double proc_ms = duration_cast<microseconds>(
                                 proc_end - proc_start).count() / 1000.0;

// ===== METRICS DASHBOARD START =====
            g_dashboard.recordProcessingTime(proc_ms, queue_wait_ms);
// ===== METRICS DASHBOARD END =====

            if (rf.frame_id >= 0 && rf.frame_id < STORE_SIZE) {
                processed_store[rf.frame_id].proc_ms        = proc_ms;
                processed_store[rf.frame_id].queue_wait_ms  = queue_wait_ms;
                processed_store[rf.frame_id].net_latency_ms = rf.net_latency_us/1000.0;
                processed_store[rf.frame_id].value          = result;
                processed_store[rf.frame_id].ready.store(true, memory_order_release);
            }

            int done = speculative_done.fetch_add(1, memory_order_relaxed) + 1;

            PRINT("[PROCESSOR] Frame " << rf.frame_id
                  << "  proc=" << proc_ms << " ms"
                  << "  queue_wait=" << queue_wait_ms << " ms"
                  << "  net_lat=" << rf.net_latency_us/1000.0 << " ms"
                  << "  (" << done << "/" << offload_count.load() << " done)");
        }

        PRINT("[PROCESSOR] Exiting. " << speculative_done.load()
              << " frames processed.");
    });

    receiver_thread.join();
    processor_thread.join();

    // ===== CONTEXT MONITOR MODULE START =====
    ctx_monitor.stop();
    {
        ContextVector final_cv = ctx_monitor.getCurrentContext();
        (void)final_cv;
    }
    // ===== CONTEXT MONITOR MODULE END =====

    // Read fresh, after all receiving is done, so this reflects whatever
    // Server A's LAST boundary-sync update (or the final sentinel) said.
    int final_offload_count = offload_count.load(memory_order_acquire);
    int start_frame_id      = A_boundary.load(memory_order_acquire);

    long long phase2_start_us = duration_cast<microseconds>(
                                    high_resolution_clock::now().time_since_epoch()).count();
    long long actual_downtime_us = phase2_start_us
                                   - sentinel_recv_us.load(memory_order_acquire);

// ===== METRICS DASHBOARD START =====
    g_dashboard.setActualDowntime(actual_downtime_us / 1000.0);
// ===== METRICS DASHBOARD END =====

    cout << "\n[SCHEDULER] Vehicle entered Server B zone.\n"
         << "[SCHEDULER] Actual handover downtime : "
         << actual_downtime_us / 1000.0 << " ms\n"
         << "[SCHEDULER] (= time from sentinel receipt to first delivery)\n"
         << "[SCHEDULER] Delivering " << final_offload_count
         << " frames in priority-forward order"
         << " (starting at frame_id=" << start_frame_id << "):\n\n";

    int precomputed  = 0;
    int late_compute = 0;

    for (int i = 0; i < final_offload_count; ++i)
    {
        int fid = start_frame_id + i;

        if (fid < 0 || fid >= STORE_SIZE) continue;

        if (processed_store[fid].ready.load(memory_order_acquire))
        {
            precomputed++;
            cout << "[SCHEDULER] Frame " << fid
                 << "  [PRE-COMPUTED ✓]"
                 << "  proc="       << processed_store[fid].proc_ms       << " ms"
                 << "  queue_wait=" << processed_store[fid].queue_wait_ms  << " ms"
                 << "  net_lat="    << processed_store[fid].net_latency_ms << " ms"
                 << "  → delivering stored result instantly.\n";
        }
        else
        {
            late_compute++;
            cout << "[SCHEDULER] Frame " << fid
                 << "  [LATE-COMPUTE ⚠]  packet lost – computing now...\n";

            auto t0  = high_resolution_clock::now();
            double r = processPacket(fid);
            auto t1  = high_resolution_clock::now();
            double ms = duration_cast<microseconds>(t1 - t0).count() / 1000.0;

            processed_store[fid].value   = r;
            processed_store[fid].proc_ms = ms;
            processed_store[fid].ready.store(true, memory_order_release);

            cout << "[SCHEDULER] Frame " << fid
                 << "  [LATE-COMPUTE ⚠]  done in " << ms << " ms\n";
        }

// ===== METRICS DASHBOARD START =====
        g_dashboard.recordFramesDeliveredAfterHO(1);
        auto t_now_deliv = chrono::high_resolution_clock::now();
        double deliv_delay = chrono::duration_cast<chrono::microseconds>(
                                 t_now_deliv.time_since_epoch()).count() / 1000.0
                             - (phase2_start_us / 1000.0);
        g_dashboard.recordSchedulerDelay(deliv_delay);
// ===== METRICS DASHBOARD END =====
    }

    cout << "\n========================================\n"
         << "  METHOD C  –  SERVER B SUMMARY\n"
         << "  Offload window size        : " << final_offload_count << " frames\n"
         << "  Pre-computed (speculative) : " << precomputed   << " frames\n"
         << "  Late-computed (post-HO)    : " << late_compute  << " frames\n"
         << "  Speculative success rate   : "
         << (final_offload_count > 0
                ? 100.0 * precomputed / final_offload_count
                : 0.0)
         << " %\n"
         << "  Total lost packets         : " << total_lost.load() << "\n"
         << "  Actual handover downtime   : "
         << actual_downtime_us / 1000.0 << " ms\n"
         << "========================================\n";

    cout << "\nVehicle Crossed Boundary Zone\n"
         << "Total Lost Packets for this Run: " << total_lost.load() << "\n";

// ===== METRICS DASHBOARD START =====
    g_dashboard.printFinalReport();
// ===== METRICS DASHBOARD END =====

    close(socketid);

    cout << "\n[SERVER B] Handover processing complete. "
            "Holding pod active for logs...\n";
    while (true) sleep(10);
    return 0;
}
