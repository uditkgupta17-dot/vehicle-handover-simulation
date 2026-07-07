/*
 * server_a.cpp  –  Method C (Speculative Background Offload) + Method B (unchanged)
 *
 * ═══════════════════════════════════════════════════════════════
 *  METHOD C  –  CAPACITY-AWARE SPECULATIVE OFFLOAD
 * ═══════════════════════════════════════════════════════════════
 *
 *  BOUNDARY MATH (done once before any thread starts)
 *  ─────────────────────────────────────────────────
 *  dwell_time_ms   = (RSU_radius_m / speed_ms) × 1000
 *  A_boundary      = floor(dwell_time_ms / PROC_MS_PER_FRAME)
 *                    ← frames Server A can finish locally
 *
 *  B_capacity      = floor(dwell_time_ms / PROC_MS_PER_FRAME)
 *                    ← frames Server B can speculatively finish
 *                       before the vehicle arrives
 *                       (same CPU speed assumed; extend later for
 *                        heterogeneous RSUs)
 *
 *  Speculative window sent to B:
 *      [A_boundary + B_capacity]  ↓  down to  [A_boundary + 1]
 *
 *  If  A_boundary + B_capacity > N,  the window is clamped to N.
 *
 *  THREAD LAYOUT
 *  ─────────────
 *  Thread 1  (LOCAL)   process frames  [1 … A_boundary]  sequentially
 *  Thread 2  (SENDER)  send frames     [window_top … A_boundary+1]
 *                      in reverse order, one packet per 0.5 ms yield
 *
 *  Both threads are created first, then released simultaneously via
 *  std::atomic<bool> start_flag  (store-release / load-acquire).
 *
 *  After both threads join, the main thread sends a single
 *  HANDOVER SENTINEL  (frame_id = -1, is_handover_packet = true)
 *  carrying  handover_status = A_boundary  and
 *             file_size_mb   = B_capacity
 *  so Server B knows the exact range to schedule.
 *
 *  EXTENSIBILITY NOTE
 *  ──────────────────
 *  The architecture is designed for N future RSUs.
 *  Currently one RSU (Server B) is instantiated.
 *  Adding Server C means: compute a second capacity window starting
 *  at  A_boundary + B_capacity + 1,  open a second socket, and launch
 *  a third sender thread.  No structural changes needed.
 *
 * ═══════════════════════════════════════════════════════════════
 *  EXECUTION CONTINUITY DECISION ENGINE INTEGRATION
 * ═══════════════════════════════════════════════════════════════
 *  Vehicle -> Receiver/Dataset -> Context Monitor -> Context Vector ->
 *  State Analyzer -> Decision Engine -> Action Executor ->
 *  Existing Speculative Pipeline (t_local / t_sender, unchanged) -> Server B
 *
 *  Context Monitor, State Analyzer and Decision Engine never change pipeline
 *  behaviour themselves. Action Executor is the only component allowed to:
 *    - flag the reverse queue (hand remaining local work to the sender path)
 *    - flag a RESTART (discard local state)
 *    - queue a small control packet describing the chosen strategy
 *  t_local/t_sender poll these flags at points they already visit; neither
 *  thread's core loop structure changes.
 */

#include <algorithm>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <sstream>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <cmath>
#include <cstdlib>
#include <netdb.h>
#include <iomanip>
#include "custom_payload.pb.h"

// ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
#include "context_vector.h"
#include "context_monitor.h"
#include "scenario_config.h"
#include "state_analyzer.h"
#include "decision_engine.h"
#include "action_executor.h"
#include "csv_logger.h"
#include "failure_injector.h"
#include "traffic_load.h"
// ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

using namespace std;
using namespace chrono;

// ── console mutex keeps interleaved thread prints readable ───────────────────
static mutex g_print_mtx;
#define PRINT(x) { lock_guard<mutex> _lg(g_print_mtx); cout << x << "\n"; }

// ── simulated CPU workload  (~40-100 ms on a typical desktop) ────────────────
//    2 000 000 iterations of sqrt() prevent the compiler from optimising away.
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

struct DeepSenseRow { int index; string img_path; };

int main()
{
    // ── socket setup ─────────────────────────────────────────────────────────
    int socketid = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketid < 0) { cout << "Socket didnt form\n"; return 1; }

    const char* host_env = getenv("SERVER_B_HOST");
    if (!host_env) host_env = "127.0.0.1";

    hostent* h = gethostbyname(host_env);
    if (!h) {
        cout << "Error: Could not resolve " << host_env << "\n";
        close(socketid); return 1;
    }

    sockaddr_in target{};
    target.sin_family = AF_INET;
    target.sin_port   = htons(8080);
    target.sin_addr   = *((in_addr**)h->h_addr_list)[0];

    atomic<int> g_seq{0};          // global sequence counter, shared across threads

    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
    // Scenario parameters (RSU geometry, simulated ranges, failure injection)
    // are loaded once here so neither Method C nor Method B below contain
    // any hardcoded experiment constants.
    ScenarioConfig scenario = ScenarioConfig::load("scenario.conf");
    CsvLogger      csv_logger(scenario.getString("csv_log_path", "continuity_log.csv"));
    FailureInjector failure_injector(scenario);
    ActionSignals   action_signals;
    ActionExecutor  action_executor(action_signals, csv_logger, "SERVER_A");

    // Sends whatever control packet ActionExecutor most recently queued (if
    // any) over the socket that already exists for this run. This is the
    // ONLY place server_a.cpp performs networking on ActionExecutor's
    // behalf; ActionExecutor itself never touches the socket.
    auto sendControlPacketIfPending = [&](double speed_kmh_for_pkt)
    {
        if (!action_signals.has_pending_control_packet.exchange(false, memory_order_acq_rel))
            return;

        string payload;
        {
            lock_guard<mutex> lg(action_signals.pending_payload_mtx);
            payload = action_signals.pending_control_payload;
        }
        int action_int = action_signals.pending_control_action.load(memory_order_relaxed);
        int seq = g_seq.fetch_add(1, memory_order_relaxed) + 1;

        edgesim::VehicleMetadata ctrl;
        ctrl.set_frame_id(-2); // distinct from -1 (handover sentinel)
        ctrl.set_sequence_number(seq);
        ctrl.set_is_control_packet(true);
        ctrl.set_continuity_action(action_int);
        ctrl.set_state_payload(payload);
        ctrl.set_vehicle_speed_kmh(speed_kmh_for_pkt);
        ctrl.set_timestamp_ns(
            duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count());

        string out;
        if (!ctrl.SerializeToString(&out)) {
            PRINT("[ACTION-EXECUTOR] ERROR: failed to serialize control packet - skipping send.");
            return;
        }
        sendto(socketid, out.c_str(), out.size(), 0, (sockaddr*)&target, sizeof(target));

        PRINT("[ACTION-EXECUTOR] Sent control packet  action=" << action_int
              << "  payload=" << payload);
    };

    // TrafficLoad models background vehicles competing for THIS server's
    // resources - it never generates application packets, only real CPU/
    // memory/queue pressure (see traffic_load.h). Shared across whichever
    // method runs below.
    TrafficLoad traffic_load;
    // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

    // ── user menu ────────────────────────────────────────────────────────────
    int    choice;
    double speed_kmh;
    cout << "========================================\n"
         << "Select Vehicular Workload:\n"
         << "1. Compute / Safety (Method C - Speculative Offload)\n"
         << "2. Entertainment / Video (Method B - Downlink Cache Shift)\n"
         << "Choice: ";
    cin >> choice;
    cout << "Enter Vehicle Speed (km/h): ";
    cin >> speed_kmh;
    cout << "========================================\n";

    // ===== EXECUTION CONTINUITY: VEHICLE CONTROLLER LIVE COMMANDS START =====
    // live_speed_kmh replaces the fixed speed_kmh wherever the Context
    // Monitor reads vehicle speed, so a live "speed" command from the
    // Vehicle Controller is felt on the very next sample. The original
    // upfront capacity plan (A_boundary/window) is NOT recomputed - only
    // the adaptive ceiling below reacts to it, by design.
    atomic<double> live_speed_kmh{speed_kmh};

    // Starts a UDP listener on controller_port that applies "speed",
    // "traffic" and "fail" commands from the standalone vehicle_controller
    // tool, then wakes the given ContextMonitor for an immediate
    // re-evaluation. This is purely additive: it never touches the existing
    // data socket/receive path on port 8080.
    auto startControllerListener = [&](ContextMonitor& ctx_monitor)
    {
        int controller_port = scenario.getInt("controller_port", 9090);
        return thread([&ctx_monitor, &live_speed_kmh, &traffic_load, &failure_injector, controller_port]()
        {
            int csock = socket(AF_INET, SOCK_DGRAM, 0);
            if (csock < 0) { PRINT("[CONTROLLER] Could not open control socket."); return; }

            sockaddr_in caddr{};
            caddr.sin_family      = AF_INET;
            caddr.sin_addr.s_addr = INADDR_ANY;
            caddr.sin_port        = htons(controller_port);
            if (::bind(csock, (sockaddr*)&caddr, sizeof(caddr)) < 0) {
                PRINT("[CONTROLLER] Could not bind control port " << controller_port << ".");
                close(csock);
                return;
            }
            PRINT("[CONTROLLER] Listening for live commands on UDP " << controller_port << ".");

            char cbuf[2048];
            sockaddr_in from{};
            socklen_t   fromlen = sizeof(from);
            while (true) {
                int n = recvfrom(csock, cbuf, sizeof(cbuf), 0, (sockaddr*)&from, &fromlen);
                if (n <= 0) continue;

                edgesim::ControllerCommand cmd;
                if (!cmd.ParseFromArray(cbuf, n)) continue;

                if (cmd.command() == "speed") {
                    live_speed_kmh.store(cmd.value(), memory_order_relaxed);
                    PRINT("[CONTROLLER] Speed set to " << cmd.value() << " km/h.");
                } else if (cmd.command() == "traffic") {
                    traffic_load.setLevel(static_cast<int>(cmd.value()));
                    PRINT("[CONTROLLER] Traffic level set to " << cmd.value() << ".");
                } else if (cmd.command() == "fail") {
                    failure_injector.setOverride(cmd.fault_name(), cmd.enable());
                    PRINT("[CONTROLLER] Failure '" << cmd.fault_name() << "' forced "
                          << (cmd.enable() ? "ON" : "OFF") << ".");
                }
                ctx_monitor.triggerImmediateSample();
            }
        });
    };
    // ===== EXECUTION CONTINUITY: VEHICLE CONTROLLER LIVE COMMANDS END =====

    // ════════════════════════════════════════════════════════════════════════
    //  METHOD C
    // ════════════════════════════════════════════════════════════════════════
    if (choice == 1)
    {
        // ── load CSV ─────────────────────────────────────────────────────────
        vector<DeepSenseRow> dataset;
        {
            ifstream f("scenario3.csv");
            if (!f.is_open()) {
                cout << "Error: could not open scenario3.csv\n";
                close(socketid); return 1;
            }
            string line;
            getline(f, line);               // skip header
            while (getline(f, line)) {
                stringstream ss(line);
                string a, b;
                getline(ss, a, ',');
                getline(ss, b, ',');
                if (a.empty()) continue;
                dataset.push_back({stoi(a), b});
            }
        }

        int N = static_cast<int>(dataset.size());
        if (N == 0) { cout << "Dataset empty.\n"; close(socketid); return 1; }
        cout << "[SERVER A] Loaded " << N << " frames from scenario3.csv\n";

        // ── CAPACITY MATH ─────────────────────────────────────────────────────
        const double RSU_RADIUS_M      = scenario.getDouble("rsu_radius_m", 200.0);
        const double RSU_GAP_M         = scenario.getDouble("rsu_gap_m", 5.0);
        const double PROC_MS_PER_FRAME = scenario.getDouble("proc_ms_per_frame", 70.0);
        const int    WIRE_TASK_TYPE    = scenario.getInt("method_c.wire_task_type", 0);

        double speed_ms      = speed_kmh * (5.0 / 18.0);
        double dwell_ms      = (RSU_RADIUS_M / speed_ms) * 1000.0;

        int A_boundary  = max(1, static_cast<int>(dwell_ms / PROC_MS_PER_FRAME));
        A_boundary      = min(A_boundary, N - 1);

        // Server B has dwell_ms of preparation time before the vehicle arrives.
        // BUG FIX: at very low speeds dwell_ms/PROC_MS_PER_FRAME can vastly
        // exceed the frames actually left in the dataset (e.g. 5142 computed
        // vs. 1487 total), which then gets reported/sent as-is. Every
        // adaptive partition must satisfy 0 <= capacity <= remaining frames.
        int raw_B_capacity = static_cast<int>(dwell_ms / PROC_MS_PER_FRAME);
        int B_capacity  = clamp(raw_B_capacity, 0, max(0, N - A_boundary));

        // clamp window top to dataset size (0-based index)
        int window_top  = min(A_boundary + B_capacity, N) - 1;   // dataset index (inclusive)
        int window_bot  = A_boundary;                              // dataset index (inclusive lower)

        int offload_count = window_top - window_bot + 1; // FIXED: inclusive range

        cout << "\n[PREDICTION ENGINE] ──────────────────────────────────\n"
             << "  Vehicle speed        : " << speed_kmh << " km/h ("
             << speed_ms << " m/s)\n"
             << "  Dwell time in zone   : " << dwell_ms << " ms\n"
             << "  A_boundary (local)   : frames 1 .. " << dataset[A_boundary-1].index
             << "  (" << A_boundary << " frames)\n"
             << "  B_capacity (offload) : " << offload_count << " frames\n"
             << "  Speculative window   : frame " << dataset[window_bot].index
             << " .. " << dataset[window_top].index
             << "  (sent in reverse)\n"
             << "──────────────────────────────────────────────────────\n\n";

        if (offload_count <= 0) {
            cout << "[SERVER A] Nothing to offload (B_capacity = 0).\n";
            close(socketid); return 0;
        }

        // ===== CONTEXT MONITOR MODULE START =====
        atomic<int> local_done{0};
        atomic<int> sent_done{0};

        // ===== EXECUTION CONTINUITY: ADAPTIVE REVERSE-PRIORITY QUEUE STATE =====
        // reverse_cursor is a priority frontier, not a FIFO: it starts at the
        // highest reclaimable index (A_boundary-1) and only ever counts DOWN,
        // so the highest frame ID always has the highest transmission
        // priority (1485, 1484, 1483, ... never the reverse). It is read/
        // written by t_reverse_priority only; t_local treats it as a
        // read-only "already sent, do not touch" high-water mark - this is
        // what lets a later boundary EXPANSION reclaim still-pending frames
        // without ever re-sending or re-processing one that already crossed
        // the wire. Local processing is only "covered" (fully accounted for,
        // either processed or sent) once local_done exceeds reverse_cursor.
        atomic<int> reverse_cursor{A_boundary - 1};
        atomic<int> reverse_sent_done{0};

        ContextMonitorInputs cm_inputs;
        cm_inputs.rsu_id        = "RSU-A";
        cm_inputs.rsu_radius_m  = RSU_RADIUS_M;
        cm_inputs.rsu_gap_m     = RSU_GAP_M;
        cm_inputs.task_type     = TaskType::PACKET_STREAM;
        cm_inputs.file_size_mb  = offload_count;
        cm_inputs.baseline_state_size_mb    = scenario.getDouble("server_a.baseline_state_size_mb", 1.0);
        cm_inputs.state_growth_amplitude_mb = scenario.getDouble("server_a.state_growth_amplitude_mb", 3.0);
        cm_inputs.bandwidth_mbps = { scenario.getDouble("server_a.bandwidth_base_mbps", 10.0),
                                     scenario.getDouble("server_a.bandwidth_amplitude_mbps", 10.0),
                                     scenario.getDouble("server_a.bandwidth_frequency", 0.3) };
        cm_inputs.latency_ms     = { scenario.getDouble("server_a.latency_base_ms", 0.05),
                                     scenario.getDouble("server_a.latency_amplitude_ms", 0.01),
                                     scenario.getDouble("server_a.latency_frequency", 1.1) };
        cm_inputs.rssi_dbm       = { scenario.getDouble("server_a.rssi_base_dbm", -55.0),
                                     scenario.getDouble("server_a.rssi_amplitude_dbm", -15.0),
                                     scenario.getDouble("server_a.rssi_frequency", 0.2) };
        cm_inputs.cpu_util_pct   = { scenario.getDouble("server_a.cpu_base_pct", 40.0),
                                     scenario.getDouble("server_a.cpu_amplitude_pct", 40.0),
                                     scenario.getDouble("server_a.cpu_frequency", 0.5) };
        cm_inputs.memory_mb      = { scenario.getDouble("server_a.memory_base_mb", 128.0),
                                     scenario.getDouble("server_a.memory_amplitude_mb", 32.0),
                                     scenario.getDouble("server_a.memory_frequency", 0.4) };
        cm_inputs.packet_loss_pct_fixed = scenario.getDouble("server_a.packet_loss_pct_fixed", 1.0);
        cm_inputs.speed_kmh_atomic      = &live_speed_kmh;
        cm_inputs.traffic_load          = &traffic_load;
        cm_inputs.progress_done         = &local_done;
        cm_inputs.interval_s            = scenario.getInt("context_monitor_interval_s", 1);

        // Adaptive boundary state: local_ceiling starts at the full plan and
        // is only ever lowered, once local processing has produced a few
        // real throughput samples, using CURRENT (possibly live-updated)
        // remaining dwell time - this is what makes the reverse queue
        // dynamic instead of a one-shot all-or-nothing hand-off.
        // It also doubles as the LIVE progress denominator (BUG FIX: the
        // denominator must shrink along with the ceiling, otherwise
        // progress percentages/CSV rows freeze below 100% forever once an
        // adaptive shrink happens).
        action_signals.local_ceiling.store(A_boundary, memory_order_relaxed);
        cm_inputs.progress_total = &action_signals.local_ceiling;
        atomic<bool> threads_started{false};
        high_resolution_clock::time_point threads_start_time;

        // ===== EXECUTION CONTINUITY: BOUNDARY SYNCHRONIZATION START =====
        // The reverse-queue extras (frame indices [ceiling, window_bot)) are
        // always contiguous with the original speculative window
        // ([window_bot, window_top]), so BOTH can be described to Server B
        // as one combined range starting at whatever the ceiling currently
        // is. Both helpers clamp to window_bot so a ceiling that hasn't
        // shrunk yet reproduces the original plan exactly.
        auto currentStartFrameId = [&](int ceiling_idx) {
            return dataset[clamp(ceiling_idx, 0, window_bot)].index;
        };
        auto currentTotalCount = [&](int ceiling_idx) {
            return window_top - clamp(ceiling_idx, 0, window_bot) + 1;
        };

        // Sent over the EXISTING data socket (never a new one) the instant
        // the ceiling shrinks, so Server B's offload_count/start-frame
        // bookkeeping is updated as soon as Server A decides the new plan -
        // not just at the final handover sentinel.
        auto sendBoundaryUpdate = [&](int new_ceiling_idx)
        {
            int new_start_frame_id = currentStartFrameId(new_ceiling_idx);
            int new_total_count    = currentTotalCount(new_ceiling_idx);

            int seq = g_seq.fetch_add(1, memory_order_relaxed) + 1;
            edgesim::VehicleMetadata upd;
            upd.set_frame_id(-3); // distinct from -1 (sentinel) and -2 (control packet)
            upd.set_sequence_number(seq);
            upd.set_is_boundary_update(true);
            upd.set_handover_status(new_start_frame_id);
            upd.set_file_size_mb(new_total_count);
            upd.set_vehicle_speed_kmh(live_speed_kmh.load(memory_order_relaxed));
            upd.set_timestamp_ns(
                duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count());

            string payload;
            if (!upd.SerializeToString(&payload)) {
                PRINT("[BOUNDARY-SYNC] ERROR: failed to serialize boundary update - skipping send.");
                return;
            }
            sendto(socketid, payload.c_str(), payload.size(), 0, (sockaddr*)&target, sizeof(target));

            PRINT("[BOUNDARY-SYNC] Sent update to Server B: start_frame_id=" << new_start_frame_id
                  << "  total_count=" << new_total_count);
        };
        // ===== EXECUTION CONTINUITY: BOUNDARY SYNCHRONIZATION END =====

        // Instantiate and start the Context Monitor. Every tick it hands its
        // freshly sampled ContextVector to the rest of the continuity
        // pipeline via on_sample; the monitor itself makes no decisions.
        ContextMonitor ctx_monitor(cm_inputs, [&](const ContextVector& raw_cv)
        {
            ContextVector cv = failure_injector.apply(raw_cv);
            StateAnalysis sa = StateAnalyzer::analyze(cv);
            Decision decision = DecisionEngine::evaluate(cv, sa);
            action_executor.execute(decision, cv, sa);
            sendControlPacketIfPending(cv.vehicle_speed_kmh);

            // Adaptive boundary: recompute how much local work is feasible
            // using empirically observed throughput (local_done / elapsed)
            // against the CURRENT remaining dwell time, which already
            // reflects any live speed change. Runs every tick, in BOTH
            // directions - shrinks when conditions worsen, expands back
            // when the vehicle slows down again - not gated on which
            // strategy the Decision Engine happens to name, since the
            // physical capacity estimate is independent of that label.
            //
            // safe_upper_cap prevents an expansion from ever reclaiming a
            // frame the reverse-priority thread has ALREADY sent: once
            // reverse_cursor has counted down past a frame, that frame is
            // committed to Server B and local_ceiling can never climb back
            // above it. This is what guarantees no retransmission and no
            // duplicate execution across a shrink-then-expand cycle.
            if (threads_started.load(memory_order_acquire))
            {
                int done = local_done.load(memory_order_relaxed);
                if (done >= 3) {
                    double elapsed_ms = duration_cast<microseconds>(
                        high_resolution_clock::now() - threads_start_time).count() / 1000.0;
                    if (elapsed_ms > 0.0) {
                        double throughput_per_ms = done / elapsed_ms;
                        int ideal_ceiling = done + static_cast<int>(
                            throughput_per_ms * cv.remaining_dwell_s * 1000.0);
                        int safe_upper_cap = min(A_boundary, reverse_cursor.load(memory_order_acquire) + 1);
                        int new_ceiling = clamp(ideal_ceiling, done, safe_upper_cap);
                        int old_ceiling = action_signals.local_ceiling.load(memory_order_relaxed);
                        if (new_ceiling != old_ceiling) {
                            action_signals.local_ceiling.store(new_ceiling, memory_order_release);
                            sendBoundaryUpdate(new_ceiling);
                            PRINT("[DECISION-ENGINE] Adaptive boundary "
                                  << (new_ceiling < old_ceiling ? "shrink" : "expansion") << ": local ceiling "
                                  << old_ceiling << " -> " << new_ceiling
                                  << "  (remaining_dwell=" << cv.remaining_dwell_s << "s)");
                        }
                    }
                }
            }
        });
        ctx_monitor.start();
        startControllerListener(ctx_monitor).detach();
        // ===== CONTEXT MONITOR MODULE END =====

        // ── rendezvous flag – both threads released at same instant ──────────
        atomic<bool> start_flag{false};

        // ────────────────────────────────────────────────────────────────────
        //  THREAD 1  –  LOCAL PROCESSOR
        //  Processes frames dataset[0] … dataset[A_boundary-1] sequentially.
        //  Its effective upper bound is min(local_ceiling, reverse_cursor+1):
        //  the ceiling is the Decision Engine's policy, reverse_cursor+1 is
        //  the hard physical fact of what the reverse-priority thread has
        //  ALREADY sent - the smaller of the two always wins, which is what
        //  makes a boundary expansion safe (it can only reclaim frames that
        //  are still actually pending, never one already on the wire).
        //  Never exits early on a shrink: it just polls and waits, because
        //  the ceiling may expand again later and hand it more work.
        // ────────────────────────────────────────────────────────────────────
        thread t_local([&]() {
            while (!start_flag.load(memory_order_acquire))
                this_thread::yield();

            PRINT("[THREAD-LOCAL] Started. Processing frames 1.."
                  << dataset[A_boundary-1].index << " locally.");

            bool waiting_logged = false;
            while (true) {
                int i       = local_done.load(memory_order_acquire);
                int ceiling = action_signals.local_ceiling.load(memory_order_acquire);
                int cursor  = reverse_cursor.load(memory_order_acquire);
                int effective_bound = min(ceiling, cursor + 1);

                if (i >= effective_bound) {
                    if (i > cursor) break; // covered: nothing left on either side, done for good
                    if (!waiting_logged) {
                        PRINT("[THREAD-LOCAL] Waiting at frame " << dataset[i].index
                              << " (ceiling=" << ceiling << ") - may resume if the boundary expands.");
                        waiting_logged = true;
                    }
                    this_thread::sleep_for(milliseconds(5));
                    continue;
                }
                waiting_logged = false;

                auto t0  = high_resolution_clock::now();
                double r = processPacket(dataset[i].index);
                auto t1  = high_resolution_clock::now();
                double ms = duration_cast<microseconds>(t1-t0).count()/1000.0;

                local_done.fetch_add(1, memory_order_release);

                PRINT("[THREAD-LOCAL] Frame " << dataset[i].index
                      << "  proc=" << ms << " ms"
                      << "  (checksum=" << (long long)(r)%9999 << ")");
                (void)r;
            }
            PRINT("[THREAD-LOCAL] Done. " << local_done.load() << " frames processed.");
        });

        // ────────────────────────────────────────────────────────────────────
        //  THREAD 2  –  SPECULATIVE SENDER
        //  Sends dataset[window_top] down to dataset[window_bot]  (reverse).
        //  Carries raw frame descriptor; Server B calls processPacket() there.
        // ────────────────────────────────────────────────────────────────────
        thread t_sender([&]() {
            while (!start_flag.load(memory_order_acquire))
                this_thread::yield();

            PRINT("[THREAD-SENDER] Started. Sending "
                  << offload_count << " frames  ["
                  << dataset[window_top].index << " → "
                  << dataset[window_bot].index << "]  (reverse).");

            for (int i = window_top; i >= window_bot; --i) { // FIXED: inclusive lower bound
                int seq = g_seq.fetch_add(1, memory_order_relaxed) + 1;

                edgesim::VehicleMetadata pkt;
                pkt.set_frame_id(dataset[i].index);
                pkt.set_sensor_path(dataset[i].img_path);
                pkt.set_task_type(WIRE_TASK_TYPE);
                pkt.set_sequence_number(seq);
                pkt.set_is_handover_packet(false);
                pkt.set_vehicle_speed_kmh(speed_kmh);
                // file_size_mb carries offload_count so Server B knows the total
                pkt.set_file_size_mb(offload_count);
                // FIXED: send actual starting frame_id so Server B iterates correct IDs in Phase-2
                pkt.set_handover_status(dataset[window_bot].index);
                pkt.set_timestamp_ns(
                    duration_cast<microseconds>(
                        high_resolution_clock::now().time_since_epoch()).count());

                string payload;
                if (!pkt.SerializeToString(&payload)) {
                    PRINT("[THREAD-SENDER] ERROR: failed to serialize frame " << dataset[i].index
                          << " - skipping send.");
                } else {
                    sendto(socketid, payload.c_str(), payload.size(), 0,
                           (sockaddr*)&target, sizeof(target));

                    int done = sent_done.fetch_add(1, memory_order_relaxed) + 1;
                    PRINT("[THREAD-SENDER] Sent frame " << dataset[i].index
                          << "  seq=" << seq
                          << "  (" << done << "/" << offload_count << ")");
                }

                // 0.5 ms kernel yield – interleaves with t_local, no busy spin
                timespec ts{0, 500'000};
                nanosleep(&ts, nullptr);
            }
            PRINT("[THREAD-SENDER] Done. " << sent_done.load() << " frames sent.");
        });

        // ────────────────────────────────────────────────────────────────────
        //  THREAD 3  –  ADAPTIVE REVERSE-PRIORITY SENDER
        //  Runs concurrently with t_local from the very start (not just
        //  after a shrink is detected). It is a priority countdown, not a
        //  FIFO: reverse_cursor starts at A_boundary-1 and only ever moves
        //  down, so whatever it sends is always the CURRENT highest
        //  reclaimed frame first (1485, 1484, 1483, ... never ascending).
        //  It only ever sends a frame that is at or above the CURRENT
        //  ceiling; if the ceiling later expands back above the cursor, this
        //  thread simply goes idle there - t_local becomes free to claim
        //  that range instead, with no coordination beyond both threads
        //  reading the same two atomics.
        // ────────────────────────────────────────────────────────────────────
        thread t_reverse_priority([&]() {
            while (!start_flag.load(memory_order_acquire))
                this_thread::yield();

            PRINT("[THREAD-REVERSE] Started. Priority countdown armed from frame "
                  << dataset[A_boundary - 1].index << " downward (idle until the boundary shrinks).");

            bool waiting_logged = false;
            while (true) {
                int cursor  = reverse_cursor.load(memory_order_acquire);
                int ceiling = action_signals.local_ceiling.load(memory_order_acquire);

                if (cursor < ceiling) {
                    int done = local_done.load(memory_order_acquire);
                    if (done > cursor) break; // covered: nothing left on either side, done for good
                    if (!waiting_logged) {
                        PRINT("[THREAD-REVERSE] Idle at frame " << (cursor >= 0 ? dataset[cursor].index : -1)
                              << " (ceiling=" << ceiling << ") - may resume if the boundary shrinks again.");
                        waiting_logged = true;
                    }
                    this_thread::sleep_for(milliseconds(5));
                    continue;
                }
                waiting_logged = false;

                int seq = g_seq.fetch_add(1, memory_order_relaxed) + 1;
                edgesim::VehicleMetadata pkt;
                pkt.set_frame_id(dataset[cursor].index);
                pkt.set_sensor_path(dataset[cursor].img_path);
                pkt.set_task_type(WIRE_TASK_TYPE);
                pkt.set_sequence_number(seq);
                pkt.set_is_handover_packet(false);
                pkt.set_vehicle_speed_kmh(live_speed_kmh.load(memory_order_relaxed));
                pkt.set_file_size_mb(offload_count);
                pkt.set_handover_status(dataset[window_bot].index);
                pkt.set_timestamp_ns(
                    duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count());

                string payload;
                if (!pkt.SerializeToString(&payload)) {
                    PRINT("[THREAD-REVERSE] ERROR: failed to serialize frame " << dataset[cursor].index
                          << " - skipping send.");
                } else {
                    sendto(socketid, payload.c_str(), payload.size(), 0,
                           (sockaddr*)&target, sizeof(target));

                    int done_count = reverse_sent_done.fetch_add(1, memory_order_relaxed) + 1;
                    PRINT("[THREAD-REVERSE] Sending frame " << dataset[cursor].index
                          << "  seq=" << seq << "  (priority countdown, " << done_count << " sent)");
                }

                reverse_cursor.store(cursor - 1, memory_order_release);

                // 0.5 ms kernel yield - same pacing as the main sender, keeps
                // this interleaved with t_local rather than busy-spinning.
                timespec ts{0, 500'000};
                nanosleep(&ts, nullptr);
            }
            PRINT("[THREAD-REVERSE] Done. " << reverse_sent_done.load() << " adaptive frame(s) sent.");
        });

        // ── release all three threads simultaneously ──────────────────────
        threads_start_time = high_resolution_clock::now();
        threads_started.store(true, memory_order_release);
        start_flag.store(true, memory_order_release);
        t_local.join();
        t_sender.join();
        t_reverse_priority.join();

        // ===== CONTEXT MONITOR MODULE START =====
        ctx_monitor.stop();
        {
            ContextVector final_cv = ctx_monitor.getCurrentContext();
            (void)final_cv;   // suppress unused-variable warning
        }
        // ===== CONTEXT MONITOR MODULE END =====

        // ── HANDOVER SENTINEL ─────────────────────────────────────────────────
        //    frame_id      = -1          (sentinel marker)
        //    handover_status = start_frame_id for Server B's Phase-2 range
        //    file_size_mb    = total frame count in that range
        //    is_handover_packet = true   (triggers Phase-2 scheduler on B)
        //    BUG FIX: these two values MUST reflect the final adaptive
        //    ceiling, not the original plan - otherwise this sentinel would
        //    silently overwrite every earlier boundary-sync update with
        //    stale numbers right at the moment Server B needs them most.
        //    Uses local_done (the REAL number of frames t_local actually
        //    finished) rather than action_signals.local_ceiling: by the time
        //    both threads have joined, local_done and reverse_cursor+1 are
        //    guaranteed equal (that equality is their shared termination
        //    condition) and together describe the true physical split -
        //    local_ceiling is only ever the Decision Engine's policy target,
        //    which can momentarily sit above the frontier the reverse
        //    sender actually reached before both threads stopped.
        {
            int final_ceiling = local_done.load(memory_order_acquire);
            int final_start_frame_id = currentStartFrameId(final_ceiling);
            int final_total_count    = currentTotalCount(final_ceiling);

            int seq = g_seq.fetch_add(1, memory_order_relaxed) + 1;
            edgesim::VehicleMetadata sentinel;
            sentinel.set_frame_id(-1);
            sentinel.set_task_type(WIRE_TASK_TYPE);
            sentinel.set_sequence_number(seq);
            sentinel.set_is_handover_packet(true);
            sentinel.set_vehicle_speed_kmh(speed_kmh);
            sentinel.set_handover_status(final_start_frame_id);
            sentinel.set_file_size_mb(final_total_count);
            sentinel.set_timestamp_ns(
                duration_cast<microseconds>(
                    high_resolution_clock::now().time_since_epoch()).count());

            string payload;
            if (!sentinel.SerializeToString(&payload)) {
                cout << "\n[SERVER A] ERROR: failed to serialize HANDOVER SENTINEL - skipping send.\n";
            } else {
                sendto(socketid, payload.c_str(), payload.size(), 0,
                       (sockaddr*)&target, sizeof(target));

                cout << "\n[SERVER A] HANDOVER SENTINEL sent  (seq=" << seq << ")\n";
            }
        }

        cout << "\n========================================\n"
             << "  METHOD C  –  SERVER A SUMMARY\n"
             << "  Local frames processed        : " << local_done.load() << "\n"
             << "  Speculative pkts sent (window) : " << sent_done.load() << "\n"
             << "  Adaptive reverse-priority sent : " << reverse_sent_done.load() << "\n"
             << "  B_capacity (computed)          : " << B_capacity << "\n"
             << "  Last continuity action         : " << action_signals.last_decision.load() << "\n"
             << "========================================\n";
    }

    // ════════════════════════════════════════════════════════════════════════
    //  METHOD B  –  DOWNLINK CACHE SHIFT  (original logic, unchanged)
    // ════════════════════════════════════════════════════════════════════════
    else if (choice == 2)
    {
        const double FAST_SPEED_THRESHOLD_KMH = scenario.getDouble("method_b.fast_speed_threshold_kmh", 100.0);
        const int    READ_BY_CAR_NORMAL        = scenario.getInt("method_b.read_by_car_normal", 9);
        const int    READ_BY_CAR_FAST          = scenario.getInt("method_b.read_by_car_fast", 7);
        const int    TOTAL_PACKETS             = scenario.getInt("method_b.total_packets", 10);
        const int    WIRE_TASK_TYPE            = scenario.getInt("method_b.wire_task_type", 1);

        cout << "\n[STATE] Server A initially holds video packets 1 to " << TOTAL_PACKETS << ".\n"
             << "[STATE] Server B is pre-loaded with video packets " << (TOTAL_PACKETS + 1)
             << " to " << (TOTAL_PACKETS * 2) << ".\n";

        int read_by_car = (speed_kmh > FAST_SPEED_THRESHOLD_KMH) ? READ_BY_CAR_FAST : READ_BY_CAR_NORMAL;
        if (speed_kmh > FAST_SPEED_THRESHOLD_KMH)
            cout << "\n[PREDICTION] High speed detected. Car leaving zone early.\n";
        else
            cout << "\n[PREDICTION] Nominal speed. Car finishing most packets.\n";

        cout << "Car read up to packet " << read_by_car << " before crossing boundary.\n"
             << "\n[NETWORK ALARM] Initiating Overflow Shift for unread packets...\n";

        int total_shift = TOTAL_PACKETS - read_by_car;
        bool first_ho   = true;
        int  seq        = 0;

        // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
        atomic<int> sent_progress{0};
        atomic<int> progress_total_b{max(1, total_shift)}; // fixed for Method B's lifetime

        ContextMonitorInputs cm_inputs;
        cm_inputs.rsu_id       = "RSU-A";
        cm_inputs.rsu_radius_m = scenario.getDouble("rsu_radius_m", 200.0);
        cm_inputs.rsu_gap_m    = scenario.getDouble("rsu_gap_m", 5.0);
        cm_inputs.task_type    = TaskType::LARGE_FILE_TRANSFER;
        cm_inputs.file_size_mb = total_shift;
        cm_inputs.baseline_state_size_mb    = scenario.getDouble("server_a.baseline_state_size_mb", 1.0);
        cm_inputs.state_growth_amplitude_mb = scenario.getDouble("server_a.state_growth_amplitude_mb", 3.0);
        cm_inputs.bandwidth_mbps = { scenario.getDouble("server_a.bandwidth_base_mbps", 10.0),
                                     scenario.getDouble("server_a.bandwidth_amplitude_mbps", 10.0),
                                     scenario.getDouble("server_a.bandwidth_frequency", 0.3) };
        cm_inputs.latency_ms     = { scenario.getDouble("server_a.latency_base_ms", 0.05),
                                     scenario.getDouble("server_a.latency_amplitude_ms", 0.01),
                                     scenario.getDouble("server_a.latency_frequency", 1.1) };
        cm_inputs.rssi_dbm       = { scenario.getDouble("server_a.rssi_base_dbm", -55.0),
                                     scenario.getDouble("server_a.rssi_amplitude_dbm", -15.0),
                                     scenario.getDouble("server_a.rssi_frequency", 0.2) };
        cm_inputs.cpu_util_pct   = { scenario.getDouble("server_a.cpu_base_pct", 40.0),
                                     scenario.getDouble("server_a.cpu_amplitude_pct", 40.0),
                                     scenario.getDouble("server_a.cpu_frequency", 0.5) };
        cm_inputs.memory_mb      = { scenario.getDouble("server_a.memory_base_mb", 128.0),
                                     scenario.getDouble("server_a.memory_amplitude_mb", 32.0),
                                     scenario.getDouble("server_a.memory_frequency", 0.4) };
        cm_inputs.packet_loss_pct_fixed = scenario.getDouble("server_a.packet_loss_pct_fixed", 1.0);
        cm_inputs.speed_kmh_atomic      = &live_speed_kmh;
        cm_inputs.traffic_load          = &traffic_load;
        cm_inputs.progress_total        = &progress_total_b;
        cm_inputs.progress_done         = &sent_progress;
        cm_inputs.interval_s            = scenario.getInt("context_monitor_interval_s", 1);

        ContextMonitor ctx_monitor(cm_inputs, [&](const ContextVector& raw_cv)
        {
            ContextVector cv = failure_injector.apply(raw_cv);
            StateAnalysis sa = StateAnalyzer::analyze(cv);
            Decision decision = DecisionEngine::evaluate(cv, sa);
            action_executor.execute(decision, cv, sa);
            sendControlPacketIfPending(cv.vehicle_speed_kmh);
        });
        ctx_monitor.start();
        startControllerListener(ctx_monitor).detach();
        // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====

        for (int j = read_by_car + 1; j <= TOTAL_PACKETS; ++j) {
            ++seq;
            edgesim::VehicleMetadata pkt;
            pkt.set_frame_id(j);
            pkt.set_task_type(WIRE_TASK_TYPE);
            pkt.set_sensor_path("Video_Chunk_" + to_string(j) + ".mp4");
            pkt.set_file_size_mb(total_shift);
            pkt.set_sequence_number(seq);
            if (first_ho) { pkt.set_is_handover_packet(true); first_ho = false; }
            else            pkt.set_is_handover_packet(false);
            pkt.set_timestamp_ns(
                duration_cast<microseconds>(
                    high_resolution_clock::now().time_since_epoch()).count());

            string payload;
            if (!pkt.SerializeToString(&payload)) {
                cout << "[METHOD B TELEMETRY] ERROR: failed to serialize packet " << j
                     << " - skipping send.\n";
            } else {
                cout << "[METHOD B TELEMETRY] Seq: " << seq
                     << " | Shifting Packet " << j << " -> Server B\n";
                sendto(socketid, payload.c_str(), payload.size(), 0,
                       (sockaddr*)&target, sizeof(target));
                sent_progress.fetch_add(1, memory_order_relaxed);
            }
            sleep(1);
        }

        // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES START =====
        ctx_monitor.stop();
        // ===== EXECUTION CONTINUITY DECISION ENGINE MODULES END =====
    }

    cout << "\nTrace execution successfully ended.\n";
    close(socketid);
    return 0;
}