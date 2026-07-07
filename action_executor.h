#ifndef ACTION_EXECUTOR_H
#define ACTION_EXECUTOR_H

#include <atomic>
#include <limits>
#include <mutex>
#include <string>
#include "context_vector.h"
#include "csv_logger.h"
#include "decision_engine.h"
#include "state_analyzer.h"

// Signals ActionExecutor exposes so the *existing* packet-pipeline threads
// can react to a Decision without ActionExecutor ever touching a socket or
// a thread directly. Every field here is safe to poll from any thread.
struct ActionSignals
{
    // How many local work items (e.g. frames) are currently allowed to be
    // processed before the rest is handed downstream. Starts at the
    // original plan's full allocation and is only ever lowered - never
    // by ActionExecutor itself, which has no notion of frame indices
    // (that's workload-specific), but by whichever caller owns that
    // domain (e.g. server_a.cpp's Method C) once the Decision Engine
    // judges the original plan is no longer optimal. Existing loops poll
    // this at a point they already visit; this is the adaptive reverse
    // queue's only trigger, replacing a one-shot all-or-nothing flag.
    std::atomic<int> local_ceiling{std::numeric_limits<int>::max()};

    // True only for RESTART: locally held execution state should be discarded.
    std::atomic<bool> restart_requested{false};

    // Last decision, exposed for logging/observability only.
    std::atomic<int> last_decision{0};

    // A small control datagram describing the chosen strategy, for the
    // caller's already-open socket to relay to the peer server. ActionExecutor
    // decides *what* and *when*; it never opens or writes to sockets itself.
    std::atomic<bool> has_pending_control_packet{false};
    std::atomic<int>  pending_control_action{0};
    std::mutex        pending_payload_mtx;
    std::string       pending_control_payload;
};

// The only component allowed to translate a Decision into pipeline
// behaviour. Per strategy it performs the real, minimal action described in
// the research design (never CRIU, never full-process/container migration):
//   NORMAL_PIPELINE        -> no-op, existing speculative pipeline continues
//   SPECULATIVE_PREWARM    -> flags a prewarm control packet for the next RSU
//   MESSAGE_STATE_TRANSFER -> flags a control packet carrying only queue/ACK metadata
//   POINTER_STATE_TRANSFER -> flags a control packet carrying only file-id/offset metadata
//   CARRY_STATE            -> flags a control packet carrying the minimum useful state descriptor
//   RESTART                -> flags local state for discard + a reset control packet
class ActionExecutor
{
public:
    ActionExecutor(ActionSignals& signals, CsvLogger& logger, std::string server_id);

    void execute(const Decision&      decision,
                 const ContextVector& cv,
                 const StateAnalysis& sa,
                 double               recomputation_overhead_ms = 0.0,
                 double               downtime_ms               = 0.0,
                 bool                 continuity_success        = true);

private:
    ActionSignals& signals_;
    CsvLogger&     logger_;
    std::string    server_id_;

    static std::string buildStatePayload(ContinuityDecision action,
                                          const ContextVector& cv,
                                          const StateAnalysis& sa);
};

#endif
