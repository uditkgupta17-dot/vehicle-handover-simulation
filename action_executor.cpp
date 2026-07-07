#include "action_executor.h"

ActionExecutor::ActionExecutor(ActionSignals& signals, CsvLogger& logger, std::string server_id)
    : signals_(signals), logger_(logger), server_id_(std::move(server_id))
{}

std::string ActionExecutor::buildStatePayload(ContinuityDecision   action,
                                               const ContextVector& cv,
                                               const StateAnalysis& sa)
{
    switch (action) {
        case ContinuityDecision::SPECULATIVE_PREWARM:
            return "predicted_gap_s=" + std::to_string(cv.predicted_gap_duration_s) +
                   ";next_rsu_dist_m=" + std::to_string(cv.dist_to_next_rsu_m);

        case ContinuityDecision::MESSAGE_STATE_TRANSFER:
            // Only the packet queue / sequence / ACK metadata - never execution memory.
            return "queue_length=" + std::to_string(cv.queue_length) +
                   ";task_progress_pct=" + std::to_string(cv.task_progress_pct);

        case ContinuityDecision::POINTER_STATE_TRANSFER:
            // Only file-id/offset/bitmap metadata - never the complete file.
            return "file_size_mb=" + std::to_string(cv.file_size_mb) +
                   ";offset_pct=" + std::to_string(cv.task_progress_pct);

        case ContinuityDecision::CARRY_STATE:
            // Only the minimum useful execution state - never the full process.
            return "min_useful_state_mb=" + std::to_string(sa.minimum_useful_state_size_mb) +
                   ";description=" + sa.execution_state_description;

        case ContinuityDecision::RESTART:
            return "reason=cost_exceeds_recomputation";

        case ContinuityDecision::NORMAL_PIPELINE:
        default:
            return "";
    }
}

void ActionExecutor::execute(const Decision&      decision,
                              const ContextVector& cv,
                              const StateAnalysis& sa,
                              double               recomputation_overhead_ms,
                              double               downtime_ms,
                              bool                 continuity_success)
{
    signals_.last_decision.store(static_cast<int>(decision.action), std::memory_order_relaxed);

    signals_.restart_requested.store(decision.action == ContinuityDecision::RESTART,
                                      std::memory_order_release);

    if (decision.action != ContinuityDecision::NORMAL_PIPELINE) {
        std::string payload = buildStatePayload(decision.action, cv, sa);
        {
            std::lock_guard<std::mutex> lg(signals_.pending_payload_mtx);
            signals_.pending_control_payload = payload;
        }
        signals_.pending_control_action.store(static_cast<int>(decision.action),
                                               std::memory_order_relaxed);
        signals_.has_pending_control_packet.store(true, std::memory_order_release);
    }

    logger_.logRow(server_id_, cv, sa, decision,
                    recomputation_overhead_ms, downtime_ms, continuity_success);
}
