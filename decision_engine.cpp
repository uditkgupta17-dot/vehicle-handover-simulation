#include "decision_engine.h"
#include <vector>
#include <algorithm>
#include <cmath>
#include <limits>

// Temporal context persistence across evaluation frames
static double s_historical_speed_kmh = -1.0;
static int    s_historical_queue_length = -1;

Decision DecisionEngine::evaluate(const ContextVector& cv, const StateAnalysis& sa)
{
    // Initialize history context on initialization pass
    if (s_historical_speed_kmh < 0.0) s_historical_speed_kmh = cv.vehicle_speed_kmh;
    if (s_historical_queue_length < 0) s_historical_queue_length = cv.queue_length;

    // Continuous delta derivations
    double speed_change = cv.vehicle_speed_kmh - s_historical_speed_kmh;
    double queue_growth = static_cast<double>(cv.queue_length - s_historical_queue_length);

    struct StrategyEvaluator {
        ContinuityDecision decision;
        double cost_score;
        std::string reasoning;
    };

    std::vector<StrategyEvaluator> cost_matrix;
    const double MAT_INFINITY = std::numeric_limits<double>::max();

    // =====================================================================
    // CONTINUOUS COST SCORING MATRIX (ML-READY WEIGHT MODEL)
    // =====================================================================

    // 1. RESTART COST MODEL
    // Cost scales with discarded work and prolonged degradation across handover gap duration.
    double cost_restart = (sa.useful_work_preserved_pct * 2.5) + (cv.predicted_gap_duration_s * 4.0);
    cost_matrix.push_back({ContinuityDecision::RESTART, cost_restart, "Discarding work and initializing clean state post-handover."});

    // 2. NORMAL PIPELINE COST MODEL
    // Cost increases proportionally to network decay and velocity constraints relative to RSU exit boundary.
    double proximity_risk = cv.vehicle_speed_kmh / std::max(0.1, cv.remaining_dwell_s);
    double cost_normal = (cv.packet_loss_pct * 3.5) + (proximity_risk * 1.8) + (cv.cpu_util_pct * 0.5);
    cost_matrix.push_back({ContinuityDecision::NORMAL_PIPELINE, cost_normal, "Maintaining basic sequential processing path without state shifting."});

    // 3. SPECULATIVE PREWARM COST MODEL
    // Cost drops when high acceleration signals early boundary exit, balanced by target zone gap duration.
    double mobility_urgency_factor = (speed_change * 2.0) - cv.remaining_dwell_s;
    double cost_prewarm = std::max(10.0, 150.0 - mobility_urgency_factor + (cv.predicted_gap_duration_s * 1.5));
    cost_matrix.push_back({ContinuityDecision::SPECULATIVE_PREWARM, cost_prewarm, "Proactively prepping next target node container resources."});

    // 4. MESSAGE STATE TRANSFER COST MODEL
    // Feasible for stream configurations. Cost scales with network bottleneck constraints and buffer size.
    double cost_message = (cv.task_type == TaskType::PACKET_STREAM) ? 30.0 : 200.0;
    cost_message += (sa.estimated_transfer_cost_s * 3.0) + (static_cast<double>(cv.queue_length) * 1.2);
    cost_matrix.push_back({ContinuityDecision::MESSAGE_STATE_TRANSFER, cost_message, "Migrating structured message descriptors and buffer registers."});

    // 5. POINTER STATE TRANSFER COST MODEL
    // Highly optimal when metadata index maps dominate large payloads. High payload volume penalizes other routes.
    double payload_to_bandwidth_ratio = static_cast<double>(cv.file_size_mb) / std::max(0.1, cv.bandwidth_mbps);
    double cost_pointer = (sa.minimum_useful_state_size_mb * 5.0) + payload_to_bandwidth_ratio;
    if (cv.task_type == TaskType::LARGE_FILE_TRANSFER) {
        cost_pointer -= 40.0; // Mathematically aligned cost offset for file routing mechanics
    }
    cost_matrix.push_back({ContinuityDecision::POINTER_STATE_TRANSFER, cost_pointer, "Shifting structural index maps and offset storage pointers."});

    // 6. CARRY STATE COST MODEL
    // Evaluates moving execution context mid-flight. Severely penalized if transfer time exceeds remaining dwell time.
    double cost_carry = MAT_INFINITY;
    if (cv.remaining_dwell_s > sa.estimated_transfer_cost_s) {
        double resource_headroom = cv.memory_mb - sa.dynamic_execution_state_size_mb;
        double load_weight = (cv.cpu_util_pct * 1.5) + (queue_growth * 5.0);
        
        cost_carry = (sa.estimated_transfer_cost_s * 4.0) + load_weight;
        if (resource_headroom < 0.0) {
            cost_carry += std::abs(resource_headroom) * 10.0; // Severe memory starvation penalty
        }
    }
    cost_matrix.push_back({ContinuityDecision::CARRY_STATE, cost_carry, "Serializing active execution variables for direct processing continuation."});

    // =====================================================================
    // PEER-UNAVAILABILITY OVERRIDE
    // If the peer RSU/server cannot be reached, every strategy that depends
    // on transferring something to it is infeasible regardless of cost;
    // only RESTART (fully local) and NORMAL_PIPELINE remain viable.
    // =====================================================================
    if (cv.peer_server_unavailable) {
        for (auto& s : cost_matrix) {
            if (s.decision != ContinuityDecision::RESTART &&
                s.decision != ContinuityDecision::NORMAL_PIPELINE) {
                s.cost_score = MAT_INFINITY;
            }
        }
    }

    // =====================================================================
    // STRATEGY SELECTION via ARGMIN
    // =====================================================================
    auto best_strategy = std::min_element(cost_matrix.begin(), cost_matrix.end(),
        [](const StrategyEvaluator& a, const StrategyEvaluator& b) {
            return a.cost_score < b.cost_score;
        });

    // Persist system parameters for next cyclic delta computations
    s_historical_speed_kmh = cv.vehicle_speed_kmh;
    s_historical_queue_length = cv.queue_length;

    Decision execution_decision;
    execution_decision.action = best_strategy->decision;
    execution_decision.explanation = best_strategy->reasoning;
    execution_decision.calculated_cost = best_strategy->cost_score;
    
    // Continuous calculation mapping the cost matrix optimization margin to a confidence scale
    execution_decision.confidence = std::clamp(1.0 - (best_strategy->cost_score / 350.0), 0.01, 0.99);

    return execution_decision;
}