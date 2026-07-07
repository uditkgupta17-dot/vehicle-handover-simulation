#include "state_analyzer.h"
#include <algorithm>
#include <cmath>

StateAnalysis StateAnalyzer::analyze(const ContextVector& cv)
{
    StateAnalysis analysis;
    
    // 1. Direct workload preservation metric
    analysis.useful_work_preserved_pct = cv.task_progress_pct;

    // 2. Compute dynamic execution state footprint based on resource allocations
    double dynamic_footprint = cv.baseline_execution_state_size_mb;
    
    switch (cv.task_type)
    {
        case TaskType::PACKET_STREAM:
            // State size scales continuously with in-flight buffered allocations
            dynamic_footprint += (static_cast<double>(cv.queue_length) * 0.02); 
            analysis.execution_state_description = "Streaming pipeline buffer state descriptors";
            break;

        case TaskType::LONG_RUNNING_EXECUTION:
            // State size is a function of memory utilization scaling with CPU footprint
            dynamic_footprint += (cv.memory_mb * (cv.cpu_util_pct / 100.0) * 0.15);
            analysis.execution_state_description = "Active compute registers and stack execution state";
            break;

        case TaskType::LARGE_FILE_TRANSFER:
            // State is proportional to the remaining file chunks to transfer
            double remaining_fraction = 1.0 - (cv.task_progress_pct / 100.0);
            dynamic_footprint += (static_cast<double>(cv.file_size_mb) * remaining_fraction * 0.05);
            analysis.execution_state_description = "File chunk indexing maps and sequence tables";
            break;
    }
    analysis.dynamic_execution_state_size_mb = dynamic_footprint;

    // 3. Mathematical estimation of the Minimum Useful Execution State
    // Under high network degradation (packet loss, latency) or impending handover,
    // compute the stripped structural essence required for task continuity.
    double degradation_factor = (cv.packet_loss_pct / 100.0) + (cv.avg_latency_ms / 100.0);
    double compression_ratio = std::clamp(1.0 - degradation_factor, 0.05, 1.0);
    
    analysis.minimum_useful_state_size_mb = dynamic_footprint * compression_ratio;

    // 4. Rate-based transfer cost calculation (Time = Volume / Bandwidth)
    double effective_bandwidth = std::max(0.01, cv.bandwidth_mbps);
    analysis.estimated_transfer_cost_s = (analysis.dynamic_execution_state_size_mb * 8.0) / effective_bandwidth;

    return analysis;
}