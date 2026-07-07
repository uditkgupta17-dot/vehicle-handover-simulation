#include "csv_logger.h"
#include <chrono>

CsvLogger::CsvLogger(const std::string& path)
{
    bool write_header = true;
    {
        std::ifstream probe(path);
        write_header = !probe.good() || probe.peek() == std::ifstream::traits_type::eof();
    }

    out_.open(path, std::ios::app);

    if (write_header && out_.is_open()) {
        out_ << "timestamp_ns,server_id,vehicle_speed_kmh,bandwidth_mbps,packet_loss_pct,"
                "avg_latency_ms,cpu_util_pct,memory_mb,queue_length,task_progress_pct,"
                "decision,minimum_useful_state_mb,transfer_cost_s,useful_work_preserved_pct,"
                "recomputation_overhead_ms,downtime_ms,continuity_success,confidence\n";
        out_.flush();
    }
}

std::string CsvLogger::decisionToString(ContinuityDecision d)
{
    switch (d) {
        case ContinuityDecision::NORMAL_PIPELINE:        return "NORMAL_PIPELINE";
        case ContinuityDecision::SPECULATIVE_PREWARM:    return "SPECULATIVE_PREWARM";
        case ContinuityDecision::MESSAGE_STATE_TRANSFER: return "MESSAGE_STATE_TRANSFER";
        case ContinuityDecision::POINTER_STATE_TRANSFER: return "POINTER_STATE_TRANSFER";
        case ContinuityDecision::CARRY_STATE:            return "CARRY_STATE";
        case ContinuityDecision::RESTART:                return "RESTART";
    }
    return "UNKNOWN";
}

void CsvLogger::logRow(const std::string&   server_id,
                        const ContextVector& cv,
                        const StateAnalysis& sa,
                        const Decision&      decision,
                        double               recomputation_overhead_ms,
                        double               downtime_ms,
                        bool                 continuity_success)
{
    std::lock_guard<std::mutex> lg(mtx_);
    if (!out_.is_open()) return;

    long long ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

    out_ << ts_ns << ','
         << server_id << ','
         << cv.vehicle_speed_kmh << ','
         << cv.bandwidth_mbps << ','
         << cv.packet_loss_pct << ','
         << cv.avg_latency_ms << ','
         << cv.cpu_util_pct << ','
         << cv.memory_mb << ','
         << cv.queue_length << ','
         << cv.task_progress_pct << ','
         << decisionToString(decision.action) << ','
         << sa.minimum_useful_state_size_mb << ','
         << sa.estimated_transfer_cost_s << ','
         << sa.useful_work_preserved_pct << ','
         << recomputation_overhead_ms << ','
         << downtime_ms << ','
         << (continuity_success ? 1 : 0) << ','
         << decision.confidence << '\n';
    out_.flush();
}
