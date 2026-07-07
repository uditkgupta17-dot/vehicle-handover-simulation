#ifndef CSV_LOGGER_H
#define CSV_LOGGER_H

#include <fstream>
#include <mutex>
#include <string>
#include "context_vector.h"
#include "decision_engine.h"
#include "state_analyzer.h"

// Append-only CSV writer for one row per continuity-decision tick.
// Thread-safe; a single instance may be shared across all of a server's
// threads (the decision tick, the receiver, etc.).
class CsvLogger
{
public:
    explicit CsvLogger(const std::string& path);

    void logRow(const std::string&    server_id,
                const ContextVector&  cv,
                const StateAnalysis&  sa,
                const Decision&       decision,
                double                recomputation_overhead_ms,
                double                downtime_ms,
                bool                  continuity_success);

private:
    std::mutex    mtx_;
    std::ofstream out_;

    static std::string decisionToString(ContinuityDecision d);
};

#endif
