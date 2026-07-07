#ifndef STATE_ANALYZER_H
#define STATE_ANALYZER_H

#include <string>
#include "context_vector.h" // Provides ContextVector and TaskType

struct StateAnalysis
{
    double minimum_useful_state_size_mb;
    double dynamic_execution_state_size_mb;
    double estimated_transfer_cost_s;
    double useful_work_preserved_pct;
    std::string execution_state_description;
};

class StateAnalyzer
{
public:
    // Pure analytical observer utilizing the full multi-dimensional ContextVector
    static StateAnalysis analyze(const ContextVector& cv);
};

#endif