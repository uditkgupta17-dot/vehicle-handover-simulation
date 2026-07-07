#ifndef DECISION_ENGINE_H
#define DECISION_ENGINE_H

#include <string>
#include "context_vector.h"
#include "state_analyzer.h"

enum class ContinuityDecision
{
    NORMAL_PIPELINE,
    SPECULATIVE_PREWARM,
    MESSAGE_STATE_TRANSFER,
    POINTER_STATE_TRANSFER,
    CARRY_STATE,
    RESTART
};

struct Decision
{
    ContinuityDecision action;
    std::string explanation;
    double confidence;
    double calculated_cost;
};

class DecisionEngine
{
public:
    // Orchestrator that evaluates mathematical cost metrics for every continuity vector
    static Decision evaluate(const ContextVector& cv, const StateAnalysis& sa);
};

#endif