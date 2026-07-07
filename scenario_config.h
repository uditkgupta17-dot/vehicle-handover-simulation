#ifndef SCENARIO_CONFIG_H
#define SCENARIO_CONFIG_H

#include <string>
#include <unordered_map>

// Loads flat "key=value" scenario parameters (RSU geometry, simulated
// network/compute ranges, failure injection) from a text file so
// experiments can vary these without recompiling. Missing file or missing
// keys silently fall back to the default passed to each getter - callers
// never need to null-check.
class ScenarioConfig
{
public:
    static ScenarioConfig load(const std::string& path);

    double      getDouble(const std::string& key, double def) const;
    int         getInt(const std::string& key, int def) const;
    bool        getBool(const std::string& key, bool def) const;
    std::string getString(const std::string& key, const std::string& def) const;

private:
    std::unordered_map<std::string, std::string> values_;
};

#endif
