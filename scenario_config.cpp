#include "scenario_config.h"
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>

namespace {

std::string trim(const std::string& s)
{
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

} // namespace

ScenarioConfig ScenarioConfig::load(const std::string& path)
{
    ScenarioConfig cfg;
    std::ifstream file(path);
    if (!file.is_open()) {
        // Missing config is not an error: every getter has a default that
        // reproduces the framework's original hardcoded behaviour.
        return cfg;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#' || trimmed[0] == ';') continue;

        size_t eq = trimmed.find('=');
        if (eq == std::string::npos) continue;

        std::string key   = trim(trimmed.substr(0, eq));
        std::string value = trim(trimmed.substr(eq + 1));
        if (!key.empty()) cfg.values_[key] = value;
    }
    return cfg;
}

double ScenarioConfig::getDouble(const std::string& key, double def) const
{
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    try { return std::stod(it->second); } catch (...) { return def; }
}

int ScenarioConfig::getInt(const std::string& key, int def) const
{
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    try { return std::stoi(it->second); } catch (...) { return def; }
}

bool ScenarioConfig::getBool(const std::string& key, bool def) const
{
    auto it = values_.find(key);
    if (it == values_.end()) return def;
    std::string v = it->second;
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return std::tolower(c); });
    if (v == "1" || v == "true" || v == "yes" || v == "on")  return true;
    if (v == "0" || v == "false" || v == "no" || v == "off") return false;
    return def;
}

std::string ScenarioConfig::getString(const std::string& key, const std::string& def) const
{
    auto it = values_.find(key);
    return (it == values_.end()) ? def : it->second;
}
