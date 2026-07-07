/*
 * vehicle_controller.cpp - Terminal 3: live experiment control.
 *
 * Standalone REPL that sends small UDP commands to Server A's control port
 * ONLY (separate from the data port 8080, so none of the existing packet
 * pipeline is touched). Server A is always the single source of truth for
 * vehicle behaviour: this tool never talks to Server B directly. Whatever
 * Server B needs to know (the current adaptive partition) is something
 * Server A itself forwards over the existing data channel once it reacts
 * to a command here - see server_a.cpp's boundary-sync logic.
 *
 * This tool never sends application data (frames/files); it only carries
 * ControllerCommand messages that update Server A's live context (speed/
 * traffic/failure).
 */

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include "custom_payload.pb.h"

using namespace std;
using namespace chrono;

namespace {

sockaddr_in resolveTarget(const char* host_env_var, int port)
{
    const char* host = getenv(host_env_var);
    if (!host) host = "127.0.0.1";

    hostent* h = gethostbyname(host);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    addr.sin_addr   = h ? *((in_addr**)h->h_addr_list)[0] : in_addr{INADDR_LOOPBACK};
    return addr;
}

void sendCommand(int sock, const sockaddr_in& target, const edgesim::ControllerCommand& cmd)
{
    string payload;
    if (!cmd.SerializeToString(&payload)) {
        cout << "[ERROR] Failed to serialize controller command - skipping send.\n";
        return;
    }
    sendto(sock, payload.c_str(), payload.size(), 0, (sockaddr*)&target, sizeof(target));
}

edgesim::ControllerCommand makeCommand(const string& command, double value,
                                        const string& fault_name = "", bool enable = false)
{
    edgesim::ControllerCommand cmd;
    cmd.set_command(command);
    cmd.set_value(value);
    cmd.set_fault_name(fault_name);
    cmd.set_enable(enable);
    cmd.set_timestamp_ns(duration_cast<microseconds>(high_resolution_clock::now().time_since_epoch()).count());
    return cmd;
}

void printHelp()
{
    cout << "Commands:\n"
         << "  speed <kmh>                 - set vehicle speed immediately\n"
         << "  accel <target_kmh> <secs>   - ramp speed linearly to target over secs\n"
         << "  traffic <0-100>             - set background traffic/load level\n"
         << "  fail <name> <on|off>        - force a failure condition\n"
         << "                                (bandwidth_drop, packet_loss_spike, cpu_overload,\n"
         << "                                 memory_shortage, vehicle_exit_early,\n"
         << "                                 prediction_failure, server_unavailable)\n"
         << "  status                      - print last known values sent\n"
         << "  help                        - show this message\n"
         << "  quit                        - exit\n";
}

} // namespace

int main()
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { cout << "Socket didnt form\n"; return 1; }

    int controller_port = 9090;
    if (const char* p = getenv("CONTROLLER_PORT")) controller_port = atoi(p);

    // Server A only - it is the single source of truth for vehicle
    // behaviour and forwards anything Server B needs to know.
    sockaddr_in target_a = resolveTarget("SERVER_A_HOST", controller_port);

    cout << "========================================\n"
         << " Execution Continuity Vehicle Controller\n"
         << "========================================\n"
         << " Targeting Server A only on UDP " << controller_port << "\n"
         << " (set SERVER_A_HOST / CONTROLLER_PORT env vars to change)\n\n";
    printHelp();

    double      last_speed   = 0.0;
    int         last_traffic = 0;
    atomic<int> ramp_generation{0}; // invalidates any in-flight accel ramp when a new command arrives

    string line;
    cout << "\n> ";
    while (getline(cin, line)) {
        istringstream iss(line);
        string verb;
        iss >> verb;

        if (verb.empty()) { cout << "> "; continue; }

        if (verb == "quit" || verb == "exit") {
            break;
        } else if (verb == "help") {
            printHelp();
        } else if (verb == "speed") {
            double kmh;
            if (iss >> kmh) {
                ramp_generation.fetch_add(1, memory_order_relaxed);
                sendCommand(sock, target_a, makeCommand("speed", kmh));
                last_speed = kmh;
                cout << "Sent: speed -> " << kmh << " km/h\n";
            } else {
                cout << "Usage: speed <kmh>\n";
            }
        } else if (verb == "accel") {
            double target_kmh, secs;
            if (iss >> target_kmh >> secs && secs > 0.0) {
                int my_generation = ramp_generation.fetch_add(1, memory_order_relaxed) + 1;
                double start_kmh = last_speed;
                last_speed = target_kmh;
                thread([sock, target_a, start_kmh, target_kmh, secs, my_generation, &ramp_generation]() {
                    const int steps = 10;
                    for (int i = 1; i <= steps; ++i) {
                        if (ramp_generation.load(memory_order_relaxed) != my_generation) return; // superseded
                        double frac = static_cast<double>(i) / steps;
                        double kmh  = start_kmh + (target_kmh - start_kmh) * frac;
                        sendCommand(sock, target_a, makeCommand("speed", kmh));
                        this_thread::sleep_for(milliseconds(static_cast<int>((secs * 1000.0) / steps)));
                    }
                }).detach();
                cout << "Ramping speed " << start_kmh << " -> " << target_kmh
                     << " km/h over " << secs << "s\n";
            } else {
                cout << "Usage: accel <target_kmh> <secs>\n";
            }
        } else if (verb == "traffic") {
            int level;
            if (iss >> level) {
                sendCommand(sock, target_a, makeCommand("traffic", level));
                last_traffic = level;
                cout << "Sent: traffic -> " << level << "\n";
            } else {
                cout << "Usage: traffic <0-100>\n";
            }
        } else if (verb == "fail") {
            string name, state;
            if (iss >> name >> state && (state == "on" || state == "off")) {
                sendCommand(sock, target_a, makeCommand("fail", 0.0, name, state == "on"));
                cout << "Sent: fail " << name << " " << state << "\n";
            } else {
                cout << "Usage: fail <name> <on|off>\n";
            }
        } else if (verb == "status") {
            cout << "Last speed sent   : " << last_speed << " km/h\n"
                 << "Last traffic sent : " << last_traffic << "\n";
        } else {
            cout << "Unknown command '" << verb << "'. Type 'help' for a list.\n";
        }

        cout << "> ";
    }

    close(sock);
    cout << "\nVehicle Controller exiting.\n";
    return 0;
}