#ifndef VEHICULAR_EDGE_CONTEXT_H
#define VEHICULAR_EDGE_CONTEXT_H

#include <string>

// Execution-oriented workload classification
enum class TaskType
{
    PACKET_STREAM,
    LONG_RUNNING_EXECUTION,
    LARGE_FILE_TRANSFER
};

// Complete multi-dimensional context vector reflecting system state
struct ContextVector
{
    // --- Mobility Context ---
    double vehicle_speed_kmh;       
    std::string current_rsu_id;     
    double dist_to_exit_m;          
    double dist_to_next_rsu_m;      
    double remaining_dwell_s;       
    double predicted_gap_duration_s; 

    // --- Network Context ---
    double bandwidth_mbps;
    double avg_latency_ms;
    double packet_loss_pct;
    double rssi_dbm;
    bool   peer_server_unavailable = false; // set by failure injection / health checks

    // --- Compute Context ---
    double cpu_util_pct;            
    double memory_mb;               
    int    queue_length;            

    // --- Task / Workload Context ---
    double task_progress_pct;       
    double baseline_execution_state_size_mb; // Renamed from checkpoint_size_mb
    TaskType task_type;             
    int file_size_mb;               
};

#endif