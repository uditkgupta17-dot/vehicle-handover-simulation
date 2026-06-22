#include <iostream>
#include <map>
#include <netinet/in.h>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include "custom_payload.pb.h"

using namespace std;

int main(){

    int socketid = socket(AF_INET , SOCK_DGRAM ,0);
    if(socketid < 0){
        cout<<"Socket didnt form"<<endl;
        return 0;
    }

    sockaddr_in saddr;
    memset(&saddr , 0 , sizeof(saddr));
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = INADDR_ANY ;
    saddr.sin_port = htons(8080);

    if(::bind(socketid,(struct sockaddr*)&saddr , sizeof(saddr)) < 0){
        cout<<"Binding didnt happen"<<endl;
        return 0;
    }

    cout<<"Server B is working"<<endl;
    cout << "--- SERVER B (DEFINITIVE V9 FIXED VERSION) ---" << endl;
    cout<<"Received packets" << endl;

    map<int, string> queue;

    sockaddr_in veh;
    socklen_t addlen = sizeof(veh);

    char buffer[1024];
    int pac_process = 0;
    int boundary_index = -1;

    while(pac_process < 3){
        int bytes_received = recvfrom(socketid, buffer, sizeof(buffer), 0, (struct sockaddr*)&veh, &addlen);

        if (bytes_received < 0) continue;
        edgesim::VehicleMetadata in_pac;
        if (!in_pac.ParseFromArray(buffer, bytes_received)) {
            std::cerr << "Warning: Failed to parse incoming packet!" << std::endl;
        }   

        cout << "\n Network Overhead: Received Packet. Size = " << bytes_received << " bytes." << endl;
        cout << "Got Frame ID: " << in_pac.frame_id() << " ,Payload: " << in_pac.sensor_path() << endl;

        queue[in_pac.frame_id()] = in_pac.sensor_path();
        boundary_index = in_pac.handover_status();

        auto recv_time = chrono::high_resolution_clock::now();
        long long current_micros = chrono::duration_cast<chrono::microseconds>(recv_time.time_since_epoch()).count();

        long long latency = current_micros - in_pac.timestamp_ns();
        
        cout << "Handover Latency for Frame " << in_pac.frame_id() << ": " << latency << " microseconds (" << (double)latency/1000.0 << " ms)" << endl;

        pac_process++;
    }

    cout << "\nVehicle Crossed Boundary Zone"<<endl;
    cout << "Connection switched to Server B , process resumes from index " << boundary_index << endl;

    for (int i = boundary_index; i <= boundary_index + 2; i++){
        if (queue.find(i) != queue.end()) {
            cout << "Processing Frame " << i << " Data: " << queue[i] << endl;
        }
    }
    
    close(socketid);
    
    cout << "\n[SERVER B] Handover processing complete. Holding pod active for logs..." << endl;
    while(true) {
        sleep(10);
    }

    return 0;
}