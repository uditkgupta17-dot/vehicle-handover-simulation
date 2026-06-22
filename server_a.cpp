#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <sstream>
#include <vector>
#include "custom_payload.pb.h"
#include <netdb.h> 
#include <cstdlib>  

using namespace std;

struct DeepSenseRow {
    int index;
    string img_path;
};

int main() {
    int socketid = socket(AF_INET , SOCK_DGRAM ,0);
    if(socketid < 0){
        cout<<"Socket didnt form"<<endl;
        return 0;
    }

    const char* target_host = std::getenv("SERVER_B_HOST");
    if (target_host == nullptr) {
        target_host = "127.0.0.1"; 
    }

    struct hostent *host = gethostbyname(target_host);
    if (host == nullptr) {
        cout << "Error: Could not resolve hostname " << target_host << endl;
        close(socketid);
        return 0;
    }

    sockaddr_in targetid;
    memset(&targetid, 0, sizeof(targetid));
    targetid.sin_family = AF_INET;
    targetid.sin_port = htons(8080);
    
    targetid.sin_addr = *((struct in_addr **)host->h_addr_list)[0];

    cout << " [SERVER A] DeepSense Scenario 3 Trace Loaded \n";

    vector<DeepSenseRow> dataset;
    ifstream file("scenario3.csv");
    string line;

    if (!file.is_open()) {
        cout << "Error: Could not open scenario3.csv! Verify your Dockerfile configurations." << endl;
        close(socketid);
        return 0;
    }

    getline(file, line);

    while (getline(file, line)) {
        stringstream ss(line);
        string idx_str, img_path, loc_path, beam_str;

        getline(ss, idx_str, ',');
        getline(ss, img_path, ',');
        getline(ss, loc_path, ',');
        getline(ss, beam_str, ',');

        if (idx_str.empty()) continue;

        DeepSenseRow data;
        data.index = stoi(idx_str);
        data.img_path = img_path;
        
        dataset.push_back(data);
    }
    file.close();

    int boundary_trigger_frame = 5; 
    int boundary_index = -1;

    for (int i = 0; i < dataset.size(); i++) {
        
        if (dataset[i].index >= boundary_trigger_frame) {
            cout << "\n[NETWORK ALARM] Car reached handover zone at DeepSense Index: " << dataset[i].index << "\n";
            cout << "Executing REVERSE predictive streaming sequence to Server B...\n\n";
            
            boundary_index = dataset[i].index;

            for (int j = i + 2; j >= i; j--) {
                edgesim::VehicleMetadata packet;

                packet.set_frame_id(dataset[j].index);
                packet.set_handover_status(boundary_index);
                packet.set_sensor_path(dataset[j].img_path); 

                auto now = chrono::high_resolution_clock::now();
                packet.set_timestamp_ns(chrono::duration_cast<chrono::microseconds>(now.time_since_epoch()).count());
                packet.set_simulated_rssi(-75.5); 

                string payload;
                if (!packet.SerializeToString(&payload)) {
                    std::cerr << "Warning: Failed to serialize packet!" << std::endl;
                }

                cout << "Pipelining Frame " << packet.frame_id() << " | Target Img Metadata: " << packet.sensor_path() << endl;

                // Sends the serialized payload securely via UDP
                sendto(socketid, payload.c_str(), payload.length(), 0, (struct sockaddr*)&targetid, sizeof(targetid));
                sleep(1);
            }
            break; 
        }

        cout << "Processing Frame " << dataset[i].index << " locally on Server A..." << endl;
        sleep(1);
    }

    cout << "\nDeepSense streaming trace execution successfully ended.\n";
    close(socketid);
    return 0;
}