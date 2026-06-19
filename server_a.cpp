#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>

using namespace std;

struct frame {
    int frameid;
    int handover;
    char sensorp[32];
    long long time;
};

int main() {
    int socketid = socket(AF_INET , SOCK_DGRAM ,0);
    if(socketid < 0){
        cout<<"Socket didnt form"<<endl;
        return 0;
    }

    sockaddr_in targetid;
    memset(&targetid, 0, sizeof(targetid));
    targetid.sin_family = AF_INET;
    targetid.sin_port = htons(8080);
    inet_pton(AF_INET, "127.0.0.1", &targetid.sin_addr);

    cout << " [SERVER A] Handover initiated \n";
    cout << "Processing Frames 0, 1, 2 locally.\n";
    cout << "Pipelining remainder to Server B in REVERSE order.\n\n";

    int totalpac = 5;
    int order_ids[5] = {1,2,3,4,5};
    const char* payloads[] = {"Task A", "Task B", "Task C"};
    
    int boundary = 3;

    for (int i = totalpac-1 ; i >= 2; i--) {
        frame packet;
        packet.frameid = order_ids[i];
        packet.handover = boundary; 
        strcpy(packet.sensorp, payloads[i-2]);

        auto now = chrono::high_resolution_clock::now();
        
        packet.time = chrono::duration_cast<chrono::microseconds>(now.time_since_epoch()).count();

        cout << "Sending Frame " << packet.frameid << " with timestamp: " << packet.time << endl;
        
        sendto(socketid, &packet, sizeof(packet), 0,(struct sockaddr*)&targetid, sizeof(targetid));

        sleep(1);
    }

    cout << "Reverse Pre-streaming completed. Connection closed.\n";
    
    close(socketid);
    return 0;
}