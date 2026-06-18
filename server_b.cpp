#include<iostream>
#include<vector>
#include<netinet/in.h>
#include<cstring>
#include<sys/socket.h>
#include<unistd.h>

using namespace std;

struct frame{
    int frameid;
    int handover;
    char sensorp[32];
};

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
    cout<<"Received packets";

    vector<string> queue(6);

    //network will dump the sender's IP address here to this variable 
    sockaddr_in veh;

    // i am using IPv4 here so 
    socklen_t addlen = sizeof(veh);

    frame in_pac;

    int pac_process =0;
    int boundary_index = -1;

    while(pac_process < 3){
        recvfrom(socketid , &in_pac ,sizeof(in_pac) ,0, (struct sockaddr*)&veh, &addlen);

        cout << "[Network] Got Frame ID: " << in_pac.frameid << " | Payload: " << in_pac.sensorp << endl;

        queue[in_pac.frameid] = string(in_pac.sensorp);
        boundary_index = in_pac.handover;

        pac_process++;
    }

    cout << "Vehicle Crossed Boundary Zone"<<endl;
    cout << "Connection switched to Server B , process resumes from index " << boundary_index << endl;

    for (int i = boundary_index; i <= 5; i++){
        if (!queue[i].empty()) {
            cout << "Processing Frame " <<i<< "Data: " <<queue[i] <<endl;
        }
    }
    close(socketid);

    return 0;

}