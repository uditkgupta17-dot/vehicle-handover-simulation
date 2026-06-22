FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    g++ \
    libprotobuf-dev \
    protobuf-compiler \
    pkg-config \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# MAKE SURE THESE SAY SERVER_B
COPY server_b.cpp .  
COPY custom_payload.proto .
RUN protoc --cpp_out=. custom_payload.proto

# MAKE SURE THIS COMPILES AND RUNS SERVER_B
RUN g++ -std=c++17 server_b.cpp custom_payload.pb.cc -o server_b $(pkg-config --cflags --libs protobuf)

CMD ["./server_b"]