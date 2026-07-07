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
COPY scenario.conf .
COPY context_vector.h .
COPY context_monitor.h .
COPY scenario_config.h .
COPY scenario_config.cpp .
COPY state_analyzer.h .
COPY state_analyzer.cpp .
COPY decision_engine.h .
COPY decision_engine.cpp .
COPY csv_logger.h .
COPY csv_logger.cpp .
COPY traffic_load.h .
COPY traffic_load.cpp .
RUN protoc --cpp_out=. custom_payload.proto

# MAKE SURE THIS COMPILES AND RUNS SERVER_B
RUN g++ -std=c++17 server_b.cpp custom_payload.pb.cc \
    scenario_config.cpp state_analyzer.cpp decision_engine.cpp csv_logger.cpp traffic_load.cpp \
    -o server_b $(pkg-config --cflags --libs protobuf)

# UDP 8080 = application data path only. Server B never binds the
# controller port (9090) - Server A is the sole owner; see scenario.conf.
EXPOSE 8080/udp

CMD ["./server_b"]