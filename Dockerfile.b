FROM gcc:latest
WORKDIR /app
COPY server_b.cpp .
RUN g++ -std=c++11 server_b.cpp -o server_b
CMD ["./server_b"]