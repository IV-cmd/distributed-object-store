FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ cmake ninja-build libgrpc++-dev libprotobuf-dev \
    protobuf-compiler protobuf-compiler-grpc libgtest-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY CMakeLists.txt ./
COPY proto proto
COPY src src
COPY tests tests
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel 2 \
    && ctest --test-dir build --output-on-failure

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51t64 libprotobuf32t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /work/build/src/mgm/mgm /usr/local/bin/mgm
COPY --from=build /work/build/src/fst/fst /usr/local/bin/fst
COPY --from=build /work/build/src/client/client /usr/local/bin/client
USER 10001:10001
CMD ["mgm", "0.0.0.0:50051"]
