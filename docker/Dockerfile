FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ cmake ninja-build libgrpc++-dev libprotobuf-dev \
    protobuf-compiler protobuf-compiler-grpc libgtest-dev libxxhash-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY CMakeLists.txt ./
COPY proto proto
COPY src src
COPY tests tests
RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --parallel 2 \
    && ctest --test-dir build --output-on-failure

FROM ubuntu:24.04 AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51t64 libprotobuf32t64 libxxhash0 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /work/build/src/mgm/mgm /usr/local/bin/mgm
COPY --from=build /work/build/src/fst/fst /usr/local/bin/fst
COPY --from=build /work/build/src/client/client /usr/local/bin/client
RUN mkdir -p /var/lib/objectstore && chown 10001:10001 /var/lib/objectstore
USER 10001:10001
CMD ["fst", "0.0.0.0:50052", "/var/lib/objectstore"]

FROM runtime-base AS fault-tools
USER 0:0
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*
USER 10001:10001

FROM runtime-base AS runtime
