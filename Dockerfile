ARG DEBIAN_VERSION=bookworm

FROM --platform=$BUILDPLATFORM golang:${DEBIAN_VERSION} AS builder

# Provided by buildx for every target platform.
ARG TARGETOS
ARG TARGETARCH

WORKDIR /build
# Copy the dependency manifests first to leverage the Docker layer cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

ARG VERSION
ARG REVISION

# The SQL backend is pure Go; build a fully static binary without cgo.
# The builder stage runs on the native build platform and cross-compiles
# for the requested target, so multi-arch image builds need no QEMU.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /go/bin/bigquery-emulator \
    -ldflags "-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
    ./cmd/bigquery-emulator

FROM debian:${DEBIAN_VERSION}

COPY --from=builder /go/bin/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

# Run the embedded wasm SQL engine under the wazero compiler instead of
# the interpreter default. For the emulator's workload (a long-lived
# server answering many queries) the compiler is ~2x faster per query
# and uses far less RSS; the only cost is a one-time AOT compile on the
# first query. GOOGLESQLITE_WASM_CACHE_DIR amortises that compile across
# container restarts. The cache cannot be pre-warmed at image-build time
# because wazero's AOT output is architecture-specific and this image is
# cross-compiled for multiple target arches.
ENV GOOGLESQLITE_WASM_COMPILATION_MODE=compiler
ENV GOOGLESQLITE_WASM_CACHE_DIR=/tmp/googlesqlite-wasm-cache

ENTRYPOINT ["/bin/bigquery-emulator"]
