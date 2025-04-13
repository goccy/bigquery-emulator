ARG DEBIAN_VERSION=bookworm

FROM golang:${DEBIAN_VERSION} AS cgo_builder
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install --no-install-recommends clang

WORKDIR /build
# We copy the depenencies first to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY server ./server
COPY types ./types

ENV CGO_ENABLED=1
ENV CC=clang
ENV CGO_CFLAGS="-fPIC"
ENV CXX=clang++
ENV CGO_CPPFLAGS="-fPIC"
ENV CGO_CXXFLAGS="-fPIC"
ARG TARGETPLATFORM
ARG VERSION
ARG REVISION

RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
        export STATIC_LINK_FLAGS="-extldflags -static"; \
    fi \
    && go build -o /go/bin/bigquery-emulator \
    -ldflags "-s -w -X main.version=${VERSION} -X main.revision=${REVISION} -linkmode=external $STATIC_LINK_FLAGS" \
    ./cmd/bigquery-emulator


# AMD64 would be fine using scratch, as long as we also create an empty
# /tmp directory and copy over /usr/share/zoneinfo. However, static linking
# fails with ARM64, so we need to use a base image with the same glibc.
FROM debian:${DEBIAN_VERSION}

COPY --from=cgo_builder /go/bin/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

ENTRYPOINT ["/bin/bigquery-emulator"]
