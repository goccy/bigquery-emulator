ARG DEBIAN_VERSION=bookworm

FROM golang:${DEBIAN_VERSION} AS builder

WORKDIR /build
# Copy the dependency manifests first to leverage the Docker layer cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

ARG VERSION
ARG REVISION

# The SQL backend is pure Go; build a fully static binary without cgo.
RUN CGO_ENABLED=0 go build -o /go/bin/bigquery-emulator \
    -ldflags "-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
    ./cmd/bigquery-emulator

FROM debian:${DEBIAN_VERSION}

COPY --from=builder /go/bin/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

ENTRYPOINT ["/bin/bigquery-emulator"]
