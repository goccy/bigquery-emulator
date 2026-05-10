# Builds bigquery-emulator with CGO enabled. zetasql-wasm itself is
# pure Go (wazero), but the SQLite driver (mattn/go-sqlite3) the fork
# uses is CGO-bound, so the binary needs gcc + glibc to link. The
# golang:bookworm builder ships gcc; the final image is
# distroless/base-debian12 (carries glibc + ca-certs + tzdata).
ARG GO_VERSION=1.26
ARG DEBIAN_VERSION=bookworm

FROM golang:${GO_VERSION}-${DEBIAN_VERSION} AS builder

WORKDIR /build
# Copy the dep manifests first to leverage the Docker layer cache for
# `go mod download`.
COPY go.mod go.sum ./
RUN go mod download

COPY . ./

ARG REVISION

# CGO_ENABLED=1 because the SQLite driver requires it. With buildx +
# QEMU each platform builds for itself, so we do not set GOOS/GOARCH
# explicitly — they default to the running platform.
RUN CGO_ENABLED=1 \
    go build \
    -trimpath \
    -ldflags "-s -w -X main.revision=${REVISION}" \
    -o /go/bin/bigquery-emulator \
    ./cmd/bigquery-emulator

# distroless/base-debian12 carries glibc, ca-certificates, and tzdata
# — what the CGO-linked binary needs at runtime, nothing more.
FROM gcr.io/distroless/base-debian12

COPY --from=builder /go/bin/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

ENTRYPOINT ["/bin/bigquery-emulator"]
