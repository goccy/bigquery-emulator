VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

# The WASM-based analyzer (zetasql-wasm) replaced the previous CGO-based
# go-zetasql, so the build is pure-Go now and the Linux static-link / clang
# toolchain previously required for the C++ ZetaSQL bridge is no longer
# needed.
emulator/build:
	go build -o bigquery-emulator \
		-ldflags='-s -w -X main.version=${VERSION} -X main.revision=${REVISION}' \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . --build-arg VERSION=${VERSION}
