VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

# The SQL backend is pure Go, so the emulator builds without cgo and links a
# fully static binary on every platform.
emulator/build:
	CGO_ENABLED=0 go build -o bigquery-emulator \
		-ldflags='-s -w -X main.version=${VERSION} -X main.revision=${REVISION}' \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . --build-arg VERSION=${VERSION}

# Run the multi-language client conformance suite (Python/Ruby/PHP/Node.js/bq/Java).
# Requires a running Docker daemon; skips automatically when none is found.
test/e2e:
	go test -count=1 -timeout 30m -v ./test/e2e/...
