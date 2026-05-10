VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)

.PHONY: \
	emulator/build \
	docker/build \
	docker/up \
	docker/down \
	docker/logs \
	docker/healthcheck \
	e2e

# The WASM-based analyzer (zetasql-wasm) replaced the previous
# CGO-based go-zetasql, so the build no longer needs the C++
# static-link toolchain. The SQLite driver (mattn/go-sqlite3) is
# still CGO-bound, so plain `go build` with the host toolchain is
# what runs here.
emulator/build:
	go build -o bigquery-emulator \
		-ldflags='-s -w -X main.revision=${REVISION}' \
		./cmd/bigquery-emulator

# Build the container image. Tags as `bigquery-emulator:${VERSION}`
# (default "latest"). REVISION flows in as a build-arg so
# `bigquery-emulator --version` inside the image carries the SHA.
docker/build:
	docker build -t bigquery-emulator:${VERSION} \
		--build-arg REVISION=${REVISION} .

# Bring the emulator up via the e2e compose definition. Always
# rebuilds the image first (`--build`) so an `up` after a code or
# Dockerfile change picks up the latest binary instead of an old
# cached image.
docker/up:
	docker compose -f e2e/compose.yml up -d --build

docker/down:
	docker compose -f e2e/compose.yml down --volumes

docker/logs:
	docker compose -f e2e/compose.yml logs -f bigquery-emulator

# Polls the emulator's HTTP port from the host. distroless/base has
# no shell, so a compose-level `CMD-SHELL` healthcheck cannot run
# inside the container. This target provides the equivalent probe
# from outside, with a short retry window so it tolerates a
# just-started container.
#
# `curl -s` (no `-f`) on purpose: bigquery-emulator returns HTTP 500
# for `GET /` ("unexpected request path") since `/` is not a real
# endpoint. We only need to confirm the port is reachable, so any
# HTTP response — including 500 — counts as healthy. curl exits
# non-zero only on connection-level failures (refused, timeout).
docker/healthcheck:
	@for i in 1 2 3 4 5; do \
		if curl -s -m 2 -o /dev/null http://localhost:9050/; then \
			echo "OK: emulator responding on :9050"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "FAIL: emulator not reachable on :9050" >&2; \
	echo "       (run \`make docker/up\` first)" >&2; \
	exit 1

# End-to-end smoke: assumes the emulator container is already up
# (use `make docker/up` to start it). The docker/healthcheck
# dependency gates the test on a reachable :9050 with a clear
# error if the user forgot to bring it up. Runs every
# testdata/query/*.sql through the BigQuery REST query endpoint
# and diffs against .golden.tsv. Independent of `go test ./...`
# (which does not see e2e without the build tag).
e2e: docker/healthcheck
	go test -tags=e2e -count=1 -v ./e2e/...
