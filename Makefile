VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)
UNAME_OS := $(shell uname -s)
DOCKER_PLATFORM ?= linux/amd64
BIN_PATH ?= ./bin

ifneq ($(UNAME_OS),Darwin)
	STATIC_LINK_FLAG := -linkmode external -extldflags "-static"
endif

build:
	CGO_ENABLED=1 CXX=clang++ go build \
		-o $(BIN_PATH)/bigquery-emulator \
		-ldflags='-s -w -X main.version=${VERSION} -X main.revision=${REVISION} ${STATIC_LINK_FLAG}' \
		./cmd/bigquery-emulator

docker-build:
	docker \
		build \
		-t bigquery-emulator \
		--build-arg VERSION=$(VERSION) \
		--platform=$(DOCKER_PLATFORM) \
		.
