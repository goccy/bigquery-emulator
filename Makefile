VERSION ?= latest
REVISION := $(shell git rev-parse --short HEAD)
UNAME_OS := $(shell uname -s)
ifneq ($(UNAME_OS),Darwin)
	STATIC_LINK_FLAG := -linkmode external -extldflags "-static"
endif

emulator/build-debug:
	CGO_ENABLED=1 CXX=clang++ go build -o bigquery-emulator \
		-gcflags "all=-N -l" \
		-ldflags='-X main.version=${VERSION} -X main.revision=${REVISION} ${STATIC_LINK_FLAG}' \
		./cmd/bigquery-emulator


emulator/build:
	CGO_ENABLED=1 CXX=clang++ go build -o bigquery-emulator \
		-ldflags='-s -w -X main.version=${VERSION} -X main.revision=${REVISION} ${STATIC_LINK_FLAG}' \
		./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator . --build-arg VERSION=${VERSION}
