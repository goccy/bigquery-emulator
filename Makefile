emulator/build:
	CGO_ENABLED=1 CXX=clang++ go build -o bigquery-emulator -ldflags='-extldflags "-static"' ./cmd/bigquery-emulator

docker/build:
	docker build -t bigquery-emulator .
