FROM golang:1.20.1-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends clang