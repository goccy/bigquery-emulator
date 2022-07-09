FROM golang:1.18.3-bullseye

ARG VERSION

WORKDIR /work

RUN apt-get update && apt-get install -y --no-install-recommends clang

COPY ./go.* ./
RUN go mod download

COPY . ./

RUN make emulator/build

FROM debian:bullseye AS emulator

COPY --from=0 /work/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

CMD ["/bin/bigquery-emulator"]
