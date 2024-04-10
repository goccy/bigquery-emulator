FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM ghcr.io/goccy/go-zetasql:latest

RUN apt-get update && apt-get upgrade -y

COPY --from=xx / /

ARG TARGETPLATFORM

# There's a slightly weird combo of stuff that needs to be installed for the $BUILDPLATFORM
# and for the cross-toolchain, at least for this CGO-enabled target.
RUN apt-get install -y gcc-multilib g++-multilib
RUN xx-apt install -y musl-dev gcc libstdc++-12-dev

ARG VERSION

WORKDIR /work

COPY . ./

RUN go mod edit -replace github.com/goccy/go-zetasql=../go-zetasql
RUN go mod download

# Replace `RUN make emulator/build` with a bog-standard xx-go invocation, so it manages
# compiler, linker, options, everything.
ENV CGO_ENABLED=1
RUN xx-go build -o bigquery-emulator \
  ./cmd/bigquery-emulator && xx-verify bigquery-emulator

FROM debian:bullseye AS emulator

COPY --from=1 /work/bigquery-emulator /bin/bigquery-emulator

WORKDIR /work

ENTRYPOINT ["/bin/bigquery-emulator"]
