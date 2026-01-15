PKG := github.com/goccy/go-zetasqlite

GOBIN := $(CURDIR)/bin
PKGS := $(shell go list ./... | grep -v cmd | grep -v benchmarks )
COVER_PKGS := $(foreach pkg,$(PKGS),$(subst $(PKG),.,$(pkg)))

COMMA := ,
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
COVERPKG_OPT := $(subst $(SPACE),$(COMMA),$(COVER_PKGS))

$(GOBIN):
	@mkdir -p $(GOBIN)

.PHONY: build
build:
	cd ./cmd/zetasqlite-cli && go build .

.PHONY: cover
cover:
	go test -coverpkg=$(COVERPKG_OPT) -coverprofile=cover.out ./...

.PHONY: cover-html
cover-html: cover
	go tool cover -html=cover.out

.PHONY: lint
lint: lint/install
	$(GOBIN)/golangci-lint run --timeout 30m

lint/fix: lint/install
	$(GOBIN)/golangci-lint run --fix --timeout 30m

lint/install: | $(GOBIN)
	GOBIN=$(GOBIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.56.2
