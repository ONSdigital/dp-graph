SHELL=bash

.PHONY: all
all: audit test build

.PHONY: audit
audit:
	set -o pipefail; go list -json -m all | nancy sleuth --exclude-vulnerability-file ./.nancy-ignore

.PHONY: test
test:
	go test -race -cover ./...

.PHONY: build
build:
	go build ./...

lint:
	exit
.PHONY: lint
