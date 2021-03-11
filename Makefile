.PHONY: all
all: audit test build

.PHONY: audit
audit:
	go list -m all | nancy sleuth

.PHONY: test
test:
	go test -count=1 -race -cover ./...

.PHONY: build
build:
	go test -count=1 -race -cover ./...

.PHONY: build debug test
