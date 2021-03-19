.PHONY: all
all: audit test build

.PHONY: audit
audit:
	go list -json -m all | nancy sleuth --exclude-vulnerability-file ./.nancy-ignore

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test -race -cover ./...