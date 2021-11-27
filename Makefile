.PHONY: deps fmt lint build

all: build

PROG=gosnmp-playground

deps:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.43.0

fmt:
	go fmt ./...

lint:
	golangci-lint run ./...

build:
	go build -o $(PROG)
