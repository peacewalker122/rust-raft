APP_NAME := rust-raft
DOCKER_IMAGE ?= $(APP_NAME):local

.PHONY: build run test fmt lint check clean dev docker-build help

help:
	@echo "Targets: build, run, test, fmt, lint, check, clean, dev, docker-build"

build:
	cargo build

run:
	cargo run

test:
	cargo test

fmt:
	cargo fmt

lint:
	cargo clippy -- -D warnings

check:
	cargo check

clean:
	cargo clean

dev:
	@echo "Use 'cargo watch -x run' if installed for live reload."

docker-build:
	docker build -t $(DOCKER_IMAGE) .
