APP_NAME := rust-raft
DOCKER_IMAGE ?= $(APP_NAME):local

.PHONY: build run test fmt lint check clean dev docker-build docker-integration help

help:
	@echo "Targets: build, run, test, fmt, lint, check, clean, dev, docker-build, docker-integration"

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
	docker buildx build --load --cache-from=type=local,src=.buildx-cache --cache-to=type=local,dest=.buildx-cache-new,mode=max -t $(DOCKER_IMAGE) . && rm -rf .buildx-cache && mv .buildx-cache-new .buildx-cache

docker-integration:
	docker compose -f docker-compose.test.yml up --build --wait

docker-down:
	docker compose -f docker-compose.test.yml down
