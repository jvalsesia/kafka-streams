.PHONY: all up down init-topics build run-producer run-producer-rust run-fraud run-streams run-notif test-1000 clean

SDK_INIT = bash -c 'source "$$HOME/.sdkman/bin/sdkman-init.sh" 2>/dev/null;

all: build

up:
	docker compose up -d

down:
	docker compose down

init-topics:
	./scripts/init-topics.sh

build:
	@echo "==> Building Spring Boot apps..."
	$(SDK_INIT) cd transaction-producer && mvn clean package -DskipTests'
	$(SDK_INIT) cd fraud-alert-streams && mvn clean package -DskipTests'
	@echo "==> Building Rust apps..."
	cargo build --manifest-path transaction-producer-rust/Cargo.toml
	cargo build --manifest-path fraud-detector/Cargo.toml
	cargo build --manifest-path notification-service/Cargo.toml

run-producer:
	$(SDK_INIT) cd transaction-producer && mvn spring-boot:run'

run-producer-rust:
	cargo run --manifest-path transaction-producer-rust/Cargo.toml -- --count 1000

run-fraud:
	cargo run --manifest-path fraud-detector/Cargo.toml

run-streams:
	$(SDK_INIT) cd fraud-alert-streams && mvn spring-boot:run'

run-notif:
	cargo run --manifest-path notification-service/Cargo.toml

test-1000:
	./scripts/trigger-producer.sh 1000

clean:
	$(SDK_INIT) cd transaction-producer && mvn clean'
	$(SDK_INIT) cd fraud-alert-streams && mvn clean'
	cargo clean --manifest-path transaction-producer-rust/Cargo.toml
	cargo clean --manifest-path fraud-detector/Cargo.toml
	cargo clean --manifest-path notification-service/Cargo.toml
