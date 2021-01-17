include ../ENV.list
export $(shell sed 's/=.*//' ../ENV.list)

.PHONY: release, test, run

release:
	cargo update
	cargo build --release
	strip target/release/procurement_microservice

build:
	cargo update
	cargo build

run:
	cargo run

test:
	cargo test