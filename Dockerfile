FROM rust:1.93-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

COPY crates ./crates
COPY examples ./examples

RUN cargo build --release -p axum-api

RUN cargo build --release -p jobrail-worker


FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/axum-api /usr/local/bin/axum-api
COPY --from=builder /app/target/release/worker /usr/local/bin/worker
COPY --from=builder /app/target/release/scheduled_scheduler /usr/local/bin/scheduled_scheduler
COPY --from=builder /app/target/release/repeatable_scheduler /usr/local/bin/repeatable_scheduler

CMD ["axum-api"]