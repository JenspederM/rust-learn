# STAGE 1 is to build the binary
# Use rust-based image for container
FROM rust:1.62 AS builder

RUN apt-get update && apt-get install -y \
    cmake make

WORKDIR /usr/src/mqtt_adls_bridge

COPY . .

RUN cargo install --path .

FROM debian:buster-slim

RUN apt-get update && apt-get install -y \
    libssl-dev

COPY --from=builder /usr/local/cargo/bin/mqtt_adls_bridge /usr/local/bin/mqtt_adls_bridge

CMD ["mqtt_adls_bridge"]