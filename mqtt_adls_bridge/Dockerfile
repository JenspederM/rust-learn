# Dockerfile for creating a statically-linked Rust application using docker's
# multi-stage build feature. This also leverages the docker build cache to avoid
# re-downloading dependencies if they have not changed.
FROM rust:alpine3.15 as build

# install system dependencies
RUN USER=root apk upgrade --update-cache --available && \
    apk add pkgconfig openssl-dev cmake build-base && \
    rm -rf /var/cache/apk/*

WORKDIR /usr/src

# Create a dummy project and build the app's dependencies.
# If the Cargo.toml or Cargo.lock files have not changed,
# we can use the docker build cache and skip these (typically slow) steps.
RUN USER=root cargo new mqtt_adls_bridge
WORKDIR /usr/src/mqtt_adls_bridge
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release
RUN rm src/*.rs

# Copy the source and build the application.
COPY src ./src
RUN cargo build --release

FROM scratch
COPY --from=build /usr/src/mqtt_adls_bridge/target/release/mqtt_adls_bridge .
USER 1000
CMD ["./mqtt_adls_bridge"]