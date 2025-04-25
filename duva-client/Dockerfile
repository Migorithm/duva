FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app
# Original code from https://github.com/LukeMathWalker/cargo-chef?tab=readme-ov-file#how-to-use

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder 
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin duva-client

# We do not need the Rust toolchain to run the binary!
FROM debian:bookworm-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/duva-client /usr/local/bin
ENTRYPOINT ["/usr/local/bin/duva-client"]
