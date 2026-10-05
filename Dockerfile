# syntax=docker/dockerfile:1
FROM rust:1.85-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin fighter-server

FROM debian:bookworm-slim AS runtime
RUN useradd --system --uid 10001 --create-home fighter
COPY --from=builder /src/target/release/fighter-server /usr/local/bin/fighter-server
COPY --from=builder /src/config/server.example.toml /etc/fighter/server.toml
USER fighter
EXPOSE 8080/tcp
EXPOSE 7780/udp
ENV FIGHTER_CONFIG=/etc/fighter/server.toml
ENTRYPOINT ["/usr/local/bin/fighter-server"]
