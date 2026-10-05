# Imagen del binario tachyon.
#
# Etapa 1: compila desde fuente. rdkafka usa cmake-build: librdkafka se
# compila desde fuente, por lo que el builder necesita un compilador C y
# cmake (la imagen rust oficial trae gcc; cmake se instala aquí).
FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY tachyon tachyon
RUN cargo build --release --bin tachyon

# Etapa 2: runtime mínimo con glibc y certificados (rustls native-roots
# lee los del sistema para hablar con Redpanda/Kinesis/SQS vía TLS).
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/tachyon /usr/local/bin/tachyon
WORKDIR /app
ENTRYPOINT ["tachyon"]
CMD ["--help"]
