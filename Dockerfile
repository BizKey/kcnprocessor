FROM rust:1.91.1-alpine3.22 AS builder

RUN apk add --no-cache musl-dev openssl-dev pkgconfig openssl-libs-static

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:3.22

RUN apk add --no-cache libgcc openssl ca-certificates

WORKDIR /app

COPY --from=builder /app/target/release/kcnmonitoring /app/

RUN chmod +x /app/kcnmonitoring

RUN adduser -D -u 1000 myuser
USER myuser

ENV RUST_LOG=INFO

CMD ["/app/kcnmonitoring"]