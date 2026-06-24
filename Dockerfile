### Stage 1: build the Rust binary
FROM rust:1.92-slim-bookworm AS builder

WORKDIR /app

# Cache dependency compilation separately from source
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Build the real binary
COPY src ./src
RUN touch src/main.rs && cargo build --release

### Stage 2: runtime image
FROM debian:bookworm-slim

# Tools:
#   megatools  — megadl CLI for Mega.nz
#   python3 + pip — needed for gallery-dl
#   ffmpeg     — gallery-dl uses it for video muxing
#   ca-certificates — TLS roots for HTTPS downloads
RUN apt-get update && apt-get install -y --no-install-recommends \
      megatools \
      python3 \
      python3-pip \
      ffmpeg \
      ca-certificates \
    && pip3 install --break-system-packages gallery-dl \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/media-manager /usr/local/bin/media-manager

ENV DOWNLOAD_DIR=/downloads

VOLUME ["/downloads"]

EXPOSE 3000

CMD ["media-manager"]
