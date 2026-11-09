# syntax=docker/dockerfile:1
# Full source build for the local reference stack.
# Trixie provides the glibc version required by the ONNX Runtime binary.
FROM rust:1-trixie AS builder

# Build deps: C toolchain for `onig`/tokenizers, cmake, openssl headers (for any
# native-tls crate), curl for model download. ort downloads its own ONNX Runtime
# binaries at build time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake clang curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Build context excludes credentials, generated outputs and private Git metadata.
COPY . .

# CPU embeddings and syntax-aware code chunking.
RUN --mount=type=cache,target=/build/target --mount=type=cache,target=/usr/local/cargo/registry cargo build --release -p coralswarm-backend --features code-chunking \
    --bin coralswarm-backend --bin migrate_control --bin migrate_tenants && \
    mkdir -p /build/artifacts && cp /build/target/release/coralswarm-backend /build/target/release/migrate_control /build/target/release/migrate_tenants /build/artifacts/ && \
    mkdir -p /build/runtime-libs && find /build/target/release -maxdepth 1 -name 'libonnxruntime.so*' -exec cp -L {} /build/runtime-libs/ \;

# A missing model is a build failure; release startup requires real embeddings.
RUN mkdir -p /build/models && \
    curl -fL --retry 3 \
      "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/onnx/model.onnx" \
      -o /build/models/bge-small-en-v1.5.onnx && \
    curl -fL --retry 3 \
      "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/tokenizer.json" \
      -o /build/models/bge-small-en-v1.5-tokenizer.json

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:trixie-slim AS runtime

# ca-certificates: TLS to Slack/Anthropic/R2. libgomp1: ONNX Runtime OpenMP.
# libssl3: any dynamically-linked openssl.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libgomp1 libssl3 libstdc++6 libzstd1 zlib1g \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The backend binary, the ONNX Runtime shared lib (copy-dylibs placed it next to
# the binary), and the model files.
COPY --from=builder /build/artifacts/coralswarm-backend /app/coralswarm-funded-backend
COPY --from=builder /build/runtime-libs/ /app/
COPY --from=builder /build/models /app/models

# Explicit one-shot migrations run before the API starts.
COPY --from=builder /build/artifacts/migrate_control /app/migrate_control
COPY --from=builder /build/artifacts/migrate_tenants /app/migrate_tenants

# ort dynamically links libonnxruntime.so — make it findable.
ENV LD_LIBRARY_PATH=/app
ENV EMBED_MODEL_PATH=/app/models/bge-small-en-v1.5.onnx
ENV EMBED_TOKENIZER_PATH=/app/models/bge-small-en-v1.5-tokenizer.json

COPY scripts/local/start.sh /app/local-start.sh
EXPOSE 8080
CMD ["/app/coralswarm-funded-backend"]
