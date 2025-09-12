# Dockerfile for bdns-ingest
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV DUCKDB_VERSION=1.3.2
ENV DUCKDB_CLI_URL=https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/duckdb_cli-linux-amd64.gz
ENV DUCKDB_CLI_SHA256=6156fb4e80828f04f0dde5c33d343ba230fddae0c4bc8f376e3590d255ffb999

WORKDIR /app

# Copy sources
COPY . /app

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       bash \
       wget \
       gzip \
       uuid-runtime \
       jq \
    && pip install --upgrade pip \
    && pip install -r /app/requirements.txt \
    && chmod +x /app/bin/*.sh || true \
    && wget "$DUCKDB_CLI_URL" \
    && echo "$DUCKDB_CLI_SHA256  $(basename $DUCKDB_CLI_URL)" | sha256sum --check - \
    && cat $(basename $DUCKDB_CLI_URL) | gunzip > /usr/local/bin/duckdb \
    && chmod +x /usr/local/bin/duckdb \
    && rm -rf /var/lib/apt/lists/*

# Default entry: show help by default
ENTRYPOINT ["./bin/bdns-ingest.sh"]
CMD ["-h"]
