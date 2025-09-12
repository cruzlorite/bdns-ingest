# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1
ENV DUCKDB_VERSION=1.3.2
ENV DUCKDB_CLI_URL=https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/duckdb_cli-linux-amd64.gz
ENV DUCKDB_CLI_SHA256=6156fb4e80828f04f0dde5c33d343ba230fddae0c4bc8f376e3590d255ffb999

WORKDIR /app

# -------------------------------------------------------------------
# Install system dependencies
# -------------------------------------------------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    bash \
    uuid-runtime \
    jq \
    shellcheck \
    bats \
 && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Install Python dependencies separately (cached unless requirements.txt changes)
# -------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# -------------------------------------------------------------------
# Stage to download DuckDB CLI
# -------------------------------------------------------------------
FROM base AS duckdb-downloader
RUN apt-get update && apt-get install -y --no-install-recommends wget gzip  \
 && wget "$DUCKDB_CLI_URL" \
 && echo "$DUCKDB_CLI_SHA256  $(basename $DUCKDB_CLI_URL)" | sha256sum --check - \
 && gunzip -c "$(basename $DUCKDB_CLI_URL)" > /usr/local/bin/duckdb \
 && chmod +x /usr/local/bin/duckdb \
 && rm *.gz \
 && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Final runtime image
# -------------------------------------------------------------------
FROM base

# Copy DuckDB CLI from builder
COPY --from=duckdb-downloader /usr/local/bin/duckdb /usr/local/bin/duckdb

# Copy the actual application last (changes most often)
COPY . .

# Ensure scripts are executable
RUN chmod +x /app/bin/*.sh || true

ENTRYPOINT ["./bin/bdns-ingest.sh"]
CMD ["-h"]

