# BDNS Ingest

gcloud storage hmac create \
  --project= \
  mysa

[![English](https://img.shields.io/badge/lang-English-blue.svg)](README.md)
[![Spanish](https://img.shields.io/badge/lang-Español-red.svg)](README.es.md)

CLI tools to fetch data from BDNS endpoints, ingest into DuckDB-backed Parquet datasets, and optionally repartition existing Parquet files. Uses lightweight Bash scripts, JSON schemas, and Jinja-templated SQL to type, deduplicate, and attach per-batch metadata.

> BDNS refers to Spain’s “Base de Datos Nacional de Subvenciones”. These utilities work with any JSONL output produced by a compatible `bdns-fetch` CLI.

[Versión en español](README.es.md)

## Features

- Ingest JSONL from BDNS endpoints into Parquet via DuckDB
- Validate and cast input using per-endpoint/version JSON schemas
- Deduplicate across runs using a stable row hash
- Attach ingestion metadata (batch ID, timestamp, row hash)
- Repartition existing Parquet with configurable target size
- Dry-run mode to preview commands and SQL

## Repo Layout

- `bin/bdns-ingest.sh`: Calls `bdns-fetch`, renders DuckDB SQL via Jinja, ingests and deduplicates to Parquet (one file per batch).
- `bin/bdns-repartition.sh`: Rewrites a Parquet directory into new files with target size and compression.
- `sql/ingest_from_file.sql.j2`: Jinja template that loads JSONL, enriches with metadata, deduplicates, and writes Parquet.
- `sql/repartition.sql.j2`: Jinja template that reads Parquet and writes repartitioned Parquet.
- `schemas/*.json`: DuckDB type maps per endpoint/version, e.g. `schemas/convocatorias_v1.json`.

## Requirements

Install the following and ensure they’re on your `PATH`:

- DuckDB CLI (`duckdb`)
- Jinja CLI (`jinja`) from the `jinja-cli` Python package
- `jq` for JSON processing
- `uuidgen` (commonly from `util-linux` or `uuid-runtime`)
- `bdns-fetch` capable of emitting JSONL for the desired BDNS endpoint

Examples:

```bash
# macOS (Homebrew)
brew install duckdb jq
pipx install jinja-cli  # or: pip install --user jinja-cli

# Debian/Ubuntu
sudo apt-get install -y duckdb jq uuid-runtime
pipx install jinja-cli  # or: pip install --user jinja-cli

# bdns-fetch is external; install per its README
```

## Data Model and Behavior

- Default output directory: `./data/bdns/<endpoint>/<version>`.
- Each run produces one Parquet file named `<batch_id>.parquet`.
- Deduplication: computes `row_hash` from the alphabetical list of non-metadata column names (as emitted by `jq | keys`) and anti-joins against all existing Parquet files in the output directory.
- Metadata: adds a `__metadata` struct with `batch_id`, `ingest_time`, and `row_hash`.

## Quick Start

1) Create a schema for your endpoint if missing. Example `schemas/convocatorias_v1.json`:

```json
{
  "numeroConvocatoria": "VARCHAR",
  "mrr": "BOOLEAN",
  "descripcion": "VARCHAR",
  "descripcionLeng": "VARCHAR",
  "fechaRecepcion": "DATE",
  "nivel1": "VARCHAR",
  "nivel2": "VARCHAR",
  "nivel3": "VARCHAR",
  "codigoInvente": "VARCHAR"
}
```

2) Ensure the output directory exists (the ingest script does not create it):

```bash
mkdir -p ./data/bdns/convocatorias/v1
```

3) Run a dry run to preview SQL and commands:

```bash
bin/bdns-ingest.sh \
  -e convocatorias \
  -v v1 \
  -d -- \
  # any additional bdns-fetch flags go here (optional)
```

4) Execute the ingest:

```bash
bin/bdns-ingest.sh \
  -e convocatorias \
  -v v1 \
  -- \
  # any additional bdns-fetch flags go here (optional)
```

After completion, check `./data/bdns/convocatorias/v1/*.parquet`.

## Usage

### Ingest

```text
Usage: bin/bdns-ingest.sh
        -e <endpoint> -v <version> [-o <output_path>] [-c <compression>]
        [-s <schema>] [-d] [-h] [-- bdns-fetch additional args...]

Options:
  -e, --endpoint    BDNS endpoint (required)
  -v, --version     Endpoint version (required)
  -s, --schema      Schema path (default: ./schemas/<endpoint>_<version>.json)
  -o, --output      Output path (default: ./data/bdns/<endpoint>/<version>)
  -c, --compression Compression codec for Parquet output (default: ZSTD)
  -t, --temp-file   Temp JSONL file name (default: __temp__<batch_id>.jsonl)
  -d, --dry-run     Print commands and SQL without executing
  -h, --help        Show help

All arguments after '--' are passed directly to bdns-fetch.
```

Notes:

- Default schema path: `./schemas/<endpoint>_<version>.json`.
- Default output path: `./data/bdns/<endpoint>/<version>`.
- Create the output directory beforehand.
- The script reads JSONL from `bdns-fetch` stdout, writes a temporary file, and removes it at the end.

Examples:

```bash
# Minimal example (no extra fetch args)
mkdir -p ./data/bdns/beneficiarios/v1
bin/bdns-ingest.sh -e beneficiarios -v v1

# Override schema or compression
bin/bdns-ingest.sh -e convocatorias -v v1 -s ./schemas/convocatorias_v1.json -c ZSTD

# Pass additional flags to bdns-fetch after '--'
bin/bdns-ingest.sh -e ayudasestado -v v1 -- --help
```

### Repartition

```text
Usage: bin/bdns-repartition.sh -i <input_path> -o <output_path> [-c <compression>] [-f <file_size_mb>] [-h]

Options:
  -i, --input              Input path (required)
  -o, --output             Output path (required)
  -f, --file-size-mb       Max size of each output Parquet (default: 128)
  -c, --compression        Compression codec (default: ZSTD)
  -p, --file-name-pattern  Filename pattern for outputs (default: part-{uuid})
  -d, --dry-run            Print generated SQL without executing
  -h, --help               Show help
```

Example:

```bash
bin/bdns-repartition.sh \
  -i ./data/bdns/convocatorias/v1 \
  -o ./data/bdns/convocatorias/v1-repart \
  -f 256 \
  -c ZSTD
```

## Adding or Updating Schemas

Schemas are simple JSON objects mapping field names to DuckDB types. Naming convention: `schemas/<endpoint>_<version>.json`.

```json
{
  "id": "INTEGER",
  "descripcion": "VARCHAR"
}
```

Guidelines:

- Use DuckDB-compatible types (`VARCHAR`, `INTEGER`, `BIGINT`, `TIMESTAMP`, `DATE`, `BOOLEAN`, `DECIMAL(18,2)`, etc.).
- The row hash uses the alphabetical list of keys (`jq | keys`). Adding/removing/renaming fields changes the hash.
- Extra input fields not in the schema are ignored; missing schema fields become `NULL`.

## How It Works

1) `bdns-fetch` writes JSONL to a temp file.
2) The Jinja template (`sql/ingest_from_file.sql.j2`) generates DuckDB SQL using the schema.
3) DuckDB:
   - Reads JSONL with explicit `columns = { ... }` typing
   - Adds `__metadata` with batch info and a deterministic `row_hash`
   - Anti-joins existing Parquet in the output directory to drop duplicates
   - Writes a Parquet file named after the batch ID

## Troubleshooting

- `jinja: command not found`: install `jinja-cli` (e.g., `pipx install jinja-cli`).
- `duckdb: command not found`: install the DuckDB CLI.
- `jq: command not found`: install `jq`.
- `uuidgen: command not found`: install `uuid-runtime` (Debian/Ubuntu) or `util-linux`.
- Permissions/paths: ensure the output directory exists and is writable.
- Empty output file created: the template first creates an empty Parquet, then the second COPY overwrites with actual data.

## Notes and Limitations

- The ingest script expects a JSONL stream from `bdns-fetch`. Ensure your version supports the endpoint and flags.
- Changing the set of schema fields alters `row_hash` and deduplication behavior.
- For large historical datasets, consider `bdns-repartition.sh` to balance file sizes.

## License

The scripts under `bin/` include GPLv3 headers. If you need a top-level `LICENSE` file, add one matching your distribution model.

## Acknowledgements

- DuckDB for fast local analytics
- Jinja for simple SQL templating
- `bdns-fetch` for retrieving BDNS data
