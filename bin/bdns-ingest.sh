#!/usr/bin/env bash
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <https://www.gnu.org/licenses/>.
#
# -----------------------------------------------------------------------------
# bdns-ingest.sh
# A CLI tool to fetch data from BDNS endpoints and ingest it into DuckDB
# Supports a dry-run mode that prints info and optionally validates SQL
# -----------------------------------------------------------------------------

set -euo pipefail
IFS=$'\n\t'
shopt -s inherit_errexit nullglob

# -------------------------------
# Helper functions
# -------------------------------
log() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*"; }
log_dry_run() { log "[DRY-RUN] $*"; }
error() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; }

usage() {
    cat <<EOF
Usage: $0
        -e <endpoint> -v <version> [-o <output_path>] [-c <compression>]
        [-s <schema>] [-d] [-h] [-- bdns-fetch additional args...]

Options:
  -e, --endpoint    BDNS endpoint (required)
  -v, --version     Endpoint version (required)
  -s, --schema      Schema version (default: ./schemas/<endpoint>_<version>.json)
  -o, --output      Output path (default: ./data/bdns/<endpoint>/<version>)
  -c, --compression Compression codec for Parquet output (default: ZSTD)
  -t, --temp-file   File to use for temporary storage (default: __temp__<batch_id>.jsonl)
  -d, --dry-run     Print commands and SQL, optionally validate SQL without running
  -h, --help        Show this help message

All arguments after '--' are passed directly to bdns-fetch.
EOF
    exit 1
}

# -------------------------------
# Default values
# -------------------------------
ENDPOINT=""
VERSION=""
OUTPUT_PATH=""
SCHEMA=""
COMPRESSION="ZSTD"
DRY_RUN=false
TEMP_FILE=""
BDNS_FETCH_ARGS=()

# -------------------------------
# Parse CLI arguments
# -------------------------------
PARSED=$(getopt -o e:v:o:s:c:h --long endpoint:,version:,output:,schema:,compression:,dry-run,help -- "$@") || usage
eval set -- "$PARSED"

while true; do
    case "$1" in
        -e|--endpoint) ENDPOINT="$2"; shift 2 ;;
        -v|--version) VERSION="$2"; shift 2 ;;
        -o|--output) OUTPUT_PATH="$2"; shift 2 ;;
        -s|--schema) SCHEMA="$2"; shift 2 ;;
        -c|--compression) COMPRESSION="$2"; shift 2 ;;
        -t|--temp-file) TEMP_FILE="$2"; shift 2 ;;
        -d|--dry-run) DRY_RUN=true; shift ;;
        -h|--help) usage ;;
        --) shift; BDNS_FETCH_ARGS=("$@"); break ;;
        *) break ;;
    esac
done

# -------------------------------
# Validate required parameters
# -------------------------------
if [[ -z "$ENDPOINT" || -z "$VERSION" ]]; then
    error "Endpoint and version are required."
    usage
fi

# -------------------------------
# Set defaults
# -------------------------------
readonly BATCH_ID="$(uuidgen -r)"
readonly OUTPUT_PATH="${OUTPUT_PATH:-./data/bdns/${ENDPOINT}/${VERSION}}"
readonly SCHEMA="${SCHEMA:-./schemas/${ENDPOINT}_${VERSION}.json}"
readonly COMPRESSION="${COMPRESSION:-ZSTD}"
readonly TEMP_FILE="${TEMP_FILE:-__temp__${BATCH_ID}.jsonl}"

log "Starting ingestion for endpoint: $ENDPOINT, version: $VERSION"
log "Output path: $OUTPUT_PATH"
log "Output temp file: $TEMP_FILE"
log "Using compression: $COMPRESSION"
log "Using schema: $SCHEMA"
log "Additional bdns-fetch args: ${BDNS_FETCH_ARGS[*]:-none}"

# -------------------------------
# Generate SQL from Jinja template
# -------------------------------
OUTPUT_FILE="$OUTPUT_PATH/$BATCH_ID.parquet"

# Read JSON schema directly with jq
COLUMNS_LIST=$(jq -r 'keys | join(", ")' "$SCHEMA")
COLUMNS=$(<"$SCHEMA")  # raw JSON content

SQL=$(jinja "./sql/ingest_from_file.sql.j2" \
    -D input_file "$TEMP_FILE" \
    -D batch_id "$BATCH_ID" \
    -D output_path "$OUTPUT_PATH" \
    -D columns "$COLUMNS" \
    -D columns_list "$COLUMNS_LIST" \
    -D compression "$COMPRESSION" \
)

log "Generated SQL successfully."

# -------------------------------
# Functions
# -------------------------------
dry_run_report() {
    log_dry_run "Batch ID: $BATCH_ID"
    log_dry_run "Output path: $OUTPUT_PATH"
    log_dry_run "Columns list: $COLUMNS_LIST"
    log_dry_run "bdns-fetch command: bdns-fetch $ENDPOINT ${BDNS_FETCH_ARGS[*]}"
    log_dry_run "SQL to be executed:"
    echo "$SQL"
}

fetch_and_ingest() {
    log "Running bdns-fetch ${ENDPOINT} ${BDNS_FETCH_ARGS[*]}"
    bdns-fetch $ENDPOINT ${BDNS_FETCH_ARGS[@]} > ${TEMP_FILE}

    log "Running DuckDB ingestion..."
    duckdb "$OUTPUT_FILE" -c "$SQL"
    log "Ingestion run successfully into '$OUTPUT_FILE'."

    RECORDS_FETCHED=$(wc -l < "$TEMP_FILE" | tr -d ' ')
    RECORDS_INGESTED=$(
        duckdb -json -c "SELECT COUNT(*) AS count FROM read_parquet('$OUTPUT_FILE');" | \
            jq -r '.[0].count')

    log "Cleaning up temporary file '$TEMP_FILE'"
    rm -f "$TEMP_FILE"

    log "Records fetched: $RECORDS_FETCHED"
    log "Records ingested: $RECORDS_INGESTED ($(( RECORDS_INGESTED * 100 / RECORDS_FETCHED ))%)"
}

# -------------------------------
# Dry-run or execute
# -------------------------------
if [[ "$DRY_RUN" == true ]]; then
    dry_run_report
    exit 0
else
    fetch_and_ingest
fi

