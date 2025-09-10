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
# bdns-repartition.sh
# A CLI tool to repartition existing Parquet files in a BDNS ingest path
# -----------------------------------------------------------------------------

set -euo pipefail
IFS=$'\n\t'

# -------------------------------
# Helper functions
# -------------------------------
log() {
    echo -e "[INFO] $*"
}

error() {
    echo -e "[ERROR] $*" >&2
}

usage() {
    cat <<EOF
Usage: $0 -i <input_path> -o <output_path> [-c <compression>] [-h]
Options:
  -i, --input           Input path (required)
  -o, --output          Output path (required)
  -f, --file-size-mb    Maximum size of each output Parquet file in MB (default: 128)
  -c, --compression     Compression codec for Parquet output (default: ZSTD)
  -d, --dry-run         Print commands and SQL, optionally validate SQL without running
  -h, --help            Show this help message
EOF
    exit 1
}

# -------------------------------
# Default values
# -------------------------------
INPUT_PATH=""
OUTPUT_PATH=""
COMPRESSION="ZSTD"
DRY_RUN=false
FILE_SIZE_MB=128
FILE_NAME_PATTERN="part-{uuid}"

# -------------------------------
# Parse CLI arguments
# -------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        -i|--input)
            INPUT_PATH="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        -c|--compression)
            COMPRESSION="$2"
            shift 2
            ;;
        -f|--file-size-mb)
            FILE_SIZE_MB="$2"
            shift 2
            ;;
        -p|--file-name-pattern)
            FILE_NAME_PATTERN="$2"
            shift 2
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [[ -z "$INPUT_PATH" || -z "$OUTPUT_PATH" ]]; then
    error "Input and output paths are required."
    usage
fi

log "Loading from: '$INPUT_PATH'"
log "   repartitioning into: '$OUTPUT_PATH'"
log "   with compression: '$COMPRESSION'"

# Execute sql/repartition.sql.j2 with DuckDB
SQL=$(jinja ./sql/repartition.sql.j2 \
    -D input_path "$INPUT_PATH" \
    -D output_path "$OUTPUT_PATH" \
    -D compression "$COMPRESSION" \
    -D file_size_bytes $(( FILE_SIZE_MB * 1024 * 1024 )) \
    -D file_name_pattern "$FILE_NAME_PATTERN"
)

if [[ -z "$SQL" ]]; then
    error "Failed to generate SQL from template."
    exit 1
fi

if [[ "${DRY_RUN:-false}" == true ]]; then
    log "Generated SQL (dry run):"
    echo "$SQL"
    exit 0
else
    log "Running repartitioning SQL..."
    duckdb -c "$SQL"
    log "Repartitioning completed successfully."
fi
