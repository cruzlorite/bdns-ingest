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

# -----------------------------------------------------------------------------
# bdns-ingest.sh
# A CLI tool to fetch data from the BDNS and ingests into Parquet using DuckDB

set -euo pipefail
shopt -s inherit_errexit nullglob

# Get script path and app home
SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"
APP_HOME="$(dirname "$(dirname "$SCRIPT_PATH")")"

# -------------------------------
# Prepare list of available schemas
# -------------------------------
schema_list=$(\
    ls "$APP_HOME/schemas"/*.json 2>/dev/null \
        | xargs -n1 basename \
        | sed 's/\.json$//' \
        | sed 's/^/\t\t\t- /'\
)

# -------------------------------
# Helper functions
# -------------------------------
YELLOW=$'\e[33m'; GREEN=$'\e[32m'; MAGENTA=$'\e[35m'; CYAN=$'\e[36m'
RED=$'\e[31m'; RESET=$'\e[0m'; BOLD=$'\e[1m'; DIM=$'\e[2m'
ts() { date '+%Y-%m-%d %H:%M:%S'; }
log()   { printf '%s%s [INFO]%s  %s\n'  "$YELLOW" "$(ts)" "$RESET" "$*"; }
error() { printf '%s%s [ERROR]%s %s\n' "$RED"    "$(ts)" "$RESET" "$*"; } >&2

usage() {
    cat <<EOF
Usage: $0 -e <endpoint> -s <schema> [-o <output_path>] [-c <compression>]
          [-n] [-d] [-h] [-- bdns-fetch additional args...]

This script fetches data from a specified BDNS endpoint and ingests it into a Parquet file.

Required arguments:
  -e, --endpoint    BDNS endpoint (sectores, convocatorias, etc.).
  -s, --schema      Schema to use (one of):
 ${schema_list}

Optional arguments:
  -o, --output      Output path (default: ./data/bdns/<endpoint>/<schema>).
  -c, --compression One of [ZSTD, GZIP, LZ4, SNAPPY] (default: ZSTD).

Flags:
  -n, --no-dedup    Skip SQL deduplication step. Disabling it allows concurrent
                    writes in the same path (default: false).
  -d, --dry-run     Print commands and SQL, without executing any commands (default: false).
  -h, --help        Show help message and exit.

All arguments after '--' are passed directly to bdns-fetch.
EOF
    exit 1
}

# -------------------------------
# Parse CLI arguments
# -------------------------------
PARSED=$(getopt -o e:o:s:c:ndh --long endpoint:,output:,schema:,compression:,no-dedup,dry-run,help -- "$@")
eval set -- "$PARSED"

while true; do
    case "$1" in
    -e|--endpoint) endpoint="$2"; shift 2 ;;
    -o|--output) output_path="$2"; shift 2 ;;
    -s|--schema) schema="$2"; shift 2 ;;
    -c|--compression) compression="$2"; shift 2 ;;
    -n|--no-dedup) deduplication=false; shift ;;
    -d|--dry-run) dry_run=true; shift ;;
    -h|--help) usage ;;
    --) shift; bdns_fetch_args=("$@"); break ;;
        *) break ;;
    esac
done

# -------------------------------
# Validate required parameters
# -------------------------------
if [[ -z "${endpoint-}" || -z "${schema-}" ]]; then
  error "endpoint and schema are required." >&2
  usage
  exit 1
fi

if [[ ! -f "$APP_HOME/schemas/${schema}.json" ]]; then
    error "Schema '$schema' not found in $APP_HOME/schemas/. Run bdns-ingest.sh -h for help."
    usage
fi

# -------------------------------
# Prepare temp file and cleanup
# -------------------------------
export temp_file=$(mktemp --suffix=.bdns-fetch.jsonl)
trap 'rm -f "$temp_file"' EXIT

# -------------------------------
# Set defaults and prepare parameters
# -------------------------------
readonly batch_id="$(date +%Y%m%d_%H%M%S)_$(uuidgen -r | cut -c1-8)"
readonly input_file="$temp_file"
readonly output_path="${output_path:-./data/bdns/$schema}"
readonly output_file="$output_path/$batch_id.parquet"
readonly schema_path="$APP_HOME/schemas/${schema}.json"
readonly columns=$(< "$schema_path")
readonly compression="${compression:-ZSTD}"
readonly deduplication="${deduplication:-true}"
readonly dry_run="${dry_run:-false}"
readonly json_config="{
  \"batch_id\":         \"$batch_id\",
  \"endpoint\":         \"$endpoint\",
  \"schema\":           \"$schema\",
  \"input_file\":       \"$input_file\",
  \"output_path\":      \"$output_path\",
  \"output_temp_file\": \"$temp_file\",
  \"columns\":          $columns,
  \"compression\":      \"$compression\",
  \"deduplication\":    \"$deduplication\",
  \"bdns_fetch_args\":  \"${bdns_fetch_args[*]:-""}\",
  \"dry_run\":          \"$dry_run\"
}"

# --------------------------------
# Print banner
# --------------------------------
echo -e "${RESET}"
echo -e "${YELLOW}BDNS Ingest Pipeline${RESET} — started at ${BOLD}$(date '+%Y-%m-%d %H:%M:%S %Z')${RESET}"
echo "────────────────────────────────────────────────────────────────────────"
echo -e "${CYAN}Batch ID      ${RESET}: ${BOLD}${batch_id}${RESET}"
echo -e "${CYAN}Endpoint      ${RESET}: ${BOLD}${endpoint}${RESET}"
if [[ "${#bdns_fetch_args[@]}" -gt 0 ]]; then
echo -e "${CYAN}Fetch args    ${RESET}: ${BOLD}${bdns_fetch_args[*]}${RESET}"
fi
echo -e "${CYAN}Schema        ${RESET}: ${BOLD}${schema}${RESET}"
echo -e "${CYAN}Output path   ${RESET}: ${BOLD}${output_path}${RESET}"
echo -e "${CYAN}Output file   ${RESET}: ${BOLD}${output_file}${RESET}"
echo -e "${CYAN}Compression   ${RESET}: ${BOLD}${compression}${RESET}"
echo -e "${CYAN}Deduplication ${RESET}: ${BOLD}${deduplication}${RESET}"
echo -e "${CYAN}Dry run       ${RESET}: ${BOLD}${dry_run}${RESET}"
echo "────────────────────────────────────────────────────────────────────────"
echo
echo -e " ${DIM}This program is distributed in the hope that it will be useful, but"
echo " WITHOUT ANY WARRANTY; without even the implied warranty of"
echo " MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU"
echo " General Public License for more details.${RESET}"
echo

# -------------------------------
# Compile SQL template
# -------------------------------
sql=$(echo "$json_config" | jinjanate "$APP_HOME/sql/ingest_from_file.sql.j2" - --quiet -f json)
log "SQL compiled successfully..."

# -------------------------------
# Functions
# -------------------------------

dry_run_report() {
    log "Dry run mode enabled. No commands will be executed."
    log "BDNS Fetch command to be executed:"
    log "bdns-fetch $endpoint ${bdns_fetch_args[@]} > ${temp_file}"
    log "SQL to be executed:"
    echo "$sql"
}

fetch_and_ingest() {
    log "Running bdns-fetch ${endpoint}..."
    bdns-fetch $endpoint ${bdns_fetch_args[@]} > ${temp_file}
    log "bdns-fetch results successfully saved to '$temp_file'!"

    log "Running DuckDB ingestion..."
    duckdb :memory: -c "$sql" > /dev/null
    log "Ingestion run successfully into '$output_file'!"

    records_fetched=$(wc -l < "$temp_file" | tr -d ' ')
    records_ingested=$(\
        duckdb -json -c "SELECT COUNT(*) AS count FROM read_parquet('$output_file');" | \
            jq -r '.[0].count')

    log "Records fetched: $records_fetched"
    log "Records ingested (dedup=$deduplication): $records_ingested ($(( records_ingested * 100 / records_fetched ))%)"
}

# -------------------------------
# Dry-run or execute
# -------------------------------

if [[ "$dry_run" == true ]]; then
    dry_run_report
else
    fetch_and_ingest
fi

log "Pipeline completed successfully!"

