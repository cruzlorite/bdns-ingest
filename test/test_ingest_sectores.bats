#!/usr/bin/env bats

setup() {
  export TEST_TMPDIR=$(mktemp -d)
  export OUTPUT_DIR="$TEST_TMPDIR/output"
  mkdir -p "$OUTPUT_DIR"
}

teardown() {
  rm -rf "$TEST_TMPDIR"
}

@test "bdns-ingest.sh ingests sectores sample and deduplicates" {
  # Mock bdns-fetch by overriding PATH
  export PATH="$TEST_TMPDIR:$PATH"
  echo '#!/bin/bash' > "$TEST_TMPDIR/bdns-fetch"
  echo 'cat "$1"' >> "$TEST_TMPDIR/bdns-fetch"
  chmod +x "$TEST_TMPDIR/bdns-fetch"

  # Run ingest script with sample data
  run ./bin/bdns-ingest.sh \
    -e sectores \
    -s sectores_v1 \
    -o "$OUTPUT_DIR" \
    -- "./test/sectores_sample.jsonl"

  [ "$status" -eq 0 ]
  # Check that a Parquet file was created
  ls "$OUTPUT_DIR"/*.parquet

  # Check deduplication: only 2 unique rows should be present
  row_count=$(duckdb -noheader -c "SELECT COUNT(*) FROM read_parquet('$OUTPUT_DIR/*.parquet');")
  [ "$row_count" -eq 2 ]
}
