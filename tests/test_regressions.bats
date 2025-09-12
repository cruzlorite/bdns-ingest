# !/usr/bin/env bats
# Integration tests for bdns-ingest.sh using Bats

setup() {
    # Create a temporary directory for mocks
    export TEMP_DIR=$(mktemp -d)
    
    # Add the mocks directory to PATH
    chmod +x "$BATS_TEST_DIRNAME/mocks/bdns-fetch"
    export PATH="$BATS_TEST_DIRNAME/mocks:$PATH"
}

teardown() {
    rm -rf "$TEMP_DIR"
}

parquet2csv() {
    local parquet_path="$1"
    local tmp_csv="$(mktemp -u "$TEMP_DIR/tmp.XXXXXX.csv")"

    duckdb -csv -c "
        SELECT * EXCLUDE(__metadata), __metadata.row_hash AS row_hash
        FROM read_parquet('$parquet_path/*.parquet')" \
        > "$tmp_csv"
    echo "$tmp_csv"
}


@test "Ingest matches golden file for sectores" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores.jsonl")"

    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR"
    [ "$status" -eq 0 ]

    # Now compare actual output (if any) with the golden file
    ACTUAL="$(parquet2csv "$TEMP_DIR")"
    GOLDEN="$BATS_TEST_DIRNAME/golden/sectores.csv"

    echo "run diff -u $GOLDEN $ACTUAL"
    run diff -u "$GOLDEN" "$ACTUAL"
    [ "$status" -eq 0 ]
}

@test "Ingest data with duplicates fails" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores_with_duplicates.jsonl")"

    # Run the ingest script
    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR"
    [ "$status" -ne 0 ]
    [[ "$output" == *"Duplicated rows found in input file"* ]]
}

@test "Ingest data with duplicates and --no-dedup succeeds" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores_with_duplicates.jsonl")"

    # Run the ingest script
    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR --no-dedup"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR" --no-dedup
    [ "$status" -eq 0 ]
}

@test "Ingest with data missing columns does not fail" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores_with_missing_columns.jsonl")"

    # Run the ingest script
    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR"
    [ "$status" -eq 0 ]
}

@test "Ingest data with extra columns does not fail" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores_with_extra_columns.jsonl")"

    # Run the ingest script
    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR"
    [ "$status" -eq 0 ]
}

@test "Ingest with wrong data types fails" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores_with_wrong_data_types.jsonl")"

    # Run the ingest script
    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR"
    [ "$status" -ne 0 ]
}

@test "Ingest with malformed JSON fails" {
    # Prepare mock data
    export BDNS_FETCH_OUTPUT="$(cat "$BATS_TEST_DIRNAME/data/sectores_malformed.jsonl")"

    # Run the ingest script
    echo "run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o $TEMP_DIR"
    run ./bin/bdns-ingest.sh -e sectores -s sectores_v1 -o "$TEMP_DIR"
    [ "$status" -ne 0 ]
}
