


docker run --env-file .env bdns-ingest:latest \
    --endpoint ayudasestado-busqueda  \
    --schema ayudasestado_v1 \
    --output gs://bdns/ayudaestado_v1 \
    -- --fechaDesde 2000-01-01 -np 0

# ./bin/bdns-ingest.sh \
#     --endpoint partidospoliticos-busqueda  \
#     --schema partidospoliticos_v1 \
#     --output gs://bdns/partidospoliticos_v1 \
#     -- --fechaDesde 2000-01-01 -np 0


# duckdb :memory: -c "
#     INSTALL httpfs;
#     LOAD httpfs;

#     CREATE SECRET s3_secret (
#         TYPE s3,
#         PROVIDER credential_chain
#     );

#     SELECT * FROM read_parquet('gs://bdns/partidospoliticos_v1/*.parquet');
# "