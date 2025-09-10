# BDNS Ingest

[![English](https://img.shields.io/badge/lang-English-blue.svg)](README.md)
[![Spanish](https://img.shields.io/badge/lang-Español-red.svg)](README.es.md)

Herramientas de línea de comandos para obtener datos de endpoints de BDNS, ingerirlos en conjuntos de datos Parquet mediante DuckDB y, opcionalmente, reparticionar ficheros Parquet existentes. Usa scripts Bash ligeros, esquemas JSON y SQL plantillado con Jinja para tipar, desduplicar y adjuntar metadatos por lote.

> BDNS es la “Base de Datos Nacional de Subvenciones” (España). Estas utilidades funcionan con cualquier salida JSONL producida por un `bdns-fetch` compatible.

## Características

- Ingesta de JSONL desde endpoints BDNS a Parquet usando DuckDB
- Tipado y validación de entrada con esquemas JSON por endpoint/versión
- Desduplicación entre ejecuciones mediante un hash estable de fila
- Metadatos de ingesta por registro (batch ID, timestamp, row hash)
- Reparticionado de Parquet con tamaño objetivo configurable
- Modo simulación (dry-run) para previsualizar comandos y SQL

## Estructura del repositorio

- `bin/bdns-ingest.sh`: Llama a `bdns-fetch`, genera SQL con Jinja, ingiere y desduplica a Parquet (un fichero por lote).
- `bin/bdns-repartition.sh`: Reescribe un directorio Parquet a nuevos ficheros con tamaño y compresión objetivo.
- `sql/ingest_from_file.sql.j2`: Plantilla Jinja que carga JSONL, enriquece con metadatos, desduplica y escribe Parquet.
- `sql/repartition.sql.j2`: Plantilla Jinja que lee Parquet y escribe Parquet reparticionado.
- `schemas/*.json`: Mapeos de tipos DuckDB por endpoint/versión (ej. `schemas/convocatorias_v1.json`).

## Requisitos

Instala estas utilidades y asegúrate de tenerlas en el `PATH`:

- DuckDB CLI (`duckdb`)
- Jinja CLI (`jinja`) del paquete `jinja-cli` de Python
- `jq` para procesado JSON
- `uuidgen` (suele venir de `util-linux` o `uuid-runtime`)
- `bdns-fetch` capaz de emitir JSONL para el endpoint BDNS deseado

Ejemplos de instalación:

```bash
# macOS (Homebrew)
brew install duckdb jq
pipx install jinja-cli  # o: pip install --user jinja-cli

# Debian/Ubuntu
sudo apt-get install -y duckdb jq uuid-runtime
pipx install jinja-cli  # o: pip install --user jinja-cli

# bdns-fetch es externo; instala según su README
```

## Modelo de datos y comportamiento

- Directorio de salida por defecto: `./data/bdns/<endpoint>/<version>`.
- Cada ejecución genera un Parquet llamado `<batch_id>.parquet`.
- Desduplicación: calcula `row_hash` a partir de las columnas no-metadato y hace un anti-join contra todos los Parquet existentes en el directorio de salida.
- Metadatos: añade una estructura `__metadata` con `batch_id`, `ingest_time` y `row_hash`.

## Inicio rápido

1) Crea un esquema para tu endpoint si no existe aún. Ejemplo `schemas/convocatorias_v1.json`:

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

2) Asegura que el directorio de salida existe (el script de ingesta no lo crea):

```bash
mkdir -p ./data/bdns/convocatorias/v1
```

3) Lanza una simulación (dry-run) para previsualizar SQL y comandos:

```bash
bin/bdns-ingest.sh \
  -e convocatorias \
  -v v1 \
  -d -- \
  # aquí irían flags adicionales de bdns-fetch (opcional)
```

4) Ejecuta la ingesta:

```bash
bin/bdns-ingest.sh \
  -e convocatorias \
  -v v1 \
  -- \
  # aquí irían flags adicionales de bdns-fetch (opcional)
```

Tras finalizar, revisa `./data/bdns/convocatorias/v1/*.parquet`.

## Uso

### Ingesta

```text
Usage: bin/bdns-ingest.sh
        -e <endpoint> -v <version> [-o <output_path>] [-c <compression>]
        [-s <schema>] [-d] [-h] [-- bdns-fetch additional args...]

Options:
  -e, --endpoint    BDNS endpoint (required)
  -v, --version     Endpoint version (required)
  -s, --schema      Schema version (default: ./schemas/<endpoint>_<version>.json)
  -o, --output      Output path (default: ./data/bdns/<endpoint>/<version>)
  -c, --compression Compression codec for Parquet output (default: ZSTD)
  -t, --temp-file   File for temporary JSONL (default: __temp__<batch_id>.jsonl)
  -d, --dry-run     Print commands and SQL without executing
  -h, --help        Show help

All arguments after '--' are passed directly to bdns-fetch.
```

Notas:

- Ruta de esquema por defecto: `./schemas/<endpoint>_<version>.json`.
- Ruta de salida por defecto: `./data/bdns/<endpoint>/<version>`.
- Crea el directorio de salida antes de ejecutar.
- El script lee JSONL de `bdns-fetch` (stdout), escribe un fichero temporal y lo borra al final.

Ejemplos:

```bash
# Ejemplo mínimo (sin flags extra)
mkdir -p ./data/bdns/beneficiarios/v1
bin/bdns-ingest.sh -e beneficiarios -v v1

# Sobrescribir esquema o compresión
bin/bdns-ingest.sh -e convocatorias -v v1 -s ./schemas/convocatorias_v1.json -c ZSTD

# Pasar flags a bdns-fetch después de '--'
bin/bdns-ingest.sh -e ayudasestado -v v1 -- --help
```

### Reparticionado

```text
Usage: bin/bdns-repartition.sh -i <input_path> -o <output_path> [-c <compression>] [-f <file_size_mb>] [-h]

Options:
  -i, --input           Input path (required)
  -o, --output          Output path (required)
  -f, --file-size-mb    Max size of each output Parquet (default: 128)
  -c, --compression     Compression codec (default: ZSTD)
  -p, --file-name-pattern  Filename pattern for outputs (default: part-{uuid})
  -d, --dry-run         Print generated SQL without executing
  -h, --help            Show help
```

Ejemplo:

```bash
bin/bdns-repartition.sh \
  -i ./data/bdns/convocatorias/v1 \
  -o ./data/bdns/convocatorias/v1-repart \
  -f 256 \
  -c ZSTD
```

## Añadir o actualizar esquemas

Los esquemas son objetos JSON que mapean nombres de campos a tipos de DuckDB. Convención de nombre: `schemas/<endpoint>_<version>.json`.

```json
{
  "id": "INTEGER",
  "descripcion": "VARCHAR"
}
```

Pautas:

- Usa tipos compatibles con DuckDB (`VARCHAR`, `INTEGER`, `BIGINT`, `TIMESTAMP`, `DATE`, `BOOLEAN`, `DECIMAL(18,2)`, etc.).
- El hash de filas usa la lista de claves en orden alfabético (por cómo `jq` emite `keys`). Añadir, quitar o renombrar campos cambiará el hash.
- Si la entrada contiene campos extra no definidos en el esquema se ignoran; los ausentes quedan en `NULL`.

## Cómo funciona

1) `bdns-fetch` escribe JSONL a un fichero temporal.
2) La plantilla Jinja (`sql/ingest_from_file.sql.j2`) genera SQL para DuckDB usando el esquema.
3) DuckDB:
   - Lee JSONL con tipado explícito `columns = { ... }`
   - Añade `__metadata` con info del lote y `row_hash` determinista
   - Anti-join contra los Parquet existentes en el directorio de salida para eliminar duplicados
   - Escribe un Parquet nombrado con el `batch_id`

## Problemas comunes

- `jinja: command not found`: instala `jinja-cli` (ej. `pipx install jinja-cli`).
- `duckdb: command not found`: instala la CLI de DuckDB.
- `jq: command not found`: instala `jq`.
- `uuidgen: command not found`: instala `uuid-runtime` (Debian/Ubuntu) o `util-linux`.
- Permisos o rutas: asegúrate de que el directorio de salida existe y es escribible.
- Fichero de salida vacío: la plantilla primero crea un Parquet vacío; la segunda COPY lo sobrescribe con los datos reales.

## Notas y limitaciones

- El script de ingesta espera un flujo JSONL desde `bdns-fetch`. Verifica que tu versión soporte el endpoint y flags utilizados.
- Cambiar el conjunto de campos del esquema altera el `row_hash` y, por tanto, la desduplicación.
- Para grandes históricos, considera `bdns-repartition.sh` para equilibrar tamaños de fichero.

## Licencia

Los scripts bajo `bin/` incluyen cabeceras GPLv3. Si necesitas un fichero `LICENSE` en la raíz, añade uno acorde a tu modelo de distribución.

## Agradecimientos

- DuckDB por analítica local rápida
- Jinja por el templating sencillo de SQL
- `bdns-fetch` por recuperar datos de BDNS
