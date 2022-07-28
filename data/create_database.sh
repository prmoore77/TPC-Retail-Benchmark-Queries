#!/bin/bash

# This script assumes you have duckdb (with all extensions) installed and it is on your system PATH
# You should run ./get_data.sh first if you do not have the source parquet data on the local filesystem...

set -e

TPCH_SCALE_FACTOR=${1:?You MUST provide the TPC-H Scale Factor!}

echo "TPCH_SCALE_FACTOR=${TPCH_SCALE_FACTOR}"

DATABASE_FILE="./tpch_${TPCH_SCALE_FACTOR}.duckdb"

echo -e "(Re)creating database file: ${DATABASE_FILE}"

rm -f "${DATABASE_FILE}"

duckdb "${DATABASE_FILE}" << EOF
.bail on
.echo on
CREATE OR REPLACE TABLE lineitem AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/lineitem/*');
CREATE OR REPLACE TABLE customer AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/customer/*');
CREATE OR REPLACE TABLE nation AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/nation/*');
CREATE OR REPLACE TABLE orders AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/orders/*');
CREATE OR REPLACE TABLE part AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/part/*');
CREATE OR REPLACE TABLE partsupp AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/partsupp/*');
CREATE OR REPLACE TABLE region AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/region/*');
CREATE OR REPLACE TABLE supplier AS SELECT * FROM read_parquet('${TPCH_SCALE_FACTOR}/supplier/*');
EOF

echo "All done."
