#!/bin/bash
# Creates pagila, chinook, employees, and tpch databases and loads their
# SQL data. Runs inside the PostgreSQL container during first-time
# initialization. After tpch loads, applies pg-cdc-bootstrap.sql so the
# flink_cdc role + publication are ready for Postgres CDC consumers.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE pagila;
  CREATE DATABASE chinook;
  CREATE DATABASE employees;
  CREATE DATABASE tpch;
EOSQL

echo "Loading pagila dataset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d pagila < /data/pagila.sql

echo "Loading chinook dataset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d chinook < /data/chinook.sql

echo "Loading employees dataset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d employees < /data/employees.sql

if [ -f /data/tpch.sql.gz ]; then
  echo "Loading tpch dataset (gzipped)..."
  gunzip -c /data/tpch.sql.gz | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d tpch
elif [ -f /data/tpch.sql ]; then
  echo "Loading tpch dataset..."
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d tpch -f /data/tpch.sql
else
  echo "SKIP tpch — no dump found at /data/tpch.sql[.gz]"
fi

if [ -f /data/pg-cdc-bootstrap.sql ]; then
  echo "Applying CDC bootstrap (flink_cdc role + publication)..."
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB" -f /data/pg-cdc-bootstrap.sql
fi

echo "Sample databases ready."
