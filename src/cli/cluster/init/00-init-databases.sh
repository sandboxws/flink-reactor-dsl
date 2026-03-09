#!/bin/bash
# Creates pagila, chinook, and employees databases and loads their SQL data.
# Runs inside the PostgreSQL container during first-time initialization.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
  CREATE DATABASE pagila;
  CREATE DATABASE chinook;
  CREATE DATABASE employees;
EOSQL

echo "Loading pagila dataset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d pagila < /data/pagila.sql

echo "Loading chinook dataset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d chinook < /data/chinook.sql

echo "Loading employees dataset..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d employees < /data/employees.sql

echo "Sample databases ready."
