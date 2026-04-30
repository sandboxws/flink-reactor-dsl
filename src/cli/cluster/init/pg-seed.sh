#!/bin/bash
# Idempotent PostgreSQL sample database seeder.
# Runs as a Kubernetes Job — connects to the postgres Service over the network.
set -e

PGHOST="${PGHOST:-postgres}"
PGUSER="${PGUSER:-reactor}"
export PGHOST PGUSER PGPASSWORD="${PGPASSWORD:-reactor}"

DATABASES=("pagila" "chinook" "employees" "tpch")
SCHEMAS=("public" "public" "employees" "public")

for i in "${!DATABASES[@]}"; do
  db="${DATABASES[$i]}"
  schema="${SCHEMAS[$i]}"

  # tpch may ship gzipped (the SF=0.1 dump exceeds 50MB uncompressed)
  if [ "$db" = "tpch" ] && [ -f "/data/tpch.sql.gz" ]; then
    dump="/data/tpch.sql.gz"
  else
    dump="/data/${db}.sql"
  fi

  if [ ! -f "$dump" ]; then
    echo "SKIP ${db} — dump not found at ${dump}"
    continue
  fi

  # Ensure the database exists (idempotent).
  psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${db}'" 2>/dev/null \
    | grep -q 1 || psql -d postgres -c "CREATE DATABASE ${db}"

  count=$(psql -d "$db" -tAc \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema = '${schema}'" 2>/dev/null || echo "0")

  if [ "$count" -gt 0 ]; then
    echo "SKIP ${db} — already seeded (${count} tables)"
    continue
  fi

  echo "LOAD ${db}..."
  if [[ "$dump" == *.gz ]]; then
    gunzip -c "$dump" | psql -v ON_ERROR_STOP=1 -d "$db"
  else
    psql -v ON_ERROR_STOP=1 -d "$db" -f "$dump"
  fi
  echo "DONE ${db}"
done

# Apply CDC bootstrap (flink_cdc role + publication on tpch tables).
# Idempotent — guarded with IF NOT EXISTS / DROP+CREATE patterns.
if [ -f "/data/pg-cdc-bootstrap.sql" ]; then
  echo "Applying CDC bootstrap..."
  psql -v ON_ERROR_STOP=1 -d postgres -f /data/pg-cdc-bootstrap.sql
fi

echo "PostgreSQL seeding complete."
