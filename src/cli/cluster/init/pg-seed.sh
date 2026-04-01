#!/bin/bash
# Idempotent PostgreSQL sample database seeder.
# Runs as a Kubernetes Job — connects to the postgres Service over the network.
set -e

PGHOST="${PGHOST:-postgres}"
PGUSER="${PGUSER:-reactor}"
export PGHOST PGUSER PGPASSWORD="${PGPASSWORD:-reactor}"

DATABASES=("pagila" "chinook" "employees")
SCHEMAS=("public" "public" "employees")

for i in "${!DATABASES[@]}"; do
  db="${DATABASES[$i]}"
  schema="${SCHEMAS[$i]}"
  dump="/data/${db}.sql"

  if [ ! -f "$dump" ]; then
    echo "SKIP ${db} — dump not found"
    continue
  fi

  count=$(psql -d "$db" -tAc \
    "SELECT count(*) FROM information_schema.tables WHERE table_schema = '${schema}'" 2>/dev/null || echo "0")

  if [ "$count" -gt 0 ]; then
    echo "SKIP ${db} — already seeded (${count} tables)"
    continue
  fi

  echo "LOAD ${db}..."
  psql -v ON_ERROR_STOP=1 -d "$db" -f "$dump"
  echo "DONE ${db}"
done

echo "PostgreSQL seeding complete."
