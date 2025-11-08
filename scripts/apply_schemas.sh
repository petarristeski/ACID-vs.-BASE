#!/usr/bin/env bash
set -euo pipefail

# Defaults (override via env)
PG_CONTAINER=${PG_CONTAINER:-postgres16}
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_DB=${POSTGRES_DB:-testdb}

MONGO_CONTAINER=${MONGO_CONTAINER:-mongodb6}
MONGO_USER=${MONGO_USER:-root}
MONGO_PASSWORD=${MONGO_PASSWORD:-root}
MONGO_AUTH_DB=${MONGO_AUTH_DB:-admin}

CASSANDRA_CONTAINER=${CASSANDRA_CONTAINER:-cassandra41}

echo "Applying database schemas..."

wait_for_healthy() {
  local name=$1
  echo -n "Waiting for ${name} to be healthy"
  for i in {1..60}; do
    status=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}healthy{{end}}' "$name" 2>/dev/null || echo "missing")
    if [[ "$status" == "healthy" ]]; then
      echo " - ready"
      return 0
    fi
    if [[ "$status" == "missing" ]]; then
      echo "\nContainer '$name' not found. Is docker compose up?" >&2
      exit 1
    fi
    printf '.'
    sleep 2
  done
  echo "\nTimeout waiting for $name to be healthy" >&2
  exit 1
}

# Ensure services are up/healthy
wait_for_healthy "$PG_CONTAINER"
wait_for_healthy "$MONGO_CONTAINER"
wait_for_healthy "$CASSANDRA_CONTAINER"

# PostgreSQL
if ls databases/postgresql/*.sql >/dev/null 2>&1; then
  echo "\n[PostgreSQL] Applying SQL schemas to $POSTGRES_DB on $PG_CONTAINER"
  for f in databases/postgresql/*.sql; do
    echo "- $f"
    docker exec -i "$PG_CONTAINER" \
      psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -1 < "$f"
  done
else
  echo "[PostgreSQL] No .sql files found under databases/postgresql/"
fi

# MongoDB
if ls databases/mongodb/*.js >/dev/null 2>&1; then
  echo "\n[MongoDB] Applying JS schemas on $MONGO_CONTAINER"
  for f in databases/mongodb/*.js; do
    echo "- $f"
    docker exec -i "$MONGO_CONTAINER" \
      mongosh --username "$MONGO_USER" --password "$MONGO_PASSWORD" \
      --authenticationDatabase "$MONGO_AUTH_DB" --quiet < "$f"
  done
else
  echo "[MongoDB] No .js files found under databases/mongodb/"
fi

# Cassandra
if ls databases/cassandra/*.cql >/dev/null 2>&1; then
  echo "\n[Cassandra] Applying CQL schemas on $CASSANDRA_CONTAINER"
  for f in databases/cassandra/*.cql; do
    echo "- $f"
    docker exec -i "$CASSANDRA_CONTAINER" cqlsh < "$f"
  done
else
  echo "[Cassandra] No .cql files found under databases/cassandra/"
fi

echo "\nDone."

