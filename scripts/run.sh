#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE=(docker compose --project-name pgstream-sequence-lab --file "$ROOT_DIR/docker-compose.yml")
VARIANT=${1:-}

if [[ "$VARIANT" != "baseline" && "$VARIANT" != "fixed" ]]; then
  echo "Usage: $0 baseline|fixed" >&2
  exit 2
fi

writer_service="kafka2pg-$VARIANT"

sql_value() {
  local service=$1
  local sql=$2
  "${COMPOSE[@]}" exec -T "$service" psql -XAtq -U postgres -d lab -c "$sql"
}

wait_for_value() {
  local service=$1
  local sql=$2
  local expected=$3
  local description=$4
  local actual=""

  for _ in $(seq 1 120); do
    actual=$(sql_value "$service" "$sql" 2>/dev/null || true)
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for $description; last value: ${actual:-<empty>}" >&2
  "${COMPOSE[@]}" logs --tail 120 pg2kafka "$writer_service" >&2
  return 1
}

echo "==> Resetting the lab"
"${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true

echo "==> Building PostgreSQL and pgstream ($VARIANT writer)"
"${COMPOSE[@]}" build source pg2kafka "$writer_service"

echo "==> Starting two PostgreSQL databases and Kafka"
"${COMPOSE[@]}" up -d --wait source target kafka

echo "==> Creating the one-partition Kafka topic"
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic pgstream-sequence-lab \
  --partitions 1 \
  --replication-factor 1 >/dev/null

echo "==> Seeding 1,000 source rows with an explicit CREATE SEQUENCE default"
"${COMPOSE[@]}" exec -T source psql -X -U postgres -d lab < "$ROOT_DIR/db/seed.sql" >/dev/null

echo "==> Taking the initial snapshot with pg_dump"
"${COMPOSE[@]}" exec -T source pg_dump -U postgres --no-owner --no-privileges lab \
  | "${COMPOSE[@]}" exec -T target psql -X -v ON_ERROR_STOP=1 -U postgres -d lab >/dev/null

source_snapshot=$(sql_value source "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")
target_snapshot=$(sql_value target "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")
if [[ "$source_snapshot" != "1000:1000" || "$target_snapshot" != "1000:1000" ]]; then
  echo "Snapshot invariant failed: source=$source_snapshot target=$target_snapshot" >&2
  exit 1
fi

echo "==> Starting source -> Kafka -> target CDC"
"${COMPOSE[@]}" up -d pg2kafka "$writer_service"
wait_for_value source "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'pgstream_sequence_lab_slot';" "1" "the pgstream replication slot"

echo "==> Inserting 500 additional rows at the source"
sql_value source "INSERT INTO lab.events (payload) SELECT 'cdc-' || value FROM generate_series(1, 500) AS value;" >/dev/null

wait_for_value target "SELECT count(*) FROM lab.events;" "1500" "1,500 rows at the target"

echo "==> Stopping CDC and testing a target-side default insert"
"${COMPOSE[@]}" stop pg2kafka "$writer_service" >/dev/null

source_state=$(sql_value source "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")
target_state=$(sql_value target "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")

set +e
cutover_output=$("${COMPOSE[@]}" exec -T target psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d lab \
  -c "INSERT INTO lab.events (payload) VALUES ('target-cutover') RETURNING id;" 2>&1)
cutover_status=$?
set -e

echo
echo "Variant:                 $VARIANT"
echo "After snapshot (max:seq): source=$source_snapshot target=$target_snapshot"
echo "After CDC (max:seq):      source=$source_state target=$target_state"

if [[ "$VARIANT" == "baseline" ]]; then
  if [[ "$source_state" != "1500:1500" || "$target_state" != "1500:1000" ]]; then
    echo "FAIL: baseline did not reproduce the sequence lag" >&2
    exit 1
  fi
  if [[ $cutover_status -eq 0 || "$cutover_output" != *"duplicate key value violates unique constraint"* ]]; then
    echo "FAIL: expected the target default insert to collide on id 1001" >&2
    echo "$cutover_output" >&2
    exit 1
  fi
  echo "Target default insert:    expected duplicate-key failure on id 1001"
  echo "PASS: issue #1203 reproduced"
else
  if [[ "$source_state" != "1500:1500" || "$target_state" != "1500:1500" ]]; then
    echo "FAIL: fixed writer did not synchronize the sequence" >&2
    exit 1
  fi
  if [[ $cutover_status -ne 0 || "$cutover_output" != "1501" ]]; then
    echo "FAIL: expected the target default insert to allocate id 1501" >&2
    echo "$cutover_output" >&2
    exit 1
  fi
  echo "Target default insert:    succeeded with id $cutover_output"
  echo "PASS: patch prevents the cutover collision"
fi

echo
echo "The containers are still available for inspection. Run: make clean"
