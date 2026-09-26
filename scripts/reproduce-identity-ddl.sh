#!/usr/bin/env bash
set -Eeuo pipefail

# Separate project/volumes/ports: never resets the original sequence lab.
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export SOURCE_PORT=${IDENTITY_SOURCE_PORT:-55442}
export TARGET_PORT=${IDENTITY_TARGET_PORT:-55443}
COMPOSE=(docker compose --project-name pgstream-identity-ddl-lab --file "$ROOT_DIR/docker-compose.yml")

case "${1:-run}" in
  clean)
    "${COMPOSE[@]}" down --volumes --remove-orphans
    echo "Removed only the identity-DDL lab containers and volumes; its test data cannot be recovered."
    exit 0
    ;;
  run) ;;
  *) echo "Usage: bash scripts/reproduce-identity-ddl.sh [run|clean]" >&2; exit 2 ;;
esac

if [[ -n "$("${COMPOSE[@]}" ps -aq)" || -n "$(docker volume ls -q --filter label=com.docker.compose.project=pgstream-identity-ddl-lab)" ]]; then
  echo "Identity-DDL lab already exists; refusing to overwrite its data." >&2
  echo "To discard only that lab, run: bash scripts/reproduce-identity-ddl.sh clean" >&2
  exit 1
fi

sql() {
  "${COMPOSE[@]}" exec -T "$1" psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d lab -c "$2"
}

diagnostics() {
  echo "Reproduction stopped. Containers remain available for inspection." >&2
  "${COMPOSE[@]}" logs --tail 60 pg2kafka kafka2pg-fixed >&2 || true
}
trap 'trap - ERR; if [[ "$BASH_SUBSHELL" == 0 ]]; then diagnostics; fi' ERR

wait_for() {
  local service=$1 query=$2 expected=$3 description=$4 actual=""
  for ((attempt=0; attempt<120; attempt++)); do
    actual=$(sql "$service" "$query" 2>/dev/null || true)
    [[ "$actual" == "$expected" ]] && return 0
    sleep 1
  done
  echo "Timed out: $description; expected=$expected actual=${actual:-<empty>}" >&2
  return 1
}

expect() {
  if [[ "$1" != "$2" ]]; then
    echo "FAIL: $3; expected=$2 actual=$1" >&2
    return 1
  fi
}

state() {
  sql "$1" "SELECT id || ':' || (SELECT last_value FROM lab.identity_events_id_seq) FROM lab.identity_events WHERE payload = '$2';"
}

echo "==> Using the existing discovery patch, with no additional pgstream changes"
if [[ "${LAB_BUILD:-0}" == "1" ]] || ! docker image inspect \
  pgstream-sequence-lab-postgres:16.11 \
  pgstream-sequence-lab-pgstream:baseline \
  pgstream-sequence-lab-pgstream:fixed >/dev/null 2>&1; then
  "${COMPOSE[@]}" build --quiet source pg2kafka kafka2pg-fixed
fi

echo "==> Starting isolated identity-DDL lab on ports $SOURCE_PORT / $TARGET_PORT"
"${COMPOSE[@]}" up -d --wait source target kafka
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic pgstream-sequence-lab --partitions 1 --replication-factor 1 >/dev/null

echo "==> Creating an identity table and snapshotting 10 rows"
sql source "CREATE SCHEMA lab;
CREATE TABLE lab.identity_events (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payload text NOT NULL UNIQUE
);
INSERT INTO lab.identity_events (payload)
SELECT 'seed-' || value FROM generate_series(1, 10) AS value;" >/dev/null
"${COMPOSE[@]}" exec -T source pg_dump -U postgres --no-owner --no-privileges lab \
  | "${COMPOSE[@]}" exec -T target psql -Xq -v ON_ERROR_STOP=1 -U postgres -d lab >/dev/null
expect "$(state source seed-10)" "10:10" "source snapshot"
expect "$(state target seed-10)" "10:10" "target snapshot"

"${COMPOSE[@]}" up -d pg2kafka kafka2pg-fixed
wait_for source "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'pgstream_sequence_lab_slot' AND active;" 1 "active source replication slot"

echo "==> Before DDL: generate source ID 1001 to exercise lazy discovery"
sql source "SELECT setval('lab.identity_events_id_seq', 1000, true);
INSERT INTO lab.identity_events (payload) VALUES ('before-ddl');" >/dev/null
wait_for target "SELECT count(*) FROM lab.identity_events WHERE payload = 'before-ddl';" 1 "pre-DDL row"
wait_for target "SELECT last_value FROM lab.identity_events_id_seq;" 1001 "pre-DDL sequence update"
before_source=$(state source before-ddl)
before_target=$(state target before-ddl)
expect "$before_source" "1001:1001" "source before DDL"
expect "$before_target" "1001:1001" "patched writer preserves ID and sequence before DDL"

echo "==> Applying a real unrelated ALTER TABLE on the source"
sql source "ALTER TABLE lab.identity_events ADD COLUMN note text;"
wait_for target "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'lab' AND table_name = 'identity_events' AND column_name = 'note';" 1 "DDL applied on target"

echo "Identity metadata after ALTER (a = ALWAYS; no ordinary default):"
for service in source target; do
  metadata=$(sql "$service" "SELECT a.attidentity::text || ':' || COALESCE(pg_get_expr(d.adbin, d.adrelid), '<NULL>') FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE a.attrelid = 'lab.identity_events'::regclass AND a.attname = 'id';")
  expect "$metadata" "a:<NULL>" "$service identity metadata"
  printf '  %s: %s\n' "$service" "$metadata"
done

echo "==> After DDL: generate source ID 2001 (gap exposes accidental target generation)"
sql source "SELECT setval('lab.identity_events_id_seq', 2000, true);
INSERT INTO lab.identity_events (payload, note) VALUES ('after-ddl', 'DDL arrived');" >/dev/null
wait_for target "SELECT count(*) FROM lab.identity_events WHERE payload = 'after-ddl' AND note = 'DDL arrived';" 1 "post-DDL row"
# Graceful stop drains the writer before reading final sequence state.
"${COMPOSE[@]}" stop kafka2pg-fixed
after_source=$(state source after-ddl)
after_target=$(state target after-ddl)
expect "$after_source" "2001:2001" "source after DDL"
expect "$(sql target 'SELECT count(*) FROM lab.identity_events;')" 12 "target row count"
expect "$after_target" "1002:1002" "known identity-DDL bug signature (a corrected writer would give 2001:2001)"

echo "==> Restarting only the writer to test recovery with empty in-memory caches"
"${COMPOSE[@]}" up -d kafka2pg-fixed
sql source "SELECT setval('lab.identity_events_id_seq', 3000, true);
INSERT INTO lab.identity_events (payload, note) VALUES ('after-restart', 'fresh cache');" >/dev/null
wait_for target "SELECT count(*) FROM lab.identity_events WHERE payload = 'after-restart';" 1 "post-restart row"
"${COMPOSE[@]}" stop pg2kafka kafka2pg-fixed
restart_source=$(state source after-restart)
restart_target=$(state target after-restart)
expect "$restart_source" "3001:3001" "source after restart"
expect "$restart_target" "3001:3001" "fresh catalog lookup after restart"
expect "$(sql target 'SELECT count(*) FROM lab.identity_events;')" 13 "final target row count"
# Restart repairs future metadata use, not the already miscopied row.
expect "$(sql target "SELECT id FROM lab.identity_events WHERE payload = 'after-ddl';")" 1002 "previously miscopied row remains unchanged"

printf '\n%-24s %-18s %-18s\n' 'Phase (row ID:sequence)' Source Target
printf '%-24s %-18s %-18s\n' 'Before DDL' "$before_source" "$before_target"
printf '%-24s %-18s %-18s\n' 'After DDL' "$after_source" "$after_target"
printf '%-24s %-18s %-18s\n' 'After writer restart' "$restart_source" "$restart_target"
echo "PASS: identity-DDL regression reproduced with the existing discovery patch."
echo "This is an expected-bug reproduction, not a passing correctness test."
echo "The databases and Kafka remain available; CDC is stopped."
echo "Inspect: SOURCE_PORT=$SOURCE_PORT TARGET_PORT=$TARGET_PORT docker compose -p pgstream-identity-ddl-lab exec target psql -U postgres -d lab"
echo "Cleanup (deletes only this scenario's data): bash scripts/reproduce-identity-ddl.sh clean"
