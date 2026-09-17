#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MODE=${1:-baseline}

run_demo() {
  local variant=$1

  echo
  echo "============================================================"
  echo "pgstream sequence demo: $variant"
  echo "============================================================"
  echo

  LAB_QUIET=1 SHOW_KAFKA_EVENT=1 "$ROOT_DIR/scripts/run.sh" "$variant"
}

case "$MODE" in
  baseline)
    echo "This run reproduces issue #1203 and prints the real duplicate-key error."
    run_demo baseline
    ;;
  fixed)
    echo "This run applies the candidate fix and proves the target can allocate id 1501."
    run_demo fixed
    ;;
  both)
    echo "This runs the broken and fixed variants with the same data."
    run_demo baseline
    run_demo fixed
    ;;
  *)
    echo "Usage: $0 [baseline|fixed|both]" >&2
    exit 2
    ;;
esac
