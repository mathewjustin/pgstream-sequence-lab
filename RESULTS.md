# Validated results

Validated locally on 2026-09-17 with Docker Compose v5.3.1.

## Baseline

Command: `./scripts/run.sh baseline`

```text
Variant:                 baseline
After snapshot (max:seq): source=1000:1000 target=1000:1000
After CDC (max:seq):      source=1500:1500 target=1500:1000
Target default insert:    expected duplicate-key failure on id 1001
PASS: issue #1203 reproduced
```

## Fixed

Command: `./scripts/run.sh fixed`

```text
Variant:                 fixed
After snapshot (max:seq): source=1000:1000 target=1000:1000
After CDC (max:seq):      source=1500:1500 target=1500:1500
Target default insert:    succeeded with id 1501
PASS: patch prevents the cutover collision
```

The patched `pkg/wal/processor/postgres` package also passes its focused Go test suite against pgstream `v1.3.1`.
