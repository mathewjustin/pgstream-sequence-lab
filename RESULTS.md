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

## Identity-DDL regression with the existing patch

Validated locally on 2026-09-27 using the existing lab PostgreSQL 16.11,
Kafka 4.3.0, baseline reader, and `v1.3.1` writer with the bundled discovery
patch. No production code was changed for this reproduction.

Command: `bash scripts/reproduce-identity-ddl.sh`

```text
Identity metadata after ALTER (a = ALWAYS; no ordinary default):
  source: a:<NULL>
  target: a:<NULL>

Phase (row ID:sequence)  Source             Target
Before DDL               1001:1001          1001:1001
After DDL                2001:2001          1002:1002
After writer restart     3001:3001          3001:3001
PASS: identity-DDL regression reproduced with the existing discovery patch.
```

The script also verified 12 target rows after DDL and 13 after writer restart,
the replicated `note` value, and the fact that the earlier incorrect ID 1002
remained unchanged after restart. Equal row counts therefore did not establish
correct replication. The expected correct ID for the `after-ddl` row is 2001.

`PASS` means the existing regression was reproduced. It is not a correctness
pass, and a writer that fixes the bug should fail this expected-bug assertion.
This scenario complements the original explicit-unowned-sequence test rather
than replacing it.
