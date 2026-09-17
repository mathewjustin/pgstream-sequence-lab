# pgstream sequence lab

A small, reproducible lab for [xataio/pgstream issue #1203](https://github.com/xataio/pgstream/issues/1203). It demonstrates why an explicit PostgreSQL sequence can fall behind after CDC, and validates a candidate fix against the same workload.

For a concise explanation of pgstream's internal sequence handling, the catalog dependency that caused the bug, and how the fix works, see [`HOW_IT_WORKS.md`](HOW_IT_WORKS.md).

## What the lab proves

The schema deliberately uses `CREATE SEQUENCE` plus a direct `DEFAULT nextval(...)` without `OWNED BY`:

```sql
CREATE SEQUENCE lab.id_sequence;

CREATE TABLE lab.events (
    id bigint PRIMARY KEY DEFAULT nextval('lab.id_sequence'::regclass),
    payload text NOT NULL
);
```

The scenario has four phases:

1. Insert 1,000 rows at the source.
2. Snapshot source to target with `pg_dump`; both databases now have `max(id) = 1000` and sequence `last_value = 1000`.
3. Start `source PostgreSQL -> pgstream -> Kafka -> pgstream -> target PostgreSQL`, then insert 500 more source rows.
4. Stop CDC and perform a normal target-side insert that relies on the default sequence.

The baseline writer copies IDs 1001 through 1500 but does not recognize the explicit sequence dependency. The target sequence remains at 1000, so the first target-side insert generates 1001 and collides.

The fixed writer discovers the sequence through the column default's `pg_attrdef -> pg_depend -> pg_class` relationship. pgstream's existing DML adapter then emits `setval` alongside the replicated inserts. The target sequence reaches 1500, and the target-side insert safely receives 1501.

## Run it

Requirements: Docker with Compose and about 1.5 GB of free memory.

```bash
make baseline
make fixed
```

Run both validations in sequence:

```bash
make test
```

See [`RESULTS.md`](RESULTS.md) for the captured baseline and fixed outcomes from a complete local run.

The latest environment remains running for inspection:

```bash
docker compose exec target psql -U postgres -d lab
make logs
make clean
```

The source and target are also exposed at `localhost:55432` and `localhost:55433`. Both use database `lab`, user `postgres`, and password `postgres`.

## Expected results

| Variant | Source after CDC | Target after CDC | First target-side insert |
| --- | --- | --- | --- |
| Baseline `v1.3.1` | `max=1500, seq=1500` | `max=1500, seq=1000` | Duplicate key on `id=1001` |
| Patched `v1.3.1` | `max=1500, seq=1500` | `max=1500, seq=1500` | Succeeds with `id=1501` |

## Why the patch is narrow

The baseline catalog query only follows sequences with an automatic ownership dependency (`pg_depend.deptype = 'a'`). That covers `serial` and explicitly owned sequences, but not a standalone sequence merely referenced by a column default.

The patch in [`patches/1203-explicit-sequence.patch`](patches/1203-explicit-sequence.patch):

- recognizes canonical direct `nextval(sequence)` defaults;
- keeps identity-column support through its internal dependency;
- returns the sequence's actual schema, including cross-schema sequences;
- avoids treating derived expressions such as `nextval(sequence) + 100` as a sequence-backed column.

Both images are built from the same pgstream `v1.3.1` tag. The only difference is whether that patch is applied.
