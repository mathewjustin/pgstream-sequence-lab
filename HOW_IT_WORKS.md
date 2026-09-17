# How pgstream handles PostgreSQL sequences

## The short version

A PostgreSQL CDC event contains the finished row value, such as `id = 1001`. It does not say that PostgreSQL produced the value by calling `nextval('lab.id_sequence')`.

When pgstream writes that event to the target, it inserts the ID explicitly:

```sql
INSERT INTO lab.events (id, payload) VALUES (1001, 'cdc-1');
```

An explicit ID does not invoke the column default, so PostgreSQL does not advance the target sequence. pgstream therefore has separate logic that discovers sequence-backed columns and follows an insert with:

```sql
SELECT setval('lab.id_sequence', 1001, true);
```

The bug was in sequence discovery. The `setval` behavior already existed.

## The complete flow

```text
Source INSERT
    |
    | PostgreSQL chooses id=1001 using nextval(...)
    v
Logical WAL event: { id: 1001, payload: "cdc-1" }
    |
    v
pgstream -> Kafka -> pgstream
    |
    | target schema observer maps id -> lab.id_sequence
    v
Target INSERT with explicit id=1001
    |
    v
Target setval('lab.id_sequence', 1001, true)
```

For a batch of inserts, pgstream uses the largest observed value and emits one `setval` per sequence.

## Why the snapshot worked

`pg_dump` handles sequence state separately from table rows. After dumping the first 1,000 rows, it restores the equivalent of:

```sql
SELECT pg_catalog.setval('lab.id_sequence', 1000, true);
```

That is why both databases begin the CDC phase with:

```text
max(id) = 1000
sequence last_value = 1000
```

The later CDC inserts carry IDs 1001 through 1500, but those explicit IDs alone cannot move the target sequence.

## Why the original discovery query missed this sequence

PostgreSQL records object relationships in `pg_depend`. The important detail is that these two schema styles create different relationships.

For `serial`, or a sequence declared `OWNED BY` a column:

```text
sequence -- automatic dependency ('a') --> table column
```

For a standalone sequence referenced only by a default:

```text
column default (pg_attrdef) -- normal dependency ('n') --> sequence
```

The original pgstream query only followed the first relationship:

```sql
d.refobjid = table_oid
AND d.refobjsubid = column_number
AND d.deptype = 'a'
```

Our test schema uses the second form:

```sql
CREATE SEQUENCE lab.id_sequence;

CREATE TABLE lab.events (
    id bigint DEFAULT nextval('lab.id_sequence'::regclass)
);
```

The sequence is referenced by the default but is not owned by the column. The old query therefore returned no sequence mapping for `id`. Without that mapping, pgstream generated the target `INSERT` but skipped its existing `setval` query.

The result after replicating 500 rows was:

```text
target max(id) = 1500
target sequence last_value = 1000
```

The next normal target insert called `nextval`, received 1001, and collided with the already replicated row.

## What the fix changes

The patch changes only the catalog lookup in pgstream's PostgreSQL schema observer.

It now:

1. Finds each table column and its `pg_attrdef` default.
2. Follows dependencies from that default to referenced sequence objects.
3. Confirms that the default is exactly a direct `nextval(sequence)` expression.
4. Returns the sequence's real schema and name.
5. Keeps identity columns working through their internal dependency (`deptype = 'i'`).

Once the observer returns this mapping:

```text
"id" -> "lab"."id_sequence"
```

the rest of pgstream is unchanged. Its existing DML adapter sees the inserted `id`, emits `setval`, and the target finishes at sequence value 1500. A target-side insert then safely receives 1501.

## Why the expression check matters

It would be unsafe to treat every default that mentions a sequence as a plain sequence-generated ID. For example:

```sql
DEFAULT nextval('lab.id_sequence') + 100
```

Here the inserted column value and the underlying sequence value differ by 100. Calling `setval(sequence, inserted_id)` would be incorrect.

The fix accepts the canonical direct `nextval(sequence)` form and rejects derived expressions like this one.

## Why pgstream cannot simply compare every primary key

- A primary key can be a UUID, text, composite key, or manually assigned number.
- A sequence-backed column does not have to be the primary key.
- One sequence can live in a different schema from its table.
- Querying PostgreSQL catalogs for every row would add unnecessary work.

pgstream instead discovers and caches the table's sequence-column mapping, then applies it while building target insert queries.

## Relevant pgstream code

- Schema discovery: [`postgres_schema_observer.go`](https://github.com/xataio/pgstream/blob/v1.3.1/pkg/wal/processor/postgres/postgres_schema_observer.go)
- Insert and `setval` generation: [`postgres_wal_dml_adapter.go`](https://github.com/xataio/pgstream/blob/v1.3.1/pkg/wal/processor/postgres/postgres_wal_dml_adapter.go)
- Batched insert handling: [`postgres_wal_dml_adapter_bulk.go`](https://github.com/xataio/pgstream/blob/v1.3.1/pkg/wal/processor/postgres/postgres_wal_dml_adapter_bulk.go)
- Candidate fix used by this lab: [`patches/1203-explicit-sequence.patch`](patches/1203-explicit-sequence.patch)
