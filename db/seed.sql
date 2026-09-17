\set ON_ERROR_STOP on

CREATE SCHEMA lab;

-- Intentionally not OWNED BY the table column. This is the form from issue #1203.
CREATE SEQUENCE lab.id_sequence;

CREATE TABLE lab.events (
    id bigint PRIMARY KEY DEFAULT nextval('lab.id_sequence'::regclass),
    payload text NOT NULL
);

INSERT INTO lab.events (payload)
SELECT 'snapshot-' || value
FROM generate_series(1, 1000) AS value;
