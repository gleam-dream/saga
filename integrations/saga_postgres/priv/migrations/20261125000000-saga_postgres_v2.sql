--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('saga-postgres-migrate:' || current_schema(), 0))) AS l;

ALTER TABLE saga_executions DROP COLUMN data;

CREATE TABLE saga_records (execution_id text NOT NULL REFERENCES saga_executions (id) ON DELETE CASCADE, key text NOT NULL, data bytea NOT NULL, PRIMARY KEY (execution_id, key));

DROP INDEX saga_executions_unfinished;

CREATE INDEX saga_executions_listing ON saga_executions (phase, id COLLATE "C");

INSERT INTO saga_schema_migrations (version) VALUES (2);

--- migration:down

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('saga-postgres-migrate:' || current_schema(), 0))) AS l;

DROP INDEX saga_executions_listing;

CREATE INDEX saga_executions_unfinished ON saga_executions (created_at, id) WHERE phase <> 'finished';

DROP TABLE saga_records;

ALTER TABLE saga_executions ADD COLUMN data bytea NOT NULL DEFAULT ''::bytea;

DELETE FROM saga_schema_migrations WHERE version = 2;

--- migration:end
