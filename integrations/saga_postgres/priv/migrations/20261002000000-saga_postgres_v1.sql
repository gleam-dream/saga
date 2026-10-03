--- migration:up

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('saga-postgres-migrate:' || current_schema(), 0))) AS l;

CREATE TABLE saga_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp());

CREATE TABLE saga_executions (id text PRIMARY KEY, revision bigint NOT NULL CHECK (revision >= 0), generation bigint NOT NULL CHECK (generation >= 0), cancelled boolean NOT NULL DEFAULT false, phase text NOT NULL DEFAULT 'pending' CHECK (phase IN ('pending', 'suspended', 'finished')), data bytea NOT NULL, owner_token text, lease_until timestamptz, created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), CHECK ((owner_token IS NULL) = (lease_until IS NULL)));

CREATE INDEX saga_executions_unfinished ON saga_executions (created_at, id) WHERE phase <> 'finished';

INSERT INTO saga_schema_migrations (version) VALUES (1);

--- migration:down

SELECT true FROM (SELECT pg_advisory_xact_lock(hashtextextended('saga-postgres-migrate:' || current_schema(), 0))) AS l;

DROP TABLE saga_executions;

DROP TABLE saga_schema_migrations;

--- migration:end
