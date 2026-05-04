-- Postgres CDC bootstrap for the bundled tpch database.
-- Idempotent: safe to re-run on an already-bootstrapped postgres.
-- Invoked by 00-init-databases.sh after tpch.sql has loaded so
-- CREATE PUBLICATION FOR TABLE resolves the named relations.
--
-- Wired up by OpenSpec change 53-prepare-postgres-for-cdc.

-- ── 1. flink_cdc role ──────────────────────────────────────────────
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'flink_cdc') THEN
    CREATE ROLE flink_cdc WITH REPLICATION LOGIN PASSWORD 'flink_cdc';
  END IF;
END $$;

GRANT CONNECT ON DATABASE tpch TO flink_cdc;

-- CREATE on database is required by Flink CDC 3.6's pipeline-connector-postgres,
-- which auto-creates a publication (the 3.6 release dropped the
-- `publication.name` source option, so we can't point it at the
-- pre-created flink_cdc_pub below). Without this grant the source enumerator
-- crashes during slot creation with `ERROR: permission denied for database tpch`.
GRANT CREATE ON DATABASE tpch TO flink_cdc;

-- Debezium auto-creates the publication in `filtered` mode (FOR TABLE
-- <captured>), and CREATE PUBLICATION FOR TABLE requires the role to own
-- the tables. The seed databases load with `reactor` as the table owner,
-- so granting flink_cdc membership in reactor is the least-invasive way
-- to keep ownership transitive without rewriting every dump's ownership
-- on import. Membership lets Debezium issue CREATE PUBLICATION as if it
-- were the table owner. Acceptable in this dev cluster because reactor
-- is itself a non-superuser app role.
GRANT reactor TO flink_cdc;

-- ── 2. Database-scoped grants (must run inside tpch) ───────────────
\c tpch

GRANT USAGE ON SCHEMA public TO flink_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flink_cdc;

-- The connector also publishes Debezium-managed publication metadata; that
-- requires owning the tables (or a role to which the table owner has been
-- granted). Re-owning would conflict with the seed loader so we instead
-- grant ownership of the public schema's heartbeat-style helpers via
-- table-level GRANT — see CREATE PUBLICATION below; this user becomes the
-- publication owner because it created it.

-- ── 3. Publication ─────────────────────────────────────────────────
-- DROP IF EXISTS + CREATE rather than IF NOT EXISTS so the column
-- list stays in sync if tasks ever extend the publication.
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_publication WHERE pubname = 'flink_cdc_pub') THEN
    DROP PUBLICATION flink_cdc_pub;
  END IF;
  CREATE PUBLICATION flink_cdc_pub FOR TABLE orders, lineitem, customer;
EXCEPTION
  WHEN undefined_table THEN
    RAISE NOTICE 'Skipping flink_cdc_pub: tpch tables not yet loaded';
END $$;

-- ── 4. REPLICA IDENTITY FULL on CDC tables ─────────────────────────
-- Required so UPDATE/DELETE emit before-images in the logical
-- replication stream. Without it, the changelog is incomplete and
-- retract semantics downstream produce ambiguous events.
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders') THEN
    EXECUTE 'ALTER TABLE public.orders REPLICA IDENTITY FULL';
  END IF;
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'lineitem') THEN
    EXECUTE 'ALTER TABLE public.lineitem REPLICA IDENTITY FULL';
  END IF;
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'customer') THEN
    EXECUTE 'ALTER TABLE public.customer REPLICA IDENTITY FULL';
  END IF;
END $$;
