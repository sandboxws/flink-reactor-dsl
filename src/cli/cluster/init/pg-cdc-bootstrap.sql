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

-- ── 2. Database-scoped grants (must run inside tpch) ───────────────
\c tpch

GRANT USAGE ON SCHEMA public TO flink_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO flink_cdc;

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
