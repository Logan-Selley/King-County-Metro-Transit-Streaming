-- ============================================================================
-- 07: schema ownership for dbt  (build step 5C)
-- ============================================================================
-- WHY THIS IS SQL AND NOT TERRAFORM. access.tf owns roles and grants, because
-- those describe roles. This is a MIGRATION: it changes which role owns two
-- schemas that already exist and already hold objects, and a migration has an
-- order and a history that a converge does not (ADR 0009).
--
-- THE PROBLEM. dbt_transform has to be able to replace what it builds. Every view
-- in staging and every table in marts was created by `transit`, because that is
-- the role dbt ran as, and CREATE OR REPLACE on an object you do not own fails
-- with "must be owner of view stg_bunching_alerts". Granting CREATE on the schema
-- is not enough, and neither is ALTER SCHEMA ... OWNER TO: ownership is a property
-- of each view and table, so the ALTER route means reassigning every one of them,
-- and remembering to reassign whatever gets created next.
--
-- THE CHOICE: transfer ownership, in place. Two things decided it, both measured
-- on 2026-09-24.
--
-- The first is that the obvious alternative, DROP SCHEMA ... CASCADE and let dbt
-- recreate everything, destroys the GRANTS attached to those schemas. A migrate
-- that wipes postgresql_grant.airflow_ops_marts shows up afterwards as permanent
-- Terraform drift, and it makes the order of `tf-apply` and `make migrate`
-- load-bearing in a way nothing would report. Reassigning ownership leaves grants
-- where they are, because an ACL survives ALTER ... OWNER TO.
--
-- The second is the outage. Dropping the marts empties four tables that the
-- stall check reads hourly, so every migrate would have to be followed by a dbt
-- build before the next :45. ALTER is instant and offline.
--
-- Reassigning the OBJECTS matters as much as the schemas: `create or replace`
-- checks ownership of the view or table being replaced, not of the schema it
-- lives in. The loop below covers tables, partitioned tables, views,
-- materialized views, sequences and foreign tables, and skips anything already
-- owned by dbt_transform, so a second migrate does nothing.
--
-- IDEMPOTENT, which matters because `make migrate` re-applies every file in this
-- directory on every run. The other guard is the role itself: during first-time
-- initdb the postgres entrypoint runs every file in this directory before
-- Terraform has created a single role, and an unguarded ALTER would fail there and
-- break the clean-clone path that 5E exists to prove.

DO $$
DECLARE
    target_schema text;
    obj           record;
    stmt          text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbt_transform') THEN
        RAISE NOTICE 'dbt_transform does not exist yet: run `make tf-apply R=core` then `make migrate`';
        RETURN;
    END IF;

    FOREACH target_schema IN ARRAY ARRAY['staging', 'marts'] LOOP
        EXECUTE format('ALTER SCHEMA %I OWNER TO dbt_transform', target_schema);

        FOR obj IN
            SELECT c.relname, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = target_schema
              AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
              AND pg_get_userbyid(c.relowner) <> 'dbt_transform'
            ORDER BY c.relname
        LOOP
            stmt := format('ALTER %s %I.%I OWNER TO dbt_transform',
                           CASE obj.relkind
                               WHEN 'v' THEN 'VIEW'
                               WHEN 'm' THEN 'MATERIALIZED VIEW'
                               WHEN 'S' THEN 'SEQUENCE'
                               WHEN 'f' THEN 'FOREIGN TABLE'
                               ELSE 'TABLE'
                           END,
                           target_schema, obj.relname);
            EXECUTE stmt;
            RAISE NOTICE '%', stmt;
        END LOOP;
    END LOOP;
END $$;