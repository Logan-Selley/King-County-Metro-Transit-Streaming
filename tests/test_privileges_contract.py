"""Phase 5C contract: what each warehouse role can and cannot do, checked live.

terraform/core/access.tf is the prose half of this spec. This is the half that
runs: it connects as the admin role, and inside ONE transaction that is always
rolled back it becomes each client role in turn (SET LOCAL ROLE) and tries the
things that role must be able to do and the things it must not. Every attempt
sits in its own savepoint, so a permission error is recorded and the
transaction carries on. Nothing it does survives the test, including the
partition it creates and the view dbt_transform replaces.

Behaviour, not catalog lookups, wherever it matters. has_table_privilege()
would say whether a grant exists; it cannot say whether an upsert through a
partitioned parent into a partition created after the grant succeeds, which is
the question access.tf 3a-3c actually asks.

Needs the running warehouse on localhost:$WAREHOUSE_PORT and .env. Skips the
module when the warehouse does not answer, like the schema-compatibility suite,
so CI (which has no stack) reports it as skipped rather than passed. Marked
`wip` until 5C lands.
"""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

pytestmark = [pytest.mark.contract]

ROOT = Path(__file__).resolve().parents[1]

ROLES = ("connect_sink", "static_loader", "enrichment", "dbt_transform", "airflow_ops")

# Password variable per role, as .env names it. The login test uses these; the
# behaviour tests do not need them (SET ROLE from the admin connection).
PASSWORD_VARS = {r: f"{r.upper()}_PASSWORD" for r in ROLES}

SINK_TABLES = ("raw.enriched_vehicle_positions", "raw.bunching_alerts",
               "raw.prediction_accuracy")

# A day no real partition will ever cover, so the partition this creates is
# unambiguously the test's and rolls back with it.
PROBE_DAY = "2099-01-01"


def _dsn(user=None, password=None) -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass
    return (f"host=localhost port={os.environ.get('WAREHOUSE_PORT', '5434')} "
            f"dbname={os.environ.get('POSTGRES_DB', 'transit')} "
            f"user={user or os.environ.get('POSTGRES_USER', 'transit')} "
            f"password={password or os.environ.get('POSTGRES_PASSWORD', '')} "
            "connect_timeout=3")


@pytest.fixture(scope="module")
def admin():
    try:
        conn = psycopg.connect(_dsn())
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse not reachable: {exc}")
    yield conn
    conn.rollback()
    conn.close()


class Session:
    """One rolled-back transaction, with a per-attempt savepoint."""

    def __init__(self, conn):
        self.conn = conn

    def attempt(self, role: str, sql: str, params=None) -> str | None:
        """Run sql as role. None on success, else the SQLSTATE.

        EVERY ATTEMPT ROLLS BACK, success or failure, and that is not
        bookkeeping. The session is module-scoped, so a change one test makes is
        visible to the next test sharing the transaction. TestDbtTransform drops
        marts.mart_feed_health, which it may do now that 5C gave it ownership, and
        TestAirflowOps could then not read a table that no longer existed inside
        that same transaction. The suite failed on correct code and would have
        passed on broken code, because before 5C the drop was denied.

        Every assertion here asks whether a statement succeeded, so no effect is
        ever needed and rolling back is strictly more correct. It resets the role
        too, since SET LOCAL is transactional.
        """
        with self.conn.cursor() as cur:
            cur.execute("SAVEPOINT probe")
            try:
                cur.execute(f'SET LOCAL ROLE "{role}"')
                cur.execute(sql, params)
            except psycopg.Error as exc:
                cur.execute("ROLLBACK TO SAVEPOINT probe")
                return exc.sqlstate or "unknown"
            cur.execute("ROLLBACK TO SAVEPOINT probe")
            cur.execute("RELEASE SAVEPOINT probe")
            return None

    def admin(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else None


@contextmanager
def session(conn):
    conn.rollback()
    try:
        yield Session(conn)
    finally:
        conn.rollback()


def _roles_exist(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("select rolname from pg_roles where rolname = any(%s)", (list(ROLES),))
        return {r for (r,) in cur.fetchall()}


@pytest.fixture(scope="module")
def s(admin):
    missing = set(ROLES) - _roles_exist(admin)
    if missing:
        pytest.fail(f"roles not created yet: {sorted(missing)} (terraform/core/access.tf)")
    with session(admin) as sess:
        yield sess


def _upsert_sql(s: Session, table: str) -> str:
    """The statement shape the JDBC sink emits in insert.mode=upsert:
    INSERT ... ON CONFLICT (pk) DO UPDATE SET every non-key column =
    EXCLUDED.column. `WHERE false` means no row is written, and Postgres
    still checks every privilege the statement needs, at plan time."""
    schema, name = table.split(".")
    pk = [r[0] for r in s.admin(
        """select a.attname from pg_index i
           join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
           where i.indrelid = %s::regclass and i.indisprimary
           order by array_position(i.indkey, a.attnum)""", (table,))]
    cols = s.admin(
        """select column_name, udt_name from information_schema.columns
           where table_schema = %s and table_name = %s order by ordinal_position""",
        (schema, name))
    names = ", ".join(c for c, _ in cols)
    nulls = ", ".join(f"NULL::{t}" for _, t in cols)
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c, _ in cols if c not in pk)
    return (f"INSERT INTO {table} ({names}) SELECT {nulls} WHERE false "
            f"ON CONFLICT ({', '.join(pk)}) DO UPDATE SET {sets}")


# =============================================================================
# connect_sink: upserts into the three sink tables, nothing else
# =============================================================================

class TestConnectSink:
    @pytest.mark.parametrize("table", SINK_TABLES)
    def test_can_upsert(self, s, table):
        assert s.attempt("connect_sink", _upsert_sql(s, table)) is None

    def test_rows_reach_a_partition_created_after_the_grant(self, s):
        """access.tf 3b: transit_partitions creates tomorrow's partition every
        day, long after any GRANT ran. A real row, routed into a partition
        made a moment ago, rolled back with everything else."""
        s.admin("select raw.ensure_partition('raw.bunching_alerts', %s::date)", (PROBE_DAY,))
        assert s.attempt(
            "connect_sink",
            "INSERT INTO raw.bunching_alerts (vehicle_id_a, vehicle_id_b, window_end) "
            "VALUES ('probe-a', 'probe-b', %s::timestamptz + interval '12 hours') "
            "ON CONFLICT (vehicle_id_a, vehicle_id_b, window_end) "
            "DO UPDATE SET gap_ft = EXCLUDED.gap_ft",
            (PROBE_DAY,)) is None

    @pytest.mark.parametrize("sql", [
        "DROP TABLE raw.bunching_alerts",
        "TRUNCATE raw.enriched_vehicle_positions",
        "DELETE FROM raw.prediction_accuracy WHERE false",
        "SELECT 1 FROM static.trips LIMIT 1",
        "CREATE TABLE raw.probe (x int)",
        "SELECT 1 FROM marts.mart_feed_health LIMIT 1",
    ])
    def test_cannot(self, s, sql):
        assert s.attempt("connect_sink", sql) is not None, sql


# =============================================================================
# static_loader: writes static.*
# =============================================================================

class TestStaticLoader:
    @pytest.mark.parametrize("sql", [
        "INSERT INTO static.feed_version (version_id, feed_etag, loaded_at, ready) "
        "SELECT 0, 'x', now(), false WHERE false",
        "UPDATE static.feed_version SET ready = ready WHERE false",
        "DELETE FROM static.trips WHERE false",
        # static/load.py reloads the neighborhood layer with TRUNCATE, which
        # is its own privilege, separate from DELETE.
        "TRUNCATE static.neighborhoods",
    ])
    def test_can(self, s, sql):
        assert s.attempt("static_loader", sql) is None, sql

    @pytest.mark.parametrize("sql", [
        "DROP TABLE static.trips",
        "SELECT 1 FROM raw.enriched_vehicle_positions LIMIT 1",
        "INSERT INTO raw.bunching_alerts (vehicle_id_a, vehicle_id_b, window_end) "
        "SELECT 'a', 'b', now() WHERE false",
    ])
    def test_cannot(self, s, sql):
        assert s.attempt("static_loader", sql) is not None, sql


# =============================================================================
# enrichment: reads static.*
# =============================================================================

class TestEnrichment:
    @pytest.mark.parametrize("table", ["static.trips", "static.stop_times",
                                       "static.shape_lines", "static.neighborhoods",
                                       "static.feed_version"])
    def test_can_read(self, s, table):
        assert s.attempt("enrichment", f"SELECT 1 FROM {table} LIMIT 1") is None

    @pytest.mark.parametrize("sql", [
        "UPDATE static.feed_version SET ready = ready WHERE false",
        "SELECT 1 FROM raw.enriched_vehicle_positions LIMIT 1",
        "SELECT raw.ensure_partition('raw.bunching_alerts', '2099-01-03')",
    ])
    def test_cannot(self, s, sql):
        assert s.attempt("enrichment", sql) is not None, sql


# =============================================================================
# dbt_transform: reads raw and static, owns staging and marts
# =============================================================================

class TestDbtTransform:
    @pytest.mark.parametrize("table", [*SINK_TABLES, "static.routes"])
    def test_can_read_sources(self, s, table):
        """Including source freshness, which reads raw.* too (access.tf 3e)."""
        assert s.attempt("dbt_transform", f"SELECT 1 FROM {table} LIMIT 1") is None

    def test_can_replace_an_existing_model(self, s):
        """access.tf 3d: the existing views were created by `transit`, and
        CREATE OR REPLACE on a view you do not own fails."""
        assert s.attempt(
            "dbt_transform",
            "CREATE OR REPLACE VIEW staging.stg_bunching_alerts AS "
            "SELECT * FROM staging.stg_bunching_alerts") is None

    def test_can_rebuild_a_mart_table(self, s):
        assert s.attempt("dbt_transform",
                         "DROP TABLE marts.mart_feed_health") is None

    @pytest.mark.parametrize("sql", [
        "DROP TABLE raw.enriched_vehicle_positions",
        "INSERT INTO raw.bunching_alerts (vehicle_id_a, vehicle_id_b, window_end) "
        "SELECT 'a', 'b', now() WHERE false",
        "UPDATE static.feed_version SET ready = ready WHERE false",
    ])
    def test_cannot(self, s, sql):
        assert s.attempt("dbt_transform", sql) is not None, sql


# =============================================================================
# airflow_ops: partition maintenance and reading marts
# =============================================================================

class TestAirflowOps:
    def test_can_create_a_partition(self, s):
        """access.tf 3c: CREATE TABLE ... PARTITION OF needs ownership of the
        parent, so this only works through a SECURITY DEFINER function."""
        assert s.attempt(
            "airflow_ops",
            "SELECT raw.ensure_partition('raw.prediction_accuracy', '2099-01-02')") is None

    def test_can_drop_old_partitions(self, s):
        assert s.attempt(
            "airflow_ops",
            "SELECT raw.drop_partitions_before('raw.bunching_alerts', '1999-01-01')") is None

    def test_can_read_the_feed_health_mart(self, s):
        assert s.attempt("airflow_ops",
                         "SELECT 1 FROM marts.mart_feed_health LIMIT 1") is None

    @pytest.mark.parametrize("sql", [
        "DROP TABLE raw.bunching_alerts_default",
        "CREATE TABLE raw.probe (x int)",
        "INSERT INTO raw.bunching_alerts (vehicle_id_a, vehicle_id_b, window_end) "
        "SELECT 'a', 'b', now() WHERE false",
        "SELECT 1 FROM raw.enriched_vehicle_positions LIMIT 1",
    ])
    def test_cannot(self, s, sql):
        assert s.attempt("airflow_ops", sql) is not None, sql


class TestMaintenanceFunctions:
    @pytest.mark.parametrize("fn", ["ensure_partition", "drop_partitions_before"])
    def test_run_as_their_owner_with_a_pinned_search_path(self, s, fn):
        """SECURITY DEFINER without SET search_path lets a caller who can
        create objects on the path shadow what the function resolves, and run
        it with the owner's rights."""
        (secdef, config), = s.admin(
            "select prosecdef, proconfig from pg_proc "
            "where pronamespace = 'raw'::regnamespace and proname = %s", (fn,))
        assert secdef, f"raw.{fn} is not SECURITY DEFINER"
        assert any(c.startswith("search_path=") for c in (config or [])), (
            f"raw.{fn} has no pinned search_path")

    @pytest.mark.parametrize("fn", ["ensure_partition(text,date)",
                                    "drop_partitions_before(text,date)"])
    def test_public_cannot_execute(self, s, fn):
        """Every new function is executable by PUBLIC unless revoked."""
        (ok,), = s.admin(
            "select has_function_privilege('public', %s, 'EXECUTE')", (f"raw.{fn}",))
        assert not ok


# =============================================================================
# Logins, and nothing left on the superuser
# =============================================================================

@pytest.mark.parametrize("role", ROLES)
def test_role_logs_in_with_its_env_password(admin, role):
    var = PASSWORD_VARS[role]
    password = os.environ.get(var)
    if not password:
        pytest.fail(f"{var} not set in .env")
    with psycopg.connect(_dsn(role, password)) as conn:
        assert conn.execute("select current_user").fetchone()[0] == role


# Static: no client config still logs in as the superuser (access.tf, 5).
# These need no warehouse, but live here with the rest of 5C.

def test_connectors_do_not_log_in_as_the_superuser():
    for f in (ROOT / "connect").glob("*.json"):
        assert "POSTGRES_USER" not in f.read_text(), f.name


def test_dbt_does_not_log_in_as_the_superuser():
    assert "POSTGRES_USER" not in (ROOT / "dbt" / "profiles.yml").read_text()


def test_airflow_does_not_log_in_as_the_superuser():
    text = (ROOT / "docker-compose.airflow.yml").read_text()
    conn = re.search(r"AIRFLOW_CONN_WAREHOUSE:\s*(\S+)", text).group(1)
    assert "POSTGRES_USER" not in conn and "POSTGRES_PASSWORD" not in conn


@pytest.mark.parametrize("dag", ["transit_dbt.py", "transit_static_refresh.py"])
def test_dags_hand_their_containers_a_role_not_the_superuser(dag):
    src = (ROOT / "airflow" / "dags" / dag).read_text()
    assert 'os.environ["POSTGRES_USER"]' not in src, dag
