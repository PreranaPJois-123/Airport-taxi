"""SQLite storage for everything the app creates. Survives restarts (M5, state rule)."""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime

from .config import DB_PATH

# DATABASE_URL (e.g. a Neon connection string) switches storage from SQLite to PostgreSQL.
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS ride_requests (
    request_id     TEXT PRIMARY KEY,
    traveler_name  TEXT NOT NULL,
    flight_id      TEXT NOT NULL,
    hotel_id       TEXT NOT NULL,
    adults         INTEGER NOT NULL,
    child_ages     TEXT NOT NULL,              -- JSON list of ints
    large_bags     INTEGER NOT NULL,
    small_bags     INTEGER NOT NULL,
    pickup         TEXT NOT NULL,              -- local time of arrival airport, YYYY-MM-DDTHH:MM
    car_type       TEXT,                       -- NULL = any type (G2)
    price_inr      INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'OPEN',   -- OPEN | BOOKED | CANCELLED
    source         TEXT NOT NULL DEFAULT 'APP',    -- APP | SEED (open_requests.json)
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS offers (               -- an "assigned taxi"
    offer_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT NOT NULL REFERENCES ride_requests(request_id),
    company_id  TEXT NOT NULL,
    vehicle_id  TEXT NOT NULL,
    price_inr   INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'OPEN',  -- OPEN | BOOKED | REJECTED | NOT_CHOSEN | CAR_TAKEN | CANCELLED
    hold_until  TEXT,                          -- guaranteed window: the car is held for this traveler until then
    share_group TEXT,                          -- S1 shared ride: both families' offers have the same group
    shared_pickup TEXT,                        -- S1: pickup of the shared car (the later of the two pickups)
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bookings (
    booking_no  TEXT PRIMARY KEY,              -- unique 10-digit number
    request_id  TEXT NOT NULL REFERENCES ride_requests(request_id),
    offer_id    INTEGER NOT NULL REFERENCES offers(offer_id),
    company_id  TEXT NOT NULL,
    vehicle_id  TEXT NOT NULL,
    flight_id   TEXT NOT NULL,
    pickup      TEXT NOT NULL,
    busy_start  TEXT NOT NULL,                 -- R6 busy time of the car
    busy_end    TEXT NOT NULL,
    price_inr   INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'CONFIRMED',  -- CONFIRMED | CANCELLED
    share_group TEXT,                          -- S1: set while the car is shared with another family
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS change_requests (
    change_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    booking_no     TEXT NOT NULL REFERENCES bookings(booking_no),
    new_flight_id  TEXT,                         -- NULL = same flight
    new_pickup     TEXT,                         -- NULL = earliest pickup of the (new) flight
    old_flight_id  TEXT,                         -- the booking before the change (for the history)
    old_pickup     TEXT,
    status         TEXT NOT NULL DEFAULT 'WAITING',  -- WAITING | ACCEPTED | REJECTED
    reason         TEXT,
    created_at     TEXT NOT NULL,
    decided_at     TEXT
);
"""


PG_SCHEMA = (SCHEMA.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
             .replace("INTEGER NOT NULL REFERENCES offers(offer_id)", "BIGINT NOT NULL REFERENCES offers(offer_id)"))


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# PostgreSQL support: a thin wrapper that gives psycopg the sqlite3-style API the rest of the
# code uses (execute / fetchone / lastrowid / row["col"] / commit / rollback).
# ---------------------------------------------------------------------------
_PG_LOCK_KEY = 7_340_001          # one advisory lock = SQLite's "BEGIN IMMEDIATE" write lock
_RETURNING = {"offers": "offer_id", "change_requests": "change_id"}
_pool = None


class PgRow(tuple):
    """A row that can be read by position and by column name, like sqlite3.Row."""
    _names: tuple = ()

    def __getitem__(self, key):
        if isinstance(key, str):
            return tuple.__getitem__(self, self._names.index(key))
        return tuple.__getitem__(self, key)

    def keys(self):
        return list(self._names)


def _row_factory(cursor):
    names = tuple(c.name for c in (cursor.description or ()))
    cls = type("PgRow", (PgRow,), {"_names": names})
    return lambda values: cls(values)


def _pg_sql(sql: str) -> tuple[str, str | None]:
    """Translate the SQLite dialect used here into PostgreSQL. Returns (sql, returning_column)."""
    sql = sql.replace("%", "%%").replace("?", "%s")
    if re.match(r"\s*INSERT\s+OR\s+IGNORE", sql, re.I):
        sql = re.sub(r"INSERT\s+OR\s+IGNORE", "INSERT", sql, count=1, flags=re.I).rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    m = re.match(r"\s*INSERT\s+INTO\s+(\w+)", sql, re.I)
    returning = _RETURNING.get(m.group(1).lower()) if m else None
    if returning and "RETURNING" not in sql.upper():
        sql = sql.rstrip().rstrip(";") + f" RETURNING {returning}"
    return sql, returning


class PgCursor:
    def __init__(self, cur, returning=None):
        self._cur, self.lastrowid = cur, None
        self.rowcount = cur.rowcount
        if returning:
            row = cur.fetchone()
            self.lastrowid = row[0] if row else None

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur.fetchall())


class PgConnection:
    def __init__(self, raw, pool=None):
        self._raw, self._pool = raw, pool

    def execute(self, sql, params=()):
        stripped = sql.strip().upper()
        if stripped == "BEGIN IMMEDIATE":     # take the write lock; it is released by commit/rollback
            self._raw.execute("SELECT pg_advisory_xact_lock(%s)", (_PG_LOCK_KEY,))
            return None
        if stripped.startswith("PRAGMA"):
            return None
        sql, returning = _pg_sql(sql)
        return PgCursor(self._raw.execute(sql, tuple(params)), returning)

    def commit(self):
        self._raw.commit()

    def rollback(self):
        self._raw.rollback()

    def close(self):
        try:
            self._raw.rollback()
        except Exception:
            pass
        if self._pool is not None:
            self._pool.putconn(self._raw)
        else:
            self._raw.close()


def _get_pool():
    global _pool
    if _pool is None:
        from psycopg.rows import dict_row  # noqa: F401  (import check)
        from psycopg_pool import ConnectionPool
        _pool = ConnectionPool(
            DATABASE_URL, min_size=1, max_size=int(os.environ.get("DB_POOL_MAX", "8")),
            kwargs={"row_factory": _row_factory}, check=ConnectionPool.check_connection,
            max_idle=240, max_lifetime=1800, open=True, timeout=30,
        )
    return _pool


def connect(path=None):
    if USE_PG:
        pool = _get_pool()
        return PgConnection(pool.getconn(), pool)
    # check_same_thread=False: FastAPI may open the connection (in get_db) on one worker thread
    # and run the endpoint on another. That is safe here because every HTTP request gets its
    # own connection, which is never shared. timeout: wait up to 10 s if another request is writing.
    conn = sqlite3.connect(path or DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_db():
    """FastAPI dependency: one connection per HTTP request."""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db(conn) -> None:
    if USE_PG:
        conn.execute("SELECT pg_advisory_xact_lock(%s)" % _PG_LOCK_KEY)   # one worker creates the tables
        for statement in PG_SCHEMA.split(";"):
            if statement.strip():
                conn.execute(statement)
        conn.commit()
        return
    conn.executescript(SCHEMA)
    # Older databases had offers.expires_at and an EXPIRED status: move them to the guarantee model.
    columns = {row[1] for row in conn.execute("PRAGMA table_info(offers)")}
    for column in ("hold_until", "share_group", "shared_pickup"):
        if column not in columns:
            conn.execute(f"ALTER TABLE offers ADD COLUMN {column} TEXT")
    if "share_group" not in {row[1] for row in conn.execute("PRAGMA table_info(bookings)")}:
        conn.execute("ALTER TABLE bookings ADD COLUMN share_group TEXT")
    conn.execute("UPDATE offers SET status = 'OPEN' WHERE status = 'EXPIRED'")
    change_columns = {row[1] for row in conn.execute("PRAGMA table_info(change_requests)")}
    for column in ("old_flight_id", "old_pickup"):
        if column not in change_columns:
            conn.execute(f"ALTER TABLE change_requests ADD COLUMN {column} TEXT")
    conn.commit()


def seed_open_requests(conn, open_requests: list[dict]) -> int:
    """Load open_requests.json once. INSERT OR IGNORE keeps it safe across restarts."""
    added = 0
    for r in open_requests:
        cur = conn.execute(
            """INSERT OR IGNORE INTO ride_requests
               (request_id, traveler_name, flight_id, hotel_id, adults, child_ages,
                large_bags, small_bags, pickup, car_type, price_inr, status, source, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?, 'OPEN', 'SEED', ?)""",
            (
                r["request_id"], r.get("traveler_name") or "Traveler", r["flight_id"], r["hotel_id"],
                r["adults"], json.dumps(r.get("child_ages") or []), r["large_bags"], r["small_bags"],
                r["pickup"][:16], r.get("car_type"), r["price_inr"], now_iso(),
            ),
        )
        added += cur.rowcount
    conn.commit()
    return added
