"""When DATABASE_URL points at PostgreSQL, every test starts from empty tables."""
import os

import pytest


@pytest.fixture(autouse=True)
def _clean_postgres():
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith(("postgres://", "postgresql://")):
        import psycopg
        with psycopg.connect(url, autocommit=True) as c:
            c.execute("DROP TABLE IF EXISTS change_requests, bookings, offers, ride_requests CASCADE")
    yield
