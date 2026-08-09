import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path


_DATABASE_FILE = Path(__file__).resolve().parent.parent / "data" / "portfolio_snapshots.sqlite3"

SNAPSHOT_COLUMNS = (
    "snapshot_at",
    "tbank_main_rub",
    "tbank_iis_rub",
    "crypto_usd",
    "ibkr_usd",
    "rub_per_usd",
    "rub_per_eur",
    "usd_per_eur",
    "total_rub",
    "total_usd",
    "total_eur",
)


def _connect() -> sqlite3.Connection:
    connection = sqlite3.connect(_DATABASE_FILE, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


@contextmanager
def _open_connection():
    connection = _connect()
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize_database() -> None:
    """Create the snapshot database and its one-row-per-local-date constraint."""
    _DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _open_connection() as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                snapshot_at TEXT NOT NULL,
                tbank_main_rub REAL,
                tbank_iis_rub REAL,
                crypto_usd REAL,
                ibkr_usd REAL,
                rub_per_usd REAL,
                rub_per_eur REAL,
                usd_per_eur REAL,
                total_rub REAL,
                total_usd REAL,
                total_eur REAL
            )
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_portfolio_snapshots_local_date
            ON portfolio_snapshots(substr(snapshot_at, 1, 10))
            """
        )


def has_snapshot_for_date(snapshot_date: date) -> bool:
    initialize_database()
    with _open_connection() as connection:
        row = connection.execute(
            """
            SELECT 1
            FROM portfolio_snapshots
            WHERE substr(snapshot_at, 1, 10) = ?
            LIMIT 1
            """,
            (snapshot_date.isoformat(),),
        ).fetchone()
    return row is not None


def insert_snapshot(snapshot: dict) -> bool:
    """Insert a daily snapshot without replacing an existing row for that date."""
    initialize_database()
    placeholders = ", ".join("?" for _ in SNAPSHOT_COLUMNS)
    columns = ", ".join(SNAPSHOT_COLUMNS)
    values = tuple(snapshot.get(column) for column in SNAPSHOT_COLUMNS)

    with _open_connection() as connection:
        cursor = connection.execute(
            f"INSERT OR IGNORE INTO portfolio_snapshots ({columns}) "
            f"VALUES ({placeholders})",
            values,
        )
    return cursor.rowcount == 1


def get_all_snapshots() -> list[dict]:
    """Return all snapshots in chronological order."""
    initialize_database()
    columns = ", ".join(SNAPSHOT_COLUMNS)
    with _open_connection() as connection:
        rows = connection.execute(
            f"SELECT {columns} FROM portfolio_snapshots ORDER BY snapshot_at ASC"
        ).fetchall()
    return [dict(row) for row in rows]
