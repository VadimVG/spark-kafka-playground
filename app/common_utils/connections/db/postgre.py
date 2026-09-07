import psycopg2
from psycopg2.extensions import connection, cursor

from typing import Optional
from config import DBConfig, pg_config


class PostgresConnection:
    """Wrapper around psycopg2 connection with context manager support."""

    def __init__(self, config: DBConfig):
        self.config = config
        self._conn: Optional[connection] = None

    def _connect(self) -> None:
        """Create connection if not exists or closed."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.db_name,
            )

    def _close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def get_cursor(self) -> cursor:
        self._connect()
        return self._conn.cursor()

    def __enter__(self) -> "PostgresConnection":
        self._connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._close()

            

def get_pg_connect() -> PostgresConnection:
    return PostgresConnection(config=pg_config)