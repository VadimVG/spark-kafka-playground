from pyspark.sql import Row
from typing import Callable, Iterable, Sequence, Iterator

from app.common_utils.connections.db.postgre import get_pg_connect


def make_partition_upserter(sql: str, columns: Sequence[str]) -> Callable[[Iterator], None]:
    def upsert_partition(iterator: Iterable[Row]) -> None:
        """
        Build a function that upserts one partition of rows into PostgreSQL.

        sql     - the SQL query with %s placeholders (INSERT ... ON CONFLICT ...)
        columns - names of Row fields, in the same order as %s placeholders in sql
        """
        with get_pg_connect() as pg:
            cursor = pg.get_cursor()
            try:
                for row in iterator:
                    # get values from the row, in the order given by "columns"
                    params = tuple(getattr(row, col) for col in columns)
                    cursor.execute(sql, params)
                pg.commit()
            except Exception as e:
                pg.rollback()
                print(f"Upsert partition failed: {e}")
                raise
            finally:
                cursor.close()
    return upsert_partition