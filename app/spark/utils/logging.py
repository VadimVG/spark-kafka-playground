import functools
import logging
from typing import Callable

from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


def log_spark_batch(sink_name: str):
    """
    Decorator for foreachBatch functions: logs row count around the write,
    so individual jobs don't repeat count()+print()/logging themselves.

    Usage:
        @log_spark_batch("sales")
        def write_to_postgres(batch_df, batch_id):
            ...
    """
    def decorator(func: Callable[[DataFrame, int], None]):
        @functools.wraps(func)
        def wrapper(batch_df: DataFrame, batch_id: int) -> None:
            count = batch_df.count()
            logger.info(f"Batch %s [%s]: writing %s rows", batch_id, sink_name, count)
            func(batch_df, batch_id)
            logger.info("Batch %s [%s]: done", batch_id, sink_name)
        return wrapper
    return decorator