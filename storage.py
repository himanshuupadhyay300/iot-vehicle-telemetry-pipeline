"""Storage helpers that hide the Parquet-vs-Delta difference from the pipeline code.

Why both formats?
  * Parquet lets the project run on any laptop with no extra downloads (CI-friendly).
  * Delta Lake is the production target on Azure Databricks (ACID, MERGE, time travel).

The two code paths implement the same *semantics*:
  append            - add rows
  replace_partitions- atomically replace specific partitions (idempotent re-computation)
  upsert            - insert new keys / update existing keys (late + duplicate events)
  overwrite_table   - replace a whole (small) table

Caveat: the Parquet path is *not* transactional. A crash in the middle of replace_partitions can leave a
partition missing; the next run re-creates it. Delta gives true atomicity. See docs/DESIGN_DECISIONS.md.
"""
from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.transformations.dedupe import deduplicate_latest
from src.utils.exceptions import PipelineError
from src.utils.logging_utils import get_logger

log = get_logger(__name__)


def _hadoop_fs(spark: SparkSession, path: str):
    jvm = spark._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, hpath


def path_exists(spark: SparkSession, path: str) -> bool:
    """Existence check that works for local paths, dbfs:/ and abfss:// locations."""
    fs, hpath = _hadoop_fs(spark, path)
    return bool(fs.exists(hpath))


def table_exists(spark: SparkSession, path: str, fmt: str) -> bool:
    """True when a table has been written at ``path``."""
    if not path_exists(spark, path):
        return False
    if fmt == "delta":
        from delta.tables import DeltaTable

        return bool(DeltaTable.isDeltaTable(spark, path))
    return True


def read_table(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    """Read a table (Parquet or Delta)."""
    if not table_exists(spark, path, fmt):
        raise PipelineError(f"Table does not exist at {path}")
    return spark.read.format(fmt).load(path)


def append(df: DataFrame, path: str, fmt: str, partition_by: list[str] | None = None) -> None:
    """Append rows to a table."""
    writer = df.write.format(fmt).mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def overwrite_table(df: DataFrame, path: str, fmt: str) -> None:
    """Replace the entire table with ``df`` (only for small, fully recomputed tables)."""
    df.write.format(fmt).mode("overwrite").save(path)


def replace_partitions(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    fmt: str,
    part_col: str,
    values: Iterable[object],
) -> None:
    """Replace the partitions ``part_col in values`` with the rows of ``df``.

    Partitions listed in ``values`` that have no rows in ``df`` are emptied. This matters for correctness:
    when a late event corrects a reading so that an alert disappears, the old alert must not linger.
    """
    wanted = sorted({str(v) for v in values})
    if not wanted:
        return
    if fmt == "delta":
        condition = f"{part_col} IN (" + ", ".join(f"'{v}'" for v in wanted) + ")"
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", condition)
            .partitionBy(part_col)
            .save(path)
        )
        return

    present = {str(r[0]) for r in df.select(part_col).distinct().collect()}
    if path_exists(spark, path):
        for value in set(wanted) - present:
            fs, hpath = _hadoop_fs(spark, f"{path}/{part_col}={value}")
            if fs.exists(hpath):
                fs.delete(hpath, True)
    if present:
        df.write.format("parquet").mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy(
            part_col
        ).save(path)


def upsert(
    spark: SparkSession,
    new_df: DataFrame,
    path: str,
    fmt: str,
    keys: list[str],
    order_cols: list[str],
    part_col: str,
) -> None:
    """Insert new keys and update existing ones (latest ``order_cols`` wins).

    ``new_df`` must already be unique on ``keys``. Re-running with the same data yields the same table.
    """
    affected = sorted({str(r[0]) for r in new_df.select(part_col).distinct().collect()})
    if not affected:
        return
    if not table_exists(spark, path, fmt):
        replace_partitions(spark, new_df, path, fmt, part_col, affected)
        return

    if fmt == "delta":
        from delta.tables import DeltaTable

        target = DeltaTable.forPath(spark, path)
        on = " AND ".join(f"t.{k} = s.{k}" for k in keys) + f" AND t.{part_col} = s.{part_col}"
        dates = ", ".join(f"'{a}'" for a in affected)
        on += f" AND t.{part_col} IN ({dates})"
        (
            target.alias("t")
            .merge(new_df.alias("s"), on)
            .whenMatchedUpdateAll(condition=f"s.{order_cols[0]} >= t.{order_cols[0]}")
            .whenNotMatchedInsertAll()
            .execute()
        )
        return

    existing = read_table(spark, path, fmt).filter(F.col(part_col).cast("string").isin(affected))
    combined = deduplicate_latest(existing.unionByName(new_df, allowMissingColumns=True), keys, order_cols)
    # Materialise BEFORE touching the files we are reading from (Spark is lazy).
    combined = combined.localCheckpoint(eager=True)
    replace_partitions(spark, combined, path, fmt, part_col, affected)
