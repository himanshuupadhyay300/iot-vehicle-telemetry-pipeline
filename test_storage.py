from src.utils import storage


def test_replace_partitions_empties_missing_partition(spark, tmp_path):
    path = str(tmp_path / "t")
    df = spark.createDataFrame([("a", "2026-01-01"), ("b", "2026-01-02")], "v string, d string")
    storage.replace_partitions(spark, df, path, "parquet", "d", ["2026-01-01", "2026-01-02"])
    assert storage.read_table(spark, path, "parquet").count() == 2
    # recompute: 2026-01-01 now has no rows (e.g. a late event removed an alert)
    new = spark.createDataFrame([("c", "2026-01-02")], "v string, d string")
    storage.replace_partitions(spark, new, path, "parquet", "d", ["2026-01-01", "2026-01-02"])
    rows = storage.read_table(spark, path, "parquet").collect()
    assert [(r.v, str(r.d)) for r in rows] == [("c", "2026-01-02")]


def test_table_exists(spark, tmp_path):
    assert not storage.table_exists(spark, str(tmp_path / "nope"), "parquet")


def test_upsert_is_idempotent(spark, tmp_path):
    path = str(tmp_path / "u")
    schema = "k string, d string, _ingested_at int, val int"
    storage.upsert(spark, spark.createDataFrame([("a", "2026-01-01", 1, 10)], schema), path, "parquet", ["k"], ["_ingested_at"], "d")
    newer = spark.createDataFrame([("a", "2026-01-01", 2, 20), ("b", "2026-01-01", 2, 30)], schema)
    for _ in range(2):  # twice == once
        storage.upsert(spark, newer, path, "parquet", ["k"], ["_ingested_at"], "d")
    rows = {r.k: r.val for r in storage.read_table(spark, path, "parquet").collect()}
    assert rows == {"a": 20, "b": 30}
