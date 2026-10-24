import shutil

import pytest

from src.ingestion.bronze_ingest import discover_files, file_sha256, ingest_to_bronze
from src.utils import storage
from src.utils.config import state_file_path, table_path
from src.utils.exceptions import IngestionError
from src.utils.state import StateStore

HEADER = "vehicle_id,timestamp,speed_kmph,engine_temp_c,fuel_level_pct,battery_voltage_v,latitude,longitude\n"


def write(cfg, name, text):
    folder = cfg["storage"]["input_path"]
    import os

    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return path


def test_sha_and_discovery(cfg):
    a = write(cfg, "a.csv", HEADER)
    write(cfg, "b.txt", "x")
    assert [p.name for p in discover_files(cfg["storage"]["input_path"], ["*.csv"])] == ["a.csv"]
    assert len(file_sha256(__import__("pathlib").Path(a))) == 64


def test_missing_folder_raises(cfg):
    with pytest.raises(IngestionError):
        discover_files(cfg["storage"]["input_path"], ["*.csv"])


def test_csv_ingest_adds_lineage_and_keeps_raw_text(spark, cfg):
    write(cfg, "a.csv", HEADER + "VH-0001,2026-01-05T08:00:00Z,abc,90,,13.8,18.5,73.8\n")
    state = StateStore(state_file_path(cfg))
    res = ingest_to_bronze(spark, cfg, state)
    assert (res.files_ingested, res.rows_ingested) == (1, 1)
    row = storage.read_table(spark, table_path(cfg, "bronze"), "parquet").first()
    assert row["speed_kmph"] == "abc"  # not coerced
    assert row["_batch_id"] == res.batch_id and row["_source_format"] == "csv" and row["_source_file"].endswith("a.csv")


def test_copy_with_other_name_is_skipped(spark, cfg):
    path = write(cfg, "a.csv", HEADER + "VH-0001,2026-01-05T08:00:00Z,1,90,50,13.8,18.5,73.8\n")
    state = StateStore(state_file_path(cfg))
    assert ingest_to_bronze(spark, cfg, state).files_ingested == 1
    shutil.copy(path, path.replace("a.csv", "a_copy.csv"))
    again = ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))
    assert (again.files_ingested, again.files_skipped, again.batch_id) == (0, 2, None)
    assert storage.read_table(spark, table_path(cfg, "bronze"), "parquet").count() == 1


def test_missing_csv_column_is_rejected(spark, cfg):
    write(cfg, "bad.csv", "vehicle_id,timestamp\nVH-0001,2026-01-05T08:00:00Z\n")
    with pytest.raises(IngestionError, match="missing required columns"):
        ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))


def test_json_corrupt_line_preserved(spark, cfg):
    good = '{"vehicle_id":"VH-0001","timestamp":"2026-01-05 08:00:00","speed_kmph":50,"engine_temp_c":90.5,"fuel_level_pct":60,"battery_voltage_v":13.8,"latitude":18.5,"longitude":73.8}'
    write(cfg, "a.json", good + "\n" + '{"vehicle_id": "VH-0002", \n')
    res = ingest_to_bronze(spark, cfg, StateStore(state_file_path(cfg)))
    assert res.rows_ingested == 2
    df = storage.read_table(spark, table_path(cfg, "bronze"), "parquet")
    assert df.filter("_corrupt_record is not null").count() == 1
    assert df.filter("vehicle_id = 'VH-0001'").first()["engine_temp_c"] == "90.5"  # JSON number kept as text
