.PHONY: install sample-data test lint run batch1 batch2 sql delta clean

PY ?= python

install:
	$(PY) -m pip install -r requirements-dev.txt

sample-data:
	$(PY) -m src.ingestion.generate_synthetic_data --output data/sample --seed 42

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check .

# Demo of incremental loading: batch 1 first, then batch 2 arrives (with late + duplicate events).
batch1:
	$(PY) -m src.pipeline --pattern "telemetry_batch_001.csv"

batch2:
	$(PY) -m src.pipeline

run: batch1 batch2

sql:
	$(PY) -m src.utils.sql_runner sql/gold_queries.sql

# Same pipeline on Delta Lake (needs internet on first run to fetch Delta jars)
delta:
	$(PY) -m src.pipeline --format delta --base-path output/lakehouse_delta

clean:
	rm -rf output spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
