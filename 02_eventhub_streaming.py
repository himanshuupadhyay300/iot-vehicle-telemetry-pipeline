# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIONAL - Event Hubs streaming into the same Bronze/Silver/Gold tables
# MAGIC **Status: template, never executed against Azure.** Requires an Event Hubs namespace + hub, and the
# MAGIC connection string stored in a Key Vault-backed secret scope.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from src.ingestion.eventhub_stream import start_stream
from src.utils.config import load_config

# COMMAND ----------

os.environ["EVENTHUB_NAMESPACE"] = dbutils.secrets.get("kv-telemetry", "eventhub-namespace")
os.environ["EVENTHUB_NAME"] = "vehicle-telemetry"
os.environ["EVENTHUB_CONNECTION_STRING"] = dbutils.secrets.get("kv-telemetry", "eventhub-connection-string")

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
cfg = load_config(
    os.path.join(repo_root, "config", "pipeline_config.yaml"),
    {
        "storage": {
            "format": "delta",
            "base_path": "abfss://lakehouse@<storage_account>.dfs.core.windows.net/telemetry",
            "state_path": "/Volumes/main/telemetry/state",
        },
        "streaming": {"checkpoint_path": "abfss://lakehouse@<storage_account>.dfs.core.windows.net/_checkpoints/eventhub"},
    },
)

# COMMAND ----------

query = start_stream(spark, cfg)
query.awaitTermination()
