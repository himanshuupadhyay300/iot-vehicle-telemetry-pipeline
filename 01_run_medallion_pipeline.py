# Databricks notebook source
# MAGIC %md
# MAGIC # IoT Vehicle Telemetry - Medallion pipeline (Bronze -> Silver -> Gold)
# MAGIC
# MAGIC **Status: template. Not executed against a real workspace from this repository.**
# MAGIC
# MAGIC Prerequisites
# MAGIC 1. Repo cloned into Databricks Repos (so `src/` is importable).
# MAGIC 2. ADLS Gen2 account with a `lakehouse` container; cluster has access (service principal or managed identity).
# MAGIC 3. A landing folder reachable by plain Python **and** Spark - a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/<volume>`) is recommended.
# MAGIC 4. Secrets live in a Key Vault-backed secret scope. Never paste keys into a notebook.

# COMMAND ----------

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container", "lakehouse")
dbutils.widgets.text("landing_path", "/Volumes/main/telemetry/landing")
dbutils.widgets.text("state_path", "/Volumes/main/telemetry/state")
dbutils.widgets.text("reports_path", "/Volumes/main/telemetry/reports")
dbutils.widgets.text("file_pattern", "*.csv")
dbutils.widgets.dropdown("stage", "all", ["all", "bronze", "silver", "gold"])

# COMMAND ----------

import os
import sys

# Databricks Repos puts the repo root on the notebook's working directory parent
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, repo_root)

from src.pipeline import run_pipeline
from src.utils.config import load_config
from src.utils.logging_utils import configure_logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage authentication (example, OAuth with a service principal stored in a secret scope)
# MAGIC Uncomment and adapt once the secret scope `kv-telemetry` exists.

# COMMAND ----------

# account = dbutils.widgets.get("storage_account")
# spark.conf.set(f"fs.azure.account.auth.type.{account}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{account}.dfs.core.windows.net",
#                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{account}.dfs.core.windows.net",
#                dbutils.secrets.get("kv-telemetry", "sp-client-id"))
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account}.dfs.core.windows.net",
#                dbutils.secrets.get("kv-telemetry", "sp-client-secret"))
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account}.dfs.core.windows.net",
#                f"https://login.microsoftonline.com/{dbutils.secrets.get('kv-telemetry', 'tenant-id')}/oauth2/token")

# COMMAND ----------

account = dbutils.widgets.get("storage_account")
container = dbutils.widgets.get("container")

cfg = load_config(
    os.path.join(repo_root, "config", "pipeline_config.yaml"),
    {
        "app": {"environment": "databricks"},
        "storage": {
            "format": "delta",
            "base_path": f"abfss://{container}@{account}.dfs.core.windows.net/telemetry",
            "input_path": dbutils.widgets.get("landing_path"),
            "state_path": dbutils.widgets.get("state_path"),
            "reports_path": dbutils.widgets.get("reports_path"),
        },
    },
)
configure_logging("INFO")

# COMMAND ----------

summary = run_pipeline(
    cfg,
    stage=dbutils.widgets.get("stage"),
    patterns=[dbutils.widgets.get("file_pattern")],
    spark=spark,
)
print(summary["status"], summary["table_row_counts"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Delta tables for SQL / Power BI (run once; see `sql/databricks_create_tables.sql`)

# COMMAND ----------

display(spark.read.format("delta").load(f"abfss://{container}@{account}.dfs.core.windows.net/telemetry/gold/vehicle_kpis"))
