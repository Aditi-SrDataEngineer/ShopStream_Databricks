# Databricks notebook source
# MAGIC %md
# MAGIC # _config — shared settings (imported by every notebook with `%run ./_config`)
# MAGIC Change the **catalog** widget once and the whole project follows.
# MAGIC On Databricks Free Edition, if you are not allowed to create a catalog, set `catalog = workspace`.

# COMMAND ----------

import os, sys

dbutils.widgets.text("catalog", "shopstream", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog").strip()

# Schemas = medallion layers + ops
LANDING, BRONZE, SILVER, GOLD, OPS = "landing", "bronze", "silver", "gold", "ops"

# Unity Catalog Volumes (governed file storage — no DBFS mounts needed)
RAW_VOLUME = f"/Volumes/{CATALOG}/{LANDING}/raw_files"
CHECKPOINT_VOLUME = f"/Volumes/{CATALOG}/{LANDING}/checkpoints"

ENTITIES = ["customers", "products", "orders", "payments"]

# Make the shared library in ../src importable (works in Git folders and bundle deployments)
_src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

def tbl(schema: str, name: str) -> str:
    """Fully-qualified three-level UC name."""
    return f"{CATALOG}.{schema}.{name}"

def checkpoint(layer: str, entity: str) -> str:
    return f"{CHECKPOINT_VOLUME}/{layer}/{entity}"

print(f"Catalog={CATALOG} | raw files={RAW_VOLUME}")
