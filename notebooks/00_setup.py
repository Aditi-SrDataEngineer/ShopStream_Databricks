# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Unity Catalog objects
# MAGIC Creates the catalog, medallion schemas, Volumes for raw files & checkpoints, and the ops tables
# MAGIC (data-quality quarantine + pipeline audit log). Idempotent: safe to re-run.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:  # Free Edition / restricted metastore
    print(f"Could not create catalog '{CATALOG}' ({type(e).__name__}). "
          "Re-run with the catalog widget set to an existing catalog, e.g. 'workspace'.")
    raise

spark.sql(f"USE CATALOG {CATALOG}")
for schema, comment in [
    (LANDING, "Raw files as delivered by source systems (Volumes)"),
    (BRONZE, "Raw, append-only, schema-on-read copies of source data + ingestion metadata"),
    (SILVER, "Cleaned, conformed, de-duplicated, SCD-managed entities"),
    (GOLD, "Business-level star schema and aggregates for BI"),
    (OPS, "Pipeline audit, data-quality quarantine and monitoring"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING}.raw_files COMMENT 'Landing zone for source drops'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{LANDING}.checkpoints COMMENT 'Streaming checkpoints & Auto Loader schemas'")

for e in ENTITIES:
    dbutils.fs.mkdirs(f"{RAW_VOLUME}/{e}")

# COMMAND ----------

# Ops tables ------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(OPS, 'dq_quarantine')} (
  entity STRING, record STRING, dq_failed_rules ARRAY<STRING>,
  source_file STRING, quarantined_at TIMESTAMP)
COMMENT 'Rows rejected by silver data-quality rules, with the reason'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(OPS, 'pipeline_audit')} (
  run_id STRING, layer STRING, entity STRING, rows_in BIGINT, rows_out BIGINT,
  rows_quarantined BIGINT, status STRING, message STRING, logged_at TIMESTAMP)
COMMENT 'One row per layer/entity per run'
""")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
