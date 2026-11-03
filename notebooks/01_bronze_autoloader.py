# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — incremental ingestion with Auto Loader
# MAGIC
# MAGIC | Capability shown | How |
# MAGIC |---|---|
# MAGIC | Incremental, exactly-once file ingestion | `cloudFiles` + checkpoint (each file is processed once, ever) |
# MAGIC | Schema inference & **evolution** | `cloudFiles.schemaLocation` + `schemaEvolutionMode = addNewColumns` |
# MAGIC | Never lose bad data | `_rescued_data` column captures values that don't fit the schema |
# MAGIC | Lineage to source | `_metadata.file_path`, `_metadata.file_modification_time`, `_ingest_ts` |
# MAGIC | Batch-cost streaming | `trigger(availableNow=True)` — process what's new, then stop (serverless-friendly) |
# MAGIC
# MAGIC Bronze is **append-only and faithful to the source**: CSVs are kept as strings; typing happens in Silver.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("entity", "all", ["all"] + ENTITIES, "Entity to ingest")
ENTITY = dbutils.widgets.get("entity")

from pyspark.sql import functions as F
import uuid, datetime as dt

RUN_ID = str(uuid.uuid4())

SOURCES = {
    "customers": {"format": "csv",  "options": {"header": "true", "cloudFiles.inferColumnTypes": "false"}},
    "products":  {"format": "csv",  "options": {"header": "true", "cloudFiles.inferColumnTypes": "false"}},
    "payments":  {"format": "csv",  "options": {"header": "true", "cloudFiles.inferColumnTypes": "false"}},
    "orders":    {"format": "json", "options": {
        "cloudFiles.inferColumnTypes": "true",
        # pin types of fields we care about so a bad file can't silently change them
        "cloudFiles.schemaHints": "order_ts STRING, event_ts STRING, "
                                  "items ARRAY<STRUCT<product_id:STRING, quantity:BIGINT, unit_price:DOUBLE, discount_pct:DOUBLE>>",
    }},
}


def ingest_to_bronze(entity: str) -> int:
    cfg = SOURCES[entity]
    target = tbl(BRONZE, f"{entity}_raw")
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0

    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", cfg["format"])
              .option("cloudFiles.schemaLocation", checkpoint("bronze_schema", entity))
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .option("rescuedDataColumn", "_rescued_data"))
    for k, v in cfg["options"].items():
        reader = reader.option(k, v)

    df = (reader.load(f"{RAW_VOLUME}/{entity}")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_run_id", F.lit(RUN_ID)))

    q = (df.writeStream
         .option("checkpointLocation", checkpoint("bronze", entity))
         .option("mergeSchema", "true")                 # let new source columns flow into the Delta table
         .trigger(availableNow=True)
         .toTable(target))
    q.awaitTermination()

    added = spark.table(target).count() - before
    spark.createDataFrame(
        [(RUN_ID, "bronze", entity, added, added, 0, "SUCCESS", None, dt.datetime.utcnow())],
        spark.table(tbl(OPS, "pipeline_audit")).schema,
    ).write.mode("append").saveAsTable(tbl(OPS, "pipeline_audit"))
    return added


# COMMAND ----------

# Auto Loader stops the stream with UnknownFieldException when it detects a NEW column
# (that's by design: it records the new schema, then asks for a restart). Retry once.
from pyspark.errors import StreamingQueryException

targets = ENTITIES if ENTITY == "all" else [ENTITY]
for e in targets:
    for attempt in (1, 2):
        try:
            n = ingest_to_bronze(e)
            print(f"bronze.{e}_raw  +{n} rows")
            break
        except StreamingQueryException as ex:
            msg = str(ex).upper()
            if attempt == 1 and ("UNKNOWN_FIELD" in msg or "UNKNOWNFIELDEXCEPTION" in msg):
                print(f"{e}: schema evolved ({str(ex)[:120]}...) — restarting with the new schema")
                continue
            raise

# COMMAND ----------

# MAGIC %md ### Inspect: schema evolution & rescued data

# COMMAND ----------

orders_raw = spark.table(tbl(BRONZE, "orders_raw"))
print("orders_raw columns:", orders_raw.columns)   # coupon_code / device appear after the day-3 drop
agg = [F.count("*").alias("rows"), F.count("_rescued_data").alias("rows_with_rescued_data")]
if "coupon_code" in orders_raw.columns:
    agg.append(F.count("coupon_code").alias("rows_with_coupon_code"))
display(orders_raw.groupBy("_source_file").agg(*agg).orderBy("_source_file"))
