# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — clean, validate, de-duplicate, MERGE
# MAGIC
# MAGIC Bronze tables are read **incrementally as a stream** (only new rows since the last run) and each
# MAGIC micro-batch is upserted with `foreachBatch` + Delta `MERGE`.
# MAGIC
# MAGIC | Entity | Pattern | Key techniques |
# MAGIC |---|---|---|
# MAGIC | `dim_customer` | **SCD Type 2** from a CDC feed (INSERT/UPDATE/DELETE) | row-hash change detection, staged-union MERGE, out-of-order protection, soft deletes |
# MAGIC | `products` | **SCD Type 1** | MERGE only when the source is newer |
# MAGIC | `orders` | Latest-state upsert of an event stream | status lifecycle, replay-safe, late-arriving data, schema evolution |
# MAGIC | `order_status_history` | Append-only, idempotent | insert-if-not-exists MERGE |
# MAGIC | `order_items` | Exploded nested JSON | `posexplode`, computed money columns |
# MAGIC | `payments` | De-dup + upsert | |
# MAGIC
# MAGIC Every rejected row goes to `ops.dq_quarantine` **with the rule(s) it failed** — nothing is silently dropped.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("entity", "all", ["all"] + ENTITIES, "Entity")
ENTITY = dbutils.widgets.get("entity")

import uuid, datetime as dt
from pyspark.sql import DataFrame, functions as F
from pyspark.errors import StreamingQueryException
from shopstream import transforms as T

RUN_ID = str(uuid.uuid4())
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md ## Target tables (explicit DDL = explicit contracts)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(SILVER, 'dim_customer')} (
  customer_sk     BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key (one per version)',
  customer_id     STRING NOT NULL COMMENT 'Business key',
  first_name STRING, last_name STRING, email STRING, phone STRING,
  city STRING, state STRING, signup_date DATE, loyalty_tier STRING,
  row_hash        STRING    COMMENT 'SHA-256 of tracked attributes',
  effective_from  TIMESTAMP, effective_to TIMESTAMP,
  is_current      BOOLEAN, is_deleted BOOLEAN,
  _updated_at     TIMESTAMP)
CLUSTER BY (customer_id)
COMMENT 'Customer dimension, SCD Type 2'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(SILVER, 'products')} (
  product_id STRING NOT NULL, product_name STRING, category STRING, sub_category STRING, brand STRING,
  unit_price DECIMAL(12,2), cost_price DECIMAL(12,2), is_active BOOLEAN,
  updated_at_ts TIMESTAMP, _updated_at TIMESTAMP)
CLUSTER BY (product_id) COMMENT 'Product master, SCD Type 1'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(SILVER, 'orders')} (
  order_id STRING NOT NULL, customer_id STRING, order_ts TIMESTAMP, event_ts TIMESTAMP,
  status STRING, channel STRING, ship_city STRING, ship_state STRING, ship_pincode STRING,
  coupon_code STRING, device STRING, _source_file STRING, _updated_at TIMESTAMP)
CLUSTER BY (order_ts, customer_id) COMMENT 'One row per order, latest status'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(SILVER, 'order_status_history')} (
  order_id STRING NOT NULL, status STRING, event_ts TIMESTAMP)
CLUSTER BY (order_id) COMMENT 'Every status transition an order went through'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(SILVER, 'order_items')} (
  order_id STRING NOT NULL, line_no INT, product_id STRING, quantity INT,
  unit_price DECIMAL(12,2), discount_pct DECIMAL(5,2), gross_amount DECIMAL(14,2), net_amount DECIMAL(14,2))
CLUSTER BY (order_id) COMMENT 'Order lines exploded from nested JSON'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl(SILVER, 'payments')} (
  payment_id STRING NOT NULL, order_id STRING, payment_method STRING, amount DECIMAL(14,2),
  payment_status STRING, paid_at TIMESTAMP, _updated_at TIMESTAMP)
CLUSTER BY (order_id) COMMENT 'Payments, de-duplicated'
""")

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

def quarantine(entity: str, bad: DataFrame) -> int:
    n = bad.count()
    if n:
        payload = [c for c in bad.columns if c not in ("dq_failed_rules", "_source_file")]
        src_file = F.col("_source_file") if "_source_file" in bad.columns else F.lit(None).cast("string")
        (bad.select(F.lit(entity).alias("entity"),
                    F.to_json(F.struct(*payload)).alias("record"),
                    "dq_failed_rules",
                    src_file.alias("source_file"),
                    F.current_timestamp().alias("quarantined_at"))
            .write.mode("append").saveAsTable(tbl(OPS, "dq_quarantine")))
    return n


def audit(entity: str, rows_in: int, rows_out: int, rows_bad: int, status="SUCCESS", msg=None):
    spark.createDataFrame(
        [(RUN_ID, "silver", entity, rows_in, rows_out, rows_bad, status, msg, dt.datetime.utcnow())],
        spark.table(tbl(OPS, "pipeline_audit")).schema,
    ).write.mode("append").saveAsTable(tbl(OPS, "pipeline_audit"))


def run_incremental(entity: str, batch_fn) -> None:
    """Stream new bronze rows into `batch_fn` (foreachBatch). Retries once if bronze schema evolved."""
    for attempt in (1, 2):
        try:
            (spark.readStream.table(tbl(BRONZE, f"{entity}_raw"))
                 .writeStream
                 .foreachBatch(batch_fn)
                 .option("checkpointLocation", checkpoint("silver", entity))
                 .trigger(availableNow=True)
                 .start()
                 .awaitTermination())
            return
        except StreamingQueryException as ex:
            if attempt == 1 and "SCHEMA" in str(ex).upper():
                print(f"{entity}: bronze schema changed, restarting stream with new schema")
                continue
            raise

# COMMAND ----------

# MAGIC %md ## Customers → SCD Type 2

# COMMAND ----------

SCD2_MERGE = f"""
MERGE INTO {tbl(SILVER, 'dim_customer')} AS t
USING (
    -- (1) every incoming row keyed on its business key -> used to EXPIRE / DELETE current versions
    SELECT customer_id AS merge_key, * FROM customer_updates
    UNION ALL
    -- (2) changed rows again with a NULL key -> never match -> INSERT the new version
    SELECT NULL AS merge_key, u.*
    FROM customer_updates u
    JOIN {tbl(SILVER, 'dim_customer')} c
      ON u.customer_id = c.customer_id AND c.is_current
    WHERE u.op <> 'DELETE' AND u.row_hash <> c.row_hash AND u.updated_at_ts > c.effective_from
) AS s
ON t.customer_id = s.merge_key AND t.is_current
WHEN MATCHED AND s.op = 'DELETE' AND s.updated_at_ts > t.effective_from THEN UPDATE SET
    is_current = false, is_deleted = true, effective_to = s.updated_at_ts, _updated_at = current_timestamp()
WHEN MATCHED AND s.row_hash <> t.row_hash AND s.updated_at_ts > t.effective_from THEN UPDATE SET
    is_current = false, effective_to = s.updated_at_ts, _updated_at = current_timestamp()
WHEN NOT MATCHED AND s.op <> 'DELETE' THEN INSERT
    (customer_id, first_name, last_name, email, phone, city, state, signup_date, loyalty_tier,
     row_hash, effective_from, effective_to, is_current, is_deleted, _updated_at)
  VALUES
    (s.customer_id, s.first_name, s.last_name, s.email, s.phone, s.city, s.state, s.signup_date, s.loyalty_tier,
     s.row_hash,
     -- brand-new customer: valid since sign-up (so historical orders join); new version: since the change
     CASE WHEN s.merge_key IS NULL THEN s.updated_at_ts
          ELSE least(s.updated_at_ts, cast(s.signup_date AS TIMESTAMP)) END,
     NULL, true, false, current_timestamp())
"""


def process_customers(batch: DataFrame, batch_id: int) -> None:
    cleaned = T.clean_customers(batch)
    valid, bad = T.apply_dq_rules(cleaned, T.CUSTOMER_DQ_RULES)
    n_bad = quarantine("customers", bad)
    # exact duplicates + multiple changes in one batch -> keep the latest event per customer
    latest = T.dedupe_latest(valid, ["customer_id"], "updated_at_ts")
    latest = T.add_row_hash(latest, T.CUSTOMER_SCD2_COLS)
    latest.createOrReplaceTempView("customer_updates")
    batch.sparkSession.sql(SCD2_MERGE)
    audit("customers", batch.count(), latest.count(), n_bad)

# COMMAND ----------

# MAGIC %md ## Products → SCD Type 1

# COMMAND ----------

def process_products(batch: DataFrame, batch_id: int) -> None:
    valid, bad = T.apply_dq_rules(T.clean_products(batch), T.PRODUCT_DQ_RULES)
    n_bad = quarantine("products", bad)
    T.dedupe_latest(valid, ["product_id"], "updated_at_ts").createOrReplaceTempView("product_updates")
    batch.sparkSession.sql(f"""
        MERGE INTO {tbl(SILVER, 'products')} t USING product_updates s ON t.product_id = s.product_id
        WHEN MATCHED AND s.updated_at_ts > t.updated_at_ts THEN UPDATE SET
             product_name = s.product_name, category = s.category, sub_category = s.sub_category,
             brand = s.brand, unit_price = s.unit_price, cost_price = s.cost_price,
             is_active = s.is_active, updated_at_ts = s.updated_at_ts, _updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
             (product_id, product_name, category, sub_category, brand, unit_price, cost_price,
              is_active, updated_at_ts, _updated_at)
        VALUES (s.product_id, s.product_name, s.category, s.sub_category, s.brand, s.unit_price,
                s.cost_price, s.is_active, s.updated_at_ts, current_timestamp())""")
    audit("products", batch.count(), valid.count(), n_bad)

# COMMAND ----------

# MAGIC %md ## Orders → latest state + status history + exploded lines

# COMMAND ----------

ORDER_COLS = ["order_id", "customer_id", "order_ts", "event_ts", "status", "channel", "ship_city",
              "ship_state", "ship_pincode", "coupon_code", "device", "_source_file"]


def process_orders(batch: DataFrame, batch_id: int) -> None:
    ss = batch.sparkSession
    cleaned = T.clean_orders(batch)
    valid, bad = T.apply_dq_rules(cleaned, T.ORDER_DQ_RULES)
    n_bad = quarantine("orders", bad)

    # status history: every distinct (order, status) event, idempotent on replays
    valid.select("order_id", "status", "event_ts").dropDuplicates(["order_id", "status"]) \
         .createOrReplaceTempView("status_events")
    ss.sql(f"""
        MERGE INTO {tbl(SILVER, 'order_status_history')} t USING status_events s
        ON t.order_id = s.order_id AND t.status = s.status
        WHEN NOT MATCHED THEN INSERT *""")

    # header: latest event per order wins; target is only updated by a NEWER event (late data safe)
    latest = T.dedupe_latest(valid, ["order_id"], "event_ts")
    latest.select(*ORDER_COLS).createOrReplaceTempView("order_updates")
    set_cols = ", ".join(f"{c} = s.{c}" for c in ORDER_COLS if c not in ("order_id", "coupon_code", "device"))
    ss.sql(f"""
        MERGE INTO {tbl(SILVER, 'orders')} t USING order_updates s ON t.order_id = s.order_id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET {set_cols},
             coupon_code = coalesce(s.coupon_code, t.coupon_code),
             device      = coalesce(s.device, t.device),
             _updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({", ".join(ORDER_COLS)}, _updated_at)
             VALUES ({", ".join("s." + c for c in ORDER_COLS)}, current_timestamp())""")

    # lines: immutable once placed -> insert only
    T.explode_order_items(latest).createOrReplaceTempView("order_lines")
    ss.sql(f"""
        MERGE INTO {tbl(SILVER, 'order_items')} t USING order_lines s
        ON t.order_id = s.order_id AND t.line_no = s.line_no
        WHEN NOT MATCHED THEN INSERT *""")
    audit("orders", batch.count(), latest.count(), n_bad)

# COMMAND ----------

# MAGIC %md ## Payments

# COMMAND ----------

def process_payments(batch: DataFrame, batch_id: int) -> None:
    valid, bad = T.apply_dq_rules(T.clean_payments(batch), T.PAYMENT_DQ_RULES)
    n_bad = quarantine("payments", bad)
    T.dedupe_latest(valid, ["payment_id"], "paid_at").createOrReplaceTempView("payment_updates")
    batch.sparkSession.sql(f"""
        MERGE INTO {tbl(SILVER, 'payments')} t USING payment_updates s ON t.payment_id = s.payment_id
        WHEN MATCHED AND s.paid_at > t.paid_at THEN UPDATE SET
             payment_status = s.payment_status, amount = s.amount, paid_at = s.paid_at, _updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (payment_id, order_id, payment_method, amount, payment_status, paid_at, _updated_at)
             VALUES (s.payment_id, s.order_id, s.payment_method, s.amount, s.payment_status, s.paid_at, current_timestamp())""")
    audit("payments", batch.count(), valid.count(), n_bad)

# COMMAND ----------

# MAGIC %md ## Run

# COMMAND ----------

PROCESSORS = {"customers": process_customers, "products": process_products,
              "orders": process_orders, "payments": process_payments}

for e in (ENTITIES if ENTITY == "all" else [ENTITY]):
    run_incremental(e, PROCESSORS[e])
    print(f"silver {e}: done")

# COMMAND ----------

# MAGIC %md ## Prove SCD2 works: customers with history

# COMMAND ----------

display(spark.sql(f"""
  SELECT customer_id, customer_sk, city, loyalty_tier, email, effective_from, effective_to, is_current, is_deleted
  FROM {tbl(SILVER, 'dim_customer')}
  WHERE customer_id IN (SELECT customer_id FROM {tbl(SILVER, 'dim_customer')} GROUP BY 1 HAVING count(*) > 1 LIMIT 5)
  ORDER BY customer_id, effective_from"""))
