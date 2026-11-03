# Databricks notebook source
# MAGIC %md
# MAGIC # ShopStream — Lakeflow Declarative Pipeline (formerly Delta Live Tables)
# MAGIC
# MAGIC The **declarative** twin of notebooks 01–03. Same data, same rules, ~200 lines instead of ~600,
# MAGIC because the framework handles checkpoints, retries, ordering, schema evolution and SCD for you.
# MAGIC
# MAGIC | Imperative (notebooks) | Declarative (this file) |
# MAGIC |---|---|
# MAGIC | Auto Loader + `writeStream` + checkpoint | `@dlt.table` returning `spark.readStream.format("cloudFiles")` |
# MAGIC | `apply_dq_rules` + quarantine table | `@dlt.expect_all_or_drop` + inverse-rule quarantine table |
# MAGIC | Hand-written SCD2 `MERGE` | `dlt.apply_changes(..., stored_as_scd_type=2)` |
# MAGIC | `CREATE OR REPLACE TABLE ... AS` | Materialized views (incrementally refreshed on serverless) |
# MAGIC
# MAGIC **Pipeline configuration keys** (set in `resources/pipeline.yml`):
# MAGIC `shopstream.raw_path`, `shopstream.src_path`.

# COMMAND ----------

import sys

import dlt
from pyspark.sql import functions as F

RAW = spark.conf.get("shopstream.raw_path")
sys.path.insert(0, spark.conf.get("shopstream.src_path"))
from shopstream import transforms as T  # noqa: E402


def autoloader(entity: str, fmt: str, **opts):
    r = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", fmt)
         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
         .option("rescuedDataColumn", "_rescued_data"))
    for k, v in opts.items():
        r = r.option(k, v)
    return (r.load(f"{RAW}/{entity}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingest_ts", F.current_timestamp()))


def quarantine_filter(rules: dict) -> str:
    """Rows failing ANY rule (NULL counts as failure)."""
    return " OR ".join(f"NOT coalesce(({r}), false)" for r in rules.values())

# COMMAND ----------

# MAGIC %md ## Bronze

# COMMAND ----------

CSV = {"header": "true", "cloudFiles.inferColumnTypes": "false"}


@dlt.table(name="bronze_customers", comment="Raw customer CDC files")
def bronze_customers():
    return autoloader("customers", "csv", **CSV)


@dlt.table(name="bronze_products", comment="Raw product master files")
def bronze_products():
    return autoloader("products", "csv", **CSV)


@dlt.table(name="bronze_payments", comment="Raw payment files")
def bronze_payments():
    return autoloader("payments", "csv", **CSV)


@dlt.table(name="bronze_orders", comment="Raw order events (nested JSON)")
def bronze_orders():
    return autoloader("orders", "json", **{
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaHints": "order_ts STRING, event_ts STRING, items ARRAY<STRUCT<product_id:STRING, "
                                  "quantity:BIGINT, unit_price:DOUBLE, discount_pct:DOUBLE>>"})

# COMMAND ----------

# MAGIC %md ## Silver — expectations + quarantine + AUTO CDC (apply_changes)

# COMMAND ----------

# ---------- customers: clean -> SCD2
@dlt.view(name="customers_clean")
def customers_clean():
    return T.clean_customers(dlt.read_stream("bronze_customers"))


@dlt.table(name="customers_valid", comment="Customer CDC rows that passed DQ")
@dlt.expect_all_or_drop(T.CUSTOMER_DQ_RULES)
def customers_valid():
    return dlt.read_stream("customers_clean")


@dlt.table(name="quarantine_customers", comment="Customer rows that failed DQ")
def quarantine_customers():
    return dlt.read_stream("customers_clean").where(quarantine_filter(T.CUSTOMER_DQ_RULES))


dlt.create_streaming_table("dim_customer_scd2", comment="Customer dimension - SCD Type 2 via AUTO CDC")
dlt.apply_changes(
    target="dim_customer_scd2",
    source="customers_valid",
    keys=["customer_id"],
    sequence_by=F.col("updated_at_ts"),          # out-of-order safe
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "_source_file"],
    stored_as_scd_type=2,
    track_history_column_list=T.CUSTOMER_SCD2_COLS,   # only these columns create a new version
)

# ---------- products: SCD1
@dlt.table(name="products_valid")
@dlt.expect_all_or_drop(T.PRODUCT_DQ_RULES)
def products_valid():
    return T.clean_products(dlt.read_stream("bronze_products"))


dlt.create_streaming_table("products", comment="Product master - SCD Type 1")
dlt.apply_changes(target="products", source="products_valid", keys=["product_id"],
                  sequence_by=F.col("updated_at_ts"), stored_as_scd_type=1)

# ---------- orders: latest state (SCD1 keyed on order_id, sequenced by event time)
@dlt.view(name="orders_clean")
def orders_clean():
    return T.clean_orders(dlt.read_stream("bronze_orders"))


@dlt.table(name="orders_valid", comment="Order events that passed DQ")
@dlt.expect_all_or_drop(T.ORDER_DQ_RULES)
def orders_valid():
    return dlt.read_stream("orders_clean")


@dlt.table(name="quarantine_orders")
def quarantine_orders():
    return dlt.read_stream("orders_clean").where(quarantine_filter(T.ORDER_DQ_RULES))


dlt.create_streaming_table("orders", comment="One row per order, latest status")
dlt.apply_changes(target="orders", source="orders_valid", keys=["order_id"],
                  sequence_by=F.col("event_ts"), except_column_list=["_source_file"], stored_as_scd_type=1)


@dlt.table(name="order_items", comment="Exploded order lines (lines are immutable once placed)")
@dlt.expect("non_negative_net", "net_amount >= 0")            # warn-only expectation (tracked in event log)
def order_items():
    # every event of an order carries the same lines -> explode all, keep the first copy of each line.
    # (late-arriving orders may only ever arrive as DELIVERED, so don't filter on PLACED)
    return (T.explode_order_items(dlt.read_stream("orders_valid"))
            .dropDuplicates(["order_id", "line_no"]))


# ---------- payments
@dlt.table(name="payments_valid")
@dlt.expect_all_or_drop(T.PAYMENT_DQ_RULES)
def payments_valid():
    return T.clean_payments(dlt.read_stream("bronze_payments"))


dlt.create_streaming_table("payments")
dlt.apply_changes(target="payments", source="payments_valid", keys=["payment_id"],
                  sequence_by=F.col("paid_at"), stored_as_scd_type=1)

# COMMAND ----------

# MAGIC %md ## Gold — materialized views

# COMMAND ----------

@dlt.table(name="gold_fact_sales", comment="Order-line sales with point-in-time customer version")
def gold_fact_sales():
    i, o, p = dlt.read("order_items"), dlt.read("orders"), dlt.read("products")
    c = dlt.read("dim_customer_scd2").select(
        F.col("customer_id").alias("c_customer_id"), F.col("city").alias("customer_city_at_order"),
        F.col("loyalty_tier").alias("tier_at_order"), "__START_AT", "__END_AT")
    return (i.join(o.drop("items"), "order_id")
             .join(p.select("product_id", "category", "cost_price"), "product_id", "left")
             .join(c, (F.col("customer_id") == F.col("c_customer_id"))
                      & (F.col("order_ts") >= F.col("__START_AT"))
                      & (F.col("order_ts") < F.coalesce(F.col("__END_AT"), F.lit("9999-12-31").cast("timestamp"))),
                   "left")
             .select("order_id", "line_no", "order_ts", "customer_id", "customer_city_at_order", "tier_at_order",
                     "product_id", "category", "channel", "status", "quantity", "net_amount",
                     (F.col("net_amount") - F.col("quantity") * F.col("cost_price")).alias("margin_amount"),
                     (F.col("status") != "CANCELLED").alias("is_revenue")))


@dlt.table(name="gold_daily_sales", comment="Daily revenue by category & channel")
def gold_daily_sales():
    return (dlt.read("gold_fact_sales").where("is_revenue")
            .groupBy(F.to_date("order_ts").alias("order_date"), "category", "channel")
            .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"),
                 F.round(F.sum("net_amount"), 2).alias("net_revenue"),
                 F.round(F.sum("margin_amount"), 2).alias("margin")))
