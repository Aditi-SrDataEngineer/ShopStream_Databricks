# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — star schema & business marts
# MAGIC
# MAGIC ```
# MAGIC                 dim_date
# MAGIC                    │
# MAGIC  dim_customer ── fact_sales ── dim_product        (order-line grain)
# MAGIC  (SCD2, -1 = Unknown)   │
# MAGIC        ┌───────────────┼──────────────────┬────────────────────┬─────────────────────┐
# MAGIC  agg_daily_sales  agg_product_perf.  customer_360 (RFM)  payment_reconciliation  agg_fulfilment_sla
# MAGIC ```
# MAGIC * **Point-in-time join**: each sale gets the customer *version* valid at order time
# MAGIC   (a customer who moved Pune → Nagpur keeps old sales in Pune).
# MAGIC * **Late-arriving / orphan customers** map to the Unknown member `-1` instead of being dropped.
# MAGIC * **Liquid clustering** (`CLUSTER BY`) instead of static partitioning.
# MAGIC * All SQL lives in `src/shopstream/gold_sql.py` and is unit-tested locally.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from datetime import date
from pyspark.sql import functions as F
from shopstream.gold_sql import GOLD_TABLES, render
from shopstream import transforms as T

SILVER_NS, GOLD_NS = f"{CATALOG}.{SILVER}", f"{CATALOG}.{GOLD}"
dbutils.widgets.text("as_of_date", "2026-11-02", "RFM as-of date")
AS_OF = dbutils.widgets.get("as_of_date")

# COMMAND ----------

# Policies from notebook 06 (column mask / row filter) are detached before a rebuild and
# re-applied by 06, so CREATE OR REPLACE never trips over them.
for stmt in [f"ALTER TABLE {GOLD_NS}.dim_customer ALTER COLUMN email DROP MASK",
             f"ALTER TABLE {GOLD_NS}.customer_360 DROP ROW FILTER"]:
    try:
        spark.sql(stmt)
    except Exception:
        pass   # first run: table or policy doesn't exist yet

for t in GOLD_TABLES:
    target = f"{GOLD_NS}.{t['name']}"
    spark.sql(f"""
        CREATE OR REPLACE TABLE {target}
        CLUSTER BY ({', '.join(t['cluster_by'])})
        COMMENT '{t['comment'].replace("'", "")}'
        AS {render(t['sql'], SILVER_NS, GOLD_NS)}
    """)
    print(f"{target:<55} {spark.table(target).count():>8,} rows")

# COMMAND ----------

# MAGIC %md ### customer_360 with RFM segmentation (PySpark window functions)

# COMMAND ----------

c360 = T.rfm_segment(spark.table(f"{GOLD_NS}.customer_base"), AS_OF)
c360.createOrReplaceTempView("c360_stage")
spark.sql(f"""CREATE OR REPLACE TABLE {GOLD_NS}.customer_360 CLUSTER BY (segment)
              COMMENT 'Customer 360 with RFM scores and segment' AS SELECT * FROM c360_stage""")
display(spark.table(f"{GOLD_NS}.customer_360").groupBy("segment")
        .agg(F.count("*").alias("customers"), F.round(F.avg("lifetime_value"), 0).alias("avg_ltv"))
        .orderBy(F.desc("avg_ltv")))

# COMMAND ----------

# MAGIC %md ### BI-friendly denormalised view (for Databricks SQL dashboards / Genie)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_NS}.vw_sales AS
SELECT d.calendar_date, d.month_name, d.day_name, d.is_weekend,
       p.category, p.sub_category, p.brand, p.product_name,
       c.customer_name, c.city AS customer_city_at_order, c.loyalty_tier AS tier_at_order,
       f.order_id, f.channel, f.status, f.ship_state, f.coupon_code,
       f.quantity, f.net_amount, f.margin_amount, f.is_revenue
FROM {GOLD_NS}.fact_sales f
JOIN {GOLD_NS}.dim_date d      ON f.order_date_key = d.date_key
JOIN {GOLD_NS}.dim_product p   ON f.product_id = p.product_id
JOIN {GOLD_NS}.dim_customer c  ON f.customer_sk = c.customer_sk
""")
spark.sql(f"COMMENT ON VIEW {GOLD_NS}.vw_sales IS 'Denormalised sales for BI; customer attributes as of order time'")

# COMMAND ----------

display(spark.sql(f"""
  SELECT category, CAST(sum(net_revenue) AS DECIMAL(16,0)) AS revenue, sum(orders) AS orders
  FROM {GOLD_NS}.agg_daily_sales GROUP BY category ORDER BY revenue DESC"""))
