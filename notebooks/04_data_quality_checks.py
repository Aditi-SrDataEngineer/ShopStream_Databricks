# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data-quality gates (fail the job if the data is wrong)
# MAGIC Runs after Gold in the Workflow. Any failed **blocking** check raises an exception → the job fails →
# MAGIC alerting fires and downstream consumers never see bad numbers. Results are also logged to `ops.dq_results`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import datetime as dt
S, G = f"{CATALOG}.{SILVER}", f"{CATALOG}.{GOLD}"

CHECKS = [
    # (name, SQL returning a single number, expectation lambda, blocking?)
    ("silver.dim_customer: one current version per customer",
     f"SELECT count(*) FROM (SELECT customer_id FROM {S}.dim_customer WHERE is_current GROUP BY 1 HAVING count(*) > 1)",
     lambda v: v == 0, True),
    ("silver.dim_customer: no overlapping validity windows",
     f"""SELECT count(*) FROM {S}.dim_customer a JOIN {S}.dim_customer b
         ON a.customer_id = b.customer_id AND a.customer_sk < b.customer_sk
         AND a.effective_from < coalesce(b.effective_to, TIMESTAMP'9999-12-31')
         AND b.effective_from < coalesce(a.effective_to, TIMESTAMP'9999-12-31')""",
     lambda v: v == 0, True),
    ("silver.orders: order_id unique",
     f"SELECT count(*) - count(DISTINCT order_id) FROM {S}.orders", lambda v: v == 0, True),
    ("silver.order_items: no non-positive quantities",
     f"SELECT count(*) FROM {S}.order_items WHERE quantity <= 0", lambda v: v == 0, True),
    ("silver.order_items: every line has a header",
     f"SELECT count(*) FROM {S}.order_items i LEFT ANTI JOIN {S}.orders o USING (order_id)", lambda v: v == 0, True),
    ("gold.fact_sales: line count reconciles with silver",
     f"SELECT (SELECT count(*) FROM {G}.fact_sales) - (SELECT count(*) FROM {S}.order_items)", lambda v: v == 0, True),
    ("gold.fact_sales: revenue reconciles with silver (INR)",
     f"""SELECT abs((SELECT sum(net_amount) FROM {G}.fact_sales WHERE is_revenue)
              - (SELECT sum(i.net_amount) FROM {S}.order_items i JOIN {S}.orders o USING (order_id)
                 WHERE o.status <> 'CANCELLED'))""", lambda v: v < 1, True),
    ("gold.fact_sales: orphan (Unknown customer) share < 2%",
     f"SELECT avg(CASE WHEN customer_sk = -1 THEN 1.0 ELSE 0 END) * 100 FROM {G}.fact_sales", lambda v: v < 2, False),
    ("ops.dq_quarantine: rejected order events < 1% of bronze",
     f"""SELECT (SELECT count(*) FROM {CATALOG}.{OPS}.dq_quarantine WHERE entity = 'orders') * 100.0
              / (SELECT count(*) FROM {CATALOG}.{BRONZE}.orders_raw)""", lambda v: v < 1, False),
    ("freshness: latest order is < 3 days old vs as-of",
     f"SELECT datediff(DATE'2026-11-03', max(to_date(order_ts))) FROM {S}.orders", lambda v: v <= 3, False),
]

results, failures = [], []
for name, sql, ok, blocking in CHECKS:
    value = spark.sql(sql).first()[0]
    value = float(value) if value is not None else 0.0
    passed = bool(ok(value))
    results.append((name, value, passed, blocking, dt.datetime.utcnow()))
    flag = "PASS" if passed else ("FAIL" if blocking else "WARN")
    print(f"[{flag}] {name}: {value:,.2f}")
    if not passed and blocking:
        failures.append(name)

spark.createDataFrame(results, "check STRING, value DOUBLE, passed BOOLEAN, blocking BOOLEAN, checked_at TIMESTAMP") \
     .write.mode("append").saveAsTable(f"{CATALOG}.{OPS}.dq_results")

# COMMAND ----------

display(spark.sql(f"""
  SELECT entity, explode(dq_failed_rules) AS rule, count(*) AS rows
  FROM {CATALOG}.{OPS}.dq_quarantine GROUP BY ALL ORDER BY entity, rows DESC"""))

# COMMAND ----------

if failures:
    raise AssertionError(f"{len(failures)} blocking data-quality check(s) failed: {failures}")
print("All blocking data-quality checks passed ✅")
