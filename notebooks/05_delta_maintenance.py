# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Delta Lake internals, performance & maintenance
# MAGIC
# MAGIC 1. Table history & **time travel** (audit / debugging)
# MAGIC 2. **RESTORE** after a bad write (disaster recovery in seconds)
# MAGIC 3. **Change Data Feed** — what exactly changed in `dim_customer`
# MAGIC 4. **OPTIMIZE** + **liquid clustering**, file-level stats, data skipping
# MAGIC 5. **VACUUM** & retention, **ANALYZE** for the optimizer
# MAGIC
# MAGIC > On serverless / UC managed tables, **Predictive Optimization** can run OPTIMIZE/VACUUM/ANALYZE
# MAGIC > automatically; running them here shows you understand what it does.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

S, G = f"{CATALOG}.{SILVER}", f"{CATALOG}.{GOLD}"

# COMMAND ----------

# MAGIC %md ## 1 · History & time travel

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {S}.orders")
        .select("version", "timestamp", "operation", "operationMetrics.numTargetRowsInserted",
                "operationMetrics.numTargetRowsUpdated"))

# COMMAND ----------

# order statuses right after the first load (v1; v0 is the empty CREATE) vs now
display(spark.sql(f"""
  SELECT 'v1 (first load)' AS version, status, count(*) AS orders FROM {S}.orders VERSION AS OF 1 GROUP BY status
  UNION ALL
  SELECT 'current', status, count(*) FROM {S}.orders GROUP BY status
  ORDER BY version, status"""))

# COMMAND ----------

# MAGIC %md ## 2 · Simulate an accident, then RESTORE

# COMMAND ----------

before = spark.sql(f"DESCRIBE HISTORY {G}.dim_product LIMIT 1").first()["version"]
spark.sql(f"UPDATE {G}.dim_product SET unit_price = 0 WHERE category = 'Electronics'")   # oops!
print("zero-priced products after accident:", spark.sql(f"SELECT count(*) FROM {G}.dim_product WHERE unit_price = 0").first()[0])

spark.sql(f"RESTORE TABLE {G}.dim_product TO VERSION AS OF {before}")
print("after RESTORE:", spark.sql(f"SELECT count(*) FROM {G}.dim_product WHERE unit_price = 0").first()[0])

# COMMAND ----------

# MAGIC %md ## 3 · Change Data Feed on the SCD2 dimension

# COMMAND ----------

display(spark.sql(f"""
  SELECT _change_type, count(*) AS rows
  FROM table_changes('{S}.dim_customer', 1)
  GROUP BY _change_type"""))

# COMMAND ----------

# MAGIC %md ## 4 · OPTIMIZE, clustering & data skipping

# COMMAND ----------

for t in [f"{S}.orders", f"{S}.order_items", f"{S}.dim_customer", f"{G}.fact_sales"]:
    m = spark.sql(f"OPTIMIZE {t}").select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
    d = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes", "clusteringColumns").first()
    print(f"{t:<45} files +{m[0]}/-{m[1]} -> {d.numFiles} files, {d.sizeInBytes/1e6:.1f} MB, clustered by {d.clusteringColumns}")

# COMMAND ----------

# Query that benefits from clustering on order_date_key (check 'files pruned' in the query profile)
display(spark.sql(f"""
  SELECT order_date_key, sum(net_amount) AS revenue
  FROM {G}.fact_sales WHERE order_date_key BETWEEN 20261025 AND 20261031 GROUP BY 1 ORDER BY 1"""))

# COMMAND ----------

# MAGIC %md ## 5 · Statistics & VACUUM

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {G}.fact_sales COMPUTE STATISTICS FOR ALL COLUMNS")
# Default retention is 7 days: VACUUM removes files no longer referenced AND older than that.
# Never lower retention below your longest-running reader / time-travel requirement.
display(spark.sql(f"VACUUM {S}.orders DRY RUN"))
spark.sql(f"VACUUM {S}.orders")
