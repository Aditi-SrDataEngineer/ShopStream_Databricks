# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Land source files into the Volume
# MAGIC Simulates source systems dropping files every day.
# MAGIC * `day = 1` → initial full load (30 days of order history)
# MAGIC * `day = 2`, `day = 3` → incremental drops (CDC customers, price changes, new orders,
# MAGIC   status updates, late-arriving data, **schema evolution** on day 3)
# MAGIC * `day = all` → land all three at once
# MAGIC
# MAGIC Run day 1 → full pipeline → day 2 → pipeline → day 3 → pipeline, to *see* incremental processing.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

dbutils.widgets.dropdown("day", "1", ["1", "2", "3", "all"], "Which drop to land")
DAY = dbutils.widgets.get("day")

gen_dir = os.path.abspath(os.path.join(os.getcwd(), "..", "data_generator"))
sys.path.insert(0, gen_dir)
import importlib, generate_data
importlib.reload(generate_data)

stats = generate_data.generate(RAW_VOLUME, days=3, only_day=None if DAY == "all" else int(DAY))
for d, s in stats.items():
    if DAY == "all" or int(DAY) == d:
        print(f"landed day {d}: {s}")

# COMMAND ----------

display(dbutils.fs.ls(f"{RAW_VOLUME}/orders"))
