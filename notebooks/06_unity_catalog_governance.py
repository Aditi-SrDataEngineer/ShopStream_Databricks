# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Unity Catalog governance
# MAGIC * Role-based **GRANTs** on catalog / schema / table
# MAGIC * **Column mask**: email & phone visible only to the `pii_readers` group
# MAGIC * **Row filter**: regional analysts only see their state(s)
# MAGIC * **Tags & comments** for discovery; **lineage** is captured automatically (Catalog Explorer → Lineage)
# MAGIC
# MAGIC Create the groups first (Settings → Identity & access → Groups): `data_engineers`, `analysts`,
# MAGIC `pii_readers`, `analysts_west`. On Free Edition you can still run everything; grants to
# MAGIC groups that don't exist will error, so the GRANT cell is wrapped.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

S, G, F_ = f"{CATALOG}.{SILVER}", f"{CATALOG}.{GOLD}", f"{CATALOG}.{OPS}"

grants = [
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `analysts`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.{GOLD} TO `analysts`",      # analysts: gold only
    f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG} TO `data_engineers`",
]
for g in grants:
    try:
        spark.sql(g); print("OK  ", g)
    except Exception as e:
        print("SKIP", g, "->", type(e).__name__)

# COMMAND ----------

# MAGIC %md ### Column masking (PII)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {F_}.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
            ELSE concat(left(email, 2), '****@', split_part(email, '@', 2)) END
""")
spark.sql(f"ALTER TABLE {G}.dim_customer ALTER COLUMN email SET MASK {F_}.mask_email")
display(spark.sql(f"SELECT customer_id, email FROM {G}.dim_customer LIMIT 5"))

# COMMAND ----------

# MAGIC %md ### Row-level security

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {F_}.state_filter(state STRING)
RETURN is_account_group_member('data_engineers')
    OR is_member('admins')                      -- workspace admins (you, on Free Edition)
    OR (is_account_group_member('analysts_west') AND state IN ('Maharashtra', 'Gujarat', 'Rajasthan'))
""")
spark.sql(f"ALTER TABLE {G}.customer_360 SET ROW FILTER {F_}.state_filter ON (state)")
display(spark.sql(f"SELECT state, count(*) FROM {G}.customer_360 GROUP BY state ORDER BY 2 DESC"))

# COMMAND ----------

# MAGIC %md ### Tags (searchable in Catalog Explorer)

# COMMAND ----------

spark.sql(f"ALTER TABLE {G}.dim_customer ALTER COLUMN email SET TAGS ('pii' = 'email')")
spark.sql(f"ALTER TABLE {G}.fact_sales SET TAGS ('domain' = 'sales', 'tier' = 'gold', 'owner' = 'data-eng')")

# To undo:  ALTER TABLE ... ALTER COLUMN email DROP MASK;   ALTER TABLE ... DROP ROW FILTER;
# NOTE: notebook 03 detaches these policies before rebuilding gold, so this notebook runs as the
# last task of the job to re-apply them every time.
