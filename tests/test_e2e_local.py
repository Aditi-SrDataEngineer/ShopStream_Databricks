"""
End-to-end smoke test on the REAL generated data, runnable on a laptop (plain PySpark, no Delta).

It replays the 3 daily drops through the same transforms used in Databricks, emulates the
silver MERGE semantics in Python (Delta MERGE itself is exercised in Databricks), then runs
every gold SQL statement from src/shopstream/gold_sql.py and checks business invariants.

    python data_generator/generate_data.py --out data/landing --days 3
    pytest -q -s tests/test_e2e_local.py
"""
import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from shopstream import transforms as T          # noqa: E402
from shopstream.gold_sql import GOLD_TABLES, render  # noqa: E402

LANDING = os.path.join(os.path.dirname(__file__), "..", "data", "landing")
pytestmark = pytest.mark.skipif(not os.path.exists(f"{LANDING}/orders/orders_day3.json"),
                                reason="run data_generator first")


def read_day(spark, entity, day):
    path = f"{LANDING}/{entity}/{entity}_day{day}." + ("json" if entity == "orders" else "csv")
    if not os.path.exists(path):
        return None
    df = spark.read.json(path) if entity == "orders" else spark.read.option("header", True).csv(path)
    return df.withColumn("_source_file", F.lit(path))


def scd2_apply(versions: dict, updates: list) -> None:
    """Python mirror of the SCD2 MERGE in notebooks/02_silver_transform.py."""
    for u in updates:
        hist = versions.setdefault(u["customer_id"], [])
        cur = next((v for v in hist if v["is_current"]), None)
        if u["op"] == "DELETE":
            if cur and u["updated_at_ts"] > cur["effective_from"]:
                cur.update(is_current=False, is_deleted=True, effective_to=u["updated_at_ts"])
            continue
        if cur is None:
            signup = datetime.combine(u["signup_date"], datetime.min.time()) if u["signup_date"] else u["updated_at_ts"]
            hist.append({**u, "effective_from": min(u["updated_at_ts"], signup), "effective_to": None,
                         "is_current": True, "is_deleted": False})
        elif u["row_hash"] != cur["row_hash"] and u["updated_at_ts"] > cur["effective_from"]:
            cur.update(is_current=False, effective_to=u["updated_at_ts"])
            hist.append({**u, "effective_from": u["updated_at_ts"], "effective_to": None,
                         "is_current": True, "is_deleted": False})


def test_end_to_end(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    versions, quarantined = {}, {}
    orders_valid, products_valid, payments_valid = [], [], []

    for day in (1, 2, 3):
        c = read_day(spark, "customers", day)
        v, bad = T.apply_dq_rules(T.clean_customers(c), T.CUSTOMER_DQ_RULES)
        quarantined["customers"] = quarantined.get("customers", 0) + bad.count()
        latest = T.add_row_hash(T.dedupe_latest(v, ["customer_id"], "updated_at_ts"), T.CUSTOMER_SCD2_COLS)
        scd2_apply(versions, [r.asDict() for r in latest.drop("_source_file").collect()])

        p = read_day(spark, "products", day)
        if p is not None:
            v, bad = T.apply_dq_rules(T.clean_products(p), T.PRODUCT_DQ_RULES)
            products_valid.append(v)

        o = T.clean_orders(read_day(spark, "orders", day))
        v, bad = T.apply_dq_rules(o, T.ORDER_DQ_RULES)
        quarantined["orders"] = quarantined.get("orders", 0) + bad.count()
        orders_valid.append(v)

        pay = read_day(spark, "payments", day)
        v, bad = T.apply_dq_rules(T.clean_payments(pay), T.PAYMENT_DQ_RULES)
        quarantined["payments"] = quarantined.get("payments", 0) + bad.count()
        payments_valid.append(v)

    # ---- silver (state after 3 days, equivalent to the incremental MERGEs)
    rows = [dict(customer_sk=i + 1, **{k: v[k] for k in (
        "customer_id", "first_name", "last_name", "email", "phone", "city", "state", "signup_date",
        "loyalty_tier", "row_hash", "effective_from", "effective_to", "is_current", "is_deleted")})
        for i, v in enumerate(x for h in versions.values() for x in h)]
    spark.createDataFrame(rows).write.mode("overwrite").saveAsTable("silver.dim_customer")

    T.dedupe_latest(T.union_all(products_valid), ["product_id"], "updated_at_ts") \
        .write.mode("overwrite").saveAsTable("silver.products")

    all_orders = T.union_all(orders_valid)
    all_orders.select("order_id", "status", "event_ts").dropDuplicates(["order_id", "status"]) \
        .write.mode("overwrite").saveAsTable("silver.order_status_history")
    latest_orders = T.dedupe_latest(all_orders, ["order_id"], "event_ts")
    latest_orders.drop("items").write.mode("overwrite").saveAsTable("silver.orders")
    T.explode_order_items(latest_orders).write.mode("overwrite").saveAsTable("silver.order_items")
    T.dedupe_latest(T.union_all(payments_valid), ["payment_id"], "paid_at") \
        .write.mode("overwrite").saveAsTable("silver.payments")

    # ---- gold: run every production SQL statement
    counts = {}
    for t in GOLD_TABLES:
        df = spark.sql(render(t["sql"], "silver", "gold"))
        df.write.mode("overwrite").saveAsTable(f"gold.{t['name']}")
        counts[t["name"]] = spark.table(f"gold.{t['name']}").count()
    c360 = T.rfm_segment(spark.table("gold.customer_base"), "2026-11-02")
    seg = {r.segment: r["count"] for r in c360.groupBy("segment").count().collect()}

    # ---- invariants
    dimc = spark.table("silver.dim_customer")
    assert dimc.filter("is_current").groupBy("customer_id").count().filter("count > 1").count() == 0, \
        "a customer has more than one current version"
    assert dimc.filter("is_deleted").count() > 0, "CDC deletes not applied"
    assert dimc.groupBy("customer_id").count().filter("count > 1").count() > 50, "SCD2 history missing"
    assert spark.table("silver.orders").groupBy("order_id").count().filter("count > 1").count() == 0
    assert spark.table("silver.order_items").filter("quantity <= 0").count() == 0
    fact = spark.table("gold.fact_sales")
    assert fact.count() == spark.table("silver.order_items").count(), "fact lost or duplicated lines"
    assert fact.filter("customer_sk = -1").count() > 0, "orphans should map to Unknown member"
    assert set(r.recon_status for r in spark.table("gold.payment_reconciliation")
               .select("recon_status").distinct().collect()) >= {"MATCHED", "UNDERPAID", "NO_PAYMENT", "CANCELLED"}
    assert spark.table("silver.orders").filter("coupon_code IS NOT NULL").count() > 0, "schema evolution lost"

    rev = spark.table("gold.agg_daily_sales").agg(F.sum("net_revenue")).first()[0]
    print("\n=== E2E summary ===")
    print("quarantined:", quarantined)
    print("dim_customer versions:", dimc.count(), "| current:", dimc.filter("is_current").count(),
          "| deleted:", dimc.filter("is_deleted").count())
    print("gold row counts:", counts)
    print("RFM segments:", seg)
    print(f"total net revenue (excl. cancelled): INR {float(rev):,.0f}")
    spark.table("gold.payment_reconciliation").groupBy("recon_status").count().orderBy("recon_status").show()
    spark.table("gold.agg_product_performance").orderBy(F.desc("revenue")).limit(5).show(truncate=False)
