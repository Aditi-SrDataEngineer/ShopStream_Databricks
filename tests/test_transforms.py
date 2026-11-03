"""
Unit tests for src/shopstream/transforms.py
Run locally:   pip install pyspark==3.5.3 pytest && pytest -q tests/
Run in Databricks: open tests/run_tests notebook (it calls pytest.main).
"""
import os
import sys
from datetime import datetime

import time

import pytest
from pyspark.sql import Row, SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from shopstream import transforms as T  # noqa: E402


def test_parse_ts_handles_all_source_formats(spark):
    df = spark.createDataFrame([("2026-10-31 10:15:00",), ("31/10/2026 10:15:00",),
                                ("2026-10-31T10:15:00Z",), ("garbage",), (None,)], ["ts"])
    out = [r[0] for r in df.select(T.parse_ts("ts")).collect()]
    assert out[:3] == [datetime(2026, 10, 31, 10, 15)] * 3
    assert out[3] is None and out[4] is None


def test_dedupe_latest_keeps_newest(spark):
    df = spark.createDataFrame([("A", 1, "old"), ("A", 3, "new"), ("B", 2, "only")], ["k", "v", "tag"])
    res = {r.k: r.tag for r in T.dedupe_latest(df, ["k"], "v").collect()}
    assert res == {"A": "new", "B": "only"}


def test_apply_dq_rules_splits_and_tags(spark):
    df = spark.createDataFrame([(1, "a@b.com"), (-1, "bad"), (2, None)], ["qty", "email"])
    valid, quar = T.apply_dq_rules(df, {"qty_pos": "qty > 0", "email_ok": f"email RLIKE '{T.EMAIL_REGEX}'"})
    assert valid.count() == 1
    rules = {r.qty: sorted(r.dq_failed_rules) for r in quar.collect()}
    assert rules[-1] == ["email_ok", "qty_pos"]
    assert rules[2] == ["email_ok"]          # NULL treated as a failure, not silently passed


def test_clean_customers_standardises(spark):
    raw = spark.createDataFrame([Row(customer_id=" C000001 ", first_name="", last_name="sharma",
                                     email="A.B@GMAIL.COM ", phone="12345", city="  nagpur ",
                                     state="maharashtra", signup_date="2025-01-01", loyalty_tier="gold",
                                     op="update", updated_at="31/10/2026 09:00:00", _source_file="f")])
    r = T.clean_customers(raw).first()
    assert (r.customer_id, r.city, r.email, r.loyalty_tier, r.op) == ("C000001", "Nagpur", "a.b@gmail.com", "GOLD", "UPDATE")
    assert r.phone is None and r.first_name is None
    assert r.updated_at_ts == datetime(2026, 10, 31, 9, 0)


def test_row_hash_changes_only_on_tracked_cols(spark):
    df = spark.createDataFrame([("C1", "Pune", "x"), ("C1", "Pune", "y"), ("C1", "Nagpur", "x")], ["id", "city", "noise"])
    h = [r.row_hash for r in T.add_row_hash(df, ["id", "city"]).collect()]
    assert h[0] == h[1] and h[0] != h[2]


def test_explode_order_items_amounts(spark):
    orders = spark.createDataFrame([Row(order_id="O1", items=[
        Row(product_id="P1", quantity=2, unit_price=100.0, discount_pct=10),
        Row(product_id="P2", quantity=1, unit_price=50.0, discount_pct=0)])])
    lines = {r.product_id: r for r in T.explode_order_items(orders).collect()}
    assert float(lines["P1"].net_amount) == 180.0 and lines["P1"].line_no == 1
    assert float(lines["P2"].gross_amount) == 50.0


def test_clean_orders_tolerates_schema_evolution(spark):
    base = Row(order_id="O1", customer_id="C1", order_ts="2026-10-01 10:00:00", event_ts="2026-10-01 10:00:00",
               status="placed", channel="app", shipping_address=Row(city="pune", state="maharashtra", pincode="411001"),
               items=[Row(product_id="P1", quantity=1, unit_price=1.0, discount_pct=0)], _source_file="f")
    r = T.clean_orders(spark.createDataFrame([base])).first()          # v1 schema: no coupon/device
    assert r.coupon_code is None and r.status == "PLACED" and r.ship_city == "Pune"


def test_clean_payments_negative_and_garbage_amounts(spark):
    df = spark.createDataFrame([("PAY1", "O1", "upi", "-10.00", "success", "2026-10-01 10:00:00"),
                                ("PAY2", "O2", "card", "abc", "success", "2026-10-01 10:00:00")],
                               ["payment_id", "order_id", "payment_method", "amount", "payment_status", "paid_at"])
    cleaned = T.clean_payments(df)
    valid, quar = T.apply_dq_rules(cleaned, T.PAYMENT_DQ_RULES)
    assert valid.count() == 0 and quar.count() == 2      # ANSI-safe: 'abc' -> NULL, not a crash


def test_rfm_segments(spark):
    # C10 ordered most recently, most often, spent most -> Champion. C1 is the opposite.
    rows = [(f"C{i}", datetime(2026, 10, 20 + i).date(), i, float(i * 1000)) for i in range(1, 11)]
    df = spark.createDataFrame(rows, ["customer_id", "last_order_date", "order_count", "lifetime_value"])
    out = {r.customer_id: r.segment for r in T.rfm_segment(df, "2026-11-02").collect()}
    assert out["C10"] == "Champions"
    assert out["C1"] == "Hibernating"


def test_order_with_negative_line_is_quarantined(spark):
    item = lambda q: Row(product_id="P1", quantity=q, unit_price=10.0, discount_pct=0.0)  # noqa: E731
    mk = lambda oid, q: Row(order_id=oid, customer_id="C1", order_ts="2026-10-01 10:00:00",  # noqa: E731
                            event_ts="2026-10-01 10:00:00", status="PLACED", channel="WEB",
                            shipping_address=Row(city="a", state="b", pincode="1"),
                            items=[item(1), item(q)], _source_file="f")
    orders = T.clean_orders(spark.createDataFrame([mk("O1", 2), mk("O2", -1)]))
    valid, quar = T.apply_dq_rules(orders, T.ORDER_DQ_RULES)
    assert [r.order_id for r in valid.collect()] == ["O1"]
    assert quar.first().dq_failed_rules == ["items_valid"]
