"""
ShopStream synthetic data generator
===================================

Generates 3 daily "drops" of raw source-system files for an Indian
omnichannel retailer (ShopStream). Data is deliberately dirty so the
pipeline has real problems to solve:

  * duplicate records (exact + replayed events)
  * nulls in key / mandatory columns
  * invalid emails, phone numbers, negative quantities / amounts
  * inconsistent casing & whitespace, mixed timestamp formats
  * orphan foreign keys (orders for customers that don't exist)
  * late-arriving data (day-3 file contains day-1 orders)
  * order status lifecycle (same order arrives several times)
  * CDC feed for customers (INSERT / UPDATE / DELETE)  -> SCD Type 2
  * schema evolution (day-3 orders add `coupon_code` + `device` fields)

Usage (locally):
    python generate_data.py --out ../data/landing --days 3

Usage (inside Databricks - writes straight into a UC Volume):
    python generate_data.py --out /Volumes/shopstream/landing/raw_files --days 1
    (or run notebooks/00b_generate_data.py)

Output layout:
    <out>/customers/customers_day{N}.csv
    <out>/products/products_day{N}.csv
    <out>/orders/orders_day{N}.json        (JSON lines, nested items array)
    <out>/payments/payments_day{N}.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta

SEED = 42
BASE_DATE = datetime(2026, 10, 1)          # history starts here
DAY1_DATE = datetime(2026, 10, 31)         # first daily drop "as of" date

CITIES = [
    ("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Nagpur", "Maharashtra"),
    ("Bengaluru", "Karnataka"), ("Mysuru", "Karnataka"), ("Chennai", "Tamil Nadu"),
    ("Coimbatore", "Tamil Nadu"), ("Hyderabad", "Telangana"), ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"), ("Surat", "Gujarat"),
    ("Jaipur", "Rajasthan"), ("Lucknow", "Uttar Pradesh"), ("Kochi", "Kerala"),
    ("Indore", "Madhya Pradesh"), ("Bhopal", "Madhya Pradesh"), ("Chandigarh", "Chandigarh"),
]
FIRST = ["Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna",
         "Ishaan", "Rohan", "Ananya", "Diya", "Aditi", "Isha", "Kavya", "Meera", "Priya",
         "Riya", "Saanvi", "Neha", "Rahul", "Sneha", "Pooja", "Amit", "Karan", "Nikhil",
         "Shreya", "Tanvi", "Varun", "Yash"]
LAST = ["Sharma", "Verma", "Patel", "Iyer", "Nair", "Reddy", "Gupta", "Joshi", "Kulkarni",
        "Deshmukh", "Mehta", "Shah", "Rao", "Das", "Banerjee", "Singh", "Khan", "Pillai",
        "Chatterjee", "Agarwal", "Vaidya", "Menon"]
TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
CATALOG = {
    "Electronics": {"Mobiles": ["Samsung", "Xiaomi", "OnePlus", "Apple"],
                    "Audio": ["boAt", "Sony", "JBL"],
                    "Laptops": ["Dell", "HP", "Lenovo"]},
    "Fashion": {"Men Apparel": ["Peter England", "Allen Solly", "Levis"],
                "Women Apparel": ["Biba", "W", "FabIndia"],
                "Footwear": ["Bata", "Puma", "Nike"]},
    "Home": {"Kitchen": ["Prestige", "Pigeon", "Borosil"],
             "Furnishing": ["Bombay Dyeing", "Spaces"]},
    "Grocery": {"Staples": ["Tata", "Aashirvaad", "Fortune"],
                "Beverages": ["Tata Tea", "Bru", "Nescafe"]},
    "Beauty": {"Skincare": ["Lakme", "Mamaearth", "Nivea"]},
}
PRICE_BANDS = {"Mobiles": (8000, 90000), "Audio": (800, 25000), "Laptops": (35000, 150000),
               "Men Apparel": (600, 4000), "Women Apparel": (700, 5000), "Footwear": (900, 9000),
               "Kitchen": (300, 8000), "Furnishing": (500, 6000), "Staples": (60, 900),
               "Beverages": (120, 800), "Skincare": (150, 1500)}
CHANNELS = ["WEB", "APP", "STORE"]
PAY_METHODS = ["UPI", "CARD", "NETBANKING", "COD", "WALLET"]


def ts_fmt(dt: datetime, messy: bool = False) -> str:
    """Return ISO timestamp; occasionally a different source format (dirty)."""
    if messy:
        return random.choice([dt.strftime("%d/%m/%Y %H:%M:%S"),   # dd/MM/yyyy
                              dt.strftime("%Y-%m-%dT%H:%M:%SZ")])
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def rand_dt(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


# --------------------------------------------------------------------------- customers
def make_customer(cid: int, as_of: datetime) -> dict:
    fn, ln = random.choice(FIRST), random.choice(LAST)
    city, state = random.choice(CITIES)
    email = f"{fn.lower()}.{ln.lower()}{cid}@{random.choice(['gmail.com', 'yahoo.in', 'outlook.com'])}"
    return {
        "customer_id": f"C{cid:06d}",
        "first_name": fn,
        "last_name": ln,
        "email": email,
        "phone": f"+91{random.randint(6000000000, 9999999999)}",
        "city": city,
        "state": state,
        "signup_date": rand_dt(BASE_DATE - timedelta(days=700), BASE_DATE).strftime("%Y-%m-%d"),
        "loyalty_tier": random.choices(TIERS, weights=[50, 30, 15, 5])[0],
        "op": "INSERT",
        "updated_at": ts_fmt(as_of - timedelta(hours=random.randint(1, 20))),
    }


def dirty_customer(row: dict) -> dict:
    r = dict(row)
    issue = random.choice(["email", "case", "phone", "null_name", "space"])
    if issue == "email":
        r["email"] = r["email"].replace("@", "_at_")            # invalid email
    elif issue == "case":
        r["city"] = r["city"].upper()                             # inconsistent case
    elif issue == "phone":
        r["phone"] = "12345"                                      # invalid phone
    elif issue == "null_name":
        r["first_name"] = ""
    else:
        r["city"] = f"  {r['city'].lower()} "                     # whitespace + lower
    return r


# --------------------------------------------------------------------------- products
def make_products() -> list[dict]:
    rows, pid = [], 1
    for cat, subs in CATALOG.items():
        for sub, brands in subs.items():
            for brand in brands:
                for variant in range(random.randint(4, 7)):
                    lo, hi = PRICE_BANDS[sub]
                    price = round(random.uniform(lo, hi), -1)
                    rows.append({
                        "product_id": f"P{pid:05d}",
                        "product_name": f"{brand} {sub} Model-{variant + 1}",
                        "category": cat,
                        "sub_category": sub,
                        "brand": brand,
                        "unit_price": f"{price:.2f}",
                        "cost_price": f"{price * random.uniform(0.55, 0.8):.2f}",
                        "is_active": "Y",
                        "updated_at": ts_fmt(DAY1_DATE - timedelta(days=30)),
                    })
                    pid += 1
    return rows


# --------------------------------------------------------------------------- orders
def make_order(oid: int, cust_ids: list[str], products: list[dict], when: datetime,
               status: str, schema_v2: bool = False) -> dict:
    n_items = random.choices([1, 2, 3, 4], weights=[55, 25, 13, 7])[0]
    items = []
    for p in random.sample(products, n_items):
        items.append({
            "product_id": p["product_id"],
            "quantity": random.choices([1, 2, 3], weights=[75, 20, 5])[0],
            "unit_price": float(p["unit_price"]),
            "discount_pct": random.choice([0, 0, 0, 5, 10, 15, 20]),
        })
    city, state = random.choice(CITIES)
    o = {
        "order_id": f"O{oid:08d}",
        "customer_id": random.choice(cust_ids),
        "order_ts": ts_fmt(when, messy=random.random() < 0.03),
        "event_ts": ts_fmt(when),
        "status": status,
        "channel": random.choice(CHANNELS),
        "shipping_address": {"city": city, "state": state,
                             "pincode": str(random.randint(110001, 855999))},
        "items": items,
    }
    if schema_v2:                                              # schema evolution
        o["coupon_code"] = random.choice([None, None, "DIWALI10", "FESTIVE20", "NEWUSER"])
        o["device"] = random.choice(["android", "ios", "desktop", None])
    return o


def order_total(o: dict) -> float:
    return round(sum(i["quantity"] * i["unit_price"] * (1 - i["discount_pct"] / 100)
                     for i in o["items"]), 2)


def make_payment(pid: int, o: dict, when: datetime) -> dict:
    method = random.choices(PAY_METHODS, weights=[45, 25, 8, 15, 7])[0]
    amount = order_total(o)
    status = "SUCCESS"
    r = random.random()
    if r < 0.03:
        status = "FAILED"
    elif r < 0.05:
        amount = round(amount * random.uniform(0.5, 0.95), 2)   # under-payment mismatch
    return {
        "payment_id": f"PAY{pid:09d}",
        "order_id": o["order_id"],
        "payment_method": method,
        "amount": f"{amount:.2f}",
        "payment_status": status,
        "paid_at": ts_fmt(when + timedelta(minutes=random.randint(0, 30))),
    }


# --------------------------------------------------------------------------- writers
def write_csv(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# --------------------------------------------------------------------------- main
def generate(out: str, days: int = 3, only_day: int | None = None) -> dict:
    random.seed(SEED)
    stats = {}
    products = make_products()
    customers = {f"C{i:06d}": make_customer(i, DAY1_DATE) for i in range(1, 2001)}
    next_cust, next_order, next_pay = 2001, 1, 1
    open_orders: list[dict] = []          # orders that can still progress in status

    for day in range(1, days + 1):
        as_of = DAY1_DATE + timedelta(days=day - 1)
        cust_rows, prod_rows, order_rows, pay_rows = [], [], [], []

        # ---------------- customers
        if day == 1:
            cust_rows = [dirty_customer(c) if random.random() < 0.04 else dict(c)
                         for c in customers.values()]
            cust_rows += random.sample(cust_rows, 25)                      # exact duplicates
            bad = dict(random.choice(cust_rows)); bad["customer_id"] = ""  # null key
            cust_rows.append(bad)
        else:
            for cid in random.sample(list(customers), 120):               # UPDATEs
                c = dict(customers[cid])
                change = random.choice(["city", "tier", "email", "phone"])
                if change == "city":
                    c["city"], c["state"] = random.choice(CITIES)
                elif change == "tier":
                    c["loyalty_tier"] = TIERS[min(TIERS.index(c["loyalty_tier"]) + 1, 3)]
                elif change == "email":
                    c["email"] = c["email"].replace("@", ".new@")
                else:
                    c["phone"] = f"+91{random.randint(6000000000, 9999999999)}"
                c["op"] = "UPDATE"
                c["updated_at"] = ts_fmt(as_of - timedelta(hours=random.randint(1, 20)))
                customers[cid] = c
                cust_rows.append(c)
            for _ in range(80):                                            # INSERTs
                c = make_customer(next_cust, as_of)
                customers[c["customer_id"]] = c
                cust_rows.append(c)
                next_cust += 1
            for cid in random.sample(list(customers), 5):                  # DELETEs
                c = dict(customers[cid]); c["op"] = "DELETE"
                c["updated_at"] = ts_fmt(as_of)
                cust_rows.append(c)
            # same customer updated twice in one file -> must keep latest
            twice = dict(cust_rows[0]); twice["loyalty_tier"] = "PLATINUM"
            twice["updated_at"] = ts_fmt(as_of - timedelta(minutes=5))
            cust_rows.append(twice)

        # ---------------- products (SCD1 price changes on later days)
        if day == 1:
            prod_rows = products
        else:
            for p in random.sample(products, 15):
                p["unit_price"] = f"{float(p['unit_price']) * random.choice([0.9, 0.95, 1.05, 1.1]):.2f}"
                p["updated_at"] = ts_fmt(as_of - timedelta(hours=3))
                prod_rows.append(dict(p))
            if day == 3:
                p = random.choice(products); p["is_active"] = "N"
                p["updated_at"] = ts_fmt(as_of); prod_rows.append(dict(p))

        # ---------------- orders
        cust_ids = list(customers)
        schema_v2 = day >= 3
        if day == 1:
            n_new, window_start = 8000, BASE_DATE
        else:
            n_new, window_start = 700, as_of - timedelta(days=1)
        for _ in range(n_new):
            when = rand_dt(window_start, as_of)
            o = make_order(next_order, cust_ids, products, when, "PLACED", schema_v2)
            next_order += 1
            order_rows.append(o)
            if random.random() < 0.97:
                pay_rows.append(make_payment(next_pay, o, when)); next_pay += 1
            open_orders.append(o)

        # status progression for previously seen orders (same order_id, newer event_ts)
        progressed = []
        for o in random.sample(open_orders, min(len(open_orders), 1500 if day > 1 else 3000)):
            nxt = {"PLACED": "SHIPPED", "SHIPPED": "DELIVERED"}.get(o["status"])
            if not nxt:
                continue
            if random.random() < 0.06:
                nxt = "CANCELLED"
            o2 = dict(o); o2["status"] = nxt
            ev = datetime.strptime(o["event_ts"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=random.randint(6, 30))
            o2["event_ts"] = ts_fmt(min(ev, as_of))
            if schema_v2 and "coupon_code" not in o2:
                o2["coupon_code"] = None; o2["device"] = None
            progressed.append(o2)
        order_rows += progressed
        open_orders = [o for o in open_orders if o["status"] not in ("DELIVERED", "CANCELLED")]
        idx = {o["order_id"]: o for o in open_orders}
        for o2 in progressed:
            if o2["status"] in ("DELIVERED", "CANCELLED"):
                idx.pop(o2["order_id"], None)
            elif o2["order_id"] in idx:
                idx[o2["order_id"]] = o2
        open_orders = list(idx.values())

        # dirt: replayed duplicates, negative qty, orphan customer, null ts, late data
        order_rows = [dict(o) for o in order_rows]          # don't corrupt in-memory state
        order_rows +=[dict(o) for o in random.sample(order_rows, 40)]
        for o in random.sample(order_rows, 12):
            o["items"] = [dict(i) for i in o["items"]]
            o["items"][0]["quantity"] = -1
        for o in random.sample(order_rows, 10):
            o["customer_id"] = f"C{random.randint(900000, 999999):06d}"   # orphan FK
        for o in random.sample(order_rows, 6):
            o["order_ts"] = None
        if day == 3:                                                       # late-arriving
            for _ in range(50):
                when = rand_dt(BASE_DATE, BASE_DATE + timedelta(days=3))
                o = make_order(next_order, cust_ids, products, when, "DELIVERED", True)
                next_order += 1
                order_rows.append(o)
                pay_rows.append(make_payment(next_pay, o, when)); next_pay += 1
        random.shuffle(order_rows)

        pay_rows += random.sample(pay_rows, 15)                           # duplicate payments
        for p in random.sample(pay_rows, 5):
            p["amount"] = "-" + p["amount"]                                # negative amount

        if only_day is None or only_day == day:
            write_csv(f"{out}/customers/customers_day{day}.csv", cust_rows)
            if prod_rows:
                write_csv(f"{out}/products/products_day{day}.csv", prod_rows)
            write_jsonl(f"{out}/orders/orders_day{day}.json", order_rows)
            write_csv(f"{out}/payments/payments_day{day}.csv", pay_rows)
        stats[day] = {"customers": len(cust_rows), "products": len(prod_rows),
                      "order_events": len(order_rows), "payments": len(pay_rows)}
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="../data/landing")
    ap.add_argument("--days", type=int, default=3, help="number of daily drops to simulate")
    ap.add_argument("--only-day", type=int, default=None,
                    help="write files only for this day (simulate incremental arrival)")
    a = ap.parse_args()
    for d, s in generate(a.out, a.days, a.only_day).items():
        print(f"day {d}: {s}")
