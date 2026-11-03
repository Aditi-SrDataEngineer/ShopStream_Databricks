# ShopStream Lakehouse — end-to-end Databricks Data Engineering project

**Business problem.** ShopStream is an Indian omnichannel retailer (web, app, stores). Four source
systems drop files every day: a customer CRM (as a CDC feed), a product master, an order-events
stream (nested JSON) and a payment gateway. The data is late, duplicated, inconsistently formatted,
and its schema changes. Leadership wants one trusted place for revenue, customer, product, finance
and fulfilment numbers.

**What you build.** A governed medallion lakehouse on Databricks that ingests every drop
incrementally, cleans and validates it, keeps customer history (SCD Type 2), publishes a star schema
plus business marts, blocks bad data with quality gates, and runs itself on a schedule — built twice:
once imperatively (notebooks + Workflows) and once declaratively (Lakeflow Declarative Pipelines).

## Architecture

```
 Source drops (UC Volume)          BRONZE (append-only)        SILVER (clean, conformed)          GOLD (star schema + marts)
 ───────────────────────          ──────────────────          ─────────────────────────          ──────────────────────────
 customers_dayN.csv  (CDC) ─┐     customers_raw  ─────────▶  dim_customer   (SCD2)  ───┐      dim_customer (+ Unknown -1)
 products_dayN.csv          ├─AL─ products_raw   ─────────▶  products       (SCD1)  ───┤      dim_product, dim_date
 orders_dayN.json (nested)  │     orders_raw     ─────────▶  orders (latest state)  ───┼────▶ fact_sales (point-in-time SCD2 join)
 payments_dayN.csv         ─┘     payments_raw   ─────────▶  order_items, status_hist ─┤      agg_daily_sales, agg_product_performance
                                  (+ _rescued_data,           payments                 ─┘      customer_360 (RFM), payment_reconciliation
   AL = Auto Loader                 file lineage)             ops.dq_quarantine (rejects)      agg_fulfilment_sla, vw_sales
                                                                                ▼
                                                  ops.pipeline_audit · ops.dq_results · DQ gates fail the job
```

## Databricks capabilities demonstrated

| Area | Feature | Where |
|---|---|---|
| Ingestion | Auto Loader, schema inference + hints, **schema evolution**, rescued data, `availableNow` | `01_bronze_autoloader` |
| Storage | Delta Lake, MERGE, Change Data Feed, time travel, RESTORE, OPTIMIZE, VACUUM, ANALYZE | `02`, `05` |
| Modelling | **SCD Type 2** (hand-written MERGE + `apply_changes`), SCD1, star schema, point-in-time joins, unknown member | `02`, `03`, pipeline |
| Streaming | Structured Streaming from Delta, `foreachBatch` upserts, idempotent replays, late data | `02` |
| Data quality | Rule engine with quarantine + reasons, Lakeflow expectations, blocking DQ gates | `transforms.py`, `04`, pipeline |
| Performance | **Liquid clustering**, file compaction, data skipping, Photon/serverless | `03`, `05` |
| Orchestration | Lakeflow Jobs with parallel task DAG, parameters, schedules, file-arrival trigger, alerts | `resources/jobs.yml` |
| Declarative ETL | Lakeflow Declarative Pipelines (DLT), AUTO CDC, materialized views | `pipelines/` |
| Governance | Unity Catalog, Volumes, grants, **column masks**, **row filters**, tags, lineage | `00`, `06` |
| DevOps | Databricks Asset Bundles (dev/prod targets), unit + end-to-end tests, shared library | `databricks.yml`, `tests/` |
| BI | Databricks SQL / AI-BI dashboard queries, BI view | `sql/`, `03` |

## Repository layout

```
shopstream-lakehouse/
├── databricks.yml                 # Asset Bundle (dev / prod targets)
├── resources/jobs.yml             # daily DAG, weekly maintenance, file-arrival trigger
├── resources/pipeline.yml         # Lakeflow Declarative Pipeline
├── data_generator/generate_data.py# deterministic, deliberately dirty source data (3 daily drops)
├── data/landing/                  # pre-generated sample of all 3 drops (~7 MB)
├── src/shopstream/transforms.py   # cleaning, DQ rules, dedupe, explode, RFM  (unit-tested)
├── src/shopstream/gold_sql.py     # all gold SQL (unit-tested)
├── notebooks/
│   ├── _config.py                 # catalog widget, paths, helpers
│   ├── 00_setup.py                # catalog, schemas, volumes, ops tables
│   ├── 00b_generate_data.py       # land day 1 / 2 / 3 into the Volume
│   ├── 01_bronze_autoloader.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_build.py
│   ├── 04_data_quality_checks.py
│   ├── 05_delta_maintenance.py
│   └── 06_unity_catalog_governance.py
├── pipelines/shopstream_lakeflow_pipeline.py
├── sql/dashboard_queries.sql
└── tests/ (conftest, test_transforms, test_e2e_local, run_tests notebook)
```

## The data (and the problems planted in it)

| File | Grain | Day 1 | Day 2 / Day 3 | Planted problems |
|---|---|---|---|---|
| `customers_dayN.csv` | CDC row | 2,026 rows (full load) | ~206 rows: 120 updates, 80 inserts, 5 deletes | exact duplicates, empty key, invalid emails/phones, CITY casing & whitespace, same customer updated twice in one file |
| `products_dayN.csv` | product | 175 products | 15–16 price changes; 1 product deactivated (day 3) | — (SCD1 changes) |
| `orders_dayN.json` | order **event** | 11,040 events (30 days history) | ~2,250 events | status lifecycle PLACED→SHIPPED→DELIVERED/CANCELLED, replayed events, negative quantities, orphan customers, null & mixed-format timestamps, **50 late-arriving orders (day 3)**, **new `coupon_code`/`device` fields (day 3)** |
| `payments_dayN.csv` | payment | 7,799 | ~700 | duplicates, negative amounts, failed payments, under-payments, missing payments |

**Columns.** customers: `customer_id, first_name, last_name, email, phone, city, state, signup_date, loyalty_tier, op, updated_at` ·
products: `product_id, product_name, category, sub_category, brand, unit_price, cost_price, is_active, updated_at` ·
orders: `order_id, customer_id, order_ts, event_ts, status, channel, shipping_address{city,state,pincode}, items[{product_id, quantity, unit_price, discount_pct}]` (+ `coupon_code, device` from day 3) ·
payments: `payment_id, order_id, payment_method, amount, payment_status, paid_at`.

## Execution steps

### Option A — Databricks UI only (works on the free **Databricks Free Edition**)

1. **Get a workspace.** Sign up for Databricks Free Edition (serverless, Unity Catalog included).
2. **Put the code in Git.** Create a GitHub repo, push this folder. In Databricks: *Workspace → Create →
   Git folder* → paste the repo URL. (Alternative: *Workspace → Import* the files.)
3. **Setup.** Open `notebooks/00_setup`, attach **Serverless**, set the `catalog` widget
   (`shopstream`; if catalog creation is not allowed use `workspace`) → *Run all*.
4. **Land day 1.** Open `00b_generate_data`, `day = 1` → *Run all*. Check *Catalog → shopstream → landing → raw_files*.
5. **Run the medallion.** Run `01_bronze_autoloader` → `02_silver_transform` → `03_gold_build` →
   `04_data_quality_checks` (each with `entity = all`).
6. **See incremental processing.** Land `day = 2`, re-run 01→04. Then land `day = 3`, re-run 01→04.
   Watch: only new files are read (`ops.pipeline_audit`), customers gain history rows in
   `silver.dim_customer`, `coupon_code` appears in bronze (schema evolution), late orders slot into October.
7. **Delta internals.** Run `05_delta_maintenance` (history, time travel, RESTORE, CDF, OPTIMIZE, VACUUM).
8. **Governance.** Create groups `analysts`, `pii_readers`, `analysts_west`, `data_engineers` (optional),
   run `06_unity_catalog_governance`, then open *Catalog Explorer → gold.fact_sales → Lineage*.
9. **Orchestrate.** *Jobs & Pipelines → Create Job*: recreate the DAG from `resources/jobs.yml`
   (setup → 4 bronze tasks in parallel → 4 silver tasks → gold → dq_gates → governance), job parameter
   `catalog`, schedule 06:30 Asia/Kolkata, failure e-mail. Click *Run now* and screenshot the DAG.
10. **Declarative version.** *Jobs & Pipelines → Create → ETL pipeline*: source file
    `pipelines/shopstream_lakeflow_pipeline.py`, default catalog `shopstream`, schema `lakeflow`, serverless,
    configuration `shopstream.raw_path = /Volumes/shopstream/landing/raw_files` and
    `shopstream.src_path = /Workspace/Users/<you>/<repo>/src`. Start it; screenshot the graph and the
    *Data quality* tab (expectation pass/fail counts).
11. **Dashboard.** *SQL Editor / Dashboards → Create dashboard*, add the queries from `sql/dashboard_queries.sql`
    as datasets: KPI tiles, daily trend, category bars, RFM segments, reconciliation, DQ monitor. Publish.
12. **Tests.** Run `tests/run_tests` in Databricks (or `pytest -q tests/` locally).

### Option B — Infrastructure-as-code with Asset Bundles (CLI)

```bash
# 1. install the Databricks CLI (v0.218+) and log in
databricks auth login --host https://<your-workspace>.cloud.databricks.com
# 2. edit `host` in databricks.yml, then:
databricks bundle validate -t dev
databricks bundle deploy   -t dev
# 3. land data (or run notebook 00b) and run everything
databricks bundle run shopstream_daily_job -t dev
databricks bundle run shopstream_lakeflow   -t dev
```

### Local development (no Databricks needed)

```bash
pip install -r requirements-dev.txt          # needs Java 11/17
python data_generator/generate_data.py --out data/landing --days 3
pytest -q -s tests/                          # 11 tests incl. a full 3-day end-to-end replay
```

## Expected results after all 3 days (use these to validate your run)

| Check | Value |
|---|---|
| bronze rows: customers / products / orders / payments | 2,438 / 206 / 15,570 / 9,233 |
| quarantined rows: customers / orders / payments | 14 / 54 / 17 |
| `silver.dim_customer` versions / current / deleted | 2,379 / 2,140 / 9 |
| `silver.orders` (unique orders) | 9,432 |
| `gold.fact_sales` (order lines) | 16,099 |
| net revenue excl. cancelled | ≈ INR 30.9 crore |
| reconciliation: MATCHED / PAYMENT_FAILED / NO_PAYMENT / CANCELLED / UNDERPAID | 8,379 / 265 / 264 / 352 / 172 |
| RFM segments | Hibernating 568, Loyal 557, Champions 262, New/Promising 269, At Risk 238, Needs Attention 124 |

(Counts come from the deterministic generator, seed 42, and are asserted by `tests/test_e2e_local.py`.)

## Design decisions worth explaining in an interview

* **Bronze keeps CSVs as strings** — no data is lost to a bad cast; typing and validation are Silver's job.
* **Quarantine, don't drop** — every rejected row is stored with the exact rule(s) it failed.
* **A whole order is quarantined if any line is bad** — partial orders would make revenue wrong silently.
* **SCD2 MERGE uses the staged-union trick** (one MERGE both expires and inserts), a row hash so only
  real changes create versions, and `updated_at > effective_from` so out-of-order CDC can't rewrite history.
* **First customer version is back-dated to `signup_date`** so historical orders find a dimension row.
* **Orders are an event stream** → silver keeps the latest state *and* the full status history (for SLAs).
* **Late-arriving / orphan customers** map to the Unknown member `-1`, so facts never disappear.
* **Liquid clustering over partitioning** — data volumes this size would create tiny-file partitions.
* **`availableNow` streaming** — exactly-once incremental processing with batch-job cost.
* **DQ gates fail the job before dashboards refresh**; non-critical checks only warn.

## Resume bullets (edit to taste)

* Built an end-to-end Databricks lakehouse (Auto Loader, Delta Lake, Unity Catalog) ingesting 4 daily
  source feeds through Bronze/Silver/Gold with incremental, exactly-once processing and schema evolution.
* Implemented SCD Type 2 customer history via Delta MERGE and Lakeflow AUTO CDC, enabling point-in-time
  revenue attribution in a star schema.
* Designed a rule-based data-quality framework with quarantine and blocking quality gates; reconciled
  order value against payments to surface under-paid and unpaid orders for finance.
* Orchestrated a parallel task DAG with Lakeflow Jobs deployed via Databricks Asset Bundles;
  applied liquid clustering, OPTIMIZE/VACUUM, column masking and row-level security.
