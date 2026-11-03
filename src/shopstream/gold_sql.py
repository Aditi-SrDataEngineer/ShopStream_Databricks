"""
Gold layer SELECT statements (star schema + business marts).

Templates use {silver} / {gold} placeholders so the same SQL runs on Databricks
(`shopstream.silver`) and in local tests (`silver`). The notebook wraps each in
`CREATE OR REPLACE TABLE ... CLUSTER BY (...) AS`.

Order matters: later tables read earlier gold tables.
"""

GOLD_TABLES: list[dict] = [
    {
        "name": "dim_customer",
        "cluster_by": ["customer_id"],
        "comment": "Customer dimension (SCD2 versions) + Unknown member (-1) for late/orphan customers",
        "sql": """
            SELECT customer_sk, customer_id, concat_ws(' ', first_name, last_name) AS customer_name,
                   email, city, state, loyalty_tier, signup_date,
                   effective_from, effective_to, is_current, is_deleted
            FROM {silver}.dim_customer
            UNION ALL
            SELECT -1, 'UNKNOWN', 'Unknown customer', NULL, 'Unknown', 'Unknown', 'UNKNOWN', NULL,
                   TIMESTAMP'1900-01-01 00:00:00', NULL, true, false
        """,
    },
    {
        "name": "dim_product",
        "cluster_by": ["product_id"],
        "comment": "Product dimension with current price/cost and list margin",
        "sql": """
            SELECT product_id, product_name, category, sub_category, brand, unit_price, cost_price,
                   round((unit_price - cost_price) / unit_price * 100, 2) AS list_margin_pct, is_active
            FROM {silver}.products
        """,
    },
    {
        "name": "dim_date",
        "cluster_by": ["date_key"],
        "comment": "Calendar dimension",
        "sql": """
            SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key, d AS calendar_date,
                   year(d) AS year, quarter(d) AS quarter, month(d) AS month, date_format(d, 'MMM') AS month_name,
                   weekofyear(d) AS week_of_year, date_format(d, 'EEEE') AS day_name,
                   dayofweek(d) IN (1, 7) AS is_weekend
            FROM (SELECT explode(sequence(DATE'2026-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d)
        """,
    },
    {
        "name": "fact_sales",
        "cluster_by": ["order_date_key", "product_id"],
        "comment": "Order-line grain sales fact. customer_sk is resolved POINT-IN-TIME against the SCD2 dimension",
        "sql": """
            SELECT
                i.order_id, i.line_no,
                CAST(date_format(o.order_ts, 'yyyyMMdd') AS INT)  AS order_date_key,
                o.order_ts,
                coalesce(c.customer_sk, -1)                        AS customer_sk,
                o.customer_id,
                i.product_id, o.channel, o.status, o.ship_state, o.coupon_code,
                i.quantity, i.unit_price, i.discount_pct, i.gross_amount, i.net_amount,
                CAST(i.quantity * p.cost_price AS DECIMAL(14,2))   AS cost_amount,
                CAST(i.net_amount - i.quantity * p.cost_price AS DECIMAL(14,2)) AS margin_amount,
                o.status <> 'CANCELLED'                            AS is_revenue
            FROM {silver}.order_items i
            JOIN {silver}.orders o            ON i.order_id = o.order_id
            LEFT JOIN {silver}.products p     ON i.product_id = p.product_id
            LEFT JOIN {silver}.dim_customer c ON o.customer_id = c.customer_id
                 AND o.order_ts >= c.effective_from
                 AND o.order_ts <  coalesce(c.effective_to, TIMESTAMP'9999-12-31 00:00:00')
        """,
    },
    {
        "name": "agg_daily_sales",
        "cluster_by": ["order_date"],
        "comment": "Daily revenue by category and channel (revenue excludes cancelled orders)",
        "sql": """
            SELECT to_date(f.order_ts) AS order_date, p.category, f.channel,
                   count(DISTINCT f.order_id)                         AS orders,
                   sum(f.quantity)                                    AS units,
                   CAST(sum(f.net_amount) AS DECIMAL(16,2))           AS net_revenue,
                   CAST(sum(f.margin_amount) AS DECIMAL(16,2))        AS margin,
                   CAST(sum(f.net_amount) / count(DISTINCT f.order_id) AS DECIMAL(14,2)) AS avg_order_value
            FROM {gold}.fact_sales f
            JOIN {gold}.dim_product p USING (product_id)
            WHERE f.is_revenue
            GROUP BY ALL
        """,
    },
    {
        "name": "agg_product_performance",
        "cluster_by": ["category"],
        "comment": "Product KPIs and rank within category",
        "sql": """
            WITH s AS (
              SELECT f.product_id,
                     sum(CASE WHEN f.is_revenue THEN f.quantity END)            AS units_sold,
                     sum(CASE WHEN f.is_revenue THEN f.net_amount END)          AS revenue,
                     sum(CASE WHEN f.is_revenue THEN f.margin_amount END)       AS margin,
                     count(DISTINCT f.order_id)                                 AS orders,
                     count(DISTINCT CASE WHEN f.status = 'CANCELLED' THEN f.order_id END) AS cancelled_orders
              FROM {gold}.fact_sales f GROUP BY f.product_id)
            SELECT p.product_id, p.product_name, p.category, p.brand, s.units_sold,
                   CAST(s.revenue AS DECIMAL(16,2)) AS revenue, CAST(s.margin AS DECIMAL(16,2)) AS margin,
                   round(s.cancelled_orders / s.orders * 100, 2) AS cancel_rate_pct,
                   dense_rank() OVER (PARTITION BY p.category ORDER BY s.revenue DESC) AS rank_in_category
            FROM s JOIN {gold}.dim_product p USING (product_id)
        """,
    },
    {
        "name": "customer_base",
        "cluster_by": ["customer_id"],
        "comment": "Per-customer behaviour (input to customer_360 / RFM)",
        "sql": """
            WITH fav AS (
              SELECT customer_id, category,
                     row_number() OVER (PARTITION BY customer_id ORDER BY sum(net_amount) DESC) AS rn
              FROM {gold}.fact_sales JOIN {gold}.dim_product USING (product_id)
              WHERE is_revenue GROUP BY customer_id, category)
            SELECT c.customer_id, c.customer_name, c.city, c.state, c.loyalty_tier,
                   min(f.order_ts)                         AS first_order_ts,
                   to_date(max(f.order_ts))                AS last_order_date,
                   count(DISTINCT f.order_id)              AS order_count,
                   CAST(sum(f.net_amount) AS DOUBLE)       AS lifetime_value,
                   CAST(sum(f.net_amount) / count(DISTINCT f.order_id) AS DECIMAL(14,2)) AS avg_order_value,
                   max(fav.category)                       AS favourite_category
            FROM {gold}.dim_customer c
            JOIN {gold}.fact_sales f ON f.customer_id = c.customer_id AND f.is_revenue
            LEFT JOIN fav ON fav.customer_id = c.customer_id AND fav.rn = 1
            WHERE c.is_current
            GROUP BY ALL
        """,
    },
    {
        "name": "payment_reconciliation",
        "cluster_by": ["recon_status"],
        "comment": "Order value vs successful payments - finance exception report",
        "sql": """
            WITH ord AS (
              SELECT o.order_id, o.customer_id, o.status, o.order_ts, sum(i.net_amount) AS order_amount
              FROM {silver}.orders o JOIN {silver}.order_items i USING (order_id)
              GROUP BY ALL),
            pay AS (
              SELECT order_id,
                     sum(CASE WHEN payment_status = 'SUCCESS' THEN amount ELSE 0 END) AS paid_amount,
                     max(payment_method) AS payment_method,
                     max(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS has_failed_attempt
              FROM {silver}.payments GROUP BY order_id)
            SELECT ord.*, coalesce(pay.paid_amount, 0) AS paid_amount, pay.payment_method,
                   CAST(ord.order_amount - coalesce(pay.paid_amount, 0) AS DECIMAL(14,2)) AS outstanding,
                   CASE WHEN ord.status = 'CANCELLED'               THEN 'CANCELLED'
                        WHEN pay.order_id IS NULL                  THEN 'NO_PAYMENT'
                        WHEN pay.paid_amount = 0 AND pay.has_failed_attempt = 1 THEN 'PAYMENT_FAILED'
                        WHEN abs(ord.order_amount - pay.paid_amount) <= 1 THEN 'MATCHED'
                        WHEN pay.paid_amount < ord.order_amount    THEN 'UNDERPAID'
                        ELSE 'OVERPAID' END AS recon_status
            FROM ord LEFT JOIN pay USING (order_id)
        """,
    },
    {
        "name": "agg_fulfilment_sla",
        "cluster_by": ["ship_state"],
        "comment": "Average hours from PLACED to SHIPPED to DELIVERED, by state",
        "sql": """
            WITH h AS (
              SELECT order_id,
                     max(CASE WHEN status = 'PLACED'    THEN event_ts END) AS placed_ts,
                     max(CASE WHEN status = 'SHIPPED'   THEN event_ts END) AS shipped_ts,
                     max(CASE WHEN status = 'DELIVERED' THEN event_ts END) AS delivered_ts
              FROM {silver}.order_status_history GROUP BY order_id)
            SELECT o.ship_state,
                   count(*)                                                                     AS orders,
                   round(avg((unix_timestamp(h.shipped_ts) - unix_timestamp(h.placed_ts)) / 3600), 1)    AS avg_hours_to_ship,
                   round(avg((unix_timestamp(h.delivered_ts) - unix_timestamp(h.shipped_ts)) / 3600), 1) AS avg_hours_to_deliver,
                   round(avg(CASE WHEN h.delivered_ts IS NOT NULL THEN 1.0 ELSE 0 END) * 100, 1)        AS delivered_pct
            FROM h JOIN {silver}.orders o USING (order_id)
            GROUP BY o.ship_state
        """,
    },
]


def render(sql: str, silver: str, gold: str) -> str:
    return sql.format(silver=silver, gold=gold)
