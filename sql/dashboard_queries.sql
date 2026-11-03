-- =============================================================================
-- ShopStream · Databricks SQL / AI-BI Dashboard queries
-- Paste each block as a dataset in a new Dashboard (SQL Warehouse: Serverless Starter).
-- Replace `shopstream` if you used a different catalog.
-- =============================================================================

-- 1. KPI tiles: revenue, orders, AOV, margin %
SELECT CAST(sum(net_revenue) AS DECIMAL(18,0))                  AS revenue_inr,
       sum(orders)                                             AS orders,
       round(sum(net_revenue) / sum(orders), 0)                AS avg_order_value,
       round(sum(margin) / sum(net_revenue) * 100, 1)          AS margin_pct
FROM shopstream.gold.agg_daily_sales;

-- 2. Daily revenue trend by channel (line chart)
SELECT order_date, channel, sum(net_revenue) AS revenue
FROM shopstream.gold.agg_daily_sales
GROUP BY ALL ORDER BY order_date;

-- 3. Revenue & margin by category (bar chart)
SELECT category, sum(net_revenue) AS revenue, sum(margin) AS margin
FROM shopstream.gold.agg_daily_sales GROUP BY category ORDER BY revenue DESC;

-- 4. Top 3 products per category
SELECT category, product_name, brand, units_sold, revenue, cancel_rate_pct
FROM shopstream.gold.agg_product_performance
WHERE rank_in_category <= 3 ORDER BY category, rank_in_category;

-- 5. Customer segments (RFM)
SELECT segment, count(*) AS customers, round(avg(lifetime_value)) AS avg_ltv,
       round(avg(order_count), 1) AS avg_orders
FROM shopstream.gold.customer_360 GROUP BY segment ORDER BY avg_ltv DESC;

-- 6. Revenue by state - customers' state AS OF ORDER TIME (SCD2 in action)
SELECT c.state, CAST(sum(f.net_amount) AS DECIMAL(18,0)) AS revenue
FROM shopstream.gold.fact_sales f
JOIN shopstream.gold.dim_customer c ON f.customer_sk = c.customer_sk
WHERE f.is_revenue GROUP BY c.state ORDER BY revenue DESC;

-- 7. Finance exception report: payment reconciliation
SELECT recon_status, count(*) AS orders, CAST(sum(outstanding) AS DECIMAL(18,2)) AS outstanding_inr
FROM shopstream.gold.payment_reconciliation GROUP BY recon_status ORDER BY orders DESC;

-- 8. Fulfilment SLA by state
SELECT * FROM shopstream.gold.agg_fulfilment_sla ORDER BY avg_hours_to_deliver DESC;

-- 9. Coupon effectiveness (only possible because of day-3 schema evolution)
SELECT coalesce(coupon_code, 'NO COUPON') AS coupon, count(DISTINCT order_id) AS orders,
       round(sum(net_amount) / count(DISTINCT order_id)) AS aov
FROM shopstream.gold.fact_sales
WHERE is_revenue AND order_ts >= '2026-11-01'
GROUP BY ALL ORDER BY orders DESC;

-- 10. Data-quality monitor (ops)
SELECT entity, explode(dq_failed_rules) AS rule, count(*) AS rejected_rows
FROM shopstream.ops.dq_quarantine GROUP BY ALL ORDER BY rejected_rows DESC;

-- 11. Pipeline run audit
SELECT layer, entity, sum(rows_in) AS rows_in, sum(rows_out) AS rows_out,
       sum(rows_quarantined) AS rows_quarantined, max(logged_at) AS last_run
FROM shopstream.ops.pipeline_audit GROUP BY ALL ORDER BY layer, entity;

-- 12. Time travel: how many customers changed tier in the last load?
SELECT count(*) AS tier_changes
FROM shopstream.silver.dim_customer cur
JOIN shopstream.silver.dim_customer prev
  ON cur.customer_id = prev.customer_id AND prev.effective_to = cur.effective_from
WHERE cur.loyalty_tier <> prev.loyalty_tier;
