"""Realistic report queries over the retail schema, part W: behavioral and comparative analyses, as product and finance
analysts write them -- a funnel converted within 30 minutes by a self-join, products bought together with
their support and confidence, the top three of each category by a correlated count, the first and last words of
product names, payments converted at the as-of exchange rate (the latest on or before the day, one month missing),
web sessions cut at 30 idle minutes, the last referrer seen by LAST_VALUE IGNORE NULLS, each month's share of its
quarter, year over year by a self-join of monthly totals, and customers with no order in their signup quarter by an
anti-join."""

BEHAVIOR = [
    ("funnel_to_cart_within_30_minutes",
     """WITH views AS (
           SELECT w.id, w.customer_id, w.device, w.event_ts FROM web_events w
           WHERE w.customer_id IS NOT NULL AND w.url LIKE '%/products/%'),
         carted AS (
           SELECT DISTINCT v.id FROM views v JOIN web_events c ON c.customer_id = v.customer_id
             AND c.url LIKE '%/cart%' AND c.event_ts > v.event_ts AND DATEDIFF(MINUTE, v.event_ts, c.event_ts) <= 30)
         SELECT v.device, COUNT(*) AS product_views, COUNT(k.id) AS carted_within_30,
           CAST(100.0 * COUNT(k.id) / COUNT(*) AS NUMERIC(5,1)) AS conversion_pct
         FROM views v LEFT JOIN carted k ON k.id = v.id
         GROUP BY v.device ORDER BY v.device"""),
    ("products_bought_together",
     """WITH baskets AS (SELECT DISTINCT order_id, product_id FROM order_items),
         singles AS (SELECT product_id, COUNT(*) AS orders_with FROM baskets GROUP BY product_id)
         SELECT a.product_id AS first_product, b.product_id AS second_product, COUNT(*) AS orders_together,
           s.orders_with AS orders_with_first,
           CAST(100.0 * COUNT(*) / s.orders_with AS NUMERIC(5,1)) AS confidence_pct
         FROM baskets a JOIN baskets b ON b.order_id = a.order_id AND b.product_id > a.product_id
           JOIN singles s ON s.product_id = a.product_id
         GROUP BY a.product_id, b.product_id, s.orders_with
         HAVING COUNT(*) >= 2
         ORDER BY orders_together DESC, first_product, second_product"""),
    ("web_sessions_cut_at_30_idle_minutes",
     """WITH gaps AS (
           SELECT w.id, w.customer_id, w.event_ts, w.device,
                  CASE WHEN LAG(w.event_ts) OVER (PARTITION BY w.customer_id ORDER BY w.event_ts, w.id) IS NULL
                         OR DATEDIFF(MINUTE, LAG(w.event_ts) OVER (PARTITION BY w.customer_id ORDER BY w.event_ts, w.id),
                                     w.event_ts) > 30
                       THEN 1 ELSE 0 END AS starts_session
           FROM web_events w WHERE w.customer_id IS NOT NULL),
         numbered AS (
           SELECT id, customer_id, event_ts, device,
                  SUM(starts_session) OVER (PARTITION BY customer_id ORDER BY event_ts, id ROWS UNBOUNDED PRECEDING)
                    AS session_no
           FROM gaps)
         SELECT customer_id, session_no, MIN(event_ts) AS started, MAX(event_ts) AS ended,
           DATEDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) AS minutes, COUNT(*) AS pages, MIN(device) AS device
         FROM numbered WHERE customer_id <= 10
         GROUP BY customer_id, session_no ORDER BY customer_id, session_no"""),
    ("last_referrer_seen",
     """SELECT w.customer_id, w.id, w.event_ts, w.referrer,
           LAST_VALUE(w.referrer IGNORE NULLS) OVER (PARTITION BY w.customer_id ORDER BY w.event_ts, w.id
                                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_referrer
         FROM web_events w WHERE w.customer_id IN (2, 3, 5, 8)
         ORDER BY w.customer_id, w.event_ts, w.id"""),
    ("customers_without_an_order_in_signup_quarter",
     """SELECT c.id, TRIM(c.last_name) AS customer, c.signup_date,
           YEAR(c.signup_date) * 10 + QUARTER(c.signup_date) AS signup_quarter
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.status <> 'cancelled'
           AND YEAR(o.order_ts) = YEAR(c.signup_date) AND QUARTER(o.order_ts) = QUARTER(c.signup_date)
         WHERE o.id IS NULL
         ORDER BY c.id"""),
]

COMPARISONS = [
    ("top_three_by_correlated_count",
     """SELECT p.category, p.id, p.name, p.unit_price
         FROM products p
         WHERE (SELECT COUNT(*) FROM products q WHERE q.category = p.category
                AND (q.unit_price > p.unit_price OR q.unit_price = p.unit_price AND q.id < p.id)) < 3
         ORDER BY p.category, p.unit_price DESC, p.id"""),
    ("first_and_last_words_of_names",
     """SELECT id, name,
           CASE WHEN LOCATE(name, ' ') = 0 THEN name ELSE LEFT(name, LOCATE(name, ' ') - 1) END AS first_word,
           CASE WHEN LOCATE(name, ' ') = 0 THEN name ELSE SUBSTR(name, LOCATE(name, ' ', -1) + 1) END AS last_word,
           LENGTH(name) - LENGTH(REPLACE(name, ' ', '')) + 1 AS words
         FROM products ORDER BY words DESC, id"""),
    ("payments_at_the_as_of_rate",
     """SELECT p.id, CAST(p.paid_ts AS DATE) AS paid_on, p.amount, r.rate_date, r.usd_rate,
           CAST(p.amount / r.usd_rate AS NUMERIC(12,2)) AS amount_in_cad
         FROM payments p JOIN exchange_rates r ON r.currency = 'CAD'
           AND r.rate_date = (SELECT MAX(r2.rate_date) FROM exchange_rates r2
                              WHERE r2.currency = 'CAD' AND r2.rate_date <= p.paid_ts)
         WHERE p.paid_ts >= '2024-05-01' AND p.paid_ts < '2024-10-01'
         ORDER BY p.id"""),
    ("month_share_of_its_quarter",
     """WITH monthly AS (
           SELECT YEAR(o.order_ts) AS yr, QUARTER(o.order_ts) AS qtr, MONTH(o.order_ts) AS mon,
                  SUM(oi.quantity * oi.unit_price) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY YEAR(o.order_ts), QUARTER(o.order_ts), MONTH(o.order_ts))
         SELECT yr, qtr, mon, revenue, SUM(revenue) OVER (PARTITION BY yr, qtr) AS quarter_revenue,
           CAST(100 * revenue / SUM(revenue) OVER (PARTITION BY yr, qtr) AS NUMERIC(5,1)) AS share_of_quarter
         FROM monthly WHERE yr = 2024 ORDER BY yr, mon"""),
    ("monthly_year_over_year_by_self_join",
     """WITH monthly AS (
           SELECT YEAR(o.order_ts) AS yr, MONTH(o.order_ts) AS mon, COUNT(DISTINCT o.id) AS order_count,
                  SUM(oi.quantity * oi.unit_price) AS revenue
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY YEAR(o.order_ts), MONTH(o.order_ts))
         SELECT cur.yr, cur.mon, cur.revenue, prev.revenue AS revenue_year_before,
           cur.revenue - COALESCE(prev.revenue, 0) AS change,
           CASE WHEN prev.revenue IS NULL THEN NULL
                ELSE CAST(100 * (cur.revenue - prev.revenue) / prev.revenue AS NUMERIC(8,1)) END AS change_pct
         FROM monthly cur LEFT JOIN monthly prev ON prev.yr = cur.yr - 1 AND prev.mon = cur.mon
         WHERE cur.yr = 2025 ORDER BY cur.mon"""),
]

CASES = [("behavior_reports", BEHAVIOR), ("comparison_reports", COMPARISONS)]
