"""Realistic report queries over the retail schema, part P: data quality and operations, as analysts write them --
contact details validated by REGEXP, SKUs audited against their pattern, an order's flags packed into a bitmask and
read back with &, the gap to a customer's next order by LEAD, first and last values over frames, list prices
against sold prices by GREATER and LESSER, masked e-mails by REPLICATE and STUFF, payments reconciled against order
totals, support tickets against an SLA by priority, overlapping promotions, gaps in the exchange-rate feed, the
second-highest salary per region by nested subqueries, and days of cover projected to a run-out date. Quotients a
report shows are cast to a fixed NUMERIC, as such reports do."""

QUALITY = [
    ("customers_with_invalid_contact_details",
     """SELECT c.id, c.last_name,
           CASE WHEN c.email IS NULL THEN 'missing'
                WHEN NOT (c.email REGEXP '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}') THEN 'malformed'
                WHEN c.email <> LOWER(c.email) THEN 'mixed case' ELSE 'ok' END AS email_check,
           CASE WHEN c.phone IS NULL THEN 'missing'
                WHEN c.phone REGEXP '\\([0-9]{3}\\) [0-9]{3}-[0-9]{4}' THEN 'ok'
                WHEN c.phone REGEXP '[0-9]{3}-[0-9]{3}-[0-9]{4}' THEN 'unformatted' ELSE 'malformed' END AS phone_check
         FROM customers c
         WHERE c.email IS NULL OR c.phone IS NULL OR c.email <> LOWER(c.email)
            OR NOT (c.phone REGEXP '\\([0-9]{3}\\) [0-9]{3}-[0-9]{4}')
         ORDER BY c.id"""),
    ("sku_pattern_audit",
     """SELECT p.category, COUNT(*) AS products,
           SUM(CASE WHEN p.sku REGEXP '[A-Z]{3}-[A-Z]{3}-[0-9]{3}' THEN 1 ELSE 0 END) AS well_formed,
           SUM(CASE WHEN LEFT(p.sku, 3) = UPPER(LEFT(p.category, 3)) THEN 1 ELSE 0 END) AS prefix_matches_category,
           MIN(p.sku) AS first_sku, MAX(p.sku) AS last_sku
         FROM products p GROUP BY p.category ORDER BY p.category"""),
    ("order_flags_bitmask",
     """WITH flagged AS (
           SELECT o.id,
                  (CASE WHEN o.ship_date IS NULL THEN 1 ELSE 0 END)
                | (CASE WHEN o.discount_pct > 0 THEN 2 ELSE 0 END)
                | (CASE WHEN o.shipping_fee = 0 THEN 4 ELSE 0 END)
                | (CASE WHEN o.sales_rep_id IS NULL THEN 8 ELSE 0 END) AS flags
           FROM orders o)
         SELECT flags, COUNT(*) AS orders,
           CASE WHEN flags & 1 = 1 THEN 'unshipped ' ELSE '' END || CASE WHEN flags & 2 = 2 THEN 'discounted ' ELSE '' END
             || CASE WHEN flags & 4 = 4 THEN 'free-shipping ' ELSE '' END || CASE WHEN flags & 8 = 8 THEN 'no-rep' ELSE '' END AS meaning
         FROM flagged GROUP BY flags ORDER BY flags"""),
    ("web_urls_by_pattern",
     """SELECT CASE WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/(products|category)/.*' THEN 'browse'
                    WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/(cart|checkout).*' THEN 'checkout'
                    WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/order/confirmation' THEN 'purchase'
                    WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/search\\?q=.*' THEN 'search'
                    ELSE 'other' END AS page_kind,
           w.device, COUNT(*) AS events, COUNT(DISTINCT w.customer_id) AS customers
         FROM web_events w
         GROUP BY CASE WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/(products|category)/.*' THEN 'browse'
                    WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/(cart|checkout).*' THEN 'checkout'
                    WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/order/confirmation' THEN 'purchase'
                    WHEN LOWER(w.url) REGEXP 'https://shop\\.example\\.com/search\\?q=.*' THEN 'search'
                    ELSE 'other' END, w.device
         ORDER BY 1, 2"""),
    ("masked_customer_emails",
     """SELECT c.id,
           LEFT(LOWER(c.email), 2) || REPLICATE('*', LOCATE(c.email, '@') - 3) || SUBSTRING(LOWER(c.email), LOCATE(c.email, '@')) AS masked_email,
           STUFF(c.phone, 2, 3, 'XXX') AS masked_phone,
           UPPER(LEFT(TRIM(c.first_name), 1)) || '. ' || TRIM(c.last_name) AS display_name
         FROM customers c WHERE c.email IS NOT NULL AND LOCATE(c.email, '@') > 3
         ORDER BY c.id"""),
    ("supplier_name_duplicates",
     """SELECT a.id AS first_id, b.id AS second_id, TRIM(a.name) AS first_name, TRIM(b.name) AS second_name, a.city
         FROM suppliers a JOIN suppliers b ON a.id < b.id AND a.city = b.city
           AND LOWER(TRIM(a.name)) = LOWER(TRIM(b.name))
         ORDER BY a.id, b.id"""),
]

OPERATIONS = [
    ("gap_to_next_order",
     """WITH seq AS (
           SELECT o.customer_id, o.id, o.order_ts,
                  LEAD(o.order_ts) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id) AS next_ts
           FROM orders o WHERE o.status <> 'cancelled')
         SELECT customer_id, COUNT(*) AS orders, COUNT(next_ts) AS repeat_orders,
           MIN(DATEDIFF(DAY, order_ts, next_ts)) AS shortest_gap_days,
           MAX(DATEDIFF(DAY, order_ts, next_ts)) AS longest_gap_days,
           CAST(AVG(DATEDIFF(DAY, order_ts, next_ts) * 1.0) AS NUMERIC(8,1)) AS average_gap_days
         FROM seq GROUP BY customer_id HAVING COUNT(next_ts) >= 2
         ORDER BY customer_id"""),
    ("first_and_last_purchase_per_customer",
     """SELECT DISTINCT o.customer_id,
           FIRST_VALUE(o.id) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_order,
           LAST_VALUE(o.id) OVER (PARTITION BY o.customer_id ORDER BY o.order_ts, o.id
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_order,
           COUNT(*) OVER (PARTITION BY o.customer_id) AS orders
         FROM orders o WHERE o.status IN ('shipped', 'returned')
         ORDER BY o.customer_id"""),
    ("sold_below_list_price",
     """SELECT p.id, p.name, p.unit_price AS list_price,
           MIN(oi.unit_price) AS lowest_sold, MAX(oi.unit_price) AS highest_sold,
           GREATER(MAX(oi.unit_price) - p.unit_price, 0) AS most_above_list,
           LESSER(MIN(oi.unit_price) - p.unit_price, 0) AS most_below_list,
           SUM(CASE WHEN oi.unit_price < p.unit_price THEN oi.quantity ELSE 0 END) AS units_below_list
         FROM products p JOIN order_items oi ON oi.product_id = p.id
         GROUP BY p.id, p.name, p.unit_price
         HAVING MIN(oi.unit_price) <> p.unit_price OR MAX(oi.unit_price) <> p.unit_price
         ORDER BY p.id"""),
    ("payments_reconciled_against_orders",
     """WITH owed AS (
           SELECT o.id, o.status, SUM(oi.quantity * oi.unit_price - COALESCE(oi.line_discount, 0)) + o.shipping_fee AS due
           FROM orders o JOIN order_items oi ON oi.order_id = o.id
           GROUP BY o.id, o.status, o.shipping_fee),
         paid AS (SELECT order_id, SUM(amount) AS paid, COUNT(*) AS payments FROM payments GROUP BY order_id),
         settled AS (
           SELECT w.id, w.status, w.due, COALESCE(p.paid, 0) AS paid, COALESCE(p.payments, 0) AS payments,
                  w.due - COALESCE(p.paid, 0) AS balance,
                  CASE WHEN p.paid IS NULL THEN 'unpaid' WHEN ABS(w.due - p.paid) < 0.01 THEN 'settled'
                       WHEN p.paid > w.due THEN 'overpaid' ELSE 'partial' END AS state
           FROM owed w LEFT JOIN paid p ON p.order_id = w.id)
         SELECT id, status, due, paid, payments, balance, state
         FROM settled
         WHERE status <> 'cancelled' AND state <> 'settled'
         ORDER BY id"""),
    ("ticket_sla_breaches_by_agent",
     """WITH timed AS (
           SELECT t.agent_id, t.priority,
                  DATEDIFF(HOUR, t.opened_ts, COALESCE(t.closed_ts, CAST('2025-12-31 23:59:59' AS TIMESTAMP))) AS hours_open,
                  CASE t.priority WHEN 'urgent' THEN 4 WHEN 'high' THEN 24 WHEN 'normal' THEN 72 ELSE 168 END AS sla_hours
           FROM support_tickets t WHERE t.agent_id IS NOT NULL)
         SELECT agent_id, COUNT(*) AS tickets,
           SUM(CASE WHEN hours_open > sla_hours THEN 1 ELSE 0 END) AS breaches,
           CAST(100.0 * SUM(CASE WHEN hours_open > sla_hours THEN 1 ELSE 0 END) / COUNT(*) AS NUMERIC(5,1)) AS breach_pct,
           MAX(hours_open - sla_hours) AS worst_overrun_hours
         FROM timed GROUP BY agent_id ORDER BY breach_pct DESC, agent_id"""),
    ("promotions_overlapping_in_category",
     """SELECT a.code AS first_code, b.code AS second_code, COALESCE(a.category, 'all') AS category,
           GREATER(a.starts, b.starts) AS overlap_starts, LESSER(a.ends, b.ends) AS overlap_ends,
           DATEDIFF(DAY, GREATER(a.starts, b.starts), LESSER(a.ends, b.ends)) + 1 AS overlap_days
         FROM promotions a JOIN promotions b ON a.id < b.id
           AND a.starts <= b.ends AND b.starts <= a.ends
           AND (a.category = b.category OR a.category IS NULL OR b.category IS NULL)
         ORDER BY a.code, b.code"""),
    ("exchange_rate_feed_gaps",
     """WITH feed AS (
           SELECT currency, rate_date, usd_rate,
                  LAG(rate_date) OVER (PARTITION BY currency ORDER BY rate_date, id) AS prior_date,
                  DATEDIFF(DAY, LAG(rate_date) OVER (PARTITION BY currency ORDER BY rate_date, id), rate_date) AS days_between,
                  CAST(100 * (usd_rate - LAG(usd_rate) OVER (PARTITION BY currency ORDER BY rate_date, id))
                       / NULLIF(LAG(usd_rate) OVER (PARTITION BY currency ORDER BY rate_date, id), 0) AS NUMERIC(7,3)) AS change_pct
           FROM exchange_rates)
         SELECT currency, prior_date, rate_date, days_between, change_pct
         FROM feed
         WHERE days_between IS NOT NULL AND (days_between > 31 OR change_pct > 2 OR change_pct < -2)
         ORDER BY currency, rate_date"""),
    ("second_highest_salary_per_region",
     """SELECT r.name AS region,
           (SELECT MAX(e.salary) FROM employees e WHERE e.region_id = r.id) AS highest,
           (SELECT MAX(e.salary) FROM employees e WHERE e.region_id = r.id
              AND e.salary < (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.region_id = r.id)) AS second_highest,
           (SELECT COUNT(*) FROM employees e WHERE e.region_id = r.id) AS headcount
         FROM regions r ORDER BY r.name"""),
    ("days_of_cover_and_runout_date",
     """WITH daily AS (
           SELECT oi.product_id, SUM(oi.quantity) * 1.0 / 90 AS units_per_day
           FROM order_items oi JOIN orders o ON o.id = oi.order_id
           WHERE o.status <> 'cancelled' AND o.order_ts >= '2025-10-01' AND o.order_ts < '2025-12-30'
           GROUP BY oi.product_id),
         latest AS (
           SELECT i.product_id, i.snapshot_date, i.on_hand, i.reorder_point
           FROM inventory i
           WHERE i.snapshot_date = (SELECT MAX(i2.snapshot_date) FROM inventory i2 WHERE i2.product_id = i.product_id))
         SELECT l.product_id, l.snapshot_date, l.on_hand, l.reorder_point,
           CAST(d.units_per_day AS NUMERIC(8,3)) AS units_per_day,
           CASE WHEN d.units_per_day IS NULL OR d.units_per_day = 0 THEN NULL
                ELSE CAST(FLOOR(l.on_hand / d.units_per_day) AS INTEGER) END AS days_of_cover,
           CASE WHEN d.units_per_day IS NULL OR d.units_per_day = 0 THEN NULL
                ELSE DATEADD(DAY, CAST(FLOOR(l.on_hand / d.units_per_day) AS INTEGER), l.snapshot_date) END AS runout_date
         FROM latest l LEFT JOIN daily d ON d.product_id = l.product_id
         ORDER BY l.product_id"""),
    ("orders_shipped_on_weekends",
     """SELECT DATENAME(WEEKDAY, o.ship_date) AS ship_day, COUNT(*) AS orders,
           CAST(AVG(DATEDIFF(DAY, CAST(o.order_ts AS DATE), o.ship_date) * 1.0) AS NUMERIC(6,2)) AS average_lead_days,
           MIN(o.ship_date) AS first_ship, MAX(o.ship_date) AS last_ship
         FROM orders o WHERE o.ship_date IS NOT NULL AND DOW(o.ship_date) IN (1, 7)
         GROUP BY DATENAME(WEEKDAY, o.ship_date) ORDER BY 1"""),
    ("customer_activity_bits_by_quarter",
     """WITH q AS (
           SELECT o.customer_id,
                  SUM(DISTINCT CASE QUARTER(o.order_ts) WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 4 ELSE 8 END) AS quarters
           FROM orders o WHERE YEAR(o.order_ts) = 2025 AND o.status <> 'cancelled'
           GROUP BY o.customer_id)
         SELECT q.quarters,
           (q.quarters & 1) + (q.quarters & 2) / 2 + (q.quarters & 4) / 4 + (q.quarters & 8) / 8 AS active_quarters,
           COUNT(*) AS customers
         FROM q GROUP BY q.quarters ORDER BY q.quarters"""),
    ("lifetime_value_bands_with_running_share",
     """WITH ltv AS (
           SELECT o.customer_id, SUM(oi.quantity * oi.unit_price) AS value
           FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
           GROUP BY o.customer_id),
         banded AS (
           SELECT customer_id, value,
                  CASE WHEN value >= 5000 THEN 'platinum' WHEN value >= 2000 THEN 'gold' WHEN value >= 500 THEN 'silver'
                       ELSE 'bronze' END AS band
           FROM ltv)
         SELECT band, COUNT(*) AS customers, SUM(value) AS band_value,
           CAST(100 * SUM(value) / SUM(SUM(value)) OVER () AS NUMERIC(6,2)) AS value_share_pct,
           CAST(100 * SUM(SUM(value)) OVER (ORDER BY MIN(value) DESC, band ROWS UNBOUNDED PRECEDING) / SUM(SUM(value)) OVER ()
                AS NUMERIC(6,2)) AS running_share_pct
         FROM banded GROUP BY band ORDER BY MIN(value) DESC, band"""),
    ("product_names_normalized",
     """SELECT p.id, p.name,
           UPPER(LEFT(p.category, 1)) || LOWER(SUBSTRING(p.category, 2)) || ' / ' || p.subcategory AS path,
           LENGTH(p.name) - LENGTH(REPLACE(p.name, ' ', '')) + 1 AS words,
           CASE WHEN PATINDEX('%[0-9]%', p.name) > 0 THEN 'has digits' ELSE 'letters only' END AS digits,
           REPLACE(LOWER(TRIM(p.name)), ' ', '-') AS slug
         FROM products p ORDER BY p.id"""),
]

CASES = [("data_quality", QUALITY), ("ops_checks", OPERATIONS)]
