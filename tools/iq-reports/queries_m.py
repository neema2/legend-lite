"""Realistic report queries over the retail schema, part M: the patterns analysts reach for again and again -- gaps and
islands of monthly activity, web sessions cut at a 30-minute gap, market-basket pairs with support, confidence and lift,
amounts converted at the exchange rate in force on the day (as-of lookups by a correlated MAX and by a join), promotion
windows matched by a date range, the latest snapshot per product, month-over-month retention by a self-join, top three
per category with the leader, percentiles and spread by channel, ticket SLAs in IQ's HOURS and MINUTES, a paged list;
then grouping sets, INTERSECT and EXCEPT, e-mail domains, phone numbers cleaned to digits, name and badge formatting,
cumulative new customers with LEAD and LAST_VALUE, products gone quiet, the same month a year before, and order-value
bands with CUME_DIST. Quotients a report shows are cast to a fixed NUMERIC, as such reports do."""

PATTERNS = [
    ("order_month_streaks",
     """WITH months AS (SELECT DISTINCT customer_id, DATEDIFF(MONTH, CAST('2023-01-01' AS DATE), CAST(order_ts AS DATE)) AS m
                       FROM orders WHERE status <> 'cancelled'),
             islands AS (SELECT customer_id, m, m - ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY m) AS grp FROM months)
         SELECT customer_id, MIN(m) AS first_month, MAX(m) AS last_month, COUNT(*) AS streak_months,
           DATEADD(MONTH, MIN(m), CAST('2023-01-01' AS DATE)) AS streak_start
         FROM islands GROUP BY customer_id, grp HAVING COUNT(*) >= 2
         ORDER BY streak_months DESC, customer_id, first_month"""),
    ("web_sessions_at_a_30_minute_gap",
     """WITH ev AS (SELECT id, customer_id, event_ts, url,
                          CASE WHEN LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, id) IS NULL
                                 OR DATEDIFF(MINUTE, LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, id), event_ts) > 30
                               THEN 1 ELSE 0 END AS new_session
                   FROM web_events WHERE customer_id IS NOT NULL),
             s AS (SELECT id, customer_id, event_ts, url,
                          SUM(new_session) OVER (PARTITION BY customer_id ORDER BY event_ts, id ROWS UNBOUNDED PRECEDING) AS session_no
                   FROM ev)
         SELECT customer_id, session_no, COUNT(*) AS events, MIN(event_ts) AS started, MAX(event_ts) AS ended,
           DATEDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) AS minutes,
           SUM(CASE WHEN url LIKE '%/checkout%' THEN 1 ELSE 0 END) AS checkout_hits
         FROM s GROUP BY customer_id, session_no ORDER BY customer_id, session_no"""),
    ("market_basket_pairs",
     """WITH baskets AS (SELECT DISTINCT oi.order_id, oi.product_id FROM order_items oi JOIN orders o ON o.id = oi.order_id
                        WHERE o.status <> 'cancelled'),
             singles AS (SELECT product_id, COUNT(*) AS orders FROM baskets GROUP BY product_id),
             total AS (SELECT COUNT(DISTINCT order_id) AS n FROM baskets)
         SELECT a.product_id AS product_a, b.product_id AS product_b, COUNT(*) AS together,
           CAST(ROUND(100.0 * COUNT(*) / MAX(t.n), 2) AS NUMERIC(6,2)) AS support_pct,
           CAST(ROUND(100.0 * COUNT(*) / MAX(sa.orders), 2) AS NUMERIC(6,2)) AS confidence_a_to_b,
           CAST(ROUND(1.0 * COUNT(*) * MAX(t.n) / (MAX(sa.orders) * MAX(sb.orders)), 3) AS NUMERIC(8,3)) AS lift
         FROM baskets a JOIN baskets b ON b.order_id = a.order_id AND b.product_id > a.product_id
           JOIN singles sa ON sa.product_id = a.product_id JOIN singles sb ON sb.product_id = b.product_id CROSS JOIN total t
         GROUP BY a.product_id, b.product_id HAVING COUNT(*) >= 2 ORDER BY together DESC, product_a, product_b"""),
    ("payments_in_euros_at_the_days_rate",
     """SELECT p.id, p.paid_ts, p.amount AS usd,
           (SELECT r.usd_rate FROM exchange_rates r WHERE r.currency = 'EUR'
              AND r.rate_date = (SELECT MAX(r2.rate_date) FROM exchange_rates r2
                                 WHERE r2.currency = 'EUR' AND r2.rate_date <= p.paid_ts)) AS eur_rate,
           CAST(ROUND(p.amount / (SELECT r.usd_rate FROM exchange_rates r WHERE r.currency = 'EUR'
              AND r.rate_date = (SELECT MAX(r2.rate_date) FROM exchange_rates r2
                                 WHERE r2.currency = 'EUR' AND r2.rate_date <= p.paid_ts)), 2) AS NUMERIC(12,2)) AS eur
         FROM payments p WHERE p.paid_ts >= '2025-01-01' ORDER BY p.id"""),
    ("monthly_receipts_in_pounds_by_an_as_of_join",
     """WITH rated AS (SELECT p.id, p.paid_ts, p.amount, MAX(r.rate_date) AS rate_date
                      FROM payments p JOIN exchange_rates r ON r.currency = 'GBP' AND r.rate_date <= p.paid_ts
                      GROUP BY p.id, p.paid_ts, p.amount)
         SELECT DATEFORMAT(x.paid_ts, 'yyyy-mm') AS ym, COUNT(*) AS payments, SUM(x.amount) AS usd,
           CAST(SUM(x.amount / r.usd_rate) AS NUMERIC(12,2)) AS gbp, MIN(r.usd_rate) AS lowest_rate, MAX(r.usd_rate) AS highest_rate
         FROM rated x JOIN exchange_rates r ON r.currency = 'GBP' AND r.rate_date = x.rate_date
         GROUP BY DATEFORMAT(x.paid_ts, 'yyyy-mm') ORDER BY ym"""),
    ("promotion_window_sales",
     """SELECT pr.code, COALESCE(pr.category, 'all') AS category, pr.starts, pr.ends, DATEDIFF(DAY, pr.starts, pr.ends) + 1 AS days,
           COUNT(DISTINCT o.id) AS orders,
           SUM(CASE WHEN pr.category IS NULL OR p.category = pr.category THEN oi.quantity * oi.unit_price ELSE 0 END) AS promo_gross,
           CAST(ROUND(COALESCE(SUM(CASE WHEN pr.category IS NULL OR p.category = pr.category THEN oi.quantity * oi.unit_price END), 0)
                      / (DATEDIFF(DAY, pr.starts, pr.ends) + 1), 2) AS NUMERIC(10,2)) AS promo_gross_per_day
         FROM promotions pr
           LEFT JOIN orders o ON o.order_ts >= pr.starts AND o.order_ts < DATEADD(DAY, 1, pr.ends) AND o.status <> 'cancelled'
           LEFT JOIN order_items oi ON oi.order_id = o.id
           LEFT JOIN products p ON p.id = oi.product_id
         GROUP BY pr.code, pr.category, pr.starts, pr.ends ORDER BY pr.starts, pr.code"""),
    ("latest_stock_per_product",
     """WITH snaps AS (SELECT i.product_id, i.snapshot_date, i.on_hand, i.reorder_point,
                             ROW_NUMBER() OVER (PARTITION BY i.product_id ORDER BY i.snapshot_date DESC, i.id DESC) AS rn,
                             LEAD(i.on_hand) OVER (PARTITION BY i.product_id ORDER BY i.snapshot_date DESC, i.id DESC) AS prior_on_hand
                      FROM inventory i)
         SELECT p.id, p.sku, p.name, s.snapshot_date, s.on_hand, s.prior_on_hand, s.on_hand - s.prior_on_hand AS change,
           CASE WHEN s.on_hand <= s.reorder_point THEN 'reorder' WHEN s.on_hand <= 2 * s.reorder_point THEN 'watch' ELSE 'ok' END AS stock_status
         FROM products p JOIN snaps s ON s.product_id = p.id AND s.rn = 1 ORDER BY p.id"""),
    ("month_over_month_retention",
     """WITH active AS (SELECT DISTINCT customer_id, YEAR(order_ts) AS y, MONTH(order_ts) AS mo, YEAR(order_ts) * 12 + MONTH(order_ts) AS mi
                       FROM orders WHERE status <> 'cancelled')
         SELECT a.y, a.mo, COUNT(*) AS active_customers, COUNT(b.customer_id) AS retained_next_month,
           CAST(ROUND(100.0 * COUNT(b.customer_id) / COUNT(*), 1) AS NUMERIC(5,1)) AS retention_pct
         FROM active a LEFT JOIN active b ON b.customer_id = a.customer_id AND b.mi = a.mi + 1
         GROUP BY a.y, a.mo, a.mi ORDER BY a.mi"""),
    ("top_three_products_per_category",
     """WITH rev AS (SELECT p.category, p.id, p.name, SUM(oi.quantity * oi.unit_price) AS revenue
                    FROM products p JOIN order_items oi ON oi.product_id = p.id JOIN orders o ON o.id = oi.order_id
                    WHERE o.status <> 'cancelled' GROUP BY p.category, p.id, p.name),
             ranked AS (SELECT category, id, name, revenue, DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS dr,
                               CAST(ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY category), 1) AS NUMERIC(5,1)) AS category_share,
                               FIRST_VALUE(name) OVER (PARTITION BY category ORDER BY revenue DESC, id
                                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS leader
                        FROM rev)
         SELECT category, dr, id, name, revenue, category_share, leader FROM ranked WHERE dr <= 3 ORDER BY category, dr, id"""),
    ("channel_order_value_spread",
     """SELECT o.channel, COUNT(*) AS orders,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.total) AS median_total,
           PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY t.total) AS p90_total,
           CAST(AVG(t.total) AS NUMERIC(10,2)) AS mean_total, CAST(STDDEV_SAMP(t.total) AS NUMERIC(10,2)) AS sd_total,
           CAST(VAR_POP(t.items) AS NUMERIC(8,3)) AS var_items
         FROM orders o JOIN (SELECT order_id, SUM(quantity * unit_price) AS total, SUM(quantity) AS items
                             FROM order_items GROUP BY order_id) t ON t.order_id = o.id
         WHERE o.status <> 'cancelled' GROUP BY o.channel ORDER BY o.channel"""),
    ("ticket_sla_in_hours_and_minutes",
     """SELECT t.priority, COUNT(*) AS tickets, SUM(CASE WHEN t.closed_ts IS NULL THEN 1 ELSE 0 END) AS still_open,
           CAST(AVG(HOURS(t.opened_ts, t.closed_ts)) AS NUMERIC(8,1)) AS avg_hours, MAX(MINUTES(t.opened_ts, t.closed_ts)) AS longest_minutes,
           SUM(CASE WHEN HOURS(t.opened_ts, t.closed_ts) <= CASE t.priority WHEN 'urgent' THEN 4 WHEN 'high' THEN 24 ELSE 72 END
                    THEN 1 ELSE 0 END) AS within_sla,
           MEDIAN(HOURS(t.opened_ts, t.closed_ts)) AS median_hours
         FROM support_tickets t GROUP BY t.priority
         ORDER BY MIN(CASE t.priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END)"""),
    ("active_customers_page_two",
     """SELECT TOP 5 START AT 6 c.id, c.last_name || ', ' || c.first_name AS customer, c.city, c.segment, c.credit_limit
         FROM customers c WHERE c.is_active = 1 ORDER BY c.last_name, c.first_name, c.id"""),
]

SETS_AND_STRINGS = [
    ("revenue_by_grouping_sets",
     """SELECT CASE WHEN GROUPING(r.name) = 1 THEN 'All regions' ELSE r.name END AS region,
           CASE WHEN GROUPING(o.channel) = 1 THEN 'All channels' ELSE o.channel END AS channel,
           COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity * oi.unit_price) AS revenue
         FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id
           JOIN order_items oi ON oi.order_id = o.id
         WHERE o.status <> 'cancelled'
         GROUP BY GROUPING SETS ((r.name, o.channel), (r.name), (o.channel), ())
         ORDER BY GROUPING(r.name), region, GROUPING(o.channel), channel"""),
    ("customers_buying_electronics_and_home",
     """SELECT o.customer_id FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE p.category = 'Electronics' AND o.status <> 'cancelled'
         INTERSECT
         SELECT o.customer_id FROM orders o JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE p.category = 'Home' AND o.status <> 'cancelled'
         ORDER BY 1"""),
    ("contacts_who_never_bought",
     """SELECT DISTINCT t.customer_id, 'ticket' AS via FROM support_tickets t
         WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = t.customer_id AND o.status <> 'cancelled')
         UNION ALL
         SELECT DISTINCT w.customer_id, 'web' FROM web_events w
         WHERE w.customer_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = w.customer_id AND o.status <> 'cancelled')
         ORDER BY 1, 2"""),
    ("email_domain_hygiene",
     """SELECT LOWER(SUBSTR(c.email, LOCATE(c.email, '@') + 1)) AS domain, COUNT(*) AS customers,
           SUM(CASE WHEN c.email LIKE '%_@_%._%' THEN 1 ELSE 0 END) AS well_formed,
           SUM(CASE WHEN c.email <> LOWER(c.email) THEN 1 ELSE 0 END) AS not_lower_case,
           LIST(c.first_name, ', ' ORDER BY c.first_name) AS names,
           MIN(LENGTH(c.email)) AS shortest, MAX(LENGTH(c.email)) AS longest
         FROM customers c WHERE c.email IS NOT NULL
         GROUP BY LOWER(SUBSTR(c.email, LOCATE(c.email, '@') + 1)) ORDER BY customers DESC, domain"""),
    ("phone_numbers_cleaned",
     """WITH d AS (SELECT id, phone, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', ''), '.', ''), '+', '') AS digits
                  FROM customers)
         SELECT id, phone, digits, LENGTH(digits) AS n_digits,
           CASE WHEN LENGTH(digits) = 10 THEN '(' || LEFT(digits, 3) || ') ' || SUBSTR(digits, 4, 3) || '-' || RIGHT(digits, 4)
                WHEN LENGTH(digits) = 11 AND LEFT(digits, 1) = '1' THEN '(' || SUBSTR(digits, 2, 3) || ') ' || SUBSTR(digits, 5, 3) || '-' || RIGHT(digits, 4)
                WHEN digits IS NULL THEN 'missing' ELSE 'check' END AS formatted
         FROM d ORDER BY id"""),
    ("staff_directory_badges",
     """SELECT e.id, UPPER(LEFT(e.first_name, 1)) || '. ' || e.last_name AS short_name, LEFT(e.first_name, 1) || LEFT(e.last_name, 1) AS initials,
           LOWER(e.first_name) || '.' || LOWER(e.last_name) || '@corp.example.com' AS email_guess,
           REPEAT('*', CAST(e.salary / 25000 AS INTEGER)) AS salary_bar,
           RIGHT('0000' || CAST(e.id AS VARCHAR(4)), 4) AS badge,
           DATEDIFF(YEAR, e.hire_date, CAST('2026-01-01' AS DATE)) AS tenure_years,
           LTRIM(RTRIM(m.first_name || ' ' || m.last_name)) AS manager,
           CAST(ROUND(100 * e.salary / NULLIF(m.salary, 0), 1) AS NUMERIC(5,1)) AS pct_of_manager_salary,
           RANK() OVER (ORDER BY e.salary DESC) AS salary_rank
         FROM employees e LEFT JOIN employees m ON m.id = e.manager_id ORDER BY e.id"""),
    ("cumulative_new_customers",
     """WITH firsts AS (SELECT customer_id, MIN(order_ts) AS first_ts FROM orders WHERE status <> 'cancelled' GROUP BY customer_id),
             monthly AS (SELECT DATEFORMAT(o.order_ts, 'yyyy-mm') AS ym, COUNT(DISTINCT o.customer_id) AS active,
                                COUNT(DISTINCT CASE WHEN DATEFORMAT(f.first_ts, 'yyyy-mm') = DATEFORMAT(o.order_ts, 'yyyy-mm')
                                                    THEN o.customer_id END) AS new_customers
                         FROM orders o JOIN firsts f ON f.customer_id = o.customer_id WHERE o.status <> 'cancelled'
                         GROUP BY DATEFORMAT(o.order_ts, 'yyyy-mm'))
         SELECT ym, active, new_customers, SUM(new_customers) OVER (ORDER BY ym ROWS UNBOUNDED PRECEDING) AS cumulative_customers,
           active - new_customers AS returning, LEAD(active) OVER (ORDER BY ym) AS next_month_active,
           LAST_VALUE(active) OVER (ORDER BY ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_active
         FROM monthly ORDER BY ym"""),
    ("products_gone_quiet",
     """SELECT p.id, p.sku, p.name, p.category,
           (SELECT MAX(o.order_ts) FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE oi.product_id = p.id) AS last_sold,
           DATEDIFF(DAY, (SELECT MAX(o.order_ts) FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE oi.product_id = p.id),
                    CAST('2026-01-01' AS TIMESTAMP)) AS days_since
         FROM products p
         WHERE p.discontinued = 0
           AND NOT EXISTS (SELECT 1 FROM order_items oi JOIN orders o ON o.id = oi.order_id
                           WHERE oi.product_id = p.id AND o.status <> 'cancelled'
                             AND o.order_ts >= DATEADD(DAY, -90, CAST('2026-01-01' AS TIMESTAMP)))
         ORDER BY days_since DESC, p.id"""),
    ("same_month_last_year",
     """WITH m AS (SELECT YEAR(o.order_ts) AS y, MONTH(o.order_ts) AS mo, SUM(oi.quantity * oi.unit_price) AS revenue
                  FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.status <> 'cancelled'
                  GROUP BY YEAR(o.order_ts), MONTH(o.order_ts))
         SELECT cur.y, cur.mo, DATENAME(MONTH, YMD(cur.y, cur.mo, 1)) AS month_name, cur.revenue, prev.revenue AS last_year,
           cur.revenue - prev.revenue AS change,
           CAST(ROUND(100 * (cur.revenue - prev.revenue) / NULLIF(prev.revenue, 0), 1) AS NUMERIC(7,1)) AS yoy_pct
         FROM m cur LEFT JOIN m prev ON prev.y = cur.y - 1 AND prev.mo = cur.mo ORDER BY cur.y, cur.mo"""),
    ("order_value_bands",
     """WITH t AS (SELECT o.id, o.customer_id, SUM(oi.quantity * oi.unit_price) AS total
                  FROM orders o JOIN order_items oi ON oi.order_id = o.id GROUP BY o.id, o.customer_id)
         SELECT id, customer_id, total, NTILE(4) OVER (ORDER BY total, id) AS quartile,
           CAST(CUME_DIST() OVER (ORDER BY total) AS NUMERIC(6,4)) AS cume,
           CASE WHEN total >= 1000 THEN 'large' WHEN total >= 250 THEN 'medium' ELSE 'small' END AS band,
           total - AVG(total) OVER () AS vs_mean
         FROM t ORDER BY total DESC, id"""),
]

CASES = [("analyst_patterns", PATTERNS), ("analyst_sets_and_strings", SETS_AND_STRINGS)]
