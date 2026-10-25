"""Realistic report queries over the retail schema, part E: the web shop's events (URLs parsed by hand, UTM parameters,
sessions, funnels), support tickets (resolution times, SLAs, backlog over a calendar, subjects classified), exchange
rates (as-of lookups, conversions), promotions over date ranges, and inventory snapshots (stock-outs, cover)."""

HOST_START = "LOCATE(url, '://') + 3"
HOST_END = f"LOCATE(url, '/', {HOST_START})"
PATH = (f"CASE WHEN LOCATE(url, '?') > 0 THEN SUBSTRING(url, {HOST_END}, LOCATE(url, '?') - {HOST_END})"
        f" ELSE SUBSTRING(url, {HOST_END}) END")
REF_DOMAIN = ("REPLACE(SUBSTRING(referrer, LOCATE(referrer, '://') + 3, LOCATE(referrer, '/', LOCATE(referrer, '://') + 3)"
              " - LOCATE(referrer, '://') - 3), 'www.', '')")

WEB = [
    ("url_parts",
     f"""SELECT id, LOWER(SUBSTRING(url, {HOST_START}, {HOST_END} - ({HOST_START}))) AS host, {PATH} AS path,
           CASE WHEN LOCATE(url, '?') > 0 THEN SUBSTRING(url, LOCATE(url, '?') + 1) END AS query_string
         FROM web_events WHERE id <= 40 ORDER BY id"""),
    ("utm_sources",
     """WITH tagged AS (
           SELECT id, customer_id,
             CASE WHEN LOCATE(url, 'utm_source=') = 0 THEN NULL
                  WHEN LOCATE(url, '&', LOCATE(url, 'utm_source=')) = 0 THEN SUBSTRING(url, LOCATE(url, 'utm_source=') + 11)
                  ELSE SUBSTRING(url, LOCATE(url, 'utm_source=') + 11,
                                 LOCATE(url, '&', LOCATE(url, 'utm_source=')) - LOCATE(url, 'utm_source=') - 11) END AS utm_source
           FROM web_events)
         SELECT COALESCE(utm_source, '(none)') AS source, COUNT(*) AS events, COUNT(DISTINCT customer_id) AS known_visitors
         FROM tagged GROUP BY COALESCE(utm_source, '(none)') ORDER BY events DESC, source"""),
    ("referrer_domains",
     f"""SELECT {REF_DOMAIN} AS referrer_domain, COUNT(*) AS visits, MIN(event_ts) AS first_seen
         FROM web_events WHERE referrer IS NOT NULL GROUP BY {REF_DOMAIN} ORDER BY visits DESC, referrer_domain"""),
    ("product_page_views",
     """SELECT p.id, p.name, COUNT(*) AS views, COUNT(DISTINCT v.customer_id) AS known_viewers
         FROM (SELECT id, customer_id,
                 CASE WHEN url LIKE '%/products/%' THEN CAST(CASE WHEN LOCATE(url, '?') > 0
                      THEN SUBSTRING(url, LOCATE(url, '/products/') + 10, LOCATE(url, '?') - LOCATE(url, '/products/') - 10)
                      ELSE SUBSTRING(url, LOCATE(url, '/products/') + 10) END AS INTEGER) END AS product_id
               FROM web_events) v
           JOIN products p ON p.id = v.product_id
         GROUP BY p.id, p.name ORDER BY views DESC, p.id"""),
    ("search_terms",
     """SELECT term, COUNT(*) AS searches, COUNT(DISTINCT device) AS devices
         FROM (SELECT REPLACE(CASE WHEN LOCATE(url, '&', LOCATE(url, 'q=')) > 0
                                   THEN SUBSTRING(url, LOCATE(url, 'q=') + 2, LOCATE(url, '&', LOCATE(url, 'q=')) - LOCATE(url, 'q=') - 2)
                                   ELSE SUBSTRING(url, LOCATE(url, 'q=') + 2) END, '+', ' ') AS term, device
               FROM web_events WHERE url LIKE '%/search?q=%') s
         GROUP BY term ORDER BY searches DESC, term"""),
    ("sessions_of_known_visitors",
     """WITH e AS (SELECT id, customer_id, event_ts, url,
                          CASE WHEN LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, id) IS NULL
                                 OR DATEDIFF(MINUTE, LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts, id), event_ts) > 30
                               THEN 1 ELSE 0 END AS new_session
                   FROM web_events WHERE customer_id IS NOT NULL),
         s AS (SELECT id, customer_id, event_ts, url,
                      SUM(new_session) OVER (PARTITION BY customer_id ORDER BY event_ts, id ROWS UNBOUNDED PRECEDING) AS session_no
               FROM e)
         SELECT customer_id, session_no, MIN(event_ts) AS started, DATEDIFF(MINUTE, MIN(event_ts), MAX(event_ts)) AS minutes,
           COUNT(*) AS pages, MAX(CASE WHEN url LIKE '%/order/confirmation%' THEN 1 ELSE 0 END) AS converted
         FROM s GROUP BY customer_id, session_no ORDER BY customer_id, session_no"""),
    ("funnel_by_device",
     """WITH visits AS (
           SELECT device, customer_id, CAST(event_ts AS DATE) AS day,
             MAX(CASE WHEN url LIKE '%/products/%' THEN 1 ELSE 0 END) AS saw_product,
             MAX(CASE WHEN url LIKE '%/cart' THEN 1 ELSE 0 END) AS saw_cart,
             MAX(CASE WHEN url LIKE '%/checkout%' THEN 1 ELSE 0 END) AS saw_checkout,
             MAX(CASE WHEN url LIKE '%/order/confirmation' THEN 1 ELSE 0 END) AS ordered
           FROM web_events GROUP BY device, customer_id, CAST(event_ts AS DATE))
         SELECT device, COUNT(*) AS visits, SUM(saw_product) AS product, SUM(saw_cart) AS cart,
           SUM(saw_checkout) AS checkout, SUM(ordered) AS ordered
         FROM visits GROUP BY device ORDER BY device"""),
    ("traffic_by_weekday_and_daypart",
     """SELECT DOW(event_ts) AS dow, DATENAME(WEEKDAY, event_ts) AS weekday,
           SUM(CASE WHEN HOUR(event_ts) < 6 THEN 1 ELSE 0 END) AS night,
           SUM(CASE WHEN HOUR(event_ts) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) AS morning,
           SUM(CASE WHEN HOUR(event_ts) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS afternoon,
           SUM(CASE WHEN HOUR(event_ts) >= 18 THEN 1 ELSE 0 END) AS evening, COUNT(*) AS total
         FROM web_events GROUP BY DOW(event_ts), DATENAME(WEEKDAY, event_ts) ORDER BY dow"""),
    ("device_mix_by_month",
     """SELECT DATEFORMAT(event_ts, 'yyyy-mm') AS month, COUNT(*) AS events,
           ROUND(100.0 * SUM(CASE WHEN device = 'mobile' THEN 1 ELSE 0 END) / COUNT(*), 1) AS mobile_pct,
           ROUND(100.0 * SUM(CASE WHEN device = 'desktop' THEN 1 ELSE 0 END) / COUNT(*), 1) AS desktop_pct
         FROM web_events GROUP BY DATEFORMAT(event_ts, 'yyyy-mm') ORDER BY month"""),
    ("visitors_who_bought_within_a_week",
     """SELECT DATEFORMAT(w.event_ts, 'yyyy-mm') AS month, COUNT(DISTINCT w.customer_id) AS visitors,
           COUNT(DISTINCT o.customer_id) AS bought_within_7_days
         FROM web_events w LEFT JOIN orders o
           ON o.customer_id = w.customer_id AND o.order_ts BETWEEN w.event_ts AND DATEADD(DAY, 7, w.event_ts)
         WHERE w.customer_id IS NOT NULL GROUP BY DATEFORMAT(w.event_ts, 'yyyy-mm') ORDER BY month"""),
    ("top_referrer_per_month",
     f"""WITH r AS (SELECT DATEFORMAT(event_ts, 'yyyy-mm') AS month, {REF_DOMAIN} AS domain, COUNT(*) AS visits
                   FROM web_events WHERE referrer IS NOT NULL GROUP BY DATEFORMAT(event_ts, 'yyyy-mm'), {REF_DOMAIN})
         SELECT month, domain, visits FROM (
           SELECT month, domain, visits, ROW_NUMBER() OVER (PARTITION BY month ORDER BY visits DESC, domain) AS rn FROM r) x
         WHERE rn = 1 ORDER BY month"""),
    ("url_depth_and_length",
     f"""SELECT id, {PATH} AS path, LENGTH({PATH}) - LENGTH(REPLACE({PATH}, '/', '')) AS depth, LENGTH(url) AS url_length,
           CASE WHEN url <> LOWER(url) THEN 'mixed case' ELSE '' END AS note
         FROM web_events WHERE id BETWEEN 30 AND 80 ORDER BY id"""),
]

SLA_HOURS = "CASE priority WHEN 'urgent' THEN 4 WHEN 'high' THEN 24 WHEN 'normal' THEN 72 ELSE 168 END"
TOPIC = """CASE WHEN LOWER(subject) LIKE '%refund%' THEN 'refund' WHEN LOWER(subject) LIKE '%cancel%' THEN 'cancellation'
                WHEN LOWER(subject) LIKE '%package%' OR LOWER(subject) LIKE '%shipping%' OR LOWER(subject) LIKE '%delivered%'
                THEN 'delivery' WHEN LOWER(subject) LIKE '%damaged%' OR LOWER(subject) LIKE '%missing%' THEN 'quality'
                WHEN LOWER(subject) LIKE '%invoice%' THEN 'billing' ELSE 'other' END"""

TICKETS = [
    ("resolution_by_priority",
     """SELECT priority, COUNT(*) AS tickets, COUNT(closed_ts) AS closed, COUNT(*) - COUNT(closed_ts) AS still_open,
           ROUND(AVG(DATEDIFF(MINUTE, opened_ts, closed_ts) / 60.0), 1) AS avg_hours,
           MEDIAN(DATEDIFF(MINUTE, opened_ts, closed_ts)) AS median_minutes, MAX(DATEDIFF(HOUR, opened_ts, closed_ts)) AS worst_hours
         FROM support_tickets GROUP BY priority
         ORDER BY CASE priority WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 WHEN 'normal' THEN 3 ELSE 4 END, priority"""),
    ("sla_breaches_by_month",
     f"""SELECT DATEFORMAT(opened_ts, 'yyyy-mm') AS month, COUNT(*) AS tickets,
           SUM(CASE WHEN closed_ts IS NULL OR DATEDIFF(MINUTE, opened_ts, closed_ts) > 60 * {SLA_HOURS} THEN 1 ELSE 0 END) AS breaches,
           SUM(CASE WHEN priority IN ('urgent', 'high') THEN 1 ELSE 0 END) AS high_priority
         FROM support_tickets WHERE opened_ts >= '2025-01-01' GROUP BY DATEFORMAT(opened_ts, 'yyyy-mm') ORDER BY month"""),
    ("backlog_at_each_month_end",
     """SELECT m.month_end, COUNT(t.id) AS open_tickets,
           SUM(CASE WHEN t.priority IN ('urgent', 'high') THEN 1 ELSE 0 END) AS open_high_priority
         FROM (SELECT DATEADD(DAY, -1, DATEADD(MONTH, row_num, '2024-06-01')) AS month_end FROM sa_rowgenerator(1, 19)) m
           LEFT JOIN support_tickets t ON t.opened_ts <= m.month_end AND (t.closed_ts IS NULL OR t.closed_ts > m.month_end)
         GROUP BY m.month_end ORDER BY m.month_end"""),
    ("agent_workload",
     """SELECT e.id, e.first_name || ' ' || e.last_name AS agent, COUNT(t.id) AS tickets, COUNT(t.closed_ts) AS resolved,
           ROUND(AVG(CASE WHEN t.closed_ts IS NOT NULL THEN DATEDIFF(MINUTE, t.opened_ts, t.closed_ts) END) / 60.0, 1) AS avg_hours,
           RANK() OVER (ORDER BY COUNT(t.closed_ts) DESC) AS rank_by_resolved
         FROM employees e LEFT JOIN support_tickets t ON t.agent_id = e.id
         WHERE e.title IN ('Account Executive', 'Sales Associate')
         GROUP BY e.id, e.first_name, e.last_name ORDER BY rank_by_resolved, e.id"""),
    ("ticket_topics",
     f"""SELECT topic, COUNT(*) AS tickets, SUM(CASE WHEN closed_ts IS NULL THEN 1 ELSE 0 END) AS open_tickets,
           MIN(opened_ts) AS first_seen
         FROM (SELECT id, closed_ts, opened_ts, {TOPIC} AS topic FROM support_tickets) x
         GROUP BY topic ORDER BY tickets DESC, topic"""),
    ("subjects_cleaned",
     """SELECT id, UPPER(LEFT(TRIM(subject), 1)) || LOWER(SUBSTRING(TRIM(subject), 2)) AS clean_subject,
           CASE WHEN LENGTH(TRIM(subject)) > 30 THEN LEFT(TRIM(subject), 27) || '...' ELSE TRIM(subject) END AS short_subject,
           LENGTH(subject) - LENGTH(TRIM(subject)) AS padding
         FROM support_tickets WHERE id <= 30 ORDER BY id"""),
    ("escalations_with_order_context",
     """SELECT t.id, TRIM(c.first_name) || ' ' || c.last_name AS customer, t.priority, o.id AS order_id, o.status AS order_status,
           DATEDIFF(DAY, o.order_ts, t.opened_ts) AS days_after_order,
           CASE WHEN o.id IS NULL THEN 'no order' WHEN DATEDIFF(DAY, o.order_ts, t.opened_ts) <= 30 THEN 'recent order'
                ELSE 'older order' END AS context
         FROM support_tickets t JOIN customers c ON c.id = t.customer_id LEFT JOIN orders o ON o.id = t.order_id
         WHERE t.priority IN ('high', 'urgent') ORDER BY t.opened_ts, t.id"""),
    ("repeat_contacts_within_90_days",
     """SELECT t1.customer_id, t1.id, t1.opened_ts, COUNT(t2.id) AS later_contacts
         FROM support_tickets t1 JOIN support_tickets t2
           ON t2.customer_id = t1.customer_id AND t2.id <> t1.id
          AND t2.opened_ts BETWEEN t1.opened_ts AND DATEADD(DAY, 90, t1.opened_ts)
         GROUP BY t1.customer_id, t1.id, t1.opened_ts ORDER BY t1.customer_id, t1.opened_ts, t1.id"""),
    ("ticket_history_per_customer",
     """SELECT customer_id, COUNT(*) AS tickets, LIST(TRIM(subject), ' | ' ORDER BY opened_ts, id) AS history,
           MAX(opened_ts) AS latest
         FROM support_tickets GROUP BY customer_id HAVING COUNT(*) >= 3 ORDER BY customer_id"""),
    ("off_hours_tickets",
     """SELECT channel, COUNT(*) AS tickets,
           SUM(CASE WHEN DOW(opened_ts) IN (1, 7) THEN 1 ELSE 0 END) AS weekend,
           SUM(CASE WHEN DOW(opened_ts) BETWEEN 2 AND 6 AND (HOUR(opened_ts) < 9 OR HOUR(opened_ts) >= 17) THEN 1 ELSE 0 END) AS after_hours
         FROM support_tickets GROUP BY channel ORDER BY channel"""),
]

COMMERCE = [
    ("as_of_exchange_rate_per_order",
     """SELECT o.id, r.country, CAST(o.order_ts AS DATE) AS order_date,
           (SELECT x.usd_rate FROM exchange_rates x
             WHERE x.currency = CASE r.country WHEN 'Germany' THEN 'EUR' ELSE 'CAD' END
               AND x.rate_date = (SELECT MAX(x2.rate_date) FROM exchange_rates x2
                                   WHERE x2.currency = CASE r.country WHEN 'Germany' THEN 'EUR' ELSE 'CAD' END
                                     AND x2.rate_date <= o.order_ts)) AS usd_rate
         FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id
         WHERE r.country <> 'USA' AND o.status = 'shipped' ORDER BY o.id"""),
    ("local_currency_revenue",
     """WITH order_rates AS (
           SELECT o.id AS order_id, MAX(CASE r.country WHEN 'Germany' THEN 'EUR' ELSE 'CAD' END) AS currency,
                  MAX(x.rate_date) AS rate_date
           FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id
             JOIN exchange_rates x ON x.currency = CASE r.country WHEN 'Germany' THEN 'EUR' ELSE 'CAD' END
                                  AND x.rate_date <= o.order_ts
           WHERE r.country IN ('Germany', 'Canada') AND o.status = 'shipped' GROUP BY o.id)
         SELECT q.currency, YEAR(o.order_ts) AS yr, SUM(oi.quantity * oi.unit_price) AS usd_amount,
           ROUND(SUM(oi.quantity * oi.unit_price / x.usd_rate), 2) AS local_amount
         FROM order_rates q JOIN orders o ON o.id = q.order_id JOIN order_items oi ON oi.order_id = o.id
           JOIN exchange_rates x ON x.currency = q.currency AND x.rate_date = q.rate_date
         GROUP BY q.currency, YEAR(o.order_ts) ORDER BY q.currency, yr"""),
    ("rate_changes_and_gaps",
     """SELECT currency, rate_date, usd_rate,
           usd_rate - LAG(usd_rate) OVER (PARTITION BY currency ORDER BY rate_date, id) AS change,
           CASE WHEN DATEDIFF(MONTH, LAG(rate_date) OVER (PARTITION BY currency ORDER BY rate_date, id), rate_date) > 1
                THEN 'gap before' ELSE '' END AS note
         FROM exchange_rates WHERE rate_date >= '2024-01-01' AND rate_date < '2025-01-01' ORDER BY currency, rate_date, id"""),
    ("yearly_rate_ranges",
     """SELECT currency, YEAR(rate_date) AS yr, MIN(usd_rate) AS low, MAX(usd_rate) AS high, ROUND(AVG(usd_rate), 4) AS avg_rate,
           MAX(usd_rate) - MIN(usd_rate) AS spread, COUNT(*) AS months
         FROM exchange_rates GROUP BY currency, YEAR(rate_date) ORDER BY currency, yr"""),
    ("orders_during_promotions",
     """SELECT pr.code, COUNT(DISTINCT o.id) AS orders, SUM(oi.quantity) AS units,
           ROUND(SUM(oi.quantity * oi.unit_price * pr.pct_off), 2) AS est_discount
         FROM promotions pr JOIN orders o ON CAST(o.order_ts AS DATE) BETWEEN pr.starts AND pr.ends
           JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id
         WHERE (pr.category IS NULL OR pr.category = p.category) AND o.status <> 'cancelled'
         GROUP BY pr.code ORDER BY pr.code"""),
    ("overlapping_promotions",
     """SELECT a.code AS promo, b.code AS overlaps_with, GREATER(a.starts, b.starts) AS overlap_from,
           LESSER(a.ends, b.ends) AS overlap_to, DATEDIFF(DAY, GREATER(a.starts, b.starts), LESSER(a.ends, b.ends)) + 1 AS overlap_days
         FROM promotions a JOIN promotions b ON a.id < b.id AND a.starts <= b.ends AND b.starts <= a.ends
         ORDER BY a.id, b.id"""),
    ("promotion_lift",
     """SELECT code, promo_days, promo_orders, orders_30_days_before,
           CASE WHEN orders_30_days_before = 0 THEN NULL
                ELSE ROUND(promo_orders * 30.0 / (promo_days * orders_30_days_before), 2) END AS lift
         FROM (SELECT pr.id, pr.code, DATEDIFF(DAY, pr.starts, pr.ends) + 1 AS promo_days,
                 (SELECT COUNT(*) FROM orders o WHERE CAST(o.order_ts AS DATE) BETWEEN pr.starts AND pr.ends) AS promo_orders,
                 (SELECT COUNT(*) FROM orders o
                   WHERE CAST(o.order_ts AS DATE) BETWEEN DATEADD(DAY, -30, pr.starts) AND DATEADD(DAY, -1, pr.starts)) AS orders_30_days_before
               FROM promotions pr) x
         ORDER BY id"""),
    ("promotion_lift_by_join",
     """WITH counts AS (
           SELECT pr.id, pr.code, DATEDIFF(DAY, pr.starts, pr.ends) + 1 AS promo_days,
             SUM(CASE WHEN CAST(o.order_ts AS DATE) BETWEEN pr.starts AND pr.ends THEN 1 ELSE 0 END) AS promo_orders,
             SUM(CASE WHEN CAST(o.order_ts AS DATE) < pr.starts THEN 1 ELSE 0 END) AS orders_30_days_before
           FROM promotions pr LEFT JOIN orders o ON CAST(o.order_ts AS DATE) BETWEEN DATEADD(DAY, -30, pr.starts) AND pr.ends
           GROUP BY pr.id, pr.code, pr.starts, pr.ends)
         SELECT code, promo_days, promo_orders, orders_30_days_before,
           CASE WHEN orders_30_days_before = 0 THEN NULL
                ELSE ROUND(promo_orders * 30.0 / (promo_days * orders_30_days_before), 2) END AS lift
         FROM counts ORDER BY id"""),
    ("promotion_labels",
     """SELECT id, code, CASE WHEN ISNUMERIC(RIGHT(code, 2)) = 1 THEN LEFT(code, LENGTH(code) - 2) || ' 20' || RIGHT(code, 2)
                        ELSE code END AS display_code,
           DATEFORMAT(starts, 'Mmm dd') || ' - ' || DATEFORMAT(ends, 'Mmm dd, yyyy') AS period,
           CAST(CAST(pct_off * 100 AS INTEGER) AS VARCHAR(3)) || '% off' || COALESCE(' ' || LOWER(category), ' everything') AS label
         FROM promotions ORDER BY id"""),
    ("promotion_days_per_month_2024",
     """SELECT MONTH(d.day) AS mo, COUNT(DISTINCT d.day) AS days, COUNT(DISTINCT CASE WHEN pr.id IS NOT NULL THEN d.day END) AS promo_days
         FROM (SELECT DATEADD(DAY, row_num - 1, '2024-01-01') AS day FROM sa_rowgenerator(1, 366)) d
           LEFT JOIN promotions pr ON d.day BETWEEN pr.starts AND pr.ends
         GROUP BY MONTH(d.day) ORDER BY mo"""),
    ("stock_status_now",
     """SELECT p.id, p.name, i.on_hand, i.reorder_point,
           CASE WHEN i.on_hand = 0 THEN 'OUT OF STOCK' WHEN i.on_hand < i.reorder_point THEN 'reorder' ELSE 'ok' END AS status
         FROM products p JOIN inventory i ON i.product_id = p.id
         WHERE i.snapshot_date = (SELECT MAX(i2.snapshot_date) FROM inventory i2 WHERE i2.product_id = p.id)
         ORDER BY CASE WHEN i.on_hand = 0 THEN 0 WHEN i.on_hand < i.reorder_point THEN 1 ELSE 2 END, p.id, i.id"""),
    ("runs_below_reorder_point",
     """WITH below AS (SELECT product_id, snapshot_date,
                              MONTH(snapshot_date) - ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date, id) AS grp
                       FROM inventory WHERE on_hand < reorder_point)
         SELECT product_id, MIN(snapshot_date) AS from_month, MAX(snapshot_date) AS to_month, COUNT(*) AS months
         FROM below GROUP BY product_id, grp ORDER BY product_id, from_month"""),
    ("inventory_movements",
     """SELECT product_id, snapshot_date, on_hand,
           on_hand - LAG(on_hand) OVER (PARTITION BY product_id ORDER BY snapshot_date, id) AS change,
           CASE WHEN on_hand - LAG(on_hand) OVER (PARTITION BY product_id ORDER BY snapshot_date, id) > 0 THEN 'restocked'
                ELSE '' END AS event
         FROM inventory WHERE product_id IN (1, 2, 3) ORDER BY product_id, snapshot_date, id"""),
    ("stock_value_by_category",
     """WITH latest AS (SELECT product_id, on_hand FROM inventory WHERE snapshot_date = '2025-12-01')
         SELECT p.category, SUM(l.on_hand) AS units, SUM(l.on_hand * p.unit_cost) AS stock_value,
           MAX(l.on_hand * p.unit_cost) AS largest_position
         FROM latest l JOIN products p ON p.id = l.product_id GROUP BY p.category ORDER BY stock_value DESC, p.category"""),
    ("days_of_cover",
     """SELECT p.id, p.name, i.on_hand, COALESCE(s.units, 0) AS units_sold_2025,
           CASE WHEN COALESCE(s.units, 0) = 0 THEN NULL ELSE i.on_hand * 365 / s.units END AS days_of_cover
         FROM products p JOIN inventory i ON i.product_id = p.id AND i.snapshot_date = '2025-12-01'
           LEFT JOIN (SELECT oi.product_id, SUM(oi.quantity) AS units FROM order_items oi JOIN orders o ON o.id = oi.order_id
                      WHERE YEAR(o.order_ts) = 2025 AND o.status <> 'cancelled' GROUP BY oi.product_id) s ON s.product_id = p.id
         ORDER BY p.id"""),
    ("customer_activity_overview",
     """SELECT c.id, TRIM(c.first_name) || ' ' || c.last_name AS customer, COALESCE(o.orders, 0) AS orders,
           COALESCE(t.tickets, 0) AS tickets, COALESCE(w.events, 0) AS web_events,
           CASE WHEN COALESCE(t.tickets, 0) >= 3 THEN 'needs attention'
                WHEN COALESCE(w.events, 0) > 0 AND COALESCE(o.orders, 0) = 0 THEN 'browser only' ELSE 'normal' END AS flag
         FROM customers c
           LEFT JOIN (SELECT customer_id, COUNT(*) AS orders FROM orders WHERE status <> 'cancelled' GROUP BY customer_id) o
             ON o.customer_id = c.id
           LEFT JOIN (SELECT customer_id, COUNT(*) AS tickets FROM support_tickets GROUP BY customer_id) t ON t.customer_id = c.id
           LEFT JOIN (SELECT customer_id, COUNT(*) AS events FROM web_events WHERE customer_id IS NOT NULL GROUP BY customer_id) w
             ON w.customer_id = c.id
         ORDER BY c.id"""),
]

CASES = [("web", WEB), ("tickets", TICKETS), ("commerce", COMMERCE)]
