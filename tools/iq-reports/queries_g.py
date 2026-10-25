"""Realistic report queries over the retail schema, part G: web analytics by LEAD (time on page, exits, bounces),
collections (days sales outstanding, how much is paid how soon), people (salary bands, spans of control), stock
classes, conditional windows, and the formatted text analysts build by hand (currency, zero padding, percent labels)."""

LINE = "oi.quantity * oi.unit_price - oi.line_discount"

ANALYTICS = [
    ("time_on_page_by_lead",
     """SELECT id, customer_id, event_ts, url,
           DATEDIFF(SECOND, event_ts, LEAD(event_ts) OVER (PARTITION BY customer_id, device ORDER BY event_ts, id)) AS seconds_on_page
         FROM web_events WHERE customer_id BETWEEN 1 AND 6 ORDER BY customer_id, event_ts, id"""),
    ("exit_pages",
     """SELECT CASE WHEN LOCATE(url, '?') > 0 THEN SUBSTRING(url, 25, LOCATE(url, '?') - 25) ELSE SUBSTRING(url, 25) END AS page,
           COUNT(*) AS exits
         FROM (SELECT id, LOWER(url) AS url, LEAD(id) OVER (PARTITION BY customer_id, device ORDER BY event_ts, id) AS next_id
               FROM web_events WHERE customer_id IS NOT NULL) e
         WHERE next_id IS NULL
         GROUP BY CASE WHEN LOCATE(url, '?') > 0 THEN SUBSTRING(url, 25, LOCATE(url, '?') - 25) ELSE SUBSTRING(url, 25) END
         ORDER BY exits DESC, page"""),
    ("bounce_rate_by_device",
     """WITH visits AS (SELECT device, customer_id, CAST(event_ts AS DATE) AS day, COUNT(*) AS pages
                     FROM web_events WHERE customer_id IS NOT NULL GROUP BY device, customer_id, CAST(event_ts AS DATE))
         SELECT device, COUNT(*) AS visits, SUM(CASE WHEN pages = 1 THEN 1 ELSE 0 END) AS bounces,
           ROUND(100.0 * SUM(CASE WHEN pages = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS bounce_pct, MAX(pages) AS deepest
         FROM visits GROUP BY device ORDER BY device"""),
    ("entry_and_exit_per_visit",
     """SELECT DISTINCT customer_id, device, CAST(event_ts AS DATE) AS day,
           FIRST_VALUE(url) OVER (PARTITION BY customer_id, device, CAST(event_ts AS DATE) ORDER BY event_ts, id
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS entry_url,
           LAST_VALUE(url) OVER (PARTITION BY customer_id, device, CAST(event_ts AS DATE) ORDER BY event_ts, id
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS exit_url
         FROM web_events WHERE customer_id BETWEEN 1 AND 10 ORDER BY customer_id, device, day"""),
    ("conditional_running_counts",
     """SELECT id, customer_id, event_ts,
           SUM(CASE WHEN url LIKE '%/cart%' THEN 1 ELSE 0 END) OVER (PARTITION BY customer_id ORDER BY event_ts, id
                                                                 ROWS UNBOUNDED PRECEDING) AS carts_so_far,
           SUM(CASE WHEN url LIKE '%/order/confirmation%' THEN 1 ELSE 0 END) OVER (PARTITION BY customer_id) AS orders_total
         FROM web_events WHERE customer_id IN (2, 4, 6, 8) ORDER BY customer_id, event_ts, id"""),
]

FINANCE = [
    ("days_sales_outstanding_by_quarter",
     f"""WITH invoiced AS (SELECT o.id, o.order_ts, SUM({LINE}) AS amount FROM orders o JOIN order_items oi ON oi.order_id = o.id
                          WHERE o.status <> 'cancelled' GROUP BY o.id, o.order_ts),
         settled AS (SELECT order_id, MAX(paid_ts) AS paid_in_full, SUM(amount) AS paid FROM payments GROUP BY order_id)
         SELECT YEAR(i.order_ts) AS yr, QUARTER(i.order_ts) AS qtr, COUNT(*) AS invoices,
           ROUND(AVG(DATEDIFF(DAY, i.order_ts, s.paid_in_full) * 1.0), 1) AS avg_days_to_pay,
           MAX(DATEDIFF(DAY, i.order_ts, s.paid_in_full)) AS slowest
         FROM invoiced i JOIN settled s ON s.order_id = i.id
         GROUP BY YEAR(i.order_ts), QUARTER(i.order_ts) ORDER BY yr, qtr"""),
    ("collection_curve",
     """SELECT CASE WHEN DATEDIFF(DAY, o.order_ts, p.paid_ts) <= 1 THEN '0-1 days' WHEN DATEDIFF(DAY, o.order_ts, p.paid_ts) <= 7 THEN '2-7 days'
                     WHEN DATEDIFF(DAY, o.order_ts, p.paid_ts) <= 30 THEN '8-30 days' ELSE 'over 30 days' END AS paid_within,
           COUNT(*) AS payments, SUM(p.amount) AS collected,
           SUM(SUM(p.amount)) OVER (ORDER BY MIN(DATEDIFF(DAY, o.order_ts, p.paid_ts))) AS cumulative
         FROM payments p JOIN orders o ON o.id = p.order_id
         GROUP BY CASE WHEN DATEDIFF(DAY, o.order_ts, p.paid_ts) <= 1 THEN '0-1 days' WHEN DATEDIFF(DAY, o.order_ts, p.paid_ts) <= 7 THEN '2-7 days'
                       WHEN DATEDIFF(DAY, o.order_ts, p.paid_ts) <= 30 THEN '8-30 days' ELSE 'over 30 days' END
         ORDER BY MIN(DATEDIFF(DAY, o.order_ts, p.paid_ts))"""),
    ("formatted_invoice_lines",
     f"""SELECT o.id, 'INV' || RIGHT(REPEAT('0', 6) || CAST(o.id AS VARCHAR(6)), 6) AS invoice,
           '$' || CAST(CAST(SUM({LINE}) AS NUMERIC(12,2)) AS VARCHAR(20)) AS amount_text,
           CAST(CAST(o.discount_pct * 100 AS INTEGER) AS VARCHAR(3)) || '%' AS discount_text,
           UPPER(LEFT(o.status, 1)) || SUBSTRING(o.status, 2) || ' / ' || o.channel AS status_text
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.id <= 25
         GROUP BY o.id, o.discount_pct, o.status, o.channel ORDER BY o.id"""),
    ("payment_method_mix_by_segment",
     """SELECT c.segment,
           SUM(CASE WHEN p.method = 'card' THEN p.amount ELSE 0 END) AS card,
           SUM(CASE WHEN p.method = 'paypal' THEN p.amount ELSE 0 END) AS paypal,
           SUM(CASE WHEN p.method = 'invoice' THEN p.amount ELSE 0 END) AS invoice,
           SUM(p.amount) AS total
         FROM payments p JOIN orders o ON o.id = p.order_id JOIN customers c ON c.id = o.customer_id
         GROUP BY c.segment ORDER BY c.segment"""),
]

PEOPLE = [
    ("salary_bands",
     """SELECT CASE WHEN salary < 60000 THEN 'under 60k' WHEN salary < 100000 THEN '60k-100k' WHEN salary < 150000 THEN '100k-150k'
                     ELSE '150k+' END AS band,
           COUNT(*) AS people, MIN(salary) AS low, MAX(salary) AS high, LIST(last_name, ', ' ORDER BY salary, id) AS who
         FROM employees
         GROUP BY CASE WHEN salary < 60000 THEN 'under 60k' WHEN salary < 100000 THEN '60k-100k' WHEN salary < 150000 THEN '100k-150k'
                       ELSE '150k+' END
         ORDER BY MIN(salary)"""),
    ("span_of_control",
     """SELECT m.id, m.first_name || ' ' || m.last_name AS manager, COUNT(e.id) AS direct_reports,
           COUNT(e2.id) AS second_level, CAST(AVG(e.salary) AS NUMERIC(10,2)) AS avg_report_salary
         FROM employees m JOIN employees e ON e.manager_id = m.id LEFT JOIN employees e2 ON e2.manager_id = e.id
         GROUP BY m.id, m.first_name, m.last_name ORDER BY direct_reports DESC, m.id"""),
    ("tenure_in_years_and_months",
     """SELECT id, first_name || ' ' || last_name AS name, hire_date,
           DATEDIFF(MONTH, hire_date, '2026-01-01') / 12 AS years, MOD(DATEDIFF(MONTH, hire_date, '2026-01-01'), 12) AS months,
           CAST(DATEDIFF(MONTH, hire_date, '2026-01-01') / 12 AS VARCHAR(3)) || 'y ' || CAST(MOD(DATEDIFF(MONTH, hire_date, '2026-01-01'), 12) AS VARCHAR(2)) || 'm' AS tenure
         FROM employees ORDER BY hire_date, id"""),
]

STOCK = [
    ("abc_xyz_matrix",
     f"""WITH monthly AS (SELECT oi.product_id, MONTH(o.order_ts) AS mo, SUM(oi.quantity) AS units
                         FROM order_items oi JOIN orders o ON o.id = oi.order_id
                         WHERE YEAR(o.order_ts) = 2025 AND o.status <> 'cancelled' GROUP BY oi.product_id, MONTH(o.order_ts)),
         stats AS (SELECT product_id, SUM(units) AS units, COUNT(*) AS months_sold, MAX(units) - MIN(units) AS swing
                   FROM monthly GROUP BY product_id)
         SELECT s.product_id, p.name, s.units, s.months_sold,
           CASE WHEN s.units >= 30 THEN 'A' WHEN s.units >= 10 THEN 'B' ELSE 'C' END AS volume_class,
           CASE WHEN s.months_sold >= 6 AND s.swing <= 5 THEN 'X' WHEN s.months_sold >= 3 THEN 'Y' ELSE 'Z' END AS steadiness
         FROM stats s JOIN products p ON p.id = s.product_id ORDER BY s.units DESC, s.product_id"""),
    ("reorder_suggestions",
     """SELECT p.id, p.name, i.on_hand, i.reorder_point, GREATER(i.reorder_point * 2 - i.on_hand, 0) AS suggested_order,
           CASE WHEN i.on_hand = 0 THEN 'urgent' WHEN i.on_hand < i.reorder_point THEN 'soon' ELSE 'none' END AS urgency
         FROM inventory i JOIN products p ON p.id = i.product_id
         WHERE i.snapshot_date = '2025-12-01' AND p.discontinued = 0 ORDER BY suggested_order DESC, p.id"""),
    ("months_out_of_stock_per_category",
     """SELECT p.category, SUM(CASE WHEN i.on_hand = 0 THEN 1 ELSE 0 END) AS product_months_out,
           COUNT(DISTINCT CASE WHEN i.on_hand = 0 THEN i.product_id END) AS products_affected, COUNT(*) AS snapshots
         FROM inventory i JOIN products p ON p.id = i.product_id GROUP BY p.category ORDER BY p.category"""),
]

CASES = [("analytics", ANALYTICS), ("collections", FINANCE), ("staff", PEOPLE), ("stock", STOCK)]
