"""Realistic report queries over the retail schema, part J: the pitfalls analysts fall into, written as they write them --
NOT IN over a column holding NULLs, ANY and ALL beside NULLs, a filter of an outer join's NULL side in WHERE against
in ON, COUNT(*) against COUNT(column) after a LEFT JOIN, aggregates over no rows, integer division, MOD of negatives,
ROUND at a half, BETWEEN over timestamps at a day's end, blank padding in comparisons, a CASE with no ELSE, NULL
groups and NULL order, DISTINCT over NULLs, and string-to-number comparisons."""

PITFALLS = [
    ("not_in_over_a_nullable_column",
     """SELECT c.id, c.last_name FROM customers c
         WHERE c.region_id NOT IN (SELECT e.region_id FROM employees e WHERE e.title = 'Sales Associate') ORDER BY c.id"""),
    ("not_in_with_the_nulls_filtered",
     """SELECT c.id, c.last_name FROM customers c
         WHERE c.region_id NOT IN (SELECT e.region_id FROM employees e WHERE e.title = 'Sales Associate' AND e.region_id IS NOT NULL)
         ORDER BY c.id"""),
    ("not_exists_instead_of_not_in",
     """SELECT c.id, c.last_name FROM customers c
         WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.title = 'Sales Associate' AND e.region_id = c.region_id) ORDER BY c.id"""),
    ("greater_than_all_beside_a_null",
     """SELECT id, last_name, salary FROM employees
         WHERE salary > ALL (SELECT salary * 0.5 FROM employees WHERE manager_id IS NULL) ORDER BY id"""),
    ("outer_join_filtered_in_where",
     """SELECT c.id, c.last_name, COUNT(o.id) AS orders FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
         WHERE o.channel = 'web' GROUP BY c.id, c.last_name ORDER BY c.id"""),
    ("outer_join_filtered_in_on",
     """SELECT c.id, c.last_name, COUNT(o.id) AS orders FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.channel = 'web'
         GROUP BY c.id, c.last_name ORDER BY c.id"""),
    ("count_star_against_count_column",
     """SELECT c.id, COUNT(*) AS rows_counted, COUNT(o.id) AS orders, COUNT(o.ship_date) AS shipped, COUNT(DISTINCT o.channel) AS channels
         FROM customers c LEFT JOIN orders o ON o.customer_id = c.id GROUP BY c.id ORDER BY c.id"""),
    ("aggregates_over_no_rows",
     """SELECT COUNT(*) AS n, SUM(amount) AS total, AVG(amount) AS mean, MAX(paid_ts) AS latest, COALESCE(SUM(amount), 0) AS total_or_zero
         FROM payments WHERE amount < 0"""),
    ("scalar_subquery_over_no_rows",
     """SELECT c.id, (SELECT SUM(p.amount) FROM payments p JOIN orders o ON o.id = p.order_id WHERE o.customer_id = c.id) AS paid,
           COALESCE((SELECT SUM(p.amount) FROM payments p JOIN orders o ON o.id = p.order_id WHERE o.customer_id = c.id), 0) AS paid_or_zero,
           (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS orders
         FROM customers c WHERE c.id IN (1, 5, 22, 30) ORDER BY c.id"""),
    ("integer_division_and_remainders",
     """SELECT id, quantity, quantity / 4 AS packs, quantity % 4 AS left_over, MOD(quantity - 5, 4) AS signed_mod,
           quantity / 4.0 AS exact_packs, CAST(quantity AS NUMERIC(5,1)) / 4 AS cast_first
         FROM order_items WHERE id <= 20 ORDER BY id"""),
    ("rounding_at_a_half",
     """SELECT id, unit_price, ROUND(unit_price / 2, 1) AS half_price, ROUND(unit_price * 0.15, 0) AS tip_rounded,
           TRUNCNUM(unit_price * 0.15, 0) AS tip_truncated, CEILING(unit_price * 0.15) AS tip_up, FLOOR(-unit_price / 3) AS floor_neg
         FROM products ORDER BY id"""),
    ("between_dates_on_timestamps",
     """SELECT COUNT(*) AS between_dates, SUM(CASE WHEN CAST(order_ts AS DATE) BETWEEN '2025-03-01' AND '2025-03-31' THEN 1 ELSE 0 END) AS by_date,
           SUM(CASE WHEN order_ts >= '2025-03-01' AND order_ts < '2025-04-01' THEN 1 ELSE 0 END) AS half_open
         FROM orders WHERE order_ts BETWEEN '2025-03-01' AND '2025-03-31'"""),
    ("blank_padded_comparisons",
     """SELECT id, first_name, CASE WHEN first_name = TRIM(first_name) THEN 'equal' ELSE 'differs' END AS trimmed_equal,
           CASE WHEN first_name LIKE TRIM(first_name) THEN 'like' ELSE 'not like' END AS trimmed_like,
           LENGTH(first_name) AS len
         FROM customers WHERE id IN (3, 13, 23, 4) ORDER BY id"""),
    ("case_with_no_else",
     """SELECT segment, COUNT(*) AS customers, COUNT(CASE WHEN credit_limit > 50000 THEN 1 END) AS high_limit,
           SUM(CASE WHEN is_active = 1 THEN credit_limit END) AS active_limit, MAX(CASE WHEN birth_date IS NULL THEN 'missing' END) AS any_missing
         FROM customers GROUP BY segment ORDER BY segment"""),
    ("null_groups_and_null_order",
     """SELECT sales_rep_id, COUNT(*) AS orders, MIN(order_ts) AS first_order FROM orders GROUP BY sales_rep_id ORDER BY sales_rep_id"""),
    ("null_order_descending",
     """SELECT id, ship_date FROM orders WHERE id <= 30 ORDER BY ship_date DESC, id"""),
    ("distinct_over_nulls",
     """SELECT DISTINCT region_id, CASE WHEN email IS NULL THEN 'no email' ELSE 'email' END AS has_email FROM customers ORDER BY region_id, has_email"""),
    ("count_distinct_ignores_nulls",
     """SELECT COUNT(DISTINCT sales_rep_id) AS reps, COUNT(sales_rep_id) AS orders_with_rep, COUNT(*) AS orders,
           COUNT(DISTINCT COALESCE(sales_rep_id, 0)) AS reps_counting_none
         FROM orders"""),
    ("string_to_number_comparisons",
     """SELECT id, sku, RIGHT(sku, 3) AS tail, CAST(RIGHT(sku, 3) AS INTEGER) AS tail_number,
           CASE WHEN CAST(RIGHT(sku, 3) AS INTEGER) < 10 THEN 'single digit' ELSE '' END AS small
         FROM products ORDER BY id"""),
    ("nullif_to_avoid_division_by_zero",
     """SELECT o.id, o.shipping_fee, COUNT(oi.id) AS lines, o.shipping_fee / NULLIF(SUM(CASE WHEN oi.line_discount > 0 THEN 1 ELSE 0 END), 0) AS fee_per_discounted_line
         FROM orders o JOIN order_items oi ON oi.order_id = o.id WHERE o.id <= 25 GROUP BY o.id, o.shipping_fee ORDER BY o.id"""),
    ("coalesce_chains_and_isnull",
     """SELECT id, COALESCE(email, phone, 'no contact') AS contact, ISNULL(phone, '-') AS phone_or_dash,
           COALESCE(CAST(birth_date AS VARCHAR(10)), 'unknown') AS birthday
         FROM customers ORDER BY id"""),
    ("having_versus_where",
     """SELECT o.channel, COUNT(*) AS orders, SUM(o.shipping_fee) AS fees
         FROM orders o WHERE o.status <> 'cancelled' GROUP BY o.channel HAVING COUNT(*) > 20 AND MAX(o.shipping_fee) > 0 ORDER BY o.channel"""),
    ("timestamps_compared_to_dates",
     """SELECT id, order_ts, ship_date, CASE WHEN ship_date = CAST(order_ts AS DATE) THEN 'same day' WHEN ship_date > order_ts THEN 'later'
                                       ELSE 'earlier or same' END AS when_shipped
         FROM orders WHERE ship_date IS NOT NULL AND id <= 30 ORDER BY id"""),
]

CASES = [("pitfalls", PITFALLS)]
