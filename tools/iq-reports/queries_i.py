"""Realistic report queries over the retail schema, part I: international suppliers' UTF-8 text as reports read it --
sorted, folded, measured in characters and bytes, searched by LIKE and LOCATE, cut by LEFT and SUBSTRING, keyed for
duplicates by trimming, folding and transliterating, laid out in fixed width -- and their products: the cheapest
supplier of each, lead times and costs by country."""

FOLD = ("LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(name), 'ä', 'ae'), 'ö', 'oe'), 'ü', 'ue'),"
        " 'ß', 'ss'), 'é', 'e'), 'è', 'e'), 'ç', 'c'))")

TEXT = [
    ("suppliers_sorted_by_name",
     """SELECT id, name, country FROM suppliers ORDER BY name, id"""),
    ("names_in_characters_and_bytes",
     """SELECT id, name, LENGTH(name) AS characters, BYTE_LENGTH(name) AS bytes, BYTE_LENGTH(name) - LENGTH(name) AS extra_bytes,
           LENGTH(TRIM(name)) AS trimmed
         FROM suppliers ORDER BY id"""),
    ("folded_names",
     """SELECT id, name, UPPER(name) AS upper_name, LOWER(name) AS lower_name, UPPER(LEFT(TRIM(name), 1)) AS first_letter
         FROM suppliers ORDER BY id"""),
    ("accented_text_searches",
     """SELECT id, name, city,
           CASE WHEN name LIKE '%ü%' OR city LIKE '%ü%' THEN 'umlaut' ELSE '' END AS has_umlaut,
           LOCATE(name, 'ö') AS o_umlaut_at, LOCATE(city, 'ó') AS o_acute_at,
           CASE WHEN name LIKE 'Caf_ Cr_me' THEN 'cafe creme' ELSE '' END AS cafe_spelling
         FROM suppliers ORDER BY id"""),
    ("cut_by_characters",
     """SELECT id, LEFT(TRIM(name), 3) AS first_three, RIGHT(TRIM(name), 4) AS last_four,
           SUBSTRING(city, 2, 3) AS city_middle, LEFT(contact, LOCATE(contact, ' ') - 1) AS contact_first_name
         FROM suppliers WHERE contact IS NOT NULL ORDER BY id"""),
    ("duplicate_suppliers_by_folded_name",
     f"""SELECT {FOLD} AS name_key, COUNT(*) AS suppliers, LIST(CAST(id AS VARCHAR(3)), ',' ORDER BY id) AS ids, MIN(since) AS first_since
         FROM suppliers GROUP BY {FOLD} HAVING COUNT(*) > 1 ORDER BY name_key"""),
    ("fixed_width_supplier_list",
     """SELECT id, LEFT(TRIM(name) || REPEAT('.', 30), 30) || ' ' || RIGHT(REPEAT(' ', 12) || COALESCE(country, '?'), 12)
                 || ' ' || COALESCE(CAST(rating AS VARCHAR(4)), ' n/a') AS line
         FROM suppliers ORDER BY id"""),
    ("contact_initials_and_email_domains",
     """SELECT id, contact,
           UPPER(LEFT(contact, 1)) || UPPER(SUBSTRING(contact, LOCATE(contact, ' ') + 1, 1)) AS initials,
           LOWER(SUBSTRING(email, LOCATE(email, '@') + 1)) AS domain,
           RIGHT(LOWER(email), LOCATE(REVERSE(email), '.') - 1) AS tld
         FROM suppliers WHERE contact IS NOT NULL AND email IS NOT NULL ORDER BY id"""),
    ("phone_digits_and_country_codes",
     """SELECT id, phone, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(phone, '+', ''), ' ', ''), '(', ''), ')', ''), '-', ''), '.', '') AS digits,
           CASE WHEN phone LIKE '+%' THEN SUBSTRING(phone, 2, LOCATE(phone, ' ') - 2) ELSE '1' END AS country_code
         FROM suppliers WHERE phone IS NOT NULL ORDER BY id"""),
    ("suppliers_per_country_listed",
     """SELECT country, COUNT(*) AS suppliers, LIST(TRIM(name), '; ' ORDER BY id) AS names, ROUND(AVG(rating), 2) AS avg_rating,
           MIN(since) AS longest_since
         FROM suppliers GROUP BY country ORDER BY suppliers DESC, country"""),
]

SOURCING = [
    ("cheapest_supplier_per_product",
     """SELECT product_id, product, supplier, unit_cost, lead_days FROM (
           SELECT p.id AS product_id, p.name AS product, TRIM(s.name) AS supplier, sp.unit_cost, sp.lead_days,
                  ROW_NUMBER() OVER (PARTITION BY p.id ORDER BY sp.unit_cost, sp.lead_days, sp.id) AS rn
           FROM supplier_products sp JOIN products p ON p.id = sp.product_id JOIN suppliers s ON s.id = sp.supplier_id) x
         WHERE rn = 1 ORDER BY product_id"""),
    ("lead_times_by_country",
     """SELECT s.country, COUNT(*) AS offers, MIN(sp.lead_days) AS fastest, MAX(sp.lead_days) AS slowest,
           ROUND(AVG(sp.lead_days * 1.0), 1) AS avg_days, SUM(CASE WHEN sp.lead_days <= 7 THEN 1 ELSE 0 END) AS within_a_week
         FROM supplier_products sp JOIN suppliers s ON s.id = sp.supplier_id GROUP BY s.country ORDER BY avg_days, s.country"""),
    ("sourcing_savings",
     """SELECT p.id, p.name, p.unit_cost AS book_cost, MIN(sp.unit_cost) AS best_cost, p.unit_cost - MIN(sp.unit_cost) AS saving,
           COUNT(*) AS offers, CASE WHEN COUNT(*) = 1 THEN 'single source' ELSE '' END AS risk
         FROM products p JOIN supplier_products sp ON sp.product_id = p.id GROUP BY p.id, p.name, p.unit_cost ORDER BY saving DESC, p.id"""),
    ("supplier_scorecard",
     """SELECT s.id, TRIM(s.name) AS supplier, s.rating, COUNT(sp.id) AS products, MIN(sp.lead_days) AS fastest,
           DATEDIFF(YEAR, s.since, '2026-01-01') AS years,
           DENSE_RANK() OVER (PARTITION BY s.country ORDER BY s.rating DESC) AS rank_in_country,
           CASE WHEN s.rating >= 4.5 THEN 'preferred' WHEN s.rating >= 3.5 THEN 'approved' WHEN s.rating IS NULL THEN 'unrated' ELSE 'watch' END AS status
         FROM suppliers s LEFT JOIN supplier_products sp ON sp.supplier_id = s.id
         GROUP BY s.id, s.name, s.rating, s.since, s.country ORDER BY s.id"""),
    ("minimum_order_values",
     """SELECT sp.id, TRIM(s.name) AS supplier, p.name AS product, sp.min_order, sp.unit_cost,
           COALESCE(sp.min_order, 1) * sp.unit_cost AS min_order_value,
           CASE WHEN sp.min_order IS NULL THEN 'any quantity' WHEN sp.min_order >= 100 THEN 'bulk' ELSE 'standard' END AS terms
         FROM supplier_products sp JOIN suppliers s ON s.id = sp.supplier_id JOIN products p ON p.id = sp.product_id
         WHERE sp.id <= 25 ORDER BY sp.id"""),
]

CASES = [("supplier_text", TEXT), ("sourcing", SOURCING)]
