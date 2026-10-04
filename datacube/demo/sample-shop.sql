CREATE DATABASE shop;
\c shop
CREATE SCHEMA sales;
CREATE TABLE sales.orders (
  id integer PRIMARY KEY,
  ordered_at timestamptz NOT NULL,
  channel text NOT NULL,
  region text NOT NULL,
  product text NOT NULL,
  quantity integer NOT NULL,
  unit_price numeric(10,2) NOT NULL
);
INSERT INTO sales.orders
SELECT g,
       timestamptz '2026-01-01 00:00:00+00' + g * interval '37 minutes',
       (ARRAY['web','store','phone'])[1 + g % 3],
       (ARRAY['north','south','east','west'])[1 + g % 4],
       (ARRAY['widget','gadget','gizmo','doohickey','sprocket'])[1 + g % 5],
       1 + g % 7,
       round((5 + (g % 50) * 1.37)::numeric, 2)
FROM generate_series(1, 5000) AS g;
CREATE ROLE reader LOGIN PASSWORD 'secret';
GRANT USAGE ON SCHEMA sales TO reader;
GRANT SELECT ON ALL TABLES IN SCHEMA sales TO reader;
