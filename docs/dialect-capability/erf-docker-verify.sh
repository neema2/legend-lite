#!/usr/bin/env bash
# Reproducible live Docker verification for meta::legend::lite::erf (S308).
# Runs against cached/official images for Postgres 16, ClickHouse, Trino, MySQL 8.4, and MariaDB 11.
set -euo pipefail

cleanup() {
  docker rm -f erf_pg erf_ch erf_trino erf_mysql erf_mariadb >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

echo "=== 1. Postgres 16 (native erf) ==="
docker run -d --rm --name erf_pg -e POSTGRES_PASSWORD=pw postgres:16 >/dev/null
for _ in $(seq 1 30); do
  docker exec erf_pg pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec erf_pg psql -U postgres -c \
  "SELECT erf(0.5) AS e_05, erf(-0.5) AS e_neg05, erf(0.0) AS e_0, erf(4.5) AS e_45, erf(27.0) AS e_27, erf(NULL::double precision) AS e_null;"

echo "=== 2. ClickHouse 24.8 (native erf) ==="
docker run -d --rm --name erf_ch clickhouse/clickhouse-server:24.8 >/dev/null
for _ in $(seq 1 30); do
  docker exec erf_ch clickhouse-client -q "SELECT 1" >/dev/null 2>&1 && break
  sleep 1
done
docker exec erf_ch clickhouse-client --format PrettyCompact -q \
  "SELECT erf(0.5) AS e_05, erf(-0.5) AS e_neg05, erf(0.0) AS e_0, erf(4.5) AS e_45, erf(27.0) AS e_27, erf(CAST(NULL AS Nullable(Float64))) AS e_null"

echo "=== 3. Trino 468 (2 * normal_cdf(0, 0.7071067811865475, CAST(x AS DOUBLE)) - 1) ==="
docker run -d --rm --name erf_trino trinodb/trino:468 >/dev/null
for _ in $(seq 1 40); do
  docker exec erf_trino trino --execute "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
docker exec erf_trino trino --output-format TSV_HEADER --execute "
SELECT x,
       2 * normal_cdf(0, 0.7071067811865475, CAST(x AS DOUBLE)) - 1 AS erf_trino
FROM (VALUES
  (0.0), (0.3), (0.46875), (-0.46875), (0.5), (-0.5),
  (4.0), (-4.0), (4.5), (5.0), (-5.0), (26.9), (27.0), (CAST(NULL AS DOUBLE))
) AS t(x)"

echo "=== 4. MySQL 8.4 (confirm no native erf) ==="
docker run -d --rm --name erf_mysql -e MYSQL_ROOT_PASSWORD=pw mysql:8.4 >/dev/null
for _ in $(seq 1 40); do
  docker exec erf_mysql mysql -uroot -ppw -D mysql -e "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
docker exec erf_mysql mysql -uroot -ppw -D mysql -e "SELECT erf(0.5);" 2>&1 || true

echo "=== 5. MariaDB 11 (confirm no native erf) ==="
docker run -d --rm --name erf_mariadb -e MARIADB_ROOT_PASSWORD=pw mariadb:11 >/dev/null
for _ in $(seq 1 40); do
  docker exec erf_mariadb mariadb -uroot -ppw -e "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
docker exec erf_mariadb mariadb -uroot -ppw -e "SELECT erf(0.5);" 2>&1 || true
