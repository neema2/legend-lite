# Live Docker Verification for `meta::legend::lite::erf` (S308)

Reproduced by running [`erf-docker-verify.sh`](./erf-docker-verify.sh) against pinned official Docker images (`postgres:16` — note Postgres <= 15 has no built-in `erf`, matching CI's Postgres 16 floor; `clickhouse/clickhouse-server:24.8`; `trinodb/trino:468`; `mysql:8.4`; `mariadb:11`):

```text
=== 1. Postgres 16 (native erf) ===
        e_05        |       e_neg05       | e_0 |        e_45        | e_27 | e_null 
--------------------+---------------------+-----+--------------------+------+--------
 0.5204998778130465 | -0.5204998778130465 |   0 | 0.9999999998033839 |    1 |       
(1 row)

=== 2. ClickHouse (native erf) ===
   ┌───────────────e_05─┬─────────────e_neg05─┬─e_0─┬───────────────e_45─┬─e_27─┬─e_null─┐
1. │ 0.5204998778130465 │ -0.5204998778130465 │   0 │ 0.9999999998033839 │    1 │   ᴺᵁᴸᴸ │
   └────────────────────┴─────────────────────┴─────┴────────────────────┴──────┴────────┘

=== 3. Trino (2 * normal_cdf(0, 0.7071067811865475, CAST(x AS DOUBLE)) - 1) ===
x	erf_trino
0.0	0.0
0.3	0.32862675945912745
0.46875	0.49261347321793814
-0.46875	-0.492613473217938
0.5	0.5204998778130467
-0.5	-0.5204998778130466
4.0	0.9999999845827421
-4.0	-0.9999999845827421
4.5	0.9999999998033839
5.0	0.9999999999984626
-5.0	-0.9999999999984626
26.9	1.0
27.0	1.0
	
=== 4. MySQL 8.4 (confirm no native erf) ===
ERROR 1305 (42000) at line 1: FUNCTION mysql.erf does not exist

=== 5. MariaDB 11 (confirm no native erf) ===
ERROR 1305 (42000) at line 1: FUNCTION erf does not exist
```
