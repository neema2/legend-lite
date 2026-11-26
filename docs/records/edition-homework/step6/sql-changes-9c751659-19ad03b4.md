# The product's SQL: 9c751659 -> 19ad03b4

## Totals

| set | database | queries | changed |
|---|---|---|---|
| channel_a | DuckDB | 2451 | 10 |
| channel_a | H2 | 2451 | 10 |
| channel_a | SQLite | 2451 | 10 |
| core | own | 3527 | 6 |
- channel_a (DuckDB / own runtime): characters of the changed queries' SQL, before 708, after 876
- core (DuckDB / own runtime): characters of the changed queries' SQL, before 709, after 1375

## (i) SQL text differs: 13


### acos/asin (not equality): 13

- `channel_a#1568` (DuckDB) `|2.0->meta::pure::functions::math::acos()`
  - before (92 chars): `SELECT (CASE WHEN (2.0E0) BETWEEN -1 AND 1 THEN acos(2.0E0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (105 chars): `SELECT CASE WHEN abs(2.0E0) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(2.0E0) END AS value`
- `channel_a#1569` (DuckDB) `|0->meta::pure::functions::math::acos()`
  - before (84 chars): `SELECT (CASE WHEN (0) BETWEEN -1 AND 1 THEN acos(0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (113 chars): `SELECT CASE WHEN abs(CAST(0 AS DOUBLE)) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(0) END AS value`
- `channel_a#1570` (DuckDB) `|meta::pure::functions::math::acos(-1)`
  - before (86 chars): `SELECT (CASE WHEN (-1) BETWEEN -1 AND 1 THEN acos(-1) ELSE 'NaN'::DOUBLE END) AS value`
  - after (115 chars): `SELECT CASE WHEN abs(CAST(-1 AS DOUBLE)) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(-1) END AS value`
- `channel_a#1571` (DuckDB) `|0.5->meta::pure::functions::math::acos()`
  - before (92 chars): `SELECT (CASE WHEN (0.5E0) BETWEEN -1 AND 1 THEN acos(0.5E0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (105 chars): `SELECT CASE WHEN abs(0.5E0) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(0.5E0) END AS value`
- `channel_a#1572` (DuckDB) `|2.0->meta::pure::functions::math::asin()`
  - before (92 chars): `SELECT (CASE WHEN (2.0E0) BETWEEN -1 AND 1 THEN asin(2.0E0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (105 chars): `SELECT CASE WHEN abs(2.0E0) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE asin(2.0E0) END AS value`
- `channel_a#1573` (DuckDB) `|0->meta::pure::functions::math::asin()`
  - before (84 chars): `SELECT (CASE WHEN (0) BETWEEN -1 AND 1 THEN asin(0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (113 chars): `SELECT CASE WHEN abs(CAST(0 AS DOUBLE)) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE asin(0) END AS value`
- `channel_a#1574` (DuckDB) `|1->meta::pure::functions::math::asin()`
  - before (84 chars): `SELECT (CASE WHEN (1) BETWEEN -1 AND 1 THEN asin(1) ELSE 'NaN'::DOUBLE END) AS value`
  - after (113 chars): `SELECT CASE WHEN abs(CAST(1 AS DOUBLE)) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE asin(1) END AS value`
- `channel_a#1575` (DuckDB) `|meta::pure::functions::math::asin(-0.5)`
  - before (94 chars): `SELECT (CASE WHEN (-0.5E0) BETWEEN -1 AND 1 THEN asin(-0.5E0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (107 chars): `SELECT CASE WHEN abs(-0.5E0) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE asin(-0.5E0) END AS value`
- `core#2157` (own) `|#TDS
val
1
#->extend(~a: x | $x.val->acos())`
  - before (146 chars): `SELECT _tds0.val AS val, (CASE WHEN (_tds0.val) BETWEEN -1 AND 1 THEN acos(_tds0.val) ELSE 'NaN'::DOUBLE END) AS a
FROM (VALUES (1)) AS _tds0(val)`
  - after (175 chars): `SELECT _tds0.val AS val, CASE WHEN abs(CAST(_tds0.val AS DOUBLE)) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(_tds0.val) END AS a
FROM (VALUES (1)) AS _tds0(val)`
- `core#2158` (own) `|#TDS
val
0
#->extend(~a: x | $x.val->asin())`
  - before (146 chars): `SELECT _tds0.val AS val, (CASE WHEN (_tds0.val) BETWEEN -1 AND 1 THEN asin(_tds0.val) ELSE 'NaN'::DOUBLE END) AS a
FROM (VALUES (0)) AS _tds0(val)`
  - after (175 chars): `SELECT _tds0.val AS val, CASE WHEN abs(CAST(_tds0.val AS DOUBLE)) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE asin(_tds0.val) END AS a
FROM (VALUES (0)) AS _tds0(val)`
- `core#2355` (own) `|meta::pure::functions::math::acos_Number_1__Float_1_->eval(0.5)`
  - before (92 chars): `SELECT (CASE WHEN (0.5E0) BETWEEN -1 AND 1 THEN acos(0.5E0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (105 chars): `SELECT CASE WHEN abs(0.5E0) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(0.5E0) END AS value`
- `core#3226` (own) `|2.0->acos()`
  - before (233 chars): `SELECT (CASE WHEN (CAST(2.0 AS DOUBLE PRECISION)) BETWEEN -1 AND 1 THEN acos(LEAST(GREATEST(CAST(2.0 AS DOUBLE PRECISION), -1), 1)) WHEN (CAST(2.0 AS DOUBLE PRECISION)) IS NOT NULL THEN CAST('NaN' AS DOUBLE PRECISION) END) AS "value"`
  - after (191 chars): `SELECT CASE WHEN abs(CAST(2.0 AS DOUBLE PRECISION)) > 1 THEN SIGNAL('45000', CHAR(31) || ('LG#1') || CHAR(31)) ELSE acos(LEAST(GREATEST(CAST(2.0 AS DOUBLE PRECISION), -1), 1)) END AS "value"`
- `core#3254` (own) `|2.0->acos()`
  - before (92 chars): `SELECT (CASE WHEN (2.0E0) BETWEEN -1 AND 1 THEN acos(2.0E0) ELSE 'NaN'::DOUBLE END) AS value`
  - after (105 chars): `SELECT CASE WHEN abs(2.0E0) > 1 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE acos(2.0E0) END AS value`

## (ii) refused before, planned now: 3


### strict typing: > Float with Integer: 3

- `channel_a#6` (DuckDB) `|false && ((12 / 0) > 0)`
  - before: ModelException: > compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)
  - after (0 chars): ``
- `channel_a#31` (DuckDB) `|true || ((12 / 0) > 0)`
  - before: ModelException: > compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)
  - after (0 chars): ``
- `core#1910` (own) `|#TDS
    id, grp, name
    1, 2, A
    2, 1, B
    3, 3, C
    4, 4, D
    5, 2, E
    6, 1, F
    7, 3, G
    8, 1, H
    9, 5, I
    10, 0, J
#->extend(over(~grp), ~partSum:{p,w,r|$r.id}:y|$y->plus())->extend(~pct: x | $x.id * 100 / $x.partSum)->filter(x | $x.pct > 40)`
  - before: ModelException: > compares Float with Integer: two values are equal only when they have the same type and the same value, so this comparison is a mistake -- write both sides in one type (1.0 for 1, toFloat(), toString(), an enumeration's own value)
  - after (624 chars): `SELECT t0.id AS id, t0.grp AS grp, t0.name AS name, t0.partSum AS partSum, CASE WHEN t0.partSum = 0 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE (CAST(t0.id * 100 AS DOUBLE) / CAST(t0.partSum AS DOUBLE)) END AS pct
FROM (
  SELECT _tds0.*, SUM(_tds0.id) OVER (PARTITION BY _tds0.grp) AS partSum
  FROM (VALUES (1, 2, 'A'), (2, 1, 'B'), (3, 3, 'C'), (4, 4, 'D'), (5, 2, 'E'), (6, 1, 'F'), (7, 3, 'G'), (8, 1, 'H'), (9, 5, 'I'), (10, 0, 'J')) AS _tds0(id, grp, name)
) AS t0
WHERE CASE WHEN t0.partSum = 0 THEN error(chr(31) || ('LG#1') || chr(31)) ELSE (CAST(t0.id * 100 AS DOUBLE) / CAST(t0.partSum AS DOUBLE)) END > 40`

## (iii) planned before, refused now: 0


## channel_a queries whose change differs in kind across DuckDB, H2 and SQLite: 0

