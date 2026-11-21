import duckdb
cases = {
# assert 0 fails (1 = 2); assert 1 is toOne over an EMPTY relation, its raise inside the relation
"naive: wrap assert 1 in CASE WHEN v0": """
WITH v0 AS (SELECT (1 = 2) AS ok),
m1 AS (SELECT CASE WHEN COUNT(*) <> 1 THEN error('toOne: empty') ELSE MAX(x) END AS val FROM E)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, CASE WHEN (SELECT ok FROM v0) THEN ((SELECT val FROM m1) = 5) END
ORDER BY ix""",
"pushed: v0 in the raise site's guard": """
WITH v0 AS (SELECT (1 = 2) AS ok),
m1 AS (SELECT CASE WHEN (SELECT ok FROM v0) AND COUNT(*) <> 1 THEN error('toOne: empty') ELSE MAX(x) END AS val FROM E)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, (SELECT val FROM m1) = 5
ORDER BY ix""",
"pushed, MATERIALIZED frame": """
WITH v0 AS MATERIALIZED (SELECT (1 = 2) AS ok),
m1 AS MATERIALIZED (SELECT CASE WHEN (SELECT ok FROM v0) AND COUNT(*) <> 1 THEN error('toOne: empty') ELSE MAX(x) END AS val FROM E)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, (SELECT val FROM m1) = 5
ORDER BY ix""",
"pushed, per-row raise (at(0) of each row's list)": """
WITH v0 AS (SELECT (1 = 2) AS ok),
m1 AS (SELECT CASE WHEN (SELECT ok FROM v0) AND len(l) = 0 THEN error('at: empty') ELSE l[1] END AS val FROM L)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, (SELECT bool_and(val = 1) FROM m1)
ORDER BY ix""",
"pushed, constant raise": """
WITH v0 AS (SELECT (1 = 2) AS ok)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, CASE WHEN (SELECT ok FROM v0) THEN error('boom') ELSE TRUE END
ORDER BY ix""",
"control: assert 0 passes, assert 1 raises": """
WITH v0 AS (SELECT (1 = 1) AS ok),
m1 AS (SELECT CASE WHEN (SELECT ok FROM v0) AND COUNT(*) <> 1 THEN error('toOne: empty') ELSE MAX(x) END AS val FROM E)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, (SELECT val FROM m1) = 5
ORDER BY ix""",
"unguarded database error (cast of data)": """
WITH v0 AS (SELECT (1 = 2) AS ok)
SELECT 0 AS ix, (SELECT ok FROM v0) AS verdict
UNION ALL SELECT 1, (SELECT bool_and(CAST(s AS INTEGER) = 1) FROM S)
ORDER BY ix""",
}
c = duckdb.connect()
c.execute("CREATE TABLE E (x INTEGER)")
c.execute("CREATE TABLE L (l INTEGER[])"); c.execute("INSERT INTO L VALUES ([1]), ([])")
c.execute("CREATE TABLE S (s VARCHAR)"); c.execute("INSERT INTO S VALUES ('1'), ('abc')")
for name, sql in cases.items():
    try:
        print(f"{name:50} -> {c.execute(sql).fetchall()}")
    except Exception as e:
        print(f"{name:50} -> RAISED {type(e).__name__}: {str(e).splitlines()[0][:90]}")
