# The databases' own plain text of each Pure type's value (2026-11-10, the D0 extension). Run with a Python that has
# duckdb; sqlite3 is the stdlib's; Postgres through psql on a throwaway local server (args: psql connection words).
import sys, subprocess, sqlite3, duckdb
EXPRS = [
  ("Float 1.0", "CAST(1.0 AS DOUBLE)", "CAST(1.0 AS REAL)", "CAST(1.0 AS DOUBLE PRECISION)"),
  ("Float 1.5", "CAST(1.5 AS DOUBLE)", "CAST(1.5 AS REAL)", "CAST(1.5 AS DOUBLE PRECISION)"),
  ("Float 0.1+0.2", "CAST(0.1 AS DOUBLE)+CAST(0.2 AS DOUBLE)", "CAST(0.1 AS REAL)+CAST(0.2 AS REAL)", "CAST(0.1 AS DOUBLE PRECISION)+CAST(0.2 AS DOUBLE PRECISION)"),
  ("Float 1e-5", "CAST(1e-5 AS DOUBLE)", "CAST(1e-5 AS REAL)", "CAST(1e-5 AS DOUBLE PRECISION)"),
  ("Float 1e20", "CAST(1e20 AS DOUBLE)", "CAST(1e20 AS REAL)", "CAST(1e20 AS DOUBLE PRECISION)"),
  ("Float 123456789.0", "CAST(123456789.0 AS DOUBLE)", "CAST(123456789.0 AS REAL)", "CAST(123456789.0 AS DOUBLE PRECISION)"),
  ("Float -0.0", "CAST('-0.0' AS DOUBLE)", "-0.0", "CAST('-0.0' AS DOUBLE PRECISION)"),
  ("Float NaN", "CAST('NaN' AS DOUBLE)", "NULL", "CAST('NaN' AS DOUBLE PRECISION)"),
  ("Float Infinity", "CAST('Infinity' AS DOUBLE)", "9e999", "CAST('Infinity' AS DOUBLE PRECISION)"),
  ("Decimal 1.50 (10,2)", "CAST(1.5 AS DECIMAL(10,2))", "CAST(1.5 AS NUMERIC)", "CAST(1.5 AS DECIMAL(10,2))"),
  ("Integer 42", "CAST(42 AS BIGINT)", "42", "CAST(42 AS BIGINT)"),
  ("Boolean true", "true", "true", "true"),
  ("StrictDate", "DATE '2024-01-02'", "date('2024-01-02')", "DATE '2024-01-02'"),
  ("DateTime whole", "TIMESTAMP '2024-01-02 10:30:00'", "datetime('2024-01-02 10:30:00')", "TIMESTAMP '2024-01-02 10:30:00'"),
  ("DateTime .123", "TIMESTAMP '2024-01-02 10:30:00.123'", "strftime('%Y-%m-%d %H:%M:%f','2024-01-02 10:30:00.123')", "TIMESTAMP '2024-01-02 10:30:00.123'"),
  ("DateTime ISO strftime", "strftime(TIMESTAMP '2024-01-02 10:30:00.123', '%Y-%m-%dT%H:%M:%S.%g')", "strftime('%Y-%m-%dT%H:%M:%f','2024-01-02 10:30:00.123')", "to_char(TIMESTAMP '2024-01-02 10:30:00.123', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS')"),
]
d = duckdb.connect(); s = sqlite3.connect(":memory:")
print("| value | DuckDB 1.4.4 | SQLite " + sqlite3.sqlite_version + " | Postgres 16 |\n|---|---|---|---|")
for name, de, se, pe in EXPRS:
    dv = d.execute(f"SELECT CAST({de} AS VARCHAR)").fetchone()[0]
    sv = s.execute(f"SELECT CAST({se} AS TEXT)").fetchone()[0]
    pv = subprocess.run(["psql", *sys.argv[1:], "-Atc", f"SELECT CAST({pe} AS TEXT)"], capture_output=True, text=True).stdout.strip()
    print(f"| {name} | `{dv}` | `{sv}` | `{pv}` |")
