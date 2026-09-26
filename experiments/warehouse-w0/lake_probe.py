import duckdb, subprocess, sys, time
def attach(con, catalog):
    con.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite")
    con.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH 'lake/data/')")
catalog = sys.argv[1] if len(sys.argv) > 1 else 'sqlite:lake/catalog.sqlite'
role = sys.argv[2] if len(sys.argv) > 2 else 'setup'
con = duckdb.connect()
attach(con, catalog)
if role == 'setup':
    con.execute("CREATE TABLE lake.trades AS SELECT i AS id, 'EMEA' AS region, i * 1.0 AS notional FROM range(1000) r(i)")
    print('setup ok, rows', con.execute("SELECT count(*) FROM lake.trades").fetchone()[0])
elif role == 'writer':
    for b in range(5):
        con.execute(f"INSERT INTO lake.trades SELECT 1000000 + {b}*100 + i, 'AMER', 1.0 FROM range(100) r(i)")
        time.sleep(0.2)
    print('writer done, rows', con.execute("SELECT count(*) FROM lake.trades").fetchone()[0])
elif role == 'reader':
    seen = []
    for _ in range(8):
        seen.append(con.execute("SELECT count(*) FROM lake.trades").fetchone()[0]); time.sleep(0.15)
    print('reader saw', seen)
