import duckdb, time, threading, subprocess, sys, os
path = 'w0c.duckdb'
db = duckdb.connect(path)
db.execute("CREATE TABLE t AS SELECT i, i % 1000 AS k, random() AS v FROM range(20000000) r(i)")
Q = "SELECT k, sum(v), count(*) FROM t GROUP BY k"
def timed(conn):
    s = time.perf_counter(); conn.execute(Q).fetchall(); return time.perf_counter() - s
threads = db.execute("SELECT current_setting('threads')").fetchone()[0]
print(f"cores {os.cpu_count()}, duckdb threads {threads}")
base = timed(db.cursor())
print(f"1 query alone: {base*1000:.0f} ms")
for n in (2, 4, 8):
    conns = [db.cursor() for _ in range(n)]
    out = [0]*n
    def run(i): out[i] = timed(conns[i])
    ts = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    s = time.perf_counter(); [t.start() for t in ts]; [t.join() for t in ts]; wall = time.perf_counter() - s
    print(f"{n} concurrent (own connection each): wall {wall*1000:.0f} ms, per query avg {sum(out)/n*1000:.0f} ms, throughput x{n*base/wall:.2f} vs one-at-a-time")
w = db.cursor(); r = db.cursor()
w.execute("BEGIN"); w.execute("UPDATE t SET v = 0 WHERE k = 1")
before = r.execute("SELECT sum(v) FROM t WHERE k = 1").fetchone()[0]
w.execute("COMMIT")
after = r.execute("SELECT sum(v) FROM t WHERE k = 1").fetchone()[0]
print(f"reader during uncommitted write sees old data: {before != 0}; after commit sees new: {after == 0}")
w2 = db.cursor(); w3 = db.cursor()
w2.execute("BEGIN"); w2.execute("UPDATE t SET v = 1 WHERE k = 2")
w3.execute("BEGIN")
try:
    w3.execute("UPDATE t SET v = 2 WHERE k = 2"); w3.execute("COMMIT"); print("two writers, same rows: both committed (!)")
except Exception as e:
    print("two writers, same rows: second refused ->", str(e).split('\n')[0][:90])
    w3.execute("ROLLBACK")
w2.execute("COMMIT")
w4 = db.cursor(); w5 = db.cursor()
w4.execute("BEGIN"); w4.execute("UPDATE t SET v = 1 WHERE k = 3")
w5.execute("BEGIN"); w5.execute("UPDATE t SET v = 1 WHERE k = 4")
w4.execute("COMMIT"); w5.execute("COMMIT"); print("two writers, different rows: both committed")
[c.close() for c in (w, r, w2, w3, w4, w5)]
code = "import duckdb,sys\ntry:\n  duckdb.connect(sys.argv[1], read_only=(sys.argv[2]=='ro')).execute('select count(*) from t').fetchone(); print('opened')\nexcept Exception as e: print('refused:', str(e).split(chr(10))[0][:100])"
for mode in ('rw', 'ro'):
    print(f"second process, {mode}, while first holds it:", subprocess.run([sys.executable, '-c', code, path, mode], capture_output=True, text=True).stdout.strip())
db.close()
print("second process, ro x2 after first closed:", [subprocess.run([sys.executable, '-c', code, path, 'ro'], capture_output=True, text=True).stdout.strip() for _ in range(2)])
