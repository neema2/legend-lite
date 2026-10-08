# Evidence: the execution plan boundary

The measurements `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` cites. Nothing here is built or read by a test.

## `plans/` — real plans from legend-engine 4.145.0 (2026-10-05)

`gen.py` posts `model.pure` and each query of `queries.tsv` to a legend-engine 4.145.0 server on
`127.0.0.1:6300` (the release's own server, started from its distribution) and writes each plan to `out/<name>.json`;
`out/_types.json` counts the node kinds per plan. `fm/texts.json` is the template census of §6: every FreeMarker
template text in legend-engine's 22 fixture plans plus the 13 plans in `out/`.

## `probes/` — what the databases do with a bound value (2026-10-07)

Each probe is one Java file run straight from source with one JDBC driver on the class path, at the versions the
product pins (`tools/deps/jars_table.bzl`): DuckDB JDBC 1.4.4.0, H2 2.1.214, Postgres JDBC 42.7.13. Postgres is the
pinned Postgres 16 (`@embedded_postgres`), started with `initdb -U postgres -A trust` and `pg_ctl ... -p 55432`.

```
java -cp duckdb_jdbc-1.4.4.0.jar BindProbe.java jdbc:duckdb:
java -cp h2-2.1.214.jar          BindProbe.java jdbc:h2:mem:p
java -cp postgresql-42.7.13.jar  BindProbe.java jdbc:postgresql://localhost:55432/postgres postgres ""
java -cp <driver>                EnumIndex.java <url> <user> <password>
```

- `BindProbe.java` → `bind-results.txt`: a value passed to the database as a value, not pasted into the SQL — a
  string with a quote, a list as one array (`ID = ANY(?)`), an empty list, a list of strings, a null. All three
  databases answer every case through `createArrayOf`; DuckDB refuses a bare Java array (`setObject(Integer[])`).
- `EnumIndex.java` → `enum-index-results.txt`: five ways to compare an enum parameter with a column that stores
  codes (400,000 rows, an index on the column; `ACTIVE` stored as `'A'` or `'X'`, `CLOSED` as `'C'`), read for
  whether the database uses the index. DuckDB reports its real scan only under `EXPLAIN ANALYZE` (plain `EXPLAIN`
  shows a sequential scan even for a key lookup), so the probe uses it there. H2's times are not comparable: H2
  reuses the result of a repeated identical query, so its index column is the measurement.

| Form | DuckDB | H2 | Postgres | a two-code value |
|---|---|---|---|---|
| F0 the runner translates (control; upstream's way) | index | index | index | right |
| F1 a value table: `STATUS IN (SELECT code FROM (VALUES …) m(code, name) WHERE name = ?)` | index | index | index | right |
| F2 `STATUS = (CASE ? WHEN … THEN 'A' … END)` | index | index | index | **wrong** (one code) |
| F3 `STATUS = ANY(CASE ? WHEN … THEN ARRAY[…] … END)` | index | **fails** | index | right |
| F4 the column decoded: `(CASE STATUS WHEN 'A' THEN 'ACTIVE' … END) = ?` | **scan** | **scan** | **scan** (~24 ms vs ~0.4 ms) | right |

F1 is the one form that uses the index on all three and is right for a value stored under two codes.

## `sharing/` — when two runs may share an in-memory database (2026-10-08)

`results.md`: the stress corpus with a shared session per test data against a fresh one per test, and how often a
run changes a shared database in the stress corpus and the relational corpus, counted by `count-probe.patch`
(applied, measured, reverted). The basis of decision A in the plan's §9.
