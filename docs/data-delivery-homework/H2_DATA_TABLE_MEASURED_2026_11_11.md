# H2's `data:` table, measured (2026-11-11, before B2a)

The B brief's §4: H2 has no `JSON_TABLE` or JSON `UNNEST`, so a `data:` payload takes the model-data road on H2 (a
session table of JSON cells, filled by typed literals). Before building it: visibility across sessions, ON COMMIT
behaviour, whether a drop can reach a user's table, and whether the navigation matches the JSON source frame's VALUES
path. Measured on lite's H2 (2.5.252) and the referee's (legend-engine's 2.1.214); the probes are
`probes/H2Data.java.txt`, `probes/H2Drop.java.txt`, `probes/H2Schema.java.txt` (`java -cp <h2 jar> H2Data.java`).
Identical on both versions except where noted.

## What H2 does

1. **A `LOCAL TEMPORARY` table is its session's alone.** Another session's read of it is "table not found"; each
   session may hold its own of the same name. It lives in the schema it is created in (`PUBLIC` unqualified), its
   `TABLE_TYPE` `LOCAL TEMPORARY`.
2. **A plain `CREATE LOCAL TEMPORARY TABLE` commits the caller's open transaction**; with `TRANSACTIONAL` it does not.
   `ON COMMIT PRESERVE ROWS` is not H2 syntax; the default keeps rows across a commit. `ON COMMIT DROP TRANSACTIONAL`
   drops the table when the transaction ends; `ON COMMIT DELETE ROWS` empties it at each commit (an autocommit insert's
   rows are gone at once).
3. **A rollback does not undo the create**: the table stays, its rows rolled back.
4. **A drop by name reaches a user's table.** With no temporary table of that name in the session, `DROP TABLE IF
   EXISTS t`, `PUBLIC.t` and `SESSION.t` each drop a user's `PUBLIC.t`. A user may also create a persistent table of
   the name another session holds as temporary. **`CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS t` over a user's `t`
   silently uses the user's table** (no create; its type `BASE TABLE`), so a following `DELETE` or `INSERT` would write
   the user's rows. A plain create over a user's `t` is refused ("already exists").
5. **A drop of a local temporary table inside an open transaction commits it**, `TRANSACTIONAL` or not.
6. **`CREATE SCHEMA IF NOT EXISTS` commits the caller's open transaction**, even when the schema exists.
7. **A local temporary table in a schema of its own** (`CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS LEGEND_SESSION.t (data
   JSON) TRANSACTIONAL`): session-private as in 1; the create, a `DELETE` and the `INSERT`s commit nothing (a rollback
   leaves the caller's other rows uncommitted); `DROP TABLE IF EXISTS LEGEND_SESSION.t` drops the session's own and
   never a user's `PUBLIC.t` or another session's, and is a no-op where the session holds none. The schema itself is a
   persistent, empty object of the database once made.
8. **Navigation.** On 2.5.252 a JSON column filled with `JSON '...'` literals (or `? FORMAT JSON`) reads exactly as the
   frame's `VALUES (JSON '...', ordinal)` does through the dialect's spelling, `CAST(TRIM(BOTH '"' FROM NULLIF(CAST((t.data)
   ."K" AS VARCHAR), 'null')) AS T)`: an integer, a string with an escaped quote, a decimal, a boolean, a JSON null and an
   absent key alike (`[1|a|1.5|true, 2|null|null|null, 3|q\"x|null|null]` both ways). An `INTEGER` ordinal column beside
   the JSON one is fine. **On 2.1.214 (the referee) the field navigation `(j)."K"` does not exist** ("Invalid value JSON
   for parameter ROW"), over a table or over VALUES: the referee replays legend-engine's own text, which reads no lite
   `data:` table, so B2b's referee replays the plan's test-data loads and never a `data:` table.

## What it means for B2a

H2's `data:` table, so that it holds exactly the plan's rows, is the session's own, commits nothing a load runs inside
and can never reach a user's table:

- **made** (the load's steps): `CREATE SCHEMA IF NOT EXISTS LEGEND_SESSION` (H2's DDL commits, as design §3 rule 1 already
  says of DDL in a load on H2); `CREATE LOCAL TEMPORARY TABLE IF NOT EXISTS LEGEND_SESSION.legend_data_<16 hex> (data JSON[,
  ordinal INTEGER]) TRANSACTIONAL`; `DELETE FROM LEGEND_SESSION.legend_data_<16 hex>`; the content blob's inserts. No
  drop before the make (5: it would commit), and no `IF NOT EXISTS` outside lite's schema (4: it would capture a
  user's table);
- **read** qualified, `LEGEND_SESSION.legend_data_<16 hex>`;
- **released** (the target's `AT_RELEASE` load): `DROP TABLE IF EXISTS LEGEND_SESSION.legend_data_<16 hex>`, outside any
  transaction of the run's.

DuckDB's road stays as built (A4: dropped qualified to `temp`, created `TEMPORARY`, its drop and create in the load).
The difference is the dialect's: the session table's make, read and release spelled by `SqlDialect` per database,
never a planner branch. The one cost on H2: the empty schema `LEGEND_SESSION` stays in a database a `data:` source
once ran on (lite's namespace, as `__project_` is a host's).
