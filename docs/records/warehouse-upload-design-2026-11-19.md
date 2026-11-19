# A file's load on a warehouse: the upload-then-read delivery (design for the user's ruling, 2026-11-19)

Track Q2's design note, written on the orchestrator's word of 2026-11-19 for the user's ruling. Nothing here is built.
Branch `claude/warehouse-binds-q2`.

## The case

A test body's `loadCsvToDbTable` is planned as `ExecutionPlan.LoadStep.Delivery.Reads`. The user ruled that delivery on
2026-11-09: "the database reads the file itself". The runner writes the verified bytes to a file of its own and binds
that file's path to the load statement's one `?`. DuckDB's statement is `read_csv(?, ...)`; H2's is `CSVREAD`.

That works only where the database shares the runner's filesystem. The warehouse's DuckDB runs on a server, and its
lockdown (`Database.lockDown`) lets it read its own `<data>/import` folder alone. So the corpus's
`meta::relational::tests::loadCsv::testLoadCsv` cannot load on `//spec:corpus_warehouse`.

**What is built today (S372):** the load is refused by name.
- **At planning:** for a run whose target runs on a server (`ExecuteOptions.withTargetOnAServer`, set by the warehouse
  lane's backend).
- **At the run:** on a session made as a server's (`Sessions.Reach`).
- **Upstream:** legend-pure's `loadCsvToDbTable` reads the file itself and batch-inserts the rows over the connection
  (`RelationalGen.bulkInsertInDb`), so its load reaches a server.

## What upload-then-read needs

`docs/DATA_IN_THE_PLAN_2026_11_09.md` §2.5 lists it as later: `Uploaded(stage)`, "warehouses with an upload stage:
uploads the verified bytes, then runs the stage's load".

### U1: the plan format

- **The delivery:** `LoadStep.Delivery` gains `Uploaded(upload, statements, after, sites)` beside `Reads`.
- **The versions:** `PlanJson` and `TestPlanJson` take a version bump.
- **The other runners:** TypeScript's and Python's plan runners read the new delivery, or refuse it by name.
- **Choosing it:** the planner picks it by the target's capability, the fact built today
  (`PlanningOptions.targetReadsRunnerFiles`). A target that takes uploads says so: the warehouse lane's backend, and
  later a declared warehouse connection.

### U3: the upload's channel

- **(a) A statement the warehouse answers (recommended).** It is an admin statement of the warehouse's own, as its
  GRANTs are (`AdminStatements`), and owner-only.
  - The verified bytes are its one bound value, as base64 text, since the SQL API binds no BLOB.
  - The server checks the digest again and stores `<import>/<digest>`, content-addressed.
  - It answers the stored path as one row, and the file is deleted when its session closes.
  - Core stays JDBC-only: exec sends a planned statement with bound values, as it always does. The JDBC census is
    unchanged.
- **(b) An HTTP upload route plus a JDBC vendor call.** Core cannot name the warehouse client's classes, so the seam
  would reach the call through `Connection.unwrap`. That is the census's OPENS act on `JdbcStatements`, a row move, and
  a second channel beside the statement API.

### U4: the read, and the exec act it needs

- **Measured on DuckDB 1.5.5.1 (2026-11-19, with the warehouse's lockdown settings):** a relative name is refused even
  with `file_search_path` set to the import folder. The error is "Permission Error: Cannot access file "abc123.csv" -
  file system operations are disabled by configuration", because `allowed_directories` checks the name as given.
- **So:** the read statement must bind the absolute server path, and only the upload's answer knows it.
- **The new act:** a value read from one planned statement's answer, bound into the next statement's `?`.
- **Why it needs the user:** AGENTS.md invariant 3 and `docs/TENET_CHARTER.md` A3 list the three things exec may do
  beyond sending the plan's text: keyed regions, planned branches and declared read-backs. The user ratified that list.
  The nearest precedent is the keyed region, where a key read by the plan's first statement is bound into each copy.
- **What it would take:** a fourth item on that list, or the keyed region's rule widened to name it.

### The server-side alternative, and why I advise against it

The upload statement could carry the read statement too. The server would store the bytes, bind its own path into the
carried statement, and run it. Exec would then need no new act. I advise against it, because:

- **Opaque SQL:** the server would hold and run SQL text a plan handed it as a value. The warehouse's statement API
  takes SQL to run as its `sql`, never inside another statement's value, and its authorizer reads only that `sql`.
- **A second evaluator:** it is a composite the warehouse alone understands. A second place where a load's statements
  are assembled is against G1's one road.
- **Hidden steps:** the plan no longer shows what runs. The read statement sits inside the upload's value, so the guards
  that witness every sent statement (`SentStatementsAreThePlansTest`) would see one statement where two run.

## What the user rules

1. **U4:** may exec bind a value read from one planned statement into the next (a fourth A3 item), or is the
   server-side composite preferred despite the above?
2. **U3:** the statement channel (a) or the HTTP route (b).
3. **U1:** the plan-format bump, which waits on 1 and 2.

Size if ruled (a) with the new act: about 600-700 lines with tests across the plan format, the planner, exec, the
warehouse server and the TS and Python plan readers.
