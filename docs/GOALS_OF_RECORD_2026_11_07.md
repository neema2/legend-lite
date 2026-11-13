# The goals of record (2026-11-07)

> **Status: RATIFIED by the user on 2026-11-07** ("Yes perfect", after a full review of this file).
> - Goals G1-G6 are the user's own, in their words.
> - G7-G11 were proposed and the user accepted them: "I really love it! Including the missing ones you added!"
> - Section 3 records the user's rulings, and section 5 the decisions, all ruled.
>
> This file is the first thing to read before reopening any "where should this live / may this run here" debate. It
> exists to stop the loop. A change to it is the user's call alone, and is dated here.
>
> It sits above the older tenet documents and does not replace them: `docs/TENETS.md` (eager knowledge, lazy work),
> `docs/TENET_CHARTER.md` (what Java may and may not do), `docs/WORLD_MAP.md` (natives, platform semantics, programs).
> Where this file and an older one disagree, this file wins and the older one is amended. The charter's amendments of
> 2026-11-07 are its Clause 7.

## 1. The goals

Each goal has the same parts:
- what it means concretely;
- what is allowed;
- the mechanical guard that proves it (existing, or to be built);
- its state on 2026-11-07.

### G1. One road: one compiler, one planner, one exec

Every statement goes through the ONE compiler, is planned by the ONE planner into ONE `ExecutionPlan`, and is run by the
ONE runner (`PlanRunner`). This covers a product query, a service, and a test body with its asserts. There is no second
evaluator: no walk, no interpreter, and no statement-by-statement path.

- **Allowed:** the test door (`TestDoor`, `BodyRouter`) plans a body as a program with verdict steps. It uses the same
  compiler, the same planner and the same runner, so it is an extension of the road, not a second road.
- **Guard:**
  - `ArchitectureTest`: one caller of the program entry per door.
  - The walk's door is deleted, with nothing left to open it.
- **State:**
  - The walk was deleted on 2026-11-06.
  - Its leftover branches were measured unreached on every lane and deleted on 2026-11-07 (`a55c3f63`).
  - Known remnant: the test-data generator's describe-render-run code, which has no main caller (delete it).

### G2. Judge parity: the host and database judges agree

The two judges implement the SAME comparison rules, from one spec: upstream's `equal`, with the ruled differences in
`docs/SEMANTICS_REGISTER.md`.
- The host judge compares typed rows in Java (ruled: correct in Java).
- The database judge has the database compute the verdict in SQL.

They reach the same verdict on every test of every lane. A disagreement is a bug in one judge, never a tolerance.

- **Guard:** the agreement judge (`BOTH`) on every lane; the gained and lost registers empty.
- **State:**
  - DuckDB corpus: identical (108 failures under each judge).
  - H2: not at parity (task #20: 66 lost, 8 gained).

### G3. A lean product path: typed values, lean SQL, fast exec

The product path answers TYPED values, matched by type and value. There is no string formatting of values on the product
path: a value's text is an edge concern (G5). The SQL is as small as the query needs; judge or canon wrappers never
appear in product SQL. Exec streams typed rows and does nothing else.

- **Allowed:** decoding a cell into the carrier its declared type names (charter C1.2 carriage).
- **Guard (to build):**
  - The writer guard extended to `exec`'s decode (today it scans only `result/`).
  - A product-SQL size baseline and an exec speed baseline, both shrink-only.
- **State:** open. See section 4, P3 and P4.

**The tie-breaker between G3 and G6 (agreed 2026-11-07):** product MEANING goes to the database even when the SQL
grows a little (relation `toOne()`'s guard, D2). Anything that exists only to match upstream's PRESENTATION goes to Java
at the edge (G5). Lean SQL never means moving product meaning into Java.

### G4. Provable separation: plan only plans, exec only execs, judge and driver included

Planning never touches a database. A run never plans: no typing, lowering, resolving or rendering while a plan runs.
This holds for the judges and the test driver too, not only the product.

- **The plan is SQL and data only.** It holds no Java code, no planning objects, and nothing stuffed into it to dodge
  this rule.
- **Exec runs exactly the plan's statements**, and nothing the plan does not contain.
- **Planning names targets; the run opens sessions.**
- **Proven twice:**
  - at run time, by the wall (`RunScope`), which refuses a crossing by name;
  - at compile time, by package structure and ArchUnit rules, so run code cannot reference planning code and planning
    code cannot hold a session.

  Together these are strong, but not a formal proof. The rules prove structure; the wall proves behaviour on the paths
  the lanes exercise. Guards catch drift, not adversaries.
- **Guard:**
  - `RunScope` on every planning seam (done: I1).
  - `RunScopeTest`.
  - ArchUnit rules R1-R4 (to build: I3d).
  - The runner refusing a side that is not in the plan (to build).
  - A plan JSON round-trip test, proving the plan is data (to build).
- **State:**
  - The runtime wall is total on every lane (I1, `75b95694`).
  - Planning names targets (I3a-c, `254f1dd4`).
  - Open:
    - exec runs statements the plan does not contain (the split rung, message rows, appeal sides);
    - planning objects are read at run;
    - the package split and ArchUnit rules.

### G5. Compatibility lives at the edge, never intertwined with the product

Everything whose only purpose is to match legend-engine's or legend-pure's observable behaviour sits at the EDGE: in one
contained place, depending on the product, never the reverse. Section 2 defines the edge.

- **Guard (to build):** an ArchUnit rule that no product package depends on the edge package.
- **State:** open. Compatibility code is spread through the product today:
  - the judges' `COMPILED_PURE_COMPAT` branches;
  - the engine-text printer;
  - the writers' Pure-text spellings;
  - legend-engine's per-statement SQL comment stamp (D3).

### G6. The database does the work

As much of the product's logic as possible runs in the database. Java orchestrates (charter Clause 1). It does not
derive values, decide types from values, reshape rows, or count rows (charter Clause 2).

- **Allowed (the user, 2026-11-07):** where matching upstream would make the product's SQL bad, a SMALL Java formatting
  step at the edge (section 2), in one contained place, is preferred to bloating product SQL. The same goes for appeals
  and the referee: raw JDBC is acceptable there, contained at the edge.
- **Guard:**
  - `JavaEvalLedgerTest`, shrink-only, with its text corrected (section 4, P3).
  - The writer guard extended to `exec`.
- **State:** open. Section 4 lists the product-path Java computation found on 2026-11-07.

### Additional goals: proposed, and accepted by the user (2026-11-07)

- **G7. No fallbacks, no silent defaults, anywhere.** A shape lite does not handle fails by name. This includes RUNTIME
  fallbacks: a rejected statement retried another way, a decline that rescues a failed text, a positional read when a
  name is missing, a value fabricated for a null. This is AGENTS.md invariant 4, extended explicitly to exec and the
  judges.
- **G8. Upstream fidelity, verified and ledgered.**
  - Lite does what upstream does, read from the pinned sources or asked of the engine runner.
  - Every difference is a user-ruled row of `docs/SEMANTICS_REGISTER.md`.
  - The edge (G5) is where fidelity is bought.
  - This restates the AGENTS.md upstream-fidelity tenet, scoped to the edge.
- **G9. Every goal has a mechanical guard, honestly labelled.**
  - A goal without a test that fails when it is broken is a wish.
  - A guard must scan where the code actually is. The exec decode was unguarded on 2026-11-07.
  - Ledger text must match the code. The ledger still names deleted classes.
- **G10. Determinism.** Lite's own tests are deterministic. An operation that reads an order the program never defined
  is refused at planning. This is S131's narrowing, already ruled; it is listed here so it is not forgotten.
- **G11. The plan is portable, host-agnostic data.** A plan can be serialized, read back, and run by any runner,
  including the WebAssembly planner's host. Proof: every plan of every lane round-trips through its JSON form
  unchanged.

## 2. The edge (compatibility), defined

**What is at the edge:** code whose only reason to exist is matching upstream's observable output or judging lite against
upstream's goldens:
- the engine-text SQL printers (`EngineStyleH2` and its family);
- legacy output formats and their value spellings (the Pure-text forms of Float, date and decimal in legacy JSON/CSV/TDS
  text);
- the compatibility judging rules (`JudgingRules.COMPILED_PURE_COMPAT`'s leniencies);
- the SQL-text appeal and the referee;
- golden replays;
- corpus-authored raw SQL (`RawSqlBoundary`);
- the PCT adapter (`ValueBridge`);
- legend-engine's per-statement SQL comment stamp (D3);
- the corpus and PCT harnesses.

**The rules of the edge:**
- **E1. One contained place.** One package (or module) per concern, named as the edge, with no edge logic scattered
  through product classes. A product class that branches on a compatibility mode is a defect to move out.
- **E2. One direction.** The edge depends on the product; no product package depends on the edge. Proven by ArchUnit.
- **E3. The edge may use Java and raw JDBC.** It may format text, compare rows, and run a golden on a connection
  (ruled: the appeals and the referee, 2026-11-07). It may NOT put Java code or edge-only data into a product plan, and
  may NOT change what the product plans or sends to serve compatibility. The edge adapts the product's typed answer;
  the product never bends to the edge.
- **E4. Every edge behaviour that differs from the product's own is named.** That means a register row the user ruled,
  so the edge is measurable and can shrink.

## 3. Rulings of record (the user's words)

- **2026-11-06:**
  - The walk is deleted.
  - Plan ahead, don't refuse: the engine-text appeal is planned ahead.
  - The test-data generator tests are skipped (S143).
- **2026-11-07:**
  - "The host judge is correct to be in java."
  - "The appeals are okay to be in java if that's cleaner … bespoke … confined to edges"; for the appeals and referee,
    "raw jdbc is fine but should be contained and confined to edges".
  - "What's not okay is java code or any other code or data structures in the plan."
  - "It's not okay to stuff random stuff into the plan to side step … plan only plans and exec only execs."
  - "Keyed is fine." The runner may write a keyed region once per key read from the database: the dynamic pivot.
  - "If at the edges of compatibility we need java code to do small formatting things so that our product SQL doesn't
    get super bad then that's okay but it should be … confined to the edges and be in one place."
  - On relation `toOne()`: "sql guard is good" (D2).
  - On the type hole, the comment stamp, MariaDB's read-back and the charter amendments: "I love everything" (D1 as
    first proposed, D3, D4, D5).
  - On run-time parameters typed by their declared Pure type as upstream does: "Yes perfect" (D1, revised).

## 4. Plan of attack

The phases run in order. Each phase ends when its guard is green on every lane: core, both corpus lanes, PCT on DuckDB,
H2 and channel B, both stress suites, and the core Postgres tests as an unprivileged user. Each step lands on the
branch, never on main without the user's word.

**P0. Ratify this file: DONE (2026-11-07).** G7-G11 accepted, every decision ruled, the charter amended (Clause 7),
and AGENTS.md points here.

**P1. Finish the separation (G4, G1).**
- **Status (2026-11-08):** P1.0-P1.3, the report-mode rules and the milestone audit are done (branch
  `claude/corpus-walk-deletion`; GATES 2026-11-07). The rest of P1 -- the package split, R1-R4 hard and the plan as
  data -- is ONE program, designed and ruled in `docs/PLAN_AS_DATA_2026_11_07.md` (two plan types, the test plan's
  judgments as data, the edge's typed extension point, exec as the one walker with a callback, no `driver` package,
  the no-code-in-the-plan guards). Its order of work (section 4) is P1's order from here.
- **First, the enabling steps (agreed 2026-11-07):**
  - **Make the lanes trustworthy.** Find what drives the DuckDB stress lane's memory to ~11 GB, by measurement, before
    choosing a fix. Per-test seeding is NOT the answer: it took the lane from minutes to ~50. Run the heavy lanes one
    at a time, with the disk kept clear.
  - **Baselines for G3:** product SQL size per corpus test, and time per lane.
  - **The plan JSON round-trip** over every lane's plans (G11).
- **The ArchUnit rules are written first in report mode.** They list every violation; then the list is pinned and
  ratcheted to zero, the way the runtime wall was built.
- Exec runs only the plan's statements, and the runner's reader refuses a side that is not in the plan:
  - the verdict batch's split rung is decided at planning, and a rejected statement fails by name (no runtime retry);
  - caller-message rows use the plan's own per-verdict message side;
  - the appeal plans and runs its own statements at the edge after the body's run, never through the body's reader.
- Planning objects leave run data:
  - relation-level `toOne()` is checked in SQL (D2), so no typed tree is read at run and Java counts no rows;
  - the runner reports which statement failed (no dialect parsing of error text);
  - the dead `CanonRider` construction and the inert typed trees and environments in planned checks are deleted.
- The package split, plus ArchUnit rules R1-R4 (R1: run code reaches no planning package; R2: planning holds no session;
  R3: only the router sees both halves; R4: the judge package holds no session).
- The plan JSON round-trip test (G11).
- Delete the dead test-data generator transcript path.

**P2. Build the edge (G5).** One edge package. Move into it:
- the engine-text printers;
- the appeals and referee wiring;
- the compatibility judging rules;
- the legacy writers' Pure-text spellings (charter Clause 7, A1);
- `RawSqlBoundary`;
- legend-engine's per-statement SQL comment stamp (D3).

Then add the ArchUnit rule that the product never depends on the edge.

**P3. Lean product, part 1: no Java value computation on the product path (G6, G3).** For each product-path site found
on 2026-11-07, either the database does it (a cast in SQL), or it moves to the edge, or a ruled register row names it:
- a REAL re-parsed through its shortest text;
- BigQuery's scale padding;
- the TIMESTAMPTZ shift to UTC;
- the BC-year repair;
- kinds chosen from a cell's text (`.`/`e`, `{`/`[`);
- the positional read fallback;
- the dynamic-pivot envelope built at run from compiler types.

Then extend the writer guard to `exec`, and correct the ledger's text.

**P4. Lean product, part 2: no SQL edited or sent outside the plan (G4, G6).**
- Delete the type hole. Run-time parameters are typed by their declared Pure type, per database, as upstream does (D1).
- The product's traceability: a planned comment plus exec's own trace (D3).
- MariaDB/MySQL's raise read-back declared by the plan's target (D4).
- AGENTS.md invariant 3's "one designed hole" text corrected (Clause 7, A4).

**P5. Judge parity (G2).** Bring H2 to parity (task #20), then keep the agreement judge on every lane with empty
registers.

**P6. No fallbacks, honest appeals (G7, I4).**
- Sorted pages compared in order.
- Kinds and labels checked by the referee.
- S132 option (a): spellings of one value fail.
- Empty-versus-empty counted unproven.
- The null-to-`""` fabrication deleted.
- Register rows for the appeal and the Java-judged helpers.

**Then:** the corpus and stress burn-down. The stress batch (CSV seeds, service parameters bound as lets, the M2M
chain, the legacy join's verbatim condition) is parked in the stash, its patch in the session scratchpad. The stress
lane's memory also needs fixing: it keeps one seeded DuckDB per test runtime open, at about 11 GB.

## 5. Decisions (all ruled by the user, 2026-11-07)

- **D1. The type hole, and run-time parameters: RULED (the user, 2026-11-07: "I love everything", then "Yes
  perfect" on the revision below).**
  - **Delete the type hole.** Today the runner writes a SQL type into `CAST(? AS ___)`, chosen by the bound value's kind
    and digits. That breaks charter C2.2, C5.1 and C5.2.
  - **Services bind their parameters at compile time, as today.** The server turns each `{name, value}` of a call into
    `let name = value;` ahead of the body (`PureV1Api.boundParameters`). The compiler types each value against the
    parameter's declared type, and the plan is made with the value known. This works because each call is compiled and
    planned again.
  - **Run-time parameters ("plan once, run many") are typed by their DECLARED Pure type, per database, as upstream
    does.** This was read in legend-engine's pinned sources: `dbExtension.pure` `processPlaceHolder`,
    `extensionDefaults.pure`, and each database's extension. Upstream writes a template `${param}` whose prefix and
    suffix its database extension picks from the declared type, fixed when the plan is made. Only the value's text is
    substituted at run time.
    - The planner writes each placeholder with the SQL type upstream's extension uses for that declared type on that
      database. The runner checks the caller's value against the declared type and binds it. Nothing at run time writes
      a type into the statement, and the SQL text is the same on every call. (Corrected 2026-11-09 by the tenets
      re-audit's B-4: where a number is bound bare, the driver passes the value's own class -- a `BigDecimal`, a
      `Double`, a `Long` -- and the database types the value from it, as upstream's bare literal is typed by its
      digits. That is the database typing a value, not the runner choosing a type, and no divergence.)
    - **`Date`** (a StrictDate or a DateTime) is a timestamp almost everywhere. Upstream writes `TIMESTAMP'…'` on H2,
      Postgres, Trino, Presto, Redshift and Spanner, `'…'::timestamp` on Snowflake, and `'…'::DateTime` on ClickHouse.
      On BigQuery it is a date (`PARSE_DATE`). A StrictDate value passed to a `Date` parameter becomes a midnight
      timestamp, as upstream. For a database upstream has no extension for (its default writes an untyped string the
      database coerces), lite uses a timestamp, the majority choice. That is a register row.
    - **`StrictDate`** is a date. **`DateTime`** is a timestamp at the database's finest precision.
    - **Numbers.** Upstream writes the bare value, so the database types it from its digits; on H2 a Float is
      `CAST(… AS FLOAT)`. Lite binds the bare value on the databases that type a bound value as its literal (DuckDB and
      Postgres, by lite's probes). On H2, which types a placeholder before it sees the value, lite uses the closest fixed
      type: Integer `BIGINT`, Float `FLOAT` (upstream's own), Decimal and Number `DECFLOAT` (exact). Each place an answer
      can differ from upstream's literal is a register row for the user to rule (a Decimal parameter's computed-result
      scale on H2, for instance).
    - **Lists** are bound as one array (`= ANY(?)`), as now.
    - **Time zones.** Where a database time zone is configured, upstream converts date parameters into it (`GMTtoTZ`).
      Lite plans the same conversion.
  - **Later decision:** whether product services move from compile-per-call to plan-once, run-many. To be decided when
    measured.
- **D2. Relation `toOne()`: RULED (the user, 2026-11-07: "sql guard is good").** A guard in SQL, proven on each
  database. As built (`lowering/ToOneGuard`, P1.3) and ruled on 2026-11-08 ("yes" to keeping it): each row carries a
  `COUNT(*) OVER ()` window count and the rows are left-joined onto a one-row source, under `WHERE CASE WHEN
  COALESCE(count, 0) = 1 THEN TRUE ELSE error(...) END` -- one read of the relation; the one-row source is what makes
  the zero-row case raise (a window column alone would miss it). The aggregate-joined-back shape first written here
  reads the relation twice and needs the same left join for zero rows; it was prototyped beside the built one and not
  kept, which the user was told only on 2026-11-08. The message is Pure's: `Cannot cast a collection of size N to multiplicity [1]`. The Java row
  count, and the typed tree read at run to decide it, are deleted. This is done in P1.
- **D3. The SQL comment stamp: RULED (2026-11-07).**
  - **Product:** traceability without editing SQL at run time. At planning, the dialect writes a fixed comment naming the
    plan and the statement. Exec records the per-execution id beside the plan id in lite's own trace. Where a database
    supports a session tag, a planned setup statement may carry the execution id.
  - **Edge:** legend-engine's exact per-statement stamp, `-- "executionTraceID" : "<uuid>"`, moves to compatibility
    mode only.
- **D4. MariaDB/MySQL raise read-back: RULED (2026-11-07). It is product, not compatibility.**
  - MariaDB and MySQL cannot raise a custom message from a SELECT. So the SQL keeps Pure's message in `@legend_raise`
    and forces a generic error, and exec reads the message back after the failure. Without it, every Pure error on
    those databases would read "Subquery returns more than 1 row".
  - The plan's target declares the two read-back statements as data, and exec sends them only after a failure
    (charter Clause 7, A3(c)).
- **D5. Charter amendments: RULED (2026-11-07).** Written into `docs/TENET_CHARTER.md` as Clause 7:
  - A1: C2.5 split into product (typed, no print rules) and edge (print rules in Java at the edge);
  - A2: the compile-time constant-string fold;
  - A3: C1.4's three named exec cases (keyed regions, planned branches, declared error read-back);
  - A4: the guards' text corrected as the code changes.
