# The goals of record (2026-11-07)

> **Status: DRAFT for the user's ratification.** Goals G1-G6 are the user's own, in their words (2026-11-07). G7-G11
> were proposed and the user accepted them (2026-11-07: "I really love it! Including the missing ones you added!"). Section 3 records rulings the user made on 2026-11-06/07.
> Section 5 records the decisions, all ruled on 2026-11-07. Once ratified, this file is the first thing to read before reopening any
> "where should this live / may this run here" debate: it exists to stop the loop.
>
> It sits above the older tenet documents and does not replace them: `docs/TENETS.md` (eager knowledge, lazy work),
> `docs/TENET_CHARTER.md` (what Java may and may not do), `docs/WORLD_MAP.md` (natives, platform semantics, programs).
> Where this file and an older one disagree, this file wins and the older one is amended (section 5 lists the known
> conflicts).

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
- **State:** open. See section 4 items P1-P8.

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
  - the SQL comment stamp.

### G6. The database does the work

As much of the product's logic as possible runs in the database. Java orchestrates (charter Clause 1). It does not
derive values, decide types from values, reshape rows, or count rows (charter Clause 2).

- **Allowed (the user, 2026-11-07):** where matching upstream would make the product's SQL bad, a SMALL Java formatting
  step at the edge (section 2), in one contained place, is preferred to bloating product SQL. The same goes for appeals
  and the referee: raw JDBC is acceptable there, contained at the edge.
- **Guard:**
  - `JavaEvalLedgerTest`, shrink-only, with its text corrected (section 4, P8).
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

## 4. Plan of attack

The phases run in order. Each phase ends when its guard is green on every lane: core, both corpus lanes, PCT on DuckDB,
H2 and channel B, both stress suites, and the core Postgres tests as an unprivileged user. Each step lands on the
branch, never on main without the user's word.

**P0. Ratify this file.** The user corrects it. G7-G11 are accepted or struck, and the open decisions (section 5) are
ruled. Then AGENTS.md points here.

**P1. Finish the separation (G4, G1).**
- Exec runs only the plan's statements, and the runner's reader refuses a side that is not in the plan:
  - the verdict batch's split rung is decided at planning, and a rejected statement fails by name (no runtime retry);
  - caller-message rows use the plan's own per-verdict message side;
  - the appeal plans and runs its own statements at the edge after the body's run, never through the body's reader.
- Planning objects leave run data:
  - the to-one decision moves to planning (see P4);
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
- the legacy writers' Pure-text spellings;
- `RawSqlBoundary`;
- the SQL comment stamp, if the user rules it compatibility.

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
- The type hole (decision D1).
- Relation-level `toOne()` (decision D2).
- The per-statement comment stamp (decision D3).
- MariaDB's raise read-back (decision D4).

**P5. Judge parity (G2).** Bring H2 to parity (task #20), then keep the agreement judge on every lane with empty
registers.

**P6. No fallbacks, honest appeals (G7, I4).**
- Sorted pages compared in order.
- Kinds and labels checked by the referee.
- S132 option (a): spellings of one value fail.
- Empty-versus-empty counted unproven.
- The null-to-`""` fabrication deleted.
- Register rows for the appeal and the Java-judged helpers.

**Then:** the corpus and stress burn-down (parked), and the stress lane's memory, which keeps one seeded DuckDB per test
runtime open at ~11 GB.

## 5. Decisions (all ruled by the user, 2026-11-07)

- **D1. The type hole: RULED (the user, 2026-11-07: "I love everything").**
  - **Ruling:** delete it. The runner writes a SQL type into `CAST(? AS ___)`, chosen by the bound value's kind and
    digits (it breaks charter C2.2, C5.1 and C5.2). No product caller needs it.
  - **Services keep their parameters; they bind them at compile time.** The server turns each `{name, value}` of a call
    into `let name = value;` ahead of the query's body (`PureV1Api.boundParameters`, used by `execute`, `executeArrow`
    and the hosted runs). The compiler then types each value against the parameter's declared type, and the plan is
    made with the value known, so its SQL types are exact. A value may be an expression (`today()`), which the database
    computes.
  - **Run-time parameters:** Integer, String, Boolean, StrictDate and DateTime keep the fixed SQL types they already
    have. One typed Float, Decimal, Number or Date is refused at planning, by name: bind it at compile time.
  - **Plan-once, run-many:** if service traffic ever needs it, a fixed cast per declared Pure type would serve it, decided
    when measured.
- **D2. Relation `toOne()`: RULED (the user, 2026-11-07: "sql guard is good").** An aggregate guard in SQL
  (`CASE WHEN COUNT(*) = 1 … ELSE error(...)` joined back), proven on each database. A window column alone would miss
  the zero-row case. The Java row count is deleted.
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
