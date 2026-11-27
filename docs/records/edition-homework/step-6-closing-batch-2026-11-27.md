# Step 6: everything that is left, as one batch (2026-11-27)

The user, 2026-11-27: "We shouldn't land with the planner knowing editions", and "How can we batch up all the
remaining step 6 issues and do them all at once instead of keep pushing things one by one - this is tempting us to
push serious bugs and bad design."

This page is the one list. Nothing of step 6 is fixed, pushed or run again outside it. The evidence for each row is
in `step-6-one-equality-2026-11-27.md` (same folder).

## How the batch is run

1. **Freeze.** No more one-by-one fixes. The scratch branch (`claude/step-6-numbers`, head `84cf07872`) stays as it
   is; nothing goes to the pull request's branch or the program branch. The checks already running (the full round,
   seven database lanes, the local gate) finish as a mid-point reading and are not repeated until the batch is done.
2. **One list** (below), each row with a proposed outcome: build in the batch, record as a known limit, or outside
   step 6. The user rules the list in one sitting.
3. **One design note** for the rows marked "design", written before any code, and read by an independent reviewer
   before any code. The reviewer is given the note and the rules, never the author's conclusions.
4. **One build**, in the order the design note fixes, on one branch, with the fast loop (one test class at a time)
   and no pushes between pieces.
5. **One verification** at the end: the full round, the relational corpus and the local gate, the acceptance measure,
   all ten database lanes, one independent review of the whole batch.
6. **One landing summary** for the user: every answer that moved, every pin that moved, every register row, every
   limit that stays.

## A. Design: must change before landing

| # | What is wrong today | Proposed |
|---|---|---|
| A1 | **The planner works out who is asking.** `planner/RootComparison` plans lite's strict comparison only when the door's table named no number rule for the `==` (`Lowerer.numbersOf`), which is the planner inferring the door from a side signal. | The planner asks nothing. It offers the whole-side plan to the lowering; the lowering's ONE existing lookup (`EditionTable.lowersAs`, step 5's ruled design) decides at the call: a rule the door named, or the product's own, which plans whole sides. `Lowerer.numbersOf` and `NamedRules.numbers` are deleted. |
| A2 | **Two paths for one comparison.** A comparison that is the query's last statement is planned by a pre-pass (`ProductStatements.lower` calling `RootComparison.plan`); the same comparison inside an expression by a hook the lowering calls. Two entries, kept equal by a test. | One path: the query's last statement is lowered like any expression, and its `==` arm asks for the plan. Falls out of A1. The statement's SQL is the planned statement itself, not a wrapper around it. |
| A3 | **The empty-side rule asks the door's table a yes/no** (`EditionTable.emptySideIsFalse`): the product and legend-pure's door answer false, legend-engine's door keeps the database's NULL. A second kind of question to the door, added today. | No question. Pure's answer (false) wherever a comparison's value is kept, at every door. At legend-engine's door that is a new recorded difference where legend-engine's database answers no value; what legend-engine answers there is run first. **The user's ruling.** |
| A4 | **"Is this side's order defined?" is judged on the tree after the store is resolved**, where a class query has become table reads. The rule (`OrderView`) was written for the query as written; it missed a class query mapped to a list until a probe found it. | Judged once, on the query as typed (before the store is resolved), and carried to the planner as a fact of the comparison. |
| A5 | **`hasSameElements` is checked for two types where it is planned**, not where the query is typed; `==` is checked in the type-checker. | Registered with the type rule (`EqualityClasses`), so it is checked where `==` is, and a model's compile check reports it. |
| A6 | **The planning-time refusal of two column types (`HeldKinds`) runs "when no door named a rule"** by its position in the code, which nothing states. | Stated as part of the product's own rule for the call, reached through the same one lookup as A1; no other condition. |
| A7 | **The Java checker's tie rule reads a per-door form** (`HostForms.exactNumbersByValue`), added in step 6 beside the existing per-door forms. | Kept: the door hands the Java checker its forms object already (an existing mechanism, removed with the rest by the edition program's closing guards). Listed so it is a decision, not an accident. |

## B. Wrong or missing answers still inside step 6's promise

| # | What | Proposed |
|---|---|---|
| B1 | A comparison of lists, maps or tables whose sides read the row they stand in (a filter comparing each row's list): the database's own comparison, which is wrong for a map's entry order and a table's columns, and for a Decimal list on SQLite. | Right where the database's own list comparison is the rule (typed lists on a database with lists); refused by name everywhere else. Never a wrong answer. Build. |
| B2 | A computed LIST of two types whose computation would raise answers a quiet false (single values are evaluated since today). | Record as a limit (it needs list values inside an expression). |
| B3 | Two sides of two types that may both be empty: the database's comparison (Pure: equal only when both are empty). | Build: the planned form answers it at the root and inside an expression already; make the single-value case take it. |
| B4 | One value read outside and inside a lambda is still two evaluations (`$ks->filter(k \| $k == $ks->at(0))`; Pure true, lite likely false). Not run. | Probe first; build if confirmed. |
| B5 | A caller's lambda loses its author's mark at four desugaring sites (the fold's map unroll and static `if`, the legacy column forms, project's and groupBy's rebuilds). | Build (carry the mark at each). |
| B6 | A generic function's `Number` parameters called with `1` and `1.0`: a compile error where the rule says false. | Record as a limit: the fix retypes every such function's arithmetic. |
| B7 | Two tables whose column names differ are answered false without evaluating either side. | Record (Pure's own answer for two tables is identity, never an evaluation of rows). |
| B8 | The fresh-draw check reads the body, not the lets the environment seeds. | Build (small). |
| B9 | A mixed list holding an Integer and Decimals more than 38 digits apart; `date(2015, 4)` against a date-time; which of two equal exact numbers `max` keeps. | Record as limits. |
| B10 | `max`/`min` at legend-engine's door: an Integer against a Decimal column, a Decimal against a Float column keep the winner's type where legend-engine's H2 answers its promoted type. | Record: measured on one database. |
| B11 | The two test checkers' wording for a JSON value read through `Any`: three of four defects fixed in code, not run; `assertSameElements` prints two identical texts without types. | Run the fix in the batch; the fourth recorded. |
| B12 | The two date facts (first day of week, month, quarter, year is a day; a day moved by days stays a day) apply at every door with no register row. The corpus shows no test moved. | Register row; in the landing summary. |
| B13 | The tolerance assert is the database checker's own formula, not the product's. | Record as a cleanup (no answer changes). |
| B14 | The bodies the database checker cannot judge (6, 2, 6 in the measurement) are not read one by one. | Read them from the batch's acceptance measure. |

## C. Not yet verified, or housekeeping

| # | What |
|---|---|
| C1 | Not run: the rename to `hasSameElements`; the JSON wording fix. |
| C2 | The time-series bundle budget (the user: "Time series budget fine"): the pin moves with the measured size. |
| C3 | The acceptance measure (176 differing bodies at the start) has not been re-run. |
| C4 | Seven database lanes, the local gate and the stress suites are still running on `83cb22ae2`, which predates the batch. Back so far on that commit: the relational corpus (10 of 10, nothing moved), SQLite, MongoDB, BigQuery (no outcome differs). |
| C5 | The parser ratchet's history line; the differences page and the plain-words page brought to the batch's end state. |

## D. Outside step 6, as already agreed

- Lists on a database with no list type (H2; partly SQLite, MySQL, MariaDB): the database program.
- SQLite's date-time text: its own slice right after step 6.
- A table's cell count in `assertSize`: step 10. A third date-time type: after Stage B. A Float's text at
  legend-engine's door: step 8.
- A Float COLUMN combined with a Decimal: kept as it is (the user, "Okay keep the float thing then as is").
- The door's table read at the lowering's one lookup (step 5's ruled design) goes with the edition program's closing
  guards, when the type-checker's rewrite replaces it.

## What the user rules

1. A1 and A2: the lowering's one lookup decides, the planner asks nothing, one path. (Recommended.)
2. A3: Pure's answer for an empty side at legend-engine's door too, as a recorded difference. (Recommended, after
   legend-engine's own answer is run.)
3. A7: the Java checker's per-door form stays until the edition program's closing guards. (Recommended.)
4. B1: refuse by name where the database's own list comparison is not the rule. (Recommended.)
5. B2, B6, B7, B9, B10, B13 as recorded limits. (Recommended.)
