# D, Phase 2: typed raises with operands (design note, amended 2026-11-16)

The edge program (`docs/records/total-wall-edge-program-2026-11-16.md`, branch `claude/total-wall-orch`), Phase 2,
table row 9.

**State:**
- The shape is APPROVED (the orchestrator, 2026-11-16, §2.1), with conditions 1-8.
- E-Java's objection stands: the judges word a raise at RUN time, and `testexec` reaches no `testplan` (D72). So the
  third `PlanStyle` facet is WITHDRAWN, and the words travel IN THE PLAN as templates (E-Java's counter, adopted as part
  of the format-56 bump).
- **Order:** Phase 1's CI list goes green first. Then step 1 (the product). Step 2 (the edge's templates) waits for the
  golden net, which is its proof.
- **Review:** this amendment goes to E-Java for the judges' half, then to the orchestrator.

## The goal
Every raise carries its kind plus its operands as data, and the sentence is filled in from a template the plan carries.
No sentence is built in SQL. No edge code runs at run time.

## Today (d-stage @ 83719a707, measured)
- `SqlExpr.RaiseText(kind, operands, asData, where)` is built at 43 sites:
  - 26 in `lowering/`;
  - 16 in the SQL layer;
  - 1 in `SqlRewriter`.
- `RaiseKind` has 26 kinds.
- **The product** writes `MARK kind [MARK where]` (`AnsiSqlRenderer.raiseMessage`, the one writer of the mark). Any
  operand computed in the query is dropped.
- **The edge** builds upstream's sentence in SQL: 13 `raiseMessage` overrides call `PureMessages.sentence`, which is 25
  kinds of `CONCAT`/`CAST`, plus a `FLOAT_PRINT` inside the sqrt sentence.
- The run decodes with `exec.RaisedErrors` into `DataError(kind, where)`.
- `PlanJson.raises` maps each kind to the product's sentence.

## The shape
1. **One fixed operand list per kind (condition 3).**
   - `RaiseKind` declares each kind's operands, typed (Integer, Float, Decimal, String, StrictDate, DateTime), and each
     one either FIXED (written at planning) or COMPUTED (in the query).
   - Every site of a kind writes the whole list. A site that would write a different list is a different kind.
   - Today's varying kinds split:
     - `DIVIDE_BY_ZERO` (none) and `DIVIDE_BY_ZERO_OF(Integer dividend)`;
     - `SIZE_NOT_MULTIPLICITY_EMPTY(String bound)` and `SIZE_NOT_MULTIPLICITY(String bound, Integer size)`;
     - `DATE_COMPONENT_INVALID(String part, <value>)` and `DAY_INVALID(Integer y, Integer m, Integer d)`;
     - `VALUE_NOT_HELD`, `FRACTION_NOT_KEPT` and `CSV_TOO_WIDE`, one kind per distinct `parts` shape (listed at build).
   - `where` is an ordinary declared FIXED operand wherever a kind has one: never optional.
   - `RaiseKindOperandsTest` holds every construction site to its kind's list.
2. **The text in SQL, and its framing (condition 1).**
   - The text is `MARK kind (MARK operand)*`, in the declared order.
   - Rule: a kind declares **at most ONE COMPUTED String operand, and it is LAST**. The reader splits the first n-1
     operands at `MARK` and takes the rest of the text verbatim as the last operand, so a `U+001C` in user data cannot
     forge an operand or a kind.
   - No other operand can contain `MARK`: numbers and dates by type; fixed texts are checked at planning and refused by
     name if they contain it.
   - `RaiseKindOperandsTest` holds the rule at the declaration.
   - The reader refuses by name an operand count other than the declared one, or a kind the statement did not declare.
     No partial decode (G7).
3. **NULL (condition 2).**
   - A computed non-String operand is written `COALESCE(<its text>, NULLMARK)`, where `NULLMARK` is `U+001D`. It reads
     back as a typed null. It cannot collide, because a number's or a date's text never holds it.
   - The computed String operand is proven non-null where its kind fires: `TEXT_NOT_INTEGER` and `TEXT_NOT_DATE` fire
     only on a non-null text that did not parse. The test asserts each site's guard.
   - A kind whose String operand could be null where it fires is refused at build until it is handled.
   - The probe gets a NULL case wherever a NULL is possible (the numeric operands).
4. **The operand's text (conditions 4 and 6).**
   - A computed operand is the database's plain cast to text, as carried, and is read back typed by the shared result
     readers.
   - The probe asserts EXACT read-back for each operand TYPE on each database:
     - Integer: `at`;
     - Float: `sqrt` of a computed negative;
     - StrictDate and DateTime (nanoseconds included): a date part;
     - String: a text that is not an Integer;
     - Decimal: if any kind declares one.
   - Where a plain cast does not read back exactly (a Float on Snowflake, at least), that database carries an exact
     form in ONE call. That function IS Phase 8's Float carrier, one per database, shared by both. Its home is agreed
     with E-SQL before building.
5. **Decode (one reader).** `RaisedErrors` reads the kind and the operands, typed by the plan's map, into
   `DataError(kind, List<Object>)`. The same rule is ported to the TypeScript and Python hosts, with one shared fixture.
6. **The words travel in the plan: format 56 (approved).**
   - `raises` becomes `kind -> {operands: [type...], template: "..."}`:
     - the product's template comes from `ErrorKind`;
     - an edge plan's template is legend-pure's sentence, supplied at planning by the edge's style, and `PRODUCT`
       refuses it by name.
   - **Template syntax:** positional slots `{0}`, `{1}`, ..., with ONE escape, `{{` for a literal `{`. It is read by
     the same rule in Java (`PlanJson`), TypeScript and Python, with one shared fixture. No `MessageFormat`.
   - `RaisedErrors` fills the template from the map, in exec, the test runner and the hosts. No edge code runs at run
     time.
   - A test plan's verdict statements declare their raises into the same map (E-Java's `SideShapes`:
     `SIZE_NOT_MULTIPLICITY(String bound, Integer size)`). The test plan embeds the product plan's raises, so
     TestPlanJson goes 25 -> 26.
   - A 55 plan is refused by its version.
   - Every version pin and golden moves with a dated reason.
7. **Typed slots under an edge template (the orchestrator's ruling 3).**
   - A FIXED Float or date operand under an edge template is printed AT PLANNING by the edge, in Java, with the
     existing print: exact. The upstream sqrt test is `sqrt(-1.0)`, a literal.
   - No formatter in exec (G5).
   - A COMPUTED Float or date operand under an edge template: MEASURE FIRST. List every kind and operand type, and
     count the PCT, corpus, core and pure/v1 golden outputs that reach one with a computed operand (expected: 0).
   - **Interim:** step 2 does not build those slots. A kind with such an operand keeps its edge SQL sentence until the
     count is in and the orchestrator chooses between (i) the database's text (a register row) and (ii) an edge
     presenter at run (a new mechanism, the user's call). Nothing picks one silently.
8. **MongoDB (condition 7).** The pipeline writes the text with `$concat` of the mark, the kind and each operand's
   `$toString`. A null operand is written `$ifNull: [..., NULLMARK]`. The same reader reads it back, and A-S5's
   `everyKindAStatementMarksIsDeclared` covers it.
9. **Message length (condition 5).** Each database's limit is measured on the probe with the longest declared list:
   MariaDB and MySQL (`SIGNAL`'s `MESSAGE_TEXT` is 128 characters, but lite's raise reads the message back from
   `@legend_raise`, so it is measured on that path), SQLite, ClickHouse `throwIf`, BigQuery `ERROR`, Snowflake, Trino,
   Databricks and MongoDB. A database that cuts the text gets a kind-only fallback that is **refused at planning**,
   never a silent cut.

## What gets deleted (step 2)
- `PureMessages.sentence` (the SQL builder) and its SQL helpers.
- The 13 edge `raiseMessage` overrides.
- The SQL `FLOAT_PRINT` in the sqrt sentence.
- The guard rows these carry.

The edge's sentences move into templates, line for line.

## The proof
- **Step 1:**
  - the probe: each operand type on each database, with NULL and length cases;
  - the shared template fixture in Java, TypeScript and Python;
  - every version pin.
- **Step 2:**
  - the golden net's difference is EMPTY (PCT A on DuckDB, H2 and SQLite; channel B; both corpora; pure/v1);
  - any moved byte stops the line.
- **Lean SQL (condition 8):** the product's raises grow by the operand casts and the edge's shrink. I report the
  lean_sql and capture deltas per database at the end of step 1. That re-pin goes through the user.

## Size
- Step 1 (product: kinds split, marker, reader, format 56, hosts, probe): about 6 hours.
- Step 2 (edge templates and the deletions): about 3 hours after the net.
