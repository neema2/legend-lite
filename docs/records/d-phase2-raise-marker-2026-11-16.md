# D, Phase 2: typed raises with operands (design note, 2026-11-16)

The edge program (`docs/records/total-wall-edge-program-2026-11-16.md`, branch `claude/total-wall-orch`), Phase 2,
table row 9. **Not built: sent to the orchestrator and to E-Java (who uses it for `SideShapes`) before any code.**

## The goal
Every raise that carries a value raises a **marker**: its kind plus its operands as data. The product words it
(`ErrorKind`) in Java, and so does the edge (`edge.PureMessages`). No sentence is built in SQL, by the product or by
the edge.

## Today (d-stage @ 97c178a44, measured)
- A raise is `SqlExpr.RaiseText(kind, operands, asData, where)`, built at 43 sites:
  - 26 in `lowering/` (`Scalars` 9, `DateCtorRule` 5, `Coercions` 3, nine others with 1);
  - 16 in the SQL layer (`AnsiSqlRenderer` 4, SQLite 5, MariaDB 4, Postgres, ClickHouse, BigQuery and `FrameGuard`
    1 each);
  - `SqlRewriter` 1.
- **The product** writes `MARK kind [MARK where]` (`AnsiSqlRenderer.raiseMessage`, the one writer of the mark).
  `where` is fixed at planning. Any operand computed in the query is **dropped**: a division's dividend, `at`'s offset
  and size, a date part's date, `toOne`'s size.
- **The edge** writes the whole upstream sentence **in SQL**: 13 `raiseMessage` overrides (`PureText`, `EngineStyleH2`)
  calling `PureMessages.sentence`, which is 25 kinds of `CONCAT`/`CAST`. `SQRT_OF_NEGATIVE` even prints a Float
  with `FLOAT_PRINT` in SQL. An edge plan declares no raises, because its text is the sentence.
- The run decodes the error in `exec.RaisedErrors` (`kindOf`, `where`) into `DataError(kind, where)`.
  `PlanJson.raises` maps each kind to the product's sentence, for the hosts (TS, Python).

## The shape (proposed)
1. **Text in SQL:** `MARK kind (MARK operand)*`, one `MARK` (U+001C) before each operand, in the order the kind
   declares. A fixed operand is its text, written at planning. A computed one is the database's plain cast to text
   (`str`, `CAST(x AS VARCHAR)`). The operand is carried as text to be read back typed, never printed in a form.
   `where` becomes operand 0 where it is fixed, so today's `placed` sites are unchanged.
2. **Declared operand types:** `RaiseKind` gains each kind's operand list, with its Pure type
   (e.g. `INDEX_OUT_OF_BOUNDS(Integer offset, Integer size)`, `SIZE_NOT_MULTIPLICITY(String bound, Integer size)`).
   The plan's `raises` map carries them, so a host reads the operands typed. **Plan format 56 (a bump).**
3. **Decode:** `RaisedErrors` reads the kind and splits the operands. Each is read back to its declared type
   (Integer, Float, Decimal, String, Date or DateTime: the same readers the result decode uses), giving
   `DataError(kind, List<Object> operands)`. One reader, for exec and the hosts.
4. **Words, Java only:**
   - the product's `ErrorKind.sentence(operands)`, its template `%1$s`;
   - the edge's `PureMessages.raised(kind, operands)`, with legend-pure's sentences moved from `PureMessages.sentence`
     line for line, a Float operand printed by the Phase 3 presenter.
   The edge's `raiseMessage` overrides are deleted, so an edge plan raises the same marker as the product's and
   declares its raises.
5. **Where the edge words it:**
   - pure/v1's error answer: `PureMessages.compile`, extended to a `DataError`;
   - PCT and the corpus: the judges' failure text, through the same function, handed in as the hook already hands
     `PureMessages::compile`.
   **Open (a new mechanism?):** the judges' road needs the edge's words for a `DataError`. Proposed: a third member
   on the existing `PlanStyle` facet list (`testplan.PlanStyleFacets.KINDS`), not a new hook.

## The exact-carriage caveat (Phase 8's rule, §4.2)
A computed Float operand must read back exactly. **Snowflake's double-to-text cast writes ten digits** (the plan's
row 20), and whether each other database's cast round-trips is not yet measured. Only `SQRT_OF_NEGATIVE` has a Float
operand today. Before building, the probe measures it on every database. Where the cast does not round-trip, the
operand carries an exact form in one call, or waits for Phase 8's carrier: the orchestrator's choice.

## What gets deleted
- `PureMessages.sentence` (the SQL builder) and its helpers `cat`, `str`, `parts`, `dateComponent`'s SQL half.
- The 13 edge `raiseMessage` overrides.
- The SQL `FLOAT_PRINT` inside the sqrt sentence.
- The guard rows these carry (`CompatSpellingGuardTest` K1/K3 literals in the edge's SQL path).

## How the golden net proves the edge didn't move
The net (Phase 0, E-SQL) records PCT A on DuckDB, H2 and SQLite, channel B on DuckDB, both corpora and pure/v1. Every
assertError golden compares upstream's sentence, which is now Java's instead of SQL's: **the net's difference must be
empty.** Any byte that moves (a Float operand's digits, say) stops the line and goes to the orchestrator. The product
probe (`//core:product_raise_probe`) adds an operand case per database: `at` out of bounds names the offset and the
size.

## Size and order
About 6 hours:
1. `RaiseKind` operand types, the marker writer, and the `RaisedErrors` reader, with the plan bump (product first).
2. The edge's Java words, and the overrides deleted.
3. The judges' facet.
4. Net and probe.
Each step is green before the next.
