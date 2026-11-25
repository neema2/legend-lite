# E3's homework: the texts and parses an edition must reach, measured (B2, 2026-11-25)

Research for the independent review's must-fix 4 (`independent-review-2026-11-25.md` §6, its MUST-FIX 4; the START HERE's §8.4) and
for the user's decision 5 ("what is `pure`?"). **It is not a start on E3**: the edition is paused by the user, the
probes below were temporary edits (never committed), and no pin, ledger or roster moved. Every move is data, per
audience and per database. Nothing here recommends an answer to decision 5.

The code is read at `06e3d56b0` (B2's head); the design at `claude/compat-b3-nufdlk` `7985a39c7`.

## 1. Where a value's text comes from today

The edge's SQL passes (`edge/PurePrintForms.passes`, every edge dialect's `spellingPasses`) rewrite three SQL nodes and
one cast after the lowering has written them, whichever construct wrote them; and each of the twelve edge dialects
overrides `floatToText` (`PureText.java:292-532`) to write legend-engine's cast for a `FLOAT_TO_TEXT` no pass reached,
so taking the passes off alone leaves a Float's text as it is (the first probe of §4 learned this the hard way). The
table is every place the lowering writes each node, and the typed call that reaches it.

| SQL node | Written by (lowering) | The typed call that reaches it | A typer row sees it? |
|---|---|---|---|
| `FLOAT_TO_TEXT` | `Scalars` toString rule (`Scalars.java:2444`) | `toString(x)`, `x` a Float | yes, at the call |
| | `Scalars.pureToString`'s Float arm (`:2937`) | `format`'s `%s` over a number (`:3619`); a mixed list's element (`TaggedLists.java:585`, `Scalars.java:1013`); a `Pair`'s parts (its own recursion); the edge's `%r` (`ReprParts`, `EdgePureOnlyTexts`) | at `format` / `makeString` / the `Pair` class's `toString`, not at the element |
| | `format`'s `%f` (`Scalars.java:3563`) | `format(fmt, args)`, a literal `%f` | at `format` |
| | `ProductRelationText` (`:369`) | a relation's `toString` / `toCSV` / a TDS's `csv` used further (the product's style always) | at the relation's print |
| `DATETIME_TO_TEXT` | the style's `DateTimeTexts` two-form (`Scalars.java:2436`, `:3685-3693`; `Styled`) | `toString(x)`, `x` a DateTime; `format`'s `%s` / `%t` over a DateTime | yes / at `format` |
| | `ProductRelationText` (`:366`) | a relation's text used further | at the relation's print |
| `CELL_TEXT` | `makeString`'s element (`Scalars.java:1025`) | `makeString` / `joinStrings` over a list | at the join |
| | the row-cell join (`Lowerer.java:4280`, `tc.rowCells()`) | `$r.values->makeString(...)` (a collection the lowering recognizes as a row's cells) | at the join; "row cells" is decided in the lowering |
| | the `cellText` native (`Coercions.java:166`) | a TDS cell's `toString`, ALREADY an explicit call minted by the typer (`compiler/spec/TdsDesugars.java:162-167`) | yes |
| `PARSE_INT`, the VARCHAR->DOUBLE `USER` cast (F31) | the parse natives' lowering | `parseInteger(s)`, `parseFloat(s)` | yes, at the call |

**Not through any of these nodes, so neither today's pass nor a `toString` row reaches it:** `makeString` /
`joinStrings` over a homogeneous list writes each element with the database's own cast
(`PureSql.elementText`, `PureSql.java:47-62`: `CAST(x AS VARCHAR)`), on every door alike. A Float list joined is the
database's text of a double there, not S311's six decimals (a product question noted in §7, never measured as a
difference).

**Folded before any row:** `StaticFold` (inside normalized bodies, `Overloads.java:406`) folds a constant Float's
`toString` with Java's `String.valueOf` (`StaticFold.java:624-628`: `1.0E-7`, `0.30000000000000004`), and `==` with Java
`equals` (`:608-611`, `:877-890`). That text is neither S311's, nor legend-pure's print, nor legend-engine's cast.

## 2. Generic bodies and format: what a row must be to reach them

- **Typed bodies live one query.** `TypedQuery` makes a new `SpecCompiler` per query (`TypedQuery.java:36`); its memo is
  per compiler (`SpecCompiler.java:41-48`). No other cache holds typed bodies or plans (`cache/HandleStore` holds
  sessions; no `ContentStore` is used). The prelude's bodies (`Pair.toString`, `List.toString`, `prelude.pure:20-36`)
  and normalized bodies are typed inside the query's compiler. What outlives a query is the MODEL (`HeldModels`, keyed
  by text): if the edition is a field of the model, a held model must be keyed by edition too (the review's must-fix
  5). Nothing else would serve one edition's typed body to another.
- **A row keyed on the call, not on the operand's type, reaches a generic body.** The prelude's `$value->toString()` is
  typed once per query with `T` unknown. A row that maps EVERY `toString` (any operand type) to one explicit typed
  call per edition (`engineText(x)`, `pureText(x)`; lite's own `toString`) is applied there with `T` unknown, and the
  lowering resolves the operand's type after inlining, as it does today for `toString`. The same holds for `format` (a
  row over `format` itself; `%f`/`%s`'s operand types are read in the lowering, `Scalars.java:3545-3625`) and for
  `makeString`/`joinStrings`. This is the shape of the review's alternative (a) for `substring`, without its placement
  question: a text does not depend on where it runs.
- **Where it breaks:**
  - `StaticFold` runs before the row, in normalized bodies: a constant Float's `toString` is folded by Java first.
    Either the fold leaves `toString` (and `==`) to the row, or the row is applied inside the fold.
  - `LiteralUnroll` at G½ folds literal-only comparisons after the typer (the review §3 item 1): not a text, but the
    same "after the row" gap.
  - The row-cell join is recognized in the lowering (`tc.rowCells()`), so `TDSNull`'s row needs the typer to mint an
    explicit call for "a row's values as text" first (the design's E3 precondition). A TDS cell's own `toString` is
    already explicit (`cellText`, `TdsDesugars`).
  - A Float literal is no call: its kind (DECIMAL on five dialects, `PureText.java:263, 310, 333, 370, 481`) is the
    dialect's, and H2's literal processor (`PRINT_AS_FLOAT`) prints a literal under `toString` (the review §3 item 1).

## 3. What each audience gets today (read from the planned SQL, 2026-11-25)

A temporary test (never committed) printed each statement at legend-pure's door (`PureText.UPSTREAM`, which PCT's
channel A and B and the relational corpus plan under) and at the product (`PlanStyle.PRODUCT`) on DuckDB, with the
answers the PCT route gives:

| Expression | legend-pure's door today (SQL, answer) | lite's product (answer) | legend-pure 5.99.0 |
|---|---|---|---|
| `1.3421e-8->toString()` | `CAST(0.000000013421 AS VARCHAR)` (the literal processor's bare print), `0.000000013421` | `0.0` (S311) | `0.000000013421` |
| `(1.3421e-8 * 1.0)->toString()` | `CAST(1.3421E-8 * 1.0 AS VARCHAR)`, `1.3421e-08` (DuckDB's text of a double) | `0.0` | `0.000000013421` |
| `format('%f\|%s', [1.0E-7, 2.5])` | `printf('%s\|%s', CAST(0.0000001 AS VARCHAR), CAST(2.5 AS VARCHAR))`, `0.0000001\|2.5` | `0.0\|2.5` | (not read) |
| `[1.5, 1.0E-7]->makeString(',')` | `list_transform(..., x -> CAST(x AS VARCHAR))`, `1.5,1e-07` | the same SQL, `1.5,1e-07` | `1.5,0.0000001` |
| `%2014-01-01T10:20:30.5->toString()` | folded at lowering (`EdgeDateTimeTexts`), `2014-01-01T10:20:30.5+0000` | `2014-01-01T10:20:30.500000000Z` (S312) | `2014-01-01T10:20:30.5+0000` |
| `'abc'->parseInteger()` | `CAST('abc' AS BIGINT)` (F31), the database's error | the guarded parse, `Could not convert string 'abc' to INT64` (TEXT_NOT_INTEGER) | legend-pure's error |

So today PCT's door is not legend-pure's text either: a literal Float prints legend-pure's text by the literal
processor, a COMPUTED Float prints the database's text of a double (an exponent on DuckDB), and a list's elements print
the database's text on every door.

## 4. E3's moves per audience, measured

Two temporary probes (never committed) took each of the edge's texts off legend-pure's door (`PureText.UPSTREAM`) and
ran PCT's channel A on DuckDB, SQLite and H2, channel B (all its lanes), and the edge contract's goldens
(`//pct:edge_net`: channel A's answers and outcomes per database, channel B's outcomes, the pure/v1 and edge-case
goldens). Each answers one reading of decision 5 for PCT's door; neither is a recommendation.

- **P-lite: `pure` takes lite's rules for these texts (the design's §3 as written).** The edge's passes off (Float,
  DateTime, `TDSNull`, F31), each edge dialect's `floatToText` override and `UPSTREAM`'s DateTime two-form
  (`EdgeDateTimeTexts`) back to the product's.
- **P-pure: `pure` takes legend-pure's texts where lite can compute them.** A Float's text is lite's SQL spelling of
  legend-pure's print, `FLOAT_SHORTEST_TEXT` (in the pass and in the twelve dialect overrides); the DateTime two-form
  and its pass kept as today (legend-pure's print); `TDSNull` and F31 off.

**PCT (legend-pure's door), per database:**

| | DuckDB channel A | DuckDB channel B | SQLite channel A | SQLite channel B | H2 (A, B) |
|---|---|---|---|---|---|
| P-lite | 6 PASS -> FAIL | 6 PASS -> ERROR | 6 PASS -> FAIL | 6 PASS -> ERROR | 0 (its lanes run the Relation suite alone) |
| P-pure | 0 | 0 | 0 | 0 | 0 |

The six, the same on both databases and both channels:
- a DateTime's text, 5: `toString::testDateTimeToString` (expected `2014-01-01T00:00:00.000+0000`, P-lite
  `2014-01-01T00:00:00.000000000Z`), `testDateTimeWithTimezoneToString` (`...00.0000+0000` against the same),
  `testSimpleDateToString` (`2014-01-02T01:54:27.352+0000` against `...27.352000000Z`), `format::testSimpleFormatDate`
  (`%s` of a date), `format::testFormatRepr` (`%r` of a date, `%2014-01-01T00:00:00.000+0000`);
- a Float's text, 1: `toString::testFloatToStringWithNegativeExponent` (expected `0.000000013421`, P-lite `0.0`).

`TDSNull` and F31 off moved no PCT case in either probe (nor in S6's and S6c's). P-pure changes one text no PCT case
covers: a computed Float (`(1.3421e-8 * 1.0)->toString()`) moves from DuckDB's `1.3421e-08` to legend-pure's
`0.000000013421`.

**legend-engine's door (the corpus on DuckDB and H2, pure/v1, the engine lanes).** E3 as designed gives `engine` the
forms it has today, so it moves nothing if its rows reach every place the passes reach (§1, §2). What the passes carry
there, measured by taking them off (S6, S6c; B2's record):
- `TDSNull`: 112 corpus tests on DuckDB, 107 on H2. By shape on DuckDB: 76 a row's cells joined, 10 a cell's
  `toString`, 8 both, 16 others. A cell's `toString` is already explicit (`cellText`); a row's cells joined is
  recognized in the lowering (§2), so it needs the typer's explicit call first.
- a Float's text: 2 per corpus lane (the validation showcase: an integer-held Float in a constraint message used
  further, legend-engine `1000000`, lite `1000000.0`), a direct `toString` in a constraint body; and on the engine
  lanes, H2's two exponent cases.
- F31: one pure/v1 golden (`raise.parseInteger` on H2), a store-bound parse; plus `InterimEdgeSitesTest`'s computed
  sites 4 -> 5 if F31 went.
- In both probes the pure/v1 goldens moved by F31 alone: no pure/v1 golden prints a Float or a DateTime.

**lite's own projects:** no move (the product's style is untouched by every row).

## 5. A DateTime row

P-lite measured what the design's §3 leaves out: five of PCT's six moves are a DateTime's text, written today by the
lowering's two-form (`EdgeDateTimeTexts`, a literal at its written precision) and the pass's `DATETIME_TO_TEXT` arm
(`PureDateTimePrint`). Either a DateTime row keeps them (legend-pure's print under `pure`; under `engine` the shared
pass writes legend-pure's print today too, and what legend-engine itself answers for a DateTime's text inside a store
query has not been read or asked), or the move is admitted: 5 cases on each of DuckDB and SQLite, in both channels.

## 6. The SQL each option writes (DuckDB, the probe's statements)

| Expression | today at legend-pure's door | P-lite (lite's six decimals) | P-pure (`FLOAT_SHORTEST_TEXT`) |
|---|---|---|---|
| `1.3421e-8->toString()` | 47 characters (`CAST(0.000000013421 AS VARCHAR)`) | 183 | more than 490 |
| `(0.1 + 0.2)->toString()` | 42 | 199 | more than 490 |
| `format('%f\|%s', [1.0E-7, 2.5])` | 81 | 362 | more than 490 |

`FLOAT_SHORTEST_TEXT`'s portable spelling reads the cast some nine times per value (`dialect/FloatShortestText`); the
probe printed 500 characters of each statement. The lean-SQL pins would move either way except under today's cast.

## 7. Noticed on the way (not E3's; recorded, nothing changed)

- **The product writes two Float texts.** `toString` is S311's six decimals; `makeString`/`joinStrings` over a
  homogeneous Float list writes the database's cast (`[1.5, 1.0E-7]->makeString(',')` is `1.5,1e-07`), and so does
  every door. No register row states it.
- **`StaticFold`'s Java text** of a constant Float in a normalized body (§1) is a third spelling.
- **An abstract `Date`** (`adjust`'s result) prints the database's own timestamp text at every door
  (`2014-01-01 10:20:30.5`): S312's recorded G2 gap.
- **The design's "Pure's exact print has no database equivalent"** is not quite so: `FLOAT_SHORTEST_TEXT` is
  `result.CellText.carriedFloat`'s text inside its windows (whole values below 1e38, fractions from 1e-17 below 2^53),
  which equals legend-pure's `DecimalFormat` print except where that format's digits are not the shortest (`1e23`:
  `99999999999999990000000.0` against `100000000000000000000000.0`) and outside the windows.

## 8. What the design amendment needs from E3 (for B3)

- **The rows:** Float text, a DateTime text (§5), the empty cell's text (`TDSNull`) and the parses, each over the call
  that produces the text (`toString`, `format`, `makeString`/`joinStrings`, a relation's print; any operand type), so
  that the prelude's generic bodies and `format`'s operands are reached (§2). A row per type would miss them.
- **The preconditions:** an explicit typed call for a row's cells as text (`TDSNull`); `StaticFold` leaving a
  `toString` it would fold to the row; a held model keyed by edition (E1).
- **The moves:** under P-lite's reading of `pure`, 6 PCT cases on DuckDB and on SQLite in each channel (5 DateTime, 1
  Float); under P-pure's, none at PCT. `engine` and `lite`: none, given the rows above.
- **Must-fix 7, asked the same day:** legend-engine answers `1 == 1.0` `true` in an in-memory M2M transform and joins an
  in-memory XStore association across an Integer and a Float, so its in-memory code is not Pure's `false`
  (`engine-m2m-xstore-equality-2026-11-25.md`, beside this file).
