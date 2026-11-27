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
| A4 | **"Is this side's order defined?" has a permissive default.** (Corrected on writing the design: the miss of a class query mapped to a list was not the order rule, which answered rightly on the resolved tree; the side had been read as a table.) What remains: a native the rule does not know is taken as ordered, and after the store is resolved lite's internal store-reading natives fall into that default. | Each internal native classified in the rule, a test that fails for one left out, and a matrix test over every way a side is made from a store. |
| A5 | **`hasSameElements` is checked for two types where it is planned**, not where the query is typed; `==` is checked in the type-checker. | Registered with the type rule (`EqualityClasses`), so it is checked where `==` is, and a model's compile check reports it. |
| A6 | **The planning-time refusal of two column types (`HeldKinds`) runs "when no door named a rule"** by its position in the code, which nothing states. | Stated as part of the product's own rule for the call, reached through the same one lookup as A1; no other condition. |
| A7 | **The Java checker's tie rule reads a per-door form** (`HostForms.exactNumbersByValue`), added in step 6 beside the existing per-door forms. | Kept: the door hands the Java checker its forms object already (an existing mechanism, removed with the rest by the edition program's closing guards). Listed so it is a decision, not an accident. |

| A8 | **(review)** The planner still reads the door for older reasons (steps 2 and 3: stamping a test body's code; at least nine read sites), removed only by the edition program's closing guards. | Not this batch. Stated to the user as it is: after step 6 no COMPARISON decision is made by door in the planner, but "the planner knows nothing of editions" is the edition program's end state. **The user's ruling.** |
| A9 | **(review)** The empty-side rule marks the places a value is kept (an open list; a miss is a wrong answer): it misses an aggregate's input, a window column, a list lambda's result, a comparison used as an operand, a let's value. | The mark turned around: conditions are the closed list; every other equality is guarded. A miss costs SQL size, never an answer. Design note 5.3. |

## B. Wrong or missing answers still inside step 6's promise

| # | What | Proposed |
|---|---|---|
| B1 | A comparison of lists, maps or tables whose sides read the row they stand in (a filter comparing each row's list): the database's own comparison, which is wrong for a map's entry order and a table's columns, and for a Decimal list on SQLite. | Right where the database's own list comparison is the rule (typed lists on a database with lists); refused by name everywhere else. Never a wrong answer. Build. |
| B2 | A computed LIST of two types whose computation would raise answers a quiet false (single values are evaluated since today). | **(review)** A value where Pure raises is in AGENTS.md's "never accepted for cost" class. Not an ordinary limit: either built, or the user's explicit exception. **The user's ruling.** |
| B3 | Two sides of two types that may both be empty: the database's comparison (Pure: equal only when both are empty). | Build: the planned form answers it at the root and inside an expression already; make the single-value case take it. |
| B4 | One value read outside and inside a lambda is still two evaluations (`$ks->filter(k \| $k == $ks->at(0))`; Pure true, lite likely false). Not run. | Probe first; build if confirmed. |
| B5 | A caller's lambda loses its author's mark at four desugaring sites (the fold's map unroll and static `if`, the legacy column forms, project's and groupBy's rebuilds). | Build (carry the mark at each). |
| B6 | A generic function's `Number` parameters called with `1` and `1.0`: a compile error where the rule says false. | Record as a limit: the fix retypes every such function's arithmetic. |
| B7 | Two tables whose column names differ are answered false without evaluating either side. | Record (Pure's own answer for two tables is identity, never an evaluation of rows). |
| B8 | The fresh-draw check reads the body, not the lets the environment seeds. | Build (small). |
| B9 | A mixed list holding an Integer and Decimals more than 38 digits apart; `date(2015, 4)` against a date-time; which of two equal exact numbers `max` keeps. | Record as limits. **(review)** The `max` tie needs Pure's own answer read and stated beside lite's, and a register row. |
| B10 | `max`/`min` at legend-engine's door: an Integer against a Decimal column, a Decimal against a Float column keep the winner's type where legend-engine's H2 answers its promoted type. | Record: measured on one database. **(review)** It departs from legend-engine's answer, so it is a register row the user rules, not a note. |
| B11 | The two test checkers' wording for a JSON value read through `Any`: three of four defects fixed in code, not run; `assertSameElements` prints two identical texts without types. | Run the fix in the batch; the fourth recorded. |
| B12 | The two date facts (first day of week, month, quarter, year is a day; a day moved by days stays a day) apply at every door with no register row. The corpus shows no test moved. | Register row; in the landing summary. |
| B13 | The tolerance assert is the database checker's own formula, not the product's. | Record as a cleanup (no answer changes). |
| B14 | The bodies the database checker cannot judge (7, 3, 7 in the measure of `89e51a7dc`) are not read one by one. | Read them from the batch's acceptance measure. |
| B15 | **The acceptance measure still shows verdicts that move between the assert and the plain comparison** (run 2026-11-27 at `89e51a7dc`: 99 bodies differ, from 176): dates and written precision (7 on DuckDB, 7 on H2, 14 on SQLite); number types (3 on H2, 3 on SQLite: `assertEquals([1, 2], [1D, 2.00D])` passes as an assert and fails as a comparison there); one Variant date-time on SQLite; `assertSameElements(['1']->cast(@Any), [1])`; one body `==` refuses for an unordered side where the assert lets it through. Each is a place the two are not yet one equality. | Read every one; build the ones that are the product's (not a database's missing list form); the measure is the batch's acceptance: no verdict moves between the two forms except for a named database limit. |

| B16 | **(review)** Two sides that may BOTH be empty and are not both plain columns (`$p.nick == $p.alias->toUpper()` as a column): no value where Pure says false, or true when both are empty. Ordinary input. | Build: the null-safe equality, each side written once. Design note 5.3. |
| B17 | **(review)** A comparison that reads a query's PARAMETER is planned as the last statement and left to the database inside an `if`. | Build: parameters are readable names of the one entry. Probe first. Design note 5.2. |
| B18 | **(review)** The planning-time refusal of two column types does not apply to WHOLE sides: a list of a Number property held as whole numbers against one held as floats answers false where single values are refused. | Build: the same refusal where whole sides are planned. |
| B19 | **(review)** The compatibility doors share the product's equality code under their number rule, so three answers moved at the doors without being listed: the empty ladder for computed lists, a date-time carried as text compared by its instant, one value against a list. | Read each against upstream (run, not recalled); listed in the landing summary with what moved. |
| B20 | **(review)** Older than step 6, found on the way: a test body's seed queries are typed with lite's setting whatever door admitted the test; `indexOf` has no door row; a keyed sort's ties at a door are lite's; a lowerer made with no planner takes the database's list comparison silently. | The last is built (refused by name). The first three recorded for the edition program's closing steps. |

| B21 | **(second review; wrong today, before step 6)** A filter under `!` over `&&` or `||` drops the row whose comparison has an empty side (`filter(r \| !($r.nick == 'a' \|\| $r.id == 7))`; Pure keeps it), and a `forAll` whose comparison has an empty side answers true where Pure says false. | Build: the empty-side rule's corrected meaning of "a condition" (design 6.1) covers both. Each answer that moves is listed. |
| B22 | **(second review; introduced by step 6)** At the two compatibility doors a date built to the hour equals a full date-time (`date(2015, 4, 16, 14) == %2015-04-16T14:00:00`: true; upstream false, run 2026-11-27; a database error before step 6). | Build: the doors answer upstream's false (design 6.5). Lite's own rule (S399) unchanged. |
| B23 | **(second review)** A sort over store rows on a key that is not unique leaves ties in arrival order: a "sorted" side can still differ between runs. | Record: a register row; the rule cannot see a key's uniqueness. |
| B24 | **(second review)** At the doors `assertNotEquals` is left as written, so a side the order rule newly calls unordered is a new refusal there; six more readers of the order rule move with it. | Listed from the corpus and PCT rounds; each to the user. |

## Probed before the build (RUN 2026-11-27 at `5713d5158`, DuckDB, H2 and SQLite; a temporary test, never committed)

SQLite answered as DuckDB on every row; H2 differed only where it holds no list (refused by name, as before).

| Row | Written | Lite today | Pure | So |
|---|---|---|---|---|
| B2 | `[1, 2]->map(x \| [1, 2]->at($x + 5))->size()` | the database's error (an element past the list's end) | raises | the list does raise when it is evaluated |
| B2 | `x::sameNums(<that list>, [1.5, 2.5])` (parameters declared `Number[*]`); `x::sameAnys(<that list>, ['a', 'b'])` | **false**, quietly | raises | confirmed |
| B2 | `x::among(1.5, <that list>)`; `x::whereIn(1.5, <that list>)` | **false**; **-1**, quietly | raises | confirmed |
| B2, wider | `<that list>->cast(@Number) == [1.5, 2.5]->cast(@Number)` written in the QUERY | **false**, quietly | raises | not only a function's comparison: the planner answers two held number kinds with a constant, evaluating neither side (the first review's B18 is the same place). The build covers both. |
| A9 | `($o.nick == 'a') == false` as a column, `nick` empty | no value | true | confirmed wrong |
| A9 | `($o.nick == 'a')->toString()` as a column; `([1, 2]->filter(x \| $x > 5)->first() == 1)->toString()` | no value | `'false'` | confirmed wrong |
| A9 | a window column `extend(over(~id), ~e: {p, w, r \| $r.nick == 'a'})` | no value | false | confirmed wrong (a plain `extend` answers false) |
| A9 | `groupBy(~[], ~[c: r \| $r.nick == 'a' : y \| $y->count()])` over three rows, one `nick` empty | 2 | 3 | confirmed wrong |
| A9 | `[1, 2]->map(x \| [1]->filter(y \| $y == $x)->first() == 1)` | one element, `[true]` | `[true, false]` | confirmed wrong: an element is lost |
| A9 | `$o.nick == $o.alias` (two plain columns, both may be empty); `->map(o \| $o.nick == 'a')`; a `let` of the comparison | right | | already right |
| B16 | `$o.nick == $o.alias->toUpper()` | a compile error (`toUpper` takes one value) | the same | the first review's example is no program; the row needs another shape, found in the build's tests |
| B17 | `{names: String[*] \| $names == ['a', 'b']}` | refused: "its first side's order is not defined (a store or frame read ...)" | | wrong words: a parameter's order is the caller's. The order rule must know a parameter. |
| B17 | the same inside an `if` | refused: "list parameter 'names' is bound only as a whole list (in, contains)" | | a refusal by name on both paths, never a wrong answer; the one entry gives both the same words |
| 2.1 | `let a = <list>; $a == [1, 2]`, and a `let` of a sorted table compared with itself, as the last statement and inside an `if` | true / `same` on both paths | | the regression cases for the one entry |
| B4 | `let ks = [3, 1, 2]->sort(); $ks->filter(k \| $k == $ks->at(0))`; a comparison of lists inside the lambda | `[1]`; 1 | the same | right |
| B4 | `let g = generateGuid(); $g == $g` | refused by name (drawn afresh, read twice) | true | a named refusal |
| B4 | `let ks = [generateGuid(), generateGuid()]; $ks->filter(k \| $k == $ks->at(0))->size()` | **0** | 1 | confirmed wrong: the fresh-draw check sees a value that IS a draw, not a list HOLDING draws. Build: the same refusal. |

## C. Not yet verified, or housekeeping

| # | What |
|---|---|
| C1 | Not run: the rename to `hasSameElements`; the JSON wording fix. |
| C2 | The time-series bundle budget (the user: "Time series budget fine", to 148,500 from 148,200). Measured at `83cb22ae2`: **148,502** bytes gzipped (148,728 before the comments were shortened), two bytes over the ruled figure. The pin moves to 148,500 and no further: the batch trims the model's comments to fit, and the pin's comment names the ruling and the measured size. |
| C3 | The acceptance measure was re-run at `89e51a7dc`: 99 bodies differ (176 at the start); row B15. Run once more at the batch's end. |
| C6 | The full round at `89e51a7dc`: 89 of 91 targets pass, 1 skipped, 1 the bundle budget (C2). All ten database lanes and the relational corpus on `83cb22ae2`: no test's outcome differs from the base (MariaDB's channel B is the flake it was). A mid-point reading. |
| C4 | **All back on `83cb22ae2`** (a mid-point reading; it predates the batch). The local gate (`//gates:local`, lock file checked): 572 tests, 556 pass, 1 skipped (arm64 only), 15 fail -- 14 only because the container runs as root (all 14 pass run unprivileged), and the bundle budget (C2). The stress suites: DuckDB 5,699 pass and 340 fail of 6,087, H2 5,603 and 436, both identical to the base's. The relational corpus 10 of 10, nothing moved. All ten database lanes: no outcome differs from the base. |
| C5 | The parser ratchet's history line; the differences page and the plain-words page brought to the batch's end state. |

## D. Outside step 6, as already agreed

- Found on the way (2026-11-27, piece 3's run), older than step 6: a class's rows under `exists` or `forAll` as a
  query's own answer (`|p::O.all()->exists(o | $o.nick == 'a')`) is a raw database error on DuckDB ("Invalid LIST
  argument during lambda function binding"), a refusal by name on H2, and answered on SQLite. A task for after the
  batch; the row-level forms inside a filter work and are what the batch's tests use.

- Lists on a database with no list type (H2; partly SQLite, MySQL, MariaDB): the database program.
- SQLite's date-time text: its own slice right after step 6.
- A table's cell count in `assertSize`: step 10. A third date-time type: after Stage B. A Float's text at
  legend-engine's door: step 8.
- A Float COLUMN combined with a Decimal: kept as it is (the user, "Okay keep the float thing then as is").
- The door's table read at the lowering's one lookup (step 5's ruled design) goes with the edition program's closing
  guards, when the type-checker's rewrite replaces it.

## The user's rulings (2026-11-27, their words)

- **1 (A1, A2):** "1 lowerer one lookup yes". The planner asks nothing; the lowering's one lookup decides; one path.
- **2 (A3):** "2 is too hard / complex / not worth doing both upstreams faithfully", then "We've already have two
  different rule sets for upstream right?  Pure and engine?  Can you teach me - Why is #2 different", then "Is it
  very expensive to be faithful to both". The author's first reading (one answer everywhere, because matching both
  is "the per-door switch this batch removes") was WRONG and is withdrawn: what the batch removes is the planner
  guessing the door; a yes/no on the door's own table is step 5's ruled design, and it is already built. Faithful to
  both costs one line more (legend-engine's door answers false outside a database query) and two test cases. The
  expensive part is the rule's holes (row A9), the same under either answer. **Awaiting the user's word.**
- **2 (A3, A9), ruled:** "1 okay" (to: faithful to both doors, with the mark turned around). The door's table keeps
  its yes/no; legend-engine's door answers false outside a database query; conditions are the closed list.
- **A8, ruled:** "2 okay" (to: the planner's older reads of the door go at the end of the edition program, not in
  step 6).
- **3 (A7), ruled:** "Java checker seems ok for now".
- **4 (B1), ruled:** "per row refused okay".
- **A4 (the order rule), the user's direction:** "Weren't we checking this for compatibility already?", "shouldn't
  it match what assertEqual means for same thing? Obviously fix the bugs but maybe okay if sometimes says \"use
  hasSameElements\" when it doesn't know?" -- one rule for `==` and `assertEquals` (it is: one function); where the
  rule does not know, it refuses, naming the fixes. Design 5.4, revised: the fallback alone changes; nothing is
  classified wholesale.
- **The handoff of steps 8 and 9 to another account:** "Wait on handoff".
- **B2, ruled:** "3 size before we decide", then, on the sizing and the probe: "Build 3". Built in the batch.
- **5 (B6, B7, B9, B10, B13):** "other 5 do you suggest to keep for now" -- the author's answer: yes, all five, with
  a register row for B9's `max` tie and for B10, written in the batch and shown in the landing summary. Put to the
  user twice with no objection; the two register rows come back to them in the landing summary for their word.

### B2, sized (read in the code, not run)

- **What it is.** A function whose author declared its inputs loosely (`Any`, `Number`, the abstract `Date`), called
  with two different types, where `==`, `in`, `contains` or `indexOf` has a LIST side that is a computation which can
  fail. Pure computes both sides first, so the failure raises. Lite sees "two types, never equal" and answers false
  (or -1) without computing the list (`LiteCrossTypeRule.atTheCall`: the constant where `manyValued`). Single values
  are evaluated already.
- **The build.**
  1. The type-checker stops taking the shortcut for a list side that computes (about 5 lines).
  2. A list side standing on its own: the planner builds each side as rows, as it does for a whole-side comparison,
     and one new small statement form reads every element of each side and answers false (the single-value form's
     marked-text trick, one aggregate a side, each side written once). About 100 lines across `SideEqualities`,
     `RootComparison` and `Lowerer.runTimeComparison`, through the one entry the batch builds anyway (design 2.1).
  3. A list side that reads the row it stands in, or whose elements have no text form (class instances): refused by
     name, the refusal ruled for B1. Never a quiet false.
  4. Tests: five cases in `RunTimeComparisonTest` on the in-process databases.
- **Cost.** Two to three hours in the batch; no SQL changes outside this exact shape; covered by the batch's one
  verification. Risks: a corpus or PCT body that takes the quiet false today over a per-row list would become a named
  refusal (the verification shows any); `Lowerer`'s method-length guard is tight.
- **The alternative.** The user's explicit exception to "never accepted for cost: a NULL where Pure raises", a
  register row, no work.
- **Recommended: build.**

## What the user rules

1. A1 and A2: the lowering's one lookup decides, the planner asks nothing, one path. (Recommended.)
2. A3: Pure's answer for an empty side at legend-engine's door too, as a recorded difference. (Recommended, after
   legend-engine's own answer is run.)
3. A7: the Java checker's per-door form stays until the edition program's closing guards. (Recommended.)
4. B1: refuse by name where the database's own list comparison is not the rule. (Recommended.)
5. B2, B6, B7, B9, B10, B13 as recorded limits. (Recommended.)
