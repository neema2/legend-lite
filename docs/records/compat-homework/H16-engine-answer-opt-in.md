# H16: the engine-answer opt-in, sized (2026-11-19)

B1-R (track O), on the compat owner's ask of ~19:06 UTC, which relays the user's word: "Yes size the engine-answer
opt-in please". A sizing only: nothing is built, no PR is opened, and no pin, ceiling, ledger or allowlist moves. The
measurements ran in a scratch worktree that is never pushed. Its diff is kept here as evidence (`h16/`).

**The opt-in:** a third value of the detector's setting, `LEGEND_LITE_PURE_MEANING=engine`, beside `warn` and
`refuse`. It applies at the legend-engine door only. Before planning, it rewrites the request's program so that each
store-bound String `substring` or `indexOf` means what legend-engine meant. The core keeps one meaning, Pure's, and
the rewrite yields an ordinary Pure program.

## The answer, on one page

1. **It reproduces every answer legend-engine gives on the batteries:** H2 46 of 46, DuckDB 47 of 47. That takes H5's
   typed use-site translator plus two DuckDB library rules this note adds (negative length, and a literal's negative
   start). With H5's library unchanged it was H2 46, DuckDB 43. The battery's other requests are legend-engine's
   refusals: its 3-argument `indexOf` on both databases, and a list `indexOf` on H2 that both sides fail.
2. **The corpus:** on every answer, the use-site rewrite equals today's core flag (H5's V3, measured: H2 2,285
   against the flag's 2,289, DuckDB 2,447 against 2,452).
   - 4 of the gap, on both databases, judge legend-engine's plan or SQL text alone (CP-4: not promised). They pass
     today only through the engine-style printer, which A2 deletes, so after A2 the flag loses them too.
   - DuckDB's fifth is a host-side assert that only the old meaning made vacuous; it now fails on the product's
     cross-kind `!=` defect.
3. **Cost:** nothing unless the setting is `engine` and the detector flagged the request. Then about 1 to 2 ms for the
   rewrite and one more typing of the query (about 5 to 7 ms on the battery's model). The model with the compatibility
   library is built once per model and held: 3.4 to 4.1 s on the corpus world (H5), far less on a real project.
4. **Size on top of the detector:** about 2 to 2.5 days.
   - About 300 to 350 lines of edge code, the two libraries as edge resources (about 85 Pure lines), and about 250
     lines of tests.
   - One more funnel-register row, and two new battery pins (new files, not moves).
   - **No core hook.**
5. **The rules of record: it holds rules 1 to 4 and tenets 1, 2, 5 and 6, but it is a translator at the door.** That
   reverses D1's "Pure's way, LOUD, no translator" and the final plan's §2 part 2 ("The door ... does not translate
   programs") for requests that opt in. So it needs the user's explicit word amending both. It is posted here, not
   assumed.

## 1. The batteries: how many of legend-engine's 49 pinned answers it reproduces

The batteries: H3's 25 `substring`/`indexOf` requests over the trades seed's `desk` column (`col.`), and the same 24
over a literal (`lit.`). legend-engine 4.145.0's answers are pinned at
`tools/engine-runner/src/test/resources/batteries/engine_{h2,duckdb}_answers.tsv`.

**The harness** (`h16/harness.patch.txt`; it is #124's head, f832a6671, plus the following):
- H5's core change: `Scalars.java`, so `substring` and `indexOf` mean Pure's (H5's `95ec86746`, applied unchanged).
- H5's `EngineTranslator` (its delta here is `h16/engine-translator-h16-delta.patch.txt`), with one addition:
  `translateQueryAt`, the typed retarget over a request's query.
- The batteries' per-push runner (`LiteBatteries`), with `--translate=<library>`. For each request it:
  1. types the query against the model plus the library;
  2. collects the String-overload call positions (`collectStringCalls`);
  3. renames only those calls to their `meta::legend::compat::engine::` twins;
  4. sends the rewritten lambda through the same pure/v1 execute route (`PureV1Routes.execute` over
     `EdgeServer.runner`).
- Compared with legend-engine's pinned answers by the batteries' own comparison (`Batteries compare`).

| lite | H2 | DuckDB |
|---|---|---|
| today (#124: SQL's meaning in the core, no rewrite; `pct/batteries/agreement_*.tsv`) | 46 agree, 2 differ, 1 both fail | 31 agree, 18 differ |
| Pure's meaning in the core, no rewrite (after A3, with `warn` or `refuse`) | 16 agree, 32 differ, 1 both fail | 10 agree, 39 differ |
| Pure's core + H5's rewrite, H5's library as it is | 46 agree, 2 differ, 1 both fail | 43 agree, 6 differ |
| **Pure's core + H5's rewrite, with H16's two DuckDB rules** | **46 agree, 2 differ, 1 both fail** | **47 agree, 2 differ** |

The rows are in `h16/agreement-*.tsv`. The rewrite renamed 50 calls (28 `substring/3`, 12 `substring/2`, 10
`indexOf/2`). It kept the list `indexOf` (`col.i05`, by its typed overload) and the two 3-argument `indexOf` calls.

**What each miss needs:**

| miss | requests | legend-engine | lite with the rewrite | what it needs |
|---|---|---|---|---|
| 3-argument `indexOf` | `col.i04`, `lit.i04` (both databases) | refuses ("No SQL translation") | answers, in Pure's meaning | nothing a rewrite can give. E-R2 rules that the legend-engine route answers everything lite can; under `engine` it could refuse instead, but only by a ruling |
| the list `indexOf` | `col.i05` (H2) | refuses ("couldn't find dynafunction") | fails | both fail; not a rewrite question |
| a negative length on DuckDB | `col.s17`, `lit.s17` (`substring(2, -1)`) | `'C'`: DuckDB counts a negative length backwards | `''` with H5's library ("a negative length is not reproduced") | **one rule**, measured: the `\|b\|` characters before the start position. With it, both agree |
| a literal's negative start on DuckDB | `lit.s12`, `lit.s13` (`'Credit'->substring(-7, 2)`, `(-8, 2)`) | `'C'` and `''`: DuckDB's constant folder keeps the window where its runtime raises the position to 1 (H3 §3) | `'Cr'` twice | **a literal's twin**, `substringConst`, chosen when the string argument is a literal: the window not raised. With it, both agree. The column forms already agree |

The other kinds the owner named all agree already, on both databases:
- **the literal-0 quirks:** H2 reads start 0 as position 1; DuckDB's 3-argument form drops a character;
- **not-found:** `indexOf` answers 0, which Pure's -1 plus 1 gives;
- **negative starts** over a column.

**Not covered by these 49:** a `substring` whose arguments are other expressions over the row (positions computed
from `length()` agree: `col.s07`), and databases other than H2 and DuckDB. On another database `engine` would
refuse by name: its library is not written or measured.

## 2. The corpus with it on

H5 measured this with its translator at the corpus door (`docs/records/compat-homework/h5/corpus/` on
`claude/compat-translator-spike`):

| | H2 | DuckDB |
|---|---|---|
| the core flag (today's main, V0) | 2,289 pass | 2,452 pass |
| nothing (Pure's core, no rewrite, V2) | 2,281 | 2,443 |
| **the opt-in's rewrite: Pure's core, use-site (store-bound code only), V3** | **2,285** | **2,447** |
| for comparison: H5's typed definition-site rewrite (it also rewrites host-side code), V6 | 2,285 | 2,448 |

The opt-in is the use-site rewrite. legend-engine gives these functions the database's meaning only in code it sends
to the store (H5's F1, confirmed above for a store-free `let`), so V3 is the measurement.

**The tests the flag passes and the use-site rewrite does not: 4 on H2, 5 on DuckDB.**
- **4 on both databases.** Each judges legend-engine's plan or SQL text only, never an answer, and each is in B2's
  CP-4 list (classes a1 and a2: no replay the appeal can run):
  - `meta::pure::executionPlan::tests::testModelConnectionAgg`
  - `meta::pure::executionPlan::tests::testModelConnectionMultipleAgg`
  - `meta::pure::executionPlan::tests::testSubstringIndexingCorrectedByFeatureFlag`
  - `meta::relational::tests::functions::sqlstring::testToSQLStringWithPosition`

  They pass at main only because the edge's engine-style printer reproduces legend-engine's text. A2 deletes that
  printer (CP-4), and with it these 4 under the flag as well.
- **1 more on DuckDB: `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeVariableESTTimeZone`.** It
  passed at main vacuously (H5 F4).
  - Its host-side assert `$planAsString->indexOf('...') != -1 || ...` was always true while `indexOf` answered 0 on
    a miss.
  - Under Pure's meaning the left side is a real check, and false. The right side then compares a String with an
    Integer (legend-engine's own typo), which DuckDB refuses with a conversion error.
  - That is the product's cross-kind `!=` defect, not the rewrite's: the code is host-side and correctly not
    rewritten.
  - V6 passes it only because it rewrites host-side code too, which legend-engine does not do.

So **on answers the use-site rewrite equals the flag**. The one remaining difference is a host-side assert that only
the old meaning made vacuous.

## 3. Its cost per request, and keeping it off every other request's path

- **Off the path by construction.** `warn` and `refuse` never rewrite. Under `engine`, a request the detector did not
  flag runs exactly as today. The detector's typing of the query, which it does anyway, is the rewrite's input.
- **Per flagged request**, measured on the batteries (49 requests, warm JVM):
  - the typing plus the rewrite: 340 to 353 ms in all, about 7 ms per request;
  - the rewrite's own walk: 55 to 87 ms in all, 1 to 2 ms per request;
  - then the route types the rewritten query once more, about the same as the first typing.
- **The model with the library**, model text plus the target's library text, is a second model. `HeldModels` is keyed
  by text, so it is built once per (model, target) and held beside the original:
  - 3.4 to 4.1 s on the corpus world's 11,168 elements (H5's `buildModule`), far less on a real project;
  - its memory is the model's again, for models that ever get a flagged request.
- **Kept off the first request's path, optionally:** the load-time report already types the model's bodies on its
  background thread. When it finds a flagged site under `engine`, it can build the extended model there too, so the
  first flagged request usually finds it held. If it is not ready, the request waits for it: no fallback to Pure's
  answer.
- **The reach costs more.** User functions the query calls carry flagged calls in their own bodies. They are cloned
  under `meta::legend::compat::engine::clone::` with those calls retargeted, so a host-side caller keeps Pure's
  meaning. The clones join the extended model: still one build per (model, target), since every function with a
  flagged call is cloned once.

## 4. Its size on top of the detector

| part | where | lines |
|---|---|---|
| the rewrite: rename the detector's flagged sites in the request's lambda, by position and by the typed callee's arity. It has no name comparisons, so `IdentityGuardrailTest` does not move | `edge/PureMeaningRewrite.java` (new) | about 150 (H5's record-rebuilding map and rename, of its 584 lines) |
| user-function clones, for the query's reach | the same file | about 100 |
| the libraries, H2 and DuckDB, with H16's two rules | edge resources | about 85 Pure lines |
| `engine` as the setting's third value, the extended model held, and the header under `engine` naming what was rewritten | `EdgeServer`, `PureV1Routes`, `TestDataRoutes` | about 50 |
| error words: a failure inside a rewritten call names the compatibility twin today (H5 (c)), so the edge's words map it back to the user's name | the edge's error words | about 30 |
| tests: the batteries' per-push half under `engine` (pins `agreement_engine_{h2,duckdb}.tsv`: H2 46/2/1, DuckDB 47/2); a host-side `let` not rewritten; the corrected option keeping `substring` as Pure's; a reached user function cloned, with a host-side caller kept on Pure's; an unverified site and a mapping transform refused under `engine`; another database refused by name | `PureMeaningRewriteTest`, the batteries | about 250 |

**What it would not cover in its first form, refused under `engine` by name (still warned under `warn`):**
- qualified properties: a property read is not a call the rewrite can redirect without rewriting the access;
- mapping transforms: an M2M mapping chained over a relational store reaches SQL in legend-engine for a TDS query
  (`core/store/m2m/storeContract.pure:89-91`, `chain.pure:111-116`; `executionPlanTest.pure:2190`'s expected
  `substring("root".fullname, 0, locate(' ', ...))`), but for a graphFetch it runs in Java, so with Pure's meaning;
  the same holds for a `Relation` (`~func`) class mapping's expressions;
- the 3-argument `indexOf`, unless the user rules that `engine` refuses it.

**Guard moves on top of the detector's:** one funnel-register row (`PureMeaningRewrite.java`), and two new battery
pins (new files). Nothing else, by design:
- the library is an edge resource;
- the rewrite catches only the typing exception;
- no static state: the extended models are held by the server's mount.

**A core hook: none.** The rewritten program is ordinary Pure, the library is model source, and both reach the one
compiler through its public doors (`Compiler.query`, the held models). This was checked by building the harness in
`//pct:pct_batteries`, which reaches only the product's public API and the edge.

## 5. The rules of record

| rule or tenet | holds? | why |
|---|---|---|
| 1: the database calculates everything | yes | the rewrite is input: the twins are Pure over Pure's `substring`, `min`, `max`, `if` and `length`, all computed by the database; Java computes no value |
| 2 and 3: presentation | yes | nothing in presentation changes; the answer is lite's typed rows, written as today |
| 4: the host judge | not touched | |
| tenet 3: "compatibility re-spells, never re-computes ... answered Pure's way and recorded" | **no, as worded** | read at the door, the opt-in is exactly a "fix" that changes the answer for some input. That is its purpose |
| tenet 6: no modes in the core | yes | the mode is at the door; the core means Pure's alone |
| **D1: "Pure's way, LOUD, no translator"** and **final plan §2 part 2: "The door accepts legend-engine's input unchanged. It does not translate programs."** | **no** | the opt-in is a typed translator at the door, used per request when a server chooses it |

So, per the rules' own procedure, this stops here and goes to the user. **The user's word would amend D1, §2 part 2
and tenet 3's door reading**, for example:

> "With `LEGEND_LITE_PURE_MEANING=engine`, the legend-engine door may rewrite `substring` and `indexOf` in store-bound
> code to legend-engine's meaning, on H2 and DuckDB, and refuses what it cannot rewrite."

Each difference that remains is a register row: the 3-argument `indexOf` answered, and the other databases refused.

## Evidence (`h16/`)

- `agreement-pure-core-{h2,duckdb}.tsv` and `agreement-rewrite-{h2,duckdb}.tsv`: each request's two answers.
- `harness.patch.txt`, `engine-translator-h16-delta.patch.txt`, `compat-engine-{h2,duckdb}.pure.txt`: the scratch
  harness and the libraries as run (the DuckDB file with H16's two rules).
- `hostside-requests-{1,2}.tsv` and `hostside-engine-{1,2}.jsonl`: legend-engine 4.145.0 asked, through slice R's
  oracle, whether a store-free `let` is host-side (for the detector's shape note). It is: `{|let x =
  'abcdef'->substring(1, 3); ...filter(r|$r.region == $x)...}` plans as SQL `where region = 'bc'`, Pure's answer
  computed before the query, and `|let x = ...; $x` plans as a `platform` node. A literal `substring` inside the
  routed lambda goes to SQL as written (`select substring('abcdef', 1, 3)`).

## Addendum (~19:50): the opt-in as a swap to piece 1's natives

**The owner's question, for the user** (who finds the door rewrite complex next to a core flag): could `engine` simply
swap each call the detector flags for A3 piece 1's two natives? Those are `meta::legend::lite::sqlSubstring` and
`sqlPosition`, which Cypher, SQL and IQ use, each "the database's function verbatim". The swap would replace H5's
85-line Pure library.

**Measured, and the measurement already exists.** Piece 1's natives lower to exactly the calls today's core rules
emit (the A3 note's PR1 table):
- `sqlSubstring` is `SqlFn.SUBSTRING` with the arguments as given;
- `sqlPosition` is `SqlFn.STRPOS`.

Today's lite, at #124's head, lowers String `substring` and 2-argument `indexOf` to those same two calls:
`Scalars.java` before A3, `RULES.put(f, (n, args) -> new SqlExpr.Call(SqlFn.SUBSTRING, args))` and
`new SqlExpr.Call(SqlFn.STRPOS, args)`. So the swap sends the battery the SQL today's lite sends, and today's pinned
agreement, which CI checks per push, is its measurement (`pct/batteries/agreement_{h2,duckdb}.tsv`):

| | H2 | DuckDB |
|---|---|---|
| the swap (equal to today's lite on these calls) | 46 agree, 2 differ, 1 both fail: **every one of legend-engine's 46 answers** | **31 agree, 18 differ**: 16 of legend-engine's 47 answers missed |
| H5's library with H16's two rules (above) | 46 | 47 |

The one call the swap does not touch is the 3-argument `indexOf`. legend-engine refuses it (`col.i04`, `lit.i04`),
and so it differs under both designs.

**What the swap misses, and why: lite's own dialects clamp `SUBSTRING`.**
- `sql/dialect/SubstringClamp` raises every `SUBSTRING` start below 1 to 1. Its javadoc: "H2 clamps a sub-1 start at
  its own runtime; DuckDB counts empties -- so the DuckDB EXECUTION dialect clamps".
- It runs in the DuckDB, Postgres, Snowflake, Trino, MariaDB, SQLite, BigQuery, Databricks and ClickHouse dialects.
- So whatever the swap writes, the database never receives legend-engine's arguments as given. It computes H2's
  answer. legend-engine sends DuckDB the SQL unclamped.
- **The 16 DuckDB misses are every request with a start of 0 or below, or a negative length:** `col.s01`, `s06`,
  `s07`, `s11`, `s14`, `s15`, `s18`, `col.i07` (a `substring` from 0 up to an `indexOf`), and their literal twins
  plus `lit.s12` and `lit.s13`. For example, `substring(0, 2)` on `'Credit'`: legend-engine answers `'C'`, the swap
  `'Cr'`.
- So the swap reproduces legend-engine's answer only where the database's own `SUBSTRING` treats a start below 1 as
  H2 does: **H2 alone** among the databases measured. Elsewhere it is unmeasured, and on DuckDB it is wrong in 16
  of 47.
- `indexOf` is fine everywhere: `STRPOS(s, t)` means what legend-engine's `locate(t, s)` means (1-based, 0 on a miss).

**Making the swap exact on every database needs the clamp skipped for these calls.** That means a `SUBSTRING` the
dialects send exactly as given: a new MIR variant (or a flag on the call) that only the edge's rewrite would produce.
- That is a product addition serving the edge alone. `EdgeOnlyDataTest` and the edge-only `SqlFn` census would count
  it, so it is a core-hook question: **stopped here, for the user**.
- Piece 1's natives cannot themselves lose the clamp. Cypher, SQL and IQ rely on today's clamped answers, and piece
  1's gate is byte-identical SQL.

**What the swap removes, and what stays:**

| | H5's library | the swap |
|---|---|---|
| the Pure library (about 85 lines) | needed | **gone**: the natives are platform natives in every model |
| the extended model, its 3.4-4.1 s build on the corpus world and its held copy | needed, once per (model, target) | **gone for the query itself**: the rewritten lambda types against the request's own held model |
| user-function clones | needed | **still needed** for a reached function whose body holds a flagged call, so that a host-side caller keeps Pure's meaning. A clone is a new element, so its model is built and held again, once per (model, clone set). **Or** `engine` refuses such a request by name in its first form, and no rebuild exists at all |
| DuckDB's two library rules (negative length; a literal's constant-folder window) | written and measured | not needed where the database computes verbatim, but the clamp prevents that (above) |
| error words naming the substitute | map back | map back (the natives' names instead of `meta::legend::compat::engine::`) |

**The new size, if the user takes the swap:**
- **about 1 to 1.5 days on top of the detector**: the rename of flagged sites to the two natives, by position and
  arity (about 100 lines), the setting's `engine` value, error words mapped back, and about 150 lines of tests;
- **with `engine` offered on H2 alone**, where it is measured exact, and refused by name on every other target;
- **with reached user functions refused** under `engine`.
- Clones would add about half a day and bring back one held rebuild per (model, clone set).
- Offering it on DuckDB and the others needs the clamp question ruled first: a core change.

**The rules of record:** the same as section 5. It is still a rewrite of the program at the door, so it needs the
user's amendment of D1 and the final plan's §2 part 2. It adds no hook to the core unless the clamp is to be skipped.
