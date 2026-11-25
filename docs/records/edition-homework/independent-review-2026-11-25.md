# Independent review of the edition design, 2026-11-25

*An adversarial reviewer with no stake in the design read the design, its census, the code at 7c2ac303 and the project's rules, and wrote this report. It is kept as written below; the rewording keeps every finding, number and file reference. The design's answers to it are tracked in `docs/records/edition-program-START-HERE-2026-11-25.md` §8.*

## Verdict

The core idea is sound for comparisons (E1, E2). The typer writes an explicit instruction once, and nothing later needs to know who asked. But several of the design's factual claims are wrong in the code. And the answer to "why can't substring be exact" exposes a structural limit the design does not state: **legend-engine's answer depends on where a call runs, and a typer that types each function body once cannot know that.** E3's "no moves" and E4's substring row do not hold as written.

**Sound with must-fixes for E0c to E2. Not ready to approve for E3/E4 or the §11 amendments until the must-fixes are done.**

---

## 1. Why can't `substring` stay exact?

### What today's rewrite (A3) does

`PureV1Routes.checkMeaning` (`edge/PureV1Routes.java:331-335`) runs it for pure/v1's `execute`, `generatePlan` and `executeArrow` (:273, :314, :357), the tab's host route (`HostAnswers.java:423`) and the test-data routes (`TestDataRoutes.java:420`).

1. It splits the query's statements into those that read a store and those that don't (`PureMeaningCheck.java:227-254`). A `let` that reads no store keeps Pure's meaning, because legend-engine computes it in Java with Pure's meaning (the H16 probe, cited at :49-52).
2. It follows user calls transitively (:346 `follow`) and inlines each store-reading statement on its own (`PureMeaningRewrite.java:107-113`). Then it rewrites the flagged calls by identity, per call site (:131-154). So one function called from a host-side `let` and from a store-reading statement is rewritten at the store-side use only (`PureMeaningRewriteTest.aFunctionCalledHostSideAndStoreSideIsRewrittenAtTheStoreSideUseAlone`).

**Where it's exact:** on H2 and DuckDB only (`PureText.java:31-43`), for:
- store-reading query code;
- the helpers and qualified properties it reaches;
- statements that read no store, left as Pure's.

**Where it refuses by name, never giving a wrong answer:**
- any other database;
- mapping code (`~func`, chained M2M transforms);
- a graph fetch's derived property (`PureMeaningEngineTest.aGraphFetchsQualifiedPropertyIsRefusedByName`);
- a function the inliner keeps as a call;
- a function value it can't follow;
- a lambda `let` shared between host-side and store-side code (`PureMeaningRewrite.java:167-190`).

**Where it doesn't run at all:** the relational corpus (`MinimalCorpus.java:626` calls only `UpstreamTestBodies.admitted`) and PCT's ENGINE lane (`PctPlanRoute`: `EngineComparisons` only). There, `substring` means Pure's today.

### The design got A3 wrong

§3 says A3's rows "have the same blind spot as comparisons: a helper function that calls them doesn't get the rewrite." **That is false for A3.** It already reaches helpers, per call site. Its real gaps are mapping code, graph fetch properties and unchecked databases.

### What an edition rule would do instead, and why it's less exact

The typer types a function body once per query, cached by the function's identity (`SpecCompiler.java:48`, :81-95). That happens before, and apart from, any call site, and nothing after the typer may tell sites apart. So the rule must pick one answer per function. The design picks "the database's everywhere" (§10b-2 #3).

What changes at pure/v1 under E4 as designed:

| Case (today's test) | Today | Under E4 as designed | legend-engine |
|---|---|---|---|
| `let x = 'xabc'->substring(1,4); T->filter(r\|$r.NAME==$x)` (`PureMeaningEngineTest:389-398`) | Pure's `'abc'` (matches) | the database's SUBSTRING (the test's own comment: SQL gives `'xab'`) | `'abc'` |
| `let x = 'ab'->substring(1,3); ...` | raises "out of bounds" (S383) | no raise | raises |
| `let x = t::firstThree('abcdef')` beside a store-side `t::firstThree($r.NAME)` | host: Pure's; store: the database's | both the database's | host: Pure's; store: the database's |
| a graph fetch's derived property `initials()` | refused | the database's | the database's (E-M1 probe): **better** |
| a `~func` body using `substring` | refused | the database's | the database's: **better** |
| chained M2M, TDS query | refused | Pure's (the normalizer marks M2M "in memory") | **the database's**: `chain.pure:104-116` pushes the transforms into the SQL; **a wrong answer where today there's a refusal** |
| a checked graph fetch constraint using `substring` | Pure's (A3 skips constraints, `PureMeaningCheck.java:453-458`) | the database's | Java, Pure's: **a regression** |
| corpus `testWithFunction.pure:102`, `substring(1,5)=='John'` inside `execute()` | Pure's (raises, S383) | the database's | the database's: **better** (roster not verified) |
| corpus helper `sortCsv` (`planExecutionTestUtility.pure:130-133`: `indexOf`, `substring(0,$mid)`, run host-side upstream) | Pure's | the database's | Pure's: **a regression if lite's corpus runs it** (not verified) |
| PCT ENGINE lane `substring` tests | Pure's | the database's | legend-engine's own PCT ledgers them as failures (S130) |

### Designs that keep it exact within the constraints

- **(a) A placement-resolved explicit instruction (recommended to evaluate).**
  - Under `engine` the typer writes `meta::legend::lite::engineSubstring(...)`, and the same for `indexOf` and `toOne` of an empty value.
  - Its meaning is fixed and edition-free: "the database's SUBSTRING where legend-engine sends the code to the database; Pure's otherwise".
  - One planner pass stamps each occurrence with its statement's placement, reusing `StatementEffects` as A3 does. It must run before G½ folds host-side `let`s into store-reading statements; the store resolver supplies chained M2M and graph fetch.
  - The edition isn't read after the typer, and no door is involved.
  - It's exact where A3 is exact, and also reaches mapping code and helpers called from both kinds of site. `CORRECT_SQL_SUBSTRING_INDEXING` can be resolved per site the same way.
  - Database coverage moves to the dialect: a dialect without the arm refuses by name.
  - Cost: legend-engine's placement rule lives in the core, as a native's documented meaning. That is in tension with G5, as is the whole edition package.
- **(b) Per-placement typed bodies.** Cache typed bodies by function and placement, and move A3's pass into the typer's query entry. Exact, but it breaks the "one read site in `Overloads`" claim and is more complex.
- **(c) Hybrid.** Keep A3 at the door for the query and its helpers. Add an `engine` row only for code the store resolver demands, which always reads a store except M2M graph fetch. Strictly better than today, but it keeps one door meaning rewrite.
- **(d) The design's choice.** The database's answer everywhere, with a register row for each lost case, posted to the user. The simplest, but less exact than today at pure/v1.

---

## 2. What gets worse or changes, by phase and audience

| Phase | Lite's product | legend-engine's door (pure/v1, corpus) | legend-pure's door (PCT) |
|---|---|---|---|
| E0c | No change, except: **`FeatureRules` isn't dead.** `requireConsumed` (`FeatureRules.java:117`) is the live refusal of `USE_DB_NATIVE_IMPLICIT_NULL_ORDERING` and `VARIANT_TYPE_AS_INPUT`, and what lets `CORRECT_SQL_SUBSTRING_INDEXING` through. Deleting it unguarded is a silent fallback. | same | same |
| E1 | None, unless held models (below) | **The tab's pure/v1 routes use the host's held model** (`EdgeBoundary.java:37-52` → `planner/Boundary.java:45,99-105` → `HeldModels.withText`, keyed by text alone). If the held model is `lite`, a `generatePlan` with a crossing is refused at legend-engine's door. If it's keyed by edition, the trading model compiles twice (5 to 6 seconds, per `HeldModels`' javadoc). | none |
| E2 | Crossings in function bodies, mapping code and `~func` become compile errors. Today they're unmarked (`Typer.inFunctionScope`, and `Overloads.java:131` for normalized bodies). The platform's prelude and normalized platform bodies are typed `lite` too, not verified free of crossings. | The corpus: "the one in-memory XStore comparison gets Pure's `false`, legend-engine's real behaviour" **contradicts S127's measured evidence**: `upstream-api/s127-number-equality.txt` says legend-engine's generated-Java executor answers `1 == 1.0` **true**. Not verified for XStore or M2M specifically. | A crossing in a **function body** moves from the database's comparison (S127's note) to `false`, and where both sides may be empty, to **a new refusal** (`PureComparisons.java:58`). The design says "no moves". |
| E3 | none | Only Float text written as a direct `toString` call can move to the typer. **Float text also comes from** `format`'s `%f`/`%s` (`Scalars.java:3563`, :2937), lists, `Pair`/`List` toString and **generic bodies** (the prelude's `List.toString` maps a `T`), whose type is known only after inlining, so a typer rule misses them. Today's SQL pass catches every case at the SQL tree. Moves are likely despite "Expected moves: none". | **Moves, contrary to "none":** under `pure`, §3 gives lite's rules, but today PCT gets the edge's forms (applied to `UPSTREAM`). Float text goes from the database's cast to six decimals (further from Pure's `0.30000000000000004`). DateTime text goes from legend-pure's print to ISO nanoseconds (§3 has no DateTime row, yet §9 deletes that arm). An empty cell goes from `TDSNull` to skipped. A parse error goes from the database's cast to lite's checked parse. Hard case 1 admits PCT moves; E3 says none. |
| E4 | none | `substring`: see §1. The corpus would inherit pure/v1's A3, which it doesn't have today. | §3 has `pure` **refuse** `assertSize` over a relation and be **strict** for `assertJsonStringsEqual`. **Those rewrites are legend-pure's own meaning**: S137 says upstream counts a relation as one value, and S144's grammar is legend-pure's `CompiledSupport.jsonStringEquals`. PCT channel B applies them today (`UpstreamTestBodies.java:47-49`, `ChannelB.java:411`). So that moves away from Pure. S137 measured 0 PCT hits; S144 and the order-free rewrite weren't measured. |
| E5 to E7 | — | Plain raises (`ENGINE_UNCHECKED`) apply today at pure/v1 only (`PureText.java:169`, `EdgeLoweringForms.java:27-30`): divide by zero, sqrt, shifts, `toOne` size, slice, range, cast and parses; the corpus uses checked raises. One `engine` edition merges the two, so corpus error tests may move. Upstream's text inside a pure/v1 program (`planToString`, `executionPlan`) is answered today (`PURE_V1.facet` delegates to `UPSTREAM`); under ruling 2 it's **refused**, a capability loss. | H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` leave PCT (hard case 1), so H2's null ordering moves. The DECIMAL Float literals (`PureText.java:263,310,333,370,481`) leave PCT, so arithmetic moves to DOUBLE. |

**SQL size:** `pure`'s literal `false`, and the database's `substring` without raise guards, make SQL smaller. PCT's six-decimal Float text adds about 33 characters per value (S311), so the lean-SQL pins will move.

---

## 3. Hidden decisions and factual errors

1. **"Every edition-dependent construct passes through one site in `Overloads`" is false.**
   - Float **literals** (DECIMAL on 5 databases) aren't calls.
   - Casts and slices go through their own checkers (`Typer.java`'s `CAST` and `SLICE` arms), which matters for future raise rows.
   - `StaticFold` folds `==` with Java `equals`, and a Float's `toString` with `String.valueOf`, **before** that site, inside normalized bodies (`Overloads.java:406`; `StaticFold.java:608-624`, :877-890).
   - `LiteralUnroll` folds literal-only `in` / `contains` / `==` across kinds to Pure's `false` **after** the typer, at G½ (`LiteralUnroll.java:390-438`, :586-590; `UserCallInliner.java:1360`). So under `engine`, `1->in([1.0])` is `false`, not the database's `true`. That's the same as today, but §3's `engine` column misstates it.
   - Generic code: a crossing or a Float's text is known only after inlining.
   - H2's settings aren't expressions at all.
2. **"The table is the entire list" is false.** The census's own meaning items are missing from §3:
   - DateTime text (S312);
   - list text (S316c);
   - legend-pure-only texts (S314);
   - the DECIMAL Float literal (S20);
   - `isNumeric` (S370), `parseDecimal(5,2)`, lenient `parseJSON`;
   - the ten plain raise kinds;
   - H2's settings;
   - `executeValues`' bare single object.
3. **The `pure` edition isn't legend-pure's meaning.** It's lite's rules plus Pure's comparisons (rows 2, 3, 6, 7 and 8), named and sold as "legend-pure's meaning".
4. **One `engine` edition merges two audiences that differ today.** pure/v1 has A3 and plain raises. The corpus has neither, and has the test-body rewrites. And the corpus's own test code runs on compiled legend-pure upstream, with only `execute()` sent to the database, so "the database's meaning everywhere" is wrong for that code.
5. **"In memory" for M2M is a property of the query, not the mapping.** `chain.pure:104-116` folds chained M2M into the store query for TDS results, while graph fetch runs in Java. A static record from the normalizer can't express that.
6. **"Per project, the same answers through either door" and "today each door admits one origin's code" are both false in the tab.** One `HeldModels` instance serves lite's API and pure/v1 for the same text, including a model layered over the timeseries library (`HeldModels.java:44-49`, :74-76). And §4.1's per-project declaration isn't scheduled in any phase: E1 to E4 deliver per door only.
7. **The architecture guard as written fails on the doors that must set the edition** (§10's E1 lists `ModelInput`, `HostAnswers`, `LiteV1Api`, `MinimalCorpus`, PCT). It must distinguish setting from branching. `LiteV1Api.Models.with(text, ...)` (`LiteV1Api.java:96-106`) has no edition parameter.
8. **The typer will also read feature flags**: those written in the program (`LEGACY_SQL_NULL_UNSAFE_EQUALS`, `CORRECT_SQL_SUBSTRING_INDEXING`), and "runner default flags attached by the door" (§10c). That's a second exception to the switch rule, so "the switch counts read one" is false. And flags scoped to a `from(...)` subtree can't apply to cached function bodies at all.
9. **H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` change answers**, such as null order. Calling them "door input" means the door chooses a meaning, which breaks "meaning follows the code". It also reverses B4's plan to remove them (the stage B plan's B4 row).
10. **Ruling 3 was made on a wrong fact.** legend-engine's JSON comparator compares numbers **exactly** (`JsonNodeComparator.java:153-157`). The 2-ULP tolerance was removed on 2026-11-24 (`EqualToJson.java:88-89`; its javadoc header still says 2 ULP). And a Java JSON comparator as an "edition row" is a category error: it's a judge, and §5 says judges never see the edition.
11. **The migration report ruled on 2026-11-24 is lost.** It lists every comparison legend-engine's door loosened (`CrossTypeComparisons.in` exists but is unused). Under `engine`, no marker survives to list.
12. **There are two E5s.** §10 defines E5 as per-package editions and §10b as formats to presenters, so the order in §12 is ambiguous.
13. **E5's per-package editions "change no other part" is false.** The rules object is "picked once when the typer is created"; per-package editions need rules per body. Normalized bodies are typed inside the caller's context (`Overloads.java:131`), and the prelude needs an edition.
14. **§10c's "legend-engine's PCT runner switching on the substring fix" is the testable runner** (`TestExecutionContextHelper.java:46`). PCT adapters set no flags (S130; `PctPlanRoute`).
15. **The E4 row hook is too narrow.** The order-free `assertEquals` rewrite reads let-bound goldens across statements (`UpstreamTestBodies.asRows`). A call-local `rules.apply(call)` can't do that.

---

## 4. Conflicts with the goals, tenets and switch rule

- **The switch rule:** "read at the door … before the planner sees anything". The typer runs on demand inside the planner (the store resolver, the test planner). The amendment covers the edition, but not the flags written in programs or attached by doors (item 8).
- **Tenet 6:** the amendment is honest that in the first version the edition is "who is asking". Its wording, "a property of code", isn't true until projects declare an edition.
- **G5 (compatibility at the edge, contained):** legend-engine's meanings (Float text, `TDSNull`, plain parses, `substring`, the assert rewrites, possibly json-simple's grammar) move into a core package. Not addressed.
- **G3 and the upstream-fidelity tenet** ("lite answers Pure's way, departures posted and ledgered"): §11.3 lets lite projects choose `engine`, which would make cross-type equality and legend-engine's other departures a product mode. Not amended.
- **Tenet 5 / G6:** moving `ServiceTestRunner`'s Java comparator into the product adds a second comparison rulebook to the product.

---

## 5. The census's hard cases (§10b-2)

| # | Handled as claimed? |
|---|---|
| 1 | Partly. Right that the split moves PCT, but E3 says no moves, and the `pure` column assigns lite's rules, not Pure's |
| 2 | Only with a hook for literals outside `Overloads`. The "refused at the door (B6)" route is unclear: whole-database refusal (pure/v1 on Postgres loses every query), or per-construct refusal in the dialect (better) |
| 3 | No: see §1. Its fallback, "A3 … helper blind spot", rests on a false premise |
| 4 | Not a format: a door-chosen meaning (item 9) |
| 5 | Breaks the "one exception" claim (item 8) |
| 6 | OK as a ruling, but a capability loss at pure/v1 |
| 7 | Plausible: graph keys need re-spelling of JSON the database produced |
| 8, 9 | OK (step 6) |
| 10 | OK if measured |
| 11 | Ruled on a wrong fact (item 10) |
| 12 | OK |

---

## 6. Lists

**MUST-FIX before approval:**
1. Correct §3's A3 "helper blind spot" claim, and rewrite the `substring` row with one of §1's alternatives, listing the cases that change.
2. Make §3 the real list, with the census's meaning items, or state which phase handles each missing one.
3. Correct "one read site": name the literal, cast, slice, fold and generic-code gaps, and how each is handled.
4. E3's "Expected moves: none": restate it per audience, add a DateTime row or admit the move, and address Float text from `format`, lists and generic bodies.
5. E1: specify how `HeldModels`, `LiteV1Api.Models` and layered libraries serve two doors.
6. Correct ruling 3's 2-ULP premise and re-ask it.
7. Check the M2M / XStore "Pure's answer" claims against S127 and `chain.pure`, by asking the engine runner.
8. Amend the switch rule honestly for the flags the typer reads, or move the flags elsewhere.

**SHOULD-FIX:**
- keep `FeatureRules`' refusal in E0c;
- reword the guard so the doors may set the edition;
- settle the duplicate E5;
- keep the migration report;
- decide the edition of the prelude and of normalized bodies;
- fix §10c's PCT-runner slip;
- widen the E4 hook beyond single calls;
- amend G5, G3 and the fidelity tenet, or bound §11.3.

**Decisions the user hasn't been asked to make:**
1. Is `pure` legend-pure's meaning, or lite's rules with Pure's comparisons?
2. Should the corpus and pure/v1 share one `engine` edition, given they differ today?
3. For `substring`: exact (alternatives a to c), or simpler (d, with recorded losses)?
4. H2's settings and the DECIMAL literals: keep them as legend-engine's meaning (the design), or delete them (B4's plan)?
5. May lite's own projects choose `engine` (§11.3)?
6. pure/v1 losing `planToString` and `executionPlan` inside programs.

**Not verified:**
- whether lite's corpus runs `sortCsv`;
- PCT move counts for E2, E3 and E4;
- whether the prelude and normalized platform bodies contain crossings;
- legend-engine's XStore and M2M answer for `1 == 1.0`.
