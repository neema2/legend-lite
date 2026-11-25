# The edition: design and plan

*The design of record, ruled by the user on 2026-11-25. Building is paused at the user's word until an independent design review has reported and the user has read the final design; start at `docs/records/edition-program-START-HERE-2026-11-25.md`.*

**Ruled so far (the user, 2026-11-25):** "Edition seems best of all options"; per project ("Can start per project"); the names `pure`, `engine`, `lite`; M2M, `~func`, derived properties in mappings and graph fetch strict in `lite` and upstream's meaning elsewhere, by the typer, "not like previous door way"; doors only set editions at first; "Yes to all the amendments" (§11); "Yes to 1&2" (legend-engine's feature flags in a `lite` program refused by name; upstream's text as a value test-harness only); `ServiceTestRunner` kept ("we will need right").

## 1. The problem

In a few places, lite, legend-pure and legend-engine give different answers to the same Pure code. The clearest case is comparing two different types:

- **legend-pure:** `1 == 1.0` is `false`. Two values of different types are never equal.
- **legend-engine:** `1 == 1.0` is `true`. It sends the comparison to the database, and SQL promotes the integer.
- **Lite's product:** `1 == 1.0` is a compile error (your strict rule).

Today lite types the code first, then each compatibility door rewrites the typed query into its audience's answer. That works for the query itself. But function bodies, derived properties, mapping code and library code are typed later, on demand, deep inside the planner, where a door can't reach them. Every attempt to reach them has needed a workaround, and each workaround uncovered the next one.

## 2. The idea

Every piece of code has an **edition**: the set of meanings it was written for. The compiler reads a piece of code's edition at the one moment it types that code, which is whenever a query first needs it, and writes the answer as an explicit instruction. From then on nothing else in lite knows editions exist. The planner, the SQL and the runtime only ever see explicit instructions such as "refuse", "the database's comparison" or "Pure's `false`".

**Meaning follows the code. Format follows the door.**

- The edition decides what code means.
- The door a request came through decides only how the answer is written out: result envelopes, error wording, a printed table.

## 3. The three editions

| Code construct | `lite` (lite's own projects) | `pure` (legend-pure's meaning) | `engine` (legend-engine's meaning) |
|---|---|---|---|
| Comparison across known types (`1 == 1.0`, an enumeration against a String) | compile error | Pure's answer: equality `false`, membership `false`, `indexOf` `-1`, an ordering compared as written | the database's comparison (`1 = 1.0` is true) |
| A Float's text inside a program (`toString`, a message built from it) | lite's rule, six decimals (S311) | lite's rule, the difference recorded (Pure's exact print has no database equivalent) | the database's own cast, as legend-engine sends it (`1000000`; `1.3421E8` on H2) |
| An empty table cell's text inside a program (a row's cells joined) | lite's rule: skipped in a join, `null` alone (S313, S316) | lite's rule, recorded | `TDSNull`, legend-engine's empty cell |
| `parseInteger` / `parseFloat` of bad text | lite's checked parse, with lite's error | lite's checked parse | the database's plain cast, with the database's error (F31) |
| `substring` / `indexOf` / `toOne` of an empty value | Pure's meaning | Pure's meaning | the database's function (today a door rewrite, A3; phase E4) |
| `assertSize` / `assertEmpty` over a relation | refused, naming the assert to write | refused | counts the relation as one value (today a door rewrite, S137; phase E4) |
| `assertJsonStringsEqual` | strict JSON | strict JSON | legend-pure's lenient grammar (today a door rewrite, S144; phase E4) |
| `assertEquals` of rows with no defined order | refused (order-dependent) | refused | compared in any order (today a door rewrite, step 4; phase E4) |

Everything not in this table means the same thing in every edition. The table is the entire list. It can only grow by your ruling, and each row needs a register entry.

The last four rows are legend-engine meanings that today's door rewrites apply to the test body alone. They have the same blind spot as comparisons: a helper function that calls them (like `assertConversion` calling `assertEquals`) doesn't get the rewrite. That's why they belong here, moved one at a time and measured (E4).

**Not editions:** islands (`#SQL`, IQ, Cypher, TS) and the relational mapping language (joins, filters, column expressions, enumeration matches) always compare by SQL's rule (ruling 1), because they are other languages. Inside them the typer uses a fixed "SQL rules" object instead of the edition's (today's `marking` switch, generalized).

**Pure written inside a mapping follows the edition, like any other Pure:** M2M transforms, `~func` bodies, cross-store (XStore) expressions, derived and qualified properties used in mappings and graph fetch trees, and checked graph fetch constraints. The typer applies the rules to that code at the moment it types it, including when the store resolver reaches it late, so nothing has to "reach" mapping code. In `lite` a comparison across types there is a compile error, listed by the model check. (This replaces S403's "not strict for now", which was a limitation of the old door-rewrite design.)

**What that needs:** today the normalizer turns relational mappings and M2M/`~func` mappings into the same kind of synthesized function, so the typer can't tell SQL from Pure in mapping code. The normalizer will record each mapping function's language when it creates it (it knows which mapping it is building): `relational` (SQL's rules) or `pure` (the edition's), and for `pure`, whether legend-engine runs it in memory (M2M, in-memory XStore) or as a database query (`~func`).

**Where legend-engine runs code, and why it matters:** legend-engine sends code that reads a store to the database, and runs other code in Java with Pure's meaning. For mapping code the normalizer's record says which (M2M and in-memory XStore run in Java), so the `engine` edition answers those Pure's way, as legend-engine really does: an M2M transform's `1 == 1.0` is `false`. Inside an ordinary function body the typer can't tell, because the same function may be called from both kinds of code, so there the `engine` edition gives the database's answer. Today's 5a door does the same, so that part is recorded, not new. It matters most for `substring`, which today's A3 rewrite handles precisely, so E4 measures before moving it.

## 4. Where a piece of code's edition comes from

1. **The code declares it.** A lite project declares its edition once, in its project settings; the default is `lite`. A project that wants to run legend-engine-style code unchanged declares `engine`. This is your "run even new code as backwards compatible".
2. **Code that doesn't declare one takes its door's default.**

   | Door | Default edition |
   |---|---|
   | lite's own API and IDE | `lite` |
   | legend-engine's API (`pure/v1`), and the corpus run as legend-engine's tests | `engine` |
   | legend-pure's door (PCT) | `pure` |

   Code arriving through legend-engine's API was written for legend-engine, so that's the honest default.
3. **First version:** one edition per compiled model. Every function in a request's model shares it, which covers every door and project we have today. If a lite project later depends on an upstream library, each package can carry its own edition, so your code stays strict and the library keeps `engine`. That is a later step and changes no other part of this design.

## 5. Where it is read: one package, and nowhere else

The model carries its edition as data. When the typer is created, it reads the edition once and picks a rules object, `EditionRules.of(model.edition())`. Every native call the typer resolves already passes through one spot in `Overloads`, where step 5a's crossing check sits today. There the typer calls `rules.apply(call)`, and never names an edition itself. The rules object writes the explicit instruction:

- `lite`: a crossing becomes the existing marker, which lite refuses by name, and which the model check lists as a compile error.
- `pure`: a crossing becomes Pure's answer directly, for example a literal `false`.
- `engine`: a crossing stays the plain comparison; a Float's text becomes a call to the database's cast; an empty cell's text gets `TDSNull` as its argument; a parse becomes the plain cast.

**What the rule classes look like:** one class per row of §3, not one class per edition. Rows will grow over time; editions almost never will. Each class reads like its register entry, and Java's exhaustive `switch` forces every edition to be answered, with no `default`:

```java
package com.legend.compiler.spec.edition;   // the only package that may reference Edition

public enum Edition { LITE, PURE, ENGINE }

sealed interface EditionRule permits CrossTypeComparison, FloatText, EmptyCellText, Parse /* ... */ {
    boolean governs(TypedNativeCall call);                     // by the callee and the argument types, never row data
    TypedSpec meaning(Edition edition, TypedNativeCall call);  // ordinary code, the same in every edition
}

final class FloatText implements EditionRule {                 // register row S311
    public boolean governs(TypedNativeCall call) { return isToString(call) && argType(call, 0) == FLOAT; }
    public TypedSpec meaning(Edition edition, TypedNativeCall call) {
        return switch (edition) {
            case LITE, PURE -> call;                                  // lite's text (S311)
            case ENGINE -> DatabaseCast.toText(call.args().get(0));   // the database's own cast
        };
    }
}

public final class EditionRules {                              // what the typer holds
    private static final List<EditionRule> ROWS =
            List.of(new CrossTypeComparison(), new FloatText(), new EmptyCellText(), new Parse());
    private final Edition edition;
    public static EditionRules of(Edition edition) { return new EditionRules(edition); }
    public TypedSpec apply(TypedNativeCall call) {             // the one hook
        for (EditionRule row : ROWS) if (row.governs(call)) return row.meaning(edition, call);
        return call;
    }
}
```

**Why it can't spread:**
- An architecture test fails the build if any class outside `compiler.spec.edition` (and the model, which stores it) references `Edition`. You can't write `if (edition)` in the planner, the lowering or the runtime; the build rejects it.
- A ratchet pins the number of rows; a new row needs your ruling and a register entry.
- A matrix test runs every row in every edition.
- The plan's data format has no edition field, so nothing after the typer could branch on it even in principle.
- The planner, the lowering, the SQL dialects, the runtime and the judges can't see it, because by the time code reaches them, it holds only explicit instructions that mean the same thing in every edition.

This is the discipline that makes Rust's editions work: only the front of the compiler reads them.

## 6. How it works with demand-driven compilation

Nothing is typed early, and nothing is rewritten late. A function's edition is known before it is typed, from its model's declaration. Its body is typed only when a query first needs it, and is typed directly under its own edition.

- **A lite query calls your function `isBig(x) { $x > 1.5 }` with an Integer `x`.** The function is typed when the query needs it, under `lite`, so the comparison is a compile error. The model check already listed it in the IDE before you ran anything.
- **A lite M2M mapping writes `$src.salary < 50000`, a Decimal against an Integer.** When a query first reaches the mapping, the resolver demands its function, the typer types it under `lite`, and the comparison is a compile error. The IDE's model check showed it before. Lite's own `SalaryBandMapping` test is exactly this, and its model gets fixed to `50000D`.
- **A corpus test calls legend-engine's library code**, `toPostgresModel` with its `$p->last() == true`. The library is typed on demand under `engine`, so it's the plain comparison, exactly as before today. The door does nothing, and the test shape is untouched.
- **PCT through legend-pure's door compares `1 == 1.0`.** It's typed under `pure`, so it becomes `false`, Pure's answer. No door rewrite is needed.

## 7. What stays at the doors

Doors keep **input and output**:
- **Input:** which edition a request's undeclared code gets, and reading upstream's protocol.
- **Output:** result envelopes, error wording, a table or value printed as the last step (the database returns typed rows and Java writes upstream's text), and SQL-text asserts (step 6).

That's tenet 6 as written: differences between audiences live at the door and in the presenters.

Doors lose their meaning rewrites in stages:
- E2: the comparison rewrites (`EngineComparisons`, `PureComparisons`);
- E3: the SQL passes that edit finished SQL after planning: Float and DateTime text, the `TDSNull` join, the unguarded parses;
- E4: the `substring` rewrite (A3), and the test-body assert rewrites (S137, S144, the order-free `assertEquals`), each measured first. Until each moves, it stays a door rewrite with its helper-function blind spot recorded.

## 8. Per project or per run: what changes

**Per project** (recommended): the edition is a fact of the code. **Per run**: the caller picks one edition for a whole request, and every function the request reaches is typed under it.

| | Per project | Per run |
|---|---|---|
| Does the same function always mean the same thing? | Yes | No: it depends on who runs it |
| Your strict code calls an upstream library | Works: your code is checked strictly, the library keeps `engine` | A `lite` run makes the library's own crossings compile errors (legend-engine's `toPostgresModel` would stop compiling); a `engine` run switches off strictness for your code |
| "Is my project valid?" (IDE, model check, build) | One answer | "Valid under which edition?": every check needs an edition |
| The same project queried through lite's API and through legend-engine's API (an old client) | Same answers; only the envelope differs | Different answers for the same query depending on the door |
| Tenet 6, "the core never branches on who is asking" | Kept: the core branches on the code's edition | Broken: the edition is exactly who is asking |
| Cost to build | Each model carries its edition; per-package editions later | Simplest: one value carried with the request |

**For today's doors the two behave identically**, because each door admits one request made of one origin's code. They differ only when code from different places mixes, or when one project is reached through more than one door. Per run is the dangerous kind of mode: the setting you deleted on 11-20 was per run. Per project is the safe kind.

## 9. What this replaces and deletes

- **Deleted:** the doors' comparison rewrites (`EngineComparisons`, `PureComparisons`), and today's uncommitted function-body work (the inlining, the discovery, the handed-over bodies; never committed).
- **Deleted, with B2's held S6 work:**
  - the edge's SQL passes that change SQL after planning: `PurePrintForms`' Float and DateTime arms, the `TDSNull` join pass, and `UnguardedParses` (F31);
  - with them goes the last place compatibility edits SQL after the planner, which closes E0 = A.
- **Kept:**
  - step 5a's product refusal: in the `lite` edition it is simply what the edition says;
  - ruling 1 and the islands' rule;
  - S403 is rewritten: M2M and `~func` code is strict in `lite`, and follows upstream's meaning in the other editions;
  - the doors' presenters and the print lift.
- **Not needed any more:**
  - S404, the H2 exponent cases moving away from legend-engine: under `engine` they keep legend-engine's text;
  - the 2 validation-showcase corpus failures: they keep passing;
  - the one-test question from today: gone.

## 10. The plan

Each phase is measured before it is built, runs the full round with `//gates:local`, gets an independent review, and is pushed only when green.

- **E0. Ratify.** You rule on this design, the questions in §12, and the rule amendments in §11. Recorded in the rules of record.
- **E1. Plumbing, no behaviour change.**
  - The edition type: three values, `lite` the default.
  - A compiled model carries its edition, set by whoever compiles it: lite's API and IDE `lite`; the corpus harness and `pure/v1` `engine`; the PCT routes `pure`.
  - The one read site exists but changes nothing yet.
  - The architecture guard.
  - Expected moves: none on any lane.
- **E2. Comparisons move into the edition.**
  - The typer writes each crossing per edition, in the query and in function bodies alike.
  - The doors' comparison rewrites are deleted.
  - In `lite`, a crossing in any Pure is a compile error when typed: function bodies, derived properties, M2M transforms, `~func` bodies, XStore expressions, graph fetch's property paths, checked constraints. The model check lists them all.
  - The normalizer records each mapping function's language (`relational` or `pure`, and for `pure` whether legend-engine runs it in memory); the typer picks SQL's rules or the edition's from it.
  - Measured first: rerun the probe with graph fetch, mapping-level derived properties and each mapping kind counted separately.
  - Expected moves: none at either door except the corpus's one in-memory XStore comparison, which gets Pure's answer, legend-engine's real behaviour. In lite's own code, one M2M test model (`SalaryBandMapping`) becomes a compile error and is fixed to compare in one type; the probe found nothing else outside the rule's own tests.
- **E3. B2's held work as edition rules** (B2 builds this):
  - legend-engine's Float text (the database's cast), `TDSNull`, and the plain parses;
  - then the edge's SQL passes are deleted.
  - Expected moves: none. The same SQL is produced, now at typing instead of edited after planning. The 2 validation tests and the H2 exponent cases keep matching legend-engine.
- **E4. Later, one at a time, each measured first:** the remaining door meaning rewrites become edition rows: `substring`/`indexOf`/`toOne` (A3, which needs care because legend-engine only sends store-reading code to the database), S137's `assertSize`/`assertEmpty`, S144's JSON grammar, and step 4's order-free `assertEquals`. Each fixes the helper-function blind spot for its row.
- **E5. Later:** per-package editions, for projects that mix their own code with upstream libraries.

## 10b. The full cleanup: where every audience difference ends up

The edition is half the cleanup. Today lite does different things for different audiences in two places:

1. **The doors' meaning rewrites:** comparisons, `substring` (A3), the assert rewrites (S137, S144, order-free), and the SQL passes edited after planning. E2 to E4 delete all of these.
2. **Switches already inside the core:** the planner, the lowering and the test planner consult the edge through `PlanStyle`, an interface the edge supplies (31 core classes reference it), and 17 core classes read `platform.Feature`, legend-engine's own feature flags. That is "switches everywhere" in polymorphic form, and the edition alone doesn't remove it.

Every place that does something different for an audience is classified and given one fate:

| Kind | Example | Fate |
|---|---|---|
| **Meaning:** what code computes | comparisons, Float text, `TDSNull`, parses, `substring`, the assert rewrites | an edition row in the typer; the old code deleted |
| **Format:** how an answer looks | envelopes, error wording, a table printed at the last step, upstream's plan text | the door's presenters, after the plan; never consulted by the planner |
| **Test harness** | SQL-text asserts, the appeals, the H2 referee | test tooling (step 6, already designed) |
| **Product option** | `DETERMINISTIC_AS_OF` | stays a declared planning option, not an audience switch |
| **Dead** | anything with no remaining reader | deleted |

**The end state, enforced by the build:**
- only the edition package mentions `Edition`;
- nothing in the compiler, planner, lowering, SQL, runtime or test planner references an audience facet of `PlanStyle` or `platform.Feature`;
- `PlanStyle` is deleted, or reduced to nothing the core consults;
- the doors hold only input (upstream's protocol, the edition for code that doesn't declare one) and output (upstream's formats).

**Phases added to §10:**
- **E0b, the census** (done: `docs/records/edition-census-2026-11-25.md`): every edge class and every core class that reads `PlanStyle`, `platform.Feature` or an audience, each classified with its fate. It becomes the program's ledger, and each phase shrinks it.

  | | Meaning | Format | Test harness | Product option | Dead | Carriers or mixed |
  |---|---|---|---|---|---|---|
  | `PlanStyle`'s 30 parts | 10 | 2 | 10 | 1 | 6 | 1 |
  | 44 core classes | 9 | 0 | 15 | 1 | 6 | 13 |
  | 85 edge classes | 13 | 20 | 48 | 0 | 3 | 1 |

  The largest piece is the test harness: 48 edge classes, 15 core classes and 19 run-time classes exist only to judge upstream's tests. Moving them to test tooling (step 6 and its follow-ons) is the biggest part of the cleanup.
- **E0c, delete the dead parts now:** six `LoweringForms` forms that are identical in both forms, the unused `style` parameters of `PlanMaker.sql` and `JudgePlans.shapeOf`, `JsonGrammar`, a dead branch in `DatabaseAsserts`, and `FeatureRules` with the feature registry behind a flag the core never acts on. No behaviour changes.
- **E5:** format differences move behind the plan, into the presenters; `PlanStyle`'s remaining facets become plan data a presenter reads, or are deleted.
- **E6:** `platform.Feature` leaves the core (B5's existing goal): each flag becomes an edition row if it changes meaning, or is deleted.
- **E7, the closing guard:** the build rules above, and the census at zero outside the four allowed kinds.

This pulls together threads that exist today: step 6 (the test harness), B2's S5 to S7 (formats), and B5 (`platform.Feature`). It is weeks of measured slices, not one change.

## 10b-2. The census's hard cases, and how the design handles each

1. **One style serves two audiences today.** `PureText.UPSTREAM` serves both legend-pure's PCT and legend-engine's corpus. So legend-engine's meanings reach legend-pure's tests (the unguarded parses, `TDSNull`, the DECIMAL Float literal, the H2 settings, `isNumeric`, `parseDecimal`, lenient `parseJSON`), and legend-pure's prints reach the corpus. **The editions fix this:** each rule is assigned to `pure` or `engine`. That will move some PCT and corpus results, towards each audience's true answer. Measured in E2 and E3, each move shown to you.
2. **Some meanings depend on the database.** For example, legend-engine writes a Float literal as DECIMAL on five databases only. **Handled by the dialects, which is their normal job:** the edition writes an explicit instruction ("legend-engine's Float literal", "the database's cast"), and each dialect renders it for its database. Which database is not an audience. A database where legend-engine's answer isn't checked yet is still refused at the door (B6).
3. **Store-boundness is decided after typing.** legend-engine sends `substring` to the database only from code that reads a store. The typer can't see that, so the `engine` edition uses the database's `substring` everywhere. **E4 measures that first.** If it moves verdicts, A3 stays a door rewrite on the query, with its helper blind spot recorded. You decide at E4.
4. **Session settings aren't expressions.** H2's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` affect every statement. **Not an edition matter:** they describe legend-engine's H2 connection, so the door states them as connection data when it builds legend-engine's runtime, and the core opens the session the connection describes. Door input.
5. **Some flags are written in the program,** such as `LEGACY_SQL_NULL_UNSAFE_EQUALS` in a query's options. **The `engine` edition's rows read them; the `lite` edition refuses them by name** (your ruling, §12).
6. **Upstream's text as a value inside a program:** `planToString`, `toSQLString`, `toDDL`, `executionPlan` handle rows, `executeLegendQuery` envelopes, activity SQL. No edition can produce these, since lite never promises upstream's SQL text (CP-4). **Proposed:** they exist only in the test harness, and are refused by name in any program outside it (your ruling, §12).
7. **Formats the database builds into JSON:** a graph fetch key's text, a connection description, legend-engine's bare single object, the trace comment. **At the last step they're formats** (the presenter re-spells them); inside a program they'd be meaning (edition rows). In practice they're almost always the last step.
8. **The harness prints a relation in Java** (`UpstreamPrints`). **Harness only**, moving with step 6. In a product program, a relation's text used further is lite's text in every edition, as you ruled on 11-19.
9. **Error words read inside a program:** PCT's `assertError` compares legend-pure's message inside the body. **The harness compares the error record, not the words** (step 6).
10. **Answer-neutral SQL shape:** `existsJoin` and `EngineWhereZones` copy legend-engine's join shape, with the same answer. **Deleted**, since CP-4 never promises upstream's SQL text, unless measuring shows a test needs it.
11. **`ServiceTestRunner`, `EqualToJson`, `TdsObjectsWriter`:** a product-like feature, running a user's service test suites, built on legend-engine's comparator, and nothing in main code calls it. **Your ruling (§12):** keep it as a lite feature with lite's comparator, or delete it.
12. **Register housekeeping:** S311 to S316 live in a separate records file, not in `SEMANTICS_REGISTER.md`. They are folded in when their rows become edition rows.

## 10c. `PlanStyle` and `platform.Feature`: what they are and where each part goes

**`platform.Feature`** is legend-engine's own list of feature flags, mirrored member for member, because legend-engine code can switch them on in a query (`FeatureFlagOption` on `execute`), and legend-engine's runners set some by default. Today the lowering reads them: the exception your switch rule marked "to be removed in B5".

| Flag | What it does | Fate |
|---|---|---|
| `CORRECT_SQL_SUBSTRING_INDEXING` | fixes `substring`'s 0-based indexes in SQL | an input to the `engine` edition's `substring` row (E4), read by the typer |
| `LEGACY_SQL_NULL_UNSAFE_EQUALS` | plain `=` instead of null-safe equality | an input to the `engine` edition's comparison row, read by the typer |
| `PUSH_DOWN_ENUM_TRANSFORM` | changes only the plan text legend-engine shows | the presenter of legend-engine's plan endpoint |
| `USE_DB_NATIVE_IMPLICIT_NULL_ORDERING`, `VARIANT_TYPE_AS_INPUT` | already refused if set | stay refused by name; their code goes |

In the `lite` edition the flags don't exist: a lite query that sets one is refused by name. A runner's default flags, such as legend-engine's PCT runner switching on the `substring` fix, are attached by the door to the code it admits, like the edition itself, so they stop being run state.

**`PlanStyle`** is lite's own mechanism for writing a plan upstream's way: the door hands the planner a style object that the planner and lowering consult without knowing who's asking. It's legal under the switch rule, but it is a switch in polymorphic form (31 core classes reference it).

| Part | What it does | Fate |
|---|---|---|
| `dialect(...)` | the SQL dialect; the edge's subclasses override some spellings | the meaning overrides (Float text, parses, `TDSNull`) become edition rows (E3); then the planner uses the product dialects directly |
| `traceCell()` | legend-engine's trace comment around an execution id in a cell | the door's presenter, after the plan |
| `graphKeyDate` / `graphKeyString` | how a graph fetch key is written | the door's presenter, after the plan |
| `twoForm(...)` | a closed list of spots where upstream has its own form | each kind classified by the census: an edition row or a presenter |
| `facet(...)`, `judgesEngineText()` | judging SQL-text asserts in upstream's tests | test tooling, with step 6 |

**`PlanStyle` does not read the edition.** Formatting follows the door, not the edition: a `engine`-edition project queried through lite's API gets lite's formats; through legend-engine's API, legend-engine's envelope. Once every part above has gone to its home, nothing is left, and **`PlanStyle` is deleted**.

## 10d. How this maps onto Stage B

The edition program doesn't replace Stage B wholesale. It becomes the governing design for what's left of it, towards one end state.

| Stage B item | State | With the edition |
|---|---|---|
| B1: errors as plain records | landed | unchanged |
| Item 3: asserts as typed records | landed | unchanged |
| B3 steps 1 to 4 | landed | unchanged |
| B3 step 5: the one equality, as door rewrites | 5a landed | replaced by E1 and E2: comparisons become an edition row; 5a's door rewrites deleted |
| B3's door rewrites of legend-engine's test code (S137, S144, order-free `assertEquals`), and A3's `substring` | landed as door rewrites | E4 edition rows, fixing their helper-function blind spot |
| B3 step 6: SQL-text fallback into test tooling | designed | unchanged (test harness); it also removes `PlanStyle`'s test facets |
| B2: formatters replace SQL-built text | S1 to S5 done | format work unchanged; S6's meaning parts (Float text, `TDSNull`, parses) become E3; S7 unchanged |
| B4: legacy H2 settings, float-literal and unchecked-maths switches, DataCube workarounds | after B3 | the census classifies each: an edition row, or deleted |
| B5: delete the switch machinery and legend-engine's feature flags in the core; guards | last | E5 to E7: `PlanStyle` deleted, `platform.Feature` out of the lowering, the guards on. The switch counts end at one (the edition), not zero |

| B6: legend-engine's answer on the other databases, each checked before it is switched on | alongside, in the warm sessions | unchanged: the door's gate refusing an unchecked database by name stays at the door, as input. The edition's `engine` rows are checked database by database the same way |

Stage B's plan of record (`docs/records/compat-stage-b-plan-2026-11-20.md` on `claude/compat-program`) is amended to point here for B3's remaining step 5, B2's S6, B4 and B5.

## 11. Rules this amends (yours to ratify)

1. **The switch rule (11-19)**, "nothing in the core reads a switch", becomes: *"Switches are read at the door. The one exception is the edition, a property of code: only the typer reads it, and turns it into explicit instructions, so nothing after the typer can see it."* The registry gets its first entry, the edition: three values, default `lite`, owner the compatibility program, reader the typer's one site, permanent (no removal date).
2. **Tenet 6**, "no modes in the core: the core never branches on who is asking", gains: *"One exception: the edition, a property of code, read once by the typer."* In the first version the door sets it, so for now it effectively is who is asking; that's why the tenet is amended rather than reinterpreted.
3. **Your 11-20 ruling**, "make engine only mode for engine for everything": a request through legend-engine's door still gets the `engine` edition. The difference is reach: it now covers function bodies, mappings and library code, not just the query; and later a lite project can choose it too.
4. **B5's goal**, "the switch counts read zero", becomes "the switch counts read one: the edition".
5. **Replaced:** S403's "M2M and `~func` not strict for now" (strict in `lite`); and today's door-rewrite approach to function bodies, withdrawn.

## 12. Questions for you

Ruled 2026-11-25: per project; the names `pure`, `engine`, `lite`; M2M, `~func` and mapping-level properties strict in `lite`; doors only set editions at first. The order of phases is E0c, E1, E2, E3, E4, then E5 to E7.

**Doors only at first, the implications:**
- lite's own API, IDE and clients (DataCube, Python) always use `lite`;
- legend-engine code gets `engine` meaning only through legend-engine's door, and a legend-engine project loaded into lite's own tools is checked strictly as `lite`;
- no mixing yet: everything in one model shares one edition until per-package editions;
- until projects can declare an edition, the edition follows the door, which is why tenet 6 is amended rather than reinterpreted;
- adding project settings later is one field read where a project's model is compiled; the typer, the rules and the guards don't change.

Ruled the same day, the last of them:
1. legend-engine's feature flags written in a `lite` program are refused by name.
2. Upstream's text as a value (`planToString`, `toSQLString`, `toDDL`, `executionPlan` handles, `executeLegendQuery`) exists only in the test harness and is refused by name in any other program.
3. `ServiceTestRunner` is kept ("we will need right"). It moves out of the edge into the product, and its comparator becomes an edition row: in `engine`, legend-engine's rules (a null equals a missing field, arrays in any order, Floats within 2 ULP); in `lite`, lite's own typed comparison, the rules of lite's asserts. A product route to it stays future work under `docs/DEFERRED_TEST_EXECUTION.md`.
4. The amendments in §11: "Yes to all the amendments".

## Who builds what

- **B3:** E0c, E1 and E2, on a base at B2's 7c2ac303, which contains B3's 96f0d33c.
- **B2:** E3, its held S6 work as edition rows, once E1 has landed. Its other format work continues.
- **Later:** E4 to E7, assigned as each is reached.

## 13. Feature flags and backout flags later

The same discipline extends to them, but as a separate kind of switch, never as more editions.

- **An edition** says what code means for an audience. It's permanent, belongs to the code, and is read by the typer.
- **A feature or backout flag** switches lite's own new behaviour on or off during a rollout. It's temporary, usually set per deployment, and often affects a later stage (a planning strategy, a SQL shape, a run-time optimization), so the typer is the wrong reader.

Each flag goes in the switch registry you ratified on 11-19: name, values, default, owner, its one reader, a removal date that fails the build when passed, and a ceiling of 5. It is read in exactly one place and turned into an explicit choice before the stages after it. If it affects planning, the plan records the choice as data, so the runtime never reads the flag. lite already has the start of this: planning options (`PlanOption`, such as `DETERMINISTIC_AS_OF`) are chosen once and travel with the query. The edition would be the registry's one permanent entry.

## 14. Costs and risks

- **It is a mode.** People must know which edition their project is. It's mitigated by only three values and one read site, and code with no declaration gets its door's default.
- **Editions can grow.** Every row added to §3 is a new way for code to differ. The table only grows by your ruling with a register entry, and the switch registry's ceiling still applies.
- **The `engine` edition computes everything in the database.** legend-engine itself runs some code in Java with Pure's meaning, for code that reads no store. Where those differ, lite uses the database's answer and records it; today's 5a door already does the same.
- **E2 moves typing work.** The legend-pure door's answers move from a rewrite after typing into the typer. It's the same answers, measured to be identical.

## Appendix: for whoever builds it

- **E2, the read site:** the comparison classification (`EqualityClasses`) and the crossing site in `Overloads`, which already turns the check off for islands and mapping code (`Typer.mappingCode`, `markingCrossings`).
- **Deleted in E2:** `EngineComparisons`, `PureComparisons`, their door calls in `UpstreamTestBodies` and `PureV1Routes`, and `PureComparisonsTest`'s rewrite cases, which move to the typer's tests.
- **Kept and reused:**
  - `TypedCrossTypeComparison`, as `lite`'s instruction;
  - the model check (`Compiler.modelCheck`, `SpecCompiler.crossingsOf`, already written in today's draft) for `lite`;
  - register row S403, amended to the edition wording.
- **E1, the editions:** set where each door compiles its model: `MinimalCorpus.buildModule`, the PCT routes' `compileModel`, `edge/ModelInput`, `HostAnswers`, `LiteV1Api`.
