# The compiler done right, reconciled with the rebuild plan (2026-10-07, revision 2)

**Status: ruled 2026-10-07 (§6); the plan's D25 carries the decision.** Revision 2 replaces the same day's revision 1
(commit "Docs: the compiler done right -- the design for the user's decision, with its measurement"). Revision 1 was
written from the code and one measurement **without reading the compiler rebuild's own plan**,
`docs/EXECUTION_PLAN_2026_09_26.md` (revision H4, approved by the user on 2026-09-29; its work landed on main through
the nineteen "— Rebuild" entries in `docs/GATES.md`; it stopped on 2026-10-04 at its "Now" line when the build program
began). Every finding of revision 1 is an item of that plan, usually a smaller version of it. This revision keeps the
measurement, adds two more taken today (the stress corpus end to end; the probe's residue taken apart), maps each
finding to its item, says what today's numbers change, and reduces the decision to one question: where the rebuild
resumes.

The user's framing (2026-10-07) stands: parity of OUTPUT with legend-pure and legend-engine is the goal and the oracle;
the compiler itself should be streamlined, robust, fast, close to mainstream compilers, expert-level. The plan says the
same in its own words: D15 (our own inference engine, judged by observable outcomes) and D17 (cleaner, more
bulletproof, much less code, faster, cruft deleted).

## 0. In plain words

- **There is already a plan for this**, and it is a good one. It is a catalogue of about sixty items (W0 to W7) in
  five phases, ordered by its §4: fix the known wrong answers and learn what is unknown first (Phase 1, ending at a
  decision point C1), then the gates that make every later change safe (Phase 2), then the middle, where the wrong
  answers and the worst tangle live (the 36k-line store resolver and the SQL, Phase 3), then the front end (names, the
  typer, Phase 4), then the back end (Phase 5). Twenty-odd items are done. It stopped at a cleanup step: "one
  substitution engine" (decision D24).
- **What I measured today, in plain words.** (1) When the compiler types the whole world, 40 cents of every dollar
  go to re-answering "what can this bare name mean". The plan's item W2.3a removes exactly that (a name resolved once,
  into an id, and every later check comparing ids); the plan knew the problem but not its size. (2) When the stress
  corpus runs end to end, 14 cents of every dollar go to re-reading the test-data islands: each island is copied into
  a new string padded with one newline for every line above it, lexed again and indexed again. One corpus file has
  14,948 islands in 291,278 lines, so that is about 2.2 billion characters of padding per run. The plan does not have
  this item; it is a half-session fix with no semantic risk. (3) The back half, the store resolver, the lowering and
  the SQL, has no hot spot of that kind. Its cost is its shape, which is the plan's Phase 3. (4) Of the 365 bodies of
  platform code the typer cannot type, about 260 name functions, types and elements lite does not serve (a decision,
  the plan's D9, not a fix), about 60 come from the typer's own shape (desugaring interleaved with typing, checks that
  match an argument's spelling instead of its type, lambdas typed before their expected type is known), about 25 from
  the overload rule, about 10 from the inference kernel. The plan's Phase 4 removes the 60 and the 25 by construction.
- **The things the user named** on 2026-10-07, typing arguments twice and re-typing a property several times, are
  the plan's W3.4 and W4.2 (one substitution engine over the typed tree, which never re-types) and W2.4 (member
  access). The plan's "Now" line, the D24 cleanup, is their first step: six substitution engines become one. So the
  plan's resume point is already the user's priority.
- **The one question** is whether to resume the plan where it stopped and keep its order (recommended, with seven
  small amendments below), or to pull the names item forward because of the 40%.
- **The bump's Phase 3 branch becomes a reference either way**: its facts (legend-pure's ranking rules, the names
  calls resolve to, the legacy TDS forms, the corpus fixes) survive; its code is written again on the new typer; the
  bump's remaining phases follow on that base, the prelude with them.

## 1. What was measured today

### 1.1 Typing the whole world (revision 1's measurement, kept)

`//spec:eager_corpus_compile` types every body of the corpus's world, 9,942 bodies, after building the model: build
1.8 s, typing 2.0 s (three runs, this desk). Under Java Flight Recorder (809 samples over three runs; a sample belongs
to the first phase found on its stack): names **40%**, type 24%, parse 10%, model 4%, normalize 3%, other 17%. The
names share is `BareNames.catalogTiered` 24% inclusive, `BareNames.catalog` 20%, `ResolvedNames.referents` 11%,
`NameResolver.resolveVs` 13%: the resolver tries each bare call under the imports, the element's own package and the
32 core packages, and every later check asks again from the spelling. The earlier ledger row (PARK-5) put it at 19% of
typing time; across the whole compile it is 40% of everything. Evidence: `EAGER_COMPILE_PROFILE_2026_10_07.md` in
`docs/build-inventory/program/evidence/compiler/`.

### 1.2 The stress corpus end to end (new)

`//core:stress_tool` over the stress corpus (202 files, 344,824 lines, 4,745 service tests on DuckDB; D23's base):
4,705 pass, 15 fail, 16 skipped; 3,669 samples. Evidence: `STRESS_PROFILE_2026_10_07.md` (with `jfr_other.py` and
`jfr_callers.py` beside `jfr_phases.py`).

| phase | share | what it is |
|---|---|---|
| other | 33.0% | loading and execution (the CSV seeds, the DuckDB appender), the mapping include closure, SQL rendering, the test runner, Java streams and strings |
| parse | 16.3% | **14.4% is one method, `MappingProtocolParser.readIsland`**; the rest is lexing |
| type | 13.2% | `Typer.synth`, `applyFunction`, `Overloads.checkGeneric` |
| store-resolve | 12.7% | `StoreResolver.resolveChain`, `resolveObject`, `anchoredNode`, `TemporalFrame`, string-keyed binding maps: spread, no single hot spot |
| names | 12.7% | the same frames as 1.1 |
| model | 5.1% | `ModelBuilder.findMapping` resolving the mapping's FQN string on every call |
| lower | 3.7% | |
| inline | 1.6% | |

**The islands.** Every island (`#{ … }#`: test data, external-format blocks, embedded values, assertions) is read by
copying its text into a new string, padded with one newline per line above it and one space per column, so that the
second lexing reports the island's true position; the padded string is lexed and given its own line index. The
comment above the code cites the 2026-08 deep audit's finding that the island path was quadratic and the fix (the
stream's cached line index); the padding kept the cost. Of the method's 528 samples the leaves are the padding loop
(299), the lexer skipping the padding (134) and the line index of the padded string (81). Done right, a nested grammar
is parsed from the same token stream, a slice with the island's bounds and the line index shared, exactly as
`TokenStream.slice` already does for sections; no copy, no padding, no second lexing; spans unchanged.

**The include closure.** `MappingDefinition.withIncludes` walks a mapping's includes transitively every time the
bindings of some classes are asked for (2.5%), and `ModelBuilder.findMapping` resolves the mapping's FQN string
through the symbol table on each call (2.4%, a quarter of it `String.equals`). The closure is a fact of the built
model; identity by spelling is the plan's rule 0b.8's subject (no string identity for a declaration).

**Not a hot spot.** The store resolver's 12.7% and the lowering's 3.7% are spread over many frames (the biggest,
`resolveObject`, is 3.4% inclusive, its leaves its own logic). Nothing in the back half re-derives a whole-model fact
per call the way the names and the islands do. Its cost is its structure: the plan's W4.3 and the D11 question.

### 1.3 The residue taken apart (new)

Of the probe's 1,620 failing bodies, 822 are the engine's JSON protocol serializers (walled: the platform speaks its own
protocol), 428 are test bodies (the roster's), 5 autogeneration; **365 are ours**, 319 of them in `meta::relational`.
By message shape (`residue_shapes.py`; evidence `EAGER_RESIDUE_2026_10_07.md`, the bodies in
`eager-residue-2026-10-07.txt`):

| bodies | what fails | what it is |
|---|---|---|
| 220 | an unknown function | platform functions never ported: the cut list and the manifest world (D9, W4.4a) |
| 29 + 8 | an element or type not known | functions referenced as values by their mangled names; elements and types never admitted (W2.6, D9) |
| 26 | a `validate` call reached the typer | a desugar that runs before typing and misses these forms: desugaring interleaved with typing (W2.3a push 2a, W3.6) |
| 21 | no overload fits | the overload rule and the catalogue (W3.2, W3.3, W1.13) |
| ~15 | "`renameColumns` expects literal pairs", "`generateTestData` needs its lambda INLINE", "`tableReference` expects (database, …)", "`flatten` expects (source, ~column)", "`join` expects …", "`cast` expects …", "`from()` argument must be a reference" | **shape checks**: a checker matches an argument's syntax and refuses the same value written another way; a compiler types values whatever their spelling. The few that need a compile-time value belong inside D8's fence (W4.2); the rest become typed arguments (W3.1, W3.3; `CallShapes` is in W2.3a push 2a's list) |
| 6 | "expected a function-typed parameter, got `LambdaFunction<Any>`" | a lambda typed before its expected type is known: the solver's "reverse inference" rule (W3.3-1) |
| ~10 | a type variable bound wrong, no common supertype, a property on `V` | the solver and the kernel's second half (W3.3, W3.5) |
| 6 | walled by name | by design |

### 1.4 The correctness state, from the plan's own records

The plan's §1b and §3: the type checker agrees with legend-pure on about 72,000 calls (769 differ, the reference
lane's OVERLOAD rows); seven of W0.6's thirteen wrong-answer pushes are done (1, 2, 3, 7, 8, 11, 13); the wrong-rows
tool compared both engines on the seed data (4,686 of 4,729 equal, 22 row disagreements to attribute, GATES "Rebuild
D23 (1)") and on damaged data (D23 (2)); the remaining pushes 9, 4, 5, 5b, 10 are judged by that tool's rows, 6 and 6b
by the reference lane, 12 by its repros; D20 and D21 are open decisions that block one fix each. Today's stress run's
15 failures are that attribution's work list. None of this was in revision 1.

## 2. Each finding, mapped to the plan

| finding (revision 1's number) | in plain words | the plan's item and phase | what today adds |
|---|---|---|---|
| F1 names resolved more than once | the resolver finds what a bare name can mean; every later check asks again from the spelling | **W2.3a** (pushes 2c and 4b switch the 121 name readers to ids and delete `candidateFqns`), D12, W2.1; Phase 4; the bump's PARK-5 | measured: 40% of a whole-world compile, 12.7% of the stress run |
| F2 arguments typed more than once | seven places re-enter the typer on rewritten syntax | **W3.4** (the inliner substitutes recorded instantiations, never re-types), **W4.2** (one hygienic engine over the typed tree replaces six: SourceSubst, UserCallInliner, StaticFold's inlining, AlphaRename, StatementInline, LiteralMapUnroll), the D24 cleanup (the "Now" line), W2.4 (member access, the auto-map rewrite), W2.7; Phase 3 and the cleanup; the bump's PARK-6 | the seven sites confirmed by reading; W1.0b's run will count re-entries per node |
| F3 desugaring interleaved with typing | untyped rewrites inside the typer's call dispatch | W2.3a push 2a (the five rewriters through the query layer), **W3.6** (forms by declaration), W3.3-3 (the checker routes); Phase 4 | the 26 `validate` bodies and the shape checks are its correctness face |
| F4 overloads as a score | a numeric score and five tie-breaks | **W3.2a/b** (the matcher: one lexicographic key over type and multiplicity distances, no score), **W3.3** (lite's own solver, D15, with a shadow period), W1.13; Phase 4; the bump's PARK-7..10; the bump's `FunctionMatch` holds legend-pure's ranking facts | 21 residue bodies |
| F5 type variables by name | a callee's `T` is a string, renamed apart at every call | W3.3-2 keys the inference context by (context, name) as well: **amendment A3** | — |
| F6 scopes copy; lets parked as syntax | `Env.with` copies a map per binding; deferred lets hold raw syntax | W2.5 (`VarId`), W3.3a (type facts before the loop); Phase 4 | the six `LambdaFunction<Any>` bodies |
| F7 errors as strings, positions stop at the function | | **W1.2** in four pushes per the h4 design: (a) Phase 2, (b) to (d) Phase 4 | — |
| F8 probes in the hot path | 39 static hooks, two environment reads in product code | W0.7 (Phase 1), the build program's D8 (delete `probe`), W6.4 | — |
| F9 no compile benchmark | | **W1.0b** (Phase 1 step 4): plan latency p50/p95 over the 69 DataCube-shaped queries in `wasm/corpus/queries.tsv`, `//core:scale`, the reference-lane buckets | not built; today's two profiles are the stopgap: **amendment A2** |
| F10 the back half unmeasured | | W4.3, W3.7 and D11; Phase 3 | measured today: no hot spot; structure |
| F11 the parser must carry spans; the lazy-load guard | | W1.2(b), W1.9; W0.7 | — |
| **F12 (new) islands copied, padded, lexed again** | | not in the plan: **amendment A1** | 14.4% of the stress run |
| **F13 (new) the include closure and string-keyed model lookups** | | W2.1 (world tables), W2.6 (`Ref<Kind>`), D24's "mapping elaboration as one owner": **amendment A4** | 2.5% + 2.4% |
| **F14 (new) shape checks refuse valid Pure** | | W3.1, W3.3, W4.2 and D8's fence: **amendment A5** | about 15 residue bodies plus the 26 `validate` ones |
| **F15 (new) lambdas typed before their expected type** | | W3.3-1's rule table already names reverse inference | 6 bodies |

## 3. What revision 1 got wrong

1. It was written without the plan, so it re-derived W2.3a, W3.4, W4.2, W3.2, W3.3, W2.5, W1.2, W0.7, W1.0b and W4.3
   as "findings" and gave them a new order. The plan's rule 0b.18 ("build, don't re-plan" until C1) exists for this.
2. "F9 comes first" proposed a new benchmark target; W1.0b already specifies one, in Phase 1.
3. "F1 first after F9" put the front end before the middle; the plan's re-cut (approved 2026-09-29) argues the
   opposite from the facts in §1.4, and today's measurement changes the weight of one item, not that argument. §4
   puts the two options side by side.
4. It said the back half was unmeasured and left it at that. Measured today; §1.2.
5. It had no correctness findings. §1.3 and §1.4 are the correctness side; two of today's new findings (F14, F15) are
   correctness, not speed.
6. It did not know the plan's own open decisions (D9, D11, D19, D20, D21), which are the real decisions, all placed
   at C1.

## 4. The one decision: where the rebuild resumes

**Option A, recommended: resume at the plan's "Now" line and keep its order, with the amendments in §5.** The "Now"
line is the D24 cleanup (one substitution engine, unique variable ids on the typed tree, mapping elaboration as one
owner; self-contained in `compiler/spec` and the resolver, no SQL changes), then the rest of Phase 1: the remaining
W0.6 pushes judged by the wrong-rows tool, W1.0b, the D11 experiment (W3.7), W0.7, then C1, where the user rules D9,
D11, D19 to D21 and re-fits every size from logged cost. Why: the plan's reasoning holds (the typer already agrees
with legend-pure on about 72,000 calls; the wrong answers and the worst tangle are in the middle); the items the user
named are the resume point itself and Phase 3; and the one number that is new, the 40%, is a speed number with a
low-risk item behind it, which C1 can move forward with the number in hand (A6). Cost: the names item waits for C1
(roughly eight to twelve sessions of Phase 1 remain, by the plan's sizes and what is done).

**Option B: pull the names item forward.** After the D24 cleanup, run Phase 4's prefix, W2.1 → W2.2 → W2.2b(1) →
W2.3a (eleven to fifteen sessions), before the rest of Phase 1. Why: the 40% is the biggest single number in the
compiler, its semantic risk is low (a name resolves to the same set, computed once), and it makes every later typer
item simpler. Cost: fifteen to twenty sessions on the front end before the wrong rows are attributed and before C1's
decisions; W2.3a's pushes touch the same files as the bump's Phase 3 branch (which waits in either option); and it is
a re-plan before C1, which the plan's rule 0b.18 forbids unless the user rules it.

Revision 1's order (measure, names, one typing pass, overloads, diagnostics) is **withdrawn**: it is Option B plus a
re-ordering of Phase 4 that the plan's catalogue already contains in better detail.

## 5. The amendments to the plan (small; they apply under either option)

- **A1. Islands read from the same stream** (F12): `readIsland` returns a slice of the outer token stream with the
  island's bounds, the line index shared, instead of a padded copy lexed again; the fourteen call sites (three parsers:
  mapping, service stub data, relation islands) keep their spans. A Phase 1 push of at most half a session; gates: the parser-parity lane (spans unchanged), the stress corpus's
  rows, the profile (parse from 16% to about 2% of the stress run).
- **A2. W1.0b runs first**, before the cleanup, and its metrics gain the two phase shares measured today (names of a
  whole-world compile; parse of the stress run), so every landing from here is a number moved, not a claim.
- **A3. Type variables have identity** (F5): W3.3's rule table and solver key the inference context by a fresh
  variable per instantiation, not by (context, name); W2.5 gives binders ids, this gives type variables the same.
- **A4. The include closure is a built fact** (F13): computed once per model in W2.1's world tables; mapping lookups
  by `Ref<Kind>` (W2.6); D24's "mapping elaboration as one owner" names it.
- **A5. Every shape check becomes a typed argument or a fenced evaluation** (F14): W3.1 inventories the family
  (the "expects literal" refusals), W3.3 types the arguments, W4.2's D8 fence takes the ones that need a compile-time
  value; a refusal lite keeps on purpose becomes a `SEMANTICS_REGISTER.md` row, per rule 0b.13.
- **A6. At C1, W2.3a's place is re-weighed with the 40%** (the plan already says every size is re-fitted there); if
  the user wants the names win before the middle, it is the first item after C1.
- **A7. The bump's Phase 3 branch is reference material** for W2.3a (forms by id, `candidateFqns` deleted there too)
  and W3.2 (`FunctionMatch`, legend-pure's ranking rules), not a landing; its five commits are read before those items
  start.

## 6. Decisions (ruled 2026-10-07; the user: "let's do the evidence way")

1. **Option A**, sharpened in conversation: the order after C1 is decided there on the attributed defect list with a stated
   criterion (mostly local resolver fixes → the identity items W2.1, W2.5, W2.6, W2.3a before the middle; mostly structural →
   the plan's order), with the names share in hand.
2. The bump's Phase 3 branch is a reference, not a landing (A7).
3. **The measurement base is the stress corpus and the eager probe; nothing new is built to measure** (the user: "use the
   stress corpus instead of making a whole new thing to measure"). A2 is amended: W1.0b reads its numbers from the stress tool
   (per-service compile latency, phase shares) and the eager probe, not from a new benchmark target.

Recorded in the plan as D25 (its §2), its Now line, W0.8 (new), W1.0b, W2.1, W3.1, W3.3, C1 and the Phase 4 note. Everything
else is the plan's own: D9, D11, D19, D20, D21 at C1.

## 7. What does not change

The output oracle and the gates (the corpus rosters, PCT, parser parity, the reference lane, the stress corpus and
the wrong-rows tool); the plan's rules (§0b: no string identity, no caches before the algorithm is right, no tolerant
modes, net deletion, build don't re-plan); the layer contract (`AGENTS.md`); the no-PR landing procedure with an
audit, the local gate and one CI run per landing; design before code for each item (W2.3a, W3.3 and W1.2 have theirs
in `docs/plan-audit-2026-09-26/`).

## 8. Evidence

`docs/build-inventory/program/evidence/compiler/`: `EAGER_COMPILE_PROFILE_2026_10_07.md` and `jfr_phases.py`
(revision 1's measurement); `STRESS_PROFILE_2026_10_07.md`, `jfr_other.py`, `jfr_callers.py` (§1.2);
`EAGER_RESIDUE_2026_10_07.md`, `residue_shapes.py`, `eager-residue-2026-10-07.txt` (§1.3). The plan's numbers are its
§1b and §3; the gate entries are `docs/GATES.md`, "— Rebuild". The bump's ledger rows PARK-5 to PARK-14 are on the
Phase 3 branch (`docs/PARKED_WORK_LEDGER.md` there); their code anchors were checked today and hold.
