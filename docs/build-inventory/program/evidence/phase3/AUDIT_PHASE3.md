# Audit: build rebuild Phase 3 (`build/phase3`, 293318dda..bc0de1f70), 2026-10-07

Auditor: independent, read-only (no edits, commits or pushes to the repository). Wrapped up early at the
coordinator's request: items not finished are marked **not verified**.

## Verdict

**Ready after fixes.** One blocker: the new ranking picks the wrong overload when one candidate takes a
function-typed parameter and another takes a type parameter or `Any`. I reproduced this end to end: a valid program
that compiles on main fails at the tip. The fix is small. Six should-fix items follow: one is performance (+18% in
the typing phase, almost all from one uncached lookup), and the rest are stale claims, unrecorded changes to the
plan, and small risks in the port. The corpus claim holds: every result file of all six passes is identical to
main's own outputs, not only to the baseline built from the experiment code.

Counts: **1 blocker, 6 should-fix, 5 nits.**

## Blockers

### B1. A function-typed parameter now ranks after a type parameter and after `Any`. This is a regression from main and does not match legend-pure.
- Where: `core/src/main/java/com/legend/compiler/spec/InferenceKernel.java:1315-1323` (`typeFit` unwraps the
  `Function<{...}>` / `FunctionDefinition<{...}>` carrier and recurses), `:1345` (an unwrapped function type becomes
  `Kind.FUNCTION`), and `FunctionMatch.java:73` (`Kind` order `SIMPLE, NON_CONCRETE, RELATION, FUNCTION, ...`).
- How m3 does it (`GenericTypeMatch.newGenericTypeMatch`, `TypeMatch.newTypeMatch`): a `Function<{...}>` formal
  against a `LambdaFunction<{...}>` value is a **SimpleTypeMatch** on the raw carrier classes. The function-type match
  sits *inside* the type arguments. A Simple match beats NON_CONCRETE (a type parameter) and beats `Any`'s larger
  distance. The port compares the unwrapped function type at the top level as `FUNCTION`, and `FUNCTION` ranks after
  NON_CONCRETE. That reverses the order.
- Main's additive score got this right: a function formal scored 1, a type variable or `Any` scored 0.
- Evidence (my probes, using the tip's and main's jars from their execroots; scripts in `audit_tmp/probe/`):
  - `FromProbe.java`, catalog `meta::pure::mapping::from` on `(LambdaFunction<{->Integer[*]}>[1], Runtime[1])`:
    - tip chooses `from(t:T[m], runtime)` and returns the function type itself;
    - main chooses `from(func:FunctionDefinition<{->T[m]}>, runtime)` and returns `Integer[*]`.
  - `FromE2E.java`, `function test::viaParamToOne(q: FunctionDefinition<{->Integer[*]}>[1]): Integer[*] { $q->from(^meta::core::runtime::Runtime()); }`:
    - main compiles it (0 walls);
    - tip fails: "declares return type Integer but body returns { -> Integer[*]}". A `->map(x|$x + 1)` variant fails
      too: "no common supertype for { -> Integer[*]} and Integer".
  - `FnProbe.java`: `f<T>(T)` beats `f(Function<{T[1]->Boolean[1]}>)`, and `f(Any)` beats it too, for both a
    `LambdaFunction<...>` value and a bare function-type value.
- Reach: a scan of the catalog and the upstream Pure sources for overloads of equal arity where one has a function
  formal and another has `T` or `Any` at the same position found:
  - `meta::pure::mapping::from`, 2- and 3-argument versions (the 3-argument `FunctionDefinition` version is upstream
    only);
  - the private `meta::pure::router::preeval::printDebug`.
  A lambda literal is unaffected: it is typed later and ranks as untyped. So is a let-bound lambda passed to a form
  (`CallShapes.expandLetBoundLambdaArgs`). A typed function value is affected: a parameter, a function-valued
  expression, or a property. The corpus, PCT and the reference lane do not exercise this, which is why the gate is
  green.
- Fix: when `typeFit` unwraps a carrier formal, return the m3 shape instead of recursing at the top level. That is,
  `TypeFit.simple(<distance of the formal's carrier in the value's carrier linearization, 0 for a bare function type>)
  .withArguments(List.of(typeFit(nf, na, anyConcrete)), List.of())`. Add a kernel test for the `from` pair above and
  the end-to-end case. Re-run the reference lane and the six corpus passes.

## Should-fix

### S1. Performance: `ResolvedNames.form` recomputes `BareNames.catalog` on every form check of a bare name.
- Where: `ResolvedNames.java:57` (`form`) → `:24-34` (`referents` copies `candidateFqns` and calls
  `BareNames.catalog(name)`) → `BareNames.java:115`, which builds strings for every core-import package, a `TreeSet`
  of the form's owned names, and a hash lookup per name, with no memo. The 21 rewritten sites call it, several per
  node: `Typer.applyFunction`, `DeferredArgs.isOverCall` per parameter, the CAST and LET probes, and `SortChecker`.
  `TdsLegacy.matches` adds its own reads.
- Measured: see Performance. `BareNames.catalog` is +193 of the +198 extra profiler samples, and typing is +18%.
- Fix: memoize `BareNames.catalog` / `tiered` per name in a static `ConcurrentHashMap`; the inputs are static
  tables. Or add a fast path in `form`: return `CoreFn.of(...)` / empty without computing referents when the bare
  name is not the simple name of any FQN in `CoreFn.OWNS`.

### S2. Stale claims in the GATES entry, IN_FLIGHT and the plan's Status bullet.
- `docs/GATES.md:6808-6813` says "Bodies: 50 newly typed, 13 newly failing … `testViewChainsWithBusinessDate`
  (`toSQLString` over an `SQLResult`)". At the tip, comparing the golden's failed-bodies sets, it is **51 newly typed,
  12 newly failing**. `testViewChainsWithBusinessDate` types again (the dot-call rule in commit 4). The table was
  updated for commit 4 (1,469) but this sentence was not.
- `docs/GATES.md:6783-6784` says "a shrink-only ratchet, 110". The final value is 109 (the later bullet explains,
  but the first statement reads as final).
- IN_FLIGHT (main, c2f0a2728) says "26 versions get rows". It is 27: 293318dda added the `executeInDb` family line
  but not the count.
- Plan Status bullet (`origin/docs/bazel-first-class-plan`, REBUILD_PROGRAM "Phase 3" Status) says "three commits",
  "AGREE 73103 -> 74578", "bodies … 1508 -> 1471". The branch has four commits, AGREE 74586 and 1469 bodies. The
  bullet also omits PROPERTY_AS_CALL 39 -> 1, the corpus account (four tests broken by step 2, then fixed), and the
  three commit-4 changes.

### S3. Two parts of the agreed plan were done differently and the change is not recorded.
- The plan says "`TdsLegacy`'s 17 functions get rows by id" and "`GroupByChecker` recognizes `agg` by resolved id".
  What was built: `TdsLegacy.matches` (`TdsLegacy.java:62-70`) reads the resolver's `candidateFqns`, falling back to
  the spelling. That is recognition by resolved name. No implementation-table rows were added for these functions.
  This may well be the right call, but neither the plan's Status bullet nor GATES says the plan was changed.
- The plan says "The boot layer's own versions of 29 upstream names get rows (ours runs)". What was built:
  `SystemMetamodel.shadows` by id (`SystemMetamodel.java:1480-1495`), and the Status bullet records "about 12
  upstream versions … run upstream's body as before" as open. GATES item 3 does not mention that open item.
- Check "as before": the old `shadows` dropped a graph function with the same parameter types whatever its
  multiplicities and return type. The new one requires the same function id. A same-types version with another
  multiplicity or return type used to be shadowed and is now kept, so its upstream body can run. I did not verify
  which of the ~12 are affected, so "as before" is not verified.

### S4. Port simplifications not listed where the port is described.
The FunctionMatch javadoc and GATES call it a port of m3, but these departures are not stated as such:
- A bare relation-type formal ranks `RELATION` with no column comparison (`InferenceKernel.java:1348`); m3's
  `RelationTypeMatch` compares column types and multiplicities.
- A `SchemaAlgebra` formal ranks `NULL` (`:1349`); m3 gives a type operation (no raw type) a NON_CONCRETE match.
- Type arguments are compared position by position, and only when the argument counts agree. m3 maps them through
  the class hierarchy (`resolveClassTypeParameterUsingInheritance`). The javadoc mentions this one.
- `contravariantTypeFit` ranks a parameter m3 would reject as `simple(1)` (`:1455`). That value is arbitrary;
  `PLATFORM_RULE_DISTANCE` would be the consistent choice.
- m3's literal order breaks ties only in `resolveOverload`. In `rankNonLambda` (`:1226`, through
  `Overloads.selectRankedByPresentArgs`) declaration order breaks them. This is not a regression: the old score tied
  there too. But GATES' "m3's literal order settling a remaining tie" reads as general.

Fix: a short "known departures" list in the FunctionMatch javadoc and a sentence in GATES. B1 is the one departure
that needs a code change.

### S5. The dot-call rule types the receiver twice when no qualified property matches.
- Where: `Typer.java:501`. The new condition sends **every** dot call with parameters into the property branch,
  which types the receiver (`synth`). If no derived or milestoned property matches, it falls to
  `applyGeneric(af, env)`, which types the receiver again.
- Before, this happened only when no function of the call's arity existed. `Overloads.checkGenericTyped`'s own
  comment warns that "a second synth re-registers typer state: TDS literals, plan params".
- The profile showed no cost on the corpus (Typer.synth samples went down), so the risk is correctness when a
  receiver registers state, not speed.
- Fix: pass the already typed receiver to the generic path, or check for the property before typing the receiver.
  The rule itself is principled (see the assessment below).

### S6. The corpus "identical, every output file" wording is slightly too strong.
- Every **result** file is byte-identical: the six ledgers, the rosters, the registers and the verdicts. I checked
  against `judges_base` and also against main's own outputs (see Verified).
- The logs differ, as expected: timings, and the removed "suppressed" lines.
- The SQL the executor sends differs by about +2.2K characters per pass with the same statement counts. I dumped the
  SQL (`-Dlegend.diagnostics=dump-sql`) on main and the tip:
  - H2 host pass: exactly 5 statements differ, and only in the reflection rows' function ids
    (`'fn:d32d125:8843'` → `'fn:dcbd61a5:8851'`), consistent with a few more functions in the world;
  - DuckDB host pass: 6 statements differ; after normalizing those ids, the one left differs only by a random
    `executionTraceID` UUID.
  This is benign.
- Fix: say "every result file" and note the id renumbering.

## Hacks and shortcuts: assessment

- **`Any` ranked with the type parameters, and an `Any`-typed argument non-concrete in the primary ranking**
  (`InferenceKernel.java` `match(…, false)`, `typeFit`, `nominalTypeFit`). Kept to one flag, documented, and measured
  on the reference lane (relation::in vs collection::in). Plan 3b records it as a known soft spot. In practice only
  an `Any` formal is affected: acceptance admits an `Any` argument only for `Any` and type-parameter formals.
  **Acceptable recorded deviation.**
- **m3's literal order as the last tie-break in `resolveOverload` but not in `rankNonLambda`.** No regression
  (old scores tied there too). Documented in the javadoc; GATES wording overstates it (S4).
  **Acceptable; fix the wording.**
- **`PLATFORM_RULE_DISTANCE`.** Ranks fits that only lite's acceptance admits and m3 has no equivalent for (a
  relation into `TabularDataSet`). Kept to one place and documented. **Principled.**
- **The kept tie-breaks.** Same-shape duplicates (first wins), native over module, most specific, Nil narrowing, then
  literal order instead of the old breadth-first `nearestInLinearization`. All were there before. I argued the
  following and did not test it: the new ranking creates no new ties, because equal match vectors mean equal old
  per-parameter scores. **Principled; recorded as a soft spot in plan 3b.**
- **The StatementInline guard restored** (commit 4). The net diff from main restores main's first check unchanged and
  rewrites its comment. The second check changed from "not if the row is a rule or form" to "only if the row is
  Body". So walled, subsumed, unimplemented and undeclared rows are no longer inlined. That matches the `Subsumed`
  contract ("its body is never spliced"), so it is safer. The comment's "nor a version … it refuses (no row)" case
  cannot be reached, because the first check already excludes every catalog FQN. **Principled.**
- **The dot-call property-first rule.** Matches m3: in `FunctionExpressionProcessor.matchFunction` a dot call with
  parameters is a `qualifiedPropertyName` and never searches the function library. Lite is more lenient (it falls
  back), which is fine. `->` calls are unchanged. Forms and TDS functions are dispatched earlier (`Typer.java` around
  `:458`), so this branch cannot misroute them. **Principled**, apart from the double typing (S5).
- **The executeInDb(String, ConnectionStore) row.** The family arm reads only the SQL and never evaluates the
  connection (`StatementExecutor.java` around `:2839`, the executeInDb doc comment). **Principled.**
- **Probes, debug output, local paths, binaries.** None added. The diff only removes the two suppression
  `System.err` lines, which went with the drops.
- **Pins and ratchets.** Every move is downward and dated: DynaFn 162→142; `UNROWED_MAX` = 109 is new; the identity
  pins were lowered. The `SubsumedRegistryTest` platform-owned clause was deleted with a dated note; it means nothing
  once no name is owned. **Principled.** The pin comments lost their history (nit N1).
- **Deferred work.** The boot-layer open item is recorded in the plan only; the TdsLegacy and `agg` rows are not
  recorded (S3).

## Performance

Method: same machine, same session, main's jars versus the tip's, runs alternated. Main's jars come from an export of
293318dda (see Artifacts), built from Bazel's disk cache. Each pass was rerun outside Bazel from its own execroot, with
outputs sent to a scratch directory.

| Measurement | main | tip | Change |
|---|---|---|---|
| Eager corpus compile, `typeAll` (EagerCorpusCompileProbe, 6 alternated pairs) | 2030, 2051, 2136, 2218, 2231, 2217 ms; mean **2,147** | 2305, 2494, 2526, 2606, 2623, 2642 ms; mean **2,533** | **+18%** (tip slower in every pair) |
| Eager compile, `build` (world compile) | about 1.9 s | about 1.9 s | none |
| Bodies typed / failed in that run | 9,942 / 1,620 | 9,976 / 1,630 | +0.3% bodies |
| Corpus host pass, H2 (4 pairs) | mean 24,859 ms | mean 24,840 ms | none |
| Corpus host pass, DuckDB (3 pairs) | 38,473, 38,255, 38,155 ms; mean 38,294 | 38,803, 40,111, 40,067 ms; mean 39,660 | **+3.6%** |
| All six passes run by Bazel (concurrent, different times; confounded) | judges_base 498,608 ms; main's cached outputs 496,576 ms | 513,123 ms | +2.9% / +3.3% |
| `wasm/planner/classes.wasm` | 4,666,502 B | 4,677,707 B | +0.24% |
| `sdlc-server/page/classes.wasm` | 3,301,736 B | 3,312,694 B | +0.33% |
| `core/libcompiler.jar` | 894,337 B | 905,197 B | +1.2% |

Where the time goes (JFR, 1 ms sampling, one eager-compile run each):

| Method (inclusive samples) | tip | main |
|---|---|---|
| All samples | 2,400 | 2,202 |
| `BareNames.catalog` | **455** | **262** |
| `ResolvedNames.referents` | 288 | 138 |
| `ResolvedNames.form` (new) | 74 | — |
| `DeferredArgs.isOverCall` | 17 | 2 |
| `TdsLegacy.matches` | 23 | 14 |
| `InferenceKernel.match` + `rankNonLambda` / old `score` + `scoreNonLambda` | 20 + 8 | 11 + 7 |
| C3 `Linearizer` | not visible | — |
| `FunctionCompiler.functionsAt` | 2 | 8 |

Heap lines in the judge logs show no material change.

Bottom line: the slowdown is real and modest. It is about 18% of typing time in compile-heavy runs and 0 to 4% end to
end in corpus passes. Nearly all of it is `BareNames.catalog`, recomputed by the new `ResolvedNames.form` at every
form check (S1). The new ranking itself (FunctionMatch records, C3 memo per kernel, two passes) costs very little:
about 10 more samples in the profile. `FunctionCompiler.functionsAt` is cached through `PureModelContext.findFunction`
(its HashSet runs once per name per context) and is not a factor. `ImplementationTable`'s `catalogFqns` is built once
per table.

Not measured: core test.xml durations (there is no fair main baseline), compile time in the browser runtime, and the
server.

## Nits (one-line fixes)
- **N1.** `IdentityGuardrailTest.java` pins `CATALOG_LOOKUP_BY_NAME`, `FAMILY_LOOKUP_BY_NAME`,
  `FUNCTION_CATEGORY_CHECK` and `NAME_CUTTING`: the new dated comments *replaced* the earlier dated history instead of
  adding to it. Keep the old notes, for example NAME_CUTTING's T4a reason.
- **N2.** A NO_ROW refusal reaches the user as `WalledBodyException("walled body '…': the version … has no row …")`
  (`UserCallInliner.java:315`). The label calls a missing row a wall; say "no row" for that reason.
- **N3.** The `FunctionMatch.Linearizer` javadoc says "a ranking never throws". But `generalizationsOf` →
  `ctx.findClass` can throw for a class that fails to compile in a tolerant build, where acceptance (`isSubtype`)
  tolerated it. Catch it there and treat the class as having no generalizations.
- **N4.** `ImplementationTable.java:191` gives NO_ROW's message ("a function the platform implements") to any bodied
  version at a catalog name. That includes a program's twin of one of the 71 *Unimplemented* natives, which the
  platform does not implement. In the spec table no such case exists (see Verified), so this is wording only.
- **N5.** `StatementInline.java` (around `:221`): the comment on the second check names a "no row" case that the
  first check already makes impossible.

## What I verified clean
- **Acceptance unchanged.** `match()` rejects exactly when `paramTypeScore < 0 || paramMultScore < 0`, as `score()`
  did. Null (untyped) arguments get `NULL` fits, as before when they were skipped. In `selectRankedByPresentArgs`, a
  rejected or wrong-arity candidate (`null` rank) sorts last, ties go by declaration index, and the comparator is
  consistent, which preserves the old -1 behavior.
- **Port fidelity, apart from B1 and S4.**
  - `FunctionMatch.compareTo`: all type matches left to right, then all multiplicity matches.
  - TypeFit order SIMPLE(distance) < NON_CONCRETE < RELATION / FUNCTION < BOTTOM < NULL (m3's comparator; m3's own
    Relation-vs-Function comparison is asymmetric and never meets in practice).
  - MultFit order EXACT < NON_CONCRETE < concrete (upper gap, then lower gap) < NULL, with a multiplicity-parameter
    value fitting only as the widest concrete match.
  - `compareLists`, the C3 merge, `Any` appended, the cycle fallback.
  - Primitives are classes in lite's model: Integer→Number = 1, →Any = 2, StrictDate→Date = 1 (RankProbe).
- **Corpus baseline adequacy.** Building main's six judges in a fresh output base took all 164 actions from the disk
  cache: outputs keyed by main's exact inputs. Their result files equal both `judges_base` and the tip's fresh outputs
  (I rebuilt the six judges at bc0de1f70, because the earlier outputs were from a slightly different tree). The
  "identical" claim therefore holds against main itself.
- **Reference lane golden.** It matches the GATES table: AGREE 74586, OVERLOAD 58, DRIFT 0, PROPERTY_AS_CALL 1,
  bodies FAILED 1469. The only disagreement class new at the tip is OVERLOAD `average` (Number vs Integer, 3 calls),
  and GATES documents it.
- **NO_ROW.**
  - The 109 unrowed ids include no catalog id: I recomputed the 864 catalog ids from native-claims.tsv.
  - Every NO_ROW version sits at a name whose catalog rows are all Intrinsic.
  - There is no name where the platform implements nothing and versions are still refused.
  - The ratchets add up: Body −136 = 109 newly NO_ROW + 27 newly Intrinsic.
- **The 27 new rows.**
  - Their signatures match upstream, and the generated `Pure.java` / `native-claims.tsv` / `offer-facts.ts` diffs look
    machine-made.
  - The generated `AT_*` groups absorb them in Scalars, Aggregates, LiteralUnroll and the resolver. `Aggregates`'
    BOOL_AND/BOOL_OR registration by group is right: the 2-argument `and`/`or` live at other names.
  - The `[0..1]` Boolean comparisons get upstream's empty→false behavior through
    `NullSemantics.optionalOperandGuards`.
  - The `executeInDb` arm reads only the SQL.
- **The 21 rewritten sites.** Each passes the same `AppliedFunction` whose name it read before.
- **`ResolvedNames.form` and `TdsLegacy.matches` by case:**
  - a full name;
  - a bare name with candidates;
  - a bare name without (falls back to the spelling);
  - lite `INTERNAL_DESUGAR` names, still refused when written bare;
  - the mixed case, where a form wins and `ReceiverOwnedFunctions` decides by the receiver's type, as before.
- **IN_FLIGHT** lists all 44 files the four commits touch.
- **No local paths or binaries** in the committed diff.

## Not verified
- Empty-input behavior of each of the 27 rows against upstream's bodies, beyond the comparison guard and the shared
  lowering. Not executed.
- That the generated files match their generators byte for byte. I relied on the gate's 306/306 and did not rerun
  the generators.
- Whether "as before" holds for the ~12 boot-layer versions (S3).
- PCT suites: not rerun.
- Linux and Windows: not run. The diff adds no platform-specific code, and its new hash maps are used for lookups
  only, never for output order.

## Artifacts left in place
- `runs/homework/phase3x/audit_main/`: a `git archive` export of 293318dda (**not** a git worktree). Its Bazel output
  base is `<output_base>`; that server is shut down.
- `runs/homework/phase3x/audit_tmp/`: the A/B scripts (`ab_run.sh`, `eager_run.sh`, `eager_jfr.sh`), probes
  (`probe/*.java`), scans, JFR recordings and all run outputs.
- I rebuilt the worktree's `bazel-bin/spec/judge_*` at bc0de1f70 (fresh outputs; result files identical).
- Small helper scripts in the session scratchpad (`catalog_ids.py`).
