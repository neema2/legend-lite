# The reference differential: what the real Pure compiler resolved, call by call

`RefResolutions.java` compiles every module on the classpath with the real Pure compiler (the
interpreted runtime inside legend-engine's shaded server jar, over the jar's own `.pure` sources
and manifests), then writes one row per function-call expression in every function body under
the given source-id prefixes: source id, line, column, the spelling written, the resolved
function's declared name and its signature id, and the enclosing function's.

It is the oracle for name binding and overload choice: our compiler's answer for the same call
is right when it equals this, and every difference is either our bug or an upstream fact we did
not know. `spec/.../OurResolutionsTest` produces our side in the same shape
(`-Dour.resolutions=<module>`, run as
`bazel test //spec:spec_tests --test_env=JAVA_TOOL_OPTIONS=-Dour.resolutions=core_relational
--test_arg=--select-class=com.legend.generators.OurResolutionsTest`; the dump lands in the lane's
`test.outputs/`). Since 2026-09-26 (execution plan step 1) the join is CALL BY CALL, by the
call-name token's (source, line, column): `join.py`'s docstring has the rules, `source_drift.py`
marks the sources whose text differs between the jar's version and our pinned trees (241 of
1,218: a position there means nothing), and the docstring names the two spellings that need
their own rule (operator runs, property reads).

## Running it (no build system, no install)

The engine jar and a JDK are already on this machine (see the session memory
"upstream-engine-no-install"): the jar at `~/legend/engine-dist/legend-engine-server-4.138.5-shaded.jar`
(4.138.5 is the newest release that publishes a shaded jar; our pin is 4.145.0, so a few
declarations differ — the join tolerates that by keying on names, never on line numbers), and
JetBrains' bundled JDK at `/Applications/WebStorm.app/Contents/jbr/Contents/Home/bin`.

```
JB="/Applications/WebStorm.app/Contents/jbr/Contents/Home/bin"
J=~/legend/engine-dist/legend-engine-server-4.138.5-shaded.jar
"$JB/javac" -proc:none -cp "$J" -d out tools/reference/RefResolutions.java
"$JB/java" -Xmx12g -Xss16m -cp "$J:out" RefResolutions ref-resolutions.tsv /core_relational/ /platform/ /core_functions_ /core/
```

Loading and compiling the whole system takes a few minutes and ~10 GB. `loadAndCompileCore()`
must precede `loadAndCompileSystem()` (the M3 bootstrap creates the Root package the system
compile walks).

## What the positional join found on 2026-09-26 (receipts: `join-positional-pre-step2.txt`, `source-drift-4.138.5-vs-4.145.0.tsv`)

Functions: the reference typed 11,973 names, we 16,101 (1,480 failed); 11,480 in both; 493 the
reference typed that we never saw (the 32 walls); 1,319 the reference typed that we FAILED. Calls
in bodies both typed, outside drifted sources: AGREE 42,589; OVERLOAD 450 (`isEmpty` 283, `average`
32, `elementToPath` 24, `max` 21, `hasGeneratedMilestoningPropertyStereotype` 34, `min` 18,
`stdDevSample` 10, `median` 8, `or` 6, `sum` 5, the rest ≤2); PACKAGE 11 (`size` 8 — our TDS
erasure at typing time, `homework-2026-09-26.md` §1; `divide` 3 — an operator-span artefact);
SOURCE_DRIFT 52,266; ABSENT 42,174 (what the reference has as calls and we as nodes or rewrites:
`letFunction` 10,956, `map` 4,490 of which most are the automap rewrite, `new` 4,259, `cast` 2,383,
`getAll` 1,958, `if` 1,854, `eval` 1,548, `filter` 1,486, `project` 1,264, `extractEnumValue` 1,205,
`colSpec` 1,162, `plus` 946 from run splitting, `match` 716 …); PROPERTY_AS_CALL 31
(`connectionByElement`, a qualified property we model as a function — step A4); EXTRA 12,971
(calls of ours the reference has none for: `toOne` 2,504 and `elementToPath` 1,518 are calls our
typer INSERTS — a finding for steps 3 and 6; `string::plus` 1,001 run splitting; `isEmpty` 620,
`equal` 369 …). Every OVERLOAD row is an instance of the reference's match orderings
(`docs/plan-audit-2026-09-26/reference-matching.md` 7–10); the acceptance test for step 3 is
OVERLOAD 0 and PACKAGE 0 with the `divide` artefact explained or fixed in the parser's span.

## What the first (name-keyed) join found on 2026-09-25 (superseded; receipts in the study directory, `receipts/reference-differential/`)

Over 10,161 function bodies both compilers type: the reference resolved 66,608 calls to exactly
the overload we chose; 28 to a function in a different package (`sort` and `distinct` on relations
and TDS, `size` of a relation, `plus` on strings vs numbers, `contains` on strings vs collections);
799 to a different overload of the same function — 507 of them `isEmpty`, where the reference
picks the `[0..1]` overload and we the `[*]` one, and the rest the same shape: the reference
prefers the overload whose multiplicity and numeric type are closest to the argument's
(`max(Integer[1..*])` over `max(Integer[*])`, `average(Integer[*])` over `average(Number[*])`,
`between(DateTime…)` over `between(Date…)`). 104,399 reference calls have no call row on our side:
property reads, `let`, `new`, `cast`, `if`, `match` and the forms (`map`, `filter`, `project`,
`groupBy`, …) are typed as nodes, not calls, so the join cannot yet check which declaration a form
resolved to — that is what binding forms to declarations (plan steps A and B) makes checkable.

## The reference's overload rule (legend-pure `FunctionExpressionMatcher` / `FunctionMatch`, pinned tree)

For every candidate with the right parameter count, a `FunctionMatch` holds one `GenericTypeMatch`
and one `MultiplicityMatch` per parameter (the argument's type against the parameter's, the
argument's multiplicity against the parameter's). Candidates are ordered by comparing, left to
right, ALL the type matches first and only then ALL the multiplicity matches; the smallest wins;
several smallest is a compilation error, "Too many matches", listing them. Our kernel ranks
differently (it does not prefer the closer multiplicity or the closer numeric type), which is the
whole of the 799 overload disagreements above. Reproducing this rule — the two match orderings
included — is task #47, the first thing done before step A (`docs/A1_HOMEWORK_2026_09_25.md`
has the orderings written out and the change list); the binder's acceptance test is this
differential at zero.

## The implicit import group (`RefImports.java`)

`RefImports.java` runs the same way as `RefResolutions.java` and prints, for every source the
reference compiled (2,108 on 2026-09-25), the packages its import group makes visible. Twenty-nine
packages appear in every source: the implicit imports the Pure parser adds to each section. Our
generated `NameResolver.CORE_IMPORTS` is those 29 plus three the newer engine added. So a bare
name in Pure source (the corpus included) resolves by the section's imports and this group alone;
the engine's handler surface is a rule for engine input, not for Pure source. The list is in the
homework note.
