# The reference differential: what the real Pure compiler resolved, call by call

`RefResolutions.java` compiles every module on the classpath with the real Pure compiler (the
interpreted runtime inside legend-engine's shaded server jar, over the jar's own `.pure` sources
and manifests), then writes one row per function-call expression in every function body under
the given source-id prefixes: source id, line, column, the spelling written, the resolved
function's declared name and its signature id, and the enclosing function's.

It is the oracle for name binding and overload choice: our compiler's answer for the same call
is right when it equals this, and every difference is either our bug or an upstream fact we did
not know. `spec/.../OurResolutionsTest` produces our side in the same shape
(`-Dour.resolutions=<module>`); the join is by enclosing function and spelling.

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

## What it found on 2026-09-25 (receipts in the study directory, `receipts/reference-differential/`)

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
included — is step D's first item and the binder's acceptance test is this differential at zero.
