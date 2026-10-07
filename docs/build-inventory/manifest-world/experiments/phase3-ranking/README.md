# Phase 3 pre-experiment: legend-pure's overload ranking (2026-10-06)

The question: what changes if the compiler ranks overloads the way legend-pure does (`FunctionMatch.compareTo`:
the parameters' type matches left to right, then their multiplicity matches left to right; the first difference
decides), in place of today's sum of per-parameter scores?

The change, on branch `exp/overload-ranking` (not landed): `InferenceKernel.resolveOverload` compares each
candidate's vector of per-parameter scores in that order. The per-parameter scores themselves are today's
(type exact 2 / subtype 1 / type variable or `Any` 0; multiplicity exact 5 down to 0). `run.sh` ran it behind a
JVM switch; the full local gate ran with it made the default, so every target that links the typer reran,
the browser build included.

## Results

| Run | Old rule | New rule |
| --- | --- | --- |
| Local gate (`//gates:local`, 290 targets; 136 reran) | all pass | 289 pass; 1 unit test fails (below) |
| Corpus, six judge passes | baseline | identical outputs |
| PCT, 17 suites | all pass | all pass |
| Reference lane (core_relational, call by call against legend-pure) | AGREE 73103, OVERLOAD 745 | byte-identical report |
| Phase 2b world (upstream core's 45 extra overloads), corpus host passes | identical | identical |
| Phase 2b world, the 4 PCT suites | fail (`get`) | pass |

The one failing test, `InferenceKernelTest.overload_incomparableSignaturesAreAmbiguous`, asserts the old rule's
tie: `f(Integer, Number)` against `f(Number, Integer)` on `(Integer, Integer)` is an error. legend-pure picks
`f(Integer, Number)`, because the first parameter decides. The test changes with the rule.

## The 745 OVERLOAD rows are not the ranking rule

Phase 2b's note said the 745 shared the `get` problem's root. They do not: the new rule moved none of them.
`classify.py` splits them by whether legend-pure's pick exists in our world at all (`overload_split.txt`):

- **617: legend-pure's pick is not in our world.** The compiler drops every upstream definition of a name the
  platform implements ("PCT.function 'X' suppressed (native is the definition)"), so only our built-in overloads
  remain. `isEmpty` (523): upstream has `isEmpty(Any[0..1])` written in Pure and the built-in `isEmpty(Any[*])`; we
  keep only the second, so `$context->isEmpty()` on an optional value cannot bind as legend-pure binds it. Also
  `average(Integer[*])`/`(Float[*])`, the `[1..*]` versions of `max`/`min`/`stdDev*`/`or`, `median`. Phase 3's
  removal of the name-based suppressions is what brings these back.
- **128: both overloads exist; we choose another.** 79 are class-hierarchy choices
  (`hasGeneratedMilestoningPropertyStereotype(Function)` against `(ElementWithStereotypes)`, `elementToPath(Type)`
  against `(PackageableElement)`, `propertyMappingsByPropertyName`), where legend-pure measures the distance in the
  type hierarchy and we have only "subtype" plus tie-breaks. 49 are numbers and optional values (`greaterThan` on a
  `[0..1]` argument, `sum`/`plus`/`times` on `Number` against `Integer`/`Float`), where we likely type the
  argument differently from legend-pure; not checked call by call.

## Reproduce

`run.sh` from the worktree root (it needs the Phase 2b harness under `runs/homework/`). The reference lane's report
does not see a `--test_env` switch (the test reads the report built by `//spec:reference_lane_report`); run that
action by hand with the switch added to its params file, with the two checkout roots pointed at the real
directories (the census's file walk does not follow the execroot's symlinks), then
`python3 classify.py <dir with reflane.log and reflane/core_relational.txt>`.
