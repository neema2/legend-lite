# Phase 2b: the end-state experiment (2026-10-06)

The question: once upstream's own declarations are in the world, does any call bind to an overload we do not
implement? Today's world drops every upstream declaration at a name the catalog implements (by name); the end state
keeps them all and lets the implementation table decide.

## The extra overloads (ids the catalog lacks, at names it implements)

| Scope | Extras | Native (no body) | With a Pure body | Names |
|---|---|---|---|---|
| upstream core (platform* + core_functions_*) | 45 | 6 | 39 | 27 |
| files the S1 world contains | 117 | 6 | 111 | 55 |
| all of upstream | 184 | 6 | 178 | 74 |

The 6 natives: `eval` with 7 arguments, the variant `to` and `toMany` with 4, `collection::get(T[*], String)`,
`newMap(Pair[*], Property[*])`, `loadCsvToDbTable` (`extras_core.tsv`; `extract.py` and `Extras2b.java` compute it,
the ids by our own `SignatureMangle`).

## Results (the S1D world plus upstream core's 45, against today)

- User side (`CallIds2b.java`: 56 projects and 3 demos, every body compiled, every call's resolved overload): the
  same 1,703 bindings in 189 bodies; the same walls.
- Corpus (`run_corpus_pct.sh`, experiment 6's runner): every pass changed; newly failing tests only (DuckDB host:
  the engine's `toPostgresModel` tests; H2 host: `testExplodeReturnTypeAcceptedNames`; both warehouse passes).
- PCT: 13 of 17 suites identical; the essential suites (DuckDB, Postgres, channel B) and channel B's unclassified
  fail: 8 errors "upstream native 'collection::get' is declared by the spec and not implemented", and channel B's
  indexOf off by one.
- Control (the S1D world alone, this commit): identical.
- The S1D world plus the 44 extras without `get(T[*], String)`: all six corpus passes and all 17 PCT suites
  identical. One overload explains every change.
- The faithful 117 (engine files included) does not boot without closing over their dependencies (`Runtime`,
  `ConnectionStore`): Phase 4's closure work.

## The cause: overload ranking

Two upstream natives share `collection::get`: legend-pure's map lookup `get<U,V>(m:Map<U,V>[1], key:U[1]):V[0..1]`
(ours) and the engine's `get<T>(set:T[*], key:String[1]):T[0..1]`. For `$map->get('k')` both match.

- legend-pure (`FunctionExpressionMatcher.getBestFunctionMatch`, `FunctionMatch.compareTo`) ranks
  lexicographically: type matches parameter by parameter, left to right, then multiplicity matches; the first
  parameter that differs decides. The first parameter (a Map against Map versus against T) picks the map lookup:
  381 of legend-pure's 387 `get` calls in upstream code bind to it (the reference dump).
- Our compiler (`InferenceKernel.resolveOverload`) SUMS a specificity score over all parameters: the engine's
  version loses the first parameter and wins the second (String exact versus the type variable U) by more, so it
  wins. Today's name filter hides the engine's declaration, so the difference never showed.
- The reference lane already counts 745 calls where our choice differs from legend-pure's (`OVERLOAD`, e.g.
  `isEmpty` 523); they do not change answers. Same root.

## What it means for Phase 3

1. Rank overloads as legend-pure does (`FunctionMatch`), measured against the corpus, PCT and the reference lane
   (the `OVERLOAD` class should close).
2. The name-based suppressions go (seen here: "platform-owned function 'loadCsvToDbTable': 1 user definition(s)
   suppressed").
3. A call that binds to an upstream native we do not implement already fails loudly ("declared by the spec and not
   implemented"); the implementation table keeps that.
