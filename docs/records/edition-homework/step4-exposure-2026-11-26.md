# Step 4's exposure: where lite's own code compares across types outside the query (measured 2026-11-26)

*Measured by a separate session (measure only: nothing committed or pushed) on base `7647731`, for step 4 of
`docs/records/edition-plan-to-finish-stage-b-2026-11-25.md` ("lite is strict inside functions and mapping code": "the
list of what breaks is ruled"). This is the session's report as it sent it, lightly set out. The step is not started;
the list below is what the user rules on first.*

## How it was measured

- A probe at the type-checker's one place (`compiler/spec/Overloads`, where a comparison across types is typed but not
  marked): one line per such comparison, with the scope it was typed in (a function's body, a body typed into its
  call, an island), the operator, the two types, the source position and the test that reached it.
- DataCube runs the planner in a wasm module whose error stream is switched off; it was switched on for the run, so the
  DataCube lanes were measured in the real module.
- A comparison in a relational mapping's function that carries no source position is one the normalizer wrote (the
  relational language, or the enumeration decode): excluded. One with a position is the user's Pure.

**Lanes run, all with the probe:**

| Lanes | Targets | Result |
|---|---|---|
| core's tests (every shard, the judges, the H2 and SQLite copies), the timeseries lanes, the stress suites, the corpus differential, the warehouse's and the projects' tests | 125 | all pass |
| DataCube's tests, its package and browser tests, the wasm tests | 228 | all pass |
| the python bindings, DataCube's python tests, the warehouse's native tests | 17 | 14 pass, 3 fail; the 3 fail without the probe too (below) |

**Not measured:** the H2 stress suites, the relational corpus, PCT (out of scope); the Postgres-backed core tests (the
container runs as root); DataCube's `verify_app_test`; the heavy gate and the manual measures. The wasm and python
counts are by query, not by test name. A comparison that typing never reaches is not seen: 63 of the timeseries
library's 253 comparisons with an integer literal were reached.

## The result: 92 places, 81 of them step 4's

| Where the comparison is written | Places | Reached by |
|---|---|---|
| The timeseries library (`projects/timeseries/model.pure`, the same text as DataCube's `pure-library-text.ts`), typed into each call | 65 | 2,627 bodies in the JVM timeseries lanes, 876 in DataCube's wasm, 93 in python |
| A core test's model: the M2M `SalaryBandMapping` (the user's Pure) | 4 | about 2 tests |
| **The product's server tool `CreateClass`:** the mapping and the function it generates | 5 | 6 `CreateClassTest` methods |
| A core test's function `f::r` (a String against `TDSNull`) | 1 | `TdsNullTypingPinTest` |
| DataCube's test model `spike::` | 3 | 1 test |
| No function at all: one stress service's query; `SpecCompilerTest`'s `typeExpression` (deliberate) | 2 | 2 tests |
| *Excluded:* the relational language (1), `#SQL` islands (3), the `#TS` translation (8) | 12 | |

By client:

| Client | Places of its own | Reached by |
|---|---|---|
| core's tests | 5 in test models, 5 in the `CreateClass` tool, 1 stress service | about 11 tests |
| DataCube | 62 of the library's 65, 3 in a test model | 876 bodies |
| python | none of its own; 24 of the library's | 93 bodies |
| the warehouse (JVM and native) | none | |
| every project under `projects/` | none | |
| the platform's own boot layer (the metamodel's functions, the prelude) | none | typed in every lane |

## The timeseries library: one edit does not fix it all

65 places: 63 compare a Float with an Integer, 2 a day with a date-time.

- **(a) 40 compare a column the library computes itself** (`$x.pk`, `$x.s`, `$x.m2`...), a Float by construction, with
  `0`, `1` or `-1`. Writing `0.0`, `1.0`, `-1.0` fixes them for every caller. One edit pattern.
- **(b) 23 compare the caller's column** (`$r->read($value)->toOne() == 0`). The function is generic over the
  caller's column type and is typed per call, so `0` is across types for a Float or Decimal column, and `0.0` would be
  for an Integer or Decimal one. The stress tests reach Decimal columns. No literal fixes these: they need a form that
  is neutral to the type (`->toFloat() == 0.0`, `sign()`), or a ruling on how a literal reads against a type variable.
  **A design question for the user.**
- **(c) 2 date comparisons** need a conversion on one side.

The ten most reached are all in the library: `returns` (`== 0`, 2,357 bodies), `windowProduct` (three, 369 each),
`returns` (`<= 0`, 335), `summaryStatistics` (237), `performanceStatistics` (several, 182 each).

## The other places, each a one-line fix

- **`SalaryBandMapping`** (two test files; `salary` is a Decimal): `< 50000` becomes `< 50000D`.
- **`CreateClass`, the product's tool:** the request's `extend(~big: x|$x.notional > 100)` over a `DECIMAL(18,2)`
  column is copied into the mapping and the function the tool generates. The door never checks it today; under step 4
  the generated model stops compiling. Either the test writes `100D`, or the tool refuses at the request.
- **`TdsNullTypingPinTest`'s `f::r`:** `$v != meta::pure::tds::TDSNull` (a String against the SQL-null class):
  `$v->isEmpty()`, or the rule leaves the SQL null out (it is a sentinel, not a value's class).
- **A stress service** (`99-vocab-services.pure:15120`): `$x.rate->toDecimal() >= 2.375` becomes `2.375D`. It is
  typed through a service entry that does not check today; step 4 catches it only if the service's door checks.
- **DataCube's `spike::` test model:** three comparisons with `0`, the library's fix.

## Already failing, before step 4

Three python targets fail on the base without the probe (19 cases): 12 are `Object of type Decimal is not JSON
serializable`; 6 are the query-level rule refusing a query (`filter(x|$x.qty > 5)` over a Float in a notebook test,
and five engine tests: "compares Float with Integer"); 1 unread. **So python's tests already fail on the query-level
rule that landed with B3 step 5.**

## What it cost

Setup 8 minutes; the JVM lanes 17; DataCube and wasm 22 without the probe and 21 with; python 8.
