# Can a user reach legend-engine's plan-text functions? (asked and read 2026-11-26)

*The user's question of 2026-11-26, on `toSQLString`, `planToString` and their neighbours: "I think those are platform
functions - useful if we need for backwards compatibility - but I don't immediately see a product use for it - I
don't think users can use it - can we check somehow?" A measuring session asked legend-engine 4.145.0 itself, read its
sources at the pin, and read and ran lite's own legend-engine door. This is its report, set out. It decides whether
about 25 classes and 5,300 lines of lite's edge (`EnginePlans`, `PlanText` and the rest: the step 9 inventory's
"stays" group) are product or test machinery.*

## The answer

**At legend-engine's own API a user can reach none of them: each fails when the request is compiled.** The user's
reading is right. **Lite's legend-engine door answers some of them today, where legend-engine refuses:** a difference
nobody recorded.

## 1. Asked of legend-engine itself

Instrument: the engine runner's plan generation and plan executor, as its server's routes answer them, over a small
relational model on H2 (a class, a relational mapping, a runtime). The baseline class query answers on both routes.

| Request | legend-engine's answer |
|---|---|
| `relationalExtensions()->size()` | 500, COMPILATION: "Function does not exist 'meta::relational::extension::relationalExtensions()'". So a user cannot even build the `extensions` argument |
| `toSQLString(\|q, M, DatabaseType.H2, [])` | COMPILATION: "Function does not exist 'meta::relational::functions::sqlstring::toSQLString(...)'" |
| `toSQLStringPretty` (both forms), `toSQL(...).sql` | the same |
| `executionPlan(\|q, M, RT, [])`, `planToString(executionPlan(...), [])`, `planToString` of a hand-built plan | the same, on `executionPlan` or on `planToString` |
| the call inside a store query's filter, or bound by a `let` before the query | the same |
| a model function whose body calls `toSQLString` | **the whole model fails to compile**, and with it a query that never calls the function |

Always at compilation (legend-engine's Java compiler), never at the router or at plan generation.

## 2. Read in legend-engine's sources

- The functions are plain Pure functions (`toSQLString.pure:35-93`, `executionPlan_print.pure:32,37`,
  `executionPlan_generation.pure:25-176`, `extension.pure:64`), with no stereotype.
- **The compiler's gate:** it resolves a call only to a registered handler or to the user's own functions
  (`CompileContext.java:476-484, 544-570`; `Handlers.java`); no Java source registers a handler for any of them.
- **A second gate behind it:** the Java platform binding prohibits any function whose signature uses a type from the
  execution plan, mapping, runtime, metamodel or router packages (`conventions.pure:1218-1330`).
- **Who does call them:** legend-engine's own Java, with arguments Java builds (`PlanGenerator.java:119-153` for
  generatePlan and execute; `planToString` from GraphQL's debug response and the test-data generators); and Pure
  tests: 63 of the 64 `.pure` files that call `toSQLString(` are tests.

## 3. Lite's legend-engine door today (run against lite's own server, then read)

| Request | lite's answer | legend-engine's |
|---|---|---|
| `relationalExtensions()` | refused: unknown function | refused |
| `toSQLString(...)` in a store query's filter | **200**: the SQL compares with the SQL text as a literal, and rows come back | refused at compile |
| `let s = toSQLString(...); <query using $s>` | **200** | refused at compile |
| a model function wrapping `toSQLString` | **the model compiles** | the whole model is refused |
| `executionPlan`, `planToString`, `toSQL` with a packaged runtime | refused, but for a type mismatch, not as unknown | refused |

How lite reaches them: the functions are natives of lite's platform registry (`builtin/Pure.java:1816-1875`); the
planner replaces a call by its text through the plan style's facet (`planner/PlanTextRoutines.java:24-30`,
`ProductStatements.java:169-170`). The product's style refuses by name; the legend-engine door's style answers with
the edge's `EnginePlans`, as PCT's and the corpus's styles do (`PureText.java:80-81, 248-262`).

**Reached by lite's routes themselves, with no user code** (so product code of that door whatever is decided):
`PlanSupportFunctions` (every generatePlan answer, `PureV1Routes.java:442`) and `EnginePlans.testDataTimestampText`
(the three test-data routes, `TestDataRoutes.java:110-117, 295`).

## What follows

- To match legend-engine, its door in lite refuses these functions in a user's query or model by name, as lite's own
  door already does, and keeps them for the test doors (the corpus and PCT, where legend-pure's runtime runs them).
- Then the classes behind them are test machinery and can leave the product with step 9's later slices; the two
  route-own pieces above stay.
- Not tried: a model function that calls them at lite's door with a runtime given (it compiles; it would answer).
