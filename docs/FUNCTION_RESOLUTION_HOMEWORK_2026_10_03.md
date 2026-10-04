# How a function name in a call is resolved — legend-engine vs legend-lite (homework, 2026-10-03)

Why this was read: Studio-lite stores `.pure` files with imports and serves fully-qualified entity JSON
(design `docs/STUDIO_DESIGN_2026_10_02.md` S5, S14). For a call like `$x->filter(...)` the JSON must make
legend-engine bind the same function lite bound, so lite must know the engine's rule exactly. Read in
legend-engine `230c159` and probed on legend-engine 4.145.0 (`:6300`) and legend-lite (`:18777`) side by side
with `POST /api/pure/v1/compilation/compile` (probes: `.scratch/probe-fn*.mjs`).

## The engine's rule (read in code)

`CompileContext.resolveFunctionBuilder` (legend-engine-language-pure-compiler `CompileContext.java:544-571`):

1. **Strip a platform package.** If the written name's package is a *registered meta package* — the package of
   some registered handler (`Handlers.registerMetaPackage`, `Handlers.java:3153-3163`) — it is reduced to the
   short name (`extractMetaFunctionName`, `:573-586`). So `meta::pure::functions::collection::filter` → `filter`.
2. **Look the name up in the handler map.** Platform functions are registered by **short name**
   (`h("meta::pure::functions::collection::filter_T_MANY__Function_1__T_MANY_", "filter", …)`, e.g.
   `Handlers.java:1488-1496`, plus every compiler extension's handlers, `:2025-2032`); several platform
   functions share one short name (relation, TDS and collection `filter` all under `"filter"`), the arguments
   choosing among them. **User functions are registered by full path** (`FunctionCompilerExtension.java:97`,
   `Handlers.register(UserDefinedFunctionHandler)`, `:3176-3200`).
3. **Otherwise search the imports.** `searchImports` (`:588-633`) tries `import + "::" + name` for each import —
   the element's section imports plus the ~30 auto-imports (`META_IMPORTS`, `CompileContext.java:~80-121`,
   added "regardless the type of the section", `:169-177`). Exactly one → that function; more than one →
   "Can't resolve the builder for function 'f' - multiple matches found [...]"; none → "Function does not exist".

Consequences:
- a **platform short name always wins**; an imported user function with that name is unreachable by short name;
- a user function is reached by **full path**, or by short name **through an import** when no platform function
  has that name;
- there is **no own-package tier**: a function in the caller's own package needs an import or its full path;
- for user code, "platform" is **exactly the handler registry** — a platform function written in Pure without
  a handler is not callable at all (`elementToPath`, below);
- Studio-style JSON has no `SectionIndex`, so only the auto-imports remain.

## Probes (engine 4.145.0 vs lite, `compilation/compile`, text model)

| # | Case | Engine | lite |
|---|---|---|---|
| A | imported user `myFunc()` by short name | OK | OK |
| A' | same, as Studio-style JSON (no imports) | **refused** (does not exist) | — |
| B | user `myFunc()` short, no import | refused | refused |
| C | short `filter` with an imported user `my::util::filter(String)` | platform `filter` chosen → refused | refused (message names `my::util::filter`) |
| D, I, K | user function by full path | OK | OK |
| E, F | platform `filter` by short name / full meta path | OK | OK |
| G | user `toUpper(String):Integer` imported, caller expects `Integer` | platform wins → refused | refused |
| H | same, caller expects `String` | OK | OK |
| **J** | user `toUpper(String, Integer)` imported, called `toUpper(2)` — only the user fits | **refused** (platform name claims it) | **accepted** |
| **L** | platform Pure function without a handler (`elementToPath`), short | **refused** (does not exist) | **accepted** |
| M | same, full path | refused | — |
| N, O | user `elementToPath` imported (no platform handler by that name) | OK | — |
| **P** | same-package call, no import | **refused** | **accepted** |
| **Q** | two imports both define `f()` (same signature) | **refused** (multiple matches) | **accepted** |
| **R** | two imports define `g()` / `g(String)` | **refused** (multiple matches) | **accepted** |

## The divergences (lite accepts what the engine refuses)

1. **J — union across user and platform.** lite unions call candidates across imports and the platform and lets
   the signature pick (`NameResolver.java:1743-1791`, following legend-pure: "real pure has no user/platform
   tiering"); the engine tiers.
2. **L — platform functions the engine does not expose.** lite's platform includes functions the engine's handler
   registry does not have.
3. **P — own-package tier.** lite resolves a call through the caller's own package (`resolveCallCandidates`,
   `NameResolver.java:372-387`); its own comment says the reference has no such tier.
4. **Q — ambiguity accepted.** Two imports defining the same signature: the engine refuses; lite compiles (a tie
   rule picks one — S7's deterministic tie, `docs/SEMANTICS_REGISTER.md`).
5. **R — overloads across packages.** Two imports defining the name at different arities: the engine refuses
   ("multiple matches"), lite overloads across them.

None of these is in `docs/SEMANTICS_REGISTER.md` (no row on function matching tiers).

## What it means

- For **published JSON** (design S14): lite must not publish what the engine cannot compile. With lite's current
  rules, J, P, Q, R and L compile in lite and fail on the engine — and on lite's own pointer path the moment it is
  compiled by the engine.
- For **(a)** (fully-qualified entity JSON): with the engine's rule the binding of a call *name* is decided by name
  alone (platform short name → as written; otherwise exactly one import → full path), so (a) needs no type
  information once lite follows it.

## How upstream avoids the question: two compilers

legend-engine's own Pure libraries (relational, relation functions, …) and their tests are compiled by
**legend-pure's** compiler at build time (`legend-pure-maven-compiler`, goals `build-pure-jar` /
`build-pure-compiled-jar`, e.g. the relational core module's `pom.xml`) and their tests run on legend-pure's runtime
(`PureTestBuilderCompiled.buildSuite(... "meta::relational::tests::connEquality" ...)`). Only user code reaches the
engine's protocol compiler and its handler registry. lite has one compiler for both, so it must carry the
difference as a **context** passed in by the caller.

## Impact measurement (2026-10-04)

A scratch probe in `NameResolver` (reverted, never committed) classified every call against the engine's rule, with
the compiling entry point, over `bazel test //...` (150 targets) and then the 9 suites with hits.

- **J: 1** — `NameResolutionContractTest.java:161`, a test asserting lite's union ("the prelude native joins the
  candidate set instead of being shadowed").
- **P: 0. Q/R: 0. X (a different single function chosen): 0** — anywhere: corpus, PCT, specs, stress, lite's tests.
- **L: 56,544** calls to platform functions the engine does not expose to user code, after asking the engine
  itself about each of the 1,058 distinct names (`compile` of `name()`: "Function does not exist" = unreachable,
  1,038; a handler error = registered, 20 — the relation quantifier family `equalAll`, `greaterThanAny`, … which
  the engine registers through a list, `Handlers.java:292`, and a source scan of `h(...)`/`register(...)` missed —
  so the registry must come from the engine's answers, not from scanning its source). By caller:

  | Caller | L | Context it belongs to |
  |---|---|---|
  | `com.legend.test.PureTestRunner` (reference corpus and PCT) | 47,747 | platform (upstream: legend-pure's runtime) |
  | `normalizer.ModelNormalizer` (compiler-generated bodies) | 8,377 | platform (lite's own synthesized code) |
  | `server.QueryService` reached from the PCT lanes and `core_tests` | 42 | platform when PCT runs through it; user when `pure/v1` does |
  | lite's own unit tests compiling directly | ~380 | per test |

Conclusions:
1. J, P, Q and R can follow the engine for user code at the cost of one contract test.
2. L is the real context question, and the entry point cannot decide it: `Compiler.resolveQuery` and
   `QueryService` serve both the reference harness and `pure/v1`. The caller must pass the context.
3. The engine's set of user-reachable platform functions must be taken from the engine (its answers), recorded as
   data with a test that re-asks it.

## Open questions for the fix

- **Scope by context, passed by the caller.** Platform context (lite's prelude, compiler-synthesized bodies, the
  reference harness) keeps legend-pure's rules; user context (`pure/v1`, Studio, published projects) gets the
  engine's.
- **"Platform" for user code = the engine's reachable set,** from the engine's answers (above), not a source scan.
