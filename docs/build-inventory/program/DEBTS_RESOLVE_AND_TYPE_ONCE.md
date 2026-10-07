# The two biggest debts: a call resolved once (PARK-5), an argument typed once (PARK-6)

Research from 2026-10-07 (the Phase 3 audit and its follow-up), written down so the fix starts from the evidence.
The ledger rows are in `docs/PARKED_WORK_LEDGER.md`; both are fixed after the program lands, correctly, with a design
agreed with the user first.

## PARK-5: a call to a platform function is never resolved once

### The measurement

- Probe: the audit's eager corpus compile (`com.legend.rcorpus.EagerCorpusCompileProbe`, run outside Bazel from its
  execution root, scripts in `runs/build-rebuild/runs/homework/phase3x/audit_tmp/`), main's jars against Phase 3's,
  runs alternated. Typing (`typeAll`): main 2,147 ms mean, Phase 3 2,533 ms (+18%, slower in every pair). Whole corpus
  passes: H2 host unchanged, DuckDB host +3.6%.
- Profile: JFR, 1 ms sampling, one run each. `jfr print` keeps 5 frames per stack by default, which hides the
  callers; re-exported with `jfr print --json --stack-depth 200 --events jdk.ExecutionSample rec.jfr`.
- **`BareNames.catalog` holds 19% of all samples on main (425 of 2,202) and 29% after Phase 3 (688 of 2,400).**

Who calls it (samples, Phase 3 / main):

| Call site | Question asked | Phase 3 | main |
|---|---|---|---|
| `TdsDesugars.tdsSchemaDesugars` (the `map` over `.columns` check) | `ResolvedNames.names` | 170 | 141 |
| `Typer.applyFunction` (form dispatch) | `ResolvedNames.form` | 153 | — |
| `TdsDesugars.tdsGetterDesugars` (the TDS `toString` check) | `names` | 135 | 145 |
| `ReceiverOwnedFunctions.of` | `declaredNatives` | 86 | 100 |
| `DeferredArgs.isOverCall` (each argument of each call) | `form` | 69 | — |
| `Typer.applyFunction` (the `instanceOf` rule) | `names` | 34 | 24 |
| `SourceSubst.letName`, `Typer.deferredLetRhs` | `form` | 24 | — |
| `StaticFold` | `referents`, `names` | 15 | 11 |

Inside `catalog`: the name is spelled under each of the 32 packages of `CoreImports.SEQUENCE`, then each spelling is
looked up (`Pure.nativeFunctionsAt`) and put in a `HashMap` and a `HashSet`; most of the time is hashing freshly
built strings.

### The cause

- `NameResolver` (the `AppliedFunction` case, around `NameResolver.java:1697-1740`) records which of the program's own
  declarations a call can mean (`candidateFqns`). It adds the platform's declarations only when it also found one of
  the program's (`captured && scope.prelude()`); otherwise a call to a platform function stays bare, with no
  candidates.
- Every later check that asks what a bare call can be works it out again from the spelling:
  `ResolvedNames.referents` → `BareNames.catalog` (the three tiers: the engine surface, the core-import packages, the
  form's own declarations).
- Calls built after the resolver are bare too: 232 `new AppliedFunction(` sites in core's main sources, of which the
  typer's `compiler/spec` 92, the mapping normalizer about 96 (`RelOpTranslator` 33, `MappingNormalizer` 24,
  `JoinChainEmission` 13, `ViewRelation` 10, `DeclaredCoercions` 7, others), the parser 29 (before the resolver: fine),
  validation 5.
- Phase 3 made 21 checks read forms by the names a call resolves to (right), which multiplied the re-working.

### Options weighed on 2026-10-07

- **The full fix (the right one, after the program):** the resolver resolves every bare call to platform functions
  once and records the answer on the call; calls built after the resolver are built resolved (one constructor that
  resolves when it builds, or the full name); `ResolvedNames` reads the record. Size: the resolver, about 200
  call-building sites, and the 10 files that read `candidateFqns()` (several treat an empty list as "the resolver
  found nothing: use the spelling": `TdsLegacy.matches`, `Overloads.candidatesOf`'s bare path, `ResolvedNames.form`).
  It is the identity program's direction (dispatch on the resolved declaration), so the identity guard's counts go
  down.
- **The contained version:** work out a call's names once in `Typer.applyFunction` and hand them to the checks that
  run on that same call. 4-5 files. Downsides: checks on other calls and passes outside the typer still re-work;
  two ways to ask the same question; a rewritten call needs fresh names; estimated, not measured.
- **Rejected:** memoizing `BareNames.catalog` (a cache before the root cause is fixed); a second copy of the tier rule
  written as string checks on names (the identity guard rejected it, rightly: it adds name-string identity code);
  reordering checks so the expensive question is asked less (it offset the cost elsewhere and fixed nothing).

### What closes it

PARK-5's acceptance: names worked out once and recorded; typing on the eager compile at or below main's; the six
corpus passes, PCT and the reference lane unchanged. Measure with the same probe and the profile above.

## PARK-6: an argument typed more than once

### Where (each types an argument, then types a rewritten call that contains it again)

1. `Typer.applyFunction`, the dot-call branch: `TypedSpec recv = synth(af.parameters().get(0), env)` to read the
   receiver's class, then the route taken (`applyGeneric` of the property's body call, or of the call itself) types
   it again.
2. `CallShapes.autoMapReceiver`: types a dot call's receiver to see whether it is many-valued, before
   `Overloads.checkGeneric` types every argument.
3. `Overloads.derivedShadow`: after typing, re-applies the property's body call (`applyGeneric`), typing the receiver
   again.
4. The `map` rewrites (the dot-call branch's auto-map, `autoMapReceiver`): `synth(map(receiver, lambda))` types the
   receiver again inside the `map` form.
5. The legacy-TDS receiver checks (`TdsDesugars`, `Typer`: `grecv = synth(...)`), then the rewritten call.
6. `ReceiverOwnedFunctions.of`: types the receiver to choose between a form and a model function.
7. `Overloads.inlineNormalized`: substitutes the untyped arguments into the inlined body, so an argument is typed once
   per use of its parameter.

### Facts that bound the fix

- Typing twice changes no result today: the typer keeps no per-typing registry (`Typer`'s fields are the context, the
  kernel, the annotations, the desugars and the overloads; `Overloads` keeps two balanced stacks and a fresh-name
  counter). The old warning in `Overloads.checkGenericTyped` ("a second synth re-registers typer state") was stale
  and is corrected. The cost is time, doubling at each level of a chain of such calls.
- `ValueSpecification` is a sealed interface in the protocol package, so a "typed hole" carrying an already-typed
  argument cannot be added without the protocol depending on the compiler.
- `Overloads.checkGenericTyped(af, typedArgs)` already takes typed arguments (`ConcatenateChecker` uses it), but the
  rest of `applyGeneric` (multiplicity desugar, auto-map, format slots, inlining, the shadow) and the form checkers
  start from untyped calls.

### What closes it

Legend-pure's order (`FunctionExpressionProcessor`): type each argument once, choose the function and route on the
typed arguments (lambdas after), and have the checkers take typed arguments. It is the same rework PARK-7 needs (the
`Any` adjustment imitates that order). Acceptance: PARK-6's.
