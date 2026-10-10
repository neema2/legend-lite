# The two biggest debts: a call resolved once (PARK-5), an argument typed once (PARK-6)

Research from 2026-10-07 (the Phase 3 audit and its follow-up), written down so the fix starts from the evidence.
The ledger rows are in `docs/PARKED_WORK_LEDGER.md`; both are fixed after the program lands, correctly, with a design
agreed with the user first.

## PARK-5: a call to a platform function is never resolved once

### The measurement

- Probe: the audit's eager corpus compile (`com.legend.rcorpus.EagerCorpusCompileProbe`, run outside Bazel from its
  execution root, scripts and each run's numbers in `evidence/phase3/park5/`), main's jars against Phase 3's,
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

### When it is fixed: the user's open decision

PARK-5 is the one debt whose timing is open (PARK-11 and PARK-12 close inside the program; the rest after it). The
options, with sizes:
- **(a) Land Phase 3 now with PARK-5 recorded**; fix it after the program, the full way.
- **(b) The contained fix first:** the typer works out a call's names once, in `Typer.applyFunction`, and hands them to
  the checks that run on that same call (form dispatch, the TDS desugars, the `instanceOf` rule, the receiver-owned
  check). 4 or 5 files in `compiler/spec`. Estimated (not measured) to take this lookup from 29% to about 13% of
  typing, below main's 19%. Leaves the checks on other calls and the passes outside the typer; two ways to ask the same
  question until the full fix.
- **(c) The full fix first:** the resolver records every call's names; the calls built after it are built resolved;
  the checks read the record. The resolver plus about 200 call-building sites and the 10 files that read
  `candidateFqns()`. Its own design and audit.

### Step 2 landed (2026-10-10; `L7_RESOLVE_ONCE_DESIGN_2026_10_10.md`)

The resolver records, on every bare call the program declares nothing for, the platform's names at the call's arity:
the list main's read-time rule produced, recorded (`AppliedFunction.referents`, the record `candidateFqns` was); an
ambiguous call's record is as before (the program's candidates and the platform's names). `ResolvedNames.referents`
reads the record and runs the rule only for a call with no record (a call built after the resolver: step 3's); a call
with a record is not resolved again (the design note's §10 has each departure from the design's text and its reason). Measured (the judges in the GATES
entry "L7 step 2"): the probe's candidate sets identical at every site (7,198 (name, candidates) rows on main's core and
on the change); the census identical (27 walls, 16,133 bodies typed, 1,120 failing); the eager compile's typing
below main's in every one of six alternated pairs (run 1 of `evidence/l7/eager_timings_step2.tsv`, a quiet machine, before the audit's fixes; run by `evidence/l7/eager_ab.sh` outside Bazel's cache on 2026-10-10): `typeAll` main 2,431 to 2,927 ms, median 2,701; the change 1,913 to 2,392 ms, median 2,188 (459 to 675 ms less in each pair); the `build` phase (parse and resolve, where the bare-name rule now runs once per bare call) median 2,017 → 2,088 ms; the whole compile's medians 4,718 → 4,275 ms; the profile (`evidence/l7/jfr_step2.txt`, one run each side, 1 ms samples, the whole probe): `ResolvedNames.referents` 698 of 2,378 samples on main (29%) → 359 of 2,249 on the change (16%), all of it now the no-record path (`BareNames.catalog` 351 samples: the 1,076 built calls and the 95 probes, read by the rule at every read; step 3's); `Typer.applyFunction` 1,334 → 1,089; the resolver's `resolveVs` 369 → 434 (the rule once per bare call the program does not declare). Repeated on the final tree under load (run 2 of `eager_timings_step2.tsv`): `typeAll` medians main 3,794 → change 2,855 ms, below main's in every pair. Of the 7,149 record-less calls the typer still meets, 5,978 are written in full (the name is the referent),
95 are bare parsed names nothing declares (property probes), and 1,076 are built calls: step 3's.
For step 4 (the audit of 2026-10-10, N1): the typer's bare path (`Overloads.candidatesOf` when the record is empty)
also merged the model's declarations at tier names that have no native at the call's arity, which a record built from
the natives leaves out; no engine-only name has more than one package today, so the probe's rows agree, and the
deletion of the bare path must keep that merge or show it empty. The audit's full table: `evidence/l7/AUDIT_L7_STEP2.md`.

### How to measure a fix

The numbers above came from the audit's scripts, copied with the runs' numbers to `evidence/phase3/park5/` (`eager_timings.tsv`;
`eager_run.sh` runs the probe from its Bazel execution root with a hard-coded macOS class path; `ab_run.sh` alternates
two trees; `eager_jfr.sh` adds the recording; `jfr_agg.py` reads it). To redo them anywhere:
- The probe is `com.legend.rcorpus.EagerCorpusCompileProbe`, the committed target `//spec:eager_corpus_compile`
  (manual, a 4 GB `java_run`; its outputs land in `bazel-bin/spec/eager_corpus_compile/`, and the timing line
  `build=<ms> typeAll=<ms>` is in its log).
- Bazel caches the action, so to time it repeatedly run the same Java command outside Bazel: the action's command
  line (`bazel aquery //spec:eager_corpus_compile`) gives the class path and flags; run it from the execution root. Build
  with `bazel build --remote_download_all` first: Bazel 9 leaves the jars that hit its cache off the disk, and the
  command fails with `NoClassDefFoundError` (2026-10-10; `evidence/l7/eager_ab.sh` does both).
- Compare main's tree and the change's tree on the same machine, alternating runs (six pairs), and read the median.
- For the profile, add `-XX:StartFlightRecording:filename=<file>,settings=profile,jdk.ExecutionSample#period=1ms`,
  then `jfr print --json --stack-depth 200 --events jdk.ExecutionSample <file>`.

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
