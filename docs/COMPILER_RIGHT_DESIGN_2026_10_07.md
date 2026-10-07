# The compiler done right: what to fix, upgrade or redo, and in what order (2026-10-07)

**Status: a design for the user's decision. No code before it is agreed.** Written from the code and from
measurements taken today, not from the earlier documents; where an earlier document's claim was checked, this says
so. The user's framing (2026-10-07): parity of OUTPUT with legend-pure and legend-engine is the goal and the oracle,
and the compiler itself should be better than theirs — streamlined, robust, fast, close to mainstream compilers; an
expert-level compiler. So legend-pure's algorithm is not the template here; the corpus, PCT, parser parity and the
reference lane are the judges of "same output", and the question for each piece is what a mainstream compiler would
do.

## 0. What the compiler is today, measured

**Size** (lines of Java under `core/src/main/java/com/legend`, 2026-10-07): lexer 1.4k; parser 15.3k (hand-written
recursive descent, one parser per grammar section: Pure, mapping, relational, connections, services); normalizer 11.2k
(the legacy mapping DSL into functions); name resolver 2.1k; element compiler 6.1k (the model from definitions); the
typer 22.1k (`compiler/spec`: `InferenceKernel` 2.0k, `Typer` 1.8k, `UserCallInliner` 1.6k, `Overloads` 1.2k, 39
checkers, folds, unrolls, desugars); the store resolver 35.9k (the biggest package: object queries into relation
pipelines against the mapping); lowering 23.8k; SQL and dialects 16.8k; the native catalog (`builtin/Pure.java`) 2.8k
with 837 native signatures. The typed tree has 43 node kinds; the type language 8 (primitives, decimals, classes,
enums, type variables, function types, relation types, schema algebra); every typed node carries `(type, multiplicity)`.

**The pipeline for one query** (`Compiler.query` → `TypedQuery.plan`): parse → `NameResolver.resolveQuery` (a whole-tree
rewrite that records, on every call node, the user functions it can mean) → the typer (`SpecCompiler.typeQueryBody` →
`Typer.synth`, one recursive pass that infers each node's type) → the inliner (every user call β-inlined) → the store
resolver (phase H) → the lowering (phase I) → the dialect (J). Function bodies are typed on demand and memoized per
function (`SpecCompiler.compile`), never twice.

**The measurement.** `//spec:eager_corpus_compile` types every body of the corpus's world — 9,942 bodies — after
building the model: build 1.8 s, typing 2.0 s, about 4 s in all (three runs, 2026-10-07, this machine). Under Java
Flight Recorder (809 samples over three runs; a sample is attributed to the first phase found on its stack):

| phase | share | what it is |
|---|---|---|
| names | **40%** | `BareNames.catalogTiered` 24% inclusive, `BareNames.catalog` 20%, `ResolvedNames.referents` 11%, `BareNames.tiered` 11%, `NameResolver.resolveVs` 13%, `resolveCallCandidates` 7%, `Pure.nativeFunctionsAt` 4%; the hottest leaf frames are `HashMap.getNode` and `ArrayList.removeIf` |
| type | 24% | `Typer.synth` 7.5%, `applyFunction` 6.7%, `Overloads.checkGeneric` 4.9%, `applyCore` 2.5%, the TDS desugars 3.6%, `accessProperty` 2.1%, `resolveOverload` 2.0%, `TdsLegacy.bare` 1.6% (a name match per call), `typeLambda` 1.4% |
| parse | 10% | lexer and parser |
| model | 4% | the element compiler |
| normalize | 3% | the mapping normalizer |
| other | 17% | the JVM, collection, the probe |

So the single biggest cost of compiling the world is **re-deriving what a bare name can mean**: the resolver tries
each bare call under the imports, the element's own package and the 32 core packages (`resolveCallCandidates`), and
then every later check asks again from the spelling (`ResolvedNames.names` → `BareNames.catalog`: the same 32
packages, about five times per call). The earlier ledger row (PARK-5) put it at 19% of typing time; measured across
the whole compile it is 40% of everything. Typing itself is a quarter. Parsing is a tenth.

**What is not measured yet:** the per-query path in the product (DataCube, Query, Studio): parse → names → type →
inline → store-resolve → lower → render for one query against a built model, where the store resolver and the
lowering — 60k lines that the eager probe never runs — carry their own cost. That measurement is the first item below.

## 1. What a mainstream compiler does that this one does not (the findings)

Each finding names the code, says what the evidence is, and what "done right" looks like. The order is by what the
numbers and the risks say, not by the ledger's numbering.

### F1. Names are never resolved once (the 40%)

**Now.** `NameResolver` (phase D) records on a call node the user functions it can mean (`candidateFqns`), but a call
to a platform function stays bare — unless the resolver also found a user function of that name, in which case it
merges the platform's by walking `BareNames.catalogTiered` (`NameResolver.java:1697-1752`). Every later consumer asks
again from the spelling: `ResolvedNames.referents(af)` (`compiler/ResolvedNames.java:24`) rebuilds the catalog's answer
for the name — the 32 core packages, the engine handlers, the forms' owned names, then a `removeIf` for the lite
partition (`BareNames.tiered`, `:60-80`) — and it is called from `Typer.applyFunction:481` for every applied function
(the `instanceOf` check), from `TdsDesugars:112`, `StatementInline:285`, `LiteralMapUnroll:58`, the mapping normalizer
(three sites), the lineage scan, the plan allocations, test-data generation. `TdsLegacy.bare` matches 18 legacy names
by spelling per call (`Typer.applyFunction:367-422`). The ~200 calls the typer and the normalizer *build* after
resolution are bare too, so they go through the same path.

**Done right.** Resolution happens once, in one place, and produces an identity, not a spelling: every call node
carries a **referent** — an interned symbol naming a function id or a native overload group — assigned by the resolver
for parsed calls and by the constructor for calls built later (a call built by the compiler is built from a symbol,
never from a string). Every "does this call name X" check becomes an identity comparison. `BareNames.catalog`,
`ResolvedNames`, the spelling matches (`TdsLegacy.bare`, `CoreFn.parseNames` by name) go. The name→symbol table is
built once per model (the prelude's part once per process). This is what every mainstream front end does (a symbol
table and interned names), and it is the ledger's PARK-5 acceptance, generalized to the resolver's own tiering.

**Judge.** The profile: `names` from 40% to a few percent; whole-world typing at or below today's 2.0 s; every lane
unchanged (the corpus rosters, PCT, parser parity, the reference lane's overload picks). **Risk:** low in semantics
(a name resolves to the same set, computed once), medium in reach (the ~200 construction sites). **Size:** medium.

### F2. Arguments are typed more than once (the typer's shape)

**Now.** The typer is one recursive `synth` (`Typer.java:150`), and `applyFunction` (`:360-590`) decides a call's
route by typing its receiver and then **re-entering `synth` on rewritten syntax**: the row-getter check types the
receiver (`:388`), the qualified-property route rebuilds the call and types its arguments again (`:458`, `:470`,
`:543`), the auto-map rewrite wraps the receiver in a `map` and re-types it (`:525`, and `accessProperty:1294`), the
derived zero-argument property re-applies its body function over the receiver (`:1316`), the derived shadow and the
format-slot rewrite in `Overloads.applyGeneric` re-enter with raw arguments (`:123`, `:188`), and a function that must
be inlined has the caller's **untyped** syntax substituted into its body and the whole typed from scratch
(`Overloads.inlineNormalized:316-354`). Seven places; the ledger's PARK-6 lists them and the code confirms each. In a
chain of such calls the work doubles at each level. The typer records nothing per typing, so results do not change;
time does.

**Done right.** Type each argument once, then decide on typed facts — *elaboration on the typed tree*. The
type-directed rewrites (auto-map, qualified-property routing, the derived shadow, the format slots, must-inline)
become transformations that take the already-typed arguments: a rewrite builds a typed node from typed nodes, never
syntax it then re-synthesizes. The must-inline path substitutes typed arguments. One guard keeps it: a test that counts
`synth` entries per source node (exactly one).

**Judge.** Same results on every lane; typing time; the guard. **Risk:** medium — this is the typer's spine; the
checkers that take raw syntax today (`TdsDesugars`, `CallShapes`, `ReceiverOwnedFunctions`) change their inputs.
**Size:** medium-large; it is the heart of the typer rework.

### F3. Desugaring is interleaved with typing

**Now.** `applyFunction` begins with a run of *untyped* rewrites — `col(fn, 'name')` to a column spec, `tdsRows`,
`restrict`, `extractEnumValue`, TDS literals, path literals, the infix carrier — each a `return synth(rewritten)`
(`Typer.java:367-431`, `:154`, `:179`); `CallShapes.toMultiplicityDesugar` and `expandLetBoundLambdaArgs` do the same
inside the generic path. Some rewrites need types (auto-map); these do not.

**Done right.** A **desugaring pass** between resolution and typing — surface syntax to core syntax, on the AST, once —
so the typer sees only core forms. `applyFunction` shrinks to the core-construct dispatch (`applyCore`, exhaustive over
`CoreFn`) plus the generic signature path. This is the standard front-end shape (surface → core → typed).

**Judge.** The same results; `applyFunction`'s length; the desugar pass's own unit tests. **Risk:** low. **Size:**
small-medium; it falls out of F2.

### F4. Overload resolution is a score, not a specification

**Now.** `InferenceKernel.score` (`:1248`) sums `typeScore * 20 + multScore` per parameter (exact 2, subtype 1, type
variable or `Any` 0), then `resolveOverload` (`:1068-1215`) breaks ties with five older rules: duplicate signatures
(first wins), a native over a module function, the most specific, `Nil` narrowing, the class linearization; the
"present arguments" ranking (`Overloads.selectRankedByPresentArgs:781`) ranks before lambdas are typed and retries
candidates in order (`checkWithDeferred:536-585`). The ledger rows PARK-7 to PARK-10 record what differs from
legend-pure (`Any` ranked with the type parameters, tie-breaks legend-pure does not have, an acceptance test that
admits what it rejects, three parts of its ranking not ported). The Phase 3 branch's step 1 ("overloads ranked as
legend-pure ranks them", `FunctionMatch`) holds the facts.

**Done right.** Overload resolution as a **stated partial order with laws** — most specific wins; `Any` is a concrete
top type; a lambda argument is typed against each candidate it could fit (as today, bounded); a genuine tie is an
error, named — written down in one place (`FunctionMatch`), with a table of legend-pure's picks as the test (the Phase
3 probe's data), and no numeric score. Which programs compile where legend-pure refuses (PARK-9, PARK-14) is a
semantic choice for the user, recorded in `SEMANTICS_REGISTER.md` either way.

**Judge.** The reference lane's overload picks (the probe), the corpus, PCT. **Risk:** medium (a pick that changes is
an output change — which is exactly what the lanes catch). **Size:** medium.

### F5. The inference engine is sound but by names

**Now.** `Bindings` maps type-variable *names* to types (`Bindings.java:54-55`), so a callee's `T` must be renamed
apart from the caller's at every call (`SignatureApart`, `InferenceKernel.resolveChosen:1265`); unification is a
structural switch over the type language with a nominal lattice, `Nil` as bottom, the schema algebra for relation
columns (`InferenceKernel.unify:104-331`). It works (the corpus says so); it is more fragile than it needs to be.

**Done right.** Type variables with **identity** (fresh per instantiation, never a string), a substitution keyed by
them, an occurs check; the same unification otherwise. **Judge:** the kernel's own tests, then the lanes. **Risk:**
low-medium. **Size:** small-medium. Not first: nothing measured points at it; it pays when F2 and F4 are done.

### F6. Scopes copy, and some bindings are syntax

**Now.** `Env.with` copies a `LinkedHashMap` per binding (`Env.java:45`): quadratic in a body's lets. "Deferred" lets
park raw syntax (trees, column specs) to be typed at a consuming call (`Env.withDeferred`, `SpecCompiler:300-320`,
`Typer:286-292`): a value without a type until something reads it.

**Done right.** A chained or persistent scope (constant-time binding, no copy); and the deferred kinds typed as the
values they are (a graph-fetch tree literal has a type; a column spec is a value of a spec type), so nothing is parked
as syntax. **Judge:** the same results; the lets-heavy corpus bodies. **Risk:** low for the scope; medium for the
deferred kinds (their consumers are several checkers). **Size:** small for the scope, medium for the kinds.

### F7. Errors are strings, and positions stop at the function

**Now.** 232 `throw new TypeInferenceException("…" + …)` in the typer; a failure names the enclosing function
("Positions stopgap … the expression-level [line:col] is the deferred big lift", `SpecCompiler.compile:97-105`); the
first error ends a body's typing. The tolerant mode (`buildModule`) collects walls per element, not per expression.
The AST carries positions on some nodes (`AppliedFunction.pos`), not all.

**Done right.** A **diagnostic** type (a code, a span, a message, notes), spans on every node from the parser,
recoverable typing (an error node poisons its parents and typing continues, so a body reports all its errors), and
the same diagnostics feeding the LSP (`ide`) and the test runners. This is what makes a compiler feel expert to its
user. **Judge:** message tests; the corpus rosters and PCT pin some messages by text, so a message change has a cost
to plan (a mapping from old to new texts in one commit). **Risk:** low in semantics, high in surface. **Size:** medium.

### F8. Instrumentation sits in the hot path

**Now.** 39 `DecisionProbe.…` hooks in 10 product files, inside `applyFunction`, `candidatesOf`, the resolver (the
`INSTALLED != null` guards); `LL_TDG_DEBUG` and `LEGEND_LITE_RAW_EXPAND_TRACE` environment reads in product code
(`InferenceKernel:1277`, PARK-13). The build program's D8 deletes `probe`.

**Done right.** One tracing seam — an interface the typer calls through a single field, a no-op by default — or
nothing; no static hooks, no environment variables. **Judge:** the guard that forbids them (`ObservabilityGuardrailTest`
exists; extend). **Risk:** none. **Size:** small; do it with D8.

### F9. There is no compile benchmark

**Now.** The eager probe measures the whole-world compile; nothing measures the per-query path, which is what the
product's users feel. The 60k lines of store resolver and lowering have never been profiled.

**Done right.** A **benchmark target** (`//core:compile_bench`, manual, a measurement on CI like the probe): a fixed set
of queries (the corpus's, DataCube's shapes, Studio's) against a built model, parse → SQL, p50 and p99 per phase, JFR
on request; its numbers in the evidence folder whenever a landing above claims a gain. **Risk:** none. **Size:** small.
**It comes first**, because every other item is judged by it.

### F10. The back half, unmeasured

The store resolver (`StoreResolver` 3.5k, `Substitution` 3.5k, `GraphEmission` 3.3k, `CorrelatedSubselects` 2.9k,
`TemporalFrame` 2.8k, …) and the lowering (`Lowerer` 3.5k, `Scalars` 3.5k, `VerdictSql` 1.7k, `Fold` 1.4k) are the
two biggest packages. Nothing in today's measurement reaches them. **Decide after F9's numbers**, not before.

### F11. Smaller things, true but not urgent

Invariant 5 (lazy loading of user elements) has no guard since the engine module's deletion (`AGENTS.md`); the
parser is 15k lines of hand-written recursive descent (mainstream, fine) but must carry spans for F7; `Pure.java`
registers 837 natives as static fields (fine; F1 interns them); the inliner rewrites the typed tree before lowering
(inherent to Pure-to-SQL; keep).

## 2. The order, and why

1. **F9 — measure first** (small): the benchmark, so every claim below is a number on CI.
2. **F1 — names once** (medium): the 40%, the lowest semantic risk, and it simplifies every check the typer makes.
3. **F2 + F3 — the typer's single pass** (medium-large): desugar before typing; type once; elaborate on typed trees.
   This is the typer rework the ledger's PARK-6 and PARK-7 wait for, and the one piece that needs its own design
   detail before code (the seven routes, each as a typed transformation; the must-inline path; the guard).
4. **F4 — overload resolution as a specification** (medium): on the single pass, with the Phase 3 branch's step 1
   as the facts and legend-pure's picks as the test; PARK-7..10 close; PARK-9 and PARK-14 are the user's semantic
   choices.
5. **F7 — diagnostics** (medium): spans everywhere, errors as data, recoverable typing; the message pins migrated
   in one commit.
6. **F5, F6** as F2 and F4 reveal the need (fresh type variables; the scope; the deferred kinds as values).
7. **F8** with D8, any time.
8. **F10** on F9's numbers.

**Where Phase 3's branch goes.** Its five commits touch exactly the files items 2-4 rewrite (`Typer`, `Overloads`,
`InferenceKernel`, `FunctionMatch`, `StatementInline`, `UserCallInliner`, `ResolvedNames`); the user's call on
2026-10-07 is that the compiler is fixed before the bump's restructuring lands. So the branch is **a reference, not a
landing**: its facts survive (legend-pure's ranking rules, the names calls resolve to, the forms and the legacy TDS
knowledge, the corpus fixes), its code is written again on the new typer; the bump phases (candidates and
implementations by id, forms, boot-layer versions by resolved names) follow on that base, and the prelude with them.

## 3. What does not change

The output oracle and the gates (the corpus rosters, PCT, parser parity, the reference lane, the stress corpus); the
layer contract (`AGENTS.md`); "no fallbacks, no defaulting"; the no-PR landing procedure with an audit, the local gate
and one CI run per landing; design before code for each item above (F2+F3 and F7 get a short design of their own).

## 4. Decisions for the user

1. The order above, and that Phase 3's branch becomes a reference.
2. F7's cost: messages pinned by text in the corpus rosters and PCT change in one commit (a mapping, not a drift).
3. The semantic choices the ledger leaves open: a dot call with no qualified property (PARK-14: refuse as legend-pure
   does, or keep the leniency and record it); the acceptance test's extra admissions (PARK-9: match legend-pure's, or
   record ours).
4. F9's query set: which product queries define "fast".

## 5. Evidence

The eager probe's report and the Flight Recorder aggregation of 2026-10-07 go to
`docs/build-inventory/program/evidence/compiler/` with this document's landing (the probe is
`bazel build //spec:eager_corpus_compile`; the profile ran the action's own command under
`-XX:StartFlightRecording=settings=profile` with a writable repository, three times, 809 samples). The ledger rows
PARK-5 to PARK-14 are on the Phase 3 branch (`docs/PARKED_WORK_LEDGER.md` there); their anchors were checked against
the code today and hold.
