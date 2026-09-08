# The system prelude — tenets, placement, the two lists, diagnostics (2026-09-08)

Status: DESIGN, decided with the user on 2026-09-08 after batch 147. It amends
`docs/WORLD_MAP.md` (§8 there points here) and replaces three batch-148 items
(the function-reference leg, the per-property rule, the phantom-native census)
with one. Written in plain terms on purpose; the receipts are the file and line
references.

## 1. Where this came from

Batch 147 walked the five `testConnectionEquality*` tests strictly and found
that to read ONE field of the engine's extension record we had to declare about
thirty natives we do not implement, load two dozen engine files, and invent
rules for who wins when the engine's definition and ours share a name. The user
called it: "we can't be mixing and matching random stuff … a hard-coded mess".
Working back from there produced a small set of rules that were mostly already
in the world map, plus two we had been breaking without naming them.

The concrete symptom that made it clear: the `toString` lowering in
`lowering/Scalars.java` carries hand-written Java arms for `Pair` and `List`
(`isPairCarrier`, `isListCarrier`; the `List` arm's own comment says "real
anonymousCollections List.toString()"). Those arms are the spec's Pure bodies
for `Pair.toString()` and `List.toString()` re-typed in Java — a port of a
program, which the world map forbids — and they exist because the prelude
generator copies a class's stored properties and drops its derived ones.

## 2. The tenets, restated

1. **Java orchestrates, the database computes.** No value is computed in Java.
2. **The engine and pure checkouts are the spec.** Read to learn meaning; never
   loaded into the running platform as definitions; never ported.
3. **The platform and the programs it runs touch only through data.** The
   platform hands programs facts (rows in the system tables, query results);
   programs hand the platform code to compile. The platform never exposes a
   function it does not implement; a program never calls the platform's
   internals.
4. **One question places everything:** *if this vanished, would an ordinary
   user query stop compiling to SQL on OUR platform?* Yes → platform semantics,
   Java from the spec. No → a program, compiled.
5. **The spec already draws the native/program line.** `native function` in the
   spec means "the runtime implements this": for us Pure.java + one SQL
   lowering, or a named wall. `function` with a body, and a class's derived
   property, mean "written in Pure": for us a program, compiled like user code.

## 3. Where things go

| what it is | where it lives | the rule |
|---|---|---|
| a built-in the spec marks `native function` (`toLower`, `filter`, `at`) | `Pure.java` signature + exactly one SQL lowering rule in Java | a signature with no lowering may not exist; a native with no SQL meaning is a named wall (§6) |
| a class or enum shape | `Prelude.java`, GENERATED from the spec | shapes only, never hand-edited; regenerated when the admitted source set changes |
| a class's DERIVED property, or a spec `function` with a body (`Pair.toString`, `paginated`, `union`, `reactivate/1`, `whenSubType`) | `Prelude.java`, GENERATED from the spec as a Pure body, compiled like user code | a program; never a Java arm; the Java arms for `Pair`/`List` `toString` are deleted when their bodies compile |
| the few shapes the Java is coupled to (m3 bootstrap, primitives, carriers, system-store classes) | `Pure.java`, by hand, with a receipt line | the only hand-written shapes |
| how a query becomes SQL: routing, mapping resolution, plan generation, SQL rendering, milestoning, verdicts | Java (compiler passes, resolver, lowering), designed from the spec | question 4 says yes; never a port of engine Pure |
| reads over the platform's own facts (`$mapping.classMappings`, plan-node closure) | `SystemMetamodel.java` — Pure views over the system tables | reads only: no branching on computed values, no recursion |
| everything else written in Pure: tests, helpers, engine-authored tree walks (toPostgresModel) | stays Pure; loaded from files, compiled by the same compiler | question 4 says no; if it walls, the compiler is missing something |
| code that needs the engine's internals to produce a value | a wall, named and counted | no scaffolding, no phantom natives, no laziness |

Two consequences the last batches made explicit:

- A file is admitted as a program library only if every function in it is a
  program (question 4 = no). A file mixing programs with engine internals
  (`relational/extensions/extension.pure`) is not admitted whole; the tests
  whose SUBJECT is the internals are walls.
- Overloads are Pure semantics and always honoured (`x->toString()` runs the
  class's own `toString()` first — `Typer.derivedShadow`, the
  `ClassWithComplexToString` pin). What decides `Pair.toString` is not whether a
  class may override `toString` but where the body comes from: generated into
  the prelude from the spec (allowed, §4), never loaded from the checkout at run
  time (tenet 2 — the class-level flip in batch 147 broke exactly this).

## 4. The prelude is system code that looks and acts like user code

`SystemMetamodel.java` is already this: a Java file whose content is Pure
source (29 functions, classes, a relational mapping), compiled by the same
compiler as a user query. The prelude becomes the same thing for the
language's own definitions, generated instead of typed:

- **Shapes** — as today (`PreludeGeneratorTest`, `Prelude.java`), from the
  spec by demand, with the closure of what they name.
- **Derived properties** — the generator emits each class's derived properties
  WITH their Pure bodies. Each lifts to a function (`<owner>$prop$<name>`) and
  compiles when referenced, exactly like a user class's derived property.
- **Spec `function` bodies** — the generator emits the spec's Pure-bodied
  functions that programs reach (`paginated`, `union`, `reactivate/1`,
  `whenSubType` ×3, the date `firstDayOfThis*` wrappers, …). These are the
  functions batch 147 registered as PHANTOM natives; they were never natives in
  the spec. The phantom registrations are deleted as their bodies land.
- **Natives** — `Pure.java` keeps signatures for the spec's `native function`
  declarations; each has a lowering or a §6 row.

Compilation is already by demand at the name level: `FunctionCompiler.compileAll(fqn)`
compiles the SIGNATURES of one name's overloads on first lookup; a body is typed
when first inlined at a call. There is no whole-world eager pass today. A
production build will want one (compile every body once, on purpose); that is
the §7 governance test promoted to a build step — a problem for another day
that needs no new design.

Sizes, measured 2026-09-08 over legend-pure's platform packages
(`platform`, `platform_dsl_*`, `platform_store_relational`, `platform_precise_primitives`):

| | count |
|---|---|
| classes | 363 |
| derived properties with Pure bodies | 24 |
| Pure-bodied functions (non-test) | 293 |
| `native function` declarations | 222 (173 distinct names) |
| of those, registered in `Pure.java` | 138 |
| spec natives we lack | 35 — mostly reflection and effects: `new`, `copy`, `evaluate`, `sourceInformation`, `stereotype`, `tag`, `generalizations`, `pathToElement`, `random`, `logActivities`, `openVariableValues` |

Legend-engine's packages (`core`, `core_relational`, …) are NOT prelude
material: they implement the engine (question 4). Engine-authored PROGRAMS the
corpus imports stay admitted by name (`Corpus.LIBRARY_FILES`) under the
per-file rule in §3.

## 5. Compile is three stages; a failure belongs to exactly one

1. **Load.** Parse, resolve names, check declarations (every named type exists,
   no duplicates). Eager over everything loaded. A body's unknown name does not
   fail here; it carries forward.
2. **Type.** A body is typed when first reached. Unknown functions, properties,
   types, or a typer gap fail here. A REFLECTIVE call (`evaluateAndDeactivate`,
   `eval` of a function value) types fine when its signature is registered:
   typing asks "does this fit", not "can I run it".
3. **Lower and run.** Only when a statement executes is its typed tree turned
   into SQL. A native with no lowering fails here: "no SQL meaning". This is the
   only stage where reflection bites, and only for statements that reach it.

Compiling never executes. Nothing needs commenting out for the world to compile.

## 6. The two lists

- **Typing work list (should trend to ZERO).** Every body the generator emits
  is typed once by a governance test; each failure is a row with its reason —
  missing vocabulary (generate it), a typer gap (fix it), or, essentially never,
  a spec type error. Not a permanent list: its length measures how complete the
  vocabulary and typer are.
- **Lowering permanent list.** Every `native function` either has a lowering
  rule or sits in a named list with its reason (reflection over the live graph,
  evaluating a run-time function value, effects, IO). Pinned shrink-only in
  `NativeCatalogGovernanceTest`. A program that reaches one at run time walls,
  naming the native.

The DROP-AT-OVERLOAD tolerance in `FunctionCompiler.compileAll` (a broken
overload silently leaves the candidate set) must become the poison sentinel its
own comment names (task #56) before the typing list means anything.

## 7. Diagnostics: the lowering list as a compile-time fact

After a body types, one pass walks its typed tree and asks each native call
"do you have a lowering?" against the same registry §6 pins. The result is a
diagnostic ON THE FUNCTION, non-blocking:

| severity | meaning | blocking |
|---|---|---|
| ERROR | typing failed (unknown name, typer gap) | yes |
| WARNING | typed, but reaches a native with no SQL meaning — directly or through anything it calls | no — predicts the run-time wall |
| INFO | typed and lowers, but reaches a known platform limitation (a dialect capability) | no |

Diagnostics are FACTS, so they are rows: (function, call site, native, reason),
riding the compile artifact beside the program facts we already produce — never
a static sink, never an environment flag. Warnings propagate through calls with
the same body descent `Compiler.callsVerdict` uses. Boundary: the pass sees the
natives the typed tree names; a run-time wall from data (an unpredicted match
arm, a dialect refusing a spelling) is not a compile-time fact and is not
claimed as one.

What it buys: the corpus harness knows before running which tests are predicted
to wall and why; the ledger's engine-machinery and residue buckets become a
query over diagnostics; a test that fails WITHOUT a predicted wall — or passes
WITH one — is surfaced automatically.

## 8. Corrections this makes to earlier decisions

- **The five `testConnectionEquality*` tests** are ENGINE-MACHINERY: their
  subject is the engine's plug-in registry (`relationalExtensions()` →
  `routerExtensions` → the `connectionEquality` closures), which is engine
  internals by question 4. Not code-as-data; not a Java rewrite; not by-need.
  If the platform ever needs connection identity itself, that is a real native
  with a real lowering, and it still would not make these five pass.
- **By-need record fields** (CODE_AS_DATA_HOMEWORK §4) are rejected as a
  mechanism: they let the compiler partially execute engine internals, decided
  field by field, with no principle for the next field. Batch 147's row-18
  chase and class flip were that mechanism in disguise.
- **Function references as values** (batch 147 ledger row 18) is not a Phase 5
  item; it returns only if a kind-3 witness asks for it.
- **The typing-surface natives** (`relationalExtensions`, `setUpDataSQLs*`,
  `createDbConfig`, `moduleExtension`, and batch 147's additions) are §6
  violations: each becomes a generated body (a spec `function`), a real
  lowering, or is deleted with its tests naming the wall.
- **`Any` stays property-free**; m3's `classifierGenericType` is served by the
  Typer like `elementOverride` — a measured fact (batch 147 row 12), not a rule
  change.
- **The `Any`-to-text rendering gap**: a compiled `$x->toString()` on an
  `Any`-carried string came back JSON-quoted (batch 147, `testPairCollectionToString`
  under the flip). A lowering bug with that test as its witness once
  `Pair.toString` is a generated body.

## 9. Batch 148, in order

1. Amend `docs/WORLD_MAP.md` (§8 there) — this document is the text.
2. The governance test that types every generated body once and produces the
   typing work list as rows; the poison sentinel for broken overloads.
3. Generator: derived properties and spec `function` bodies emitted with the
   shapes, receipts pinned. First witnesses: `Pair.toString` / `List.toString`
   (delete the two Java arms; fix the `Any`-to-text rendering), then the
   phantom natives' spec bodies (`paginated`, `union`, `reactivate/1`,
   `whenSubType`), deleting each phantom registration as its body lands.
4. Lowering coverage: every native has a lowering or a named row;
   `NativeCatalogGovernanceTest` pins the list shrink-only.
5. Diagnostics (§7) as rows on the compile artifact; the corpus census reads
   them.
6. The five connection tests move to the engine-machinery bucket; Phase 5's
   code-as-data pool is the remaining 11 (`docs/PHASE5_SIZING_2026_09_08.md`).

## 10. The prelude is a MODULE (decided 2026-09-08, batch 151)

USER: "why do we have two different ways to do derived properties? why doesn't everything go through the user pipeline?"
The generated prelude lived in `Pure.java`'s static catalog (`Pure.nativeClass(...)` per declaration, no imports, no
resolver, no normalizer), so its derived properties needed a second lift path (`FunctionCompiler`'s on-demand lift) and
its bodies would have needed a protocol-to-Pure printer. The user pipeline already does both for user classes, and
`SystemMetamodel` already IS system Pure compiled through it (the boot layer, §V2 2026-09-02).

**Decision.** The generated prelude is a Pure SOURCE (`core/src/main/resources/com/legend/builtin/prelude.pure`):
one `###Pure` section per spec file, the file's `import` lines, then the shapes exactly as printed today plus every
derived property COPIED VERBATIM from the spec declaration. `Prelude.java` is a small hand-written reader. The compiler
adds it to the BOOT LAYER beside the system metamodel: resolved (the section imports qualify the derived bodies),
normalized (derived properties lift to `<owner>$prop$<name>` like a user class's), cached once per process, joined into
every graph. The resolver's bare-name fallback knows the prelude's names as it knew the catalog's; a graph element
redefining a prelude class or enum is dropped in favour of the prelude (what the catalog-first lookup did silently).

**Phases (one batch each, lanes exact between them).**
1. Mechanism only (this batch): the shapes move from the catalog to the boot layer; no shape changes; the census's
   last 3 rows close because the derived properties exist.
2. Migrate the 84 hand-declared classes of `Pure.java` into the generated module a family at a time
   (docs/HAND_SHAPE_DIVERGENCE_2026_09_08.md §4); kinds A–D of the sweep dissolve by construction. `Pure.java` keeps
   native signatures, `Lite`, and the bootstrap handful with receipts.
3. The bootstrap handful: `Any`'s layout rule (reflection-typed properties have no slot) then its spec properties;
   the Typer's hand-served `classifierGenericType`/`elementOverride` go.
4. The store-shaped divergences (`PropertyOwnerImplementation` rows; `Enum`-typed enum-value rows).
5. `tools/shape_sweep.py` becomes the governance pin; `FunctionCompiler`'s on-demand lift is deleted with the last
   catalog derived property.

**Tenets and homework (added the same day):** `docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md` — T1 the prelude is what exists before
any program (legend-pure platform + Java vocabulary + closure); T2 the graph is what programs declare or import by file (engine
modules included; "the corpus names it" is not a prelude reason); T3 a prelude declaration names only prelude/catalog types; T4 a
name on both sides is a modeling error (prelude wins transitionally, receipt list to zero); T5 the module is a closed library the
boot layer checks. Emission = the spec declaration VERBATIM under its file's imports (no re-printing). Phase 1 = mechanism with
today's demand; phase 3 re-scopes demand to T1/T2.
