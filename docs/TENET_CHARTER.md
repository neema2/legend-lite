# The Tenet Charter — "Java orchestrates, the DATABASE executes"

> **F0.2 of `docs/FOUNDATIONS_PLAN.md`** (2026-08-16). This document is the adjudication
> authority for every tenet question: the `java.sql` funnel's exemption list (F1.3; since 2026-10-30 the JDBC
> census register's classes, each pinned to its acts: `JdbcSurfaceCensusTest.REGISTER`, D69), the
> host-channel invariant (F1.5), the tenet ratchet's licensed seam (F1.10), and Phase 4's
> Java residue each cite a clause here. It restores the two charter clauses audit round 1
> demanded (V0.2 model-space vs data-space, V0.6 the host channel) and round 2 re-demanded
> (`docs/TENET_AUDIT_2026_08_16.md` §8) — the reason finding A9 was "adjudicable only by
> hand" is that this document did not exist.
>
> A clause is cited by rule ID (C1.3 = clause 1, item 3). Changing a clause is a reviewed
> design decision, not an edit-in-passing.

## Clause 1 — Orchestration: what Java MAY do

Java is the driver, the transport, and the boundary. It MAY:

- **C1.1 — JDBC transport and session management.** Open/close connections, manage
  transactions and savepoints, submit statements, stream results, bound the driver
  (fetch size, timeouts).
- **C1.2 — Typed value CARRIAGE.** Move a value from a `ResultSet` to a consumer without
  computing from it: no arithmetic, no comparison that decides a result, no reformatting
  beyond what the JDBC accessor itself returns for the column's declared type.
- **C1.3 — Compile-time-fact emission ("types drive construction").** Emit envelopes,
  headers, and schemas from the COMPILED PLAN's facts — column names, Pure types,
  multiplicities — never from inspecting values. The CSV header row, the JSON envelope,
  the TDS schema line are model-space artifacts.
- **C1.4 — Control flow.** Statement ordering, phase sequencing, per-statement error
  routing, retry/refusal policy — over OPAQUE results, without reading into them.
- **C1.5 — Byte transport.** Carrying DB-rendered artifacts (a rendered CSV line, a
  DB-built JSON document) to sockets, files, and asserts untouched.
- **C1.6 — Model-space computation.** Anything computed over the MODEL — types, mappings,
  metamodel navigation, plan text, declared column precision lattices — is compilation,
  not execution. `MetamodelWalk` and `GraphEmission` are the certified examples
  (audit §9). The test: could this run with no database attached and no data loaded?

## Clause 2 — Execution: what Java MAY NOT do

Any of the following over a value that crossed (or will cross) a `ResultSet` is execution,
and belongs in the database:

- **C2.1 — Deriving values.** No arithmetic, hashing, string transformation, concatenation,
  or any computation whose input is a result value and whose output reaches a consumer.
- **C2.2 — Deciding types from values.** No type, kind, or multiplicity chosen by
  inspecting a value's magnitude, text shape, or precision (the audit's §6-Q5(b): "reads
  magnitude"). The type is a fact the compiler already owns; ask the plan, never the cell.
- **C2.3 — Reshaping results.** No sorting, filtering, deduplicating, aggregating,
  grouping, slicing, or unnesting of result rows. Row order is the database's; a
  comparison POLICY that tolerates unordered results is legitimate (it judges, it does
  not repair), but it must be gated on a compile-time fact (`sortedChain()`), counted,
  and one-sided.
- **C2.4 — Fabricating values.** No value a consumer can observe that the platform never
  computed: no manufactured UUIDs, no empty collections standing in for unrecorded data,
  no `get(0)` fallbacks, no defaults that lose a type (`default -> "String"`). Absence is
  a loud `NotImplementedException`, never a plausible value.
- **C2.5 — Rendering ruled representations.** No Java rendering of a value whose print
  form has a REPRESENTATION RULE (float repr, date/subsecond forms, decimal scale). Those
  rules live in the SQL emission path (`Scalars.floatRepr`, `DateFmt`) and render in the
  database (Phase 4). The only exception is Clause 4.

## Clause 2b — Platform natives (ratified 2026-08-18)

Tenet #1 governs DATA evaluation: any value derived from stored data reaches the user
through SQL. It does NOT require pushing every Pure construct into the database. A
legend-pure semantic may be implemented as a NATIVE JAVA PLATFORM FUNCTION where pushing
it down is senseless or wrong — asserts, unordered/multiset checks, metamodel operations,
comparison policies over already-produced results. Three conditions make such a native
legitimate rather than a shadow implementation: (1) ONE owner, in the platform
({@code com.legend}), on the compiled surface — never a harness-private copy; (2) the
engine/legend-pure {@code .pure} source is the SPEC it is ported from and verified
against; (3) it is registered — the eval ledger distinguishes "platform native" from
"evaluation residue awaiting eviction". This is how legend-pure itself is built (the
reference interpreter's natives are Java); an implementation of Pure is not un-Pure for
having Java natives. What remains banned: data-derived values computed host-side, and
SECOND implementations of semantics the platform already owns (the harness's private
equality/envelope/decode copies are migration targets, not exemplars).

## Clause 2c — Two worlds, one spec (ratified 2026-08-19, phase-2 deep audit)

Equality (and every value semantic) necessarily exists in TWO worlds: **World 1 — the
host adjudication layer** (`PureAsserts`/`GridCompare`/`JsonCompare`: comparing an
EXPECTED value against a FETCHED result to produce a test verdict), and **World 2 — the
compiler** (an in-query `x == y` must lower to SQL; the lowering rules ARE that world).
World 2 cannot not exist — it is the compiler; World 1 is Clause 2b's adjudication
grant. The doctrine: **verdicts are World 1's job, in-query computation is World 2's,
and NEITHER world reimplements the other's job.** Compiling the assert library's pure
bodies into SQL to produce verdicts violates this clause exactly as a harness-private
comparator does (the Phase-4 seam arms were this violation's cost, witnessed). Both
worlds cite the same legend-pure spec; their agreement on shared ground — and every
DECLARED divergence (SQL null-vs-pure-true, dialect coercions) — is pinned by the
`EqualityWorldsConformanceTest` fixture: drift in either world is a red test, never a
discovery made three phases later.

**Z2 ruling — verdict-channel scope (ratified 2026-08-19).** World 1's equality
(`PureAsserts.equalScalar`/`equal`) carries three kinds of arm, adjudicated separately:
**SPEC** arms (integral×Decimal numeric equality, scale-blind Decimal, IEEE
non-finite — witnessed pure semantics, sound for any caller); **CARRIER-DECODE**
bridges (the temporal string carrier — the platform's designed wire representation,
sound for any consumer of the wire); and **TEST-CHANNEL TOLERANCES** (the TDSNull
sentinel, the 2-ULP dialect-arithmetic leniency — sound ONLY because every caller is
an adjudicator). The ruling: `PureAsserts` equality is the VERDICT CHANNEL and nothing
else. Product equality is World 2 — an in-query `equal()` lowers to SQL and the
database is the authority — so no product surface may route value equality through
`PureAsserts`; if a genuine product host-side equality need ever appears, it gets its
own SPEC-ONLY comparator (no tolerance arms) as a witnessed design leg. Enforced
mechanically: `VerdictChannelRegisterTest` pins the caller file set (comment-stripped
source scan) to the adjudication cluster — a new caller is a red build until
consciously registered here with its tenet argument.

## Clause 3 — Provenance, not arms (the host channel)

**No `ResultSet`-derived value may be EVALUATED in Java on the host channel.** The
invariant is on the PROVENANCE of the value, never on an interpreter arm list: audit §8
proved a ~6-line dispatch edge reclassified all 47 then-existing arms at once — 18 were
dual-use (`fold`, `map`, `at`, `size`, `eq`, `in`, `filter`…) and became execution the
moment a data-space value flowed through them. *(Re-scoped 2026-08-18,
`ADVERSARIAL_TENET_AUDIT_2026_08_18.md` §5: `HostEval.eval()` no longer exists — the
interpreter was DELETED under the oracle-not-runtime principle.)* Today the host channel
is `GridReads.tryLower` (grid chains COMPILE to SQL; DB values flow only through carriage
into results, never through computation) and `StoreNav` (compiled-model reads, no DB
values), and everything else walls loudly. Enforcement: the interpreter's nonexistence,
`ArchitectureTest.theInterpreterPerformsNoJdbc`, and the `JavaEvalLedgerTest` register.
Known enforcement limits are recorded in the audit's §3 — the guards catch drift, not
adversaries; residue DELETION (the relation-typed `fetchDb` leg) is the durable fix.

## Clause 4 — The literal exception, once (the LiteralFold rule)

Java may answer without the database ONLY for a value that satisfies both conditions:

1. **Syntactic value, zero computation** — the value appears VERBATIM in the typed AST
   (a bare literal node), never a composite, variable, or call.
2. **Representation-trivial round trip** — no coercion, width, scale, or format rule would
   be applied on the SQL path. If a rule exists, answering in Java duplicates the rule in
   a second place, and the answer belongs to the database even for a literal.

The canonical instance is `LiteralFold.ADMITTED = {String, Boolean}` — Integer fails
(driver width + lattice promotion), Float/Decimal fail (scale/format), Date fails
(partial-date carriers). The admitted set is pinned differentially by
`ConstantPlanParityTest`: **admitting a kind is a green differential, not an argument.**

The SAME rule with the SAME pinning mechanism governs every other Java-answers-locally
site: Phase 4's literal identity-render arm, and any future fold. One rule, N
applications, zero new judgment calls. A site that wants a wider set must first widen
`LiteralFold` behind a green differential — there are no site-local admission rules.

## Clause 5 — The ingress mirror

Ingress obeys the same split in reverse:

- **C5.1** — Values entering the database (CSV seeds, JSON payloads, generated data) are
  bound as literals/parameters and the DATABASE casts them to the model's column types —
  "uniform policy, no host-side type dispatch" (`TestDataGenerator.loadSide` is the
  certified instance). Java never types an ingress token by regexing its text while the
  resolved column type is in scope (the typed-fact test).
- **C5.2** — Model-derived SQL is spelled correctly the FIRST time from the type in hand.
  Java never emits text it must then rewrite (`RawSqlBoundary` is for corpus-AUTHORED
  text only — text whose origin is another dialect).

## Clause 6 — Programs are input; the compiler compares, the database computes (ratified 2026-09-03)

The conceptual map is `docs/WORLD_MAP.md`. Pure code is one of three kinds: NATIVES (meaning =
one Java SQL lowering rule each), PLATFORM SEMANTICS (Java, designed from the engine's Pure as
spec; the deletion test — "would any user query stop compiling without it?" — decides), and
PROGRAMS (input to the compiler, whoever wrote them). Consequences:

- **C6.1 — A program is never ported to Java.** A wall in a program is compiler work: a
  missing structural fold, residual form, lowering, or loading rule. Java that knows a
  program's name is a violation (`ObservabilityGuardrailTest` string-dispatch pin).
- **C6.2 — The unroll compares; it never computes.** `LiteralUnroll` may decide only what is
  visible in the program text: arm choice by a literal's class, spelled fields and defaults,
  list shape over spelled lists, identity of two spelled scalars of ONE kind, a spelled enum's
  name, `newMap`/`get`/`groupBy` over spelled pairs, `assert(true)`. Any operation that
  produces a NEW value (`toLower`, `+`, arithmetic, cross-kind `==`) is the database's, carried
  as a residual (`CASE` over scalars, lists, same-class structs; conditional membership).
  This is Clause 4 applied to the unroll: token identity carries no representation rule,
  so it needs no differential; a value-producing fold does, and is therefore not admitted.
- **C6.2a — Computing a query's schema is type checking, ring-fenced** (ruled 2026-09-29, the user). Some library
  functions compute their own result columns: `columnValueDifference` builds `quantity_1` with `$vc + '_1'`, loops over a
  list of column names with `map`, and branches on a column's type with `if($col.type == Integer, …)`
  (`core_relational/relational/tds/tdsExtension.pure`). The compiler must know those columns to type the rest of the
  query and to report the result, with no database present. The type checker therefore evaluates such expressions at
  compile time, inside this fence and nowhere else:
  1. **Where:** only the schema positions of a body marked `<<functionType.NormalizeRequiredFunction>>` (whether the fence
     reaches unmarked helpers such a body calls, e.g. `extendMatchColumns`, is decision D19, open), and the
     column-metadata reads (`$tds.columns`, `$col.name`, `$col.type`). A schema position is one whose value becomes part
     of the query's type: a column name, a list of column names, a rename pair, a column spec's name, or the condition
     of an `if` that selects between column expressions.
  2. **What:** a closed, pinned list of operations over names and column metadata (string `+` over strings, `map`,
     `filter`, `sortBy`, `indexOf`, `in`, `concatenate`, `removeDuplicates`, `removeAll` over lists of names, `pair`,
     `$col.name`, `$col.type ==`), illustrative until W4.2's homework pins the list from what those library bodies
     actually use.
  3. **Never a row value.** An expression in any other position is left for the database, even when every input is a
     literal (`'a' + 'b'` inside a filter stays SQL). A test pins that a data-position expression is never folded.
  4. **Anything else is a clear error**, naming the expression and the fence; never a guess and never a partial fold.
  5. Every row value such a function produces is computed by the database, as today.
- **C6.3 — The prelude ships declarations only** (class/enum shapes with keys and defaults,
  native signatures), verified against the real `.pure`; the ONE body exception is a Pure
  QUERY over the platform's own system tables (no control flow on computed values, no
  recursion).
- **C6.4 — Residue is named, never ported**: recursion over row-backed trees (tier 2),
  effects, IO, reflection over the live graph.

## Enforcement map

| Clause | Mechanical enforcement |
|---|---|
| C1/C2 boundary | F1.3 `java.sql` funnel, its exemption the JDBC census register's classes with their acts (`JdbcSurfaceCensusTest`, one list for both guards; F1.3b, the root class-list pin, deleted 2026-10-30 as subsumed, D69), and `noJdbcTypeInAPublicSignatureOutsideTheSeam`; F1.10 tenet ratchet over ResultSet-consumption sites outside the C1.2/C1.5/Clause-4 seam |
| C2.3 | F1.4 positive harness rule (`sortedChain()`-gated, enumerated allowlist) |
| Clause 2c Z2 | `VerdictChannelRegisterTest` (caller file-set pin over `PureAsserts.equal*`) |
| Clause 3 | F1.5 `HostChannelPredicateTest` |
| Clause 4 | `ConstantPlanParityTest` (exists); Phase 4 render arm cites it |
| C5.2 | F1.6 R0 ledger (shrink-only) → F7.4 makes the contract true |
| C6.1 | `ObservabilityGuardrailTest` string-dispatch pin; `ArchitectureTest` metamodel-channel class-list (shrinks with the toPostgresModel leg) |
| C6.2 | `LiteralUnrollLedgerTest` — the exact fold set of `LiteralUnroll`, compare-only (lands with the residual leg) |
| C6.2a | The schema evaluator's pinned operation list and its never-a-row-value test (lands with execution plan W4.2) |

Until a clause's enforcement lands, the clause still governs adjudication — "the guard is
not built yet" is a schedule fact, not a license.
