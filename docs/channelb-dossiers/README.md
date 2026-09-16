# Channel B burndown — design dossiers

Produced 2026-08-27 by a **read-only** research session. No file inside
`/Users/neema/legend/legend-lite` was written, no JVM was run, no process
was signalled. The burn session owns that tree exclusively.

## Path substitution (read this first)

The commissioning brief asked for these dossiers at
`/Users/neema/legend/channelb-dossiers/`. That directory could not be
created: `/Users/neema/legend` is `drwxr-xr-x  neema:staff` and this
session runs as `neemsandv`, so the path is readable but **not writable**
by us. The dossiers are therefore written to

    /Users/neemsandv/legend/channelb-dossiers/

which is also outside every repository, and is readable from the burn
session (both home directories are group `staff` with `r-x`). Move them
if you prefer the original location; nothing here depends on the path.

## Sources of truth used

| Role | Path | Notes |
|---|---|---|
| Charter (hypotheses) | `/Users/neema/legend/legend-lite/docs/CHANNELB_BURNDOWN_HANDOFF.md` | §3 treated as claims to verify, never as fact |
| Our code (read-only) | `/Users/neema/legend/legend-lite/core/src/main/java` | the ACTIVE tree the burn session is editing |
| Reference spec | `/Users/neemsandv/legend/legend-engine`, `/Users/neemsandv/legend/legend-pure` | the ONLY authoritative engine source |
| Explicitly excluded | `/Users/neema/legend/legend-engine` | stale July tag — never read |

`/Users/neema/legend/legend-lite` and `/Users/neemsandv/legend/legend-lite`
are **distinct checkouts** (verified: different inodes, different owners)
that share the remote `github.com/neema2/legend-lite`. Citations of "our
code" in these dossiers are line numbers **in the active `/Users/neema`
tree** as of 2026-08-27; verify against `git status` before trusting a
line number after further commits land.

## Verified environment facts

- **`Lowerer.java` is exactly 3500 lines** — measured 2026-08-27,
  `core/src/main/java/com/legend/lowering/Lowerer.java`. It is AT the hard
  guardrail, not near it. **No leg may add a line to it.** Any design that
  must touch Lowerer forces the seam-split first; every dossier states
  whether it touches Lowerer.
- Positions exist and are well-engineered at PARSE time
  (`TokenStreamCursor.spanOf`, "file-absolute; correct by construction" —
  `docs/SPAN_ORIGIN_CONSOLIDATION.md:23`), and a `SpanOrigin` value type
  names every span-construction quirk (landed `c9982067`, same doc §
  "Migration order"). Positions are therefore *lost downstream*, not
  *absent upstream*.

## Facts supplied by the commissioning session (build on, do not re-derive)

1. **`TypedSpec` nodes carry NO source position.** This is leg 2's central
   gap. Stamping `pos` onto `TypedNativeCall` perturbs record equality and
   `UserCallInliner`'s `sameRefs` — enumerated in leg 2's dossier.
2. **`StructLit.Field.declared` now renders as `CAST(NULL AS <slot>)`** in
   the DuckDB dialect.
3. **The F17 `+=` desugar lands via `AppliedProperty` at
   `NewChecker.checkCopy`** (leg 1, in flight — not covered here).

## The rules every design in these dossiers is judged against

Quoted from the project's own authorities so no dossier paraphrases them.

### From `AGENTS.md` (architectural invariants)

- **Layer ownership (AGENTS.md:117-130).** The Lowerer owns
  `TypedSpec → SqlQuery` and is forbidden "naming SQL functions; SQL
  syntax; importing a dialect; any `String` field encoding a SQL
  operation". `SqlDialect` owns `SqlQuery → SQL string` and may not infer
  types, rewrite HIR, or consult the model. `NameResolver` owns simple
  name → FQN and may not consult the compiled model or type-check.
- **Invariant 1 (AGENTS.md:142-158).** The frontend (phase F element
  compiler; phase G `SpecCompiler`/`Typer`/`InferenceKernel`/checkers) is
  the single source of truth for types. "If a type is missing downstream,
  the frontend has a bug — fix the frontend."
- **Invariant 2 (AGENTS.md:160-175).** The Lowerer does no type
  inference; structural dispatch only, never `instanceof CInteger` type
  dispatch.
- **Invariant 3 (AGENTS.md:177-203).** The dialect owns ALL SQL
  rendering. One entry point, `SqlDialect.java:14`. Render methods are
  switch **expressions** with **no `default ->`**; an inexpressible
  variant THROWS `UnsupportedOperationException` — that is still an arm.
- **Invariant 3a (AGENTS.md:205-243).** The MIR is closed, sealed, pure
  data. **"New native = new MIR variant + new render arm."** No
  `FunctionCall(String, args)` catch-all; no `String` field encoding a SQL
  operation (sole carve-out `SqlExpr.Cast(expr, pureTypeName)`); MIR never
  holds a Pure AST node.
- **Invariant 4 (AGENTS.md:244-252).** NO FALLBACKS. NO DEFAULTING.
  "Every defaulting branch is a bug hiding behind a safety net."
- **G½ (AGENTS.md:79, 89-90).** `UserCallInliner.inlineBody` runs on
  **every** execution path.
- **Phases (AGENTS.md:92-95).** `PARSE, RESOLVE, NORMALIZE, MODEL, TYPE,
  MAPPING, LOWER, EXECUTE`. **There is no RENDER phase.**
- **Guard files (AGENTS.md:10-15).** A ceiling, pin, or allowlist entry
  moves ONLY with a dated justification comment naming the task or
  incident — never as an edit-in-passing to make a build green.

### From `docs/TENET_CHARTER.md` (the adjudication authority)

- **C1.6 — model-space computation** (`:33-36`). Types, mappings,
  metamodel navigation and plan text are *compilation, not execution*.
  The litmus test: **"could this run with no database attached and no data
  loaded?"** Legs 3, 3b, 4, 6b and 7b pass this test outright, so the
  execution tenet does not constrain them.
- **C2.2 — deciding types from values** (`:44-46`). "No type, kind, or
  multiplicity chosen by inspecting a value's magnitude, text shape, or
  precision. The type is a fact the compiler already owns; **ask the plan,
  never the cell.**" (Governs leg 7.)
- **C2.3 — reshaping results** (`:48-51`). "**No sorting, filtering,
  deduplicating, aggregating, grouping, slicing, or unnesting of result
  rows.** Row order is the database's" — a tolerating comparison *policy*
  is legitimate but must be gated on the compile-time fact
  `sortedChain()`, counted, and one-sided. (Governs leg 5; settles it
  without further argument.)
- **C2.4 — fabricating values** (`:52-55`). "**Absence is a loud
  `NotImplementedException`, never a plausible value.**" A wrong line
  number, an invented supertype, an arbitrary overload tiebreak and a
  silently-skipped grammar construct are all this violation.
- **C2.5 — rendering ruled representations** (`:56-59`). No Java
  rendering of a value whose print form has a representation rule; "those
  rules live in the SQL emission path (`Scalars.floatRepr`, `DateFmt`) and
  render in the database". (Governs leg 6a.)
- **Clause 2b — platform natives** (`:61-76`). A Pure semantic MAY be a
  native Java platform function where pushing it down is senseless —
  "asserts, unordered/multiset checks, **metamodel operations**,
  comparison policies over already-produced results" — subject to three
  conditions: one owner in `com.legend` on the compiled surface; the
  `.pure` source is its spec; it is registered in the eval ledger.
  (Licenses leg 3b and leg 2's `AssertErrorNative`.)
- **Clause 2c — two worlds** (`:78-92`). Verdicts are World 1's job
  (`PureAsserts`/`GridCompare`), in-query computation is World 2's, and
  "**neither world reimplements the other's job**". Compiling the assert
  library's Pure bodies into SQL to produce verdicts is a violation.
  (Governs leg 2.)
- **Clause 4 — the literal exception** (`:126-144`). Java answers locally
  only for a verbatim literal with a representation-trivial round trip;
  `LiteralFold.ADMITTED = {String, Boolean}` — Integer, Float, Decimal and
  **Date** all fail. "Admitting a kind is a green differential, not an
  argument." There are no site-local admission rules.

## The legs

Leg 1 (F17 fold) is IN FLIGHT in the burn session and is not covered here.
Leg 8 is a ledger write-up with no code and is not covered here.

| Dossier | Leg | Charted rows | Verdict |
|---|---|---|---|
| `leg2.md` | assertError source positions | 6 | **ALREADY LANDED** during this research |
| `leg3.md` | function-type common supertype | 3 | rule confirmed; **second wall found** |
| `leg3b.md` | the `deactivate` platform function | 1 | portable, not ledgerable; **second wall found** |
| `leg4.md` | named-function references as values | 2 | machinery exists; **name-qualification fix** |
| `leg5.md` | comparator / key ordering | 3 | **1 winnable, 2 → ledger** |
| `leg6.md` | instance `toString` + overload tie | 3 | charter's mechanism wrong on all 3 |
| `leg7.md` | `parseDate` kinds | 1 | **not a date leg** — boolean-carrier defect |
| `leg7b.md` | the grammar walls | 4 (+3 blocked) | **banks ZERO rows** |

Each dossier is structured: (a) reference semantics, (b) our seam,
(c) minimum design as decisions, (d) traps, (e) confidence and the live
probes still needed — ending in open questions rather than guesses.

---

# SYNTHESIS — read this before planning the burn

The charter's §3 mechanism notes were treated as hypotheses. **Most did not
survive.** Seven findings change the plan.

### 1. Leg 2 landed while this research ran — the charter's §1 roster is stale

Between **17:33:25 and 17:46:30 on 2026-08-27** the burn session landed leg 2 in
full (11 files, mtimes in `leg2.md` §b.0). Consequences:

- The commissioning brief's premise "`TypedSpec` carries NO source position" is
  now false — `TypedNativeCall` has a `pos` component with hand-written equality
  that excludes it.
- The charter's `Essential 297/327` is stale: `ChannelBEssentialTest.java:78` now
  pins `pass >= 305`, measured **305**.
- **Every line citation in these dossiers is a moving target.** Re-verify before
  acting on any of them.

### 2. Leg 7b banks ZERO rows — the largest single correction

The six parse-wall files contain **no `<<PCT.test>>` function at all**. The "3
blocked essential discovery rows" never existed — a `PCT.test` string-match
miscount (two comments + one Profile named `PCT.testQualifierProfile`), proven by
set difference in both directions. The 4 Grammar ERROR rows are **not** parse
walls. Burning all six walls moves PASS by 0 and discovery by 0; it moves only the
wall count, 19 → 13.

**The only row-bearing item in that neighbourhood is ~2 lines in
`PureModelContext.elementFqns()`** (primitive-extension FQNs are recorded by
`ModelBuilder` but never published to the resolver), worth **2 Grammar rows**.

> **LIVE-STATE ADDENDUM — three legs landed while this research ran.**
> As of `origin/main` @ **2aa40bee** (fetched 18:25):
>
> | commit | leg |
> |---|---|
> | `183aeb33` | Channel B leg 1 (F17 fold): `+=` copy-add desugar + declared-slot CAST on NULL struct fields |
> | `9465a235` | Channel B leg 2: assertError positions — the raise emission carries its source span |
> | `2aa40bee` | Channel B leg 3: FunctionType common supertype **+ match branches through a let-bound variable** |
>
> **Leg 3's commit title independently confirms finding 3 below.** The burn
> session hit the same second wall this research identified, and built exactly the
> design `leg3.md` recommends as D5 option **(B)**: `Env.java:21` now carries
> `Map<String, ValueSpecification> exprAliases` with an `exprAlias(name)` accessor
> (**:75**), and `MatchChecker.branches` now takes an `Env` (**:304**) and resolves
> a bound variable through it (**:316** `return branches(bound, env)`).
>
> **Consequence for readers:** `leg2.md` and `leg3.md` are now *reviews of landed
> work*, not designs — their value is the reference-semantics sections, the traps,
> and the residual open questions. Line citations for `Env`, `MatchChecker`,
> `InferenceKernel`, `Typer`, `SpecCompiler`, `NameResolver`, `TypedNativeCall`,
> `PureSql`, `Scalars`, `AnsiSqlRenderer`, `H2` and `RaisedErrors` are **already
> stale**. Re-verify before acting on any of them.

### 3. `MatchChecker` is the hidden blocker for FOUR rows across two legs

Neither the charter nor either leg's §3 note mentions it:

- **Leg 3** (3 rows): `MatchChecker.branches` (`:299-308`) acquires branch lambdas
  **syntactically** and cannot see through a `let` variable. Fixing
  `commonSupertype` alone moves all three rows from one compile error to
  `match expects a collection of branch lambdas`. *This is why the three inline
  "twin" tests pass while the three `let`-indirected ones fail.*
- **Leg 3b** (1 row): `MatchChecker`'s narrowing returns the *selected branch's*
  type (`Integer`), not the declared LUB (`Any`), so even a perfect `deactivate`
  port leaves the row failing `expected Any / actual Integer`.

**Probe `MatchChecker` before writing code on either leg.**

### 4. Two of leg 5's three rows are not winnable — the ledger grows 9 → 11

`testSimpleSortWithKey` and `testSimpleSortWithFunctionVariables` are **not**
ordering bugs. The comparator and direction are already correct; the rows fail on
the **A1-adjudicated 1-based `substring` divergence** the charter itself forbids
re-attempting. The charter's parenthetical "(expected DESC …)" is wrong — the
expectation is ASC on a substring key. Leg 5's real content is one row, and it is
a genuine silent-wrong-answer defect (cross-kind `sort()` orders by literal
spelling).

### 5. "Burning Channel B burns Channel A for free" does not hold for leg 4

Channel A **already passes** both leg-4 rows (327 run, 0 failures) because the
reference composer hands us an inlined brace-lambda, never a function reference.
The two channels' shapes genuinely diverge here. The charter's §Mission claim
(`:8-11`) is not general.

### 6. Two charter legs are misclassified at the mechanism level

- **Leg 7 is not a date-kind leg.** Date kinds are modelled completely and
  selected correctly. The row fails on the already-CONFIRMED **A24/D92
  boolean-carrier defect**: `has*` over a non-literal emits `IntLit(1)` under a
  `Boolean[1]` stamp, and `SqlTyping.reconcileLabels` silently adopts BIGINT —
  which is also why the type census cannot see it. One emission arm + two pins.
- **Leg 6b is a candidate-set problem, not a scoring problem.** The reference's
  scoring **ties identically** on `Nil`; it is saved by repository layering
  (`core_functions_relation` depends on `platform`, so `relation::toString` does
  not exist at that call site). "Fix the scoring to the engine's real rule" has no
  target. Our registry unions overloads by bare name across packages with no
  provenance dimension.

### 7. Two Grammar ERROR rows have NO leg owner

From leg 7b's measured census, the 4 Grammar ERRORs are:

1. `getAll::testBasic` — `class query requires an execution context` — **unowned**
2. `eq::testEqPrimitiveExtension` — `unknown type 'ExtendedInteger'` — leg 7b R0
3. `equal::testEqualPrimitiveExtension` — same — leg 7b R0
4. `plus::testPlusInIterate` — `a scalar query has no row scope for $p.lastName` —
   **unowned**

Rows 1 and 4 appear in no leg of the charter.

---

## Revised winnable count

Charter: **21 Essential + 4 Grammar = 25**. After verification:

| Leg | Charted | Actually winnable | Note |
|---|---|---|---|
| 2 | 6 | **LANDED** (`9465a235`) | residual cleanups + 2 adjudications only |
| 3 | 3 | **LANDED** (`2aa40bee`) | LUB + the let-bound-branch second wall |
| 3b | 1 | 1 | needs the port **and** a declared-type accessor |
| 4 | 2 | 2 | name qualification only |
| 5 | 3 | **1** | 2 rows → A1 ledger (9 → 11) |
| 6 | 3 | 3 | 2 different mechanisms, both re-diagnosed |
| 7 | 1 | 1 | boolean-carrier emission arm |
| 7b | 4 (+3) | **2** | via `elementFqns()`, not parser work |
| — | — | *2 unowned* | Grammar rows 1 and 4 above |

**≈ 10 winnable rows remain of the charter's 25**, plus 2 rows that need a new
owner and 2 that move to the ledger.

## Suggested order (cheapest-and-most-certain first)

Legs 1–3 are landed; this is the remaining queue.

1. **Leg 7b R0** — ~2 lines, 2 rows, very low risk. Not parser work; do it before
   any parser work.
2. **Leg 7 D2** — one emission arm, net line-negative, 1 row, and it retires a
   CONFIRMED unsoundness. Probe the carrier class first.
3. **Leg 4** — resolver qualification, 2 rows. Run the FQN falsifier first; if the
   fully-qualified spellings pass, the whole leg is confirmed.
4. **Leg 3b** — the `deactivate` port. **Re-probe first:** leg 3 has just changed
   `MatchChecker`, so the narrowing behaviour §b.5 predicts may have moved.
5. **Leg 5 D4a/D4b** — 1 row; decide honesty (loud wall) vs the point (comparable
   channel). Amend the ledger to 11 rows in the same commit.
6. **Leg 6** — largest; 6a needs a dispatch decision (D5) that needs a probe, 6b
   wants the registry-provenance partition that also fixes `[]->sum()`, `[]->max()`
   and `->sort([])`.
7. **Leg 7b R1–R4** — drop-in parity only; re-justify as such. Split
   `SpecParser.java` (3440/3500) first — four of the six fixes land in it.

## Two standing hazards for every leg

- **`Lowerer.java` is at exactly 3500 = the cap.** No leg's design requires
  touching it, and every dossier states so explicitly. `SpecParser.java` (3440) is
  the next binding constraint and is leg 7b's real blocker.
- **Six Channel-B pins are at ZERO slack** (`pass >= 305`, `agreePass >= 293`,
  `wireBug <= 9`, `declined <= 0`, `trueWireBug == 0`, grammar `pass >= 133`). Any
  change that reclassifies one row into a different bucket fails the suite at three
  pins at once. Re-measure the full lane after each leg, not at the end.
