# The compatibility program: handoff to its owner session (2026-11-19)

The user's words, 2026-11-19 (~03:50 UTC): "yes set it up. Can we make it so that I can follow that session and it be
the place that owns the compatibility program including getting all the pure/engine stuff out of the product?"

This document hands the compatibility program to one owner session, which the user follows and talks to directly.
The previous orchestrator (session `session_0195rQEoRb1AGCHxtZChjWsg`) keeps product correctness and landings
(see §5) and stays reachable for coordination.

## 1. The mission

Get everything that means legend-pure or legend-engine out of lite's product, so that the product is one fast, lean
meaning and compatibility lives at the edges without leaking in. "Done" means: the core (compiler, planner, plan,
exec) has no mode, style, hook or facet that knows an upstream exists; every upstream spelling, envelope, message,
print form and refusal lives in the edge's input adapter or output presenters; the census of upstream meaning in the
product (B2) is blocking at zero, with only ruled exceptions; and lite still answers upstream clients and PCT exactly
where it did, or better, measured against upstream itself.

## 2. The architecture (the design under which the program is re-cut)

Written in full in `docs/records/compat-homework-2026-11-19.md` §1 (on branch
`claude/refactor-code-quality-review-9fqhug`). In short: the core is one meaning with no modes; compatibility is an
INPUT ADAPTER (rewrite the upstream program into the lite program that computes upstream's answer, or refuse by name)
plus OUTPUT PRESENTERS (pure functions from typed results, error records and the plan to upstream's bytes, after
exec, in the host); upstream's SQL text and internal shapes are never promised; the product (LITE) keeps BOTH judges
with parity, and compatibility judging (PURE, ENGINE) is host-only (the user's amendment).

## 3. The evidence (the homework spike, all four records done)

| | Record | Branch | Headline |
|---|---|---|---|
| H1 | `docs/records/compat-homework/H1-differences.md` (+ TSVs) | `claude/compat-homework-h1` | 281 known differences; every one fits one class; none needs a mode or a hook. P 40, R0 35, R1 24, J 13, X 48, S 121. 18 named capabilities (19 with scan order), 3 of them switches. 13 P rows need facts the typed result lacks. 21 X rows swing on (e'). |
| H2 | `docs/records/compat-homework/H2-text-in-queries.md` (+ data) | `claude/compat-homework-h2` | Text built in a query and then used: 0 upstream sites for Float, Decimal, dates; 14 sites, all Integer or enum. The R1 path is driven by PCT's toString/format unit tests, not real programs. |
| H3 | `docs/records/compat-homework/H3-prototypes.md` (+ evidence) | `claude/compat-homework-h3` | (c) legend-engine's TDS envelope from a presenter after the run: byte-identical, 0 product lines. (a) substring/indexOf as an ENGINE rewrite: H2 25/25, DuckDB 24/25, but the rewrite must know the TARGET database and ENGINE programs are whole MODELS. (b) A Float's text through one capability: H2 26/26, DuckDB 25/26, but the capability must be ONE MIR node (a lowering tree cost 350x the SQL). |
| H4 | `docs/records/compat-homework/H4-host-presenters.md` (+ evidence) | `claude/compat-homework-h4` | One Java presenter per audience, reached by the tab through WASM and by Python through the native library they already use. Today 12 rules are hand-kept in 2-3 languages (25+ copies), 7 already differ, none caught. Pilot: the store's refusal rule (HOSTS-9) with a cross-host byte-parity check. |

Also: the census (`docs/records/census-b2-inventory-2026-11-19.md`, branch `claude/census-b2`), the legend-engine
oracle (`docs/records/engine-oracle-pure-v1-2026-11-18.md`, branch `claude/presentation-r`), the old three-mode design
(`docs/records/presentation-program-design-2026-11-18.md`, branch `claude/presentation-design`; its safety net, census
and oracle carry over, its mode records and words-in-the-plan do not), and the PR #118 audit
(`docs/records/pr118-audit-2026-11-18/`, with `FOLLOW_UPS.md` §6-§8 the user's rulings and the P0 list).

## 4. The plan's outline (to be written as the re-cut plan, then ratified by the user)

| Phase | What |
|---|---|
| 0. Rulings | R-a (if not ruled by then), E-R1, R4; the policy for tests that check upstream's SQL text (588 toSQLString calls); the capability policy (each of the 18 a function with its own meaning, written as ONE node); the typed facts the result must carry; the database-version residue (legend-engine's patched H2 2.1.214 LEGACY, DuckDB 1.3.0) recorded, not chased |
| 1. Output: presenters (proven; first) | One Java presenter per audience over typed results plus the plan; the typed result's missing facts; error sites as kind, where and literal operands, worded by the presenter (replacing S1's words-in-the-plan extension); the hosts call the Java presenter (H4's pilot first, cross-host byte parity); the SQL-built presentation deleted (about 2,000 edge lines, RelationTextLowering, the envelope SQL); a class for envelope fields that are upstream's SQL (legend-engine's COUNT(*) label, the database's error text over upstream's statement): refused, or declared not promised |
| 2. The core without modes | PlanStyle, the two-form hooks and facets out of the product (45 parameters, 37 call sites); upstream SQL-shape reproduction deleted, checked by answers; the judges split (LITE both with parity; compatibility host-only, its database-judge half deleted: the census's 38 + 13 judge hits) |
| 3. Input: the adapter | A MODEL-level, TARGET-aware rewrite at the edge's doors; the capabilities as compact nodes; ENGINE's refusal table derived from the manifests; first separate the two product dependencies on today's meaning (the Cypher translator writes Pure in legend-engine's substring meaning; the ###Relational substring dynaFunction shares the Pure function's rule) |
| 4. Portability | Export and import of legend-engine plans (the oracle exists) |
| Throughout | The census blocking at the end; a guard that the product holds no mode branch; host byte parity; the per-push lanes (d') |

## 5. Ownership boundary

**The compatibility session owns:** the re-cut plan and the whole program; B1 (presentation lead, session
`session_0195rQEnAa5CYXHygAxEFh3Y`, paused before S2 on `claude/presentation-b1`, three guard items pending the user:
EdgeOnlyDataTest's Site#extensions row, EdgeOnlyMembersTest's two codec entries, wasm engine_routes's two re-pinned
hashes, all likely superseded by the re-plan); B1-R (the oracle, session `session_0195rQEkcevkNQ66fixkk4FF`,
`claude/presentation-r` and `claude/compat-homework-h3`; pending: a jars_table entry and per-push); B2 (the census,
session `session_0195rQEjuF2imym274M9Qvva`, `claude/census-b2`, report-only today); B3 (the separation close-out, not
started); the homework records; the compatibility rulings listed in §4 phase 0, plus (d') per-push lanes for the
compatibility checks, the prelude's register row (the user ruled the prelude and parser stay, 2026-11-19), and the
register corrections H1 found (S138, S63).

**The previous orchestrator keeps:** PR #121 (Q8, Q9, the edge's undeclared doors; R-a rides with it); Q2 (the
warehouse JDBC binds, `claude/warehouse-binds-q2`); A5 (judge parity: Q3b fixed, J3 pending a pin, Q3's design);
Q1 and the small-bugs sweep; main's CI; the other P0 wrong answers. Where a product fix touches compatibility (for
example Q3's list positions), the two coordinate by message before either builds.

## 6. Rules

- AGENTS.md at `main` is binding, above all: the upstream-fidelity tenet (upstream's answer is read from the pinned
  sources or asked of the engine runner, never recalled); no fallbacks or defaults; the guard files are load-bearing
  (a pin, ceiling, ledger or allowlist entry moves only with the user's word and a dated note).
- The user follows this session and gives rulings to it directly. A landing of more than one change goes through a PR;
  nothing merges to `main` without the user's word in that session; no force push.
- Sub-sessions at xhigh effort (the user's standing ask). Brief them fully; they report to this session.
- Never read, print or copy a credential.
