# The compatibility program's log (from 2026-11-19)

The running record of the program: every ruling, every finding, every number, every session and branch, written as it
happens (the user, 2026-11-19 ~10:45 UTC: "write down all the learning/outcomes/details immediately").
- **The plan:** `docs/records/compat-program-plan-2026-11-19.md`, on this branch (`claude/compat-program`).
- **The owner:** this session (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **Order:** newest entries go at the end of each section.

## 1. Rulings (the user's words, verbatim)

| When (UTC) | Words | What it rules |
|---|---|---|
| 2026-11-19 ~04:02 | "1-3 yes" | R4: lite's `substring`/`indexOf` follow Pure (CP-2); E-R1 (CP-3, later switched); upstream's SQL text never promised (CP-4) |
| ~04:02 | "Explain 4 clearly, what's the alternative? Is this a sneaky way to have two sql paths" | led to CP-5's revision: no special functions, pending the count (0.4 (j)) |
| ~04:02 | "Let's not worry or even support older database versions" | CP-8: legend-engine's H2 2.1.214 LEGACY and DuckDB 1.3.0 are neither emulated nor chased; one blanket register row |
| ~04:30 | "Switch e-r1" | CP-3 is E-R2: the legend-engine route answers everything lite can |
| ~04:30 | "We definitively should not delete the fallback that checks answers when SQL doesn't match right? That's one of the crown jewels of this project" | the SQL-text appeal and the referee are KEPT; under CP-4 they decide every SQL-text assert by answer |
| ~10:45 | "yes to 1-5 and the independent review, do as much homework, research, measurements right now up front as possible and write down all the learning/outcomes/details immediately" | 1, CP-1 (translator in, formatters out, no modes); 2, sessions may push their own `claude/compat-*` branches; 3, two new sessions now (judges, translator), hosts in week 2, xhigh; 4, CP-9 (per-push checks, the oracle's `jars_table` entry); 5, CP-10 (housekeeping); and an independent review |

**Still open:**
- at the phase-0 checkpoint: CP-6 (typed facts), CP-7 (error words out of the plan, one guard row), the 6
  plan-structure tests (not promised), PCT under the LITE judges (§12 I5);
- later, each with its numbers: CP-5 (special functions: none?), CP-11 (SQLite Decimal as REAL).

**Held by the previous orchestrator:** R-a (a fractional cell under an Integer property, PR #121).

## 2. Findings, with their evidence

| # | When | Finding | Evidence | Where it went |
|---|---|---|---|---|
| F1 | 04:20 | **CP-4's cost:** 143 distinct relational-corpus tests (DuckDB 143, H2 139) pass today only through legend-engine's SQL text and become "not promised". By cause: a check on a string with nothing to re-run (22 compared strings, 53 plan printouts lite cannot produce); legend-engine's expected SQL not runnable on the referee (23 written for DB2 or Composite, 22 missing setup, syntax errors or now-relative dates); lite's rows underivable on the referee (19); SQL read out of legend-engine's plan (4) | B2: `docs/records/census-b2-sqltext-probe-2026-11-19.md` on `claude/census-b2` at `32ef19118` | plan CP-4 |
| F2 | 04:20 | **6 tests assert legend-engine's plan structure** (`testSupportStreamFlag*`, a `FunctionParametersValidationNode`'s `supportsStream`), not SQL text | as F1 | plan CP-4; for the user at the checkpoint |
| F3 | 04:20 | **legend-engine's `joinStrings` SQL appends the separator** (`concat(a, ' ', b, '\|')`), against Pure's `joinStrings`; lite answers Pure (`testToSQLStringForTDSStringJoin`, `testHashFunctions`) | as F1 | an upstream-defect register row (0.5) |
| F4 | 04:20 | **A lite parity bug:** `firstDayOfWeek` answers Monday on DuckDB (Pure's rule) and Sunday on H2 (`testToSqlGenerationFirstDayOfWeek`; lite's `date_trunc('week')` lowering) | as F1 | handed to the product owner (`session_0195rQEoRb1AGCHxtZChjWsg`), 04:2x |
| F5 | 04:08 | **B1's review of r1:** the EngineShape family (`relations`, `keyedCounts`, `computedPruning`, `view`, `wireCast`, `unionFrame`) differs only in legend-engine's TEXT; `traceCell` and the graph keys are ANSWERS; the compatibility database judge writes through the text facets 1.3 deletes, so 2.2 must come first; executed generated DDL is not text; `RelationalRootForm.primaryKeyColumns` is the product's key rule | B1's message, against `00cd4295d` | plan r3 |
| F6 | 04:13 | **What "feeds" means:** the text's content is read (compare, concatenate, sort, group, join keys, filter, length), not merely carried (select, rename, limit, slice, a `let` to the root). Prints nested inside a returned structure are presentable by position | B1's review of r2 | plan r4, 1.3 |
| F7 | 04:13 | **"No special functions" has a measurable cost:** pure/v1's `raise.sqrtOfNegative` matches legend-engine today only through `LoweringForms.raise`'s plain set, and ENGINE answers on H2 and DuckDB rely on S20's bare-DECIMAL literal | B1's review of r2 | plan 0.4 (j) |
| F8 | 04:35 | **The appeal can move into the edge judge as one slice** (3 to 4 days), if the judge is one object with two narrow interfaces (planning, run) and the product keeps two call sites (`TestDoor`, `TestRunner`): the appeal needs planning-time artifacts the plan does not carry (the referee's seed copies from the CSV load's MIR) and the live session after the body runs | B1's message | plan 2.2, done-when 1 |
| F9 | 04:30 | **The goals of record already rule the appeal and the referee in, at the edge** (GOALS_OF_RECORD G6 and §2 E3, 2026-11-07; `docs/SQLTEXT_ROW_VERDICT_CHARTER.md`, 2026-09-01, and its 2026-09-21 ruling); r3 deleting them was this session's error | the documents | plan r5 |
| F10 | 04:30 | DataCube's calculated-column help writes `substring(0, 2)` in Pure's meaning (`datacube/src/calc.ts:88`), so R4 makes the product's own example right; saved queries and cubes using `substring`/`indexOf` will change answers | `grep` at `main` | plan §12 I1 |

## 3. Sessions and branches

| Session | Role | Branch | State |
|---|---|---|---|
| this session | owner, orchestrator | `claude/compat-program` | |
| B1 `session_0195rQEnAa5CYXHygAxEFh3Y` | presentation lead, phase 1 | `claude/presentation-b1`; 0.2 on a new `claude/compat-*` branch | |
| B1-R `session_0195rQEkcevkNQ66fixkk4FF` | legend-engine oracle, phase 5 | `claude/presentation-r` | |
| B2 `session_0195rQEjuF2imym274M9Qvva` | census, guards, measurements | `claude/census-b2` | |
| previous orchestrator `session_0195rQEoRb1AGCHxtZChjWsg` | product correctness, landings | PR #121, others | coordinates by message |
| translator `session_0195rQEn5jsUCYS6QSWLrxgF` (new, xhigh) | the translator: H5 spike, 3.1 sizing, the 0.6 ledger draft | `claude/compat-translator-spike` | started 10:48 |
| judges `session_0195rQEm3vCjTiBifDsQTVB3` (new, xhigh) | the judges split's homework; PCT under LITE's judges (§12 I5) | `claude/compat-judges-homework` | started 10:49 |
| independent reviewer `session_0195rQEnCA5MRQEk6iGfgDnb` (new, xhigh) | an adversarial review of the design and the plan | `claude/compat-review` | started 10:49 |

## 4. Work in flight (started 2026-11-19 ~10:50 UTC)

| Workstream | Session | What | Record | Report |
|---|---|---|---|---|
| W1 | B1 | 0.2: the net lands as a PR (`claude/compat-s0`: S0 and the requests cherry-picked, the net per push); then phase-1 homework: 0.4 (f) prints by position and use, (g) G7 over the net, (i) Float round-trip through each dialect's JSON, the Operand shape's coverage | `docs/records/compat-homework/H9-phase1-measures.md` on `claude/compat-b1-homework` | the PR number; counts |
| W2 | B1-R | 0.3: the ENGINE PCT lane against the manifests (a classifying table under E-R2, sampled against the oracle); H3's batteries made permanent (pinned per push, re-asked in heavy); the per-push cost; the `jars_table` entry (CP-9, ruled) | `docs/records/compat-homework/H7-engine-lane.md` on `claude/compat-engine-lane` | counts per class and database |
| W3 | B2 | 0.4 (a) the strand map confirmed and the ratchet's start; (c) legend-engine's unchecked operations; (d) the strands' doors; (e) executed generated DDL and the lanes' time under the appeal; (h) the corpus's H2 lane without LEGACY, and the referee's settings; (j) CP-5's cost by case | `docs/records/compat-homework/H8-phase0-measures.md` | counts after each item |
| W4 | translator | H5: a model-level, target-aware translator spike through the corpus's 15 tests; 3.1's split sized; the 0.6 ledger's first draft | `docs/records/compat-homework/H5-translator-spike.md` | counts first |
| W5 | judges | the 36 combinations; the exact map of compatibility judging in the core; the two narrow interfaces sketched; PCT under LITE's judges with BOTH (§12 I5) | `docs/records/compat-homework/H6-judges.md` | counts first |
| W6 | reviewer | the independent review: its own view first, then the plan attacked; 12+ claims checked | `docs/records/compat-independent-review-2026-11-19.md` | verdict and top five |

**The phase-0 checkpoint** comes when W1-W6 have reported: the exact lists, the counts and the review go to the user,
who confirms or changes phases 1-3.

## 5. Learnings (as they arrive)
