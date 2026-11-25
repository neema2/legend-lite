# The edition program: the plan to finish Stage B (2026-11-25, late night)

*Written by the orchestrator session that took over from B3 (`session_0195rQEk16FvwAXgknSU73Fi`, another account). For a session with no context: read `edition-program-START-HERE-2026-11-25.md` §0 first, then this file. This file is the order of work; the design it builds is `edition-design-v3-2026-11-25.md` with every ruling in `edition-homework/v2-amendments-ruled-2026-11-25.md`, except the mechanism, which is §2 below.*

**Status.** The steps and their order were presented to the user on 2026-11-25 and are being worked step by step. The user's rulings on them are quoted in §6. **The mechanism (§2) is proposed, not ruled:** the user asked "Is the table thing going to work?"; step 4 opens with a probe that answers it, and the rest of step 4 waits for that result.

---

## 1. What "Stage B done" means

- **The core** holds no upstream-specific code and no switch, and guards keep it at zero.
- **Compatibility** lives at three edges: the door (input), the presenters (output), the test harness.
- **The gates:**
  - PCT's Pure run fails only on ledgered database limits;
  - in PCT's engine run on DuckDB, nothing legend-engine passes fails in lite;
  - the corpus rosters lose nothing;
  - the product's SQL never grows for compatibility.
- **Every difference** is a register row the user ruled.

## 2. The mechanism: one table at lowering

**An edition is a table the door hands in, consulted where the lowering already chooses.** It maps a native function to another product-named rule (`substring` to `substringAsGiven`), a raise kind to checked or plain, and a small closed set of named choices (the cross-type marker's answer, a few whole-statement passes). The product's own table is empty. The edge builds `pure`'s and `engine`'s.

**Why this, after three designs.** Each earlier design looked for one place that sees both all the code and legend-engine's placement. Function bodies and mapping code are visible only late, after inlining and store resolution; lets and `execute()` boundaries only early. No one place has both. The table decides meaning late, where the code is all there, and leaves placement to the few cases that need it.

**Checked in the code (2026-11-25):**
- The lowering already takes a door-supplied object on every path: `lowering/Styled.forms()`, built per frame by `planner/PlanEnv`. It is consulted at about 20 sites.
- What the edge supplies today is a short, closed list: about 15 lowering forms (`edge/EdgeLoweringForms`), the feature-flag rules (`lowering/FeatureRules`), two text forms, and in the edge's dialects three SQL passes (`edge/PurePrintForms.passes`) and `floatToText`.
- Everything on that list is decided at lowering or later. Only the door's rewrites of the typed query (`EngineComparisons`, `PureComparisons`, A3) are decided earlier, and those move later, which adds reach.
- `substringAsGiven`, `sqlPosition` and `trustOne` are product natives with their own rules in `Scalars.RULES`. `round`, `dateDiff`, `splitPart`, `toDecimal`, `rem`, `divide` and `toString` are entries of the same table.
- The cross-type marker is refused at two lowering sites (`Lowerer.java:3667`, `CollectionLanes.java:137`).
- A planned relation frame of the corpus is lowered on its own; a spliced one sits inside a `TypedFrom` that reaches the lowering.

**The three ways it could fail,** each under a read-only trace whose report goes to `edition-homework/r3/`:
1. A pass between typing and lowering that matches on `==` may not see through the marker (step 5).
2. Some rows need the whole statement, not one call (`TDSNull` in a joined cell). Those stay statement passes the table chooses.
3. The corpus boundary for class-rooted frames is not traced end to end (step 7).

**Not through the selection today:** three of the four `toOne` guards (`Coercions.java:66,105,178`; `ToOneGuard.java:70`), and the folds that answer a crossing before lowering.

## 3. The steps

Numbers are labels and stay fixed (the user's rulings cite them). "Closes" names the Stage B item or edition phase.

| # | Step | Closes | Done when |
|---|---|---|---|
| | **Land what's ruled** | | |
| 1 | The edge's DECIMAL Float literal deleted (the saved patch, 8 re-pins) | B4 | full round green |
| 2 | `toCSV` exact and S7a (B2's push merged) | B2b | full round green |
| 3 | Dead code; the keyed count (S-13); `acos`/`asin` raise; H2's lateral; SQLite's `chunk`; channel A on compiled legend-pure | v3 §6.1 | full round green |
| | **Meaning (the critical path, serial)** | | |
| 4 | The table at lowering, carrying today's behaviour per door. Opens with a two-row probe (the plain parses out of the dialect pass, `isNumeric` out of the forms) | R2 | SQL byte-identical on every lane |
| 5 | Strict typing in function bodies and Pure mapping code; the marker answered at lowering; folds never fold a crossing; the door comparison rewrites deleted | B3 5a-2 | the compile check and the run agree; rosters unchanged |
| 6 | `engine`'s rows, one at a time: plain raises and parses; the delegations (`round`, `dateDiff` weeks, the casts' text, `splitPart`, `toDecimal`, `rem`, enumeration by name); `isNumeric`, `parseDecimal`, `parseJSON`; the null-unsafe flag; `substring`/`indexOf` for the corpus's `execute()` and PCT's engine run | B4, B5's flags | each row measured, then ruled |
| 7 | The corpus split: harness `pure`, `execute()` `engine` | R3 | rosters lose nothing |
| 8 | PCT's Pure run stops inheriting legend-engine's meanings | R4 | its failures equal the ledger |
| | **Equality (after 5)** | | |
| 9 | Types known only at run time: the tag beside the value | B3 5b | exactly one of `<`, `==`, `>` on every database |
| 10 | Asserts as their Pure bodies; `VerdictSql`'s comparison layer deleted | B3 5c | both judges agree; about 2,500 lines out |
| | **Presentation (a parallel lane)** | | |
| 11 | The scalar lift (B2's S6a: built, held for the user's ruling) | B2b S6a | PCT's six text cases don't move |
| 12 | The 16 presentation rows to the presenters; the SQL-built text deleted | B2b, R4 | printed-table tests and result goldens byte-identical |
| | **Harness (a parallel lane)** | | |
| 13 | The SQL-text fallback as test tooling; the test runner's public halves; referee and appeals out of the core (`edition-homework/b3-step6-design.md`) | B3 step 6 | corpus result lists byte-identical; about 3,400 lines out |
| 14 | Harness classes out of the product (48 edge, about 15 core, per the census) | R7 | the census's test-harness count at zero |
| | **Close** | | |
| 15 | `EngineWhereZones` measured and deleted; DataCube's legend-engine workarounds (no plan yet) | B4 | moved tests listed first |
| 15b | The folds cut back to the rule: a probe first, the list ruled by the user, then the allowlist guard | v3 §3.5 | the nine disagreements with Pure gone; guard green |
| 16 | `PlanStyle`, feature flags in the lowering, `LoweringForms` and the edge's dialect subclasses deleted; A3's fate decided | B5 | census at zero |
| 17 | The guards on; `engine` refused by name on databases other than DuckDB and H2 | B5, B6 | guards green |
| 18 | Full rounds on every gate, an independent audit, the pull request | R8 | the user's word |

**Order.** Steps 1 to 3 block nothing. Steps 4 to 8 are serial; 9 and 10 follow 5. The presentation and harness lanes can run beside them. The close needs all of them.

**Not in Stage B:** per-project editions; PMCD and two-way SQL plans; exact placement for M2M under graph fetch; checking legend-engine's answers on databases beyond DuckDB and H2; a lite-native service test runner.

**Open, with no owner:** `//pct:pct_channel_b_twice_h2_relation` (a manual lane) fails 4 cases on a clean tree, "DialectCapability: LIST_GET reached a dialect without a list encoding" (B2's M2 record; B3's handover).

## 4. How each step is run

The fast dev loop of `AGENTS.md` ("The fast dev loop"), with the user's word of 2026-11-25 (§6):
1. A fast check after editing: the guards, the census, the touched packages.
2. One full round per step, with `//gates:local` (`--lockfile_mode=error`); the round is `edition-homework/handover/probes/round-decimal.sh` and `round-template.sh.txt`.
3. One independent audit per step, by a subagent on Opus 5.5, given the diff and the rules, not the author's conclusion.
4. **Step by step at the beginning:** after each step the orchestrator stops and brings the user the round's result, the audit's findings and every moved test, and waits for the go.
5. CI at milestones through a pull request, opened when the user asks.

**Branches.** Work in progress is committed to the orchestrator session's own branch, `claude/modest-galileo-h6swr4`, which sits on top of `claude/compat-b3-nufdlk`. The program's branch moves only when a step is verified, audited and the user has said go. Nothing goes to `main` without the user's word in that session.

**Reading a round.** On a clean tree only the 14 root-only targets are red (initdb refuses root); they pass through `tools/postgres/as_unprivileged.sh`. A warm full round is 1.5 to 2 hours; a fresh container starts cold (`docs/CLOUD_SESSIONS.md`).

## 5. State on 2026-11-25

- **On `main`:** B1, the one error model, item 3 (PR #132, merged 2026-11-24).
- **On `claude/compat-b3-nufdlk`** (91 commits ahead of `main` at `339e5574`, then B3's handover `296732e2`): B3 steps 1 to 5a, B2a, B2b through S5c, every record.
- **Step 1** is in progress on `claude/modest-galileo-h6swr4`: the patch applied, the guard's row removed, the regenerations running.
- **Awaited from B2** (`claude/compat-b2-nufdlk`, another account): `toCSV` exact and S7a, `engine-java-part.md`, the S6a stash as a patch. B2 reports its pushed head to the user.
- **`claude/compat-program` is stale** (last commit 2026-11-21, 267 behind `main`): PRs #130 to #132 went straight to `main`.

## 6. The user's rulings on this plan (2026-11-25, late night)

1. **A3 stays until step 16:** "Substring you can leave until 16 yes". The `engine` table takes `substring`/`indexOf` for the corpus's `execute()` and PCT's engine run; pure/v1 keeps A3 as it is. The user's earlier fallback (delete A3 and record the host-side `let` tests) stays available at step 16.
2. **The folds are in the plan, late:** "Folds we need to fix but can be a later step in the plan at the end". Step 15b. Before it, step 5 makes only the change the editions need: a fold never answers a crossing.
3. **The working loop:** "Landing - read agents.md for fast dev loop with interim milestones that have independent audits - you can do subagents for audits and tesimf using opus 5.5 but I want to go step by step at the beginning at least".

**Not yet ruled:** the mechanism of §2; the decisions of §7.

## 7. Decisions still to come

Each arrives with its measurement, one at a time:
- the scalar lift (step 11);
- PCT's engine run: everything through SQL, or legend-engine's pre-evaluation of multi-expression functions (third review, decision 2);
- comparisons whose types are known only at run time, under `engine` (S-11);
- feature flags scoped per `execute()`;
- SQLite's Decimal arithmetic under `pure` (T-10);
- DataCube's legend-engine workarounds (step 15);
- the fold list (step 15b);
- the amendments to the rules of record: its edition paragraph still says the typer reads the edition (third review, must-fix 7);
- one pull request straight to `main`, or through `claude/compat-program` first.
