# Audit — Foundations Phases 0/1/2 (+ spillover), 2026-08-16

> Independent audit of `739d5af9..79ca9929` (**27 commits**) against
> `docs/FOUNDATIONS_PLAN.md`'s per-task acceptance criteria. Six auditors; one was permitted to
> build and **broke each guard deliberately** to test whether it goes red.
>
> **Governing question:** is each guard REAL, or does it pass vacuously? A guard that cannot
> fail is worse than no guard — it converts an unchecked area into a checked-*looking* one.

---

## 1. Verdict

**The work is substantially real, unusually honest, and behaviour-neutral as promised. Two
items are not: one census is vacuous, and one guard's charter mapping is false.**

The strongest single result: **13 of 13 ratchet seeds recomputed exactly.** No inflation, no
headroom padding, in a codebase whose documented failure mode is flattering self-claims.

**Regression: zero.** All **67 corpus families** machine-compared — `2575 / 2347 / 73 / 69 / 86`
identical at both ends. The scoreboard was rewritten only to add three columns, and it is
**machine-regenerated on every sweep**, so identical numbers after real sweeps are positive
evidence of behaviour-neutrality, not a stale artifact. **No ratchet was loosened anywhere.**
PCT 1109→1110 because F1.11 *added* a test; the exclusion ledger has zero diff.

| Phase | Verdict |
|---|---|
| F0.1 baseline | REAL (16/16 ratchet anchors correct); delta pass covers **16 of 27** commits |
| F0.2 charter | **REAL — the best artifact in the range** (§3) |
| F0.3 HostEval census | WEAK — drops the dual-use framing that was the point |
| F1.1 / F1.2 harness move | **REAL** — 17 × `R100`, zero `src/main` change, no test-jar, 4 exemptions deleted |
| F1.3 `java.sql` funnel | REAL — verified red on deliberate violation, incl. **transitive** reach |
| F1.4 harness discipline | REAL, narrower than named |
| F1.5 host-channel pin | REAL on its own acceptance; **its charter mapping is false** (§4) |
| F1.6 R0 ledger | REAL as a ledger; the R0 *rule* is not made real |
| F1.7a nested scanning | REAL and free (0 offenders) — introduces one masking defect |
| F1.7b default-literal | WEAK — one regex; misses `default -> null` at **56 sites** |
| F1.8 dead-method | **WEAK — cannot see the four methods it was written to catch** |
| F1.9 gate wiring | REAL; `CorpusDifferentialTest` relabeled, not fixed |
| F1.10 tenet ratchet | WEAK — acceptance clause unmet; count off by one |
| F1.11 / PX.1 | REAL, genuinely adversarial self-audit |
| F2.1 soft-pass columns | REAL — **off by 2** (§6) |
| F2.2 SQL-text rescue | **REAL — the finding that justifies the phase** (§5) |
| F2.3 decline census | REAL on the gate; not in the scoreboard |
| F2.4 leniency census | Wiring REAL; **census dark** (stderr-only, off by default) |
| F2.5 nine claims | WEAK — 3 of 9 false at HEAD, invalidated by later commits in the same chain |
| F2.6 GAP census | **VACUOUS (§7)** |
| F3.1a `server/Json` | WEAK — reader fixed, response path still doubles |
| F5.1 PCT `pureType()` | **REAL** — `sqlType()` consumers 2 → 0 |
| F5.3 Stage A | REAL probe, executed **better than the plan specified** |

---

## 2. The finding that matters most: the guards are real, the tenet is not yet mechanical

F1.3 was verified genuinely enforcing. A deliberate `import java.sql.ResultSet;` in
`com/legend/lowering/Scalars.java` goes **red**, and so does a *transitive* reach with **no
import at all** (ArchUnit sees the bytecode call to `ResultSet.getString`). That is the
realistic shape of the leak and the funnel catches it. Four exemptions, matching the plan's ≤5.

**But the audit's own worked calibration example was reconstructed, and every guard stayed
green.** A Java hash computed over a live JDBC cell, in a package the funnel forbids. Two moves:

1. **Wrap the `ResultSet` behind a `com.legend`-typed interface in a permitted package.**
   ArchUnit matches bytecode types, so a `com.legend` wrapper is invisible to F1.3 *by
   construction*.
2. **Read it with `getNString` instead of `getString`** — F1.10's accessor pattern omits
   `getNString`, `getURL`, `getClob`, `getRowId`, `getCharacterStream`.

**The funnel makes the *import* mechanical; it does not yet make the *tenet* mechanical.**
That is not a failure of F1.3 — it is the correct scope of an import rule. It means the tenet
ratchet (F1.10) is the load-bearing guard, and it currently has holes.

Compounding: **`com.legend.exec` is a permitted package with no class-list pin** — unlike
`com.legend` root, which F1.3b pins to three classes. A brand-new
`com.legend.exec.ProbePermitted` hashing a live cell is green. That is the widest open door in
the funnel, **and it is where the interpreter lives.**

### Ranked bypasses found (all verified green)

| Guard | Bypass |
|---|---|
| F1.3 | `com.legend`-typed wrapper in a permitted package; bare `Class.forName("java.sql.DriverManager")` (no `java.lang.reflect` type, so the F1.11 ban does not see it); **`org.sqlite..` is a declared `core` dependency and is not banned** |
| F1.3b | Nest the class inside `StatementExecutor` (`(\$.*)?` rides along — documented); or put it in `com.legend.exec` |
| F1.4 | A manual selection sort in the same file; `PriorityQueue`; **anything outside the two scanned roots** — `integration/` holds **20 sort spellings in `ExecutionResult`-handling files**, unscanned |
| F1.5 | `HostEval.READ_CHAIN_FNS` and `PlatformTypes.isStoreNavFn` are **unpinned** — adding entries widens the 894-line interpreter's reach with the whole suite green |
| F1.6 | `import static …RawSqlBoundary.*;` + bare `h2ToDuckDb(sql)` |
| F1.7b | `default -> null` (56 sites), `default -> 0`, `default -> CONST`, line-wrapped forms |
| F1.8 | Any dead **recursive** private method (`uses` counts self-calls); anything outside `core/src/main` |
| F1.10 | `getNString`/`getURL`/`getClob`/`getRowId`/`getCharacterStream`; any consumption in a file that never spells `java.sql` |
| reflection ban | Three classes exempted **wholesale by name regex with no count** — `com.legend.server.Json` is a reflection free-for-all, described in its javadoc as "frozen shrink-only" |

---

## 3. The charter is the best artifact in the range

`docs/TENET_CHARTER.md` (116 lines, 5 clauses + enforcement map) **exceeds the plan**:

- **Clause 3 gets the keystone right.** Titled *"Provenance, not arms"*, it states the rule
  exactly and names arm-enumeration as the approach that **would have failed**, citing the
  ~6-line dispatch edge, the 47 arms, and the 18 dual-use. There is no arm list in the document.
- **Clause 4 generalises further than asked** — `LiteralFold`'s admission rule becomes
  governing, with **no site-local admission rules** and *"admitting a kind is a green
  differential, not an argument."* That closes a hole the plan left open.
- **Clause 5 (ingress mirror) is new** and correct — the `Ddl.spell`/`CsvSeed` findings promoted
  from tasks to doctrine.
- **C1.6's operational test** — *"could this run with no database attached and no data
  loaded?"* — is the most useful single sentence in the document.
- **The closing line is the right one:** *"'the guard is not built yet' is a schedule fact, not
  a license."*

Every clause is cited by the guard enforcing it. Two warts: the "18 dual-use" figure is
enumerated nowhere, so no reader can re-derive it; and see §4.

---

## 4. Charter Clause 3's enforcement mapping is false

The charter says *"No `ResultSet`-derived value may reach `HostEval.eval()`"* and maps
enforcement to F1.5. **The mapping is falsified by the code F1.5 is green on.**

`HostEval.eval` (`HostEval.java:377-382`) calls `DbMetaData.query`, which opens an H2 connection
and builds a `HostResultSet` via `rs.getObject(i)` (`DbMetaData.java:144`); the interpreter's
collection arms then compute over it (`:705-745`). So `eval()` does not merely *receive*
ResultSet-derived values — **it manufactures them. Audit A9 is still in the tree.**

F1.5 is a real, well-built pin — it goes red when `chainBottom` is rewired to the A9 containment
mechanism (the 2096→408 shape), verified. But it pins the **admission shape** of
`wantsHostEval`, not the **provenance invariant** the charter claims. This is the one place the
new documentation makes a self-flattering claim.

**Fix:** either narrow the charter's enforcement column to what F1.5 actually pins, or make the
provenance rule real (A9 re-siting — plan task F6.6). Do not leave the map asserting the
stronger claim.

---

## 5. F2.2 is the finding that justifies the phase

The plan predicted the true SQL-divergence rate was `244 + <uncounted rescues>`. The counter
came back: **636 rescued asserts against 325 text-matched — a 66% divergence rate** in the
golden-SQL channel, expressed in no artifact before this phase.

The branch is exactly the one the audit identified, and the unit is **asserts** (confirmed —
`sqlTextVerify` is invoked once per assert). One caveat: the commit compares 636 (asserts)
against 247 (tests). The assert-level counterpart, `advisorySqlDiffs`, is **stdout-only**, so
the honest statement is ≈ **883–935 divergent asserts against 325 matched** — a bigger finding
than the commit states, and the scoreboard structurally cannot express it.

---

## 6. The numbers, verified

All recomputed from the committed scoreboard; the baseline doc agrees exactly (no drift).

| quantity | claimed | recomputed |
|---|---:|---:|
| tests / pass / fail / error / shape | 2575 / 2347 / 73 / 69 / 86 | ✓ all, and per-family for 67 families |
| sqldiff-pass | 247 | ✓ |
| adv-pass | 293 | ✓ |
| 0-asserts | 27 | ✓ (inside the audit's ≤33 bound) |
| rescued | 617 | ✓ |
| soft (union) | 929 | **931** |
| clean | 1418 | **1416** |

**The off-by-2:** `Runner.java:1301-1306` emits `"vacuous placeholder (engine body = true)"`
PASSes that **bypass `score()` entirely**, so they land in `clean`. The commit's justification
reasons correctly about `score()` — but `score()` is not the only PASS producer.

**Two framing problems, both worth fixing before burn-down consumes these numbers:**

1. **The reconciliation *sentence* destroys the gradient the *columns* preserve.** A
   `0-asserts` pass is verified **zero** times; a `rescued` pass is verified **twice** (our
   DuckDB rows *and* the engine's golden SQL replayed on H2). Calling 617 doubly-verified tests
   "soft" alongside 27 unverified ones **overstates the remediation surface ~5×.**
2. **Three of the four columns are ungated.** No ceiling on `rescued`, `advisory`, `0-asserts`,
   or `soft`. A renderer change producing 200 more rescued divergences moves `soft` 929→1129
   with pass counts identical, `sqlDiffs` unchanged, and the build green. **A printed, ungated
   number will drift.** Cheapest fix matching the repo's own idiom: a `maxSoft` ceiling beside
   the existing `maxAdvisorySqlDiffs`.

---

## 7. F2.6 is vacuous, and net-negative

All 17 `@Disabled("GAP: …")` sites are **empty method stubs** — zero statements, zero
assertions. So the commit's evidence, *"un-disabled and both PASS (283 run / 0 failures)"*, is
unfalsifiable: an empty `@Test` passes unconditionally, and would have passed identically if
XStore had never been implemented.

**The two "retirements" converted two honest `@Disabled` markers into two permanently-green
tests named `"GAP: XStore cross-store mapping"` and `"GAP: AggregationAware mapping"`** — a
scoreboard now reporting those features covered, forever, on evidence of nothing. Precisely the
failure mode the brief names. (`testRelationClassMapping` twenty lines away is identically
empty and was correctly left `@Disabled` — the treatment is inconsistent within one screen.)

**And the census has a scheduled self-destruct.** `docs/OUTSTANDING.md` is machine-generated —
`scripts/outstanding.py:130` opens it `"w"` — so the next sweep silently deletes all 15 rows.
The commit acknowledges this and shipped anyway.

**Fix:** re-`@Disable` both; move the census to a hand-owned file or teach the generator;
adjudicate a GAP by *writing the test*, never by un-disabling an empty stub.

---

## 8. The live wrong-answer found in the delta pass

`envelopeSizeCheck` (`EngineTestExecutor.java:163-197`), added **during the burn-down**
(`497de6bd`), answers a size assertion with `carriers = tds ? 1L : av.size()` — so
**`assertSize($result.values, 1)` passes for any tabular result of any row count.**

In the same commit, **six existing unit assertions across five methods** were rewritten to a
different spelling so they would keep discriminating. The commit message calls it *"three lite
unit pins"* — it is six, and the error runs downward.

**This is the strongest evidence for the pause decision**, and it is stronger than the baseline's
own summary of it: the burn-down was manufacturing harness compensation and adjusting the tests
around it while the audit recommending the pause was being written.

---

## 9. Corrections to `docs/TENET_AUDIT_2026_08_16.md`

Round 2 was wrong four times. All four were caught by the implementing session or this audit,
and all four are recorded here so the audit is not cited unamended.

| Audit claim | Correct value |
|---|---|
| §7 T18 — "**11** parser-equivalence classes with real assertions run in no gate" | **9** orphans, of which **2** are assert-bearing. The other 7 are zero-assertion printers that say so in their own javadoc. |
| §7 T21 — "**20** `@Disabled("GAP:")` sites" | **17**, all in one file. |
| §1 — "`StaticFold` is **frozen**, byte-identical across 691 commits" (cited as one of three structural facts carrying "the query compiler is clean") | **No longer true.** `9162d8d4` and `1d962f78` added `isEmpty`/`isNotEmpty`, `toOneMany`, 2-arg `toOne`, `toString`-over-static-scalars **during the burn**. §6 still lists the freeze as a thing to protect. |
| §4.1 — predicted three PCT concealment classes: wrong type, wrong multiplicity, **wrong name** | **Wrong-name is empty** — verified column-by-column; the apparent cases are shell-quoting artifacts of the same identifier. A **fourth class dominates** that the audit did not predict: a cosmetic `[1]`-spelling skew. |

---

## 10. The PCT inventory is ~half what its headline says

Re-derived from the 138 patterns / 383 occurrences:

| residual class | occurrences | real defect? |
|---|---:|---|
| Cosmetic `[1]`-spelling skew | **214 (56%)** | **No** — `buildTypedHeader` omits `[1]` on all but the last column |
| `Date → String` | 65 | **Yes — the dominant real class** |
| Multiplicity `[1]` vs `[0..1]` | 58 | Yes — dies with F5.2 |
| `Variant → String` | 26 | Yes |
| `Number → Float` | 20 | **No** — the wire is *more* specific |

**Real Stage B work: 65 patterns / 169 occurrences**, over half of it dates-as-String
(already ticketed as F5.4). Two reconciliation gaps the doc never closes: 405 → 383 is
explainable (the probe logs only mismatches) but unstated; **322 reconciles to nothing** at any
unit — either the audit's figure used a third unit or one of the two is wrong.

**F5.1 did more good than its own commit claims** — the Float→Decimal class was **17 patterns /
33 results**, not the "5×" recorded, ~6.6× larger.

---

## 11. Ranked follow-ups

**Correctness (do first)**
1. **Re-`@Disable` the two empty GAP tests** (§7) — the only change in the range that made a
   guard weaker while looking stronger.
2. **Fix or re-scope Charter Clause 3's enforcement mapping** (§4).
3. **`envelopeSizeCheck`** (§8) — schedule the removal; it is a live constant-answer.
4. **Fix the F2.1 off-by-2** and re-file the 2 vacuous placeholders as soft.

**Guard completeness**
5. **Ban `org.sqlite..`** — a live funnel bypass today.
6. **Extend F1.10's accessor pattern** (`getNString`/`getURL`/`getClob`/`getRowId`/
   `getCharacterStream`) and remove the one non-JDBC site (`getBytes` on a `String`).
7. **Class-list-pin `com.legend.exec`**, as F1.3b already does for `com.legend` root.
8. **Point `mainSources()` at every module and both source roots** — one change fixes F1.7b's
   and F1.8's population problem together.
9. **Fix F1.8's `uses` rule** to exclude self-recursion.
10. **Pin `READ_CHAIN_FNS` and `isStoreNavFn`** alongside `HOST_CONSTRUCTION_CLASSES`.
11. **Count the reflection exemptions** rather than pardoning three classes wholesale.

**Measurement**
12. **Add a `maxSoft` ceiling** — the single highest-value item for whether this phase survives
    the next burn.
13. **Restate the soft decomposition by rung**, not as a binary (§6).
14. **Surface `advisorySqlDiffs`** in the scoreboard (§5).

**Bookkeeping**
15. Refresh the three stale headers F5.1/F3.1a invalidated; record F1.10's seed in the baseline;
    reconcile `docs/GATES.md` with `allgates.sh`; delete `Column.sqlType` (zero consumers).
16. Re-file **A7** as its own task with `JsonAssertCanon.java:76-87` in a `Files:` line — it is
    currently absorbed into F6.5, which can be completed and closed without touching it.
17. Classify the **11 unclassified commits** in the F0.1 delta pass; one (`08c3df60`) contains
    an unlisted S1-shaped compensation.

---

## 12. What must not be regressed

- **The seed honesty.** 13/13 exact. Any future re-pin should be held to the same standard, and
  the four **exact-match** pins (`HarnessDisciplineTest`, `RawSqlLedgerTest`,
  `HostChannelPredicateTest`, `PctDisciplineTest`) fail in *both* directions — strictly stronger
  than the house `assertTrue(n <= K)` idiom and the right pattern to copy.
- **F1.2's ratchet re-pins banked zero slack** (32/32, 87/87, 18/18) when the harness's departure
  freed real headroom. The easy invisible move was to keep the old numbers.
- **The disclosed deviations.** Both F1.2's `ParserBoundaryArchTest` carve-out and F1.9's "your
  11 did not reproduce" were written down with checkable reasons, and both reasons check out.
  A session that quietly wrote "11 classes gated" would have been far harder to catch.
- **F5.3 Stage A chose the safe reading** of an ambiguous instruction (kept the overlay, logged
  alongside) and said so.
- **PCT remains free of Java-side comparison** — independently re-confirmed at 0, now pinned by
  `PctDisciplineTest`.
