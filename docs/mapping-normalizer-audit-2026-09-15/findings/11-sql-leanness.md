# 11 — SQL leanness

**The most heavily evidenced dimension in this audit.** All SQL quoted below is **real emitted
output**, not doc claims.

## Method / evidence base

1. **Live corpus capture.**
   `LEGEND_LITE_DUMP_SQL=1 mvn -Denforcer.skip=true -pl core test -Dtest='RelationalMappingIntegrationTest,RelationalMappingCompositionTest,AssociationIntegrationTest,UnionJoinMappedPropertyTest,UnionTargetLeanJoinTest'`
   → 487 tests green, **400 emitted statements, 344 distinct SELECTs**.
2. **Targeted probes.** ~45 hand-built mapping fixtures driven through
   `com.legend.Compiler.plan(model, query, runtime).sql()`.
3. **Engine comparison** against the real goldens in `/Users/neemsandv/legend/legend-engine`, plus
   DuckDB `EXPLAIN` and timings wherever a cost is claimed.

---

## 1. Where the authoritative expected-SQL lives

**There are no golden-SQL fixture files in this repo.** Nothing under `core/src/test/resources`,
`spec/src/test/resources/rcorpus` (test-name rosters only), `pct/`, or `parser-equivalence/` contains
expected query SQL. The one `.sql` file in the tree
(`docs/type-audit-2026-08/harness/fixture/ddl.sql`) is seed DDL.

Expected SQL is pinned in exactly two places:

**(a) Inline Java asserts, all in `core/src/test/java`:**

- ~20 exact-text goldens. The mapping-relevant ones:
  `resolver/ResolveSimpleClassTest.java:145,160,245` and `CompilerFacadeTest.java:93`.
- **~150 structural count asserts** — *this is the real mechanism.*
  `resolver/ResolveNavigationTest.java` alone carries 56 (join budgets for mapping navigation);
  `lowering/LowerRelationTest.java` 26; `integration/RelationalMappingIntegrationTest.java` 12.
- ~210 substring asserts.
- **The sharpest leanness pin in the repo** is `integration/UnionTargetLeanJoinTest.java:120-129`:
  exactly one ` ON `, no ` OR ` in the ON clause, no `coalesce`, and a regex on the join condition.

**(b) The engine corpus — goldens read live off disk, and text is *not* the verdict.**
`spec/src/test/java/com/legend/rcorpus/Corpus.java:49` reads `$HOME/legend/legend-engine`;
`core/src/main/java/com/legend/SqlTextVerdicts.java:144` does the byte diff, but `:29-33` states text
match is a *census number* and a row divergence fails regardless.
`docs/RELATIONAL_CORPUS.md:3`: *"row equality is the contract, golden SQL is advisory."*
Text-decided escapes are capped at `MinimalCorpusTest.java:487-493`.

**One caveat worth flagging:** `integration/CorpusDifferentialTest.java` is **dead** — its own javadoc
(`:16-23`) says `scripts/corpus/differential.py` runs in no gate and the `Assumptions` guard skips it
in every build. **It verifies nothing.**

---

## 2. Emitted SQL per shape

**Plain table-backed mapping** (probe 1) — lean, demand-pruned, no wrapper:

```sql
SELECT t0.FIRSTNAME AS fn, t0.LASTNAME AS ln
FROM PERSON AS t0
```

**Join, the `legacyNavigate` path** (probe 2) — one join; and **no join at all** when the property
isn't read (probe 2b):

```sql
SELECT t0.FIRSTNAME AS fn, t1.LEGALNAME AS firm
FROM PERSON AS t0
LEFT OUTER JOIN FIRM AS t1 ON t0.FIRMID = t1.ID
```

**VIEW-backed ("view is the frame", `7da6acaa8`)** — a plain or filtered view at the root **fully
flattens** (probe 3):

```sql
SELECT t0.NAME AS n
FROM PEOPLE AS t0
WHERE t0.ACTIVE = 1
```

In a join position the frame projects only the demanded columns (probe `V1`):

```sql
SELECT t0.N AS n, t2.fn AS f
FROM TP AS t0
LEFT OUTER JOIN (
  SELECT t1.ID AS id, t1.FN AS fn
  FROM TF AS t1
  WHERE t1.OK = 1
) AS t2 ON t0.FID = t2.id
```

**`~groupBy`** (probe 4) — wraps whenever an aggregate is demanded:

```sql
SELECT t1.acct AS a, t1.total AS t
FROM ( SELECT t0.ACC_NUM AS acct, SUM(t0.QTY) AS total
       FROM TRADE AS t0 GROUP BY t0.ACC_NUM ) AS t1
```

**`~distinct`** (probe 5) — wraps, dedups over every mapped column:

```sql
SELECT t1.TAG AS t
FROM ( SELECT DISTINCT t0.ID, t0.TAG FROM TAGS AS t0 ) AS t1
```

**`~filter` plain** (probe 6a) — folded into `WHERE`, flat, not exists-shaped:

```sql
SELECT t0.NAME AS n
FROM PEOPLE AS t0
WHERE t0.ACTIVE = 1
```

**`~filter` join-mediated, the INNER row-exploding form** (probe 6b) — `LEFT OUTER JOIN` +
null-rejecting `WHERE`, flat:

```sql
SELECT t0.NAME AS n
FROM PEOPLE AS t0
LEFT OUTER JOIN FIRM AS t1 ON t0.FIRMID = t1.ID
WHERE t1.ACTIVE = 1
```

**Operation UNION** (probe `E`) — the leanest thing in the codebase:

```sql
SELECT t0.NAME AS n, t3.lastName AS e
FROM FIRM AS t0
LEFT OUTER JOIN (
  SELECT t1.LAST AS lastName, t1.FIRM_ID AS __route0_0 FROM P1 AS t1
  UNION ALL
  SELECT t2.LAST AS lastName, t2.FIRM_ID AS __route0_0 FROM P2 AS t2
) AS t3 ON t0.ID = t3.__route0_0
```

An outer filter is applied above the union, not pushed into the arms (probe 7b) — the engine does the
same; DuckDB pushes it down itself.

**`otherwise`-embedded** (probe 8) — the inline hit emits **zero joins**:

```sql
SELECT t0.NAME AS n, t0.FIRM_NAME AS fn
FROM PERSON AS t0
```

Only a fallback-only property pulls the join in (probe 8b).

---

## 3. Findings

### HIGH-1 — the union subtree is materialized twice when an `exists` filter and an aggregate both read it

**Measured, not theoretical.** Emitted (probe `R1`, and in the corpus from `UnionJoinMappedPropertyTest`):

```sql
SELECT t0.NAME AS n, CASE WHEN t4.agg_0 IS NOT NULL THEN t4.agg_0 ELSE 0 END AS c
FROM FIRM AS t0
LEFT OUTER JOIN (
  SELECT t3.__route0_0, COUNT(1) AS agg_0
  FROM ( SELECT t1.FIRM_ID AS __route0_0 FROM P1 AS t1
         UNION ALL SELECT t2.FIRM_ID AS __route0_0 FROM P2 AS t2 ) AS t3
  GROUP BY t3.__route0_0
) AS t4 ON t0.ID = t4.__route0_0
LEFT OUTER JOIN (
  SELECT DISTINCT t7.__route0_0
  FROM ( SELECT t5.FIRM_ID AS __route0_0 FROM P1 AS t5
         UNION ALL SELECT t6.FIRM_ID AS __route0_0 FROM P2 AS t6 ) AS t7
  WHERE TRUE
) AS t8 ON t0.ID = t8.__route0_0
WHERE t8.__route0_0 IS NOT NULL
```

DuckDB `EXPLAIN` on 2,000 firms × 200,000 people: **4 table scans and 2 hash joins, vs 2 scans and 1
join for the CTE form; 2.26 ms vs 1.12 ms (best of 7) — 2.0×.**

**No CSE/CTE pass exists:** `SqlWith` is constructed only by `SqlPostProcessors.extractCtes` (an
opt-in connection feature for engine parity), never by an optimizer. Also visible here: a dead
`WHERE TRUE`, and a `LEFT OUTER … WHERE … IS NOT NULL` pair that is an `INNER JOIN` spelled long.

Lean version:

```sql
WITH emp AS ( SELECT FIRM_ID AS k FROM P1 UNION ALL SELECT FIRM_ID AS k FROM P2 )
SELECT f.NAME AS n, a.c AS c
FROM FIRM f
JOIN ( SELECT k, COUNT(1) AS c FROM emp GROUP BY k ) a ON f.ID = a.k
```

Probe `R2` shows two *aggregates* already share one union correctly — **so the duplication is specific
to the `exists` filter building its own `DISTINCT` copy instead of reusing the grouped relation.**

### HIGH-2 — `~groupBy` wraps, and projects dead columns, where the engine does neither

Ours (probe `Q1`, project 2 of 4 mapped properties):

```sql
SELECT t1.gsn AS g, t1.qty AS q
FROM (
  SELECT t0.ACC_NUM AS acct, t0.PRODUCT_ID AS prod, t0.GSN AS gsn, SUM(t0.QTY) AS qty
  FROM TRADE AS t0
  GROUP BY t0.ACC_NUM, t0.PRODUCT_ID, t0.GSN
) AS t1
```

Two SELECTs, and `acct` / `prod` are projected but never read.

**The engine's golden for the identical shape is flat *and* clean** —
`core_relational/relational/tests/mapping/groupBy/testGroupBy.pure:74-79`, `testGroupByMappingProject`,
query `Position.all()->project([#/Position/gsn#,#/Position/quantity#])->sort('gsn')`:

```sql
select "root".GSN as "gsn", sum("root".QTY) as "quantity"
from TRADE as "root" group by "root".ACC_NUM, "root".PRODUCT_ID, "root".GSN order by "gsn" asc
```

The engine wraps only when a filter touches an aggregate. **Our wrapper fires on the mere presence of
an aggregate column** — probe `Q3` (keys only, no aggregate) proves the flat form is reachable:

```sql
SELECT t0.GSN AS g FROM TRADE AS t0 GROUP BY t0.ACC_NUM, t0.PRODUCT_ID, t0.GSN
```

**Root cause of the dead columns:** `SubselectPrune` refuses to prune grouped selects by rule
(`core/src/main/java/com/legend/lowering/SubselectPrune.java:33-36`) — correct for `DISTINCT`,
unnecessary for `GROUP BY`.

**Mitigation:** DuckDB's plan is identical for both forms, so this is a **text and parity defect, not
a measured cost**. (The `PRODUCT_ID` in the `GROUP BY` but not the select list is the engine's
behaviour too — parity, not a defect.)

**Counterpoint in our favour:** on the HAVING case we are **leaner** than the engine. Probe `A3`,
`filter(x|$x.total > 10)`:

```sql
SELECT t0.ACC_NUM AS a
FROM TRADE AS t0
GROUP BY t0.ACC_NUM
HAVING SUM(t0.QTY) > 10
```

The engine wraps there.

### MED-3 — an `AssociationMapping`-mediated navigation out of a `~filter`-ed class isolates the root into `SELECT *`

Clean A/B: same model, same rows, only the mapping spelling differs.

Direct property mapping (`firm: [test::DB] @PersonFirm`) — probe `X1`, **one SELECT**:

```sql
SELECT t0.NAME AS name, t1.LEGAL_NAME AS firm
FROM T_PERSON AS t0
LEFT OUTER JOIN T_FIRM AS t1 ON t0.FIRM_ID = t1.ID
WHERE t0.ID <= 3
```

Via `AssociationMapping` — probe `Y1`, **two SELECTs and a star**:

```sql
SELECT t1.NAME AS name, t2.LEGAL_NAME AS firm
FROM ( SELECT * FROM T_PERSON AS t0 WHERE t0.ID <= 3 ) AS t1
LEFT OUTER JOIN T_FIRM AS t2 ON t1.FIRM_ID = t2.ID
```

Corpus witness: `AssociationIntegrationTest.testAssociationWithMappingFilter`, fixture at
`core/src/test/java/com/legend/integration/AssociationIntegrationTest.java:660-708`. **The lean form is
already reachable — it is what the direct mapping emits.**

### MED-4 — `SELECT *` inside subselects (11 of 344 distinct corpus queries, 3.2%)

**Root cause:** `core/src/main/java/com/legend/lowering/Lowerer.java:533` lowers a bare
`TypedTableReference` to `SqlSelect.starOf(...)`, and `SubselectPrune` skips star projections by
design (`:369`). So any isolation frame sitting straight over a table keeps the `*`:

```sql
SELECT t1.NAME AS name, t1.PRICE AS price
FROM (
  SELECT *
  FROM ITEMS AS t0
  WHERE t0.CATEGORY = 'Tools'
  ORDER BY t0.PRICE DESC NULLS FIRST
  LIMIT 2
) AS t1
```

DuckDB prunes it anyway — the `SEQ_SCAN Projections` lists are identical to the hand-written form. The
real cost is **portability** and exactly the failure mode the prune pass was written for
(`SubselectPrune.java:24-29`: corpus stores whose model declares columns the physical table never
carries). Note **the view path already does this right** (probe `V1` above emits
`SELECT t1.ID AS id, t1.FN AS fn … WHERE t1.OK = 1`, not `*`).

### MED-5 — navigation chains nest one derived table per hop past the first

Probe `H`, a 4-hop class navigation reading only the leaf:

```sql
SELECT t5.c_d_DN AS dn
FROM TA AS t0
LEFT OUTER JOIN (
  SELECT t1.ID AS ID, t4.d_DN AS c_d_DN
  FROM TB AS t1
  LEFT OUTER JOIN (
    SELECT t2.ID AS ID, t3.DN AS d_DN
    FROM TC AS t2
    LEFT OUTER JOIN TD AS t3 ON t2.DID = t3.ID
  ) AS t4 ON t1.CID = t4.ID
) AS t5 ON t0.BID = t5.ID
```

An N-hop class navigation yields N−1 nesting levels. Lean is one flat `FROM` with N−1 left joins.
**EXPLAIN shows DuckDB does not reassociate it** — the nested form pins a right-deep join tree
(`TA ⋈ (TB ⋈ TC)`) where the flat form gives left-deep. Rows identical; the flat form leaves join
order to the planner.

**Important scoping: column-mapping chains do *not* suffer this.** Probes `S1`–`S6` over
`deptName: @P_D | T_DEPT.NAME` plus `orgName: @P_D > @D_O | T_ORG.NAME` share joins perfectly and stay
at 1 SELECT / 2 joins. **The nesting is specific to class-typed navigation hops.**

### LOW-6 — alias and generated-column readability

Aliases are opaque counters (`t0…tN`) where the engine uses table-derived names (`"root"`,
`"firmtable_0"`, `"account_info_1"`). Generated columns leak internals: `c_d_DN`, `F_T_total`,
`__route0_0`, `k1__PRODUCT_ID`, `agg_0`. No churn, no collisions — purely readability.

### LOW-7 — missed CSE across independently-named Relation-API navigations

```sql
SELECT t0.NAME, t1.NAME AS deptName, t3.NAME AS orgName
FROM T_PERSON AS t0
LEFT OUTER JOIN T_DEPT AS t1 ON t0.DEPT_ID = t1.ID
LEFT OUTER JOIN T_DEPT AS t2 ON t0.DEPT_ID = t2.ID
LEFT OUTER JOIN T_ORG AS t3 ON t1.ORG_ID = t3.ID
```

`T_DEPT` joined twice on an identical condition. This is
`RelationalMappingIntegrationTest.testTwoIndependentTraversals` (`:1337-1345`), where the *user* wrote
two `navigate(~x1…)` / `navigate(~x3…)` chains with distinct binding names, and `:1571` pins 3 joins
deliberately. **Correctly attributed: not a mapping-normalizer defect**, but a CSE the lowerer could take.

### NOT a defect — `~distinct`'s "every property column plus slot pseudo-columns"

**Required semantics and exact engine parity.** `MappingNormalizer.java:1900-1908` documents it, and
undemanded slots *are* dropped by the materializer. The engine's golden is structurally identical —
`tests/mapping/distinct/testDistinct.pure:62`:

```sql
select "root".IF_NAME as "name" from (select distinct "account_info_1".IF_CODE as IF_CODE, "account_info_1".IF_NAME as IF_NAME from ACCOUNT_INFO as "account_info_1") as "root" order by "name" asc
```

— and `testDistinct.pure:54` carries the engine's own `//todo: could optimize to collapse the sub
select back`. Neither form is lean; ours is no worse.

### NOT a defect — and better than the engine: `LEFT OUTER JOIN` + null-rejecting `WHERE` for the INNER `~filter`

`JoinChainEmission.java:962-972` documents the `LEFT + WHERE ≡ INNER + WHERE` equivalence and throws
loudly on the null-tolerant case rather than being silently wrong. Good engineering. And for the
explicit `(INNER)` form **the engine is *worse*** — it isolates and dumps every physical column
(`testClassMappingFilterWithInnerJoin.pure:38-43`):

```sql
select "root".ID as "pk_0", "root".FIRSTNAME as "firstName"
from (select "root".ID as ID, "root".FIRSTNAME as FIRSTNAME, "root".LASTNAME as LASTNAME,
             "root".AGE as AGE, "root".ADDRESSID as ADDRESSID, "root".FIRMID as FIRMID,
             "root".MANAGERID as MANAGERID
      from personTable as "root"
      inner join firmTable as "firmtable_0" on ("firmtable_0".ID = "root".FIRMID)
      where "firmtable_0".LEGALNAME = 'Firm X') as "root"
```

Seven columns and two SELECTs, against our one flat SELECT with the demanded columns only.

> **Caveat:** `03-join-chain-emission.md` H4 finds that our null-rejection test checks the condition's
> *shape*, never that it constrains the *joined side*. The leanness win is real; the correctness of
> the equivalence is not fully guarded.

---

## 4. Engine comparison — where we win and lose

- **Union arms.** Engine golden (`tests/mapping/union/testUnion.pure:384`, `:188-193`) projects
  **5 columns per arm** for a 2-set union — `"root".ID as "pk_0_0", null as "pk_0_1", …, FirmID_0,
  null as FirmID_1` — with pk columns the projection never consumes, and joins on an **OR**:
  `on ("unionBase".FirmID_0 = "firm_0".ID or "unionBase".FirmID_1 = "firm_0".ID)`. We project **2**,
  one merged key, one equi-join. The union-over-views golden (`functions/tests/projection/testView.pure:62-72`)
  reaches **5 SELECTs** for a two-column result. `docs/UNION_OR_JOIN_REMOVAL_DESIGN_2026_09_13.md §11-12`
  measures the predicate difference: OR → `BLOCKWISE_NL_JOIN` 2.77 s vs ours `HASH_JOIN` 3.8–5.0 ms at
  20k firms / 400k people.
- **View frames.** Engine golden (`functions/tests/projection/testView.pure:24-30`) inlines the view
  body projecting **all 5 view columns and executing all 3 view joins** to answer a one-column query,
  then wraps. Ours: a 2-column frame with no extra joins, or fully flattened at the root.
- **`~filter (INNER)`.** As above — we are flat, the engine wraps and dumps 7 columns.
- **`getAll()` pk columns.** The engine injects `"root".ID as "pk_0"` into nearly every rooted query.
  We don't.
- **HAVING folding** on `~groupBy` mappings.
- **We lose on `~groupBy` project** (HIGH-2) — the only shape where the engine is leaner.

| shape | engine wraps? | we wrap? | verdict |
|---|---|---|---|
| plain table | no | no | parity |
| to-one join | no | no | parity |
| view-backed | **yes**, all view columns + all view joins | no (or 2-col frame) | **we win** |
| `~groupBy` project | **no** | **yes**, + 2 dead columns | **we lose** |
| `~groupBy` + agg filter | yes | no (HAVING fold) | **we win** |
| `~distinct` | yes | yes | parity (engine's own `//todo`) |
| `~filter` plain / LEFT | no | no | parity |
| `~filter (INNER)` | **yes**, all physical columns | **no** | **we win** |
| Operation union | yes, + pk/key null padding + OR join | yes, one merged key, equi-join | **we win big** |
| `otherwise` | no | no | parity |

---

## 5. Census

| | mapping corpus (344 distinct SELECTs) |
|---|---|
| single flat SELECT | **292 (85%)** |
| nested (>1 SELECT) | 52 (15%) |
| — json/graph-fetch envelope (inherent) | 20 |
| — sort/limit isolation (mostly necessary) | 17 |
| — `SELECT *` subselect | 11 |
| — grouped subselect | 8 |
| — union member subselect | 6 |
| — distinct subselect | 4 |
| — other | 6 |
| repeated identical subselect subtree | **0** |
| ≥1 join | 167 |
| ≥3 joins | 12 |
| unnecessary `DISTINCT` | 0 |
| `CAST` / `COALESCE` outside the JSON envelope | 0 of 29 / 0 of 23 |

Widening the run to `MetamodelMappingStoreTest` (the metamodel-as-relations store, itself a mapping)
gives 375 distinct queries with **37 `SELECT *` subselects and one 6-SELECT query** — that store is by
far the heaviest consumer of the star-frame path and is where a fix to MED-4 would pay most.

---

## Verdict: yes, the SQL is lean — with four named gaps

**85% of mapping-driven queries are a single flat SELECT** carrying exactly the demanded columns and
exactly the demanded joins. Demand pruning genuinely works: an unread property emits no join, an
unread column no projection, a plain view no subselect, an `otherwise` inline hit no join at all. On
the two shapes the reference engine handles worst — unions and view frames — **the rewrite is
materially leaner than the engine**, with measured 100×-plus plan wins on the union path. Across 344
corpus queries there is zero subexpression duplication, zero unnecessary `DISTINCT`, and no gratuitous
`CAST`/`COALESCE`.

The four gaps, in payback order:

1. **HIGH-1, union double-materialization** — measured 2.0× (2.26 ms vs 1.12 ms, 4 scans vs 2) on
   200k rows. Needs a CSE/CTE pass; none exists today.
2. **HIGH-2, `~groupBy` wrapper plus dead columns** — the one parity *regression*. Zero measured cost
   on DuckDB, but the shape where we are provably behind.
3. **MED-3, `AssociationMapping` + `~filter` root isolation** — the A/B shows the flat form is already
   reachable.
4. **MED-4/5, `SELECT *` frames and per-hop chain nesting** — DuckDB absorbs both today; the chain
   nesting does pin a right-deep join tree, which will matter on engines with weaker join-order search.

**The structural root of 2–5 is the same:** `core/src/main/java/com/legend/lowering/SubselectPrune.java`
is the *only* post-lowering SQL optimizer, and it prunes columns without ever collapsing a wrapper or
touching a star. **A conservative select-merge pass — fold a subselect into its parent when the parent
adds no `DISTINCT`/`GROUP BY`/`LIMIT` conflict — would close most of the nesting census in one place.**
