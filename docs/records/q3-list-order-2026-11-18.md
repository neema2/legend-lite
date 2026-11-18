# Q3: MariaDB's list order under BOTH, the diagnosis and three options (2026-11-18)

Track A5 of PR #118's follow-ups (branch `claude/parity-a5`). Q3 is `docs/records/pr118-audit-2026-11-18/FOLLOW_UPS.md`
§1 (on `claude/refactor-code-quality-review-9fqhug`): "MariaDB channel B under BOTH is nondeterministic: the host judge
reads list rows in an unstable order (about 20 essential tests flip between runs)". It is a G2 break (judge parity) and
a G10 break (determinism). The orchestrator's ruling of 2026-11-18: the choice of option goes to the user after PR #118
lands, with the recommendation of (A'); A' is not started. Nothing for Q3 is committed beyond this record. Paths are at
`b46a6990a`, under `core/src/main/java/com/legend/`.

## 1. The evidence

**The lane session's runs** (`//pct:pct_channel_b_mariadb_*`, default judge BOTH, five runs over 815de5810, 68f20eb4d
and f250b02d6; its extracts are in its container, `/root/lane-keep/q3_mariadb_both.txt`):

- essential: 36 tests fail in some run. 12 always fail, the ledgered rows; 24 flip. Failure counts per run were 24,
  25, 17, 21 and 27.
- grammar: 8 flip. relation: 2. standard: 1. unclassified: 1.
- Every flip is `JudgesDisagree ... host ... assertEquals failed ...; database ... held`. The wrong list is a rotation
  of the right one, on the expected side or on the actual side.
- Under DATABASE, none of these fail, in any run of any commit.

**This session's runs** at `0b508b3d0` (`//pct:pct_channel_b_mariadb_essential --runs_per_test=3`, embedded MariaDB
11.4): 14 distinct tests flipped, 4 to 9 per run. In every flip, the side read out of order was the EXPECTED literal
list. Examples:

- `assertEquals(['Smith', 'Doe', 'Branche'], ...)` read as `['Branche', 'Smith', 'Doe']`;
- `split`'s `['Hello', 'World']` read as `['World', 'Hello']`;
- `zip`'s lists of pairs, rotated.

## 2. The diagnosis: the plan states no order for a list's elements

**A list's side.** A list is planned as `SELECT UNNEST(<list>) AS value`, with no FROM. Measured for
`['Hello', 'World']`: `SqlSelect[projections=[UNNEST(CompactList(ArrayLit[...]))], from=Dual]`.

**The order the plan states for it.** `lowering/SideOrdinal.java:64` (`defined`) gives a side an ordinal for a VALUES
source, a UNION ALL (`branchOrder`, `:191`) and a sorted select. It gives none for this shape. So both judges number
the side's rows "as the database returns them", with `ROW_NUMBER() OVER ()`:

- **The host judge.** Its side batch reads consecutive asserts' sides by one statement. A side with no ordinal is
  numbered `ROW_NUMBER() OVER ()` over its derived table (`lowering/HostSideBatch.java:208`).
  `VerdictSql.inBranchOrder` (`testplan/HostSideBatching.java:168`, `testplan/JudgePlans.java:246`) only touches
  unions.
- **The database judge.** It numbers the side the same way in its CTE (`SideOrdinal.withOrdinal`, `:52`, through
  `numberedInside`, `:277`).

**How MariaDB renders the two kinds of list:**

- A LITERAL list: a UNION ALL of one-row selects.
- A COMPUTED list: the dialect's JSON_TABLE explosion, `FOR ORDINALITY`, ordered by that key in a derived table
  (`sql/dialect/MariaDb.java:447`; an ordered derived table takes a LIMIT, `:432`).

**What MariaDB does with them.** In both shapes, MariaDB 11.4 numbers `ROW_NUMBER() OVER ()` over the derived table in
an order that is not the table's:

- inside the host batch's CTE and union, unstably between runs (Q3 as reported);
- in the database judge's CTEs, the same way for both of its sides.

**Why the database judge held.** It was not right; it was wrong by symmetry: both of its sides were scrambled alike.
Option A below shows this.

**Upstream.** A Pure list literal has its order, and so does every list a function returns. This is not a ruling: lite
must read each list in its order. `SideOrdinal`'s own javadoc already names the class ("a database need not keep a
derived table's or a CTE's order through the selects that read it", measured on MariaDB and Trino, 2026-10-20).
`branchOrder` was added for the same class on 2026-11-06 (BigQuery's list of instances, "promises none, so the test
passed or failed with the run").

## 3. Option A, measured (not committed)

Option A: in `SideOrdinal.defined`, a literal list exploded with no FROM becomes a select over a VALUES source of its
elements, numbered by the existing `indexed` arm. So each element's ordinal is its place in the list, for both judges
and every database. The patch is in the appendix.

**The SQL it plans** (MariaDB, `['Hello', 'World']`, the host batch):
`SELECT "__l"."value", "__l"."__ord" FROM (SELECT 'Hello' AS "value", 1 AS "__ord" UNION ALL SELECT 'World', 2) AS "__l"`.
The batch then reads `"__b"."__ord"` as the row order. The database judge's CTE numbers by
`ROW_NUMBER() OVER (ORDER BY ("side"."__ord" IS NULL), "side"."__ord")`.

**MariaDB essential, three runs:**
- The 14 expected-literal flips are gone.
- New failures, 3 of 3 runs, all `host held; database failed`: `testReverse`, `testAddWithOffset`, `testFoldFiltering`
  and `testFoldToMany`. For example, in `assertEquals(['c', 'b', 'a'], ['a', 'b', 'c']->reverse())` the database judge
  reads the computed actual as `['a', 'b', 'c']`.
- A few flips remain (`removeDuplicates`, `slice`).

The computed list is still numbered by arrival, and with the expected side now right, the database judge's symmetric
luck is gone. **Option A alone is not a fix.**

**Its cost on the reference lane** (DuckDB, `bazel build //pct:ratchets` against the committed
`pct/src/test/resources/org/finos/legend/lite/pct/channelb/ratchets.tsv`): judge statements and discovery are
unchanged. Characters and plan JSON grow:

| Pin | Committed | With option A | Growth |
|---|---|---|---|
| `channel_b.essential.judge.chars` | 1,717,207 | 1,722,309 | +5,102 (0.30%) |
| `channel_b.essential.judge.plan` | 28,684,638 | 28,754,291 | +69,653 (0.24%) |
| `channel_b.grammar.judge.chars` | 670,493 | 671,452 | +959 (0.14%) |
| `channel_b.grammar.judge.plan` | 11,586,101 | 11,599,276 | +13,175 (0.11%) |
| `channel_b.relation.judge.chars` | 2,006,804 | 2,006,986 | +182 (0.01%) |
| `channel_b.relation.judge.plan` | 34,502,872 | 34,505,862 | +2,990 (0.01%) |
| `channel_b.standard.judge.chars` | 1,647,537 | 1,647,745 | +208 (0.01%) |
| `channel_b.standard.judge.plan` | 24,542,270 | 24,545,155 | +2,885 (0.01%) |
| `channel_b.unclassified.judge.chars` | 233,207 | 233,349 | +142 (0.06%) |
| `channel_b.unclassified.judge.plan` | 4,133,908 | 4,135,912 | +2,004 (0.05%) |

`ChannelBLane.judgeSqlHeld` fails on any growth, so each of these is a pin the user's word moves.

**What else it touched:** these pass with option A applied: `//core:lean_sql_baseline_test`, `:core_tests_testplan`,
`:core_tests_testexec`, `:core_tests_lowering`, `:core_tests_sql`, `:core_tests_integration`,
`:core_tests_integration_sqlite` and `:core_tests_integration_h2`.

## 4. The three options

**(A') The full fix, recommended by the orchestrator.** An exploded list's ordinal is its elements' positions, on every
database.
- Literal lists: option A's VALUES index.
- Computed lists: a new MIR node for an element's position under `UNNEST`. AGENTS.md invariant 3a: a new typed variant
  and one render arm per dialect. The positions each database already has:
  - MariaDB and MySQL: JSON_TABLE's `FOR ORDINALITY` key, already rendered.
  - DuckDB: `generate_subscripts`.
  - Postgres and Trino: `WITH ORDINALITY`.
  - BigQuery: `WITH OFFSET`.
  - Snowflake: `FLATTEN`'s `INDEX`.
  - ClickHouse: `arrayEnumerate`.
  - Databricks: `posexplode`.
  - SQLite: `json_each`'s key.
  - H2 has no lists.
- Cost: every dialect, the lean-SQL counts, and the reference lane's judge-SQL pins, which grow at least by option A's
  table. A day or two of work, with lane rounds on the server and cloud databases.

**(B) MariaDB and MySQL only.** Option A, plus the dialect numbering an empty-window `ROW_NUMBER()` over its own
explosion by the key it already has. Smaller, but special-cased to one family; every other database keeps reading
arrival order, which none promises.

**(C) Park Q3 with this diagnosis**, the flake registered honestly, until (A') is scheduled.

## 5. Appendix: option A's patch (against `b46a6990a`, uncommitted)

```diff
diff --git a/core/src/main/java/com/legend/lowering/SideOrdinal.java b/core/src/main/java/com/legend/lowering/SideOrdinal.java
index 0173fc0a0..283aab3f2 100644
--- a/core/src/main/java/com/legend/lowering/SideOrdinal.java
+++ b/core/src/main/java/com/legend/lowering/SideOrdinal.java
@@ -84,6 +84,10 @@ final class SideOrdinal {
             // H2's corpus: the union's ordinal written beside a SUM, "must be in the GROUP BY list")
             return null;
         }
+        SqlSelect listed = literalList(s);
+        if (listed != null) {
+            return defined(listed);
+        }
         String alias;
         SqlSource from;
         if (s.from() instanceof SqlSource.Values v) {
@@ -224,6 +228,62 @@ final class SideOrdinal {
                 List.of(), null, null, outs);
     }
 
+    /**
+     * A LITERAL LIST exploded with no FROM -- {@code SELECT UNNEST(<its elements>) AS c}, the lowering's list literal --
+     * as the same select over its elements' rows, a VALUES source, so its ordinal is each element's place in the list
+     * ({@link #indexed}), the order Pure's list literal defines (the PR #118 follow-ups' Q3, track A5, 2026-11-18). Numbered
+     * by its arrival ({@code ROW_NUMBER() OVER ()}), the order was the database's: MariaDB 11.4 read a literal list's
+     * rows -- a UNION ALL of one-row selects there -- in another order on some runs inside the host judge's side batch,
+     * about twenty of channel B's essential tests under BOTH flipping between runs (the same class as
+     * {@link #branchOrder}'s BigQuery list of instances). Null where {@code s} is no such select: elements that are not
+     * all constants (a constant compares nothing and reads no row), an empty list, or a NULL element, which the list's
+     * compaction would drop.
+     */
+    private static @com.legend.base.Nullable SqlSelect literalList(SqlSelect s) {
+        if (!(s.from() instanceof SqlSource.Dual) || s.where() != null || s.qualify() != null
+                || s.projections().size() != 1 || s.outputs().size() != 1) {
+            return null;
+        }
+        SqlSelect.Projection p = s.projections().get(0);
+        if (!(p.expr() instanceof SqlExpr.Call c) || c.fn() != com.legend.sql.SqlFn.UNNEST || c.args().size() != 1) {
+            return null;
+        }
+        SqlExpr list = c.args().get(0) instanceof SqlExpr.CompactList cl ? cl.list() : c.args().get(0);
+        if (!(list instanceof SqlExpr.ArrayLit a) || a.elements().isEmpty()) {
+            return null;
+        }
+        List<List<SqlExpr>> rows = new ArrayList<>(a.elements().size());
+        for (SqlExpr e : a.elements()) {
+            if (e instanceof SqlExpr.NullLit || !constant(e)) {
+                return null;
+            }
+            rows.add(List.of(e));
+        }
+        OutputCol out = s.outputs().get(0);
+        String alias = "__l";
+        SqlSource.Values elements = new SqlSource.Values(rows, List.of(out.name()), alias, List.of(out));
+        return new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of(alias, out), p.alias(), p.out())), false,
+                elements, null, List.of(), null, null, List.of(), null, null, s.outputs());
+    }
+
+    /** Whether {@code e} is a constant: a literal, or a cast, list or struct of literals. */
+    private static boolean constant(SqlExpr e) {
+        return switch (e) {
+            case SqlExpr.StringLit x -> true;
+            case SqlExpr.IntLit x -> true;
+            case SqlExpr.FloatLit x -> true;
+            case SqlExpr.DecimalLit x -> true;
+            case SqlExpr.BoolLit x -> true;
+            case SqlExpr.DateLit x -> true;
+            case SqlExpr.TimestampLit x -> true;
+            case SqlExpr.TimeLit x -> true;
+            case SqlExpr.Cast x -> constant(x.value());
+            case SqlExpr.ArrayLit x -> x.elements().stream().allMatch(SideOrdinal::constant);
+            case SqlExpr.StructLit x -> x.fields().stream().allMatch(f -> constant(f.value()));
+            default -> false;
+        };
+    }
+
     /** A literal's rows, each with its index. */
     private static SqlSource.Values indexed(SqlSource.Values v) {
         List<List<SqlExpr>> rows = new ArrayList<>(v.rows().size());
```
