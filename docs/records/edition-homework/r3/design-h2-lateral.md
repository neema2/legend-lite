# H2: a correlated-filter lateral as a join — design

Prepared read-only in /home/user/ll-edit at 95b1adf1 (branch claude/edit-ahead). The ruled item: docs/records/edition-design-v3-2026-11-25.md:269 (item 8);
evidence docs/records/edition-homework/r2/pct-sort.md:392 (D3) and :418 (D5 item 3). Nothing was built or run here.
I took the SQL shapes from lite's own recorded H2 failures (the edge goldens), not from a probe.

## 0. Headline

- ClickHouse already does this rewrite, at render time: `ClickHouse.java:342-390`. Its helpers are at
  `filterOnly` :581, `aliasesOf` :673, `TablesRead` :683, `InnerAsOutput` :700 and `conjuncts` :735. The new pass is
  that logic as a `SqlRewriter`, with three checks ClickHouse lacks: alias shadowing, reads of a scope further out,
  and leaving the join as it is instead of throwing.
- The pass can only change statements that fail on H2 today. Any CROSS/LEFT_LATERAL that reaches H2's renderer gets
  `Function "LATERAL" not found`. So no H2 test that passes now can change its answer. The risk is limited to a wrong
  answer on a shape that fails loudly today, and the conditions in §2 rule that out.

## 1. The MIR the lowering writes

**The lateral arm.** `Lowerer.lateral` at `core/src/main/java/com/legend/lowering/Lowerer.java:1126-1157`, dispatched
from :1252-1255 (`NativeFn.LowererForm.LATERAL`):
- left: `relation(arg0)`, then `asLeftJoinSide` (:3197-3199). A bare select unwraps to its source, a join tree
  included. Anything else wraps as `Subselect(side, nextAlias())`.
- The lambda's row parameter: a resolver is pushed on `enclosing` (:1130-1144). It resolves `$x.name` with
  `resolveOrThrow(SqlSelect.starOf(leftSide), name)` (:2402), which calls `Fold.resolveInto` → `resolveIntoExact` →
  `sourceColumn(from, column, ...)` (Fold.java:770-806). The result is a `SqlExpr.Column` **qualified by the left
  source's alias** (for a join tree, the alias of the side that owns the name).
- right: `relation(lambda body)`, wrapped as `new SqlSource.Subselect(right, nextAlias(), null)` (:1151-1152). It is
  ALWAYS a Subselect, even when `right` is bare.
- the join: `new SqlSource.Join(leftSide, sub, CROSS_LATERAL, null)` (:1151-1154). The result is `SqlSelect.starOf(join)`
  (:1156), a star frame whose outputs are `Join.delivered(left, right, kind)` (SqlSource.java:355-372).

**A TDS literal.** `tdsLiteral` (Lowerer.java:2840-2874) returns `starOf(new SqlSource.Values(rows, names, "_tds"+n,
outputsOf(info)))`. Its outputs are declared, so a star over it maps names exactly.

**The filter.** `filter` (Lowerer.java:2189-2235): `src.withWhere(mergeWhere(src.where(), predicate, stamp))`. It is the
same select with a WHERE, not a new layer, unless the slot is ISOLATE. `==` in filter position renders as plain `=`
here (the recorded SQL below).

**Aliases.** `nextAlias()` is `"t"+aliasCounter++` (:1159-1161). TDS aliases are `"_tds"+tdsCounter++`. Both counters
belong to the Lowerer, so aliases are distinct within a statement (inferred from the code; the pass still checks it,
§3).

The four bodies, as the H2 goldens record them after all passes
(`pct/src/test/resources/edge/h2_relation_answers.tsv:117-120`; channel B
`channel_b_h2_relation_outcomes.tsv:246-249`). VALUES rows are elided.

| test (lateral.pure line) | Join tree | right `Subselect` (alias) : inner `SqlSelect` |
|---|---|---|
| testLateralJoin (:25) | `Join(Values _tds0, Sub t0, CROSS_LATERAL, null)` | t0: projections `[]` (star), FROM `Values _tds1(employeeId, employeeName, department)`, WHERE `_tds1.department = _tds0.departmentId` |
| testLateralJoinAreInnerJoins (:121) | same | t0: star, FROM `Values _tds1(departmentId, departmentName)`, WHERE `_tds0.department = _tds1.departmentId` (the outer column on the LEFT of `=`) |
| testLateralJoin_Chained (:65) | `Join(Join(Values _tds0, Sub t0, CROSS_LATERAL), Sub t1, CROSS_LATERAL)`. The second lateral's left is the bare first join, unwrapped by `asLeftJoinSide` | t0: star over `_tds1(did, Budget, Program)` WHERE `_tds1.did = _tds0.departmentId`; t1: star over `_tds2(...)` WHERE `_tds2.department = _tds0.departmentId`. The second `$x.departmentId` resolves through the join tree to `_tds0` |
| testLateralJoin_CorrelatedColumnNameAlsoOnInner (:162) | `Join(Values _tds0(personId, ref), Sub t0, CROSS_LATERAL)` | t0: projections `[_tds1.itemId AS itemId]` (`select(~itemId)` folded onto the filtered select), FROM `Values _tds1(itemId, ref)`, WHERE `_tds1.itemId = _tds0.ref` |

The select above the join is `starOf(join)` in every case. The renderer writes it as a qualified column list
(`"_tds0"."departmentId" AS ..., "t0"."employeeId" AS ...`). In channel B the body's `sort` adds an ORDER BY on that
select, e.g. `ORDER BY "_tds0"."personId" NULLS LAST` (channel_b golden :249). The outer select does not change.

## 2. The rewrite, stated precisely

**Shape.** The rule applies to `Join(L, Subselect(S, a, f), CROSS_LATERAL | LEFT_LATERAL, on)` where S is a `SqlSelect`
and `S.where() != null`. Definitions:
- `OUT` = the aliases of L's top-level join tree (the `aliasesOf` walk; Dual has no alias, and a Subselect contributes
  its own alias only).
- `OWN` = the aliases of `S.from()`'s top-level join tree.
- `DEF` = every alias defined anywhere inside S: the FROM tree, nested subselects, and subqueries inside expressions.

**Conditions.** All must hold, or the join is returned unchanged:
1. S only filters. There is no GROUP BY, HAVING, QUALIFY, DISTINCT, ORDER BY, LIMIT or OFFSET (ClickHouse's
   `filterOnly`, :581-586). Each of these is computed over the rows the correlated predicate kept, so moving the
   predicate out would change it. Examples: sql-parity's `lateral_top_one_per_department` (ORDER BY + LIMIT) and
   `lateral_aggregate_per_row` (`count(*)` with no GROUP BY, which yields a row even when nothing matches)
   (`core/src/test/resources/sql-parity/lite/lateral.yaml:39-42`).
2. Every projection is a plain `SqlExpr.Column` whose table is in OWN, or a `SqlExpr.Star` (its table in OWN, or
   null), or the projection list is empty (a star frame). This excludes aggregates (which also make S grouped),
   window calls (computed over the filtered rows), scalar subqueries and any computed value. `StarExcept` is refused.
   (A later relaxation could admit a computed scalar that reads OWN only and holds no aggregate, window or subquery.)
3. No shadowing: `DEF ∩ OUT = ∅`. This keeps "qualified by an OUT alias" equal to "a reference to the left row".
4. Outside S's WHERE, nothing reads OUT and nothing reads an alias outside DEF. That covers projections, the FROM tree
   (including a nested subselect that correlates), and the remaining clauses. A read further out would leave a
   correlated derived table, which H2 cannot evaluate either.
5. Split S's WHERE into its top-level AND conjuncts (flattened). For each conjunct C, with T(C) = the tables its
   columns read:
   - if `T(C) ∩ OUT = ∅`: C is **local**. It needs `T(C) ⊆ DEF` (condition 4's rule) and stays in S.
   - else C is **correlated**. It needs: no subquery node in C (`ScalarSubquery`, `Exists`, `InSubquery`,
     `Quantified`), no unqualified column in C, and `T(C) ⊆ OUT ∪ OWN`.
   - at least one conjunct must be correlated. Otherwise this is not the pass's shape (an uncorrelated lateral, or the
     UNNEST shapes, which the other passes own).
6. Every OWN column in a correlated conjunct is respelled as `Column.of(a, S.outputs(), outName)`, where `outName`
   comes from this map:
   - an empty projection list (star): for each leaf of S's FROM tree, `leaf.alias()+"."+oc.name() → oc.name()` over
     `leaf.outputs()`. A leaf with no declared outputs, or repeated names in `S.outputs()`, means refuse.
   - `Star(tab)`: the same over the leaves `tab` names.
   - `Column(tab, n)` projected as `p.outputName()` (`SqlSelect.java:158`): `tab.n → outputName`. The first wins when
     a column is projected twice (any copy is the same value).
   - If an OWN column read by a correlated conjunct is **not** in the map (not projected, as in `SELECT x ... WHERE
     y = outer.k`), refuse. Adding a hidden column would change `Join.outputs`, which the star frame above already
     captured.

**Result.**
`Join(L, Subselect(S.withWhere(AND(local) | null), a, f), CROSS_LATERAL ? INNER : LEFT, AND(respelled correlated ++ [on if not TRUE]))`.
The subselect keeps its alias and `S.outputs()`, so `Join.delivered` gives the same columns. `padsRight` is true for
both LEFT and LEFT_LATERAL (SqlSource.java:337-348), so nullability is unchanged as well. Every read above the join
(`t0.employeeId`) stays valid. The record requires an ON for INNER/LEFT (SqlSource.java:297-307), and the rewrite
always has one.

**Why the answer is the same.** For each left row l, a lateral yields `{r ∈ S'(rows) | p(l, r)}`, where S' is S with
the correlated conjuncts removed. Every column p reads from a row of S' is available under its output name (condition
6), so `p'(l, r) ≡ p(l, r)`. Conditions 1 and 2 make the rows of S' map one-to-one onto filtered source rows. An INNER
join `ON p'` gives exactly that set for each l.

**LEFT_LATERAL.** `LEFT JOIN LATERAL (S) ON c` gives `{r | p(l,r) ∧ c(l,r)}`, or one NULL-padded row when that set is
empty. `LEFT JOIN (S') ON p' ∧ c` gives the same set, and the same single padded row when nothing matches. So the
answer is the same. A predicate that evaluates to UNKNOWN is "no row" in a WHERE and "no match" in an ON, which is
again the same. None of the four tests needs this; it is safe to include, with a test.

**When a condition fails.** Return the join untouched. H2 then refuses it by name, as today. No throw in the pass, no
fallback (AGENTS.md invariant 4).

## 3. How to tell a correlated reference

`Column(table, name)` with `table ∈ OUT` is a read of the left row. With `table ∈ OWN` it is a read of the inner
source. Names are irrelevant; only the qualifier counts. Test 4 is exactly this: `_tds1.ref` and `_tds0.ref` share a
name, the lowering qualifies `$x.ref` with the left alias (`leftCols` → `sourceColumn`, Lowerer.java:1130-1133), and
the pass keys on `_tds0 ∈ OUT`. This is sound only if no alias in OUT is redefined inside S, which is why condition 3
checks `DEF ∩ OUT = ∅` rather than trusting the counters.

An unqualified column (`table == null`):
- in a moved conjunct: refuse, because it cannot be respelled.
- in S's body or a local conjunct: allowed. Its scope inside S is unchanged. If it had bound to the left row, H2 now
  fails loudly ("column not found"), which is not a silent change.

## 4. Where it goes; other dialects

**Order.** In `H2.passes()` (`core/src/main/java/com/legend/sql/dialect/H2.java:142-171`), insert it right after
`ps.add(new LateralExplodeToUnion());` (:151), before `AggregatesDeliverDouble`, SelectMerge and the other merges.
- The passes do not fight. `ConstantLateralsAsRows.rowsOf` needs `u.where() == null` (or constant Dual-only branches)
  (ConstantLateralsAsRows.java:105-114), and `LateralExplodeToUnion` needs `u.where() == null` and an UNNEST
  projection (LateralExplodeToUnion.java:36-49). This pass needs a WHERE with a correlated conjunct and plain-column
  projections.
- Every join the other two rewrite is already INNER or LEFT, not a lateral, so running this pass after them means it
  only sees what they left.
- It runs before SelectMerge, ProjectionWrapsFold and SelectLayers, so they see an ordinary joined derived table,
  which they already handle (inferred; the PCT run confirms it).
- Update the javadoc at H2.java:140-141 and the capability note at :26.

**Dialects without LATERAL**, and what each does today:

| dialect | today | the four tests |
|---|---|---|
| H2 | refused by the database | fail (pinned) |
| SQLite | in the renderer: `json_each((SELECT json_group_array(json_object(...)) FROM (S) AS __l))` (SQLite.java:539-597) | pass |
| MariaDB | in the renderer: `JSON_TABLE` over `JSON_ARRAYAGG` (MariaDb.java:766-830); MySQL has LATERAL (`correlatesDerived`, :785-786) | pass |
| BigQuery | in the renderer: `UNNEST(ARRAY(SELECT AS STRUCT * FROM (S)))` (BigQuery.java:580-625) | pass |
| ClickHouse | in the renderer: this decorrelation (ClickHouse.java:342-390), any other lateral refused (:415-418) | pass |
| MongoDB | `$lookup` (MongoDbPipeline.java:605-619) | n/a |

The lean SQL baseline for this family (`core/src/test/resources/lean-sql/baseline.tsv`, the
`lateral_Relation_1__Function_1__Relation_1_` rows): SQLite 1017 chars, MariaDB 998, BigQuery 832, ClickHouse 833,
DuckDB 353. My recommendation:
- **H2 only now** (the ruled item).
- Follow-ups for the user:
  - (a) Give the pass to SQLite, MariaDB and BigQuery for leaner SQL. Their answers already pass, so each needs its
    lane round, and the baseline only goes down.
  - (b) Move ClickHouse's render-time version onto this pass. SqlRewriter's javadoc says "rewrites belong here, never
    inside render methods" (SqlRewriter.java:13-16).

## 5. Sketch (`core/src/main/java/com/legend/sql/dialect/CorrelatedFiltersAsJoins.java`)

The signatures come from the records I read. I have not compiled it. Marked spots are unchecked.

```java
package com.legend.sql.dialect;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.util.*;

/** H2-family structural pass: a lateral whose subquery only filters its own source, correlated through its WHERE's
 *  top-level conjuncts alone, is that source joined on those conjuncts -- INNER for CROSS_LATERAL, LEFT for
 *  LEFT_LATERAL -- each inner column read through the subquery's output of it (H2 has no LATERAL: Function "LATERAL"
 *  not found). It rearranges a shape and computes nothing (the world-map tenet). Any other lateral is left as it is,
 *  refused where it is written. ClickHouse's renderer holds the same rule at render (ClickHouse.java:342). */
final class CorrelatedFiltersAsJoins extends SqlRewriter {

    @Override
    protected SqlSource source(SqlSource s) {
        if (!(s instanceof SqlSource.Join j)
                || (j.kind() != SqlSource.Join.Kind.CROSS_LATERAL && j.kind() != SqlSource.Join.Kind.LEFT_LATERAL)
                || !(j.right() instanceof SqlSource.Subselect sub) || !(sub.inner() instanceof SqlSelect in)
                || in.where() == null || !filterOnly(in)) {
            return s;
        }
        Set<String> out = new HashSet<>();
        topAliases(j.left(), out);
        Set<String> own = new HashSet<>();
        topAliases(in.from(), own);
        Reads all = Reads.of(in);                       // DEF: every alias defined inside S
        if (!Collections.disjoint(all.defined, out)) {
            return s;                                   // condition 3: an outer alias redefined inside
        }
        Reads body = Reads.of(in.withWhere(null));      // condition 4
        if (!Collections.disjoint(body.read, out) || !all.defined.containsAll(body.read)) {
            return s;
        }
        Map<String, String> outputOf = outputs(in, own);
        if (outputOf == null) {
            return s;                                   // condition 2
        }
        List<SqlExpr> local = new ArrayList<>();
        List<SqlExpr> moved = new ArrayList<>();
        for (SqlExpr c : conjuncts(in.where())) {
            Reads r = Reads.of(c);
            if (Collections.disjoint(r.read, out)) {
                if (!all.defined.containsAll(r.read)) {
                    return s;                           // reads a scope further out
                }
                local.add(c);
                continue;
            }
            Set<String> allowed = new HashSet<>(out);
            allowed.addAll(own);
            if (!r.subqueries.isEmpty() || !r.unqualified.isEmpty() || !allowed.containsAll(r.read)) {
                return s;                               // condition 5
            }
            Respell rs = new Respell(own, outputOf, sub.alias(), in.outputs());
            SqlExpr x = rs.of(c);
            if (!rs.missing.isEmpty()) {
                return s;                               // condition 6: an inner column not output
            }
            moved.add(x);
        }
        if (moved.isEmpty()) {
            return s;
        }
        if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
            moved.add(j.on());
        }
        SqlSelect filtered = in.withWhere(and(local));
        return new SqlSource.Join(j.left(), new SqlSource.Subselect(filtered, sub.alias(), sub.frameName()),
                j.kind() == SqlSource.Join.Kind.CROSS_LATERAL ? SqlSource.Join.Kind.INNER : SqlSource.Join.Kind.LEFT,
                java.util.Objects.requireNonNull(and(moved)));
    }

    private static boolean filterOnly(SqlSelect s) {
        return !s.distinct() && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null
                && s.orderBy().isEmpty() && s.limit() == null && s.offset() == null;
    }

    /** Each inner column's output name, "alias.column" -> name; null where a projection is not a plain inner column
     *  or a star, or a star's source declares no columns. */
    private static @com.legend.base.Nullable Map<String, String> outputs(SqlSelect in, Set<String> own) {
        Map<String, String> m = new HashMap<>();
        if (in.projections().isEmpty()) {
            return stars(in.from(), null, m) && distinctNames(in.outputs()) ? m : null;
        }
        for (SqlSelect.Projection p : in.projections()) {
            if (p.expr() instanceof SqlExpr.Star st && (st.table() == null || own.contains(st.table()))) {
                if (!stars(in.from(), st.table(), m)) {
                    return null;
                }
            } else if (p.expr() instanceof SqlExpr.Column c && c.table() != null && own.contains(c.table())
                    && p.outputName() != null) {
                m.putIfAbsent(c.table() + "." + c.name(), p.outputName());
            } else {
                return null;
            }
        }
        return distinctNames(in.outputs()) ? m : null;
    }

    /** The leaves of {@code src}'s join tree (those {@code table} names, or all), each column under its own name. */
    private static boolean stars(SqlSource src, @com.legend.base.Nullable String table, Map<String, String> into) {
        if (src instanceof SqlSource.Join j) {
            return stars(j.left(), table, into) && stars(j.right(), table, into);
        }
        if (src instanceof SqlSource.Dual || (table != null && !table.equals(src.alias()))) {
            return true;
        }
        if (src.outputs().isEmpty()) {
            return false;                               // UNCHECKED: a Table read may carry no outputs
        }
        src.outputs().forEach(oc -> into.putIfAbsent(src.alias() + "." + oc.name(), oc.name()));
        return true;
    }

    private static boolean distinctNames(List<OutputCol> outs) {
        return outs.stream().map(OutputCol::name).distinct().count() == outs.size();
    }

    private static void topAliases(SqlSource src, Set<String> into) {
        if (src instanceof SqlSource.Join j) {
            topAliases(j.left(), into);
            topAliases(j.right(), into);
        } else if (!(src instanceof SqlSource.Dual)) {
            into.add(src.alias());
        }
    }

    private static List<SqlExpr> conjuncts(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            List<SqlExpr> out = new ArrayList<>();
            c.args().forEach(x -> out.addAll(conjuncts(x)));
            return out;
        }
        return List.of(e);
    }

    private static @com.legend.base.Nullable SqlExpr and(List<SqlExpr> parts) {
        return parts.isEmpty() ? null : parts.size() == 1 ? parts.get(0) : new SqlExpr.Call(SqlFn.AND, parts);
    }

    /** What a query or an expression reads and defines: aliases of every source inside it, the tables its columns
     *  read, its unqualified columns, its subquery nodes. Final fields only (CodeShapeGuardrailTest's mutable-field
     *  rule). */
    private static final class Reads extends SqlRewriter {
        final Set<String> defined = new HashSet<>();
        final Set<String> read = new HashSet<>();
        final List<SqlExpr.Column> unqualified = new ArrayList<>();
        final List<SqlExpr> subqueries = new ArrayList<>();

        static Reads of(SqlQuery q) {
            Reads r = new Reads();
            r.rewrite(q);
            return r;
        }

        static Reads of(SqlExpr e) {
            Reads r = new Reads();
            r.rewriteExpr(e);
            return r;
        }

        @Override
        protected SqlSource source(SqlSource s) {
            if (!(s instanceof SqlSource.Join) && !(s instanceof SqlSource.Dual)) {
                defined.add(s.alias());
            }
            return s;
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c) {
                if (c.table() == null) {
                    unqualified.add(c);
                } else {
                    read.add(c.table());
                }
            } else if (e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists
                    || e instanceof SqlExpr.InSubquery || e instanceof SqlExpr.Quantified) {
                subqueries.add(e);
            }
            return e;                                   // UNCHECKED: Star(table) reads are projections, seen by outputs()
        }
    }

    /** An inner column read as the subquery's output of it, typed by that output. */
    private static final class Respell extends SqlRewriter {
        private final Set<String> own;
        private final Map<String, String> outputOf;
        private final String alias;
        private final List<OutputCol> outs;
        final List<SqlExpr.Column> missing = new ArrayList<>();

        Respell(Set<String> own, Map<String, String> outputOf, String alias, List<OutputCol> outs) {
            this.own = own; this.outputOf = outputOf; this.alias = alias; this.outs = outs;
        }

        SqlExpr of(SqlExpr e) {
            return rewriteExpr(e);
        }

        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Column c && c.table() != null && own.contains(c.table())) {
                String name = outputOf.get(c.table() + "." + c.name());
                if (name == null) {
                    missing.add(c);
                    return e;
                }
                return SqlExpr.Column.of(alias, outs, name);   // stamps the output's type and nullability
            }
            return e;
        }
    }
}
```

The SqlRewriter facts these rely on: `source(SqlSource)` is a post-order hook that fires for every source at every
depth, a join after its sides (SqlRewriter.java:96-140). So a chain is rewritten inner join first, and the outer join
sees an already-rewritten left. `rewriteExpr` descends into Exists, InSubquery, Quantified and StoredRead
(:155-250), so `Reads` sees nested subqueries' aliases and columns. `ScalarSubquery` descent was past the lines I read
(inferred).

H2.java:151, the new line:
`ps.add(new CorrelatedFiltersAsJoins());   // a lateral that only filters its source by the row before it: a join (H2 has no LATERAL)`

**Unit tests.** There are none for ConstantLateralsAsRows or LateralExplodeToUnion. Model the new ones on
`core/src/test/java/com/legend/sql/dialect/SelectMergeAliasTest.java`: build the MIR by hand, take the pass from
`new H2().passes()` (protected, same package), call `rewrite`, and assert on the tree.
`CorrelatedFiltersAsJoinsTest`:
- `aFilterCorrelatedToTheRowBeforeIsAnInnerJoinOnIt` (test 1's tree: INNER, ON `t0.department = _tds0.departmentId`,
  inner WHERE gone, `Join.outputs` equal to the lateral's)
- `theRowBeforeMayStandOnEitherSide` (test 2)
- `aChainOfLateralsIsAChainOfJoins` (test 3: the second ON reads `_tds0` through the first join)
- `anInnerColumnIsReadByItsProjectedName` (test 4, plus a renamed projection `_tds1.itemId AS id` → ON `t0.id`)
- `aNameBothSidesHoldKeepsItsSide` (test 4's `ref`)
- `aLeftLateralIsALeftJoinOnItsFilterAndItsOwnCondition`
- `localConjunctsStayInside`
- The refusals, each asserting the identical tree comes back: `aSortWithALimitStaysLateral`,
  `anAggregateStaysLateral`, `aDistinctStaysLateral`, `aWindowStaysLateral`, `aProjectionReadingTheRowBeforeStaysLateral`,
  `aCorrelatedSubqueryInTheFilterStaysLateral`, `aFilterOnAColumnNotOutputStaysLateral`, `aReadFurtherOutStaysLateral`,
  `anAliasDefinedOnBothSidesStaysLateral`, `anUnnestLateralIsLeftToItsPasses`
- `h2HoldsThePassAfterTheLateralExplosions` (the order in `passes()`)
- End to end: `H2CorrelatedLateralTest`, modeled on `H2FirstInGroupTest` (same directory: in-memory H2 plus
  `com.legend.testing.Lean.run(model, query, runtime, connection)`, Lean.java:47). It runs the four bodies over
  `#>{local::DB.t}#` tables and asserts the rows.

## 6. Ledgers and guards that move

- `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_RelationFunctions_PCT.java:70-74`: delete the
  comment and the four `one(...)` pins. Line 66 then becomes the last element, so drop its trailing comma.
- `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv:115-118`: delete the four rows.
- Edge goldens, regenerated by the edge net (pct/BUILD.bazel:943-955):
  - `edge/h2_relation_answers.tsv:117-120`: ERROR becomes RELATION. Compare with `sqlite_relation_answers.tsv:117-120`
    and `duckdb_...:117-120`, which should match except perhaps `relationalType`.
  - `edge/channel_b_h2_relation_outcomes.tsv:246-249`
  - `edge/h2_relation_outcomes.tsv:115-118`: SUCCESSFUL today, presumably the pin matched. Read the regenerated line;
    I have not verified it.
- `core/src/test/resources/lean-sql/baseline.tsv`, the `channel_a H2 ...lateral_Relation_1__Function_1__Relation_1_`
  row (now `0 0 0 0 0 1`): statements and chars rise from 0 as refused queries start answering. Regenerate with
  `bazel run //core:update_lean_sql_baseline` and put the date in the commit. `LeanSqlBaselineTest.java:107` says a
  refusal counts so it never reads as a shrink; check how drift treats a refused query becoming an answered one.
- Guards:
  - `core/src/test/java/com/legend/SqlFnSortedTest.java:195`: the `AND` entry of `BOTH_WRITERS` lists product
    classes that name `SqlFn.AND`, renderers aside (ClickHouse is absent), and "a comparison reads as a write". Add
    `com.legend.sql.dialect.CorrelatedFiltersAsJoins` with a dated comment. Do not dodge the guard.
  - `CodeShapeGuardrailTest`: the 250-line method cap (:37) and mutable fields (:47, final fields only), both kept in
    the sketch.
  - `TestLaneOrderGuardrailTest:66`: no RowOrder is minted, so it is not tripped.
  - `DialectLeakRatchetTest`: the dialect package is an owner, so not tripped.
  - `EdgeOnlyDataTest`: an existing variant, so not tripped.
  - `JavaEvalLedgerTest`: dialect passes are not registered there (none of SelectMerge or the others are), so not
    tripped (inferred from grep).
  - No BUILD edit: the srcs are globbed (core/BUILD.bazel:46, :55).
- Records: pct-sort.md D3/D5 item 3; `docs/PCT_PIN_WORKAROUNDS_2026_10_12.md` mentions the four.
- No SEMANTICS_REGISTER row: lite answers what Pure does. legend-engine's own H2 fails the four (pct-sort.md:386), so
  this is a fidelity question, not a meaning one. Flag it to the user if a row is wanted.

## 7. Risks

- **Other H2 laterals.** The two existing passes need `where == null`. The shapes that reach H2 without a WHERE are
  unaffected: InstanceProjection's LEFT_LATERAL anchors and UNNESTs (InstanceProjection.java:157-195),
  Lowerer.java:2156-2162's `Fold.lateralElem` and the flatten laterals. CheckedEnvelope's laterals
  (CheckedEnvelope.java:43-51) carry computed or aggregate projections, which condition 2 refuses.
  `core/src/test/resources/lanes/h2.txt:115` (`testProjectStructArrayWithEmptyArray`, "Should use LEFT JOIN LATERAL")
  asserts a text shape on an UNNEST lateral with no WHERE, so it is unaffected (inferred).
- **Nothing passing can change.** A lateral that survives to H2's renderer fails today. grep found no core or corpus
  expectation of `"LATERAL" not found` beyond the four pins (spec/ holds no LATERAL text). sql-parity's `lateral.yaml`
  runs on DuckDB and Postgres, not H2 (core/BUILD.bazel:2136-2145).
- **Later passes.** SelectMerge, ProjectionWrapsFold and SelectLayers now see `(SELECT * FROM (VALUES ...) _tds1) t0`
  as an INNER-join derived table. They handle that shape elsewhere, but it is not proven for this tree. The PCT run on
  H2 decides it.
- **Wrong answers.** The only way to a wrong answer is a condition that is too loose, mainly aggregates and windows.
  The refusal tests in §5 pin each one.
