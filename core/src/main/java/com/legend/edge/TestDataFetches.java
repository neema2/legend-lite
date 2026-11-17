// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.lineage.ScanRelations;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.sql.NameQuoting;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import com.legend.testdatagen.TestDataGenerator;
import com.legend.testdatagen.TestDataGenerator.Located;

/**
 * legend-engine's planTestDataGeneration SHAPE, AS DATA (moved from the product's testdatagen.TestDataGenerator, J6 of
 * the edge audit, 2026-11-17): the generator plan's fetch tree -- a fetch per table of the query's relation tree, its
 * result named {@code res_c0}, {@code res_c0_c1}, ..., a child read through its parent's VarSetPlaceHolder, a root with
 * no row identifiers legend-engine's error node over a {@code top 5} sample -- and the view walk it needs
 * (generateRelationColumnMap's views). The edge's text of it is {@link TestDataPlanText}; the product's test-data
 * generator plans its own fetches ({@code testdatagen.TestDataPlanner}) and names none of these.
 */
public final class TestDataFetches {

    private TestDataFetches() {
    }

    static void collectColMap(ModelContext ctx,
            ScanRelations.Rel rel, Map<String, List<String>> colMap,
            @com.legend.base.Nullable String parentDb) {
        rel = expandIfView(ctx, rel, parentDb);
        TestDataGenerator.mergeCols(ctx, rel, colMap);
        for (ScanRelations.Rel child : rel.children()) {
            collectColMap(ctx, child, colMap, rel.db());
        }
    }

    /** A VIEW-backed relation swaps in its seed-table expansion (engine
     * generateTestDataForNestedViewTree): the seed fetches with the
     * view's internal join web as children; the view node's own join
     * condition and its class-join children translate view-side refs to
     * base columns. {@code parentDb} resolves the connecting join when
     * the view is a CHILD. */
    static ScanRelations.Rel expandIfView(ModelContext ctx,
            ScanRelations.Rel r, @com.legend.base.Nullable String parentDb) {
        if (!ScanRelations.isView(ctx, r.db(), r.table())) {
            return r;
        }
        String vt = java.util.Objects.requireNonNull(r.table(), "r.table()");
        ScanRelations.ViewExpansion ve =
                ScanRelations.viewExpansion(ctx, r.db(), r.table());
        ScanRelations.Rel base = ve.tree();
        RelationalOperation cond = null;
        if (r.cond() != null || r.joinName() != null) {
            RelationalOperation op = r.cond() != null ? r.cond()
                    : TestDataGenerator.findJoin(ctx, r.joinName(), r.db(), parentDb)
                            .operation();
            cond = substituteViewRefs(op, vt, java.util.Objects.requireNonNull(
                    ve.mainTable()), ve.colToBase());
        }
        List<ScanRelations.Rel> kids = new ArrayList<>(base.children());
        for (ScanRelations.Rel c : r.children()) {
            RelationalOperation op = c.cond() != null ? c.cond()
                    : TestDataGenerator.findJoin(ctx, c.joinName(), c.db(), r.db())
                            .operation();
            kids.add(new ScanRelations.Rel(c.db(), c.table(), c.joinName(),
                    substituteViewRefs(op, vt, java.util.Objects.requireNonNull(
                            ve.mainTable()), ve.colToBase()),
                    c.cols(), c.children()));
        }
        return new ScanRelations.Rel(base.db(), base.table(), r.joinName(),
                cond, base.cols(), kids);
    }

    /** Rewrite {@code op}'s refs to {@code viewName} as refs to the
     * view's SEED table with the column translated through the plain
     * column map (a view-side join condition against the fetch temp). */
    static RelationalOperation substituteViewRefs(
            RelationalOperation op, String viewName, String mainTable,
            Map<String, String> colToBase) {
        return switch (op) {
            case RelationalOperation.ColumnRef r -> {
                if (!viewName.equals(r.table())) {
                    yield r;
                }
                String base = colToBase.get(r.column());
                if (base == null) {
                    throw new NotImplementedException("testDataGen: view"
                            + " join reads '" + r.column() + "' of '"
                            + viewName + "' which is not a plain"
                            + " column-mapped view column");
                }
                yield new RelationalOperation.ColumnRef(r.databaseName(),
                        mainTable, base);
            }
            case RelationalOperation.Comparison c ->
                    new RelationalOperation.Comparison(
                            substituteViewRefs(c.left(), viewName,
                                    mainTable, colToBase),
                            c.op(),
                            substituteViewRefs(c.right(), viewName,
                                    mainTable, colToBase));
            case RelationalOperation.BooleanOp b ->
                    new RelationalOperation.BooleanOp(
                            substituteViewRefs(b.left(), viewName,
                                    mainTable, colToBase),
                            b.op(),
                            substituteViewRefs(b.right(), viewName,
                                    mainTable, colToBase));
            case RelationalOperation.Group g ->
                    new RelationalOperation.Group(substituteViewRefs(
                            g.inner(), viewName, mainTable, colToBase));
            case RelationalOperation.IsNull n ->
                    new RelationalOperation.IsNull(substituteViewRefs(
                            n.operand(), viewName, mainTable, colToBase));
            case RelationalOperation.IsNotNull n ->
                    new RelationalOperation.IsNotNull(substituteViewRefs(
                            n.operand(), viewName, mainTable, colToBase));
            default -> op.mapChildren(x -> substituteViewRefs(x, viewName,
                    mainTable, colToBase));
        };
    }

    // ===== the tdg PLAN's FETCHES (planTestDataGeneration), as data =====

    /** A column of a generator plan's fetch: its name as demanded and its declared relational type. */
    public record FetchColumn(String name, com.legend.model.RelationalDataType type) {
    }

    /** A node of the generator's plan (legend-engine's planTestDataGeneration), as DATA: the fetches the generator
     *  builds and their SQL trees; legend-engine's text of them is the edge's ({@code edge.TestDataPlanText}, Phase
     *  6's B13, 2026-11-17: the generator names no engine text). */
    public sealed interface FetchNode permits Fetch, Unidentified {
    }

    /** A fetch: its result name ({@code res_c0}, {@code res_c0_c1}, ... in tree order), its table, its demanded
     *  columns, its query (the root's by its identifiers, a child's by its parent's join) and its children's. */
    public record Fetch(String res, String db, String schema, String table, List<FetchColumn> columns,
            SqlSelect query, List<FetchNode> children) implements FetchNode {
    }

    /** A root with no row identifiers: legend-engine plans an Error node over a sample of the root's primary key
     *  columns ({@code select top 5}, testDataGeneration.pure:511, getRowIdentifiersForRoot's assert). */
    public record Unidentified(String db, String schema, String table, List<FetchColumn> keys, SqlSelect sample)
            implements FetchNode {
    }

    /** The generator plan's root fetches for {@code resolvedQuery} over {@code mappingFqn}, from {@code rowIds} and
     *  live on {@code dates}: each a {@link Fetch} with its children, or an {@link Unidentified} root. Pure data, no
     *  execution. */
    public static List<FetchNode> planFetches(ModelContext ctx, LambdaFunction resolvedQuery, String mappingFqn,
            List<TestDataGenerator.TableRowIds> rowIds, @com.legend.base.Nullable TestDataGenerator.MilestoningDates dates) {
        List<ScanRelations.Rel> roots =
                ScanRelations.relTree(ctx, resolvedQuery, mappingFqn);
        roots = roots.stream().map(r -> expandIfView(ctx, r, null))
                .toList();
        Map<String, List<String>> colMap = new LinkedHashMap<>();
        for (ScanRelations.Rel r : roots) {
            collectColMap(ctx, r, colMap, null);
        }
        List<FetchNode> out = new ArrayList<>();
        int ri = 0;
        for (ScanRelations.Rel r : roots) {
            out.add(planNode(ctx, r, null, "res_c" + ri++, rowIds, dates, colMap));
        }
        return out;
    }

    private static FetchNode planNode(ModelContext ctx, ScanRelations.Rel rel,
            ScanRelations.@com.legend.base.Nullable Rel parent, String res,
            List<TestDataGenerator.TableRowIds> rowIds,
            @com.legend.base.Nullable TestDataGenerator.MilestoningDates dates, Map<String, List<String>> colMap) {
        Located loc = TestDataGenerator.locate(ctx, rel.db(), rel.table());
        String tbl = java.util.Objects.requireNonNull(rel.table(), "rel.table()");
        List<String> cols = java.util.Objects.requireNonNull(
                colMap.get(loc.schema() + "\n" + tbl),
                "no column map for " + tbl);
        if (parent == null && rootIds(loc, rel, rowIds) == null) {
            // the sample selects the PRIMARY KEY columns only
            List<String> pkCols = loc.def().columns().stream()
                    .filter(c -> c.primaryKey()).map(c -> c.name()).toList();
            String alias = tbl.toLowerCase(java.util.Locale.ROOT) + "_0";
            return new Unidentified(rel.db(), loc.schema(), tbl, columns(loc, pkCols),
                    sample(5, pkCols, alias, loc, tbl));
        }
        SqlSelect query = parent == null ? planRootFetch(loc, rel, cols, rowIds, dates)
                : planChildFetch(ctx, loc, parent, rel, cols, res, dates);
        List<FetchNode> children = new ArrayList<>();
        int ci = 0;
        for (ScanRelations.Rel child : rel.children()) {
            children.add(planNode(ctx, child, rel, res + "_c" + ci++, rowIds, dates, colMap));
        }
        return new Fetch(res, rel.db(), loc.schema(), tbl, columns(loc, cols), query, children);
    }

    private static List<FetchColumn> columns(Located loc, List<String> names) {
        return names.stream().map(c -> new FetchColumn(c, TestDataGenerator.column(loc.def(), c).dataType())).toList();
    }

    private static @com.legend.base.Nullable TestDataGenerator.TableRowIds rootIds(Located loc,
            ScanRelations.Rel rel, List<TestDataGenerator.TableRowIds> rowIds) {
        TestDataGenerator.TableRowIds ids = null;
        for (TestDataGenerator.TableRowIds t : rowIds) {
            if (t.table().equals(rel.table())
                    && t.schema().equals(loc.schema())) {
                ids = t;
            }
        }
        return ids;
    }

    /** The engine's root fetch in its plan: the demanded columns of the root, under {@code "root"}, the rows of its
     *  identifiers (the engine's combineFilters reverses its operand list, pureToSqlQuery.pure:389), live on the
     *  milestoning dates, the first 20. */
    private static SqlSelect planRootFetch(Located loc, ScanRelations.Rel rel, List<String> cols,
            List<TestDataGenerator.TableRowIds> rowIds, @com.legend.base.Nullable TestDataGenerator.MilestoningDates dates) {
        TestDataGenerator.TableRowIds ids = rootIds(loc, rel, rowIds);
        if (ids == null) {
            throw new NotImplementedException("testDataGen plan: no row"
                    + " identifiers for root '" + rel.table() + "'");
        }
        List<SqlExpr> rows = new ArrayList<>();
        for (int i = ids.ids().size() - 1; i >= 0; i--) {
            TestDataGenerator.RowId id = ids.ids().get(i);
            SqlExpr row = null;
            for (int c = 0; c < id.cols().size(); c++) {
                row = TestDataGenerator.and(row, SqlExpr.Call.of(SqlFn.EQUAL, TestDataGenerator.stored(TestDataGenerator.ROOT, loc.def(), id.cols().get(c)),
                        planLiteral(id.values().get(c))));
            }
            row = java.util.Objects.requireNonNull(row, "a row identifier with no column");
            rows.add(id.cols().size() == 1 ? row : new SqlExpr.Group(row));
        }
        SqlExpr pk = rows.get(0);
        for (int i = 1; i < rows.size(); i++) {
            pk = SqlExpr.Call.of(SqlFn.OR, pk, rows.get(i));
        }
        if (rows.size() > 1) {
            pk = new SqlExpr.Group(pk);
        }
        SqlExpr mf = planMilestoneFilter(loc.def(), TestDataGenerator.ROOT, dates);
        return sample(20, cols, TestDataGenerator.ROOT, loc, java.util.Objects.requireNonNull(rel.table(), "rel.table()"))
                .withWhere(mf == null ? pk : SqlExpr.Call.of(SqlFn.AND, mf, pk));
    }

    /** The engine's sampled select: {@code select top N "alias".c as "c", ... from <table> as "alias"} (the plan's
     *  root fetch and the no-seed error node's sample). */
    private static SqlSelect sample(int top, List<String> cols, String alias, Located loc, String table) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (String c : cols) {
            ps.add(new SqlSelect.Projection(TestDataGenerator.stored(alias, loc.def(), c), c, null));
        }
        return new SqlSelect(ps, false, TestDataGenerator.storeTable(loc, table, alias), null, List.of(), null, null, List.of(),
                (long) top, null, List.of());
    }

    /** The engine's child fetch in its plan: the child table joined to its parent's result, the allocation
     *  {@code ${res}} read as {@code "root"}, the first 20 (each child fetch joins one table, so its alias is
     *  {@code <table>_0}). */
    private static SqlSelect planChildFetch(ModelContext ctx, Located loc, ScanRelations.Rel parent,
            ScanRelations.Rel child, List<String> cols, String res, @com.legend.base.Nullable TestDataGenerator.MilestoningDates dates) {
        String parentRes = res.substring(0, res.lastIndexOf("_c"));
        String ct = java.util.Objects.requireNonNull(child.table(), "child.table()");
        String alias = ct.toLowerCase(java.util.Locale.ROOT) + "_0";
        RelationalOperation op = child.cond() != null ? child.cond()
                : TestDataGenerator.findJoin(ctx, child.joinName(), child.db(), parent.db()).operation();
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (String c : cols) {
            ps.add(new SqlSelect.Projection(TestDataGenerator.stored(alias, loc.def(), c), c, null));
        }
        SqlSource parentRows = new SqlSource.Subselect(SqlSelect.starOf(new SqlSource.VarSetPlaceholder(parentRes,
                TestDataGenerator.ROOT, List.of())), TestDataGenerator.ROOT, null);
        return new SqlSelect(ps, false, new SqlSource.Join(parentRows, TestDataGenerator.storeTable(loc, ct, alias),
                SqlSource.Join.Kind.INNER, planCondition(op, parent.table(), ct, alias, loc.def())),
                planMilestoneFilter(loc.def(), alias, dates), List.of(), null, null, List.of(), 20L, null, List.of());
    }

    /** The engine's join condition in its plan: a parent column reads the allocation's result columns
     *  ({@code "root"."col"}), a child column the joined table. */
    private static SqlExpr planCondition(RelationalOperation op, @com.legend.base.Nullable String parentTable,
            String childTable, String childAlias, DatabaseDefinition.TableDefinition childDef) {
        return switch (op) {
            // the allocation's result columns are its select's quoted aliases ("root"."type")
            case RelationalOperation.ColumnRef r -> bareTable(r.table()).equals(parentTable)
                    ? SqlExpr.Column.physical(TestDataGenerator.ROOT, r.column(), true)
                    : TestDataGenerator.stored(childAlias, childDef, r.column());
            case RelationalOperation.Comparison c -> SqlExpr.Call.of(switch (c.op()) {
                case EQ -> SqlFn.EQUAL;
                case NEQ -> SqlFn.NOT_EQUAL;
                case LT -> SqlFn.LESS;
                case LTE -> SqlFn.LESS_EQUAL;
                case GT -> SqlFn.GREATER;
                case GTE -> SqlFn.GREATER_EQUAL;
            }, planCondition(c.left(), parentTable, childTable, childAlias, childDef),
                    planCondition(c.right(), parentTable, childTable, childAlias, childDef));
            case RelationalOperation.BooleanOp b -> SqlExpr.Call.of(
                    b.op() == com.legend.model.LogicalOp.AND ? SqlFn.AND : SqlFn.OR,
                    planCondition(b.left(), parentTable, childTable, childAlias, childDef),
                    planCondition(b.right(), parentTable, childTable, childAlias, childDef));
            case RelationalOperation.Group g -> new SqlExpr.Group(
                    planCondition(g.inner(), parentTable, childTable, childAlias, childDef));
            case RelationalOperation.IsNull n -> SqlExpr.Call.of(SqlFn.IS_NULL,
                    planCondition(n.operand(), parentTable, childTable, childAlias, childDef));
            case RelationalOperation.IsNotNull n -> SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                    planCondition(n.operand(), parentTable, childTable, childAlias, childDef));
            default -> throw new NotImplementedException(
                    "testDataGen plan: condition node "
                    + op.getClass().getSimpleName() + " pending");
        };
    }

    /** The engine's milestoning filter in its plan: the PROCESSING dimension first (its bitemporal order). */
    private static @com.legend.base.Nullable SqlExpr planMilestoneFilter(DatabaseDefinition.TableDefinition def,
            String alias, @com.legend.base.Nullable TestDataGenerator.MilestoningDates d) {
        var ms = def.milestoning();
        if (ms == null || d == null) {
            return null;
        }
        SqlExpr parts = null;
        if (ms.processing() != null) {
            var pr = ms.processing();
            if (pr.snapshotDate() != null) {
                parts = TestDataGenerator.and(parts, TestDataGenerator.onDate(alias, def, pr.snapshotDate(), SqlFn.EQUAL,
                        TestDataGenerator.requireDate(d.processing(), def, "processingDate")));
            } else {
                String pd = TestDataGenerator.requireDate(d.processing(), def, "processingDate");
                parts = TestDataGenerator.and(parts, TestDataGenerator.onDate(alias, def, java.util.Objects.requireNonNull(pr.in(),
                        "milestoning processing block without PROCESSING_IN"), SqlFn.LESS_EQUAL, pd));
                parts = TestDataGenerator.and(parts, TestDataGenerator.onDate(alias, def, java.util.Objects.requireNonNull(pr.out(),
                        "milestoning processing block without PROCESSING_OUT"),
                        pr.outIsInclusive() ? SqlFn.GREATER_EQUAL : SqlFn.GREATER, pd));
            }
        }
        if (ms.business() != null) {
            var b = ms.business();
            if (b.snapshotDate() != null) {
                parts = TestDataGenerator.and(parts, TestDataGenerator.onDate(alias, def, b.snapshotDate(), SqlFn.EQUAL,
                        TestDataGenerator.requireDate(d.snapshot(), def, "snapshotDate")));
            } else {
                String bd = TestDataGenerator.requireDate(d.business(), def, "businessDate");
                parts = TestDataGenerator.and(parts, TestDataGenerator.onDate(alias, def, java.util.Objects.requireNonNull(b.from(),
                        "milestoning business block without BUS_FROM"), SqlFn.LESS_EQUAL, bd));
                parts = TestDataGenerator.and(parts, TestDataGenerator.onDate(alias, def, java.util.Objects.requireNonNull(b.thru(),
                        "milestoning business block without BUS_THRU"),
                        b.thruIsInclusive() ? SqlFn.GREATER_EQUAL : SqlFn.GREATER, bd));
            }
        }
        return parts;
    }

    /** A row identifier's value in the plan: a string as a string, a whole number an integer, any other number its
     *  exact value, a boolean its truth. */
    private static SqlExpr planLiteral(Object v) {
        if (v instanceof String str) {
            return new SqlExpr.StringLit(str);
        }
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return new SqlExpr.IntLit(((Number) v).longValue());
        }
        if (v instanceof Number n) {
            return new SqlExpr.DecimalLit(new java.math.BigDecimal(n.toString()));
        }
        if (v instanceof Boolean b) {
            return new SqlExpr.BoolLit(b);
        }
        throw new NotImplementedException("testDataGen plan: row identifier value " + v.getClass());
    }

    private static String bareTable(String t) {
        int dot = t.lastIndexOf('.');
        return dot < 0 ? t : t.substring(dot + 1);
    }
}
