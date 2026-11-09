// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

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

/**
 * The engine's {@code meta::relational::testDataGeneration::generateTestData}
 * (#46): given a query, a mapping and seed row identifiers for the root
 * table, walk the query's {@link ScanRelations} relation tree and extract
 * the minimal supporting rows for every reached table, plus the fetch SQL
 * trail and a CSV dump ({@code schema\ntable\ncols\nrows...-----\n}).
 *
 * <p>Java ORCHESTRATES, the database EXECUTES (tenet #1): every node's
 * rows land in a session TEMP table ({@code CREATE TEMP TABLE ... AS
 * SELECT}, the engine's own temp-table discipline), child fetches JOIN
 * the parent's temp table in the database, and per-table dedup is a
 * DB-side {@code UNION}. Row values only cross into Java as display
 * strings for the CSV text. (The CSV compare, never called since
 * assertTestData folds through setUpDataSQLs, was deleted 2026-10-16.)
 *
 * <p>Every statement of a run is a plan statement the plan runner runs on the body's session (the doors,
 * 2026-10-29; until then this class ran JDBC statements of its own on the session's connection): the temps and
 * their drops as effects ({@link PlanRunner#effect}), the read-backs as typed rows of the columns the database
 * describes ({@link PlanRunner#describe}, {@link PlanRunner#rows}: each cell by its reported kind's slot), the CSV's
 * text columns as reported columns ({@link PlanRunner#columns}). The generator holds no JDBC.
 *
 * <p>Walls are LOUD ({@link NotImplementedException} naming the
 * pending shape) — but the previously-listed examples (view-backed
 * relations, hashStrings, temporal milestoning dates) are IMPLEMENTED
 * now; hashStrings and the CSV scrub are spelled IN SQL (A5/A6 — the
 * Java SHA-256 over rs.getString was the tenet's oldest open breach).
 * Only genuinely unhandled shapes throw today.
 */
public final class TestDataGenerator {

    private TestDataGenerator() {
    }

    /** One {@code createRowIdentifier([cols],[values])}. */
    public record RowId(List<String> cols, List<Object> values) {
    }

    /** One {@code createTableRowIdentifiers(db, schema, table, ids)}. */
    public record TableRowIds(String schema, String table,
            List<RowId> ids) {
    }

    /** The engine's TemporalMilestoningDates: forced dates that FILTER
     * every milestoned table's fetch. */
    public record MilestoningDates(@com.legend.base.Nullable String business,
            @com.legend.base.Nullable String processing,
            @com.legend.base.Nullable String snapshot) {
    }

    static void collectColMap(ModelContext ctx,
            ScanRelations.Rel rel, Map<String, List<String>> colMap,
            @com.legend.base.Nullable String parentDb) {
        rel = expandIfView(ctx, rel, parentDb);
        Located loc = locate(ctx, rel.db(), rel.table());
        String key = loc.schema() + "\n" + rel.table();
        // engine sortBy(name) is the plain pure string sort — ASCII
        // case-sensitive (bitemporal golden: ID < PLACE < from_z; the
        // earlier all-uppercase evidence fit either order)
        TreeSet<String> merged = new TreeSet<>();
        merged.addAll(colMap.getOrDefault(key, List.of()));
        for (String c : fetchCols(ctx, loc, rel)) {
            // demanded names arrive in MAPPING spelling — canonicalize
            // to the DDL case before the (case-sensitive) engine sort
            merged.add(loc.def().columns().stream()
                    .map(DatabaseDefinition.ColumnDefinition::name)
                    .filter(n -> n.equalsIgnoreCase(c))
                    .findFirst().orElse(c));
        }
        colMap.put(key, List.copyOf(merged));
        for (ScanRelations.Rel child : rel.children()) {
            collectColMap(ctx, child, colMap, rel.db());
        }
    }

    record Located(String schema,
            DatabaseDefinition.TableDefinition def) {
    }

    /** {@code alias.column <op> DATE 'date'}. */
    static SqlExpr onDate(String alias, DatabaseDefinition.TableDefinition def, String column, SqlFn op,
            String date) {
        return SqlExpr.Call.of(op, stored(alias, def, column), new SqlExpr.DateLit(date));
    }

    static String requireDate(@com.legend.base.Nullable String v,
            DatabaseDefinition.TableDefinition def, String name) {
        if (v == null) {
            throw new NotImplementedException("testDataGen: table '"
                    + def.name() + "' is milestoned but '" + name
                    + "' was not passed in TemporalMilestoningDates");
        }
        return v;
    }

    // ===== column demand (engine generateRelationColumnMap) =====

    /** PK + non-nullable + milestoning + scanned + join-condition columns,
     * name-sorted (the engine sorts fetch columns by name). */
    static List<String> fetchCols(ModelContext ctx, Located loc,
            ScanRelations.Rel rel) {
        TreeSet<String> out = new TreeSet<>();
        java.util.Set<String> known = new LinkedHashSet<>();
        for (DatabaseDefinition.ColumnDefinition c : loc.def().columns()) {
            known.add(c.name());
            if (c.primaryKey() || c.notNull()) {
                out.add(c.name());
            }
        }
        var ms = loc.def().milestoning();
        if (ms != null) {
            if (ms.business() != null) {
                addIf(out, known, ms.business().from());
                addIf(out, known, ms.business().thru());
                addIf(out, known, ms.business().snapshotDate());
            }
            if (ms.processing() != null) {
                addIf(out, known, ms.processing().in());
                addIf(out, known, ms.processing().out());
                addIf(out, known, ms.processing().snapshotDate());
            }
        }
        for (String c : rel.cols()) {
            if (known.contains(c)) {
                out.add(c);
            }
        }
        // both sides of every child edge's join ride along (the child
        // fetch joins the parent TEMP table, so its condition columns
        // must have been fetched)
        for (ScanRelations.Rel child : rel.children()) {
            RelationalOperation op = child.cond() != null ? child.cond()
                    : findJoin(ctx, child.joinName(), child.db(),
                            rel.db()).operation();
            collectTableCols(op, rel.table(), out, known);
        }
        if (rel.joinName() != null || rel.cond() != null) {
            // this node's own inbound-join child-side columns
            RelationalOperation op = rel.cond() != null ? rel.cond()
                    : findJoin(ctx, rel.joinName(), rel.db(),
                            rel.db()).operation();
            collectTableCols(op, rel.table(), out, known);
        }
        return List.copyOf(out);
    }

    static void addIf(TreeSet<String> out,
            java.util.Set<String> known, @com.legend.base.Nullable String col) {
        if (col != null && known.contains(col)) {
            out.add(col);
        }
    }

    static void collectTableCols(RelationalOperation op,
            @com.legend.base.Nullable String table, TreeSet<String> out, java.util.Set<String> known) {
        switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                if (bare(cr.table()).equals(table) && known.contains(cr.column())) {
                    out.add(cr.column());
                }
            }
            case RelationalOperation.TargetColumnRef tr -> {
                if (known.contains(tr.column())) {
                    out.add(tr.column());
                }
            }
            case RelationalOperation.Comparison c -> {
                collectTableCols(c.left(), table, out, known);
                collectTableCols(c.right(), table, out, known);
            }
            case RelationalOperation.BooleanOp b -> {
                collectTableCols(b.left(), table, out, known);
                collectTableCols(b.right(), table, out, known);
            }
            case RelationalOperation.Group g ->
                    collectTableCols(g.inner(), table, out, known);
            case RelationalOperation.IsNull n ->
                    collectTableCols(n.operand(), table, out, known);
            case RelationalOperation.IsNotNull n ->
                    collectTableCols(n.operand(), table, out, known);
            case RelationalOperation.FunctionCall fc -> {
                for (RelationalOperation a : fc.args()) {
                    collectTableCols(a, table, out, known);
                }
            }
            default -> {
            }
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
                    : findJoin(ctx, r.joinName(), r.db(), parentDb)
                            .operation();
            cond = substituteViewRefs(op, vt, java.util.Objects.requireNonNull(
                    ve.mainTable()), ve.colToBase());
        }
        List<ScanRelations.Rel> kids = new ArrayList<>(base.children());
        for (ScanRelations.Rel c : r.children()) {
            RelationalOperation op = c.cond() != null ? c.cond()
                    : findJoin(ctx, c.joinName(), c.db(), r.db())
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

    // ===== lookups =====

    static Located locate(ModelContext ctx, String dbFqn,
            @com.legend.base.Nullable String table0) {
        String table = java.util.Objects.requireNonNull(table0,
                "testDataGen: relation without a physical table");
        ArrayDeque<String> work = new ArrayDeque<>();
        java.util.Set<String> seen = new LinkedHashSet<>();
        work.add(dbFqn);
        while (!work.isEmpty()) {
            String fqn = work.poll();
            if (!seen.add(fqn)) {
                continue;
            }
            var dbo = ctx.findDatabase(fqn);
            if (dbo.isEmpty()) {
                continue;
            }
            DatabaseDefinition db = dbo.get();
            // NAMED schemas first: the top-level table list may flatten
            // schema-owned tables, and the CSV contract spells the
            // OWNING schema (testQualifier's productSchema)
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                for (DatabaseDefinition.TableDefinition t : s.tables()) {
                    if (t.name().equals(table)) {
                        return new Located(s.name(), t);
                    }
                }
                for (DatabaseDefinition.ViewDefinition v : s.views()) {
                    if (v.name().equals(table)) {
                        throw new NotImplementedException("testDataGen:"
                                + " view-backed relation '" + table
                                + "' — view slice pending");
                    }
                }
            }
            for (DatabaseDefinition.TableDefinition t : db.tables()) {
                if (t.name().equals(table)) {
                    return new Located("default", t);
                }
            }
            for (DatabaseDefinition.ViewDefinition v : db.views()) {
                if (v.name().equals(table)) {
                    throw new NotImplementedException("testDataGen:"
                            + " view-backed relation '" + table
                            + "' — view slice pending");
                }
            }
            work.addAll(db.includes());
        }
        throw new NotImplementedException("testDataGen: table '" + table
                + "' not found in database '" + dbFqn + "'");
    }

    static DatabaseDefinition.JoinDefinition findJoin(
            ModelContext ctx, @com.legend.base.Nullable String name0,
            @com.legend.base.Nullable String... dbFqns) {
        String name = java.util.Objects.requireNonNull(name0,
                "testDataGen: join edge without a join name");
        ArrayDeque<String> work = new ArrayDeque<>();
        for (String db : dbFqns) {
            if (db != null) {
                work.add(db);
            }
        }
        java.util.Set<String> seen = new LinkedHashSet<>();
        while (!work.isEmpty()) {
            String fqn = work.poll();
            if (fqn == null || !seen.add(fqn)) {
                continue;
            }
            var dbo = ctx.findDatabase(fqn);
            if (dbo.isEmpty()) {
                continue;
            }
            for (DatabaseDefinition.JoinDefinition j : dbo.get().joins()) {
                if (j.name().equals(name)) {
                    return j;
                }
            }
            work.addAll(dbo.get().includes());
        }
        throw new NotImplementedException("testDataGen: join '" + name
                + "' not found in " + List.of(dbFqns));
    }

    static DatabaseDefinition.ColumnDefinition column(
            DatabaseDefinition.TableDefinition def, String name) {
        // quote-bearing demand names strip to the stored declaration
        if (name.length() > 1 && name.startsWith("\"")
                && name.endsWith("\"")) {
            name = name.substring(1, name.length() - 1);
        }
        for (DatabaseDefinition.ColumnDefinition c : def.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        // unquoted SQL identifiers are case-insensitive (H2 uppercases
        // them in engine goldens)
        for (DatabaseDefinition.ColumnDefinition c : def.columns()) {
            if (c.name().equalsIgnoreCase(name)) {
                return c;
            }
        }
        throw new NotImplementedException("testDataGen: column '" + name
                + "' not on table '" + def.name() + "'");
    }

    /** The engine's {@code toRepresentation()} (platform
     * toRepresentation.pure): row-identifier cells are PURE SOURCE, so
     * strings take BACKSLASH escapes, dates the {@code %} form,
     * decimals the {@code D} suffix. (Audit 2026-08-18 finding E: the
     * old inline speller had NO escaping and JDBC-default spellings;
     * the fix is this spec port, not the SQL {@code lit()} speller —
     * SQL doubles quotes, Pure backslash-escapes them.)
     * Package-private for the Tier-1 regression pin (PureReprTest). */
    static String pureRepr(@com.legend.base.Nullable Object v) {
        // Phase 2a: toRepresentation has ONE owner (PureAsserts.repr —
        // this port generalized there); only the row-identifier NULL
        // contract stays local (a pk cell must have produced a value)
        if (v == null) {
            throw new NotImplementedException(
                    "testDataGen: NULL row-identifier cell — a primary"
                    + " key produced no value");
        }
        // the cells arrive as the typed method's carriers (a temporal as PureDateLiteral, the one wire temporal:
        // the doors, 2026-10-29; until then this generator read its own result set and converted the driver's
        // temporals here), so the one repr owner spells them as it spells every judged value
        return com.legend.testexecutionplan.Verdicts.repr(v);
    }

    // ===== the tdg PLAN PRINTER (planTestDataGeneration text) =====

    /** The engine's planTestDataGeneration plan text: MultiResultSequence
     * of per-fetch Allocations (res_cN names in tree order), each a
     * Relational node whose SQL is the ENGINE-H2 spelling of the fetch —
     * parents referenced as {@code ${res_cN}} placeholders. Pure text,
     * no execution. */
    public static String planText(ModelContext ctx,
            LambdaFunction resolvedQuery, String mappingFqn,
            List<TableRowIds> rowIds, MilestoningDates dates,
            com.legend.sql.dialect.EngineTextDialect text, String connection) {
        List<ScanRelations.Rel> roots =
                ScanRelations.relTree(ctx, resolvedQuery, mappingFqn);
        roots = roots.stream().map(r -> expandIfView(ctx, r, null))
                .toList();
        Map<String, List<String>> colMap = new LinkedHashMap<>();
        for (ScanRelations.Rel r : roots) {
            collectColMap(ctx, r, colMap, null);
        }
        StringBuilder kids = new StringBuilder();
        int ri = 0;
        for (ScanRelations.Rel r : roots) {
            planNode(ctx, r, null, "res_c" + ri++, rowIds, dates, colMap,
                    kids, text, connection);
        }
        return "MultiResultSequence\n(\n"
                + "  type = meta::pure::metamodel::type::Any\n"
                + "  (\n"
                + com.legend.plan.PlanText.indent(kids.toString(), "    ")
                + "  )\n)\n";
    }

    private static void planNode(ModelContext ctx, ScanRelations.Rel rel,
            ScanRelations.@com.legend.base.Nullable Rel parent, String res,
            List<TableRowIds> rowIds,
            @com.legend.base.Nullable MilestoningDates dates, Map<String, List<String>> colMap,
            StringBuilder out, com.legend.sql.dialect.EngineTextDialect text, String connection) {
        Located loc = locate(ctx, rel.db(), rel.table());
        String tbl = java.util.Objects.requireNonNull(rel.table(), "rel.table()");
        List<String> cols = java.util.Objects.requireNonNull(
                colMap.get(loc.schema() + "\n" + tbl),
                "no column map for " + tbl);
        String relType = relationType(rel, loc, cols);
        if (parent == null && rootIds(loc, rel, rowIds) == null) {
            // the engine's plan for a ROOT without row identifiers is an
            // Error node carrying the top-5 sample query
            // (testDataGeneration.pure:511, planTestDataGeneration's
            // getRowIdentifiersForRoot assert): message + the sampled
            // Relational, no Allocation
            // the sample selects the PRIMARY KEY columns only
            List<String> pkCols = loc.def().columns().stream()
                    .filter(c -> c.primaryKey()).map(c -> c.name()).toList();
            out.append(planRootErrorNode(loc, rel, tbl, pkCols,
                    relationType(rel, loc, pkCols), text, connection));
            return;
        }
        // the fetch as the engine prints the query it built (its own aliases), from IR
        String sql = text.renderAsAliased(parent == null ? planRootFetch(loc, rel, cols, rowIds, dates)
                : planChildFetch(ctx, loc, parent, rel, cols, res, dates));
        String inner = "Relational\n(\n"
                + "  type = " + relType + "\n"
                + "  resultSizeRange = *\n"
                + "  resultColumns = [" + cols.stream()
                        .map(c -> "(\"" + c + "\", "
                                + com.legend.plan.PlanText.spell(
                                        column(loc.def(), c).dataType())
                                + ")")
                        .collect(java.util.stream.Collectors
                                .joining(", ")) + "]\n"
                + "  sql = " + sql + "\n"
                + "  connection = " + connection + "\n"
                + ")\n";
        out.append(com.legend.plan.PlanText.allocation(res,
                "  type = " + relType + "\n  resultSizeRange = *\n",
                inner));
        int ci = 0;
        for (ScanRelations.Rel child : rel.children()) {
            planNode(ctx, child, rel, res + "_c" + ci++, rowIds, dates,
                    colMap, out, text, connection);
        }
    }

    private static String relationType(ScanRelations.Rel rel, Located loc,
            List<String> cols) {
        return "Relation[name=" + rel.table() + ", type=TABLE, schema="
                + loc.schema() + ", database=" + rel.db() + ", columns=["
                + cols.stream().map(c -> "(\"" + c + "\","
                        + com.legend.plan.PlanText.spell(
                                column(loc.def(), c).dataType()) + ")")
                        .collect(java.util.stream.Collectors.joining(", "))
                + "]]";
    }

    private static @com.legend.base.Nullable TableRowIds rootIds(Located loc,
            ScanRelations.Rel rel, List<TableRowIds> rowIds) {
        TableRowIds ids = null;
        for (TableRowIds t : rowIds) {
            if (t.table().equals(rel.table())
                    && t.schema().equals(loc.schema())) {
                ids = t;
            }
        }
        return ids;
    }

    /** The engine's Error node for a root without row identifiers:
     * the assert message (primary key columns as name:TYPE) over the
     * sampled {@code select top 5} of the root table. */
    private static String planRootErrorNode(Located loc,
            ScanRelations.Rel rel, String tbl, List<String> cols,
            String relType, com.legend.sql.dialect.EngineTextDialect text, String connection) {
        String alias = tbl.toLowerCase(java.util.Locale.ROOT) + "_0";
        String pk = loc.def().columns().stream()
                .filter(c -> c.primaryKey())
                .map(c -> c.name() + ":" + com.legend.plan.PlanText.spell(
                        c.dataType()))
                .collect(java.util.stream.Collectors.joining(","));
        String qualified = "default".equals(loc.schema()) ? tbl
                : loc.schema() + "." + tbl;
        String sql = text.renderAsAliased(sample(5, cols, alias, loc, tbl));
        String inner = "Relational\n(\n"
                + "  type = " + relType + "\n"
                + "  resultSizeRange = *\n"
                + "  resultColumns = [" + cols.stream()
                        .map(c -> "(\"" + c + "\", "
                                + com.legend.plan.PlanText.spell(
                                        column(loc.def(), c).dataType())
                                + ")")
                        .collect(java.util.stream.Collectors
                                .joining(", ")) + "]\n"
                + "  sql = " + sql + "\n"
                + "  connection = " + connection + "\n"
                + ")\n";
        return "Error\n(\n"
                + "  type = meta::pure::metamodel::type::Any\n"
                + "  message = Row Identifers should be provided for the root"
                + " table: " + rel.db() + "." + qualified + " [" + pk
                + "]\\n\n"
                + "  (\n" + com.legend.plan.PlanText.indent(inner, "    ")
                + "  )\n)\n";
    }

    /** The engine's root fetch in its plan: the demanded columns of the root, under {@code "root"}, the rows of its
     *  identifiers (the engine's combineFilters reverses its operand list, pureToSqlQuery.pure:389), live on the
     *  milestoning dates, the first 20. */
    private static SqlSelect planRootFetch(Located loc, ScanRelations.Rel rel, List<String> cols,
            List<TableRowIds> rowIds, @com.legend.base.Nullable MilestoningDates dates) {
        TableRowIds ids = rootIds(loc, rel, rowIds);
        if (ids == null) {
            throw new NotImplementedException("testDataGen plan: no row"
                    + " identifiers for root '" + rel.table() + "'");
        }
        List<SqlExpr> rows = new ArrayList<>();
        for (int i = ids.ids().size() - 1; i >= 0; i--) {
            RowId id = ids.ids().get(i);
            SqlExpr row = null;
            for (int c = 0; c < id.cols().size(); c++) {
                row = and(row, SqlExpr.Call.of(SqlFn.EQUAL, stored(ROOT, loc.def(), id.cols().get(c)),
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
        SqlExpr mf = planMilestoneFilter(loc.def(), ROOT, dates);
        return sample(20, cols, ROOT, loc, java.util.Objects.requireNonNull(rel.table(), "rel.table()"))
                .withWhere(mf == null ? pk : SqlExpr.Call.of(SqlFn.AND, mf, pk));
    }

    /** The engine's sampled select: {@code select top N "alias".c as "c", ... from <table> as "alias"} (the plan's
     *  root fetch and the no-seed error node's sample). */
    private static SqlSelect sample(int top, List<String> cols, String alias, Located loc, String table) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (String c : cols) {
            ps.add(new SqlSelect.Projection(stored(alias, loc.def(), c), c, null));
        }
        return new SqlSelect(ps, false, storeTable(loc, table, alias), null, List.of(), null, null, List.of(),
                (long) top, null, List.of());
    }

    /** The engine's child fetch in its plan: the child table joined to its parent's result, the allocation
     *  {@code ${res}} read as {@code "root"}, the first 20 (each child fetch joins one table, so its alias is
     *  {@code <table>_0}). */
    private static SqlSelect planChildFetch(ModelContext ctx, Located loc, ScanRelations.Rel parent,
            ScanRelations.Rel child, List<String> cols, String res, @com.legend.base.Nullable MilestoningDates dates) {
        String parentRes = res.substring(0, res.lastIndexOf("_c"));
        String ct = java.util.Objects.requireNonNull(child.table(), "child.table()");
        String alias = ct.toLowerCase(java.util.Locale.ROOT) + "_0";
        RelationalOperation op = child.cond() != null ? child.cond()
                : findJoin(ctx, child.joinName(), child.db(), parent.db()).operation();
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (String c : cols) {
            ps.add(new SqlSelect.Projection(stored(alias, loc.def(), c), c, null));
        }
        SqlSource parentRows = new SqlSource.Subselect(SqlSelect.starOf(new SqlSource.VarSetPlaceholder(parentRes,
                ROOT, List.of())), ROOT, null);
        return new SqlSelect(ps, false, new SqlSource.Join(parentRows, storeTable(loc, ct, alias),
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
                    ? SqlExpr.Column.physical(ROOT, r.column(), true)
                    : stored(childAlias, childDef, r.column());
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
            String alias, @com.legend.base.Nullable MilestoningDates d) {
        var ms = def.milestoning();
        if (ms == null || d == null) {
            return null;
        }
        SqlExpr parts = null;
        if (ms.processing() != null) {
            var pr = ms.processing();
            if (pr.snapshotDate() != null) {
                parts = and(parts, onDate(alias, def, pr.snapshotDate(), SqlFn.EQUAL,
                        requireDate(d.processing(), def, "processingDate")));
            } else {
                String pd = requireDate(d.processing(), def, "processingDate");
                parts = and(parts, onDate(alias, def, java.util.Objects.requireNonNull(pr.in(),
                        "milestoning processing block without PROCESSING_IN"), SqlFn.LESS_EQUAL, pd));
                parts = and(parts, onDate(alias, def, java.util.Objects.requireNonNull(pr.out(),
                        "milestoning processing block without PROCESSING_OUT"),
                        pr.outIsInclusive() ? SqlFn.GREATER_EQUAL : SqlFn.GREATER, pd));
            }
        }
        if (ms.business() != null) {
            var b = ms.business();
            if (b.snapshotDate() != null) {
                parts = and(parts, onDate(alias, def, b.snapshotDate(), SqlFn.EQUAL,
                        requireDate(d.snapshot(), def, "snapshotDate")));
            } else {
                String bd = requireDate(d.business(), def, "businessDate");
                parts = and(parts, onDate(alias, def, java.util.Objects.requireNonNull(b.from(),
                        "milestoning business block without BUS_FROM"), SqlFn.LESS_EQUAL, bd));
                parts = and(parts, onDate(alias, def, java.util.Objects.requireNonNull(b.thru(),
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

    /** The alias a fetch reads its seed table through, and the one a child fetch reads its parent's temp
     *  through. */
    static final String ROOT = "root";

    static final String PARENT = "main";

    /** The alias the CSV envelope's row text reads its cells through. */
    static final String CSV_ROW = "_r";

    /** A column of the store table {@code def}, read through {@code alias}, spelled as its DDL declared it (bare, or
     *  quoted with its case kept). */
    static SqlExpr stored(@com.legend.base.Nullable String alias, DatabaseDefinition.TableDefinition def,
            String c) {
        DatabaseDefinition.ColumnDefinition cd = column(def, c);
        return SqlExpr.Column.physical(alias, cd.name(), cd.quoted());
    }

    /** The store table a fetch reads, by its schema's name and its own, quoted as its store declared them. */
    static SqlSource.Table storeTable(Located loc, String table, String alias) {
        String name = loc.schema() == null || loc.schema().isEmpty() || "default".equals(loc.schema()) ? table
                : loc.schema() + "." + table;
        return new SqlSource.Table(name, alias, List.of(), false, Map.of(),
                new NameQuoting(loc.def().schemaQuoted(), loc.def().quoted()));
    }

    static SqlExpr and(@com.legend.base.Nullable SqlExpr left, SqlExpr right) {
        return left == null ? right : SqlExpr.Call.of(SqlFn.AND, left, right);
    }

    static String bare(String table) {
        int dot = table.lastIndexOf('.');
        return dot < 0 ? table : table.substring(dot + 1);
    }
}
