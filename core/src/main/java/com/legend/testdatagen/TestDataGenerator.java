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

    /** {@code rel}'s table's demanded columns merged into {@code colMap} (the node half of the engine's
     *  generateRelationColumnMap; its children not walked). */
    public static void mergeCols(ModelContext ctx, ScanRelations.Rel rel, Map<String, List<String>> colMap) {
        Located loc = locate(ctx, rel.db(), rel.table());
        String key = loc.schema() + "\n" + rel.table();
        // engine sortBy(name) is the plain pure string sort — ASCII
        // case-sensitive (bitemporal golden: ID < PLACE < from_z; the
        // earlier all-uppercase evidence fit either order)
        TreeSet<String> merged = new TreeSet<>();
        merged.addAll(colMap.getOrDefault(key, List.of()));
        for (String c : fetchCols(ctx, loc, rel)) {
            // a demanded name is the model's: the declared column of that exact name (column)
            merged.add(column(loc.def(), c).name());
        }
        colMap.put(key, List.copyOf(merged));
    }

    public record Located(String schema,
            DatabaseDefinition.TableDefinition def) {
    }

    /** {@code alias.column <op> DATE 'date'}. */
    public static SqlExpr onDate(String alias, DatabaseDefinition.TableDefinition def, String column, SqlFn op,
            String date) {
        return SqlExpr.Call.of(op, stored(alias, def, column), new SqlExpr.DateLit(date));
    }

    public static String requireDate(@com.legend.base.Nullable String v,
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
    public static List<String> fetchCols(ModelContext ctx, Located loc,
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

    public static void collectTableCols(RelationalOperation op,
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

    // ===== lookups =====

    public static Located locate(ModelContext ctx, String dbFqn,
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

    public static DatabaseDefinition.JoinDefinition findJoin(
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

    public static DatabaseDefinition.ColumnDefinition column(
            DatabaseDefinition.TableDefinition def, String name) {
        // quote-bearing demand names strip to the stored declaration
        if (name.length() > 1 && name.startsWith("\"")
                && name.endsWith("\"")) {
            name = name.substring(1, name.length() - 1);
        }
        // by its exact name, as upstream resolves a column (HelperRelationalBuilder.getColumn; a row identifier's
        // column, testDataGeneration.pure: $x.name == $cv.first) -- every name here is the model's or a row
        // identifier's, never a database's folding (the orchestrator's go, 2026-11-12: no case leniency)
        for (DatabaseDefinition.ColumnDefinition c : def.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        throw new NotImplementedException("testDataGen: column '" + name
                + "' not on table '" + def.name() + "'");
    }

    /** The alias a fetch reads its seed table through, and the one a child fetch reads its parent's temp
     *  through. */
    public static final String ROOT = "root";

    public static final String PARENT = "main";

    /** The alias the CSV envelope's row text reads its cells through. */
    public static final String CSV_ROW = "_r";

    /** A column of the store table {@code def}, read through {@code alias}, spelled as its DDL declared it (bare, or
     *  quoted with its case kept). */
    public static SqlExpr stored(@com.legend.base.Nullable String alias, DatabaseDefinition.TableDefinition def,
            String c) {
        DatabaseDefinition.ColumnDefinition cd = column(def, c);
        return SqlExpr.Column.physical(alias, cd.name(), cd.quoted());
    }

    /** The store table a fetch reads, by its schema's name and its own, quoted as its store declared them. */
    public static SqlSource.Table storeTable(Located loc, String table, String alias) {
        String name = loc.schema() == null || loc.schema().isEmpty() || "default".equals(loc.schema()) ? table
                : loc.schema() + "." + table;
        return new SqlSource.Table(name, alias, List.of(), false, Map.of(),
                new NameQuoting(loc.def().schemaQuoted(), loc.def().quoted()));
    }

    public static SqlExpr and(@com.legend.base.Nullable SqlExpr left, SqlExpr right) {
        return left == null ? right : SqlExpr.Call.of(SqlFn.AND, left, right);
    }

    public static String bare(String table) {
        int dot = table.lastIndexOf('.');
        return dot < 0 ? table : table.substring(dot + 1);
    }
}
