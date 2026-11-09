// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import static com.legend.testdatagen.TestDataGenerator.*;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Statements;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lineage.ScanRelations;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlTyping;
import com.legend.sql.dialect.SqlDialect;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * THE TEST-DATA GENERATOR'S OWN RUN, kept for its test (ReservedNamesGenerationTest) and the generator's redesign (S143:
 * the generator is not offered): it plans and runs interleaved -- a fetch run, its rows read, the next planned over
 * them -- so it belongs to neither half of the product, and lives in the tests (the plan-as-data design's slice 5,
 * 2026-11-09: until then it was the product's {@code TestDataGenerator.generate}, the one class that planned and ran).
 * The generator's planning ({@link TestDataGenerator#planText}) stays the product's.
 */
final class TestDataGeneratorRun {

    private TestDataGeneratorRun() {
    }


    public record Result(List<String> sqls, @com.legend.base.Nullable String dataCsvString,
            @com.legend.base.Nullable List<String[]> tables,
            @com.legend.base.Nullable List<Fetch> fetches) {
        public Result(List<String> sqls, String dataCsvString) {
            this(sqls, dataCsvString, null, null);
        }

        public Result(List<String> sqls, @com.legend.base.Nullable String dataCsvString,
                @com.legend.base.Nullable List<String[]> tables) {
            this(sqls, dataCsvString, tables, null);
        }
    }

    /** Per-fetch transcript entry, aligned 1:1 with {@link Result#sqls}
     * — the engine's own return shape (generateTestData pairs every SQL
     * with its ResultSet). {@code rows} are the fetch's LIVE-SESSION
     * results, read from the materialized temp before it drops; {@code
     * parentIndex} is the sqls index of the fetch whose rows seeded this
     * fetch's parent temp (-1 for roots/id-fetches/view fetches);
     * {@code table} is the fetched relation's name — the engine spells
     * a chained hop's parent temp {@code 'testDataGen_Temp_' +
     * table(parentIndex)}. */
    public record Fetch(int parentIndex, String table, List<String> columns,
            List<List<Object>> rows) {
    }

    /** The VIEW's fetch SQL, rendered by the driver's COMPILER (the engine's
     *  own shape, testDataGeneration.pure 377–412: the view planned by the SQL
     *  generator, the fetched base tables substituted by their temps
     *  ({@code fixTables}), printed): {@code tableToTemp} maps each fetched
     *  table's name (bare and {@code SCHEMA.NAME}) to its temp. The
     *  generator never builds view SQL itself. */
    public interface ViewSql {
        String render(String dbFqn, String viewName, Map<String, String> tableToTemp);
    }

    /** The generator's run on a session of {@code sessions} for {@code target}, whose database {@code dialect}
     *  writes every statement: the fetches into its temp tables, their read-backs and the CSV envelope (the dialect
     *  program's Phase 3, 2026-10-16: until then this class wrote one SQL text it hoped both H2 and DuckDB would read).
     *  Every statement is the plan runner's, under the TDG origin (the doors, 2026-10-29: until then the generator
     *  read the session's JDBC connection itself, the fold's residue of D30); a database failure is the runner's
     *  {@code DataError}. */
    public static Result generate(ModelContext ctx,
            LambdaFunction resolvedQuery, String mappingFqn,
            List<TableRowIds> rowIds, @com.legend.base.Nullable MilestoningDates dates,
            boolean hashStrings, PlanSessions.Source sessions, ExecutionPlan.Target target, SqlDialect dialect,
            ViewSql viewSql) {
        List<ScanRelations.Rel> roots =
                ScanRelations.relTree(ctx, resolvedQuery, mappingFqn);
        // engine generateRelationColumnMap: column demand merges PER
        // TABLE across ALL tree nodes (a self-join's two fetches of one
        // table share one column set). fetchRoot expands views itself
        // (it must KEEP the view identity to emit the view fetch).
        Map<String, List<String>> colMap = new LinkedHashMap<>();
        for (ScanRelations.Rel r : roots) {
            collectColMap(ctx, r, colMap, null);
        }
        List<String> sqls = new ArrayList<>();
        // the per-fetch transcript, 1:1 with sqls (engine parity: the
        // engine returns every fetch's ResultSet beside its SQL)
        List<Fetch> fetches = new ArrayList<>();
        // tableKey (schema\ntable) -> fetch temps, in first-fetch order
        Map<String, Fetched> fetched = new LinkedHashMap<>();
        // per-call temp-name counter (temps drop in finally, so names
        // never collide across sequential invocations)
        List<String> temps = new ArrayList<>();
        // the generator's statements, its temps' drops among them, counted under TDG (the mark on its sessions)
        Session st = new Session(PlanSessions.marked(sessions, com.legend.exec.StatementOrigin.TDG), target, dialect);
        try (var __t = new Temps(st, temps)) {
            for (ScanRelations.Rel r : roots) {
                fetchRoot(ctx, r, rowIds, st, sqls, fetches, fetched,
                        temps, colMap, dates, viewSql);
            }
            String csv = csvEnvelope(st, fetched, hashStrings);
            return new Result(List.copyOf(sqls), csv, null,
                    List.copyOf(fetches));
        }
    }

    private record Fetched(String schema, String table, DatabaseDefinition.TableDefinition def, List<String> cols,
            List<String> temps) {
    }

    // ===== tree walk =====

    private static void fetchRoot(ModelContext ctx, ScanRelations.Rel rel,
            List<TableRowIds> rowIds, Session st, List<String> sqls,
            List<Fetch> fetches,
            Map<String, Fetched> fetched, List<String> temps,
            Map<String, List<String>> colMap, @com.legend.base.Nullable MilestoningDates dates,
            ViewSql viewSql) {
        // a VIEW-backed root generates for its UNDERLYING tree (engine
        // generateTestDataForNestedViewTree): the view's seed table is
        // the row-identifier target, its join web fetches as children,
        // and the group closes with the VIEW's OWN fetch over the temps
        // (one per view layer, inner-first — §8.2)
        String viewDb = rel.db();
        String viewName = ScanRelations.isView(ctx, rel.db(), rel.table())
                ? rel.table() : null;
        rel = expandIfView(ctx, rel, null);
        Located loc = locate(ctx, rel.db(), rel.table());
        String tbl = java.util.Objects.requireNonNull(rel.table(), "rel.table()");
        List<String> cols = java.util.Objects.requireNonNull(
                colMap.get(loc.schema() + "\n" + tbl),
                "no column map for " + tbl);
        TableRowIds ids = null;
        for (TableRowIds t : rowIds) {
            if (t.table().equals(rel.table())
                    && t.schema().equals(loc.schema())) {
                ids = t;
            }
        }
        if (ids == null) {
            throw new NotImplementedException("testDataGen: no row"
                    + " identifiers for root table '" + loc.schema() + "."
                    + rel.table() + "' (generateWithDefaultPKs pending)");
        }
        SqlSelect fetch = idFetch(ids, loc, tbl, cols, dates);
        String temp = materialize(st, fetch, tbl, temps);
        int idx = sqls.size();
        sqls.add(st.dialect().renderInline(fetch));
        fetches.add(new Fetch(-1, tbl, cols, tempRows(st, temp)));
        record(fetched, loc, tbl, cols, temp);
        for (ScanRelations.Rel child : rel.children()) {
            fetchChild(ctx, rel, temp, idx, child, st, sqls, fetches,
                    fetched, temps, colMap, rowIds, dates, viewSql);
        }
        if (viewName != null) {
            emitViewFetches(ctx, viewDb, viewName, st, sqls, fetches,
                    fetched, viewSql);
        }
    }

    /** The fetch of a table's rows by their identifiers: its demanded columns, under {@code "root"}, the rows whose
     *  identifiers match ({@code (a and b) or (…)}) and are live on the milestoning dates, at most 20. */
    private static SqlSelect idFetch(TableRowIds ids, Located loc, String table, List<String> cols,
            @com.legend.base.Nullable MilestoningDates dates) {
        SqlExpr where = null;
        for (RowId id : ids.ids()) {
            SqlExpr one = null;
            for (int i = 0; i < id.cols().size(); i++) {
                one = and(one, SqlExpr.Call.of(SqlFn.EQUAL, stored(ROOT, loc.def(), id.cols().get(i)),
                        literal(id.values().get(i), column(loc.def(), id.cols().get(i)))));
            }
            SqlExpr group = new SqlExpr.Group(java.util.Objects.requireNonNull(one, "a row identifier with no column"));
            where = where == null ? group : SqlExpr.Call.of(SqlFn.OR, where, group);
        }
        SqlExpr mf = milestoningFilter(loc.def(), ROOT, dates);
        SqlExpr rows = java.util.Objects.requireNonNull(where, "no row identifiers");
        return fetch(cols, loc.def(), ROOT, storeTable(loc, table, ROOT), mf == null ? rows
                : SqlExpr.Call.of(SqlFn.AND, new SqlExpr.Group(rows), mf));
    }

    /** {@code select <alias>.c, ... from <from> [where <where>] limit 20}: the demanded columns of the table
     *  {@code alias} reads, unaliased, so the database names its temp's columns as it names a read column (H2 upper
     *  case, DuckDB as stored), as when this was text. */
    private static SqlSelect fetch(List<String> cols, DatabaseDefinition.TableDefinition def, String alias,
            SqlSource from, @com.legend.base.Nullable SqlExpr where) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (String c : cols) {
            ps.add(new SqlSelect.Projection(stored(alias, def, c), null, null));
        }
        return new SqlSelect(ps, false, from, where, List.of(), null, null, List.of(), 20L, null, List.of());
    }

    private static void fetchChild(ModelContext ctx, ScanRelations.Rel parent,
            String parentTemp, int parentIdx, ScanRelations.Rel child,
            Session st,
            List<String> sqls, List<Fetch> fetches,
            Map<String, Fetched> fetched,
            List<String> temps, Map<String, List<String>> colMap,
            List<TableRowIds> rowIds, @com.legend.base.Nullable MilestoningDates dates,
            ViewSql viewSql) {
        String viewDb = child.db();
        String viewName = ScanRelations.isView(ctx, child.db(), child.table())
                ? child.table() : null;
        child = expandIfView(ctx, child, parent.db());
        Located loc = locate(ctx, child.db(), child.table());
        String ct = java.util.Objects.requireNonNull(child.table(), "child.table()");
        List<String> cols = java.util.Objects.requireNonNull(
                colMap.get(loc.schema() + "\n" + ct),
                "no column map for " + ct);
        // a VIEW child's SEED with EXPLICIT row identifiers fetches BY
        // ITS IDS, not the parent join (engine
        // generateTestDataStartingFromNode's isView arm: tablePk
        // non-empty short-circuits the parent-derived ids —
        // testViewEmbeddedInChainedJoin pins personTable 1,2,3,5 while
        // the join reaches only 1,2). PLAIN table children stay
        // join-derived (a self-join's manager fetch must follow the
        // join even when the table's ROOT ids are configured).
        for (TableRowIds t : viewName == null ? List.<TableRowIds>of()
                : rowIds) {
            if (t.table().equals(ct) && t.schema().equals(loc.schema())) {
                SqlSelect idFetch = idFetch(t, loc, ct, cols, dates);
                String idTemp = materialize(st, idFetch, ct, temps);
                int idIdx = sqls.size();
                sqls.add(st.dialect().renderInline(idFetch));
                fetches.add(new Fetch(-1, ct, cols, tempRows(st, idTemp)));
                record(fetched, loc, ct, cols, idTemp);
                for (ScanRelations.Rel sub : child.children()) {
                    fetchChild(ctx, child, idTemp, idIdx, sub, st, sqls,
                            fetches, fetched, temps, colMap, rowIds, dates, viewSql);
                }
                if (viewName != null) {
                    emitViewFetches(ctx, viewDb, viewName, st, sqls,
                            fetches, fetched, viewSql);
                }
                return;
            }
        }
        RelationalOperation op = child.cond() != null ? child.cond()
                : findJoin(ctx, child.joinName(), child.db(),
                        parent.db()).operation();
        String alias = ct.equals(parent.table()) ? "t_" + ct : ct;
        SqlExpr cond = condition(op, parent.table(), locate(ctx, parent.db(), parent.table()).def(),
                child.table(), alias, loc.def(), String.valueOf(child.joinName()));
        SqlSelect fetch = fetch(cols, loc.def(), alias, new SqlSource.Join(tempTable(parentTemp, PARENT),
                storeTable(loc, ct, alias), SqlSource.Join.Kind.INNER, cond),
                milestoningFilter(loc.def(), alias, dates));
        String temp = materialize(st, fetch, child.table(), temps);
        int idx = sqls.size();
        sqls.add(st.dialect().renderInline(fetch));
        fetches.add(new Fetch(parentIdx, ct, cols, tempRows(st, temp)));
        record(fetched, loc, child.table(), cols, temp);
        for (ScanRelations.Rel sub : child.children()) {
            fetchChild(ctx, child, temp, idx, sub, st, sqls, fetches,
                    fetched, temps, colMap, rowIds, dates, viewSql);
        }
        if (viewName != null) {
            emitViewFetches(ctx, viewDb, viewName, st, sqls, fetches,
                    fetched, viewSql);
        }
    }

    // ===== the view fetch (engine generateTestDataForNestedViewTree) =====

    /** After a view group's base fetches, the engine emits the VIEW's own
     * query re-pointed at the fetched temp tables (fixTables oldToNew)
     * and executes it — one per view layer, INNER-FIRST for view-on-view
     * stacks. The view result pairs with a View (not a Table), so it
     * adds no CSV block; the SQL and its execution are the contract. */
    private static void emitViewFetches(ModelContext ctx, String db,
            String viewName, Session st, List<String> sqls,
            List<Fetch> fetches,
            Map<String, Fetched> fetched, ViewSql viewSql) {
        List<String> chain = ScanRelations
                .viewExpansion(ctx, db, viewName).viewChain();
        // the fetched tables by every spelling the lowering may use for a
        // source name (bare, SCHEMA.NAME) -> the LAST temp of each
        Map<String, String> tableToTemp = new LinkedHashMap<>();
        for (Fetched f : fetched.values()) {
            if (f.temps().isEmpty()) {
                continue;
            }
            String temp = f.temps().get(f.temps().size() - 1);
            tableToTemp.put(f.table(), temp);
            tableToTemp.put(f.schema() + "." + f.table(), temp);
        }
        for (int i = chain.size() - 1; i >= 0; i--) {
            String sql = viewSql.render(db, chain.get(i), tableToTemp);
            sqls.add(sql);
            List<String> viewCols = new ArrayList<>();
            List<List<Object>> viewRows = captureRows(st, sql, viewCols);
            fetches.add(new Fetch(-1, chain.get(i), viewCols, viewRows));
        }
    }

    /** The ONE whole-temp read (csvEnvelope's union arms and the transcript capture share it — single owner). */
    private static SqlSelect selectAll(String temp) {
        return SqlSelect.starOf(tempTable(temp, temp));
    }

    /** The materialized temp's LIVE contents — the fetch's transcript
     * rows (engine parity: generateTestData pairs each SQL with its
     * ResultSet). */
    private static List<List<Object>> tempRows(Session st, String temp) {
        return captureRows(st, st.render(selectAll(temp)), null);
    }

    /** {@code sql}'s rows as typed cells (the doors, 2026-10-29; until then a JDBC result set read cell by cell as
     *  the driver's objects): the statement DESCRIBED by the database first ({@link PlanRunner#describe}: a prepare,
     *  no row), then run as a relation of the columns it reported, each cell read by its reported kind's slot
     *  ({@link PlanRunner#rows}, the typed method's carriers); a column the database reports in no kind of the
     *  vocabulary is read as text (a raw grid's, since the seam program's S3c, is read as the driver's own kind: {@code Statements.reportedRows}). {@code colsOut}, when given,
     *  takes the reported labels. */
    private static List<List<Object>> captureRows(Session st, String sql,
            @com.legend.base.Nullable List<String> colsOut) {
        List<ExecutionPlan.Column> columns = new ArrayList<>();
        for (Statements.Reported r : PlanRunner.describe(st.sql(sql), st.sessions())) {
            String slot = com.legend.executionplan.ColumnFacts.slot(r.type() == null
                    ? com.legend.sql.SqlType.Scalar.VARCHAR : r.type());
            columns.add(new ExecutionPlan.Column(r.name(), com.legend.exec.WireTypes.pureTypeNameOf(slot),
                    new ExecutionPlan.Multiplicity(0, 1), slot, null, null));
            if (colsOut != null) {
                colsOut.add(r.name());
            }
        }
        return rows(st, new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION, columns, st.sql(sql)));
    }

    /** {@code node}'s rows, run by the plan runner on the run's session. */
    private static List<List<Object>> rows(Session st, ExecutionPlan.TdsResult node) {
        try {
            return PlanRunner.rows(ExecutionPlan.reading(List.of(), node), Map.of(), st.sessions()).rows();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** {@code statement} run as a plan effect on the run's session. */
    private static void effect(Session st, String statement) {
        try {
            PlanRunner.effect(new ExecutionPlan.Effect(st.sql(statement)), st.sessions());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** The engine's getMilestoningFilter: forced temporal dates filter a
     * MILESTONED table's fetch (business/processing from-thru ranges,
     * snapshot equality), the table read through {@code alias}. A
     * milestoned table without its date is the engine's own assert — a
     * loud wall. */
    private static @com.legend.base.Nullable SqlExpr milestoningFilter(
            DatabaseDefinition.TableDefinition def, String alias,
            @com.legend.base.Nullable MilestoningDates d) {
        var ms = def.milestoning();
        if (ms == null || d == null) {
            return null;
        }
        SqlExpr parts = null;
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
        return parts;
    }

    /** The fetch's rows into a temp table of the session's own, named {@code tdg_<n>_<table>} (the referee knows a
     *  chained fetch by that prefix). */
    private static String materialize(Session st, SqlSelect fetch,
            String table, List<String> temps) {
        String temp = "tdg_" + temps.size() + "_"
                + table.replaceAll("[^A-Za-z0-9_]", "_");
        effect(st, st.dialect().render(new SqlDml.CreateTempTableAs(temp, fetch)));
        temps.add(temp);
        return temp;
    }

    private static void record(Map<String, Fetched> fetched, Located loc,
            String table, List<String> cols, String temp) {
        Fetched f = fetched.computeIfAbsent(loc.schema() + "\n" + table,
                k -> new Fetched(loc.schema(), table, loc.def(), cols, new ArrayList<>()));
        if (!f.cols().equals(cols)) {
            throw new IllegalStateException("testDataGen: table '" + table
                    + "' fetched twice with differing column sets "
                    + f.cols() + " vs " + cols);
        }
        f.temps().add(temp);
    }

    // ===== the join condition =====

    /** A child edge's join condition over the parent's temp table ({@code main}) and the child table
     *  ({@code childAlias}): a self-join's plain spelling is the PARENT side, its {@code {target}} spelling the
     *  child (engine reprocessAliases). */
    private static SqlExpr condition(RelationalOperation op,
            @com.legend.base.Nullable String parentTable, DatabaseDefinition.TableDefinition parentDef,
            String childTable, String childAlias,
            DatabaseDefinition.TableDefinition childDef, String joinName) {
        return switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                String t = bare(cr.table());
                if (t.equals(parentTable)) {
                    yield SqlExpr.Column.derived(PARENT, tempColumn(parentDef, cr.column()));
                }
                if (t.equals(childTable)) {
                    yield stored(childAlias, childDef, cr.column());
                }
                throw new NotImplementedException("testDataGen: join '"
                        + joinName + "' references table '" + t
                        + "' outside the parent/child pair — multi-table"
                        + " join conditions pending");
            }
            case RelationalOperation.TargetColumnRef tr -> stored(childAlias, childDef, tr.column());
            case RelationalOperation.Literal l -> literal(l.value(), null);
            case RelationalOperation.Comparison c -> SqlExpr.Call.of(switch (c.op()) {
                case EQ -> SqlFn.EQUAL;
                case NEQ -> SqlFn.NOT_EQUAL;
                case LT -> SqlFn.LESS;
                case LTE -> SqlFn.LESS_EQUAL;
                case GT -> SqlFn.GREATER;
                case GTE -> SqlFn.GREATER_EQUAL;
            }, condition(c.left(), parentTable, parentDef, childTable, childAlias, childDef, joinName),
                    condition(c.right(), parentTable, parentDef, childTable, childAlias, childDef, joinName));
            case RelationalOperation.BooleanOp b -> SqlExpr.Call.of(
                    b.op() == com.legend.model.LogicalOp.AND ? SqlFn.AND : SqlFn.OR,
                    condition(b.left(), parentTable, parentDef, childTable, childAlias, childDef, joinName),
                    condition(b.right(), parentTable, parentDef, childTable, childAlias, childDef, joinName));
            case RelationalOperation.Group g -> new SqlExpr.Group(
                    condition(g.inner(), parentTable, parentDef, childTable, childAlias, childDef, joinName));
            case RelationalOperation.IsNull n -> SqlExpr.Call.of(SqlFn.IS_NULL,
                    condition(n.operand(), parentTable, parentDef, childTable, childAlias, childDef, joinName));
            case RelationalOperation.IsNotNull n -> SqlExpr.Call.of(SqlFn.IS_NOT_NULL,
                    condition(n.operand(), parentTable, parentDef, childTable, childAlias, childDef, joinName));
            default -> throw new NotImplementedException("testDataGen: join '"
                    + joinName + "' condition node "
                    + op.getClass().getSimpleName() + " pending");
        };
    }

    private static String headerCase(String col) {
        return col.matches("[A-Za-z_][A-Za-z0-9_]*")
                ? col.toUpperCase(java.util.Locale.ROOT) : col;
    }

    // ===== CSV =====

    private static String csvEnvelope(Session st,
            Map<String, Fetched> fetched, boolean hashStrings) {
        StringBuilder out = new StringBuilder();
        for (Fetched f : fetched.values()) {
            if (out.length() > 0) {
                out.append("-----\n");
            }
            // dedup ACROSS fetches of one table = DB-side UNION
            SqlQuery union = f.temps().size() == 1 ? selectAll(f.temps().get(0)).withDistinct()
                    : new com.legend.sql.SqlUnion(f.temps().stream().<SqlQuery>map(TestDataGeneratorRun::selectAll)
                            .toList(), false, List.of());
            // engine parity: H2 UPPERCASES unquoted result labels; plain
            // identifiers print uppercase, exotic names ride as-is.
            // The CSV column order sorts on the DISPLAY (uppercased)
            // name — the fetch/plan order stays the stored-name sort
            // (bitemporal plan text pins ID < PLACE < from_z while the
            // CSV goldens pin B_PERSONID < ID)
            List<String> cs = new ArrayList<>(f.cols());
            cs.sort(java.util.Comparator.comparing(
                    TestDataGeneratorRun::headerCase));
            out.append(f.schema()).append('\n').append(f.table())
                    .append('\n').append(String.join(",", cs.stream()
                            .map(TestDataGeneratorRun::headerCase).toList()))
                    .append('\n');
            // A5/A6: hashing AND the CSV scrub are SQL — Java only
            // displays. Text columns learn from the union's SCHEMA (a
            // LIMIT-0 metadata read, not value sniffing).
            java.util.Set<String> textCols = new java.util.HashSet<>();
            for (Statements.Reported r : PlanRunner.columns(st.sql(st.render(
                    SqlSelect.starOf(new SqlSource.Subselect(union, "_u", null)).withLimit(0L))), st.sessions())) {
                // a text column by the kind the session reports it in (JDBC's CHAR, VARCHAR and LONGVARCHAR, with
                // their national twins, are the vocabulary's VARCHAR: WireTypes.sqlTypeOf)
                if (r.type() == com.legend.sql.SqlType.Scalar.VARCHAR) {
                    // without case: H2 names a temp's column upper case, DuckDB as stored
                    textCols.add(r.name().toUpperCase(java.util.Locale.ROOT));
                }
            }
            List<SqlSelect.Projection> projs = new ArrayList<>(cs.size());
            for (String c : cs) {
                String tc = tempColumn(f.def(), c);
                SqlExpr cell = SqlExpr.Column.derived(null, tc);
                if (!textCols.contains(tc.toUpperCase(java.util.Locale.ROOT))) {
                    // non-string kinds pass through (engine
                    // hashStrings(): s:String -> hash, Any unchanged);
                    // their display forms never carry scrub characters
                    projs.add(new SqlSelect.Projection(cell, null, null));
                } else if (hashStrings) {
                    projs.add(new SqlSelect.Projection(hashString(cell), tc, null));
                } else {
                    // the CSV scrub (quote/comma/newline) in SQL — the
                    // old Java replace chain was A6's lossy scrub at
                    // the wrong layer
                    projs.add(new SqlSelect.Projection(replace(replace(replace(cell,
                            SqlExpr.Call.of(SqlFn.CHR, new SqlExpr.IntLit(39)), " "), new SqlExpr.StringLit(","), ";"),
                            SqlExpr.Call.of(SqlFn.CHR, new SqlExpr.IntLit(10)), " "), tc, null));
                }
            }
            // E5: the ROW TEXT is SQL — cells cast to their display
            // text, the '---null---' token and the comma joins all in
            // the projection; Java appends DB-produced lines only. The
            // OUTER query orders by the projected display columns
            // (left-to-right — the old ordinal semantics; the sort keys
            // need not be projected).
            List<SqlExpr> parts = new ArrayList<>();
            for (String c : cs) {
                if (!parts.isEmpty()) {
                    parts.add(new SqlExpr.StringLit(","));
                }
                parts.add(SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Cast(
                        SqlExpr.Column.derived(CSV_ROW, tempColumn(f.def(), c)), com.legend.sql.SqlType.Scalar.VARCHAR),
                        new SqlExpr.StringLit("---null---")));
            }
            SqlSelect cells = new SqlSelect(projs, false, new SqlSource.Subselect(union, "_u", null), null, List.of(),
                    null, null, List.of(), null, null, List.of());
            SqlSelect lines = new SqlSelect(List.of(new SqlSelect.Projection(parts.size() == 1 ? parts.get(0)
                    : new SqlExpr.Call(SqlFn.CONCAT, parts, SqlTyping.UNKNOWN), "_csv_line", null)), false,
                    new SqlSource.Subselect(cells, CSV_ROW, null), null, List.of(), null, null,
                    cs.stream().map(c -> SqlSelect.SortKey.asc(SqlExpr.Column.derived(CSV_ROW, tempColumn(f.def(), c))))
                            .toList(), null, null, List.of());
            // the lines as one text column's values (a VALUE plan of one VARCHAR column)
            for (List<Object> row : rows(st, new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.VALUE,
                    List.of(new ExecutionPlan.Column("_csv_line", "String", new ExecutionPlan.Multiplicity(0, 1),
                            "VARCHAR", null, null)), st.sql(st.render(lines))))) {
                out.append(row.get(0)).append('\n');
            }
        }
        if (out.length() > 0) {
            out.append("-----\n");
        }
        return out.toString();
    }

    /** The engine's hashString (testDataGeneration.pure:656) IN SQL: the first 5 hex characters of the string's
     *  SHA-256, tiled to its length (whole repeats, then the LAST {@code length % 5} characters). Hex needs no
     *  scrub. A NULL string stays NULL. */
    private static SqlExpr hashString(SqlExpr s) {
        SqlExpr h = SqlExpr.Call.of(SqlFn.SUBSTRING, SqlExpr.Call.of(SqlFn.SHA256, s), new SqlExpr.IntLit(1),
                new SqlExpr.IntLit(5));
        SqlExpr length = SqlExpr.Call.of(SqlFn.LENGTH, s);
        SqlExpr tiled = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.REPEAT_STR, h, SqlExpr.Call.of(SqlFn.INT_DIVIDE, length, new SqlExpr.IntLit(5))),
                SqlExpr.Call.of(SqlFn.RIGHT, h, SqlExpr.Call.of(SqlFn.MOD, length, new SqlExpr.IntLit(5))));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, s), new SqlExpr.NullLit())),
                tiled);
    }

    private static SqlExpr replace(SqlExpr s, SqlExpr from, String to) {
        return SqlExpr.Call.of(SqlFn.REPLACE, s, from, new SqlExpr.StringLit(to));
    }

    // ===== rendering primitives =====

    /** A row identifier's or a join's value as a literal: a string of a DATE or TIMESTAMP column that type's
     *  literal, a whole number an integer, any other number its exact value. */
    private static SqlExpr literal(@com.legend.base.Nullable Object v,
            @com.legend.base.Nullable DatabaseDefinition.ColumnDefinition col) {
        if (v == null) {
            return new SqlExpr.NullLit();
        }
        if (v instanceof String s) {
            if (col != null) {
                RelationalDataType t = col.dataType();
                if (t instanceof RelationalDataType.Date_) {
                    return new SqlExpr.DateLit(s);
                }
                if (t instanceof RelationalDataType.Timestamp) {
                    return new SqlExpr.TimestampLit(s);
                }
                if (t instanceof RelationalDataType.Time) {
                    return new SqlExpr.TimeLit(s);
                }
                if (t instanceof RelationalDataType.TimestampNs) {
                    return new SqlExpr.TimestampLit(s);
                }
            }
            return new SqlExpr.StringLit(s);
        }
        if (v instanceof Boolean b) {
            return new SqlExpr.BoolLit(b);
        }
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return new SqlExpr.IntLit(((Number) v).longValue());
        }
        if (v instanceof Number n) {
            return new SqlExpr.DecimalLit(new java.math.BigDecimal(n.toString()));
        }
        throw new NotImplementedException(
                "testDataGen: row identifier value " + v.getClass());
    }

    /** The plan runner's source for the session every statement of a run goes to, the target it is checked
     *  against, and the session's dialect, which writes them (the doors, 2026-10-29: until then a JDBC statement of
     *  the generator's own). */
    private record Session(PlanSessions.Source sessions, ExecutionPlan.Target target, SqlDialect dialect) {
        String render(SqlQuery q) {
            return dialect.render(q);
        }

        /** {@code text} as a plan statement of the target (no slot: the generator binds nothing). */
        ExecutionPlan.Sql sql(String text) {
            return ExecutionPlan.Sql.of(text, target);
        }
    }

    /** A temp's column, as a query over the temp names it: a column its store declared quoted by its name, any
     *  other upper case. The database named it as it names a read column: H2 upper case (unquoted), DuckDB as stored,
     *  and DuckDB compares names without case, so both read it so (the dialect program's Phase 3, 2026-10-16: a
     *  declared-quoted column was upper-cased too while this was text, which H2 cannot find). */
    private static String tempColumn(DatabaseDefinition.TableDefinition def, String c) {
        DatabaseDefinition.ColumnDefinition cd = column(def, c);
        return cd.quoted() ? cd.name() : cd.name().toUpperCase(java.util.Locale.ROOT);
    }

    /** A temp table of the run's own. */
    private static SqlSource.Table tempTable(String temp, String alias) {
        return new SqlSource.Table(temp, alias, List.of(), false, Map.of(), NameQuoting.BARE);
    }

    /** The temps of a run, dropped when the run ends, each as a plan effect (try-with-resources: a drop that fails
     *  after a run that failed rides the run's own error, suppressed; one after a run that succeeded is the error --
     *  the doors, 2026-10-29: until then a failed drop was swallowed, the temps dying with the connection). */
    private record Temps(Session st, List<String> names) implements AutoCloseable {
        /** Every temp dropped, whatever an earlier drop did: the first refusal is the error, the later ones suppressed
         *  onto it (the doors' audit, 2026-10-29, finding 4: the loop stopped at the first). */
        @Override
        public void close() {
            com.legend.error.DataError first = null;
            for (String t : names) {
                try {
                    effect(st, st.dialect().render(new SqlDdl.DropTable(null, t, NameQuoting.BARE)));
                } catch (com.legend.error.DataError e) {
                    if (first == null) {
                        first = e;
                    } else {
                        first.addSuppressed(e);
                    }
                }
            }
            if (first != null) {
                throw first;
            }
        }
    }
}
