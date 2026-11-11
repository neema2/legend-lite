// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.lineage.ScanRelations;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlTyping;
import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.legend.testdatagen.TestDataGenerator.CSV_ROW;
import static com.legend.testdatagen.TestDataGenerator.PARENT;
import static com.legend.testdatagen.TestDataGenerator.ROOT;
import static com.legend.testdatagen.TestDataGenerator.and;
import static com.legend.testdatagen.TestDataGenerator.bare;
import static com.legend.testdatagen.TestDataGenerator.collectColMap;
import static com.legend.testdatagen.TestDataGenerator.column;
import static com.legend.testdatagen.TestDataGenerator.findJoin;
import static com.legend.testdatagen.TestDataGenerator.locate;
import static com.legend.testdatagen.TestDataGenerator.onDate;
import static com.legend.testdatagen.TestDataGenerator.requireDate;
import static com.legend.testdatagen.TestDataGenerator.stored;
import static com.legend.testdatagen.TestDataGenerator.storeTable;

/**
 * THE TEST-DATA GENERATOR AS ONE PLANNED STATEMENT (B2d, the design APPROVED by the user on 2026-11-10,
 * docs/data-delivery-homework/TEST_DATA_GENERATOR_DESIGN_2026_11_10.md §3): given a query, its mapping, the starting
 * rows (row identifiers) and the milestoning dates, the slice of a database the query needs is ONE read-only statement
 * whose answer is the test-data text -- per table its schema, its name, its columns, its lines, and {@code -----}.
 *
 * <p>Every fetch of the query's relation tree is a {@code WITH} clause: the starting rows (the identifiers' rows of the
 * root table, ordered by key, at most 20), then one clause per navigation step joined to its parent's (a self-reference
 * one per step, never recursive SQL), each at most 20 rows by key. Each table's lines are the distinct rows of all its
 * fetches, their text cells hashed or written faithfully (RFC 4180 quoting) IN SQL (which columns are text: the store's declared types, at
 * planning), a NULL {@code ---null---}, joined in order by the database ({@code STRING_AGG}); the headers are the
 * store's, planning constants. Nothing is created on the database, so nothing is left after a failure, and no Java
 * reads a row: the database answers the text. {@link Planned#sqls} are the fetches' own texts (each its clauses and
 * its rows), one per fetch as upstream returns one per fetch; their text differs from upstream's (no temporary
 * tables), by design.
 *
 * <p>Not yet planned here, each refused by name: a view in the relation tree, and the default starting rows (no row
 * identifiers) -- B2d's next slices.
 */
public final class TestDataPlanner {

    private TestDataPlanner() {
    }

    /** The most rows a fetch takes: upstream's {@code top 20}, ordered by key here so every query picks the same. */
    static final long FETCH_LIMIT = 20;

    /** The generator's plan: one text per fetch ({@code sqls}), and the statement answering the test-data text in its
     *  one cell ({@code csv}), both written in the dialect the caller renders them in. */
    public record Planned(List<SqlQuery> fetches, SqlQuery csv) {
        public Planned {
            fetches = List.copyOf(fetches);
        }
    }

    /** The test data {@code query} over {@code mappingFqn} needs, starting from {@code rowIds}, the milestoned tables
     *  read on {@code dates}, text cells hashed where {@code hashStrings} says. */
    public static Planned plan(ModelContext ctx, LambdaFunction query, String mappingFqn,
            List<TestDataGenerator.TableRowIds> rowIds, TestDataGenerator.@com.legend.base.Nullable MilestoningDates dates,
            boolean hashStrings) {
        List<ScanRelations.Rel> roots = ScanRelations.relTree(ctx, query, mappingFqn);
        // the engine's generateRelationColumnMap: a table's demanded columns merged across every node that fetches it
        Map<String, List<String>> colMap = new LinkedHashMap<>();
        for (ScanRelations.Rel r : roots) {
            collectColMap(ctx, r, colMap, null);
        }
        Walk walk = new Walk(ctx, colMap, rowIds, dates);
        for (ScanRelations.Rel r : roots) {
            walk.root(r);
        }
        List<SqlQuery> fetches = new ArrayList<>(walk.ctes.size());
        for (int i = 0; i < walk.ctes.size(); i++) {
            SqlWith.Cte cte = walk.ctes.get(i);
            fetches.add(SqlWith.prepend(walk.ctes.subList(0, i + 1), SqlSelect.starOf(
                    new SqlSource.Cte(cte.name(), cte.name(), List.of()))));
        }
        return new Planned(fetches, csv(walk, hashStrings));
    }

    /** One table's fetches: its schema, name, definition, demanded columns and the clauses that fetch it. */
    private record Fetched(String schema, String table, DatabaseDefinition.TableDefinition def, List<String> cols,
            List<String> ctes) {
    }

    /** The relation tree's walk: each fetch a {@code WITH} clause, named {@code tdg_<n>} in fetch order. */
    private static final class Walk {
        private final ModelContext ctx;
        private final Map<String, List<String>> colMap;
        private final List<TestDataGenerator.TableRowIds> rowIds;
        private final TestDataGenerator.@com.legend.base.Nullable MilestoningDates dates;
        private final List<SqlWith.Cte> ctes = new ArrayList<>();
        private final Map<String, Fetched> fetched = new LinkedHashMap<>();

        Walk(ModelContext ctx, Map<String, List<String>> colMap, List<TestDataGenerator.TableRowIds> rowIds,
                TestDataGenerator.@com.legend.base.Nullable MilestoningDates dates) {
            this.ctx = ctx;
            this.colMap = colMap;
            this.rowIds = rowIds;
            this.dates = dates;
        }

        void root(ScanRelations.Rel rel) {
            refuseView(rel);
            TestDataGenerator.Located loc = locate(ctx, rel.db(), rel.table());
            String table = java.util.Objects.requireNonNull(rel.table(), "rel.table()");
            List<String> cols = cols(loc, table);
            TestDataGenerator.TableRowIds ids = null;
            for (TestDataGenerator.TableRowIds t : rowIds) {
                if (t.table().equals(table) && t.schema().equals(loc.schema())) {
                    ids = t;
                }
            }
            if (ids == null) {
                throw new NotImplementedException("testDataGen: no row identifiers for root table '" + loc.schema()
                        + "." + table + "' (the default starting rows: B2d's next slice)");
            }
            String cte = add(loc, table, cols, idFetch(ids, loc, table, cols));
            for (ScanRelations.Rel child : rel.children()) {
                child(rel, cte, child);
            }
        }

        void child(ScanRelations.Rel parent, String parentCte, ScanRelations.Rel child) {
            refuseView(child);
            TestDataGenerator.Located loc = locate(ctx, child.db(), child.table());
            String table = java.util.Objects.requireNonNull(child.table(), "child.table()");
            List<String> cols = cols(loc, table);
            RelationalOperation op = child.cond() != null ? child.cond()
                    : findJoin(ctx, child.joinName(), child.db(), parent.db()).operation();
            String alias = table.equals(parent.table()) ? "t_" + table : table;
            TestDataGenerator.Located parentLoc = locate(ctx, parent.db(), parent.table());
            SqlExpr cond = condition(op, parent.table(), parentLoc.def(), table, alias, loc.def(),
                    String.valueOf(child.joinName()));
            SqlSource from = new SqlSource.Join(new SqlSource.Cte(parentCte, PARENT, List.of()),
                    storeTable(loc, table, alias), SqlSource.Join.Kind.INNER, cond);
            String cte = add(loc, table, cols, fetch(cols, loc.def(), alias, from,
                    milestoningFilter(loc.def(), alias, dates)));
            for (ScanRelations.Rel sub : child.children()) {
                child(child, cte, sub);
            }
        }

        private List<String> cols(TestDataGenerator.Located loc, String table) {
            return java.util.Objects.requireNonNull(colMap.get(loc.schema() + "\n" + table),
                    () -> "no column map for " + table);
        }

        private void refuseView(ScanRelations.Rel rel) {
            if (ScanRelations.isView(ctx, rel.db(), rel.table())) {
                throw new NotImplementedException("testDataGen: the view '" + rel.table() + "' in the query's"
                        + " relation tree (its fetch over the planned clauses: B2d's next slice)");
            }
        }

        /** The fetch {@code q} of {@code table} as the next clause, recorded among the table's fetches. */
        private String add(TestDataGenerator.Located loc, String table, List<String> cols, SqlSelect q) {
            String name = "tdg_" + ctes.size();
            ctes.add(new SqlWith.Cte(name, q));
            Fetched f = fetched.computeIfAbsent(loc.schema() + "\n" + table,
                    k -> new Fetched(loc.schema(), table, loc.def(), cols, new ArrayList<>()));
            if (!f.cols().equals(cols)) {
                throw new IllegalStateException("testDataGen: table '" + table + "' fetched twice with differing"
                        + " column sets " + f.cols() + " vs " + cols);
            }
            f.ctes().add(name);
            return name;
        }

        /** The fetch of a table's rows by their identifiers: {@code (a and b) or (...)}, live on the dates. */
        private SqlSelect idFetch(TestDataGenerator.TableRowIds ids, TestDataGenerator.Located loc, String table,
                List<String> cols) {
            SqlExpr where = null;
            for (TestDataGenerator.RowId id : ids.ids()) {
                SqlExpr one = null;
                for (int i = 0; i < id.cols().size(); i++) {
                    one = and(one, SqlExpr.Call.of(SqlFn.EQUAL, stored(ROOT, loc.def(), id.cols().get(i)),
                            literal(id.values().get(i), column(loc.def(), id.cols().get(i)))));
                }
                SqlExpr group = new SqlExpr.Group(java.util.Objects.requireNonNull(one,
                        "a row identifier with no column"));
                where = where == null ? group : SqlExpr.Call.of(SqlFn.OR, where, group);
            }
            SqlExpr rows = java.util.Objects.requireNonNull(where, "no row identifiers");
            SqlExpr mf = milestoningFilter(loc.def(), ROOT, dates);
            return fetch(cols, loc.def(), ROOT, storeTable(loc, table, ROOT), mf == null ? rows
                    : SqlExpr.Call.of(SqlFn.AND, new SqlExpr.Group(rows), mf));
        }
    }

    /** {@code select <alias>.c as "C", ... from <from> [where <where>] order by <key> limit 20}: the demanded columns
     *  of the table {@code alias} reads, named as the next clause reads them, the rows taken by key. */
    private static SqlSelect fetch(List<String> cols, DatabaseDefinition.TableDefinition def, String alias,
            SqlSource from, @com.legend.base.Nullable SqlExpr where) {
        List<SqlSelect.Projection> ps = new ArrayList<>(cols.size());
        for (String c : cols) {
            ps.add(new SqlSelect.Projection(stored(alias, def, c), tempColumn(def, c), null));
        }
        List<SqlSelect.SortKey> order = new ArrayList<>();
        for (String c : cols) {
            if (column(def, c).primaryKey()) {
                order.add(SqlSelect.SortKey.asc(stored(alias, def, c)));
            }
        }
        if (order.isEmpty()) {
            for (String c : cols) {
                order.add(SqlSelect.SortKey.asc(stored(alias, def, c)));
            }
        }
        return new SqlSelect(ps, false, from, where, List.of(), null, null, order, FETCH_LIMIT, null, List.of());
    }

    /** The statement answering the test-data text: every fetch's clause, then one cell -- each table's block (its
     *  header, a planning constant; its lines, the database's, joined in order; {@code -----}) concatenated. Each
     *  table's lines are a one-row aggregate in the statement's FROM, the tables cross-joined, never a scalar
     *  subquery: the statement is read where a value stands (B2d.2, a scalar subquery of its own), and H2 2.5.252
     *  does not resolve a nested {@code WITH}'s clauses from a scalar subquery in its body once that {@code WITH} is
     *  inside a common table expression -- as the database judge's verdict statement places a side (measured
     *  2026-11-11: "Table not found"; from a derived table in the body's FROM it resolves them, as DuckDB and SQLite
     *  do either way). */
    private static SqlQuery csv(Walk walk, boolean hashStrings) {
        if (walk.fetched.isEmpty()) {
            throw new IllegalStateException("testDataGen: a relation tree with no table");
        }
        List<SqlExpr> blocks = new ArrayList<>();
        SqlSource from = null;
        int n = 0;
        for (Fetched f : walk.fetched.values()) {
            // the CSV's column order sorts on the display (upper-cased) name, as upstream's goldens pin
            List<String> cs = new ArrayList<>(f.cols());
            cs.sort(java.util.Comparator.comparing(TestDataPlanner::headerCase));
            String header = f.schema() + "\n" + f.table() + "\n"
                    + String.join(",", cs.stream().map(TestDataPlanner::headerCase).toList()) + "\n";
            String alias = "_t" + n++;
            SqlSource lines = new SqlSource.Subselect(lines(walk, f, cs, hashStrings), alias, null);
            from = from == null ? lines : new SqlSource.Join(from, lines, SqlSource.Join.Kind.CROSS, null);
            blocks.add(new SqlExpr.StringLit(header));
            blocks.add(SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.derived(alias, LINES),
                    new SqlExpr.StringLit("")));
            blocks.add(new SqlExpr.StringLit("-----\n"));
        }
        SqlExpr text = new SqlExpr.Call(SqlFn.CONCAT, blocks, SqlTyping.UNKNOWN);
        SqlSelect cell = new SqlSelect(List.of(new SqlSelect.Projection(text, "dataCsvString", null)), false,
                java.util.Objects.requireNonNull(from), null, List.of(), null, null, List.of(), null, null, List.of());
        return SqlWith.prepend(walk.ctes, cell);
    }

    /** A table's lines' one column. */
    private static final String LINES = "_csv_lines";

    /** {@code f}'s lines as one text: its fetches' distinct rows, each cell's display text (a text cell hashed or
     *  quoted where it holds a comma, a quote or a line break, a NULL {@code ---null---}) joined by commas, each line ending in a line break, in the cells' order. */
    private static SqlQuery lines(Walk walk, Fetched f, List<String> cs, boolean hashStrings) {
        SqlQuery union = null;
        for (String cte : f.ctes()) {
            SqlQuery one = SqlSelect.starOf(new SqlSource.Cte(cte, cte, List.of()));
            union = union == null ? one : new com.legend.sql.SqlUnion(List.of(union, one), false, List.of());
        }
        SqlQuery distinct = union instanceof SqlSelect s ? s.withDistinct()
                : java.util.Objects.requireNonNull(union, "a table with no fetch");
        List<SqlSelect.Projection> projs = new ArrayList<>(cs.size());
        for (String c : cs) {
            String tc = tempColumn(f.def(), c);
            SqlExpr cell = SqlExpr.Column.derived("_u", tc);
            if (!isText(column(f.def(), c).dataType())) {
                projs.add(new SqlSelect.Projection(cell, tc, null));
            } else if (hashStrings) {
                projs.add(new SqlSelect.Projection(hashString(cell), tc, null));
            } else {
                projs.add(new SqlSelect.Projection(faithful(cell), tc, null));
            }
        }
        SqlSelect cells = new SqlSelect(projs, false, new SqlSource.Subselect(distinct, "_u", null), null, List.of(),
                null, null, List.of(), null, null, List.of());
        List<SqlExpr> parts = new ArrayList<>();
        for (String c : cs) {
            if (!parts.isEmpty()) {
                parts.add(new SqlExpr.StringLit(","));
            }
            parts.add(SqlExpr.Call.of(SqlFn.COALESCE, new SqlExpr.Cast(SqlExpr.Column.derived(CSV_ROW,
                    tempColumn(f.def(), c)), SqlType.Scalar.VARCHAR), new SqlExpr.StringLit("---null---")));
        }
        parts.add(new SqlExpr.StringLit("\n"));
        SqlExpr line = new SqlExpr.Call(SqlFn.CONCAT, parts, SqlTyping.UNKNOWN);
        List<SqlSelect.SortKey> order = cs.stream()
                .map(c -> SqlSelect.SortKey.asc(SqlExpr.Column.derived(CSV_ROW, tempColumn(f.def(), c)))).toList();
        SqlExpr joined = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG, List.of(line, new SqlExpr.StringLit("")), false,
                order);
        return new SqlSelect(List.of(new SqlSelect.Projection(joined, LINES, null)), false,
                new SqlSource.Subselect(cells, CSV_ROW, null), null, List.of(), null, null, List.of(), null, null,
                List.of());
    }

    /** Whether a column of {@code t} holds text, which the CSV hashes or quotes: its declared type, at planning. */
    static boolean isText(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.Varchar v -> true;
            case RelationalDataType.Char_ c -> true;
            case RelationalDataType.BigInt x -> false;
            case RelationalDataType.SmallInt x -> false;
            case RelationalDataType.TinyInt x -> false;
            case RelationalDataType.Integer_ x -> false;
            case RelationalDataType.Float_ x -> false;
            case RelationalDataType.Double_ x -> false;
            case RelationalDataType.Real x -> false;
            case RelationalDataType.Bit x -> false;
            case RelationalDataType.Binary x -> false;
            case RelationalDataType.Varbinary x -> false;
            case RelationalDataType.Decimal x -> false;
            case RelationalDataType.Numeric x -> false;
            case RelationalDataType.Timestamp x -> false;
            case RelationalDataType.Date_ x -> false;
            case RelationalDataType.Time x -> false;
            case RelationalDataType.TimestampNs x -> false;
            case RelationalDataType.Distinct x -> false;
            case RelationalDataType.Other x -> false;
            case RelationalDataType.SemiStructured x -> false;
            case RelationalDataType.Array x -> false;
            case RelationalDataType.Object_ x -> false;
        };
    }

    /** The CSV's header name: a plain name upper case, any other as it is. */
    static String headerCase(String col) {
        return col.matches("[A-Za-z_][A-Za-z0-9_]*") ? col.toUpperCase(java.util.Locale.ROOT) : col;
    }

    /** A fetched column as the next clause reads it: a name its store declared quoted as it is, any other upper case
     *  (a clause's label, written quoted, read the same on every database). */
    static String tempColumn(DatabaseDefinition.TableDefinition def, String c) {
        DatabaseDefinition.ColumnDefinition cd = column(def, c);
        return cd.quoted() ? cd.name() : cd.name().toUpperCase(java.util.Locale.ROOT);
    }

    /** A child edge's join condition over the parent's clause ({@link TestDataGenerator#PARENT}) and the child table
     *  ({@code childAlias}): a self-join's plain spelling the PARENT side, its {@code {target}} spelling the child
     *  (the engine's reprocessAliases). */
    private static SqlExpr condition(RelationalOperation op, @com.legend.base.Nullable String parentTable,
            DatabaseDefinition.TableDefinition parentDef, String childTable, String childAlias,
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
                throw new NotImplementedException("testDataGen: join '" + joinName + "' references table '" + t
                        + "' outside the parent/child pair: multi-table join conditions pending");
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
            default -> throw new NotImplementedException("testDataGen: join '" + joinName + "' condition node "
                    + op.getClass().getSimpleName() + " pending");
        };
    }

    /** The engine's getMilestoningFilter: forced temporal dates filter a MILESTONED table's fetch (business and
     *  processing from-thru ranges, snapshot equality), the table read through {@code alias}. A milestoned table
     *  without its date is refused by name, as the engine asserts. */
    private static @com.legend.base.Nullable SqlExpr milestoningFilter(DatabaseDefinition.TableDefinition def,
            String alias, TestDataGenerator.@com.legend.base.Nullable MilestoningDates d) {
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

    /** The engine's hashString (testDataGeneration.pure:656) in SQL: the first 5 hex characters of the string's
     *  SHA-256, tiled to its length (whole repeats, then the last {@code length % 5} characters). A NULL stays NULL. */
    private static SqlExpr hashString(SqlExpr s) {
        SqlExpr h = SqlExpr.Call.of(SqlFn.SUBSTRING, SqlExpr.Call.of(SqlFn.SHA256, s), new SqlExpr.IntLit(1),
                new SqlExpr.IntLit(5));
        SqlExpr length = SqlExpr.Call.of(SqlFn.LENGTH, s);
        SqlExpr tiled = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.REPEAT_STR, h, SqlExpr.Call.of(SqlFn.INT_DIVIDE, length, new SqlExpr.IntLit(5))),
                SqlExpr.Call.of(SqlFn.RIGHT, h, SqlExpr.Call.of(SqlFn.MOD, length, new SqlExpr.IntLit(5))));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, s),
                new SqlExpr.NullLit())), tiled);
    }

    /**
     * A text cell written FAITHFULLY (the user's ruling 3, 2026-11-10, B_BRIEF §9: "faithful to the real data, line
     * breaks included"; register S191): as it is, unless it holds a comma, a double quote or a line break (CR or LF),
     * when it is one quoted cell, RFC 4180 -- its quotes doubled, the whole in quotes; a single quote as it is, nothing
     * replaced and nothing lost (upstream's Pure writer replaces {@code '} with a space, {@code ,} with {@code ;} and a
     * line break with a space). The database decides each cell, in SQL; a NULL stays NULL ({@code ---null---} after).
     */
    private static SqlExpr faithful(SqlExpr cell) {
        SqlExpr quoted = holds(cell, new SqlExpr.StringLit(","));
        for (SqlExpr special : List.of(new SqlExpr.StringLit("\""), SqlExpr.Call.of(SqlFn.CHR, new SqlExpr.IntLit(10)),
                SqlExpr.Call.of(SqlFn.CHR, new SqlExpr.IntLit(13)))) {
            quoted = SqlExpr.Call.of(SqlFn.OR, quoted, holds(cell, special));
        }
        SqlExpr doubled = new SqlExpr.Call(SqlFn.CONCAT, List.of(new SqlExpr.StringLit("\""),
                replace(cell, new SqlExpr.StringLit("\""), "\"\""), new SqlExpr.StringLit("\"")), SqlTyping.UNKNOWN);
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(quoted, doubled)), cell);
    }

    /** Whether {@code cell} holds {@code text}. */
    private static SqlExpr holds(SqlExpr cell, SqlExpr text) {
        return SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, cell, text), new SqlExpr.IntLit(0));
    }

    private static SqlExpr replace(SqlExpr s, SqlExpr from, String to) {
        return SqlExpr.Call.of(SqlFn.REPLACE, s, from, new SqlExpr.StringLit(to));
    }

    /** A row identifier's or a join's value as a literal: a string of a DATE or TIMESTAMP column that type's literal,
     *  a whole number an integer, any other number its exact value. */
    private static SqlExpr literal(@com.legend.base.Nullable Object v,
            DatabaseDefinition.@com.legend.base.Nullable ColumnDefinition col) {
        if (v == null) {
            return new SqlExpr.NullLit();
        }
        if (v instanceof String s) {
            if (col != null) {
                RelationalDataType t = col.dataType();
                if (t instanceof RelationalDataType.Date_) {
                    return new SqlExpr.DateLit(s);
                }
                if (t instanceof RelationalDataType.Timestamp || t instanceof RelationalDataType.TimestampNs) {
                    return new SqlExpr.TimestampLit(s);
                }
                if (t instanceof RelationalDataType.Time) {
                    return new SqlExpr.TimeLit(s);
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
        throw new NotImplementedException("testDataGen: row identifier value " + v.getClass());
    }
}
