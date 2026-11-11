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
     *  one cell ({@code csv}), both written in the dialect the caller renders them in; and what a comparison with an
     *  expected text and the slice read ({@link #same}, {@link #slice}): the fetches' clauses in order, each table
     *  fetched, and whether its text cells are hashed. */
    public record Planned(List<SqlQuery> fetches, SqlQuery csv, List<SqlWith.Cte> clauses, List<Table> tables,
            boolean hashStrings) {
        public Planned {
            fetches = List.copyOf(fetches);
            clauses = List.copyOf(clauses);
            tables = List.copyOf(tables);
        }
    }

    /** One table the generator fetched: its schema and name, its definition, its fetched columns in the test-data
     *  text's order (by display name) and the clauses that fetch it. */
    public record Table(String schema, String table, DatabaseDefinition.TableDefinition def, List<String> columns,
            List<String> clauses) {
        public Table {
            columns = List.copyOf(columns);
            clauses = List.copyOf(clauses);
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
        List<Table> tables = new ArrayList<>(walk.fetched.size());
        for (Fetched f : walk.fetched.values()) {
            tables.add(new Table(f.schema(), f.table(), f.def(), headerOrder(f.cols()), f.ctes()));
        }
        return new Planned(fetches, csv(walk, hashStrings), walk.ctes, tables, hashStrings);
    }

    /** {@code cols} in the test-data text's order: sorted on the display (upper-cased) name, as upstream's goldens
     *  pin. */
    private static List<String> headerOrder(List<String> cols) {
        List<String> cs = new ArrayList<>(cols);
        cs.sort(java.util.Comparator.comparing(TestDataPlanner::headerCase));
        return cs;
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
            List<String> cs = headerOrder(f.cols());
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
        SqlQuery distinct = fetchedRows(f.ctes());
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

    /** A table's rows, distinct: every clause that fetched it ({@code ctes}), the union of their rows. */
    private static SqlQuery fetchedRows(List<String> ctes) {
        SqlQuery union = null;
        for (String cte : ctes) {
            SqlQuery one = SqlSelect.starOf(new SqlSource.Cte(cte, cte, List.of()));
            union = union == null ? one : new com.legend.sql.SqlUnion(List.of(union, one), false, List.of());
        }
        return union instanceof SqlSelect s ? s.withDistinct()
                : java.util.Objects.requireNonNull(union, "a table with no fetch");
    }

    /** A fetched cell of {@code c} read through {@code alias} as the slice holds it: a text cell hashed where
     *  {@code hashStrings} says, any other as it is (the test-data text's cell before it is written). */
    private static SqlExpr sliceCell(DatabaseDefinition.TableDefinition def, String c, String alias,
            boolean hashStrings) {
        SqlExpr cell = SqlExpr.Column.derived(alias, tempColumn(def, c));
        return hashStrings && isText(column(def, c).dataType()) ? hashString(cell) : cell;
    }

    // ===== the comparison with an expected text (B2d.2: assertTestData) =====

    /** One block of an expected test-data text, as lite's reader reads it: its schema, table, header's names and rows,
     *  a null cell an absent one. */
    public record Expected(String schema, String table, List<String> header, List<List<String>> rows) {
        public Expected {
            header = List.copyOf(header);
            rows = List.copyOf(rows);
        }
    }

    /** Whether {@code planned}'s data is {@code expected}'s: {@code fails} where planning decides it does not (why),
     *  else {@code statement} the database answers in one Boolean cell, or neither where nothing is left to compare
     *  (it is). */
    public record Comparison(@com.legend.base.Nullable String fails, @com.legend.base.Nullable SqlQuery statement) {
    }

    /**
     * Whether {@code planned}'s slice holds {@code expected}'s data on {@code dbFqn}'s tables (B2d.2 of the total-wall
     * program, 2026-11-11: assertTestData as the database's SET comparison per table, the user's ruling -- "upstream's
     * comparison by other means"). Upstream reads each text into its load statements ({@code setUpDataSQLs}: one insert
     * per table block, each cell by its column's type, an empty or {@code ---null---} cell null) and compares the two
     * lists' elements; here, per table, the expected header's columns (resolved in the table as upstream resolves them,
     * case ignored and matching quotes stripped) must be the generated header's where the expected text holds rows, and
     * the two row sets must be equal -- each cell as its text (the generated cell as the text writes it, the expected
     * as the text reads; an empty cell null on both sides), each row of each set found in the other: two anti-joins
     * with null-safe equality, the database's work. A table the expected text names that the database does not hold,
     * or a column its table does not, fails as upstream's load fails; rows expected of a table the generator did not
     * fetch, or under another header, fail at planning.
     */
    public static Comparison same(Planned planned, List<Expected> expected, ModelContext ctx, String dbFqn) {
        Map<String, Table> generated = new LinkedHashMap<>();
        for (Table t : planned.tables()) {
            generated.put(t.schema() + "\n" + t.table(), t);
        }
        Map<String, List<String>> headers = new LinkedHashMap<>();
        Map<String, List<List<String>>> rows = new LinkedHashMap<>();
        for (Expected e : expected) {
            boolean defaultSchema = "default".equals(e.schema());
            DatabaseDefinition.TableDefinition def = ctx.findTableDefinition(dbFqn,
                    defaultSchema ? e.table() : e.schema() + "." + e.table()).orElse(null);
            if (def == null) {
                return new Comparison("Table \"" + e.table() + "\" not found in Schema \"" + e.schema()
                        + "\" in Database(s) " + dbFqn, null);
            }
            List<String> header = new ArrayList<>(e.header().size());
            for (String h : e.header()) {
                DatabaseDefinition.ColumnDefinition col = null;
                for (DatabaseDefinition.ColumnDefinition c : def.columns()) {
                    if (unquoted(c.name()).equalsIgnoreCase(unquoted(h.strip()))) {
                        col = c;
                        break;
                    }
                }
                if (col == null) {
                    return new Comparison("Column \"" + h + "\" not found in Table \"" + e.table()
                            + "\" in Database(s) " + dbFqn, null);
                }
                header.add(col.name());
            }
            String key = e.schema() + "\n" + e.table();
            List<String> before = headers.putIfAbsent(key, header);
            if (before != null && !before.equals(header)) {
                throw new NotImplementedException("testDataGen: the expected text's table '" + e.table() + "' in two"
                        + " blocks under two headers " + before + " and " + header + ": not compared");
            }
            List<List<String>> rs = rows.computeIfAbsent(key, k -> new ArrayList<>());
            for (List<String> r : e.rows()) {
                List<String> cells = new ArrayList<>(header.size());
                for (int i = 0; i < header.size(); i++) {
                    cells.add(i < r.size() ? r.get(i) : null);
                }
                rs.add(cells);
            }
        }
        java.util.Set<String> keys = new java.util.LinkedHashSet<>(generated.keySet());
        keys.addAll(headers.keySet());
        List<SqlSource> counts = new ArrayList<>();
        for (String key : keys) {
            Table g = generated.get(key);
            List<List<String>> want = rows.getOrDefault(key, List.of());
            String name = key.replace('\n', '.');
            if (!want.isEmpty() && g == null) {
                return new Comparison("the test data holds no rows of " + name + ", " + want.size() + " expected",
                        null);
            }
            if (g == null) {
                continue;
            }
            if (!want.isEmpty() && !g.columns().equals(headers.get(key))) {
                return new Comparison("the test data's columns of " + name + " are " + g.columns() + ", "
                        + headers.get(key) + " expected", null);
            }
            SqlQuery got = comparedRows(g, planned.hashStrings());
            if (want.isEmpty()) {
                counts.add(count(got, null, 0, counts.size()));
            } else {
                SqlQuery wanted = expectedRows(want, g.columns().size());
                counts.add(count(got, wanted, g.columns().size(), counts.size()));
                counts.add(count(wanted, got, g.columns().size(), counts.size()));
            }
        }
        if (counts.isEmpty()) {
            return new Comparison(null, null);
        }
        SqlExpr all = null;
        SqlSource from = null;
        for (SqlSource c : counts) {
            String alias = ((SqlSource.Subselect) c).alias();
            all = and(all, SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.derived(alias, "n"), new SqlExpr.IntLit(0)));
            from = from == null ? c : new SqlSource.Join(from, c, SqlSource.Join.Kind.CROSS, null);
        }
        SqlExpr verdict = new SqlExpr.Case(List.of(new SqlExpr.Case.When(java.util.Objects.requireNonNull(all),
                new SqlExpr.BoolLit(true))), new SqlExpr.BoolLit(false));
        SqlSelect cell = new SqlSelect(List.of(new SqlSelect.Projection(verdict, "same", null)), false,
                java.util.Objects.requireNonNull(from), null, List.of(), null, null, List.of(), null, null,
                List.of(new com.legend.sql.OutputCol("same", SqlType.Scalar.BOOLEAN, false)));
        // each table's rows read from a derived table in the FROM, never a subquery in an expression: H2's nested WITH
        // (csv's javadoc)
        return new Comparison(null, SqlWith.prepend(planned.clauses(), cell));
    }

    /** {@code name} with a pair of matching double quotes around it stripped (upstream's stripMatchingQuotes). */
    private static String unquoted(String name) {
        return name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")
                ? name.substring(1, name.length() - 1) : name;
    }

    /** {@code g}'s rows as the comparison reads them: distinct, each cell its text (a text cell hashed where the text
     *  hashes it), an empty text null, beside a marker column the anti-join reads. */
    private static SqlQuery comparedRows(Table g, boolean hashStrings) {
        List<SqlSelect.Projection> ps = new ArrayList<>(g.columns().size() + 1);
        for (int i = 0; i < g.columns().size(); i++) {
            SqlExpr text = new SqlExpr.Cast(sliceCell(g.def(), g.columns().get(i), "_u", hashStrings),
                    SqlType.Scalar.VARCHAR);
            ps.add(new SqlSelect.Projection(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL, text, new SqlExpr.StringLit("")), new SqlExpr.NullLit())), text),
                    "v" + i, null));
        }
        ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(1), MARKER, null));
        return new SqlSelect(ps, true, new SqlSource.Subselect(fetchedRows(g.clauses()), "_u", null), null,
                List.of(), null, null, List.of(), null, null, List.of());
    }

    /** The expected rows {@code rows} of {@code width} text cells each (a null cell NULL), beside the marker column. */
    private static SqlQuery expectedRows(List<List<String>> rows, int width) {
        List<String> columns = new ArrayList<>(width + 1);
        List<com.legend.sql.OutputCol> outputs = new ArrayList<>(width + 1);
        for (int i = 0; i < width; i++) {
            columns.add("v" + i);
            outputs.add(new com.legend.sql.OutputCol("v" + i, SqlType.Scalar.VARCHAR, true));
        }
        columns.add(MARKER);
        outputs.add(new com.legend.sql.OutputCol(MARKER, SqlType.Scalar.INTEGER, false));
        List<List<SqlExpr>> values = new ArrayList<>(rows.size());
        for (List<String> r : rows) {
            List<SqlExpr> cells = new ArrayList<>(width + 1);
            for (String cell : r) {
                cells.add(cell == null ? new SqlExpr.Cast(new SqlExpr.NullLit(), SqlType.Scalar.VARCHAR)
                        : new SqlExpr.StringLit(cell));
            }
            cells.add(new SqlExpr.IntLit(1));
            values.add(cells);
        }
        return SqlSelect.starOf(new SqlSource.Values(values, columns, "_e", outputs));
    }

    /** {@code (SELECT COUNT(*) AS n FROM (left) l [LEFT JOIN (right) r ON l.v0 <=> r.v0 ... WHERE r.marker IS NULL])
     *  AS _d<k>}: the rows of {@code left} not in {@code right} (all of them where {@code right} is null), counted. */
    private static SqlSource count(SqlQuery left, @com.legend.base.Nullable SqlQuery right, int width, int k) {
        SqlSource from = new SqlSource.Subselect(left, "_l", null);
        SqlExpr where = null;
        if (right != null) {
            SqlExpr on = null;
            for (int i = 0; i < width; i++) {
                on = and(on, SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL, SqlExpr.Column.derived("_l", "v" + i),
                        SqlExpr.Column.derived("_r", "v" + i)));
            }
            from = new SqlSource.Join(from, new SqlSource.Subselect(right, "_r", null), SqlSource.Join.Kind.LEFT,
                    java.util.Objects.requireNonNull(on, "a table compared on no column"));
            where = SqlExpr.Call.of(SqlFn.IS_NULL, SqlExpr.Column.derived("_r", MARKER));
        }
        SqlSelect n = new SqlSelect(List.of(new SqlSelect.Projection(
                new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), "n", null)), false, from, where,
                List.of(), null, null, List.of(), null, null, List.of());
        return new SqlSource.Subselect(n, "_d" + k, null);
    }

    /** The marker column the anti-joins read: a row's presence. */
    private static final String MARKER = "_m";

    // ===== the slice, reloaded read-only (B2d.2: loadAndTestExecution) =====

    /**
     * Every table of the database {@code dbFqn} (its includes' too) as the generated slice reloaded holds it, by the
     * table's physical name ({@code TABLE} in the default schema, else {@code SCHEMA.TABLE}): the user's ruling of
     * 2026-11-11, "Read only reload is fine". Upstream's {@code loadAndTestExecution} drops and re-creates EVERY table of
     * the database ({@code setUpDataSQLs} -> {@code schemaAndTableSetup(dbConfig, [])}, {@code toDDL.pure:155-196}) and
     * loads the generated text into it; here each table is a statement the query reads instead: a table the slice
     * holds, its distinct rows -- each fetched column as the text holds it (a text cell hashed where the text hashes
     * it), every other column NULL of its declared type; any other table, no row. Each statement is self-contained:
     * the generator's clauses read inline as derived tables, so it stands wherever the table stood.
     */
    public static Map<String, SqlQuery> slice(Planned planned, ModelContext ctx, String dbFqn) {
        Map<String, SqlQuery> inlined = new LinkedHashMap<>();
        for (SqlWith.Cte c : planned.clauses()) {
            inlined.put(c.name(), inline(c.query(), inlined));
        }
        Map<String, Table> generated = new LinkedHashMap<>();
        for (Table t : planned.tables()) {
            generated.put(physical(t.schema(), t.table()), t);
        }
        Map<String, SqlQuery> out = new LinkedHashMap<>();
        for (Map.Entry<String, DatabaseDefinition.TableDefinition> e : tablesOf(ctx, dbFqn, new java.util.HashSet<>())
                .entrySet()) {
            DatabaseDefinition.TableDefinition def = e.getValue();
            Table g = generated.get(e.getKey());
            List<SqlSelect.Projection> ps = new ArrayList<>(def.columns().size());
            for (DatabaseDefinition.ColumnDefinition c : def.columns()) {
                SqlExpr cell = g != null && g.columns().contains(c.name())
                        ? sliceCell(def, c.name(), "_u", planned.hashStrings())
                        : new SqlExpr.Cast(new SqlExpr.NullLit(), sqlType(c, def));
                ps.add(new SqlSelect.Projection(cell, tempColumn(def, c.name()), null));
            }
            SqlSelect rows = g == null
                    ? new SqlSelect(ps, false, new SqlSource.Dual(), new SqlExpr.BoolLit(false), List.of(), null,
                            null, List.of(), null, null, List.of())
                    : new SqlSelect(ps, true, new SqlSource.Subselect(inline(fetchedRows(g.clauses()), inlined), "_u",
                            null), null, List.of(), null, null, List.of(), null, null, List.of());
            out.put(e.getKey(), rows);
        }
        return out;
    }

    /** A table's physical name: {@code TABLE} in the default schema, else {@code SCHEMA.TABLE}. */
    private static String physical(String schema, String table) {
        return "default".equals(schema) ? table : schema + "." + table;
    }

    /** Every table of {@code dbFqn} and of the databases it includes, by physical name (the first a name meets). */
    private static Map<String, DatabaseDefinition.TableDefinition> tablesOf(ModelContext ctx, String dbFqn,
            java.util.Set<String> seen) {
        Map<String, DatabaseDefinition.TableDefinition> out = new LinkedHashMap<>();
        if (!seen.add(dbFqn)) {
            return out;
        }
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElseThrow(() -> new IllegalStateException(
                "testDataGen: no database '" + dbFqn + "' to reload the slice into"));
        for (DatabaseDefinition.TableDefinition t : db.tables()) {
            out.putIfAbsent(t.name(), t);
        }
        for (DatabaseDefinition.SchemaDefinition sc : db.schemas()) {
            for (DatabaseDefinition.TableDefinition t : sc.tables()) {
                out.putIfAbsent(physical(sc.name(), t.name()), t);
            }
        }
        for (String include : db.includes()) {
            tablesOf(ctx, include, seen).forEach(out::putIfAbsent);
        }
        return out;
    }

    /** {@code q} with each clause it reads ({@code clauses}, already inlined) read in place as a derived table. */
    private static SqlQuery inline(SqlQuery q, Map<String, SqlQuery> clauses) {
        return new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlSource source(SqlSource s) {
                if (s instanceof SqlSource.Cte c && clauses.containsKey(c.name())) {
                    return new SqlSource.Subselect(clauses.get(c.name()), c.alias(), null);
                }
                return s;
            }
        }.rewriteRoot(q);
    }

    /** The SQL type a NULL of column {@code c} is cast to: its declared type's. A binary column has none in the SQL
     *  layer: refused by name. */
    private static SqlType sqlType(DatabaseDefinition.ColumnDefinition c, DatabaseDefinition.TableDefinition def) {
        return switch (c.dataType()) {
            case RelationalDataType.Varchar x -> SqlType.Scalar.VARCHAR;
            case RelationalDataType.Char_ x -> SqlType.Scalar.VARCHAR;
            case RelationalDataType.Distinct x -> SqlType.Scalar.VARCHAR;
            case RelationalDataType.Other x -> SqlType.Scalar.VARCHAR;
            case RelationalDataType.BigInt x -> SqlType.Scalar.BIGINT;
            case RelationalDataType.SmallInt x -> SqlType.Scalar.INTEGER;
            case RelationalDataType.TinyInt x -> SqlType.Scalar.INTEGER;
            case RelationalDataType.Integer_ x -> SqlType.Scalar.INTEGER;
            case RelationalDataType.Float_ x -> SqlType.Scalar.DOUBLE;
            case RelationalDataType.Double_ x -> SqlType.Scalar.DOUBLE;
            case RelationalDataType.Real x -> SqlType.Scalar.DOUBLE;
            case RelationalDataType.Bit x -> SqlType.Scalar.BOOLEAN;
            case RelationalDataType.Decimal d -> new SqlType.Decimal(d.precision(), d.scale());
            case RelationalDataType.Numeric n -> new SqlType.Decimal(n.precision(), n.scale());
            case RelationalDataType.Date_ x -> SqlType.Scalar.DATE;
            case RelationalDataType.Timestamp x -> SqlType.Scalar.TIMESTAMP;
            case RelationalDataType.TimestampNs x -> SqlType.Scalar.TIMESTAMP;
            case RelationalDataType.Time x -> SqlType.Scalar.TIME;
            case RelationalDataType.SemiStructured x -> SqlType.Scalar.JSON;
            case RelationalDataType.Array x -> SqlType.Scalar.JSON;
            case RelationalDataType.Object_ x -> SqlType.Scalar.JSON;
            case RelationalDataType.Binary x -> throw new NotImplementedException("testDataGen: the binary column "
                    + def.name() + "." + c.name() + " reloaded empty: the SQL layer has no binary type (not planned)");
            case RelationalDataType.Varbinary x -> throw new NotImplementedException("testDataGen: the binary column "
                    + def.name() + "." + c.name() + " reloaded empty: the SQL layer has no binary type (not planned)");
        };
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
