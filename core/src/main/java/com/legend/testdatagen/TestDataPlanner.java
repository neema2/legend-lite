// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.compiler.element.ModelContext;
import com.legend.error.AssertFailed;
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

    /**
     * A view's own fetch (B2d.3, the design's §2.3): the view {@code view} of {@code db} as the compiler plans a read of
     * it -- its lifted relation, its filter, distinct, grouping and joins -- each table it reads, by physical name, read
     * from {@code tables} (the clause that fetched it) for each identity of {@code reads} ({@code store|physical}), as
     * the reload's swap reads a slice. The planner's, not the generator's: the generator's package never lowers.
     */
    @FunctionalInterface
    public interface Views {
        SqlQuery fetch(String db, String view, java.util.Set<String> reads, Map<String, SqlQuery> tables);
    }

    /** The test data {@code query} over {@code mappingFqn} needs, starting from {@code rowIds}, the milestoned tables
     *  read on {@code dates}, text cells hashed where {@code hashStrings} says, a view's own fetch planned by
     *  {@code views}. */
    public static Planned plan(ModelContext ctx, LambdaFunction query, String mappingFqn,
            List<TestDataGenerator.TableRowIds> rowIds, TestDataGenerator.@com.legend.base.Nullable MilestoningDates dates,
            boolean hashStrings, Views views) {
        return planned(ctx, query, mappingFqn, rowIds, dates, hashStrings, views, null, false);
    }

    /**
     * The test data of the server's routes (B2d.3e; upstream's {@code TestDataGenerationAPI}:
     * {@code generateTestData_WithSeed} and {@code generateTestData_WithDefaultSeed}, which run the generator's PLAN
     * form, {@code planTestDataGenerationWithParameterValuePairs}, and write its rows with
     * {@code TestDataGeneration.manageTestDataResult}): {@code query} over {@code mappingFqn} from {@code rowIds}, or,
     * for a root none of them names, from {@code seed} -- the default seed, its chosen keys the statement's first
     * clause and the root's rows read through it (every row, by key, where it holds none: upstream's
     * {@code orFilters([])}); the text's tables in the routes' order, by schema then table ({@code manageTestDataResult},
     * :282), each cell written as the function writes it (the user's ruling R5, register S191). A view in the query's
     * relation tree is refused by name: upstream's plan form fetches a view's own rows {@code top 20} and a child view's
     * main table through its parent (TDG:1023-1201), which lite does not plan. A root no identifier names and no seed
     * starts is upstream's error node ({@link Unseeded}).
     */
    public static Planned route(ModelContext ctx, LambdaFunction query, String mappingFqn,
            List<TestDataGenerator.TableRowIds> rowIds, @com.legend.base.Nullable Seed seed, boolean hashStrings) {
        return planned(ctx, query, mappingFqn, rowIds, null, hashStrings, (db, view, reads, tables) -> {
            throw new IllegalStateException("a view on the server's route is refused before it is fetched");
        }, seed, true);
    }

    private static Planned planned(ModelContext ctx, LambdaFunction query, String mappingFqn,
            List<TestDataGenerator.TableRowIds> rowIds, TestDataGenerator.@com.legend.base.Nullable MilestoningDates dates,
            boolean hashStrings, Views views, @com.legend.base.Nullable Seed seed, boolean route) {
        List<ScanRelations.Rel> roots = ScanRelations.relTree(ctx, query, mappingFqn);
        if (route) {
            for (ScanRelations.Rel r : roots) {
                noView(ctx, r);
            }
        }
        // the engine's generateRelationColumnMap: a table's demanded columns merged across every node that fetches it
        // (a view's: its own tables', and its children's, TDG:567-583)
        Map<String, List<String>> colMap = new LinkedHashMap<>();
        for (ScanRelations.Rel r : roots) {
            columns(ctx, r, colMap);
        }
        Walk walk = new Walk(ctx, colMap, rowIds, dates, views, seed, route);
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
        return new Planned(fetches, csv(walk, hashStrings, route), walk.ctes, tables, hashStrings);
    }

    /** A view in {@code rel}'s tree, refused by name on the server's route ({@link #route}). */
    private static void noView(ModelContext ctx, ScanRelations.Rel rel) {
        if (ScanRelations.isView(ctx, rel.db(), rel.table())) {
            throw new NotImplementedException("testDataGen: the view '" + rel.table() + "' in the query's relation"
                    + " tree on the server's route: not planned (B2d.3e; upstream's plan form fetches a view's own rows"
                    + " top 20 and a child view's main table through its parent, TDG:1023-1201)");
        }
        for (ScanRelations.Rel c : rel.children()) {
            noView(ctx, c);
        }
    }

    /** The demanded columns of every table {@code rel}'s tree fetches: a table's as {@link TestDataGenerator#mergeCols}
     *  merges them; a view's, the tables of its own tree (its seed table and its join web, {@code nestedViewTree}),
     *  then its children's -- the children join the view's own fetch, so nothing of theirs reaches the view's
     *  tables. */
    private static void columns(ModelContext ctx, ScanRelations.Rel rel, Map<String, List<String>> colMap) {
        if (ScanRelations.isView(ctx, rel.db(), rel.table())) {
            columns(ctx, ScanRelations.viewExpansion(ctx, rel.db(), rel.table()).tree(), colMap);
        } else {
            TestDataGenerator.mergeCols(ctx, rel, colMap);
        }
        for (ScanRelations.Rel child : rel.children()) {
            columns(ctx, child, colMap);
        }
    }

    /** {@code cols} in the test-data text's order: sorted on the display (upper-cased) name, as upstream's goldens
     *  pin. */
    private static List<String> headerOrder(List<String> cols) {
        List<String> cs = new ArrayList<>(cols);
        cs.sort(java.util.Comparator.comparing(TestDataPlanner::headerCase));
        return cs;
    }

    /** {@code cols} of {@code def} in the server routes' order (the B2d.3 audit's M1): upstream's plan form sorts a
     *  table's columns by their model names ({@code sortBy(x | $x.name)}, TDG:885, :1043) -- Pure's string order,
     *  case-sensitive, a quoted name with its quotes -- before it aliases each, and the route writes them in that
     *  order. A planning constant: the model's names. */
    private static List<String> routeOrder(DatabaseDefinition.TableDefinition def, List<String> cols) {
        List<String> cs = new ArrayList<>(cols);
        cs.sort(java.util.Comparator.comparing(c -> pureName(def, c)));
        return cs;
    }

    /** A column's name in the server routes' header (the B2d.3 audit's M1): upstream aliases it
     *  {@code $y.name->addQuotesIfNoQuotes()} (TDG:885) and its writer prints the label without its quotes, in double
     *  quotes where it holds a space ({@code SQLResultColumn.getQuotedLabelIfContainSpace},
     *  TestDataGeneration.java:306): the declared name, its case kept. */
    private static String routeHeader(DatabaseDefinition.TableDefinition def, String c) {
        String name = column(def, c).name();
        return name.contains(" ") ? "\"" + name + "\"" : name;
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
        private final Views views;
        /** The default seed the server's route starts its root from, or null. */
        private final @com.legend.base.Nullable Seed seed;
        /** Whether this is the server's route (its root's error is the route's, {@link Unseeded}). */
        private final boolean route;
        private final List<SqlWith.Cte> ctes = new ArrayList<>();
        private final Map<String, Fetched> fetched = new LinkedHashMap<>();
        /** Each table clause's table, by physical name: what a view's own fetch reads it from. */
        private final Map<String, String> clauseTables = new LinkedHashMap<>();
        /** Each table clause's fetch: its table's definition and columns. */
        private final Map<String, Fetched> clauseFetched = new LinkedHashMap<>();

        Walk(ModelContext ctx, Map<String, List<String>> colMap, List<TestDataGenerator.TableRowIds> rowIds,
                TestDataGenerator.@com.legend.base.Nullable MilestoningDates dates, Views views,
                @com.legend.base.Nullable Seed seed, boolean route) {
            this.ctx = ctx;
            this.colMap = colMap;
            this.rowIds = rowIds;
            this.dates = dates;
            this.views = views;
            this.seed = seed;
            this.route = route;
        }

        void root(ScanRelations.Rel rel) {
            if (ScanRelations.isView(ctx, rel.db(), rel.table())) {
                String v = view(rel);
                for (ScanRelations.Rel child : rel.children()) {
                    child(rel, v, child);
                }
                return;
            }
            TestDataGenerator.Located loc = locate(ctx, rel.db(), rel.table());
            String table = java.util.Objects.requireNonNull(rel.table(), "rel.table()");
            String cte = startingRows(rel.db(), loc, table, null);
            for (ScanRelations.Rel child : rel.children()) {
                child(rel, cte, child);
            }
        }

        void child(ScanRelations.Rel parent, String parentCte, ScanRelations.Rel child) {
            if (ScanRelations.isView(ctx, child.db(), child.table())) {
                // a child view: its own tree from its main table's given identifiers, whatever the parent's rows
                // (the interpreted path, TDG:425-438, :429), then its own fetch; its children join that fetch
                String v = view(child);
                for (ScanRelations.Rel sub : child.children()) {
                    child(child, v, sub);
                }
                return;
            }
            TestDataGenerator.Located loc = locate(ctx, child.db(), child.table());
            String table = java.util.Objects.requireNonNull(child.table(), "child.table()");
            List<String> cols = cols(loc, table);
            RelationalOperation op = child.cond() != null ? child.cond()
                    : findJoin(ctx, child.joinName(), child.db(), parent.db()).operation();
            String alias = table.equals(parent.table()) ? "t_" + table : table;
            SqlExpr cond = condition(op, parent.table(), parentColumns(parent), table, alias, loc.def(),
                    String.valueOf(child.joinName()));
            SqlSource from = new SqlSource.Join(new SqlSource.Cte(parentCte, PARENT, List.of()),
                    storeTable(loc, table, alias), SqlSource.Join.Kind.INNER, cond);
            String cte = add(loc, table, cols, fetch(cols, loc.def(), alias, from,
                    milestoningFilter(loc.def(), alias, dates)));
            for (ScanRelations.Rel sub : child.children()) {
                child(child, cte, sub);
            }
        }

        /** How a child's join reads a column of its parent's clause: a table's fetched column by its clause's label,
         *  a view's column by the view's own name for it (its own fetch's column, as the view's relation names it). */
        private java.util.function.Function<String, SqlExpr> parentColumns(ScanRelations.Rel parent) {
            String name = java.util.Objects.requireNonNull(parent.table(), "a parent with no relation");
            if (ScanRelations.isView(ctx, parent.db(), name)) {
                DatabaseDefinition.ViewDefinition vd = ScanRelations.viewDef(ctx, parent.db(), name);
                return c -> {
                    for (DatabaseDefinition.ViewDefinition.ViewColumnMapping m : vd.columnMappings()) {
                        // the view's column of the join's exact name, its quotes aside (no case leniency: upstream
                        // resolves a view's column by its exact name)
                        if (bareName(m.name()).equals(bareName(c))) {
                            return SqlExpr.Column.derived(PARENT, m.name());
                        }
                    }
                    throw new NotImplementedException("testDataGen: a join reads the column '" + c + "' of the view '"
                            + parent.table() + "', which the view does not declare");
                };
            }
            TestDataGenerator.Located parentLoc = locate(ctx, parent.db(), parent.table());
            return c -> SqlExpr.Column.derived(PARENT, tempColumn(parentLoc.def(), c));
        }

        /**
         * A view {@code v} of the query's tree (B2d.3, the design's §2; upstream's interpreted path, TDG:339-418): its
         * main table's rows by their given identifiers, then the tables of its own join web
         * ({@code nestedViewTree}), each joined to its parent as any child; then its own fetch, one per view layer,
         * inner first, each the view's relation over those clauses -- each table it reads from the FIRST clause that
         * fetched it (upstream's {@code oldToNew}, TDG:388-405), no limit (TDG:406-409: the interpreted fetch takes
         * none). Returns the outer layer's clause, which the view's children join. The view's own rows are never
         * written: the text holds tables only (TDG:193).
         */
        private String view(ScanRelations.Rel v) {
            ScanRelations.ViewExpansion ve = ScanRelations.viewExpansion(ctx, v.db(), v.table());
            ScanRelations.Rel base = ve.tree();
            TestDataGenerator.Located loc = locate(ctx, base.db(), base.table());
            String table = java.util.Objects.requireNonNull(base.table(), "a view with no main table");
            int first = ctes.size();
            String seed = startingRows(base.db(), loc, table, v.table());
            for (ScanRelations.Rel c : base.children()) {
                child(base, seed, c);
            }
            Map<String, SqlQuery> tables = new LinkedHashMap<>();
            for (int i = first; i < ctes.size(); i++) {
                String clause = ctes.get(i).name();
                String physical = clauseTables.get(clause);
                Fetched f = clauseFetched.get(clause);
                if (physical != null && f != null) {
                    tables.putIfAbsent(physical, declaredNames(clause, f));
                }
            }
            java.util.Set<String> reads = new java.util.LinkedHashSet<>();
            for (String identity : TestDataPlanner.reads(ctx, v.db())) {
                if (tables.containsKey(identity.substring(identity.indexOf('|') + 1))) {
                    reads.add(identity);
                }
            }
            String outer = null;
            for (int i = ve.viewChain().size() - 1; i >= 0; i--) {
                outer = "tdg_" + ctes.size();
                ctes.add(new SqlWith.Cte(outer, views.fetch(v.db(), ve.viewChain().get(i), reads, tables)));
            }
            return java.util.Objects.requireNonNull(outer, "a view with no layer");
        }

        /** The clause {@code clause}'s rows, each column labelled by its declared name: as the reload's slice labels a
         *  table's columns, the swap reading each by that label (lowering/TableSwaps; the B checkpoint's G4). */
        private static SqlSelect declaredNames(String clause, Fetched f) {
            List<SqlSelect.Projection> ps = new ArrayList<>(f.cols().size());
            for (String c : f.cols()) {
                ps.add(new SqlSelect.Projection(SqlExpr.Column.derived(clause, tempColumn(f.def(), c)),
                        column(f.def(), c).name(), null));
            }
            return new SqlSelect(ps, false, new SqlSource.Cte(clause, clause, List.of()), null, List.of(), null, null,
                    List.of(), null, null, List.of());
        }

        /** The starting rows of {@code table}: its given identifiers' rows (by key, at most 20), as the next clause; no
         *  identifiers is upstream's error ({@link #noIdentifiers}). */
        private String startingRows(String db, TestDataGenerator.Located loc, String table,
                @com.legend.base.Nullable String view) {
            TestDataGenerator.TableRowIds ids = null;
            for (TestDataGenerator.TableRowIds t : rowIds) {
                if (t.table().equals(table) && t.schema().equals(loc.schema())) {
                    if (ids != null) {
                        // two entries for one table: upstream's assert, before anything else (the B2d.3 audit's L2;
                        // getRowIdentifiersForTable, TDG:487, and the plan form's, TDG:1208; getQualifiedTableName)
                        throw new AssertFailed("Multiple table row identifiers passed for the table: "
                                + physical(loc.schema(), table));
                    }
                    ids = t;
                }
            }
            if (ids == null) {
                Seed s = seed;
                if (s != null && view == null && s.table().equals(table) && s.loc().schema().equals(loc.schema())) {
                    return seeded(loc, table, s);
                }
                throw route ? unseeded(ctx, db, loc, table, view) : noIdentifiers(ctx, db, loc, table, view);
            }
            List<String> cols = cols(loc, table);
            return add(loc, table, cols, idFetch(ids, loc, table, cols));
        }

        /** The root {@code table}'s rows from the default seed {@code s} (B2d.3e; the design's §1.3): the seed's chosen
         *  keys as the next clause, then the table's rows by key, at most 20 -- those whose keys the seed holds, or,
         *  where it holds none, any (upstream's {@code orFilters([])}). One statement: no second round. */
        private String seeded(TestDataGenerator.Located loc, String table, Seed s) {
            String keys = "tdg_" + ctes.size();
            ctes.add(new SqlWith.Cte(keys, s.rows()));
            SqlExpr match = null;
            for (String k : s.keys()) {
                match = and(match, SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.derived("_seed",
                        tempColumn(loc.def(), k)), stored(ROOT, loc.def(), k)));
            }
            SqlExpr held = new SqlExpr.Exists(one(new SqlSource.Cte(keys, "_seed", List.of()), match));
            SqlExpr none = SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(one(new SqlSource.Cte(keys, "_any",
                    List.of()), null)));
            List<String> cols = cols(loc, table);
            return add(loc, table, cols, fetch(cols, loc.def(), ROOT, storeTable(loc, table, ROOT),
                    SqlExpr.Call.of(SqlFn.OR, none, held)));
        }

        /** {@code select 1 from <from> [where <where>]}. */
        private static SqlSelect one(SqlSource from, @com.legend.base.Nullable SqlExpr where) {
            return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "_one", null)), false, from,
                    where, List.of(), null, null, List.of(), null, null, List.of());
        }

        private List<String> cols(TestDataGenerator.Located loc, String table) {
            return java.util.Objects.requireNonNull(colMap.get(loc.schema() + "\n" + table),
                    () -> "no column map for " + table);
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
            clauseTables.put(name, physical(loc.schema(), table));
            clauseFetched.put(name, f);
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
            // no identifiers in the list is no filter: the table's rows by key, at most 20 (upstream's
            // orFilters([]), TDG; the design's §1.2, "an empty seed is no filter")
            SqlExpr mf = milestoningFilter(loc.def(), ROOT, dates);
            return fetch(cols, loc.def(), ROOT, storeTable(loc, table, ROOT), where == null ? mf : mf == null ? where
                    : SqlExpr.Call.of(SqlFn.AND, new SqlExpr.Group(where), mf));
        }
    }

    /**
     * No identifiers for a starting table -- the query's root, or a view's main table: upstream's error, raised where
     * {@code generateTestData} is planned (the user's ruling R3, 2026-11-11, "yes fine"; register S300). Upstream's
     * interpreted generator reads the table's first 5 keys and fails its own assert ({@code getRowIdentifiersForTable},
     * TDG:482-518; a view's, TDG:510, :558) with {@code Row Identifers should be provided for the root table[ of the
     * view (V)]: <db>.<[schema.]table> [<key>:<type>,...]\nExample:\n} and those keys written as
     * {@code createTableRowIdentifiers(...)} code. Planning reads no row, so the example is written with no keys: the
     * message is upstream's, its example's list empty.
     */
    static AssertFailed noIdentifiers(ModelContext ctx, String db, TestDataGenerator.Located loc, String table,
            @com.legend.base.Nullable String view) {
        String owner = owningDatabase(ctx, db, loc.def());
        return new AssertFailed(unidentified(owner, loc, table, view) + "Example:\n"
                + "  meta::relational::testDataGeneration::createTableRowIdentifiers(" + owner + ", '" + loc.schema()
                + "', '" + table + "', [\n\n  ])\n");
    }

    /** Upstream's no-identifiers text up to its example, both forms': {@code Row Identifers should be provided for the
     *  root table[ of the view (V)]: <db>.<[schema.]table> [<key>:<type>,...]\n}. */
    private static String unidentified(String owner, TestDataGenerator.Located loc, String table,
            @com.legend.base.Nullable String view) {
        String keys = loc.def().columns().stream().filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                .map(c -> c.name() + ":" + com.legend.plan.PlanText.spell(c.dataType()))
                .collect(java.util.stream.Collectors.joining(","));
        return "Row Identifers should be provided for the root table" + (view == null ? "" : " of the view (" + view
                + ")") + ": " + owner + "." + physical(loc.schema(), table) + " [" + keys + "]\n";
    }

    /**
     * No identifiers and no seed for a root on the server's route (B2d.3e): upstream's plan form's error node
     * (TDG:1244-1252), which the route answers as its 500's text -- the node's message, {@code Example:\n} and the
     * table's identifiers as JSON ({@code TestDataGeneration.manageTestDataResult}, :230-262). Upstream's example holds
     * the table's first 5 keys, read for it; planning reads no row, so the route's example holds none (register S300,
     * as the function's). The message, and the table the example names, are data: the route writes the JSON.
     */
    public static final class Unseeded extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final String database;
        private final String schema;
        private final String table;

        Unseeded(String message, String database, String schema, String table) {
            super(message);
            this.database = database;
            this.schema = schema;
            this.table = table;
        }

        public String database() {
            return database;
        }

        public String schema() {
            return schema;
        }

        public String table() {
            return table;
        }
    }

    private static Unseeded unseeded(ModelContext ctx, String db, TestDataGenerator.Located loc, String table,
            @com.legend.base.Nullable String view) {
        String owner = owningDatabase(ctx, db, loc.def());
        return new Unseeded(unidentified(owner, loc, table, view), owner, loc.schema(), table);
    }

    /** The database that declares {@code def}, searched from {@code db} through its includes (upstream's
     *  {@code $table.schema.database}). */
    private static String owningDatabase(ModelContext ctx, String db, DatabaseDefinition.TableDefinition def) {
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>(List.of(db));
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (!work.isEmpty()) {
            String fqn = work.poll();
            if (!seen.add(fqn)) {
                continue;
            }
            DatabaseDefinition d = ctx.findDatabase(fqn).orElse(null);
            if (d == null) {
                continue;
            }
            if (d.tables().contains(def) || d.schemas().stream().anyMatch(s -> s.tables().contains(def))) {
                return fqn;
            }
            work.addAll(d.includes());
        }
        throw new IllegalStateException("testDataGen: no database of " + db + "'s includes declares " + def.name());
    }

    /** {@code name} without the double quotes a quoted name carries. */
    private static String bareName(String name) {
        return unquoted(name);
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
    private static SqlQuery csv(Walk walk, boolean hashStrings, boolean route) {
        if (walk.fetched.isEmpty()) {
            throw new IllegalStateException("testDataGen: a relation tree with no table");
        }
        List<Fetched> order = new ArrayList<>(walk.fetched.values());
        if (route) {
            // the server's routes' order: by schema and table, as upstream's writer sorts its blocks
            // (manageTestDataResult, :282: schemaName + '.' + relationName) -- the model's names, a planning constant
            order.sort(java.util.Comparator.comparing(f -> f.schema() + "." + f.table()));
        }
        List<SqlExpr> blocks = new ArrayList<>();
        SqlSource from = null;
        int n = 0;
        for (Fetched f : order) {
            // the route's header is its writer's (the B2d.3 audit's M1, 2026-11-12): each column's declared name, in
            // double quotes where it holds a space (getQuotedLabelIfContainSpace, TestDataGeneration.java:306), the
            // columns sorted by their names case-sensitively (the plan form's sortBy(x | $x.name), TDG:1043, :885);
            // the function's, its H2-folded header (upper case, sorted so)
            List<String> cs = route ? routeOrder(f.def(), f.cols()) : headerOrder(f.cols());
            String header = f.schema() + "\n" + f.table() + "\n"
                    + String.join(",", cs.stream().map(c -> route ? routeHeader(f.def(), c) : headerCase(c)).toList())
                    + "\n";
            String alias = "_t" + n++;
            SqlSource lines = new SqlSource.Subselect(lines(walk, f, cs, hashStrings), alias, null);
            from = from == null ? lines : new SqlSource.Join(from, lines, SqlSource.Join.Kind.CROSS, null);
            blocks.add(new SqlExpr.StringLit(header));
            // a table with no row is a blank line, as upstream writes it on both paths (the B2d.3 audit's M2,
            // 2026-11-12): the route's Collectors.joining("\n", "", "\n") and the function's joinStrings('', '\n',
            // '\n') answer their suffix for no row
            blocks.add(SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.derived(alias, LINES),
                    new SqlExpr.StringLit("\n")));
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

    // ===== the seed: the starting rows a query reaches (B2d.3; the user's ruling R2, 2026-11-11) =====

    /** The user's query lowered by the planner as a product query over the generator's mapping and runtime: the
     *  statement the seed carries its keys through. The planner's, not the generator's. */
    @FunctionalInterface
    public interface Queries {
        SqlQuery lowered(LambdaFunction query);
    }

    /** The seed of a query: its starting table (the one root, or a view root's main table: {@code db} names the
     *  database it is reached through), the table's keys, and {@code rows}, the statement whose rows are the keys
     *  chosen -- each column labelled as a fetch labels it ({@link #tempColumn}). */
    public record Seed(String db, TestDataGenerator.Located loc, String table, List<String> keys, SqlSelect rows) {
        public Seed {
            keys = List.copyOf(keys);
        }
    }

    /** The most keys a seed takes: upstream's {@code top 10}. */
    static final int SEED_LIMIT = 10;

    /**
     * The seed of {@code query} over {@code mappingFqn}: upstream's rule ({@code generateSeedDataGenerationFunction},
     * TDG:256-290; {@code columnProjectionsFromRoot}, pureToSQLQuery.pure:7328) -- the query's one root table (a view
     * root's main table, where {@code viewRoot} admits one: the server's route; the seed string refuses it, TDG:222),
     * its primary key (else its non-null columns but a text or binary one over 1000, else refused with upstream's
     * words), carried to the query's root select ({@link #carried}), DISTINCT -- and of those distinct keys, by the
     * user's ruling R2 ("Order by but pick some row numbers from beginning middle end"; register S198) rather than any
     * 10 the database picks: sorted by key, all of them where there are 10 or fewer, else the 10 at rows
     * {@code 1 + floor(i * (n - 1) / 9)}, {@code i = 0..9} -- the first, the last and 8 evenly between. Deterministic
     * on every database; the database does it.
     */
    public static Seed seed(ModelContext ctx, LambdaFunction query, String mappingFqn, boolean viewRoot,
            Queries queries) {
        List<ScanRelations.Rel> roots = ScanRelations.relTree(ctx, query, mappingFqn);
        if (roots.size() != 1) {
            throw new AssertFailed(viewRoot
                    ? "Seed data generation currently supports queries with a single root table/view (unions are not"
                            + " supported)"
                    : "Seed data generation currently supports queries with a single root (got: " + roots.size()
                            + ")");
        }
        // a set operation at the query's root is two roots for upstream's relation tree (one per branch), whose
        // lineage lite's relation tree does not split: refused with upstream's words, its count the branches' (the
        // B2d.3 audit's L6, 2026-11-12 -- until then refused as keys that cannot be carried)
        SqlQuery lowered = queries.lowered(query);
        if (lowered instanceof com.legend.sql.SqlUnion u) {
            throw new AssertFailed(viewRoot
                    ? "Seed data generation currently supports queries with a single root table/view (unions are not"
                            + " supported)"
                    : "Seed data generation currently supports queries with a single root (got: " + branches(u) + ")");
        }
        ScanRelations.Rel root = roots.get(0);
        String db = root.db();
        String table = java.util.Objects.requireNonNull(root.table(), "a root with no relation");
        if (ScanRelations.isView(ctx, db, table)) {
            if (!viewRoot) {
                // upstream's words (the B2d.3 audit's L9): the root node's type()->makeString(), the relation tree
                // node's class, whose name Pure's toString answers (TDG:222)
                throw new AssertFailed("Seed data generation currently supports queries with a root table"
                        + " (unions/views are not supported, got: RelationTree)");
            }
            ScanRelations.Rel base = ScanRelations.viewExpansion(ctx, db, table).tree();
            db = base.db();
            table = java.util.Objects.requireNonNull(base.table(), "a view with no main table");
        }
        TestDataGenerator.Located loc = locate(ctx, db, table);
        List<String> keys = seedKeys(loc.def(), table);
        SqlSelect carried = carried(lowered, physical(loc.schema(), table), loc.def(), keys);
        List<SqlSelect.Projection> distinct = new ArrayList<>(keys.size());
        List<String> labels = new ArrayList<>(keys.size());
        for (String k : keys) {
            String label = tempColumn(loc.def(), k);
            labels.add(label);
            distinct.add(new SqlSelect.Projection(SqlExpr.Column.derived(ROOT, carriedName(k)), label, null));
        }
        SqlSelect distinctKeys = new SqlSelect(distinct, true, new SqlSource.Subselect(carried, ROOT, null), null,
                List.of(), null, null, List.of(), null, null, List.of());
        List<SqlSelect.SortKey> byKeys = new ArrayList<>(labels.size());
        List<SqlSelect.Projection> ranked = new ArrayList<>(labels.size() + 2);
        for (int i = 0; i < labels.size(); i++) {
            String l = labels.get(i);
            // a text key ordered by its code points, whatever the column's collation (the B2d.3 audit's M4)
            byKeys.add(SqlSelect.SortKey.asc(byCodePoint(SqlExpr.Column.derived("_d", l),
                    column(loc.def(), keys.get(i)))));
            ranked.add(new SqlSelect.Projection(SqlExpr.Column.derived("_d", l), l, null));
        }
        ranked.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER,
                List.of()), List.of(), byKeys, null), "_rn", null));
        ranked.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                false, List.of()), List.of(), List.of(), null), "_n", null));
        SqlSelect rankedKeys = new SqlSelect(ranked, false, new SqlSource.Subselect(distinctKeys, "_d", null), null,
                List.of(), null, null, List.of(), null, null, List.of());
        SqlExpr rn = SqlExpr.Column.derived("_s", "_rn");
        SqlExpr n = SqlExpr.Column.derived("_s", "_n");
        SqlExpr chosen = SqlExpr.Call.of(SqlFn.LESS_EQUAL, n, new SqlExpr.IntLit(SEED_LIMIT));
        for (int i = 0; i < SEED_LIMIT; i++) {
            SqlExpr at = SqlExpr.Call.of(SqlFn.PLUS, new SqlExpr.IntLit(1), SqlExpr.Call.of(SqlFn.INT_DIVIDE,
                    SqlExpr.Call.of(SqlFn.TIMES, new SqlExpr.IntLit(i), new SqlExpr.Group(SqlExpr.Call.of(SqlFn.MINUS,
                            n, new SqlExpr.IntLit(1)))), new SqlExpr.IntLit(SEED_LIMIT - 1)));
            chosen = SqlExpr.Call.of(SqlFn.OR, chosen, SqlExpr.Call.of(SqlFn.EQUAL, rn, at));
        }
        List<SqlSelect.Projection> out = new ArrayList<>(labels.size());
        List<SqlSelect.SortKey> order = new ArrayList<>(labels.size());
        for (int i = 0; i < labels.size(); i++) {
            String l = labels.get(i);
            out.add(new SqlSelect.Projection(SqlExpr.Column.derived("_s", l), l, null));
            order.add(SqlSelect.SortKey.asc(byCodePoint(SqlExpr.Column.derived("_s", l),
                    column(loc.def(), keys.get(i)))));
        }
        SqlSelect rows = new SqlSelect(out, false, new SqlSource.Subselect(rankedKeys, "_s", null), chosen,
                List.of(), null, null, order, null, null, List.of());
        return new Seed(db, loc, table, keys, rows);
    }

    /**
     * {@code seed} as upstream's seed string ({@code generateSeedDataString}, TDG:216-230): Pure code naming the table
     * and its keys -- {@code \ncreateTableRowIdentifiers(<db>, '<schema>', '<table>', [\n}, a line per key row
     * {@code       createRowIdentifier(['<key>',...], [<value>,...])}, the lines sorted and joined by {@code ,\n},
     * then {@code \n  ])\n} -- each value as Pure's {@code toRepresentation} writes it (a text quoted, its {@code \},
     * {@code '} and line feed escaped; a whole number its digits), in SQL; one statement, its one cell the text. The
     * lines are sorted by the database's own order of their text, which is upstream's sort for any text of characters
     * up to U+FFFF (the user's ruling R4: register S198's note).
     */
    public static SqlQuery seedString(ModelContext ctx, Seed seed) {
        // the names are the table's PRIMARY KEY's, as upstream writes them ($mainTable.primaryKey, TDG:229) -- none
        // for a table with no primary key, whose values are its non-null columns' (the B2d.3 audit's L4)
        String names = seed.loc().def().columns().stream().filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                .map(c -> "'" + pureName(seed.loc().def(), c.name()) + "'")
                .collect(java.util.stream.Collectors.joining(","));
        List<SqlExpr> parts = new ArrayList<>();
        parts.add(new SqlExpr.StringLit("       meta::relational::testDataGeneration::createRowIdentifier([" + names
                + "], ["));
        for (int i = 0; i < seed.keys().size(); i++) {
            if (i > 0) {
                parts.add(new SqlExpr.StringLit(","));
            }
            String k = seed.keys().get(i);
            parts.add(representation(SqlExpr.Column.derived("_k", tempColumn(seed.loc().def(), k)),
                    column(seed.loc().def(), k), seed.table()));
        }
        parts.add(new SqlExpr.StringLit("])"));
        SqlExpr line = new SqlExpr.Call(SqlFn.CONCAT, parts, SqlTyping.UNKNOWN);
        // the lines in code-point order, whatever the database's collation (the B2d.3 audit's M4; R4's note, S198)
        SqlExpr lines = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG, List.of(line, new SqlExpr.StringLit(",\n")), false,
                List.of(SqlSelect.SortKey.asc(SqlExpr.Call.of(SqlFn.BINARY_COLLATE, line))));
        String owner = owningDatabase(ctx, seed.db(), seed.loc().def());
        SqlExpr text = new SqlExpr.Call(SqlFn.CONCAT, List.of(new SqlExpr.StringLit(
                "\nmeta::relational::testDataGeneration::createTableRowIdentifiers(" + owner + ", '"
                        + seed.loc().schema() + "', '" + seed.table() + "', [\n"),
                SqlExpr.Call.of(SqlFn.COALESCE, lines, new SqlExpr.StringLit("")),
                new SqlExpr.StringLit("\n  ])\n")), SqlTyping.UNKNOWN);
        return new SqlSelect(List.of(new SqlSelect.Projection(text, "seedDataString", null)), false,
                new SqlSource.Subselect(seed.rows(), "_k", null), null, List.of(), null, null, List.of(), null, null,
                List.of());
    }

    /**
     * {@code seed} as upstream's seed route answers it (B2d.3e; {@code generateSeedData},
     * {@code SeedDataGeneration.streamSeedDataResult}): the JSON object {@code {"table":{"_type":"table","table":T,
     * "schema":S,"database":D},"rowIdentifiers":[{"columnValuePairs":[{"name":k,"value":v},...]},...]}}, built by the
     * database -- each value its JSON for the key's cell, the identifiers in the keys' order (R2, register S198) --
     * one statement, its one cell the text. A key's name is its column's, in double quotes where it holds a space
     * (upstream's {@code getQuotedLabelIfContainSpace}).
     */
    public static SqlQuery seedJson(ModelContext ctx, Seed seed) {
        DatabaseDefinition.TableDefinition def = seed.loc().def();
        List<SqlExpr> pairs = new ArrayList<>(seed.keys().size());
        List<SqlExpr.JsonArrayAgg.Key> order = new ArrayList<>(seed.keys().size());
        for (String k : seed.keys()) {
            String name = bareName(k);
            SqlExpr cell = SqlExpr.Column.derived("_k", tempColumn(def, k));
            pairs.add(new SqlExpr.JsonObject(List.of(new SqlExpr.StringLit("name"), new SqlExpr.StringLit(
                    name.contains(" ") ? "\"" + name + "\"" : name), new SqlExpr.StringLit("value"), cell)));
            order.add(new SqlExpr.JsonArrayAgg.Key(byCodePoint(cell, column(def, k)), false));
        }
        SqlExpr identifier = new SqlExpr.JsonObject(List.of(new SqlExpr.StringLit("columnValuePairs"),
                new SqlExpr.JsonArray(pairs)));
        SqlExpr table = new SqlExpr.JsonObject(List.of(new SqlExpr.StringLit("_type"), new SqlExpr.StringLit("table"),
                new SqlExpr.StringLit("table"), new SqlExpr.StringLit(seed.table()), new SqlExpr.StringLit("schema"),
                new SqlExpr.StringLit(seed.loc().schema()), new SqlExpr.StringLit("database"),
                new SqlExpr.StringLit(owningDatabase(ctx, seed.db(), def))));
        SqlExpr answer = new SqlExpr.JsonObject(List.of(new SqlExpr.StringLit("table"), table,
                new SqlExpr.StringLit("rowIdentifiers"), new SqlExpr.JsonArrayAgg(identifier, order)));
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Cast(answer, SqlType.Scalar.VARCHAR),
                "seedData", null)), false, new SqlSource.Subselect(seed.rows(), "_k", null), null, List.of(), null,
                null, List.of(), null, null, List.of());
    }

    /**
     * {@code query} read through {@code mapping} on {@code runtime}, whatever mappings the runtime declares: its last
     * statement {@code ->from(mapping, runtime)}, as upstream executes the seed's function with the given mapping
     * ({@code execute($seedGenerationFunction, $mapping, $runtime, ...)}, TDG:226, :812; the B2d.3 audit's L5 -- until
     * then a runtime that did not declare the mapping was refused by name). A last statement that is already a
     * {@code from(...)} keeps its own. The call is written by {@code parser} (the caller's: the product's query parser)
     * and its first argument made the query's last statement by the one builder of calls after the resolver
     * ({@code Calls.like}): no protocol node is spelled by hand below the parser.
     */
    public static LambdaFunction underMapping(LambdaFunction query, String mapping, String runtime,
            java.util.function.Function<String, com.legend.protocol.spec.ValueSpecification> parser) {
        List<com.legend.protocol.spec.ValueSpecification> body = new ArrayList<>(query.body());
        com.legend.protocol.spec.ValueSpecification last = body.get(body.size() - 1);
        if (last instanceof com.legend.protocol.spec.AppliedFunction af && (af.function().equals("from")
                || af.function().equals(FROM))) {
            return query;
        }
        com.legend.protocol.spec.ValueSpecification written = parser.apply("|" + FROM + "([], " + mapping + ", "
                + runtime + ")");
        if (!(written instanceof LambdaFunction lf && lf.body().size() == 1
                && lf.body().get(0) instanceof com.legend.protocol.spec.AppliedFunction from)) {
            throw new IllegalStateException("testDataGen: the parser did not write " + FROM + "(...) as one call");
        }
        body.set(body.size() - 1, com.legend.compiler.spec.Calls.like(from, List.of(last, from.parameters().get(1),
                from.parameters().get(2))));
        return query.withBody(body);
    }

    /** Pure's {@code from(t, mapping, packageableRuntime)}, by its full name (a call built after the name resolver):
     *  the registry's overload a named mapping and runtime choose. */
    private static final String FROM = com.legend.builtin.Pure.FROM__T_m__MAPPING_1__PACKAGEABLE_RUNTIME_1
            .qualifiedName();

    /** A key value as Pure's {@code toRepresentation} writes it (legend-pure {@code toRepresentation.pure:40-51}), by
     *  its column's declared type: a text quoted, {@code \} {@code '} and line feed escaped; a whole number its
     *  digits; any other type refused by name (no test pins its spelling). */
    private static SqlExpr representation(SqlExpr v, DatabaseDefinition.ColumnDefinition c, String table) {
        if (isText(c.dataType())) {
            SqlExpr escaped = replace(replace(v, new SqlExpr.StringLit("\\"), "\\\\"), new SqlExpr.StringLit("'"),
                    "\\'");
            escaped = replace(escaped, SqlExpr.Call.of(SqlFn.CHR, new SqlExpr.IntLit(10)), "\\n");
            return new SqlExpr.Call(SqlFn.CONCAT, List.of(new SqlExpr.StringLit("'"), escaped,
                    new SqlExpr.StringLit("'")), SqlTyping.UNKNOWN);
        }
        return switch (c.dataType()) {
            case RelationalDataType.BigInt x -> new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR);
            case RelationalDataType.Integer_ x -> new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR);
            case RelationalDataType.SmallInt x -> new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR);
            case RelationalDataType.TinyInt x -> new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR);
            default -> throw new NotImplementedException("testDataGen: the seed string's key " + table + "."
                    + c.name() + " of type " + com.legend.plan.PlanText.spell(c.dataType()) + ": its Pure"
                    + " representation is not planned (B2d.3 writes a text key and a whole-number key)");
        };
    }

    /** A column's name as upstream's model writes it: a quoted name with its quotes. */
    private static String pureName(DatabaseDefinition.TableDefinition def, String c) {
        DatabaseDefinition.ColumnDefinition cd = column(def, c);
        return cd.quoted() ? "\"" + cd.name() + "\"" : cd.name();
    }

    /** The seed's key columns of {@code def}: its primary key in declaration order, else its non-null columns but a
     *  text or binary column longer than 1000 (TDG:266-271), else upstream's refusal (TDG:272). */
    static List<String> seedKeys(DatabaseDefinition.TableDefinition def, String table) {
        List<String> pk = def.columns().stream().filter(DatabaseDefinition.ColumnDefinition::primaryKey)
                .map(DatabaseDefinition.ColumnDefinition::name).toList();
        if (!pk.isEmpty()) {
            return pk;
        }
        List<String> nonNull = def.columns().stream().filter(c -> c.notNull() && !long1000(c.dataType()))
                .map(DatabaseDefinition.ColumnDefinition::name).toList();
        if (nonNull.isEmpty()) {
            throw new AssertFailed("Table : " + table + " must have atleast 1 primary key or non-null column to"
                    + " utilise this feature");
        }
        return nonNull;
    }

    private static boolean long1000(RelationalDataType t) {
        return t instanceof RelationalDataType.Varchar v && v.size() > 1000
                || t instanceof RelationalDataType.Varbinary b && b.size() > 1000;
    }

    /** {@code e}, a cell of column {@code c}, as the seed orders it: a text by its code points -- the dialect's
     *  binary collation ({@link SqlFn#BINARY_COLLATE}; the design's §3.1, ruled with R4; the B2d.3 audit's M4), never
     *  the column's or the database's own collation, refused by name on a dialect whose binary collation is not
     *  measured -- any other value as it is. By the column's declared type, at planning. */
    private static SqlExpr byCodePoint(SqlExpr e, DatabaseDefinition.ColumnDefinition c) {
        return isText(c.dataType()) ? SqlExpr.Call.of(SqlFn.BINARY_COLLATE, e) : e;
    }

    /** A carried key column's name: upstream's {@code gen_col_projection_<name>}, its quotes stripped, a space an
     *  underscore. */
    private static String carriedName(String key) {
        return "gen_col_projection_" + bareName(key).replace(' ', '_');
    }

    /**
     * {@code q} with the key columns {@code keys} of the table {@code physical} carried to its root select
     * (upstream's {@code recursiveAddColumnsToRoot}, pureToSQLQuery.pure:7371-7398): a select reading the table as
     * its first source projects them; a select reading a derived table first carries them up from it; each select on
     * the way that groups is grouped by them as well. A query whose root is none of these -- a set operation, a
     * clause the lowering extracted, a star select -- is refused by name.
     */
    private static SqlSelect carried(SqlQuery q, String physical, DatabaseDefinition.TableDefinition def,
            List<String> keys) {
        if (!(q instanceof SqlSelect s)) {
            throw notCarried("its statement is a " + q.getClass().getSimpleName());
        }
        if (s.projections().isEmpty()) {
            throw notCarried("its root select reads every column (a star)");
        }
        SqlSource first = leftmost(s.from());
        List<SqlExpr> read = new ArrayList<>(keys.size());
        SqlSource from = s.from();
        if (first instanceof SqlSource.Table t && !t.call() && t.name().equals(physical)) {
            for (String k : keys) {
                read.add(stored(t.alias(), def, k));
            }
        } else if (first instanceof SqlSource.Subselect sub) {
            SqlSelect inner = carried(sub.inner(), physical, def, keys);
            from = replaceLeftmost(s.from(), new SqlSource.Subselect(inner, sub.alias(), sub.frameName()));
            for (String k : keys) {
                read.add(SqlExpr.Column.derived(sub.alias(), carriedName(k)));
            }
        } else {
            throw notCarried("its root reads " + first.getClass().getSimpleName() + " first, not the table "
                    + physical);
        }
        List<SqlSelect.Projection> ps = new ArrayList<>(s.projections());
        // a select that reads every column of the derived table it carries the keys up from ({@code t1.*}) carries
        // them already: projected again they would be two columns of one name, which H2 refuses ("Duplicate column
        // name"; found by the B2d.3 audit's L6 test, 2026-11-12)
        boolean starOverSub = first instanceof SqlSource.Subselect sub && s.projections().stream().anyMatch(p ->
                p.expr() instanceof SqlExpr.Star star && (star.table() == null || star.table().equals(sub.alias())));
        for (int i = 0; i < keys.size() && !starOverSub; i++) {
            ps.add(new SqlSelect.Projection(read.get(i), carriedName(keys.get(i)), null));
        }
        List<SqlExpr> groupBy = s.groupBy();
        if (!groupBy.isEmpty()) {
            groupBy = new ArrayList<>(groupBy);
            groupBy.addAll(read);
        }
        return new SqlSelect(ps, s.distinct(), from, s.where(), groupBy, s.having(), s.qualify(), s.orderBy(),
                s.limit(), s.offset(), List.of());
    }

    /** The branches of {@code u}, a nested union's flattened. */
    private static int branches(com.legend.sql.SqlUnion u) {
        int n = 0;
        for (SqlQuery b : u.branches()) {
            n += b instanceof com.legend.sql.SqlUnion inner ? branches(inner) : 1;
        }
        return n;
    }

    private static SqlSource leftmost(SqlSource s) {
        return s instanceof SqlSource.Join j ? leftmost(j.left()) : s;
    }

    private static SqlSource replaceLeftmost(SqlSource s, SqlSource with) {
        return s instanceof SqlSource.Join j
                ? new SqlSource.Join(replaceLeftmost(j.left(), with), j.right(), j.kind(), j.on(), j.outputs())
                : with;
    }

    private static NotImplementedException notCarried(String why) {
        return new NotImplementedException("testDataGen: the seed's keys cannot be carried through the query: " + why
                + " (B2d.3 carries them through selects and derived tables, as upstream's columnProjectionsFromRoot)");
    }

    // ===== the comparison with an expected text (B2d.2: assertTestData, as upstream decides it) =====

    /** One block of an expected test-data text, as lite's reader reads it: its schema, table, header cells and rows,
     *  each cell as written. */
    public record Expected(String schema, String table, List<String> header, List<List<String>> rows) {
        public Expected {
            header = List.copyOf(header);
            rows = List.copyOf(rows);
        }
    }

    /** The comparison planned: {@code fails}, the failure's words, where planning decides it (upstream's load of the
     *  expected text fails, or the texts' rows differ whatever the database holds); else {@code holds}, a statement
     *  whose one Boolean cell says whether the rows are the same, and {@code differences}, a statement whose one cell
     *  names the rows that differ (lite's failure message). */
    public record Comparison(@com.legend.base.Nullable String fails, @com.legend.base.Nullable SqlQuery holds,
            @com.legend.base.Nullable SqlQuery differences) {
        public Comparison {
            if ((fails == null) == (holds == null) || (holds == null) != (differences == null)) {
                throw new IllegalArgumentException("a comparison is decided at planning or by two statements");
            }
        }
    }

    /**
     * Whether {@code planned}'s slice holds {@code expected}'s data on {@code dbFqn}'s tables, decided EXACTLY as
     * upstream decides it (the user's ruling of 2026-11-11, "Agreed"; the failure's words are lite's own, a register
     * row). Upstream's {@code assertTestData} reads each text into its load statements ({@code setUpDataSQLs},
     * {@code toDDL.pure:94-130}): per block, the schema ({@code toOne}), the table and each header column (case ignored,
     * matching quotes stripped, the name trimmed) looked up -- a miss fails with upstream's own words -- then one insert
     * per row whose cells are written by the column's type ({@code convertValuesToCsvH2},
     * {@code h2Extension2_1_214.pure:118-145}: an empty or {@code ---null---} cell null, a BIT cell by
     * {@code bitValueFromString}, any other cell its text) -- a row narrower than its header fails reading the missing
     * cell, a wider one's extra cells are not read -- and compares the two statement lists sorted
     * ({@code assertSameElements}): the same inserts, each as many times. So here: per table and header, each row's
     * cells under that rule, COUNTED on both sides in the database -- the generated rows the text's lines (its fetches'
     * distinct rows, each cell as the text writes it before quoting) -- every (row, count) of each side found in the
     * other; two anti-joins, null-safe. A header-only block writes no insert and compares nothing. Expected rows under a
     * header the generator does not write, or of a table it did not fetch, differ whatever the database holds: decided
     * at planning. Every failure's words begin with {@code lead}.
     */
    public static Comparison same(Planned planned, List<Expected> expected, ModelContext ctx, String dbFqn,
            String lead) {
        Map<String, Table> generated = new LinkedHashMap<>();
        for (Table t : planned.tables()) {
            generated.put(groupKey(t.schema(), t.table(), t.columns()), t);
        }
        // the expected rows by table and header, in text order (upstream's lookups fail in that order)
        Map<String, List<List<String>>> rows = new LinkedHashMap<>();
        Map<String, String> names = new LinkedHashMap<>();
        for (Expected e : expected) {
            boolean defaultSchema = "default".equals(e.schema());
            if (!defaultSchema && !hasSchema(ctx, dbFqn, e.schema(), new java.util.HashSet<>())) {
                return fails(lead + "There should be exactly 1 schema with name " + e.schema() + " found in the"
                        + " provided runtime database(s) [" + dbFqn + "] at the given connection.");
            }
            // the default schema's table by its schema too: a bare name would find a named schema's table of that name
            // (upstream's schema('default')->table(T) does not; the B2d.3 audit's L1)
            DatabaseDefinition.TableDefinition def = ctx.findTableDefinition(dbFqn, e.schema() + "." + e.table())
                    .orElse(null);
            if (def == null) {
                return fails(lead + "Table \"" + e.table() + "\" not found in Schema \"" + e.schema()
                        + "\" in Database(s) " + dbFqn);
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
                    return fails(lead + "Column \"" + h + "\" not found in Table \"" + e.table()
                            + "\" in Database(s) " + dbFqn);
                }
                header.add(col.name());
            }
            String key = groupKey(e.schema(), e.table(), header);
            names.putIfAbsent(key, e.schema() + "." + e.table() + " (" + String.join(",", header) + ")");
            List<List<String>> rs = rows.computeIfAbsent(key, k -> new ArrayList<>());
            for (int r = 0; r < e.rows().size(); r++) {
                List<String> row = e.rows().get(r);
                if (row.size() < header.size()) {
                    return fails(lead + "refused: the expected test data's row " + (r + 1) + " of " + e.schema()
                            + "." + e.table() + " has " + row.size() + " cell(s) for " + header.size() + " column(s)"
                            + " (upstream's load fails reading the missing cell)");
                }
                rs.add(row.subList(0, header.size()));
            }
        }
        for (Map.Entry<String, List<List<String>>> e : rows.entrySet()) {
            if (!e.getValue().isEmpty() && !generated.containsKey(e.getKey())) {
                return fails(lead + "the generated test data writes no rows of " + names.get(e.getKey()) + ": "
                        + e.getValue().size() + " expected");
            }
        }
        List<SqlQuery> unmatched = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        List<Integer> widths = new ArrayList<>();
        for (Map.Entry<String, Table> e : generated.entrySet()) {
            Table g = e.getValue();
            SqlQuery got = counted(generatedCells(g, planned.hashStrings()), g, "_g");
            List<List<String>> want = rows.getOrDefault(e.getKey(), List.of());
            String label = g.schema() + "." + g.table() + " (" + String.join(",", g.columns()) + ")";
            if (want.isEmpty()) {
                unmatched.add(got);
                labels.add(label + " generated");
                widths.add(g.columns().size());
                continue;
            }
            SqlQuery wanted = counted(expectedCells(want, g), g, "_x");
            unmatched.add(antiJoin(wanted, got, g.columns().size()));
            labels.add(label + " expected");
            widths.add(g.columns().size());
            unmatched.add(antiJoin(got, wanted, g.columns().size()));
            labels.add(label + " generated");
            widths.add(g.columns().size());
        }
        SqlExpr all = null;
        SqlSource counts = null;
        SqlSource texts = null;
        List<SqlExpr> message = new ArrayList<>();
        message.add(new SqlExpr.StringLit(lead + "the generated test data's rows are not the expected's (each table's rows"
                + " counted, upstream's comparison); the rows of one side the other does not hold as often:\n"));
        for (int k = 0; k < unmatched.size(); k++) {
            int width = widths.get(k);
            SqlSelect n = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(),
                    false, List.of()), "n", null)), false, new SqlSource.Subselect(unmatched.get(k), "_u", null), null,
                    List.of(), null, null, List.of(), null, null, List.of());
            SqlSource dk = new SqlSource.Subselect(n, "_d" + k, null);
            counts = counts == null ? dk : new SqlSource.Join(counts, dk, SqlSource.Join.Kind.CROSS, null);
            all = and(all, SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.derived("_d" + k, "n"), new SqlExpr.IntLit(0)));
            List<SqlExpr> parts = new ArrayList<>();
            parts.add(new SqlExpr.StringLit(labels.get(k) + " x"));
            parts.add(new SqlExpr.Cast(SqlExpr.Column.derived("_u", COUNT), SqlType.Scalar.VARCHAR));
            parts.add(new SqlExpr.StringLit(": "));
            for (int i = 0; i < width; i++) {
                if (i > 0) {
                    parts.add(new SqlExpr.StringLit(","));
                }
                parts.add(SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.derived("_u", "v" + i),
                        new SqlExpr.StringLit("---null---")));
            }
            parts.add(new SqlExpr.StringLit("\n"));
            SqlExpr line = new SqlExpr.Call(SqlFn.CONCAT, parts, SqlTyping.UNKNOWN);
            SqlSelect lines = new SqlSelect(List.of(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                    List.of(line, new SqlExpr.StringLit("")), false, List.of(SqlSelect.SortKey.asc(line))), "t", null)),
                    false, new SqlSource.Subselect(unmatched.get(k), "_u", null), null, List.of(), null, null,
                    List.of(), null, null, List.of());
            SqlSource tk = new SqlSource.Subselect(lines, "_t" + k, null);
            texts = texts == null ? tk : new SqlSource.Join(texts, tk, SqlSource.Join.Kind.CROSS, null);
            message.add(SqlExpr.Call.of(SqlFn.COALESCE, SqlExpr.Column.derived("_t" + k, "t"),
                    new SqlExpr.StringLit("")));
        }
        SqlExpr verdict = new SqlExpr.Case(List.of(new SqlExpr.Case.When(java.util.Objects.requireNonNull(all,
                "a comparison of no table"), new SqlExpr.BoolLit(true))), new SqlExpr.BoolLit(false));
        SqlSelect holds = new SqlSelect(List.of(new SqlSelect.Projection(verdict, "same", null)), false,
                java.util.Objects.requireNonNull(counts), null, List.of(), null, null, List.of(), null, null,
                List.of(new com.legend.sql.OutputCol("same", SqlType.Scalar.BOOLEAN, false)));
        SqlSelect differences = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.Call(SqlFn.CONCAT,
                message, SqlTyping.UNKNOWN), "differences", null)), false, java.util.Objects.requireNonNull(texts),
                null, List.of(), null, null, List.of(), null, null, List.of());
        // each table's rows read from a derived table in the FROM, never a subquery in an expression: H2's nested WITH
        // (csv's javadoc)
        return new Comparison(null, SqlWith.prepend(planned.clauses(), holds),
                SqlWith.prepend(planned.clauses(), differences));
    }

    private static Comparison fails(String why) {
        return new Comparison(why, null, null);
    }

    /** A group of rows: a table and the columns its rows are written under, in order. */
    private static String groupKey(String schema, String table, List<String> columns) {
        return schema + "\n" + table + "\n" + String.join("\n", columns);
    }

    /** Whether {@code dbFqn} (or a database it includes) declares the schema {@code name}. */
    private static boolean hasSchema(ModelContext ctx, String dbFqn, String name, java.util.Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return false;
        }
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return false;
        }
        for (DatabaseDefinition.SchemaDefinition sc : db.schemas()) {
            if (sc.name().equals(name)) {
                return true;
            }
        }
        for (String include : db.includes()) {
            if (hasSchema(ctx, include, name, seen)) {
                return true;
            }
        }
        return false;
    }

    /** {@code name} with a pair of matching double quotes around it stripped (upstream's stripMatchingQuotes). */
    private static String unquoted(String name) {
        return name.length() >= 2 && name.startsWith("\"") && name.endsWith("\"")
                ? name.substring(1, name.length() - 1) : name;
    }

    /** {@code g}'s rows as its text writes them -- its fetches' distinct rows, each cell's text before quoting (a text
     *  cell hashed where the text hashes it, NULL where the text writes {@code ---null---}) -- each cell by upstream's
     *  rule ({@link #upstreamCell}). */
    private static SqlQuery generatedCells(Table g, boolean hashStrings) {
        List<SqlSelect.Projection> ps = new ArrayList<>(g.columns().size());
        for (int i = 0; i < g.columns().size(); i++) {
            String c = g.columns().get(i);
            ps.add(new SqlSelect.Projection(upstreamCell(new SqlExpr.Cast(sliceCell(g.def(), c, "_u", hashStrings),
                    SqlType.Scalar.VARCHAR), column(g.def(), c).dataType()), "v" + i, null));
        }
        return new SqlSelect(ps, false, new SqlSource.Subselect(fetchedRows(g.clauses()), "_u", null), null,
                List.of(), null, null, List.of(), null, null, List.of());
    }

    /** The expected rows {@code rows}, each cell's text as written, by upstream's rule ({@link #upstreamCell}). */
    private static SqlQuery expectedCells(List<List<String>> rows, Table g) {
        int width = g.columns().size();
        List<String> columns = new ArrayList<>(width);
        List<com.legend.sql.OutputCol> outputs = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            columns.add("c" + i);
            outputs.add(new com.legend.sql.OutputCol("c" + i, SqlType.Scalar.VARCHAR, true));
        }
        List<List<SqlExpr>> values = new ArrayList<>(rows.size());
        for (List<String> r : rows) {
            List<SqlExpr> cells = new ArrayList<>(width);
            for (String cell : r) {
                cells.add(new SqlExpr.StringLit(cell));
            }
            values.add(cells);
        }
        List<SqlSelect.Projection> ps = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            ps.add(new SqlSelect.Projection(upstreamCell(SqlExpr.Column.derived("_e", "c" + i),
                    column(g.def(), g.columns().get(i)).dataType()), "v" + i, null));
        }
        return new SqlSelect(ps, false, new SqlSource.Values(values, columns, "_e", outputs), null, List.of(), null,
                null, List.of(), null, null, List.of());
    }

    /**
     * A cell's text {@code x} as upstream's insert writes it ({@code convertValuesToCsvH2},
     * {@code h2Extension2_1_214.pure:118-145}), by its column's type {@code t}: an empty or {@code ---null---} cell (or a
     * NULL) null; a BIT cell by {@code bitValueFromString} -- its text trimmed and lower-cased {@code y}, {@code 1} or
     * {@code true} is {@code true}, {@code n}, {@code 0} or {@code false} is {@code false}, any other its text as
     * written; any other cell its text (the quoting of a text, date or timestamp cell writes one insert per text, so
     * the text decides). The database computes it.
     */
    private static SqlExpr upstreamCell(SqlExpr x, RelationalDataType t) {
        SqlExpr absent = SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.IS_NULL, x),
                SqlExpr.Call.of(SqlFn.EQUAL, x, new SqlExpr.StringLit(""))),
                SqlExpr.Call.of(SqlFn.EQUAL, x, new SqlExpr.StringLit("---null---")));
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        whens.add(new SqlExpr.Case.When(new SqlExpr.Group(absent), new SqlExpr.Cast(new SqlExpr.NullLit(),
                SqlType.Scalar.VARCHAR)));
        if (t instanceof RelationalDataType.Bit) {
            SqlExpr bit = SqlExpr.Call.of(SqlFn.LOWER, SqlExpr.Call.of(SqlFn.TRIM, x));
            whens.add(new SqlExpr.Case.When(oneOf(bit, "y", "1", "true"), new SqlExpr.StringLit("true")));
            whens.add(new SqlExpr.Case.When(oneOf(bit, "n", "0", "false"), new SqlExpr.StringLit("false")));
        }
        return new SqlExpr.Case(whens, x);
    }

    /** Whether {@code x} is one of {@code texts}. */
    private static SqlExpr oneOf(SqlExpr x, String... texts) {
        SqlExpr out = null;
        for (String t : texts) {
            SqlExpr eq = SqlExpr.Call.of(SqlFn.EQUAL, x, new SqlExpr.StringLit(t));
            out = out == null ? eq : SqlExpr.Call.of(SqlFn.OR, out, eq);
        }
        return new SqlExpr.Group(java.util.Objects.requireNonNull(out));
    }

    /** {@code cells}' rows (columns {@code v0..}) counted: each distinct row, how many times ({@link #COUNT}), beside
     *  the marker column the anti-joins read. */
    private static SqlQuery counted(SqlQuery cells, Table g, String alias) {
        int width = g.columns().size();
        List<SqlSelect.Projection> ps = new ArrayList<>(width + 2);
        List<SqlExpr> by = new ArrayList<>(width);
        for (int i = 0; i < width; i++) {
            SqlExpr v = SqlExpr.Column.derived(alias, "v" + i);
            ps.add(new SqlSelect.Projection(v, "v" + i, null));
            by.add(v);
        }
        ps.add(new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(), false, List.of()), COUNT, null));
        ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(1), MARKER, null));
        return new SqlSelect(ps, false, new SqlSource.Subselect(cells, alias, null), null, by, null, null, List.of(),
                null, null, List.of());
    }

    /** {@code (SELECT l.v0.., l.n, l.m FROM (left) l LEFT JOIN (right) r ON l.v0 <=> r.v0 .. AND l.n = r.n WHERE
     *  r.m IS NULL)}: the counted rows of {@code left} that {@code right} does not hold as often. */
    private static SqlQuery antiJoin(SqlQuery left, SqlQuery right, int width) {
        SqlExpr on = SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.derived("_l", COUNT),
                SqlExpr.Column.derived("_r", COUNT));
        List<SqlSelect.Projection> ps = new ArrayList<>(width + 2);
        for (int i = 0; i < width; i++) {
            on = SqlExpr.Call.of(SqlFn.AND, on, SqlExpr.Call.of(SqlFn.NULL_SAFE_EQUAL,
                    SqlExpr.Column.derived("_l", "v" + i), SqlExpr.Column.derived("_r", "v" + i)));
            ps.add(new SqlSelect.Projection(SqlExpr.Column.derived("_l", "v" + i), "v" + i, null));
        }
        ps.add(new SqlSelect.Projection(SqlExpr.Column.derived("_l", COUNT), COUNT, null));
        ps.add(new SqlSelect.Projection(SqlExpr.Column.derived("_l", MARKER), MARKER, null));
        return new SqlSelect(ps, false, new SqlSource.Join(new SqlSource.Subselect(left, "_l", null),
                new SqlSource.Subselect(right, "_r", null), SqlSource.Join.Kind.LEFT, on),
                SqlExpr.Call.of(SqlFn.IS_NULL, SqlExpr.Column.derived("_r", MARKER)), List.of(), null, null,
                List.of(), null, null, List.of());
    }

    /** The marker column the anti-joins read: a row's presence. */
    private static final String MARKER = "_m";

    /** A counted row's count column. */
    private static final String COUNT = "_n";

    // ===== the slice, reloaded read-only (B2d.2: loadAndTestExecution) =====

    /**
     * Every table of the database {@code dbFqn} (its includes' too) as the generated slice reloaded holds it, by the
     * table's physical name ({@code TABLE} in the default schema, else {@code SCHEMA.TABLE}): the user's ruling of
     * 2026-11-11, "Read only reload is fine". Upstream's {@code loadAndTestExecution} drops and re-creates EVERY table of
     * the database ({@code setUpDataSQLs} -> {@code schemaAndTableSetup(dbConfig, [])}, {@code toDDL.pure:155-196}) and
     * loads the generated text into it; here each table is a statement the query reads instead: a table the slice
     * holds, a row for each of the text's rows (its fetches' distinct rows; upstream inserts each text row, so two that
     * only its reading of an empty or ---null--- cell makes equal stay two: the B checkpoint's G4, 2026-11-12) -- each
     * fetched column as the text holds it (a text cell hashed where the text hashes it), every other column NULL of its
     * declared type, each labelled by the column's declared name, which the swap reads it by (lowering/TableSwaps); any
     * other table, no row. Each statement is self-contained: the generator's clauses read inline as derived tables, so
     * it stands wherever the table stood.
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
                        ? reloaded(sliceCell(def, c.name(), "_u", planned.hashStrings()), c, sqlType(c, def))
                        : new SqlExpr.Cast(new SqlExpr.NullLit(), sqlType(c, def));
                // labelled by the column's declared name: the swap reads each column by it (lowering/TableSwaps)
                ps.add(new SqlSelect.Projection(cell, c.name(), null));
            }
            SqlSelect rows = g == null
                    ? new SqlSelect(ps, false, new SqlSource.Dual(), new SqlExpr.BoolLit(false), List.of(), null,
                            null, List.of(), null, null, List.of())
                    : new SqlSelect(ps, false, new SqlSource.Subselect(inline(fetchedRows(g.clauses()), inlined), "_u",
                            null), null, List.of(), null, null, List.of(), null, null, List.of());
            out.put(e.getKey(), rows);
        }
        return out;
    }

    /** A text cell {@code cell} of column {@code c} as upstream's reload writes it: an empty or {@code ---null---}
     *  text NULL ({@code convertValuesToCsvH2}, {@code h2Extension2_1_214.pure:118-127}: the reload reads the text,
     *  and those cells load as null; B2d.2's audit, M4); any other cell as it is. */
    private static SqlExpr reloaded(SqlExpr cell, DatabaseDefinition.ColumnDefinition c, SqlType type) {
        if (!isText(c.dataType())) {
            return cell;
        }
        SqlExpr absent = SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.EQUAL, cell, new SqlExpr.StringLit("")),
                SqlExpr.Call.of(SqlFn.EQUAL, cell, new SqlExpr.StringLit("---null---")));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(new SqlExpr.Group(absent), new SqlExpr.Cast(
                new SqlExpr.NullLit(), type))), cell);
    }

    /**
     * The table identities a reload of {@code dbFqn} replaces, each {@code store|physical name}: a database's reference
     * to a table of the slice's physical name whose one declaration in that database's include closure is the reloaded
     * closure's own declaration of it -- the reloaded database's tables, an included database's, and an including
     * database's reference to either (upstream's reload drops and re-creates the table itself, whatever database
     * names it; the audit's M1: by typed identity, never by a bare name). A reference of the slice's physical name
     * that is none of these is refused by name where it is lowered ({@code lowering/TableSwaps}).
     */
    public static java.util.Set<String> reads(ModelContext ctx, String dbFqn) {
        Map<String, String> owners = new LinkedHashMap<>();
        tablesOf(ctx, dbFqn, new java.util.HashSet<>(), new LinkedHashMap<>(), owners);
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (DatabaseDefinition d : ctx.databases().toList()) {
            Map<String, List<String>> declared = new java.util.HashMap<>();
            declarations(ctx, d.qualifiedName(), new java.util.HashSet<>(), declared);
            for (Map.Entry<String, String> e : owners.entrySet()) {
                List<String> by = declared.get(e.getKey());
                if (by != null && by.size() == 1 && by.get(0).equals(e.getValue())) {
                    out.add(d.qualifiedName() + "|" + e.getKey());
                }
            }
        }
        return java.util.Collections.unmodifiableSet(out);
    }

    /** Each physical name the include closure of {@code dbFqn} declares, with the database of each declaration. */
    private static void declarations(ModelContext ctx, String dbFqn, java.util.Set<String> seen,
            Map<String, List<String>> out) {
        if (!seen.add(dbFqn)) {
            return;
        }
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElseThrow(() -> new IllegalStateException(
                "testDataGen: no database '" + dbFqn + "', an include of the model's"));
        // the default schema's own tables: the flat list holds every schema's too, by bare name (the B checkpoint
        // audit's G2, 2026-11-11)
        for (DatabaseDefinition.TableDefinition t : db.defaultSchemaTables()) {
            out.computeIfAbsent(t.name(), k -> new ArrayList<>()).add(dbFqn);
        }
        for (DatabaseDefinition.SchemaDefinition sc : db.schemas()) {
            for (DatabaseDefinition.TableDefinition t : sc.tables()) {
                out.computeIfAbsent(physical(sc.name(), t.name()), k -> new ArrayList<>()).add(dbFqn);
            }
        }
        for (String include : db.includes()) {
            declarations(ctx, include, seen, out);
        }
    }

    /** A table's physical name: {@code TABLE} in the default schema, else {@code SCHEMA.TABLE}. */
    private static String physical(String schema, String table) {
        return "default".equals(schema) ? table : schema + "." + table;
    }

    /** Every table of {@code dbFqn} and of the databases it includes, by physical name. Two tables of one physical
     *  name in the closure are refused by name: the reload's rows of that name would be ambiguous (B2d.2's audit, M1;
     *  upstream's {@code allSchemas} merges a schema's tables across includes by the table). */
    private static Map<String, DatabaseDefinition.TableDefinition> tablesOf(ModelContext ctx, String dbFqn,
            java.util.Set<String> seen) {
        Map<String, DatabaseDefinition.TableDefinition> out = new LinkedHashMap<>();
        Map<String, String> owners = new LinkedHashMap<>();
        tablesOf(ctx, dbFqn, seen, out, owners);
        return out;
    }

    private static void tablesOf(ModelContext ctx, String dbFqn, java.util.Set<String> seen,
            Map<String, DatabaseDefinition.TableDefinition> out, Map<String, String> owners) {
        if (!seen.add(dbFqn)) {
            return;
        }
        DatabaseDefinition db = ctx.findDatabase(dbFqn).orElseThrow(() -> new IllegalStateException(
                "testDataGen: no database '" + dbFqn + "' to reload the slice into"));
        // the default schema's own tables, each schema's under its own name below (G2)
        for (DatabaseDefinition.TableDefinition t : db.defaultSchemaTables()) {
            add(out, owners, t.name(), t, dbFqn);
        }
        for (DatabaseDefinition.SchemaDefinition sc : db.schemas()) {
            for (DatabaseDefinition.TableDefinition t : sc.tables()) {
                add(out, owners, physical(sc.name(), t.name()), t, dbFqn);
            }
        }
        for (String include : db.includes()) {
            tablesOf(ctx, include, seen, out, owners);
        }
    }

    private static void add(Map<String, DatabaseDefinition.TableDefinition> out, Map<String, String> owners,
            String name, DatabaseDefinition.TableDefinition t, String dbFqn) {
        String owner = owners.putIfAbsent(name, dbFqn);
        if (owner != null) {
            throw new NotImplementedException("the test-data generator's slice reloaded: the table "
                    + name + " is declared by " + owner + " and by " + dbFqn + ", both in the reloaded database's"
                    + " includes: not planned (B2d reloads a table by one declaration)");
        }
        out.put(name, t);
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
            java.util.function.Function<String, SqlExpr> parentDef, String childTable, String childAlias,
            DatabaseDefinition.TableDefinition childDef, String joinName) {
        return switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                String t = bare(cr.table());
                if (t.equals(parentTable)) {
                    yield parentDef.apply(cr.column());
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
