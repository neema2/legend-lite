// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import com.legend.protocol.sql.InputDialect.Problem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A Postgres query ({@link SqlParser}'s tree) to a Pure Relation expression: the {@code #SQL{...}#} island's meaning
 * (docs/LEGEND_SQL_STUDY_2026_10_11.md §3). It builds the protocol tree a person would write in Pure, before name
 * resolution, as the parser does; the compiler resolves and types it like any query. It never types anything itself:
 * what a FROM source's columns are, it asks the caller ({@link Sources}), and the compiler types the whole result once.
 *
 * <p>Postgres is the spec. Where a Postgres construct has a Pure spelling with the same meaning, it is translated;
 * where it has none yet, it is refused by name and position, never approximated (AGENTS.md invariant 4). The refusals
 * that are Postgres's own ("column must appear in the GROUP BY clause") carry Postgres's wording.
 *
 * <p><b>Names</b> (the study's D1, 2026-10-11): a quoted identifier matches exactly; an unquoted one matches the name
 * equal to its Postgres folding (lowercase) when there is one, else the one name equal to it ignoring case; two such
 * names are an ambiguity to quote. Every query Postgres accepts resolves as Postgres resolves it; a camelCase model
 * name resolves unquoted too. Output names keep the case they are written in.
 *
 * <p><b>Dialects</b>: what a SQL dialect's reader and rules differ on -- its parse, its names, where NULLs sort, an
 * unnamed column's name, constants' and columns' types, operators, CASE's one type, its functions, casts, LIKE, the
 * wording of its own refusals -- is its {@link InputDialect}'s. Postgres's is {@link PostgresDialect}, the
 * {@code #SQL{...}#} island's ({@link InputLanguages}).
 *
 * <p>Step 1 of the study's phase 1: one FROM source, {@code tb('pkg::Database.schema.table')}.
 */
public final class SqlToPure {

    /** What a FROM source's columns are: the caller types the source; the translator never types. */
    public interface Sources {
        /** Reports the columns of {@code source}, an unresolved relation expression, to {@code column}, in order. */
        void columns(ValueSpecification source, Column column);

        /** The writes design's leg 4 (2026-10-20): the columns of the primary key of the table {@code table} of the
         *  Database {@code database} ({@code schema.table}, or a table of its default schema), as the store declares
         *  them (none where it declares none); null where the table is not known (the caller compiles against no
         *  model, or its model declares no such table: PR #67's review, 2026-10-22, an unknown table had read as one
         *  of no key). */
        default @com.legend.base.Nullable List<String> primaryKey(String database, String table) {
            return null;
        }

        /** IQ island, 2026-10-14 (M1e): reports the model's tables a dialect may name without a source function
         *  ({@link InputDialect#table}) to {@code table}: none unless the caller compiles against a model. */
        default void tables(Table table) {
        }

        /** IQ island, 2026-10-14 (M1e): reports the type of the enclosing Pure query's variable {@code name} in scope to
         *  {@code type}; false when none of that name is ({@link InputDialect#parameter}): none unless the caller types
         *  in a scope. */
        default boolean variable(String name, Scoped type) {
            return false;
        }

        /** IQ island, 2026-10-14 (review 5's m1): reports the names of the enclosing Pure query's variables in scope to
         *  {@code names}, so that a dialect may match a host variable's name as its database does (IQ: without case):
         *  none unless the caller types in a scope. */
        default void variables(java.util.function.Consumer<String> names) {
        }

        /** IQ island, 2026-10-16: {@code relation}, a derived table's or a common table expression's whose own query
         *  computes window functions, as an inline view -- its windows computed over all of its rows, before the
         *  enclosing query's WHERE, as SQL computes them (the compiler's marker, Pure.Lite.INLINE_VIEW; Pure's
         *  relation composition would fold a later filter under them). Itself unless the caller compiles. */
        default ValueSpecification inlineView(ValueSpecification relation) {
            return relation;
        }
    }

    /** IQ island, 2026-10-14 (M1e): a table of the model ({@link Sources#tables}): its Database's path, its schema's
     *  name ({@code default} for a table declared outside any schema) and its own, as the model spells them. */
    @FunctionalInterface
    public interface Table {
        void table(String database, String schema, String name);
    }

    /** IQ island, 2026-10-14 (M1e): a Pure variable's type ({@link Sources#variable}): its type's name, as a source
     *  column's ({@link Column}), whether it may hold no value (multiplicity lower bound 0) and whether it may hold
     *  more than one (upper bound above 1, or not known). */
    @FunctionalInterface
    public interface Scoped {
        void variable(String type, boolean nullable, boolean many);
    }

    /** IQ island, 2026-10-14 (M1e): a table {@link Sources#tables} reports. */
    public record CatalogTable(String database, String schema, String table) {
    }

    /** IQ island, 2026-10-14 (M1e): a variable's type {@link Sources#variable} reports. */
    public record VariableType(String type, boolean nullable, boolean many) {
    }

    /** IQ island, 2026-10-14 (M1e): the tables {@code sources} reports. */
    public static List<CatalogTable> tables(Sources sources) {
        List<CatalogTable> out = new ArrayList<>();
        sources.tables((database, schema, name) -> out.add(new CatalogTable(database, schema, name)));
        return out;
    }

    /** IQ island, 2026-10-14 (M1e): the type {@code sources} reports for the variable {@code name}, or null. */
    public static @com.legend.base.Nullable VariableType variable(Sources sources, String name) {
        VariableType[] found = new VariableType[1];
        boolean scoped = sources.variable(name, (type, nullable, many) -> found[0] = new VariableType(type, nullable,
                many));
        return scoped ? found[0] : null;
    }

    /** IQ island, 2026-10-14 (review 5's m1): the names of the variables {@code sources} reports in scope. */
    public static List<String> variables(Sources sources) {
        List<String> out = new ArrayList<>();
        sources.variables(out::add);
        return out;
    }

    /** One source column: its name as the model spells it, its Pure type's name ({@code Integer},
     *  {@code Decimal(10,2)}, {@code String}, {@code StrictDate}, an enumeration's path, ...), whether it may hold
     *  no value (SQL's NULL; Pure's {@code [0..1]}), and its declared store type as the store spells it
     *  ({@code SMALLINT}, {@code CHAR(5)}, {@code NUMERIC(10,2)}), or null when the source has none (a relation that
     *  is no table). A dialect reads it where the Pure type does not say enough ({@link InputDialect#detailOf}). */
    @FunctionalInterface
    public interface Column {
        void column(String name, String type, boolean nullable, @com.legend.base.Nullable String storeType);

        /** A column of no declared store type. */
        default void column(String name, String type, boolean nullable) {
            column(name, type, nullable, null);
        }
    }

    /** The one table function this step reads: {@code tb('pkg::Database.schema.table')}, upstream's dynamic source. */
    private static final String TABLE_FUNCTION = "tb";

    private static final String NULL_ORDER = "meta::pure::functions::relation::NullOrder";
    private static final String JOIN_KIND = "meta::pure::functions::relation::JoinKind";
    private static final String SYNTHETIC = "_sql_";

    private final Sources sources;
    final InputDialect dialect;
    private final Env env;
    /** This query level's lambda parameters: a row, a group, a join's two sides, a subquery's row. Each level has
     *  its own, so a correlated subquery's lambdas can reach the enclosing query's row. */
    final String row;
    /** IQ island, 2026-10-17: the scalar subqueries the item being translated computes once ({@link #output}), by
     *  node, each the column computed before it. */
    private final Map<SqlAst.Expr, Col> hoisted = new java.util.IdentityHashMap<>();
    /** IQ island, 2026-10-17: the grouping being translated's dialect guards ({@link Aggregation#guard}). */
    private final List<ValueSpecification> guards = new ArrayList<>();
    final String group;
    private final String leftRow;
    private final String rightRow;
    /** A subquery's row, in the {@code exists} and {@code map} that read an expression subquery. */
    private final String sub;
    /** A window column's partition and window parameters: {@code {p, w, r | ...}}. */
    private final String partition;
    private final String window;
    private int fresh;
    /** The operands of integer arithmetic over constants, whose range the arithmetic around them holds
     *  ({@link PostgresCasts#heldToWidth}). */
    final Set<SqlAst.Expr> heldOutside = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    private SqlToPure(Sources sources, InputDialect dialect, Env env) {
        this.sources = sources;
        this.dialect = dialect;
        this.env = env;
        String level = env.level() == 0 ? "" : Integer.toString(env.level());
        this.row = "r" + level;
        this.group = "y" + level;
        this.leftRow = "a" + level;
        this.rightRow = "b" + level;
        this.sub = "s" + level;
        this.partition = "p" + level;
        this.window = "w" + level;
    }

    /** What a query level sees beyond its own FROM: the WITH queries in force, the enclosing query's scope and the
     *  lambda parameter its row is in (for a correlated subquery), and how deep it is. */
    private record Env(Map<String, Rel> ctes, @com.legend.base.Nullable Scope outer,
            @com.legend.base.Nullable String outerRow, int level) {

        /** The same level with {@code more} WITH queries in force (a FROM subquery, a WITH query or body). */
        Env with(Map<String, Rel> more) {
            return new Env(java.util.Collections.unmodifiableMap(new LinkedHashMap<>(more)), outer, outerRow, level);
        }
    }

    /** A translated query: its relation and its output columns. */
    private record Rel(ValueSpecification vs, List<OutCol> columns) {
    }

    /** A FROM item a dialect reads as a relation of its own ({@link InputDialect#source}): its Pure relation, and its
     *  columns in order. */
    public record Relation(ValueSpecification vs, List<RelationColumn> columns) {
        public Relation {
            columns = List.copyOf(columns);
        }
    }

    /** A column of a dialect's own relation: its name, what is known of its type, whether it may be NULL, and what the
     *  dialect knows of its type beyond its kind. */
    public record RelationColumn(String name, Kind kind, boolean nullable,
            InputDialect.@com.legend.base.Nullable TypeDetail detail) {
    }

    /** What is known of a query's column's type: its kind, whether it may be NULL, and what the dialect knows of it
     *  beyond its kind ({@link InputDialect#setColumn}). */
    public record ColumnType(Kind kind, boolean nullable, InputDialect.@com.legend.base.Nullable TypeDetail detail) {
    }

    /** An output column: the name the query shows, what is known of its type, whether it may be NULL, and what the
     *  dialect knows of its type beyond its kind (a source column's; none once computed). */
    private record OutCol(String name, Kind kind, boolean nullable,
            InputDialect.@com.legend.base.Nullable TypeDetail detail) {

        OutCol(String name, Kind kind, boolean nullable) {
            this(name, kind, nullable, null);
        }
    }

    /** The Pure relation expression {@code sql}, a query in {@code dialect}, means: read by the dialect's parse and
     *  its rules; {@link SqlTranslationException} when it has none (its kind says whether the text is not SQL, the
     *  translation is not built yet, or the dialect's database rejects the query too). A statement that writes a table
     *  (the writes design's leg 4, docs/WRITES_DESIGN_2026_10_18.md §5) means L1's write, update, delete or merge, its
     *  value the database's count of the rows it wrote; one that returns them (RETURNING) is a program of statements,
     *  {@link #program}, which no expression is. */
    public static ValueSpecification translate(String sql, Sources sources, InputDialect dialect) {
        SqlAst.Statement statement = parsed(sql, dialect);
        InputDialect reading = dialect.reading(statement);
        SqlToPure translation = new SqlToPure(sources, reading, new Env(Map.of(), null, null, 0));
        if (statement instanceof SqlAst.Write w) {
            List<ValueSpecification> program = translation.write(w);
            if (program.size() != 1) {
                throw SqlTranslationException.unsupported("a statement that writes a table in more than one statement"
                        + " (one with RETURNING, an ON CONFLICT DO UPDATE) is a query's own statement, not a value"
                        + " inside one", w.pos());
            }
            return program.get(0);
        }
        Rel translated;
        try {
            translated = translation.statement(statement);
        } catch (SqlTranslationException e) {
            throw reading.failed(e);
        }
        reading.translated();
        // the island's own result is an inline view too (2026-10-16, the landing's review): its windows are computed
        // over all of its rows before a Pure step after the island (a filter) reads them
        return translation.inlineView(translated, statement).vs();
    }

    /** Whether {@code sql} is a statement that writes a table in more than one statement, a program of them
     *  ({@link #program}): one that returns the rows it wrote (RETURNING), or an ON CONFLICT DO UPDATE, whose rows are
     *  first held to one each of their key ({@link #onceEach}). One that does not parse is none: its error is
     *  {@link #translate}'s. */
    public static boolean isProgram(String sql, InputDialect dialect) {
        try {
            return dialect.parse(sql) instanceof SqlAst.Write w && (!returning(w).isEmpty()
                    || w instanceof SqlAst.Insert i && i.onConflict() != null && i.onConflict().update() != null);
        } catch (SqlSyntaxException e) {
            return false;
        }
    }

    /** The statements {@code sql}, a statement that writes a table, means: the rows it writes staged
     *  ({@code let s = rows->stage()}, computed once before the write), the write, and last the rows it returns, read
     *  from the staged rows. */
    public static List<ValueSpecification> program(String sql, Sources sources, InputDialect dialect) {
        SqlAst.Statement statement = parsed(sql, dialect);
        if (!(statement instanceof SqlAst.Write w)) {
            throw new IllegalStateException("a program of statements is a statement that writes a table's");
        }
        return new SqlToPure(sources, dialect.reading(statement), new Env(Map.of(), null, null, 0)).write(w);
    }

    private static SqlAst.Statement parsed(String sql, InputDialect dialect) {
        try {
            return dialect.parse(sql);
        } catch (SqlSyntaxException e) {
            throw new SqlTranslationException(e.unsupported() ? SqlTranslationException.Kind.UNSUPPORTED
                    : SqlTranslationException.Kind.SYNTAX, e.reason(), e.line(), e.column());
        }
    }

    // ------------------------------------------------------------------ INSERT, UPDATE, DELETE (writes leg 4)

    /** Lite's store writes (docs/WRITES_DESIGN_2026_10_18.md §4, L1), beside upstream's {@code write}, and the rows
     *  staged once ({@code stage}: a temporary table of the session): the islands' translators' (the protocol layer
     *  reads no catalog, so each name is spelled once here, the Cypher writer's too). */
    public static final String UPDATE = "meta::legend::lite::update";
    public static final String DELETE = "meta::legend::lite::delete";
    public static final String MERGE = "meta::legend::lite::merge";
    public static final String STAGE = "meta::legend::lite::stage";

    private static List<SqlAst.SelectItem> returning(SqlAst.Write w) {
        return switch (w) {
            case SqlAst.Insert i -> i.returning();
            case SqlAst.Update u -> u.returning();
            case SqlAst.Delete d -> d.returning();
        };
    }

    /** The statements {@code w} means: its write alone, or (RETURNING) the staged rows, the write and the rows. */
    private List<ValueSpecification> write(SqlAst.Write w) {
        return switch (w) {
            case SqlAst.Insert i -> insert(i);
            case SqlAst.Update u -> update(u);
            case SqlAst.Delete d -> delete(d);
        };
    }

    /** A statement's table: the store's table its source function names, its columns as they are declared (each one's
     *  Pure type and store type beside, as the source reports them), the FROM-like source its own clauses read (under
     *  its qualifier: its alias, else the function's name). */
    private record Target(ValueSpecification table, List<OutCol> columns, List<String> pureTypes,
            List<@com.legend.base.Nullable String> storeTypes, Source source, SqlAst.Ident qualifier, String name,
            String database, String path) {

        @com.legend.base.Nullable OutCol column(String name) {
            for (OutCol c : columns) {
                if (c.name().equals(name)) {
                    return c;
                }
            }
            return null;
        }

    }

    /** The column of {@code t} {@code id} names, by the dialect's rule for names (Postgres's: the folded name, else the
     *  one name equal ignoring case); null for none. */
    private @com.legend.base.Nullable OutCol column(Target t, SqlAst.Ident id) {
        String name = dialect.resolve(id, t.columns().stream().map(OutCol::name).toList(), "column");
        return name == null ? null : t.column(name);
    }

    private Target target(SqlAst.FromItem item, String statement) {
        if (!(item instanceof SqlAst.TableFunction f) || !isSourceFunction(f)) {
            throw SqlTranslationException.unsupported(statement + " names its table by the source function for now:"
                    + " write " + TABLE_FUNCTION + "('pkg::Database.schema.table')", item.pos());
        }
        ValueSpecification table = tableFunction(f);
        List<OutCol> columns = new ArrayList<>();
        List<String> pureTypes = new ArrayList<>();
        List<@com.legend.base.Nullable String> storeTypes = new ArrayList<>();
        sources.columns(table, (name, type, nullable, storeType) -> {
            columns.add(new OutCol(name, dialect.kindOf(type, storeType), nullable, dialect.detailOf(type, storeType)));
            pureTypes.add(type);
            storeTypes.add(storeType);
        });
        Rel raw = new Rel(table, columns);
        if (read(raw, f.pos()) != raw) {
            // (a column the dialect reads as another type than the store holds: written as it is held, never so)
            throw SqlTranslationException.unsupported(statement + " of a table with a column the dialect reads"
                    + " as another type is not supported yet", item.pos());
        }
        String name = ((SqlAst.StringLit) java.util.Objects.requireNonNull(f.args().get(0).value())).value();
        SqlAst.Ident qualifier = f.alias() != null ? f.alias() : dialect.functionName(f);
        int dot = name.indexOf('.');
        return new Target(table, columns, pureTypes, storeTypes, bound(raw, qualifier, List.of(), f.pos()), qualifier,
                name.substring(name.lastIndexOf('.') + 1), name.substring(0, dot), name.substring(dot + 1));
    }

    /** {@code INSERT INTO t [(columns)] query}: L1's write of the query's rows, each column the one of t its place
     *  names (as Postgres assigns them: a VALUES list's literal typed by its column). ON CONFLICT (keys) DO NOTHING
     *  writes the rows no row of t holds the keys of; DO UPDATE SET of each other column to its EXCLUDED value is L1's
     *  merge by the keys. RETURNING reads the rows staged before the write, a column the INSERT does not write as
     *  none (the store declares no defaults). */
    private List<ValueSpecification> insert(SqlAst.Insert ins) {
        Target t = target(ins.target(), "INSERT");
        List<OutCol> into = new ArrayList<>();
        if (ins.columns().isEmpty()) {
            into.addAll(t.columns());
        } else {
            for (SqlAst.Ident c : ins.columns()) {
                OutCol col = column(t, c);
                if (col == null) {
                    throw SqlTranslationException.invalid("column \"" + c.text() + "\" of relation \"" + t.name()
                            + "\" does not exist", c.pos());
                }
                if (into.contains(col)) {
                    throw SqlTranslationException.invalid("column \"" + c.text() + "\" specified more than once",
                            c.pos());
                }
                into.add(col);
            }
        }
        // (the source's columns are read by their places, never their names: two of one name are written so too)
        SqlAst.Statement source = positional(ins.source());
        // (a VALUES list's literals, and a plain SELECT's own, are typed by the columns they are written into, as
        // Postgres leaves them unknown there; a set operation's are text)
        if (ins.values() || source instanceof SqlAst.Select || source instanceof SqlAst.With) {
            source = typedValues(source, into, ins.values());
        }
        Rel rows = statement(source);
        if (rows.columns().size() > into.size()) {
            throw SqlTranslationException.invalid("INSERT has more expressions than target columns", ins.pos());
        }
        if (rows.columns().size() < into.size() && !ins.columns().isEmpty()) {
            throw SqlTranslationException.invalid("INSERT has more target columns than expressions", ins.pos());
        }
        rows = asStored(rows, into, t, ins.pos());
        List<ColSpec> written = new ArrayList<>();
        List<OutCol> staged = new ArrayList<>();
        for (int i = 0; i < rows.columns().size(); i++) {
            OutCol from = rows.columns().get(i);
            OutCol to = into.get(i);
            Tx value = asColumnType(heldTo(assigned(columnValue(row, Col.of(from.name(), from.kind(),
                    from.nullable(), from.detail())), to, ins.pos()), storeType(t, to)),
                    t.pureTypes().get(t.columns().indexOf(to)));
            written.add(new ColSpec(to.name(), lambda(row, held(value)), null, null));
            staged.add(new OutCol(to.name(), value.kind(), value.nullable(), value.detail()));
        }
        ValueSpecification values = call("project", rows.vs(), new ColSpecArray(written));
        List<String> keys = null;
        SqlAst.OnConflict conflict = ins.onConflict();
        if (conflict != null) {
            keys = conflictKeys(conflict, t, written);
            if (conflict.update() == null) {
                // (DO NOTHING: the rows whose keys no row of the table holds)
                ValueSpecification same = null;
                for (String k : keys) {
                    ValueSpecification eq = call("equal", property(sub, k), property(row, k));
                    same = same == null ? eq : call("and", same, eq);
                }
                ValueSpecification held = call("exists", t.table(), lambda(sub, java.util.Objects.requireNonNull(
                        same)));
                values = call("filter", values, lambda(row, call("not", held)));
                keys = null;
            } else {
                SqlAst.Ident alias = ins.target() instanceof SqlAst.TableFunction f ? f.alias() : null;
                if (alias != null && "excluded".equals(alias.quoted() ? alias.text() : fold(alias.text()))) {
                    // (the target named as the proposed rows are: PR #67's second review, 2026-10-24)
                    throw SqlTranslationException.invalid(dialect.message(Problem.AMBIGUOUS_TABLE, "excluded"),
                            alias.pos());
                }
                mergedByKeys(conflict, t, keys, written);
            }
        }
        if (ins.returning().isEmpty()) {
            if (keys == null) {
                return List.of(call("write", values, t.table()));
            }
            return List.of(onceEach(values, keys), call(MERGE, values, t.table(), new ColSpecArray(
                    keys.stream().map(SqlToPure::colSpec).toList())));
        }
        if (keys != null) {
            throw SqlTranslationException.unsupported("INSERT ... ON CONFLICT DO UPDATE ... RETURNING is not supported"
                    + " yet", ins.pos());
        }
        // the rows as written: each column the INSERT does not write as none
        List<ColSpec> unwritten = new ArrayList<>();
        for (OutCol c : t.columns()) {
            if (written.stream().noneMatch(w -> w.name().equals(c.name()))) {
                unwritten.add(new ColSpec(c.name(), lambda(row, held(nullOf(c.kind()))), null, null));
                staged.add(new OutCol(c.name(), c.kind(), true, c.detail()));
            }
        }
        returnsOnlyWritten(ins, t, unwritten.stream().map(ColSpec::name)
                .collect(java.util.stream.Collectors.toSet()));
        String stage = SYNTHETIC + "written";
        ValueSpecification rowsWritten = unwritten.isEmpty() ? values : call("extend", values,
                new ColSpecArray(unwritten));
        List<ColSpec> writtenNames = written.stream().map(w -> colSpec(w.name())).toList();
        return List.of(call("letFunction", new CString(stage), call(STAGE, rowsWritten)),
                call("letFunction", new CString(RETURNING), call(STAGE, returned(stage, staged, t, ins.returning(),
                        ins.pos()))),
                call("write", call("select", new Variable(stage), new ColSpecArray(writtenNames)), t.table()),
                new Variable(RETURNING));
    }

    /** The let of the rows a statement's RETURNING returns: computed, and staged, before the statement writes, so
     *  that a subquery in it reads the table as the statement found it, as Postgres's snapshot does (PR #67's second
     *  review, 2026-10-24: {@code RETURNING (SELECT count(*) FROM t)} had counted the row the INSERT wrote). */
    private static final String RETURNING = SYNTHETIC + "returning";

    /** An ON CONFLICT DO UPDATE's rows each of their key once: two rows of one key are Postgres's error ("ON CONFLICT
     *  DO UPDATE command cannot affect row a second time", PR #67's second review, 2026-10-24: DuckDB's MERGE applied
     *  both), raised by the database before the merge. */
    private ValueSpecification onceEach(ValueSpecification values, List<String> keys) {
        String n = SYNTHETIC + "rows";
        ValueSpecification counted = call("groupBy", values, new ColSpecArray(keys.stream().map(SqlToPure::colSpec)
                .toList()), new ColSpecArray(List.of(new ColSpec(n, lambda("x", new CInteger(1L)), lambda("y",
                call("count", new Variable("y"))), null))));
        // (the condition the row's, read inside its value: H2 computes a grouped query's items before its HAVING)
        return call("project", counted, new ColSpecArray(List.of(new ColSpec(SYNTHETIC + "once", lambda(row, call("if",
                call("greaterThan", property(row, n), new CInteger(1L)), thunk(call("toOne", call("cast", empty(),
                typeArg("Integer")))), thunk(new CInteger(1L)))), null, null))));
    }

    /** {@code rows} each written as the column it lands in holds it (PR #67's second review, 2026-10-24): a value of
     *  another kind, or into a {@code numeric(p, s)} or a {@code real}, cast to the column's type as Postgres's
     *  assignment casts it -- a numeric into an integer rounded half away from zero, a double half to even, an
     *  integer, a numeric, a boolean or a date into text as its text -- where the database's own cast had rounded
     *  otherwise ({@code avg} of 2 and 3 into an integer: 2 on DuckDB, 3 in Postgres); what the casts refuse (a double,
     *  or a quotient, average or root of numerics, into a numeric or an integer) is refused by name. So the keys an ON
     *  CONFLICT compares and the rows RETURNING reads are the values as stored. A value no assignment cast takes is
     *  Postgres's error ({@link #assignable}). */
    private Rel asStored(Rel rows, List<OutCol> into, Target t, SqlAst.Pos pos) {
        String name = SYNTHETIC + "assigned";
        List<SqlAst.SelectItem> items = new ArrayList<>();
        boolean cast = false;
        for (int i = 0; i < rows.columns().size(); i++) {
            OutCol from = rows.columns().get(i);
            OutCol to = into.get(i);
            int at = t.columns().indexOf(to);
            assignable(from.kind(), to, t.storeTypes().get(at), pos);
            SqlAst.Ident id = new SqlAst.Ident(from.name(), true, pos);
            SqlAst.Expr ref = new SqlAst.ColumnRef(List.of(id), pos);
            SqlAst.TypeName type = from.kind() == Kind.NULL ? null : declared(to, from, t.pureTypes().get(at),
                    t.storeTypes().get(at), pos);
            cast |= type != null;
            items.add(new SqlAst.SelectExpr(type == null ? ref : new SqlAst.Cast(ref, type, pos), id, pos));
        }
        if (!cast) {
            return rows;
        }
        SqlAst.Select assigned = new SqlAst.Select(false, items, List.of(new SqlAst.TableName(List.of(
                new SqlAst.Ident(name, true, pos)), null, List.of(), pos)), null, List.of(), null, List.of(), List.of(),
                null, null, pos);
        Map<String, Rel> ctes = new LinkedHashMap<>(env.ctes());
        ctes.put(name, rows);
        return new SqlToPure(sources, dialect, env.with(ctes)).statement(assigned);
    }

    /** The store type of the column {@code to} of {@code t}, as the store spells it ({@code VARCHAR(50)}); null for a
     *  column the store declares none for. */
    private static @com.legend.base.Nullable String storeType(Target t, OutCol to) {
        return t.storeTypes().get(t.columns().indexOf(to));
    }

    /** Text written into a {@code varchar(n)} ({@code storeType}): longer than n characters, Postgres's error ("value
     *  too long for type character varying(n)"), unless what passes n is spaces, which are cut away -- Postgres's
     *  assignment, not its cast, which cuts any text (PR #67's second review, 2026-10-24: DuckDB wrote all 60
     *  characters into a varchar(50)). The database raises it, an empty value read as one. */
    private static Tx heldTo(Tx value, @com.legend.base.Nullable String storeType) {
        if (value.kind() != Kind.STRING || isNullConstant(value) || storeType == null
                || !storeType.startsWith("VARCHAR(") || !storeType.endsWith(")")) {
            return value;
        }
        CInteger n = new CInteger(Long.parseLong(storeType.substring("VARCHAR(".length(), storeType.length() - 1)));
        return strict(Kind.STRING, List.of(value), v -> call("if", call("greaterThan", call("length", v.get(0)), n),
                thunk(call("if", call("greaterThan", call("length", call("rtrim", v.get(0))), n), thunk(call("toOne",
                        call("cast", empty(), typeArg("String")))), thunk(call("left", v.get(0), n)))),
                thunk(v.get(0))));
    }

    /** An INSERT's RETURNING reading a column the INSERT does not write is refused by name (PR #67's second review,
     *  2026-10-24): RETURNING reads the rows staged before the write, and such a column's value is the database's (a
     *  default, a sequence, a trigger), which no model declares, so {@code INSERT INTO t (name) VALUES ('x') RETURNING
     *  id} over a serial id read NULL where Postgres returns the id it gave. */
    private void returnsOnlyWritten(SqlAst.Insert ins, Target t, Set<String> unwritten) {
        if (unwritten.isEmpty()) {
            return;
        }
        for (SqlAst.SelectItem item : ins.returning()) {
            List<SqlAst.ColumnRef> refs = new ArrayList<>();
            if (item instanceof SqlAst.SelectExpr se) {
                columnRefs(se.expr(), refs);
            }
            for (SqlAst.ColumnRef ref : refs) {
                OutCol c = column(t, ref.parts().get(ref.parts().size() - 1));
                if (c != null && unwritten.contains(c.name())) {
                    throw SqlTranslationException.unsupported("RETURNING a column the INSERT does not write ("
                            + c.name() + ") is not supported: its value is the database's (a default, a sequence, a"
                            + " trigger), which the model does not declare", ref.pos());
                }
            }
            if (item instanceof SqlAst.AllColumns) {
                throw SqlTranslationException.unsupported("RETURNING * of an INSERT that does not write every column"
                        + " (" + String.join(", ", new java.util.TreeSet<>(unwritten)) + ") is not supported: their"
                        + " values are the database's (a default, a sequence, a trigger), which the model does not"
                        + " declare", item.pos());
            }
        }
    }

    /** The keys an ON CONFLICT reads: the table's primary key, as the store declares it, which Postgres's unique index
     *  is (any other, Postgres's error), each a column the INSERT writes. */
    private List<String> conflictKeys(SqlAst.OnConflict conflict, Target t, List<ColSpec> written) {
        if (conflict.keys().isEmpty()) {
            throw SqlTranslationException.unsupported("ON CONFLICT with no columns of a unique index is not supported"
                    + " yet: name them, ON CONFLICT (key, ...)", conflict.pos());
        }
        List<String> keys = new ArrayList<>();
        for (SqlAst.Ident k : conflict.keys()) {
            OutCol key = column(t, k);
            if (key == null) {
                throw SqlTranslationException.invalid("column \"" + k.text() + "\" does not exist", k.pos());
            }
            keys.add(key.name());
        }
        List<String> declared = sources.primaryKey(t.database(), t.path());
        if (declared == null) {
            throw SqlTranslationException.unsupported("ON CONFLICT over a table whose primary key no model declares"
                    + " is not supported (" + t.database() + "." + t.path() + ")", conflict.pos());
        }
        if (!new HashSet<>(declared).equals(new HashSet<>(keys))) {
            throw SqlTranslationException.invalid("there is no unique or exclusion constraint matching the ON"
                    + " CONFLICT specification", conflict.pos());
        }
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            if (written.stream().noneMatch(w -> w.name().equals(key))) {
                throw SqlTranslationException.unsupported("ON CONFLICT on a column the INSERT does not write is not"
                        + " supported yet", conflict.keys().get(i).pos());
            }
        }
        return keys;
    }

    /** DO UPDATE: each column the INSERT writes, but the keys, set to its EXCLUDED value (L1's merge); any other
     *  update refused by name. */
    private void mergedByKeys(SqlAst.OnConflict conflict, Target t, List<String> keys, List<ColSpec> written) {
        if (conflict.where() != null) {
            throw SqlTranslationException.unsupported("ON CONFLICT DO UPDATE ... WHERE is not supported yet",
                    conflict.pos());
        }
        Set<String> set = new java.util.LinkedHashSet<>();
        for (SqlAst.Assignment a : java.util.Objects.requireNonNull(conflict.update())) {
            OutCol to = column(t, a.column());
            if (to == null) {
                throw SqlTranslationException.invalid("column \"" + a.column().text() + "\" of relation \""
                        + t.name() + "\" does not exist", a.pos());
            }
            SqlAst.Ident excluded = a.value() instanceof SqlAst.ColumnRef ref && ref.parts().size() == 2
                    ? ref.parts().get(0) : null;
            OutCol from = excluded != null && "excluded".equals(excluded.quoted() ? excluded.text()
                    : fold(excluded.text())) ? column(t, ((SqlAst.ColumnRef) a.value()).parts().get(1)) : null;
            if (from == null || !from.name().equals(to.name())) {
                throw SqlTranslationException.unsupported("ON CONFLICT DO UPDATE SET of a value but the column's"
                        + " EXCLUDED one is not supported yet", a.pos());
            }
            set.add(to.name());
        }
        Set<String> others = new java.util.LinkedHashSet<>();
        written.forEach(w -> others.add(w.name()));
        keys.forEach(others::remove);
        if (!set.equals(others)) {
            throw SqlTranslationException.unsupported("ON CONFLICT DO UPDATE that sets other columns than the ones the"
                    + " INSERT writes is not supported yet", conflict.pos());
        }
    }

    /** An INSERT's source with each select list item its place's name: Postgres writes the source's columns by place,
     *  so two of one name (two {@code ?column?}s, a column read twice) are written too. A column already named keeps
     *  its name; a VALUES list's are named by their places already. */
    private SqlAst.Statement positional(SqlAst.Statement source) {
        return switch (source) {
            case SqlAst.SetOperation o when o.values() -> o;
            case SqlAst.SetOperation o -> new SqlAst.SetOperation(o.op(), o.all(), positional(o.left()),
                    positional(o.right()), o.orderBy(), o.limit(), o.offset(), o.pos(), o.values());
            case SqlAst.Select s -> {
                List<SqlAst.SelectItem> items = new ArrayList<>();
                // (an ORDER BY of an unnamed item's own name, Postgres's output name, reads it renamed: PR #67's
                // second review, 2026-10-24)
                Map<String, List<SqlAst.Ident>> renamed = new LinkedHashMap<>();
                for (int i = 0; i < s.items().size(); i++) {
                    SqlAst.SelectItem item = s.items().get(i);
                    if (item instanceof SqlAst.SelectExpr x && x.alias() == null) {
                        SqlAst.Ident place = new SqlAst.Ident(SYNTHETIC + "c" + (i + 1), true, x.pos());
                        renamed.computeIfAbsent(dialect.defaultName(x), k -> new ArrayList<>()).add(place);
                        item = new SqlAst.SelectExpr(x.expr(), place, x.pos(), x.text());
                    }
                    items.add(item);
                }
                List<SqlAst.SortItem> orderBy = new ArrayList<>();
                for (SqlAst.SortItem o : s.orderBy()) {
                    List<SqlAst.Ident> places = o.expr() instanceof SqlAst.ColumnRef r && r.parts().size() == 1
                            ? renamed.get(r.parts().get(0).quoted() ? r.parts().get(0).text()
                                    : fold(r.parts().get(0).text())) : null;
                    orderBy.add(places == null || places.size() != 1 ? o : new SqlAst.SortItem(new SqlAst.ColumnRef(
                            List.of(places.get(0)), o.expr().pos()), o.descending(), o.nullsFirst(), o.pos()));
                }
                yield new SqlAst.Select(s.distinct(), items, s.from(), s.where(), s.groupBy(), s.having(),
                        s.windows(), orderBy, s.limit(), s.offset(), s.pos());
            }
            case SqlAst.With w -> new SqlAst.With(w.recursive(), w.ctes(), positional(w.body()), w.pos());
            case SqlAst.Write w -> w;
        };
    }

    /** An INSERT's source written into {@code into}: each literal of no type yet (a string, a NULL) cast to its
     *  column's type, as Postgres types an INSERT's VALUES and a plain SELECT's list by the columns they are written
     *  into; in a VALUES list ({@code values}) each number or text of another type than its column's too, as
     *  Postgres casts each row's value to its column (PR #67's second review, 2026-10-24: {@code VALUES (1), (2.5)}
     *  into an integer was a union of two types). A DISTINCT or grouped SELECT's literals stay text, as Postgres
     *  resolves them before the INSERT reads them; a WITH's body is typed; an item after a {@code *} is not, its
     *  column's place unknown. */
    private static SqlAst.Statement typedValues(SqlAst.Statement source, List<OutCol> into, boolean values) {
        return switch (source) {
            case SqlAst.SetOperation o -> new SqlAst.SetOperation(o.op(), o.all(), typedValues(o.left(), into, values),
                    typedValues(o.right(), into, values), o.orderBy(), o.limit(), o.offset(), o.pos(), o.values());
            case SqlAst.Select row when !values && (row.distinct() || !row.groupBy().isEmpty()) -> row;
            case SqlAst.Select row -> {
                List<SqlAst.SelectItem> items = new ArrayList<>();
                boolean placed = true;
                for (int i = 0; i < row.items().size(); i++) {
                    SqlAst.SelectItem item = row.items().get(i);
                    placed &= !(item instanceof SqlAst.AllColumns);
                    if (placed && i < into.size() && item instanceof SqlAst.SelectExpr x) {
                        SqlAst.Expr typed = values ? typedValue(x.expr(), into.get(i)) : typedLiteral(x.expr(),
                                into.get(i));
                        if (typed != x.expr()) {
                            item = new SqlAst.SelectExpr(typed, x.alias(), x.pos(), x.text());
                        }
                    }
                    items.add(item);
                }
                yield new SqlAst.Select(row.distinct(), items, row.from(), row.where(), row.groupBy(), row.having(),
                        row.windows(), row.orderBy(), row.limit(), row.offset(), row.pos());
            }
            case SqlAst.With w -> new SqlAst.With(w.recursive(), w.ctes(), typedValues(w.body(), into, values),
                    w.pos());
            case SqlAst.Write w -> w;
        };
    }

    /** A VALUES row's value written into {@code to}: a literal of no type yet as {@link #typedLiteral} types it; a
     *  number literal into a number column of another kind, or any literal into text, cast to the column's type. */
    private static SqlAst.Expr typedValue(SqlAst.Expr e, OutCol to) {
        SqlAst.Expr typed = typedLiteral(e, to);
        if (typed != e) {
            return typed;
        }
        Kind literal = e instanceof SqlAst.IntegerLit ? Kind.INTEGER : e instanceof SqlAst.NumericLit ? Kind.DECIMAL
                : e instanceof SqlAst.BooleanLit ? Kind.BOOLEAN : null;
        String type = typeName(to.kind());
        if (literal == null || literal == to.kind() || type == null
                || !(literal.numeric() && to.kind().numeric() || to.kind() == Kind.STRING)) {
            return e;
        }
        return new SqlAst.Cast(e, new SqlAst.TypeName(type, List.of(), e.pos()), e.pos());
    }

    /** A literal of no type yet (a NULL, a string into a column of another type) as a value of the column's type, as
     *  Postgres assigns it; any other expression as it is. */
    private static SqlAst.Expr typedLiteral(SqlAst.Expr e, OutCol to) {
        if (e instanceof SqlAst.NullLit || e instanceof SqlAst.StringLit && to.kind() != Kind.STRING) {
            String type = typeName(to.kind());
            if (type != null) {
                return new SqlAst.Cast(e, new SqlAst.TypeName(type, List.of(), e.pos()), e.pos());
            }
        }
        return e;
    }

    /** The Postgres type a column of {@code kind} is spelled as in a cast; null for a kind none names. */
    private static @com.legend.base.Nullable String typeName(Kind kind) {
        return switch (kind) {
            case INTEGER, WHOLE -> "bigint";
            case FLOAT, INEXACT -> "float8";
            case DECIMAL, NUMBER -> "numeric";
            case STRING -> "text";
            case BOOLEAN -> "boolean";
            case DATE -> "date";
            case DATETIME -> "timestamp";
            case TIME -> "time";
            case JSON -> "jsonb";
            case ZONED, ZONED_TIME, DURATION, NULL, OTHER -> null;
        };
    }

    /** A value written into the column {@code to} (an INSERT's or an UPDATE's), already cast to its type
     *  ({@link #asStored}, {@link #storedValue}): a NULL as the column's; a value of the column's kind, or a number into
     *  a number column, as it is. Any other is Postgres's error. */
    private static Tx assigned(Tx value, OutCol to, SqlAst.Pos pos) {
        if (value.kind() == Kind.NULL) {
            return nullOf(to.kind());
        }
        if (value.kind() == to.kind() || value.kind().numeric() && to.kind().numeric()) {
            return value;
        }
        throw SqlTranslationException.invalid("column \"" + to.name() + "\" is of type " + typeNameOr(to.kind())
                + " but expression is of type " + typeNameOr(value.kind()), pos);
    }

    /** Whether a value of {@code from} may be written into the column {@code to} (its store type {@code storeType}):
     *  the kinds Postgres's assignment casts take -- a number into a number, anything into text, a timestamp into a
     *  date and a date into a timestamp -- which the cast to the column's type then reads, or refuses by name (PR
     *  #67's second review, 2026-10-24: an integer into text was refused with Postgres's own error, which Postgres
     *  does not raise). Any other is Postgres's error, its column's type as the store spells it. */
    private static void assignable(Kind from, OutCol to, @com.legend.base.Nullable String storeType, SqlAst.Pos pos) {
        Kind into = to.kind();
        if (from == Kind.NULL || from == into || from.numeric() && into.numeric() || into == Kind.STRING
                || from == Kind.DATETIME && into == Kind.DATE || from == Kind.DATE && into == Kind.DATETIME) {
            return;
        }
        throw SqlTranslationException.invalid("column \"" + to.name() + "\" is of type " + (storeType != null
                ? storeType.toLowerCase(java.util.Locale.ROOT) : typeNameOr(into)) + " but expression is of type "
                + typeNameOr(from), pos);
    }

    private static String typeNameOr(Kind kind) {
        String name = typeName(kind);
        return name != null ? name : kind.name().toLowerCase(java.util.Locale.ROOT);
    }

    /** {@code UPDATE t SET column = value, ... [FROM items] [WHERE]}: L1's update of the rows the condition holds for,
     *  each value read from the row as it was. With FROM, a row is updated when some row of the items meets the
     *  condition with it, from that row ({@code EXISTS} and a scalar subquery over it; several such rows are the
     *  scalar subquery's error, where Postgres updates from an unspecified one). RETURNING reads the rows staged
     *  before the write, each set to its value. */
    private List<ValueSpecification> update(SqlAst.Update upd) {
        Target t = target(upd.target(), "UPDATE");
        if (!upd.from().isEmpty() && !upd.returning().isEmpty()) {
            throw SqlTranslationException.unsupported("UPDATE ... FROM ... RETURNING is not supported yet", upd.pos());
        }
        if (!upd.from().isEmpty()) {
            List<SqlAst.Expr> read = new ArrayList<>();
            upd.sets().forEach(a -> read.add(a.value()));
            if (upd.where() != null) {
                read.add(upd.where());
            }
            oneLevel(t, upd.from(), read);
        }
        Input input = new Input(t.source().bindings(), t.source().star(), "WHERE");
        SqlAst.Expr cond = upd.from().isEmpty() ? upd.where() : exists(upd.from(), upd.where(), upd.pos());
        ValueSpecification where = cond == null ? new CBoolean(true) : condition(cond, input, "WHERE");
        List<ColSpec> sets = new ArrayList<>();
        Map<String, Tx> values = new LinkedHashMap<>();
        for (SqlAst.Assignment a : upd.sets()) {
            OutCol to = column(t, a.column());
            if (to == null) {
                throw SqlTranslationException.invalid("column \"" + a.column().text() + "\" of relation \""
                        + t.name() + "\" does not exist", a.pos());
            }
            if (values.containsKey(to.name())) {
                throw SqlTranslationException.invalid("multiple assignments to same column \"" + to.name() + "\"",
                        a.pos());
            }
            SqlAst.Expr e = typedLiteral(a.value(), to);
            if (!upd.from().isEmpty()) {
                e = new SqlAst.ScalarSubquery(valueOver(e, upd.from(), upd.where(), e.pos()), e.pos());
            }
            Tx value = asColumnType(heldTo(assigned(storedValue(e, input.in("UPDATE"), t, to, a.pos()), to,
                    a.pos()), storeType(t, to)), t.pureTypes().get(t.columns().indexOf(to)));
            values.put(to.name(), value);
            sets.add(new ColSpec(to.name(), lambda(row, valueOf(value)), null, null));
        }
        ValueSpecification update = call(UPDATE, t.table(), lambda(row, where), new ColSpecArray(sets));
        if (upd.returning().isEmpty()) {
            return List.of(update);
        }
        // the rows as they will be: the table's rows the condition holds for, each column set its value
        List<ColSpec> after = new ArrayList<>();
        List<OutCol> staged = new ArrayList<>();
        for (OutCol c : t.columns()) {
            Tx set = values.get(c.name());
            Tx value = set != null ? set : columnValue(row, Col.of(c.name(), c.kind(), c.nullable(), c.detail()));
            after.add(new ColSpec(c.name(), lambda(row, held(value)), null, null));
            staged.add(set != null ? new OutCol(c.name(), set.kind(), set.nullable(), set.detail()) : c);
        }
        String stage = SYNTHETIC + "updated";
        return List.of(call("letFunction", new CString(stage), call(STAGE, call("project", call("filter", t.table(),
                lambda(row, where)), new ColSpecArray(after)))), call("letFunction", new CString(RETURNING),
                call(STAGE, returned(stage, staged, t, upd.returning(), upd.pos()))), update, new Variable(RETURNING));
    }

    /** {@code e} written into the column {@code to} of {@code t}, read in {@code scope}: cast to the column's type as
     *  the column holds it ({@link #asStored}'s rule, for an UPDATE's SET); a value no assignment cast takes,
     *  Postgres's error. */
    private Tx storedValue(SqlAst.Expr e, Scope scope, Target t, OutCol to, SqlAst.Pos pos) {
        Tx raw = tx(e, scope, row);
        int at = t.columns().indexOf(to);
        assignable(raw.kind(), to, t.storeTypes().get(at), pos);
        SqlAst.TypeName type = raw.kind() == Kind.NULL ? null : declared(to, new OutCol(to.name(), raw.kind(),
                raw.nullable(), raw.detail()), t.pureTypes().get(at), t.storeTypes().get(at), pos);
        return type == null ? raw : tx(new SqlAst.Cast(e, type, pos), scope, row);
    }

    /** UPDATE ... FROM and DELETE ... USING read the target and the items at one level, as Postgres does (PR #67's
     *  second review, 2026-10-24: they were an EXISTS under the target, so {@code DELETE FROM orders USING persons
     *  WHERE id = 1} read the persons' id and deleted every order): an item named as the target is a table named
     *  twice, a name of the clauses' ({@code read}) that the target and an item both have is ambiguous, and an
     *  item's subquery reads no column of the target (the items are not LATERAL to it), each Postgres's error. The
     *  items are translated here on their own to read their names. */
    private void oneLevel(Target t, List<SqlAst.FromItem> items, List<SqlAst.Expr> read) {
        List<Source> level = new ArrayList<>();
        level.add(t.source());
        SqlAst.Ident target = t.qualifier();
        String targetName = target.quoted() ? target.text() : fold(target.text());
        for (SqlAst.FromItem item : items) {
            Source s = from(item);
            for (Binding b : s.bindings()) {
                SqlAst.Ident q = b.qualifier();
                if (q != null && targetName.equals(q.quoted() ? q.text() : fold(q.text()))) {
                    throw SqlTranslationException.invalid(dialect.message(Problem.DUPLICATE_TABLE, q.text()),
                            q.pos());
                }
            }
            level.add(s);
        }
        for (SqlAst.Expr e : read) {
            itemsRead(e, level);
        }
    }

    /** {@code DELETE FROM t [USING items] [WHERE]}: L1's delete of the rows the condition holds for (every row without
     *  one); with USING, of the rows some row of the items meets the condition with ({@code EXISTS}). RETURNING reads
     *  the rows staged before the delete. */
    private List<ValueSpecification> delete(SqlAst.Delete del) {
        Target t = target(del.target(), "DELETE");
        if (!del.using().isEmpty() && !del.returning().isEmpty()) {
            throw SqlTranslationException.unsupported("DELETE ... USING ... RETURNING is not supported yet",
                    del.pos());
        }
        if (!del.using().isEmpty()) {
            List<SqlAst.Expr> read = new ArrayList<>();
            if (del.where() != null) {
                read.add(del.where());
            }
            oneLevel(t, del.using(), read);
        }
        Input input = new Input(t.source().bindings(), t.source().star(), "WHERE");
        SqlAst.Expr cond = del.using().isEmpty() ? del.where() : exists(del.using(), del.where(), del.pos());
        ValueSpecification where = cond == null ? new CBoolean(true) : condition(cond, input, "WHERE");
        ValueSpecification delete = call(DELETE, t.table(), lambda(row, where));
        if (del.returning().isEmpty()) {
            return List.of(delete);
        }
        String stage = SYNTHETIC + "deleted";
        return List.of(call("letFunction", new CString(stage), call(STAGE, call("filter", t.table(), lambda(row,
                where)))), call("letFunction", new CString(RETURNING), call(STAGE, returned(stage, t.columns(), t,
                del.returning(), del.pos()))), delete, new Variable(RETURNING));
    }

    /** A value as a column of rows that are read after (staged, written): a NULL constant as a NULL its kind's type
     *  types, one that may be none ({@code if(false, |zero, |[])}: of multiplicity [0..1], where a NULL of [0] would
     *  raise wherever it is read, and a staged column of it would be of no type); any other as it is. */
    private static ValueSpecification held(Tx value) {
        if (!isNullConstant(value) || value.kind() == Kind.BOOLEAN) {
            return valueOf(value);
        }
        ValueSpecification zero = switch (value.kind()) {
            case INTEGER, WHOLE -> new CInteger(0L);
            case FLOAT, INEXACT -> new com.legend.protocol.spec.CFloat(0.0);
            case DECIMAL, NUMBER -> new CDecimal(BigDecimal.ZERO);
            case STRING, NULL -> new CString("");
            case DATE -> new com.legend.protocol.spec.CDate(com.legend.values.PureDateLiteral.parse("1970-01-01"),
                    "1970-01-01", null);
            case DATETIME -> new com.legend.protocol.spec.CDate(com.legend.values.PureDateLiteral.parse(
                    "1970-01-01T00:00:00"), "1970-01-01T00:00:00", null);
            case BOOLEAN, TIME, ZONED, ZONED_TIME, DURATION, JSON, OTHER -> null;
        };
        return zero == null ? valueOf(value) : call("if", new CBoolean(false), thunk(zero), thunk(valueOf(value)));
    }

    /** A date or a date-time written into its column as the column's own Pure type ({@code pureType}): one the
     *  database computes ({@code d + 1}, {@code ts::date}, {@code ts + interval '1 day'}) is Pure's Date, which the
     *  write's type check takes for no StrictDate or DateTime column (PR #67's second review, its second round,
     *  2026-10-24), so it is cast to the column's type -- a type assertion where it is one already, the database's
     *  cast where it is a date-time. */
    private static Tx asColumnType(Tx value, String pureType) {
        if (isNullConstant(value) || !(value.kind() == Kind.DATE && "StrictDate".equals(pureType)
                || value.kind() == Kind.DATETIME && "DateTime".equals(pureType))) {
            return value;
        }
        return Tx.value(call("cast", valueOf(value), typeArg(pureType)), value.kind(), value.nullable(),
                value.detail());
    }

    /** {@code EXISTS (SELECT 1 FROM items [WHERE condition])}: a row of the items meets the condition. */
    private static SqlAst.Expr exists(List<SqlAst.FromItem> items, SqlAst.@com.legend.base.Nullable Expr condition,
            SqlAst.Pos pos) {
        return new SqlAst.Exists(new SqlAst.Select(false, List.of(new SqlAst.SelectExpr(new SqlAst.IntegerLit(
                java.math.BigInteger.ONE, pos), null, pos)), items, condition, List.of(), null, List.of(), List.of(),
                null, null, pos), pos);
    }

    /** {@code SELECT value FROM items [WHERE condition]}: the value over the row of the items that meets the
     *  condition (several such rows, a scalar subquery's error: Postgres updates from an unspecified one of them). */
    private static SqlAst.Statement valueOver(SqlAst.Expr value, List<SqlAst.FromItem> items,
            SqlAst.@com.legend.base.Nullable Expr condition, SqlAst.Pos pos) {
        return new SqlAst.Select(false, List.of(new SqlAst.SelectExpr(value, null, pos)), items, condition, List.of(),
                null, List.of(), List.of(), null, null, pos);
    }

    /** {@code RETURNING items} over the rows a statement wrote ({@code stage}, of the columns {@code staged}): a select
     *  list over them under the table's qualifier, each column as the table holds it (a value of another type cast to
     *  the column's, as the database assigns it). */
    private ValueSpecification returned(String stage, List<OutCol> staged, Target t, List<SqlAst.SelectItem> items,
            SqlAst.Pos pos) {
        for (SqlAst.SelectItem item : items) {
            // (Postgres's errors, raised before anything is written: PR #67's second review, 2026-10-24, the write
            // had run and RETURNING count(*) answered one aggregated row)
            String refused = item instanceof SqlAst.SelectExpr x ? notAllowedIn("RETURNING", x.expr()) : null;
            if (refused != null) {
                throw SqlTranslationException.invalid(refused, item.pos());
            }
        }
        String rows = SYNTHETIC + "returned";
        List<SqlAst.SelectItem> stored = new ArrayList<>();
        // (in the table's order: RETURNING * reads its columns so)
        for (int i = 0; i < t.columns().size(); i++) {
            String column = t.columns().get(i).name();
            OutCol s = staged.stream().filter(c -> c.name().equals(column)).findFirst().orElseThrow();
            SqlAst.Ident name = new SqlAst.Ident(s.name(), true, pos);
            SqlAst.Expr ref = new SqlAst.ColumnRef(List.of(name), pos);
            SqlAst.TypeName type = declared(t.columns().get(i), s, t.pureTypes().get(i), t.storeTypes().get(i), pos);
            stored.add(new SqlAst.SelectExpr(type == null ? ref : new SqlAst.Cast(ref, type, pos), name, pos));
        }
        SqlAst.Select held = new SqlAst.Select(false, stored, List.of(new SqlAst.TableName(List.of(new SqlAst.Ident(
                rows, true, pos)), null, List.of(), pos)), null, List.of(), null, List.of(), List.of(), null, null,
                pos);
        SqlAst.Select returned = new SqlAst.Select(false, items, List.of(new SqlAst.SubqueryFrom(held, t.qualifier(),
                List.of(), false, pos)), null, List.of(), null, List.of(), List.of(), null, null, pos);
        Map<String, Rel> ctes = new LinkedHashMap<>(env.ctes());
        ctes.put(rows, new Rel(new Variable(stage), staged));
        return new SqlToPure(sources, dialect, env.with(ctes)).statement(returned).vs();
    }

    /** The type a value written into the column {@code column} is cast to as the table holds it: a numeric of a
     *  precision and scale its own ({@code numeric(p, s)}: rounded to its places); another number type, a value of
     *  another kind (an integer column's rounding); none for a value of the column's kind. */
    private static SqlAst.@com.legend.base.Nullable TypeName declared(OutCol column, OutCol value, String pureType,
            @com.legend.base.Nullable String storeType, SqlAst.Pos pos) {
        // (the Pure type's own spelling, Decimal(p,s), its two modifiers read between its parentheses)
        String spelled = pureType.replace(" ", "");
        int comma = spelled.indexOf(',');
        if (spelled.startsWith("Decimal(") && spelled.endsWith(")") && comma > 0) {
            return new SqlAst.TypeName("numeric", List.of(Integer.parseInt(spelled.substring("Decimal(".length(),
                    comma)), Integer.parseInt(spelled.substring(comma + 1, spelled.length() - 1))), pos);
        }
        if (value.kind() == column.kind() && !"REAL".equals(storeType)) {
            return null;
        }
        String type = "REAL".equals(storeType) ? "real" : typeName(column.kind());
        if (type == null) {
            throw SqlTranslationException.unsupported("a value written into a column of type " + pureType
                    + " is not supported yet", pos);
        }
        return new SqlAst.TypeName(type, List.of(), pos);
    }

    private Rel statement(SqlAst.Statement statement) {
        return switch (statement) {
            case SqlAst.Select s -> select(s);
            case SqlAst.SetOperation s -> setOperation(s);
            case SqlAst.With w -> with(w);
            // (the island's own statement alone may write a table: SqlParser reads no write inside a query)
            case SqlAst.Write w -> throw new IllegalStateException("a statement that writes a table read as a query");
        };
    }

    // ------------------------------------------------------------------ kinds

    /**
     * What the translator knows of a value's type: enough to choose a Postgres meaning, nothing more. INEXACT is
     * Postgres's numeric computed here as Pure's Float: a quotient of numbers that are not both integers (Pure's
     * divide), an average or a statistic of numbers that are not doubles, a square root, exponential or logarithm of a
     * numeric. It agrees with Postgres's exact value to about 15 significant digits but may fall either side of it
     * ({@code 0.3 / 0.1} is 2.9999999999999996), so nothing that rounds it or spells it is translated. WHOLE is a
     * sum of integers: Postgres's bigint for a sum of int4 values, its numeric for a sum of int8 ones -- the same
     * whole number, but the one's division by an integer truncates and the other's does not, and the translator does
     * not know a column's width. Dividing it by an integer is refused.
     */
    public enum Kind {
        INTEGER, WHOLE, FLOAT, DECIMAL, NUMBER, INEXACT, STRING, BOOLEAN, DATE, DATETIME, TIME,
        /** A zoned date-time (Cypher's {@code datetime}): carried as its instant, Pure's DateTime; its zone, when the
         *  translator knows it, rides the translation's detail. */
        ZONED,
        /** A zoned time of day (Cypher's {@code time}): carried as its instant on the epoch day, Pure's DateTime (the
         *  day before or after when the offset crosses midnight); its offset rides the translation's detail. */
        ZONED_TIME,
        /** A duration (Cypher's): an instance of the model's Duration class (months, days, seconds, nanoseconds),
         *  carried as the database's struct. */
        DURATION, JSON, NULL, OTHER;

        public boolean numeric() {
            return this == INTEGER || this == WHOLE || this == FLOAT || this == DECIMAL || this == NUMBER
                    || this == INEXACT;
        }
    }

    /** {@code a / b} of two Integers, cut toward zero: lite's {@code quotient}, the database's integer division (IQ
     *  island, 2026-10-17: {@link #integerQuotient}'s estimate reads each operand about fourteen times, which
     *  nested divisions multiply). The divisor's zero is the caller's to test. */
    public static ValueSpecification wholeQuotient(ValueSpecification a, ValueSpecification b) {
        return call("meta::legend::lite::quotient", a, b);
    }

    /** Postgres's integer division, truncating toward zero, exact for every bigint. With the remainder taken off
     *  ({@code rem} keeps the dividend's sign, as Postgres's {@code %} does) the quotient is a whole number. Pure's
     *  divide computes it in double precision, within 2^-52 of it; shrunk by 2^-50 and truncated, the estimate is never
     *  beyond the quotient, so {@code A - q0 * b} (exact integer arithmetic, never beyond A) is a small multiple of the
     *  divisor, divided once more and added. A divisor of zero is Postgres's error and rem's. */
    public static ValueSpecification integerQuotient(ValueSpecification a, ValueSpecification b) {
        ValueSpecification whole = AppliedFunction.infixRun("minus", List.of(a, call("rem", a, b)));
        ValueSpecification shrunk = AppliedFunction.infixRun("times", List.of(call("divide", whole, b),
                new com.legend.protocol.spec.CFloat(1.0 - Math.pow(2, -50))));
        ValueSpecification estimate = call("if", call("greaterThanEqual", shrunk, new CInteger(0L)),
                thunk(call("floor", shrunk)), thunk(call("ceiling", shrunk)));
        ValueSpecification error = AppliedFunction.infixRun("minus", List.of(whole,
                AppliedFunction.infixRun("times", List.of(estimate, b))));
        return AppliedFunction.infixRun("plus", List.of(estimate, call("round", call("divide", error, b))));
    }

    /** Postgres's numeric promotion for {@code + - *}: integer with integer stays integer; a float wins; else exact. */
    static Kind arithmetic(Kind a, Kind b) {
        if (a == Kind.INTEGER && b == Kind.INTEGER) {
            return Kind.INTEGER;
        }
        // an inexact numeric stays inexact with a double in it: Postgres's double of the exact value is not this one
        if (a == Kind.INEXACT || b == Kind.INEXACT) {
            return Kind.INEXACT;
        }
        if (a == Kind.FLOAT || b == Kind.FLOAT) {
            return Kind.FLOAT;
        }
        if (a == Kind.NUMBER || b == Kind.NUMBER) {
            return Kind.NUMBER;
        }
        if ((a == Kind.WHOLE || b == Kind.WHOLE) && (a == Kind.INTEGER || a == Kind.WHOLE)
                && (b == Kind.INTEGER || b == Kind.WHOLE)) {
            return Kind.WHOLE;
        }
        return Kind.DECIMAL;
    }

    /**
     * A translated expression: its Pure spelling, what is known of its type, and whether it may be NULL. A BOOLEAN is
     * a CONDITION in Postgres's three-valued logic: {@code vs} holds exactly when it is TRUE and {@code whenFalse}
     * exactly when it is FALSE; when it is NULL, neither holds. Both are two-valued Pure ({@code Boolean[1]}), so
     * {@code NOT}, {@code AND} and {@code OR} keep Postgres's meaning (NOT NULL is NULL, not TRUE).
     *
     * <p>A strict operation's value ({@link #strict}) is GUARDED: {@code vs} is {@code if(guard, [], body)}, where
     * {@code body} is never empty when {@code guard} is false. An enclosing strict operation merges the guard into its
     * own and computes on the body, so a chain {@code a + 1 + 2} tests {@code a} once and writes it once; testing and
     * reading the whole guarded value instead doubles the expression at every level (the stress corpus's {@code +}
     * chains: 10 terms were 96 KB of SQL).
     */
    public record Tx(ValueSpecification vs, Kind kind, boolean nullable,
            @com.legend.base.Nullable ValueSpecification whenFalse,
            InputDialect.@com.legend.base.Nullable TypeDetail detail,
            @com.legend.base.Nullable ValueSpecification guard, @com.legend.base.Nullable ValueSpecification body) {

        /** A translation that is not a strict operation's guarded value ({@link #guarded}). */
        public Tx(ValueSpecification vs, Kind kind, boolean nullable,
                @com.legend.base.Nullable ValueSpecification whenFalse,
                InputDialect.@com.legend.base.Nullable TypeDetail detail) {
            this(vs, kind, nullable, whenFalse, detail, null, null);
        }

        /** A translation of no {@code detail}: what a dialect knows of its type beyond its kind
         *  ({@link InputDialect.TypeDetail}) is nothing, as it is in Postgres's for any value but a column's. */
        public Tx(ValueSpecification vs, Kind kind, boolean nullable,
                @com.legend.base.Nullable ValueSpecification whenFalse) {
            this(vs, kind, nullable, whenFalse, null);
        }

        /** A value that is not a condition. */
        public static Tx value(ValueSpecification vs, Kind kind, boolean nullable) {
            return new Tx(vs, kind, nullable, null);
        }

        /** A value that is not a condition, with what the dialect knows of its type beyond its kind. */
        public static Tx value(ValueSpecification vs, Kind kind, boolean nullable,
                InputDialect.@com.legend.base.Nullable TypeDetail detail) {
            return new Tx(vs, kind, nullable, null, detail);
        }

        /** A condition: when it is TRUE, when it is FALSE. */
        public static Tx condition(ValueSpecification whenTrue, ValueSpecification whenFalse, boolean nullable) {
            return new Tx(whenTrue, Kind.BOOLEAN, nullable, whenFalse);
        }

        /** A strict operation's value: empty when {@code guard} holds, else {@code body} (never empty then). */
        static Tx guarded(ValueSpecification guard, ValueSpecification body, Kind kind) {
            return new Tx(call("if", guard, thunk(empty()), thunk(body)), kind, true, null, null, guard, body);
        }

        /** A condition's FALSE case. */
        public ValueSpecification falseWhen() {
            return java.util.Objects.requireNonNull(whenFalse, "a condition has its FALSE case");
        }
    }

    /** A column of the relation in hand: {@code name} is the column the relation holds it in, {@code label} the name
     *  the query sees it by, and {@code var} the lambda parameter that reaches it when it is not this level's row
     *  (a join side's, or an enclosing query's for a correlated reference). */
    record Col(String name, Kind kind, boolean nullable, @com.legend.base.Nullable String var, String label,
            InputDialect.@com.legend.base.Nullable TypeDetail detail) {

        /** A column of no {@link InputDialect.TypeDetail}. */
        Col(String name, Kind kind, boolean nullable, @com.legend.base.Nullable String var, String label) {
            this(name, kind, nullable, var, label, null);
        }

        static Col of(String name, Kind kind, boolean nullable) {
            return new Col(name, kind, nullable, null, name);
        }

        static Col of(String name, Kind kind, boolean nullable,
                InputDialect.@com.legend.base.Nullable TypeDetail detail) {
            return new Col(name, kind, nullable, null, name, detail);
        }

        Col via(@com.legend.base.Nullable String parameter) {
            return new Col(name, kind, nullable, parameter, label, detail);
        }

        Col asNullable() {
            return new Col(name, kind, true, var, label, detail);
        }

        Col named(String to) {
            return new Col(to, kind, nullable, var, label, detail);
        }
    }

    // ------------------------------------------------------------------ names

    /** Postgres's folding of an unquoted identifier: ASCII letters to lower case, nothing else. A function's name is
     *  read so in every dialect (the catalogs' keys, {@link #functionName}); a dialect's identifiers, by its
     *  {@link InputDialect#fold} and {@link InputDialect#resolve} (Postgres's: {@link PostgresDialect}). */
    static String fold(String text) {
        char[] chars = text.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] >= 'A' && chars[i] <= 'Z') {
                chars[i] = (char) (chars[i] + ('a' - 'A'));
            }
        }
        return new String(chars);
    }

    // ------------------------------------------------------------------ scopes

    /** Where a column reference finds its column. */
    interface Scope {
        Col column(SqlAst.ColumnRef ref);

        /** The column {@code ref} names here or in an enclosing query, nearest first; null for none. */
        @com.legend.base.Nullable Col columnOrNull(SqlAst.ColumnRef ref);

        /** The column {@code ref} reads, before any grouping (as an aggregate's argument reads it); null for none. */
        default @com.legend.base.Nullable Col readOrNull(SqlAst.ColumnRef ref) {
            return columnOrNull(ref);
        }

        /** A one-value function's name read as its value, here or in an enclosing query, nearest first; null for
         *  none (Postgres reads a name as a FROM item's only after no query in scope has a column of it). */
        @com.legend.base.Nullable Col functionValue(SqlAst.ColumnRef ref);

        /** Whether a FROM item here or in an enclosing query is named {@code name}. */
        boolean hasTable(SqlAst.Ident name);

        /** The column an expression IS in a grouped scope (a group key or an aggregate); null otherwise. */
        @com.legend.base.Nullable Col grouped(SqlAst.Expr e);

        /** The clause an aggregate would be refused in, or null where aggregates have been collected. */
        @com.legend.base.Nullable String noAggregatesIn();

        /** Whether {@code ref} reads an enclosing query's column. IQ island, 2026-10-17 (the corpus lane's
         *  {@code PARTITION BY YEAR(CreationDate)} beside {@code GROUP BY YEAR(CreationDate)}, which IQ 17.1 reads):
         *  asked of a column under any expression, so a grouped scope answers it without requiring a group key. */
        default boolean enclosing(SqlAst.ColumnRef ref) {
            Col c = columnOrNull(ref);
            return c != null && c.var() != null;
        }
    }

    /** Where an expression is translated: the scope its names are read in, and the lambda parameter its row is in. A
     *  dialect's rule for an expression ({@link InputDialect#binary}, a function of its catalog, ...) translates the
     *  expression's parts here. */
    public final class At {
        private final Scope scope;
        private final String row;

        private At(Scope scope, String row) {
            this.scope = scope;
            this.row = row;
        }

        /** {@code e}'s translation here. */
        public Tx tx(SqlAst.Expr e) {
            return SqlToPure.this.tx(e, scope, row);
        }

        /** {@code b}, an AND or an OR, as Kleene's: a chain of one of them read as its operands left to right, each a
         *  condition ({@link SqlToPure#truth}), with no recursion per operator, and combined as a balanced tree (a
         *  dialect's rule for AND and OR, so that a 2,000-term OR translates on a default thread stack). */
        public Tx logical(SqlAst.Binary b) {
            return SqlToPure.this.logical(b, scope, row);
        }

        /** IQ island, 2026-10-14 (M1e): whether {@code ref} names a column here (of this query's FROM items or an
         *  enclosing query's), as a reference to it would read it ({@link InputDialect#parameter}). */
        public boolean hasColumn(SqlAst.ColumnRef ref) {
            return scope.columnOrNull(ref) != null;
        }

        /** The translation this place is in: for an input language's rules in this package, which read the
         *  translator throughout ({@link PostgresDialect}'s, D11). */
        SqlToPure translator() {
            return SqlToPure.this;
        }

        /** Where names are read here. */
        Scope scope() {
            return scope;
        }

        /** The lambda parameter the row is in here. */
        String row() {
            return row;
        }
    }

    /** One of a dialect's own aggregates as it computes it ({@link InputDialect#aggregate}): the grouped relation's
     *  columns it is computed from (each a map over the input's rows and a reduce over a group's values, as Pure's
     *  groupBy and aggregate take them), the column computed from those after the grouping when the aggregate is not
     *  one of them (an {@code extend}'s, named {@link Grouping#name()}), and what is known of its type. */
    public record Aggregation(List<ColSpec> specs, @com.legend.base.Nullable ColSpec finish, Kind kind,
            boolean nullable, InputDialect.@com.legend.base.Nullable TypeDetail detail,
            @com.legend.base.Nullable ValueSpecification guard) {
        public Aggregation {
            specs = List.copyOf(specs);
        }

        /** An aggregate of no guard. */
        public Aggregation(List<ColSpec> specs, @com.legend.base.Nullable ColSpec finish, Kind kind, boolean nullable,
                InputDialect.@com.legend.base.Nullable TypeDetail detail) {
            this(specs, finish, kind, nullable, detail, null);
        }
    }

    /** Where a dialect's aggregate is computed ({@link InputDialect#aggregate}): its arguments translated over the
     *  grouped query's input rows, the lambdas of a map (over a row), a reduce (over a group's values) and a finish
     *  (over a grouped row), fresh names for the columns it is computed from, and the name of its own column. */
    public final class Grouping {
        private final Input in;
        private final Set<String> names;
        private final String name;
        /** The grouped relation's columns so far, by the canonical form of what they are: its keys first. */
        private final Map<String, Col> grouped;

        private Grouping(Input in, Set<String> names, String name, Map<String, Col> grouped) {
            this.in = in;
            this.names = names;
            this.name = name;
            this.grouped = grouped;
        }

        /** The translation this grouping is in, its input scope and the grouped relation's names so far: for an
         *  input language's rules in this package ({@link PostgresDialect}'s aggregates, D11). */
        SqlToPure translator() {
            return SqlToPure.this;
        }

        Input input() {
            return in;
        }

        Set<String> names() {
            return names;
        }

        /** IQ island, 2026-10-14 (the third landing): whether {@code e}, no aggregate, is one of the query's group
         *  keys (as the translator matches a select item with one), one value in each group. */
        public boolean groupKey(SqlAst.Expr e) {
            return grouped.containsKey(canon(e, in));
        }

        /** {@code e}'s translation over an input row. */
        public Tx tx(SqlAst.Expr e) {
            return SqlToPure.this.tx(e, in, row);
        }

        /** The name the aggregate's own column takes. */
        public String name() {
            return name;
        }

        /** A name no other column of the grouped relation has. */
        public String fresh() {
            names.add(name);
            return SqlToPure.this.fresh(names);
        }

        /** {@code {r | body}}: a map's lambda over an input row, or a finish's over a grouped row. */
        public LambdaFunction overRow(ValueSpecification body) {
            return lambda(row, body);
        }

        /** {@code {y | body}}: a reduce's lambda over the group's values ({@link #values()}). */
        public LambdaFunction overGroup(ValueSpecification body) {
            return lambda(group, body);
        }

        /** The group's values, in a reduce. */
        public ValueSpecification values() {
            return new Variable(group);
        }

        /** The grouped relation's column {@code column}, in a finish. */
        public ValueSpecification column(String column) {
            return property(row, column);
        }

        /** Whether {@code a} and {@code b} are one expression to the translator (as a GROUP BY key and a select item
         *  are matched). */
        public boolean same(SqlAst.Expr a, SqlAst.Expr b) {
            return canon(a, in).equals(canon(b, in));
        }

        /** The input column {@code ref} reads, as the relation names it, when it is one of this query's FROM items'
         *  columns; null otherwise (an enclosing query's, or no column). */
        public @com.legend.base.Nullable String inputColumn(SqlAst.ColumnRef ref) {
            Col c = in.columnOrNull(ref);
            return c == null || c.var() != null ? null : c.name();
        }

        /** Whether {@code ref} reads an enclosing query's column (a correlated reference). */
        public boolean enclosing(SqlAst.ColumnRef ref) {
            Col c = in.columnOrNull(ref);
            return c != null && c.var() != null;
        }
    }

    /** IQ island, 2026-10-15 (windows): one of a dialect's window functions as it computes it
     *  ({@link InputDialect#window}): the columns it is computed from over its window (each an {@code extend} of the
     *  window's {@code over()} and a column of {@code {p, w, r | ...}} and an optional reduce, as Pure's extend takes
     *  them), the column computed from those over each row after them when the function is not one of them (named
     *  {@link Windowing#name()}), and what is known of its type. */
    public record WindowFunction(List<ColSpec> windows, @com.legend.base.Nullable ColSpec finish, Kind kind,
            boolean nullable, InputDialect.@com.legend.base.Nullable TypeDetail detail) {
        public WindowFunction {
            windows = List.copyOf(windows);
        }
    }

    /** IQ island, 2026-10-15 (windows): where a dialect's window function is computed ({@link InputDialect#window}):
     *  its window as written (a named window's clauses in place), its arguments as columns of the relation, the
     *  window's sort keys' types, the lambdas of a window column ({@code {p, w, r | ...}}), a reduce (over a frame's
     *  values) and a finish (over a row), fresh names, and the name of its own column. */
    public final class Windowing {
        private final Scope scope;
        private final Set<String> present;
        private final List<ColSpec> computed;
        private final SqlAst.WindowSpec spec;
        private final List<Tx> sortKeys;
        /** The name the select item gives the call (null for none), and the column's name once taken. */
        private final @com.legend.base.Nullable String want;
        private @com.legend.base.Nullable String name;

        private Windowing(Scope scope, Set<String> present, List<ColSpec> computed, SqlAst.WindowSpec spec,
                List<Tx> sortKeys, @com.legend.base.Nullable String want) {
            this.scope = scope;
            this.present = present;
            this.computed = computed;
            this.spec = spec;
            this.sortKeys = sortKeys;
            this.want = want;
        }

        /** The window, its named base's clauses in place. */
        public SqlAst.WindowSpec spec() {
            return spec;
        }

        /** Whether {@code ref} reads an enclosing query's column (a correlated reference). */
        public boolean enclosing(SqlAst.ColumnRef ref) {
            return scope.enclosing(ref);
        }

        /** The window's ORDER BY keys' translations (as the keys the window sorts by: a dialect's collation key in a
         *  string's place), in order. */
        public List<Tx> sortKeys() {
            return sortKeys;
        }

        /** {@code e}'s translation over a row of the relation the window is over. */
        public Tx tx(SqlAst.Expr e) {
            return SqlToPure.this.tx(e, scope, row);
        }

        /** {@code e} as a column of the relation the window is over (a column, a group key or an aggregate as
         *  itself; any other expression computed first, under a fresh name): its value read from a row
         *  ({@link #row()}), with its kind, nullability and type detail. */
        public Tx column(SqlAst.Expr e) {
            Col c = windowKey(e, scope, present, computed);
            return Tx.value(property(row, c.name()), c.kind(), c.nullable(), c.detail());
        }

        /** {@code value}, a translation over a row of the relation (none of the window's columns), computed first as
         *  a column of its own: its value read from a row. */
        public Tx computed(Tx value) {
            String n = SqlToPure.this.fresh(present);
            computed.add(new ColSpec(n, lambda(row, valueOf(value)), null, null));
            return Tx.value(property(row, n), value.kind(), value.nullable(), value.detail());
        }

        /** The name the function's own column takes: its select item's, else a fresh one. */
        public String name() {
            String n = name;
            if (n == null) {
                String w = want;
                n = w != null && !present.contains(w) && !w.startsWith(SYNTHETIC) ? w : SqlToPure.this.fresh(present);
                present.add(n);
                name = n;
            }
            return n;
        }

        /** A name no other column of the relation has. */
        public String fresh() {
            name();
            return SqlToPure.this.fresh(present);
        }

        /** {@code {p, w, r | body}}: a window column's lambda. */
        public LambdaFunction overWindow(ValueSpecification body) {
            return windowLambda(body);
        }

        /** {@code {y | body}}: a reduce's lambda over a frame's values ({@link #values()}). */
        public LambdaFunction overGroup(ValueSpecification body) {
            return lambda(group, body);
        }

        /** {@code {r | body}}: a finish's lambda over a row. */
        public LambdaFunction overRow(ValueSpecification body) {
            return lambda(row, body);
        }

        /** A frame's values, in a reduce. */
        public ValueSpecification values() {
            return new Variable(group);
        }

        /** The partition, the window (its frame) and the row, in a window column's lambda. */
        public ValueSpecification partition() {
            return new Variable(partition);
        }

        public ValueSpecification window() {
            return new Variable(window);
        }

        public ValueSpecification row() {
            return new Variable(row);
        }

        /** The relation's column {@code column} of a row, in a finish (the window's columns computed). */
        public ValueSpecification column(String column) {
            return property(row, column);
        }
    }

    /** A FROM item's columns as the query names them: its qualifier (an alias, a table's or function's name; none
     *  for an unaliased subquery), and its columns. */
    private record Binding(SqlAst.@com.legend.base.Nullable Ident qualifier, List<BCol> columns, boolean scalar) {

        /** A binding whose name is no value (a table's: its whole row, not supported here). */
        Binding(SqlAst.@com.legend.base.Nullable Ident qualifier, List<BCol> columns) {
            this(qualifier, columns, false);
        }

        List<String> names(boolean bareOnly) {
            List<String> names = new ArrayList<>();
            for (BCol c : columns) {
                if (c.bare() || !bareOnly) {
                    names.add(c.col().label());
                }
            }
            return names;
        }

        Binding via(@com.legend.base.Nullable String parameter) {
            List<BCol> cols = new ArrayList<>();
            for (BCol c : columns) {
                cols.add(new BCol(c.col().via(parameter), c.bare()));
            }
            return new Binding(qualifier, cols, scalar);
        }

        Binding asNullable() {
            List<BCol> cols = new ArrayList<>();
            for (BCol c : columns) {
                cols.add(new BCol(c.col().asNullable(), c.bare()));
            }
            return new Binding(qualifier, cols, scalar);
        }
    }

    /** A binding's column; {@code bare} when an unqualified name reaches it (a USING join's two sides' columns are
     *  reached only qualified: the unqualified name is the merged column). */
    private record BCol(Col col, boolean bare) {
    }

    /** The FROM clause's relation, its bindings, and its columns in {@code SELECT *}'s order. */
    private record Source(ValueSpecification relation, List<Binding> bindings, List<Col> star) {
    }

    /** The FROM clause's columns, before any grouping; names it does not have are the enclosing query's. */
    final class Input implements Scope {
        private final List<Binding> bindings;
        private final List<Col> star;
        private final String clause;

        Input(List<Binding> bindings, List<Col> star, String clause) {
            this.bindings = bindings;
            this.star = star;
            this.clause = clause;
        }

        Input in(String otherClause) {
            return new Input(bindings, star, otherClause);
        }

        /** The relation's columns, by the names it holds them under. */
        List<String> names() {
            List<String> names = new ArrayList<>();
            for (Binding b : bindings) {
                for (BCol c : b.columns()) {
                    names.add(c.col().name());
                }
            }
            return names;
        }

        /** True iff an unqualified {@code name} is a column here (Postgres reads a GROUP BY name as one first). */
        boolean has(SqlAst.Ident name) {
            for (Binding b : bindings) {
                if (dialect.resolve(name, b.names(true), "column") != null) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Col column(SqlAst.ColumnRef ref) {
            Col found = columnOrNull(ref);
            if (found == null) {
                found = functionValue(ref);
            }
            return found != null ? found : notFound(ref);
        }

        @Override
        public @com.legend.base.Nullable Col columnOrNull(SqlAst.ColumnRef ref) {
            Col found = find(ref);
            if (found != null) {
                return found;
            }
            Scope outer = env.outer();
            if (outer == null) {
                return null;
            }
            dialect.columnNotHere(ref);
            // the enclosing query's column, read from its row's lambda parameter
            Col c = outer.columnOrNull(ref);
            if (c != null) {
                dialect.enclosingColumn(ref);
            }
            return c == null || c.var() != null ? c : c.via(env.outerRow());
        }

        @Override
        public @com.legend.base.Nullable Col functionValue(SqlAst.ColumnRef ref) {
            if (ref.parts().size() != 1) {
                return null;
            }
            Binding named = binding(ref.parts().get(0));
            if (named != null) {
                // a function returning one value: its name is that value (FROM jsonb_array_elements(a) e ...
                // e ->> 'x'); a table's name is its whole row, refused by notFound
                return named.scalar() && named.columns().size() == 1 ? named.columns().get(0).col() : null;
            }
            Scope outer = env.outer();
            if (outer == null) {
                return null;
            }
            Col c = outer.functionValue(ref);
            return c == null || c.var() != null ? c : c.via(env.outerRow());
        }

        /** The column {@code ref} names here, or null for none (an enclosing query may have it). */
        @com.legend.base.Nullable Col find(SqlAst.ColumnRef ref) {
            List<SqlAst.Ident> parts = ref.parts();
            if (parts.size() > 2) {
                throw SqlTranslationException.unsupported("schema-qualified column references are not supported yet",
                        ref.pos());
            }
            SqlAst.Ident name = parts.get(parts.size() - 1);
            if (parts.size() == 2) {
                Binding b = binding(parts.get(0));
                if (b == null) {
                    return null;
                }
                String label = dialect.resolve(name, b.names(false), "column");
                if (label == null) {
                    throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_QUALIFIED_COLUMN,
                            parts.get(0).text(), name.text(), dialect.hint(name, b.names(false))), name.pos());
                }
                return column(b, label, false);
            }
            Col match = null;
            for (Binding b : bindings) {
                String label = dialect.resolve(name, b.names(true), "column");
                if (label != null) {
                    if (match != null) {
                        throw SqlTranslationException.invalid(dialect.message(Problem.AMBIGUOUS_COLUMN, name.text()),
                                name.pos());
                    }
                    match = column(b, label, true);
                }
            }
            return match;
        }

        private Col column(Binding b, String label, boolean bareOnly) {
            for (BCol c : b.columns()) {
                if (c.col().label().equals(label) && (c.bare() || !bareOnly)) {
                    return c.col();
                }
            }
            throw new IllegalStateException("resolved to a column not in the binding: " + label);
        }

        /** The binding a qualifier names, or null. */
        @com.legend.base.Nullable Binding binding(SqlAst.Ident qualifier) {
            Binding found = null;
            for (Binding b : bindings) {
                SqlAst.Ident q = b.qualifier();
                if (q != null && dialect.resolve(qualifier, List.of(q.quoted() ? q.text() : dialect.fold(q.text())),
                        "table") != null) {
                    if (found != null) {
                        throw SqlTranslationException.invalid(dialect.message(Problem.AMBIGUOUS_TABLE,
                                qualifier.text()), qualifier.pos());
                    }
                    found = b;
                }
            }
            return found;
        }

        @Override
        public boolean hasTable(SqlAst.Ident name) {
            return binding(name) != null || env.outer() != null && env.outer().hasTable(name);
        }

        private Col notFound(SqlAst.ColumnRef ref) {
            List<SqlAst.Ident> parts = ref.parts();
            SqlAst.Ident name = parts.get(parts.size() - 1);
            if (parts.size() == 2) {
                throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_TABLE, parts.get(0).text()),
                        parts.get(0).pos());
            }
            Binding named = binding(name);
            if (named != null || env.outer() != null && env.outer().hasTable(name)) {
                // Postgres reads a FROM item's name as its whole row (row_to_json(persons))
                throw SqlTranslationException.unsupported("whole-row references (\"" + name.text() + "\" as a value)"
                        + " are not supported yet", name.pos());
            }
            List<String> all = new ArrayList<>();
            for (Binding b : bindings) {
                all.addAll(b.names(false));
            }
            throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_COLUMN, name.text(),
                    dialect.hint(name, all)), name.pos());
        }

        @Override
        public @com.legend.base.Nullable Col grouped(SqlAst.Expr e) {
            return null;
        }

        @Override
        public @com.legend.base.Nullable String noAggregatesIn() {
            return clause;
        }
    }

    /** The grouped relation's columns: the group keys and the aggregates, by the canonical form of what they are. */
    private final class Grouped implements Scope {
        private final Input input;
        private final Map<String, Col> byCanon;
        private final Map<String, Col> keyBySource;

        Grouped(Input input, Map<String, Col> byCanon, Map<String, Col> keyBySource) {
            this.input = input;
            this.byCanon = byCanon;
            this.keyBySource = keyBySource;
        }

        @Override
        public Col column(SqlAst.ColumnRef ref) {
            return key(input.column(ref), ref);
        }

        @Override
        public @com.legend.base.Nullable Col columnOrNull(SqlAst.ColumnRef ref) {
            Col source = input.columnOrNull(ref);
            return source == null ? null : key(source, ref);
        }

        @Override
        public @com.legend.base.Nullable Col readOrNull(SqlAst.ColumnRef ref) {
            return input.columnOrNull(ref);
        }

        @Override
        public @com.legend.base.Nullable Col functionValue(SqlAst.ColumnRef ref) {
            Col source = input.functionValue(ref);
            return source == null ? null : key(source, ref);
        }

        @Override
        public boolean hasTable(SqlAst.Ident name) {
            return input.hasTable(name);
        }

        private Col key(Col source, SqlAst.ColumnRef ref) {
            if (source.var() != null) {
                // the enclosing query's column: one value per group, as Postgres has it
                return source;
            }
            Col key = keyBySource.get(source.name());
            if (key == null) {
                throw SqlTranslationException.invalid(dialect.message(Problem.NOT_GROUPED, text(ref), ref), ref.pos());
            }
            return key;
        }

        @Override
        public @com.legend.base.Nullable Col grouped(SqlAst.Expr e) {
            if (e instanceof SqlAst.ColumnRef) {
                return null;
            }
            return byCanon.get(canon(e, input));
        }

        @Override
        public boolean enclosing(SqlAst.ColumnRef ref) {
            Col source = input.columnOrNull(ref);
            return source != null && source.var() != null;
        }

        @Override
        public @com.legend.base.Nullable String noAggregatesIn() {
            return null;
        }
    }

    private static String text(SqlAst.ColumnRef ref) {
        return String.join(".", ref.parts().stream().map(SqlAst.Ident::text).toList());
    }

    // ------------------------------------------------------------------ the query

    /**
     * A set-returning function as a whole select-list item ({@code SELECT jsonb_array_elements(a) FROM t}): Postgres
     * reads it after FROM and WHERE, one row per element, the other items repeated; here it is a LATERAL item after
     * the query's FROM, and the item its column. One such function per select list (two run in lockstep in Postgres),
     * none with grouping, aggregates, windows or DISTINCT; one inside an expression is refused.
     */
    private record SelectListSet(SqlAst.Select select, SqlAst.@com.legend.base.Nullable TableFunction function) {
    }

    private SelectListSet selectListSetReturning(SqlAst.Select s) {
        int found = -1;
        for (int i = 0; i < s.items().size(); i++) {
            if (s.items().get(i) instanceof SqlAst.SelectExpr x) {
                if (x.expr() instanceof SqlAst.FunctionCall f && setReturning(f)) {
                    if (found >= 0) {
                        throw SqlTranslationException.unsupported("two set-returning functions in one select list are"
                                + " not supported yet", f.pos());
                    }
                    found = i;
                    for (SqlAst.Expr a : f.args()) {
                        noSetReturning(a);
                    }
                } else {
                    noSetReturning(x.expr());
                }
            }
        }
        if (found < 0) {
            return new SelectListSet(s, null);
        }
        SqlAst.SelectExpr item = (SqlAst.SelectExpr) s.items().get(found);
        SqlAst.FunctionCall f = (SqlAst.FunctionCall) item.expr();
        if (s.distinct() || !s.groupBy().isEmpty() || s.having() != null || hasAggregate(s) || !s.windows().isEmpty()
                || hasWindow(s)) {
            throw SqlTranslationException.unsupported("a set-returning function in a select list with grouping,"
                    + " aggregates, windows or DISTINCT is not supported yet", f.pos());
        }
        if (f.distinct() || f.star()) {
            throw SqlTranslationException.invalid(functionName(f.name().get(f.name().size() - 1)) + " is not an"
                    + " aggregate", f.pos());
        }
        String name = item.alias() != null ? item.alias().quoted() ? item.alias().text()
                : dialect.fold(item.alias().text()) : functionName(f.name().get(f.name().size() - 1));
        // its column's name is no name the query can write: WHERE cannot read it, nor * list it
        SqlAst.Ident table = new SqlAst.Ident(SYNTHETIC + "set", true, f.pos());
        SqlAst.Ident column = new SqlAst.Ident(SYNTHETIC + "value", true, f.pos());
        List<SqlAst.Arg> args = new ArrayList<>();
        f.args().forEach(a -> args.add(new SqlAst.Arg(null, a)));
        List<SqlAst.SelectItem> items = new ArrayList<>(s.items());
        items.set(found, new SqlAst.SelectExpr(new SqlAst.ColumnRef(List.of(table, column), f.pos()),
                new SqlAst.Ident(name, true, f.pos()), item.pos()));
        return new SelectListSet(new SqlAst.Select(false, items, s.from(), s.where(), s.groupBy(), s.having(),
                s.windows(), s.orderBy(), s.limit(), s.offset(), s.pos()),
                new SqlAst.TableFunction(f.name(), args, table, List.of(column), f.pos()));
    }

    private static boolean setReturning(SqlAst.FunctionCall f) {
        return f.name().size() == 1 && SET_RETURNING.containsKey(functionName(f.name().get(0)));
    }

    /** Whether {@code e} calls a set-returning function anywhere in it (the parser's VALUES rows, where Postgres
     *  allows none). */
    static boolean containsSetReturning(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && setReturning(f)
                || children(e).stream().anyMatch(SqlToPure::containsSetReturning);
    }

    /** The error of {@code e} as a VALUES row's cell, or null: a set-returning function, an aggregate or a window
     *  function in it, outside a subquery (the parser's VALUES lists, which have no rows to read). */
    static @com.legend.base.Nullable String notInValues(SqlAst.Expr e) {
        return notAllowedIn("VALUES", e);
    }

    /** The error of {@code e} in the clause {@code clause}, which reads no group and no rows but its own (VALUES,
     *  RETURNING), or null: a set-returning function, a window function or an aggregate in it, outside a subquery. */
    static @com.legend.base.Nullable String notAllowedIn(String clause, SqlAst.Expr e) {
        if (containsSetReturning(e)) {
            return "set-returning functions are not allowed in " + clause;
        }
        if (containsWindow(e)) {
            return "window functions are not allowed in " + clause;
        }
        return anyAggregateCall(e) ? "aggregate functions are not allowed in " + clause : null;
    }

    private static boolean anyAggregateCall(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                && AGGREGATES.contains(functionName(f.name().get(0)))
                || children(e).stream().anyMatch(SqlToPure::anyAggregateCall);
    }

    private static void noSetReturning(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && setReturning(f)) {
            throw SqlTranslationException.unsupported("a set-returning function inside an expression is not supported"
                    + " yet", f.pos());
        }
        children(e).forEach(SqlToPure::noSetReturning);
    }

    private static boolean hasWindow(SqlAst.Select s) {
        for (SqlAst.SelectItem i : s.items()) {
            if (i instanceof SqlAst.SelectExpr x && containsWindow(x.expr())) {
                return true;
            }
        }
        return false;
    }

    private Rel select(SqlAst.Select query) {
        SelectListSet set = selectListSetReturning(dialect.grouping(query));
        SqlAst.Select s = set.select();
        SqlAst.TableFunction setFunction = set.function();

        if (s.items().isEmpty()) {
            throw SqlTranslationException.unsupported("a select list with no columns is not supported yet", s.pos());
        }
        if (s.from().isEmpty() && s.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns)) {
            throw SqlTranslationException.invalid("with no tables specified, SELECT * is not valid", s.pos());
        }
        // no FROM: one row, of no columns the query can name
        String one = "\n" + SYNTHETIC + "row\n1\n";
        Source src = !s.from().isEmpty() ? null : setFunction != null ? from(setFunction)
                : new Source(com.legend.protocol.spec.TdsLiteral.of(one, null), List.of(), List.of());
        // IQ island, 2026-10-15 (checklist 0.6): a WHERE of T-SQL outer join predicates makes the FROM clause its joins
        TsqlFrom tsql = s.from().isEmpty() ? null : tsqlOuterJoins(s, setFunction != null);
        if (tsql != null) {
            src = tsql.source();
        }
        for (SqlAst.FromItem item : tsql != null ? List.<SqlAst.FromItem>of() : s.from()) {
            // a comma is a cross join, binding looser than JOIN (the ON of a JOIN sees its own two sides only); a
            // function or a LATERAL subquery reads the items before it
            if (src != null && lateral(item)) {
                src = lateralJoin(src, item, SqlAst.JoinType.CROSS, null, item.pos());
                continue;
            }
            Source next = from(item);
            src = src == null ? next : join(src, next, SqlAst.JoinType.CROSS, null, List.of(), false, item.pos());
        }
        Source source = java.util.Objects.requireNonNull(src);
        Input input = new Input(source.bindings(), source.star(), "WHERE");
        if (env.outer() != null) {
            noOuterAggregates(s, input);
        }
        ValueSpecification relation = source.relation();
        SqlAst.Expr where = tsql != null ? tsql.where() : s.where();
        if (where != null) {
            relation = call("filter", relation, lambda(row, condition(where, input, "WHERE")));
        }
        if (setFunction != null && !s.from().isEmpty()) {
            // a select list's set-returning function reads the rows WHERE keeps, as in Postgres; * lists FROM's
            Source with = lateralJoin(new Source(relation, source.bindings(), source.star()), setFunction,
                    SqlAst.JoinType.CROSS, null, setFunction.pos());
            input = new Input(with.bindings(), source.star(), "WHERE");
            relation = with.relation();
        }
        boolean aggregate = !s.groupBy().isEmpty() || hasAggregate(s);
        return aggregate ? grouped(s, input.in("GROUP BY"), relation) : plain(s, input.in("the select list"), relation);
    }

    // ------------------------------------------------------------------ T-SQL outer joins

    /** A query's FROM clause made of its WHERE's T-SQL outer joins ({@link #tsqlOuterJoins}), and the WHERE's other
     *  conditions (null for none). */
    private record TsqlFrom(Source source, SqlAst.@com.legend.base.Nullable Expr where) {
    }

    /** Whether {@code e} is a T-SQL outer join predicate: {@code a *= b} keeps a's rows, {@code a =* b} b's. */
    private static boolean tsqlOuter(SqlAst.Expr e) {
        return e instanceof SqlAst.Operator o && o.left() != null && (o.operator().equals("*=")
                || o.operator().equals("=*"));
    }

    /**
     * IQ island, 2026-10-15 (checklist 0.6, measured on IQ 17.1): the FROM clause of a query whose WHERE has T-SQL outer
     * join predicates at its top level, where the dialect reads them ({@link InputDialect#readsTsqlOuterJoins}), as ANSI
     * joins; null when it has none. Each {@code a *= b} (or {@code b =* a}) between two FROM items keeps a's rows and
     * NULL-extends b's. The items no predicate NULL-extends are joined first, in FROM order; then each other one by a
     * LEFT JOIN whose ON holds its predicates (as equalities) and every condition of the WHERE that reads it -- alone, or
     * with the item that keeps it -- and the WHERE keeps the rest. Refused by name, as IQ raises or reads otherwise: an
     * ANSI join among the FROM items (IQ's -681); an item NULL-extended by two items (IQ's -136, a cycle) or both kept
     * and NULL-extended (IQ's -680); a predicate whose side reads no FROM item or more than one, or with a subquery; a
     * predicate below the WHERE's top level; a condition that reads two NULL-extended items, or one and an item that
     * does not keep it; a condition with a subquery left in the WHERE (whether it reads a NULL-extended item is not
     * read); SELECT * where a NULL-extended item comes before one that is not (its columns' order).
     */
    private @com.legend.base.Nullable TsqlFrom tsqlOuterJoins(SqlAst.Select s, boolean setFunction) {
        SqlAst.Expr where = s.where();
        if (where == null || !dialect.readsTsqlOuterJoins()) {
            return null;
        }
        List<SqlAst.Expr> conjuncts = new ArrayList<>();
        conjuncts(where, conjuncts);
        SqlAst.Expr first = conjuncts.stream().filter(SqlToPure::tsqlOuter).findFirst().orElse(null);
        if (first == null) {
            return null;
        }
        if (setFunction) {
            throw SqlTranslationException.unsupported("a T-SQL outer join in a query with a set-returning function in"
                    + " its select list is not supported", first.pos());
        }
        int n = s.from().size();
        List<Source> items = new ArrayList<>();
        for (SqlAst.FromItem item : s.from()) {
            if (item instanceof SqlAst.Join || lateral(item)) {
                throw SqlTranslationException.unsupported("a T-SQL outer join (*=, =*) beside a JOIN in its FROM clause"
                        + " is not supported: IQ's error -681", item.pos());
            }
            items.add(from(item));
        }
        int[] keptBy = new int[n];
        java.util.Arrays.fill(keptBy, -1);
        boolean[] keeps = new boolean[n];
        List<List<SqlAst.Expr>> on = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            on.add(new ArrayList<>());
        }
        for (SqlAst.Expr c : conjuncts) {
            if (!(tsqlOuter(c) && c instanceof SqlAst.Operator o)) {
                continue;
            }
            SqlAst.Expr left = java.util.Objects.requireNonNull(o.left());
            int l = oneItem(left, items, o);
            int r = oneItem(o.right(), items, o);
            if (l == r) {
                throw SqlTranslationException.unsupported("a T-SQL outer join whose two sides read one FROM item is not"
                        + " supported", o.pos());
            }
            int kept = o.operator().equals("*=") ? l : r;
            int nulls = kept == l ? r : l;
            if (keptBy[nulls] >= 0 && keptBy[nulls] != kept) {
                throw SqlTranslationException.unsupported("a FROM item NULL-extended by T-SQL outer joins with two items"
                        + " is IQ's error -136 (an outer join cycle)", o.pos());
            }
            keptBy[nulls] = kept;
            keeps[kept] = true;
            on.get(nulls).add(new SqlAst.Binary(SqlAst.BinaryOp.EQ, left, o.right(), o.pos()));
        }
        for (int i = 0; i < n; i++) {
            if (keptBy[i] >= 0 && keeps[i]) {
                throw SqlTranslationException.unsupported("a FROM item both kept and NULL-extended by T-SQL outer joins"
                        + " (a chain) is IQ's error -680", first.pos());
            }
        }
        List<SqlAst.Expr> rest = new ArrayList<>();
        for (SqlAst.Expr c : conjuncts) {
            if (tsqlOuter(c)) {
                continue;
            }
            if (anyTsqlOuter(c)) {
                throw SqlTranslationException.unsupported("a T-SQL outer join predicate inside OR, NOT or another"
                        + " expression is not supported", c.pos());
            }
            java.util.SortedSet<Integer> read = itemsRead(c, items);
            List<Integer> extended = read.stream().filter(i -> keptBy[i] >= 0).toList();
            if (extended.isEmpty()) {
                if (hasSubquery(c)) {
                    throw SqlTranslationException.unsupported("a condition with a subquery beside a T-SQL outer join is"
                            + " not supported yet: whether it reads the NULL-extended item is not read", c.pos());
                }
                rest.add(c);
                continue;
            }
            int nulls = extended.get(0);
            if (extended.size() > 1 || read.stream().anyMatch(i -> i != nulls && i != keptBy[nulls])) {
                throw SqlTranslationException.unsupported("a condition beside a T-SQL outer join that reads its"
                        + " NULL-extended item with another item than the one that keeps it is not supported", c.pos());
            }
            on.get(nulls).add(c);
        }
        Source joined = null;
        boolean extendedBefore = false;
        boolean reordered = false;
        for (int i = 0; i < n; i++) {
            if (keptBy[i] >= 0) {
                extendedBefore = true;
                continue;
            }
            reordered |= extendedBefore;
            joined = joined == null ? items.get(i) : join(joined, items.get(i), SqlAst.JoinType.CROSS, null, List.of(),
                    false, s.from().get(i).pos());
        }
        if (reordered && s.items().stream().anyMatch(i -> i instanceof SqlAst.AllColumns a && a.qualifier() == null)) {
            throw SqlTranslationException.unsupported("an unqualified * in the select list of a T-SQL outer join whose"
                    + " NULL-extended table comes before another table is not supported (the columns' order)",
                    first.pos());
        }
        for (int i = 0; i < n; i++) {
            if (keptBy[i] >= 0) {
                joined = join(java.util.Objects.requireNonNull(joined), items.get(i), SqlAst.JoinType.LEFT,
                        and(on.get(i)), List.of(), false, s.from().get(i).pos());
            }
        }
        return new TsqlFrom(java.util.Objects.requireNonNull(joined), rest.isEmpty() ? null : and(rest));
    }

    /** {@code e}'s conditions joined by AND at its top level, in order. */
    private static void conjuncts(SqlAst.Expr e, List<SqlAst.Expr> out) {
        if (e instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.AND) {
            conjuncts(b.left(), out);
            conjuncts(b.right(), out);
        } else {
            out.add(e);
        }
    }

    /** {@code conditions} joined by AND. */
    private static SqlAst.Expr and(List<SqlAst.Expr> conditions) {
        SqlAst.Expr all = conditions.get(0);
        for (SqlAst.Expr c : conditions.subList(1, conditions.size())) {
            all = new SqlAst.Binary(SqlAst.BinaryOp.AND, all, c, c.pos());
        }
        return all;
    }

    /** Whether {@code e} holds a T-SQL outer join predicate anywhere. */
    private static boolean anyTsqlOuter(SqlAst.Expr e) {
        return tsqlOuter(e) || children(e).stream().anyMatch(SqlToPure::anyTsqlOuter);
    }

    /** Whether {@code e} holds a subquery. */
    private static boolean hasSubquery(SqlAst.Expr e) {
        return e instanceof SqlAst.ScalarSubquery || e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery
                || e instanceof SqlAst.Quantified || children(e).stream().anyMatch(SqlToPure::hasSubquery);
    }

    /** The FROM items (their indexes in {@code items}) whose columns {@code e} reads, outside its subqueries: a name two
     *  of them have is ambiguous, one none has an enclosing query's. */
    private java.util.SortedSet<Integer> itemsRead(SqlAst.Expr e, List<Source> items) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columnRefs(e, refs);
        java.util.SortedSet<Integer> read = new java.util.TreeSet<>();
        for (SqlAst.ColumnRef ref : refs) {
            int found = -1;
            for (int i = 0; i < items.size(); i++) {
                if (new Input(items.get(i).bindings(), items.get(i).star(), "WHERE").find(ref) != null) {
                    if (found >= 0) {
                        throw SqlTranslationException.invalid(dialect.message(Problem.AMBIGUOUS_COLUMN,
                                ref.parts().get(ref.parts().size() - 1).text()), ref.pos());
                    }
                    found = i;
                }
            }
            if (found >= 0) {
                read.add(found);
            }
        }
        return read;
    }

    /** The one FROM item a side of T-SQL outer join predicate {@code o} reads. */
    private int oneItem(SqlAst.Expr side, List<Source> items, SqlAst.Operator o) {
        java.util.SortedSet<Integer> read = itemsRead(side, items);
        if (read.size() != 1 || hasSubquery(side)) {
            throw SqlTranslationException.unsupported("a T-SQL outer join whose side reads no FROM item, more than one,"
                    + " or a subquery is not supported", o.pos());
        }
        return read.first();
    }

    // ------------------------------------------------------------------ FROM

    /** One FROM item: a table, a WITH query, a subquery, or a join. */
    private Source from(SqlAst.FromItem item) {
        return switch (item) {
            case SqlAst.TableName t -> {
                Rel cte = t.name().size() == 1 ? cte(t.name().get(0)) : null;
                SqlAst.Ident alias = t.alias();
                if (cte == null) {
                    Rel own = relation(dialect.tableSource(t, sources));
                    // IQ island, 2026-10-14 (M1e): a table the dialect names in the model (Postgres's: none)
                    ValueSpecification table = own == null ? dialect.table(t, sources) : null;
                    if (table != null) {
                        List<OutCol> columns = new ArrayList<>();
                        sources.columns(table, (name, type, nullable, storeType) -> columns.add(new OutCol(name,
                                dialect.kindOf(type, storeType), nullable, dialect.detailOf(type, storeType))));
                        own = read(new Rel(table, columns), t.pos());
                    }
                    if (own == null) {
                        throw SqlTranslationException.unsupported("a table name needs a source function for now:"
                                + " write " + TABLE_FUNCTION + "('pkg::Database.schema.table')", t.pos());
                    }
                    yield bound(own, alias != null ? alias : t.name().get(t.name().size() - 1), t.columnAliases(),
                            t.pos());
                }
                yield bound(cte, alias != null ? alias : t.name().get(0), t.columnAliases(), t.pos());
            }
            case SqlAst.TableFunction f when !isSourceFunction(f) && dialectSource(f) != null -> bound(
                    java.util.Objects.requireNonNull(dialectSource(f)), f.alias() != null ? f.alias()
                            : dialect.functionName(f), f.columnAliases(), f.pos());
            case SqlAst.TableFunction f when !isSourceFunction(f) -> functionSource(f, new Input(List.of(), List.of(),
                    "FROM"));
            case SqlAst.TableFunction f -> {
                ValueSpecification relation = tableFunction(f);
                List<OutCol> columns = new ArrayList<>();
                sources.columns(relation, (name, type, nullable, storeType) -> columns.add(new OutCol(name,
                        dialect.kindOf(type, storeType), nullable, dialect.detailOf(type, storeType))));
                SqlAst.Ident alias = f.alias();
                yield bound(read(new Rel(relation, columns), f.pos()),
                        alias != null ? alias : dialect.functionName(f), f.columnAliases(), f.pos());
            }
            case SqlAst.SubqueryFrom q -> {
                // not LATERAL: the subquery sees the WITH queries, never this query's other FROM items
                Rel rel = inlineView(new SqlToPure(sources, dialect, env.with(env.ctes())).statement(q.query()),
                        q.query());
                yield bound(rel, q.alias(), q.columnAliases(), q.pos());
            }
            case SqlAst.Join j when lateral(j.right()) && j.using().isEmpty() && !j.natural() ->
                    lateralJoin(from(j.left()), j.right(), j.type(), j.on(), j.pos());
            case SqlAst.Join j -> join(from(j.left()), from(j.right()), dialect.joinType(j), j.on(), j.using(),
                    j.natural(), j.pos());
        };
    }

    /** A FROM item the dialect reads as a relation of its own ({@link InputDialect#source}), or null. */
    private @com.legend.base.Nullable Rel dialectSource(SqlAst.FromItem item) {
        return relation(dialect.source(item));
    }

    private static @com.legend.base.Nullable Rel relation(@com.legend.base.Nullable Relation own) {
        if (own == null) {
            return null;
        }
        List<OutCol> columns = new ArrayList<>();
        own.columns().forEach(c -> columns.add(new OutCol(c.name(), c.kind(), c.nullable(), c.detail())));
        return new Rel(own.vs(), columns);
    }

    /** A source's columns as the dialect reads them ({@link InputDialect#read}): the relation itself when every
     *  column reads as stored (Postgres's always do), else each column read otherwise computed in its place. */
    private Rel read(Rel source, SqlAst.Pos pos) {
        Set<String> present = taken();
        source.columns().forEach(c -> present.add(c.name()));
        List<ColSpec> computed = new ArrayList<>();
        List<ColSpec> selection = new ArrayList<>();
        Map<String, String> renames = new LinkedHashMap<>();
        List<OutCol> columns = new ArrayList<>();
        for (OutCol c : source.columns()) {
            Tx stored = columnValue(row, Col.of(c.name(), c.kind(), c.nullable(), c.detail()));
            Tx read = dialect.read(c.name(), stored, pos);
            if (read == null) {
                selection.add(colSpec(c.name()));
                columns.add(c);
                continue;
            }
            String computedName = fresh(present);
            computed.add(new ColSpec(computedName, lambda(row, valueOf(read)), null, null));
            selection.add(colSpec(computedName));
            renames.put(computedName, c.name());
            columns.add(new OutCol(c.name(), read.kind(), read.nullable(), read.detail()));
        }
        if (computed.isEmpty()) {
            return source;
        }
        ValueSpecification vs = call("select", call("extend", source.vs(), new ColSpecArray(computed)),
                new ColSpecArray(selection));
        for (Map.Entry<String, String> r : renames.entrySet()) {
            vs = call("rename", vs, colSpec(r.getKey()), colSpec(r.getValue()));
        }
        return new Rel(vs, columns);
    }

    /** The WITH query a one-part table name names, or null. */
    private @com.legend.base.Nullable Rel cte(SqlAst.Ident name) {
        String found = dialect.resolve(name, env.ctes().keySet(), "table");
        return found == null ? null : env.ctes().get(found);
    }

    /** A relation as a FROM item: its columns renamed by the alias's column list, under its qualifier. */
    private Source bound(Rel rel, SqlAst.@com.legend.base.Nullable Ident qualifier, List<SqlAst.Ident> columnAliases,
            SqlAst.Pos pos) {
        Rel read = rel;
        if (dialect.untypedNullIsText() && rel.columns().stream().anyMatch(c -> c.kind() == Kind.NULL)) {
            // the reading query's NULL text, where the dialect types the column so (2026-10-17, the review of the
            // corpus program's step 1: an untyped NULL read had answered x + 1)
            List<@com.legend.base.Nullable Tx> texts = new ArrayList<>();
            for (OutCol c : rel.columns()) {
                texts.add(c.kind() != Kind.NULL ? null : Tx.value(nullOf(Kind.STRING).vs(), Kind.STRING, true));
            }
            read = converted(rel, texts);
        }
        Rel renamed = columnAliases.isEmpty() ? read : aliased(read, columnAliases, qualifier, pos);
        List<BCol> cols = new ArrayList<>();
        List<Col> star = new ArrayList<>();
        for (OutCol c : renamed.columns()) {
            Col col = Col.of(c.name(), c.kind(), c.nullable(), c.detail());
            cols.add(new BCol(col, true));
            star.add(col);
        }
        return new Source(renamed.vs(), List.of(new Binding(qualifier, cols)), star);
    }

    /** {@code alias(a, b)}: the first columns renamed, in order (Postgres lets fewer names than columns be given). */
    private Rel aliased(Rel rel, List<SqlAst.Ident> names, SqlAst.@com.legend.base.Nullable Ident qualifier,
            SqlAst.Pos pos) {
        if (names.size() > rel.columns().size()) {
            throw SqlTranslationException.invalid("table \"" + (qualifier == null ? "" : qualifier.text()) + "\" has "
                    + rel.columns().size() + " columns available but " + names.size() + " columns specified", pos);
        }
        List<String> to = new ArrayList<>();
        for (int i = 0; i < rel.columns().size(); i++) {
            to.add(i < names.size() ? names.get(i).quoted() ? names.get(i).text() : dialect.fold(names.get(i).text())
                    : rel.columns().get(i).name());
        }
        return renamed(rel, to, pos);
    }

    /** {@code rel} with its columns named {@code to}, in order, through fresh names where two would collide. */
    private Rel renamed(Rel rel, List<String> to, SqlAst.Pos pos) {
        Set<String> seen = new HashSet<>();
        for (String n : to) {
            if (!seen.add(n)) {
                throw SqlTranslationException.unsupported("the column name \"" + n + "\" appears twice; Pure relations"
                        + " need distinct column names", pos);
            }
        }
        Set<String> taken = taken();
        taken.addAll(to);
        rel.columns().forEach(c -> taken.add(c.name()));
        List<Integer> changed = new ArrayList<>();
        boolean swaps = false;
        List<OutCol> cols = new ArrayList<>();
        for (int i = 0; i < to.size(); i++) {
            OutCol c = rel.columns().get(i);
            String target = to.get(i);
            if (!c.name().equals(target)) {
                changed.add(i);
                swaps |= rel.columns().stream().anyMatch(o -> o.name().equals(target));
            }
            cols.add(new OutCol(to.get(i), c.kind(), c.nullable(), c.detail()));
        }
        // a new name that another column holds now: every change goes through a fresh name first
        ValueSpecification vs = rel.vs();
        List<String> via = new ArrayList<>();
        for (int i : changed) {
            String from = rel.columns().get(i).name();
            if (swaps) {
                String t = fresh(taken);
                vs = call("rename", vs, colSpec(from), colSpec(t));
                from = t;
            }
            via.add(from);
        }
        for (int k = 0; k < changed.size(); k++) {
            vs = call("rename", vs, colSpec(via.get(k)), colSpec(to.get(changed.get(k))));
        }
        return new Rel(vs, cols);
    }

    /** {@code tb('pkg::Database.schema.table')}: the table as {@code #>{pkg::Database.schema.table}#}. */
    private static ValueSpecification tableFunction(SqlAst.TableFunction f) {
        if (f.name().size() != 1 || !TABLE_FUNCTION.equals(functionName(f.name().get(0)))) {
            throw SqlTranslationException.unsupported("function \"" + f.name().get(f.name().size() - 1).text()
                    + "\" in FROM is not supported yet; the source function is " + TABLE_FUNCTION + "(...)", f.pos());
        }
        if (f.args().size() != 1 || f.args().get(0).name() != null
                || !(f.args().get(0).value() instanceof SqlAst.StringLit path)) {
            throw SqlTranslationException.invalid(TABLE_FUNCTION + "(...) takes one string: " + TABLE_FUNCTION
                    + "('pkg::Database.schema.table')", f.pos());
        }
        // the database's path holds no '.', so the first one ends it
        String p = path.value();
        int dot = p.indexOf('.');
        if (dot <= 0 || dot == p.length() - 1) {
            throw SqlTranslationException.invalid("a table is named 'pkg::Database.schema.table' or"
                    + " 'pkg::Database.table', not '" + p + "'", path.pos());
        }
        return AppliedFunction.tableReference(p.substring(0, dot), p.substring(dot + 1), null);
    }

    /** {@code right} with its columns renamed apart from {@code left}'s (a Pure relation's column names are
     *  distinct): a taken name becomes the qualifier's {@code t_name}, or a fresh one. */
    private Source apart(Source left, Source right) {
        Set<String> taken = taken();
        left.bindings().forEach(b -> b.columns().forEach(c -> taken.add(c.col().name())));
        right.bindings().forEach(b -> b.columns().forEach(c -> taken.add(c.col().name())));
        // apart without regard to case (2026-10-16, the landing's review): a database that reads names so binds a
        // subselect's u.ID beside t.id to the first
        Set<String> leftNames = taken();
        left.bindings().forEach(b -> b.columns().forEach(c -> leftNames.add(c.col().name())));
        Map<String, String> renames = new LinkedHashMap<>();
        ValueSpecification rightRel = right.relation();
        for (Binding b : right.bindings()) {
            for (BCol c : b.columns()) {
                String name = c.col().name();
                if (leftNames.contains(name)) {
                    SqlAst.Ident q = b.qualifier();
                    String want = (q == null ? "t" : dialect.fold(q.text())) + "_" + name;
                    String to = taken.contains(want) ? fresh(taken) : want;
                    taken.add(to);
                    renames.put(name, to);
                    rightRel = call("rename", rightRel, colSpec(name), colSpec(to));
                }
            }
        }
        List<Binding> rights = new ArrayList<>();
        for (Binding b : right.bindings()) {
            List<BCol> cols = new ArrayList<>();
            for (BCol c : b.columns()) {
                Col col = c.col();
                String to = renames.get(col.name());
                cols.add(new BCol(to == null ? col : col.named(to), c.bare()));
            }
            rights.add(new Binding(b.qualifier(), cols, b.scalar()));
        }
        List<Col> rightStar = new ArrayList<>();
        for (Col c : right.star()) {
            String to = renames.get(c.name());
            rightStar.add(to == null ? c : c.named(to));
        }
        return new Source(rightRel, rights, rightStar);
    }

    /**
     * {@code left, item} where the item reads the left's rows: a function in FROM (always lateral in Postgres) or
     * {@code LATERAL (subquery)}, by comma, {@code CROSS JOIN} or {@code [INNER] JOIN ... ON}. Pure's {@code lateral}:
     * the item's relation for each left row, that row's columns read through the lambda's parameter, the rows paired.
     */
    private Source lateralJoin(Source left, SqlAst.FromItem item, SqlAst.JoinType type,
            SqlAst.@com.legend.base.Nullable Expr on, SqlAst.Pos pos) {
        if (type != SqlAst.JoinType.CROSS && type != SqlAst.JoinType.INNER) {
            throw SqlTranslationException.unsupported(type + " JOIN LATERAL is not supported yet", pos);
        }
        if (item instanceof SqlAst.SubqueryFrom q && q.query() instanceof SqlAst.Select one && one.from().isEmpty()
                && one.where() == null && one.groupBy().isEmpty() && one.having() == null && one.limit() == null
                && one.offset() == null && !one.distinct() && !hasWindow(one)) {
            return computedColumns(left, q, one, on);
        }
        Input outer = new Input(left.bindings(), left.star(), "a LATERAL item");
        Source right = switch (item) {
            case SqlAst.TableFunction f -> functionSource(f, outer);
            case SqlAst.SubqueryFrom q -> bound(new SqlToPure(sources, dialect, new Env(env.ctes(), outer, row,
                    env.level() + 1)).statement(q.query()), q.alias(), q.columnAliases(), q.pos());
            default -> throw new IllegalStateException("not a lateral item: " + item);
        };
        Source apart = apart(left, right);
        List<Binding> bindings = new ArrayList<>(left.bindings());
        bindings.addAll(apart.bindings());
        List<Col> star = new ArrayList<>(left.star());
        star.addAll(apart.star());
        ValueSpecification relation = call("lateral", left.relation(), lambda(row, apart.relation()));
        if (on != null) {
            relation = call("filter", relation, lambda(row, condition(on, new Input(bindings, star, "JOIN/ON"),
                    "JOIN/ON")));
        }
        return new Source(relation, bindings, star);
    }

    /**
     * IQ island, 2026-10-16 (checklist 2.16c: T-SQL's {@code CROSS APPLY (SELECT t.id * 2 AS d) x}): {@code left,
     * LATERAL (SELECT e1 AS a, ...)} of a query of one row -- no FROM, WHERE, grouping, DISTINCT, window or row limit
     * -- is each left row with its columns computed: the left relation extended by them, under the item's name (a
     * lateral of a one-row relation over an enclosing row is not lowered yet). Each column is named, by its alias or
     * the item's column list.
     */
    private Source computedColumns(Source left, SqlAst.SubqueryFrom q, SqlAst.Select s,
            SqlAst.@com.legend.base.Nullable Expr on) {
        Input outer = new Input(left.bindings(), left.star(), "a LATERAL item");
        Set<String> taken = taken();
        left.star().forEach(c -> taken.add(c.name()));
        left.bindings().forEach(b -> b.columns().forEach(c -> taken.add(c.col().name())));
        List<ColSpec> computed = new ArrayList<>();
        List<BCol> cols = new ArrayList<>();
        List<Col> star = new ArrayList<>(left.star());
        for (int i = 0; i < s.items().size(); i++) {
            if (!(s.items().get(i) instanceof SqlAst.SelectExpr x)) {
                throw SqlTranslationException.unsupported("* in a LATERAL query without FROM is not supported yet",
                        s.pos());
            }
            SqlAst.Ident label = i < q.columnAliases().size() ? q.columnAliases().get(i) : x.alias();
            if (label == null) {
                throw SqlTranslationException.unsupported("an unnamed column of a LATERAL query without FROM is not"
                        + " supported yet", x.pos());
            }
            Tx t = tx(x.expr(), outer, row);
            if (t.kind() == Kind.NULL) {
                throw SqlTranslationException.unsupported("a NULL column of a LATERAL query without FROM is not"
                        + " supported yet", x.pos());
            }
            String name = fresh(taken);
            computed.add(new ColSpec(name, lambda(row, valueOf(t)), null, null));
            Col col = new Col(name, t.kind(), t.nullable(), null, label.quoted() ? label.text()
                    : dialect.fold(label.text()), t.detail());
            cols.add(new BCol(col, true));
            star.add(col);
        }
        List<Binding> bindings = new ArrayList<>(left.bindings());
        bindings.add(new Binding(q.alias(), cols));
        ValueSpecification relation = call("extend", left.relation(), new ColSpecArray(computed));
        if (on != null) {
            relation = call("filter", relation, lambda(row, condition(on, new Input(bindings, star, "JOIN/ON"),
                    "JOIN/ON")));
        }
        return new Source(relation, bindings, star);
    }

    /**
     * IQ island, 2026-10-16: {@code rel}, a derived table's or a common table expression's relation, as an inline view
     * ({@link Sources#inlineView}) where its own query computes window functions -- a SELECT's select list, or a set
     * operation's branch's: SQL computes them over all of the view's rows, before the enclosing query's WHERE. Pure
     * composes an extend and a later filter otherwise (the filter folds under the window: legend-engine's PCT
     * testExtendFilterOutNull).
     */
    private Rel inlineView(Rel rel, SqlAst.Statement query) {
        return computesWindows(query) ? new Rel(sources.inlineView(rel.vs()), rel.columns()) : rel;
    }

    private static boolean computesWindows(SqlAst.Statement query) {
        return switch (query) {
            case SqlAst.Select s -> hasWindow(s);
            case SqlAst.SetOperation o -> computesWindows(o.left()) || computesWindows(o.right());
            case SqlAst.With w -> computesWindows(w.body());
            case SqlAst.Write w -> false;
        };
    }

    /** True iff {@code item} may read the FROM items before it: a function (not the source function) or a LATERAL
     *  subquery. */
    private boolean lateral(SqlAst.FromItem item) {
        return item instanceof SqlAst.TableFunction f && !isSourceFunction(f) && dialect.source(f) == null
                || item instanceof SqlAst.SubqueryFrom q && q.lateral();
    }

    /** Whether {@code f} is the island's source function, {@code tb('pkg::Database.table')}. */
    static boolean isSourceFunction(SqlAst.TableFunction f) {
        return f.name().size() == 1 && TABLE_FUNCTION.equals(functionName(f.name().get(0)));
    }

    /** A set-returning function in FROM, under its alias: its one column named by the column alias, else the table
     *  alias, else the function (Postgres's naming). */
    private Source functionSource(SqlAst.TableFunction f, Scope scope) {
        String fn = functionName(f.name().get(f.name().size() - 1));
        SqlAst.Ident alias = f.alias();
        // a function with an OUT parameter names its column by it (json_array_elements's value); one returning a
        // plain type by the table alias, else itself
        SetReturning kind = SET_RETURNING.get(fn);
        String column = kind != null && kind.outValue ? "value"
                : alias != null ? alias.quoted() ? alias.text() : dialect.fold(alias.text()) : fn;
        Rel rel = setReturning(f, fn, column, scope);
        Source source = bound(rel, alias != null ? alias : f.name().get(f.name().size() - 1), f.columnAliases(),
                f.pos());
        Binding b = source.bindings().get(0);
        return new Source(source.relation(), List.of(new Binding(b.qualifier(), b.columns(), true)), source.star());
    }

    /** A set-returning function ({@link #SET_RETURNING}) as Pure's {@code flatten} of a collection: one row per
     *  element. */
    private Rel setReturning(SqlAst.TableFunction f, String fn, String column, Scope scope) {
        if (f.name().size() != 1) {
            throw SqlTranslationException.unsupported("function \"" + f.name().get(f.name().size() - 1).text()
                    + "\" in FROM is not supported yet", f.pos());
        }
        List<SqlAst.Expr> argExprs = new ArrayList<>();
        for (SqlAst.Arg a : f.args()) {
            if (a.name() != null) {
                throw SqlTranslationException.unsupported("named arguments are not supported yet", f.pos());
            }
            argExprs.add(a.value());
        }
        SetReturning kind = SET_RETURNING.get(fn);
        if (kind == null) {
            throw SqlTranslationException.unsupported("function \"" + fn + "\" in FROM is not supported yet", f.pos());
        }
        return switch (kind) {
            // IQ island, 2026-10-16: a flattened column is Pure's [0..1] (the typer's), never NULL though it is
            case SERIES -> new Rel(call("flatten", series(argExprs, scope, f.pos()), colSpec(column)),
                    List.of(new OutCol(column, Kind.INTEGER, true)));
            case UNNEST -> {
                if (argExprs.size() != 1) {
                    throw SqlTranslationException.unsupported("unnest of " + argExprs.size() + " arrays is not"
                            + " supported yet", f.pos());
                }
                Tx elements = array(argExprs.get(0), scope);
                yield new Rel(call("flatten", elements.vs(), colSpec(column)),
                        List.of(new OutCol(column, elements.kind(), true)));
            }
            case ELEMENTS -> {
                // each element as jsonb (jsonb's null an element too); none for SQL's NULL; a value that is not an
                // array is Postgres's error, and Pure's toMany's
                ValueSpecification elements = jsonElements(f, fn, argExprs, scope);
                yield new Rel(call("flatten", elements, colSpec(column)),
                        List.of(new OutCol(column, Kind.JSON, true)));
            }
            case ELEMENTS_TEXT -> {
                // each element's text (->> of it: NULL for jsonb's null)
                String held = SYNTHETIC + "element";
                ValueSpecification rel = call("select", call("extend", call("flatten",
                        jsonElements(f, fn, argExprs, scope), colSpec(held)), new ColSpecArray(List.of(
                                new ColSpec(column, lambda(SYNTHETIC + "e", call("to", property(SYNTHETIC + "e", held),
                                        typeArg("String"))), null, null)))), new ColSpecArray(List.of(colSpec(column))));
                yield new Rel(rel, List.of(new OutCol(column, Kind.STRING, true)));
            }
            case OBJECT_KEYS -> {
                if (argExprs.size() != 1) {
                    throw SqlTranslationException.invalid("function " + fn + " takes one argument", f.pos());
                }
                constantNavigation(argExprs.get(0), f.pos());
                Tx x = jsonOperand(tx(argExprs.get(0), scope, row), fn, f.pos());
                // none for SQL's NULL; a value that is not an object is Postgres's error, and the Map read's
                // (to reads a Variant[0..1]: the value itself is never toOne'd, whose Postgres guard is text's)
                ValueSpecification keys = call("keys", call("toOne", call("to", scalarNull(x), new TypeAnnotation.Named(
                        new TypeExpression.Generic(MAP, List.of(new TypeExpression.NameRef("String"),
                                new TypeExpression.NameRef(VARIANT)))))));
                if (x.nullable()) {
                    keys = call("if", call("isEmpty", x.vs()), thunk(empty()), thunk(keys));
                }
                yield new Rel(call("flatten", keys, colSpec(column)),
                        List.of(new OutCol(column, Kind.STRING, false)));
            }
        };
    }

    /** What a set-returning function this step reads is: {@code generate_series}'s range, {@code unnest}'s array, a
     *  jsonb array's elements (as jsonb, or as text), a jsonb object's keys. {@code outValue}: its one column is its
     *  OUT parameter's, {@code value}, rather than named by its alias or itself. */
    private enum SetReturning {
        SERIES(false), UNNEST(false), ELEMENTS(true), ELEMENTS_TEXT(true), OBJECT_KEYS(false);

        final boolean outValue;

        SetReturning(boolean outValue) {
            this.outValue = outValue;
        }
    }

    /** The set-returning functions this step reads, in FROM or as a select-list item, by their Postgres names. */
    private static final Map<String, SetReturning> SET_RETURNING = Map.of(
            "generate_series", SetReturning.SERIES, "unnest", SetReturning.UNNEST,
            "json_array_elements", SetReturning.ELEMENTS, "jsonb_array_elements", SetReturning.ELEMENTS,
            "json_array_elements_text", SetReturning.ELEMENTS_TEXT, "jsonb_array_elements_text",
            SetReturning.ELEMENTS_TEXT, "json_object_keys", SetReturning.OBJECT_KEYS, "jsonb_object_keys",
            SetReturning.OBJECT_KEYS);

    private static final String MAP = "meta::pure::functions::collection::Map";

    /** A jsonb array's elements, Pure's toMany(@Variant). */
    private ValueSpecification jsonElements(SqlAst.TableFunction f, String fn, List<SqlAst.Expr> args, Scope scope) {
        if (args.size() != 1) {
            throw SqlTranslationException.invalid("function " + fn + " takes one argument", f.pos());
        }
        constantNavigation(args.get(0), f.pos());
        Tx x = jsonOperand(tx(args.get(0), scope, row), fn, f.pos());
        return call("toMany", scalarNull(x), typeArg(VARIANT));
    }

    /** A jsonb value with jsonb's null read as the scalar it is to Postgres: an array's elements and an object's keys
     *  are Postgres's error for any scalar ("cannot extract elements from a scalar"), where Pure reads a Variant's
     *  null as no value. The null becomes another scalar, so that Pure's toMany and to(@Map) raise their own errors
     *  for a value that is not an array or an object; SQL's NULL stays no value. */
    private static ValueSpecification scalarNull(Tx x) {
        ValueSpecification isJsonNull = call("and", call("isNotEmpty", x.vs()), call("isEmpty", call("to", x.vs(),
                typeArg("String"))));
        return call("if", isJsonNull, thunk(call(FROM_JSON, new CString("0"))), thunk(x.vs()));
    }

    /** {@code generate_series(start, stop [, step])} of integers, Pure's {@code range} (whose stop is exclusive): no
     *  rows when an argument is NULL. The step is a constant: zero is Postgres's error. */
    private ValueSpecification series(List<SqlAst.Expr> args, Scope scope, SqlAst.Pos pos) {
        if (args.size() != 2 && args.size() != 3) {
            throw SqlTranslationException.unsupported("generate_series of " + args.size() + " arguments is not"
                    + " supported yet", pos);
        }
        long step = 1;
        if (args.size() == 3) {
            SqlAst.Expr s = args.get(2);
            java.math.BigInteger k = s instanceof SqlAst.IntegerLit n ? n.value()
                    : s instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.MINUS
                            && u.operand() instanceof SqlAst.IntegerLit n ? n.value().negate() : null;
            if (k == null) {
                throw SqlTranslationException.unsupported("a generate_series step other than an integer constant is"
                        + " not supported yet", s.pos());
            }
            if (k.signum() == 0) {
                throw SqlTranslationException.invalid("step size cannot equal zero", s.pos());
            }
            if (k.bitLength() >= 64) {
                // Postgres reads it as numeric
                throw SqlTranslationException.unsupported("generate_series of values other than integers is not"
                        + " supported yet", s.pos());
            }
            step = k.longValueExact();
        }
        List<Tx> bounds = new ArrayList<>();
        for (SqlAst.Expr e : args.subList(0, 2)) {
            Tx t = tx(e, scope, row);
            if (t.kind() != Kind.INTEGER) {
                throw SqlTranslationException.unsupported("generate_series of values other than integers is not"
                        + " supported yet", e.pos());
            }
            bounds.add(t);
        }
        long by = step;
        Tx range = strict(Kind.INTEGER, bounds, v -> call("range", v.get(0),
                AppliedFunction.infixRun("plus", List.of(v.get(1), new CInteger(by > 0 ? 1L : -1L))),
                new CInteger(by)));
        return range.vs();
    }

    /** An array, as Pure's collection of its elements: {@code ARRAY[...]} of values that are never NULL (a Pure
     *  collection holds no NULL), or {@code string_to_array(text, 'delimiter')}. */
    private Tx array(SqlAst.Expr e, Scope scope) {
        if (e instanceof SqlAst.ArrayConstructor a) {
            if (a.elements().isEmpty()) {
                throw SqlTranslationException.unsupported("an empty ARRAY[] is not supported yet", a.pos());
            }
            List<ValueSpecification> values = new ArrayList<>();
            Kind kind = null;
            for (SqlAst.Expr x : a.elements()) {
                Tx t = tx(x, scope, row);
                if (t.nullable() || t.kind() == Kind.NULL) {
                    throw SqlTranslationException.unsupported("an array element that may be NULL is not supported yet"
                            + " (a Pure collection holds no NULL)", x.pos());
                }
                if (t.kind() == Kind.OTHER || t.kind() == Kind.BOOLEAN && t.whenFalse() != null) {
                    throw SqlTranslationException.unsupported("an array of this type is not supported yet", x.pos());
                }
                if (kind != null && kind != t.kind()) {
                    throw SqlTranslationException.unsupported("an array of " + kind.name().toLowerCase(Locale.ROOT)
                            + " and " + t.kind().name().toLowerCase(Locale.ROOT) + " values is not supported yet",
                            x.pos());
                }
                kind = t.kind();
                values.add(t.vs());
            }
            return Tx.value(new PureCollection(values), java.util.Objects.requireNonNull(kind), false);
        }
        if (e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                && functionName(f.name().get(0)).equals("string_to_array")) {
            if (f.args().size() != 2) {
                throw SqlTranslationException.unsupported("string_to_array with a null string is not supported yet",
                        f.pos());
            }
            if (!(f.args().get(1) instanceof SqlAst.StringLit d) || d.value().isEmpty()) {
                throw SqlTranslationException.unsupported("a string_to_array delimiter other than a non-empty constant"
                        + " is not supported yet", f.args().get(1).pos());
            }
            Tx text = tx(f.args().get(0), scope, row);
            if (text.kind() != Kind.STRING) {
                throw SqlTranslationException.invalid("function string_to_array needs text", f.args().get(0).pos());
            }
            // an empty text is an empty array in Postgres
            return strict(Kind.STRING, List.of(text), v -> call("if", call("equal", v.get(0), new CString("")),
                    thunk(empty()), thunk(call("split", v.get(0), new CString(d.value())))));
        }
        throw SqlTranslationException.unsupported("unnest of an array other than ARRAY[...] or string_to_array(...) is"
                + " not supported yet", e.pos());
    }

    /** Two FROM items joined: the right side's columns renamed where the left has the name (a Pure relation's
     *  column names are distinct), then Pure's join, its condition over the two sides' rows. An outer join's
     *  other side may be NULL; a USING or NATURAL join's columns are merged, shown once, first. */
    private Source join(Source left, Source right, SqlAst.JoinType type, SqlAst.@com.legend.base.Nullable Expr on,
            List<SqlAst.Ident> using, boolean natural, SqlAst.Pos pos) {
        for (Binding l : left.bindings()) {
            for (Binding r : right.bindings()) {
                SqlAst.Ident lq = l.qualifier();
                SqlAst.Ident rq = r.qualifier();
                if (lq != null && rq != null && (lq.quoted() ? lq.text() : dialect.fold(lq.text())).equals(
                        rq.quoted() ? rq.text() : dialect.fold(rq.text()))) {
                    throw SqlTranslationException.invalid(dialect.message(Problem.DUPLICATE_TABLE, rq.text()),
                            rq.pos());
                }
            }
        }
        Source apart = apart(left, right);
        Set<String> taken = taken();
        left.bindings().forEach(b -> b.columns().forEach(c -> taken.add(c.col().name())));
        apart.bindings().forEach(b -> b.columns().forEach(c -> taken.add(c.col().name())));
        ValueSpecification rightRel = apart.relation();
        List<Binding> rights = apart.bindings();
        List<Col> rightStar = apart.star();
        // USING and NATURAL: the columns the two sides share by name; a dialect that does not merge a NATURAL join's
        // columns joins on their equality, as an ON it ANDs with any written
        if (natural && !dialect.mergesNaturalJoin()) {
            on = naturalCondition(left, apart, on, pos);
        }
        List<SqlAst.Ident> shared = new ArrayList<>(using);
        if (natural && dialect.mergesNaturalJoin()) {
            for (Col l : left.star()) {
                for (Col r : rightStar) {
                    if (l.label().equals(r.label())) {
                        shared.add(new SqlAst.Ident(l.label(), true, pos));
                    }
                }
            }
        }
        // the condition, over the two sides' rows
        List<Binding> conditionScope = new ArrayList<>();
        left.bindings().forEach(b -> conditionScope.add(b.via(leftRow)));
        rights.forEach(b -> conditionScope.add(b.via(rightRow)));
        Input sides = new Input(conditionScope, List.of(), "JOIN conditions");
        List<Col[]> merged = new ArrayList<>();
        ValueSpecification condition;
        if (!shared.isEmpty()) {
            List<ValueSpecification> tests = new ArrayList<>();
            for (SqlAst.Ident u : shared) {
                Col l = sharedColumn(left.bindings(), u, "left");
                Col r = sharedColumn(rights, u, "right");
                List<Tx> pair = List.of(columnValue(row, l.via(leftRow)), columnValue(row, r.via(rightRow)));
                notJsonKey(pair.get(0).kind(), "JOIN/USING", u.pos());
                notJsonKey(pair.get(1).kind(), "JOIN/USING", u.pos());
                if (pair.get(0).kind() != pair.get(1).kind() && !(pair.get(0).kind().numeric()
                        && pair.get(1).kind().numeric())) {
                    throw SqlTranslationException.invalid("JOIN/USING types " + l.kind().name().toLowerCase(Locale.ROOT)
                            + " and " + r.kind().name().toLowerCase(Locale.ROOT) + " cannot be matched", u.pos());
                }
                tests.add(strictTest(pair, v -> call("equal", v.get(0), v.get(1))).vs());
                merged.add(new Col[] {l, r});
            }
            condition = tests.get(0);
            for (int i = 1; i < tests.size(); i++) {
                condition = call("and", condition, tests.get(i));
            }
        } else if (on != null) {
            condition = condition(on, sides, "JOIN/ON");
        } else {
            condition = new CBoolean(true);
        }
        String kind = switch (type) {
            case INNER, CROSS -> "INNER";
            case LEFT -> "LEFT";
            case RIGHT -> "RIGHT";
            case FULL -> "FULL";
        };
        ValueSpecification relation = call("join", left.relation(), rightRel, new EnumValue(JOIN_KIND, kind),
                lambda2(leftRow, rightRow, condition));
        // an outer join's other side may hold no row
        boolean leftNullable = type == SqlAst.JoinType.RIGHT || type == SqlAst.JoinType.FULL;
        boolean rightNullable = type == SqlAst.JoinType.LEFT || type == SqlAst.JoinType.FULL;
        List<Binding> bindings = new ArrayList<>();
        Set<String> hidden = new HashSet<>();
        for (Col[] m : merged) {
            hidden.add(m[0].name());
            hidden.add(m[1].name());
        }
        for (Binding b : left.bindings()) {
            bindings.add(hide(leftNullable ? b.asNullable() : b, hidden));
        }
        for (Binding b : rights) {
            bindings.add(hide(rightNullable ? b.asNullable() : b, hidden));
        }
        List<Col> star = new ArrayList<>();
        if (!merged.isEmpty()) {
            List<BCol> mergedCols = new ArrayList<>();
            List<ColSpec> coalesced = new ArrayList<>();
            for (Col[] m : merged) {
                Col l = m[0];
                Col r = m[1];
                Col col;
                if (type == SqlAst.JoinType.FULL) {
                    String name = fresh(taken);
                    coalesced.add(new ColSpec(name, lambda(row, call("coalesce", property(row, l.name()),
                            property(row, r.name()))), null, null));
                    col = new Col(name, l.kind(), l.nullable() && r.nullable(), null, l.label());
                } else if (type == SqlAst.JoinType.RIGHT) {
                    col = new Col(r.name(), r.kind(), r.nullable(), null, l.label(), r.detail());
                } else {
                    col = new Col(l.name(), l.kind(), l.nullable(), null, l.label(), l.detail());
                }
                mergedCols.add(new BCol(col, true));
                star.add(col);
            }
            if (!coalesced.isEmpty()) {
                relation = call("extend", relation, new ColSpecArray(coalesced));
            }
            bindings.add(0, new Binding(null, mergedCols));
        }
        for (Col c : left.star()) {
            if (!hidden.contains(c.name())) {
                star.add(leftNullable ? c.asNullable() : c);
            }
        }
        for (Col c : rightStar) {
            if (!hidden.contains(c.name())) {
                star.add(rightNullable ? c.asNullable() : c);
            }
        }
        return new Source(relation, bindings, star);
    }

    /** A NATURAL join's condition where its columns are not merged ({@link InputDialect#mergesNaturalJoin}): each
     *  column the two sides share by name (as the dialect matches names) equal on both, qualified by the one FROM item
     *  of each side that has it, ANDed with {@code on} when one is written. No shared column is an error. */
    private SqlAst.Expr naturalCondition(Source left, Source right, SqlAst.@com.legend.base.Nullable Expr on,
            SqlAst.Pos pos) {
        SqlAst.Expr condition = on;
        boolean any = false;
        for (Col l : left.star()) {
            for (Col r : right.star()) {
                if (dialect.resolve(new SqlAst.Ident(l.label(), true, pos), List.of(r.label()), "column") == null) {
                    continue;
                }
                any = true;
                SqlAst.Ident name = new SqlAst.Ident(l.label(), true, pos);
                SqlAst.Expr eq = new SqlAst.Binary(SqlAst.BinaryOp.EQ,
                        new SqlAst.ColumnRef(List.of(naturalSide(left.bindings(), name), name), pos),
                        new SqlAst.ColumnRef(List.of(naturalSide(right.bindings(), name),
                                new SqlAst.Ident(r.label(), true, pos)), pos), pos);
                condition = condition == null ? eq : new SqlAst.Binary(SqlAst.BinaryOp.AND, condition, eq, pos);
            }
        }
        if (!any) {
            throw SqlTranslationException.invalid("a NATURAL join's two sides have no column name in common", pos);
        }
        return java.util.Objects.requireNonNull(condition);
    }

    /** The qualifier of the one FROM item of a NATURAL join's side that has the column {@code name}. */
    private SqlAst.Ident naturalSide(List<Binding> side, SqlAst.Ident name) {
        SqlAst.Ident found = null;
        for (Binding b : side) {
            if (dialect.resolve(name, b.names(true), "column") != null) {
                if (found != null || b.qualifier() == null) {
                    throw SqlTranslationException.unsupported("a NATURAL join whose side has the column '" + name.text()
                            + "' in more than one table is not supported", name.pos());
                }
                found = b.qualifier();
            }
        }
        return java.util.Objects.requireNonNull(found, "a NATURAL join's shared column is on both sides");
    }

    /** A USING column on one side of a join: the one column there of that name. */
    private Col sharedColumn(List<Binding> side, SqlAst.Ident name, String which) {
        Col found = null;
        for (Binding b : side) {
            String label = dialect.resolve(name, b.names(true), "column");
            if (label != null) {
                if (found != null) {
                    throw SqlTranslationException.invalid(dialect.message(Problem.DUPLICATE_USING_COLUMN, name.text(),
                            which), name.pos());
                }
                for (BCol c : b.columns()) {
                    if (c.col().label().equals(label) && c.bare()) {
                        found = c.col();
                    }
                }
            }
        }
        if (found == null) {
            throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_USING_COLUMN, name.text(), which),
                    name.pos());
        }
        return found;
    }

    /** A binding whose USING columns are reached only qualified. */
    private static Binding hide(Binding b, Set<String> hidden) {
        List<BCol> cols = new ArrayList<>();
        for (BCol c : b.columns()) {
            cols.add(new BCol(c.col(), c.bare() && !hidden.contains(c.col().name())));
        }
        return new Binding(b.qualifier(), cols, b.scalar());
    }

    // ------------------------------------------------------------------ set operations and WITH

    /** {@code UNION [ALL]}: Pure's concatenate (and distinct), the right side's columns named as the left's, in
     *  order. {@code INTERSECT} and {@code EXCEPT}: the left's rows that some right row equals (or none does), column
     *  by column with two NULLs equal (Pure's {@code ==}), once each. Then the result's sort and slice, by output name
     *  or position only (Postgres's rule). */
    private Rel setOperation(SqlAst.SetOperation s) {
        if (s.all() && s.op() != SqlAst.SetOp.UNION) {
            throw SqlTranslationException.unsupported(s.op() + " ALL is not supported yet", s.pos());
        }
        Rel left = statement(s.left());
        Rel right = statement(s.right());
        String op = s.op().name();
        if (left.columns().size() != right.columns().size()) {
            throw SqlTranslationException.invalid(dialect.message(Problem.SET_WIDTHS, op), s.pos());
        }
        List<String> names = new ArrayList<>();
        List<OutCol> columns = new ArrayList<>();
        List<java.util.function.@com.legend.base.Nullable UnaryOperator<ValueSpecification>> setKeys = new ArrayList<>();
        List<@com.legend.base.Nullable Tx> leftValues = new ArrayList<>();
        List<@com.legend.base.Nullable Tx> rightValues = new ArrayList<>();
        for (int i = 0; i < left.columns().size(); i++) {
            OutCol l = left.columns().get(i);
            OutCol r = right.columns().get(i);
            ColumnType lt = new ColumnType(l.kind(), l.nullable(), l.detail());
            ColumnType rt = new ColumnType(r.kind(), r.nullable(), r.detail());
            ColumnType unified = dialect.setColumn(s, i, lt, rt);
            // IQ island, 2026-10-15: each side's values brought to the column's type where the dialect converts them
            leftValues.add(unified == null ? null : dialect.setValue(s, i, Tx.value(property(row, l.name()), l.kind(),
                    l.nullable(), l.detail()), rt, true));
            rightValues.add(unified == null ? null : dialect.setValue(s, i, Tx.value(property(row, r.name()),
                    r.kind(), r.nullable(), r.detail()), lt, false));
            if (unified == null && l.kind() != r.kind()) {
                if (l.kind() == Kind.NULL || r.kind() == Kind.NULL || l.kind().numeric() && r.kind().numeric()) {
                    throw SqlTranslationException.unsupported(op + " of " + l.kind().name().toLowerCase(Locale.ROOT)
                            + " and " + r.kind().name().toLowerCase(Locale.ROOT) + " columns is not supported yet",
                            s.pos());
                }
                throw SqlTranslationException.invalid(op + " types " + l.kind().name().toLowerCase(Locale.ROOT)
                        + " and " + r.kind().name().toLowerCase(Locale.ROOT) + " cannot be matched", s.pos());
            }
            java.util.function.UnaryOperator<ValueSpecification> setKey = null;
            if (!s.all()) {
                notJsonKey(l.kind(), s.op().name(), s.pos());
                Kind keyKind = unified != null ? unified.kind() : l.kind();
                Tx value = Tx.value(property(row, l.name()), keyKind, l.nullable() || r.nullable(),
                        unified != null ? unified.detail() : l.detail());
                setKey = dialect.setKey(s, i, value);
                if (setKey != null && (s.op() != SqlAst.SetOp.UNION
                        || !dialect.representative(s.op().name(), value, null))) {
                    throw SqlTranslationException.unsupported(s.op().name() + " by a key other than the values"
                            + " themselves is not supported", s.pos());
                }
            }
            setKeys.add(setKey);
            names.add(l.name());
            // INTERSECT's row is in both; EXCEPT's is the left's; a dialect's own type says its own (IQ island,
            // 2026-10-14: the #IQ{...}# island's INTERSECT keeps the left's values, NULL where they may be)
            boolean nullable = unified != null ? unified.nullable() : s.op() == SqlAst.SetOp.UNION
                    ? l.nullable() || r.nullable() : s.op() == SqlAst.SetOp.INTERSECT ? l.nullable() && r.nullable()
                    : l.nullable();
            // a keyed UNION column shows its group's least value, a min: a [0..1] whatever the values
            nullable |= setKey != null;
            // a decimal column merged from two queries takes one scale in the database (see common)
            columns.add(unified != null ? new OutCol(l.name(), unified.kind(), nullable, unified.detail())
                    : new OutCol(l.name(), l.kind() == Kind.DECIMAL ? Kind.NUMBER : l.kind(), nullable));
        }
        left = converted(left, leftValues);
        right = converted(right, rightValues);
        ValueSpecification vs = s.op() == SqlAst.SetOp.UNION ? union(s, left, right, names, setKeys)
                : rowsFound(s.op() == SqlAst.SetOp.INTERSECT, left, right);
        List<ValueSpecification> sortInfos = new ArrayList<>();
        List<ColSpec> keyed = new ArrayList<>();
        Set<String> taken = taken();
        taken.addAll(names);
        for (SqlAst.SortItem item : s.orderBy()) {
            if (dialect.ordersNothing(item.expr())) {
                continue;
            }
            String key = outputKey(item.expr(), names);
            OutCol column = columns.get(names.indexOf(key));
            notJsonKey(column.kind(), "ORDER BY", item.expr().pos());
            String sortedBy = sortedBy(key, Tx.value(property(row, key), column.kind(), column.nullable(),
                    column.detail()), null, item.expr().pos(), keyed, taken);
            boolean nullsFirst = item.nullsFirst() != null ? java.util.Objects.requireNonNull(item.nullsFirst())
                    : dialect.nullsFirst(item.descending());
            sortInfos.add(call(item.descending() ? "descending" : "ascending", colSpec(sortedBy),
                    new EnumValue(NULL_ORDER, nullsFirst ? "FIRST" : "LAST")));
        }
        if (!keyed.isEmpty()) {
            vs = call("extend", vs, new ColSpecArray(keyed));
        }
        if (!sortInfos.isEmpty()) {
            vs = call("sort", vs, new PureCollection(sortInfos));
        }
        if (!keyed.isEmpty()) {
            vs = call("select", vs, new ColSpecArray(names.stream().map(SqlToPure::colSpec).toList()));
        }
        SqlAst.Expr offset = s.offset();
        if (offset != null) {
            vs = call("drop", vs, dialect.rowCount(offset, "OFFSET"));
        }
        SqlAst.Expr limit = s.limit();
        if (limit != null) {
            vs = call("limit", vs, dialect.rowCount(limit, "LIMIT"));
        }
        return new Rel(vs, columns);
    }

    /** IQ island, 2026-10-15: {@code rel} with each column whose {@code values} entry is not null computed as that
     *  value (a set operation's side brought to its columns' types), its other columns as they are, in their order. */
    private Rel converted(Rel rel, List<@com.legend.base.Nullable Tx> values) {
        if (values.stream().allMatch(java.util.Objects::isNull)) {
            return rel;
        }
        Set<String> taken = taken();
        rel.columns().forEach(c -> taken.add(c.name()));
        List<ColSpec> computed = new ArrayList<>();
        List<ColSpec> selection = new ArrayList<>();
        List<String[]> renames = new ArrayList<>();
        List<OutCol> cols = new ArrayList<>();
        for (int i = 0; i < rel.columns().size(); i++) {
            OutCol c = rel.columns().get(i);
            Tx v = values.get(i);
            if (v == null) {
                selection.add(colSpec(c.name()));
                cols.add(c);
                continue;
            }
            String k = fresh(taken);
            computed.add(new ColSpec(k, lambda(row, valueOf(v)), null, null));
            selection.add(colSpec(k));
            renames.add(new String[] {k, c.name()});
            cols.add(new OutCol(c.name(), v.kind(), c.nullable() || v.nullable(), v.detail()));
        }
        ValueSpecification vs = call("select", call("extend", rel.vs(), new ColSpecArray(computed)),
                new ColSpecArray(selection));
        for (String[] r : renames) {
            vs = call("rename", vs, colSpec(r[0]), colSpec(r[1]));
        }
        return new Rel(vs, cols);
    }

    private ValueSpecification union(SqlAst.SetOperation s, Rel left, Rel right, List<String> names,
            List<java.util.function.@com.legend.base.Nullable UnaryOperator<ValueSpecification>> keys) {
        boolean sameNames = true;
        for (int i = 0; i < names.size(); i++) {
            sameNames &= names.get(i).equals(right.columns().get(i).name());
        }
        Rel aligned = sameNames ? right : renamed(right, names, s.pos());
        ValueSpecification vs = call("concatenate", left.vs(), aligned.vs());
        if (s.all()) {
            return vs;
        }
        if (keys.stream().allMatch(java.util.Objects::isNull)) {
            return call("distinct", vs);
        }
        // IQ island, 2026-10-15: a column keyed by the dialect groups by its key and shows its group's least value
        Set<String> taken = taken();
        taken.addAll(names);
        List<ColSpec> keyed = new ArrayList<>();
        List<ColSpec> groupKeys = new ArrayList<>();
        List<ColSpec> members = new ArrayList<>();
        List<ColSpec> selection = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            java.util.function.UnaryOperator<ValueSpecification> key = keys.get(i);
            selection.add(colSpec(name));
            if (key == null) {
                groupKeys.add(colSpec(name));
            } else {
                String k = fresh(taken);
                keyed.add(new ColSpec(k, lambda(row, key.apply(property(row, name))), null, null));
                groupKeys.add(colSpec(k));
                members.add(new ColSpec(name, lambda(row, property(row, name)), lambda(group, call("min",
                        new Variable(group))), null));
            }
        }
        return call("select", call("groupBy", call("extend", vs, new ColSpecArray(keyed)),
                new ColSpecArray(groupKeys), new ColSpecArray(members)), new ColSpecArray(selection));
    }

    /** The left's distinct rows that some row of the right equals ({@code found}), or that none does. */
    private ValueSpecification rowsFound(boolean found, Rel left, Rel right) {
        ValueSpecification match = null;
        for (int i = 0; i < left.columns().size(); i++) {
            ValueSpecification eq = call("equal", property(row, left.columns().get(i).name()),
                    property(sub, right.columns().get(i).name()));
            match = match == null ? eq : call("and", match, eq);
        }
        ValueSpecification in = call("exists", right.vs(), lambda(sub, java.util.Objects.requireNonNull(match)));
        return call("distinct", call("filter", left.vs(), lambda(row, found ? in : call("not", in))));
    }

    /** A set operation's ORDER BY key: an output column's name or position. */
    private String outputKey(SqlAst.Expr e, List<String> names) {
        if (e instanceof SqlAst.IntegerLit n) {
            if (n.value().signum() <= 0 || n.value().compareTo(java.math.BigInteger.valueOf(names.size())) > 0) {
                throw SqlTranslationException.invalid("ORDER BY position " + n.value() + " is not in select list",
                        n.pos());
            }
            return names.get(n.value().intValue() - 1);
        }
        if (e instanceof SqlAst.ColumnRef ref && ref.parts().size() == 1) {
            String found = dialect.resolve(ref.parts().get(0), names, "column");
            if (found != null) {
                return found;
            }
            throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_COLUMN, ref.parts().get(0).text(),
                    ""), ref.pos());
        }
        throw SqlTranslationException.invalid("invalid UNION/INTERSECT/EXCEPT ORDER BY clause: only result column"
                + " names can be used", e.pos());
    }

    /** {@code WITH a AS (...), b AS (...) body}: each query translated once, in order, each seeing those before it;
     *  a reference to one is its relation. */
    private Rel with(SqlAst.With w) {
        if (w.recursive()) {
            throw SqlTranslationException.unsupported("WITH RECURSIVE is not supported yet", w.pos());
        }
        Map<String, Rel> ctes = new LinkedHashMap<>(env.ctes());
        for (SqlAst.Cte cte : w.ctes()) {
            String name = cte.name().quoted() ? cte.name().text() : dialect.fold(cte.name().text());
            Rel rel = inlineView(new SqlToPure(sources, dialect, env.with(ctes)).statement(cte.query()),
                    cte.query());
            if (!cte.columns().isEmpty()) {
                rel = aliased(rel, cte.columns(), cte.name(), cte.pos());
            }
            ctes.put(name, rel);
        }
        return new SqlToPure(sources, dialect, env.with(ctes)).statement(w.body());
    }

    /** A query with no grouping: filter, computed columns, sort, the select list, then the slice. */
    private Rel plain(SqlAst.Select s, Input in, ValueSpecification relation) {
        SqlAst.Expr having = s.having();
        if (having != null) {
            throw SqlTranslationException.unsupported("HAVING without GROUP BY or an aggregate is not supported yet",
                    having.pos());
        }
        Set<String> present = taken();
        present.addAll(in.names());
        Windowed windowed = windows(s, in, relation, present);
        Scope scope = windowed.scope();
        List<Output> outputs = new ArrayList<>();
        Set<String> passed = new HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            switch (item) {
                case SqlAst.AllColumns all -> {
                    SqlAst.Ident q = all.qualifier();
                    List<Col> cols = new ArrayList<>();
                    if (q == null) {
                        cols.addAll(in.star);
                    } else {
                        Binding b = in.binding(q);
                        if (b == null) {
                            throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_TABLE, q.text()),
                                    q.pos());
                        }
                        b.columns().forEach(c -> cols.add(c.col()));
                    }
                    for (Col c : cols) {
                        outputs.add(passthrough(c, c.label(), passed, present, scope));
                    }
                }
                case SqlAst.SelectExpr x -> outputs.add(output(x, scope, passed, present));
            }
        }
        return assemble(s, scope, windowed.relation(), present, outputs);
    }

    /** A query with GROUP BY or aggregates: keys and aggregates by groupBy (or aggregate), then HAVING, then as
     *  plain. IQ island, 2026-10-17 (the landing's review): its guards are its own -- a subquery's grouping in one of
     *  its aggregates' arguments neither applies nor clears them. */
    private Rel grouped(SqlAst.Select s, Input in, ValueSpecification relation) {
        List<ValueSpecification> enclosing = new ArrayList<>(guards);
        guards.clear();
        try {
            return groupedRows(s, in, relation);
        } finally {
            guards.clear();
            guards.addAll(enclosing);
        }
    }

    private Rel groupedRows(SqlAst.Select s, Input in, ValueSpecification relation) {
        // the group keys: a column is a key as itself; any other expression is computed first
        Map<String, Col> byCanon = new LinkedHashMap<>();
        Map<String, Col> keyBySource = new HashMap<>();
        List<ColSpec> keys = new ArrayList<>();
        List<ColSpec> computedKeys = new ArrayList<>();
        List<ColSpec> representatives = new ArrayList<>();
        Set<String> names = taken();
        names.addAll(in.names());
        for (SqlAst.Expr g : s.groupBy()) {
            SqlAst.Expr key = groupKey(g, s, in);
            noAggregate(key, "GROUP BY");
            String canon = canon(key, in);
            if (byCanon.containsKey(canon)) {
                continue;
            }
            Col col;
            if (key instanceof SqlAst.ColumnRef ref) {
                Col source = in.column(ref);
                notJsonKey(source.kind(), "GROUP BY", key.pos());
                java.util.function.UnaryOperator<ValueSpecification> collation = dialect.key("GROUP BY",
                        Tx.value(property(row, source.name()), source.kind(), source.nullable(), source.detail()), key,
                        key.pos());
                if (collation == null) {
                    col = source;
                    keyBySource.put(source.name(), source);
                } else {
                    // the dialect's key, grouped by in the column's place (the dialect reads no value of it but as
                    // the key: an ORDER BY of it)
                    col = Col.of(fresh(names), source.kind(), source.nullable(), source.detail());
                    keyBySource.put(source.name(), col);
                    computedKeys.add(new ColSpec(col.name(), lambda(row, collation.apply(valueOf(tx(key, in, row)))),
                            null, null));
                    if (dialect.representative("GROUP BY", Tx.value(property(row, source.name()), source.kind(),
                            source.nullable(), source.detail()), key)) {
                        // IQ island, 2026-10-15: the key's value read as one value of its group, the least (min is
                        // a [0..1] whatever the values)
                        Col shown = Col.of(fresh(names), source.kind(), true, source.detail());
                        representatives.add(new ColSpec(shown.name(), lambda(row, property(row, source.name())),
                                lambda(group, call("min", new Variable(group))), null));
                        keyBySource.put(source.name(), shown);
                        keys.add(new ColSpec(col.name(), null, null, null));
                        byCanon.put(canon, shown);
                        continue;
                    }
                }
            } else {
                Tx t = tx(key, in, row);
                notJsonKey(t.kind(), "GROUP BY", key.pos());
                java.util.function.UnaryOperator<ValueSpecification> collation = dialect.key("GROUP BY", t, key,
                        key.pos());
                col = Col.of(fresh(names), t.kind(), t.nullable(), t.detail());
                computedKeys.add(new ColSpec(col.name(), lambda(row, collation == null ? valueOf(t)
                        : collation.apply(valueOf(t))), null, null));
                if (collation != null && dialect.representative("GROUP BY", t, key)) {
                    // IQ island, 2026-10-15: the key's value read as one value of its group, the least (min is a
                    // [0..1] whatever the values)
                    Col shown = Col.of(fresh(names), t.kind(), true, t.detail());
                    representatives.add(new ColSpec(shown.name(), lambda(row, valueOf(t)),
                            lambda(group, call("min", new Variable(group))), null));
                    keys.add(new ColSpec(col.name(), null, null, null));
                    byCanon.put(canon, shown);
                    continue;
                }
            }
            byCanon.put(canon, col);
            keys.add(new ColSpec(col.name(), null, null, null));
        }
        // the aggregates, wherever they are written
        List<ColSpec> aggregates = new ArrayList<>();
        List<ColSpec> finishes = new ArrayList<>();
        List<SqlAst.Expr> scanned = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                scanned.add(x.expr());
            }
        }
        SqlAst.Expr having = s.having();
        if (having != null) {
            scanned.add(having);
        }
        for (SqlAst.SortItem o : s.orderBy()) {
            scanned.add(o.expr());
        }
        // an aggregate that IS a select item is computed under that item's name, so it needs no copy
        Map<String, String> preferred = new HashMap<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && x.expr() instanceof SqlAst.FunctionCall f && isAggregate(f)) {
                SqlAst.Ident alias = x.alias();
                preferred.putIfAbsent(canon(f, in), alias != null ? alias.text()
                        : dialect.defaultName(x, ref -> tableOf(in, ref)));
            }
        }
        for (SqlAst.Expr e : scanned) {
            collectAggregates(e, in, byCanon, aggregates, finishes, names, preferred);
        }
        aggregates.addAll(0, representatives);
        if (!computedKeys.isEmpty()) {
            relation = call("extend", relation, new ColSpecArray(computedKeys));
        }
        if (keys.isEmpty()) {
            relation = call("aggregate", relation, new ColSpecArray(aggregates));
        } else if (aggregates.isEmpty()) {
            relation = call("distinct", call("select", relation, new ColSpecArray(keys)));
        } else {
            relation = call("groupBy", relation, new ColSpecArray(keys), new ColSpecArray(aggregates));
        }
        if (!finishes.isEmpty()) {
            relation = call("extend", relation, new ColSpecArray(finishes));
        }
        if (!guards.isEmpty()) {
            // IQ island, 2026-10-17: a dialect's test of every group (an overflow its database raises in the
            // aggregation for each group, those the statement reads or not), a filter of the grouped rows that keeps
            // each or raises
            ValueSpecification all = guards.get(0);
            for (int i = 1; i < guards.size(); i++) {
                all = call("and", all, guards.get(i));
            }
            relation = call("filter", relation, lambda(row, all));
            guards.clear();
        }
        Grouped g = new Grouped(in, byCanon, keyBySource);
        if (having != null) {
            relation = call("filter", relation, lambda(row, condition(having, g, "HAVING")));
        }
        Set<String> present = taken();
        for (Col c : byCanon.values()) {
            present.add(c.name());
        }
        Windowed windowed = windows(s, g, relation, present);
        List<Output> outputs = new ArrayList<>();
        Set<String> passed = new HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            switch (item) {
                case SqlAst.SelectExpr x -> outputs.add(output(x, windowed.scope(), passed, present));
                case SqlAst.AllColumns all -> {
                    // IQ island, 2026-10-17 (the corpus lane; measured on IQ 17.1): a star in a grouped query shows
                    // its columns, each a group key (SELECT * FROM (...) d GROUP BY id); another is not grouped
                    for (Col c : starColumns(all, in)) {
                        Col key = g.key(c, new SqlAst.ColumnRef(all.qualifier() == null
                                ? List.of(new SqlAst.Ident(c.label(), true, all.pos()))
                                : List.of(all.qualifier(), new SqlAst.Ident(c.label(), true, all.pos())), all.pos()));
                        outputs.add(passthrough(key, c.label(), passed, present, windowed.scope()));
                    }
                }
            }
        }
        return assemble(s, windowed.scope(), windowed.relation(), present, outputs);
    }

    /** The columns a star shows: the FROM items' all, or one item's. */
    private List<Col> starColumns(SqlAst.AllColumns all, Input in) {
        SqlAst.Ident q = all.qualifier();
        if (q == null) {
            return in.star;
        }
        Binding b = in.binding(q);
        if (b == null) {
            throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_TABLE, q.text()), q.pos());
        }
        return b.columns().stream().map(BCol::col).toList();
    }

    // ------------------------------------------------------------------ windows

    /** The relation with its window columns, and the scope in which each window call is its column. */
    private record Windowed(ValueSpecification relation, Scope scope) {
    }

    /** A scope in which each window call is its column; everything else is {@code base}'s. */
    private record WindowScope(Scope base, Map<SqlAst.Window, Col> calls) implements Scope {

        @Override
        public Col column(SqlAst.ColumnRef ref) {
            return base.column(ref);
        }

        @Override
        public @com.legend.base.Nullable Col columnOrNull(SqlAst.ColumnRef ref) {
            return base.columnOrNull(ref);
        }

        @Override
        public @com.legend.base.Nullable Col functionValue(SqlAst.ColumnRef ref) {
            return base.functionValue(ref);
        }

        @Override
        public boolean hasTable(SqlAst.Ident name) {
            return base.hasTable(name);
        }

        @Override
        public @com.legend.base.Nullable Col grouped(SqlAst.Expr e) {
            Col c = e instanceof SqlAst.Window w ? calls.get(w) : null;
            return c != null ? c : base.grouped(e);
        }

        @Override
        public boolean enclosing(SqlAst.ColumnRef ref) {
            return base.enclosing(ref);
        }

        @Override
        public @com.legend.base.Nullable String noAggregatesIn() {
            return base.noAggregatesIn();
        }
    }

    /** A window function's column: its {@code {p, w, r | ...}} (and an aggregate's reduce), its kind, and whether it
     *  may be NULL. */
    private record WindowColumn(LambdaFunction function, @com.legend.base.Nullable LambdaFunction reduce, Kind kind,
            boolean nullable) {

        ColSpec spec(String name) {
            return new ColSpec(name, function, reduce, null);
        }
    }

    /**
     * The select list's and ORDER BY's window calls, each a column of {@code relation}: computed after WHERE (and
     * after the grouping and HAVING), before the select list, in the order written. A partition key, a sort key or a
     * function's argument that is not a column is computed first, as a column of its own; then
     * {@code extend(over(...), ~column: {p, w, r | ...})}.
     */
    private Windowed windows(SqlAst.Select s, Scope scope, ValueSpecification relation, Set<String> present) {
        // two windows of one name are Postgres's error, whether or not a call reads them (2026-10-17, the regression
        // suite: WINDOW w AS (...), w AS (...) answered)
        Set<String> windowNames = new java.util.HashSet<>();
        for (SqlAst.NamedWindow nw : s.windows()) {
            String name = nw.name().quoted() ? nw.name().text() : dialect.fold(nw.name().text());
            if (!windowNames.add(name) && dialect.oneWindowPerName()) {
                throw SqlTranslationException.invalid("window \"" + name + "\" is already defined", nw.name().pos());
            }
        }
        List<SqlAst.Window> calls = new ArrayList<>();
        Map<SqlAst.Window, String> named = new java.util.IdentityHashMap<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                windowCalls(x.expr(), calls);
                if (x.expr() instanceof SqlAst.Window w) {
                    SqlAst.Ident alias = x.alias();
                    named.put(w, alias != null ? alias.text() : dialect.defaultName(x, ref -> tableOf(scope, ref)));
                }
            }
        }
        for (SqlAst.SortItem o : s.orderBy()) {
            windowCalls(o.expr(), calls);
        }
        if (calls.isEmpty()) {
            return new Windowed(relation, scope);
        }
        Map<SqlAst.Window, Col> columns = new java.util.IdentityHashMap<>();
        ValueSpecification rel = relation;
        for (SqlAst.Window w : calls) {
            SqlAst.WindowSpec spec = windowSpec(w.spec(), s.windows());
            List<ColSpec> computed = new ArrayList<>();
            List<String> partitionKeys = new ArrayList<>();
            for (SqlAst.Expr e : spec.partitionBy()) {
                Col key = windowKey(e, "PARTITION BY", scope, present, computed);
                notJsonKey(key.kind(), "PARTITION BY", e.pos());
                partitionKeys.add(key.name());
            }
            List<ValueSpecification> sorts = new ArrayList<>();
            List<String> sortKeys = new ArrayList<>();
            List<Tx> sortTxs = new ArrayList<>();
            Kind sortKind = null;
            for (SqlAst.SortItem o : spec.orderBy()) {
                Col key = windowKey(o.expr(), "ORDER BY", scope, present, computed);
                notJsonKey(key.kind(), "a window's ORDER BY", o.expr().pos());
                sortKind = sortKind == null ? key.kind() : sortKind;
                sortKeys.add(key.name());
                sortTxs.add(Tx.value(property(row, key.name()), key.kind(), key.nullable(), key.detail()));
                boolean nullsFirst = o.nullsFirst() != null ? java.util.Objects.requireNonNull(o.nullsFirst())
                        : dialect.nullsFirst(o.descending());
                sorts.add(call(o.descending() ? "descending" : "ascending", colSpec(key.name()),
                        new EnumValue(NULL_ORDER, nullsFirst ? "FIRST" : "LAST")));
            }
            String want = named.get(w);
            // IQ island, 2026-10-15 (windows): a dialect's own window function (Postgres's: none), computed over the
            // window's over() as its columns, then its finish
            Windowing windowing = new Windowing(scope, present, computed, spec, sortTxs, want);
            WindowFunction own = dialect.window(w.call(), windowing);
            if (own != null) {
                String ownName = windowing.name();
                SqlAst.Frame f = spec.frame();
                Bounds b = f == null ? null : frame(f, sorts.size(), sortKind);
                if (!computed.isEmpty()) {
                    rel = call("extend", rel, new ColSpecArray(computed));
                }
                for (ColSpec c : own.windows()) {
                    present.add(c.name());
                    rel = call("extend", rel, over(partitionKeys, sorts, b), c);
                }
                ColSpec finish = own.finish();
                if (finish != null) {
                    rel = call("extend", rel, new ColSpecArray(List.of(finish)));
                }
                columns.put(w, Col.of(ownName, own.kind(), own.nullable(), own.detail()));
                continue;
            }
            SqlAst.FunctionCall call = windowCall(w.call());
            SqlAst.Frame frame = spec.frame();
            String exclude = frame == null ? null : frame.exclude();
            if (frame != null && exclude != null && exclude.equals("CURRENT ROW")
                    && frame.unit() == SqlAst.FrameUnit.ROWS && !holdsCurrentRow(frame)) {
                // a ROWS frame without the current row: excluding it leaves the frame (a RANGE frame's offsets do
                // not say so: a row with a NULL key is in its NULL peers' frame whatever they are)
                frame = new SqlAst.Frame(frame.unit(), frame.start(), frame.end(), null, frame.pos());
                exclude = null;
            }
            Bounds bounds = frame == null ? null : frame(exclude == null ? frame : new SqlAst.Frame(frame.unit(),
                    frame.start(), frame.end(), null, frame.pos()), sorts.size(), sortKind);
            if (frame != null && exclude != null) {
                Excluded x = excluded(call, frame, bounds, partitionKeys, sortKeys, sorts, scope, present, computed);
                if (!computed.isEmpty()) {
                    rel = call("extend", rel, new ColSpecArray(computed));
                }
                for (ValueSpecification[] step : x.windows()) {
                    rel = call("extend", rel, step[0], (ColSpec) step[1]);
                }
                String name = want != null && !present.contains(want) && !want.startsWith(SYNTHETIC) ? want
                        : fresh(present);
                present.add(name);
                rel = call("extend", rel, new ColSpec(name, lambda(row, x.value()), null, null));
                columns.put(w, Col.of(name, x.kind(), x.nullable()));
                continue;
            }
            WindowColumn column = windowColumn(call, scope, present, computed);
            if (!computed.isEmpty()) {
                rel = call("extend", rel, new ColSpecArray(computed));
            }
            String name = want != null && !present.contains(want) && !want.startsWith(SYNTHETIC) ? want
                    : fresh(present);
            present.add(name);
            rel = call("extend", rel, over(partitionKeys, sorts, bounds), column.spec(name));
            columns.put(w, Col.of(name, column.kind(), column.nullable()));
        }
        return new Windowed(rel, new WindowScope(scope, columns));
    }

    /** The window calls of {@code e}, outermost first; one inside another's arguments or window is Postgres's
     *  error. */
    private static void windowCalls(SqlAst.Expr e, List<SqlAst.Window> out) {
        if (e instanceof SqlAst.Window w) {
            for (SqlAst.Expr inner : children(w)) {
                if (containsWindow(inner)) {
                    throw SqlTranslationException.invalid("window function calls cannot be nested", inner.pos());
                }
            }
            // one window read in several places is computed once (IQ island, 2026-10-16: NUMBER(*)'s, which the
            // statement's ORDER BY reads too)
            if (out.stream().noneMatch(seen -> seen == w)) {
                out.add(w);
            }
            return;
        }
        for (SqlAst.Expr child : children(e)) {
            windowCalls(child, out);
        }
    }

    private static boolean containsWindow(SqlAst.Expr e) {
        if (e instanceof SqlAst.Window) {
            return true;
        }
        for (SqlAst.Expr child : children(e)) {
            if (containsWindow(child)) {
                return true;
            }
        }
        return false;
    }

    /** A window with its named base's clauses: a reference may add an ORDER BY (when the base has none) and a frame
     *  (when the base has none), never a PARTITION BY (Postgres's rules). A named window builds only on one named
     *  before it. */
    private SqlAst.WindowSpec windowSpec(SqlAst.WindowSpec spec, List<SqlAst.NamedWindow> windows) {
        SqlAst.Ident base = spec.base();
        if (base == null) {
            return spec;
        }
        SqlAst.WindowSpec named = null;
        for (int i = 0; i < windows.size() && named == null; i++) {
            SqlAst.Ident n = windows.get(i).name();
            if (dialect.resolve(base, List.of(n.quoted() ? n.text() : dialect.fold(n.text())), "window") != null) {
                named = windowSpec(windows.get(i).spec(), windows.subList(0, i));
            }
        }
        if (named == null) {
            throw SqlTranslationException.invalid(dialect.message(Problem.NO_SUCH_WINDOW, base.text()), base.pos());
        }
        if (!spec.partitionBy().isEmpty()) {
            throw SqlTranslationException.invalid("cannot override PARTITION BY clause of window \"" + base.text()
                    + "\"", spec.pos());
        }
        if (!spec.orderBy().isEmpty() && !named.orderBy().isEmpty()) {
            throw SqlTranslationException.invalid("cannot override ORDER BY clause of window \"" + base.text() + "\"",
                    spec.pos());
        }
        boolean adds = !spec.orderBy().isEmpty() || spec.frame() != null;
        if (adds && named.frame() != null) {
            throw SqlTranslationException.invalid("cannot copy window \"" + base.text() + "\" because it has a frame"
                    + " clause", spec.pos());
        }
        return new SqlAst.WindowSpec(null, named.partitionBy(),
                spec.orderBy().isEmpty() ? named.orderBy() : spec.orderBy(),
                spec.frame() != null ? spec.frame() : named.frame(), spec.pos());
    }

    /** A partition key, a sort key or a window function's argument as a column of the relation: a column (or a group
     *  key, or an aggregate) as itself; any other expression computed first, under a fresh name, into
     *  {@code computed}. */
    private Col windowKey(SqlAst.Expr e, Scope scope, Set<String> present, List<ColSpec> computed) {
        Col g = scope.grouped(e);
        Col c = g != null ? g : e instanceof SqlAst.ColumnRef ref ? scope.column(ref) : null;
        if (c != null && c.var() == null) {
            return c;
        }
        Tx t = tx(e, scope, row);
        if (t.kind() == Kind.NULL) {
            throw SqlTranslationException.unsupported("a NULL constant in a window is not supported yet", e.pos());
        }
        String name = fresh(present);
        computed.add(new ColSpec(name, lambda(row, valueOf(t)), null, null));
        // IQ island, 2026-10-15 (windows): with the dialect's type detail (Postgres's: none)
        return Col.of(name, t.kind(), t.nullable(), t.detail());
    }

    /** IQ island, 2026-10-15 (windows): a PARTITION BY or a window's ORDER BY key ({@code clause}) as a column of the
     *  relation ({@link #windowKey(SqlAst.Expr, Scope, Set, List)}), or, where the dialect sorts or partitions by a
     *  key of the value ({@link InputDialect#key}; Postgres's: never), that key computed first, under a fresh name. */
    private Col windowKey(SqlAst.Expr e, String clause, Scope scope, Set<String> present, List<ColSpec> computed) {
        Col c = windowKey(e, scope, present, computed);
        java.util.function.UnaryOperator<ValueSpecification> key = dialect.key(clause, Tx.value(property(row,
                c.name()), c.kind(), c.nullable(), c.detail()), e, e.pos());
        if (key == null) {
            return c;
        }
        String name = fresh(present);
        computed.add(new ColSpec(name, lambda(row, key.apply(valueOf(tx(e, scope, row)))), null, null));
        return Col.of(name, c.kind(), c.nullable(), c.detail());
    }

    /** {@code {p, w, r | body}}: a window column's lambda over the partition, the window and the row. */
    private LambdaFunction windowLambda(ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(partition), new Variable(window), new Variable(row)),
                List.of(body));
    }

    /**
     * A window call as Postgres reads it: DISTINCT and an ORDER BY inside the call are its errors over a window; an
     * aggregate's {@code FILTER (WHERE c)} is the aggregate of the rows c holds for, {@code agg(CASE WHEN c THEN x
     * END)} -- every aggregate here skips a NULL ({@code count(*)} counts the constant 1 where c holds).
     */
    private SqlAst.FunctionCall windowCall(SqlAst.FunctionCall f) {
        if (f.name().size() != 1) {
            return f;
        }
        String fn = functionName(f.name().get(0));
        if (f.distinct()) {
            throw SqlTranslationException.invalid("DISTINCT is not implemented for window functions", f.pos());
        }
        if (!f.orderBy().isEmpty()) {
            throw SqlTranslationException.invalid("aggregate ORDER BY is not implemented for window functions",
                    f.pos());
        }
        SqlAst.Expr filter = f.filter();
        if (filter == null) {
            return f;
        }
        if (f.star() && !fn.equals("count")) {
            throw SqlTranslationException.invalid(fn + "(*) is not a function; only count(*) is", f.pos());
        }
        if (WINDOW_FUNCTIONS.contains(fn)) {
            throw SqlTranslationException.invalid("FILTER is not implemented for non-aggregate window functions",
                    f.pos());
        }
        if (containsAggregate(filter)) {
            throw SqlTranslationException.invalid("aggregate functions are not allowed in FILTER", filter.pos());
        }
        List<SqlAst.Expr> args = new ArrayList<>();
        if (f.star()) {
            args.add(new SqlAst.Case(null, List.of(new SqlAst.When(filter, new SqlAst.IntegerLit(
                    java.math.BigInteger.ONE, f.pos()))), null, f.pos()));
        } else {
            for (SqlAst.Expr a : f.args()) {
                args.add(new SqlAst.Case(null, List.of(new SqlAst.When(filter, a)), null, a.pos()));
            }
        }
        return new SqlAst.FunctionCall(f.name(), args, false, false, f.orderBy(), f.withinGroup(), null, f.pos());
    }

    /** Whether a frame's bounds hold the current row: it starts at or before it and ends at or after it (a RANGE
     *  frame's offsets are from the current row's value, which is 0 from itself; a row with a NULL key is in its NULL
     *  peers' frame whatever the bounds). */
    private static boolean holdsCurrentRow(SqlAst.Frame f) {
        return offset(f.start()) <= 0 && offset(f.end()) >= 0;
    }

    /** A bound's offset from the current row: PRECEDING negative, the unbounded ones the furthest. */
    private static long offset(SqlAst.FrameBound b) {
        return switch (b.kind()) {
            case UNBOUNDED_PRECEDING -> Long.MIN_VALUE;
            case UNBOUNDED_FOLLOWING -> Long.MAX_VALUE;
            case CURRENT_ROW -> 0;
            case PRECEDING, FOLLOWING -> {
                SqlAst.Expr e = java.util.Objects.requireNonNull(b.offset());
                long v = e instanceof SqlAst.IntegerLit n && n.value().bitLength() <= 31 ? n.value().longValue() : 0;
                yield b.kind() == SqlAst.BoundKind.PRECEDING ? -v : v;
            }
        };
    }

    /** A window aggregate over a frame with EXCLUDE: the windows its parts are computed in (each an over() and its
     *  column), then its value from them, its kind, and whether it may be NULL. */
    private record Excluded(List<ValueSpecification[]> windows, ValueSpecification value, Kind kind,
            boolean nullable) {
    }

    /** A min or max argument: Postgres has neither of a boolean (bool_and and bool_or are its), nor of jsonb. */
    static void minMaxable(Kind kind, String fn, SqlAst.Pos pos) {
        notJsonKey(kind, fn, pos);
        if (kind == Kind.BOOLEAN) {
            throw SqlTranslationException.invalid("function " + fn + "(boolean) does not exist", pos);
        }
    }

    /** Zero in a numeric kind's Pure type. */
    private static ValueSpecification zero(Kind kind) {
        return switch (kind) {
            case FLOAT, INEXACT -> new com.legend.protocol.spec.CFloat(0.0);
            case DECIMAL, NUMBER -> new CDecimal(BigDecimal.ZERO);
            default -> new CInteger(0L);
        };
    }

    /** The aggregates EXCLUDE is read for. */
    private static final Set<String> EXCLUDABLE = Set.of("count", "sum", "avg", "min", "max");

    /**
     * {@code agg(x) OVER (... frame EXCLUDE ...)}: Pure's frames have no EXCLUDE, so the aggregate is computed from
     * windows that do not need it.
     * - count, sum, avg: the frame's count and sum, less what is excluded: EXCLUDE CURRENT ROW (of a ROWS or RANGE
     *   frame) the current row's own value; EXCLUDE GROUP (of a whole partition) the current row's peers', a window
     *   partitioned by the ORDER BY keys too; EXCLUDE TIES the peers' less the current row's own. A sum or an
     *   average is NULL where no value remains, as Postgres's is.
     * - min, max with EXCLUDE CURRENT ROW (of a whole partition): the partition's, unless the current row holds it
     *   alone; then the least (greatest) of the rest.
     * Others (a frame that is not the whole partition but for CURRENT ROW's sums, min and max of GROUP or TIES, other
     * aggregates) are not supported yet.
     */
    private Excluded excluded(SqlAst.FunctionCall f, SqlAst.Frame frame, @com.legend.base.Nullable Bounds bounds,
            List<String> partitionKeys, List<String> sortKeys, List<ValueSpecification> sorts, Scope scope,
            Set<String> present, List<ColSpec> computed) {
        String exclude = java.util.Objects.requireNonNull(frame.exclude());
        String fn = f.name().size() == 1 ? functionName(f.name().get(0)) : "";
        boolean whole = frame.start().kind() == SqlAst.BoundKind.UNBOUNDED_PRECEDING
                && frame.end().kind() == SqlAst.BoundKind.UNBOUNDED_FOLLOWING
                || frame.unit() == SqlAst.FrameUnit.RANGE && sortKeys.isEmpty() && frame.start().offset() == null
                && frame.end().offset() == null;
        boolean minMax = fn.equals("min") || fn.equals("max");
        boolean currentRow = exclude.equals("CURRENT ROW");
        if (!EXCLUDABLE.contains(fn) || !whole && !(currentRow && !minMax && (frame.unit() == SqlAst.FrameUnit.ROWS
                || frame.unit() == SqlAst.FrameUnit.RANGE && holdsCurrentRow(frame))) || minMax && !currentRow) {
            throw SqlTranslationException.unsupported("EXCLUDE " + exclude + " of " + (fn.isEmpty() ? "this function"
                    : fn + "(...)") + (whole ? "" : " over this frame") + " is not supported yet", frame.pos());
        }
        Col arg = null;
        if (f.star()) {
            if (!fn.equals("count")) {
                throw SqlTranslationException.invalid(fn + "(*) is not a function; only count(*) is", f.pos());
            }
        } else {
            windowArguments(f, 1, 1);
            arg = windowKey(f.args().get(0), scope, present, computed);
            if (minMax) {
                minMaxable(arg.kind(), fn, f.pos());
            } else if (!fn.equals("count") && !arg.kind().numeric()) {
                throw SqlTranslationException.invalid("function " + fn + "(" + dialect.typeName(arg.kind())
                        + ") does not exist", f.pos());
            } else if (!fn.equals("count") && (arg.kind() == Kind.FLOAT || arg.kind() == Kind.INEXACT)) {
                // a frame's sum of doubles less the row's own loses the digits Postgres's sum of the rest keeps
                throw SqlTranslationException.unsupported("EXCLUDE " + exclude + " of " + fn + "(...) of double"
                        + " precision values is not supported yet", f.pos());
            }
        }
        ValueSpecification x = arg == null ? new CInteger(1L) : property(row, arg.name());
        boolean xNullable = arg != null && arg.nullable();
        ValueSpecification whole0 = over(partitionKeys, List.of(), null);
        List<ValueSpecification[]> windows = new ArrayList<>();
        if (minMax) {
            Col a = java.util.Objects.requireNonNull(arg);
            String best = fresh(present);
            windows.add(new ValueSpecification[] {whole0, new ColSpec(best, windowLambda(x), lambda(group,
                    call(fn, new Variable(group))), null)});
            ValueSpecification b = call("toOne", property(row, best));
            ValueSpecification one = xNullable ? call("toOne", x) : x;
            // the current row's value where it is the partition's best, and the rest's best beyond it
            String ties = fresh(present);
            ValueSpecification tie = call("if", call("equal", one, b), thunk(new CInteger(1L)), thunk(empty()));
            windows.add(new ValueSpecification[] {whole0, new ColSpec(ties, windowLambda(xNullable ? call("if",
                    call("isEmpty", x), thunk(empty()), thunk(tie)) : tie), lambda(group, call("count",
                    new Variable(group))), null)});
            String rest = fresh(present);
            ValueSpecification beyond = call("if", call(fn.equals("min") ? "greaterThan" : "lessThan", one, b),
                    thunk(one), thunk(empty()));
            windows.add(new ValueSpecification[] {whole0, new ColSpec(rest, windowLambda(xNullable ? call("if",
                    call("isEmpty", x), thunk(empty()), thunk(beyond)) : beyond), lambda(group, call(fn,
                    new Variable(group))), null)});
            ValueSpecification alone = call("and", call("equal", one, b), call("equal", property(row, ties),
                    new CInteger(1L)));
            if (xNullable) {
                alone = call("and", call("isNotEmpty", x), alone);
            }
            return new Excluded(windows, call("if", alone, thunk(property(row, rest)), thunk(property(row, best))),
                    a.kind(), true);
        }
        // the frame's (or the partition's) count and sum
        ValueSpecification frameOver = currentRow ? over(partitionKeys, sorts, bounds) : whole0;
        String count = fresh(present);
        windows.add(new ValueSpecification[] {frameOver, new ColSpec(count, windowLambda(x), lambda(group,
                call("count", new Variable(group))), null)});
        String sum = null;
        if (!fn.equals("count")) {
            sum = fresh(present);
            windows.add(new ValueSpecification[] {frameOver, new ColSpec(sum, windowLambda(x), lambda(group,
                    call("sum", new Variable(group))), null)});
        }
        // the current row's own value, and its count (1 where it holds one)
        ValueSpecification zero = zero(arg == null ? Kind.INTEGER : arg.kind());
        ValueSpecification own = xNullable ? call("if", call("isEmpty", x), thunk(zero), thunk(call("toOne", x))) : x;
        ValueSpecification ownCount = xNullable ? call("if", call("isEmpty", x), thunk(new CInteger(0L)),
                thunk(new CInteger(1L))) : new CInteger(1L);
        ValueSpecification remaining;
        ValueSpecification total = sum == null ? null : call("if", call("isEmpty", property(row, sum)),
                thunk(zero), thunk(call("toOne", property(row, sum))));
        if (currentRow) {
            remaining = AppliedFunction.infixRun("minus", List.of(property(row, count), ownCount));
            if (total != null) {
                total = AppliedFunction.infixRun("minus", List.of(total, own));
            }
        } else {
            List<String> peers = new ArrayList<>(partitionKeys);
            peers.addAll(sortKeys);
            ValueSpecification groupOver = over(peers, List.of(), null);
            String groupCount = fresh(present);
            windows.add(new ValueSpecification[] {groupOver, new ColSpec(groupCount, windowLambda(x), lambda(group,
                    call("count", new Variable(group))), null)});
            remaining = AppliedFunction.infixRun("minus", List.of(property(row, count), property(row, groupCount)));
            boolean ties = exclude.equals("TIES");
            if (ties) {
                remaining = AppliedFunction.infixRun("plus", List.of(remaining, ownCount));
            }
            if (total != null) {
                String groupSum = fresh(present);
                windows.add(new ValueSpecification[] {groupOver, new ColSpec(groupSum, windowLambda(x), lambda(group,
                        call("sum", new Variable(group))), null)});
                ValueSpecification peersSum = call("if", call("isEmpty", property(row, groupSum)), thunk(zero),
                        thunk(call("toOne", property(row, groupSum))));
                total = AppliedFunction.infixRun("minus", List.of(total, peersSum));
                if (ties) {
                    total = AppliedFunction.infixRun("plus", List.of(total, own));
                }
            }
        }
        if (fn.equals("count")) {
            return new Excluded(windows, remaining, Kind.INTEGER, false);
        }
        Kind argKind = java.util.Objects.requireNonNull(arg).kind();
        ValueSpecification none = call("equal", remaining, new CInteger(0L));
        ValueSpecification value = fn.equals("sum") ? java.util.Objects.requireNonNull(total)
                : call("divide", java.util.Objects.requireNonNull(total), remaining);
        return new Excluded(windows, call("if", none, thunk(empty()), thunk(value)),
                fn.equals("sum") ? sumKind(argKind) : statisticKind(argKind), true);
    }

    /** A window call's function: a ranking function, a value function (lag, lead, first_value, last_value, nth_value:
     *  the other row's column), or an aggregate over the window's frame. */
    private WindowColumn windowColumn(SqlAst.FunctionCall f, Scope scope, Set<String> present,
            List<ColSpec> computed) {
        if (f.name().size() != 1) {
            throw SqlTranslationException.unsupported("schema-qualified function names are not supported yet",
                    f.pos());
        }
        String fn = functionName(f.name().get(0));
        if (!f.withinGroup().isEmpty()) {
            throw SqlTranslationException.unsupported("WITHIN GROUP in a window call is not supported yet", f.pos());
        }
        if (f.star() && !fn.equals("count")) {
            throw SqlTranslationException.invalid(fn + "(*) is not a function; only count(*) is", f.pos());
        }
        Variable p = new Variable(partition);
        Variable w = new Variable(window);
        Variable r = new Variable(row);
        if (BOOLEAN_AGGREGATES.contains(fn)) {
            windowArguments(f, 1, 1);
            Col arg = windowKey(f.args().get(0), scope, present, computed);
            if (arg.kind() != Kind.BOOLEAN) {
                throw SqlTranslationException.invalid("function " + fn + "(" + dialect.typeName(arg.kind())
                        + ") does not exist", f.pos());
            }
            return new WindowColumn(windowLambda(property(row, arg.name())), lambda(group,
                    call(fn.equals("bool_or") ? "or" : "and", new Variable(group))), Kind.BOOLEAN, true);
        }
        if (fn.equals("corr")) {
            // Postgres's is NULL where a value's variance in the frame is 0; DuckDB's is NaN there (a grouped corr is
            // guarded by the pairs' variances; a window's would need them as window columns first)
            throw SqlTranslationException.unsupported("corr over a window is not supported yet", f.pos());
        }
        if (BIVARIATE.containsKey(fn)) {
            windowArguments(f, 2, 2);
            Col y = windowKey(f.args().get(0), scope, present, computed);
            Col x = windowKey(f.args().get(1), scope, present, computed);
            for (Col c : List.of(y, x)) {
                if (!c.kind().numeric()) {
                    throw SqlTranslationException.unsupported(fn + " of " + dialect.typeName(c.kind()) + " values is not"
                            + " supported yet", f.pos());
                }
            }
            return new WindowColumn(windowLambda(call("rowMapper", property(row, y.name()), property(row,
                    x.name()))), lambda(group, call(java.util.Objects.requireNonNull(BIVARIATE.get(fn)),
                    new Variable(group))), Kind.FLOAT, true);
        }
        switch (fn) {
            case "row_number", "rank", "dense_rank", "percent_rank", "cume_dist" -> {
                windowArguments(f, 0, 0);
                ValueSpecification body = fn.equals("row_number") ? call("rowNumber", p, r)
                        : call(java.util.Objects.requireNonNull(RANKINGS.get(fn)), p, w, r);
                boolean fraction = fn.equals("percent_rank") || fn.equals("cume_dist");
                return new WindowColumn(windowLambda(body), null, fraction ? Kind.FLOAT : Kind.INTEGER, false);
            }
            case "ntile" -> {
                windowArguments(f, 1, 1);
                long n = windowCount(f.args().get(0), fn, 1);
                return new WindowColumn(windowLambda(call("ntile", p, r, new CInteger(n))), null, Kind.INTEGER, false);
            }
            case "lag", "lead" -> {
                windowArguments(f, 1, 3);
                Col arg = windowKey(f.args().get(0), scope, present, computed);
                long n = f.args().size() >= 2 ? windowCount(f.args().get(1), fn, 0) : 1L;
                ValueSpecification at = n == 1L ? call(fn, p, r) : call(fn, p, r, new CInteger(n));
                ValueSpecification other = new AppliedProperty(at, arg.name());
                if (f.args().size() < 3 || f.args().get(2) instanceof SqlAst.NullLit) {
                    return new WindowColumn(windowLambda(other), null, arg.kind(), true);
                }
                return lagDefault(f.args().get(2), arg, at, other, scope, present, computed);
            }
            case "first_value", "last_value" -> {
                windowArguments(f, 1, 1);
                Col arg = windowKey(f.args().get(0), scope, present, computed);
                ValueSpecification at = call(fn.equals("first_value") ? "first" : "last", p, w, r);
                return new WindowColumn(windowLambda(new AppliedProperty(at, arg.name())), null, arg.kind(), true);
            }
            case "nth_value" -> {
                windowArguments(f, 2, 2);
                Col arg = windowKey(f.args().get(0), scope, present, computed);
                long n = windowCount(f.args().get(1), fn, 1);
                ValueSpecification at = call("nth", p, w, r, new CInteger(n));
                return new WindowColumn(windowLambda(new AppliedProperty(at, arg.name())), null, arg.kind(), true);
            }
            case "count", "sum", "avg", "min", "max", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp",
                    "var_pop" -> {
                ValueSpecification value;
                Kind argKind;
                if (f.star()) {
                    value = new CInteger(1L);
                    argKind = Kind.INTEGER;
                } else {
                    windowArguments(f, 1, 1);
                    Col arg = windowKey(f.args().get(0), scope, present, computed);
                    value = property(row, arg.name());
                    argKind = arg.kind();
                }
                Variable values = new Variable(group);
                String statistic = STATISTICS.get(fn);
                Kind kind = switch (fn) {
                    case "count" -> Kind.INTEGER;
                    case "sum" -> sumKind(argKind);
                    case "min", "max" -> argKind;
                    default -> statisticKind(argKind);
                };
                if (fn.equals("min") || fn.equals("max")) {
                    minMaxable(argKind, fn, f.pos());
                }
                if (!fn.equals("count") && !fn.equals("min") && !fn.equals("max") && !argKind.numeric()) {
                    throw SqlTranslationException.invalid("function " + fn + "(" + argKind.name().toLowerCase(
                            Locale.ROOT) + ") does not exist", f.pos());
                }
                String reduce = statistic != null ? statistic : fn.equals("avg") ? "average" : fn;
                return new WindowColumn(windowLambda(value), lambda(group, call(reduce, values)), kind,
                        !fn.equals("count"));
            }
            default -> throw SqlTranslationException.unsupported("window function " + fn + "(...) is not supported"
                    + " yet", f.pos());
        }
    }

    /**
     * {@code lag(x, n, default)}, {@code lead(x, n, default)}: the default (the current row's) where the other row is
     * outside the partition -- not where that row's value is NULL, which stays NULL. A column that is never NULL (the
     * constant 1, computed first) tells which. The value and the default resolve to one type, as Postgres's
     * anycompatible does: an untyped string constant read as the value's type, numbers widening.
     */
    private WindowColumn lagDefault(SqlAst.Expr d, Col arg, ValueSpecification at, ValueSpecification other,
            Scope scope, Set<String> present, List<ColSpec> computed) {
        Tx dflt = d instanceof SqlAst.StringLit s && arg.kind() != Kind.STRING && readsStrings(arg.kind())
                ? dialect.readString(single(s, arg.detail()), arg.kind()) : tx(d, scope, row);
        Kind kind;
        if (dflt.kind() == arg.kind()) {
            kind = arg.kind();
        } else if (dflt.kind().numeric() && arg.kind().numeric()) {
            kind = arithmetic(arg.kind(), dflt.kind());
        } else {
            throw SqlTranslationException.unsupported("a default of " + dialect.typeName(dflt.kind()) + " for "
                    + dialect.typeName(arg.kind()) + " values is not supported yet", d.pos());
        }
        if (kind == Kind.DECIMAL) {
            // the value and the default take one scale in the database (see common)
            kind = Kind.NUMBER;
        }
        if (kind == Kind.FLOAT && dflt.kind() != Kind.FLOAT) {
            Tx read = dflt;
            dflt = strict(Kind.FLOAT, List.of(read), v -> call("toFloat", v.get(0)));
        }
        ValueSpecification value = other;
        if (kind == Kind.FLOAT && arg.kind() != Kind.FLOAT) {
            value = call("if", call("isEmpty", other), thunk(empty()), thunk(call("toFloat", call("toOne", other))));
        }
        String name = fresh(present);
        computed.add(new ColSpec(name, lambda(row, valueOf(dflt)), null, null));
        Col one = windowKey(new SqlAst.IntegerLit(java.math.BigInteger.ONE, d.pos()), scope, present, computed);
        ValueSpecification body = call("if", call("isEmpty", new AppliedProperty(at, one.name())),
                thunk(property(row, name)), thunk(value));
        return new WindowColumn(windowLambda(body), null, kind, arg.nullable() || dflt.nullable());
    }

    /** Postgres's ranking functions and Pure's. */
    private static final Map<String, String> RANKINGS = Map.of("rank", "rank", "dense_rank", "denseRank",
            "percent_rank", "percentRank", "cume_dist", "cumulativeDistribution");

    private static void windowArguments(SqlAst.FunctionCall f, int min, int max) {
        if (f.star() || f.args().size() < min || f.args().size() > max) {
            throw SqlTranslationException.unsupported(functionName(f.name().get(0)) + " with "
                    + (f.star() ? "*" : f.args().size() + " arguments") + " is not supported yet", f.pos());
        }
    }

    /** A window function's count argument (ntile's buckets, lag's offset, nth_value's position): an integer constant
     *  of at least {@code min}. */
    private static long windowCount(SqlAst.Expr e, String fn, long min) {
        if (!(e instanceof SqlAst.IntegerLit n) || n.value().bitLength() > 31) {
            throw SqlTranslationException.unsupported(fn + "(...) with a computed count is not supported yet",
                    e.pos());
        }
        long v = n.value().longValue();
        if (v < min) {
            throw SqlTranslationException.unsupported(fn + "(...) with a count below " + min + " is not supported"
                    + " yet", e.pos());
        }
        return v;
    }

    /**
     * A frame as Pure's {@code rows(from, to)} or {@code _range(from, to)}: an offset signed (PRECEDING negative,
     * FOLLOWING positive, CURRENT ROW zero), UNBOUNDED as {@code unbounded()}. RANGE from the partition's start to the
     * current row's last peer is the default (none written); a whole-partition RANGE is the ROWS one.
     */
    private static @com.legend.base.Nullable Bounds frame(SqlAst.Frame f, int sortKeys,
            @com.legend.base.Nullable Kind sortKind) {
        if (f.unit() == SqlAst.FrameUnit.GROUPS) {
            throw SqlTranslationException.unsupported("GROUPS frames are not supported yet", f.pos());
        }
        String exclude = f.exclude();
        if (exclude != null) {
            throw SqlTranslationException.unsupported("EXCLUDE " + exclude + " is not supported yet", f.pos());
        }
        SqlAst.BoundKind start = f.start().kind();
        SqlAst.BoundKind end = f.end().kind();
        if (start == SqlAst.BoundKind.UNBOUNDED_FOLLOWING) {
            throw SqlTranslationException.invalid("frame start cannot be UNBOUNDED FOLLOWING", f.pos());
        }
        if (end == SqlAst.BoundKind.UNBOUNDED_PRECEDING) {
            throw SqlTranslationException.invalid("frame end cannot be UNBOUNDED PRECEDING", f.pos());
        }
        if (start == SqlAst.BoundKind.CURRENT_ROW && end == SqlAst.BoundKind.PRECEDING) {
            throw SqlTranslationException.invalid("frame starting from current row cannot have preceding rows",
                    f.pos());
        }
        if (start == SqlAst.BoundKind.FOLLOWING
                && (end == SqlAst.BoundKind.PRECEDING || end == SqlAst.BoundKind.CURRENT_ROW)) {
            throw SqlTranslationException.invalid("frame starting from following row cannot have preceding rows",
                    f.pos());
        }
        boolean offsets = f.start().offset() != null || f.end().offset() != null;
        if (f.unit() == SqlAst.FrameUnit.RANGE) {
            if (!offsets && start == SqlAst.BoundKind.UNBOUNDED_PRECEDING && end == SqlAst.BoundKind.CURRENT_ROW) {
                return null;
            }
            if (!offsets && (sortKeys == 0 || start == SqlAst.BoundKind.UNBOUNDED_PRECEDING
                    && end == SqlAst.BoundKind.UNBOUNDED_FOLLOWING)) {
                // without ORDER BY every row is the current row's peer: the frame is the whole partition
                return new Bounds(call("rows", call("unbounded"), call("unbounded")), false);
            }
            if (sortKeys != 1) {
                throw offsets ? SqlTranslationException.invalid("RANGE with offset PRECEDING/FOLLOWING requires"
                        + " exactly one ORDER BY column", f.pos())
                        : SqlTranslationException.unsupported("a RANGE frame from the current row over several sort"
                                + " keys is not supported yet", f.pos());
            }
            if (offsets && (sortKind == null || !sortKind.numeric())) {
                throw SqlTranslationException.unsupported("RANGE offsets over a non-numeric sort key are not supported"
                        + " yet", f.pos());
            }
        }
        ValueSpecification from = bound(f.start(), f.unit());
        ValueSpecification to = bound(f.end(), f.unit());
        if (from instanceof CInteger a && to instanceof CInteger b && a.value().longValue() > b.value().longValue()) {
            throw SqlTranslationException.unsupported("a frame that ends before it starts is not supported yet",
                    f.pos());
        }
        boolean range = f.unit() == SqlAst.FrameUnit.RANGE;
        return new Bounds(call(range ? "_range" : "rows", from, to), range);
    }

    /** A frame's Pure spelling, and whether it is a RANGE one (over() takes one sort key before it). */
    private record Bounds(ValueSpecification spec, boolean range) {
    }

    private static ValueSpecification bound(SqlAst.FrameBound b, SqlAst.FrameUnit unit) {
        return switch (b.kind()) {
            case UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING -> call("unbounded");
            case CURRENT_ROW -> new CInteger(0L);
            case PRECEDING, FOLLOWING -> {
                SqlAst.Expr offset = java.util.Objects.requireNonNull(b.offset());
                if (!(offset instanceof SqlAst.IntegerLit n) || n.value().bitLength() > 31) {
                    throw SqlTranslationException.unsupported("a frame offset other than an integer constant is not"
                            + " supported yet", offset.pos());
                }
                long v = n.value().longValue();
                yield new CInteger(b.kind() == SqlAst.BoundKind.PRECEDING ? -v : v);
            }
        };
    }

    /** Pure's {@code over(...)} for the keys and the frame: the partition keys, the sort (one sort key, not a list,
     *  before a RANGE frame), the frame; with no partition keys, {@code over([sorts])} ({@code OVER ()} for none),
     *  or {@code over([], [sorts], rows(...))} with a ROWS frame. */
    private static ValueSpecification over(List<String> partitionKeys, List<ValueSpecification> sorts,
            @com.legend.base.Nullable Bounds frame) {
        boolean range = frame != null && frame.range();
        List<ValueSpecification> args = new ArrayList<>();
        if (!partitionKeys.isEmpty()) {
            List<ColSpec> keys = new ArrayList<>();
            partitionKeys.forEach(k -> keys.add(colSpec(k)));
            args.add(new ColSpecArray(keys));
        } else if (frame != null && !range) {
            args.add(new PureCollection(List.of()));
        }
        if (range) {
            args.add(sorts.get(0));
        } else if (!sorts.isEmpty() || partitionKeys.isEmpty()) {
            args.add(new PureCollection(sorts));
        }
        if (frame != null) {
            args.add(frame.spec());
        }
        return call("over", args);
    }


    /** A GROUP BY item's expression: an ordinal names a select item; a bare name is an input column first, then an
     *  output name (Postgres's order). */
    private SqlAst.Expr groupKey(SqlAst.Expr g, SqlAst.Select s, Input in) {
        if (g instanceof SqlAst.IntegerLit n && dialect.groupsByPosition()) {
            return selectExpr(s, n, "GROUP BY");
        }
        if (g instanceof SqlAst.ColumnRef ref && ref.parts().size() == 1
                && !in.has(ref.parts().get(0))) {
            List<String> outputNames = new ArrayList<>();
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr x && x.alias() != null) {
                    outputNames.add(java.util.Objects.requireNonNull(x.alias()).text());
                }
            }
            String found = dialect.resolve(ref.parts().get(0), outputNames, "column");
            if (found != null) {
                for (SqlAst.SelectItem item : s.items()) {
                    if (item instanceof SqlAst.SelectExpr x && x.alias() != null
                            && java.util.Objects.requireNonNull(x.alias()).text().equals(found)) {
                        return x.expr();
                    }
                }
            }
        }
        return g;
    }

    private static SqlAst.Expr selectExpr(SqlAst.Select s, SqlAst.IntegerLit n, String clause) {
        int i = n.value().intValue();
        if (n.value().signum() <= 0 || n.value().compareTo(java.math.BigInteger.valueOf(s.items().size())) > 0) {
            throw SqlTranslationException.invalid(clause + " position " + n.value() + " is not in select list",
                    n.pos());
        }
        SqlAst.SelectItem item = s.items().get(i - 1);
        if (item instanceof SqlAst.SelectExpr x) {
            return x.expr();
        }
        throw SqlTranslationException.unsupported(clause + " position " + i + " is \"*\", which is not supported"
                + " here yet", n.pos());
    }

    // ------------------------------------------------------------------ outputs and the tail

    /** An output column: the name the query shows, the column that holds it, and its value when it is computed. */
    private record Output(String name, String internal, @com.legend.base.Nullable ValueSpecification value,
            SqlAst.@com.legend.base.Nullable Expr expr, Kind kind, boolean nullable,
            InputDialect.@com.legend.base.Nullable TypeDetail detail, List<ColSpec> pre) {

        Output {
            pre = List.copyOf(pre);
        }

        /** An output computed from the row alone: no column computed before it. */
        Output(String name, String internal, @com.legend.base.Nullable ValueSpecification value,
                SqlAst.@com.legend.base.Nullable Expr expr, Kind kind, boolean nullable,
                InputDialect.@com.legend.base.Nullable TypeDetail detail) {
            this(name, internal, value, expr, kind, nullable, detail, List.of());
        }
    }

    private Output output(SqlAst.SelectExpr x, Scope scope, Set<String> passed, Set<String> present) {
        SqlAst.Ident alias = x.alias();
        Col g = scope.grouped(x.expr());
        Col col = g != null ? g : x.expr() instanceof SqlAst.ColumnRef ref ? scope.column(ref) : null;
        if (col != null && col.var() == null) {
            // a function's name read as its value shows as that name (Postgres's FigureColname), a column as the
            // dialect names it (Postgres: as the source spells it)
            String name = alias != null ? alias.text() : x.expr() instanceof SqlAst.ColumnRef ref
                    && scope.columnOrNull(ref) != null && dialect.namedAsColumn(x)
                    ? dialect.columnName(ref.parts().get(ref.parts().size() - 1), col.label())
                    : dialect.defaultName(x, ref -> tableOf(scope, ref));
            Output o = passthrough(col, name, passed, present, scope);
            return new Output(o.name(), o.internal(), o.value(), x.expr(), o.kind(), o.nullable(),
                    narrowed(o.kind(), o.detail(), x.expr(), scope));
        }
        // IQ island, 2026-10-17 (the corpus lane's scale: a quotient of a correlated COUNT subquery repeated it 504
        // times, each check of a checked operation reading its operand again): a scalar subquery the item computes for
        // every row it is computed for is computed once, a column of the row before it (assemble), and read as one
        List<ColSpec> pre = new ArrayList<>();
        List<SqlAst.Expr> mine = new ArrayList<>();
        Tx t;
        // 2026-10-18 (landing 5's review): every column this item hoists is unbound however it ends
        try {
            for (SqlAst.ScalarSubquery q : x.expr() instanceof SqlAst.ScalarSubquery
                    ? List.<SqlAst.ScalarSubquery>of() : eagerSubqueries(x.expr())) {
                Tx sq = scalarSubquery(q, scope);
                if (sq.kind() == Kind.BOOLEAN) {
                    continue;
                }
                String column = fresh(present);
                pre.add(new ColSpec(column, lambda(row, valueOf(sq)), null, null));
                hoisted.put(q, Col.of(column, sq.kind(), true, sq.detail()));
                mine.add(q);
            }
            try {
                t = tx(x.expr(), scope, row);
            } catch (SqlTranslationException tooLarge) {
                if (!tooLarge.pastSizeBound()) {
                    throw tooLarge;
                }
                // IQ island, 2026-10-17 (the corpus lane's scale): an item past the dialect's size bound has its long
                // operands, read on every path, each computed once, a column before it (bottom up, so each reads
                // the ones below it), then is translated again; past the bound still, it is refused as before
                for (SqlAst.Expr c : eagerOperands(x.expr())) {
                    Tx ct;
                    try {
                        ct = tx(c, scope, row);
                    } catch (SqlTranslationException notAlone) {
                        continue;
                    }
                    if (ct.kind() == Kind.BOOLEAN || written(ct.vs()) < BOUND_OPERAND) {
                        continue;
                    }
                    String column = fresh(present);
                    pre.add(new ColSpec(column, lambda(row, valueOf(ct)), null, null));
                    hoisted.put(c, Col.of(column, ct.kind(), ct.nullable(), ct.detail()));
                    mine.add(c);
                }
                t = tx(x.expr(), scope, row);
            }
        } finally {
            mine.forEach(hoisted::remove);
        }
        String name = alias != null ? alias.text() : dialect.defaultName(x, ref -> tableOf(scope, ref));
        return new Output(name, internalFor(name, present), valueOf(t), x.expr(), t.kind(), t.nullable(),
                narrowed(t.kind(), t.detail(), x.expr(), scope), pre);
    }

    /** A select item's type detail: its value's, else the dialect's narrow integer where the item is one
     *  ({@link #widened}), so that a query reading the column knows it. */
    private InputDialect.@com.legend.base.Nullable TypeDetail narrowed(Kind kind,
            InputDialect.@com.legend.base.Nullable TypeDetail detail, SqlAst.Expr e, Scope scope) {
        return detail == null && kind == Kind.INTEGER && widened(e, scope) ? dialect.narrowInteger() : detail;
    }

    /** The scalar subqueries of {@code e} that it computes whenever it is computed ({@link #output}): reached through
     *  operators, casts and the arguments of functions that read each, not through a CASE, AND, OR, a NULL-reading
     *  function's later arguments, an aggregate or a window. */
    private List<SqlAst.ScalarSubquery> eagerSubqueries(SqlAst.Expr e) {
        List<SqlAst.ScalarSubquery> out = new ArrayList<>();
        eagerSubqueries(e, out);
        return out;
    }

    private void eagerSubqueries(SqlAst.Expr e, List<SqlAst.ScalarSubquery> out) {
        switch (e) {
            case SqlAst.ScalarSubquery q -> out.add(q);
            case SqlAst.Binary b when b.op() != SqlAst.BinaryOp.AND && b.op() != SqlAst.BinaryOp.OR -> {
                eagerSubqueries(b.left(), out);
                eagerSubqueries(b.right(), out);
            }
            case SqlAst.Unary u -> eagerSubqueries(u.operand(), out);
            case SqlAst.Cast c -> eagerSubqueries(c.operand(), out);
            case SqlAst.FunctionCall f when !isAggregate(f) && f.filter() == null && f.orderBy().isEmpty()
                    && f.withinGroup().isEmpty() -> {
                String fn = f.name().get(f.name().size() - 1).text().toLowerCase(Locale.ROOT);
                List<SqlAst.Expr> read = LAZY_ARGUMENTS.contains(fn) ? f.args().subList(0, Math.min(1,
                        f.args().size())) : f.args();
                read.forEach(a -> eagerSubqueries(a, out));
            }
            default -> {
            }
        }
    }

    /** The written size, in Pure nodes, from which an operand of an item past the size bound is computed once
     *  ({@link #output}). */
    private static final long BOUND_OPERAND = 64;

    /** {@code e}'s operands (not {@code e} itself) that it computes whenever it is computed, composite ones only and
     *  no subquery, each after its own ({@link #eagerSubqueries}' positions). */
    private List<SqlAst.Expr> eagerOperands(SqlAst.Expr e) {
        List<SqlAst.Expr> out = new ArrayList<>();
        eagerOperands(e, out);
        out.remove(out.size() - 1);
        return out;
    }

    private void eagerOperands(SqlAst.Expr e, List<SqlAst.Expr> out) {
        switch (e) {
            case SqlAst.Binary b when b.op() != SqlAst.BinaryOp.AND && b.op() != SqlAst.BinaryOp.OR -> {
                eagerOperands(b.left(), out);
                eagerOperands(b.right(), out);
                out.add(e);
            }
            case SqlAst.Unary u -> {
                eagerOperands(u.operand(), out);
                out.add(e);
            }
            case SqlAst.Cast c -> {
                eagerOperands(c.operand(), out);
                out.add(e);
            }
            case SqlAst.FunctionCall f when !isAggregate(f) && f.filter() == null && f.orderBy().isEmpty()
                    && f.withinGroup().isEmpty() -> {
                String fn = f.name().get(f.name().size() - 1).text().toLowerCase(Locale.ROOT);
                List<SqlAst.Expr> read = LAZY_ARGUMENTS.contains(fn) ? f.args().subList(0, Math.min(1,
                        f.args().size())) : f.args();
                read.forEach(a -> eagerOperands(a, out));
                out.add(e);
            }
            default -> out.add(e);
        }
    }

    /** {@code vs}'s nodes written out, a node shared by several parents counted for each. */
    private static long written(ValueSpecification vs) {
        return written(vs, new java.util.IdentityHashMap<>());
    }

    private static long written(ValueSpecification vs, Map<ValueSpecification, Long> memo) {
        Long known = memo.get(vs);
        if (known != null) {
            return known;
        }
        List<ValueSpecification> children = switch (vs) {
            case AppliedFunction f -> f.parameters();
            case AppliedProperty p -> List.of(p.receiver());
            case com.legend.protocol.spec.LambdaFunction l -> l.body();
            case PureCollection c -> c.values();
            default -> List.of();
        };
        long n = 1;
        for (ValueSpecification c : children) {
            n = Math.min(Long.MAX_VALUE / 2, n + written(c, memo));
        }
        memo.put(vs, n);
        return n;
    }

    /** The functions that read their later arguments for some rows only. */
    private static final Set<String> LAZY_ARGUMENTS = Set.of("coalesce", "isnull", "ifnull", "nvl", "argn", "if",
            "iif", "choose", "decode", "nullif", "greatest", "least");

    /** A column shown as itself once; shown again, or under another name, it is a computed copy. */
    private Output passthrough(Col c, String name, Set<String> passed, Set<String> present, Scope scope) {
        if (name.equals(c.name()) && passed.add(c.name())) {
            return new Output(name, c.name(), null, null, c.kind(), c.nullable(), c.detail());
        }
        return new Output(name, internalFor(name, present), property(row, c.name()), null, c.kind(), c.nullable(),
                c.detail());
    }

    /** The correlation name of the FROM item of {@code scope}'s own query that {@code ref} reads
     *  ({@link InputDialect.Tables}): null when it reads none there, or when two items have its name. */
    private SqlAst.@com.legend.base.Nullable Ident tableOf(Scope scope, SqlAst.ColumnRef ref) {
        Scope s = scope;
        while (s instanceof WindowScope || s instanceof Grouped) {
            s = s instanceof WindowScope w ? w.base() : ((Grouped) s).input;
        }
        if (!(s instanceof Input in)) {
            return null;
        }
        List<SqlAst.Ident> parts = ref.parts();
        if (parts.size() == 2) {
            Binding b = in.binding(parts.get(0));
            return b == null ? null : b.qualifier();
        }
        if (parts.size() != 1) {
            return null;
        }
        Binding found = null;
        for (Binding b : in.bindings) {
            if (dialect.resolve(parts.get(0), b.names(true), "column") != null) {
                if (found != null) {
                    return null;
                }
                found = b;
            }
        }
        return found == null ? null : found.qualifier();
    }

    /** The names a relation's columns take, compared without regard to case (2026-10-16, measured on DuckDB): a
     *  database that reads names so (DuckDB, even quoted) binds two columns of one select whose names differ in case
     *  to one of them, the first; so a computed column takes a fresh name there, its own given back last. */
    private static Set<String> taken() {
        return new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    }

    /** The column a computed output is built in: its own name when that is free, else a fresh one (renamed last). */
    private String internalFor(String name, Set<String> present) {
        if (!present.contains(name) && !name.startsWith(SYNTHETIC)) {
            present.add(name);
            return name;
        }
        return fresh(present);
    }

    String fresh(Set<String> taken) {
        String name;
        do {
            name = SYNTHETIC + (++fresh);
        } while (taken.contains(name));
        taken.add(name);
        return name;
    }

    /** The computed outputs, the sort, the select list, the renames and the slice, in Postgres's order. */
    private Rel assemble(SqlAst.Select s, Scope scope, ValueSpecification relation, Set<String> present,
            List<Output> outputs) {
        Set<String> shown = new HashSet<>();
        for (int i = 0; i < outputs.size(); i++) {
            Output o = outputs.get(i);
            if (!shown.add(o.name())) {
                if (!dialect.namesUnread(s)) {
                    throw SqlTranslationException.unsupported("the output column name \"" + o.name() + "\" appears"
                            + " twice; Pure relations need distinct column names, so give one an alias", s.pos());
                }
                // IQ island, 2026-10-14 (the third landing): a name no one reads (an EXISTS subquery's, a set
                // operation's later branch's) goes by a name of its own (the callers' lists are their own)
                outputs.set(i, new Output(fresh(present), o.internal(), o.value(), o.expr(), o.kind(), o.nullable(),
                        o.detail(), o.pre()));
            }
        }
        // a DISTINCT's keys: the values themselves, or a dialect's key whose group shows one of its values (IQ island,
        // 2026-10-15), the least (a min, a [0..1] whatever the values)
        List<java.util.function.@com.legend.base.Nullable UnaryOperator<ValueSpecification>> distinctKeys =
                new ArrayList<>();
        if (s.distinct()) {
            for (int i = 0; i < outputs.size(); i++) {
                Output o = outputs.get(i);
                notJsonKey(o.kind(), "DISTINCT", s.pos());
                java.util.function.UnaryOperator<ValueSpecification> key = distinctKey("DISTINCT", Tx.value(property(
                        row, o.internal()), o.kind(), o.nullable(), o.detail()), o.expr(), s.pos());
                distinctKeys.add(key);
                if (key != null) {
                    outputs.set(i, new Output(o.name(), o.internal(), o.value(), o.expr(), o.kind(), true,
                            o.detail(), o.pre()));
                }
            }
        }
        List<ColSpec> computed = new ArrayList<>();
        Map<String, List<ColSpec>> preOf = new HashMap<>();
        for (Output o : outputs) {
            ValueSpecification v = o.value();
            if (v != null) {
                computed.add(new ColSpec(o.internal(), lambda(row, v), null, null));
                preOf.put(o.internal(), o.pre());
            }
        }
        // the sort keys: an ordinal or an output name means that output; anything else is computed for the sort (a
        // dialect's collation key of a DISTINCT's output after the DISTINCT, in afterDistinct)
        List<ValueSpecification> sortInfos = new ArrayList<>();
        List<ColSpec> afterDistinct = new ArrayList<>();
        Set<String> sorted = new HashSet<>();
        for (SqlAst.SortItem item : s.orderBy()) {
            if (dialect.ordersNothing(item.expr())) {
                continue;
            }
            String key = sortKey(item, s, scope, outputs, computed, present, afterDistinct);
            sorted.add(key);
            boolean nullsFirst = item.nullsFirst() != null ? java.util.Objects.requireNonNull(item.nullsFirst())
                    : dialect.nullsFirst(item.descending());
            sortInfos.add(call(item.descending() ? "descending" : "ascending", colSpec(key),
                    new EnumValue(NULL_ORDER, nullsFirst ? "FIRST" : "LAST")));
        }
        // IQ island, 2026-10-16 (the user's decision; measured on IQ 17.1, SQL Anywhere 17 and Postgres 16): under a
        // row limit, the items the ORDER BY does not read are computed for the rows the dialect says, after the sort
        // and the limit (an item that can raise raises for those rows only)
        InputDialect.RowLimitItems limited = s.distinct() || s.limit() == null && s.offset() == null
                ? InputDialect.RowLimitItems.EVERY_ROW : dialect.rowLimitItems(s);
        if (limited == InputDialect.RowLimitItems.ROWS_UP_TO_THE_LAST && s.limit() == null) {
            // no last row: every row is up to it
            limited = InputDialect.RowLimitItems.EVERY_ROW;
        }
        List<ColSpec> late = new ArrayList<>();
        if (limited != InputDialect.RowLimitItems.EVERY_ROW) {
            List<ColSpec> early = new ArrayList<>();
            for (ColSpec c : computed) {
                (sorted.contains(c.name()) ? early : late).add(c);
            }
            computed = early;
            if (late.isEmpty()) {
                // nothing to compute after the limit: the limit as written
                limited = InputDialect.RowLimitItems.EVERY_ROW;
            }
        }
        List<ColSpec> earlyPre = new ArrayList<>();
        computed.forEach(c -> earlyPre.addAll(preOf.getOrDefault(c.name(), List.of())));
        for (ColSpec c : earlyPre) {
            // one extend each: a column computed once may read one computed before it
            relation = call("extend", relation, new ColSpecArray(List.of(c)));
        }
        if (!computed.isEmpty()) {
            relation = call("extend", relation, new ColSpecArray(computed));
        }
        List<ColSpec> selection = new ArrayList<>();
        for (Output o : outputs) {
            selection.add(colSpec(o.internal()));
        }
        if (s.distinct()) {
            List<ColSpec> keys = new ArrayList<>();
            List<ColSpec> keyed = new ArrayList<>();
            List<ColSpec> members = new ArrayList<>();
            for (int i = 0; i < outputs.size(); i++) {
                Output o = outputs.get(i);
                java.util.function.UnaryOperator<ValueSpecification> key = distinctKeys.get(i);
                if (key == null) {
                    keys.add(colSpec(o.internal()));
                } else {
                    String k = fresh(present);
                    keyed.add(new ColSpec(k, lambda(row, key.apply(property(row, o.internal()))), null, null));
                    keys.add(colSpec(k));
                    members.add(new ColSpec(o.internal(), lambda(row, property(row, o.internal())),
                            lambda(group, call("min", new Variable(group))), null));
                }
            }
            relation = members.isEmpty() ? call("distinct", call("select", relation, new ColSpecArray(selection)))
                    : call("select", call("groupBy", call("extend", relation, new ColSpecArray(keyed)),
                            new ColSpecArray(keys), new ColSpecArray(members)), new ColSpecArray(selection));
            if (!afterDistinct.isEmpty()) {
                relation = call("extend", relation, new ColSpecArray(afterDistinct));
            }
            if (!sortInfos.isEmpty()) {
                relation = call("sort", relation, new PureCollection(sortInfos));
            }
            if (!afterDistinct.isEmpty()) {
                relation = call("select", relation, new ColSpecArray(selection));
            }
        } else {
            if (!sortInfos.isEmpty()) {
                relation = call("sort", relation, new PureCollection(sortInfos));
            }
            if (limited != InputDialect.RowLimitItems.EVERY_ROW) {
                relation = rowsComputed(relation, s, limited);
                List<ColSpec> latePre = new ArrayList<>();
                late.forEach(c -> latePre.addAll(preOf.getOrDefault(c.name(), List.of())));
                for (ColSpec c : latePre) {
                    relation = call("extend", relation, new ColSpecArray(List.of(c)));
                }
                if (!late.isEmpty()) {
                    relation = call("extend", relation, new ColSpecArray(late));
                }
            }
            relation = call("select", relation, new ColSpecArray(selection));
        }
        for (Output o : outputs) {
            if (!o.internal().equals(o.name())) {
                relation = call("rename", relation, colSpec(o.internal()), colSpec(o.name()));
            }
        }
        SqlAst.Expr offset = s.offset();
        if (limited == InputDialect.RowLimitItems.EVERY_ROW) {
            if (offset != null) {
                relation = call("drop", relation, dialect.rowCount(offset, "OFFSET"));
            }
            SqlAst.Expr limit = s.limit();
            if (limit != null) {
                relation = call("limit", relation, dialect.rowCount(limit, "LIMIT"));
            }
        } else if (limited == InputDialect.RowLimitItems.ROWS_UP_TO_THE_LAST && offset != null) {
            relation = call("drop", relation, dialect.rowCount(offset, "OFFSET"));
        }
        List<OutCol> columns = new ArrayList<>();
        for (Output o : outputs) {
            columns.add(new OutCol(o.name(), o.kind(), o.nullable(), o.detail()));
        }
        return new Rel(relation, columns);
    }

    /** {@code relation}, sorted, cut to the rows a row limit computes its later items for
     *  ({@link InputDialect.RowLimitItems}): the rows it returns, or the rows up to its last (the caller drops its
     *  offset's after the items). */
    private ValueSpecification rowsComputed(ValueSpecification relation, SqlAst.Select s,
            InputDialect.RowLimitItems rows) {
        SqlAst.Expr offset = s.offset();
        SqlAst.Expr limit = s.limit();
        if (rows == InputDialect.RowLimitItems.ROWS_RETURNED) {
            if (offset != null) {
                relation = call("drop", relation, dialect.rowCount(offset, "OFFSET"));
            }
            return limit == null ? relation : call("limit", relation, dialect.rowCount(limit, "LIMIT"));
        }
        ValueSpecification count = dialect.rowCount(java.util.Objects.requireNonNull(limit), "LIMIT");
        return call("limit", relation, offset == null ? count
                : AppliedFunction.infixRun("plus", List.of(dialect.rowCount(offset, "OFFSET"), count)));
    }

    private String sortKey(SqlAst.SortItem item, SqlAst.Select s, Scope scope, List<Output> outputs,
            List<ColSpec> computed, Set<String> present, List<ColSpec> afterDistinct) {
        SqlAst.Expr e = item.expr();
        if (e instanceof SqlAst.IntegerLit n) {
            // a position counts the select list as expanded: SELECT * ... ORDER BY 2 is the second column
            if (n.value().signum() <= 0 || n.value().compareTo(java.math.BigInteger.valueOf(outputs.size())) > 0) {
                throw SqlTranslationException.invalid("ORDER BY position " + n.value() + " is not in select list",
                        n.pos());
            }
            notJsonKey(outputs.get(n.value().intValue() - 1).kind(), "ORDER BY", n.pos());
            return sortedBy(outputs.get(n.value().intValue() - 1), s, n.pos(), computed, present, afterDistinct);
        }
        if (e instanceof SqlAst.ColumnRef ref && ref.parts().size() == 1) {
            String found = dialect.resolve(ref.parts().get(0), outputs.stream().map(Output::name).toList(),
                    "column");
            // a DISTINCT's ORDER BY reads its outputs whatever the dialect (its keys are the outputs)
            if (found != null && (s.distinct() || dialect.ordersByOutput(s, found))) {
                if (!dialect.ordersByOutput(s, found)) {
                    // a name the dialect reads as no output is a FROM column first: an ambiguous one is its error
                    // there too (IQ island, 2026-10-16, measured: IQ's -144 under DISTINCT)
                    (scope instanceof Grouped g ? g.input : scope).column(ref);
                }
                for (Output o : outputs) {
                    if (o.name().equals(found)) {
                        notJsonKey(o.kind(), "ORDER BY", e.pos());
                        return sortedBy(o, s, e.pos(), computed, present, afterDistinct);
                    }
                }
            }
        }
        if (s.distinct()) {
            Scope inputs = scope instanceof Grouped g ? g.input : scope;
            String canon = canon(e, inputScope(inputs));
            for (Output o : outputs) {
                SqlAst.Expr oe = o.expr();
                if (oe != null && canon(oe, inputScope(inputs)).equals(canon)) {
                    notJsonKey(o.kind(), "ORDER BY", e.pos());
                    return sortedBy(o, s, e.pos(), computed, present, afterDistinct);
                }
            }
            if (e instanceof SqlAst.ColumnRef ref) {
                // IQ island, 2026-10-17 (found by verifying the corpus lane's IQ_ERROR claims: IQ 17.1 answers SELECT
                // DISTINCT u.* ... ORDER BY u.displayname): a column a * shows is one of the items, as Postgres reads it
                Col source = inputs.columnOrNull(ref);
                if (source != null && source.var() == null) {
                    for (Output o : outputs) {
                        if (o.expr() == null && (o.value() == null ? o.internal().equals(source.name())
                                : o.value() instanceof AppliedProperty p && p.receiver() instanceof Variable v
                                && v.name().equals(row) && p.property().equals(source.name()))) {
                            notJsonKey(o.kind(), "ORDER BY", e.pos());
                            return sortedBy(o, s, e.pos(), computed, present, afterDistinct);
                        }
                    }
                }
            }
            throw SqlTranslationException.invalid(dialect.message(Problem.DISTINCT_ORDER_KEY), e.pos());
        }
        Col g = scope.grouped(e);
        if (g != null) {
            notJsonKey(g.kind(), "ORDER BY", e.pos());
            return sortedBy(g.name(), Tx.value(property(row, g.name()), g.kind(), g.nullable(), g.detail()), e,
                    e.pos(), computed, present);
        }
        if (e instanceof SqlAst.ColumnRef ref) {
            Col c = scope.column(ref);
            notJsonKey(c.kind(), "ORDER BY", e.pos());
            java.util.function.UnaryOperator<ValueSpecification> collation = dialect.key("ORDER BY",
                    Tx.value(property(row, c.name()), c.kind(), c.nullable(), c.detail()), e, e.pos());
            if (collation == null) {
                return c.name();
            }
            String name = fresh(present);
            computed.add(new ColSpec(name, lambda(row, collation.apply(valueOf(tx(e, scope, row)))), null, null));
            return name;
        }
        Tx t = tx(e, scope, row);
        notJsonKey(t.kind(), "ORDER BY", e.pos());
        java.util.function.UnaryOperator<ValueSpecification> collation = dialect.key("ORDER BY", t, e, e.pos());
        String name = fresh(present);
        computed.add(new ColSpec(name, lambda(row, collation == null ? valueOf(t) : collation.apply(valueOf(t))), null,
                null));
        return name;
    }

    /** The column a sort by output {@code o} sorts by: its own, unless the dialect sorts its values by a key of its
     *  own (a collation's): then a column computed with that key -- before the select list from the output's value,
     *  or after a DISTINCT from its column ({@code afterDistinct}). */
    private String sortedBy(Output o, SqlAst.Select s, SqlAst.Pos pos, List<ColSpec> computed, Set<String> present,
            List<ColSpec> afterDistinct) {
        ValueSpecification column = property(row, o.internal());
        ValueSpecification v = o.value();
        java.util.function.UnaryOperator<ValueSpecification> collation = dialect.key("ORDER BY",
                Tx.value(column, o.kind(), o.nullable(), o.detail()), o.expr(), pos);
        if (collation == null) {
            return o.internal();
        }
        String name = fresh(present);
        if (s.distinct()) {
            afterDistinct.add(new ColSpec(name, lambda(row, collation.apply(column)), null, null));
        } else {
            computed.add(new ColSpec(name, lambda(row, collation.apply(v != null ? v : column)), null, null));
        }
        return name;
    }

    /** The column a sort by {@code column}, holding {@code value}, sorts by: itself, unless the dialect sorts the
     *  values by a key of its own (a collation's): then a column computed with that key, into {@code keys}. */
    private String sortedBy(String column, Tx value, SqlAst.@com.legend.base.Nullable Expr expr, SqlAst.Pos pos,
            List<ColSpec> keys, Set<String> taken) {
        java.util.function.UnaryOperator<ValueSpecification> collation = dialect.key("ORDER BY", value, expr, pos);
        if (collation == null) {
            return column;
        }
        String name = fresh(taken);
        keys.add(new ColSpec(name, lambda(row, collation.apply(valueOf(value))), null, null));
        return name;
    }

    /** A DISTINCT's (a set operation's) key: nothing (the values themselves tell rows apart), the dialect's key of a
     *  value whose group shows one of its values ({@link InputDialect#representative}; IQ island, 2026-10-15), or the
     *  dialect's refusal; a dialect whose key's group would show a value it chooses otherwise is refused here. */
    private java.util.function.@com.legend.base.Nullable UnaryOperator<ValueSpecification> distinctKey(String clause,
            Tx value, SqlAst.@com.legend.base.Nullable Expr expr, SqlAst.Pos pos) {
        java.util.function.UnaryOperator<ValueSpecification> key = dialect.key(clause, value, expr, pos);
        if (key != null && !dialect.representative(clause, value, expr)) {
            throw SqlTranslationException.unsupported(clause + " by a key other than the values themselves is not"
                    + " supported", pos);
        }
        return key;
    }

    private static Input inputScope(Scope scope) {
        if (scope instanceof Input i) {
            return i;
        }
        throw new IllegalStateException("not an input scope");
    }

    /** An integer constant as the Pure parser spells one: a long when it fits, else the big integer. */
    public static CInteger integer(java.math.BigInteger value) {
        return value.bitLength() < Long.SIZE ? new CInteger(value.longValue()) : new CInteger(value);
    }

    static CInteger count(SqlAst.Expr e, String clause) {
        if (e instanceof SqlAst.IntegerLit n && n.value().signum() >= 0) {
            return integer(n.value());
        }
        throw SqlTranslationException.unsupported(clause + " takes a non-negative integer constant here", e.pos());
    }

    // ------------------------------------------------------------------ aggregates

    static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max", "stddev", "stddev_samp",
            "stddev_pop", "variance", "var_samp", "var_pop", "string_agg", "bool_and", "bool_or", "every",
            "percentile_cont", "percentile_disc", "corr", "covar_pop", "covar_samp", "regr_count", "regr_avgx",
            "regr_avgy", "regr_sxx", "regr_syy", "regr_sxy", "regr_slope", "regr_intercept", "regr_r2");

    /** Postgres's aggregates of pairs and Pure's (over rowMapper pairs): double precision, a pair with a NULL
     *  skipped. */
    static final Map<String, String> BIVARIATE = Map.of("corr", "corr", "covar_pop", "covarPopulation",
            "covar_samp", "covarSample");

    /** Postgres's boolean aggregates: Pure's and() (bool_and, every) or or() (bool_or) of the group. */
    static final Set<String> BOOLEAN_AGGREGATES = Set.of("bool_and", "bool_or", "every");

    /** Postgres's statistical aggregates and Pure's: sample (n - 1) unless named population. */
    static final Map<String, String> STATISTICS = Map.of("stddev", "stdDevSample", "stddev_samp",
            "stdDevSample", "stddev_pop", "stdDevPopulation", "variance", "varianceSample", "var_samp",
            "varianceSample", "var_pop", "variancePopulation");

    /** Postgres's window functions that are not aggregates: they need an OVER clause. */
    private static final Set<String> WINDOW_FUNCTIONS = Set.of("row_number", "rank", "dense_rank", "percent_rank",
            "cume_dist", "ntile", "lag", "lead", "first_value", "last_value", "nth_value");

    /** A sum's kind: of integers a bigint or a numeric (WHOLE), of a sum of integers or of numerics a numeric, of
     *  doubles a double. */
    static Kind sumKind(Kind arg) {
        return arg == Kind.INTEGER ? Kind.WHOLE : arg == Kind.DECIMAL || arg == Kind.WHOLE ? Kind.NUMBER : arg;
    }

    /** An average's or a statistic's kind: double precision of doubles; Postgres's numeric otherwise, a Float here. */
    static Kind statisticKind(Kind arg) {
        return arg == Kind.FLOAT ? Kind.FLOAT : Kind.INEXACT;
    }

    private boolean isAggregate(SqlAst.FunctionCall f) {
        return f.name().size() == 1 && dialect.isAggregate(functionName(f.name().get(0)));
    }

    private boolean hasAggregate(SqlAst.Select s) {
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && containsAggregate(x.expr())) {
                return true;
            }
        }
        SqlAst.Expr having = s.having();
        if (having != null && containsAggregate(having)) {
            return true;
        }
        for (SqlAst.SortItem o : s.orderBy()) {
            if (containsAggregate(o.expr())) {
                return true;
            }
        }
        return false;
    }

    boolean containsAggregate(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && isAggregate(f)) {
            return true;
        }
        for (SqlAst.Expr child : children(e)) {
            if (containsAggregate(child)) {
                return true;
            }
        }
        return false;
    }

    private void noAggregate(SqlAst.Expr e, String clause) {
        if (e instanceof SqlAst.FunctionCall f && isAggregate(f)) {
            throw SqlTranslationException.invalid("aggregate functions are not allowed in " + clause, e.pos());
        }
        for (SqlAst.Expr child : children(e)) {
            noAggregate(child, clause);
        }
    }

    /** Every aggregate call of {@code e}, once each: a column of the grouped relation with its map and reduce. */
    private void collectAggregates(SqlAst.Expr e, Input in, Map<String, Col> byCanon, List<ColSpec> aggregates,
            List<ColSpec> finishes, Set<String> names, Map<String, String> preferred) {
        if (e instanceof SqlAst.FunctionCall f && isAggregate(f)) {
            for (SqlAst.Expr arg : f.args()) {
                if (containsAggregate(arg)) {
                    throw SqlTranslationException.invalid(dialect.message(Problem.NESTED_AGGREGATE), arg.pos());
                }
            }
            String canon = canon(f, in);
            if (byCanon.containsKey(canon)) {
                return;
            }
            String want = preferred.get(canon);
            String name = want != null && !names.contains(want) && !want.startsWith(SYNTHETIC) ? want : fresh(names);
            // the dialect's aggregate: its columns, and the one computed from them after the grouping
            Aggregation own = dialect.aggregate(f, new Grouping(in, names, name, byCanon));
            names.add(name);
            byCanon.put(canon, Col.of(name, own.kind(), own.nullable(), own.detail()));
            aggregates.addAll(own.specs());
            if (own.finish() != null) {
                finishes.add(own.finish());
            }
            if (own.guard() != null) {
                guards.add(own.guard());
            }
            return;
        }
        for (SqlAst.Expr child : children(e)) {
            collectAggregates(child, in, byCanon, aggregates, finishes, names, preferred);
        }
    }

    /** Refuses an aggregate of only an enclosing query's columns, in any clause: Postgres makes it an aggregate of
     *  that query, over its rows. */
    private void noOuterAggregates(SqlAst.Select s, Input in) {
        List<SqlAst.Expr> all = new ArrayList<>(s.groupBy());
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                all.add(x.expr());
            }
        }
        s.orderBy().forEach(o -> all.add(o.expr()));
        SqlAst.Expr where = s.where();
        if (where != null) {
            all.add(where);
        }
        SqlAst.Expr having = s.having();
        if (having != null) {
            all.add(having);
        }
        for (SqlAst.Expr e : all) {
            outerAggregate(e, in);
        }
    }

    private void outerAggregate(SqlAst.Expr e, Input in) {
        if (e instanceof SqlAst.FunctionCall f && isAggregate(f) && !f.star() && !f.args().isEmpty()
                && readsOnlyOuter(f.args().get(0), in)) {
            throw SqlTranslationException.unsupported("an aggregate of only an enclosing query's columns is not"
                    + " supported yet", f.pos());
        }
        for (SqlAst.Expr child : children(e)) {
            outerAggregate(child, in);
        }
    }

    /** True iff {@code e} reads columns and every one is an enclosing query's. */
    private static boolean readsOnlyOuter(SqlAst.Expr e, Input in) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columnRefs(e, refs);
        for (SqlAst.ColumnRef ref : refs) {
            if (in.find(ref) != null) {
                return false;
            }
        }
        return !refs.isEmpty();
    }

    private static void columnRefs(SqlAst.Expr e, List<SqlAst.ColumnRef> out) {
        if (e instanceof SqlAst.ColumnRef ref) {
            out.add(ref);
        }
        for (SqlAst.Expr child : children(e)) {
            columnRefs(child, out);
        }
    }

    // ------------------------------------------------------------------ expressions

    /** A WHERE, HAVING or WHEN condition: the rows where it is TRUE (NULL and FALSE alike are not TRUE). */
    ValueSpecification condition(SqlAst.Expr e, Scope scope, String clause) {
        return truth(tx(e, scope, row), clause, e.pos()).vs();
    }

    /** A boolean operand ({@code clause}'s). A NULL constant is Postgres's unknown truth value: neither true nor
     *  false. */
    public static Tx truth(Tx t, String clause, SqlAst.Pos pos) {
        if (t.kind() == Kind.NULL) {
            return Tx.condition(new CBoolean(false), new CBoolean(false), true);
        }
        if (t.kind() != Kind.BOOLEAN) {
            throw SqlTranslationException.invalid("argument of " + clause + " must be type boolean", pos);
        }
        return t;
    }

    /** A column's value; a boolean column is a condition (TRUE when it holds true, FALSE when it holds false). Either
     *  carries what the dialect knows of the column's type ({@link InputDialect#read} reads it). */
    private static Tx columnValue(String row, Col c) {
        String var = c.var();
        ValueSpecification v = property(var != null ? var : row, c.name());
        if (c.kind() == Kind.BOOLEAN) {
            Tx b = booleanValue(v, c.nullable());
            return c.detail() == null ? b : new Tx(b.vs(), b.kind(), b.nullable(), b.whenFalse(), c.detail());
        }
        return Tx.value(v, c.kind(), c.nullable(), c.detail());
    }

    /** A boolean VALUE (a column, a CASE, a coalesce) as a condition. A nullable one's tests say "known and true",
     *  "known and false" outright: {@code $v == true} would be compared in SQL, NULL for a NULL, where the test must
     *  be false (Postgres: NULL IS TRUE is false). */
    public static Tx booleanValue(ValueSpecification v, boolean nullable) {
        if (!nullable) {
            return Tx.condition(v, call("not", v), false);
        }
        ValueSpecification known = call("isNotEmpty", v);
        return Tx.condition(call("and", known, call("toOne", v)), call("and", known, call("not", call("toOne", v))),
                true);
    }

    /** An expression's Pure value: a condition becomes true, false, or (when neither holds) no value. */
    public static ValueSpecification valueOf(Tx t) {
        if (t.kind() != Kind.BOOLEAN || !t.nullable()) {
            return t.vs();
        }
        return call("if", t.vs(), thunk(new CBoolean(true)),
                thunk(call("if", t.falseWhen(), thunk(new CBoolean(false)), thunk(empty()))));
    }

    /** Postgres's strict functions and operators: NULL when any argument is NULL, else {@code build} of the values. */
    public static Tx strict(Kind kind, List<Tx> args, java.util.function.Function<List<ValueSpecification>,
            ValueSpecification> build) {
        return strict(kind, args, mergesGuards(args), build);
    }

    /**
     * {@link #strict}, merging every strict operand's NULL guard into this one's where {@code merge} (IQ island,
     * 2026-10-15): for an input dialect whose operands' bodies cannot raise where another operand is NULL -- there
     * the merge loses no error, and a chain of operations stays linear in size rather than repeating each operand
     * at each level ({@link #mergesGuards} is the safe choice where an operand may raise).
     */
    public static Tx strict(Kind kind, List<Tx> args, boolean merge,
            java.util.function.Function<List<ValueSpecification>, ValueSpecification> build) {
        for (Tx a : args) {
            if (isNullConstant(a)) {
                // a NULL constant makes a strict operator or function NULL, of the type Postgres resolves it by
                return nullOf(kind);
            }
        }
        List<ValueSpecification> values = new ArrayList<>();
        ValueSpecification anyNull = null;
        for (Tx a : args) {
            if (merge && a.guard() != null && a.body() != null) {
                // a strict operand: NULL exactly when its guard holds, else its body
                anyNull = anyNull == null ? a.guard() : call("or", anyNull, a.guard());
                values.add(a.body());
                continue;
            }
            ValueSpecification v = valueOf(a);
            if (a.nullable()) {
                anyNull = anyNull == null ? call("isEmpty", v) : call("or", anyNull, call("isEmpty", v));
                values.add(call("toOne", v));
            } else {
                values.add(v);
            }
        }
        ValueSpecification result = build.apply(values);
        if (anyNull == null) {
            return Tx.value(result, kind, false);
        }
        return Tx.guarded(anyNull, result, kind);
    }

    /** Whether a strict operation merges a strict operand's guard into its own ({@link Tx#guarded}): only when that
     *  operand is the one that may be NULL. With another that is NULL, Postgres still computes this one, and raises
     *  its error (an integer out of range, a division by zero): its whole value is tested, as before the merge. */
    private static boolean mergesGuards(List<Tx> operands) {
        return operands.stream().filter(Tx::nullable).count() == 1;
    }

    /** A strict predicate (a comparison, IN, BETWEEN, LIKE): NULL when an operand is NULL, else {@code test}. */
    public static Tx strictTest(List<Tx> operands, java.util.function.Function<List<ValueSpecification>,
            ValueSpecification> test) {
        for (Tx a : operands) {
            if (isNullConstant(a)) {
                return nullOf(Kind.BOOLEAN);
            }
        }
        List<ValueSpecification> values = new ArrayList<>();
        ValueSpecification known = null;
        boolean merge = mergesGuards(operands);
        for (Tx a : operands) {
            if (merge && a.guard() != null && a.body() != null) {
                // a strict operand: known exactly when its guard does not hold, and then its body
                ValueSpecification k = call("not", a.guard());
                known = known == null ? k : call("and", known, k);
                values.add(a.body());
                continue;
            }
            ValueSpecification v = valueOf(a);
            if (a.nullable()) {
                known = known == null ? call("isNotEmpty", v) : call("and", known, call("isNotEmpty", v));
                values.add(call("toOne", v));
            } else {
                values.add(v);
            }
        }
        ValueSpecification t = test.apply(values);
        if (known == null) {
            return Tx.condition(t, call("not", t), false);
        }
        return Tx.condition(call("and", known, t), call("and", known, call("not", t)), true);
    }

    /** A NULL of a kind, as Postgres types a NULL constant by its context: Pure's empty collection cast to the kind's
     *  type (SQL's typed NULL); for a boolean, a condition that is neither TRUE nor FALSE. A NULL constant in no
     *  context of a type is text (Postgres's unknown resolves to text). */
    public static Tx nullOf(Kind kind) {
        if (kind == Kind.BOOLEAN) {
            return Tx.condition(new CBoolean(false), new CBoolean(false), true);
        }
        String type = switch (kind) {
            case INTEGER, WHOLE -> "Integer";
            case FLOAT, INEXACT -> "Float";
            case DECIMAL, NUMBER -> "Decimal";
            case STRING, NULL -> "String";
            case DATE -> "StrictDate";
            case DATETIME -> "DateTime";
            case TIME -> "StrictTime";
            case ZONED, ZONED_TIME -> "DateTime";
            case JSON -> VARIANT;
            case BOOLEAN, OTHER, DURATION -> null;
        };
        if (type == null) {
            return Tx.value(empty(), kind, true);
        }
        return Tx.value(call("cast", empty(), typeArg(type)), kind == Kind.NULL ? Kind.STRING : kind, true);
    }

    /** Whether a value is a NULL constant: untyped ({@code NULL}) or typed ({@link #nullOf}). */
    public static boolean isNullConstant(Tx t) {
        return t.kind() == Kind.NULL || t.whenFalse() == null && t.vs().equals(nullOf(t.kind()).vs());
    }

    public static Tx not(Tx t) {
        return Tx.condition(t.falseWhen(), t.vs(), t.nullable());
    }

    Tx tx(SqlAst.Expr e, Scope scope, String row) {
        Col bound = hoisted.get(e);
        if (bound != null) {
            // IQ island, 2026-10-17: an operand the item computes once, a column before it (output)
            return columnValue(row, bound);
        }
        Col g = scope.grouped(e);
        if (g != null) {
            return columnValue(row, g);
        }
        return switch (e) {
            case SqlAst.ColumnRef ref -> columnValue(row, scope.column(ref));
            case SqlAst.IntegerLit n -> dialect.literal(n);
            case SqlAst.NumericLit n -> dialect.literal(n);
            case SqlAst.StringLit str -> dialect.literal(str);
            case SqlAst.BooleanLit b -> dialect.literal(b);
            case SqlAst.NullLit n -> dialect.literal(n);
            case SqlAst.TypedLit lit -> dialect.literal(lit);
            // IQ island, 2026-10-14 (M1e): the dialect's (Postgres's: the refusal it was)
            case SqlAst.Param p -> dialect.parameter(p, new At(scope, row), sources);
            case SqlAst.Unary u -> dialect.unary(u, new At(scope, row));
            case SqlAst.Binary b -> dialect.binary(b, new At(scope, row));
            case SqlAst.Subscript sub -> {
                Tx t = dialect.subscript(sub, new At(scope, row));
                if (t == null) {
                    throw SqlTranslationException.unsupported("subscripts are not supported", sub.pos());
                }
                yield t;
            }
            case SqlAst.ArrayConstructor a -> throw SqlTranslationException.unsupported("arrays other than unnest's"
                    + " argument are not supported yet", a.pos());
            case SqlAst.Operator o -> {
                Tx t = dialect.operator(o, new At(scope, row));
                if (t == null) {
                    throw SqlTranslationException.unsupported("the operator " + o.operator() + " is not supported yet",
                            o.pos());
                }
                yield t;
            }
            case SqlAst.IsNull n -> {
                ValueSpecification v = valueOf(tx(n.operand(), scope, row));
                Tx isNull = Tx.condition(call("isEmpty", v), call("isNotEmpty", v), false);
                yield n.negated() ? not(isNull) : isNull;
            }
            case SqlAst.IsTruth t -> isTruth(t, scope, row);
            case SqlAst.IsDistinct d -> dialect.isDistinct(d, new At(scope, row));
            case SqlAst.InList in -> dialect.inList(in, new At(scope, row));
            case SqlAst.Between b -> dialect.between(b, new At(scope, row));
            case SqlAst.Like l -> dialect.like(l, new At(scope, row));
            case SqlAst.Case c -> caseExpr(c, scope, row);
            case SqlAst.FunctionCall f -> function(f, scope, row);
            case SqlAst.Cast c -> dialect.cast(c, new At(scope, row));
            case SqlAst.ScalarSubquery q -> scalarSubquery(q, scope);
            case SqlAst.Exists q -> {
                // never NULL: a row or none
                ValueSpecification found = call("exists", subquery(q.query(), scope).vs(),
                        lambda(sub, new CBoolean(true)));
                yield Tx.condition(found, call("not", found), false);
            }
            case SqlAst.InSubquery q -> {
                // x IN (query) is x = ANY (query); NOT IN its negation (NULL stays NULL)
                Tx any = quantified(q.operand(), SqlAst.BinaryOp.EQ, false, q.query(), scope, row, q.pos());
                yield q.negated() ? not(any) : any;
            }
            case SqlAst.Quantified q -> quantified(q.left(), q.op(), q.all(), q.query(), scope, row, q.pos());
            case SqlAst.Window w -> {
                // the select list's and ORDER BY's are columns by now (windows); any other place is Postgres's error
                String clause = scope.noAggregatesIn();
                throw SqlTranslationException.invalid(clause != null && !clause.equals("the select list")
                        ? "window functions are not allowed in " + clause
                        : "window functions are not allowed here", w.pos());
            }
        };
    }

    /** The Kleene AND (or OR) of {@code ts[from, to)}, grouped as a balanced tree: AND is TRUE when every operand
     *  is, FALSE when any is; OR the other way round. */
    private static Tx kleene(boolean and, List<Tx> ts, int from, int to) {
        if (to - from == 1) {
            return ts.get(from);
        }
        int mid = from + (to - from + 1) / 2;
        Tx l = kleene(and, ts, from, mid);
        Tx r = kleene(and, ts, mid, to);
        boolean nullable = l.nullable() || r.nullable();
        return and
                ? Tx.condition(call("and", l.vs(), r.vs()), call("or", l.falseWhen(), r.falseWhen()), nullable)
                : Tx.condition(call("or", l.vs(), r.vs()), call("and", l.falseWhen(), r.falseWhen()), nullable);
    }

    /** {@code b}, an AND or an OR, as Kleene's ({@link At#logical}). */
    Tx logical(SqlAst.Binary b, Scope scope, String row) {
        // a chain of one of them is read as the list of its operands, left to right, each under the position
        // of the operator that holds it: no recursion per operator (the stress corpus's 500-term OR overflowed
        // a default thread stack here), and the conditions are combined as a balanced tree, of depth log n for
        // the typer and the lowering (Kleene's AND and OR are associative, so the grouping changes nothing)
        List<Tx> operands = new ArrayList<>();
        java.util.ArrayDeque<SqlAst.Expr> pending = new java.util.ArrayDeque<>();
        java.util.ArrayDeque<SqlAst.Pos> holders = new java.util.ArrayDeque<>();
        pending.push(b);
        holders.push(b.pos());
        while (!pending.isEmpty()) {
            SqlAst.Expr e = pending.pop();
            SqlAst.Pos holder = holders.pop();
            if (e instanceof SqlAst.Binary x && x.op() == b.op()) {
                pending.push(x.right());
                holders.push(x.pos());
                pending.push(x.left());
                holders.push(x.pos());
            } else {
                operands.add(truth(tx(e, scope, row), b.op().name(), holder));
            }
        }
        return kleene(b.op() == SqlAst.BinaryOp.AND, operands, 0, operands.size());
    }

    /** {@code l op r} for a comparison {@code op}: NULL when either side is (Postgres's comparisons are strict). */
    public static Tx compare(SqlAst.BinaryOp op, Tx l, Tx r) {
        String fn = switch (op) {
            case EQ, NE -> "equal";
            case LT -> "lessThan";
            case LE -> "lessThanEqual";
            case GT -> "greaterThan";
            default -> "greaterThanEqual";
        };
        Tx test = strictTest(List.of(l, r), v -> call(fn, v.get(0), v.get(1)));
        return op == SqlAst.BinaryOp.NE ? not(test) : test;
    }

    /** The two sides of a comparison: a string constant compared with a date takes the date's type (Postgres reads
     *  an untyped constant by its context), in ISO 8601 spelling. */
    List<Tx> comparable(SqlAst.Expr left, SqlAst.Expr right, Scope scope, String row, SqlAst.Pos pos) {
        if (left instanceof SqlAst.NullLit || right instanceof SqlAst.NullLit) {
            // NULL compared with anything is NULL in Postgres, never TRUE or FALSE: strictTest reads it so
            Tx l = left instanceof SqlAst.NullLit ? Tx.value(empty(), Kind.NULL, true) : tx(left, scope, row);
            Tx r = right instanceof SqlAst.NullLit ? Tx.value(empty(), Kind.NULL, true) : tx(right, scope, row);
            return List.of(l, r);
        }
        return comparableValues(left, right, scope, row, pos);
    }

    List<Tx> comparableValues(SqlAst.Expr left, SqlAst.Expr right, Scope scope, String row, SqlAst.Pos pos) {
        Tx l = tx(left, scope, row);
        Tx r = tx(right, scope, row);
        if (left instanceof SqlAst.StringLit s && readsStrings(r.kind())) {
            l = dialect.readString(single(s, r.detail()), r.kind());
        } else if (right instanceof SqlAst.StringLit s && readsStrings(l.kind())) {
            r = dialect.readString(single(s, l.detail()), l.kind());
        }
        comparableKinds(l, r, pos);
        return List.of(l, r);
    }

    /** {@code s}, a string read as the type of a value of {@code detail}: refused where that is a real (Postgres
     *  reads the string as a single-precision float, which Pure's Float is not: '1004.3' = a real 1004.3 is true
     *  there; 2026-10-17, the regression suite): a comparison, IN, BETWEEN, ANY and ALL, lag's and lead's default,
     *  and (through {@link #stringWithAReal}) CASE, COALESCE and the set operations. */
    SqlAst.StringLit single(SqlAst.StringLit s, InputDialect.@com.legend.base.Nullable TypeDetail detail) {
        if (dialect.singlePrecision(detail)) {
            throw stringWithAReal(s.pos());
        }
        return s;
    }

    /** The refusal of a string read as a real's type. */
    static SqlTranslationException stringWithAReal(SqlAst.Pos pos) {
        return SqlTranslationException.unsupported("a string read with a real (float4) value is not supported yet:"
                + " Postgres reads it as a real", pos);
    }

    static void comparableKinds(Tx l, Tx r, SqlAst.Pos pos) {
        notJsonKey(l.kind(), "a comparison", pos);
        notJsonKey(r.kind(), "a comparison", pos);
        boolean ok = l.kind() == r.kind() || l.kind().numeric() && r.kind().numeric()
                || isDate(l.kind()) && isDate(r.kind()) || l.kind() == Kind.OTHER || r.kind() == Kind.OTHER;
        if (!ok) {
            throw SqlTranslationException.invalid("cannot compare " + l.kind().name().toLowerCase(Locale.ROOT)
                    + " with " + r.kind().name().toLowerCase(Locale.ROOT), pos);
        }
    }

    static boolean isDate(Kind k) {
        return k == Kind.DATE || k == Kind.DATETIME;
    }

    /** Values resolved to one type, that type's kind, and what the dialect knows of the type beyond its kind (none in
     *  Postgres's). */
    public record Common(List<Tx> values, Kind kind, InputDialect.@com.legend.base.Nullable TypeDetail detail) {

        /** Values resolved to one type of no {@link InputDialect.TypeDetail}. */
        public Common(List<Tx> values, Kind kind) {
            this(values, kind, null);
        }
    }

    /** The kinds an untyped string constant compared with them is read as (Postgres: an "unknown" constant takes the
     *  other operand's type, read by that type's input rules). */
    static boolean readsStrings(Kind k) {
        return isDate(k) || k == Kind.INTEGER || k == Kind.DECIMAL || k == Kind.FLOAT || k == Kind.INEXACT
                || k == Kind.BOOLEAN;
    }

    // ------------------------------------------------------------------ subqueries in expressions

    /** A subquery in an expression: a query one level down, which sees {@code scope}'s columns beyond its own FROM
     *  (a correlated reference reads them from this level's row). */
    private Rel subquery(SqlAst.Statement query, Scope scope) {
        return new SqlToPure(sources, dialect, new Env(env.ctes(), scope, row, env.level() + 1)).statement(query);
    }

    /** {@code (query)} as a value: its one column in its one row, NULL when it has no row. The subquery stays a
     *  subquery in SQL, so more than one row is the database's error, as it is Postgres's ({@code toOne} spells the
     *  value's type; SQL answers NULL for no row, so the value is nullable). */
    private Tx scalarSubquery(SqlAst.ScalarSubquery q, Scope scope) {
        Rel rel = subquery(q.query(), scope);
        if (rel.columns().size() != 1) {
            throw SqlTranslationException.invalid("subquery must return only one column", q.pos());
        }
        OutCol c = rel.columns().get(0);
        ValueSpecification v = call("toOne", call("map", rel.vs(), lambda(sub, property(sub, c.name()))));
        return c.kind() == Kind.BOOLEAN ? booleanValue(v, true) : Tx.value(v, c.kind(), true, c.detail());
    }

    /**
     * {@code x op ANY | ALL (query)} in three-valued logic, each subquery row's comparison strict: ANY is TRUE when
     * some row's comparison is TRUE and FALSE when every row's is FALSE (so FALSE over no rows); ALL is TRUE when every
     * row's is TRUE (so TRUE over no rows) and FALSE when some row's is FALSE; otherwise each is NULL.
     */
    private Tx quantified(SqlAst.Expr left, SqlAst.BinaryOp op, boolean all, SqlAst.Statement query,
            Scope scope, String row, SqlAst.Pos pos) {
        if (left instanceof SqlAst.NullLit) {
            throw SqlTranslationException.unsupported("a NULL constant compared with a subquery's rows is not supported"
                    + " yet", pos);
        }
        Rel rel = subquery(query, scope);
        if (rel.columns().size() != 1) {
            throw SqlTranslationException.invalid("subquery has too " + (rel.columns().size() > 1 ? "many" : "few")
                    + " columns", pos);
        }
        OutCol c = rel.columns().get(0);
        Tx each = columnValue(sub, Col.of(c.name(), c.kind(), c.nullable(), c.detail()));
        Tx test = dialect.quantifiedTest(op, left, each, pos, new At(scope, row));
        ValueSpecification some = call("exists", rel.vs(), lambda(sub, all ? test.falseWhen() : test.vs()));
        ValueSpecification every = call("not", call("exists", rel.vs(),
                lambda(sub, call("not", all ? test.vs() : test.falseWhen()))));
        return all ? Tx.condition(every, some, test.nullable()) : Tx.condition(some, every, test.nullable());
    }

    /** {@code x IS [NOT] TRUE | FALSE | UNKNOWN}: never NULL; UNKNOWN is a NULL condition (neither case holds). */
    private Tx isTruth(SqlAst.IsTruth t, Scope scope, String row) {
        Tx operand = truth(tx(t.operand(), scope, row), "IS " + t.truth(), t.pos());
        ValueSpecification holds = switch (t.truth()) {
            case TRUE -> operand.vs();
            case FALSE -> operand.falseWhen();
            case UNKNOWN -> call("and", call("not", operand.vs()), call("not", operand.falseWhen()));
        };
        Tx test = Tx.condition(holds, call("not", holds), false);
        return t.negated() ? not(test) : test;
    }

    /** {@code CASE}: the first WHEN that is TRUE gives the result (a NULL WHEN is not TRUE); no ELSE is NULL. */
    private Tx caseExpr(SqlAst.Case c, Scope scope, String row) {
        SqlAst.Expr otherwise = c.otherwise();
        Tx last = otherwise == null ? Tx.value(empty(), Kind.NULL, true) : tx(otherwise, scope, row);
        List<SqlAst.When> whens = c.whens();
        List<ValueSpecification> tests = new ArrayList<>();
        List<Tx> thens = new ArrayList<>();
        for (int i = whens.size() - 1; i >= 0; i--) {
            SqlAst.When w = whens.get(i);
            SqlAst.Expr operand = c.operand();
            if (operand != null) {
                List<Tx> sides = comparable(operand, w.condition(), scope, row, w.condition().pos());
                tests.add(0, strictTest(sides, v -> call("equal", v.get(0), v.get(1))).vs());
            } else {
                tests.add(0, condition(w.condition(), scope, "CASE/WHEN"));
            }
            thens.add(0, tx(w.result(), scope, row));
        }
        // the results as one type, the ELSE's last
        List<SqlAst.Expr> exprs = new ArrayList<>();
        whens.forEach(w -> exprs.add(w.result()));
        exprs.add(otherwise != null ? otherwise : new SqlAst.NullLit(c.pos()));
        List<Tx> all = new ArrayList<>(thens);
        all.add(last);
        Common results = dialect.unify("CASE", exprs, all, c.pos());
        List<Tx> values = results.values();
        Tx otherwiseValue = values.get(values.size() - 1);
        boolean nullable = otherwiseValue.nullable();
        ValueSpecification vs = valueOf(otherwiseValue);
        for (int i = values.size() - 2; i >= 0; i--) {
            Tx v = values.get(i);
            nullable |= v.nullable();
            vs = call("if", tests.get(i), thunk(valueOf(v)), thunk(vs));
        }
        Kind kind = results.kind();
        if (kind == Kind.NULL) {
            // every result a NULL: Postgres resolves the CASE as text (ceil of it is Postgres's error)
            return nullOf(Kind.NULL);
        }
        if (results.detail() != null && values.stream().allMatch(SqlToPure::isNullConstant)) {
            // every result a NULL of the type the dialect resolved (none in Postgres's resolution): that NULL
            return Tx.value(nullOf(kind).vs(), kind, true, results.detail());
        }
        return kind == Kind.BOOLEAN ? booleanValue(vs, nullable) : Tx.value(vs, kind, nullable, results.detail());
    }

    private Tx function(SqlAst.FunctionCall f, Scope scope, String row) {
        if (isAggregate(f)) {
            String clause = scope.noAggregatesIn();
            throw SqlTranslationException.invalid(clause != null ? "aggregate functions are not allowed in " + clause
                    : dialect.message(Problem.NESTED_AGGREGATE), f.pos());
        }
        if (f.name().size() != 1) {
            throw SqlTranslationException.unsupported("schema-qualified function names are not supported yet",
                    f.pos());
        }
        String fn = functionName(f.name().get(0));
        if (WINDOW_FUNCTIONS.contains(fn)) {
            throw SqlTranslationException.invalid("window function " + fn + " requires an OVER clause", f.pos());
        }
        if (f.distinct() || f.star()) {
            throw SqlTranslationException.invalid(fn + " is not an aggregate", f.pos());
        }
        if (!f.withinGroup().isEmpty()) {
            throw SqlTranslationException.unsupported("WITHIN GROUP is not supported yet", f.pos());
        }
        if ((f.filter() != null || !f.orderBy().isEmpty()) && dialect.function(fn) != null) {
            throw SqlTranslationException.invalid((f.filter() != null ? "FILTER" : "ORDER BY") + " specified, but "
                    + fn + " is not an aggregate function", f.pos());
        }
        List<Tx> args = new ArrayList<>();
        for (SqlAst.Expr a : f.args()) {
            args.add(tx(a, scope, row));
        }
        InputDialect.FunctionRule translation = dialect.function(fn);
        if (translation == null) {
            throw SqlTranslationException.unsupported("function " + fn + "(...) is not supported yet", f.pos());
        }
        return translation.apply(f, args, new At(scope, row));
    }

    /** Postgres's 2-byte integer's names. */
    static final Set<String> SMALL_INTEGERS = Set.of("smallint", "int2");

    private static final Set<SqlAst.BinaryOp> INTEGER_ARITHMETIC = Set.of(SqlAst.BinaryOp.PLUS,
            SqlAst.BinaryOp.MINUS, SqlAst.BinaryOp.TIMES, SqlAst.BinaryOp.DIVIDE, SqlAst.BinaryOp.MODULO);

    /** Whether {@code e} is an integer the database types narrower than the island computes it (the dialect's
     *  {@link InputDialect#narrowInteger}: a smallint, 2026-10-17): a cast to smallint or an int2 constant,
     *  a column holding one, or what keeps its type: a sign, arithmetic of two, coalesce, greatest, least and CASE of
     *  such values (or NULLs), abs, nullif, min, max and the window functions of one. The review of the corpus
     *  program's step 1 found the type lost through them: {@code f1 + abs(int2 '1')}, {@code VALUES (int2 '30000')}
     *  read twice. */
    private boolean widened(SqlAst.Expr e, Scope scope) {
        InputDialect.TypeDetail narrow = dialect.narrowInteger();
        return narrow != null && widened(e, scope, narrow);
    }

    /** {@link #widened}; a column reference reads {@code scope}, or none where it is null (a subquery's own). */
    boolean widened(SqlAst.Expr e, @com.legend.base.Nullable Scope scope, InputDialect.TypeDetail narrow) {
        return switch (e) {
            case SqlAst.Cast c -> SMALL_INTEGERS.contains(c.type().name());
            case SqlAst.TypedLit t -> SMALL_INTEGERS.contains(t.type().name());
            case SqlAst.ColumnRef ref -> {
                Col c = scope == null ? null : scope.readOrNull(ref);
                yield c != null && narrow.equals(c.detail());
            }
            case SqlAst.ScalarSubquery q -> q.query() instanceof SqlAst.Select one && one.items().size() == 1
                    && one.items().get(0) instanceof SqlAst.SelectExpr x && widened(x.expr(), null, narrow);
            case SqlAst.Unary u -> u.op() != SqlAst.UnaryOp.NOT && widened(u.operand(), scope, narrow);
            case SqlAst.Binary b -> INTEGER_ARITHMETIC.contains(b.op()) && widened(b.left(), scope, narrow)
                    && widened(b.right(), scope, narrow);
            case SqlAst.Case c -> {
                List<SqlAst.Expr> results = new ArrayList<>();
                c.whens().forEach(w -> results.add(w.result()));
                if (c.otherwise() != null) {
                    results.add(c.otherwise());
                }
                yield allNarrow(results, scope, narrow);
            }
            case SqlAst.FunctionCall f when f.name().size() == 1 && !f.args().isEmpty() ->
                    switch (functionName(f.name().get(0))) {
                        case "coalesce", "greatest", "least" -> allNarrow(f.args(), scope, narrow);
                        case "abs", "nullif", "min", "max", "first_value", "last_value", "nth_value", "lag", "lead",
                                "any_value" -> widened(f.args().get(0), scope, narrow);
                        default -> false;
                    };
            default -> false;
        };
    }

    /** Whether {@code values} resolve to the narrow type: each one of it or a NULL, one at least of it. */
    private boolean allNarrow(List<SqlAst.Expr> values, @com.legend.base.Nullable Scope scope,
            InputDialect.TypeDetail narrow) {
        boolean any = false;
        for (SqlAst.Expr v : values) {
            if (v instanceof SqlAst.NullLit) {
                continue;
            }
            if (!widened(v, scope, narrow)) {
                return false;
            }
            any = true;
        }
        return any;
    }

    /** Whether {@code e} is a number or string constant, or arithmetic, a sign or a cast of constants alone. */
    static boolean constant(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> true;
            case SqlAst.NumericLit n -> true;
            case SqlAst.StringLit str -> true;
            case SqlAst.TypedLit t -> true;
            case SqlAst.Unary u -> u.op() != SqlAst.UnaryOp.NOT && constant(u.operand());
            case SqlAst.Cast c -> constant(c.operand());
            case SqlAst.Binary b -> ARITHMETIC.contains(b.op()) && constant(b.left()) && constant(b.right());
            default -> false;
        };
    }

    static final Set<SqlAst.BinaryOp> ARITHMETIC = Set.of(SqlAst.BinaryOp.PLUS, SqlAst.BinaryOp.MINUS,
            SqlAst.BinaryOp.TIMES, SqlAst.BinaryOp.DIVIDE, SqlAst.BinaryOp.MODULO);

    // ------------------------------------------------------------------ dates

    static final String DURATION_UNIT = "meta::pure::functions::date::DurationUnit";

    // ------------------------------------------------------------------ jsonb

    /** Pure's semi-structured value: Postgres's jsonb here (register S49). */
    static final String VARIANT = "meta::pure::metamodel::variant::Variant";
    static final String FROM_JSON = "fromJson";

    /** A jsonb value read out of a jsonb CONSTANT is refused: legend-lite's Postgres lowering folds the navigation of a
     *  literal Variant at compile time, and re-emits a picked string, number or boolean as a plain SQL literal, not a
     *  jsonb value (its text, read by ->>, is right). A column's, or any value's not a constant's, is not folded. */
    static void constantNavigation(SqlAst.Expr base, SqlAst.Pos pos) {
        if (isJsonConstant(base)) {
            throw SqlTranslationException.unsupported("reading a jsonb value out of a jsonb constant is not supported"
                    + " yet (its text, with ->> or #>>, is)", pos);
        }
    }

    static boolean isJsonConstant(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Cast c -> JSON_TYPES.contains(c.type().name())
                    && (c.operand() instanceof SqlAst.StringLit || isJsonConstant(c.operand()));
            case SqlAst.TypedLit t -> JSON_TYPES.contains(t.type().name());
            case SqlAst.Operator o -> o.left() != null && (o.operator().equals("->") || o.operator().equals("#>"))
                    && isJsonConstant(java.util.Objects.requireNonNull(o.left()));
            case SqlAst.Subscript sub -> isJsonConstant(sub.base());
            // an untyped constant where jsonb is read (a function's argument) is a jsonb constant
            case SqlAst.StringLit l -> true;
            default -> false;
        };
    }

    static final Set<String> JSON_TYPES = Set.of("json", "jsonb");

    static Tx jsonOperand(Tx t, String operator, SqlAst.Pos pos) {
        if (t.kind() == Kind.JSON) {
            return t;
        }
        if (t.kind() == Kind.NULL || t.kind() == Kind.OTHER) {
            throw SqlTranslationException.unsupported("the operator " + operator + " of a value that is not jsonb is"
                    + " not supported yet", pos);
        }
        throw SqlTranslationException.invalid("operator does not exist: " + t.kind().name().toLowerCase(Locale.ROOT)
                + " " + operator + " unknown", pos);
    }

    /** jsonb's order and equality are its own (an object after an array, a longer object after a shorter one, keys
     *  by length): a Variant is ordered, grouped and compared by Pure's rules, so a jsonb value is no key here. */
    static void notJsonKey(Kind kind, String what, SqlAst.Pos pos) {
        if (kind == Kind.JSON) {
            throw SqlTranslationException.unsupported(what + " of a jsonb value is not supported yet (jsonb orders and"
                    + " compares by its own rules)", pos);
        }
    }

    /** {@code @Name}: a type as a value, as {@code to(@String)} takes it. */
    public static ValueSpecification typeArg(String name) {
        return new TypeAnnotation.Named(new TypeExpression.NameRef(name));
    }

    // ------------------------------------------------------------------ canonical form

    /** An expression's structure, its column references resolved: two spellings of one expression ({@code name},
     *  {@code t.NAME}) have one canonical form. Postgres matches GROUP BY and select-list expressions this way. */
    private static String sorts(List<SqlAst.SortItem> items, Input in) {
        StringBuilder b = new StringBuilder("[");
        for (SqlAst.SortItem i : items) {
            b.append(canon(i.expr(), in)).append(i.descending() ? " desc" : "").append(" nulls ")
                    .append(i.nullsFirst()).append(';');
        }
        return b.append(']').toString();
    }

    static String canon(SqlAst.Expr e, Input in) {
        return switch (e) {
            case SqlAst.ColumnRef ref -> {
                Col c = in.column(ref);
                yield "col(" + (c.var() == null ? "" : c.var() + ".") + c.name() + ")";
            }
            case SqlAst.IntegerLit n -> "int(" + n.value() + ")";
            case SqlAst.NumericLit n -> "num(" + n.value().toPlainString() + ")";
            case SqlAst.StringLit s -> "str(" + s.value().length() + ":" + s.value() + ")";
            case SqlAst.BooleanLit b -> "bool(" + b.value() + ")";
            case SqlAst.NullLit n -> "null";
            case SqlAst.TypedLit t -> "typed(" + t.type().name() + ":" + t.value() + ")";
            case SqlAst.Param p -> "param(" + p.index() + ")";
            case SqlAst.Unary u -> u.op() + "(" + canon(u.operand(), in) + ")";
            case SqlAst.Binary b -> b.op() + "(" + canon(b.left(), in) + "," + canon(b.right(), in) + ")";
            case SqlAst.Operator o -> {
                SqlAst.Expr left = o.left();
                yield "op(" + o.operator() + "," + (left == null ? "" : canon(left, in)) + "," + canon(o.right(), in)
                        + ")";
            }
            case SqlAst.IsNull n -> "isnull" + n.negated() + "(" + canon(n.operand(), in) + ")";
            case SqlAst.IsTruth t -> "is" + t.negated() + t.truth() + "(" + canon(t.operand(), in) + ")";
            case SqlAst.IsDistinct d -> "distinct" + d.negated() + "(" + canon(d.left(), in) + ","
                    + canon(d.right(), in) + ")";
            case SqlAst.InList l -> "in" + l.negated() + "(" + canon(l.operand(), in) + list(l.values(), in) + ")";
            case SqlAst.Between b -> "between" + b.negated() + "(" + canon(b.operand(), in) + ","
                    + canon(b.low(), in) + "," + canon(b.high(), in) + ")";
            case SqlAst.Like l -> "like" + l.negated() + l.caseInsensitive() + "(" + canon(l.operand(), in) + ","
                    + canon(l.pattern(), in) + "," + (l.escape() == null ? "" : canon(
                            java.util.Objects.requireNonNull(l.escape()), in)) + ")";
            case SqlAst.Case c -> {
                StringBuilder b = new StringBuilder("case(");
                SqlAst.Expr operand = c.operand();
                b.append(operand == null ? "" : canon(operand, in));
                for (SqlAst.When w : c.whens()) {
                    b.append(",when(").append(canon(w.condition(), in)).append(",").append(canon(w.result(), in))
                            .append(")");
                }
                SqlAst.Expr otherwise = c.otherwise();
                b.append(",else(").append(otherwise == null ? "" : canon(otherwise, in)).append("))");
                yield b.toString();
            }
            case SqlAst.FunctionCall f -> {
                SqlAst.Expr filter = f.filter();
                yield "fn(" + String.join(".", f.name().stream().map(SqlToPure::functionName).toList()) + ","
                        + f.distinct() + "," + f.star() + list(f.args(), in) + sorts(f.orderBy(), in) + "within"
                        + sorts(f.withinGroup(), in) + (filter == null ? "" : "filter(" + canon(filter, in) + ")")
                        + ")";
            }
            case SqlAst.Cast c -> "cast(" + c.type().name() + c.type().modifiers() + "," + canon(c.operand(), in)
                    + ")";
            // a subquery or a window call is the one it is: matched only to itself, by where it was written
            case SqlAst.ScalarSubquery q -> "subquery@" + q.pos().line() + ":" + q.pos().column();
            case SqlAst.Exists q -> "exists@" + q.pos().line() + ":" + q.pos().column();
            case SqlAst.InSubquery q -> "in@" + q.pos().line() + ":" + q.pos().column();
            case SqlAst.Quantified q -> "quantified@" + q.pos().line() + ":" + q.pos().column();
            case SqlAst.Window w -> "window@" + w.pos().line() + ":" + w.pos().column();
            case SqlAst.Subscript sub -> "subscript(" + canon(sub.base(), in) + "," + canon(sub.index(), in) + ")";
            case SqlAst.ArrayConstructor a -> "array(" + list(a.elements(), in) + ")";
        };
    }

    private static String list(List<SqlAst.Expr> values, Input in) {
        StringBuilder b = new StringBuilder();
        for (SqlAst.Expr v : values) {
            b.append(',').append(canon(v, in));
        }
        return b.toString();
    }

    private static List<SqlAst.Expr> children(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.ColumnRef r -> List.of();
            case SqlAst.IntegerLit n -> List.of();
            case SqlAst.NumericLit n -> List.of();
            case SqlAst.StringLit s -> List.of();
            case SqlAst.BooleanLit b -> List.of();
            case SqlAst.NullLit n -> List.of();
            case SqlAst.TypedLit t -> List.of();
            case SqlAst.Param p -> List.of();
            case SqlAst.Unary u -> List.of(u.operand());
            case SqlAst.Binary b -> List.of(b.left(), b.right());
            case SqlAst.Operator o -> {
                SqlAst.Expr left = o.left();
                yield left == null ? List.of(o.right()) : List.of(left, o.right());
            }
            case SqlAst.IsNull n -> List.of(n.operand());
            case SqlAst.IsTruth t -> List.of(t.operand());
            case SqlAst.IsDistinct d -> List.of(d.left(), d.right());
            case SqlAst.InList l -> {
                List<SqlAst.Expr> all = new ArrayList<>();
                all.add(l.operand());
                all.addAll(l.values());
                yield all;
            }
            case SqlAst.Between b -> List.of(b.operand(), b.low(), b.high());
            case SqlAst.Like l -> {
                List<SqlAst.Expr> all = new ArrayList<>(List.of(l.operand(), l.pattern()));
                SqlAst.Expr esc = l.escape();
                if (esc != null) {
                    all.add(esc);
                }
                yield all;
            }
            case SqlAst.Case c -> {
                List<SqlAst.Expr> all = new ArrayList<>();
                SqlAst.Expr operand = c.operand();
                if (operand != null) {
                    all.add(operand);
                }
                for (SqlAst.When w : c.whens()) {
                    all.add(w.condition());
                    all.add(w.result());
                }
                SqlAst.Expr otherwise = c.otherwise();
                if (otherwise != null) {
                    all.add(otherwise);
                }
                yield all;
            }
            case SqlAst.FunctionCall f -> {
                List<SqlAst.Expr> all = new ArrayList<>(f.args());
                f.orderBy().forEach(o -> all.add(o.expr()));
                f.withinGroup().forEach(o -> all.add(o.expr()));
                SqlAst.Expr filter = f.filter();
                if (filter != null) {
                    all.add(filter);
                }
                yield all;
            }
            case SqlAst.Cast c -> List.of(c.operand());
            // a subquery's insides are its own query's: its aggregates are not this one's
            case SqlAst.ScalarSubquery q -> List.of();
            case SqlAst.Exists q -> List.of();
            case SqlAst.InSubquery q -> List.of(q.operand());
            case SqlAst.Quantified q -> List.of(q.left());
            // a window call is no aggregate of this query's grouping, but what it reads may be
            case SqlAst.Window w -> {
                List<SqlAst.Expr> all = new ArrayList<>(w.call().args());
                all.addAll(w.spec().partitionBy());
                w.spec().orderBy().forEach(o -> all.add(o.expr()));
                yield all;
            }
            case SqlAst.Subscript sub -> List.of(sub.base(), sub.index());
            case SqlAst.ArrayConstructor a -> a.elements();
        };
    }

    // ------------------------------------------------------------------ building Pure

    /** A function name as Postgres reads it: folded when unquoted, exact when quoted. */
    static String functionName(SqlAst.Ident id) {
        return id.quoted() ? id.text() : fold(id.text());
    }

    public static AppliedFunction call(String fn, ValueSpecification... args) {
        return call(fn, List.of(args));
    }

    static AppliedFunction call(String fn, List<ValueSpecification> args) {
        return new AppliedFunction(fn, args);
    }

    static ValueSpecification property(String row, String column) {
        return new AppliedProperty(new Variable(row), column);
    }

    static LambdaFunction lambda(String parameter, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(parameter)), List.of(body));
    }

    /** A two-parameter lambda: a join's condition over its two sides' rows. */
    private static LambdaFunction lambda2(String first, String second, ValueSpecification body) {
        return new LambdaFunction(List.of(new Variable(first), new Variable(second)), List.of(body));
    }

    public static LambdaFunction thunk(ValueSpecification body) {
        return new LambdaFunction(List.of(), List.of(body));
    }

    /** No value: Pure's {@code []}, SQL's NULL. */
    public static ValueSpecification empty() {
        return new PureCollection(List.of());
    }

    static ColSpec colSpec(String name) {
        return new ColSpec(name, null, null, null);
    }
}
