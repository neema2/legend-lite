// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import java.util.List;

/**
 * A SQL dialect as {@link SqlToPure} reads one: its parse, and the rules the dialects differ on -- which name an
 * identifier means, where NULLs sort when the query does not say, what an unnamed column is called, what a source
 * column's and a constant's types are, what an operator, a function, a cast, LIKE and an aggregate mean, how CASE's
 * results take one type, and the wording of the refusals that are the dialect's own. The rest of the translation is
 * every dialect's: the scopes, the query's shape (FROM, WHERE, grouping, windows, set operations, WITH), three-valued
 * logic, subqueries. A default here is the translator's plain reading, which a dialect overrides where it reads
 * otherwise; every rule that is a language's own -- its operators, casts, functions and aggregates, how it reads a
 * string as a value, its type names -- is that dialect's to state (the dialect program's D11, 2026-10-18, moved
 * Postgres's out of the translator and out of the defaults that called them).
 *
 * <p>{@link InputLanguages} says which dialect an island's language is read in ({@link PostgresDialect} for
 * {@code #SQL{...}#}: Postgres is that island's spec). A dialect read under options (an engine's profile) is one
 * instance per profile, so no method here takes one.
 */
public interface InputDialect {

    /** The query {@code sql} is, as the dialect's reader parses it: {@link SqlSyntaxException} when it is no query
     *  the reader accepts (its {@code unsupported()} when the syntax may be fine but is not read yet). */
    SqlAst.Statement parse(String sql);

    /** The dialect {@code statement}, as {@link #parse} read it, is translated in: this one, unless the dialect's rules
     *  depend on the statement as a whole (IQ's engine, chosen by the statement's FROM clause). */
    default InputDialect reading(SqlAst.Statement statement) {
        return this;
    }

    /** The statement {@link #reading} was given is translated, before its relation is returned: a dialect raises here
     *  an error it held while translating, which an error met later in the statement comes before (IQ island,
     *  2026-10-17: IQ's -157 for a string an integer column cannot be, before its -1001013 for comparing a string with
     *  a number, wherever each stands in the WHERE). */
    default void translated() {
    }

    /** The statement {@link #reading} was given failed to translate with {@code e}: the error to raise, {@code e} or
     *  one the dialect held that comes before it (IQ island, 2026-10-17: IQ raises a subquery's placement error after
     *  the statement's names, grouping and functions are checked). */
    default SqlTranslationException failed(SqlTranslationException e) {
        return e;
    }

    // ------------------------------------------------------------------ names

    /** The name an unquoted identifier declares ({@code AS x}, a WITH query's name): Postgres folds it to lower
     *  case. */
    String fold(String text);

    /** THE identifier rule: the name of {@code names} that {@code id} means, or null for none; a reference that means
     *  more than one is the dialect's error. {@code what} says what is named: {@code column}, {@code table} or
     *  {@code window}. */
    @com.legend.base.Nullable String resolve(SqlAst.Ident id, Iterable<String> names, String what);

    /** What to add to "does not exist" for {@code name} among {@code names}: a hint at the name meant, or nothing. */
    String hint(SqlAst.Ident name, List<String> names);

    // ------------------------------------------------------------------ order and output names

    /** Whether NULLs sort first where the query does not say ({@code NULLS FIRST} or {@code NULLS LAST} unwritten). */
    boolean nullsFirst(boolean descending);

    /** The output name of a select item written without an alias. */
    String defaultName(SqlAst.SelectExpr item);

    /** The join {@code join} is read as: its own type; a dialect may read an outer join as the inner one it is (IQ
     *  island, 2026-10-17: IQ's engine, under a WHERE that rejects the null-supplying side's NULLs). */
    default SqlAst.JoinType joinType(SqlAst.Join join) {
        return join.type();
    }

    /** The output name of a select item written without an alias, where {@code tables} says which FROM item of the
     *  item's query each of its column references reads: {@link #defaultName(SqlAst.SelectExpr)} unless the dialect
     *  names an item by the tables it reads (Postgres's does not). */
    default String defaultName(SqlAst.SelectExpr item, Tables tables) {
        return defaultName(item);
    }

    /** The name an unaliased FROM function is known by in the query (its correlation name): its own, as written
     *  (Postgres's), unless the dialect says otherwise. */
    default SqlAst.Ident functionName(SqlAst.TableFunction f) {
        return f.name().get(f.name().size() - 1);
    }

    /** Which FROM item a column reference reads, for {@link #defaultName(SqlAst.SelectExpr, Tables)}. */
    @FunctionalInterface
    interface Tables {
        /** The correlation name (its alias, else its name) of the FROM item of the reference's own query that
         *  {@code ref} reads, as written; null when it reads none there (an enclosing query's column, a column two
         *  items share). */
        SqlAst.@com.legend.base.Nullable Ident table(SqlAst.ColumnRef ref);
    }

    /** IQ island, 2026-10-14 (the third landing): {@code ref}, a column reference, names no column of its own query's
     *  FROM items, and the enclosing queries' are searched next: nothing unless the dialect reads such a name
     *  otherwise there (IQ reads an enclosing query's select-list alias before its columns: refused by name). */
    default void columnNotHere(SqlAst.ColumnRef ref) {
    }

    /** IQ island, 2026-10-14 (review 7's M1): {@code ref}, a column reference, reads an enclosing query's column (found
     *  there after {@link #columnNotHere}): nothing unless the dialect refuses such a read where it stands. */
    default void enclosingColumn(SqlAst.ColumnRef ref) {
    }

    /** IQ island, 2026-10-14 (the third landing): whether no one reads {@code s}'s output names, so that two of one
     *  name are no error (an EXISTS subquery's, a set operation's later branch's): never, unless the dialect says so
     *  (Postgres's: a relation's column names are distinct). */
    default boolean namesUnread(SqlAst.Select s) {
        return false;
    }

    /** The output name of a select item that is a column reference written without an alias: {@code written} is the
     *  reference's last part as written, {@code label} the name the column has where it is read (Postgres's). */
    default String columnName(SqlAst.Ident written, String label) {
        return label;
    }

    /** IQ island, 2026-10-14 (review 7's B2): whether {@code item}, an unaliased column reference, is named as a
     *  column ({@link #columnName}), else by {@link #defaultName(SqlAst.SelectExpr, Tables)}: always (Postgres's). */
    default boolean namedAsColumn(SqlAst.SelectExpr item) {
        return true;
    }

    // ------------------------------------------------------------------ types

    /** What the translator knows of a FROM source column's type, from its Pure type's name ({@code Integer},
     *  {@code Decimal(10,2)}, an enumeration's path, ...) and its declared store type when the source has one
     *  ({@link SqlToPure.Column}). */
    SqlToPure.Kind kindOf(String pureType, @com.legend.base.Nullable String storeType);

    /** What the dialect knows of a FROM source column's type beyond its kind, carried in {@link SqlToPure.Tx#detail()}
     *  wherever the column is read: in Postgres's, a column's char(n) or real store type alone (2026-10-17). */
    default @com.legend.base.Nullable TypeDetail detailOf(String pureType, @com.legend.base.Nullable String storeType) {
        return null;
    }

    /** A FROM source column's value as the dialect reads it, from {@code stored}, its value as the source holds it:
     *  null when it is read as stored (Postgres's always are). The translator computes it in the column's place, so
     *  every reference reads it so. */
    default SqlToPure.@com.legend.base.Nullable Tx read(String column, SqlToPure.Tx stored, SqlAst.Pos pos) {
        return null;
    }

    /** A constant's translation: an {@link SqlAst.IntegerLit}, {@link SqlAst.NumericLit}, {@link SqlAst.StringLit},
     *  {@link SqlAst.BooleanLit}, {@link SqlAst.NullLit} or {@link SqlAst.TypedLit}. */
    SqlToPure.Tx literal(SqlAst.Expr constant);

    /** A string constant the translator compares with a value of {@code kind} (a simple CASE's operand, a window
     *  function's default beside its argument), read as a value of that kind by the dialect's input rules, or the
     *  dialect's refusal (the dialect program's D11, 2026-10-18). */
    SqlToPure.Tx readString(SqlAst.StringLit s, SqlToPure.Kind kind);

    /** A kind's name in the dialect's own wording, for the translator's refusals that name a value's type
     *  ({@code function lag(text, integer) does not exist}; the dialect program's D11, 2026-10-18). */
    String typeName(SqlToPure.Kind kind);

    // ------------------------------------------------------------------ meanings

    /** {@code b}'s translation: its operands are translated {@code at} its place. */
    SqlToPure.Tx binary(SqlAst.Binary b, SqlToPure.At at);

    /** Whether a value of this store detail ({@link #detailOf}) is blank-padded text whose cast to text drops the
     *  padding (Postgres's {@code char(n)}; 2026-10-17). None by default. */
    default boolean padded(@com.legend.base.Nullable TypeDetail detail) {
        return false;
    }

    /** Whether a string compared with a value of this store detail is read as a single-precision float, which Pure's
     *  Float is not, so the comparison is refused (Postgres's {@code real}; 2026-10-17). None by default. */
    default boolean singlePrecision(@com.legend.base.Nullable TypeDetail detail) {
        return false;
    }

    /** Whether an untyped NULL that a subquery or a WITH query outputs is text to the query reading it (the column
     *  then reads as a NULL text), else it stays untyped; false by default. */
    default boolean untypedNullIsText() {
        return false;
    }

    /** The detail of an integer the dialect's database types narrower than the island's integer, and computes
     *  arithmetic of at that width, raising past it (Postgres's smallint: a cast to smallint or an int2 constant, which
     *  the island writes as a wider integer; 2026-10-17); null where there is none, by default. The translator marks
     *  such a value's column with it and refuses the arithmetic the database would compute narrow. */
    default @com.legend.base.Nullable TypeDetail narrowInteger() {
        return null;
    }

    /** Whether two WINDOW definitions of one name are an error (Postgres's, by default), else the first one counts. */
    default boolean oneWindowPerName() {
        return true;
    }

    /** {@code u}'s translation ({@code -x}, {@code +x}, {@code NOT c}). */
    SqlToPure.Tx unary(SqlAst.Unary u, SqlToPure.At at);

    /** The count of a row limit, {@code clause} {@code LIMIT} or {@code OFFSET} (the query's {@link SqlAst.Select#limit}
     *  and {@link SqlAst.Select#offset}): Postgres's is a non-negative integer constant, as written. */
    default com.legend.protocol.spec.ValueSpecification rowCount(SqlAst.Expr count, String clause) {
        return SqlToPure.count(count, clause);
    }

    /** Values that are one value's alternatives, as one type -- a CASE's results ({@code what} names the construct,
     *  for the dialect's errors): each value, read as that type where it is read otherwise ({@code exprs[i]} is
     *  {@code values[i]}'s expression), and the type's kind. */
    SqlToPure.Common unify(String what, List<SqlAst.Expr> exprs, List<SqlToPure.Tx> values, SqlAst.Pos pos);

    /** The catalog: the scalar function named {@code name} (as {@code SqlToPure} reads a function's name: folded when
     *  unquoted), or null when the dialect has none (refused as not supported). */
    @com.legend.base.Nullable FunctionRule function(String name);

    /** {@code CAST(x AS type)}'s translation (and the dialect's other spellings of one). */
    SqlToPure.Tx cast(SqlAst.Cast c, SqlToPure.At at);

    /** {@code x [NOT] LIKE pattern}'s translation. */
    SqlToPure.Tx like(SqlAst.Like l, SqlToPure.At at);

    /** {@code o}'s translation, an operator the translator reads as no other node ({@link SqlAst.Operator}: a jsonb
     *  navigation, a regular expression match); null where the dialect reads no such operator, refused as not
     *  supported (the dialect program's D11, 2026-10-18). */
    default SqlToPure.@com.legend.base.Nullable Tx operator(SqlAst.Operator o, SqlToPure.At at) {
        return null;
    }

    /** {@code s}'s translation, a subscript ({@code x[i]}); null where the dialect reads none, refused (the dialect
     *  program's D11, 2026-10-18). */
    default SqlToPure.@com.legend.base.Nullable Tx subscript(SqlAst.Subscript s, SqlToPure.At at) {
        return null;
    }

    /** {@code x [NOT] BETWEEN low AND high}'s translation. */
    SqlToPure.Tx between(SqlAst.Between b, SqlToPure.At at);

    /** {@code x [NOT] IN (value, ...)}'s translation. */
    SqlToPure.Tx inList(SqlAst.InList in, SqlToPure.At at);

    /** {@code x IS [NOT] DISTINCT FROM y}'s translation. */
    SqlToPure.Tx isDistinct(SqlAst.IsDistinct d, SqlToPure.At at);

    /** A FROM item that is one of the dialect's own relations (a system table, a system procedure), as the dialect
     *  reads it; null when it is none and the translator reads the item (Postgres's: never one). */
    default SqlToPure.@com.legend.base.Nullable Relation source(SqlAst.FromItem item) {
        return null;
    }

    /** IQ island, 2026-10-14 (review 5's M3): a FROM table name that is one of the dialect's own relations
     *  ({@link #source}), read beside the model's tables ({@link SqlToPure.Sources#tables}), which a dialect may find
     *  before its own: {@link #source}'s unless the dialect says otherwise. */
    default SqlToPure.@com.legend.base.Nullable Relation tableSource(SqlAst.TableName t, SqlToPure.Sources sources) {
        return source(t);
    }

    /** IQ island, 2026-10-14 (M1e): a FROM table named without a source function ({@code owner.table}), as the
     *  dialect binds it to one of the model's tables ({@link SqlToPure.Sources#tables}): its relation, which the
     *  translator types as it types a source function's, or the dialect's error; null when the dialect binds no name
     *  so (Postgres's: refused, a table name needs {@code tb(...)}). */
    default com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification table(SqlAst.TableName t,
            SqlToPure.Sources sources) {
        return null;
    }

    /** IQ island, 2026-10-14 (M1e): a parameter's translation {@code at} its place: a named one (a host variable) as
     *  the dialect reads it, a variable of the enclosing Pure query in scope ({@link SqlToPure.Sources#variable}).
     *  Postgres's ({@code $1}): refused. */
    default SqlToPure.Tx parameter(SqlAst.Param p, SqlToPure.At at, SqlToPure.Sources sources) {
        throw SqlTranslationException.unsupported("parameters ($" + p.index() + ") are not supported yet", p.pos());
    }

    /** One row of a subquery compared, for {@code x op ANY | ALL (query)} and {@code x [NOT] IN (query)}:
     *  {@code left op each}, {@code each} the subquery's column read in that row. */
    SqlToPure.Tx quantifiedTest(SqlAst.BinaryOp op, SqlAst.Expr left, SqlToPure.Tx each, SqlAst.Pos pos,
            SqlToPure.At at);

    /** The type of column {@code i} of {@code s}, a set operation, from its two sides' ({@code left}, {@code right}):
     *  null for the translator's own rule (Postgres's: one kind on both sides). A dialect that unifies the two (or
     *  refuses the pair by name) answers its type. */
    default SqlToPure.@com.legend.base.Nullable ColumnType setColumn(SqlAst.SetOperation s, int i,
            SqlToPure.ColumnType left, SqlToPure.ColumnType right) {
        return null;
    }

    /** IQ island, 2026-10-15: column {@code i} of one side of {@code s} ({@code left}'s or the right's), its value
     *  {@code side}, converted to the type the column takes beside the other side's {@code other} ({@link #setColumn}):
     *  null where its values are already that type's. */
    default SqlToPure.@com.legend.base.Nullable Tx setValue(SqlAst.SetOperation s, int i, SqlToPure.Tx side,
            SqlToPure.ColumnType other, boolean left) {
        return null;
    }

    /** Whether an ORDER BY key orders nothing: a constant the dialect does not read as a position (IQ island,
     *  2026-10-16, measured on IQ 17.1 and SQL Anywhere 17: an integer past an INT's range orders nothing, 2147483647
     *  past the items is IQ's -152). Postgres's: none, every integer is a position. */
    default boolean ordersNothing(SqlAst.Expr key) {
        return false;
    }

    /** Which rows a statement with a row limit computes the items of its select list for that its ORDER BY does not
     *  read ({@code SqlToPure.assemble}; IQ island, 2026-10-16, the user's decision). An item that can raise raises
     *  for those rows only. Postgres's: every row, before its sort and its limit, as SQL's order has it (measured on
     *  Postgres 16 for a division, which it computes before the sort; it puts off an item its planner costs as
     *  expensive, as {@code sql-parity/lite/math.yaml} records). */
    default RowLimitItems rowLimitItems(SqlAst.Select s) {
        return RowLimitItems.EVERY_ROW;
    }

    /** {@link #rowLimitItems}' answers. An ORDER BY's items, and all of a DISTINCT's, are computed for every row
     *  whatever the answer: the sort and the DISTINCT read them all. */
    enum RowLimitItems {
        /** every row, before the sort and the limit */
        EVERY_ROW,
        /** the rows the limit returns */
        ROWS_RETURNED,
        /** the rows up to the limit's last, the ones its offset skips too */
        ROWS_UP_TO_THE_LAST
    }

    /** IQ island, 2026-10-15: whether an ORDER BY's unqualified name that is {@code output}'s name reads that output
     *  ({@code SqlToPure.sortKey}); otherwise it is a column of the FROM clause, resolved (an ambiguous one an error) as
     *  any column is. Postgres's: any output name. */
    default boolean ordersByOutput(SqlAst.Select s, String output) {
        return true;
    }

    /** Whether {@code *=} and {@code =*} at the top level of a query's WHERE are T-SQL outer joins, which the
     *  translator makes the query's joins (IQ island, 2026-10-15, checklist 0.6: IQ's, under its tsql_outer_joins
     *  option); Postgres has no such operators. */
    default boolean readsTsqlOuterJoins() {
        return false;
    }

    /** Whether a NATURAL join shows the columns its two sides share once, merged (the standard's, Postgres's); a
     *  dialect that keeps both sides' columns, each read qualified, joining on their equality, says false. */
    default boolean mergesNaturalJoin() {
        return true;
    }

    /**
     * A value used as a key of {@code clause} ({@code ORDER BY}, {@code DISTINCT}, {@code GROUP BY}, {@code UNION},
     * {@code INTERSECT}, {@code EXCEPT}): null when the translation's ordering and equality of the values themselves
     * are the dialect's for it (Postgres's always are), else the dialect's key -- its collation's: what the clause
     * sorts or groups by in the value's place, the value's translation in, the key's out -- or the dialect's refusal.
     * {@code value} is the value as the clause reads it, {@code expr} its expression where it has one (none for a
     * star's column or a set operation's). A DISTINCT or a set operation shows one value of each group of equal keys,
     * which the translation does not choose: a dialect answers null for them or refuses; a GROUP BY key the dialect
     * answers a key for is grouped by that key, and the query must show no value of it (the dialect's to check).
     * IQ island, 2026-10-14: the dialect's key (it was a refusal only).
     */
    default java.util.function.@com.legend.base.Nullable UnaryOperator<com.legend.protocol.spec.ValueSpecification> key(
            String clause, SqlToPure.Tx value, SqlAst.@com.legend.base.Nullable Expr expr, SqlAst.Pos pos) {
        return null;
    }

    /**
     * Whether a GROUP BY key the dialect answers a {@link #key} for shows a member of each group (IQ island,
     * 2026-10-15): the groups are the key's, and where the query reads the key's value it reads one value of the group
     * -- the least of the values themselves, which is one of them -- rather than the key. A dialect whose database
     * shows a group's first value it meets (its plan's choice) may answer true: any value of the group is an answer
     * that database could give. Postgres's keys are its values (false).
     */
    default boolean representative(String clause, SqlToPure.Tx value, SqlAst.@com.legend.base.Nullable Expr expr) {
        return false;
    }

    /**
     * A set operation's key for its column {@code i} (IQ island, 2026-10-15): as {@link #key} of the operation's name,
     * where the dialect needs the column's place to know it. A UNION column keyed so shows one value of each group
     * ({@link #representative}); INTERSECT and EXCEPT read no key.
     */
    default java.util.function.@com.legend.base.Nullable UnaryOperator<com.legend.protocol.spec.ValueSpecification>
            setKey(SqlAst.SetOperation s, int i, SqlToPure.Tx value) {
        return key(s.op().name(), value, null, s.pos());
    }

    // ------------------------------------------------------------------ grouping

    /** {@code s} as the dialect reads its GROUP BY and HAVING: itself (Postgres's), unless the dialect reads a name
     *  there otherwise -- IQ reads a select-list alias before a column of its name. */
    default SqlAst.Select grouping(SqlAst.Select s) {
        return s;
    }

    /** Whether an integer constant in GROUP BY names a select item by its position (Postgres's); a dialect that
     *  groups by the constant itself says false. */
    default boolean groupsByPosition() {
        return true;
    }

    /** Whether a call of {@code name} (a function's name as the translator reads one: folded when unquoted) is one of
     *  the dialect's aggregates. */
    boolean isAggregate(String name);

    /** A call of one of the dialect's aggregates ({@link #isAggregate}), as the dialect computes it {@code at} its
     *  grouped query (the dialect program's D11, 2026-10-18: every dialect's own; the translator has none). */
    SqlToPure.Aggregation aggregate(SqlAst.FunctionCall f, SqlToPure.Grouping at);

    // ------------------------------------------------------------------ windows

    /** IQ island, 2026-10-15 (windows): the call {@code f} of a window function (its OVER clause read: a key of the
     *  window that is a string is the dialect's {@link #key} for clause {@code PARTITION BY} or {@code ORDER BY}) as
     *  the dialect computes it {@code at} its window; null for the translator's own reading (Postgres's). */
    default SqlToPure.@com.legend.base.Nullable WindowFunction window(SqlAst.FunctionCall f, SqlToPure.Windowing at) {
        return null;
    }

    // ------------------------------------------------------------------ the dialect's own refusals

    /** A refusal's message, in the dialect's wording: {@link Problem} says what {@code args} are. */
    String message(Problem problem, Object... args);

    /** A function of a dialect's catalog: the call, its arguments' translations, and the place it is translated. */
    @FunctionalInterface
    interface FunctionRule {
        SqlToPure.Tx apply(SqlAst.FunctionCall f, List<SqlToPure.Tx> args, SqlToPure.At at);
    }

    /** What a dialect knows of a value's type beyond its {@link SqlToPure.Kind} (a width, a scale, a length): opaque
     *  to the translator, which carries it in {@link SqlToPure.Tx#detail()} and asks the dialect about it
     *  ({@link #padded}, {@link #singlePrecision}). Postgres's is a column's char(n) or real store type. */
    interface TypeDetail {
    }

    /** The refusals whose wording is the dialect's own, each with the arguments its message reads, in order. */
    enum Problem {
        /** A qualified column the qualifier's FROM item does not have: the qualifier and the column as written, and
         *  the {@link InputDialect#hint}. */
        NO_SUCH_QUALIFIED_COLUMN,
        /** A column no FROM item has: the column as written, and the {@link InputDialect#hint} (or nothing). */
        NO_SUCH_COLUMN,
        /** A column more than one FROM item has: the column as written. */
        AMBIGUOUS_COLUMN,
        /** A qualifier more than one FROM item has: the qualifier as written. */
        AMBIGUOUS_TABLE,
        /** A qualifier no FROM item has: the qualifier as written. */
        NO_SUCH_TABLE,
        /** A column of a grouped query that is no group key: the reference as written (IQ island, 2026-10-14, the third
         *  landing: and the reference itself). */
        NOT_GROUPED,
        /** Two FROM items of one name: the name as written. */
        DUPLICATE_TABLE,
        /** A USING column one side has twice: the column as written, and the side ({@code left} or {@code right}). */
        DUPLICATE_USING_COLUMN,
        /** A USING column one side does not have: the column as written, and the side. */
        NO_SUCH_USING_COLUMN,
        /** A window reference to no named window: the name as written. */
        NO_SUCH_WINDOW,
        /** IQ island, 2026-10-14 (the third landing): a SELECT DISTINCT's ORDER BY key that is none of its items. */
        DISTINCT_ORDER_KEY,
        /** IQ island, 2026-10-14 (the third landing): a set operation's two sides of different widths: the operation
         *  ({@code UNION}, {@code INTERSECT}, {@code EXCEPT}). */
        SET_WIDTHS,
        /** IQ island, 2026-10-17: an aggregate call inside an aggregate's arguments. */
        NESTED_AGGREGATE
    }
}
