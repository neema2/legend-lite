// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CString;
import com.legend.protocol.sql.SqlToPure.Kind;
import com.legend.protocol.sql.SqlToPure.Tx;

import java.util.ArrayList;
import java.util.List;

/**
 * Postgres as {@link SqlToPure} reads it: the {@code #SQL{...}#} island's dialect, Postgres 16 its spec
 * (docs/LEGEND_SQL_STUDY_2026_10_11.md). Its reader is {@link SqlParser}; its names, NULL order, output names,
 * constants' and columns' types and the wording of its refusals are here; its operators and predicates, casts, CASE's
 * one type, function catalog, intervals, jsonb and aggregates are its own files' ({@link PostgresOperators},
 * {@link PostgresCasts}, {@link PostgresFunctions}, {@link PostgresDates}, {@link PostgresJsonb},
 * {@link PostgresAggregates}; the dialect program's D11, 2026-10-18, moved them out of {@link SqlToPure}), which read
 * the translator throughout ({@link SqlToPure.At}'s place).
 */
public final class PostgresDialect implements InputDialect {

    /** The one Postgres dialect: it has no options. */
    public static final PostgresDialect INSTANCE = new PostgresDialect();

    private PostgresDialect() {
    }

    @Override
    public SqlAst.Statement parse(String sql) {
        return SqlParser.parse(sql);
    }

    // ------------------------------------------------------------------ names

    /** Postgres's folding of an unquoted identifier: ASCII letters to lower case, nothing else. */
    @Override
    public String fold(String text) {
        return SqlToPure.fold(text);
    }

    /** THE identifier rule (SqlToPure's class comment): the name of {@code names} that {@code id} means, or null for
     *  none. */
    @Override
    public @com.legend.base.Nullable String resolve(SqlAst.Ident id, Iterable<String> names, String what) {
        if (id.quoted()) {
            for (String n : names) {
                if (n.equals(id.text())) {
                    return n;
                }
            }
            return null;
        }
        String folded = fold(id.text());
        List<String> loose = new ArrayList<>();
        for (String n : names) {
            if (n.equals(folded)) {
                return n;
            }
            if (fold(n).equals(folded) && !loose.contains(n)) {
                loose.add(n);
            }
        }
        if (loose.size() > 1) {
            throw SqlTranslationException.invalid(what + " reference \"" + id.text() + "\" is ambiguous: it matches "
                    + String.join(", ", loose.stream().map(n -> "\"" + n + "\"").toList())
                    + "; quote the name to choose one", id.pos());
        }
        return loose.isEmpty() ? null : loose.get(0);
    }

    /** Postgres's hint for a quoted name that differs only in case from a real one. */
    @Override
    public String hint(SqlAst.Ident name, List<String> names) {
        if (name.quoted()) {
            for (String n : names) {
                if (fold(n).equals(fold(name.text()))) {
                    return "; perhaps you meant \"" + n + "\"";
                }
            }
        }
        return "";
    }

    // ------------------------------------------------------------------ order and output names

    /** Postgres sorts NULL after every value: last ascending, first descending. */
    @Override
    public boolean nullsFirst(boolean descending) {
        return descending;
    }

    @Override
    public String defaultName(SqlAst.SelectExpr item) {
        return defaultName(item.expr());
    }

    /** Postgres's name for an unnamed select item (its FigureColname, parse_target.c): a column's name, a function's,
     *  a subquery's column's; else a cast's type or {@code case}, where what they hold has no such name (so the outer
     *  of two casts names the item: 2026-10-17, the regression suite); else {@code ?column?}. */
    private static String defaultName(SqlAst.Expr e) {
        Named n = named(e);
        return n == null ? "?column?" : n.name();
    }

    /** A name FigureColname gives, and whether it is one an enclosing cast or CASE keeps (its strength 2). */
    private record Named(String name, boolean strong) {
    }

    private static @com.legend.base.Nullable Named named(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.ColumnRef ref -> new Named(ref.parts().get(ref.parts().size() - 1).text(), true);
            case SqlAst.FunctionCall f -> new Named(SqlToPure.functionName(f.name().get(f.name().size() - 1)), true);
            case SqlAst.Window w -> new Named(SqlToPure.functionName(w.call().name().get(w.call().name().size() - 1)),
                    true);
            // a subscripted value keeps its name (Postgres's FigureColname reads through the indirection)
            case SqlAst.Subscript sub -> named(sub.base());
            case SqlAst.ArrayConstructor a -> new Named("array", true);
            case SqlAst.Exists x -> new Named("exists", true);
            case SqlAst.ScalarSubquery q -> firstColumn(q.query());
            // CASE is named by its ELSE's value where that has a name of its own (CASE ... ELSE b END is b)
            case SqlAst.Case c -> strongOr(c.otherwise() == null ? null : named(c.otherwise()), "case");
            // the operand's name once: twice a level was 2^n for n nested casts
            case SqlAst.Cast c -> strongOr(named(c.operand()), typeName(c.type()));
            // DATE '...' is a cast of the string, so its type's name
            case SqlAst.TypedLit t -> new Named(typeName(t.type()), false);
            default -> null;
        };
    }

    private static Named strongOr(@com.legend.base.Nullable Named inner, String weak) {
        return inner != null && inner.strong() ? inner : new Named(weak, false);
    }

    /** A scalar subquery's name: its first select item's, the leftmost branch's of a set operation; none for a
     *  {@code *}. */
    private static @com.legend.base.Nullable Named firstColumn(SqlAst.Statement query) {
        return switch (query) {
            case SqlAst.Select s -> s.items().isEmpty() || !(s.items().get(0) instanceof SqlAst.SelectExpr x) ? null
                    : new Named(x.alias() != null ? x.alias().text() : defaultName(x.expr()), true);
            case SqlAst.SetOperation op -> firstColumn(op.left());
            case SqlAst.With w -> firstColumn(w.body());
            case SqlAst.Write w -> null;
        };
    }

    /** A type's name as Postgres's grammar gives it (gram.y's SystemTypeName for the SQL standard's spellings), the
     *  last part of a qualified one. */
    private static String typeName(SqlAst.TypeName type) {
        String name = type.name();
        return switch (name) {
            case "int", "integer" -> "int4";
            case "smallint" -> "int2";
            case "bigint" -> "int8";
            case "real" -> "float4";
            case "double precision" -> "float8";
            // FLOAT(p): float4 to 24 binary digits, else float8
            case "float" -> !type.modifiers().isEmpty() && type.modifiers().get(0) <= 24 ? "float4" : "float8";
            case "decimal", "dec" -> "numeric";
            case "boolean" -> "bool";
            case "character", "char", "nchar" -> "bpchar";
            case "character varying", "char varying" -> "varchar";
            case "timestamp", "timestamp without time zone" -> "timestamp";
            case "timestamp with time zone" -> "timestamptz";
            case "time", "time without time zone" -> "time";
            case "time with time zone" -> "timetz";
            default -> name.substring(name.lastIndexOf('.') + 1);
        };
    }

    // ------------------------------------------------------------------ types

    /** A column's store type, where the translation reads it beyond the column's kind (2026-10-17, the regression
     *  suite): a {@code CHAR(n)}, whose padding a cast to text drops, and a {@code REAL}, as which Postgres reads a
     *  string compared with it. */
    record Stored(String type) implements InputDialect.TypeDetail {

        /** A blank-padded {@code CHAR(n)}. */
        boolean padded() {
            return type.startsWith("CHAR(");
        }

        /** A single-precision {@code REAL}. */
        boolean single() {
            return type.equals("REAL");
        }
    }

    /** A value Postgres types smallint where the island's integer is wider: a cast to smallint or an int2 constant,
     *  or a column holding one; never a smallint column of a table, which the database reads at its own width
     *  (2026-10-17, the review of the corpus program's step 1). */
    record Smallint() implements InputDialect.TypeDetail {
    }

    private static final Smallint SMALLINT = new Smallint();

    @Override
    public InputDialect.TypeDetail narrowInteger() {
        return SMALLINT;
    }

    @Override
    public boolean padded(InputDialect.@com.legend.base.Nullable TypeDetail detail) {
        return detail instanceof Stored s && s.padded();
    }

    @Override
    public boolean singlePrecision(InputDialect.@com.legend.base.Nullable TypeDetail detail) {
        return detail instanceof Stored s && s.single();
    }

    @Override
    public InputDialect.@com.legend.base.Nullable TypeDetail detailOf(String pureType,
            @com.legend.base.Nullable String storeType) {
        return storeType != null && (storeType.startsWith("CHAR(") || storeType.equals("REAL")) ? new Stored(storeType)
                : null;
    }

    /** A column's kind is its Pure type's: Postgres's translation reads no store type but for its details
     *  ({@link Stored}). */
    @Override
    public Kind kindOf(String pureType, @com.legend.base.Nullable String storeType) {
        int paren = pureType.indexOf('(');
        String base = paren < 0 ? pureType : pureType.substring(0, paren);
        return switch (base) {
            case "Integer" -> Kind.INTEGER;
            case "Float" -> Kind.FLOAT;
            case "Decimal" -> Kind.DECIMAL;
            case "Number" -> Kind.NUMBER;
            case "String" -> Kind.STRING;
            case "Boolean" -> Kind.BOOLEAN;
            case "StrictDate" -> Kind.DATE;
            case "DateTime" -> Kind.DATETIME;
            // a Variant is Postgres's jsonb (register S49)
            case "Variant", SqlToPure.VARIANT -> Kind.JSON;
            default -> Kind.OTHER;
        };
    }

    /** An integer constant is Postgres's integer (Pure's Integer, whatever its width), a numeric one its exact numeric,
     *  a string constant text, {@code DATE '...'} and the other typed constants their types. */
    @Override
    public Tx literal(SqlAst.Expr constant) {
        return switch (constant) {
            case SqlAst.IntegerLit n -> Tx.value(SqlToPure.integer(n.value()), Kind.INTEGER, false);
            case SqlAst.NumericLit n -> Tx.value(new CDecimal(n.value()), Kind.DECIMAL, false);
            case SqlAst.StringLit str -> Tx.value(new CString(str.value()), Kind.STRING, false);
            case SqlAst.BooleanLit b -> SqlToPure.booleanValue(new CBoolean(b.value()), false);
            case SqlAst.NullLit n -> Tx.value(SqlToPure.empty(), Kind.NULL, true);
            case SqlAst.TypedLit lit -> PostgresCasts.typedLiteral(lit);
            default -> throw new IllegalArgumentException("not a constant: " + constant);
        };
    }

    // ------------------------------------------------------------------ meanings: Postgres's rules (PostgresOperators,
    // PostgresCasts, PostgresFunctions, PostgresDates, PostgresJsonb), which read the translator throughout

    @Override
    public Tx binary(SqlAst.Binary b, SqlToPure.At at) {
        return PostgresOperators.binary(at.translator(), b, at.scope(), at.row());
    }

    @Override
    public Tx unary(SqlAst.Unary u, SqlToPure.At at) {
        return PostgresOperators.unary(at.translator(), u, at.scope(), at.row());
    }

    @Override
    public Tx between(SqlAst.Between b, SqlToPure.At at) {
        return PostgresOperators.between(at.translator(), b, at.scope(), at.row());
    }

    @Override
    public Tx inList(SqlAst.InList in, SqlToPure.At at) {
        return PostgresOperators.inList(at.translator(), in, at.scope(), at.row());
    }

    @Override
    public Tx isDistinct(SqlAst.IsDistinct d, SqlToPure.At at) {
        return PostgresOperators.isDistinct(at.translator(), d, at.scope(), at.row());
    }

    @Override
    public Tx quantifiedTest(SqlAst.BinaryOp op, SqlAst.Expr left, Tx each, SqlAst.Pos pos, SqlToPure.At at) {
        return PostgresOperators.quantifiedTest(op, left, each, pos, at);
    }

    @Override
    public Tx readString(SqlAst.StringLit s, Kind kind) {
        return PostgresCasts.fromString(s, kind);
    }

    @Override
    public String typeName(Kind kind) {
        return PostgresOperators.postgresName(kind);
    }

    @Override
    public @com.legend.base.Nullable Tx operator(SqlAst.Operator o, SqlToPure.At at) {
        return PostgresJsonb.readOperator(o, at);
    }

    @Override
    public @com.legend.base.Nullable Tx subscript(SqlAst.Subscript s, SqlToPure.At at) {
        return PostgresJsonb.subscript(at.translator(), s, at.scope(), at.row());
    }

    @Override
    public boolean isAggregate(String name) {
        return PostgresAggregates.postgresAggregate(name);
    }

    @Override
    public SqlToPure.Aggregation aggregate(SqlAst.FunctionCall f, SqlToPure.Grouping at) {
        return PostgresAggregates.ownAggregate(at.translator(), f, at.input(), at.name(), at.names());
    }

    /** Postgres's resolution of a set operation's column ("UNION, CASE, and Related Constructs"; 2026-10-17, the
     *  regression suite's VALUES lists, which Postgres defines as UNION ALL): an untyped NULL takes the other side's
     *  type (a UNION's only: an INTERSECT's or an EXCEPT's row match is refused over it), and an integer or a numeric
     *  beside a double precision is a double precision. Not a numeric beside an integer, or a numeric beside a NULL:
     *  the typer unites no two Decimal precisions (2.5D is a Decimal(38,1), toDecimal(1) a Decimal(38,18)), and a
     *  cast to Decimal is the database's DECIMAL, whose scale is its own (DuckDB's rounded 2.5 to 3, measured
     *  2026-10-17), so they stay refused. Null where nothing changes. */
    @Override
    public SqlToPure.@com.legend.base.Nullable ColumnType setColumn(SqlAst.SetOperation s, int i,
            SqlToPure.ColumnType left, SqlToPure.ColumnType right) {
        Kind l = left.kind();
        Kind r = right.kind();
        if (l == Kind.INTEGER && r == Kind.INTEGER && (SMALLINT.equals(left.detail())
                || SMALLINT.equals(right.detail()))) {
            // a smallint column of a set operation stays one (an operation keeps no detail it is not given), and
            // beside another integer too, which the island cannot tell from a smallint column: arithmetic over the
            // column is refused (2026-10-17)
            return new SqlToPure.ColumnType(Kind.INTEGER, left.nullable() || right.nullable(), SMALLINT);
        }
        if (l == r) {
            return null;
        }
        if (l == Kind.NULL || r == Kind.NULL) {
            SqlToPure.ColumnType other = l == Kind.NULL ? right : left;
            return s.op() != SqlAst.SetOp.UNION || exact(other.kind())
                    || !untypedNull(l == Kind.NULL ? s.left() : s.right(), i) ? null
                    : new SqlToPure.ColumnType(other.kind(), true, other.detail());
        }
        return toDouble(l, r) ? new SqlToPure.ColumnType(Kind.FLOAT, left.nullable() || right.nullable(), null) : null;
    }

    /** One side's column of a set operation as the column's type ({@link #setColumn}): an untyped NULL as the type's
     *  NULL, an integer or a numeric as a double precision. */
    @Override
    public @com.legend.base.Nullable Tx setValue(SqlAst.SetOperation s, int i, Tx side, SqlToPure.ColumnType other,
            boolean left) {
        if (side.kind() == Kind.NULL) {
            if (other.kind() == Kind.NULL || s.op() != SqlAst.SetOp.UNION || exact(other.kind())
                    || !untypedNull(left ? s.left() : s.right(), i)) {
                return null;
            }
            // a boolean's NULL is a condition's, both its cases (2026-10-17: VALUES (true), (NULL) threw)
            Tx typed = SqlToPure.nullOf(other.kind());
            return other.kind() == Kind.BOOLEAN ? typed
                    : Tx.value(typed.vs(), other.kind(), true, other.detail());
        }
        return side.kind() != Kind.FLOAT && toDouble(side.kind(), other.kind())
                ? SqlToPure.strict(Kind.FLOAT, List.of(side), v -> SqlToPure.call("toFloat", v.get(0))) : null;
    }

    /** Whether column {@code i} of {@code side}, a set operation's branch, is a NULL Postgres has not typed yet: a
     *  bare NULL in a plain SELECT of the operation's own (no DISTINCT, grouping, sort or slice). Any other NULL it
     *  has resolved as text first: a subquery's, a WITH query's, a nested operation's, a DISTINCT's (the review of
     *  2026-10-17: SELECT NULL UNION SELECT NULL UNION SELECT 1 is its error). */
    private static boolean untypedNull(SqlAst.Statement side, int i) {
        return switch (side) {
            case SqlAst.Select q -> !q.distinct() && q.groupBy().isEmpty() && q.having() == null
                    && q.windows().isEmpty() && q.orderBy().isEmpty() && q.limit() == null && q.offset() == null
                    && q.items().stream().noneMatch(item -> item instanceof SqlAst.AllColumns) && i < q.items().size()
                    && q.items().get(i) instanceof SqlAst.SelectExpr x && x.expr() instanceof SqlAst.NullLit;
            // a VALUES list's rows of NULLs in this column: its rows are typed together, NULLs beside its values
            // taking theirs (2026-10-17: VALUES (1, 1), (2, 2), (3, NULL), (4, NULL) is an integer column)
            case SqlAst.SetOperation o -> o.values() && o.orderBy().isEmpty() && o.limit() == null
                    && o.offset() == null && untypedNull(o.left(), i) && untypedNull(o.right(), i);
            case SqlAst.With w -> false;
            case SqlAst.Write w -> false;
        };
    }

    /** A subquery's or a WITH query's untyped NULL is text to the query reading it: Postgres resolves the untyped
     *  outputs of a query it reads from as text (2026-10-17: x + 1 over (VALUES (NULL)) v(x) is its error). */
    @Override
    public boolean untypedNullIsText() {
        return true;
    }

    /** A numeric's kinds, which a set operation unites with no other (see {@link #setColumn}). */
    private static boolean exact(Kind k) {
        return k == Kind.DECIMAL || k == Kind.NUMBER;
    }

    /** Whether one side is a double precision and the other an integer or a numeric. */
    private static boolean toDouble(Kind a, Kind b) {
        java.util.Set<Kind> exact = java.util.Set.of(Kind.INTEGER, Kind.DECIMAL, Kind.NUMBER);
        return a == Kind.FLOAT && exact.contains(b) || b == Kind.FLOAT && exact.contains(a);
    }

    /** Postgres's resolution of a CASE's results ("UNION, CASE, and Related Constructs"). */
    @Override
    public SqlToPure.Common unify(String what, List<SqlAst.Expr> exprs, List<Tx> values, SqlAst.Pos pos) {
        // a string resolved with a real is read as a real there (2026-10-17, as a comparison's: SqlToPure.single)
        if (values.stream().anyMatch(v -> singlePrecision(v.detail()))) {
            for (SqlAst.Expr e : exprs) {
                if (e instanceof SqlAst.StringLit s) {
                    throw SqlToPure.stringWithAReal(s.pos());
                }
            }
        }
        return PostgresOperators.common(what, exprs, values, pos);
    }

    @Override
    public @com.legend.base.Nullable FunctionRule function(String name) {
        return PostgresFunctions.rule(name);
    }

    @Override
    public Tx cast(SqlAst.Cast c, SqlToPure.At at) {
        return PostgresCasts.cast(at.translator(), c, at.scope(), at.row());
    }

    @Override
    public Tx like(SqlAst.Like l, SqlToPure.At at) {
        return PostgresOperators.like(at.translator(), l, at.scope(), at.row());
    }

    // ------------------------------------------------------------------ Postgres's wording

    @Override
    public String message(Problem problem, Object... args) {
        return switch (problem) {
            case NO_SUCH_QUALIFIED_COLUMN -> "column " + args[0] + "." + args[1] + " does not exist" + args[2];
            case NO_SUCH_COLUMN -> "column \"" + args[0] + "\" does not exist" + args[1];
            case AMBIGUOUS_COLUMN -> "column reference \"" + args[0] + "\" is ambiguous";
            case AMBIGUOUS_TABLE -> "table reference \"" + args[0] + "\" is ambiguous";
            case NO_SUCH_TABLE -> "missing FROM-clause entry for table \"" + args[0] + "\"";
            case NOT_GROUPED -> "column \"" + args[0] + "\" must appear in the GROUP BY clause or be used in an"
                    + " aggregate function";
            case NESTED_AGGREGATE -> "aggregate function calls cannot be nested";
            case DUPLICATE_TABLE -> "table name \"" + args[0] + "\" specified more than once";
            case DUPLICATE_USING_COLUMN -> "common column name \"" + args[0] + "\" appears more than once in "
                    + args[1] + " table";
            case NO_SUCH_USING_COLUMN -> "column \"" + args[0] + "\" specified in USING clause does not exist in "
                    + args[1] + " table";
            case NO_SUCH_WINDOW -> "window \"" + args[0] + "\" does not exist";
            case DISTINCT_ORDER_KEY -> "for SELECT DISTINCT, ORDER BY expressions must appear in select list";
            case SET_WIDTHS -> "each " + args[0] + " query must have the same number of columns";
        };
    }
}
