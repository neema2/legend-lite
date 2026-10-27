// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

/**
 * The syntax tree of a Postgres SQL statement: what was written, with where it was written. It is pure data and
 * decides no meaning: an identifier keeps the case it was written in and says whether it was quoted, so the reader
 * that resolves names (the translator to Pure, later the wire server) applies the folding rule, once.
 *
 * <p>Grammar-first: each node is shaped as Postgres's grammar has it, not as one consumer needs it. The tree grows
 * with the statements the parser accepts; a construct the parser does not accept is refused there, by name.
 */
public final class SqlAst {

    private SqlAst() {
    }

    /** A place in the SQL text: 1-based line and column. */
    public record Pos(int line, int column) {
    }

    /** An identifier as written: its text (an unquoted one keeps its written case) and whether it was double-quoted. */
    public record Ident(String text, boolean quoted, Pos pos) {
        public Ident {
            Objects.requireNonNull(text, "text");
            Objects.requireNonNull(pos, "pos");
        }
    }

    // ------------------------------------------------------------------ statements

    /** A query: a SELECT, a set operation of two queries, or a query with a WITH clause; or a statement that writes
     *  a table (the writes design's leg 4, docs/WRITES_DESIGN_2026_10_18.md §5): an INSERT, an UPDATE, a DELETE,
     *  which only an island's own statement may be ({@link Write}). */
    public sealed interface Statement permits Select, SetOperation, With, Write {
        Pos pos();
    }

    /** A statement that writes a table: never a query's part, only an island's own statement. */
    public sealed interface Write extends Statement permits Insert, Update, Delete {
    }

    /** {@code INSERT INTO target [(columns)] source [ON CONFLICT ...] [RETURNING ...]}: the source a query, the
     *  columns empty for every column of the table in its order; {@code values} when the source is a VALUES list,
     *  whose literals Postgres types by the columns they are written into (not text, as a query's). */
    public record Insert(FromItem target, List<Ident> columns, Statement source, boolean values,
            @com.legend.base.Nullable OnConflict onConflict, List<SelectItem> returning, Pos pos)
            implements Write {
        public Insert {
            columns = List.copyOf(columns);
            returning = List.copyOf(returning);
        }
    }

    /** {@code ON CONFLICT [(keys)] DO NOTHING} ({@code update} null) or {@code DO UPDATE SET ... [WHERE]}. */
    public record OnConflict(List<Ident> keys, @com.legend.base.Nullable List<Assignment> update,
            @com.legend.base.Nullable Expr where, Pos pos) {
        public OnConflict {
            keys = List.copyOf(keys);
            update = update == null ? null : List.copyOf(update);
        }
    }

    /** {@code column = value} of a SET. */
    public record Assignment(Ident column, Expr value, Pos pos) {
    }

    /** {@code UPDATE target SET assignments [FROM ...] [WHERE] [RETURNING ...]}. */
    public record Update(FromItem target, List<Assignment> sets, List<FromItem> from,
            @com.legend.base.Nullable Expr where, List<SelectItem> returning, Pos pos) implements Write {
        public Update {
            sets = List.copyOf(sets);
            from = List.copyOf(from);
            returning = List.copyOf(returning);
        }
    }

    /** {@code DELETE FROM target [USING ...] [WHERE] [RETURNING ...]}. */
    public record Delete(FromItem target, List<FromItem> using, @com.legend.base.Nullable Expr where,
            List<SelectItem> returning, Pos pos) implements Write {
        public Delete {
            using = List.copyOf(using);
            returning = List.copyOf(returning);
        }
    }

    /** {@code SELECT [DISTINCT] items [FROM from, ...] [WHERE] [GROUP BY] [HAVING] [ORDER BY] [LIMIT] [OFFSET]}: the
     *  FROM items as written, separated by commas (empty when there is no FROM). */
    public record Select(boolean distinct, List<SelectItem> items, List<FromItem> from,
            @com.legend.base.Nullable Expr where, List<Expr> groupBy, @com.legend.base.Nullable Expr having,
            List<NamedWindow> windows, List<SortItem> orderBy, @com.legend.base.Nullable Expr limit,
            @com.legend.base.Nullable Expr offset, Pos pos) implements Statement {
        public Select {
            items = List.copyOf(items);
            from = List.copyOf(from);
            groupBy = List.copyOf(groupBy);
            windows = List.copyOf(windows);
            orderBy = List.copyOf(orderBy);
        }
    }

    /** {@code UNION}, {@code INTERSECT}, {@code EXCEPT}. */
    public enum SetOp { UNION, INTERSECT, EXCEPT }

    /** {@code left op [ALL] right [ORDER BY] [LIMIT] [OFFSET]}: the sort and the slice are the whole result's. */
    public record SetOperation(SetOp op, boolean all, Statement left, Statement right, List<SortItem> orderBy,
            @com.legend.base.Nullable Expr limit, @com.legend.base.Nullable Expr offset, Pos pos, boolean values)
            implements Statement {
        public SetOperation {
            orderBy = List.copyOf(orderBy);
        }

        /** A set operation as written; {@code values} marks the UNION ALL a VALUES list is read as (2026-10-17: its
         *  rows are typed together, where a written UNION ALL's are typed a pair at a time). */
        public SetOperation(SetOp op, boolean all, Statement left, Statement right, List<SortItem> orderBy,
                @com.legend.base.Nullable Expr limit, @com.legend.base.Nullable Expr offset, Pos pos) {
            this(op, all, left, right, orderBy, limit, offset, pos, false);
        }
    }

    /** {@code WITH [RECURSIVE] name [(columns)] AS (query), ... body}. */
    public record With(boolean recursive, List<Cte> ctes, Statement body, Pos pos) implements Statement {
        public With {
            ctes = List.copyOf(ctes);
        }
    }

    /** One common table expression: its name, its column names when written, its query. */
    public record Cte(Ident name, List<Ident> columns, Statement query, Pos pos) {
        public Cte {
            columns = List.copyOf(columns);
        }
    }

    /** One entry of a select list. */
    public sealed interface SelectItem permits AllColumns, SelectExpr {
        Pos pos();
    }

    /** {@code *} or {@code qualifier.*}. */
    public record AllColumns(@com.legend.base.Nullable Ident qualifier, Pos pos) implements SelectItem {
    }

    /** {@code expr [[AS] alias]}; {@code text} is the expression as written, when the reader keeps it (a dialect
     *  that names an unaliased item by its text), else null (Postgres's reader). */
    public record SelectExpr(Expr expr, @com.legend.base.Nullable Ident alias, Pos pos,
            @com.legend.base.Nullable String text) implements SelectItem {

        /** An item whose text is not kept. */
        public SelectExpr(Expr expr, @com.legend.base.Nullable Ident alias, Pos pos) {
            this(expr, alias, pos, null);
        }
    }

    /** {@code expr [ASC | DESC] [NULLS FIRST | NULLS LAST]}; {@code nullsFirst} is null when not written. */
    public record SortItem(Expr expr, boolean descending, @com.legend.base.Nullable Boolean nullsFirst, Pos pos) {
    }

    // ------------------------------------------------------------------ FROM

    /** One item of a FROM clause: a table, a function, a subquery, or a join of two items. */
    public sealed interface FromItem permits TableName, TableFunction, SubqueryFrom, Join {
        Pos pos();
    }

    /** A table by (possibly qualified) name: {@code schema.table [[AS] alias [(columns)]]}. */
    public record TableName(List<Ident> name, @com.legend.base.Nullable Ident alias, List<Ident> columnAliases,
            Pos pos) implements FromItem {
        public TableName {
            name = List.copyOf(name);
            columnAliases = List.copyOf(columnAliases);
        }
    }

    /** A function in FROM: {@code name(args) [[AS] alias [(columns)]]}. */
    public record TableFunction(List<Ident> name, List<Arg> args, @com.legend.base.Nullable Ident alias,
            List<Ident> columnAliases, Pos pos) implements FromItem {
        public TableFunction {
            name = List.copyOf(name);
            args = List.copyOf(args);
            columnAliases = List.copyOf(columnAliases);
        }
    }

    /** {@code (query) [[AS] alias [(columns)]]}: Postgres 16 lets the alias go unwritten. */
    public record SubqueryFrom(Statement query, @com.legend.base.Nullable Ident alias, List<Ident> columnAliases,
            boolean lateral, Pos pos) implements FromItem {
        public SubqueryFrom {
            columnAliases = List.copyOf(columnAliases);
        }
    }

    /** A join's kind. */
    public enum JoinType { INNER, LEFT, RIGHT, FULL, CROSS }

    /** {@code left [NATURAL] type JOIN right [ON condition | USING (columns)]}; a CROSS JOIN, and a comma in FROM,
     *  have neither. */
    public record Join(JoinType type, boolean natural, FromItem left, FromItem right,
            @com.legend.base.Nullable Expr on, List<Ident> using, Pos pos) implements FromItem {
        public Join {
            using = List.copyOf(using);
        }
    }

    /** A function argument, positional or named ({@code name => value}). */
    public record Arg(@com.legend.base.Nullable Ident name, Expr value) {
    }

    // ------------------------------------------------------------------ expressions

    /** A value expression. */
    public sealed interface Expr permits ColumnRef, IntegerLit, NumericLit, StringLit, BooleanLit, NullLit, TypedLit,
            Param, Unary, Binary, Operator, IsNull, IsTruth, IsDistinct, InList, Between, Like, Case, FunctionCall,
            Cast, ScalarSubquery, Exists, InSubquery, Quantified, Window, Subscript, ArrayConstructor {
        Pos pos();
    }

    /** A column reference, possibly qualified: {@code col}, {@code t.col}. */
    public record ColumnRef(List<Ident> parts, Pos pos) implements Expr {
        public ColumnRef {
            parts = List.copyOf(parts);
        }
    }

    /** An integer constant: digits, no point, no exponent. */
    public record IntegerLit(BigInteger value, Pos pos) implements Expr {
    }

    /** A numeric constant: a point or an exponent ({@code numeric} in Postgres; exact). */
    public record NumericLit(BigDecimal value, Pos pos) implements Expr {
    }

    /** A string constant. */
    public record StringLit(String value, Pos pos) implements Expr {
    }

    /** {@code TRUE} or {@code FALSE}. */
    public record BooleanLit(boolean value, Pos pos) implements Expr {
    }

    /** {@code NULL}. */
    public record NullLit(Pos pos) implements Expr {
    }

    /** A constant of a named type: {@code type 'string'} ({@code DATE '2024-01-31'}). The type name is as written. */
    public record TypedLit(TypeName type, String value, Pos pos) implements Expr {
    }

    /** A positional parameter: {@code $1}. IQ island, 2026-10-14 (M1e): or a named one, {@code name} as written with
     *  its marker ({@code @x}, {@code :x}; null for a positional one), {@code index} then one per name. */
    public record Param(int index, @com.legend.base.Nullable String name, Pos pos) implements Expr {

        /** A positional parameter. */
        public Param(int index, Pos pos) {
            this(index, null, pos);
        }
    }

    /** A prefix operator. */
    public enum UnaryOp { MINUS, PLUS, NOT }

    /** {@code -x}, {@code +x}, {@code NOT x}. */
    public record Unary(UnaryOp op, Expr operand, Pos pos) implements Expr {
    }

    /** A binary operator. */
    public enum BinaryOp { OR, AND, EQ, NE, LT, LE, GT, GE, PLUS, MINUS, TIMES, DIVIDE, MODULO, POWER, CONCAT }

    /** {@code left op right}. */
    public record Binary(BinaryOp op, Expr left, Expr right, Pos pos) implements Expr {
    }

    /** Any other operator, as Postgres reads one ({@code ~}, {@code ->>}, {@code @>}, ...): its spelling and its
     *  operands, a prefix operator's left one null. Postgres gives them all one precedence (its {@code Op}); what one
     *  means is the translator's business, which refuses those it does not know. */
    public record Operator(String operator, @com.legend.base.Nullable Expr left, Expr right, Pos pos)
            implements Expr {
    }

    /** {@code ARRAY[e, ...]}: an array of the elements' values. */
    public record ArrayConstructor(List<Expr> elements, Pos pos) implements Expr {
        public ArrayConstructor {
            elements = List.copyOf(elements);
        }
    }

    /** A subscript: {@code base[index]} (a jsonb member or element, or an array's element). */
    public record Subscript(Expr base, Expr index, Pos pos) implements Expr {
    }

    /** {@code x IS [NOT] NULL}. */
    public record IsNull(Expr operand, boolean negated, Pos pos) implements Expr {
    }

    /** A truth value {@code IS} tests for. */
    public enum Truth { TRUE, FALSE, UNKNOWN }

    /** {@code x IS [NOT] TRUE | FALSE | UNKNOWN}. */
    public record IsTruth(Expr operand, Truth truth, boolean negated, Pos pos) implements Expr {
    }

    /** {@code x IS [NOT] DISTINCT FROM y}. */
    public record IsDistinct(Expr left, Expr right, boolean negated, Pos pos) implements Expr {
    }

    /** {@code x [NOT] IN (v, ...)}. */
    public record InList(Expr operand, List<Expr> values, boolean negated, Pos pos) implements Expr {
        public InList {
            values = List.copyOf(values);
        }
    }

    /** {@code x [NOT] BETWEEN low AND high}. */
    public record Between(Expr operand, Expr low, Expr high, boolean negated, Pos pos) implements Expr {
    }

    /** {@code x [NOT] LIKE | ILIKE pattern [ESCAPE e]}. */
    public record Like(Expr operand, Expr pattern, @com.legend.base.Nullable Expr escape, boolean negated,
            boolean caseInsensitive, Pos pos) implements Expr {
    }

    /** {@code CASE [operand] WHEN ... THEN ... [ELSE ...] END}; a simple CASE has an operand. */
    public record Case(@com.legend.base.Nullable Expr operand, List<When> whens,
            @com.legend.base.Nullable Expr otherwise, Pos pos) implements Expr {
        public Case {
            whens = List.copyOf(whens);
        }
    }

    /** {@code WHEN condition THEN result}. */
    public record When(Expr condition, Expr result) {
    }

    /** {@code name([DISTINCT] args [ORDER BY ...]) [WITHIN GROUP (ORDER BY ...)] [FILTER (WHERE ...)]} or
     *  {@code name(*)}: the last three an aggregate's only. */
    public record FunctionCall(List<Ident> name, List<Expr> args, boolean distinct, boolean star,
            List<SortItem> orderBy, List<SortItem> withinGroup, @com.legend.base.Nullable Expr filter, Pos pos)
            implements Expr {
        public FunctionCall {
            name = List.copyOf(name);
            args = List.copyOf(args);
            orderBy = List.copyOf(orderBy);
            withinGroup = List.copyOf(withinGroup);
        }

        /** A call with none of an aggregate's clauses. */
        public FunctionCall(List<Ident> name, List<Expr> args, boolean distinct, boolean star, Pos pos) {
            this(name, args, distinct, star, List.of(), List.of(), null, pos);
        }
    }

    /** {@code (query)} as a value: the one column of its one row, or NULL when it has none. */
    public record ScalarSubquery(Statement query, Pos pos) implements Expr {
    }

    /** {@code EXISTS (query)}. */
    public record Exists(Statement query, Pos pos) implements Expr {
    }

    /** {@code x [NOT] IN (query)}. */
    public record InSubquery(Expr operand, Statement query, boolean negated, Pos pos) implements Expr {
    }

    /** {@code x op ANY | SOME | ALL (query)}; {@code all} for ALL. */
    public record Quantified(BinaryOp op, boolean all, Expr left, Statement query, Pos pos) implements Expr {
    }

    /** A window function call: {@code f(args) [FILTER (WHERE ...)] OVER (window)} or {@code OVER name}. */
    public record Window(FunctionCall call, WindowSpec spec, Pos pos) implements Expr {
    }

    /** A window: {@code [name] [PARTITION BY ...] [ORDER BY ...] [frame]}, or a reference to a named one. */
    public record WindowSpec(@com.legend.base.Nullable Ident base, List<Expr> partitionBy, List<SortItem> orderBy,
            @com.legend.base.Nullable Frame frame, Pos pos) {
        public WindowSpec {
            partitionBy = List.copyOf(partitionBy);
            orderBy = List.copyOf(orderBy);
        }
    }

    /** A frame's unit. */
    public enum FrameUnit { ROWS, RANGE, GROUPS }

    /** {@code unit BETWEEN start AND end} (or {@code unit start}, ending at the current row), with any EXCLUDE. */
    public record Frame(FrameUnit unit, FrameBound start, FrameBound end, @com.legend.base.Nullable String exclude,
            Pos pos) {
    }

    /** A frame bound's kind. */
    public enum BoundKind { UNBOUNDED_PRECEDING, PRECEDING, CURRENT_ROW, FOLLOWING, UNBOUNDED_FOLLOWING }

    /** A frame bound: its kind, and the offset of a PRECEDING or FOLLOWING one. */
    public record FrameBound(BoundKind kind, @com.legend.base.Nullable Expr offset) {
    }

    /** A named window of the WINDOW clause. */
    public record NamedWindow(Ident name, WindowSpec spec) {
    }

    /** {@code CAST(x AS type)} or {@code x::type}. */
    public record Cast(Expr operand, TypeName type, Pos pos) implements Expr {
    }

    /** A type name as written, its words joined by single spaces ({@code double precision}), with its modifiers
     *  ({@code numeric(10,2)}). */
    public record TypeName(String name, List<Integer> modifiers, Pos pos) {
        public TypeName {
            modifiers = List.copyOf(modifiers);
        }
    }
}
