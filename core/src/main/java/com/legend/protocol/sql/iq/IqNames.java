// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlSyntaxException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * The name IQ gives a select item written without an alias: SQL Anywhere's own rendering of the expression (IQ's front
 * end names a result column; measured on SQL Anywhere 17, docs/IQ_SQL_STUDY_2026_10_13.md §9.4). It is a pure
 * rendering of the item as written -- a name, never a value -- read again from the item's text by
 * {@link IqParser#written}, which keeps what the tree does not (the parentheses, a simple CASE, an IF). The rules,
 * measured:
 * <ul>
 *   <li>At the top, the item's outer parentheses go, and so do a sign {@code +} and the parentheses inside it; a column
 *       then shows as its last part is written ({@code +a.id}, {@code (a.id)} are {@code id}); a parenthesized query
 *       shows as its one column does.</li>
 *   <li>A column inside an expression is qualified by the correlation name of the FROM item it reads (its alias, else
 *       its name), as declared ({@code id + 1} over {@code t a} is {@code a.id+1}); one written qualified keeps its parts
 *       as written ({@code A.ID+1}). A column of a derived table, of an enclosing query or of no known FROM item is not
 *       rendered (SQL Anywhere renders a derived table's column by its definition).</li>
 *   <li>Constants: an integer's digits; a numeric's value without trailing zeros or a zero before its point
 *       ({@code .5}), a whole one {@code n.0} up to an INT's largest and {@code n} beyond; a string quoted, its quotes
 *       doubled, each control character {@code \xHH}; NULL {@code null}.</li>
 *   <li>Operators: {@code + - * /} written tight ({@code 1+2*3}), a minus before a minus spaced ({@code 1- -2},
 *       {@code - -1}); {@code ||} and the comparisons spaced ({@code a || b}, {@code a = b}; {@code !=} is
 *       {@code <>}, {@code !<} is {@code >=}, {@code !>} is {@code <=}); {@code %} is {@code remainder(a,b)}; a sign
 *       {@code +} is dropped. Every written parenthesis stays (but the outermost).</li>
 *   <li>Words in lower case ({@code and}, {@code or}, {@code not}, {@code is null}, {@code between}, {@code like},
 *       {@code escape}, {@code case}, {@code when}, {@code then}, {@code else}, {@code end}, {@code if}, {@code endif}),
 *       a space after each unless what follows begins with {@code (} or {@code -} ({@code if(a = 1)},
 *       {@code then-1}). {@code END CASE} is {@code end}, {@code END IF} is {@code endif}; a simple CASE stays simple.
 *       {@code x IN (...)} is {@code x in( a,b ) }, its plain constants first; NOT before a LIKE, a BETWEEN, an IN or an
 *       IS NULL is written inside it ({@code x not like p}); {@code IS [NOT] DISTINCT FROM} is SQL Anywhere's
 *       {@code [not] x is not distinct from y} turned round; a truth test's NOT goes in front
 *       ({@code not x = 1 is true}).</li>
 *   <li>A call: its name as written (a reserved word in double quotes, {@code "CHAR"(65)}), its arguments joined by
 *       {@code ,} without spaces; a date part as written, in lower case ({@code DATEPART(mm,...)}); {@code CURRENT DATE}
 *       and {@code CURRENT TIMESTAMP} (or {@code CURRENT_TIMESTAMP}) in lower case, {@code f(*)} as {@code f()}
 *       (IQ island, 2026-10-14, measured). An aggregate's (M1c): {@code COUNT(*)} is {@code COUNT()}, DISTINCT is
 *       {@code distinct } before its argument, ALL is dropped, LIST's ORDER BY follows its arguments as
 *       {@code  order by k asc,j desc}; an aggregate is never in parentheses ({@code (SUM(x)) + 1} is
 *       {@code SUM(t.x)+1}).</li>
 *   <li>In a grouped query (M1c) IQ names a part of an item that is a GROUP BY item, or an aggregate the query
 *       computes, by the rendering it gives that item or aggregate where it first reads it ({@link Substitutes}).</li>
 *   <li>The name is cut to 255 characters.</li>
 * </ul>
 * Anything else -- a subquery inside the expression, a comment or a line break between its tokens (each leaves its
 * mark on SQL Anywhere's name in ways not read here), a name of more than 255 characters
 * outside ASCII in a database whose characters may take two bytes -- has no rendering here (null), and the dialect
 * refuses the item by name.
 */
final class IqNames {

    /** The longest name SQL Anywhere 17 gives a column (measured): a longer rendering is cut to it. */
    static final int LONGEST = 255;

    /** The rendering of a column inside an expression, qualified ({@code t.id}), or null when it is not rendered;
     *  {@code inAggregate}: inside an aggregate's arguments (M1c: a derived table's column is rendered there, measured:
     *  {@code SUM(d.x)}). */
    @FunctionalInterface
    interface Columns {
        @com.legend.base.Nullable String rendering(SqlAst.ColumnRef ref, boolean inAggregate);
    }

    /** What the parts of an item read in a grouped query render as (M1c): a part that is a GROUP BY item, or an
     *  aggregate the query computes, renders as IQ renders that item or aggregate where it first reads it, without the
     *  parentheses written around the part. */
    @FunctionalInterface
    interface Substitutes {
        /** The rendering {@code e}, a part of the item ({@code inAggregate}: inside an aggregate's arguments), takes in
         *  place of its own: null for its own, a {@link Part} of no text when it is not known. Asked of each part once,
         *  in the order the item is written. */
        @com.legend.base.Nullable Part replace(SqlAst.Expr e, boolean inAggregate);
    }

    /** A part's rendering in place of its own; no text when it is not known ({@link #UNKNOWN}). */
    record Part(@com.legend.base.Nullable String text) {
    }

    /** No part is replaced: an item of a query that does not group. */
    static final Substitutes NONE = (e, inAggregate) -> null;

    /** A {@link Substitutes} answer: the part's rendering is not known (the item is refused by name). */
    static final Part UNKNOWN = new Part(null);

    private final IqParser.Written written;
    private final Columns columns;
    private final Substitutes substitutes;
    /** Whether the parts rendered are inside an aggregate's arguments (an aggregate's are rendered by another). */
    private final boolean inAggregate;
    /** IQ island, 2026-10-14 (the third landing): how deep in CASE and IF values the parts rendered are -- a line
     *  break there is rendered as two spaces for each level ({@link #indent}). */
    private final int depth;

    private IqNames(IqParser.Written written, Columns columns, Substitutes substitutes, boolean inAggregate,
            int depth) {
        this.written = written;
        this.columns = columns;
        this.substitutes = substitutes;
        this.inAggregate = inAggregate;
        this.depth = depth;
    }

    /** The name IQ gives the unaliased item written {@code text} (its expression's text) under {@code settings}; null
     *  when it is not known (a parenthesized query's is its one item's: the dialect's to give). */
    static @com.legend.base.Nullable String name(@com.legend.base.Nullable String text, IqSettings settings,
            Columns columns) {
        return name(text, settings, columns, NONE);
    }

    /** The name IQ gives the unaliased item written {@code text} under {@code settings}, its parts replaced as
     *  {@code substitutes} says (M1c: an item of a grouped query); null when it is not known. */
    static @com.legend.base.Nullable String name(@com.legend.base.Nullable String text, IqSettings settings,
            Columns columns, Substitutes substitutes) {
        return name(text, settings, columns, substitutes, Chains.KEPT);
    }

    /**
     * IQ island, 2026-10-14 (review 3's M1, measured on SQL Anywhere 17): what SQL Anywhere does with the parentheses
     * inside an item that is a chain of {@code +} and {@code -} whose operands are all non-negative number constants or
     * products of them -- in a query without FROM or over DUMMY it keeps them ({@code 1+(2)}, {@code 1-(2-3)}); over a
     * table, a derived table or {@code sa_rowgenerator} it drops those around the chain's operands and its inner chains
     * ({@code 1+2}, {@code 1-2-3}, {@code 1+2*3}), keeping a factor's ({@code 1+(2)*3}); a chain over anything else
     * (a column, a negative number, NULL, a quotient) keeps them all. A FROM of DUMMY beside another item is not
     * measured: such a chain with a parenthesis is not named.
     */
    enum Chains { KEPT, DROPPED, UNKNOWN }

    /** {@link #name(String, IqProfile, Columns, Substitutes)}, with the item's query's {@link Chains}. */
    static @com.legend.base.Nullable String name(@com.legend.base.Nullable String text, IqSettings settings,
            Columns columns, Substitutes substitutes, Chains chains) {
        if (text == null) {
            return null;
        }
        IqParser.Written w;
        try {
            w = IqParser.written(text, settings);
        } catch (SqlSyntaxException e) {
            return null;
        }
        SqlAst.Expr e = w.expr();
        while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            e = u.operand();
        }
        if ((w.brokenLines() || w.commented())
                && !new IqNames(w, columns, substitutes, false, 0).breaksRead(w.expr(), 0)) {
            return null;
        }
        if (foldedCase(w.expr())) {
            // IQ island, 2026-10-14 (the third landing, measured): SQL Anywhere names an item over a table whose CASE
            // or IF has a constant test and a value that is not constant by the part the test leaves (SELECT IF 1 = 3
            // THEN ch ELSE vc ENDIF FROM t is vc, IF 1 = 1 THEN ch ENDIF is ch; with constant values, or over no
            // table, it is not folded), which is not read here
            return null;
        }
        // IQ island, 2026-10-15 (measured on IQ 17.1): an item that is EXTRACT is named as its operand would be
        // (EXTRACT(YEAR FROM a.dt) is dt, of dt + 1 a.dt+1); EXTRACT inside an item is not read (call)
        if (e instanceof SqlAst.FunctionCall x && extract(x)) {
            e = x.args().get(1);
        }
        // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): an item that is a CAST, or a CONVERT without a
        // style, is named as its operand would be (CAST(t.ts AS DATE) is ts, CAST(sm + 1 AS INT) t.sm+1, CAST(SUM(sm)
        // AS DOUBLE) SUM(t.sm), CAST(1 AS INT) 1, CAST(NULL AS INT) null)
        while (e instanceof SqlAst.Cast || e instanceof SqlAst.FunctionCall f && isConvertStyle(f)) {
            // IQ island, 2026-10-16 (measured on IQ 17.1): a CONVERT with a style too (CONVERT(VARCHAR(12), ts, 107)
            // is ts)
            e = e instanceof SqlAst.Cast c ? c.operand() : ((SqlAst.FunctionCall) e).args().get(0);
        }
        String name;
        if (e instanceof SqlAst.ColumnRef ref) {
            name = ref.parts().get(ref.parts().size() - 1).text();
        } else if (chains != Chains.KEPT && constantChain(e) && parenthesized(w, e, true)) {
            // (a line break in such a chain is not measured: refused below by breaksRead)
            name = chains == Chains.UNKNOWN ? null : new IqNames(w, columns, substitutes, false, 0).flat(e);
        } else {
            IqNames names = new IqNames(w, columns, substitutes, false, 0);
            Part replaced = substitutes.replace(e, false);
            name = replaced != null ? replaced.text() : names.bare(e);
        }
        if (name == null) {
            return null;
        }
        if (name.length() > LONGEST) {
            for (int i = 0; i < LONGEST && !settings.latin1(); i++) {
                if (name.charAt(i) > 0x7F) {
                    // a UTF-8 database cuts at 255 bytes, maybe inside a character
                    return null;
                }
            }
            name = name.substring(0, LONGEST);
        }
        return name;
    }

    /** Whether {@code e} is a chain of {@code +} and {@code -} (through parentheses) whose operands are all terms
     *  ({@link #term}). */
    private static boolean constantChain(SqlAst.Expr e) {
        return e instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS)
                && chainOperand(b.left()) && chainOperand(b.right());
    }

    private static boolean chainOperand(SqlAst.Expr e) {
        SqlAst.Expr x = unsigned(e);
        return constantChain(x) || term(x);
    }

    /** A non-negative number constant, or a product of them (a sign {@code +} is no sign). */
    private static boolean term(SqlAst.Expr e) {
        SqlAst.Expr x = unsigned(e);
        return x instanceof SqlAst.IntegerLit || x instanceof SqlAst.NumericLit
                || x instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.TIMES && term(b.left()) && term(b.right());
    }

    private static SqlAst.Expr unsigned(SqlAst.Expr e) {
        SqlAst.Expr x = e;
        while (x instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
            x = u.operand();
        }
        return x;
    }

    /** Whether a parenthesis is written inside chain {@code e} (but around {@code e} itself, at the top). */
    private static boolean parenthesized(IqParser.Written w, SqlAst.Expr e, boolean top) {
        if (!top && w.parensOf(e) > 0) {
            return true;
        }
        if (e instanceof SqlAst.Unary u) {
            return parenthesized(w, u.operand(), false);
        }
        return e instanceof SqlAst.Binary b && (parenthesized(w, b.left(), false)
                || parenthesized(w, b.right(), false));
    }

    /** A constant chain as SQL Anywhere names it over a table: its operands' and inner chains' parentheses dropped, a
     *  product's factors' kept. */
    private @com.legend.base.Nullable String flat(SqlAst.Expr e) {
        SqlAst.Expr x = unsigned(e);
        if (x instanceof SqlAst.Binary b && (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS)) {
            String l = flat(b.left());
            String r = flat(b.right());
            return l == null || r == null ? null : l + (b.op() == SqlAst.BinaryOp.PLUS ? "+" : "-") + r;
        }
        return bare(x);
    }

    /** {@code e}'s rendering with its parentheses (none around an aggregate, or a part replaced). */
    private @com.legend.base.Nullable String render(SqlAst.Expr e) {
        Part replaced = substitutes.replace(e, inAggregate);
        if ((aggregate(e) || e instanceof SqlAst.Window) && written.parensOf(e) > 0) {
            // SQL Anywhere keeps them around some aggregates and drops them around others (measured: (SUM(x)) + 1 is
            // SUM(t.x)+1, (SUM(x)) * 2 is (SUM(t.x))*2, (COUNT(*)) + 1 is (COUNT())+1), and IQ so around a window
            // (measured on IQ 17.1, 2026-10-15: (SUM(x) OVER ()) + 1 is SUM(t.x) over()+1, * 2 (SUM(t.x) over())*2):
            // not read
            return null;
        }
        if (replaced != null) {
            return replaced.text();
        }
        if (aggregate(e)) {
            return bare(e);
        }
        String core = bare(e);
        if (core == null) {
            return null;
        }
        StringBuilder b = new StringBuilder();
        for (int i = written.parensOf(e); i > 0; i--) {
            b.append('(');
        }
        b.append(core);
        for (int i = written.parensOf(e); i > 0; i--) {
            b.append(')');
        }
        return b.toString();
    }

    /** {@code e}'s rendering without the parentheses around it. */
    private @com.legend.base.Nullable String bare(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.ColumnRef ref -> columns.rendering(ref, inAggregate);
            case SqlAst.IntegerLit n -> written.constantWordAt(n.pos()) != null
                    ? java.util.Objects.requireNonNull(written.constantWordAt(n.pos())) : n.value().toString();
            case SqlAst.NumericLit n -> numeric(n.value());
            case SqlAst.StringLit s -> (written.nationalAt(s.pos()) ? "N" : "") + quoted(s.value());
            case SqlAst.NullLit n -> "null";
            case SqlAst.Unary u -> unary(u);
            case SqlAst.Binary b -> binary(b);
            case SqlAst.IsNull n -> negatable(n, false);
            case SqlAst.Between b -> negatable(b, false);
            case SqlAst.Like l -> negatable(l, false);
            case SqlAst.InList in -> negatable(in, false);
            case SqlAst.IsDistinct d -> negatable(d, false);
            case SqlAst.IsTruth t -> negatable(t, false);
            case SqlAst.Case c -> written.ifs().contains(c) ? ifExpression(c) : caseExpression(c);
            case SqlAst.FunctionCall f when isConvertStyle(f) -> convertStyle(f);
            case SqlAst.FunctionCall f -> call(f);
            case SqlAst.Window w -> window(w);
            case SqlAst.Cast c -> cast(c);
            default -> null;
        };
    }

    /**
     * IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): a CAST inside an item, {@code cast(x as type)}, a
     * style-less CONVERT {@code convert(type,x)}, the operand rendered as any is and the type as written, in lower case,
     * with no space around its parentheses and commas ({@code numeric(10,2)}, {@code varchar(10 char)}, {@code
     * float(30)}, {@code money}) -- but INT is {@code integer}, DOUBLE PRECISION {@code double} and FLOAT without a
     * precision {@code real}. A type not measured (TIMESTAMP, DATETIME, LONG VARCHAR, ...) is not read.
     */
    private @com.legend.base.Nullable String cast(SqlAst.Cast c) {
        boolean convert = c.type().name().startsWith(IqParser.CONVERT);
        String type = writtenType(c.type(), convert);
        String operand = render(c.operand());
        if (type == null || operand == null) {
            return null;
        }
        return convert ? "convert(" + type + "," + operand + ")" : "cast(" + operand + " as " + type + ")";
    }

    /** Whether {@code f} is a CONVERT with a style ({@link IqParser#CONVERT_STYLE}, {@link
     *  IqParser#CONVERT_DATE_STYLE}). */
    private static boolean isConvertStyle(SqlAst.FunctionCall f) {
        return f.name().size() == 1 && (f.name().get(0).text().equals(IqParser.CONVERT_STYLE)
                || f.name().get(0).text().equals(IqParser.CONVERT_DATE_STYLE));
    }

    /** IQ island, 2026-10-16 (measured on IQ 17.1): a CONVERT with a style inside an item, as IQ names it --
     *  {@code convert(varchar(12),t.ts,107)}: its type as written, its operand, its style; to a date, {@code
     *  convert(date,'13/01/2012',103)} (2026-10-17). */
    private @com.legend.base.Nullable String convertStyle(SqlAst.FunctionCall f) {
        List<IqLexer.Token> tokens = written.tokens();
        int i = 0;
        while (i < tokens.size() && !tokens.get(i).pos().equals(f.pos())) {
            i++;
        }
        // CONVERT, then (, then the type
        if (i + 2 >= tokens.size() || !tokens.get(i + 1).isOp("(")) {
            return null;
        }
        String type = writtenType(new SqlAst.TypeName("", List.of(), tokens.get(i + 2).pos()), true);
        if (f.name().get(0).text().equals(IqParser.CONVERT_DATE_STYLE)) {
            // IQ island, 2026-10-17 (measured on IQ 17.1): DATE and TIMESTAMP in lower case, DATETIME and
            // SMALLDATETIME (SQL Anywhere's domains) as written (convert(SmallDateTime,'13/01/2012',103))
            IqLexer.Token w = tokens.get(i + 2);
            String lower = IqLexer.lower(w.text());
            boolean alone = i + 3 < tokens.size() && tokens.get(i + 3).isOp(",");
            type = !alone ? null : lower.equals("date") || lower.equals("timestamp") ? lower
                    : lower.equals("datetime") || lower.equals("smalldatetime") ? w.text() : null;
        }
        String operand = render(f.args().get(0));
        if (type == null || operand == null || !(f.args().get(1) instanceof SqlAst.IntegerLit style)) {
            return null;
        }
        return "convert(" + type + "," + operand + "," + style.value() + ")";
    }

    /** The type names a CAST's name was measured with (the first word, lower case). */
    private static final java.util.Set<String> CAST_TYPES = java.util.Set.of("int", "integer", "bigint", "smallint",
            "numeric", "decimal", "double", "real", "float", "char", "varchar", "date", "bit", "money");

    /** A CAST's or a CONVERT's type as IQ names it ({@link #cast}), from its written tokens; null when not read. */
    private @com.legend.base.Nullable String writtenType(SqlAst.TypeName type, boolean convert) {
        List<IqLexer.Token> tokens = written.tokens();
        int i = 0;
        while (i < tokens.size() && !tokens.get(i).pos().equals(type.pos())) {
            i++;
        }
        StringBuilder b = new StringBuilder();
        int depth = 0;
        IqLexer.Token before = null;
        for (; i < tokens.size(); i++) {
            IqLexer.Token t = tokens.get(i);
            if (t.kind() == IqLexer.Kind.EOF || depth == 0 && (t.isOp(")") || convert && t.isOp(","))) {
                break;
            }
            if (t.isOp("(")) {
                depth++;
            } else if (t.isOp(")")) {
                depth--;
            }
            boolean word = t.kind() == IqLexer.Kind.WORD || t.kind() == IqLexer.Kind.KEYWORD
                    || t.kind() == IqLexer.Kind.NUMBER;
            boolean wordBefore = before != null && (before.kind() == IqLexer.Kind.WORD
                    || before.kind() == IqLexer.Kind.KEYWORD || before.kind() == IqLexer.Kind.NUMBER);
            if (word && wordBefore) {
                b.append(' ');
            }
            if (!word && !t.isOp("(") && !t.isOp(")") && !t.isOp(",")) {
                return null;
            }
            b.append(IqLexer.lower(t.text()));
            before = t;
        }
        String text = b.toString();
        int end = text.indexOf('(');
        String head = end < 0 ? text : text.substring(0, end);
        int space = head.indexOf(' ');
        String first = space < 0 ? head : head.substring(0, space);
        if (text.isEmpty() || !CAST_TYPES.contains(first)) {
            return null;
        }
        return CAST_SPELLINGS.getOrDefault(text, text);
    }

    /** The type spellings IQ names otherwise than as written (measured on IQ 17.1). */
    private static final java.util.Map<String, String> CAST_SPELLINGS = java.util.Map.of("int", "integer",
            "double precision", "double", "float", "real");

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1): a window function's call, then {@code  over name} for a named window
     * as written, else {@code  over(...)}: the base window's name, {@code partition by k,...}, {@code order by k
     * asc|desc,...} and the frame in lower case, as written (ROWS 2 PRECEDING stays {@code rows 2 preceding}), with
     * single spaces between -- {@code SUM(t.x) over(partition by t.v order by t.x asc rows between 1 preceding and
     * current row)}, {@code COUNT() over()}, {@code ROW_NUMBER() over(order by t.x asc)}. Not read: a quoted window
     * name, a FILTER.
     */
    private @com.legend.base.Nullable String window(SqlAst.Window w) {
        SqlAst.FunctionCall f = w.call();
        if (f.filter() != null || !f.withinGroup().isEmpty() || f.name().size() != 1) {
            return null;
        }
        String call = aggregate(f) ? aggregateCall(f) : call(f);
        if (call == null) {
            return null;
        }
        SqlAst.WindowSpec spec = w.spec();
        SqlAst.Ident base = spec.base();
        if (base != null && base.quoted()) {
            return null;
        }
        if (base != null && spec.pos().equals(base.pos())) {
            return call + " over " + base.text();
        }
        List<String> parts = new ArrayList<>();
        if (base != null) {
            parts.add(base.text());
        }
        if (!spec.partitionBy().isEmpty()) {
            List<String> keys = new ArrayList<>();
            for (SqlAst.Expr k : spec.partitionBy()) {
                String r = render(k);
                if (r == null) {
                    return null;
                }
                keys.add(r);
            }
            parts.add(word("partition by", String.join(",", keys)));
        }
        if (!spec.orderBy().isEmpty()) {
            List<String> keys = new ArrayList<>();
            for (SqlAst.SortItem o : spec.orderBy()) {
                String r = render(o.expr());
                if (r == null || o.nullsFirst() != null) {
                    return null;
                }
                keys.add(r + (o.descending() ? " desc" : " asc"));
            }
            parts.add(word("order by", String.join(",", keys)));
        }
        SqlAst.Frame frame = spec.frame();
        if (frame != null) {
            String unit = switch (frame.unit()) {
                case ROWS -> "rows";
                case RANGE -> "range";
                case GROUPS -> null;
            };
            String start = bound(frame.start());
            String end = bound(frame.end());
            Boolean between = betweenWritten(frame);
            if (unit == null || start == null || end == null || between == null || frame.exclude() != null) {
                return null;
            }
            parts.add(unit + " " + (between ? "between " + start + " and " + end : start));
        }
        return call + " over(" + String.join(" ", parts) + ")";
    }

    /** A frame bound in lower case, its offset as named. */
    private @com.legend.base.Nullable String bound(SqlAst.FrameBound b) {
        return switch (b.kind()) {
            case UNBOUNDED_PRECEDING -> "unbounded preceding";
            case CURRENT_ROW -> "current row";
            case UNBOUNDED_FOLLOWING -> "unbounded following";
            case PRECEDING, FOLLOWING -> {
                SqlAst.Expr offset = b.offset();
                String r = offset == null ? null : render(offset);
                yield r == null ? null : r + (b.kind() == SqlAst.BoundKind.PRECEDING ? " preceding" : " following");
            }
        };
    }

    /** Whether {@code frame} was written with BETWEEN (its unit's next token), or null when its unit is not found. */
    private @com.legend.base.Nullable Boolean betweenWritten(SqlAst.Frame frame) {
        List<IqLexer.Token> tokens = written.tokens();
        for (int i = 0; i + 1 < tokens.size(); i++) {
            if (tokens.get(i).pos().equals(frame.pos())) {
                return tokens.get(i + 1).is("between");
            }
        }
        return null;
    }

    private @com.legend.base.Nullable String unary(SqlAst.Unary u) {
        if (u.op() == SqlAst.UnaryOp.PLUS) {
            return render(u.operand());
        }
        if (u.op() == SqlAst.UnaryOp.MINUS) {
            String operand = render(u.operand());
            return operand == null ? null : "-" + (operand.startsWith("-") ? " " : "") + operand;
        }
        // NOT before a predicate that has its own negation is written inside it (measured)
        SqlAst.Expr operand = u.operand();
        if (written.parensOf(operand) == 0 && (operand instanceof SqlAst.IsNull || operand instanceof SqlAst.Between
                || operand instanceof SqlAst.Like || operand instanceof SqlAst.InList
                || operand instanceof SqlAst.IsDistinct || operand instanceof SqlAst.IsTruth)) {
            return negatable(operand, true);
        }
        String inner = render(operand);
        // IQ island, 2026-10-14 (the third landing, measured): NOT on another line than its operand's first value is
        // indented
        return inner == null ? null : word((lineAt(u.pos()) != first(operand) ? indent() : "") + "not", inner);
    }

    private @com.legend.base.Nullable String binary(SqlAst.Binary b) {
        String l = render(b.left());
        String r = render(b.right());
        if (l == null || r == null || afterEnd(b.left(), b.right())) {
            return null;
        }
        // IQ island, 2026-10-14 (the third landing, measured): the right operand beginning on another line than the
        // left ends on leaves a space and the level's indent before the operator ("1\n+ 1" is 1 +1)
        boolean broken = broken(b.left(), b.right());
        String m = broken ? " " + indent() : "";
        return switch (b.op()) {
            case PLUS -> l + m + "+" + r;
            case MINUS -> l + m + "-" + (r.startsWith("-") ? " " : "") + r;
            case TIMES -> l + m + "*" + r;
            case DIVIDE -> l + m + "/" + r;
            case MODULO -> "remainder(" + l + "," + m + r + ")";
            case CONCAT -> l + m + " || " + r;
            case EQ -> l + m + " = " + r;
            case NE -> l + m + " <> " + r;
            case LT -> l + m + " < " + r;
            case LE -> l + m + " <= " + r;
            case GT -> l + m + " > " + r;
            case GE -> l + m + " >= " + r;
            case AND -> word(then(l, broken ? indent() + "and" : "and"), r);
            case OR -> word(then(l, broken ? indent() + "or" : "or"), r);
            case POWER -> null;
        };
    }

    // ------------------------------------------------------------------ line breaks (the third landing)

    /** The spaces a line break leaves at this level: two for each CASE or IF value it is inside. */
    private String indent() {
        return "  ".repeat(depth);
    }

    /** These parts one level deeper: a CASE's or an IF's values. */
    private IqNames deeper() {
        return new IqNames(written, columns, substitutes, inAggregate, depth + 1);
    }

    /** Whether {@code b}'s first value is on another line than {@code a}'s last ends on (SQL Anywhere's line break
     *  between two parts, measured). */
    private boolean broken(SqlAst.Expr a, SqlAst.Expr b) {
        return first(b) != last(a);
    }

    /** The line {@code e} begins on, as SQL Anywhere marks it (measured): its first value's (a column, a constant, a
     *  variable; not a parenthesis, a function's name or a CASE's keywords), but a NOT's own. */
    private int first(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.NOT -> lineAt(u.pos());
            case SqlAst.Unary u -> first(u.operand());
            case SqlAst.Binary b -> first(b.left());
            case SqlAst.Case c -> first(parts(c).get(0));
            case SqlAst.IsNull n -> first(n.operand());
            case SqlAst.IsTruth t -> first(t.operand());
            case SqlAst.IsDistinct d -> first(d.left());
            case SqlAst.Between b -> first(b.operand());
            case SqlAst.Like l -> first(l.operand());
            case SqlAst.InList in -> first(in.operand());
            case SqlAst.Cast c -> first(c.operand());
            case SqlAst.FunctionCall f -> f.args().isEmpty() ? lineAt(f.name().get(f.name().size() - 1).pos())
                    : first(f.args().get(0));
            default -> {
                List<SqlAst.Pos> leaves = new ArrayList<>();
                leaves(e, leaves);
                yield leaves.isEmpty() ? -1 : lineAt(leaves.get(0));
            }
        };
    }

    /** The line {@code e}'s last value ends on. */
    private int last(SqlAst.Expr e) {
        List<SqlAst.Pos> leaves = new ArrayList<>();
        leaves(e, leaves);
        return leaves.isEmpty() ? -1 : lineAt(leaves.get(leaves.size() - 1));
    }

    /** The line of {@code e}'s first token (a keyword or an operator too; not a parenthesis). */
    private int firstToken(SqlAst.Expr e) {
        SqlAst.Pos min = firstPos(e);
        return min == null ? -1 : lineAt(min);
    }

    private SqlAst.@com.legend.base.Nullable Pos firstPos(SqlAst.Expr e) {
        SqlAst.Pos min = written.linesAt(e.pos()) != null ? e.pos() : null;
        List<SqlAst.Pos> leaves = new ArrayList<>();
        leaves(e, leaves);
        for (SqlAst.Pos p : leaves) {
            if (min == null || before(p, min)) {
                min = p;
            }
        }
        for (SqlAst.Expr c : childrenOf(e)) {
            SqlAst.Pos p = firstPos(c);
            if (p != null && (min == null || before(p, min))) {
                min = p;
            }
        }
        return min;
    }

    private static boolean before(SqlAst.Pos a, SqlAst.Pos b) {
        return a.line() < b.line() || a.line() == b.line() && a.column() < b.column();
    }

    /** The parts written inside {@code e} (a CASE's as written, an IF's without the reader's NOT). */
    private List<SqlAst.Expr> childrenOf(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.Case c -> parts(c);
            case SqlAst.Unary u -> List.of(u.operand());
            case SqlAst.Binary b -> List.of(b.left(), b.right());
            case SqlAst.IsNull n -> List.of(n.operand());
            case SqlAst.IsTruth t -> List.of(t.operand());
            case SqlAst.IsDistinct d -> List.of(d.left(), d.right());
            case SqlAst.Between b -> List.of(b.operand(), b.low(), b.high());
            case SqlAst.Like l -> l.escape() == null ? List.of(l.operand(), l.pattern())
                    : List.of(l.operand(), l.pattern(), java.util.Objects.requireNonNull(l.escape()));
            case SqlAst.InList in -> {
                List<SqlAst.Expr> all = new ArrayList<>(List.of(in.operand()));
                all.addAll(in.values());
                yield all;
            }
            case SqlAst.FunctionCall f -> {
                List<SqlAst.Expr> all = new ArrayList<>(f.args());
                f.orderBy().forEach(o -> all.add(o.expr()));
                yield all;
            }
            case SqlAst.Cast c -> List.of(c.operand());
            default -> List.of();
        };
    }

    /**
     * Whether every line break in {@code e} is one whose mark on SQL Anywhere's name is read here (measured on SQL
     * Anywhere 17, IQ island, 2026-10-14, the third landing): between the operands of an operator, AND, OR, NOT,
     * BETWEEN, LIKE and IS NULL; between a call's name and its arguments (not one named by a reserved word, not an
     * aggregate's ORDER BY); between the parts of a CASE or an IF; between an IN's values when they are named in
     * written order. Any other (IS DISTINCT FROM, a truth test, ESCAPE, NOT before a predicate with its own negation,
     * a constant chain whose parentheses SQL Anywhere drops) is not read: the item is not named.
     */
    private boolean breaksRead(SqlAst.Expr e, int level) {
        int lo = firstToken(e);
        int hi = last(e);
        boolean spans = lo != hi;
        switch (e) {
            case SqlAst.Case c -> {
                Shape shape = shape(c);
                List<SqlAst.Expr> tests = new ArrayList<>(shape.tests());
                if (shape.operand() != null) {
                    tests.add(java.util.Objects.requireNonNull(shape.operand()));
                }
                for (SqlAst.Expr t : tests) {
                    if (!breaksRead(t, level)) {
                        return false;
                    }
                }
                List<SqlAst.Expr> values = new ArrayList<>(shape.results());
                if (shape.otherwise() != null) {
                    values.add(java.util.Objects.requireNonNull(shape.otherwise()));
                }
                for (SqlAst.Expr v : values) {
                    if (!breaksRead(v, level + 1)) {
                        return false;
                    }
                }
                return true;
            }
            case SqlAst.Binary b when level == 0 && (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS)
                    && chainParens(b) -> {
                // measured: a chain of + and - outside every CASE drops the parentheses inside it where the item
                // breaks a line or holds a comment ((id\n) + 1 is t.id +1, (3) - /* c */ id is 3-t.id), keeps them
                // inside a CASE's value -- not read
                return false;
            }
            case SqlAst.IsDistinct d -> {
                if (spans) {
                    return false;
                }
            }
            case SqlAst.IsTruth t -> {
                if (spans) {
                    return false;
                }
            }
            case SqlAst.Like l -> {
                if (l.escape() != null && spans) {
                    return false;
                }
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.NOT && written.parensOf(u.operand()) == 0
                    && (u.operand() instanceof SqlAst.IsNull || u.operand() instanceof SqlAst.Between
                    || u.operand() instanceof SqlAst.Like || u.operand() instanceof SqlAst.InList
                    || u.operand() instanceof SqlAst.IsDistinct || u.operand() instanceof SqlAst.IsTruth) -> {
                if (spans) {
                    return false;
                }
            }
            case SqlAst.InList in -> {
                boolean rest = false;
                for (SqlAst.Expr v : in.values()) {
                    boolean constant = written.parensOf(v) == 0 && (v instanceof SqlAst.IntegerLit
                            || v instanceof SqlAst.NumericLit || v instanceof SqlAst.StringLit);
                    if (constant && rest && spans) {
                        // the constants are named first: which line break goes where is not measured
                        return false;
                    }
                    rest |= !constant;
                }
            }
            case SqlAst.FunctionCall f -> {
                String name = f.name().get(f.name().size() - 1).text();
                if ((IqLexer.reserved(name) || f.name().size() != 1) && callSpans(f)) {
                    // measured: SQL Anywhere marks a line break inside LEFT(...) and its kin elsewhere -- not read
                    return false;
                }
                if (!f.orderBy().isEmpty() && f.args().size() > 0 && last(f.args().get(f.args().size() - 1)) != last(f)) {
                    return false;
                }
            }
            case SqlAst.Cast c -> {
                if (spans) {
                    return false;
                }
            }
            case SqlAst.Window w -> {
                // IQ island, 2026-10-15 (measured on IQ 17.1): a line break in a window's OVER marks nothing on the
                // name of an item that is the window; one in its call marks as a call's does. Elsewhere not read.
                if (spans && w != written.expr() || !breaksRead(w.call(), level)) {
                    return false;
                }
            }
            case SqlAst.Binary b when (b.op() == SqlAst.BinaryOp.PLUS || b.op() == SqlAst.BinaryOp.MINUS)
                    && constantChain(b) && spans -> {
                return false;
            }
            default -> {
            }
        }
        for (SqlAst.Expr c : childrenOf(e)) {
            if (!breaksRead(c, level)) {
                return false;
            }
        }
        return true;
    }

    /** Whether a chain of {@code +} and {@code -} has a parenthesized operand (or inner chain). */
    private boolean chainParens(SqlAst.Binary b) {
        for (SqlAst.Expr o : List.of(b.left(), b.right())) {
            if (written.parensOf(o) > 0) {
                return true;
            }
            if (o instanceof SqlAst.Binary c && (c.op() == SqlAst.BinaryOp.PLUS || c.op() == SqlAst.BinaryOp.MINUS)
                    && chainParens(c)) {
                return true;
            }
        }
        return false;
    }

    /** Whether a token of {@code f}'s call, from its name to its closing parenthesis, begins on another line than its
     *  name. */
    private boolean callSpans(SqlAst.FunctionCall f) {
        List<IqLexer.Token> tokens = written.tokens();
        SqlAst.Pos name = f.name().get(0).pos();
        int i = 0;
        while (i < tokens.size() && !tokens.get(i).pos().equals(name)) {
            i++;
        }
        if (i >= tokens.size()) {
            return true;
        }
        int line = lineAt(name);
        int depth = 0;
        for (int k = i; k < tokens.size() && tokens.get(k).kind() != IqLexer.Kind.EOF; k++) {
            IqLexer.Token t = tokens.get(k);
            int[] l = written.linesAt(t.pos());
            if (l == null || l[0] != line) {
                return true;
            }
            if (t.isOp("(")) {
                depth++;
            } else if (t.isOp(")") && --depth == 0) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code e} holds a CASE (an IF) with a test of constants only (a simple CASE's comparison) and a value
     *  that is not constant. */
    private static boolean foldedCase(SqlAst.Expr e) {
        if (e instanceof SqlAst.Case c) {
            boolean constantTest = false;
            boolean value = c.otherwise() != null && !constant(java.util.Objects.requireNonNull(c.otherwise()));
            for (SqlAst.When when : c.whens()) {
                constantTest |= constant(when.condition());
                value |= !constant(when.result());
            }
            if (constantTest && value) {
                return true;
            }
            for (SqlAst.When when : c.whens()) {
                if (foldedCase(when.condition()) || foldedCase(when.result())) {
                    return true;
                }
            }
            return c.otherwise() != null && foldedCase(java.util.Objects.requireNonNull(c.otherwise()));
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            if (foldedCase(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean constant(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.ColumnRef r -> false;
            case SqlAst.Param p -> false;
            case SqlAst.FunctionCall f -> false;
            case SqlAst.ScalarSubquery q -> false;
            case SqlAst.Exists q -> false;
            case SqlAst.InSubquery q -> false;
            case SqlAst.Quantified q -> false;
            case SqlAst.Case k -> false;
            default -> {
                for (SqlAst.Expr child : IqDialect.children(e)) {
                    if (!constant(child)) {
                        yield false;
                    }
                }
                yield true;
            }
        };
    }

    /** The line of the token at {@code pos}'s end. */
    private int lineAt(SqlAst.Pos pos) {
        int[] l = written.linesAt(pos);
        return l == null ? -1 : l[1];
    }

    /** The positions of {@code e}'s values (its columns' parts, its constants, its variables), in written order. */
    private void leaves(SqlAst.Expr e, List<SqlAst.Pos> out) {
        switch (e) {
            case SqlAst.ColumnRef r -> r.parts().forEach(p -> out.add(p.pos()));
            case SqlAst.IntegerLit n -> out.add(n.pos());
            case SqlAst.NumericLit n -> out.add(n.pos());
            case SqlAst.StringLit n -> out.add(n.pos());
            case SqlAst.NullLit n -> out.add(n.pos());
            case SqlAst.Param n -> out.add(n.pos());
            case SqlAst.Unary u -> leaves(u.operand(), out);
            case SqlAst.Binary b -> {
                leaves(b.left(), out);
                leaves(b.right(), out);
            }
            case SqlAst.Cast c -> leaves(c.operand(), out);
            case SqlAst.IsNull n -> leaves(n.operand(), out);
            case SqlAst.IsTruth t -> leaves(t.operand(), out);
            case SqlAst.IsDistinct d -> {
                leaves(d.left(), out);
                leaves(d.right(), out);
            }
            case SqlAst.Between b -> {
                leaves(b.operand(), out);
                leaves(b.low(), out);
                leaves(b.high(), out);
            }
            case SqlAst.Like l -> {
                leaves(l.operand(), out);
                leaves(l.pattern(), out);
                if (l.escape() != null) {
                    leaves(java.util.Objects.requireNonNull(l.escape()), out);
                }
            }
            case SqlAst.InList in -> {
                leaves(in.operand(), out);
                in.values().forEach(v -> leaves(v, out));
            }
            case SqlAst.FunctionCall f -> {
                if (f.args().isEmpty()) {
                    out.add(f.name().get(f.name().size() - 1).pos());
                }
                f.args().forEach(a -> leaves(a, out));
                f.orderBy().forEach(o -> leaves(o.expr(), out));
            }
            case SqlAst.Case c -> {
                for (SqlAst.Expr part : parts(c)) {
                    leaves(part, out);
                }
            }
            case SqlAst.Window w -> {
                leaves(w.call(), out);
                w.spec().partitionBy().forEach(k -> leaves(k, out));
                w.spec().orderBy().forEach(o -> leaves(o.expr(), out));
            }
            default -> {
            }
        }
    }

    /** A CASE's (an IF's) parts as written: a simple CASE's operand once, then each WHEN's test and result, then the
     *  ELSE's value. */
    private List<SqlAst.Expr> parts(SqlAst.Case c) {
        List<SqlAst.Expr> out = new ArrayList<>();
        if (written.ifs().contains(c)) {
            out.add(c.whens().get(0).condition());
            out.add(c.whens().get(0).result());
            SqlAst.Expr otherwise = c.whens().size() > 1 ? c.whens().get(1).result() : written.droppedElse().get(c);
            if (otherwise != null) {
                out.add(otherwise);
            }
            return out;
        }
        boolean simple = written.simpleCases().contains(c);
        List<SqlAst.When> whens = new ArrayList<>(c.whens());
        whens.addAll(written.droppedWhens().getOrDefault(c, List.of()));
        for (int i = 0; i < whens.size(); i++) {
            SqlAst.When w = whens.get(i);
            if (simple && w.condition() instanceof SqlAst.Binary b) {
                if (i == 0) {
                    out.add(b.left());
                }
                out.add(b.right());
            } else {
                out.add(w.condition());
            }
            out.add(w.result());
        }
        SqlAst.Expr otherwise = written.droppedElse().containsKey(c) ? written.droppedElse().get(c) : c.otherwise();
        if (otherwise != null) {
            out.add(otherwise);
        }
        return out;
    }

    /** A predicate with a negation of its own, {@code negated} once more by a NOT before it. */
    private @com.legend.base.Nullable String negatable(SqlAst.Expr e, boolean negated) {
        return switch (e) {
            case SqlAst.IsNull n -> {
                String x = render(n.operand());
                yield x == null ? null : then(x, n.negated() != negated ? "is not null" : "is null");
            }
            case SqlAst.Between b -> {
                String x = render(b.operand());
                String low = render(b.low());
                String high = render(b.high());
                // IQ island, 2026-10-14 (the third landing, measured): a line break before a bound indents its word
                yield x == null || low == null || high == null ? null
                        : word(then(word(then(x, (broken(b.operand(), b.low()) ? indent() : "")
                                + (b.negated() != negated ? "not between" : "between")), low),
                                (broken(b.low(), b.high()) ? indent() : "") + "and"), high);
            }
            case SqlAst.Like l -> {
                String x = render(l.operand());
                String p = render(l.pattern());
                String esc = l.escape() == null ? "" : render(java.util.Objects.requireNonNull(l.escape()));
                if (x == null || p == null || esc == null) {
                    yield null;
                }
                String like = word(then(x, (broken(l.operand(), l.pattern()) ? indent() : "")
                        + (l.negated() != negated ? "not like" : "like")), p);
                yield l.escape() == null ? like : word(then(like, "escape"), esc);
            }
            case SqlAst.InList in -> {
                String x = render(in.operand());
                List<String> values = inValues(in);
                yield x == null || values == null ? null : then(x, in.negated() != negated ? "not in(" : "in(")
                        + " " + String.join(",", values) + " ) ";
            }
            case SqlAst.IsDistinct d -> {
                // SQL Anywhere's predicate is IS NOT DISTINCT FROM, IS DISTINCT FROM its negation (measured)
                String l = render(d.left());
                String r = render(d.right());
                if (l == null || r == null) {
                    yield null;
                }
                String same = word(then(l, "is not distinct from"), r);
                yield d.negated() != negated ? same : word("not", same);
            }
            case SqlAst.IsTruth t -> {
                String x = render(t.operand());
                if (x == null) {
                    yield null;
                }
                String test = then(x, "is " + t.truth().name().toLowerCase(java.util.Locale.ROOT));
                yield t.negated() != negated ? word("not", test) : test;
            }
            default -> throw new IllegalStateException("not a negatable predicate: " + e);
        };
    }

    /** An IN list's values: its plain constants (numbers, strings) first, then the rest, each in written order
     *  (measured). */
    private @com.legend.base.Nullable List<String> inValues(SqlAst.InList in) {
        List<SqlAst.Expr> values = in.values();
        List<String> constants = new ArrayList<>();
        List<String> rest = new ArrayList<>();
        SqlAst.Expr before = in.operand();
        for (SqlAst.Expr v : values) {
            String r = render(v);
            if (r == null) {
                return null;
            }
            // IQ island, 2026-10-14 (the third landing, measured): a value on another line than the one before it
            if (broken(before, v)) {
                r = " " + indent() + r;
            }
            before = v;
            boolean constant = written.parensOf(v) == 0 && (v instanceof SqlAst.IntegerLit
                    || v instanceof SqlAst.NumericLit || v instanceof SqlAst.StringLit);
            (constant ? constants : rest).add(r);
        }
        constants.addAll(rest);
        return constants;
    }

    /** {@code CASE [x] WHEN ... THEN ... [ELSE ...] END}: a simple CASE's WHEN holds its value (the reader made it
     *  {@code x = v}). IQ island, 2026-10-14 (the third landing, measured on SQL Anywhere 17): a line break between
     *  its parts leaves the level's indent ({@link #caseParts}). */
    private @com.legend.base.Nullable String caseExpression(SqlAst.Case c) {
        Shape shape = shape(c);
        String out = "case";
        if (shape.operand() != null) {
            String operand = render(java.util.Objects.requireNonNull(shape.operand()));
            if (operand == null) {
                return null;
            }
            out = word(out, operand);
        }
        return caseParts(out, shape, "when", "end");
    }

    /** A CASE's or an IF's parts as written: a simple CASE's operand (else null), its tests (a simple CASE's values),
     *  its results, its ELSE's value (else null). */
    private record Shape(SqlAst.@com.legend.base.Nullable Expr operand, List<SqlAst.Expr> tests,
            List<SqlAst.Expr> results, SqlAst.@com.legend.base.Nullable Expr otherwise) {
    }

    private Shape shape(SqlAst.Case c) {
        if (written.ifs().contains(c)) {
            SqlAst.When first = c.whens().get(0);
            // IQ island, 2026-10-14 (review 3's M5): an ELSE NULL the reader dropped is named as written
            SqlAst.Expr otherwise = c.whens().size() > 1 ? c.whens().get(1).result() : written.droppedElse().get(c);
            return new Shape(null, List.of(first.condition()), List.of(first.result()), otherwise);
        }
        boolean simple = written.simpleCases().contains(c);
        List<SqlAst.When> whens = new ArrayList<>(c.whens());
        // IQ island, 2026-10-14 (review 3's M5): the branches the reader dropped (a NULL's) are named as written
        whens.addAll(written.droppedWhens().getOrDefault(c, List.of()));
        List<SqlAst.Expr> tests = new ArrayList<>();
        List<SqlAst.Expr> results = new ArrayList<>();
        SqlAst.Expr operand = null;
        for (SqlAst.When w : whens) {
            if (simple && w.condition() instanceof SqlAst.Binary b) {
                operand = b.left();
                tests.add(b.right());
            } else {
                tests.add(w.condition());
            }
            results.add(w.result());
        }
        // (review 4's m2) the ELSE as written where the reader moved one
        SqlAst.Expr otherwise = written.droppedElse().containsKey(c) ? written.droppedElse().get(c) : c.otherwise();
        return new Shape(simple ? operand : null, tests, results, otherwise);
    }

    /**
     * A CASE's or an IF's parts after its head, named with SQL Anywhere's marks of line breaks (IQ island, 2026-10-14,
     * the third landing; measured on SQL Anywhere 17): a value ({@code THEN}'s, {@code ELSE}'s) is one level deeper; a
     * WHEN whose test begins on another line than the value before it (a simple CASE's first: its operand) ends, an
     * ELSE whose value begins on another line than the value before it begins, and the END ({@link #endBreak}) leave
     * the level's indent before their word; a THEN's value beginning on another line than its test ends, and an ELSE
     * with two line breaks or more around its word, leave the deeper level's indent after theirs.
     */
    private @com.legend.base.Nullable String caseParts(String head, Shape shape, String when, String end) {
        String out = head;
        IqNames values = deeper();
        SqlAst.Expr before = shape.operand();
        for (int i = 0; i < shape.tests().size(); i++) {
            SqlAst.Expr test = shape.tests().get(i);
            SqlAst.Expr result = shape.results().get(i);
            String t = render(test);
            String r = values.render(result);
            if (t == null || r == null || before != null && afterEnd(before, test)) {
                return null;
            }
            String w = (before != null && broken(before, test) ? indent() : "") + when;
            out = then(word(then(out, w), t), "then") + separator(broken(test, result), values, r) + r;
            before = result;
        }
        SqlAst.Expr otherwise = shape.otherwise();
        if (otherwise != null) {
            SqlAst.Expr last = shape.results().get(shape.results().size() - 1);
            if (last instanceof SqlAst.Case c && written.parensOf(c) == 0 && endBreak(shape(c))) {
                // measured both ways: how SQL Anywhere marks an ELSE after a value whose own END is marked is not read
                return null;
            }
            String e = values.render(otherwise);
            Boolean far = farElse(shape.results().get(shape.results().size() - 1), otherwise);
            if (e == null || far == null) {
                return null;
            }
            out = then(out, (endBreak(shape) ? indent() : "") + "else") + separator(far, values, e) + e;
        }
        return then(out, (endBreak(shape) ? indent() : "") + end);
    }

    /** Whether a CASE's (an IF's) END has SQL Anywhere's mark of a line break before it (measured): with an ELSE, when
     *  its value begins on another line than the value before it begins; without, when its last value begins on
     *  another line than its last test. */
    private boolean endBreak(Shape shape) {
        SqlAst.Expr last = shape.results().get(shape.results().size() - 1);
        SqlAst.Expr otherwise = shape.otherwise();
        return otherwise != null ? first(otherwise) != first(last)
                : first(shape.tests().get(shape.tests().size() - 1)) != first(last);
    }

    /** Whether {@code next} follows {@code previous} on another line where {@code previous} is a CASE or an IF whose
     *  END has a line break's mark: how SQL Anywhere marks the next part then is not read (measured both ways). */
    private boolean afterEnd(SqlAst.Expr previous, SqlAst.Expr next) {
        return previous instanceof SqlAst.Case c && written.parensOf(c) == 0 && endBreak(shape(c))
                && broken(previous, next);
    }

    /** Whether two line breaks or more stand around the ELSE before {@code otherwise}, between it and {@code last}, the
     *  value before (measured: SQL Anywhere then indents the value) -- counted between the tokens around the word and
     *  between the two values, which agree where it was measured; null where they do not (a value that ends or
     *  begins with a parenthesis or a keyword, measured both ways), or the ELSE is not found among the tokens. */
    private @com.legend.base.Nullable Boolean farElse(SqlAst.Expr last, SqlAst.Expr otherwise) {
        boolean byValues = first(otherwise) - last(last) >= 2;
        SqlAst.Pos at = firstPos(otherwise);
        List<IqLexer.Token> tokens = written.tokens();
        int i = 0;
        while (i < tokens.size() && !tokens.get(i).pos().equals(at)) {
            i++;
        }
        while (i > 0 && !tokens.get(i).is("ELSE")) {
            i--;
        }
        if (i <= 0 || i + 1 >= tokens.size() || !tokens.get(i).is("ELSE")) {
            return null;
        }
        int[] before = written.linesAt(tokens.get(i - 1).pos());
        int[] after = written.linesAt(tokens.get(i + 1).pos());
        if (before == null || after == null) {
            return null;
        }
        boolean byTokens = after[0] - before[1] >= 2;
        return byTokens == byValues ? Boolean.valueOf(byValues) : null;
    }

    /** What separates a THEN or an ELSE from its value {@code r}: a space and {@code values}' indent where a line break
     *  stands there, else a space unless the value begins with a parenthesis or a minus. */
    private static String separator(boolean broken, IqNames values, String r) {
        return broken ? " " + values.indent() : r.startsWith("(") || r.startsWith("-") ? "" : " ";
    }

    /** {@code IF c THEN a [ELSE b] ENDIF}, which the reader made {@code CASE WHEN c THEN a [WHEN NOT c THEN b] END}. */
    private @com.legend.base.Nullable String ifExpression(SqlAst.Case c) {
        String out = caseParts("", shape(c), "if", "endif");
        return out == null ? null : out.strip();
    }

    /** Whether {@code e} is a call of one of the aggregates read. */
    private static boolean aggregate(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && f.name().size() == 1
                && IqParser.READ_AGGREGATES.contains(IqLexer.lower(f.name().get(0).text()));
    }

    /** An aggregate's call (measured): {@code NAME()} for COUNT(*), {@code distinct } before a DISTINCT argument, the
     *  arguments joined by {@code ,}, then LIST's {@code  order by key asc|desc,...}. */
    private @com.legend.base.Nullable String aggregateCall(SqlAst.FunctionCall f) {
        IqNames inside = new IqNames(written, columns, substitutes, true, depth);
        List<String> args = new ArrayList<>();
        for (SqlAst.Expr a : f.args()) {
            String r = inside.render(a);
            if (r == null) {
                return null;
            }
            args.add(r);
        }
        // IQ island, 2026-10-14 (the third landing, measured): an argument on another line than the one before it
        // (the name, for the first) has a space before it, a first one before its DISTINCT
        int prev = lineAt(f.name().get(0).pos());
        boolean firstBroken = false;
        for (int i = 0; i < args.size(); i++) {
            SqlAst.Expr a = f.args().get(i);
            if (i > 0 && afterEnd(f.args().get(i - 1), a)) {
                return null;
            }
            boolean m = first(a) != prev;
            if (i == 0) {
                firstBroken = m;
            } else if (m) {
                args.set(i, " " + indent() + args.get(i));
            }
            prev = last(a);
        }
        StringBuilder b = new StringBuilder(f.name().get(0).text()).append('(');
        String joined = String.join(",", args);
        // DISTINCT is a word: no space before a parenthesis or a minus (measured: COUNT(distinct(t.a)))
        b.append(firstBroken ? " " + indent() : "").append(f.distinct() ? word("distinct", joined) : joined);
        for (int i = 0; i < f.orderBy().size(); i++) {
            SqlAst.SortItem o = f.orderBy().get(i);
            String key = inside.render(o.expr());
            if (key == null) {
                return null;
            }
            b.append(i == 0 ? " order by " : ",").append(key).append(o.descending() ? " desc" : " asc");
        }
        return b.append(')').toString();
    }

    /** Whether {@code f} is the reader's EXTRACT. */
    private static boolean extract(SqlAst.FunctionCall f) {
        return f.name().size() == 1 && !f.name().get(0).quoted() && f.name().get(0).text().equals(IqParser.EXTRACT);
    }

    /** {@code name(argument,...)}: the name as written, a reserved word double-quoted. */
    private @com.legend.base.Nullable String call(SqlAst.FunctionCall f) {
        if (f.name().size() != 1 || extract(f)) {
            return null;
        }
        if (aggregate(f)) {
            return aggregateCall(f);
        }
        String name = f.name().get(0).text();
        if (!f.name().get(0).quoted() && f.args().isEmpty()
                && (name.equals("CURRENT DATE") || name.equals("CURRENT TIMESTAMP"))) {
            // IQ island, 2026-10-14 (the merge of M1b and M1d): the reader's call for CURRENT DATE, CURRENT TIMESTAMP
            // and CURRENT_TIMESTAMP, which SQL Anywhere names in lower case (measured: current timestamp)
            return name.toLowerCase(java.util.Locale.ROOT);
        }
        List<String> args = new ArrayList<>();
        int prev = lineAt(f.name().get(0).pos());
        for (SqlAst.Expr a : f.args()) {
            // a date part is named as written, in lower case (measured: DATEPART(mm,...), DATENAME(month,...))
            String word = written.partWords().get(a);
            String r = word == null ? render(a) : word.isEmpty() || written.parensOf(a) > 0 ? null : word;
            if (r == null) {
                return null;
            }
            // IQ island, 2026-10-14 (the third landing, measured): an argument on another line than the one before it
            // (the name, for the first) has a space and the level's indent before it
            if (!args.isEmpty() && afterEnd(f.args().get(args.size() - 1), a)) {
                return null;
            }
            args.add((first(a) != prev ? " " + indent() : "") + r);
            prev = last(a);
        }
        return (IqLexer.reserved(name) ? "\"" + name + "\"" : name) + "(" + String.join(",", args) + ")";
    }

    /** {@code keyword} then {@code text}: a space between, unless the text begins with a parenthesis or a minus. */
    private static String word(String keyword, String text) {
        return keyword + (text.startsWith("(") || text.startsWith("-") ? "" : " ") + text;
    }

    /** {@code text} then {@code keyword}: a space between, unless the text ends with one (an IN list's). */
    private static String then(String text, String keyword) {
        return text + (text.endsWith(" ") ? "" : " ") + keyword;
    }

    /** A numeric constant: its value without trailing zeros or a zero before its point; a whole one {@code n.0} up to
     *  an INT's largest, {@code n} beyond (measured). */
    static String numeric(BigDecimal value) {
        BigDecimal v = value.stripTrailingZeros();
        if (v.scale() <= 0) {
            BigInteger whole = v.toBigInteger();
            return whole.abs().compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0 ? whole + ".0" : whole.toString();
        }
        String s = v.toPlainString();
        return s.startsWith("0.") ? s.substring(1) : s.startsWith("-0.") ? "-" + s.substring(2) : s;
    }

    /** A string constant quoted: its quotes doubled, each control character as {@code \xHH}. */
    static String quoted(String value) {
        StringBuilder b = new StringBuilder("'");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\'') {
                b.append("''");
            } else if (c < 0x20) {
                String h = Integer.toHexString(c).toUpperCase(java.util.Locale.ROOT);
                b.append("\\x").append(h.length() == 1 ? "0" : "").append(h);
            } else {
                b.append(c);
            }
        }
        return b.append('\'').toString();
    }
}
