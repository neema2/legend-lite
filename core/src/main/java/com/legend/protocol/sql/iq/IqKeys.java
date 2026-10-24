// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * IQ island, 2026-10-14 (the merge of M1c and the natives slice): which strings of a statement are their own collation
 * key ({@link IqDialect#collated}), so that a clause that shows one value of each group of equal keys shows the only
 * one there is.
 * <p>
 * IQ sorts, groups and tells strings apart by its collation: in an IQ database without their trailing spaces
 * ({@code 'a'} and {@code 'a '} are one value), in a {@code sqla17} profile's database with their ASCII letters folded
 * ({@code 'A'} and {@code 'a'}). Where a clause shows one value of such a group -- DISTINCT, a GROUP BY key the query
 * shows, UNION, INTERSECT and EXCEPT, MIN and MAX, LIST's order -- SQL Anywhere shows the first row it meets (measured
 * on SQL Anywhere 17: {@code SELECT DISTINCT x} over {@code 'a', 'a '} shows {@code 'a'}, over {@code 'a ', 'a'}
 * {@code 'a '}; MIN, MAX, GROUP BY, UNION and INTERSECT alike; in a larger plan the first of the generator's rows),
 * which is its plan's choice and no translation's. So such a clause is translated only where no two different values
 * of the strings shown can share a key, which this class proves from the expression's form ({@link Shape}):
 * {@code RTRIM(x)} and {@code TRIM(x)} end in no space, {@code LOWER(x)} holds no upper-case letter, a constant is
 * what it is, a derived table's column is what its definition is; any other string (a column's) is refused there by
 * name. ORDER BY shows each row's own value, and COUNT(DISTINCT x) none: both read the key, whatever the strings.
 */
final class IqKeys {

    /**
     * What the values of a string expression can hold, as far as its form proves: none ends in a space
     * ({@code noTrailingSpace}), none holds an ASCII lower-case letter ({@code noLower}) or an upper-case one
     * ({@code noUpper}).
     */
    record Shape(boolean noTrailingSpace, boolean noLower, boolean noUpper) {

        static final Shape ANY = new Shape(false, false, false);
        static final Shape NONE_HELD = new Shape(true, true, true);

        /** What one of the values of either can hold. */
        Shape or(Shape other) {
            return new Shape(noTrailingSpace && other.noTrailingSpace, noLower && other.noLower,
                    noUpper && other.noUpper);
        }
    }

    private final IqSettings settings;
    /** The query whose clauses each column reference of the statement is written in (not a subquery's). */
    private final Map<SqlAst.ColumnRef, SqlAst.Select> owners;

    private IqKeys(IqSettings settings, Map<SqlAst.ColumnRef, SqlAst.Select> owners) {
        this.settings = settings;
        this.owners = owners;
    }

    /** No statement: every column is any string. */
    static IqKeys none(IqSettings settings) {
        return new IqKeys(settings, Map.of());
    }

    /** The keys of {@code statement}'s strings under {@code settings}. */
    static IqKeys of(SqlAst.Statement statement, IqSettings settings) {
        Map<SqlAst.ColumnRef, SqlAst.Select> owners = new IdentityHashMap<>();
        for (SqlAst.Statement q : IqDialect.queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.Expr e : IqDialect.expressions(s)) {
                    List<SqlAst.ColumnRef> refs = new ArrayList<>();
                    IqDialect.columnRefs(e, refs);
                    refs.forEach(r -> owners.put(r, s));
                }
            }
        }
        return new IqKeys(settings, Collections.unmodifiableMap(owners));
    }

    /** Whether every value of {@code e} is its own key: equality and order of the values are the collation's. */
    boolean own(SqlAst.Expr e) {
        Shape s = shape(e);
        return (!settings.blankPadding() || s.noTrailingSpace()) && (settings.caseSensitive() || s.noUpper());
    }

    /** Whether no two different values of {@code e} share a key: equality of the values is the collation's (their
     *  order may not be: {@code UPPER(x)} in a case-insensitive database). */
    boolean distinct(SqlAst.Expr e) {
        Shape s = shape(e);
        return (!settings.blankPadding() || s.noTrailingSpace())
                && (settings.caseSensitive() || s.noUpper() || s.noLower());
    }

    /** Whether column {@code i} of every query {@code s} unites holds its own keys only. */
    boolean own(SqlAst.SetOperation s, int i) {
        for (SqlAst.Select branch : branches(s)) {
            if (!(item(branch, i) instanceof SqlAst.SelectExpr x) || !own(x.expr())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Item {@code i} of {@code branch}, when it is column {@code i}: null past the items, or when a {@code *} (or a
     * {@code t.*}) comes at or before it -- a star stands for as many columns as its table has, so the item at index i
     * is then some later column (IQ island, 2026-10-14, review 4's B1: {@code SELECT *, RTRIM(x) AS r ... UNION ALL
     * SELECT 9, 'zz', x ...} took the second branch's x for its own key).
     */
    private static SqlAst.@com.legend.base.Nullable SelectItem item(SqlAst.Select branch, int i) {
        if (i >= branch.items().size()) {
            return null;
        }
        for (int j = 0; j <= i; j++) {
            if (!(branch.items().get(j) instanceof SqlAst.SelectExpr)) {
                return null;
            }
        }
        return branch.items().get(i);
    }

    /**
     * Whether the query that groups by {@code key} shows a value of it: reads one of the key's columns outside its
     * aggregates in its select list, its HAVING or its ORDER BY (but an ORDER BY item that is the key's one column
     * itself), or the key belongs to no query known here.
     */
    boolean shown(SqlAst.Expr key) {
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        IqDialect.columnRefs(key, refs);
        SqlAst.Select owner = refs.isEmpty() ? null : owners.get(refs.get(0));
        if (owner == null) {
            return true;
        }
        Set<String> names = new java.util.HashSet<>();
        refs.forEach(r -> names.add(last(r)));
        SqlAst.Select q = IqAggregates.grouping(owner);
        for (SqlAst.SelectItem item : q.items()) {
            if (!(item instanceof SqlAst.SelectExpr x) || reads(x.expr(), names)) {
                return true;
            }
        }
        SqlAst.Expr having = q.having();
        if (having != null && reads(having, names)) {
            return true;
        }
        for (SqlAst.SortItem o : q.orderBy()) {
            boolean itself = key instanceof SqlAst.ColumnRef k && o.expr() instanceof SqlAst.ColumnRef r
                    && last(r).equals(last(k));
            if (!itself && reads(o.expr(), names)) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code e} reads a column of {@code names} outside its aggregates, or holds a subquery. */
    private static boolean reads(SqlAst.Expr e, Set<String> names) {
        if (e instanceof SqlAst.FunctionCall f && IqAggregates.named(f)) {
            return false;
        }
        if (e instanceof SqlAst.ColumnRef r) {
            return names.contains(last(r));
        }
        if (e instanceof SqlAst.ScalarSubquery || e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery
                || e instanceof SqlAst.Quantified) {
            return true;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            if (reads(child, names)) {
                return true;
            }
        }
        return false;
    }

    /** What {@code e}'s values can hold, from its form. */
    Shape shape(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.NullLit n -> Shape.NONE_HELD;
            case SqlAst.IntegerLit n -> Shape.NONE_HELD;
            case SqlAst.NumericLit n -> Shape.NONE_HELD;
            case SqlAst.StringLit s -> new Shape(!s.value().endsWith(" "), !has(s.value(), 'a', 'z'),
                    !has(s.value(), 'A', 'Z'));
            case SqlAst.FunctionCall f -> call(f);
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.CONCAT -> {
                Shape l = shape(b.left());
                Shape r = shape(b.right());
                // a constant that ends in no space, last: so does the whole (IQ's || reads a single NULL as '')
                boolean ends = b.right() instanceof SqlAst.StringLit c && !c.value().isEmpty() && !c.value().endsWith(" ");
                yield new Shape(ends, l.noLower() && r.noLower(), l.noUpper() && r.noUpper());
            }
            case SqlAst.Case c -> {
                Shape out = Shape.NONE_HELD;
                for (SqlAst.When w : c.whens()) {
                    out = out.or(shape(w.result()));
                }
                SqlAst.Expr otherwise = c.otherwise();
                yield otherwise == null ? out : out.or(shape(otherwise));
            }
            case SqlAst.ColumnRef r -> column(r);
            default -> Shape.ANY;
        };
    }

    /** A call's: the trims end in no space (they drop IQ's blanks, the space among them), LOWER and LCASE hold no
     *  upper-case letter, UPPER and UCASE no lower-case one, MIN and MAX what their argument holds. */
    private Shape call(SqlAst.FunctionCall f) {
        if (f.name().size() != 1 || f.name().get(0).quoted()) {
            return Shape.ANY;
        }
        String name = IqLexer.lower(f.name().get(0).text());
        if (f.args().size() != 1) {
            return Shape.ANY;
        }
        Shape x = shape(f.args().get(0));
        if (TRIMS.contains(name)) {
            return new Shape(true, x.noLower(), x.noUpper());
        }
        if (LOWERS.contains(name)) {
            return new Shape(x.noTrailingSpace(), x.noLower() && x.noUpper(), true);
        }
        if (UPPERS.contains(name)) {
            return new Shape(x.noTrailingSpace(), true, x.noLower() && x.noUpper());
        }
        if (KEEPS.contains(name) && !f.distinct() && f.orderBy().isEmpty()) {
            return x;
        }
        return Shape.ANY;
    }

    /** The calls whose value ends in no space, those that hold no upper-case letter, no lower-case one, and those
     *  that hold one of their argument's values. */
    private static final Set<String> TRIMS = Set.of("rtrim", "trim");
    private static final Set<String> LOWERS = Set.of("lower", "lcase");
    private static final Set<String> UPPERS = Set.of("upper", "ucase");
    private static final Set<String> KEEPS = Set.of("ltrim", "min", "max");

    /** A column's: a derived table's column holds what its definition does (in every branch of a set operation);
     *  any other may hold any string. */
    private Shape column(SqlAst.ColumnRef r) {
        SqlAst.Select owner = owners.get(r);
        if (owner == null) {
            return Shape.ANY;
        }
        String name = last(r);
        SqlAst.SubqueryFrom found = null;
        for (SqlAst.FromItem f : owner.from()) {
            for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                boolean candidate;
                if (r.parts().size() == 2) {
                    SqlAst.Ident c = IqDialect.correlation(leaf);
                    candidate = c != null && IqLexer.lower(c.text()).equals(IqLexer.lower(r.parts().get(0).text()));
                    if (candidate && !(leaf instanceof SqlAst.SubqueryFrom)) {
                        return Shape.ANY;
                    }
                } else {
                    candidate = leaf instanceof SqlAst.SubqueryFrom d && position(d, name) >= 0;
                }
                if (candidate && leaf instanceof SqlAst.SubqueryFrom d) {
                    if (found != null) {
                        return Shape.ANY;
                    }
                    found = d;
                }
            }
        }
        if (found == null || r.parts().size() > 2) {
            return Shape.ANY;
        }
        int i = position(found, name);
        if (i < 0) {
            return Shape.ANY;
        }
        Shape out = Shape.NONE_HELD;
        for (SqlAst.Select branch : branches(found.query())) {
            if (!(item(branch, i) instanceof SqlAst.SelectExpr x)) {
                return Shape.ANY;
            }
            out = out.or(shape(x.expr()));
        }
        return out;
    }

    /** The position of the column named {@code name} (ASCII lower case) among derived table {@code d}'s: by its
     *  column list, else by its first branch's items' names (an alias, a column's last part); -1 for none. */
    private static int position(SqlAst.SubqueryFrom d, String name) {
        if (!d.columnAliases().isEmpty()) {
            for (int i = 0; i < d.columnAliases().size(); i++) {
                if (IqLexer.lower(d.columnAliases().get(i).text()).equals(name)) {
                    return i;
                }
            }
            return -1;
        }
        List<SqlAst.SelectItem> items = branches(d.query()).get(0).items();
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i) instanceof SqlAst.SelectExpr x) {
                SqlAst.Ident alias = x.alias();
                String own = alias != null ? IqLexer.lower(alias.text()) : x.expr() instanceof SqlAst.ColumnRef c
                        ? last(c) : null;
                if (name.equals(own)) {
                    return i;
                }
            }
        }
        return -1;
    }

    /** The SELECTs a query unites, left first. */
    private static List<SqlAst.Select> branches(SqlAst.Statement q) {
        List<SqlAst.Select> out = new ArrayList<>();
        branches(q, out);
        return out;
    }

    private static void branches(SqlAst.Statement q, List<SqlAst.Select> out) {
        switch (q) {
            case SqlAst.Select s -> out.add(s);
            case SqlAst.SetOperation o -> {
                branches(o.left(), out);
                branches(o.right(), out);
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
            case SqlAst.Write w -> throw new IllegalStateException("IQ's reader makes no write");
        }
    }

    private static boolean has(String s, char from, char to) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) >= from && s.charAt(i) <= to) {
                return true;
            }
        }
        return false;
    }

    private static String last(SqlAst.ColumnRef r) {
        return IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
    }
}
