// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A select-list alias read where IQ reads it (IQ island, 2026-10-14, the third landing; measured on SQL Anywhere 17,
 * docs/IQ_SQL_STUDY_2026_10_13.md §9.16). IQ reads a name that is an alias of the select list before a column of that
 * name, anywhere in its SELECT: in WHERE, in a join's ON, in the select list (an alias defined later in the list too),
 * inside an ORDER BY expression, and in GROUP BY and HAVING ({@link IqAggregates#grouping}, already). It reads the
 * alias's expression there, exactly as if it were written there (a NUMERIC quotient's digits, an overflow, an aggregate
 * in WHERE's IQ error -150 alike), so the reader puts a fresh copy of the expression in the reference's place. The
 * rules, measured:
 * <ul>
 *   <li>Inside an alias's own definition its own name is the column ({@code id + 1 AS id} adds to the column); any
 *       other alias is read, recursively ({@code id AS a, a + 1 AS b, b * 2 AS c}). A definition that reaches itself
 *       through other aliases is IQ's error -831.</li>
 *   <li>A qualified name ({@code t.x}) is never an alias. An alias that is the column of its own name
 *       ({@code x AS x}) reads the same either way and is left as written.</li>
 *   <li>In a join's ON, the alias's expression is read in the ON's scope: a FROM item outside it named there is IQ's
 *       error -824. An unqualified column there is refused by name unless every FROM item is in the ON's scope (which
 *       item holds it is the translation's to know), and so is an expression with a subquery.</li>
 *   <li>Inside a subquery a column of the subquery's own FROM items comes first, then the enclosing query's alias
 *       before its columns (measured: {@code SELECT id AS it, (SELECT COUNT(*) FROM u WHERE u.t_id = it)} reads the
 *       alias). Such a name is recorded ({@link Read#outerReads}); the translation reads it as its own query's column
 *       where it is one, and refuses it by name otherwise ({@link IqDialect#columnNotHere}).</li>
 *   <li>An unaliased item keeps its name as written: {@code x + 1} over an alias {@code x} is named {@code x+1}
 *       ({@link Read#aliasesRead}: the aliases each such item reads, which {@link IqNames} renders as written).</li>
 * </ul>
 * Under ansinull Off an alias read in a search condition whose expression holds a host variable is refused by name:
 * where SQL Anywhere reads such a comparison two-valued is measured for comparisons written in place only.
 */
final class IqAliases {

    /** The statement with every alias read in place, the subquery names that may read an enclosing query's alias
     *  (by identity), and the aliases (lower case) each unaliased select item reads (by identity); IQ island,
     *  2026-10-14 (review 7's B2): each unaliased item read from a query without FROM that is named by that query's
     *  own alias, and those that name no derived table's column ({@link #select}), by identity. */
    record Read(SqlAst.Statement statement, Set<SqlAst.ColumnRef> outerReads,
            Map<SqlAst.SelectExpr, Set<String>> aliasesRead, Map<SqlAst.SelectExpr, SqlAst.Ident> innerNames,
            Set<SqlAst.SelectExpr> nameless) {
    }

    private final Map<SqlAst.Expr, String> spans;
    private final boolean ansinull;
    /** Whether the statement runs in SQL Anywhere's front end, which takes a subquery anywhere ({@link #fromless}). */
    private final boolean frontEnd;
    private final Set<SqlAst.ColumnRef> outerReads = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<SqlAst.SelectExpr, Set<String>> aliasesRead = new IdentityHashMap<>();
    private final Map<SqlAst.Statement, SqlAst.Statement> done = new IdentityHashMap<>();
    /** IQ island, 2026-10-14 (review 7's B2): the SELECTs whose items name a derived table's columns (its query's
     *  first, where it has no column list), by identity; the unaliased items unwrapped from a query without FROM,
     *  named by that query's alias, and those of a SELECT that names a derived table's columns. */
    private final Set<SqlAst.Select> naming = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<SqlAst.SelectExpr, SqlAst.Ident> innerNames = new IdentityHashMap<>();
    private final Set<SqlAst.SelectExpr> nameless = Collections.newSetFromMap(new IdentityHashMap<>());

    private IqAliases(Map<SqlAst.Expr, String> spans, boolean ansinull, boolean frontEnd) {
        this.spans = spans;
        this.ansinull = ansinull;
        this.frontEnd = frontEnd;
    }

    /** {@code statement} with its aliases read where IQ reads them; {@code spans} (the texts the reader keeps by
     *  identity) gains each copy's. {@code frontEnd}: the statement runs in SQL Anywhere's front end. */
    static Read resolve(SqlAst.Statement statement, Map<SqlAst.Expr, String> spans, IqSettings settings,
            boolean frontEnd) {
        IqAliases r = new IqAliases(spans, settings.ansinull(), frontEnd);
        for (SqlAst.Statement q : IqDialect.queries(statement)) {
            if (q instanceof SqlAst.Select s) {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d && d.columnAliases().isEmpty()) {
                            SqlAst.Statement first = d.query();
                            while (first instanceof SqlAst.SetOperation o) {
                                first = o.left();
                            }
                            if (first instanceof SqlAst.Select named) {
                                r.naming.add(named);
                            }
                        }
                    }
                }
            }
        }
        SqlAst.Statement out = r.statement(statement, List.of());
        return new Read(out, r.outerReads, r.aliasesRead, r.innerNames, r.nameless);
    }

    // ------------------------------------------------------------------ queries

    private SqlAst.Statement statement(SqlAst.Statement q, List<Scope> outer) {
        SqlAst.Statement seen = done.get(q);
        if (seen != null) {
            return seen;
        }
        SqlAst.Statement out = switch (q) {
            case SqlAst.Select s -> select(s, outer);
            case SqlAst.SetOperation o -> {
                SqlAst.Statement l = statement(o.left(), outer);
                SqlAst.Statement r = statement(o.right(), outer);
                yield l == o.left() && r == o.right() ? o : new SqlAst.SetOperation(o.op(), o.all(), l, r,
                        o.orderBy(), o.limit(), o.offset(), o.pos());
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
            case SqlAst.Write w -> throw new IllegalStateException("IQ's reader makes no write");
        };
        done.put(q, out);
        return out;
    }

    /** One SELECT's aliases: each (lower case) to its item, and their definitions as read (memoized). */
    private final class Scope {
        final SqlAst.Select select;
        final Map<String, SqlAst.SelectExpr> aliases = new HashMap<>();
        final Map<String, SqlAst.Expr> expanded = new HashMap<>();
        final List<String> expanding = new ArrayList<>();
        final List<Scope> outer;
        /** The FROM items' correlation names (lower case), and the names of the columns known before translating
         *  (null when some item's are not). */
        final Set<String> correlations = new java.util.HashSet<>();
        /** IQ island, 2026-10-14 (the third landing): each FROM item by its correlation name (lower case): the owner
         *  (lower case) of an unaliased table, else null. */
        final Map<String, @com.legend.base.Nullable String> owners = new HashMap<>();
        final @com.legend.base.Nullable Set<String> known;

        Scope(SqlAst.Select select, List<Scope> outer) {
            this.select = select;
            this.outer = outer;
            for (SqlAst.SelectItem item : select.items()) {
                if (item instanceof SqlAst.SelectExpr x && x.alias() != null) {
                    aliases.putIfAbsent(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()), x);
                }
            }
            Set<String> cols = new java.util.HashSet<>();
            boolean all = true;
            for (SqlAst.FromItem f : select.from()) {
                for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                    SqlAst.Ident c = IqDialect.correlationOf(leaf);
                    if (c != null) {
                        correlations.add(IqLexer.lower(c.text()));
                        owners.put(IqLexer.lower(c.text()), ownerOf(leaf));
                    }
                    Set<String> k = IqDialect.knownColumns(leaf);
                    if (k == null) {
                        all = false;
                    } else {
                        cols.addAll(k);
                    }
                }
            }
            known = all ? cols : null;
        }

        /** Whether {@code name} (lower case) is an alias read as its expression: not one that is the column of its
         *  own name. */
        boolean reads(String name) {
            SqlAst.SelectExpr x = aliases.get(name);
            if (x == null) {
                return false;
            }
            return !(x.expr() instanceof SqlAst.ColumnRef r && r.parts().size() == 1
                    && IqLexer.lower(r.parts().get(0).text()).equals(name));
        }

        /** The definition of alias {@code name} as read: its expression with every other alias read in place. */
        SqlAst.Expr expand(String name, SqlAst.Pos at) {
            SqlAst.Expr e = expanded.get(name);
            if (e != null) {
                return e;
            }
            if (expanding.contains(name)) {
                String current = expanding.get(expanding.size() - 1);
                SqlAst.SelectExpr x = java.util.Objects.requireNonNull(aliases.get(current));
                throw SqlTranslationException.invalid("the alias '" + java.util.Objects.requireNonNull(x.alias())
                        .text() + "' is defined through itself: an alias's definition reads another alias whose"
                        + " definition reads it" + IqDialect.code(-831, "42W61"), at);
            }
            expanding.add(name);
            SqlAst.SelectExpr x = java.util.Objects.requireNonNull(aliases.get(name));
            Place place = new Place(this, name, Clause.SELECT, null, null);
            e = expr(x.expr(), place);
            expanding.remove(expanding.size() - 1);
            expanded.put(name, e);
            return e;
        }
    }

    /** Where an expression is read: its SELECT, the alias whose definition it is (its own name is a column there),
     *  the clause, the ON's join (its scope), and the aliases read so far into an unaliased item (else null). */
    private record Place(Scope scope, @com.legend.base.Nullable String own, Clause clause,
            SqlAst.@com.legend.base.Nullable Join on, @com.legend.base.Nullable Set<String> read) {
    }

    /** The clauses an alias is read in: GROUP BY and HAVING are read by {@link IqAggregates#grouping} (only their
     *  subqueries are read here). */
    private enum Clause { SELECT, WHERE, ON, ORDER_BY, GROUPING }

    private SqlAst.Select select(SqlAst.Select written, List<Scope> outer) {
        // IQ island, 2026-10-14 (the third landing): a whole item that is a query without FROM is its one item's
        // value, named as that item written here (measured: SELECT (SELECT id + 1) FROM t names it t.id+1).
        // (Review 7's B2, measured on SQL Anywhere 17) Unaliased, it is named by that query's own alias where it has
        // one ((SELECT id + 1 AS z) is z; the name is no alias a clause reads: WHERE z is -143), and it names no
        // derived table's column (SELECT * FROM (SELECT (SELECT id) FROM t) d is -163, whatever its own alias)
        List<SqlAst.SelectItem> unwrapped = new ArrayList<>();
        Map<SqlAst.SelectExpr, SqlAst.Ident> innerAlias = new IdentityHashMap<>();
        Set<SqlAst.SelectExpr> unnamed = Collections.newSetFromMap(new IdentityHashMap<>());
        boolean changed = false;
        for (SqlAst.SelectItem item : written.items()) {
            SqlAst.SelectExpr inner = item instanceof SqlAst.SelectExpr x && x.expr() instanceof SqlAst.ScalarSubquery q
                    ? fromless(q.query()) : null;
            if (inner != null) {
                SqlAst.SelectExpr x = (SqlAst.SelectExpr) item;
                SqlAst.SelectExpr y = new SqlAst.SelectExpr(inner.expr(), x.alias(), x.pos(), x.alias() != null
                        ? x.text() : inner.text());
                unwrapped.add(y);
                if (x.alias() == null) {
                    unnamed.add(y);
                    if (inner.alias() != null) {
                        innerAlias.put(y, java.util.Objects.requireNonNull(inner.alias()));
                    }
                }
                changed = true;
            } else {
                unwrapped.add(item);
            }
        }
        SqlAst.Select s = !changed ? written : new SqlAst.Select(written.distinct(), unwrapped, written.from(),
                written.where(), written.groupBy(), written.having(), written.windows(), written.orderBy(),
                written.limit(), written.offset(), written.pos());
        Scope scope = new Scope(s, outer);
        List<SqlAst.FromItem> from = new ArrayList<>();
        for (SqlAst.FromItem f : s.from()) {
            SqlAst.FromItem g = fromItem(f, scope);
            changed |= g != f;
            from.add(g);
        }
        List<SqlAst.SelectItem> items = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (!(item instanceof SqlAst.SelectExpr x)) {
                items.add(item);
                continue;
            }
            SqlAst.Expr e;
            SqlAst.Ident alias = x.alias();
            if (alias != null) {
                e = scope.aliases.get(IqLexer.lower(alias.text())) == x ? scope.expand(IqLexer.lower(alias.text()),
                        x.pos()) : expr(x.expr(), new Place(scope, null, Clause.SELECT, null, null));
            } else {
                Set<String> read = new java.util.HashSet<>();
                e = expr(x.expr(), new Place(scope, null, Clause.SELECT, null, read));
                SqlAst.Expr bare = x.expr();
                while (bare instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
                    bare = u.operand();
                }
                if (bare instanceof SqlAst.ColumnRef r && r.parts().size() == 1 && e != x.expr()) {
                    // an alias read bare: shown under its name as written (measured: SELECT id + 1 AS x, x shows
                    // two columns named x)
                    SqlAst.Ident name = r.parts().get(0);
                    alias = new SqlAst.Ident(name.text(), name.quoted(), name.pos());
                }
                if (!read.isEmpty()) {
                    SqlAst.SelectExpr y = new SqlAst.SelectExpr(e, alias, x.pos(), x.text());
                    aliasesRead.put(y, read);
                    items.add(y);
                    changed = true;
                    continue;
                }
            }
            if (e != x.expr() || alias != x.alias()) {
                items.add(new SqlAst.SelectExpr(e, alias, x.pos(), x.text()));
                changed = true;
            } else {
                items.add(x);
            }
        }
        SqlAst.Expr where = s.where() == null ? null : expr(java.util.Objects.requireNonNull(s.where()),
                new Place(scope, null, Clause.WHERE, null, null));
        changed |= where != s.where();
        List<SqlAst.Expr> groupBy = new ArrayList<>();
        for (SqlAst.Expr k : s.groupBy()) {
            SqlAst.Expr g = expr(k, new Place(scope, null, Clause.GROUPING, null, null));
            changed |= g != k;
            groupBy.add(g);
        }
        SqlAst.Expr having = s.having() == null ? null : expr(java.util.Objects.requireNonNull(s.having()),
                new Place(scope, null, Clause.GROUPING, null, null));
        changed |= having != s.having();
        List<SqlAst.SortItem> order = new ArrayList<>();
        for (SqlAst.SortItem o : s.orderBy()) {
            SqlAst.Expr k = o.expr();
            // a bare name or a position names an output column (the translation's); an expression reads aliases
            SqlAst.Expr r = k instanceof SqlAst.IntegerLit || k instanceof SqlAst.ColumnRef c && c.parts().size() < 3
                    ? k : expr(k, new Place(scope, null, Clause.ORDER_BY, null, null));
            changed |= r != k;
            order.add(r == k ? o : new SqlAst.SortItem(r, o.descending(), o.nullsFirst(), o.pos()));
        }
        for (int i = 0; i < items.size(); i++) {
            if (s.items().get(i) instanceof SqlAst.SelectExpr x && items.get(i) instanceof SqlAst.SelectExpr y) {
                SqlAst.Ident name = innerAlias.get(x);
                if (name != null) {
                    innerNames.put(y, name);
                }
                if (unnamed.contains(x) && naming.contains(written)) {
                    nameless.add(y);
                }
            }
        }
        return !changed ? s : new SqlAst.Select(s.distinct(), items, from, where, groupBy, having, s.windows(), order,
                s.limit(), s.offset(), s.pos());
    }

    /** A FROM item with its ON conditions' aliases read in place and its derived tables' queries read (a derived
     *  table does not read its query's aliases, nor its columns). */
    private SqlAst.FromItem fromItem(SqlAst.FromItem f, Scope scope) {
        return switch (f) {
            case SqlAst.SubqueryFrom d -> {
                SqlAst.Statement q = statement(d.query(), scope.outer);
                yield q == d.query() ? d : new SqlAst.SubqueryFrom(q, d.alias(), d.columnAliases(), d.lateral(),
                        d.pos());
            }
            case SqlAst.Join j -> {
                SqlAst.FromItem l = fromItem(j.left(), scope);
                SqlAst.FromItem r = fromItem(j.right(), scope);
                SqlAst.Expr on = j.on() == null ? null : expr(java.util.Objects.requireNonNull(j.on()),
                        new Place(scope, null, Clause.ON, j, null));
                yield l == j.left() && r == j.right() && on == j.on() ? j : new SqlAst.Join(j.type(), j.natural(), l, r,
                        on, j.using(), j.pos());
            }
            default -> f;
        };
    }

    // ------------------------------------------------------------------ expressions

    /** {@code e} with each alias it reads in place ({@code e} itself when it reads none), its subqueries read. */
    private SqlAst.Expr expr(SqlAst.Expr e, Place p) {
        return switch (e) {
            case SqlAst.ColumnRef r -> column(r, p);
            case SqlAst.Binary b -> {
                SqlAst.Expr l = expr(b.left(), p);
                SqlAst.Expr r = expr(b.right(), p);
                yield l == b.left() && r == b.right() ? b : new SqlAst.Binary(b.op(), l, r, b.pos());
            }
            case SqlAst.Unary u -> {
                SqlAst.Expr o = expr(u.operand(), p);
                yield o == u.operand() ? u : new SqlAst.Unary(u.op(), o, u.pos());
            }
            case SqlAst.IsNull n -> {
                SqlAst.Expr o = expr(n.operand(), p);
                yield o == n.operand() ? n : new SqlAst.IsNull(o, n.negated(), n.pos());
            }
            case SqlAst.IsTruth t -> {
                SqlAst.Expr o = expr(t.operand(), p);
                yield o == t.operand() ? t : new SqlAst.IsTruth(o, t.truth(), t.negated(), t.pos());
            }
            case SqlAst.IsDistinct d -> {
                SqlAst.Expr l = expr(d.left(), p);
                SqlAst.Expr r = expr(d.right(), p);
                yield l == d.left() && r == d.right() ? d : new SqlAst.IsDistinct(l, r, d.negated(), d.pos());
            }
            case SqlAst.Between b -> {
                SqlAst.Expr o = expr(b.operand(), p);
                SqlAst.Expr lo = expr(b.low(), p);
                SqlAst.Expr hi = expr(b.high(), p);
                yield o == b.operand() && lo == b.low() && hi == b.high() ? b
                        : new SqlAst.Between(o, lo, hi, b.negated(), b.pos());
            }
            case SqlAst.InList in -> {
                SqlAst.Expr o = expr(in.operand(), p);
                List<SqlAst.Expr> values = all(in.values(), p);
                yield o == in.operand() && values == in.values() ? in
                        : new SqlAst.InList(o, values, in.negated(), in.pos());
            }
            case SqlAst.Like l -> {
                SqlAst.Expr o = expr(l.operand(), p);
                SqlAst.Expr pat = expr(l.pattern(), p);
                SqlAst.Expr esc = l.escape() == null ? null : expr(java.util.Objects.requireNonNull(l.escape()), p);
                yield o == l.operand() && pat == l.pattern() && esc == l.escape() ? l
                        : new SqlAst.Like(o, pat, esc, l.negated(), l.caseInsensitive(), l.pos());
            }
            case SqlAst.FunctionCall f -> {
                List<SqlAst.Expr> args = all(f.args(), p);
                boolean same = args == f.args();
                List<SqlAst.SortItem> order = new ArrayList<>();
                for (SqlAst.SortItem o : f.orderBy()) {
                    SqlAst.Expr k = expr(o.expr(), p);
                    same &= k == o.expr();
                    order.add(k == o.expr() ? o : new SqlAst.SortItem(k, o.descending(), o.nullsFirst(), o.pos()));
                }
                yield same ? f : copied(f, new SqlAst.FunctionCall(f.name(), args, f.distinct(), f.star(), order,
                        f.withinGroup(), f.filter(), f.pos()));
            }
            case SqlAst.Case c -> {
                SqlAst.Expr operand = c.operand() == null ? null : expr(java.util.Objects.requireNonNull(c.operand()),
                        p);
                boolean same = operand == c.operand();
                List<SqlAst.When> whens = new ArrayList<>();
                for (SqlAst.When w : c.whens()) {
                    SqlAst.Expr cond = expr(w.condition(), p);
                    SqlAst.Expr result = expr(w.result(), p);
                    same &= cond == w.condition() && result == w.result();
                    whens.add(new SqlAst.When(cond, result));
                }
                SqlAst.Expr otherwise = c.otherwise() == null ? null
                        : expr(java.util.Objects.requireNonNull(c.otherwise()), p);
                same &= otherwise == c.otherwise();
                yield same ? c : new SqlAst.Case(operand, whens, otherwise, c.pos());
            }
            case SqlAst.Cast c -> {
                SqlAst.Expr o = expr(c.operand(), p);
                yield o == c.operand() ? c : new SqlAst.Cast(o, c.type(), c.pos());
            }
            case SqlAst.ScalarSubquery q -> {
                SqlAst.SelectExpr one = frontEnd ? fromless(q.query()) : null;
                if (one != null) {
                    // in SQL Anywhere's front end a query without FROM is its one item's value wherever it stands
                    yield expr(one.expr(), p);
                }
                SqlAst.Statement s = inner(q.query(), p);
                yield s == q.query() ? q : new SqlAst.ScalarSubquery(s, q.pos());
            }
            case SqlAst.Exists q -> {
                SqlAst.Statement s = inner(q.query(), p);
                yield s == q.query() ? q : new SqlAst.Exists(s, q.pos());
            }
            case SqlAst.InSubquery q -> {
                SqlAst.Expr o = expr(q.operand(), p);
                SqlAst.Statement s = inner(q.query(), p);
                yield o == q.operand() && s == q.query() ? q : new SqlAst.InSubquery(o, s, q.negated(), q.pos());
            }
            case SqlAst.Quantified q -> {
                SqlAst.Expr l = expr(q.left(), p);
                SqlAst.Statement s = inner(q.query(), p);
                yield l == q.left() && s == q.query() ? q : new SqlAst.Quantified(q.op(), q.all(), l, s, q.pos());
            }
            // IQ island, 2026-10-15 (windows): an alias in a window's arguments, PARTITION BY and ORDER BY is read as
            // anywhere (measured on IQ 17.1: PARTITION BY an alias, SUM(alias) OVER ...)
            case SqlAst.Window w -> IqWindows.rebuilt(w, x -> expr(x, p), this::copied);
            // IQ island, 2026-10-15 (checklist 0.6): a T-SQL outer join's predicate, *= or =*
            case SqlAst.Operator o when o.left() != null -> {
                SqlAst.Expr l = expr(java.util.Objects.requireNonNull(o.left()), p);
                SqlAst.Expr r = expr(o.right(), p);
                yield l == o.left() && r == o.right() ? o : new SqlAst.Operator(o.operator(), l, r, o.pos());
            }
            default -> {
                if (!IqDialect.children(e).isEmpty()) {
                    throw new IllegalStateException("the #IQ{...}# alias reader does not walk "
                            + e.getClass().getSimpleName() + " yet");
                }
                yield e;
            }
        };
    }

    private List<SqlAst.Expr> all(List<SqlAst.Expr> values, Place p) {
        List<SqlAst.Expr> out = new ArrayList<>();
        boolean same = true;
        for (SqlAst.Expr v : values) {
            SqlAst.Expr r = expr(v, p);
            same &= r == v;
            out.add(r);
        }
        return same ? values : out;
    }

    /** A subquery's query, read with {@code p}'s SELECT the nearest enclosing one. */
    private SqlAst.Statement inner(SqlAst.Statement q, Place p) {
        List<Scope> outer = new ArrayList<>();
        outer.add(p.scope());
        outer.addAll(p.scope().outer);
        return statement(q, outer);
    }

    /** A column reference: an alias of its SELECT read in place; a name an enclosing SELECT's alias may be,
     *  recorded; {@code owner.table.column} as its table's column ({@link #owned}). */
    private SqlAst.Expr column(SqlAst.ColumnRef r, Place p) {
        if (r.parts().size() == 3) {
            return owned(r, p.scope());
        }
        if (r.parts().size() != 1) {
            return r;
        }
        String name = IqLexer.lower(r.parts().get(0).text());
        Scope s = p.scope();
        if (!name.equals(p.own()) && s.reads(name)) {
            if (!frontEnd && (p.clause() == Clause.WHERE || p.clause() == Clause.GROUPING)
                    && hasSubquery(s.expand(name, r.pos()))) {
                // IQ island, 2026-10-16 (the corpus lane; measured on IQ 17.1): read in the select list or ORDER BY it
                // is answered
                throw SqlTranslationException.unsupported("the alias '" + r.parts().get(0).text() + "' of an"
                        + " expression holding a subquery, read in a WHERE, a GROUP BY or a HAVING, is IQ's error"
                        + " -1001030 under its engine (a reference containing a scalar value subquery outside of a top"
                        + " level SELECT list or ORDER BY), refused before translating", r.pos());
            }
            if (p.clause() == Clause.GROUPING) {
                // GROUP BY and HAVING read it as IqAggregates.grouping does
                return r;
            }
            SqlAst.Expr def = s.expand(name, r.pos());
            if ((p.clause() == Clause.WHERE || p.clause() == Clause.ON) && !ansinull && hasVariable(def)) {
                throw SqlTranslationException.unsupported("the alias '" + r.parts().get(0).text() + "' read in a"
                        + " search condition under ansinull Off is not supported: its expression reads a host variable,"
                        + " and how IQ reads such a comparison there is measured written in place only", r.pos());
            }
            if (p.clause() == Clause.ON) {
                inScope(def, java.util.Objects.requireNonNull(p.on()), s, r);
            }
            if (p.read() != null) {
                java.util.Objects.requireNonNull(p.read()).add(name);
            }
            return copy(def);
        }
        if (s.aliases.containsKey(name) && !name.equals(p.own()) && p.read() != null) {
            // an alias that is the column of its own name: read as the column, named as the alias (measured:
            // SELECT id AS id, id + 1 names it id+1, where SELECT id, id + 1 names it t.id+1)
            java.util.Objects.requireNonNull(p.read()).add(name);
        }
        if (s.known != null && s.known.contains(name) || name.equals(p.own()) || s.aliases.containsKey(name)) {
            // a column of its own query's FROM items (known before translating), or its own SELECT's name
            return r;
        }
        for (Scope o : s.outer) {
            if (o.aliases.containsKey(name)) {
                if (o.reads(name)) {
                    outerReads.add(r);
                }
                return r;
            }
            if (o.known != null && o.known.contains(name)) {
                return r;
            }
        }
        return r;
    }

    /**
     * IQ island, 2026-10-14 (the third landing; measured on SQL Anywhere 17): {@code owner.table.column} reads the
     * column of the FROM item (of its query, else of the nearest enclosing one) known by {@code table} -- an
     * unaliased table of that owner (as IQ matches names: {@code DBA.t.id} over {@code FROM t} or {@code FROM DBA.t}).
     * The item known by that name being aliased, a derived table, or another owner's table is IQ's error -845; no
     * item known by it, -142.
     */
    private static SqlAst.ColumnRef owned(SqlAst.ColumnRef r, Scope scope) {
        SqlAst.Ident owner = r.parts().get(0);
        SqlAst.Ident table = r.parts().get(1);
        String name = IqLexer.lower(table.text());
        for (Scope s = scope; s != null; s = s.outer.isEmpty() ? null : s.outer.get(0)) {
            if (!s.owners.containsKey(name)) {
                continue;
            }
            String held = s.owners.get(name);
            if (held == null || !held.equals(IqLexer.lower(owner.text()))) {
                throw SqlTranslationException.invalid("the owner '" + owner.text() + "' of the column reference "
                        + owner.text() + "." + table.text() + "." + r.parts().get(2).text() + " is not the owner of the"
                        + " FROM item known as '" + table.text() + "'" + IqDialect.code(-845, "42U02"), owner.pos());
            }
            return new SqlAst.ColumnRef(r.parts().subList(1, 3), table.pos());
        }
        throw SqlTranslationException.invalid("no table or correlation name '" + table.text() + "' in the query"
                + IqDialect.code(-142, "52W02"), owner.pos());
    }

    /** The owner (lower case) of an unaliased table in FROM, as IQ knows it ({@link IqBindings}): its qualifier, else
     *  the island's user; a source function's table its schema, else the island's user; DUMMY's SYS; null for an
     *  aliased item, a derived table and a procedure (IQ island, 2026-10-14, review 7's M2, measured on SQL Anywhere
     *  17: an owner-qualified column of sa_rowgenerator in FROM is -845 whatever the owner, dbo as DBA, and whether or
     *  not FROM writes dbo; {@code sa_rowgenerator.row_num} is read). */
    private static @com.legend.base.Nullable String ownerOf(SqlAst.FromItem leaf) {
        return switch (leaf) {
            case SqlAst.TableName t -> {
                if (t.alias() != null) {
                    yield null;
                }
                SqlAst.Ident owner = IqBindings.owner(t.name());
                String last = IqLexer.lower(t.name().get(t.name().size() - 1).text());
                yield owner != null ? IqLexer.lower(owner.text()) : last.equals("dummy") ? "sys"
                        : IqLexer.lower(IqBindings.USER);
            }
            case SqlAst.TableFunction f -> {
                String fn = IqLexer.lower(f.name().get(f.name().size() - 1).text());
                if (fn.equals("sa_rowgenerator")) {
                    yield null;
                }
                if (!fn.equals("tb") || f.args().size() != 1
                        || !(f.args().get(0).value() instanceof SqlAst.StringLit path)) {
                    yield null;
                }
                String p = path.value();
                int first = p.indexOf('.');
                int last = p.lastIndexOf('.');
                // tb('...t') AS t is the island's spelling of the table t, unaliased (IQ's own t AS t is IQ's -845:
                // the island binds a table named so, IqBindings, and keeps its alias)
                SqlAst.Ident alias = f.alias();
                if (alias != null && !IqLexer.lower(alias.text()).equals(IqLexer.lower(p.substring(last + 1)))) {
                    yield null;
                }
                yield first > 0 && last > first ? IqLexer.lower(p.substring(first + 1, last))
                        : IqLexer.lower(IqBindings.USER);
            }
            default -> null;
        };
    }

    /** An alias's definition read in {@code on}'s condition reads only that join's FROM items: one of the SELECT's
     *  others named there is IQ's error -824 (measured); an unqualified column, or a subquery, is refused by name
     *  unless every FROM item of the SELECT is in the join's scope. */
    private static void inScope(SqlAst.Expr def, SqlAst.Join on, Scope s, SqlAst.ColumnRef at) {
        Set<String> here = new java.util.HashSet<>();
        for (SqlAst.FromItem leaf : IqDialect.leaves(on)) {
            SqlAst.Ident c = IqDialect.correlationOf(leaf);
            if (c != null) {
                here.add(IqLexer.lower(c.text()));
            }
        }
        boolean everything = here.containsAll(s.correlations);
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        IqDialect.columnRefs(def, refs);
        for (SqlAst.ColumnRef ref : refs) {
            if (ref.parts().size() == 2) {
                String q = IqLexer.lower(ref.parts().get(0).text());
                if (s.correlations.contains(q) && !here.contains(q)) {
                    throw SqlTranslationException.invalid("the alias '" + at.parts().get(0).text() + "' read in this"
                            + " ON names '" + ref.parts().get(0).text() + "', a FROM item outside the join"
                            + IqDialect.code(-824, "42U01"), at.pos());
                }
            } else if (!everything) {
                throw SqlTranslationException.unsupported("the alias '" + at.parts().get(0).text() + "' read in this"
                        + " ON is not supported: its expression names a column without its table, and which FROM item"
                        + " holds it is not known before translating; qualify it", at.pos());
            }
        }
        if (!everything && hasSubquery(def)) {
            throw SqlTranslationException.unsupported("the alias '" + at.parts().get(0).text() + "' read in this ON is"
                    + " not supported: its expression holds a subquery", at.pos());
        }
    }

    private static boolean hasVariable(SqlAst.Expr e) {
        if (e instanceof SqlAst.Param) {
            return true;
        }
        for (SqlAst.Expr c : IqDialect.children(e)) {
            if (hasVariable(c)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasSubquery(SqlAst.Expr e) {
        if (e instanceof SqlAst.ScalarSubquery || e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery
                || e instanceof SqlAst.Quantified) {
            return true;
        }
        for (SqlAst.Expr c : IqDialect.children(e)) {
            if (hasSubquery(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * IQ island, 2026-10-14 (the third landing; measured on SQL Anywhere 17): the one item of {@code q} when it is a
     * query without FROM that is its value -- one item, no WHERE, grouping, ORDER BY, row limit, aggregate or
     * subquery: {@code (SELECT x)} reads x in the enclosing query's row; else null. IQ's engine takes a subquery only
     * as a whole select-list item: there it is read under every engine, anywhere else in SQL Anywhere's front end
     * only (a subquery's place is IQ's restriction, the research's D-R1, read by {@link IqDialect}).
     */
    private static SqlAst.@com.legend.base.Nullable SelectExpr fromless(SqlAst.Statement q) {
        if (!(q instanceof SqlAst.Select s) || !s.from().isEmpty() || s.where() != null || !s.groupBy().isEmpty()
                || s.having() != null || !s.orderBy().isEmpty() || s.limit() != null || s.offset() != null
                || s.distinct() || s.items().size() != 1 || !(s.items().get(0) instanceof SqlAst.SelectExpr x)
                || !IqAggregates.aggregateCalls(x.expr()).isEmpty() || hasSubquery(x.expr()) || x.text() == null) {
            return null;
        }
        return x;
    }

    // ------------------------------------------------------------------ copies

    /** {@code e} copied, node by node (a subquery's query shared, as read): the copy is read where it is put, apart
     *  from the alias's own item; the reader's texts follow each node copied. */
    private SqlAst.Expr copy(SqlAst.Expr e) {
        SqlAst.Expr c = switch (e) {
            case SqlAst.ColumnRef r -> new SqlAst.ColumnRef(r.parts(), r.pos());
            case SqlAst.IntegerLit n -> new SqlAst.IntegerLit(n.value(), n.pos());
            case SqlAst.NumericLit n -> new SqlAst.NumericLit(n.value(), n.pos());
            case SqlAst.StringLit n -> new SqlAst.StringLit(n.value(), n.pos());
            case SqlAst.NullLit n -> new SqlAst.NullLit(n.pos());
            case SqlAst.Param q -> new SqlAst.Param(q.index(), q.name(), q.pos());
            case SqlAst.Binary b -> new SqlAst.Binary(b.op(), copy(b.left()), copy(b.right()), b.pos());
            case SqlAst.Unary u -> new SqlAst.Unary(u.op(), copy(u.operand()), u.pos());
            case SqlAst.IsNull n -> new SqlAst.IsNull(copy(n.operand()), n.negated(), n.pos());
            case SqlAst.IsTruth t -> new SqlAst.IsTruth(copy(t.operand()), t.truth(), t.negated(), t.pos());
            case SqlAst.IsDistinct d -> new SqlAst.IsDistinct(copy(d.left()), copy(d.right()), d.negated(), d.pos());
            case SqlAst.Between b -> new SqlAst.Between(copy(b.operand()), copy(b.low()), copy(b.high()), b.negated(),
                    b.pos());
            case SqlAst.InList in -> new SqlAst.InList(copy(in.operand()), copyAll(in.values()), in.negated(),
                    in.pos());
            case SqlAst.Like l -> new SqlAst.Like(copy(l.operand()), copy(l.pattern()), l.escape() == null ? null
                    : copy(java.util.Objects.requireNonNull(l.escape())), l.negated(), l.caseInsensitive(), l.pos());
            case SqlAst.FunctionCall f -> {
                List<SqlAst.SortItem> order = new ArrayList<>();
                f.orderBy().forEach(o -> order.add(new SqlAst.SortItem(copy(o.expr()), o.descending(),
                        o.nullsFirst(), o.pos())));
                yield new SqlAst.FunctionCall(f.name(), copyAll(f.args()), f.distinct(), f.star(), order,
                        f.withinGroup(), f.filter(), f.pos());
            }
            case SqlAst.Case k -> {
                List<SqlAst.When> whens = new ArrayList<>();
                k.whens().forEach(w -> whens.add(new SqlAst.When(copy(w.condition()), copy(w.result()))));
                yield new SqlAst.Case(k.operand() == null ? null : copy(java.util.Objects.requireNonNull(k.operand())),
                        whens, k.otherwise() == null ? null : copy(java.util.Objects.requireNonNull(k.otherwise())),
                        k.pos());
            }
            case SqlAst.Cast k -> new SqlAst.Cast(copy(k.operand()), k.type(), k.pos());
            case SqlAst.ScalarSubquery q -> new SqlAst.ScalarSubquery(q.query(), q.pos());
            case SqlAst.Exists q -> new SqlAst.Exists(q.query(), q.pos());
            case SqlAst.InSubquery q -> new SqlAst.InSubquery(copy(q.operand()), q.query(), q.negated(), q.pos());
            case SqlAst.Quantified q -> new SqlAst.Quantified(q.op(), q.all(), copy(q.left()), q.query(), q.pos());
            // IQ island, 2026-10-15 (windows)
            case SqlAst.Window w -> IqWindows.copied(w, this::copy, this::copied);
            default -> throw new IllegalStateException("the #IQ{...}# alias reader does not copy "
                    + e.getClass().getSimpleName() + " yet");
        };
        return copied(e, c);
    }

    private List<SqlAst.Expr> copyAll(List<SqlAst.Expr> values) {
        List<SqlAst.Expr> out = new ArrayList<>();
        values.forEach(v -> out.add(copy(v)));
        return out;
    }

    /** {@code to}, a copy of {@code from}, with {@code from}'s text kept by the reader. */
    private SqlAst.Expr copied(SqlAst.Expr from, SqlAst.Expr to) {
        String text = spans.get(from);
        if (text != null) {
            spans.put(to, text);
        }
        return to;
    }
}
