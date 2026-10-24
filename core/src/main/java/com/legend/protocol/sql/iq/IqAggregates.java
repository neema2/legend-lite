// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlToPure.Kind;
import com.legend.protocol.sql.SqlToPure.Tx;
import com.legend.protocol.sql.SqlTranslationException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * IQ's grouping as the {@code #IQ{...}#} island reads it (M1c; docs/IQ_SQL_STUDY_2026_10_13.md §9.5): its aggregates'
 * meanings and types, how it reads GROUP BY and HAVING, the errors it raises before running a grouped query, and the
 * names it gives a grouped query's unaliased items. Every rule was measured on SQL Anywhere 17, whose front end types
 * and names every IQ expression; the IQ engine's own answers are the research's decisions where they differ.
 * <ul>
 *   <li><b>GROUP BY and HAVING</b> read a select-list alias before a column of its name, anywhere in them (in an
 *       aggregate's argument too); an integer constant there is a constant, never a position ({@code SELECT k, COUNT(*)
 *       ... GROUP BY 1} is IQ's error -149). A GROUP BY of constants only groups all rows (IQ island, 2026-10-17,
 *       measured on IQ 17.1, where the research's D-R1 held that IQ's engine restricts it). Strings group by IQ's collation
 *       ({@code 'a'} and {@code 'a '} are one group in IQ, {@code 'A'} and {@code 'a'} in a {@code sqla17} profile;
 *       IQ island, 2026-10-14, {@link IqKeys}): a key the query shows only where each value is its own key.</li>
 *   <li><b>COUNT</b> ({@code *}, a value, DISTINCT -- of strings, the distinct collation keys) counts as everywhere; its type is SQL Anywhere's INTEGER in its
 *       front end and IQ's UNSIGNED BIGINT under the IQ engine (the research's D-F15): the value is the same, so a
 *       COUNT shown, compared or ordered is exact in both, while arithmetic on it is refused under the IQ engine
 *       (unsigned types are not computed by the island).</li>
 *   <li><b>SUM</b> of a SMALLINT or an INTEGER is an INTEGER, of a BIGINT a BIGINT, of a BIT or a TINYINT a
 *       NUMERIC(18,0), of a DOUBLE a DOUBLE, of a NUMERIC(p,s) in SQL Anywhere's front end a NUMERIC(p+8,s), under the
 *       IQ engine a NUMERIC(p+1,s) (the research's D-N1, measured on IQ 17.1, 2026-10-15). SQL Anywhere checks each running total against the type, in the plan's
 *       order (measured: 2147483647, 1, -5 raises -158 in one order and answers 2147483643 in another): the island
 *       raises IQ's -158 where some order could overflow -- the positive values' sum above the type's largest, or the
 *       negative values' below its smallest -- so that it never answers where SQL Anywhere raises; where only some
 *       orders overflow it raises and SQL Anywhere may answer (provisional decision D-E3, the study's §9.5).</li>
 *   <li><b>AVG</b> of a DOUBLE is a DOUBLE, checked as its SUM is. AVG of a NUMERIC(p,s) in SQL Anywhere's front end
 *       is a NUMERIC(p+19, s+11), SUM / COUNT by SQL Anywhere's quotient ({@link IqFunctions#quotient}; IQ island,
 *       2026-10-14), where it is shown (an operand reads it unrounded); of an integer (a NUMERIC(49,21), beyond lite's
 *       38 digits), refused. Under the IQ engine (D-N1, measured on IQ 17.1, 2026-10-15) AVG of an integer is a
 *       NUMERIC of scale 16, of a NUMERIC(p,s) one of scale s+21, SUM / COUNT cut toward zero there, wherever read.</li>
 *   <li><b>MIN, MAX</b> keep their argument's type: integers, NUMERICs, DOUBLEs, dates and timestamps; strings where
 *       each value is its own collation key ({@link IqKeys}: IQ shows the first value of the least key it meets).</li>
 *   <li><b>MEDIAN</b> of a SMALLINT or an INTEGER keeps the type, the mean of the two middle values floored (measured:
 *       -1, -4 give -3; the research's D-F17 under the IQ engine). Of a BIT, a date or a string, IQ's error -156;
 *       of a BIGINT (whose midpoint lite's databases compute in double precision), a DOUBLE (whose midpoint they
 *       compute by another formula) or a NUMERIC (an exact quotient), refused.</li>
 *   <li><b>LIST</b>{@code (x [, delimiter] ORDER BY x [ASC | DESC])} of an integer column: its values as text, in
 *       order, NULLs skipped, joined by the delimiter (',' by default; '' or NULL for none), '' over no values. Without
 *       ORDER BY, or ordered by anything but its value, the order of tied rows is the plan's: refused; strings (IQ
 *       island, 2026-10-14) only where each value is its own collation key ({@link IqKeys}), '' skipped as NULL is;
 *       dates, numerics and doubles (SQL Anywhere's own spelling) and DISTINCT refused. Two LISTs
 *       ordered differently are SQL Anywhere's error -904; the IQ engine orders both by the first (the research's
 *       D-A1): refused there.</li>
 *   <li><b>STDDEV and VARIANCE</b> are refused by the reader: SQL Anywhere's formula loses precision.</li>
 * </ul>
 */
final class IqAggregates {

    private IqAggregates() {
    }

    /** Whether {@code f} calls one of the aggregates the island reads. */
    static boolean named(SqlAst.FunctionCall f) {
        return f.name().size() == 1 && isAggregate(f.name().get(0).text());
    }

    /** Whether {@code name} is one of the aggregates the island reads (as IQ matches names). */
    static boolean isAggregate(String name) {
        return IqParser.READ_AGGREGATES.contains(IqLexer.lower(name));
    }

    /** The aggregates the island reads ({@link IqParser#READ_AGGREGATES}). */
    private enum Fn {
        COUNT, SUM, AVG, MIN, MAX, MEDIAN, LIST, PERCENTILE_CONT, PERCENTILE_DISC, ARRAY_AGG;

        static Fn of(SqlAst.FunctionCall f) {
            return valueOf(IqLexer.lower(f.name().get(0).text()).toUpperCase(java.util.Locale.ROOT));
        }
    }

    private static String code(int sqlcode, String sqlstate) {
        return " (IQ: SQLCODE " + sqlcode + ", SQLSTATE " + sqlstate + ")";
    }

    // ------------------------------------------------------------------ before translating

    /**
     * The errors IQ raises for a SELECT's aggregates and grouping before running it, and what is refused there by name,
     * whatever the engine: an aggregate's arguments counted (-154), an ORDER BY inside any aggregate but LIST (-949), a
     * position in LIST's ORDER BY (-152), an aggregate in WHERE, ON or GROUP BY (-150, measured); a LIST delimiter that
     * is no constant (IQ's -156 where a group holds two values), a NULL GROUP BY item, and a SELECT DISTINCT ordered by
     * an expression that is none of its items (IQ orders by an expression of its columns, which the translation does
     * not read).
     */
    static void parsed(SqlAst.Select s) {
        for (SqlAst.Expr e : IqDialect.expressions(s)) {
            calls(e);
        }
        // IQ island, 2026-10-17 (the differential fuzzer; measured on IQ 17.1): an aggregate inside a select item's
        // aggregate is IQ's -156 before an aggregate in WHERE (-150), and -150 itself where an IN reads the inner one
        Nested inner = nestedInItems(s);
        SqlTranslationException nested = inner == null ? null : inner.byIn()
                ? SqlTranslationException.invalid("an aggregate an IN reads inside an aggregate" + code(-150, "42W06"),
                        inner.call().pos())
                : SqlTranslationException.invalid("an aggregate's argument holds an aggregate (aggregate calls cannot"
                        + " be nested)" + code(-156, "42W08"), inner.call().pos());
        if (inner != null && inner.byIn()) {
            throw nested;
        }
        List<SqlAst.Expr> placed = new ArrayList<>(grouping(s).groupBy());
        if (s.where() != null) {
            placed.add(java.util.Objects.requireNonNull(s.where()));
        }
        for (SqlAst.FromItem f : s.from()) {
            onConditions(f, placed);
        }
        Set<String> own = new java.util.HashSet<>();
        for (SqlAst.FromItem f : s.from()) {
            for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                SqlAst.Ident name = IqDialect.correlation(leaf);
                if (name != null) {
                    own.add(IqLexer.lower(name.text()));
                }
            }
        }
        for (SqlAst.Expr e : placed) {
            List<SqlAst.FunctionCall> misplaced = new ArrayList<>(aggregateCalls(e));
            // IQ island, 2026-10-17 (the claim verification's corpus: (SELECT TOP 1 id FROM u WHERE v =
            // max(p.v)), p the outer query's): an aggregate of the outer query's columns alone, qualified by its
            // names, is the outer query's (IQ raised its subquery's -1001030 there, not -150)
            misplaced.removeIf(a -> {
                List<SqlAst.ColumnRef> refs = new ArrayList<>();
                columns(a, refs, true);
                return !refs.isEmpty() && refs.stream().allMatch(r -> r.parts().size() >= 2 && !own.contains(
                        IqLexer.lower(r.parts().get(r.parts().size() - 2).text())));
            });
            if (!misplaced.isEmpty()) {
                throw nested != null ? nested : SqlTranslationException.invalid("an aggregate is not allowed in WHERE,"
                        + " ON or GROUP BY" + code(-150, "42W06"), misplaced.get(0).pos());
            }
        }
        SqlAst.Select read = grouping(s);
        if (read.groupBy().isEmpty() && read.having() != null
                && aggregateCalls(java.util.Objects.requireNonNull(read.having())).isEmpty()) {
            // lite's planner tests a condition of no column before aggregating: no row would become one row
            throw SqlTranslationException.unsupported("HAVING without GROUP BY that reads no aggregate is not supported"
                    + " yet", java.util.Objects.requireNonNull(read.having()).pos());
        }
        if (s.distinct()) {
            for (SqlAst.SortItem o : s.orderBy()) {
                SqlAst.Expr e = o.expr();
                if (e instanceof SqlAst.IntegerLit || e instanceof SqlAst.ColumnRef) {
                    // a position, a name: the translation reads them (an output's name first)
                    continue;
                }
                boolean item = false;
                for (SqlAst.SelectItem i : s.items()) {
                    item |= i instanceof SqlAst.SelectExpr x && same(x.expr(), e);
                }
                if (!item) {
                    throw SqlTranslationException.unsupported("a SELECT DISTINCT ordered by an expression that is none"
                            + " of its items is not supported yet (IQ orders by an expression of its columns)", e.pos());
                }
            }
        }
    }

    private static void calls(SqlAst.Expr e) {
        if (e instanceof SqlAst.FunctionCall f && named(f)) {
            String fn = IqLexer.lower(f.name().get(0).text());
            int n = f.args().size();
            boolean arity = fn.equals("count") ? f.star() || n == 1 : fn.equals("list") ? n == 1 || n == 2 : n == 1;
            if (!arity || f.star() && !fn.equals("count")) {
                throw SqlTranslationException.invalid("wrong number of arguments to " + f.name().get(0).text()
                        .toUpperCase(java.util.Locale.ROOT) + code(-154, "37505"), f.pos());
            }
            if (!f.orderBy().isEmpty() && !fn.equals("list") && !fn.equals("array_agg")) {
                throw SqlTranslationException.invalid("ORDER BY is not allowed in " + f.name().get(0).text()
                        + "(...): only LIST orders its values" + code(-949, "42WA5"), f.orderBy().get(0).pos());
            }
            for (SqlAst.SortItem o : f.orderBy()) {
                if (o.expr() instanceof SqlAst.IntegerLit) {
                    throw SqlTranslationException.invalid("LIST's ORDER BY takes expressions, not positions"
                            + code(-152, "53005"), o.expr().pos());
                }
            }
            if (fn.equals("median") && n == 1 && f.args().get(0) instanceof SqlAst.Cast c
                    && NO_MEDIAN.contains(c.type().name().replace(IqParser.CONVERT, "").toUpperCase(java.util.Locale.ROOT))) {
                // IQ island, 2026-10-14 (the merge: CAST reads since M1d): MEDIAN of a BIT or a date is IQ's -156
                // (measured), raised before translating as for a column of those types
                throw SqlTranslationException.invalid("MEDIAN takes a number, not a " + c.type().name()
                        + code(-156, "42W08"), f.pos());
            }
            if (fn.startsWith("percentile_")) {
                percentileParsed(f);
            }
            if (fn.equals("list") && n == 2 && !(f.args().get(1) instanceof SqlAst.StringLit)
                    && !(f.args().get(1) instanceof SqlAst.NullLit)) {
                throw SqlTranslationException.unsupported("a LIST delimiter other than a string constant or NULL is"
                        + " not supported (IQ's error -156 where a group holds two values)", f.args().get(1).pos());
            }
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            calls(child);
        }
    }

    /** IQ island, 2026-10-15 (checklist 2.16, measured on IQ 17.1): the errors IQ raises for PERCENTILE_CONT and
     *  PERCENTILE_DISC before running: a fraction that is no numeric constant from 0 to 1 (-1005028), a WITHIN GROUP
     *  value that is a constant (-1005028), holds an aggregate (-156) or a subquery (-1001030). */
    private static void percentileParsed(SqlAst.FunctionCall f) {
        String name = f.name().get(0).text().toUpperCase(java.util.Locale.ROOT);
        SqlAst.Expr p = f.args().get(0);
        java.math.BigDecimal fraction = p instanceof SqlAst.NumericLit d ? d.value() : p instanceof SqlAst.IntegerLit i
                ? new java.math.BigDecimal(i.value()) : p instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.MINUS
                && (u.operand() instanceof SqlAst.NumericLit || u.operand() instanceof SqlAst.IntegerLit)
                ? java.math.BigDecimal.ONE.negate() : null;
        if (fraction == null || fraction.signum() < 0 || fraction.compareTo(java.math.BigDecimal.ONE) > 0) {
            throw SqlTranslationException.invalid("the argument of " + name + " must be a numeric constant from 0 to 1"
                    + code(-1005028, "QTA28"), p.pos());
        }
        SqlAst.Expr value = f.withinGroup().get(0).expr();
        if (hasAggregateCall(value)) {
            throw SqlTranslationException.invalid("an aggregate in " + name + "'s WITHIN GROUP" + code(-156, "42W08"),
                    value.pos());
        }
        if (hasSubqueryIn(value)) {
            throw SqlTranslationException.invalid("a subquery in " + name + "'s WITHIN GROUP is not supported by IQ's"
                    + " engine" + code(-1001030, "QFA2A"), value.pos());
        }
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        columns(value, refs, true);
        if (refs.isEmpty()) {
            throw SqlTranslationException.invalid("the ORDER BY item of " + name + "'s WITHIN GROUP cannot be a constant"
                    + " expression" + code(-1005028, "QTA28"), value.pos());
        }
    }

    private static boolean hasAggregateCall(SqlAst.Expr e) {
        return e instanceof SqlAst.FunctionCall f && named(f) || IqDialect.children(e).stream().anyMatch(
                IqAggregates::hasAggregateCall);
    }

    private static boolean hasSubqueryIn(SqlAst.Expr e) {
        return e instanceof SqlAst.ScalarSubquery || e instanceof SqlAst.Exists || e instanceof SqlAst.InSubquery
                || e instanceof SqlAst.Quantified || IqDialect.children(e).stream().anyMatch(IqAggregates::hasSubqueryIn);
    }

    /** The types MEDIAN takes none of (IQ's -156, measured): CAST's spellings. */
    private static final Set<String> NO_MEDIAN = Set.of("BIT", "DATE", "TIMESTAMP");

    /** The ON conditions of a FROM item's joins. */
    private static void onConditions(SqlAst.FromItem item, List<SqlAst.Expr> out) {
        if (item instanceof SqlAst.Join j) {
            onConditions(j.left(), out);
            onConditions(j.right(), out);
            if (j.on() != null) {
                out.add(java.util.Objects.requireNonNull(j.on()));
            }
        }
    }

    /**
     * The errors IQ raises before running {@code statement} that a SELECT's own reading cannot see (measured on SQL
     * Anywhere 17):
     * <ul>
     *   <li>an aggregate of a subquery over both an enclosing query's column and one of its own, the enclosing one
     *       written qualified: -861;</li>
     *   <li>in the statement's own queries and its derived tables (which read no enclosing query), where every FROM
     *       item's columns are known before translating (derived tables, DUMMY, {@code sa_rowgenerator}): a column of
     *       a FROM item read outside an aggregate in a grouped query's select list or HAVING (-149) or ORDER BY (-854)
     *       that no GROUP BY item reads.</li>
     * </ul>
     */
    static void statement(SqlAst.Statement statement) {
        outerReferences(statement, Set.of());
        unscoped(statement);
    }

    private static void outerReferences(SqlAst.Statement q, Set<String> enclosing) {
        switch (q) {
            case SqlAst.SetOperation o -> {
                outerReferences(o.left(), enclosing);
                outerReferences(o.right(), enclosing);
            }
            case SqlAst.Select s -> {
                Set<String> own = new java.util.HashSet<>();
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                        SqlAst.Ident name = IqDialect.correlationOf(leaf);
                        if (name != null) {
                            own.add(IqLexer.lower(name.text()));
                        }
                        if (leaf instanceof SqlAst.SubqueryFrom d) {
                            outerReferences(d.query(), Set.of());
                        }
                    }
                }
                List<SqlAst.Expr> all = new ArrayList<>(allExpressions(s));
                for (SqlAst.Expr e : all) {
                    for (SqlAst.FunctionCall f : aggregateCalls(e)) {
                        boolean mine = false;
                        boolean theirs = false;
                        for (SqlAst.Expr a : f.args()) {
                            List<SqlAst.ColumnRef> refs = new ArrayList<>();
                            columns(a, refs, true);
                            for (SqlAst.ColumnRef r : refs) {
                                if (r.parts().size() == 2) {
                                    String qualifier = IqLexer.lower(r.parts().get(0).text());
                                    mine |= own.contains(qualifier);
                                    theirs |= !own.contains(qualifier) && enclosing.contains(qualifier);
                                }
                            }
                        }
                        if (mine && theirs) {
                            throw SqlTranslationException.invalid(f.name().get(0).text() + "(...) reads both an"
                                    + " enclosing query's column and one of its own" + code(-861, "42W68"), f.pos());
                        }
                    }
                }
                Set<String> inner = new java.util.HashSet<>(enclosing);
                inner.addAll(own);
                for (SqlAst.Expr e : all) {
                    subqueries(e, inner);
                }
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
            case SqlAst.Write w -> throw new IllegalStateException("IQ's reader makes no write");
        }
    }

    private static void subqueries(SqlAst.Expr e, Set<String> enclosing) {
        SqlAst.Statement q = switch (e) {
            case SqlAst.ScalarSubquery x -> x.query();
            case SqlAst.Exists x -> x.query();
            case SqlAst.InSubquery x -> x.query();
            case SqlAst.Quantified x -> x.query();
            default -> null;
        };
        if (q != null) {
            outerReferences(q, enclosing);
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            subqueries(child, enclosing);
        }
    }

    /** The statement's own SELECTs and its derived tables' (none reads an enclosing query), each checked for columns
     *  no GROUP BY item reads. */
    private static void unscoped(SqlAst.Statement q) {
        switch (q) {
            case SqlAst.SetOperation o -> {
                unscoped(o.left());
                unscoped(o.right());
            }
            case SqlAst.Select s -> {
                for (SqlAst.FromItem f : s.from()) {
                    for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                        if (leaf instanceof SqlAst.SubqueryFrom d) {
                            unscoped(d.query());
                        }
                    }
                }
                if (grouped(s)) {
                    notGrouped(grouping(s));
                }
            }
            case SqlAst.With w -> throw new IllegalStateException("IQ's reader makes no WITH");
            case SqlAst.Write w -> throw new IllegalStateException("IQ's reader makes no write");
        }
    }

    /** A grouped SELECT's column read outside an aggregate that no GROUP BY item reads: IQ's -149 in the select list
     *  and HAVING, -854 in ORDER BY (measured), where the FROM items' columns are all known and include it. */
    private static void notGrouped(SqlAst.Select s) {
        if (s.from().isEmpty()) {
            return;
        }
        Set<String> known = new java.util.HashSet<>();
        Set<String> own = new java.util.HashSet<>();
        for (SqlAst.FromItem f : s.from()) {
            for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                Set<String> cols = IqDialect.knownColumns(leaf);
                SqlAst.Ident name = IqDialect.correlationOf(leaf);
                if (cols == null || name == null) {
                    return;
                }
                known.addAll(cols);
                own.add(IqLexer.lower(name.text()));
            }
        }
        Set<String> grouped = new java.util.HashSet<>();
        s.groupBy().forEach(k -> columns(k).forEach(r -> grouped.add(last(r))));
        Set<String> outputs = new java.util.HashSet<>();
        Set<String> columnOutputs = columnOutputs(s);
        List<SqlAst.Expr> listed = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                listed.add(x.expr());
                if (x.alias() != null) {
                    outputs.add(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()));
                } else if (x.expr() instanceof SqlAst.ColumnRef r) {
                    outputs.add(last(r));
                }
            }
        }
        if (s.having() != null) {
            listed.add(java.util.Objects.requireNonNull(s.having()));
        }
        // IQ island, 2026-10-14 (the merge: CAST reads since M1d, so a derived table's columns are known in more
        // statements): GROUP BY 1 groups by a constant in IQ, a position elsewhere; the syntax cases call it accepted
        // (z2_redteam-333, -334) where SQL Anywhere raises -149 -- refused naming IQ's error
        boolean positional = s.groupBy().stream().anyMatch(k -> k instanceof SqlAst.IntegerLit);
        // IQ island, 2026-10-14 (review 7's m2, measured): the ORDER BY's first (-854), then the select list's and
        // HAVING's (-149)
        for (SqlAst.SortItem o : s.orderBy()) {
            if (o.expr() instanceof SqlAst.IntegerLit || namesOutput(o.expr(), outputs, columnOutputs)) {
                continue;
            }
            notGrouped(o.expr(), known, own, grouped, -854, "42W65", false);
        }
        for (SqlAst.Expr e : listed) {
            notGrouped(e, known, own, grouped, -149, "53003", positional);
        }
    }

    /**
     * IQ island, 2026-10-14 (review 7's m2, measured on SQL Anywhere 17): whether a grouped SELECT's ORDER BY reads,
     * outside an aggregate, a column no GROUP BY item reads -- an ORDER BY item that is neither a position nor an
     * output's name (a select item's alias, or a column written bare). SQL Anywhere reports that reference first, as
     * -854, also where the select list or HAVING reads another such column ({@code SELECT k, v + 1 ... GROUP BY k
     * ORDER BY v}; {@code SELECT k, v ... GROUP BY k ORDER BY v} names the output and is -149).
     */
    static boolean ordersUngrouped(SqlAst.Select s) {
        if (s.orderBy().isEmpty() || s.groupBy().isEmpty() && s.items().stream().noneMatch(i ->
                i instanceof SqlAst.SelectExpr x && !aggregateCalls(x.expr()).isEmpty())) {
            return false;
        }
        Set<String> grouped = new java.util.HashSet<>();
        s.groupBy().forEach(k -> columns(k).forEach(r -> grouped.add(last(r))));
        Set<String> outputs = new java.util.HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                if (x.alias() != null) {
                    outputs.add(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()));
                } else if (x.expr() instanceof SqlAst.ColumnRef r) {
                    outputs.add(last(r));
                }
            }
        }
        Set<String> columnOutputs = columnOutputs(s);
        for (SqlAst.SortItem o : s.orderBy()) {
            if (o.expr() instanceof SqlAst.IntegerLit || namesOutput(o.expr(), outputs, columnOutputs)) {
                continue;
            }
            for (SqlAst.ColumnRef r : columns(o.expr())) {
                if (!grouped.contains(last(r))) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The names of a select list's items that are a column alone (unaliased, qualified or not). */
    private static Set<String> columnOutputs(SqlAst.Select s) {
        Set<String> out = new java.util.HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && x.alias() == null && x.expr() instanceof SqlAst.ColumnRef r) {
                out.add(last(r));
            }
        }
        return out;
    }

    /** Whether an ORDER BY key names an output, as IQ reads it for its grouping errors: a bare name an output's (an
     *  alias or a column item's), or a qualified column a column item's -- IQ island, 2026-10-17 (BIRD's dev set,
     *  measured on IQ 17.1: SELECT t.id, sm ... GROUP BY t.id ORDER BY t.sm is -149, the select list's, not -854). */
    private static boolean namesOutput(SqlAst.Expr e, Set<String> outputs, Set<String> columnOutputs) {
        return e instanceof SqlAst.ColumnRef r && (r.parts().size() == 1 ? outputs.contains(last(r))
                : columnOutputs.contains(last(r)));
    }

    private static void notGrouped(SqlAst.Expr e, Set<String> known, Set<String> own, Set<String> grouped, int sqlcode,
            String sqlstate, boolean split) {
        for (SqlAst.ColumnRef r : columns(e)) {
            boolean local = r.parts().size() == 1 || r.parts().size() == 2
                    && own.contains(IqLexer.lower(r.parts().get(0).text()));
            if (local && known.contains(last(r)) && !grouped.contains(last(r))) {
                String message = "'" + String.join(".", r.parts().stream().map(SqlAst.Ident::text).toList())
                        + "' is neither grouped nor aggregated" + code(sqlcode, sqlstate);
                throw split ? IqDialect.splitCases(message, r.pos())
                        : SqlTranslationException.invalid(message, r.pos());
            }
        }
    }

    // ------------------------------------------------------------------ GROUP BY and HAVING as IQ reads them

    /**
     * {@code s} with a select-list alias read where IQ reads it in GROUP BY and HAVING: before a column of its name,
     * anywhere in them (measured: {@code SELECT j AS k ... GROUP BY k} groups by j; {@code HAVING SUM(x)} sums x's
     * expression). The alias's expression takes the reference's place. A constant GROUP BY item beside one that is not
     * is dropped: it divides no group (and lite's databases would read an integer there as a position).
     */
    static SqlAst.Select grouping(SqlAst.Select s) {
        if (s.groupBy().isEmpty() && s.having() == null) {
            return s;
        }
        Map<String, SqlAst.Expr> aliases = new HashMap<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && x.alias() != null) {
                aliases.putIfAbsent(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()), x.expr());
            }
        }
        List<SqlAst.Expr> keys = new ArrayList<>();
        boolean changed = false;
        for (SqlAst.Expr k : s.groupBy()) {
            SqlAst.Expr inlined = aliases.isEmpty() ? k : inline(k, aliases);
            changed |= inlined != k;
            keys.add(inlined);
        }
        if (keys.stream().anyMatch(k -> !constant(k))) {
            changed |= keys.removeIf(IqAggregates::constant);
        }
        if (aliases.isEmpty() && !changed) {
            return s;
        }
        SqlAst.Expr having = s.having() == null || aliases.isEmpty() ? s.having()
                : inline(java.util.Objects.requireNonNull(s.having()), aliases);
        changed |= having != s.having();
        return !changed ? s : new SqlAst.Select(s.distinct(), s.items(), s.from(), s.where(), keys, having, s.windows(),
                s.orderBy(), s.limit(), s.offset(), s.pos());
    }

    /** {@code e} with each unqualified name of {@code aliases} replaced by its expression ({@code e} itself when there
     *  is none); a subquery is its own scope. */
    private static SqlAst.Expr inline(SqlAst.Expr e, Map<String, SqlAst.Expr> aliases) {
        return switch (e) {
            case SqlAst.ColumnRef r when r.parts().size() == 1
                    && aliases.containsKey(IqLexer.lower(r.parts().get(0).text())) ->
                    java.util.Objects.requireNonNull(aliases.get(IqLexer.lower(r.parts().get(0).text())));
            case SqlAst.Binary b -> {
                SqlAst.Expr l = inline(b.left(), aliases);
                SqlAst.Expr r = inline(b.right(), aliases);
                yield l == b.left() && r == b.right() ? b : new SqlAst.Binary(b.op(), l, r, b.pos());
            }
            case SqlAst.Unary u -> {
                SqlAst.Expr o = inline(u.operand(), aliases);
                yield o == u.operand() ? u : new SqlAst.Unary(u.op(), o, u.pos());
            }
            case SqlAst.IsNull n -> {
                SqlAst.Expr o = inline(n.operand(), aliases);
                yield o == n.operand() ? n : new SqlAst.IsNull(o, n.negated(), n.pos());
            }
            case SqlAst.IsTruth t -> {
                SqlAst.Expr o = inline(t.operand(), aliases);
                yield o == t.operand() ? t : new SqlAst.IsTruth(o, t.truth(), t.negated(), t.pos());
            }
            case SqlAst.IsDistinct d -> {
                SqlAst.Expr l = inline(d.left(), aliases);
                SqlAst.Expr r = inline(d.right(), aliases);
                yield l == d.left() && r == d.right() ? d : new SqlAst.IsDistinct(l, r, d.negated(), d.pos());
            }
            case SqlAst.Between b -> {
                SqlAst.Expr o = inline(b.operand(), aliases);
                SqlAst.Expr lo = inline(b.low(), aliases);
                SqlAst.Expr hi = inline(b.high(), aliases);
                yield o == b.operand() && lo == b.low() && hi == b.high() ? b
                        : new SqlAst.Between(o, lo, hi, b.negated(), b.pos());
            }
            case SqlAst.InList in -> {
                SqlAst.Expr o = inline(in.operand(), aliases);
                List<SqlAst.Expr> values = inlineAll(in.values(), aliases);
                yield o == in.operand() && values == in.values() ? in
                        : new SqlAst.InList(o, values, in.negated(), in.pos());
            }
            case SqlAst.Like l -> {
                SqlAst.Expr o = inline(l.operand(), aliases);
                SqlAst.Expr p = inline(l.pattern(), aliases);
                SqlAst.Expr esc = l.escape() == null ? null : inline(java.util.Objects.requireNonNull(l.escape()),
                        aliases);
                yield o == l.operand() && p == l.pattern() && esc == l.escape() ? l
                        : new SqlAst.Like(o, p, esc, l.negated(), l.caseInsensitive(), l.pos());
            }
            case SqlAst.FunctionCall f -> {
                List<SqlAst.Expr> args = inlineAll(f.args(), aliases);
                List<SqlAst.SortItem> order = new ArrayList<>();
                boolean same = args == f.args();
                for (SqlAst.SortItem o : f.orderBy()) {
                    SqlAst.Expr k = inline(o.expr(), aliases);
                    same &= k == o.expr();
                    order.add(k == o.expr() ? o : new SqlAst.SortItem(k, o.descending(), o.nullsFirst(), o.pos()));
                }
                yield same ? f : new SqlAst.FunctionCall(f.name(), args, f.distinct(), f.star(), order,
                        f.withinGroup(), f.filter(), f.pos());
            }
            case SqlAst.Case c -> {
                SqlAst.Expr operand = c.operand() == null ? null : inline(java.util.Objects.requireNonNull(c.operand()),
                        aliases);
                boolean same = operand == c.operand();
                List<SqlAst.When> whens = new ArrayList<>();
                for (SqlAst.When w : c.whens()) {
                    SqlAst.Expr cond = inline(w.condition(), aliases);
                    SqlAst.Expr result = inline(w.result(), aliases);
                    same &= cond == w.condition() && result == w.result();
                    whens.add(new SqlAst.When(cond, result));
                }
                SqlAst.Expr otherwise = c.otherwise() == null ? null
                        : inline(java.util.Objects.requireNonNull(c.otherwise()), aliases);
                same &= otherwise == c.otherwise();
                yield same ? c : new SqlAst.Case(operand, whens, otherwise, c.pos());
            }
            case SqlAst.InSubquery q -> {
                SqlAst.Expr o = inline(q.operand(), aliases);
                yield o == q.operand() ? q : new SqlAst.InSubquery(o, q.query(), q.negated(), q.pos());
            }
            case SqlAst.Quantified q -> {
                SqlAst.Expr l = inline(q.left(), aliases);
                yield l == q.left() ? q : new SqlAst.Quantified(q.op(), q.all(), l, q.query(), q.pos());
            }
            case SqlAst.Cast c -> {
                SqlAst.Expr o = inline(c.operand(), aliases);
                yield o == c.operand() ? c : new SqlAst.Cast(o, c.type(), c.pos());
            }
            default -> {
                // IQ island, 2026-10-15 (the corpus lane: GROUP BY CAST(creationdate AS DATE) in 95 of SEDE's and
                // SQLStorm's queries): an expression that reads no alias is itself
                if (readsAlias(e, aliases)) {
                    throw SqlTranslationException.unsupported("a select-list alias read inside this expression is not"
                            + " supported yet", e.pos());
                }
                yield e;
            }
        };
    }

    /** Whether {@code e} reads one of {@code aliases} by a bare name (not inside a subquery). */
    private static boolean readsAlias(SqlAst.Expr e, Map<String, SqlAst.Expr> aliases) {
        if (e instanceof SqlAst.ColumnRef r && r.parts().size() == 1 && aliases.containsKey(IqLexer.lower(
                r.parts().get(0).text()))) {
            return true;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            if (readsAlias(child, aliases)) {
                return true;
            }
        }
        return false;
    }

    /** Whether a GROUP BY item is a constant: no column, no aggregate (no subquery: refused there). */
    private static boolean constant(SqlAst.Expr key) {
        return columns(key).isEmpty() && aggregateCalls(key).isEmpty();
    }

    private static List<SqlAst.Expr> inlineAll(List<SqlAst.Expr> values, Map<String, SqlAst.Expr> aliases) {
        List<SqlAst.Expr> out = new ArrayList<>();
        boolean same = true;
        for (SqlAst.Expr v : values) {
            SqlAst.Expr i = inline(v, aliases);
            same &= i == v;
            out.add(i);
        }
        return same ? values : out;
    }

    // ------------------------------------------------------------------ the statement's grouped queries

    /** Whether {@code s} groups: a GROUP BY, or an aggregate in its select list, HAVING or ORDER BY (as the translator
     *  decides). */
    static boolean grouped(SqlAst.Select s) {
        if (!s.groupBy().isEmpty() || s.having() != null) {
            return true;
        }
        List<SqlAst.Expr> scanned = new ArrayList<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                scanned.add(x.expr());
            }
        }
        s.orderBy().forEach(o -> scanned.add(o.expr()));
        for (SqlAst.Expr e : scanned) {
            if (!aggregateCalls(e).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /** The calls of the aggregates read under {@code e}, in the order they are written (not into subqueries). */
    /** An aggregate inside a select item's aggregate ({@link #nestedInItems}), and whether an IN reads it. */
    private record Nested(SqlAst.FunctionCall call, boolean byIn) {
    }

    /** A select item's aggregate holding another in its arguments, as IQ raises it ({@link #parsed}): -156, or -150
     *  where an IN reads the inner aggregate (measured on IQ 17.1: SUM(CASE WHEN COUNT(*) IN (1, 2) THEN 1 END)); null
     *  for none. */
    private static @com.legend.base.Nullable Nested nestedInItems(SqlAst.Select s) {
        for (SqlAst.SelectItem i : s.items()) {
            if (!(i instanceof SqlAst.SelectExpr x)) {
                continue;
            }
            for (SqlAst.FunctionCall outer : aggregateCalls(x.expr())) {
                for (SqlAst.Expr arg : outer.args()) {
                    List<SqlAst.FunctionCall> inner = aggregateCalls(arg);
                    if (!inner.isEmpty()) {
                        return new Nested(inner.get(0), readByIn(arg, false));
                    }
                }
            }
        }
        return null;
    }

    /** Whether an aggregate under {@code e} is read by an IN ({@code in}: below one already). */
    private static boolean readByIn(SqlAst.Expr e, boolean in) {
        if (e instanceof SqlAst.FunctionCall f && named(f)) {
            return in;
        }
        boolean below = in || e instanceof SqlAst.InList;
        for (SqlAst.Expr child : IqDialect.children(e)) {
            if (readByIn(child, below)) {
                return true;
            }
        }
        return false;
    }

    static List<SqlAst.FunctionCall> aggregateCalls(SqlAst.Expr e) {
        List<SqlAst.FunctionCall> out = new ArrayList<>();
        aggregateCalls(e, out);
        return out;
    }

    private static void aggregateCalls(SqlAst.Expr e, List<SqlAst.FunctionCall> out) {
        if (e instanceof SqlAst.FunctionCall f && named(f)) {
            out.add(f);
            return;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            aggregateCalls(child, out);
        }
    }

    /**
     * What IQ raises, and what is refused, for each grouped query of {@code statement} under {@code engine}, before
     * translating; and the names its grouped queries' unaliased items take ({@link Naming}). {@code spans} holds the
     * text of each GROUP BY item and aggregate call as written ({@link IqParser.Parsed}). Refused by name:
     * <ul>
     *   <li>an expression of a column that is part of a GROUP BY expression, outside that expression and not grouped by
     *       itself ({@code SELECT 1 + k ... GROUP BY k + 1}): IQ may read it as the GROUP BY expression (measured: it
     *       does for {@code 1 + k}, it raises -149 for {@code k * 3});</li>
     *   <li>two LISTs ordered differently (the research's D-A1),
     *       which SQL Anywhere's front end rejects (-904). IQ island, 2026-10-15 (the corpus lane; measured on IQ
     *       17.1): an aggregate's DISTINCT over an expression, and one in a SELECT DISTINCT, are read under IQ's engine
     *       too -- the research's D-R1 held that it restricts them; IQ 17.1 answers COUNT(DISTINCT CASE ... END),
     *       COUNT(DISTINCT id + 1), SUM(DISTINCT sm * 2) and SELECT DISTINCT COUNT(DISTINCT sm + 1).</li>
     * </ul>
     */
    static Naming read(SqlAst.Statement statement, IqDialect.Engine engine, Map<SqlAst.Expr, String> spans) {
        Naming naming = new Naming(spans);
        for (SqlAst.Statement q : IqDialect.queries(statement)) {
            if (q instanceof SqlAst.Select raw && grouped(raw)) {
                SqlAst.Select s = grouping(raw);
                constantKeys(s, engine);
                keyMatches(s);
                lists(s, engine);
                if (engine != IqDialect.Engine.IQ) {
                    // IQ island, 2026-10-16 (the corpus lane: 56 queries; measured on IQ 17.1): IQ's engine names a
                    // grouped query's items as an ungrouped query's, each by its own spelling (SELECT SM + 1 ... GROUP
                    // BY sm is ll_t.SM+1, SELECT SUM(IT) ... HAVING sum(it) > 0 is SUM(ll_t.IT)); its front end reads
                    // the GROUP BY and HAVING first, as SQL Anywhere 17 does (SELECT X + 1 ... GROUP BY x is d.x+1)
                    naming.add(s);
                }
            }
        }
        return naming;
    }

    /** The GROUP BY items that are expressions over columns (not a column, not a constant). */
    private static List<SqlAst.Expr> expressionKeys(SqlAst.Select s) {
        List<SqlAst.Expr> out = new ArrayList<>();
        for (SqlAst.Expr k : s.groupBy()) {
            if (!(k instanceof SqlAst.ColumnRef) && !columns(k).isEmpty()) {
                out.add(k);
            }
        }
        return out;
    }

    /** The column references under {@code e}, outside aggregates and subqueries. */
    private static List<SqlAst.ColumnRef> columns(SqlAst.Expr e) {
        List<SqlAst.ColumnRef> out = new ArrayList<>();
        columns(e, out, false);
        return out;
    }

    private static void columns(SqlAst.Expr e, List<SqlAst.ColumnRef> out, boolean intoAggregates) {
        if (e instanceof SqlAst.ColumnRef r) {
            out.add(r);
        }
        if (!intoAggregates && e instanceof SqlAst.FunctionCall f && named(f)) {
            return;
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            columns(child, out, intoAggregates);
        }
    }

    private static String last(SqlAst.ColumnRef r) {
        return IqLexer.lower(r.parts().get(r.parts().size() - 1).text());
    }

    /** The expressions a grouped query reads in its groups: its items, its HAVING, its ORDER BY but positions and
     *  bare names (an output's first). */
    private static List<SqlAst.Expr> groupedExpressions(SqlAst.Select s) {
        List<SqlAst.Expr> out = new ArrayList<>();
        Set<String> outputs = new java.util.HashSet<>();
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x) {
                out.add(x.expr());
                if (x.alias() != null) {
                    outputs.add(IqLexer.lower(java.util.Objects.requireNonNull(x.alias()).text()));
                }
            }
        }
        if (s.having() != null) {
            out.add(java.util.Objects.requireNonNull(s.having()));
        }
        for (SqlAst.SortItem o : s.orderBy()) {
            if (o.expr() instanceof SqlAst.IntegerLit || o.expr() instanceof SqlAst.ColumnRef r
                    && r.parts().size() == 1 && outputs.contains(last(r))) {
                continue;
            }
            out.add(o.expr());
        }
        return out;
    }

    /** A column of a GROUP BY expression read outside it and not grouped by itself: IQ's -149. */
    private static void keyMatches(SqlAst.Select s) {
        List<SqlAst.Expr> keys = expressionKeys(s);
        if (keys.isEmpty()) {
            return;
        }
        Set<String> inKeys = new java.util.HashSet<>();
        keys.forEach(k -> columns(k).forEach(r -> inKeys.add(last(r))));
        Set<String> bare = new java.util.HashSet<>();
        for (SqlAst.Expr k : s.groupBy()) {
            if (k instanceof SqlAst.ColumnRef r) {
                bare.add(last(r));
            }
        }
        Set<String> own = new java.util.HashSet<>();
        for (SqlAst.FromItem f : s.from()) {
            for (SqlAst.FromItem leaf : IqDialect.leaves(f)) {
                SqlAst.Ident name = IqDialect.correlation(leaf);
                if (name != null) {
                    own.add(IqLexer.lower(name.text()));
                }
            }
        }
        for (SqlAst.Expr e : groupedExpressions(s)) {
            keyMatch(e, keys, inKeys, bare, own);
        }
    }

    private static void keyMatch(SqlAst.Expr e, List<SqlAst.Expr> keys, Set<String> inKeys, Set<String> bare,
            Set<String> own) {
        if (e instanceof SqlAst.FunctionCall f && named(f)) {
            return;
        }
        for (SqlAst.Expr k : keys) {
            // IQ island, 2026-10-17 (the corpus lane's CAST(d.ts AS DATE) under GROUP BY CAST(ts AS DATE); measured on
            // IQ 17.1): a column qualified or not is the same column there; the translator, which resolves each,
            // decides whether two are one
            if (unqualifiedShape(k).equals(unqualifiedShape(e))) {
                return;
            }
        }
        for (SqlAst.Expr k : keys) {
            if (commutedShape(k).equals(commutedShape(e))) {
                // measured on IQ 17.1: SELECT 1 + k ... GROUP BY k + 1 is answered -- IQ reads an expression written
                // otherwise as the key where it may; which forms it reads so is not known beyond this one
                throw SqlTranslationException.unsupported("a GROUP BY expression written otherwise here is not"
                        + " supported: IQ may read it as that GROUP BY expression (it reads 1 + k as k + 1); write it"
                        + " as GROUP BY does", e.pos());
            }
        }
        // the landing's review (2026-10-17): an enclosing query's column, qualified by none of this query's names, is
        // no column of its groups
        if (e instanceof SqlAst.ColumnRef r && inKeys.contains(last(r)) && !bare.contains(last(r))
                && (r.parts().size() < 2 || own.contains(IqLexer.lower(r.parts().get(r.parts().size() - 2).text())))) {
            // IQ island, 2026-10-17 (the corpus lane's 20 such queries sent to IQ 17.1: 4 answered once a column
            // qualified or not was read as one, and the other 15 raised -149): IQ's -149 where no expression around
            // the column is a key written otherwise
            throw SqlTranslationException.invalid("'" + String.join(".", r.parts().stream().map(SqlAst.Ident::text)
                    .toList()) + "' is read here outside the GROUP BY expression it is part of, and is no GROUP BY item"
                    + " itself" + IqDialect.code(-149, "53003"), r.pos());
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            keyMatch(child, keys, inKeys, bare, own);
        }
    }

    /** The LISTs of one query ordered alike: else SQL Anywhere's error -904, and the IQ engine's D-A1 (every LIST
     *  ordered by the first's ORDER BY), refused. */
    private static void lists(SqlAst.Select s, IqDialect.Engine engine) {
        String order = null;
        for (SqlAst.Expr e : allExpressions(s)) {
            for (SqlAst.FunctionCall f : aggregateCalls(e)) {
                if (!IqLexer.lower(f.name().get(0).text()).equals("list") || f.orderBy().isEmpty()) {
                    continue;
                }
                // a constant key orders nothing, a repeated one nothing more (measured: (x, 'a') and (x, 'b', x) are
                // one order)
                StringBuilder b = new StringBuilder();
                Set<String> seen = new java.util.HashSet<>();
                for (SqlAst.SortItem o : f.orderBy()) {
                    String key = shape(o.expr());
                    if (!columns(o.expr()).isEmpty() && seen.add(key)) {
                        b.append(key).append(o.descending() ? " desc;" : " asc;");
                    }
                }
                if (order == null) {
                    order = b.toString();
                } else if (!order.equals(b.toString())) {
                    if (engine == IqDialect.Engine.IQ) {
                        throw SqlTranslationException.unsupported("two LISTs ordered differently in one query are not"
                                + " supported under IQ's engine, which orders both by the first's ORDER BY (the"
                                + " research's D-A1, not yet verified)", f.pos());
                    }
                    throw SqlTranslationException.invalid("two LISTs of one query are ordered differently"
                            + code(-904, "42W90"), f.pos());
                }
            }
        }
    }

    private static List<SqlAst.Expr> allExpressions(SqlAst.Select s) {
        List<SqlAst.Expr> out = new ArrayList<>(IqDialect.expressions(s));
        out.addAll(s.groupBy());
        if (s.having() != null) {
            out.add(java.util.Objects.requireNonNull(s.having()));
        }
        return out;
    }

    /** IQ island, 2026-10-17 (the differential fuzzer's GROUP BY (0 + it); measured on IQ 17.1): IQ's engine reads
     *  {@code 0 + x} as {@code x} in the select list and HAVING, not in GROUP BY ({@link IqDialect}'s identity), so a
     *  key holding {@code identity}, written again there outside an aggregate, is no key of the item, which reads
     *  {@code x} ungrouped: -149 ({@code SELECT 0 + it ... GROUP BY 0 + it}), unless {@code x} is a key itself.
     *  ORDER BY reads the key (measured), and so does an item whose alias the GROUP BY names (its key is the item). */
    static void foldedOutOfGroup(SqlAst.Select s, SqlAst.Expr identity, SqlAst.Expr x) {
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr e && holds(e.expr(), identity)) {
                return;
            }
        }
        for (SqlAst.Expr k : s.groupBy()) {
            if (!holds(k, identity)) {
                continue;
            }
            if (s.groupBy().stream().anyMatch(g -> unqualifiedShape(g).equals(unqualifiedShape(x)))) {
                return;
            }
            List<SqlAst.Expr> read = new ArrayList<>();
            for (SqlAst.SelectItem item : s.items()) {
                if (item instanceof SqlAst.SelectExpr e) {
                    read.add(e.expr());
                }
            }
            if (s.having() != null) {
                read.add(java.util.Objects.requireNonNull(s.having()));
            }
            String key = unqualifiedShape(k);
            for (SqlAst.Expr e : read) {
                if (readsOutsideAggregates(e, key) && !columns(x).isEmpty()) {
                    SqlAst.ColumnRef r = columns(x).get(0);
                    throw SqlTranslationException.invalid("'" + String.join(".", r.parts().stream()
                            .map(SqlAst.Ident::text).toList()) + "' is not grouped: IQ reads " + identityText(identity)
                            + " outside GROUP BY as its column, which this GROUP BY does not hold" + code(-149,
                            "53003"), r.pos());
                }
            }
        }
    }

    /** Whether {@code e} is {@code part}, or holds it outside subqueries (by identity). */
    private static boolean holds(SqlAst.Expr e, SqlAst.Expr part) {
        return e == part || IqDialect.children(e).stream().anyMatch(c -> holds(c, part));
    }

    /** Whether {@code e} holds an expression of {@code shape} outside aggregates and subqueries. */
    private static boolean readsOutsideAggregates(SqlAst.Expr e, String shape) {
        if (unqualifiedShape(e).equals(shape)) {
            return true;
        }
        if (e instanceof SqlAst.FunctionCall f && named(f)) {
            return false;
        }
        return IqDialect.children(e).stream().anyMatch(c -> readsOutsideAggregates(c, shape));
    }

    private static String identityText(SqlAst.Expr identity) {
        return identity instanceof SqlAst.Binary b && b.op() == SqlAst.BinaryOp.MINUS ? "x - 0" : "0 + x";
    }

    /** A GROUP BY of constants only: IQ's error -149 when the select list reads a column outside an aggregate (it
     *  groups by the constant, not by a select-list position: measured); else every row is one group. IQ island,
     *  2026-10-17 (the corpus lane's SQLStorm, measured on IQ 17.1): IQ's engine reads it too ({@code GROUP BY 'a
     *  b'}, {@code GROUP BY 1}, {@code GROUP BY NULL}), where the research's D-R1 held that it restricts it; the
     *  key is computed as a column, which lite's databases never read as a position. */
    private static void constantKeys(SqlAst.Select s, IqDialect.Engine engine) {
        if (s.groupBy().isEmpty() || !s.groupBy().stream().allMatch(IqAggregates::constant)) {
            return;
        }
        for (SqlAst.SelectItem item : s.items()) {
            if (item instanceof SqlAst.SelectExpr x && !columns(x.expr()).isEmpty()) {
                SqlAst.ColumnRef r = columns(x.expr()).get(0);
                throw SqlTranslationException.invalid("'" + String.join(".", r.parts().stream()
                        .map(SqlAst.Ident::text).toList()) + "' is not grouped: IQ groups by a constant here, not by a"
                        + " select-list position; name the columns" + code(-149, "53003"), r.pos());
            }
        }
    }

    // ------------------------------------------------------------------ one expression as another

    /** Whether {@code a} and {@code b} are written as one expression: the same forms, operators and constants, names
     *  equal without regard to ASCII case, parentheses and places aside. */
    static boolean same(SqlAst.Expr a, SqlAst.Expr b) {
        return shape(a).equals(shape(b));
    }

    /** Whether {@code a} and {@code b} are written as one expression but for their columns' qualifiers. */
    static boolean sameUnqualified(SqlAst.Expr a, SqlAst.Expr b) {
        return unqualifiedShape(a).equals(unqualifiedShape(b));
    }

    /** {@code e}'s form as {@link #unqualifiedShape} writes it, but a chain of + or of * (and AND, OR) by its operands'
     *  forms in order, so that {@code 1 + k} and {@code k + 1} are one. */
    private static String commutedShape(SqlAst.Expr e) {
        if (e instanceof SqlAst.Binary b && COMMUTED.contains(b.op())) {
            List<SqlAst.Expr> operands = new ArrayList<>();
            chain(b, b.op(), operands);
            List<String> forms = new ArrayList<>(operands.stream().map(IqAggregates::commutedShape).toList());
            java.util.Collections.sort(forms);
            return b.op() + forms.toString();
        }
        return e instanceof SqlAst.ColumnRef r ? "col(" + last(r) + ")" : shapeOf(e, IqAggregates::commutedShape);
    }

    /** The operators {@link #commutedShape} reads in any order. */
    private static final Set<SqlAst.BinaryOp> COMMUTED = Set.of(SqlAst.BinaryOp.PLUS, SqlAst.BinaryOp.TIMES,
            SqlAst.BinaryOp.AND, SqlAst.BinaryOp.OR);

    /** Into {@code out}, the operands of the chain of {@code op} {@code e} is. */
    private static void chain(SqlAst.Expr e, SqlAst.BinaryOp op, List<SqlAst.Expr> out) {
        if (e instanceof SqlAst.Binary b && b.op() == op) {
            chain(b.left(), op, out);
            chain(b.right(), op, out);
        } else {
            out.add(e);
        }
    }

    /** {@code e}'s form as {@link #shape} writes it, but each column by its name alone. */
    private static String unqualifiedShape(SqlAst.Expr e) {
        return e instanceof SqlAst.ColumnRef r ? "col(" + last(r) + ")"
                : shapeOf(e, IqAggregates::unqualifiedShape);
    }

    /** {@code e}'s form, for {@link #same}. */
    static String shape(SqlAst.Expr e) {
        StringBuilder b = new StringBuilder();
        shape(e, b, null);
        return b.toString();
    }

    /** {@code e}'s form as {@link #shape} writes it, but each operand written as {@code child} names it: hash-consing
     *  bottom-up (IQ island, 2026-10-14, the scale suite's cliff 5), so the forms of n nodes cost O(n), where whole
     *  forms cost O(n * depth); equal names for equal operands give equal forms exactly when {@link #shape}'s are. */
    static String shapeOf(SqlAst.Expr e, java.util.function.Function<SqlAst.Expr, String> child) {
        StringBuilder b = new StringBuilder();
        shape(e, b, child);
        return b.toString();
    }

    private static void part(SqlAst.Expr e, StringBuilder b,
            java.util.function.@com.legend.base.Nullable Function<SqlAst.Expr, String> child) {
        if (child == null) {
            shape(e, b, null);
        } else {
            b.append(child.apply(e));
        }
    }

    private static void shape(SqlAst.Expr e, StringBuilder b,
            java.util.function.@com.legend.base.Nullable Function<SqlAst.Expr, String> child) {
        switch (e) {
            case SqlAst.ColumnRef r -> b.append("col(").append(String.join(".", r.parts().stream()
                    .map(p -> IqLexer.lower(p.text())).toList())).append(')');
            case SqlAst.IntegerLit n -> b.append("int(").append(n.value()).append(')');
            case SqlAst.NumericLit n -> b.append("num(").append(n.value().toPlainString()).append(')');
            case SqlAst.StringLit s -> b.append("str(").append(s.value().length()).append(':').append(s.value())
                    .append(')');
            case SqlAst.NullLit n -> b.append("null");
            // IQ island, 2026-10-14 (review 5's m4): a host variable by its prefix and its name as IQ matches it
            case SqlAst.Param p -> b.append("param(").append(p.name() == null ? "?" + p.index()
                    : p.name().charAt(0) + IqLexer.lower(p.name().substring(1))).append(')');
            case SqlAst.FunctionCall f -> {
                b.append("fn(").append(IqLexer.lower(f.name().get(f.name().size() - 1).text())).append(',')
                        .append(f.distinct()).append(',').append(f.star());
                for (SqlAst.Expr a : f.args()) {
                    b.append(',');
                    part(a, b, child);
                }
                for (SqlAst.SortItem o : f.orderBy()) {
                    b.append(",order(");
                    part(o.expr(), b, child);
                    b.append(o.descending()).append(')');
                }
                b.append(')');
            }
            case SqlAst.ScalarSubquery q -> b.append("subquery@").append(q.pos().line()).append(':')
                    .append(q.pos().column());
            case SqlAst.Exists q -> b.append("exists@").append(q.pos().line()).append(':').append(q.pos().column());
            default -> {
                // an operator or a predicate: its kind and what it is made of, its operands in order
                b.append(e.getClass().getSimpleName()).append('(');
                switch (e) {
                    case SqlAst.Binary x -> b.append(x.op());
                    case SqlAst.Unary x -> b.append(x.op());
                    case SqlAst.IsNull x -> b.append(x.negated());
                    case SqlAst.IsTruth x -> b.append(x.truth()).append(x.negated());
                    case SqlAst.IsDistinct x -> b.append(x.negated());
                    case SqlAst.Between x -> b.append(x.negated());
                    case SqlAst.InList x -> b.append(x.negated()).append(x.values().size());
                    case SqlAst.Like x -> b.append(x.negated()).append(x.escape() != null);
                    case SqlAst.Case x -> b.append(x.operand() != null).append(x.whens().size())
                            .append(x.otherwise() != null);
                    case SqlAst.InSubquery x -> b.append(x.negated()).append('@').append(x.pos().line()).append(':')
                            .append(x.pos().column());
                    case SqlAst.Quantified x -> b.append(x.op()).append(x.all()).append('@').append(x.pos().line())
                            .append(':').append(x.pos().column());
                    // IQ island, 2026-10-15 (the corpus lane: SELECT CAST(ts AS DATE) ... GROUP BY CAST(ts AS DATE)): a
                    // CAST by its type and sizes
                    case SqlAst.Cast x -> b.append(IqLexer.lower(x.type().name())).append(x.type().modifiers());
                    default -> b.append('@').append(e.pos().line()).append(':').append(e.pos().column());
                }
                for (SqlAst.Expr operand : IqDialect.children(e)) {
                    b.append(',');
                    part(operand, b, child);
                }
                b.append(')');
            }
        }
    }

    /** Whether IQ may read {@code a} and {@code b}, two aggregates' calls, as one aggregate though they are not written
     *  as one: the same function, the same DISTINCT, the same columns (by name), constants and operators, in any
     *  arrangement (measured: {@code SUM(v + 1)} and {@code SUM(1 + v)} are one, so are {@code SUM(a.v)} and
     *  {@code SUM(v)}). */
    static boolean alike(SqlAst.FunctionCall a, SqlAst.FunctionCall b) {
        if (!IqLexer.lower(a.name().get(0).text()).equals(IqLexer.lower(b.name().get(0).text()))
                || a.distinct() != b.distinct() || a.star() != b.star()) {
            return false;
        }
        return parts(a).equals(parts(b));
    }

    /** The column names (ASCII lower case), constants and operators of a call's arguments, sorted. */
    private static List<String> parts(SqlAst.Expr e) {
        List<String> out = new ArrayList<>();
        parts(e, out);
        Collections.sort(out);
        return out;
    }

    private static void parts(SqlAst.Expr e, List<String> out) {
        switch (e) {
            case SqlAst.ColumnRef r -> out.add("c:" + last(r));
            case SqlAst.NullLit n -> out.add("null");
            case SqlAst.IntegerLit n -> out.add("n:" + n.value());
            case SqlAst.NumericLit n -> out.add("n:" + n.value().stripTrailingZeros().toPlainString());
            case SqlAst.StringLit s -> out.add("s:" + s.value());
            case SqlAst.Binary b -> out.add("b:" + b.op());
            case SqlAst.Unary u -> out.add("u:" + u.op());
            case SqlAst.FunctionCall f -> out.add("f:" + IqLexer.lower(f.name().get(f.name().size() - 1).text()));
            default -> out.add("e:" + e.getClass().getSimpleName());
        }
        for (SqlAst.Expr child : IqDialect.children(e)) {
            parts(child, out);
        }
    }

    // ------------------------------------------------------------------ names

    /**
     * The names a statement's grouped queries give their unaliased items in SQL Anywhere's front end (measured on SQL
     * Anywhere 17 and IQ 17.1; under IQ's engine each item is named by its own spelling, {@link #read}): IQ reads each
     * part of such an item that is a GROUP BY item, or an aggregate the query computes, as that item or aggregate where
     * it first reads it, and names it by that rendering. It reads the GROUP BY first, then HAVING, then the select list
     * from the left (never ORDER BY): {@code SELECT SUM(V) ... HAVING sum(v) > 0} names the item {@code sum(t.v)},
     * {@code SELECT K ... GROUP BY k} names it {@code k}, {@code SELECT K + 1 ... GROUP BY k} {@code t.k+1},
     * {@code SELECT 2 * (k + 1) ... GROUP BY k + 1} {@code 2*t.k+1} (the parentheses go). An aggregate IQ may read as
     * an earlier one not written alike ({@link #alike}) is not known: the item is refused by name.
     */
    static final class Naming {
        private final Map<SqlAst.Expr, String> spans;
        /** Each grouped query's unaliased items: what their names are read with. */
        private final Map<SqlAst.SelectExpr, Item> items = new IdentityHashMap<>();
        /** A grouped query's item that is a column of a GROUP BY item written bare: the GROUP BY item's last part as
         *  written (its name). */
        private final Map<SqlAst.Ident, String> keyNames = new IdentityHashMap<>();

        Naming(Map<SqlAst.Expr, String> spans) {
            this.spans = spans;
        }

        /** No grouped query: every item named as written. */
        static Naming none() {
            return new Naming(Map.of());
        }

        private record Item(SqlAst.Select select, List<SqlAst.FunctionCall> earlier) {
        }

        /** The text of a select item's expression, which an alias read in GROUP BY puts there. */
        private final Map<SqlAst.Expr, String> itemTexts = new IdentityHashMap<>();

        /** The text of a GROUP BY item or an aggregate's call as written; null when it is not known. */
        private @com.legend.base.Nullable String text(SqlAst.Expr e) {
            String t = spans.get(e);
            return t != null ? t : itemTexts.get(e);
        }

        void add(SqlAst.Select s) {
            for (SqlAst.SelectItem i : s.items()) {
                if (i instanceof SqlAst.SelectExpr x && x.text() != null) {
                    itemTexts.put(x.expr(), x.text());
                }
            }
            List<SqlAst.FunctionCall> read = new ArrayList<>();
            if (s.having() != null) {
                read.addAll(aggregateCalls(java.util.Objects.requireNonNull(s.having())));
            }
            for (SqlAst.SelectItem i : s.items()) {
                if (!(i instanceof SqlAst.SelectExpr x)) {
                    continue;
                }
                items.put(x, new Item(s, List.copyOf(read)));
                read.addAll(aggregateCalls(x.expr()));
                SqlAst.Expr e = x.expr();
                while (e instanceof SqlAst.Unary u && u.op() == SqlAst.UnaryOp.PLUS) {
                    e = u.operand();
                }
                if (x.alias() == null && e instanceof SqlAst.ColumnRef r) {
                    SqlAst.ColumnRef key = bareKey(s, r);
                    if (key != null) {
                        keyNames.put(r.parts().get(r.parts().size() - 1), key.parts().get(key.parts().size() - 1)
                                .text());
                    }
                }
            }
        }

        /** The first GROUP BY item of {@code s} that is a column written bare and that {@code r} may read: the same
         *  name, the same qualifier where both are qualified. */
        private static SqlAst.@com.legend.base.Nullable ColumnRef bareKey(SqlAst.Select s, SqlAst.ColumnRef r) {
            for (SqlAst.Expr k : s.groupBy()) {
                if (k instanceof SqlAst.ColumnRef key && key.parts().size() <= 2 && r.parts().size() <= 2
                        && last(key).equals(last(r)) && (key.parts().size() == 1 || r.parts().size() == 1
                        || IqLexer.lower(key.parts().get(0).text()).equals(IqLexer.lower(r.parts().get(0).text())))) {
                    return key;
                }
            }
            return null;
        }

        /** The name a grouped query's item that is a column written bare takes: its GROUP BY item's last part as
         *  written ({@code written} is the item's last part); null when it is no such item. */
        @com.legend.base.Nullable String keyName(SqlAst.Ident written) {
            return keyNames.get(written);
        }

        /** What the parts of {@code item} render as ({@link IqNames.Substitutes}), its columns rendered by
         *  {@code columns} and found in the FROM clause by {@code tables}; {@link IqNames#NONE} when its query does not
         *  group. */
        IqNames.Substitutes substitutes(SqlAst.SelectExpr item, IqSettings settings, IqNames.Columns columns,
                com.legend.protocol.sql.InputDialect.Tables tables) {
            Item it = items.get(item);
            if (it == null) {
                return IqNames.NONE;
            }
            SqlAst.Select s = it.select();
            List<SqlAst.FunctionCall> mine = aggregateCalls(item.expr());
            int[] seen = {0};
            return (e, inAggregate) -> {
                if (inAggregate) {
                    return null;
                }
                if (e instanceof SqlAst.FunctionCall f && named(f)) {
                    int index = seen[0]++;
                    List<SqlAst.FunctionCall> before = new ArrayList<>(it.earlier());
                    before.addAll(mine.subList(0, Math.min(index, mine.size())));
                    for (SqlAst.FunctionCall c : before) {
                        if (!alike(c, f)) {
                            continue;
                        }
                        if (!same(c, f)) {
                            return IqNames.UNKNOWN;
                        }
                        String text = text(c);
                        String r = text == null ? null : IqNames.name(text, settings, columns);
                        return new IqNames.Part(r);
                    }
                    return null;
                }
                for (SqlAst.Expr k : s.groupBy()) {
                    if (k instanceof SqlAst.ColumnRef || !same(k, e)) {
                        continue;
                    }
                    String text = text(k);
                    return new IqNames.Part(text == null ? null : IqNames.name(text, settings, columns));
                }
                if (e instanceof SqlAst.ColumnRef r) {
                    SqlAst.ColumnRef key = bareKey(s, r);
                    if (key == null) {
                        return null;
                    }
                    SqlAst.Ident mineTable = tables.table(r);
                    SqlAst.Ident keyTable = tables.table(key);
                    if (mineTable == null || keyTable == null
                            || !IqLexer.lower(mineTable.text()).equals(IqLexer.lower(keyTable.text()))) {
                        return IqNames.UNKNOWN;
                    }
                    return new IqNames.Part(key.parts().size() == 2 ? key.parts().get(0).text() + "."
                            + key.parts().get(1).text() : columns.rendering(key, false));
                }
                return null;
            };
        }
    }

    // ------------------------------------------------------------------ the aggregates

    /** {@code f}, one of the aggregates read, computed {@code at} its grouped query under {@code dialect}'s engine and
     *  profile (the class's rules). */
    static SqlToPure.Aggregation aggregate(SqlAst.FunctionCall f, SqlToPure.Grouping at, IqDialect dialect) {
        IqDialect.Engine engine = java.util.Objects.requireNonNull(dialect.engine(), "the statement's engine");
        IqSettings settings = dialect.settings();
        String fn = IqLexer.lower(f.name().get(0).text());
        String upper = fn.toUpperCase(java.util.Locale.ROOT);
        if (f.star()) {
            ColSpec count = new ColSpec(at.name(), at.overRow(new CInteger(1L)), at.overGroup(SqlToPure.call("count",
                    at.values())), null);
            return new SqlToPure.Aggregation(List.of(count), null, Kind.INTEGER, false, countType(engine));
        }
        // a percentile's values are its WITHIN GROUP's, its argument the fraction
        boolean percentile = !f.withinGroup().isEmpty();
        SqlAst.Expr argument = percentile ? f.withinGroup().get(0).expr() : f.args().get(0);
        List<SqlAst.ColumnRef> refs = new ArrayList<>();
        f.args().forEach(a -> columns(a, refs, true));
        f.withinGroup().forEach(o -> columns(o.expr(), refs, true));
        boolean mine = refs.stream().anyMatch(r -> at.inputColumn(r) != null);
        boolean theirs = refs.stream().anyMatch(at::enclosing);
        if (mine && theirs) {
            // (only an enclosing query's: the translator refuses it)
            throw SqlTranslationException.invalid(f.name().get(0).text() + "(...) reads both an enclosing query's column"
                    + " and one of its own" + code(-861, "42W68"), f.pos());
        }
        Tx x = at.tx(argument);
        IqType type = x.detail() instanceof IqType t ? t : null;
        ValueSpecification values = f.distinct() ? SqlToPure.call("distinct", at.values()) : at.values();
        return switch (Fn.of(f)) {
            case COUNT -> {
                ValueSpecification counted = SqlToPure.valueOf(x);
                if (f.distinct() && x.kind() == Kind.STRING) {
                    // IQ island, 2026-10-14 (the merge): the distinct collation keys ({@link IqDialect#collated})
                    counted = SqlToPure.valueOf(SqlToPure.strict(Kind.STRING, List.of(x),
                            v -> dialect.collated(v.get(0))));
                } else if (f.distinct()) {
                    key(x, upper + "(DISTINCT ...)", f.pos());
                }
                ColSpec count = new ColSpec(at.name(), at.overRow(counted), at.overGroup(
                        SqlToPure.call("count", values)), null);
                yield new SqlToPure.Aggregation(List.of(count), null, Kind.INTEGER, false, countType(engine));
            }
            case SUM -> sum(f, at, x, type, values, dialect, false);
            case AVG -> sum(f, at, x, type, values, dialect, true);
            case MIN, MAX -> {
                if (x.kind() == Kind.NULL && engine == IqDialect.Engine.IQ) {
                    // IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine a VARCHAR(4)'s (D-N7)
                    ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.nullOf(Kind.STRING).vs()),
                            at.overGroup(SqlToPure.call(fn, values)), null);
                    yield new SqlToPure.Aggregation(List.of(spec), null, Kind.STRING, true, new IqType("VARCHAR(4)"));
                }
                if (x.kind() == Kind.NULL) {
                    // IQ's NULL here is a BIT's (measured)
                    ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.nullOf(Kind.INTEGER).vs()),
                            at.overGroup(SqlToPure.call(fn, values)), null);
                    yield new SqlToPure.Aggregation(List.of(spec), null, Kind.INTEGER, true, new IqType("BIT"));
                }
                if (x.kind() == Kind.STRING) {
                    // IQ island, 2026-10-14 (the merge): IQ shows the first value of the least (greatest) key it meets
                    if (!dialect.keys().own(argument)) {
                        yield memberOfKey(at, x, values, fn, type, dialect);
                    }
                    ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.valueOf(x)), at.overGroup(
                            SqlToPure.call(fn, values)), null);
                    yield new SqlToPure.Aggregation(List.of(spec), null, Kind.STRING, true, type);
                }
                key(x, upper, f.pos());
                if (type == null || !(x.kind().numeric() || x.kind() == Kind.DATE || x.kind() == Kind.DATETIME)) {
                    throw SqlTranslationException.unsupported(upper + " of a value whose IQ type is not known is not"
                            + " supported", f.pos());
                }
                ValueSpecification map = SqlToPure.valueOf(x);
                ValueSpecification reduce = SqlToPure.call(fn, values);
                if (x.kind() == Kind.DECIMAL) {
                    // Pure's min and max of decimals are Numbers: the decimal itself is minBy's and maxBy's of its
                    // pairs with itself (ties are one value)
                    map = SqlToPure.call("rowMapper", map, map);
                    reduce = SqlToPure.call(fn + "By", values);
                }
                ColSpec spec = new ColSpec(at.name(), at.overRow(map), at.overGroup(reduce), null);
                yield new SqlToPure.Aggregation(List.of(spec), null, x.kind(), true, type);
            }
            case MEDIAN -> median(f, at, x, type, values);
            case LIST -> list(f, at, x, type, dialect);
            case PERCENTILE_CONT, PERCENTILE_DISC -> percentile(f, at, x, type, values);
            case ARRAY_AGG -> arrayAgg(f, at, x, engine);
        };
    }

    /** The separator {@link #memberOfKey} packs a value behind its key with: below every byte of an escaped key. */
    private static final String SEPARATOR = "\u0001";

    /**
     * MIN or MAX ({@code fn}) of strings whose values may share a collation key (IQ island, 2026-10-15; the user's
     * rule: which value of the least key IQ shows is its plan's choice -- the first it meets, measured -- so any is an
     * answer IQ could give): a value whose key is the least (greatest), one of the group's own values, never the key.
     * Each value is packed behind its key ({@link IqDialect#collated}), the key escaped so that no byte of it is the
     * separator's or below ({@code \x02} as {@code \x02\x03}, then {@code \x01} as {@code \x02\x02}, which keeps
     * the keys' byte order), then the separator ({@code \x01}), then the value: the least (greatest) packed string has
     * the least (greatest) key, as the databases compare bytes, and its value is what follows the first separator.
     */
    private static SqlToPure.Aggregation memberOfKey(SqlToPure.Grouping at, Tx x, ValueSpecification values, String fn,
            @com.legend.base.Nullable IqType type, IqDialect dialect) {
        Tx packed = SqlToPure.strict(Kind.STRING, List.of(x), v -> packedByKey(v.get(0), dialect));
        String raw = at.fresh();
        ColSpec spec = new ColSpec(raw, at.overRow(SqlToPure.valueOf(packed)), at.overGroup(SqlToPure.call(fn,
                values)), null);
        Tx shown = SqlToPure.strict(Kind.STRING, List.of(Tx.value(at.column(raw), Kind.STRING, true)),
                v -> unpacked(v.get(0)));
        return new SqlToPure.Aggregation(List.of(spec), new ColSpec(at.name(), at.overRow(shown.vs()), null, null),
                Kind.STRING, true, type);
    }

    /** {@code value} packed behind its collation key, escaped, and the separator ({@link #memberOfKey}): the least
     *  (greatest) packed string has the least (greatest) key, whatever bytes the key holds. */
    static ValueSpecification packedByKey(ValueSpecification value, IqDialect dialect) {
        ValueSpecification key = SqlToPure.call("replace", SqlToPure.call("replace", dialect.collated(value),
                new CString("\u0002"), new CString("\u0002\u0003")), new CString(SEPARATOR),
                new CString("\u0002\u0002"));
        return AppliedFunction.infixRun("plus", List.of(key, new CString(SEPARATOR), value));
    }

    /** The value {@link #packedByKey} packed: what follows its first separator. */
    static ValueSpecification unpacked(ValueSpecification packed) {
        return SqlToPure.call("substring", packed, AppliedFunction.infixRun("plus", List.of(SqlToPure.call("indexOf",
                packed, new CString(SEPARATOR)), new CInteger(1L))));
    }

    /** COUNT's type: SQL Anywhere's INTEGER in its front end, IQ's UNSIGNED BIGINT under its engine (D-F15). */
    private static IqType countType(IqDialect.Engine engine) {
        return engine == IqDialect.Engine.IQ ? new IqType("UNSIGNED BIGINT")
                : IqType.integer("INTEGER", BigInteger.ZERO, IqType.INTEGER_MAX, true);
    }

    /** A value an aggregate compares (DISTINCT's, MIN's, MAX's) other than a string (IQ island, 2026-10-14: those by
     *  IQ's collation, in {@link #aggregate}). */
    private static void key(Tx x, String what, SqlAst.Pos pos) {
        if (x.kind() == Kind.OTHER || x.kind() == Kind.JSON || x.kind() == Kind.BOOLEAN) {
            throw SqlTranslationException.unsupported(what + " of a value of this type is not supported yet", pos);
        }
    }

    /**
     * SUM, and AVG (which sums in SUM's type, so overflows as SUM does): by the argument's IQ type, checked where some
     * order of the running total could leave the type ({@link IqAggregates}: the positive values' sum above its
     * largest, the negative values' below its smallest; D-E3).
     */
    private static SqlToPure.Aggregation sum(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            @com.legend.base.Nullable IqType type, ValueSpecification values, IqDialect dialect, boolean average) {
        IqDialect.Engine engine = java.util.Objects.requireNonNull(dialect.engine(), "the statement's engine");
        IqSettings settings = dialect.settings();
        String what = average ? "AVG" : "SUM";
        Kind kind = x.kind();
        if (kind == Kind.NULL) {
            // IQ's SUM of an untyped NULL is an INTEGER NULL, its AVG a NUMERIC one (measured); Postgres has no sum of
            // an untyped NULL
            throw SqlTranslationException.unsupported(what + "(NULL) is not supported yet", f.pos());
        }
        if (kind == Kind.FLOAT) {
            // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): under IQ's engine SUM and AVG of a REAL are
            // DOUBLEs, its values added in double precision
            if (type == null || !type.family().equals("DOUBLE") && !(type.family().equals("REAL")
                    && engine == IqDialect.Engine.IQ)) {
                throw SqlTranslationException.unsupported(what + " of a value of this type is not supported yet",
                        f.pos());
            }
            boolean guard = readEverywhere(f, dialect);
            return checkedSum(at, x, values, average ? "average" : "sum", Kind.FLOAT, new IqType("DOUBLE"),
                    new CFloat(0.0), new CFloat(Double.MAX_VALUE), new CFloat(-Double.MAX_VALUE), guard);
        }
        if (average && engine == IqDialect.Engine.IQ) {
            return iqAverage(f, at, x, type, values, settings);
        }
        if (average && !(type != null && type.family().equals("NUMERIC"))) {
            throw SqlTranslationException.unsupported("AVG of an integer is not supported yet: IQ's is a NUMERIC(49,21)"
                    + " quotient, beyond the 38 digits lite's decimals hold", f.pos());
        }
        if (kind == Kind.INTEGER && type != null && (type.isInteger() || type.family().equals("TINYINT"))) {
            String family = type.family();
            if (family.equals("BIT") || family.equals("TINYINT")) {
                // a NUMERIC(18,0) of the count of ones, or of small values: never out of range
                ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.valueOf(x)), at.overGroup(
                        SqlToPure.call("sum", values)), null);
                return new SqlToPure.Aggregation(List.of(spec), null, Kind.INTEGER, true, new IqType("NUMERIC(18,0)"));
            }
            if (engine == IqDialect.Engine.IQ) {
                return iqIntegerSum(at, x, values, family);
            }
            boolean guard = readEverywhere(f, dialect);
            boolean bigint = family.equals("BIGINT");
            BigInteger min = bigint ? IqType.BIGINT_MIN : IqType.INTEGER_MIN;
            BigInteger max = bigint ? IqType.BIGINT_MAX : IqType.INTEGER_MAX;
            return checkedSum(at, x, values, "sum", Kind.INTEGER, IqType.integer(bigint ? "BIGINT" : "INTEGER", min,
                    max, true), new CInteger(0L), SqlToPure.integer(max), SqlToPure.integer(min), guard);
        }
        if (kind == Kind.INTEGER && type != null && engine == IqDialect.Engine.IQ && type.spelling().equals(
                "UNSIGNED BIGINT")) {
            // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): a SUM of COUNTs (a derived table's) is a
            // NUMERIC(35,0), read as iqIntegerSum reads an INTEGER's: a whole number of 64 bits
            return iqIntegerSum(at, x, values, "BIGINT");
        }
        if ((kind == Kind.DECIMAL || kind == Kind.INTEGER) && type != null && type.family().equals("NUMERIC")) {
            if (engine == IqDialect.Engine.IQ) {
                return iqNumericSum(f, at, x, type, values, settings);
            }
            int[] ps = numeric(type);
            int p = ps[0] + 8;
            if (p > 38 || p > settings.precision()) {
                throw SqlTranslationException.unsupported("SUM of a NUMERIC(" + ps[0] + "," + ps[1] + ") is not"
                        + " supported: its type, NUMERIC(" + p + "," + ps[1] + "), is capped by the profile or needs"
                        + " more than lite's 38 digits", f.pos());
            }
            if (average) {
                return average(f, at, x, values, dialect, ps[0], ps[1]);
            }
            ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("sum",
                    values)), null);
            return new SqlToPure.Aggregation(List.of(spec), null, kind, true, new IqType("NUMERIC(" + p + ","
                    + ps[1] + ")"));
        }
        throw SqlTranslationException.unsupported(what + " of " + (kind == Kind.STRING ? "a string" : kind == Kind.DATE
                || kind == Kind.DATETIME ? "a date" : "a value of this type") + " is not supported yet (IQ converts it"
                + " to a number by its own rules)", f.pos());
    }

    /**
     * AVG of a NUMERIC(p,s) in SQL Anywhere's front end (IQ island, 2026-10-14; the natives slice's measurement, 129
     * AVGs on each database): a NUMERIC(p+19, s+11), the quotient of the SUM (a NUMERIC(p+8,s)) and the COUNT of the
     * values by SQL Anywhere's rule ({@link IqFunctions#quotient}); an empty group's NULL. Shown only: as an operand,
     * a CAST, a comparison or a key SQL Anywhere reads the quotient unrounded ({@code AVG(v)*3} is 5.00000000000 for
     * an AVG of 1.66666666667). Refused by name where it is not shown, where the profile's PRECISION caps its type, and
     * where a step would need more than lite's 38 digits.
     */
    private static SqlToPure.Aggregation average(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            ValueSpecification values, IqDialect dialect, int p, int s) {
        IqSettings settings = dialect.settings();
        int pAvg = p + 19;
        int sAvg = s + 11;
        if (!dialect.shownValue(f)) {
            throw SqlTranslationException.unsupported("AVG of a NUMERIC is supported only as a select item of a SELECT"
                    + " without DISTINCT that nothing else in the statement reads (by its alias or position, or the"
                    + " same AVG written again): SQL Anywhere rounds the quotient to its scale only where it shows it,"
                    + " and an operand, a comparison or a key reads it with more digits", f.pos());
        }
        if (pAvg > settings.precision()) {
            throw SqlTranslationException.unsupported("AVG of a NUMERIC(" + p + "," + s + ") is not supported under"
                    + " profile " + settings.spelling() + ": its NUMERIC(" + pAvg + "," + sAvg + ") is cut to the"
                    + " profile's PRECISION (" + settings.precision() + ")", f.pos());
        }
        int sumWhole = p + 8 - s;
        if (!IqFunctions.quotientFits(s, sumWhole, 0, COUNT_DIGITS, pAvg, sAvg)) {
            throw SqlTranslationException.unsupported("AVG of a NUMERIC(" + p + "," + s + ") needs a step of more than"
                    + " the 38 digits lite's decimals hold", f.pos());
        }
        String sum = at.fresh();
        String count = at.fresh();
        List<ColSpec> specs = List.of(
                new ColSpec(sum, at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("sum", values)), null),
                new ColSpec(count, at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("count", values)),
                        null));
        Tx total = Tx.value(at.column(sum), x.kind(), true);
        Tx n = Tx.value(at.column(count), Kind.INTEGER, false);
        Tx quotient = SqlToPure.strict(Kind.DECIMAL, List.of(total, n), v -> IqFunctions.quotient(
                IqDialect.wide(v.get(0)), s, sumWhole, SqlToPure.call("toDecimal", v.get(1)), 0, COUNT_DIGITS, pAvg,
                sAvg));
        return new SqlToPure.Aggregation(specs, new ColSpec(at.name(), at.overRow(quotient.vs()), null, null),
                Kind.DECIMAL, true, new IqType("NUMERIC(" + pAvg + "," + sAvg + ")"));
    }

    /**
     * SUM of an integer under IQ's engine (IQ island, 2026-10-15; the research's D-N3, measured on IQ 17.1): no
     * running total is checked -- IQ answers 2147483661 for a sum of INTEGERs past 2^31 -- and the value is the exact
     * sum. IQ types a SMALLINT's sum a BIGINT, an INTEGER's or a BIGINT's a NUMERIC(20,0) (whatever the values); lite
     * reads each as a whole number of 64 bits (the scale-0 NUMERIC IQ shows holds the same value), so that arithmetic
     * on it is integer arithmetic, which IQ's engine widens as it does a column's. A sum of BIGINTs past 64 bits is the
     * database's own overflow, where IQ answers the NUMERIC.
     */
    private static SqlToPure.Aggregation iqIntegerSum(SqlToPure.Grouping at, Tx x, ValueSpecification values,
            String family) {
        ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("sum",
                values)), null);
        return new SqlToPure.Aggregation(List.of(spec), null, Kind.INTEGER, true, IqType.integer("BIGINT",
                IqType.BIGINT_MIN, IqType.BIGINT_MAX, true, family.equals("BIGINT")));
    }

    /**
     * SUM of a NUMERIC(p,s) under IQ's engine (IQ island, 2026-10-15; the research's D-N1, measured on IQ 17.1): a
     * NUMERIC(p+1, s), whatever the values (an empty group's too; NUMERIC(126,38) stays one), the exact sum. Refused by
     * name where the type needs more than lite's 38 digits or the profile's PRECISION. IQ 17.1 answers no rows at all
     * where a sum leaves p+1 digits (a SUM over a join of NUMERIC(7,2)s past 999999.99, measured), its defect: lite
     * answers the sum.
     */
    private static SqlToPure.Aggregation iqNumericSum(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            IqType type, ValueSpecification values, IqSettings settings) {
        measuredOptions(f, settings, "SUM");
        int[] ps = numeric(type);
        int p = Math.min(ps[0] + 1, 126);
        if (p > 38 && p <= settings.precision() && ps[1] <= 37) {
            // IQ island, 2026-10-17 (a benchmark's SUM(a * (1 - b) * (1 + c)) of NUMERIC(15,2)s, IQ's NUMERIC(48,6)):
            // past 38 digits IQ's type holds what lite's 38 do at its scale; the database adds in 38, a sum past them
            // its own overflow, so lite types the value it holds
            p = 38;
        }
        if (p > 38 || p > settings.precision()) {
            throw SqlTranslationException.unsupported("SUM of a NUMERIC(" + ps[0] + "," + ps[1] + ") is not supported"
                    + " under IQ's engine: its type, NUMERIC(" + p + "," + ps[1] + "), is capped by the profile or needs"
                    + " more than lite's 38 digits", f.pos());
        }
        ColSpec spec = new ColSpec(at.name(), at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("sum",
                values)), null);
        return new SqlToPure.Aggregation(List.of(spec), null, x.kind(), true, new IqType("NUMERIC(" + p + "," + ps[1]
                + ")"));
    }

    /**
     * AVG under IQ's engine (IQ island, 2026-10-15; the research's D-N1, measured on IQ 17.1): of a SMALLINT, an
     * INTEGER, a BIGINT, a BIT or a TINYINT a NUMERIC of scale 16; of a NUMERIC(p,s) one of scale s + 21 (the
     * precision is the engine's, from the data: p + 22 where measured); the value SUM / COUNT cut toward zero at that
     * scale ({@link IqFunctions#truncatedQuotient}) wherever it is read -- HAVING compares the cut value, and
     * {@code AVG(v) * 3} is 12.9999999999999999 for 13 / 3; an empty group's NULL. Refused by name where the value
     * may need more than lite's 38 digits: a NUMERIC of more than 16 digits (its AVG's p + 22), a BIGINT whose sum may
     * pass 22 digits being bounded by the database's arithmetic.
     */
    private static SqlToPure.Aggregation iqAverage(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            @com.legend.base.Nullable IqType type, ValueSpecification values, IqSettings settings) {
        measuredOptions(f, settings, "AVG");
        int scale;
        int sumScale;
        int sumWhole;
        String spelling;
        boolean numeric = type != null && type.family().equals("NUMERIC");
        boolean counts = type != null && type.spelling().equals("UNSIGNED BIGINT");
        if (x.kind() == Kind.INTEGER && type != null && (type.isInteger() || type.family().equals("TINYINT")
                || type.family().equals("BIT") || counts)) {
            scale = 16;
            sumScale = 0;
            // a sum of BIGINTs is a whole number the databases add in 38 digits: 22 before the 16 places
            sumWhole = 22;
            // IQ island, 2026-10-15 (the corpus lane; measured on IQ 17.1): an AVG of COUNTs is a NUMERIC(51,16), of a
            // COUNT beside a signed integer (COALESCE(n, 0), a range known) a NUMERIC(45,16)
            spelling = !counts ? "NUMERIC(36,16)" : type.low() == null ? "NUMERIC(51,16)" : "NUMERIC(45,16)";
        } else if (type != null && numeric && (x.kind() == Kind.DECIMAL || x.kind() == Kind.INTEGER)) {
            int[] ps = numeric(type);
            if (ps[0] + 22 > settings.precision() || ps[1] + 21 > 37) {
                throw SqlTranslationException.unsupported("AVG of a NUMERIC(" + ps[0] + "," + ps[1] + ") is not"
                        + " supported under IQ's engine: its NUMERIC(" + (ps[0] + 22) + "," + (ps[1] + 21) + ") needs"
                        + " more than lite's 38 digits or the profile's PRECISION", f.pos());
            }
            scale = ps[1] + 21;
            sumScale = ps[1];
            sumWhole = ps[0] + 1 - ps[1];
            // IQ island, 2026-10-17: past 38 digits IQ's type holds what lite's 38 do (longQuotient refuses at run time
            // a value past them), so lite types the value it holds: its comparisons and arithmetic sized within 38
            spelling = "NUMERIC(" + Math.min(ps[0] + 22, 38) + "," + scale + ")";
        } else {
            throw SqlTranslationException.unsupported("AVG of " + (x.kind() == Kind.STRING ? "a string"
                    : x.kind() == Kind.DATE || x.kind() == Kind.DATETIME ? "a date" : "a value of this type")
                    + " is not supported yet (IQ converts it to a number by its own rules)", f.pos());
        }
        // IQ island, 2026-10-17: a quotient a single step cannot hold is divided in chunks (IqFunctions.longQuotient),
        // a value of more digits than lite's decimals hold refused at run time
        boolean chunked = !IqFunctions.truncatedFits(sumScale, sumWhole, scale) || numeric
                && numeric(java.util.Objects.requireNonNull(type))[0] + 22 > 38;
        String past = IqDialect.REFUSED_AT_RUN_TIME + "an AVG of more than " + (38 - scale) + " digits before its point"
                + " is not supported: its " + spelling + " needs more than the 38 digits lite's decimals hold";
        if (chunked && IqFunctions.longQuotient(new CDecimal(java.math.BigDecimal.ONE), sumScale, sumWhole,
                new CDecimal(java.math.BigDecimal.ONE), COUNT_DIGITS, scale, past) == null) {
            throw SqlTranslationException.unsupported("AVG of " + java.util.Objects.requireNonNull(type).spelling()
                    + " under IQ's engine needs a step"
                    + " of more than the 38 digits lite's decimals hold", f.pos());
        }
        String sum = at.fresh();
        String count = at.fresh();
        ValueSpecification value = numeric ? SqlToPure.valueOf(x) : SqlToPure.valueOf(SqlToPure.strict(Kind.DECIMAL,
                List.of(x), v -> SqlToPure.call("toDecimal", v.get(0))));
        List<ColSpec> specs = List.of(
                new ColSpec(sum, at.overRow(value), at.overGroup(SqlToPure.call("sum", values)), null),
                new ColSpec(count, at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("count", values)),
                        null));
        Tx total = Tx.value(at.column(sum), Kind.DECIMAL, true);
        Tx n = Tx.value(at.column(count), Kind.INTEGER, false);
        int s1 = sumScale;
        int whole1 = sumWhole;
        Tx quotient = SqlToPure.strict(Kind.DECIMAL, List.of(total, n), v -> chunked
                ? java.util.Objects.requireNonNull(IqFunctions.longQuotient(IqDialect.wide(v.get(0)), s1, whole1,
                        SqlToPure.call("toDecimal", v.get(1)), COUNT_DIGITS, scale, past))
                : IqFunctions.truncatedQuotient(IqDialect.wide(v.get(0)), s1, whole1, SqlToPure.call("toDecimal",
                        v.get(1)), scale));
        return new SqlToPure.Aggregation(specs, new ColSpec(at.name(), at.overRow(quotient.vs()), null, null),
                Kind.DECIMAL, true, new IqType(spelling));
    }

    /**
     * ARRAY_AGG (IQ island, 2026-10-17; the JOB corpus lane's 553, measured on IQ 17.1): under IQ's engine its -1001030
     * (an aggregate it does not support), which the dialect holds before translating ({@code IqDialect}'s placements)
     * and raises after the names, the grouping and the conversions; here, an aggregate that reads its argument and
     * shows a NULL string, which no answer ever carries. In SQL Anywhere's front end an array is no value a client
     * reads (its -1599 in a select list): refused by name.
     */
    private static SqlToPure.Aggregation arrayAgg(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            IqDialect.Engine engine) {
        if (engine != IqDialect.Engine.IQ) {
            throw SqlTranslationException.unsupported("ARRAY_AGG is not supported in SQL Anywhere's front end: its"
                    + " array is no value a client reads (its -1599 in a select list)", f.pos());
        }
        String read = at.fresh();
        ColSpec count = new ColSpec(read, at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("count",
                at.values())), null);
        ColSpec shown = new ColSpec(at.name(), at.overRow(SqlToPure.call("cast", SqlToPure.empty(),
                SqlToPure.typeArg("String"))), null, null);
        return new SqlToPure.Aggregation(List.of(count), shown, Kind.STRING, true, new IqType("LONG VARCHAR"));
    }

    /** IQ island, 2026-10-15: the IQ engine's SUM and AVG types were measured on IQ 17.1 at its PUBLIC PRECISION 126
     *  and SCALE 38 only; under other values they are refused by name (how the options cut them is not measured). */
    private static void measuredOptions(SqlAst.FunctionCall f, IqSettings settings, String what) {
        if (settings.precision() != 126 || settings.scale() != 38) {
            throw SqlTranslationException.unsupported(what + " under IQ's engine is supported at PRECISION 126 and"
                    + " SCALE 38 only (profile " + settings.spelling() + " has " + settings.precision() + " and "
                    + settings.scale() + "): how IQ cuts its type to other values is not measured", f.pos());
        }
    }

    /** The digits of a COUNT, as a quotient's divisor: a BIGINT's at most. */
    private static final int COUNT_DIGITS = 19;

    /** A NUMERIC(p,s) type's p and s. */
    private static int[] numeric(IqType t) {
        String sp = t.spelling();
        int comma = sp.indexOf(',');
        int scale = comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
        return new int[] {t.size(), scale};
    }

    /** A sum (or an average) whose running total IQ checks: the value, the positive and the negative values' sums, and
     *  the finish that raises IQ's -158 where either leaves the type. */
    /** IQ island, 2026-10-14 (review 3's B1): a checked SUM or AVG ({@link #checkedSum}) that the statement reads only
     *  for some rows or groups is refused by name ({@link IqDialect#lazilyRead}). */
    private static boolean readEverywhere(SqlAst.FunctionCall f, IqDialect dialect) {
        // IQ island, 2026-10-17 (measured on IQ 17.1: a DOUBLE sum's overflow is its engine's -1001010 in every group,
        // those its TOP drops too): read for some groups only by the statement's own SELECT (a CASE, a COALESCE, its
        // row limit), the check is a filter of every group after the aggregation (the guard); from a query around
        // it, which may keep groups from the aggregation itself, refused by name
        if (!dialect.lazilyReadAround(f)) {
            return dialect.lazilyRead(f);
        }
        if (dialect.lazilyRead(f)) {
            throw SqlTranslationException.unsupported(f.name().get(0).text().toUpperCase(java.util.Locale.ROOT)
                    + " read only for some groups (a CASE's or an IF's value or later test, a later argument of"
                    + " COALESCE, ISNULL, IFNULL or ARGN, under AND or OR, a bound of BETWEEN or a value of IN, under a"
                    + " row limit, in a subquery, a derived table or a set operation's branch)"
                    + " is not supported: IQ checks its running total for every group (D-E3), lite's databases only"
                    + " where they read it", f.pos());
        }
        return false;
    }

    private static SqlToPure.Aggregation checkedSum(SqlToPure.Grouping at, Tx x, ValueSpecification values,
            String reduce, Kind kind, IqType type, ValueSpecification zero, ValueSpecification max,
            ValueSpecification min, boolean guarded) {
        String raw = at.fresh();
        String positive = at.fresh();
        String negative = at.fresh();
        Tx up = SqlToPure.strict(kind, List.of(x), v -> IqDialect.ifThen(SqlToPure.call("greaterThan", v.get(0), zero),
                v.get(0), zero));
        Tx down = SqlToPure.strict(kind, List.of(x), v -> IqDialect.ifThen(SqlToPure.call("lessThan", v.get(0), zero),
                v.get(0), zero));
        // under DISTINCT, the distinct positive (negative) values and a zero: their sum is the distinct values'
        List<ColSpec> specs = List.of(
                new ColSpec(raw, at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call(reduce, values)), null),
                new ColSpec(positive, at.overRow(SqlToPure.valueOf(up)), at.overGroup(SqlToPure.call("sum", values)),
                        null),
                new ColSpec(negative, at.overRow(SqlToPure.valueOf(down)), at.overGroup(SqlToPure.call("sum",
                        values)), null));
        Tx p = Tx.value(at.column(positive), kind, true);
        Tx n = Tx.value(at.column(negative), kind, true);
        Tx r = Tx.value(at.column(raw), kind, true);
        Tx checked = SqlToPure.strict(kind, List.of(p, n, r), v -> IqDialect.ifThen(SqlToPure.call("or",
                SqlToPure.call("greaterThan", v.get(0), max), SqlToPure.call("lessThan", v.get(1), min)),
                kind == Kind.FLOAT ? IqDialect.floatRaise() : IqDialect.raise(), v.get(2)));
        // IQ island, 2026-10-17: the same test of every group, a filter after the aggregation, where the statement
        // reads the value for some groups only (readEverywhere)
        ValueSpecification guard = null;
        if (guarded) {
            Tx leaves = SqlToPure.strictTest(List.of(p, n), v -> SqlToPure.call("or", SqlToPure.call("greaterThan",
                    v.get(0), max), SqlToPure.call("lessThan", v.get(1), min)));
            guard = IqDialect.ifThen(leaves.vs(), SqlToPure.call("fail", new com.legend.protocol.spec.CString("a "
                    + (reduce.equals("average") ? "AVG" : "SUM") + "'s running total leaves its type in a group: IQ's"
                    + " error (-158; -1001010 under its engine for a DOUBLE), raised in its aggregation for every"
                    + " group")), new com.legend.protocol.spec.CBoolean(true));
        }
        return new SqlToPure.Aggregation(specs, new ColSpec(at.name(), at.overRow(checked.vs()), null, null), kind,
                true, type, guard);
    }

    /** MEDIAN of a SMALLINT or an INTEGER (or a TINYINT): Pure's continuous percentile at 0.5 of the values as
     *  doubles (exact for these: the midpoint of two 32-bit integers is a double's), floored, as the input type. */
    private static SqlToPure.Aggregation median(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            @com.legend.base.Nullable IqType type, ValueSpecification values) {
        String family = type == null ? "" : type.family();
        if (x.kind() == Kind.NULL || x.kind() == Kind.STRING || x.kind() == Kind.DATE || x.kind() == Kind.DATETIME
                || family.equals("BIT")) {
            throw SqlTranslationException.invalid("MEDIAN takes a number, not " + (x.kind() == Kind.NULL ? "NULL"
                    : family.equals("BIT") ? "a BIT" : "this value") + code(-156, "42W08"), f.pos());
        }
        if (f.distinct()) {
            throw SqlTranslationException.unsupported("MEDIAN(DISTINCT ...) is not supported yet: lite's Postgres takes"
                    + " no percentile of distinct values", f.pos());
        }
        if (x.kind() == Kind.FLOAT || x.kind() == Kind.DECIMAL) {
            return middles(f, at, x, java.util.Objects.requireNonNull(type, "a NUMERIC's or a DOUBLE's type"), values);
        }
        if (x.kind() != Kind.INTEGER || !(family.equals("SMALLINT") || family.equals("INTEGER")
                || family.equals("TINYINT"))) {
            throw SqlTranslationException.unsupported("MEDIAN of a " + (type == null ? "value of this type"
                    : type.spelling()) + " is not supported yet: lite's databases compute its midpoint otherwise than"
                    + " IQ (in double precision, or by another formula)", f.pos());
        }
        if (f.distinct()) {
            throw SqlTranslationException.unsupported("MEDIAN(DISTINCT ...) is not supported yet: lite's Postgres takes"
                    + " no percentile of distinct values", f.pos());
        }
        if (java.util.Objects.requireNonNull(type).hi().subtract(type.lo()).compareTo(IqType.INTEGER_MAX) > 0) {
            // IQ island, 2026-10-14 (review 3's M2, measured): SQL Anywhere computes the midpoint a + (b - a) / 2 in
            // INTEGER and wraps where two values differ by more than 2^31 - 1 (MEDIAN of 2147483647 and -2147483648
            // is -2147483648): its defect, and the IQ engine's is not measured -- refused under both
            throw SqlTranslationException.unsupported("MEDIAN of an INTEGER whose values may differ by more than"
                    + " 2147483647 is not supported: SQL Anywhere's midpoint wraps there (its defect)", f.pos());
        }
        String raw = at.fresh();
        Tx asDouble = SqlToPure.strict(Kind.FLOAT, List.of(x), v -> SqlToPure.call("toFloat", v.get(0)));
        ColSpec spec = new ColSpec(raw, at.overRow(SqlToPure.valueOf(asDouble)), at.overGroup(SqlToPure.call(
                "percentile", values, new CFloat(0.5), new com.legend.protocol.spec.CBoolean(true),
                new com.legend.protocol.spec.CBoolean(true))), null);
        Tx floored = SqlToPure.strict(Kind.INTEGER, List.of(Tx.value(at.column(raw), Kind.FLOAT, true)),
                v -> SqlToPure.call("floor", v.get(0)));
        return new SqlToPure.Aggregation(List.of(spec), new ColSpec(at.name(), at.overRow(floored.vs()), null, null),
                Kind.INTEGER, true, IqType.integer(family, java.util.Objects.requireNonNull(type).lo(), type.hi(),
                        false));
    }

    /**
     * MEDIAN of a NUMERIC(p, s) of at most 15 digits or a DOUBLE (IQ island, 2026-10-15, checklist 2.16, measured on IQ
     * 17.1): the midpoint of the two middle values -- one for an odd count --, of the input's type: a NUMERIC's cut
     * toward zero at scale s (1.25 and 2.50 give 1.87, -3.75 and 0.00 give -1.87), a DOUBLE's (lo + hi) / 2 (two values
     * of 1e308 give Infinity there). The middle values are the discrete percentiles at 0.5, ascending and descending
     * (exact values, as doubles: a NUMERIC of 15 digits is read back exactly); the midpoint is decimal arithmetic.
     */
    private static SqlToPure.Aggregation middles(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x, IqType type,
            ValueSpecification values) {
        boolean decimal = x.kind() == Kind.DECIMAL;
        int scale = decimal ? scale(type, f) : 0;
        Tx asDouble = SqlToPure.strict(Kind.FLOAT, List.of(x), v -> decimal ? SqlToPure.call("toFloat", v.get(0))
                : v.get(0));
        List<ColSpec> specs = new ArrayList<>();
        List<ValueSpecification> middle = new ArrayList<>();
        for (boolean ascending : new boolean[] {true, false}) {
            String raw = at.fresh();
            specs.add(new ColSpec(raw, at.overRow(SqlToPure.valueOf(asDouble)), at.overGroup(SqlToPure.call(
                    "percentile", values, new CFloat(0.5), new com.legend.protocol.spec.CBoolean(ascending),
                    new com.legend.protocol.spec.CBoolean(false))), null));
            middle.add(at.column(raw));
        }
        Tx lo = Tx.value(middle.get(0), Kind.FLOAT, true);
        Tx hi = Tx.value(middle.get(1), Kind.FLOAT, true);
        Tx mid = SqlToPure.strict(decimal ? Kind.DECIMAL : Kind.FLOAT, List.of(lo, hi), v -> {
            if (!decimal) {
                return SqlToPure.call("divide", AppliedFunction.infixRun("plus", List.of(v.get(0), v.get(1))),
                        new CFloat(2.0));
            }
            ValueSpecification sum = AppliedFunction.infixRun("plus", List.of(decimalAt(v.get(0), scale),
                    decimalAt(v.get(1), scale)));
            return IqFunctions.atScale(IqFunctions.places(AppliedFunction.infixRun("times", List.of(sum,
                    new CDecimal(new java.math.BigDecimal("0.5")))), scale, false), scale);
        });
        return new SqlToPure.Aggregation(specs, new ColSpec(at.name(), at.overRow(mid.vs()), null, null), x.kind(),
                true, type);
    }

    /**
     * {@code PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x [DESC])} and {@code PERCENTILE_DISC} (IQ island, 2026-10-15,
     * checklist 2.16, measured on IQ 17.1): the continuous percentile p of the group's values in that order, the two
     * values around it interpolated linearly, a DOUBLE; the discrete one, the first value whose cumulative share
     * reaches p, of the value's own type (an INTEGER's an INTEGER). Of numbers only; p a constant from 0 to 1.
     * Refused by name: a BIGINT or a NUMERIC of more than 15 digits to PERCENTILE_DISC (its value comes back as a
     * DOUBLE, exact only to 15 digits), DISTINCT.
     */
    private static SqlToPure.Aggregation percentile(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            @com.legend.base.Nullable IqType type, ValueSpecification values) {
        String name = f.name().get(0).text().toUpperCase(java.util.Locale.ROOT);
        boolean continuous = IqLexer.lower(f.name().get(0).text()).equals("percentile_cont");
        boolean descending = f.withinGroup().get(0).descending();
        java.math.BigDecimal p = f.args().get(0) instanceof SqlAst.NumericLit n ? n.value()
                : f.args().get(0) instanceof SqlAst.IntegerLit i ? new java.math.BigDecimal(i.value()) : null;
        if (p == null || p.signum() < 0 || p.compareTo(java.math.BigDecimal.ONE) > 0) {
            throw SqlTranslationException.unsupported(name + " of a fraction that is no constant from 0 to 1 is not"
                    + " supported", f.args().get(0).pos());
        }
        if (f.distinct()) {
            throw SqlTranslationException.unsupported(name + "(DISTINCT ...) is not supported", f.pos());
        }
        if (!(x.kind() == Kind.INTEGER || x.kind() == Kind.DECIMAL || x.kind() == Kind.FLOAT) || type == null) {
            throw SqlTranslationException.unsupported(name + " of a value that is no number is not supported yet",
                    f.pos());
        }
        boolean decimal = x.kind() == Kind.DECIMAL;
        int digits = decimal ? type.size() : 0;
        if (!continuous && (x.kind() == Kind.INTEGER && type.family().equals("BIGINT") || decimal && digits > 15)) {
            throw SqlTranslationException.unsupported(name + " of a " + type.spelling() + " is not supported yet: its"
                    + " value comes back as a DOUBLE, exact only to 15 digits", f.pos());
        }
        String raw = at.fresh();
        Tx asDouble = SqlToPure.strict(Kind.FLOAT, List.of(x), v -> x.kind() == Kind.FLOAT ? v.get(0)
                : SqlToPure.call("toFloat", v.get(0)));
        ColSpec spec = new ColSpec(raw, at.overRow(SqlToPure.valueOf(asDouble)), at.overGroup(SqlToPure.call(
                "percentile", values, new CFloat(p.doubleValue()), new com.legend.protocol.spec.CBoolean(!descending),
                new com.legend.protocol.spec.CBoolean(continuous))), null);
        Tx value = Tx.value(at.column(raw), Kind.FLOAT, true);
        if (continuous || x.kind() == Kind.FLOAT) {
            // Pure's percentile is a Number: the DOUBLE it is
            Tx asFloat = SqlToPure.strict(Kind.FLOAT, List.of(value), v -> SqlToPure.call("toFloat", v.get(0)));
            return new SqlToPure.Aggregation(List.of(spec), new ColSpec(at.name(), at.overRow(asFloat.vs()), null,
                    null), Kind.FLOAT, true, new IqType("DOUBLE"));
        }
        int scale = decimal ? scale(type, f) : 0;
        Tx back = SqlToPure.strict(decimal ? Kind.DECIMAL : Kind.INTEGER, List.of(value), v -> decimal
                ? decimalAt(v.get(0), scale) : SqlToPure.call("round", v.get(0)));
        return new SqlToPure.Aggregation(List.of(spec), new ColSpec(at.name(), at.overRow(back.vs()), null, null),
                x.kind(), true, type);
    }

    /** {@code d}, a DOUBLE holding a decimal of at most 15 digits and {@code scale} places, as that decimal: d times
     *  10^scale rounded to the whole number it is within a rounding of, then scaled back. */
    private static ValueSpecification decimalAt(ValueSpecification d, int scale) {
        ValueSpecification whole = SqlToPure.call("toDecimal", SqlToPure.call("round", scale == 0 ? d
                : AppliedFunction.infixRun("times", List.of(d, new CFloat(Math.pow(10, scale))))));
        return scale == 0 ? whole : IqFunctions.atScale(AppliedFunction.infixRun("times", List.of(whole,
                new CDecimal(java.math.BigDecimal.ONE.movePointLeft(scale)))), scale);
    }

    /** A NUMERIC's scale, by its IQ type. */
    private static int scale(IqType type, SqlAst.FunctionCall f) {
        String sp = type.spelling();
        int comma = sp.indexOf(',');
        if (type.size() < 0) {
            throw SqlTranslationException.unsupported("an aggregate of a NUMERIC whose precision is not known is not"
                    + " supported", f.pos());
        }
        if (type.size() > 15) {
            throw SqlTranslationException.unsupported(f.name().get(0).text().toUpperCase(java.util.Locale.ROOT) + " of a "
                    + sp + " is not supported yet: its middle values come back as DOUBLEs, exact only to 15 digits",
                    f.pos());
        }
        return comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
    }

    /**
     * LIST({@code x} [, delimiter] ORDER BY {@code key} [DESC]) under IQ's engine, key a column other than x (IQ island,
     * 2026-10-15, checklist 2.16, measured on IQ 17.1): the values in the key's order, IQ's NULL first ascending (last
     * descending), each as LIST writes it (a string without its trailing spaces, '' an item, a NULL skipped), NULL for
     * none. Read as two lists joined by the delimiter: the rows whose key is NULL, then (or, descending, after) the others
     * by Pure's ordered joinStrings, which sorts a NULL last. Rows of one key are listed in the plan's order, as IQ lists
     * them in its. Refused by name: more than one key, a key that is no column, a string key whose values may share a
     * collation key (their order is the plan's), a value that is no string or integer.
     */
    private static SqlToPure.Aggregation listByKey(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            String delimiter, IqDialect dialect) {
        SqlAst.SortItem order = f.orderBy().get(0);
        String key = order.expr() instanceof SqlAst.ColumnRef ref ? at.inputColumn(ref) : null;
        if (f.orderBy().size() != 1 || key == null) {
            throw SqlTranslationException.unsupported("LIST ordered by more than one key, or by one that is no column,"
                    + " is not supported yet", order.pos());
        }
        Tx k = at.tx(order.expr());
        if (k.kind() == Kind.STRING && !dialect.keys().own(order.expr())) {
            throw SqlTranslationException.unsupported("LIST ordered by a string whose values may share a collation key"
                    + " is not supported yet: the order of their rows is the plan's", order.pos());
        }
        boolean strings = x.kind() == Kind.STRING;
        if (!strings && !(x.kind() == Kind.INTEGER && x.detail() instanceof IqType t && t.isInteger())) {
            throw SqlTranslationException.unsupported("LIST of values of this type is not supported yet: IQ spells them"
                    + " by its own rules", f.args().get(0).pos());
        }
        Tx text = SqlToPure.strict(Kind.STRING, List.of(x), v -> strings ? IqFunctions.trailingSpacesDropped(v.get(0))
                : SqlToPure.call("toString", v.get(0)));
        ValueSpecification keyNull = SqlToPure.call("isEmpty", SqlToPure.valueOf(k));
        ValueSpecification nulls = IqDialect.ifThen(keyNull, SqlToPure.valueOf(text), SqlToPure.empty());
        ValueSpecification others = IqDialect.ifThen(keyNull, SqlToPure.empty(), SqlToPure.valueOf(text));
        String first = at.fresh();
        String rest = at.fresh();
        ColSpec byNull = new ColSpec(first, at.overGroup(SqlToPure.call("joinStrings", at.values(), at.overRow(nulls),
                new CString(delimiter))), null, null);
        ColSpec byKey = new ColSpec(rest, at.overGroup(SqlToPure.call("joinStrings", at.values(), at.overRow(others),
                new CString(delimiter), SqlToPure.call(order.descending() ? "descending" : "ascending",
                        new ColSpec(key, null, null, null)))), null, null);
        Tx lead = Tx.value(at.column(order.descending() ? rest : first), Kind.STRING, true);
        Tx trail = Tx.value(at.column(order.descending() ? first : rest), Kind.STRING, true);
        ValueSpecification both = AppliedFunction.infixRun("plus", List.of(SqlToPure.call("toOne", lead.vs()),
                new CString(delimiter), SqlToPure.call("toOne", trail.vs())));
        ValueSpecification listed = IqDialect.ifThen(SqlToPure.call("isEmpty", lead.vs()), trail.vs(), IqDialect.ifThen(
                SqlToPure.call("isEmpty", trail.vs()), lead.vs(), both));
        return new SqlToPure.Aggregation(List.of(byNull, byKey), new ColSpec(at.name(), at.overRow(listed), null, null),
                Kind.STRING, true, new IqType("LONG VARCHAR"));
    }

    /** LIST({@code x} [, delimiter] ORDER BY {@code x}) of an integer column ({@link IqAggregates}): Pure's ordered
     *  joinStrings of the values' digits, NULLs skipped, '' over none. */
    private static SqlToPure.Aggregation list(SqlAst.FunctionCall f, SqlToPure.Grouping at, Tx x,
            @com.legend.base.Nullable IqType type, IqDialect dialect) {
        SqlAst.Expr value = f.args().get(0);
        // IQ island, 2026-10-14 (the third landing, measured): a LIST of a GROUP BY key lists one value, as often as
        // its group has rows (once under DISTINCT), whatever the order; DISTINCT of anything else is refused (lite's
        // Postgres orders a DISTINCT aggregate only by its argument, here the value's text)
        boolean keyed = at.groupKey(value);
        if (f.distinct() && !keyed) {
            throw SqlTranslationException.unsupported("LIST(DISTINCT ...) is not supported yet but of a GROUP BY key:"
                    + " lite's Postgres orders a DISTINCT list only by its text", f.pos());
        }
        String delimiter = ",";
        if (f.args().size() == 2) {
            SqlAst.Expr d = f.args().get(1);
            if (d instanceof SqlAst.StringLit s) {
                delimiter = s.value();
            } else if (d instanceof SqlAst.NullLit) {
                delimiter = "";
            } else {
                throw SqlTranslationException.unsupported("a LIST delimiter other than a string constant or NULL is"
                        + " not supported (IQ's error -156 where the group holds two values)", d.pos());
            }
        }
        // IQ island, 2026-10-15 (the user's rule: any answer IQ could give is right): without ORDER BY the order of a
        // LIST's values is the plan's, IQ's as the database's -- listed in the database's order
        if (keyed) {
            // IQ island, 2026-10-14 (review 7's M2 and m1, measured on SQL Anywhere 17): a keyed LIST's ORDER BY orders
            // one value, but IQ reads it -- its errors (an aggregate inside is -156), and its keys evaluated for each
            // row (v + 1 over INTEGER's largest is -158), which lite does not compute: translated for its errors, and
            // refused where evaluating it could raise
            for (SqlAst.SortItem o : f.orderBy()) {
                if (!aggregateCalls(o.expr()).isEmpty()) {
                    throw SqlTranslationException.invalid("an aggregate inside a LIST's ORDER BY is IQ's error"
                            + IqDialect.code(-156, "42W08"), o.pos());
                }
                at.tx(o.expr());
                if (!IqParser.cannotRaise(o.expr())) {
                    throw SqlTranslationException.unsupported("a LIST of a GROUP BY key ordered by an expression that"
                            + " can raise an error is not supported yet: IQ evaluates it for each row, where lite's"
                            + " list of one value needs no order", o.pos());
                }
            }
            if (f.distinct() && !f.orderBy().isEmpty() && dialect.ordered()) {
                // measured: with LIST(DISTINCT k ORDER BY k) SQL Anywhere returns the rows of SELECT k, ... GROUP BY k
                // ORDER BY 1 out of that order (its plan takes them as sorted by the LIST's sort)
                throw SqlTranslationException.unsupported("LIST(DISTINCT ... ORDER BY ...) of a GROUP BY key in a"
                        + " statement with an ORDER BY is not supported: SQL Anywhere may then return the rows out of"
                        + " the ORDER BY's order", f.pos());
            }
        }
        SqlAst.SortItem order = keyed || f.orderBy().isEmpty() ? null : f.orderBy().get(0);
        String column = value instanceof SqlAst.ColumnRef ref ? at.inputColumn(ref) : null;
        if (order != null && dialect.engine() == IqDialect.Engine.IQ && !(column != null && f.orderBy().size() == 1
                && at.same(order.expr(), value))) {
            return listByKey(f, at, x, delimiter, dialect);
        }
        if (order != null && (f.orderBy().size() != 1 || column == null || !at.same(order.expr(), value))) {
            throw SqlTranslationException.unsupported("LIST ordered by anything but its value, a column, is not"
                    + " supported yet: the order of tied rows is the plan's", order.pos());
        }
        boolean strings = x.kind() == Kind.STRING;
        // IQ island, 2026-10-17 (the differential fuzzer's fifth round; measured on IQ 17.1's UTF8BIN database): under
        // IQ's engine in a case-sensitive database the values of one collation key differ only in trailing spaces,
        // which the list drops, so their order is not seen; ordered by the values' bytes, as IQ's blank-padded order
        // is but for a character below a space (a tab, a line break), whose row is refused at run time
        boolean byBytes = strings && order != null && !dialect.keys().own(value)
                && dialect.engine() == IqDialect.Engine.IQ && dialect.settings().caseSensitive()
                && !dialect.settings().latin1();
        if (strings && order != null && !dialect.keys().own(value) && !byBytes) {
            // IQ island, 2026-10-14 (the merge): the order of values of one collation key is the plan's
            throw SqlTranslationException.unsupported("LIST of strings is supported only where every value is its own"
                    + " collation key (a derived table's RTRIM, TRIM or constant" + (dialect.settings().caseSensitive()
                    ? "" : ", LOWER") + "): IQ orders " + (dialect.settings().caseSensitive() ? "'a' and 'a '"
                    : "'A' and 'a'") + " as one value, in its plan's order", value.pos());
        }
        if (!strings && (x.kind() != Kind.INTEGER || type == null || !(type.isInteger()
                || type.family().equals("TINYINT")))) {
            throw SqlTranslationException.unsupported("LIST of values of this type is not supported yet: IQ spells them"
                    + " by its own rules", value.pos());
        }
        String raw = at.fresh();
        // a string as itself, '' skipped as a NULL is (measured: LIST of '', NULL is ''; a blank is kept); an integer
        // by its digits. IQ island, 2026-10-15 (measured on IQ 17.1): under IQ's engine each string without its
        // trailing spaces ('x  ' is listed 'x', 'a \t' as it is), '' kept as an item, and no values at all NULL
        boolean iqEngine = dialect.engine() == IqDialect.Engine.IQ;
        Tx digits = SqlToPure.strict(Kind.STRING, List.of(x), v -> !strings ? SqlToPure.call("toString", v.get(0))
                : byBytes ? IqFunctions.refusedPast(SqlToPure.call("or", SqlToPure.call("or", SqlToPure.call(
                        "contains", v.get(0), new CString("\t")), SqlToPure.call("contains", v.get(0), new CString(
                        "\n"))), SqlToPure.call("contains", v.get(0), new CString("\r"))), IqDialect.REFUSED_AT_RUN_TIME
                        + "LIST of a string holding a tab or a line break: IQ orders it by blank padding",
                        IqFunctions.trailingSpacesDropped(v.get(0)), SqlToPure.empty())
                : iqEngine ? IqFunctions.trailingSpacesDropped(v.get(0)) : IqDialect.ifThen(SqlToPure.call("equal",
                v.get(0), new CString("")), SqlToPure.nullOf(Kind.STRING).vs(), v.get(0)));
        if (f.distinct()) {
            // a GROUP BY key's one value (an integer: of strings one key may hold several spellings), as text
            if (strings) {
                throw SqlTranslationException.unsupported("LIST(DISTINCT ...) of strings is not supported yet: one key"
                        + " of IQ's collation may hold several spellings", f.pos());
            }
            ColSpec one = new ColSpec(raw, at.overRow(SqlToPure.valueOf(x)), at.overGroup(SqlToPure.call("max",
                    at.values())), null);
            ValueSpecification kept = at.column(raw);
            ValueSpecification text = IqDialect.ifThen(SqlToPure.call("isEmpty", kept), iqEngine
                    ? SqlToPure.nullOf(Kind.STRING).vs() : new CString(""), SqlToPure.call("toString",
                    SqlToPure.call("toOne", kept)));
            return new SqlToPure.Aggregation(List.of(one), new ColSpec(at.name(), at.overRow(text), null, null),
                    Kind.STRING, iqEngine, new IqType("LONG VARCHAR"));
        }
        ColSpec spec = order == null ? new ColSpec(raw, at.overGroup(SqlToPure.call("joinStrings", at.values(),
                at.overRow(SqlToPure.valueOf(digits)), new CString(delimiter))), null, null)
                : new ColSpec(raw, at.overGroup(SqlToPure.call("joinStrings", at.values(), at.overRow(
                        SqlToPure.valueOf(digits)), new CString(delimiter), SqlToPure.call(order.descending()
                        ? "descending" : "ascending", new ColSpec(java.util.Objects.requireNonNull(column), null, null,
                        null)))), null, null);
        ValueSpecification listed = at.column(raw);
        if (iqEngine) {
            return new SqlToPure.Aggregation(List.of(spec), new ColSpec(at.name(), at.overRow(listed), null, null),
                    Kind.STRING, true, new IqType("LONG VARCHAR"));
        }
        ValueSpecification finish = IqDialect.ifThen(SqlToPure.call("isEmpty", listed), new CString(""),
                SqlToPure.call("toOne", listed));
        return new SqlToPure.Aggregation(List.of(spec), new ColSpec(at.name(), at.overRow(finish), null, null),
                Kind.STRING, false, new IqType("LONG VARCHAR"));
    }
}
