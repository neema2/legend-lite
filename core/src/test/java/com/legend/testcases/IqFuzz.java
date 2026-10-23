// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

/**
 * Random IQ queries over the parity schema (the IQ scale program, docs/IQ_SCALE_PROGRAM_2026_10_22.md P2.3): our own
 * generator, written from the grammar the island reads, every choice drawn from one seed so a finding replays. It
 * writes queries of every shape the island answers -- joins of every kind, derived tables, common table expressions,
 * grouping, windows, scalar and quantified subqueries, set operations, row limits -- typed so that most of them are
 * queries IQ answers, and a {@link Shape} beside each, its FROM, select list and WHERE apart, for the metamorphic checks
 * (IqFuzzTest). No answer is known here: the judges are the island's own consistency and the databases'.
 */
public final class IqFuzz {

    /** A value's kind, as the generator types expressions. */
    public enum Kind { INT, DEC, DBL, STR, DATE, TS }

    /** A column in scope: its qualifier, name and kind. */
    record Col(String qualifier, String name, Kind kind) {
        String ref() {
            return qualifier + "." + name;
        }
    }

    /** A query in parts: an optional WITH prefix, the select list, the FROM clause and the WHERE condition (or null).
     *  {@link #sql} joins them; the checks recombine them. */
    public record Shape(String with, List<String> items, String from, @com.legend.base.Nullable String where,
            List<Col> scope) {

        public String sql() {
            return with + "SELECT " + String.join(", ", items) + " FROM " + from
                    + (where == null ? "" : " WHERE " + where);
        }

        /** The same query with {@code more} AND-ed to its WHERE. */
        public String where(String more) {
            return with + body(more);
        }

        /** {@link #where} without the WITH: a branch of a set operation the WITH begins. */
        private String body(String more) {
            return "SELECT " + String.join(", ", items) + " FROM " + from + " WHERE "
                    + (where == null ? more : "(" + where + ") AND " + more);
        }

        /** {@code A op B}, A this query where {@code p} and B this query where {@code q}, the WITH before both: the set
         *  identities' queries (IqFuzzTest). */
        public String set(String p, String op, String q) {
            return with + body("(" + p + ")") + " " + op + " " + body("(" + q + ")");
        }

        /** This query's distinct rows where {@code p}. */
        public String distinct(String p) {
            return with + "SELECT DISTINCT " + body("(" + p + ")").substring("SELECT ".length());
        }

        /** The three partitions of {@code p} as one query: a UNION ALL nesting a parenthesized UNION ALL (the IQ scale
         *  program's Q19: DuckDB 1.4.4 loses rows from a nested UNION ALL under parallel execution). */
        public String partitions(String p) {
            return with + "(" + body("(" + p + ")") + ") UNION ALL ((" + body("NOT (" + p + ")") + ") UNION ALL ("
                    + body("(" + p + ") IS UNKNOWN") + "))";
        }
    }

    private final Random rnd;
    private final List<SqlParity.Table> tables;
    private final int depth;
    private int names;

    /** A generator over {@code tables}, its queries' nesting at most {@code depth}. */
    public IqFuzz(long seed, List<SqlParity.Table> tables, int depth) {
        this.rnd = new Random(seed);
        this.tables = tables;
        this.depth = depth;
    }

    /** A shape over a FROM of one to three items, a select list of one to four items and, half the time, a WHERE. */
    public Shape shape() {
        names = 0;
        String with = "";
        List<Col> cte = null;
        String cteName = null;
        if (rnd.nextInt(5) == 0) {
            cteName = "w" + (names++);
            Derived d = derived(depth - 1, cteName);
            with = "WITH " + cteName + " AS (" + d.sql + ") ";
            cte = d.cols;
        }
        List<Col> scope = new ArrayList<>();
        String from = from(depth, scope, cteName, cte, true);
        int n = 1 + rnd.nextInt(4);
        List<String> items = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            items.add((rnd.nextInt(8) == 0 ? scalar(pick(List.of(Kind.INT, Kind.STR, Kind.DATE)), scope, depth)
                    : expr(pick(Kind.values()), scope, depth)) + " AS c" + i);
        }
        String where = rnd.nextBoolean() ? predicate(scope, depth) : null;
        return new Shape(with, items, from, where, scope);
    }

    /** A predicate over {@code scope}, for the checks' partitions. */
    public String predicate(List<Col> scope) {
        return predicate(scope, depth);
    }

    /** A query of the island's richer forms, checked for crashes and database errors only (its answer has no second
     *  spelling here): grouping with HAVING, windows, TOP with START AT over a unique order, UNION, DISTINCT. */
    public String complex() {
        Shape s = shape();
        return switch (rnd.nextInt(7)) {
            case 0 -> grouped(s);
            case 1 -> s.with() + "SELECT DISTINCT " + String.join(", ", s.items()) + " FROM " + s.from()
                    + (s.where() == null ? "" : " WHERE " + s.where());
            // a set operation of two branches of the same kinds: the select list over the FROM, twice, the WITH
            // before the whole statement
            case 2 -> s.with() + "SELECT " + String.join(", ", s.items()) + " FROM " + s.from()
                    + (s.where() == null ? "" : " WHERE " + s.where()) + " UNION" + (rnd.nextBoolean() ? " ALL " : " ")
                    + "SELECT " + String.join(", ", s.items()) + " FROM " + s.from() + " WHERE "
                    + predicate(s.scope(), depth);
            case 3 -> s.with() + "SELECT TOP " + (1 + rnd.nextInt(5)) + (rnd.nextBoolean() ? "" : " START AT "
                    + (1 + rnd.nextInt(3))) + " " + String.join(", ", s.items()) + " FROM " + s.from()
                    + (s.where() == null ? "" : " WHERE " + s.where()) + " ORDER BY "
                    + String.join(", ", positions(s.items().size()));
            case 4 -> windowed(s);
            case 5 -> nestedSet(s);
            default -> s.sql();
        };
    }

    // ------------------------------------------------------------------ the parts

    private record Derived(String sql, List<Col> cols) {
    }

    /** A derived table's query, its columns named {@code c0..}: plain, grouped or windowed. */
    private Derived derived(int d, String qualifier) {
        List<Col> scope = new ArrayList<>();
        int mode = rnd.nextInt(3);
        // a window's FROM joins inner only: IQ reads an outer join that a NULL-rejecting predicate outside makes inner
        // before a window there numbers its rows (measured; IqDialect), so a partition's predicate may rightly change
        // the window's rows
        String from = from(Math.max(d, 0), scope, null, null, mode != 2);
        List<Col> cols = new ArrayList<>();
        List<String> items = new ArrayList<>();
        if (mode == 1) {
            // grouped: keys first, then aggregates
            Col key = pick(scope);
            items.add(key.ref() + " AS c0");
            cols.add(new Col(qualifier, "c0", key.kind()));
            int aggs = 1 + rnd.nextInt(3);
            for (int i = 0; i < aggs; i++) {
                Kind k = pick(List.of(Kind.INT, Kind.INT, Kind.DEC, Kind.STR, Kind.DATE));
                String a = aggregate(k, scope, d);
                Kind result = a.startsWith("COUNT") ? Kind.INT : k;
                items.add(a + " AS c" + (i + 1));
                cols.add(new Col(qualifier, "c" + (i + 1), result));
            }
            String sql = "SELECT " + String.join(", ", items) + " FROM " + from
                    + (rnd.nextBoolean() ? " WHERE " + predicate(scope, d) : "") + " GROUP BY " + key.ref()
                    + (rnd.nextInt(3) == 0 ? " HAVING COUNT(*) > " + rnd.nextInt(3) : "");
            return new Derived(sql, cols);
        }
        int n = 1 + rnd.nextInt(3);
        for (int i = 0; i < n; i++) {
            // the first column an integer: a join's key, as every table offers one
            Kind k = i == 0 ? Kind.INT : pick(Kind.values());
            items.add(expr(k, scope, d) + " AS c" + i);
            cols.add(new Col(qualifier, "c" + i, k));
        }
        if (mode == 2) {
            // a window whose answer does not depend on how ties are ordered
            Col part = pick(scope);
            Col order = pick(scope, Kind.INT);
            String w = switch (rnd.nextInt(4)) {
                case 0 -> "RANK() OVER (PARTITION BY " + part.ref() + " ORDER BY " + order.ref() + ")";
                case 1 -> "DENSE_RANK() OVER (ORDER BY " + order.ref() + ")";
                case 2 -> "COUNT(*) OVER (PARTITION BY " + part.ref() + ")";
                default -> "SUM(" + order.ref() + ") OVER (PARTITION BY " + part.ref() + ")";
            };
            items.add(w + " AS c" + n);
            cols.add(new Col(qualifier, "c" + n, Kind.INT));
        }
        String sql = "SELECT " + String.join(", ", items) + " FROM " + from
                + (rnd.nextBoolean() ? " WHERE " + predicate(scope, d) : "");
        return new Derived(sql, cols);
    }

    /** A FROM clause of one to three items, joined; their columns added to {@code scope}. */
    private String from(int d, List<Col> scope, @com.legend.base.Nullable String cteName,
            @com.legend.base.Nullable List<Col> cte, boolean outer) {
        StringBuilder sb = new StringBuilder(item(d, scope, cteName, cte));
        int joins = d <= 0 ? rnd.nextInt(2) : rnd.nextInt(3);
        for (int i = 0; i < joins; i++) {
            List<Col> right = new ArrayList<>();
            String item = item(d, right, cteName, cte);
            int kind = outer ? rnd.nextInt(6) : rnd.nextBoolean() ? 0 : 5;
            if (kind == 5) {
                sb.append(" CROSS JOIN ").append(item);
            } else {
                String join = switch (kind) {
                    case 0, 1 -> " JOIN ";
                    case 2, 3 -> " LEFT OUTER JOIN ";
                    case 4 -> rnd.nextBoolean() ? " RIGHT OUTER JOIN " : " FULL OUTER JOIN ";
                    default -> throw new IllegalStateException();
                };
                Col l = pick(scope, Kind.INT);
                Col r = pick(right, Kind.INT);
                String on = l.ref() + " = " + r.ref();
                if (rnd.nextInt(3) == 0) {
                    List<Col> both = new ArrayList<>(scope);
                    both.addAll(right);
                    on += " AND " + simplePredicate(both);
                }
                sb.append(join).append(item).append(" ON ").append(on);
            }
            scope.addAll(right);
        }
        return sb.toString();
    }

    /** A table, a derived table or the common table expression, under a fresh correlation name. */
    private String item(int d, List<Col> scope, @com.legend.base.Nullable String cteName,
            @com.legend.base.Nullable List<Col> cte) {
        String q = "x" + (names++);
        if (cte != null && rnd.nextInt(3) == 0) {
            for (Col c : cte) {
                scope.add(new Col(q, c.name(), c.kind()));
            }
            return cteName + " " + q;
        }
        if (d > 0 && rnd.nextInt(3) == 0) {
            Derived dt = derived(d - 1, q);
            scope.addAll(dt.cols());
            return "(" + dt.sql() + ") " + q;
        }
        SqlParity.Table t = pick(usable());
        for (SqlParity.Column c : t.columns()) {
            Kind k = kind(c.type());
            if (k != null) {
                scope.add(new Col(q, c.name(), k));
            }
        }
        return t.name() + " " + q;
    }

    private List<SqlParity.Table> usable() {
        List<SqlParity.Table> out = new ArrayList<>();
        for (SqlParity.Table t : tables) {
            if (t.columns().stream().anyMatch(c -> kind(c.type()) == Kind.INT)) {
                out.add(t);
            }
        }
        return out;
    }

    /** The kind of an IQ type, or null for the types the generator leaves alone (CHAR's padding, REAL's single
     *  precision, BIT: each judged on its own in the parity cases). */
    static @com.legend.base.Nullable Kind kind(String iqType) {
        String t = iqType.toLowerCase(Locale.ROOT);
        if (t.equals("integer") || t.equals("int") || t.equals("smallint")) {
            return Kind.INT;
        }
        if (t.startsWith("numeric") || t.startsWith("decimal")) {
            return Kind.DEC;
        }
        if (t.equals("double")) {
            return Kind.DBL;
        }
        if (t.startsWith("varchar")) {
            return Kind.STR;
        }
        if (t.equals("date")) {
            return Kind.DATE;
        }
        if (t.equals("timestamp")) {
            return Kind.TS;
        }
        return null;
    }

    // ------------------------------------------------------------------ expressions

    private String expr(Kind k, List<Col> scope, int d) {
        Col c = column(scope, k);
        if (d <= 0 || rnd.nextInt(3) == 0) {
            return c != null && rnd.nextInt(4) != 0 ? c.ref() : literal(k);
        }
        int d1 = d - 1;
        return switch (k) {
            case INT -> switch (rnd.nextInt(14)) {
                case 0 -> "(" + expr(Kind.INT, scope, d1) + " + " + expr(Kind.INT, scope, d1) + ")";
                case 1 -> "(" + expr(Kind.INT, scope, d1) + " - " + expr(Kind.INT, scope, d1) + ")";
                case 2 -> "(" + expr(Kind.INT, scope, d1) + " * " + (1 + rnd.nextInt(4)) + ")";
                case 3 -> "ABS(" + expr(Kind.INT, scope, d1) + ")";
                case 4 -> "COALESCE(" + expr(Kind.INT, scope, d1) + ", " + literal(Kind.INT) + ")";
                case 5 -> "ISNULL(" + expr(Kind.INT, scope, d1) + ", " + literal(Kind.INT) + ")";
                case 6 -> "CASE WHEN " + plainPredicate(scope, d1) + " THEN " + expr(Kind.INT, scope, d1) + " ELSE "
                        + expr(Kind.INT, scope, d1) + " END";
                case 7 -> "LENGTH(" + expr(Kind.STR, scope, d1) + ")";
                case 8 -> pick(List.of("YEAR(", "MONTH(", "DAY(")) + expr(Kind.DATE, scope, d1) + ")";
                case 9 -> "DATEDIFF(day, " + expr(Kind.DATE, scope, d1) + ", " + expr(Kind.DATE, scope, d1) + ")";
                case 10 -> "(" + expr(Kind.INT, scope, d1) + " % " + (2 + rnd.nextInt(5)) + ")";
                case 11 -> "(" + expr(Kind.INT, scope, d1) + " / " + (2 + rnd.nextInt(5)) + ")";
                case 12 -> "SIGN(" + expr(Kind.INT, scope, d1) + ")";
                default -> "CAST(" + expr(Kind.DEC, scope, d1) + " AS INTEGER)";
            };
            case DEC -> switch (rnd.nextInt(6)) {
                case 0 -> "(" + expr(Kind.DEC, scope, d1) + " + " + expr(Kind.DEC, scope, d1) + ")";
                case 1 -> "(" + expr(Kind.DEC, scope, d1) + " * " + (1 + rnd.nextInt(3)) + ")";
                case 2 -> "ROUND(" + expr(Kind.DEC, scope, d1) + ", 1)";
                case 3 -> "COALESCE(" + expr(Kind.DEC, scope, d1) + ", " + literal(Kind.DEC) + ")";
                case 4 -> "CAST(" + expr(Kind.INT, scope, d1) + " AS NUMERIC(10,2))";
                default -> "ABS(" + expr(Kind.DEC, scope, d1) + ")";
            };
            case DBL -> switch (rnd.nextInt(3)) {
                case 0 -> "(" + expr(Kind.DBL, scope, d1) + " + " + expr(Kind.DBL, scope, d1) + ")";
                case 1 -> "CAST(" + expr(Kind.INT, scope, d1) + " AS DOUBLE)";
                default -> "COALESCE(" + expr(Kind.DBL, scope, d1) + ", " + literal(Kind.DBL) + ")";
            };
            case STR -> switch (rnd.nextInt(12)) {
                case 0 -> "UPPER(" + expr(Kind.STR, scope, d1) + ")";
                case 1 -> "LOWER(" + expr(Kind.STR, scope, d1) + ")";
                case 2 -> "LEFT(" + expr(Kind.STR, scope, d1) + ", " + (1 + rnd.nextInt(3)) + ")";
                case 3 -> "RIGHT(" + expr(Kind.STR, scope, d1) + ", " + (1 + rnd.nextInt(3)) + ")";
                case 4 -> "SUBSTRING(" + expr(Kind.STR, scope, d1) + ", " + (1 + rnd.nextInt(3)) + ", "
                        + (1 + rnd.nextInt(3)) + ")";
                case 5 -> "TRIM(" + expr(Kind.STR, scope, d1) + ")";
                case 6 -> "(" + expr(Kind.STR, scope, d1) + " || " + expr(Kind.STR, scope, d1) + ")";
                case 7 -> "COALESCE(" + expr(Kind.STR, scope, d1) + ", " + literal(Kind.STR) + ")";
                case 8 -> "REPLACE(" + expr(Kind.STR, scope, d1) + ", 'a', 'q')";
                case 9 -> "CAST(" + expr(Kind.INT, scope, d1) + " AS VARCHAR(12))";
                case 10 -> "IF " + plainPredicate(scope, d1) + " THEN " + expr(Kind.STR, scope, d1) + " ELSE "
                        + expr(Kind.STR, scope, d1) + " ENDIF";
                default -> "CASE WHEN " + plainPredicate(scope, d1) + " THEN " + expr(Kind.STR, scope, d1) + " ELSE "
                        + expr(Kind.STR, scope, d1) + " END";
            };
            case DATE -> switch (rnd.nextInt(3)) {
                case 0 -> "DATEADD(day, " + (rnd.nextInt(61) - 30) + ", " + expr(Kind.DATE, scope, d1) + ")";
                case 1 -> "COALESCE(" + expr(Kind.DATE, scope, d1) + ", " + literal(Kind.DATE) + ")";
                default -> "CAST(" + expr(Kind.TS, scope, d1) + " AS DATE)";
            };
            case TS -> rnd.nextBoolean() ? "DATEADD(day, " + (rnd.nextInt(49) - 24) + ", " + expr(Kind.TS, scope, d1)
                    + ")" : "COALESCE(" + expr(Kind.TS, scope, d1) + ", " + literal(Kind.TS) + ")";
        };
    }

    /** A correlated scalar subquery of {@code k}'s kind: one row, an aggregate, over a table of its own. */
    private String scalar(Kind k, List<Col> scope, int d) {
        List<Col> inner = new ArrayList<>();
        String item = item(0, inner, null, null);
        Col agg = pick(inner, k);
        Col corr = pick(inner, Kind.INT);
        Col outer = pick(scope, Kind.INT);
        return "(SELECT MAX(" + agg.ref() + ") FROM " + item + " WHERE " + corr.ref() + " = " + outer.ref() + ")";
    }

    private String aggregate(Kind k, List<Col> scope, int d) {
        return switch (rnd.nextInt(5)) {
            case 0 -> "COUNT(*)";
            case 1 -> "COUNT(DISTINCT " + expr(k, scope, Math.min(d, 1)) + ")";
            case 2 -> k == Kind.INT || k == Kind.DEC ? "SUM(" + expr(k, scope, Math.min(d, 1)) + ")"
                    : "MIN(" + expr(k, scope, Math.min(d, 1)) + ")";
            case 3 -> "MIN(" + expr(k, scope, Math.min(d, 1)) + ")";
            default -> "MAX(" + expr(k, scope, Math.min(d, 1)) + ")";
        };
    }

    private String predicate(List<Col> scope, int d) {
        if (d <= 0 || rnd.nextInt(3) == 0) {
            return simplePredicate(scope);
        }
        int d1 = d - 1;
        return switch (rnd.nextInt(7)) {
            case 0 -> "(" + predicate(scope, d1) + " AND " + predicate(scope, d1) + ")";
            case 1 -> "(" + predicate(scope, d1) + " OR " + predicate(scope, d1) + ")";
            case 2 -> "NOT (" + predicate(scope, d1) + ")";
            case 3 -> {
                List<Col> inner = new ArrayList<>();
                String item = item(0, inner, null, null);
                yield (rnd.nextBoolean() ? "" : "NOT ") + "EXISTS (SELECT 1 FROM " + item + " WHERE "
                        + pick(inner, Kind.INT).ref() + " = " + pick(scope, Kind.INT).ref() + ")";
            }
            case 4 -> {
                List<Col> inner = new ArrayList<>();
                String item = item(0, inner, null, null);
                yield expr(Kind.INT, scope, d1) + (rnd.nextBoolean() ? " IN " : " NOT IN ") + "(SELECT "
                        + pick(inner, Kind.INT).ref() + " FROM " + item + ")";
            }
            default -> {
                Kind k = pick(Kind.values());
                String left = expr(k, scope, d1);
                String right = expr(k, scope, d1);
                // never an expression compared with itself: under IQ's engine that is two-valued outside a search
                // condition (IqDialect.selfComparison, measured), so a partition's IS UNKNOWN would rightly drop rows
                for (int tries = 0; right.equals(left) && tries < 3; tries++) {
                    right = expr(k, scope, d1);
                }
                yield left + " " + pick(List.of("=", "<>", "<", "<=", ">", ">=")) + " "
                        + (right.equals(left) ? literal(k) : right);
            }
        };
    }

    /** A predicate with no subquery: a CASE's or an IF's condition (a subquery inside a conditional is IQ's
     *  -1001030). */
    private String plainPredicate(List<Col> scope, int d) {
        if (d <= 0 || rnd.nextInt(3) == 0) {
            return simplePredicate(scope);
        }
        return switch (rnd.nextInt(4)) {
            case 0 -> "(" + plainPredicate(scope, d - 1) + " AND " + plainPredicate(scope, d - 1) + ")";
            case 1 -> "(" + plainPredicate(scope, d - 1) + " OR " + plainPredicate(scope, d - 1) + ")";
            case 2 -> "NOT (" + plainPredicate(scope, d - 1) + ")";
            default -> simplePredicate(scope);
        };
    }

    private String simplePredicate(List<Col> scope) {
        Col c = pick(scope);
        return switch (rnd.nextInt(6)) {
            case 0 -> c.ref() + (rnd.nextBoolean() ? " IS NULL" : " IS NOT NULL");
            case 1 -> c.ref() + " BETWEEN " + literal(c.kind()) + " AND " + literal(c.kind());
            case 2 -> c.ref() + (rnd.nextBoolean() ? " IN (" : " NOT IN (") + literal(c.kind()) + ", "
                    + literal(c.kind()) + ")";
            case 3 -> {
                Col s = column(scope, Kind.STR);
                yield s == null ? c.ref() + " IS NULL" : s.ref() + (rnd.nextBoolean() ? " LIKE 'a%'" : " LIKE '%b%'");
            }
            default -> c.ref() + " " + pick(List.of("=", "<>", "<", "<=", ">", ">=")) + " " + literal(c.kind());
        };
    }

    private String literal(Kind k) {
        return switch (k) {
            case INT -> Integer.toString(rnd.nextInt(24) - 4);
            case DEC -> pick(List.of("1.25", "0.50", "-3.75", "99.99", "0.00", "5.5"));
            case DBL -> pick(List.of("0.5e0", "1.5e0", "-2.25e0", "4.0e0"));
            case STR -> pick(List.of("'a'", "'b'", "'x'", "'abc'", "''", "'A'", "'w'"));
            case DATE -> "CAST('" + pick(List.of("2026-01-01", "2025-12-31", "2024-02-29", "2026-06-30")) + "' AS DATE)";
            case TS -> "CAST('" + pick(List.of("2026-01-01 10:00:00", "2025-12-31 23:59:59", "2024-02-29 00:00:00"))
                    + "' AS TIMESTAMP)";
        };
    }

    // ------------------------------------------------------------------ the richer forms

    private String grouped(Shape s) {
        Col key = pick(s.scope());
        String agg = aggregate(pick(List.of(Kind.INT, Kind.DEC, Kind.STR)), s.scope(), 1);
        return s.with() + "SELECT " + key.ref() + " AS k, " + agg + " AS a, COUNT(*) AS n FROM " + s.from()
                + (s.where() == null ? "" : " WHERE " + s.where()) + " GROUP BY " + key.ref()
                + (rnd.nextBoolean() ? " HAVING COUNT(*) >= 1" : "") + " ORDER BY 1, 2, 3";
    }

    private String windowed(Shape s) {
        Col part = pick(s.scope());
        Col order = pick(s.scope(), Kind.INT);
        return s.with() + "SELECT " + String.join(", ", s.items()) + ", RANK() OVER (PARTITION BY " + part.ref()
                + " ORDER BY " + order.ref() + ") AS rk, COUNT(*) OVER (PARTITION BY " + part.ref() + ") AS cnt FROM "
                + s.from() + (s.where() == null ? "" : " WHERE " + s.where());
    }

    /** Three branches of the select list over the FROM under two set operations of any kind, parenthesized on
     *  either side or not at all (INTERSECT binds tighter than UNION and EXCEPT), the WITH before the whole statement
     *  (so then never a parenthesis first). */
    private String nestedSet(Shape s) {
        String a = s.body(predicate(s.scope(), depth));
        String b = s.body(predicate(s.scope(), depth));
        String c = s.body(predicate(s.scope(), depth));
        String op1 = setOp();
        String op2 = setOp();
        return s.with() + switch (rnd.nextInt(s.with().isEmpty() ? 3 : 2)) {
            case 0 -> a + op1 + b + op2 + c;
            case 1 -> a + op1 + "(" + b + op2 + c + ")";
            default -> "(" + a + op1 + b + ")" + op2 + c;
        };
    }

    private String setOp() {
        return " " + pick(List.of("UNION", "UNION ALL", "UNION DISTINCT", "INTERSECT", "INTERSECT ALL", "EXCEPT",
                "EXCEPT ALL")) + " ";
    }

    private static List<String> positions(int n) {
        List<String> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            out.add(Integer.toString(i));
        }
        return out;
    }

    // ------------------------------------------------------------------ choices

    private @com.legend.base.Nullable Col column(List<Col> scope, Kind k) {
        List<Col> of = scope.stream().filter(c -> c.kind() == k).toList();
        return of.isEmpty() ? null : of.get(rnd.nextInt(of.size()));
    }

    /** A column of kind {@code k}, else any column ({@code k}'s expressions fall back to their literals). */
    private Col pick(List<Col> scope, Kind k) {
        Col c = column(scope, k);
        return c != null ? c : pick(scope);
    }

    private <T> T pick(List<T> xs) {
        return xs.get(rnd.nextInt(xs.size()));
    }

    private Kind pick(Kind[] kinds) {
        return kinds[rnd.nextInt(kinds.length)];
    }
}
