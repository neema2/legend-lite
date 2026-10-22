// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.TypeFact;

import java.util.List;
import java.util.Locale;

/**
 * The text of a MongoDB command as {@link MongoDb} writes it (docs/MONGODB_PLAN_2026_10_16.md §5.1, 2026-10-16): JSON
 * in MongoDB's Extended JSON spelling, written into the one {@link SqlWriter} like any dialect's SQL, each bound
 * parameter at its place. The helpers here spell the shapes every arm composes: a string literal (always
 * {@code $literal}, since a string beginning with {@code $} would read as a field path), an operator over its
 * arguments, a {@code $let} binding each argument once, and the null guard that gives an operator SQL's three-valued
 * answer (decision D7: a null operand answers null, which BSON's total comparisons never do).
 */
final class MongoJson {

    private MongoJson() {
    }

    /** {@code name} as a JSON string. */
    static String string(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    /** A document's field name, as a JSON key: a name MongoDB refuses as a field ({@code $}-led, holding a dot or a
     *  NUL) is refused here by name, never respelled. */
    static String key(String name) {
        if (name.isEmpty() || name.charAt(0) == '$' || name.indexOf('.') >= 0 || name.indexOf('\0') >= 0) {
            throw new DialectCapability("'" + name + "' is no MongoDB field name (a name that is empty, begins with"
                    + " $, or holds a dot or a NUL): a column or alias so named is refused, never respelled");
        }
        return string(name);
    }

    /** A field path, {@code "$a.b"}, from the parts a reference names. */
    static String path(String... parts) {
        return string("$" + String.join(".", parts));
    }

    /** A {@code $let} variable's name from an alias or a lambda parameter: MongoDB's rule (a lowercase ASCII letter
     *  first, then letters, digits and underscores), so an alias the lowering makes ({@code _tds0}) is prefixed and
     *  any other character replaced. */
    static String variable(String name) {
        StringBuilder sb = new StringBuilder("v_");
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            sb.append(Character.isLetterOrDigit(c) && c < 128 ? c : '_');
        }
        return sb.toString();
    }

    /** {@code {"$literal": "<text>"}}: a string value, never a path. */
    static SqlWriter literal(SqlWriter w, String text) {
        return w.append("{\"$literal\": ").append(string(text)).append("}");
    }

    /** {@code {"$op": [<args>]}}. */
    static SqlWriter op(SqlWriter w, String op, List<SqlExpr> args) {
        return w.append("{\"").append(op).append("\": [").list(args).append("]}");
    }

    /** {@code {"$op": <arg>}}: an operator of one argument. A literal array is written as the one element of an
     *  argument list ({@code {"$op": [[...]]}}), since the server reads a bare array as the list of arguments
     *  ({@code $reverseArray takes exactly 1 arguments. 3 were passed in}, the MongoDB lane's first run, 2026-10-17);
     *  the four numeric reducers read either form the same way and keep the bare one. */
    static SqlWriter op1(SqlWriter w, String op, SqlExpr arg) {
        boolean listed = arg instanceof SqlExpr.ArrayLit && !REDUCERS.contains(op);
        w.append("{\"").append(op).append("\": ").append(listed ? "[" : "").expr(arg, 0);
        return w.append(listed ? "]}" : "}");
    }

    private static final java.util.Set<String> REDUCERS = java.util.Set.of("$sum", "$min", "$max", "$avg");

    /** {@code {"$op": [<pieces>]}}: an operator over pieces the caller writes. */
    static SqlWriter opOf(SqlWriter w, String op, List<SqlWriter.Piece> pieces) {
        w.append("{\"").append(op).append("\": [");
        for (int i = 0; i < pieces.size(); i++) {
            if (i > 0) {
                w.append(", ");
            }
            w.piece(pieces.get(i));
        }
        return w.append("]}");
    }

    /** A piece that writes {@code text}. */
    static SqlWriter.Piece text(String text) {
        return w -> w.append(text);
    }

    /** A piece that writes {@code e}. */
    static SqlWriter.Piece of(SqlExpr e) {
        return w -> w.expr(e, 0);
    }

    /** A piece writing the variable {@code "$$name"}. */
    static SqlWriter.Piece var(String name) {
        return w -> w.append(string("$$" + name));
    }

    /** Whether {@code e} may be null, as its type fact says: a proof of non-null skips the guard (D7's optimisation,
     *  the IR's nullability stamps); anything else is guarded. */
    static boolean nullable(SqlExpr e) {
        return !(e.type() instanceof TypeFact.Typed t && !t.nullable());
    }

    /**
     * {@code args} bound once as {@code $$g0}, {@code $$g1}, ..., then {@code body} over them, answering null when any
     * nullable argument is null: SQL's three-valued operator over BSON's total one (D7). With no nullable argument the
     * body is written over the bound variables alone.
     */
    static SqlWriter guarded(SqlWriter w, List<SqlExpr> args, SqlWriter.Piece body) {
        w.append("{\"$let\": {\"vars\": {");
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                w.append(", ");
            }
            w.append("\"g").append(i).append("\": ").expr(args.get(i), 0);
        }
        w.append("}, \"in\": ");
        List<Integer> guards = new java.util.ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            if (nullable(args.get(i))) {
                guards.add(i);
            }
        }
        if (guards.isEmpty()) {
            w.piece(body);
        } else {
            w.append("{\"$cond\": [");
            anyNull(w, guards);
            w.append(", null, ").piece(body).append("]}");
        }
        return w.append("}}");
    }

    /** {@code {"$or": [{"$eq": ["$$g<i>", null]}, ...]}} over the guarded indices, or the one test alone. */
    private static void anyNull(SqlWriter w, List<Integer> guards) {
        if (guards.size() > 1) {
            w.append("{\"$or\": [");
        }
        for (int i = 0; i < guards.size(); i++) {
            if (i > 0) {
                w.append(", ");
            }
            w.append("{\"$eq\": [").append(string("$$g" + guards.get(i))).append(", null]}");
        }
        if (guards.size() > 1) {
            w.append("]}");
        }
    }

    /** {@code {"$op": ["$$g0", "$$g1", ...]}} over the guarded variables of {@code n} arguments. */
    static SqlWriter.Piece opOverVars(String op, int n) {
        return w -> {
            w.append("{\"").append(op).append("\": [");
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    w.append(", ");
                }
                w.append(string("$$g" + i));
            }
            w.append("]}");
        };
    }

    /** {@code {"$op": "$$g0"}}: an operator of one guarded argument. */
    static SqlWriter.Piece op1OverVar(String op) {
        return w -> w.append("{\"").append(op).append("\": ").append(string("$$g0")).append("}");
    }

    /** An operator of {@code n} arguments, each bound once and null-guarded (D7). */
    static SqlWriter guardedOp(SqlWriter w, String op, List<SqlExpr> args) {
        return guarded(w, args, args.size() == 1 ? op1OverVar(op) : opOverVars(op, args.size()));
    }

    /** The text of a BSON date literal from an ISO date or date-time text (a space or a {@code T} between date and
     *  time; a fraction of a second to the millisecond, BSON's precision; finer digits refused by name, never cut). */
    static String dateLiteral(String iso) {
        String t = iso.trim().replace(' ', 'T');
        if (t.length() == 10) {
            t = t + "T00:00:00";
        }
        int dot = t.indexOf('.');
        if (dot >= 0) {
            int end = dot + 1;
            while (end < t.length() && Character.isDigit(t.charAt(end))) {
                end++;
            }
            String fraction = t.substring(dot + 1, end);
            if (fraction.length() > 3 && !fraction.substring(3).chars().allMatch(c -> c == '0')) {
                throw new DialectCapability("a date-time finer than a millisecond (" + iso + ") has no BSON date:"
                        + " refused, never cut");
            }
            t = t.substring(0, dot) + "." + (fraction + "000").substring(0, 3) + t.substring(end);
        }
        if (!t.endsWith("Z") && !t.matches(".*[+-]\\d\\d:?\\d\\d$")) {
            t = t + "Z";
        }
        return "{\"$date\": " + string(t) + "}";
    }
}
