// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import java.util.ArrayList;
import java.util.List;

/**
 * THE VERDICTS' SHARED VOCABULARY (the plan-as-data design's slice 5, 2026-11-09): the few pure functions both halves
 * of a test body speak -- planning decides a verdict at planning with them, the run judges a row with them: a verdict
 * that holds, one that fails with its text, a side's values, the first difference of two texts, the engine-text
 * shapes the compatibility rules recognize, a constant cell decoded by its type's name. Each was a method of a class
 * that held both halves; here, below both, so neither half reaches the other.
 */
public final class Verdicts {

    private Verdicts() {
    }



    /** One-line first-difference sketch of two rendered texts (failure
     * message position — the full texts drown the diagnosis). */
    public static String firstTextDiff(String e, String a) {
        String[] el = e.split("\n", -1);
        String[] al = a.split("\n", -1);
        if (el.length != al.length) {
            return "line-count " + el.length + " != " + al.length
                    + " (expected first line: " + el[0] + ")";
        }
        for (int i = 0; i < el.length; i++) {
            if (!el[i].equals(al[i])) {
                return "line " + i + ": expected <" + el[i] + "> got <"
                        + al[i] + ">";
            }
        }
        return "texts differ only in leniency-adjudicated cells";
    }

    public static List<Object> decodeSideValues(
            @com.legend.base.Nullable TypedSide r) {
        return switch (r) {
            case null -> new ArrayList<>();
            case TypedSide.Scalar s -> {
                // (a list wire arriving as one JDBC array cell is decoded to a
                // Collection at the exec seam — no JDBC carrier reaches the router)
                List<Object> out = new ArrayList<>(1);
                if (s.value() != null) {
                    out.add(s.value());
                }
                yield out;
            }
            case TypedSide.Collection c -> c.values();
            // D4: a GRAPH side's values are the DATABASE-built JSON
            // array's elements (the harness Eval convention moved to
            // the owner) — parsed structures; the lattice/JsonCompare
            // judge them, never raw json text
            case TypedSide.Graph g -> {
                Object p = com.legend.sql.Json.parse(g.json());
                yield p instanceof List<?> l ? new ArrayList<Object>(l)
                        : new ArrayList<>(List.of(p));
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "assert verdict over a " + r.getClass().getSimpleName()
                    + " side — grid asserts stay with their own"
                    + " compare owners");
        };
    }

    public static TypedSide ok() {
        return new TypedSide.Scalar(Boolean.TRUE,
                "Boolean");
    }

    public static TypedSide fail(String message) {
        // the seam: verdicts speak the platform vocabulary
        throw new com.legend.error.AssertFailed(message);
    }


    /** A text that differs from its golden, in clean mode: the assert's failure, the first difference named. */
    public static TypedSide textsDiffer(String name, String golden, String ours,
            @com.legend.base.Nullable String decline) {
        return fail(name + " (sql-text): our text differs from the golden" + (decline != null ? " (" + decline + ")"
                : "") + ": " + firstTextDiff(golden, ours));
    }

    /** The engine's index-less {@code sqlRemoveFormatting($res)} over a
     * CHAINED plan prints statement 0 and appends this warning line
     * (relationalMappingExecution.pure): the referee reads the SQL and
     * drops the message before replay — a spec-text shape, like the
     * population statement's. */
    private static final String CHAINED_PLAN_WARNING =
            "\nWarning: Results only shown for first relational query.";

    public static String stripChainedPlanWarning(String golden) {
        int at = golden.indexOf(CHAINED_PLAN_WARNING);
        return at < 0 ? golden : golden.substring(0, at);
    }

    /** The engine's population statement shape (batch 67): the ONE
     * recognizer of that golden text, shared by both routes. */
    public static boolean isPopulationGolden(String golden) {
        return golden.toLowerCase(java.util.Locale.ROOT)
                .startsWith("select distinct");
    }

    public static @com.legend.base.Nullable String scalarString(
            @com.legend.base.Nullable TypedSide r) {
        return r instanceof TypedSide.Scalar s
                && s.value() instanceof String str ? str : null;
    }




    /** A verdict decided at planning: it compares nothing, so no rules are asked of it. */
    /** The NAME of the statement origin the walk's arm marked for OUR text where {@code judgment} holds an engine-text
     *  comparison whose text is a side the verdict step runs (M10c item 8: {@code SqlTextVerdicts.Collected.oursOrigin}),
     *  looked for through an unroll's parts; null where none (the step runs under its own). The run sends the step
     *  under the origin of that name. */
    public static @com.legend.base.Nullable String deferredTextOrigin(HostJudgment judgment) {
        HostJudgment j = HostJudgment.core(judgment);
        if (j instanceof HostJudgment.EngineText e) {
            return e.oursOrigin();
        }
        if (j instanceof HostJudgment.Unrolled u) {
            for (var p : u.parts()) {
                String o = deferredTextOrigin(p.judgment());
                if (o != null) {
                    return o;
                }
            }
        }
        return null;
    }

    public static TypedSide decided(HostJudgment.Decided d) {
        return d.held() ? ok()
                : fail(java.util.Objects.requireNonNull(d.message(), "a failed verdict's message"));
    }


    public static Object constantValue(String s, String type) {
        return switch (type) {
            case "String" -> s;
            case "Boolean" -> switch (s) {
                case "true" -> Boolean.TRUE;
                case "false" -> Boolean.FALSE;
                default -> throw new IllegalStateException("a Boolean constant's cell '" + s + "'");
            };
            case "Integer" -> {
                java.math.BigInteger b = new java.math.BigInteger(s);
                yield b.bitLength() < 64 ? (Object) b.longValue() : b;
            }
            default -> throw new com.legend.error.NotImplementedException("a constant side of type " + type
                    + ": the folds admit String, Integer and Boolean");
        };
    }


    public static @com.legend.base.Nullable String wireId(
            @com.legend.base.Nullable Object v) {
        return v instanceof java.util.Map<?, ?> m
                && m.get(com.legend.sql.SqlExpr.SYNTHETIC_ID)
                        instanceof String id ? id : null;
    }

    public static String repr(@com.legend.base.Nullable Object v) {
        return repr(v, null);
    }

    public static String repr(@com.legend.base.Nullable Object v,
            @com.legend.base.Nullable String instanceClass) {
        if (v instanceof java.util.Map<?, ?> m) {
            String id = wireId(v);
            if (id == null) {
                StringBuilder sb = new StringBuilder();
                for (Object val : m.values()) {
                    if (sb.length() > 0) {
                        sb.append('|');
                    }
                    sb.append(val);
                }
                id = sb.toString();
            }
            return "<" + id + " instanceOf "
                    + (instanceClass == null ? "?" : instanceClass) + ">";
        }
        return switch (v) {
            case null -> "[]";   // an empty [0..1] renders as empty
            case String s -> "'" + s.replace("\\", "\\\\")
                    .replace("'", "\\'").replace("\n", "\\n") + "'";
            case java.math.BigDecimal d -> d.toPlainString() + "D";
            case Number n -> n.toString();
            case Boolean b -> b.toString();
            // THE wire temporal type (D-arc: sql/java.time never escape
            // the fetch) — upstream's toRepresentation, '%' + its toString: a value with a minute carries +0000
            // (the engine runner, 2026-11-04, the assert bar: %2020-01-01T10:00:00 writes
            // '%2020-01-01T10:00:00+0000'; the engine spelling dropped the zone)
            case com.legend.values.PureDateLiteral d ->
                    "%" + d;
            default -> throw new com.legend.error.NotImplementedException(
                    "toRepresentation for " + v.getClass().getName()
                    + " is not modeled (spec: '<id instanceOf Type>' —"
                    + " loud until a consumer pins the id form)");
        };
    }
}
