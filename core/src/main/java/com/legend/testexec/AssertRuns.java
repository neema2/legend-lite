// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.AssertListener;

import static com.legend.testexecutionplan.Verdicts.decodeSideValues;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TypedSide;
import java.util.ArrayList;
import java.util.List;

final class AssertRuns {

    private AssertRuns() {
    }


    private static void familyEntered(String family) {
        com.legend.diagnostics.VerdictCensus.sqlFamily(family);
    }

    /** The equality arm's census route, as {@link #adjudicate} counts it once the engine-text arm answered. */
    private static void sqlTextRouted(String family) {
        com.legend.diagnostics.VerdictCensus.sqlRoute(family, "sql-text");
    }

    /** The census at a planned assert's turn: its family, counted before any side runs (as {@link #adjudicate}
     *  counts it). */
    static void enter(com.legend.testexecutionplan.Assertion a) {
        if (a.family() != null) {
            familyEntered(a.family());
        }
    }

    /** The verdict of {@code a}, judged by {@code j} over its sides as the runner read them (their rows, or the error
     *  reading them raised): a declined judgment counted as the generic path's (null); an engine-text equality's census
     *  route counted once the arm answered (M10b: never on a failed one, which raises before). */
    static @com.legend.base.Nullable TypedSide judge(com.legend.testexecutionplan.Assertion a, HostJudgment j,
            java.util.function.Supplier<List<TypedSide>> sides,
            com.legend.testexecutionplan.TestPlan.JudgingRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message, BodyRun run) {
        TypedSide v = judgedOrFellThrough(com.legend.testexec.HostJudgments.judge(j, sides, rules, message, run));
        if (v != null && HostJudgment.core(j) instanceof HostJudgment.EngineText e && e.routed()) {
            sqlTextRouted(a.name());
        }
        return v;
    }

    private static @com.legend.base.Nullable TypedSide judgedOrFellThrough(@com.legend.base.Nullable TypedSide v) {
        if (v == null) {
            com.legend.diagnostics.VerdictCensus.sqlFellThrough();
        }
        return v;
    }

    /**
     * THE PROGRAM ROAD'S VERDICT SEAM (the test-is-the-product plan's M6): {@code a}, planned before the program ran,
     * judged at its verdict step over the sides the runner read ({@code sides} shapes them, or raises the error
     * reading them raised: the judgment decides it, {@code com.legend.testexec.HostJudgments.judge}) -- counted and reported to
     * {@code listener} exactly as the deleted walk's adjudication counted and reported an assert it judged in place:
     * entered, its verdict reported, a failure (an assert that does not hold, a side's data error) counted as raised
     * and reported, any other exit counted as raised. A side the database refused is thus counted and reported as an
     * assert whose side raised inside the arm (M6's audit, S2; one path since 2026-11-03). Null where the judgment
     * declined the sides it read.
     */
    static @com.legend.base.Nullable TypedSide judgedAtItsStep(com.legend.testexecutionplan.TestPlan.Host h,
            com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener,
            java.util.function.Supplier<List<TypedSide>> sides, com.legend.testexecutionplan.TestPlan.JudgingRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message, BodyRun run) {
        com.legend.testexecutionplan.Assertion a = h.assertion();
        com.legend.testexecutionplan.Failure before = a.beforeTheArm();
        if (before != null) {
            // raised where the walk raises it, before the arm is entered: no census, no listener (S1)
            throw com.legend.testexecutionplan.Failures.raised(before);
        }
        if (listener == null) {
            enter(a);
            return judge(a, h.judgment(), sides, rules, message, run);
        }
        com.legend.diagnostics.VerdictCensus.sqlEnter();
        boolean settled = false;
        try {
            enter(a);
            TypedSide v = judge(a, h.judgment(), sides, rules, message, run);
            settled = true;
            if (v != null) {
                listener.verdict(a.listenerName(), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            settled = true;
            com.legend.diagnostics.VerdictCensus.sqlRaised();
            if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                listener.unjudged(a.listenerName(), af.unjudgedReason());
            }
            listener.verdict(a.listenerName(), false, e.getMessage());
            throw e;
        } finally {
            if (!settled) {
                com.legend.diagnostics.VerdictCensus.sqlRaised();
            }
        }
    }

    /** {@code a}'s verdict ROW, the database judge's own statement's (an assertError's, S135, 2026-11-05), judged at
     *  its step and counted and reported as {@link #judgedAtItsStep} counts and reports a host verdict: entered, its
     *  verdict to the listener, a failure counted as raised. The row's polarity and message decide. */
    static void judgedRowAtItsStep(com.legend.testexecutionplan.Assertion a, List<Object> row,
            com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener) {
        if (listener == null) {
            enter(a);
            com.legend.testexec.DatabaseVerdicts.verdictOf(a.name(), row);
            return;
        }
        com.legend.diagnostics.VerdictCensus.sqlEnter();
        boolean settled = false;
        try {
            enter(a);
            com.legend.testexec.DatabaseVerdicts.verdictOf(a.name(), row);
            settled = true;
            listener.verdict(a.listenerName(), true, null);
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            settled = true;
            com.legend.diagnostics.VerdictCensus.sqlRaised();
            if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                listener.unjudged(a.listenerName(), af.unjudgedReason());
            }
            listener.verdict(a.listenerName(), false, e.getMessage());
            throw e;
        } finally {
            if (!settled) {
                com.legend.diagnostics.VerdictCensus.sqlRaised();
            }
        }
    }

    /** The host half's verdict over its sides HELD for the agreement judge: its verdict, or the failure it raised
     *  (an assert that does not hold), never null together. */
    record HostHeld(@com.legend.base.Nullable TypedSide verdict, com.legend.error.@com.legend.base.Nullable AssertFailed failed) {
    }

    /**
     * THE AGREEMENT JUDGE'S HOST HALF AT ITS STEP (the walker-deletion program, 2026-11-02): {@code a} judged over the
     * sides the runner read, its verdict HELD -- the verdict of record is decided once the database half's rows are in
     * ({@link #agreedAtItsStep}) -- counted as the deleted walk's adjudication counted an assert it judged in place over the
     * agreement arm (entered; a side's data error counted raised, reported and thrown at once, as the agreement arm
     * lets it through; any other exit counted raised). A judgment that declined the sides it read is refused by name.
     */
    static HostHeld heldAtItsStep(com.legend.testexecutionplan.TestPlan.Host h, com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener,
            java.util.function.Supplier<List<TypedSide>> sides, com.legend.testexecutionplan.TestPlan.JudgingRules rules,
            java.util.function.@com.legend.base.Nullable Supplier<TypedSide> message, BodyRun run) {
        com.legend.testexecutionplan.Assertion a = h.assertion();
        com.legend.testexecutionplan.Failure before = a.beforeTheArm();
        if (before != null) {
            throw com.legend.testexecutionplan.Failures.raised(before);
        }
        if (listener != null) {
            com.legend.diagnostics.VerdictCensus.sqlEnter();
        }
        boolean settled = false;
        try {
            enter(a);
            TypedSide v = null;
            com.legend.error.AssertFailed failed = null;
            try {
                v = judge(a, h.judgment(), sides, rules, message, run);
            } catch (com.legend.error.AssertFailed e) {
                failed = e;
            }
            settled = true;
            if (failed != null) {
                return new HostHeld(null, failed);
            }
            if (v == null) {
                throw new com.legend.error.NotImplementedException(a.name() + " on the program road: the host"
                        + " judge declined the sides it read (the walk's generic path has no program form)");
            }
            return new HostHeld(v, null);
        } catch (com.legend.error.DataError e) {
            settled = true;
            if (listener != null) {
                com.legend.diagnostics.VerdictCensus.sqlRaised();
                listener.verdict(a.listenerName(), false, e.getMessage());
            }
            throw e;
        } finally {
            if (!settled && listener != null) {
                com.legend.diagnostics.VerdictCensus.sqlRaised();
            }
        }
    }

    /** The agreement judge's verdict of record for {@code a} ({@code agreed}: the agreement rule over both halves,
     *  {@link AgreementArm#agree}), reported to {@code listener} as the deleted walk's adjudication reported it: a pass, or a
     *  failure (an assert that does not hold, a data error) counted raised, its UNJUDGED reason and its verdict told,
     *  and thrown. */
    static TypedSide agreedAtItsStep(com.legend.testexecutionplan.Assertion a,
            com.legend.testexecutionplan.@com.legend.base.Nullable AssertListener listener,
            java.util.function.Supplier<@com.legend.base.Nullable TypedSide> agreed) {
        try {
            TypedSide v = java.util.Objects.requireNonNull(agreed.get(), "a host verdict the agreement rule dropped");
            if (listener != null) {
                listener.verdict(a.listenerName(), true, null);
            }
            return v;
        } catch (com.legend.error.AssertFailed | com.legend.error.DataError e) {
            if (listener != null) {
                com.legend.diagnostics.VerdictCensus.sqlRaised();
                if (e instanceof com.legend.error.AssertFailed af && af.unjudgedReason() != null) {
                    listener.unjudged(a.listenerName(), af.unjudgedReason());
                }
                listener.verdict(a.listenerName(), false, e.getMessage());
            }
            throw e;
        }
    }

    /** A verdict row's evidence column for a message: at most 600
     * characters (a grid text drowns the diagnosis past that). */
    static String excerpt(@com.legend.base.Nullable Object evidence) {
        String s = String.valueOf(evidence);
        return s.length() <= 600 ? s : s.substring(0, 600) + "…(" + s.length() + " chars)";
    }

    // X5's instance-key rule (the pair's shared key tree: both stamps the SAME keyed class, a side declared wider
    // counting as the other's class when every value on its wire is of it) is HostJudgment.Keys since M8a
    // (2026-11-01): the model read at planning, the wire read at judging.

    /** A class VALUE riding a JSON slot inside a struct (a polymorphic field: {@code parameters :
     *  RelationalOperationElement[*]}) arrives as object or array text -- the field's declared kind says it is a
     *  structure, so it decodes to the map or list the verdict's key restriction reads (WORLD_MAP §4: the value carries
     *  its class as {@code __type}); any other value as it is. */
    static @com.legend.base.Nullable Object structured(@com.legend.base.Nullable Object v) {
        if (v instanceof String s) {
            String t = s.trim();
            if (t.startsWith("{") || t.startsWith("[")) {
                return com.legend.sql.Json.parse(t);
            }
        }
        return v;
    }

    /** A JSON assert side's ONE document text: a GRAPH result's JSON
     * IS the {@code String[1]} (a serialize execute's {@code .values}
     * — leg 2 made its stamp String[1], and the DB-built envelope is
     * the value); any other result must decode to one string. Null =
     * not this shape (generic path, loud downstream). The side read already ({@code HostSides}, S2f). */
    static @com.legend.base.Nullable String jsonSideTextOf(TypedSide r) {
        if (r instanceof TypedSide.Graph g) {
            return g.json();
        }
        List<Object> v = decodeSide(r);
        return v.size() == 1 && v.get(0) instanceof String s ? s : null;
    }

    static List<Object> decodeSide(
            @com.legend.base.Nullable TypedSide r) {
        List<Object> side = decodeSideValues(r);
        // V7 §5-1 instrument: the side-size histogram (VALUES-literal
        // cost bracket for V12's fused-verdict design) — measurement
        // only, fed from the one place every side already flows through
        com.legend.testexec.CanonicalDivergence.v7SideRows(side.size());
        return side;
    }

    static Object one(List<Object> side, String what) {
        if (side.size() != 1) {
            throw new IllegalStateException(what + " must be one value,"
                    + " got " + side.size());
        }
        return side.get(0);
    }

    /** Row-major cell stream of a tabular HostJudge.frame (the cell-zip input). */
    /** Raw cell flatten — cells arrive ALREADY DECODED (the engine's
     * value-read conventions ride the FETCH: wrapTdsCanon conforms the
     * plan in SQL and the executor's label-driven unwrap delivers; the
     * former host-twin decode, valueRead, is deleted — Java-eviction
     * close of the disagree-9 burn). */
    static List<Object> cells(TypedSide.Tabular t) {
        List<Object> out = new java.util.ArrayList<>();
        for (com.legend.testexecutionplan.Row r : t.rows()) {
            out.addAll(r.values());
        }
        return out;
    }
}
