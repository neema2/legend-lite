// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE FLOAT TEXT INSTRUMENT (W3 of docs/records/d-plan-2026-11-16.md, the orchestrator's audit of 2026-11-16: a
 * handful of hand-picked values passed text forms that are wrong elsewhere): a Float's text ({@code toString}) on the
 * embedded databases, judged value by value against the ONE answer of register S311 -- the shortest round-trip digits
 * (Java's {@code Double.toString}), rounded half up to six places, trailing zeros trimmed, a zero unsigned. The values: a
 * seeded random sample across every magnitude and both signs, the powers of two to 2^70, values in [1e9, 1e17) with
 * fractions, six-place ties, and the two values the audit named. The external databases run the same judgment in
 * {@code ProductRaiseProbeTest}'s lanes.
 */
public class FloatTextInstrumentTest {

    /** The sample, fixed by its seed: every run judges the same values. */
    public static List<Double> sample() {
        Set<Double> values = new LinkedHashSet<>();
        values.add(9.925379613776776E34);
        values.add(2.82879384806159E17);
        values.add(0.0);
        values.add(1e17);
        values.add(99999999999999984.0);
        for (int p = 0; p <= 70; p += 3) {
            values.add(Math.scalb(1.0, p));
            values.add(-Math.scalb(1.0, p) + (p < 50 ? 0.5 : 0));
        }
        Random r = new Random(20261116L);
        for (int i = 0; i < 120; i++) {
            // a random mantissa at a random decimal magnitude, 1e-8 to 1e40, either sign
            double v = (1 + r.nextDouble() * 9) * Math.pow(10, r.nextInt(49) - 8);
            values.add(r.nextBoolean() ? v : -v);
        }
        for (int i = 0; i < 40; i++) {
            // [1e9, 1e17): the digits beyond the sixth place a printf of the binary value rounds differently
            double v = (1 + r.nextDouble() * 9) * Math.pow(10, 9 + r.nextInt(8));
            values.add(r.nextBoolean() ? v : -v);
        }
        for (int i = 0; i < 20; i++) {
            // a six-place tie as its shortest digits spell it: x.yyyyyy5
            values.add(Double.parseDouble(r.nextInt(1000) + "." + String.format(java.util.Locale.ROOT, "%06d", r.nextInt(1_000_000)) + "5"));
        }
        for (int i = 0; i < 20; i++) {
            // a six-place tie at a small magnitude, its digits written with an exponent by some databases
            values.add(Double.parseDouble("0.0000" + String.format(java.util.Locale.ROOT, "%02d", r.nextInt(100)) + "5"));
            values.add(Double.parseDouble("-0.00" + String.format(java.util.Locale.ROOT, "%04d", r.nextInt(10_000)) + "5"));
        }
        values.add(5e-7);
        values.add(-5e-7);
        values.add(4.9999999999999996e-7);
        return new ArrayList<>(values);
    }

    /** The sample's size, stated once (the orchestrator's round 63: the count a lane judges is the count stated). */
    public static final int SAMPLE_SIZE = 273;

    /** The hand-picked values the probe judged before the sample (round 54's edges), as Java writes them. */
    private static final List<String> EDGES = List.of("100.0", "10.0", "0.0", "-0.0", "123456789.0",
            "0.30000000000000004", "-0.0000001", "1.0E21", "1.0E32", "1.0E300", "-1.0E300", "1.7976931348623157E308",
            "2.2250738585072014E-308", "1.2345678901234567E40", "-9.87654321E25");

    /** What every lane's probe judges ({@code ProductRaiseProbeTest}): the edges, then the sample, each value once --
     *  0.0 is in both, so {@link #PROBE_SIZE}, not their sum. */
    public static List<String> probeValues() {
        List<String> all = new ArrayList<>(EDGES);
        for (double v : sample()) {
            if (!all.contains(Double.toString(v))) {
                all.add(Double.toString(v));
            }
        }
        return all;
    }

    /** The probe's size, stated once. */
    public static final int PROBE_SIZE = 287;

    /** The magnitude from which {@code type} refuses a Float's text by name at run time ({@code VALUE_NOT_HELD}),
     *  infinite where it answers every value: SQLite's from 1e8 (its {@code floatToText}: register S311's proposed
     *  row, W3 of docs/records/d-plan-2026-11-16.md, round 63). */
    public static double refusedFrom(DatabaseType type) {
        return type == DatabaseType.SQLite ? 1e8 : Double.POSITIVE_INFINITY;
    }

    @Test
    void theCountsAreTheStatedOnes() {
        assertEquals(SAMPLE_SIZE, sample().size(), "the sample");
        assertEquals(PROBE_SIZE, probeValues().size(), "the probe's values");
    }

    /** The product's text of {@code x} (register S311). */
    static String floatText(double x) {
        String t = new BigDecimal(Double.toString(x)).setScale(6, RoundingMode.HALF_UP).stripTrailingZeros()
                .toPlainString();
        return new BigDecimal(t).signum() == 0 ? "0" : t;
    }

    /** Each value's text on {@code type}, in the sample's order, or the failure that refused it. */
    private static Object texts(DatabaseType type, List<Double> values) {
        TestBackend backend = TestBackends.of(type);
        StringBuilder tds = new StringBuilder("#TDS\n  i:Integer, x:Float\n");
        for (int i = 0; i < values.size(); i++) {
            tds.append("  ").append(i).append(", ").append(Double.toString(values.get(i))).append('\n');
        }
        String query = "|" + tds + "#->sort(~i->ascending())->extend(~t: r|$r.x->toOne()->toString())->select(~t)->from("
                + StorelessRuntime.RUNTIME + ")";
        StringWriter out = new StringWriter();
        try {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), query)
                    .executionPlan(StorelessRuntime.RUNTIME);
            try (java.sql.Connection c = backend.open()) {
                PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                        PlanCases.Written.JSON.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
            }
        } catch (RuntimeException | java.io.IOException | java.sql.SQLException failed) {
            return failed;
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"t\":\"([^\"]*)\"").matcher(out.toString());
        List<String> got = new ArrayList<>();
        while (m.find()) {
            got.add(m.group(1));
        }
        return got;
    }

    private static void judge(DatabaseType type) {
        List<Double> values = new ArrayList<>();
        List<Double> refused = new ArrayList<>();
        for (double v : sample()) {
            (Math.abs(v) < refusedFrom(type) ? values : refused).add(v);
        }
        Object answered = texts(type, values);
        List<String> wrong = new ArrayList<>();
        // each value from the bound refused by name, at run time: the kind, never an answer
        for (double v : refused) {
            Object one = texts(type, List.of(v));
            if (!refusedByName(one)) {
                wrong.add(v + " -> " + one + " (want VALUE_NOT_HELD, refused by name)");
            }
        }
        if (!(answered instanceof List<?> got)) {
            wrong.add("refused: " + answered);
        } else {
            for (int i = 0; i < values.size(); i++) {
                Object g = i < got.size() ? got.get(i) : null;
                String want = floatText(values.get(i));
                if (!want.equals(g)) {
                    wrong.add(values.get(i) + " -> " + g + " (want " + want + ")");
                }
            }
        }
        System.out.println("[float text instrument] " + type + ": " + values.size() + " values judged, "
                + refused.size() + " refused by name from " + refusedFrom(type) + ", " + wrong.size()
                + " wrong " + wrong);
        assertEquals(List.of(), wrong, type + ": " + wrong.size() + " of " + values.size() + " wrong");
    }

    @Test
    void duckDb() {
        judge(DatabaseType.DuckDB);
    }

    /** Whether {@code answered} is the run-time refusal of a Float's text by name. */
    public static boolean refusedByName(Object answered) {
        for (Throwable t = answered instanceof Throwable th ? th : null; t != null; t = t.getCause()) {
            if (t instanceof com.legend.error.DataError d && d.kind() == com.legend.error.ErrorKind.VALUE_NOT_HELD) {
                return true;
            }
        }
        return false;
    }

    /** SQLite's: exact below its bound, ties included (round 63: a six-place tie rounded on the shortest digits, never
     *  the binary value), refused by name from it (U3; register S311's proposed row). */
    @Test
    void sqlite() {
        judge(DatabaseType.SQLite);
    }

    @Test
    void h2() {
        judge(DatabaseType.H2);
    }
}
