// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * THE ONE CENSUS OWNER (cleanup move 3, 2026-09-21). Every counter the corpus lanes
 * print or pin lives here under one name, with one snapshot; the product increments a
 * {@link Key} where the fact happens and never stores a count of its own. Nothing a
 * verdict decides reads a count — the census is measurement, read by the lanes and the
 * divergence report only. Keyed families ({@link #incKeyed}) count by a runtime name: the
 * statement origins, the compiler's refusal reasons.
 */
public final class Census {

    private Census() {
    }

    public enum Key {
        VERDICT_FUSED("verdict.fused"),
        VERDICT_FALLBACKS("verdict.fallbacks"),
        VERDICT_FLUSHES("verdict.flushes"),
        VERDICT_HOST_DECIDED("verdict.host-decided"),
        FRAME_CTE("frame.cte"),
        FRAME_PASTED("frame.pasted"),
        FRAME_CLASS("frame.class"),
        FRAME_CLASS_CTE("frame.class-cte"),
        SQL_ROUND_TRIPS("sql.round-trips"),
        SQL_CHARS("sql.chars"),
        /** Row loads an engine's own bulk API took ({@link BulkLoad}). */
        BULK_LOADS("sql.bulk-loads"),
        ULP_FIRINGS("host.ulp-firings"),
        EFFECTS_IN_SCRIPT("effects.in-script"),
        /** Test bodies the body router moved whole onto the program road (the test-is-the-product plan's M5, R16). */
        BODIES_MOVED("bodies.moved"),
        DIVERGENCE_AGREE("divergence.agree"),
        DIVERGENCE_DISAGREE("divergence.disagree"),
        DIVERGENCE_RESIDUE("divergence.residue"),
        SQL_ULP_POLICY("sql-verdict.ulp-policy"),
        ROW_ORDER_CANON("sql-verdict.row-order-canon"),
        DECIMAL_SCALE_ONLY("sql-verdict.decimal-scale-only");

        private final String label;

        Key(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    private static final LongAdder[] COUNTS = new LongAdder[Key.values().length];
    private static final ConcurrentMap<String, LongAdder> KEYED = new ConcurrentHashMap<>();

    static {
        for (int i = 0; i < COUNTS.length; i++) {
            COUNTS[i] = new LongAdder();
        }
    }

    /** The fused statement's fallback reasons, appended by the batch's split rung AT PLANNING (a fused statement the
     * dialect cannot spell, each member planned its own statement; P1.2, 2026-11-07) -- a named list beside the
     * counts (the lanes attribute them per test). */
    public static final List<String> FALLBACK_REASONS =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    /** The asserts the host planner declined to plan for the program road, each by the shape it declined (the
     * test-is-the-product plan's M10c, 2026-11-02): a named list (the lanes attribute them per test and histogram
     * them, so the remaining assert shapes are named by count). Measurement, deciding nothing: a body whose assert
     * no judge planned is refused by name whatever is written here (the walk it stayed on is deleted, 2026-11-06). */
    public static final List<String> UNPLANNED_ASSERTS =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

    /** {@link #UNPLANNED_ASSERTS}: one declined assert, by its shape. */
    public static void unplannedAssert(String shape) {
        com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", "", shape);
        UNPLANNED_ASSERTS.add(shape);
    }

    public static void inc(Key k) {
        COUNTS[k.ordinal()].increment();
    }

    public static void add(Key k, long n) {
        COUNTS[k.ordinal()].add(n);
    }

    public static long count(Key k) {
        // D71, 2026-10-30: the test-lane scan-order pass's firings were read through here from the SQL layer's own
        // counter (StableScanOrder.firings), the census's one name of a dialect; the corpus lane reads that counter
        return COUNTS[k.ordinal()].sum();
    }

    public static void reset(Key... keys) {
        for (Key k : keys) {
            COUNTS[k.ordinal()].reset();
        }
    }

    /** A keyed family: {@code family.key}. */
    public static void incKeyed(String family, String key) {
        KEYED.computeIfAbsent(family + "." + key, x -> new LongAdder()).increment();
    }

    public static long keyed(String family, String key) {
        LongAdder a = KEYED.get(family + "." + key);
        return a == null ? 0 : a.sum();
    }

    /** Every count of one keyed family, by key. */
    public static Map<String, Long> family(String family) {
        Map<String, Long> out = new java.util.TreeMap<>();
        String prefix = family + ".";
        KEYED.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.put(k.substring(prefix.length()), v.sum());
            }
        });
        return out;
    }

    /** Every count, named. */
    public static Map<String, Long> snapshot() {
        Map<String, Long> out = new java.util.TreeMap<>();
        for (Key k : Key.values()) {
            out.put(k.label(), count(k));
        }
        KEYED.forEach((k, v) -> out.put(k, v.sum()));
        return out;
    }
}
