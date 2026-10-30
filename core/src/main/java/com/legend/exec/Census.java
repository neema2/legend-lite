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
        WIRE_RETYPED("wire.retyped"),
        WIRE_SLOT_SKEW("wire.slot-skew"),
        SQL_ROUND_TRIPS("sql.round-trips"),
        SQL_CHARS("sql.chars"),
        /** Row loads an engine's own bulk API took ({@link BulkLoad}). */
        BULK_LOADS("sql.bulk-loads"),
        ULP_FIRINGS("host.ulp-firings"),
        EFFECTS_IN_SCRIPT("effects.in-script"),
        DIVERGENCE_AGREE("divergence.agree"),
        DIVERGENCE_DISAGREE("divergence.disagree"),
        DIVERGENCE_RESIDUE("divergence.residue"),
        SQL_AGREE("sql-verdict.agree"),
        SQL_DISAGREE("sql-verdict.disagree"),
        SQL_DECLINED("sql-verdict.declined"),
        SQL_ULP_POLICY("sql-verdict.ulp-policy"),
        ROW_ORDER_CANON("sql-verdict.row-order-canon"),
        DECIMAL_SCALE_ONLY("sql-verdict.decimal-scale-only"),
        SQL_TDSNULL_POLICY("sql-verdict.tdsnull-policy");

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

    /** The fused statement's fallback reasons, appended by the batch's split rung —
     * a named list beside the counts (the lanes attribute them per test). */
    public static final List<String> FALLBACK_REASONS =
            java.util.Collections.synchronizedList(new java.util.ArrayList<>());

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
