// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;


import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A statement as a dialect renders it for execution with bound values: the SQL text with a {@code ?} placeholder for each
 * bound parameter, and the parameters in placeholder order ({@link SqlDialect#renderStatement}), each with how it is
 * bound (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3).
 *
 * @param sql   the statement text, sent as it is: a placeholder is typed by its parameter's declared type when the
 *              statement is written (D1; the type hole the runner wrote into was deleted 2026-11-09, P4)
 * @param binds one per {@code ?}, in order
 */
public record RenderedStatement(String sql, List<Bind> binds, List<Region> regions) {

    public RenderedStatement {
        Objects.requireNonNull(sql, "sql");
        binds = List.copyOf(binds);
        regions = List.copyOf(regions);
        Region.check(sql.length(), binds, regions, b -> b.binding() instanceof Binding.Key);
    }

    /** A statement with no per-key region. */
    public RenderedStatement(String sql, List<Bind> binds) {
        this(sql, binds, List.of());
    }

    /**
     * A run of the statement written ONCE PER KEY of its plan's keys probe (the probe plan of a dynamic pivot,
     * 2026-10-20, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9): the text {@code [start, end)}, which the runner writes once
     * for each key in the probe's order, {@code lead} before the first copy and {@code separator} between copies, the
     * key's position in the probe written at each of {@code counters} (positions in the statement, within the run),
     * and the {@code bindCount} placeholders from {@code firstBind} bound again in each copy (a key's cell from that
     * key's probe row). With no key the run is not written at all, {@code lead} with it, and {@code empty} stands in
     * its place; a null {@code empty} means the statement has no form without a key, which the runner refuses by name.
     * The runner writes only this text: the dialect wrote every character of it when the plan was made.
     */
    public record Region(int start, int end, String lead, String separator, @com.legend.base.Nullable String empty,
            int firstBind, int bindCount, List<Integer> counters) {
        public Region {
            Objects.requireNonNull(lead, "lead");
            Objects.requireNonNull(separator, "separator");
            counters = List.copyOf(counters);
            if (start < 0 || end < start) {
                throw new IllegalArgumentException("a per-key region [" + start + ", " + end + ")");
            }
            for (int at : counters) {
                if (at < start || at > end) {
                    throw new IllegalArgumentException("a key's position at " + at + " outside its region [" + start
                            + ", " + end + ")");
                }
            }
            if (firstBind < 0 || bindCount < 0) {
                throw new IllegalArgumentException("a per-key region's placeholders from " + firstBind + ", "
                        + bindCount + " of them");
            }
        }

        /** {@code regions} in order within a statement of {@code length}, each one's placeholders among {@code binds},
         *  and every placeholder {@code keyed} says binds a key's cell inside one: a key's cell outside a region has
         *  no key to bind, refused by name. */
        public static <B> void check(int length, List<B> binds, List<Region> regions,
                java.util.function.Predicate<B> keyed) {
            int previousEnd = 0;
            int previousBinds = 0;
            java.util.BitSet inRegion = new java.util.BitSet(binds.size());
            for (Region r : regions) {
                if (r.start() < previousEnd || r.end() > length) {
                    throw new IllegalArgumentException("per-key region [" + r.start() + ", " + r.end() + ") is not after"
                            + " the previous one (" + previousEnd + ") within the statement (" + length + ")");
                }
                if (r.firstBind() < previousBinds || r.firstBind() + r.bindCount() > binds.size()) {
                    throw new IllegalArgumentException("per-key region's placeholders " + r.firstBind() + ".."
                            + (r.firstBind() + r.bindCount()) + " are not after the previous region's (" + previousBinds
                            + ") among the statement's " + binds.size());
                }
                inRegion.set(r.firstBind(), r.firstBind() + r.bindCount());
                previousEnd = r.end();
                previousBinds = r.firstBind() + r.bindCount();
            }
            for (int i = 0; i < binds.size(); i++) {
                if (keyed.test(binds.get(i)) && !inRegion.get(i)) {
                    throw new IllegalArgumentException("placeholder " + (i + 1) + " binds a key's cell outside a per-key"
                            + " region: no key to bind");
                }
            }
        }
    }

    /** What one placeholder binds: a declared parameter's value, as {@code binding} says. */
    public record Bind(String parameter, Binding binding) {
        public Bind {
            Objects.requireNonNull(parameter, "parameter");
            Objects.requireNonNull(binding, "binding");
        }
    }

    /** How a placeholder is bound. */
    public sealed interface Binding permits Binding.One, Binding.Array, Binding.Key {

        /** One value, as the caller's checked value is; an absent one a null of {@code nullType} (a
         *  {@code java.sql.JDBCType}'s name). {@code asText}: set where the placeholder is a date or date-time cast to the
         *  declared type's SQL type, which takes the value as its text (D1, P4). */
        record One(String nullType, @com.legend.base.Nullable AsText asText) implements Binding {
            public One {
                Objects.requireNonNull(nullType, "nullType");
            }
        }

        /** The parameter's whole list, as one array of {@code elementSqlType} (the target's form of a list). */
        record Array(String elementSqlType) implements Binding {
            public Array {
                Objects.requireNonNull(elementSqlType, "elementSqlType");
            }
        }

        /** A key's cell (a per-key region's placeholder, {@link Region}): the probe row's {@code cell} of the key the
         *  region's copy is of, bound as text; it names no declared parameter. */
        record Key(int cell) implements Binding {
            public Key {
                if (cell < 0) {
                    throw new IllegalArgumentException("a key's cell " + cell);
                }
            }
        }
    }

    /** A date or date-time value bound as its TEXT to a placeholder cast to its declared type's SQL type: the drivers
     *  pass no digit finer than a microsecond alike (DuckDB's cuts it, Postgres's rounds it; probes/timestamp-results.txt).
     *  {@code fraction}: the digits of a second the text keeps, the finer ones cut, as the database's literal keeps them
     *  (Postgres six); null for all nine. */
    public record AsText(@com.legend.base.Nullable Integer fraction) {
        public AsText {
            if (fraction != null && (fraction < 0 || fraction > 9)) {
                throw new IllegalArgumentException("a second's fraction of " + fraction + " digits");
            }
        }
    }
}
