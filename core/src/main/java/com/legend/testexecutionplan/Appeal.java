// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * AN ENGINE-TEXT ASSERT'S APPEAL AS DATA (docs/PLAN_AS_DATA_2026_11_07.md 2.7, the design's step 8, 2026-11-08): what
 * decides a failed text under the compatibility rules, planned with the assert and held by the test plan by verdict
 * ({@link TestPlan#extensionsByVerdict}); heard at the verdict's step (the run records the two texts it read) and
 * decided after the body's run from this record, the run's record and the model. Until this step the appeal was a
 * closure the arm built at planning and held beside the plan by number.
 *
 * @param number   the appeal's number in its body: what a verdict's engine-text judgment and a database root's row name
 * @param name     the assert's name, the failure's words
 * @param kind     what decides a failed text
 * @param contract a {@link Kind#CONTRACT} appeal's failure words, before the two texts
 * @param rows     a {@link Kind#ROWS} appeal's planned rows leg; null where the rows were not planned with the assert
 * @param batched  the database judge's row: the texts its verdict row compares (null where the host judges the text)
 */
public record Appeal(int number, String name, Kind kind, @Nullable String contract, @Nullable Rows rows,
        @Nullable Batched batched) implements Extension {

    public Appeal {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(kind, "kind");
        if ((kind == Kind.CONTRACT) != (contract != null)) {
            throw new IllegalArgumentException(name + ": a contract appeal, and it alone, carries its words");
        }
    }

    /** What decides a failed text. */
    public enum Kind {
        /** The rows leg: our rows against the golden's, replayed by the referee. */
        ROWS,
        /** No referee for the text: the text is the contract, a failed text the assert's failure. */
        CONTRACT,
        /** The test-data generator's rows leg, not offered (S143): refused by name. */
        TDG
    }

    /** The same appeal numbered {@code n} in its body. */
    public Appeal numbered(int n) {
        return new Appeal(n, name, kind, contract, rows, batched);
    }

    /** The same appeal heard on the database judge's verdict row comparing {@code b}'s texts. */
    public Appeal batchedOn(Batched b) {
        return new Appeal(number, name, kind, contract, rows, b);
    }

    /**
     * The rows leg planned with the assert.
     *
     * @param rows         our rows' read
     * @param population   the page population's read (a page-membership verdict), or none
     * @param replaySql    the golden's SQL to replay, where it is not the golden text itself
     * @param mappingFqn   the mapping our rows go through
     * @param classFqn     the root class
     * @param extentSubset whether our rows are a subset of the extent (the referee's replay fact)
     * @param ordered      whether the order is the verdict's
     * @param sortKeys     the keys an order is compared by
     * @param goldenPlan   the golden's plan text, where the golden is a plan
     * @param planBindings the plan's parameters' spellings
     * @param temps        the temp tables the golden reads
     * @param seed         the in-list seed the golden's numbered temp table takes, or none
     * @param stores       the stores whose fixtures the harness provides before the rows are read
     * @param on           the target whose session the referee replays the golden on
     */
    public record Rows(Read rows, Read population, @Nullable String replaySql, @Nullable String mappingFqn,
            @Nullable String classFqn, boolean extentSubset, boolean ordered, @Nullable List<String> sortKeys,
            @Nullable String goldenPlan, Map<String, List<String>> planBindings, List<TempTable> temps,
            @Nullable InListSeed seed, List<String> stores, ExecutionPlan.Target on, Enums enums) {
        public Rows {
            Objects.requireNonNull(rows, "rows");
            Objects.requireNonNull(population, "population");
            sortKeys = sortKeys == null ? null : List.copyOf(sortKeys);
            Map<String, List<String>> b = new LinkedHashMap<>();
            planBindings.forEach((k, v) -> b.put(k, List.copyOf(v)));
            planBindings = Collections.unmodifiableMap(b);
            temps = List.copyOf(temps);
            stores = List.copyOf(stores);
            Objects.requireNonNull(on, "on");
            Objects.requireNonNull(enums, "enums");
        }
    }

    /**
     * The enumerations the referee decodes, read from the model at planning (the plan-as-data design's slice 4, hole 1,
     * 2026-11-08: until then the referee was handed the compiled model and read them at the appeal's decision).
     *
     * @param enums      the enumeration types among our rows' column types and the root class's properties: a column
     *                   of one of these types is an enumeration's
     * @param decodes    each enumeration's decode under the appeal's mapping, a source value to the value's name (the
     *                   identity where the mapping maps none); absent where the mapping cannot decode it, and empty
     *                   where there is no mapping
     * @param properties the root class's properties of an enumeration type (inherited ones too, the most derived
     *                   winning), each to its enumeration
     */
    public record Enums(List<String> enums, Map<String, Map<String, String>> decodes, Map<String, String> properties) {
        public static final Enums NONE = new Enums(List.of(), Map.of(), Map.of());

        public Enums {
            enums = List.copyOf(enums);
            Map<String, Map<String, String>> d = new LinkedHashMap<>();
            decodes.forEach((k, v) -> d.put(k, Collections.unmodifiableMap(new LinkedHashMap<>(v))));
            decodes = Collections.unmodifiableMap(d);
            properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        }
    }

    /**
     * One side of the rows leg as the plan reads it: its statement and its shape, or the failure its planning raised
     * (raised where the appeal reads it), or neither (the arm has no such side).
     */
    public record Read(TestPlan.@Nullable Side side, @Nullable SideShape shape, @Nullable Failure failure,
            @Nullable Unlisted unlisted) {
        public Read {
            if ((side == null) != (shape == null) || side != null && (failure != null || unlisted != null)
                    || failure != null && unlisted != null) {
                throw new IllegalArgumentException("a read is a side and its shape, or a failure, or none");
            }
        }

        /** A side and its shape, or a failure on the closed list, or none. */
        public Read(TestPlan.@Nullable Side side, @Nullable SideShape shape, @Nullable Failure failure) {
            this(side, shape, failure, null);
        }

        /** No such side. */
        public static final Read NONE = new Read(null, null, null, null);
    }

    /**
     * A side's planning failure of a class the closed list does not name (the slice 2 audit, 2026-11-08): the rows leg
     * reads only its words -- the class's simple name and its message -- where it declines (our rows: rows-underivable)
     * or faults (the population), so the appeal keeps those words, never a stand-in class; the body's planning goes on,
     * as it did when the side's failure was kept until the appeal was heard.
     */
    public record Unlisted(String simpleName, @Nullable String message) {
        public Unlisted {
            Objects.requireNonNull(simpleName, "simpleName");
        }
    }

    /** A temp table the golden reads: its name, its values' kind, its values. */
    public record TempTable(String name, String kind, List<String> values) {
        public TempTable {
            values = List.copyOf(values);
        }
    }

    /** The in-list seed of the golden's one numbered temp table. */
    public record InListSeed(String kind, List<String> values) {
        public InListSeed {
            values = List.copyOf(values);
        }
    }

    /** The database judge's verdict row's texts: the golden, ours (null: read from the row's actual cell), their
     *  equality as planned. */
    public record Batched(String golden, @Nullable String ours, boolean textEqual) {
        public Batched {
            Objects.requireNonNull(golden, "golden");
        }
    }
}
