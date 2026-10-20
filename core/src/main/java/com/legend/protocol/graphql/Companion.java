// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.graphql;

import java.util.List;

/**
 * THE COMPANION MODEL'S NAMES (docs/GRAPHQL_STUDY_2026_10_18.md §5.8, G15, G20): the one rule the normalizer
 * writes the generated classes and members by and the translator reads them by. Generated classes live under a
 * reserved package, by the class's own full path: {@code meta::legend::lite::graphql::companion::demo::PersonAggregate}
 * holds {@code items: demo::Person[*]} and the aggregate members; {@code ...::demo::PersonSum} (and Avg, Min, Max)
 * holds one field per property the fold applies to. A class with a to-many property {@code p} of class {@code T}
 * carries {@code pAggregate(): TAggregate[1]}.
 */
public final class Companion {

    private Companion() {
    }

    public static final String PACKAGE = "meta::legend::lite::graphql::companion";

    /** The aggregate's collection. */
    public static final String ITEMS = "items";

    /** The aggregate's row count. */
    public static final String COUNT = "count";

    /** The folds of an aggregate (G6), in the schema's order: each a member of the aggregate class, a generated
     *  class, and a Pure function of the catalog. The numeric ones take numeric properties; the others every
     *  comparable one (numbers, dates and strings: register S104). */
    public enum Fold {
        SUM("sum", "Sum", "sum", true),
        AVG("avg", "Avg", "average", true),
        MIN("min", "Min", "min", false),
        MAX("max", "Max", "max", false);

        /** The field's and the member's name. */
        public final String field;
        /** The fold class's suffix: {@code PersonSum}. */
        public final String suffix;
        /** The Pure function's simple name: {@code avg} is {@code average}. */
        public final String pureName;
        public final boolean numericOnly;

        Fold(String field, String suffix, String pureName, boolean numericOnly) {
            this.field = field;
            this.suffix = suffix;
            this.pureName = pureName;
            this.numericOnly = numericOnly;
        }

        /** The fold a schema field names, by its declared meaning. */
        public static Fold of(String field) {
            for (Fold f : values()) {
                if (f.field.equals(field)) {
                    return f;
                }
            }
            throw new IllegalArgumentException("not a fold: " + field);
        }
    }

    /** The folds, in the schema's order. */
    public static final List<Fold> FOLDS = List.of(Fold.values());

    public static String aggregateClass(String classFqn) {
        return PACKAGE + "::" + classFqn + "Aggregate";
    }

    public static String foldClass(String classFqn, Fold fold) {
        return PACKAGE + "::" + classFqn + fold.suffix;
    }

    /** The member a to-many property {@code p} gets beside it. */
    public static String aggregateProperty(String property) {
        return property + "Aggregate";
    }
}
