// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.graphql;

import java.util.List;

/**
 * The folds of an aggregate and of a group (docs/GRAPHQL_STUDY_2026_10_18.md G6), in the schema's order: each a field
 * of the generated aggregate and group types and a Pure function of the catalog the translator's {@code aggregate}
 * and {@code groupBy} call. The numeric ones take numeric properties; the others every comparable one (numbers,
 * dates and strings: register S104). (Until 2026-10-21 the GraphQL companion also generated a model class per fold;
 * the relation node, register S118, made them unnecessary.)
 */
public enum GqlFold {
    SUM("sum", "Sum", "sum", true),
    AVG("avg", "Avg", "average", true),
    MIN("min", "Min", "min", false),
    MAX("max", "Max", "max", false);

    /** The folds, in the schema's order. */
    public static final List<GqlFold> ALL = List.of(values());

    /** The field's name. */
    public final String field;
    /** The schema's fold type's suffix: {@code PersonSum}, its input types {@code PersonSumFilter}, {@code
     *  PersonSumOrderBy}. */
    public final String suffix;
    /** The Pure function's simple name: {@code avg} is {@code average}. */
    public final String pureName;
    public final boolean numericOnly;

    GqlFold(String field, String suffix, String pureName, boolean numericOnly) {
        this.field = field;
        this.suffix = suffix;
        this.pureName = pureName;
        this.numericOnly = numericOnly;
    }

    /** The fold a schema field names, by its declared meaning. */
    public static GqlFold of(String field) {
        for (GqlFold f : values()) {
            if (f.field.equals(field)) {
                return f;
            }
        }
        throw new IllegalArgumentException("not a fold: " + field);
    }
}
