// Copyright 2025 Goldman Sachs
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.pure.dsl;

/**
 * Represents a flatten operation on a Relation with a JSON array column.
 * This produces a lateral join, creating one row per array element.
 * 
 * Example: ->flatten(~items) unnests the items array, adding 'items' column
 * for each element.
 */
public record FlattenExpression(
        RelationExpression source,
        String columnName) implements RelationExpression {
}
