package org.finos.legend.pure.dsl.legend;

import java.util.List;

/**
 * Collection/array: [a, b, c]
 */
public record Collection(List<Expression> values) implements Expression {

    public static Collection of(Expression... values) {
        return new Collection(List.of(values));
    }
}
