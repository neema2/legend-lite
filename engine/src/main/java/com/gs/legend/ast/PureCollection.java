package com.gs.legend.ast;

import java.util.List;

/**
 * Collection of values (list/array).
 *
 * <p>
 * Represents {@code [a, b, c]} in Pure. Used for:
 * <ul>
 * <li>Lambda arrays: {@code [{p|$p.name}, {p|$p.age}]}</li>
 * <li>Alias arrays: {@code ['name', 'age']}</li>
 * <li>General collections: {@code [1, 2, 3]}</li>
 * </ul>
 *
 * @param values The elements of the collection
 */
public record PureCollection(
        List<ValueSpecification> values) implements ValueSpecification {

    public PureCollection {
        values = List.copyOf(values);
    }
}
