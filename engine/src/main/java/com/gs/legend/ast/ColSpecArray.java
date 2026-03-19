package com.gs.legend.ast;

import java.util.List;

/**
 * Array of column specifications for the Relation API.
 *
 * <p>
 * Represents {@code ~[name, age, salary]} in Pure.
 *
 * <p>
 * Carried inside a {@link ClassInstance} with type "colSpecArray".
 *
 * @param colSpecs The list of column specifications
 */
public record ColSpecArray(
        List<ColSpec> colSpecs) {

    public ColSpecArray {
        colSpecs = List.copyOf(colSpecs);
    }
}
