package com.legend.compiler.element;

import java.util.List;
import java.util.Objects;

/**
 * A compiled Pure {@code Enum} (Phase F) &mdash; the typed counterpart of
 * {@link com.legend.model.EnumDefinition}. Pure data.
 *
 * <p>Structurally identical to its parser source (an enum is just a name plus
 * an ordered set of value names); it exists as its own {@code Typed*} record to
 * preserve the one-element-kind = one-record symmetry and to be referenced as a
 * {@link com.legend.compiler.element.type.Type.EnumType} by FQN.
 *
 * @param qualifiedName fully qualified enum name
 * @param values        value names in declaration order; non-empty
 */
public record TypedEnum(String qualifiedName, List<String> values) implements TypedNominal {

    public TypedEnum {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        Objects.requireNonNull(values, "values");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Enum must have at least one value: " + qualifiedName);
        }
        values = List.copyOf(values);
    }
}
