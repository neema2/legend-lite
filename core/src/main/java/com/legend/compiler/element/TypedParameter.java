package com.legend.compiler.element;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

import java.util.Objects;

/**
 * A named, typed parameter &mdash; the classified counterpart of
 * {@link com.legend.model.FunctionDefinition.ParameterDefinition}.
 * Used by {@link TypedFunction} and {@link Property.Derived}.
 *
 * @param name         parameter name
 * @param type         classified parameter type (FQN-only for nominal kinds)
 * @param multiplicity classified multiplicity
 */
public record TypedParameter(String name, Type type, Multiplicity multiplicity) {
    public TypedParameter {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(multiplicity, "multiplicity");
    }
}
