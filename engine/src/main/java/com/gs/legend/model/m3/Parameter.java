package com.gs.legend.model.m3;

import java.util.Objects;

/**
 * Typed parameter of an {@link PureFunction} — the compiled-metamodel counterpart of
 * {@link com.gs.legend.model.def.FunctionDefinition.ParameterDefinition} (parse layer).
 *
 * <p>Where the parse-layer record carries a raw {@code String type}, this record carries a
 * resolved {@link Type} — so consumers dispatch via identity / pattern-match instead of
 * string comparison. Constructed by {@link com.gs.legend.model.PureModelBuilder} when it
 * translates {@code FunctionDefinition} into the {@code m3.*} metamodel.
 *
 * @param name          Parameter name
 * @param type          Resolved Pure type (Primitive, ClassType, EnumType, FunctionType, ...)
 * @param multiplicity  Parameter multiplicity
 */
public record Parameter(String name, Type type, Multiplicity multiplicity) {
    public Parameter {
        Objects.requireNonNull(name, "Parameter name cannot be null");
        Objects.requireNonNull(type, "Parameter type cannot be null");
        Objects.requireNonNull(multiplicity, "Parameter multiplicity cannot be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Parameter name cannot be blank");
        }
    }
}
