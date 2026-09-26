package com.legend.model;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Measure} declaration &mdash; a family of unit types
 * with conversion lambdas, e.g. {@code Measure mass::Mass { *Gram: x -> $x;
 * Kilogram: x -> $x * 1000; }}. The {@code *}-marked unit is canonical.
 *
 * <p>Carried as data: nothing in legend-lite's compiler consumes unit
 * TYPES yet (a property typed {@code Mass~Gram} is a separate, unbuilt
 * leg); this record exists so {@code Measure} elements parse, index and
 * resolve instead of failing the file.
 *
 * @param qualifiedName  fully qualified measure name
 * @param canonicalUnit  the {@code *}-marked unit, or null when the measure
 *                       declares none
 * @param nonCanonicalUnits the remaining units, in declaration order
 */
public record MeasureDefinition(
        String qualifiedName,
        @com.legend.base.Nullable Unit canonicalUnit,
        List<Unit> nonCanonicalUnits) implements PackageableElement {

    public MeasureDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        nonCanonicalUnits = List.copyOf(nonCanonicalUnits);
    }

    /** One unit: name plus its conversion lambda (parameter + body), both
     *  null for a conversion-less unit. */
    public record Unit(String name,
            @com.legend.base.Nullable String paramName,
            com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification body) {
        public Unit {
            Objects.requireNonNull(name, "Unit name cannot be null");
        }
    }
}
