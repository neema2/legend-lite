package com.legend.model;

import java.util.Map;
import java.util.Objects;

/**
 * A typed element from one of the small keyed sections (ExternalFormat,
 * FileGeneration, the function-activator family, Text,
 * GenerationSpecification, DataQualityValidation, ServiceStore, MongoDB):
 * section + kind + qualified name, with keyed field values (or the raw
 * paren body for store-definition style elements) as written.
 *
 * <p>Not the opaque carrier: the element is NAMED per its declared kind,
 * its keys are structural, and each hosting section registered the kinds
 * it accepts. When a section's semantics land in lite, its elements
 * graduate from this record to a dedicated one (as Persistence and
 * Snowflake did).
 */
public record GenericSectionElementDefinition(
        String section,
        String kind,
        String qualifiedName,
        Map<String, String> fields,
        @com.legend.base.Nullable String bodySource) implements PackageableElement {

    public GenericSectionElementDefinition {
        Objects.requireNonNull(section, "Section cannot be null");
        Objects.requireNonNull(kind, "Kind cannot be null");
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        fields = Map.copyOf(fields);
    }
}
