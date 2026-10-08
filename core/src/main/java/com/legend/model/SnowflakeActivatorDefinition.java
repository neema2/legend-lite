package com.legend.model;

import java.util.Map;
import java.util.Objects;

/** A parsed {@code SnowflakeApp} / {@code SnowflakeM2MUdf} function
 *  activator &mdash; censused keys carried as a flat field map ({@code
 *  function} keeps its full pointer signature as written). */
public record SnowflakeActivatorDefinition(
        String qualifiedName,
        String kind,
        Map<String, String> fields) implements PackageableElement {

    public SnowflakeActivatorDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(kind, "Kind cannot be null");
        fields = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(fields));
    }
}
