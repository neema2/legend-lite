package com.legend.model;

import java.util.Objects;

/**
 * A parsed {@code Persistence} element &mdash; a service-fed persistence
 * pipeline. Top-level keys typed; the deep sub-DSLs (trigger body,
 * persister, output targets, notifier, tests) ride as raw source. Nothing
 * in legend-lite executes persistence; the record exists so
 * {@code ###Persistence} sections parse to typed, indexed elements.
 */
public record PersistenceDefinition(
        String qualifiedName,
        @com.legend.base.Nullable String doc,
        @com.legend.base.Nullable String triggerSource,
        @com.legend.base.Nullable String service,
        @com.legend.base.Nullable String persisterSource,
        @com.legend.base.Nullable String serviceOutputTargetsSource,
        @com.legend.base.Nullable String notifierSource,
        @com.legend.base.Nullable String testsSource) implements PackageableElement {

    public PersistenceDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
    }
}
