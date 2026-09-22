package com.legend.model;

import java.util.Objects;

/** A parsed {@code PersistenceContext} element &mdash; binds a
 *  {@link PersistenceDefinition} to a platform / parameters / sink
 *  connection, all carried as raw source. */
public record PersistenceContextDefinition(
        String qualifiedName,
        String persistence,
        @com.legend.base.Nullable String platformSource,
        @com.legend.base.Nullable String serviceParametersSource,
        @com.legend.base.Nullable String sinkConnectionSource)
        implements PackageableElement {

    public PersistenceContextDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(persistence, "Persistence pointer cannot be null");
    }
}
