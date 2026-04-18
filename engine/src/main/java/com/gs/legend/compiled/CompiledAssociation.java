package com.gs.legend.compiled;

import java.util.List;

/**
 * Compiled-state representation of a Pure association — the association
 * itself as a first-class element, with exactly two {@link CompiledAssociationEnd}s
 * and any association-declared qualified properties.
 *
 * <p>The <em>projection</em> of each end onto its target class (the
 * association-injected properties visible from {@code refdata::Person.accounts})
 * is materialized separately as {@link CompiledBackRefFragment}s and merged
 * at lookup time. That split keeps {@link CompiledClass} immutable and lets
 * Tier 2 caller-scoped extensions fit the same shape.
 */
public record CompiledAssociation(
        String qualifiedName,
        List<CompiledAssociationEnd> ends,
        List<CompiledDerivedProperty> qualifiedProperties,
        SourceLocation sourceLocation) implements CompiledElement {
}
