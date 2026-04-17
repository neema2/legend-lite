package com.gs.legend.model.m3;

import java.util.Optional;

/**
 * Lookup callback used by {@link PureClass} methods that walk the superclass chain
 * via fully qualified names rather than resolved {@link PureClass} object references.
 *
 * <p>Introduced in Phase A of the Bazel cross-project dependency work; see
 * {@code docs/BAZEL_IMPLEMENTATION_PLAN.md} §2 for the migration plan.
 *
 * <p>Typical usage: pass {@code modelContext::findClass} as the lookup.
 */
@FunctionalInterface
public interface ClassLookup {
    Optional<PureClass> find(String fqn);
}
