package com.gs.legend.compiled;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Member-level dependency data emitted by the compiler alongside a compiled
 * body ({@link CompiledExpression}).
 *
 * <p>Keyed by class FQN:
 * <ul>
 *   <li>{@code classPropertyAccesses} — which properties of each class the
 *       body statically touches (via {@code $x.prop}).</li>
 *   <li>{@code associationNavigations} — which association-injected properties
 *       of each class the body statically navigates.</li>
 * </ul>
 *
 * <p>The element-level used-set (authoritative input to Bazel's
 * {@code unused_inputs_list}) is derived via {@link #elementFqns()} and
 * aggregated across all compiled bodies at the {@code CompilationArtifacts}
 * level — it is not stored per {@code CompiledElement}.
 */
public record CompiledDependencies(
        Map<String, Set<String>> classPropertyAccesses,
        Map<String, Set<String>> associationNavigations) {

    public static final CompiledDependencies EMPTY =
            new CompiledDependencies(Map.of(), Map.of());

    /**
     * Derived: union of the two map key-sets — every class FQN statically
     * referenced by the body this record came from.
     */
    public Set<String> elementFqns() {
        var s = new LinkedHashSet<String>(classPropertyAccesses.keySet());
        s.addAll(associationNavigations.keySet());
        return Set.copyOf(s);
    }
}
