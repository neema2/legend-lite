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
 *   <li>{@code mappingFunctions} — compiled mapping function per class FQN
 *       that downstream passes (MappingResolver) need to walk. Populated as a
 *       side-effect of {@code compileMappingFunctionFor}, covering both
 *       root-class fetches and association-target fan-out. Not specific to
 *       synthesized mapping functions — the same slot accommodates
 *       user-authored mapping functions in the future.</li>
 * </ul>
 *
 * <p>The element-level used-set (authoritative input to Bazel's
 * {@code unused_inputs_list}) is derived via {@link #elementFqns()} and
 * aggregated across all compiled bodies at the {@code CompilationArtifacts}
 * level — it is not stored per {@code CompiledElement}.
 */
public record CompiledDependencies(
        Map<String, Set<String>> classPropertyAccesses,
        Map<String, Set<String>> associationNavigations,
        Map<String, CompiledFunction> mappingFunctions) {

    public static final CompiledDependencies EMPTY =
            new CompiledDependencies(Map.of(), Map.of(), Map.of());

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
