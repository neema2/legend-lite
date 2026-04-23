package com.gs.legend.compiler;

import com.gs.legend.model.m3.PureFunction;
import com.gs.legend.model.mapping.ClassMapping;

import java.util.Map;
import java.util.Optional;

/**
 * Immutable, self-contained snapshot of a single mapping scope.
 *
 * <p>Produced by {@link MappingNormalizer}, consumed by {@link MappingResolver}
 * and {@link TypeChecker} (indirectly — {@code MappingNormalizer.modelContext()}
 * overlays the snapshot's synthetic functions on the base model).
 *
 * <p>Replaces {@link com.gs.legend.model.mapping.MappingRegistry} for
 * MappingResolver. All M2M chains are pre-resolved at construction time —
 * no mutation methods exist.
 *
 * <h3>Mapping-function synthesis</h3>
 *
 * <p>Each class mapping contributes one <em>synthetic</em> {@link PureFunction}
 * whose body is the synthesized source-relation AST (relational:
 * {@code tableReference(...)->filter->extend(...)}; M2M: {@code getAll(srcClass)
 * ->filter->extend(...)}). These synthetic functions are indistinguishable
 * from user-authored functions — they live in the same per-query function
 * namespace, are compiled by {@link TypeChecker#check(PureFunction)} just like
 * any other function, and their bodies are walked by {@link MappingResolver}
 * through the same primitive used for {@code TypedUserCall}.
 *
 * <p>Synthetic functions are named {@code <classFqn>::mappingFunction} — a
 * reserved canonical suffix. A user-authored function with this FQN for a
 * given class is treated as an intentional override (same pattern as Python's
 * {@code __eq__}, Rust's {@code Default::default}, Go's {@code init}/{@code main}).
 *
 * <p>Per the cross-project-joins principle, the body of a synthetic mapping
 * function refers to upstream data by <em>class</em>, never by another mapping's
 * function FQN. Which mapping provides the upstream class is decided by the
 * active {@code NormalizedMapping} at query time.
 *
 * <h3>Encapsulation boundaries</h3>
 * <ul>
 *   <li><b>TypeChecker</b> sees the synthetic functions transparently through
 *       the {@code ModelContext.findFunction} overlay — no dedicated accessor.</li>
 *   <li><b>MappingResolver</b> reads {@link #findClassMapping}.</li>
 *   <li>Neither sees {@code MappingRegistry} directly.</li>
 * </ul>
 *
 * <h3>Why two function-indexed maps</h3>
 *
 * <p>The record carries the <em>same</em> set of synthetic {@link PureFunction}s
 * indexed two different ways because there are two hot-path lookup directions:
 * <ul>
 *   <li>{@code mappingFunctionFqns}: <em>class FQN → function FQN</em>. Consumed by
 *       {@link #findMappingFunctionFqn} when {@code TypeChecker.compileMappingFunctionFor}
 *       asks "which function materializes this class?".</li>
 *   <li>{@code mappingFunctions}: <em>function FQN → PureFunction</em>. Consumed by
 *       {@link #findMappingFunction} from the {@code ModelContext.findFunction}
 *       overlay when any function call resolves by FQN.</li>
 * </ul>
 * Collapsing to a single map would force one of the two lookups to scan O(n).
 * Both lookups are called on every compiled body, so both indices are kept.
 *
 * <p>All keys are class / function FQN strings. Internal integer-handle lookups
 * (via {@code SymbolTable}) are deliberately avoided at this module boundary
 * so callers don't need to understand the compiler's symbol interning.
 */
public record NormalizedMapping(
        Map<String, ClassMapping> classMappings,
        Map<String, String> mappingFunctionFqns,
        Map<String, PureFunction> mappingFunctions) {

    /** Empty snapshot — no mappings, no synthetic functions. */
    public static NormalizedMapping empty() {
        return new NormalizedMapping(Map.of(), Map.of(), Map.of());
    }

    /**
     * Finds a fully-resolved class mapping by class FQN.
     * Used by MappingResolver to drive per-class resolution.
     */
    public Optional<ClassMapping> findClassMapping(String classFqn) {
        return Optional.ofNullable(classMappings.get(classFqn));
    }

    /**
     * class FQN → mapping function FQN. Used by the
     * {@code ModelContext.findMappingFunctionFqn} overlay so TypeChecker can
     * locate which function body to compile for a given class's
     * {@code getAll()}.
     */
    public Optional<String> findMappingFunctionFqn(String classFqn) {
        return Optional.ofNullable(mappingFunctionFqns.get(classFqn));
    }

    /**
     * function FQN → {@link PureFunction}. Used by the
     * {@code ModelContext.findFunction} overlay so every downstream function
     * call resolution unifies synthesized mapping functions with user
     * functions through a single channel.
     */
    public Optional<PureFunction> findMappingFunction(String fnFqn) {
        return Optional.ofNullable(mappingFunctions.get(fnFqn));
    }

    /** @return true if this snapshot contains any class mappings. */
    public boolean hasClassMappings() {
        return !classMappings.isEmpty();
    }
}
