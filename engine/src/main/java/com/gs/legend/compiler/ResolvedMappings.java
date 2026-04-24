package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.TypedPropertyAccess;
import com.gs.legend.compiler.typed.TypedSpec;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * MappingResolver's single analysis artifact.
 *
 * <p>Commits every physical-mapping decision for a compiled expression tree:
 * per-node store resolutions, per-op navigation shape lists, and per-access
 * binding resolutions. PlanGenerator consumes this sidecar as a pure dictionary
 * — no tree walking, no reflection, no ambient-state threading.
 *
 * <p>Identity-keyed maps are safe here because the typed HIR is frozen between
 * MappingResolver and PlanGenerator (no IR mutation). This is the
 * industry-standard pattern (Scalac {@code Context}, Roslyn {@code SemanticModel},
 * Rust {@code TypeckResults}).
 *
 * @param storeResolutions Per typed-node (anchors and relational ops inherit
 *                         from their source) → resolved {@link StoreResolution}.
 *                         Today's MR output; semantics unchanged.
 * @param navigations      Per lambda-bearing relational op → committed list of
 *                         {@code Navigation}s (JoinNav / ExistsNav / SubqueryNav
 *                         / LateralNav). Populated starting in migration step 4.
 *                         Empty map is a legitimate value (e.g., a body with no
 *                         navigation).
 * @param accessBindings   Per {@link TypedPropertyAccess} → resolved
 *                         {@code PropertyBinding} (abstract-alias token +
 *                         physical column name). Populated starting in
 *                         migration step 4. Empty map is a legitimate value.
 */
public record ResolvedMappings(
        Map<TypedSpec, StoreResolution> storeResolutions,
        Map<TypedSpec, List<Navigation>> navigations,
        Map<TypedPropertyAccess, PropertyBinding> accessBindings) {

    /**
     * Convenience factory: wraps only the storeResolutions map, leaving
     * navigations and accessBindings empty. Used by migration step 1 where
     * MappingResolver only emits storeResolutions.
     */
    public static ResolvedMappings ofStoreResolutions(
            IdentityHashMap<TypedSpec, StoreResolution> storeResolutions) {
        return new ResolvedMappings(storeResolutions, Map.of(), Map.of());
    }

    /**
     * Placeholder sealed interface — filled in at migration step 2 with
     * {@code JoinNav / ExistsNav / SubqueryNav / LateralNav} permits. Until
     * then, the {@code navigations} map remains empty and consumers skip it.
     */
    public interface Navigation {}

    /**
     * Placeholder record — filled in at migration step 4 when MR starts
     * emitting access bindings.
     *
     * @param abstractAlias Lambda-param name (for direct / embedded accesses)
     *                      or synthetic nav token (for join-resolved accesses).
     * @param physicalColumn Terminal store's physical column name for the
     *                       property.
     */
    public record PropertyBinding(String abstractAlias, String physicalColumn) {}
}
