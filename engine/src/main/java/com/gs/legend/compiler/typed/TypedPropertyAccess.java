package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

import java.util.List;
import java.util.Optional;

/**
 * Property access: {@code $x.name} or {@code $x.a.b.c} (multi-hop).
 *
 * <p>{@code associationPath} holds the walked navigation when the access traverses
 * through associations (e.g., {@code [address, city]} for {@code $p.address.city});
 * empty for direct single-hop access.
 *
 * <p>{@code physicalColumn} is populated by {@link com.gs.legend.compiler.MappingResolver}
 * during the logical→physical rewrite pass. Pre-MR (TypeChecker output): empty.
 * Post-MR: present iff {@code source}'s class has a known
 * {@code logical→physical} column mapping for {@code property} (typical case
 * for relational mappings; absent for M2M-derived properties whose physical
 * realization is a synth-body extend alias). Lowering prefers this field and
 * falls back to the sidecar {@code columnFor} only when the field is empty —
 * the fallback is transitional and disappears with the sidecar in Phase 5 of
 * the rewrite plan.
 */
public record TypedPropertyAccess(
        TypedSpec source,
        String property,
        Optional<List<String>> associationPath,
        Optional<String> physicalColumn,
        ExpressionType info
) implements TypedSpec {
    /** TypeChecker-side construction (no physical column known yet). */
    public TypedPropertyAccess(TypedSpec source, String property,
                               Optional<List<String>> associationPath,
                               ExpressionType info) {
        this(source, property, associationPath, Optional.empty(), info);
    }
}
