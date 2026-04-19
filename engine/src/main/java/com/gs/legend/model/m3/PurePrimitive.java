package com.gs.legend.model.m3;

import com.gs.legend.model.SymbolTable;

import java.util.List;
import java.util.Objects;

/**
 * Declaration record for a Pure primitive type (Integer, String, Boolean, etc.).
 *
 * <p>Introduced in phase 2.5c.1 as the replacement for the Java {@code Type.Primitive}
 * enum. Each instance is a singleton held on {@link com.gs.legend.compiler.BuiltinRegistry}
 * as a static final field (e.g., {@code BuiltinRegistry.INTEGER}). Identity is FQN-based;
 * two {@code PurePrimitive} instances are equal iff their FQN matches.
 *
 * <p>Does <strong>not</strong> yet implement {@link Type} — that wiring happens in phase
 * 2.5c.3 when declarations become Type variants directly. In this phase, {@code PurePrimitive}
 * coexists with the legacy {@code Type.Primitive} enum; legacy callers continue to use the
 * enum, and {@code PurePrimitive} instances are registered in {@code BuiltinRegistry} only.
 *
 * @param qualifiedName  Fully qualified name (e.g., {@code "meta::pure::metamodel::type::Integer"}).
 *                       Named for symmetry with {@code ClassDefinition.qualifiedName()} and
 *                       {@code EnumDefinition.qualifiedName()}, even though {@code PurePrimitive}
 *                       lives on the runtime layer and doesn't implement {@code PackageableElement}
 *                       (that interface is sealed to def-layer types).
 * @param pureName       Simple Pure-level display name (e.g., {@code "Integer"}). Derivable from
 *                       {@code qualifiedName} but stored for readability.
 * @param superTypeFqns  FQNs of direct supertypes (e.g., Integer → {@code ["...Number"]}).
 *                       Empty list for roots ({@code Any}, {@code Nil}).
 * @param numeric        {@code true} for Number / Integer / Int64 / Int128 / Float / Decimal.
 * @param temporal       {@code true} for Date / StrictDate / DateTime / StrictTime.
 * @param date           {@code true} for Date / StrictDate / DateTime (subset of temporal).
 */
public record PurePrimitive(
        String qualifiedName,
        String pureName,
        List<String> superTypeFqns,
        boolean numeric,
        boolean temporal,
        boolean date) {

    public PurePrimitive {
        Objects.requireNonNull(qualifiedName, "PurePrimitive qualifiedName cannot be null");
        Objects.requireNonNull(pureName, "PurePrimitive pureName cannot be null");
        superTypeFqns = superTypeFqns == null ? List.of() : List.copyOf(superTypeFqns);
    }

    /** Simple name extracted from the qualified name (e.g., "Integer" from "meta::...::Integer"). */
    public String simpleName() {
        return SymbolTable.extractSimpleName(qualifiedName);
    }
}
