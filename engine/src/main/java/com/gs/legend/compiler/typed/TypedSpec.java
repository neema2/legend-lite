package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Root of the typed AST (typed HIR). Every node carries its {@link ExpressionType}
 * (type + multiplicity) embedded directly — no external sidecar.
 *
 * <p>Produced exclusively by {@code TypeChecker}. Consumed by {@code MappingResolver}
 * (sidecar keys), {@code PlanGenerator} (pattern-match dispatch), and all
 * {@code CompiledExpression} holders (function bodies, service queries,
 * mapped-class sourceSpecs, derived properties, constraints, …).
 *
 * <p><strong>Invariant:</strong> no downstream pass imports
 * {@code com.gs.legend.ast} — all post-TypeChecker expression data is {@code TypedSpec}.
 *
 * <p><strong>Base-interface design:</strong> exactly one accessor, {@link #info()}.
 * Type/schema/multiplicity are reached via {@code info().type()} and the variant's
 * own fields. Schema is already a parameter of {@code Type.Relation(schema)} —
 * no separate schema axis is needed.
 *
 * <p><strong>Tree shape:</strong> each variant embeds its structural children
 * (e.g., {@code TypedFilter(source, predicate, info)}) — no sidecar zipping,
 * no parallel metadata, no index-based field lookup.
 *
 * <p><strong>Package layout:</strong> flat, matching {@code com.gs.legend.ast}.
 * Java's sealed-type rule requires all permitted subtypes to live in the same
 * package as the sealed parent in unnamed modules.
 */
public sealed interface TypedSpec permits
        // Literals (11)
        TypedCInteger, TypedCFloat, TypedCDecimal, TypedCString, TypedCBoolean,
        TypedCDateTime, TypedCStrictDate, TypedCStrictTime, TypedCLatestDate,
        TypedCByteArray, TypedEnumValue,
        // Bindings (3)
        TypedVariable, TypedLambda, TypedCollection,
        // Relation sources (3)
        TypedGetAll, TypedTableReference, TypedTdsLiteral,
        // Relation operators (10)
        TypedFilter, TypedProject, TypedSort, TypedJoin, TypedGroupBy,
        TypedExtend, TypedSelect, TypedRename, TypedSlice,
        // Scalar operators (4) + structural extract
        TypedPropertyAccess, TypedMap, TypedFold, TypedNativeCall,
        TypedStructExtract,
        // Struct construction (1)
        TypedNewInstance {

    /** Type + multiplicity. Every typed node has this. */
    ExpressionType info();
}
