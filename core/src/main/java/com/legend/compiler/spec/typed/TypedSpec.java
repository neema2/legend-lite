package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A type-checked expression &mdash; the Phase-G HIR (engine {@code TypedSpec}).
 * Every node carries its {@link ExprType} via {@link #info()} (the
 * schema-in-{@code info()} invariant, PHASE_G_SPEC_COMPILER.md §5), so downstream
 * lowering reads the resolved type off the node rather than re-deriving it.
 *
 * <p>This {@code permits} clause grows as Phase G implements more forms (literals
 * and variables first; calls, control flow, relation operators, and the rest
 * land in later stages). Adding a variant = a new record here.
 */
public sealed interface TypedSpec permits
        TypedCInteger,
        TypedCString,
        TypedCBoolean,
        TypedCFloat,
        TypedCDecimal,
        TypedVariable,
        TypedPropertyAccess,
        TypedNativeCall,
        TypedUserCall,
        TypedLet,
        TypedCollection,
        TypedIf,
        TypedLambda,
        TypedFilter,
        TypedMap,
        TypedColSpec,
        TypedColSpecArray,
        TypedSortInfo,
        TypedFuncColSpec,
        TypedFuncColSpecArray,
        TypedAggColSpec,
        TypedAggColSpecArray,
        TypedExtend,
        TypedGroupBy,
        TypedAggregate,
        TypedEnumValue,
        TypedCDate,
        TypedCTime,
        TypedCLatestDate,
        TypedTypeRef,
        TypedCast,
        TypedMatch,
        TypedEval,
        TypedTds,
        TypedSourceUrl,
        TypedFlatten,
        TypedPivot,
        TypedSortBy,
        TypedGetAll,
        TypedFrom,
        TypedWrite,
        TypedFold,
        TypedNavigate,
        TypedGraphFetch,
        TypedSerialize,
        TypedOver,
        TypedExtendWindow,
        TypedExtendAgg,
        TypedJoin,
        TypedAsOfJoin,
        TypedSelect,
        TypedDistinct,
        TypedConcatenate,
        TypedLimit,
        TypedDrop,
        TypedSlice,
        TypedPackageableRef,
        TypedProject,
        TypedTableReference,
        TypedSort,
        TypedRename,
        TypedNewInstance {

    /** This node's resolved type and multiplicity. */
    ExprType info();

    /**
     * This node's structural children, in source order &mdash; the one traversal
     * spine every pass reuses (callee collection, lowering walks, printing).
     * Deliberately <em>not</em> defaulted: every node must declare its children,
     * so adding a node forces the traversal decision at definition time (the same
     * exhaustiveness guarantee a switch would give, kept with the structure).
     */
    List<TypedSpec> children();
}
