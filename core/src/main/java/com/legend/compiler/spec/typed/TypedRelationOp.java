// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

/**
 * A RELATION OPERATOR over one source relation: filter, project, sort, pivot,
 * group, extend, rename, select, a slice. Downstream phases treat the kind
 * alike &mdash; above a resolved class chain each is a relation-space wrapper,
 * rebuilt over its resolved source &mdash; so the kind is named here, once. A
 * phase that listed the operators one by one missed one: {@link TypedPivot}
 * appeared nowhere in the resolver, and DataCube's own shape (project, then
 * pivot) failed to plan while 23 pivot tests over {@code #TDS} literals passed.
 */
public sealed interface TypedRelationOp extends TypedSpec permits
        TypedFilter,
        TypedPivot,
        TypedProject,
        TypedSort,
        TypedSortBy,
        TypedLimit,
        TypedDrop,
        TypedSlice,
        TypedDistinct,
        TypedGroupBy,
        TypedAggregate,
        TypedExtend,
        TypedExtendWindow,
        TypedExtendAgg,
        TypedRename,
        TypedRecurse,
        TypedSelect {

    /** The relation this operator reads. */
    TypedSpec source();
}
