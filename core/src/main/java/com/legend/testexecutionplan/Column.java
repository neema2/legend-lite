package com.legend.testexecutionplan;

/**
 * One column of a judged side ({@code TypedSide}; exec's until D72, 2026-10-30): its name, its Pure type in the plan's
 * spelling (a name, as an exec column's type is: the plan-as-data design's step 1, 2026-11-08 -- until then the
 * compiler's type object), and its multiplicity where it is known.
 */
public record Column(String name, String pureType,
        com.legend.executionplan.ExecutionPlan.@com.legend.base.Nullable Multiplicity multiplicity) {

    /** A column whose multiplicity is not known at its construction (scalar envelopes, pivot-rebuilt schemas). */
    public Column(String name, String pureType) {
        this(name, pureType, null);
    }
}
