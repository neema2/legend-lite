package com.legend.judge;

import com.legend.compiler.element.type.Type;

/**
 * One column of a judged side ({@code TypedSide}; exec's until D72, 2026-10-30: a column in the compiler's types is
 * the judges', the runtime's row is typed by the plan's own column): the name and the PURE type — the OBJECT, not a
 * name string. Consumers convert values by pureType only. (The
 * informational sqlType field is DELETED, documented-debts batch
 * 2026-08-18: §11 closed its last consumer with F5.1/F5.3B, so the
 * field had become a display string nobody displayed — and feeding it
 * was the positional branch's only {@code getColumnTypeName} read.)
 * The multiplicity rides since F5.2.
 */
public record Column(String name, Type pureType,
        com.legend.compiler.element.type.@com.legend.base.Nullable Multiplicity
                multiplicity) {

    /** Pre-F5.2 arity — multiplicity unknown at this construction site
     * (scalar envelopes, pivot-rebuilt schemas). */
    public Column(String name, Type pureType) {
        this(name, pureType, null);
    }
}
