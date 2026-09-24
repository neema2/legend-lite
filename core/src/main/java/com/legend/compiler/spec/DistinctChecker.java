package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * Relation {@code distinct} (engine {@code DistinctChecker}) &mdash; fully
 * generic: whole-row dedup preserves {@code T}; the {@code ~[cols]} form narrows
 * to {@code X⊆T}. Emission reads the columns off the resolved output schema.
 */
final class DistinctChecker {

    private DistinctChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        // FQN spellings canonicalize to the parse name up front (the
        // ProjectChecker lesson): rebuilds and generic resolution key on
        // the name, and an FQN finds only its own narrow catalog entry.
        if (af.function().contains("::")) {
            af = new AppliedFunction("distinct", af.parameters());
        }
        Application a = t.checkGeneric(arrayIfBare(af), env);
        // collection::distinct (T[*] -> T[*], = removeDuplicates) shares the
        // bare name — a non-relation source is the LIBRARY overload and rides
        // the plain-call path, not the relop.
        if (!com.legend.compiler.element.type.Type
                .relationValued(a.args().get(0).info())) {
            return CallNodes.mint(t.ctx().implementations(), a.chosen(), a.args(), a.out());
        }
        java.util.List<String> columns = Args.outputColumns(a);
        // ~[] is legal where zero columns MEAN something (groupBy's
        // whole-relation aggregate); a zero-column dedup is not it — the
        // typed schema would be () while the SQL is DISTINCT * (audit).
        if (columns.isEmpty()) {
            throw new TypeInferenceException("distinct(~[]) names no columns");
        }
        return new TypedDistinct(a.args().get(0), columns, a.out());
    }

    /**
     * {@code distinct(~col)} desugars to {@code distinct(~[col])}: real Pure
     * registers ONLY the ColSpecArray overload (verified in
     * core_functions_relation/.../distinct.pure), and the engine accepts the
     * bare form as sugar — same normalize-then-one-generic-rule pattern as
     * {@code SortChecker.ascIfBare}. Never add a fake ColSpec overload.
     */
    private static AppliedFunction arrayIfBare(AppliedFunction af) {
        if (af.parameters().size() == 2
                && af.parameters().get(1) instanceof ColSpec cs) {
            List<ValueSpecification> params = new ArrayList<>(af.parameters());
            params.set(1, new ColSpecArray(List.of(cs)));
            return af.withParameters(params);
        }
        return af;
    }
}
