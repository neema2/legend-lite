// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.protocol.ParameterDefinition;

import com.legend.model.ClassDefinition;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.model.FunctionDefinition;
import com.legend.protocol.Multiplicity;
import com.legend.model.SynthHat;
import com.legend.protocol.TypeExpression;

import java.util.ArrayList;
import java.util.List;

/**
 * Derived-property LIFTING ({@code <owner>$prop$<name>} functions) — the
 * ONE synthesis shared by the normalizer's E.2 pass (parsed classes) and
 * the function resolver's on-demand arm (native-catalog classes, which
 * never see a normalizer pass). Two copies of this construction would
 * drift exactly like the {@code $prop$} string literals SynthHat exists
 * to unify.
 */
public final class DerivedProps {

    private DerivedProps() {
    }

    /**
     * Build {@code <owner>$prop$<name>(this:Owner[1], <params>):T[m]}
     * carrying the derived property's body. The leading {@code this}
     * receiver binds {@code $this} in the body; the class's type
     * parameters are propagated so a generic owner's {@code T} stays in
     * scope. The FQN uses the reserved {@code $} sigil and MUST match
     * {@code PureModelContext}'s {@code <owner>$prop$<name>} reference.
     */
    public static FunctionDefinition lift(
            ClassDefinition cd, DerivedPropertyDefinition dp) {
        TypeExpression thisType = receiverType(cd);
        List<FunctionDefinition.ParameterDefinition> params =
                new ArrayList<>(dp.parameters().size() + 1);
        params.add(new FunctionDefinition.ParameterDefinition(
                "this", thisType, Multiplicity.Concrete.PURE_ONE));
        for (ParameterDefinition p : dp.parameters()) {
            params.add(new FunctionDefinition.ParameterDefinition(
                    p.name(), p.type(), p.multiplicity()));
        }
        return new FunctionDefinition(
                SynthFqn.prop(cd.qualifiedName(), dp.name()),
                cd.typeParams(),
                List.of(),
                params,
                dp.type(),
                dp.multiplicity(),
                dp.expression(),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.PROP, cd.qualifiedName(), dp.name()));
    }

    /**
     * The {@code this} receiver type: the bare class FQN, or
     * {@code Owner<T, ...>} when the owner is generic (so the body's
     * {@code $this} carries the class's type parameters).
     */
    public static TypeExpression receiverType(ClassDefinition cd) {
        if (cd.typeParams().isEmpty()) {
            return new TypeExpression.NameRef(cd.qualifiedName());
        }
        List<TypeExpression> args = new ArrayList<>(cd.typeParams().size());
        for (String tp : cd.typeParams()) {
            args.add(new TypeExpression.NameRef(tp));
        }
        return new TypeExpression.Generic(cd.qualifiedName(), args);
    }

    /** The {@code $prop$} FQN split: {owner, name}, or null if {@code fqn}
     * is not a derived-property reference. */
    public static String @com.legend.base.Nullable [] splitPropFqn(String fqn) {
        return com.legend.model.DerivedPropertyNames.split(fqn);
    }
}
