// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * Call-shape rules the typer applies BEFORE overload resolution: pure's
 * dot-spelling auto-map over a many-valued receiver, a let-bound lambda
 * literal in a core construct's argument position, and a packageable
 * element read as its metamodel value.
 */
final class CallShapes {

    private CallShapes() {
    }

    /** {@code toMultiplicity(values, @[m])} (legend-pure lang/cast/toMultiplicity.pure:
     * {@code <T|z>(source:T[*], object:Any[z]):T[z]}) — a multiplicity COERCION
     * the platform already spells: {@code @[1]} is {@code toOne}, {@code @[1..*]}
     * is {@code toOneMany}, {@code @[*]} the identity; any other target types
     * through the native signature and is a named wall at lowering
     * (Pure.WALLED_NATIVES; parser leg, batch 174). Null when not desugared. */
    static com.legend.protocol.spec.@com.legend.base.Nullable ValueSpecification toMultiplicityDesugar(
            AppliedFunction af) {
        String fn = af.function();
        String simple = fn.substring(fn.lastIndexOf(':') + 1);
        if (!simple.equals("toMultiplicity") || af.parameters().size() != 2
                || !(af.parameters().get(1) instanceof TypeAnnotation.MultiplicityRef mr)
                || !(mr.multiplicity() instanceof com.legend.protocol.Multiplicity.Concrete m)) {
            return null;
        }
        com.legend.protocol.spec.ValueSpecification source = af.parameters().get(0);
        if (m.lowerBound() == 1 && Integer.valueOf(1).equals(m.upperBound())) {
            return new AppliedFunction("toOne", java.util.List.of(source));
        }
        if (m.lowerBound() == 1 && m.upperBound() == null) {
            return new AppliedFunction("toOneMany", java.util.List.of(source));
        }
        if (m.lowerBound() == 0 && m.upperBound() == null) {
            return source;
        }
        return null;   // other targets ([0..1], [2..5]): typed by the signature, walled at lowering
    }

    /** Pure's auto-map on the DOT spelling: {@code $xs.qp()} over a
     * many-valued receiver maps the [1]-receiver qualified property over
     * it ({@code relationalExtensions().routerExtensions()} — real m3's
     * SimpleFunctionExpression auto-map; the arrow spelling does not).
     * Null when the call is not that shape. */
    static @com.legend.base.Nullable TypedSpec autoMapReceiver(Typer t, AppliedFunction af, Env env) {
        if (!af.propertyCall() || af.parameters().isEmpty()) {
            return null;
        }
        List<TypedFunction> cands = t.functionCandidates(af).stream()
                .filter(c -> c.parameters().size() == af.parameters().size())
                .toList();
        if (cands.isEmpty() || !cands.stream().allMatch(c ->
                Multiplicity.Bounded.ONE.equals(c.parameters().get(0).multiplicity()))) {
            return null;
        }
        TypedSpec recv;
        try {
            recv = t.synth(af.parameters().get(0), env);
        } catch (TypeInferenceException notStandalone) {
            // a receiver the call's own checker types in context — no
            // auto-map decision here; the ordinary path answers loudly
            return null;
        }
        if (!recv.info().multiplicity().isMany()) {
            return null;
        }
        String e = "_am_" + af.function().replace("::", "_");
        List<ValueSpecification> rest = new ArrayList<>(af.parameters());
        rest.set(0, new Variable(e, null, null, null));
        AppliedFunction inner = new AppliedFunction(af.function(), rest, af.candidateFqns(),
                af.pos(), false, af.grouped(), af.infix());
        return t.synth(new AppliedFunction("map", List.of(af.parameters().get(0),
                new LambdaFunction(List.of(new Variable(e, null, null, null)), List.of(inner)))), env);
    }

    /** A LET-BOUND lambda literal in a CORE construct's argument position
     * ({@code let f = t|$t.quantity < 45; ->filter($f)}) is its literal:
     * pure's let is immutable and referentially transparent, and the core
     * checkers type a lambda literal against their signature
     * ({@link Args#lambda}) — the engine's router inlines the value at
     * the same point. Generic and user calls (execute's query carrier)
     * keep the variable: their checkers take the function VALUE. */
    static AppliedFunction expandLetBoundLambdaArgs(AppliedFunction af, Env env) {
        List<ValueSpecification> np = null;
        for (int i = 0; i < af.parameters().size(); i++) {
            if (af.parameters().get(i) instanceof Variable v
                    && env.exprAlias(v.name()).orElse(null) instanceof LambdaFunction bound) {
                if (np == null) {
                    np = new ArrayList<>(af.parameters());
                }
                np.set(i, bound);
            }
        }
        return np == null ? af : af.withParameters(np);
    }

    /** The platform class a packageable ELEMENT reads as when it is used
     * as a metamodel VALUE (its system-store row): a database or a
     * mapping; null for any other name. */
    static @com.legend.base.Nullable String metamodelElementClass(ModelContext ctx, String fqn) {
        if (ctx.findDatabase(fqn).isPresent()) {
            return "meta::relational::metamodel::Database";
        }
        if (ctx.findMapping(fqn).isPresent()) {
            return "meta::pure::mapping::Mapping";
        }
        // a runtime ELEMENT is upstream's PackageableRuntime (e::RT.runtimeValue
        // — the Runtime an execute/from slot takes; batch 5)
        if (ctx.findRuntime(fqn).isPresent()) {
            return com.legend.compiler.element.type.PlatformTypes.PACKAGEABLE_RUNTIME;
        }
        // a class named as a VALUE is an instance of the metaclass
        // (m3.pure:213 — LA_Person.properties, the spec's evaluate tests)
        if (ctx.findClass(fqn).isPresent()) {
            return "meta::pure::metamodel::type::Class";
        }
        // a measure named as a value (RomanLength.canonicalUnit — m3 Measure)
        if (ctx.findMeasure(fqn).isPresent()) {
            return "meta::pure::metamodel::type::Measure";
        }
        return null;
    }


    /** {@code format(fmt, args)} with a CLASS-typed slot: the slot rewritten
     * as {@code $arg->toString()} (PlatformTypes.FORMAT / printsByOwnToString);
     * null when no slot needs it. */
    static @com.legend.base.Nullable AppliedFunction formatSlotsByToString(AppliedFunction af,
            Application a) {
        if (!com.legend.compiler.element.type.PlatformTypes.FORMAT
                .equals(a.chosen().qualifiedName())
                || af.parameters().size() != 2 || a.args().size() != 2) {
            return null;
        }
        com.legend.protocol.spec.ValueSpecification argsVs = af.parameters().get(1);
        TypedSpec typedArgs = a.args().get(1);
        List<com.legend.protocol.spec.ValueSpecification> srcElems;
        List<TypedSpec> typedElems;
        if (argsVs instanceof com.legend.protocol.spec.PureCollection pc
                && typedArgs instanceof com.legend.compiler.spec.typed.TypedCollection tc
                && pc.values().size() == tc.elements().size()) {
            srcElems = pc.values();
            typedElems = tc.elements();
        } else {
            srcElems = List.of(argsVs);
            typedElems = List.of(typedArgs);
        }
        List<com.legend.protocol.spec.ValueSpecification> out = new java.util.ArrayList<>(srcElems.size());
        boolean changed = false;
        for (int i = 0; i < srcElems.size(); i++) {
            if (com.legend.compiler.element.type.PlatformTypes
                    .printsByOwnToString(typedElems.get(i).info().type())) {
                out.add(new AppliedFunction("toString", List.of(srcElems.get(i))));
                changed = true;
            } else {
                out.add(srcElems.get(i));
            }
        }
        if (!changed) {
            return null;
        }
        com.legend.protocol.spec.ValueSpecification rewritten = argsVs instanceof com.legend.protocol.spec.PureCollection pc
                ? new com.legend.protocol.spec.PureCollection(out, pc.pos()) : out.get(0);
        return af.withParameters(List.of(af.parameters().get(0), rewritten));
    }

}
