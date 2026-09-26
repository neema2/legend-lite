// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * Real pure resolves a call by TYPE; this platform dispatches its special
 * forms by bare name. Where the two disagree — a model function written as
 * the receiver's OWN ({@code join(_this:Database[1], name:String[1])},
 * relational.pure) sharing a name with an operator family (tds::join) —
 * the receiver's function wins, exactly as the derived-property routing in
 * {@link Typer} already does for qualified properties. Split from Typer for
 * size (CodeShape guard). Census batch 150.
 */
final class ReceiverOwnedFunctions {

    private ReceiverOwnedFunctions() {
    }

    /** The MODEL function this call names whose first parameter accepts
     * the receiver's class — null when the receiver is not a plain class
     * instance (a relation, a TDS, a lambda, a collection) or no such
     * function exists. */
    static @com.legend.base.Nullable TypedFunction of(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().isEmpty()) {
            return null;
        }
        ValueSpecification recv = af.parameters().get(0);
        if (recv instanceof LambdaFunction || recv instanceof PureCollection
                || recv instanceof ColSpec || recv instanceof ColSpecArray) {
            return null;
        }
        // a type-annotation argument is the special form's own spelling
        // ($x->cast(@T)) — never a model call
        if (af.parameters().stream().anyMatch(p -> p instanceof TypeAnnotation)) {
            return null;
        }
        // a special form with NO native signature (tableToTDS, new, let,
        // cast…) is the platform's definition outright; only an operator
        // FAMILY with natives (filter, join, sort) can yield to a model
        // function over a class its natives never take
        if (com.legend.compiler.ResolvedNames.declaredNatives(af).isEmpty()) {
            return null;
        }
        List<TypedFunction> cands = t.functionCandidates(af);
        // the receiver's OWN function: the spec writes a class-owned function
        // with its first parameter named _this (join(_this:Database[1], …),
        // relational.pure) — the qualified-property convention the derived-
        // property routing above already honours; an ordinary function over
        // a class (tableToTDS(table:Table[1])) never displaces a special form
        List<TypedFunction> owned = cands.stream()
                .filter(f -> !f.isNative() && f.parameters().size() == af.parameters().size()
                        && f.parameters().get(0).type() instanceof Type.ClassType
                        && "_this".equals(f.parameters().get(0).name()))
                .toList();
        if (owned.isEmpty()) {
            return null;
        }
        Type rt = t.synth(recv, env).info().type();
        if (Type.isRelation(rt) || rt instanceof Type.RelationType) {
            return null;
        }
        String raw = rt instanceof Type.ClassType c ? c.fqn()
                : rt instanceof Type.GenericType g ? g.rawFqn() : null;
        if (raw == null) {
            return null;
        }
        // the platform's OWN native taking this class receiver is the
        // definition (tableToTDS(table:Table[1]) — post-processors are
        // compiler passes, tableToTDS a platform semantic)
        for (TypedFunction n : cands) {
            if (n.isNative() && n.parameters().size() == af.parameters().size()
                    && n.parameters().get(0).type() instanceof Type.ClassType nc
                    && (nc.fqn().equals(raw) || t.model().isSubtype(raw, nc.fqn()))) {
                return null;
            }
        }
        for (TypedFunction f : owned) {
            String pc = ((Type.ClassType) f.parameters().get(0).type()).fqn();
            if (!(pc.equals(raw) || t.model().isSubtype(raw, pc))) {
                continue;
            }
            // every other NON-lambda argument must fit its parameter too
            boolean fits = true;
            for (int i = 1; i < af.parameters().size() && fits; i++) {
                ValueSpecification p = af.parameters().get(i);
                if (p instanceof LambdaFunction) {
                    continue;
                }
                fits = t.kernel().accepts(f.parameters().get(i).type(), t.synth(p, env).info().type());
            }
            if (fits) {
                return f;
            }
        }
        return null;
    }

}
