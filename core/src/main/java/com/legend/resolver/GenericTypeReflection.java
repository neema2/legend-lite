// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.List;
import java.util.Optional;

/**
 * {@code $x->genericType().rawType} over a class-query result — M3
 * reflection of each instance's CONCRETE class (inheritance testGetAll:
 * {@code [Car,Car,Car,Bicycle,Bicycle]} over a union extent). The union
 * body already carries per-row identity as MEMBERSHIP WITNESS columns
 * ({@code stc_<Fqn>___$member}, TRUE on the owning thread) — the raw
 * type materializes as the witness CASE. A single-source extent is the
 * constant class; a multi-member extent WITHOUT witnesses (same-table
 * reroute) has no per-row identity and stays loud.
 */
final class GenericTypeReflection {

    private GenericTypeReflection() {
    }

    /** The {@code genericType(<class chain>).rawType} shape. */
    static boolean matches(TypedPropertyAccess vpa) {
        return vpa.property().equals("rawType")
                && Pipelines.unwrapToOne(vpa.source())
                        instanceof com.legend.compiler.spec.typed
                                .TypedNativeCall gt
                && com.legend.builtin.NativeFn.SubtypeForm.of(gt.callee().id()).orElse(null) == com.legend.builtin.NativeFn.SubtypeForm.GENERIC_TYPE
                && !gt.args().isEmpty()
                && Type.asClassType(gt.args().get(0).info().type()) instanceof Type.ClassType;
    }

    /** Resolve the chain and wrap it with the raw-type projection; a BARE
     * class root resolves class-shaped — take the extent pipeline with
     * undemanded slots stripped (nothing else is read here). */
    static TypedSpec resolve(TypedPropertyAccess vpa,
            java.util.function.UnaryOperator<TypedSpec> resolver,
            java.util.function.Function<String, TypedSpec> bareExtent,
            java.util.Set<String> modelFqns) {
        var gt = (com.legend.compiler.spec.typed.TypedNativeCall)
                Pipelines.unwrapToOne(vpa.source());
        String fqn = ((Type.ClassType) gt.args().get(0).info().type()).fqn();
        TypedSpec rel = resolver.apply(gt.args().get(0));
        if (!Type.isRelation(rel.info().type())) {
            rel = Pipelines.materialize(bareExtent.apply(fqn),
                    java.util.Set.of(), fqn).pipeline();
        }
        return rawTypeProjection(rel, fqn, modelFqns);
    }

    /** The resolved relation wrapped with a one-column projection of the
     * instance's raw-type NAME (engine assert formatting compares by the
     * element's name). */
    static TypedSpec rawTypeProjection(TypedSpec rel, String baseClassFqn,
            java.util.Set<String> modelFqns) {
        Type.RelationType row = Type.relationSchema(rel.info().type());
        if (row == null) {
            throw new NotImplementedException("genericType().rawType over a"
                    + " non-relation resolution ("
                    + rel.info().type().typeName() + ")");
        }
        Multiplicity one = Multiplicity.Bounded.ONE;
        String v = "v_gt";
        TypedSpec rowVar = new TypedVariable(v, ExprType.one(row));
        ExprType str1 = new ExprType(Type.Primitive.STRING, one);
        TypedSpec body = null;
        for (Type.Column c : row.columns()) {
            String pfx = com.legend.model.ClassMapping
                    .witnessPrefixOf(c.name());
            if (pfx == null) {
                continue;
            }
            // EXACT inverse via the model — the __ encoding is lossy, so
            // demangling by string surgery corrupted any class whose NAME
            // contains __ (text-surgery audit §1.1 #1). elementFqns() is a
            // DEFAULT-empty surface, so a context that does not expose it
            // falls back to the demangle rather than regressing.
            String fqn = com.legend.model.ClassMapping
                    .classOfWitnessPrefix(pfx, modelFqns);
            if (fqn == null && !modelFqns.isEmpty()) {
                throw new NotImplementedException("membership witness '"
                        + c.name() + "' matches no model class by exact"
                        + " re-mangling");
            }
            if (fqn == null) {
                fqn = pfx.substring("stc_".length(),
                        pfx.length() - "___".length()).replace("__", "::");
            }
            TypedSpec read = new TypedPropertyAccess(rowVar, c.name(),
                    new ExprType(c.type(), c.multiplicity()));
            TypedSpec name = new TypedCString(simpleName(fqn), str1);
            body = body == null
                    ? new TypedIf(read, name, Optional.empty(), str1)
                    : new TypedIf(read, name, Optional.of(body), str1);
        }
        if (body == null) {
            body = new TypedCString(simpleName(baseClassFqn), str1);
        }
        // the column IS a type value (M3: GenericType.rawType : Type) — its
        // cell holds the class's simple name (the type-value wire convention)
        // and judges as a type, never as a string
        Type typeValue = new Type.ClassType("meta::pure::metamodel::type::Type");
        TypedLambda lam = new TypedLambda(List.of(v), List.of(body),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(row, one)),
                        new Type.Param(typeValue, one)), one));
        Type.RelationType out = new Type.RelationType(
                List.of(new Type.Column("rawType", typeValue, one)), List.of());
        return new TypedProject(rel, List.of(new TypedFuncCol("rawType", lam)),
                new ExprType(Type.relation(out), one));
    }

    private static String simpleName(String fqn) {
        int cut = fqn.lastIndexOf("::");
        return cut < 0 ? fqn : fqn.substring(cut + 2);
    }
}
