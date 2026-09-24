// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A candidate's signature RENAMED APART from its call site (α-renaming). The
 * kernel keys every binding by variable NAME, and one call unifies two
 * scopes: the callee's own type and multiplicity parameters, and the free
 * variables the arguments carry — the ENCLOSING function's own parameters.
 * When the names coincide they are still different variables, but a
 * name-keyed binding cannot tell them apart. The standard-library census
 * found the case 548 times: every PCT test declared {@code test<T|m>(f:
 * Function<{Function<{->T[m]}>[1]->T[m]}>[1])} calls {@code $f->eval(...)},
 * whose own declaration is {@code eval<T,V|m,n>}, and the caller's {@code T}
 * was taken for {@code eval}'s. The same tests spelled {@code <Z|y>} typed.
 *
 * <p>Only a COLLIDING name is renamed, to a spelling no argument uses; a
 * candidate with no collision is returned unchanged. The renamed signature
 * serves unification and the output type only — the RESOLVED declaration a
 * call node carries stays the original.
 */
final class SignatureApart {

    private SignatureApart() {
    }

    /** {@code c}, with any type or multiplicity parameter whose name is free
     * in {@code args} (or {@code expected}) renamed to a fresh spelling. */
    static TypedFunction of(TypedFunction c, List<ExprType> args,
            @com.legend.Nullable Type expected) {
        if (c.typeParameters().isEmpty() && c.multiplicityParameters().isEmpty()) {
            return c;
        }
        Set<String> freeTypes = new HashSet<>();
        Set<String> freeMults = new HashSet<>();
        for (ExprType a : args) {
            collect(a.type(), freeTypes, freeMults);
            collect(a.multiplicity(), freeMults);
        }
        if (expected != null) {
            collect(expected, freeTypes, freeMults);
        }
        Map<String, String> types = fresh(c.typeParameters(), freeTypes);
        Map<String, String> mults = fresh(c.multiplicityParameters(), freeMults);
        if (types.isEmpty() && mults.isEmpty()) {
            return c;
        }
        List<TypedParameter> params = new ArrayList<>(c.parameters().size());
        for (TypedParameter p : c.parameters()) {
            params.add(new TypedParameter(p.name(), rename(p.type(), types, mults),
                    rename(p.multiplicity(), mults)));
        }
        return new TypedFunction(c.qualifiedName(),
                c.typeParameters().stream().map(n -> types.getOrDefault(n, n)).toList(),
                c.multiplicityParameters().stream().map(n -> mults.getOrDefault(n, n)).toList(),
                params, rename(c.returnType(), types, mults), rename(c.returnMultiplicity(), mults),
                c.body(), c.isNative(), c.definition());
    }

    /** For each of {@code declared} that is free at the call site, a spelling
     * that is neither free there nor declared. */
    private static Map<String, String> fresh(List<String> declared, Set<String> free) {
        Map<String, String> out = new HashMap<>();
        for (String n : declared) {
            if (!free.contains(n)) {
                continue;
            }
            String f = n + "'";
            while (free.contains(f) || declared.contains(f)) {
                f = f + "'";
            }
            out.put(n, f);
        }
        return out;
    }

    private static void collect(Type t, Set<String> types, Set<String> mults) {
        switch (t) {
            case Type.TypeVar v -> types.add(v.name());
            case Type.GenericType g -> {
                g.arguments().forEach(a -> collect(a, types, mults));
                g.multArguments().forEach(m -> collect(m, mults));
            }
            case Type.FunctionType f -> {
                for (Type.Param p : f.params()) {
                    collect(p.type(), types, mults);
                    collect(p.multiplicity(), mults);
                }
                collect(f.result().type(), types, mults);
                collect(f.result().multiplicity(), mults);
            }
            case Type.RelationType r -> {
                for (Type.Column c : r.columns()) {
                    collect(c.type(), types, mults);
                    collect(c.multiplicity(), mults);
                }
                for (Type.Column c : r.dynamicColumns()) {
                    collect(c.type(), types, mults);
                    collect(c.multiplicity(), mults);
                }
            }
            case Type.SchemaAlgebra s -> {
                collect(s.left(), types, mults);
                collect(s.right(), types, mults);
            }
            default -> {
            }
        }
    }

    private static void collect(Multiplicity m, Set<String> mults) {
        if (m instanceof Multiplicity.Var v) {
            mults.add(v.name());
        }
    }

    private static Type rename(Type t, Map<String, String> types, Map<String, String> mults) {
        return switch (t) {
            case Type.TypeVar v -> types.containsKey(v.name()) ? new Type.TypeVar(types.get(v.name())) : v;
            case Type.GenericType g -> new Type.GenericType(g.rawFqn(),
                    g.arguments().stream().map(a -> rename(a, types, mults)).toList(),
                    g.multArguments().stream().map(m -> rename(m, mults)).toList());
            case Type.FunctionType f -> new Type.FunctionType(
                    f.params().stream().map(p -> new Type.Param(rename(p.type(), types, mults),
                            rename(p.multiplicity(), mults))).toList(),
                    new Type.Param(rename(f.result().type(), types, mults),
                            rename(f.result().multiplicity(), mults)));
            case Type.RelationType r -> new Type.RelationType(
                    r.columns().stream().map(c -> rename(c, types, mults)).toList(),
                    r.dynamicColumns().stream().map(c -> rename(c, types, mults)).toList());
            case Type.SchemaAlgebra s -> new Type.SchemaAlgebra(rename(s.left(), types, mults), s.op(),
                    rename(s.right(), types, mults));
            default -> t;
        };
    }

    private static Type.Column rename(Type.Column c, Map<String, String> types, Map<String, String> mults) {
        return new Type.Column(c.name(), rename(c.type(), types, mults), rename(c.multiplicity(), mults));
    }

    private static Multiplicity rename(Multiplicity m, Map<String, String> mults) {
        return m instanceof Multiplicity.Var v && mults.containsKey(v.name())
                ? new Multiplicity.Var(mults.get(v.name())) : m;
    }
}
