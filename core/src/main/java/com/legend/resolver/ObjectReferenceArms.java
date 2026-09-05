// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.resolver;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.List;
import java.util.Map;

/**
 * The objectReferenceIn ARMS beyond spelled reference strings (batch
 * 72b; extracted from {@link Substitution} at the file guardrail): the
 * engine's generator calls read as spelled pk maps, references computed
 * at runtime test pk membership over the database's decode, and the
 * lite readers get their callees minted from the catalog definition.
 */
final class ObjectReferenceArms {

    private ObjectReferenceArms() {
    }

    /** References computed AT RUNTIME (a column of an earlier result's
     * rows, a take() over it): the engine decodes them into pk values
     * and tests membership (processObjectReferenceInOperation's temp
     * table of pk values) — here the decode is the database's:
     * {@code in(pk, refs->map(r | asorPkValue(r, 0)))}, the reader typed
     * as the pk column so the SQL casts the JSON text to it. Single-pk
     * sets (the engine's multi-pk form concatenates with ','). */
    static TypedSpec runtimeRefMembership(TypedNativeCall oc, TypedSpec refs,
            TypedSpec pk, String rv, TypedFunction inCallee) {
        ExprType pkOne = new ExprType(pk.info().type(), Multiplicity.Bounded.ONE);
        var str1 = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
        var reader = liteCallee(com.legend.builtin.Pure.Lite.ASOR_PK_VALUE,
                List.of(new com.legend.compiler.element.TypedParameter("ref",
                                Type.Primitive.STRING, Multiplicity.Bounded.ONE),
                        new com.legend.compiler.element.TypedParameter("index",
                                Type.Primitive.INTEGER, Multiplicity.Bounded.ONE)),
                new Type.ClassType("meta::pure::metamodel::type::Any"));
        TypedSpec body = new TypedNativeCall(reader, List.of(
                new TypedVariable(rv, str1),
                new com.legend.compiler.spec.typed.TypedCInteger(0,
                        new ExprType(Type.Primitive.INTEGER, Multiplicity.Bounded.ONE))),
                pkOne);
        var ft = new Type.FunctionType(
                List.of(new Type.Param(Type.Primitive.STRING, Multiplicity.Bounded.ONE)),
                new Type.Param(pk.info().type(), Multiplicity.Bounded.ONE));
        TypedSpec values = new com.legend.compiler.spec.typed.TypedMap(refs,
                new com.legend.compiler.spec.typed.TypedLambda(List.of(rv),
                        List.of(body), new ExprType(ft, Multiplicity.Bounded.ONE)),
                new ExprType(pk.info().type(), Multiplicity.Bounded.ZERO_MANY));
        return new TypedNativeCall(inCallee, List.of(pk, values), oc.info());
    }

    /** A callee for a lite-internal native minted here (the resolver has
     * no model context; the catalog definition IS the dispatch key). */
    static TypedFunction liteCallee(String fqn,
            List<com.legend.compiler.element.TypedParameter> params, Type ret) {
        var defs = com.legend.builtin.Pure.nativeFunctionsAt(fqn);
        if (defs.size() != 1) {
            throw new IllegalStateException(fqn + " is not a registered lite native");
        }
        return new TypedFunction(fqn, List.of(), List.of(), params, ret,
                Multiplicity.Bounded.ONE, java.util.Optional.empty(), true, defs.get(0));
    }

    private static final String GENERATE_REFS =
            "meta::alloy::objectReference::generateObjectReferences";
    private static final String GENERATE_REFS_FOR_SET =
            "meta::alloy::objectReference::generateObjectReferencesForGivenSetId";

    /** The spelled pk maps of a generator call — {@code pkMaps} is the
     * 5th (6-arg) or 6th (7-arg) argument: {@code newMap(pair(k, v))},
     * {@code newMap([pair, pair])}, or a collection of such maps; a
     * collection of generator calls concatenates. Null = not a generator
     * shape; a generator whose pk maps are not spelled is loud. */
    static @com.legend.Nullable List<Map<String, Object>> generatorPkMaps(TypedSpec v) {
        if (v instanceof com.legend.compiler.spec.typed.TypedCollection tc) {
            if (tc.elements().isEmpty() || !tc.elements().stream().allMatch(
                    e -> e instanceof TypedNativeCall c && isGenerator(c))) {
                return null;
            }
            List<Map<String, Object>> all = new java.util.ArrayList<>();
            for (TypedSpec e : tc.elements()) {
                all.addAll(java.util.Objects.requireNonNull(generatorPkMaps(e)));
            }
            return all;
        }
        if (!(v instanceof TypedNativeCall g) || !isGenerator(g)) {
            return null;
        }
        TypedSpec maps = g.args().get(g.args().size() - 2);
        List<TypedSpec> mapNodes = maps instanceof
                com.legend.compiler.spec.typed.TypedCollection mc
                ? mc.elements() : List.of(maps);
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        for (TypedSpec mn : mapNodes) {
            if (!(mn instanceof TypedNativeCall nm)
                    || !"meta::pure::functions::collection::newMap"
                            .equals(nm.callee().qualifiedName())
                    || nm.args().size() != 1) {
                throw new NotImplementedException("generateObjectReferences pk"
                        + " maps must be spelled newMap(pair(..)) calls, got "
                        + mn.getClass().getSimpleName());
            }
            List<TypedSpec> pairs = nm.args().get(0) instanceof
                    com.legend.compiler.spec.typed.TypedCollection pc
                    ? pc.elements() : List.of(nm.args().get(0));
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            for (TypedSpec pn : pairs) {
                if (!(pn instanceof TypedNativeCall pr)
                        || !"meta::pure::functions::collection::pair"
                                .equals(pr.callee().qualifiedName())
                        || pr.args().size() != 2
                        || !(pr.args().get(0) instanceof
                                com.legend.compiler.spec.typed.TypedCString k)) {
                    throw new NotImplementedException("generateObjectReferences pk"
                            + " map entry is not a spelled pair(key, value)");
                }
                Object val = switch (pr.args().get(1)) {
                    case com.legend.compiler.spec.typed.TypedCString cs -> cs.value();
                    case com.legend.compiler.spec.typed.TypedCInteger ci ->
                            ci.value().longValue();
                    default -> throw new NotImplementedException(
                            "generateObjectReferences pk value kind: "
                                    + pr.args().get(1).getClass().getSimpleName());
                };
                m.put(k.value(), val);
            }
            out.add(m);
        }
        return out;
    }

    private static boolean isGenerator(TypedNativeCall c) {
        String q = c.callee().qualifiedName();
        return (GENERATE_REFS.equals(q) && c.args().size() == 6)
                || (GENERATE_REFS_FOR_SET.equals(q) && c.args().size() == 7);
    }

}
