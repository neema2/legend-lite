// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code relation::columns(rel)} (REAL declaration: PCT.platformOnly,
 * {@code Column<Nil,Any|*>[*]}) — relation COLUMN METADATA. The schema
 * is STATIC at compile time, so the call FOLDS to a literal collection
 * of {@code Column} instances whose {@code name} is the typed schema's
 * column name (the Lowerer's instance-property arm folds {@code .name}
 * reads; witnesses testGenerateGuidWithRelation, testHashCodeAggregate).
 * A LATE-BOUND relation walls loudly — its columns exist only at the
 * execution boundary.
 */
final class ColumnsChecker {

    private static final String COLUMN_FQN =
            "meta::pure::metamodel::relation::Column";

    private ColumnsChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 1) {
            throw new TypeInferenceException(
                    "columns(rel) takes exactly the relation");
        }
        TypedSpec rel = t.synth(af.parameters().get(0), env);
        // validate against the REGISTERED native signature — never bypassed
        t.kernel().resolveOverload(
                t.model().findFunction(
                        "meta::pure::functions::relation::columns"),
                List.of(rel.info()));
        Type.RelationType rt = Type.relationSchema(rel.info().type());
        if (rt == null) {
            throw new TypeInferenceException("columns() needs a relation,"
                    + " got " + rel.info().type().typeName());
        }
        if (rt.isLateBound()) {
            throw new com.legend.error.NotImplementedException(
                    "columns() over a LATE-BOUND relation — its columns"
                    + " exist only at the execution boundary");
        }
        Type colType = new Type.ClassType(COLUMN_FQN);
        List<TypedSpec> cols = new ArrayList<>(rt.columns().size());
        for (Type.Column c : rt.columns()) {
            cols.add(new TypedNewInstance(COLUMN_FQN,
                    Map.of("name", new TypedCString(c.name(),
                            ExprType.one(Type.Primitive.STRING))),
                    ExprType.one(colType)));
        }
        return new TypedCollection(cols, new ExprType(colType,
                new Multiplicity.Bounded(cols.size(), cols.size())));
    }
}
