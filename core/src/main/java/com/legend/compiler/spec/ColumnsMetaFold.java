// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedProperty;
import java.util.List;

/**
 * TDS COLUMN-METADATA folds — {@code .columns.name}, {@code .columns.type},
 * {@code .columns.documentation} over a typed relation are STATIC FACTS of
 * its schema (engine TabularDataSet.columns.name/.type; col()'s optional
 * documentation): they fold to string collections at typing, never
 * execute. Split from Typer at the file-size seam (batch 102).
 */
final class ColumnsMetaFold {
    private ColumnsMetaFold() {
    }

    static TypedSpec columnsMeta(Type.RelationType rt, boolean typeNames) {
        ExprType one = ExprType.one(Type.Primitive.STRING);
        List<TypedSpec> items = new java.util.ArrayList<>(rt.columns().size());
        for (Type.RelationType.Column c : rt.columns()) {
            if (typeNames) {
                // TDSColumn.type : Type[0..1] (tds.pure) — a TYPE VALUE, the
                // same node a type written as a value is (Typer.typeRef);
                // it lowers to its simple name and judges as a type
                items.add(new com.legend.compiler.spec.typed.TypedTypeRef(c.type(),
                        ExprType.one(c.type())));
            } else {
                items.add(new com.legend.compiler.spec.typed.TypedCString(c.name(), one));
            }
        }
        Type elem = typeNames
                ? new Type.ClassType("meta::pure::metamodel::type::Type")
                : Type.Primitive.STRING;
        return new com.legend.compiler.spec.typed.TypedCollection(items,
                new ExprType(elem,
                        new com.legend.compiler.element.type.Multiplicity.Bounded(
                                items.size(), items.size())));
    }

    /** TDS COLUMN-METADATA folds ({@code .columns.name/.type/
     * .documentation} — static facts of the typed relation); null when
     * the access is not one of these. */
    static @com.legend.base.Nullable TypedSpec read(Typer t, AppliedProperty ap, Env env) {
        // TDS COLUMN METADATA — engine TabularDataSet.columns.name/.type.
        // Column names and pure type names are STATIC FACTS of the typed
        // relation (no execution): they fold to string collections here.
        if (ap.receiver() instanceof AppliedProperty inner
                && inner.property().equals("columns")
                && (ap.property().equals("name") || ap.property().equals("type"))) {
            TypedSpec rel = t.synth(inner.receiver(), env);
            if (Type.schemaView(rel.info().type()) instanceof Type.RelationType rt) {
                return columnsMeta(rt, ap.property().equals("type"));
            }
        }
        // .columns.documentation — col()'s optional metadata (TDSColumn
        // .documentation is String[0..1]: undocumented columns FLATTEN
        // away). A static fact of the PROJECT node, like name/type above.
        if (ap.receiver() instanceof AppliedProperty inner2
                && inner2.property().equals("columns")
                && ap.property().equals("documentation")) {
            TypedSpec rel = t.synth(inner2.receiver(), env);
            TypedSpec un = rel;
            // column metadata is invariant under ROW ops — walk through
            // from() rescopes and relation-in/relation-out wrappers
            // (at/toOne/first — the Result-envelope peel) to the project
            boolean walked = true;
            while (walked) {
                walked = false;
                if (un instanceof com.legend.compiler.spec.typed.TypedFrom f) {
                    un = f.source();
                    walked = true;
                } else if (un instanceof TypedNativeCall w
                        && !w.args().isEmpty()
                        && Type.isRelation(w.args().get(0).info().type())) {
                    un = w.args().get(0);
                    walked = true;
                }
            }
            if (Type.isRelation(rel.info().type())) {
                if (un instanceof com.legend.compiler.spec.typed.TypedProject tp) {
                    return tp.docsFold();
                }
                // an ENVELOPE read ($result.values->at(0)...): the project
                // is only visible after the K-side splice (G-half) — emit
                // the identity-typed MARKER the splice hook resolves (the
                // .rows-marker discipline, audit 19d B2)
                return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                        rel, "columns.documentation",
                        new ExprType(Type.Primitive.STRING,
                                com.legend.compiler.element.type.Multiplicity
                                        .Bounded.ZERO_MANY));
            }
        }
        return null;
    }
}
