package com.legend.compiler.spec;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.PackageableElementPtr;

import java.util.List;

/**
 * A relational table reference {@code #>{db.TABLE}#} (engine
 * {@code TableReferenceChecker}). The parser desugars it to
 * {@code tableReference(PackageableElementPtr(db), CString(table))}; the table's
 * column schema resolves through {@code ModelContext.findTable} and types as a
 * bare {@code RelationType[1]} (the G-&alpha; relation source).
 *
 * <p><strong>The args are non-value syntactic forms</strong> (a store reference +
 * a physical table identifier), so they cannot be synthesized through the generic
 * value path &mdash; but a special form still <strong>must not bypass the
 * registered signature</strong>: the call is validated against
 * {@code tableReference(String[1], String[1]):Relation<Any>[1]} before extracting,
 * and the result MULTIPLICITY is sourced from the resolved return. Only the TYPE
 * is bespoke (the generic {@code Relation<Any>} cannot carry the resolved columns).
 */
final class TableReferenceChecker {

    private TableReferenceChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af) {
        if (af.parameters().size() != 2
                || !(af.parameters().get(0) instanceof PackageableElementPtr dbRef)
                || !(af.parameters().get(1) instanceof CString tableName)) {
            throw new TypeInferenceException(
                    "tableReference expects (database, 'TABLE'); got " + af.parameters());
        }
        // Validate the call against the registered native signature — never ignored.
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.TABLE_REFERENCE.parseName()),
                List.of(ExprType.one(Type.Primitive.STRING), ExprType.one(Type.Primitive.STRING)));

        Type.RelationType schema = t.model().findTable(dbRef.fullPath(), tableName.value())
                .orElseThrow(() -> new TypeInferenceException(
                        "unknown table '" + tableName.value() + "' in database '" + dbRef.fullPath() + "'"));
        return new TypedTableReference(dbRef.fullPath(), tableName.value(),
                new ExprType(schema, sig.output().multiplicity()));
    }
}
