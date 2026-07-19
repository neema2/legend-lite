package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.CString;
import com.legend.model.spec.PackageableElementPtr;

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
        int n = af.parameters().size();
        // Two spellings: the #>{db.TABLE}# desugar (db, 'TABLE') and the
        // REAL engine 3-arg form (db, 'SCHEMA', 'TABLE') —
        // storeContract.pure tableReference_Database_1__String_1__String_1__Table_1_.
        // A 'default' schema collapses to the bare name; any other schema
        // qualifies dotted (the ~mainTable convention findTableDef matches).
        if ((n != 2 && n != 3)
                || !(af.parameters().get(0) instanceof PackageableElementPtr dbRef)
                || !(af.parameters().get(n - 1) instanceof CString tableName)
                || (n == 3 && !(af.parameters().get(1) instanceof CString))) {
            throw new TypeInferenceException(
                    "tableReference expects (database, ['SCHEMA',] 'TABLE'); got "
                    + af.parameters());
        }
        String name = tableName.value();
        boolean strictDefault = false;
        if (n == 3) {
            String schemaName = ((CString) af.parameters().get(1)).value();
            if (schemaName.isEmpty() || "default".equals(schemaName)) {
                // ENGINE: schema('default') = TOP-LEVEL tables only
                // (audit 22b F4 — the bare-name lookup fell back to ANY
                // schema, resolving what the engine rejects). Validate
                // through the strict 'default.' spelling; the CARRIED
                // name stays bare (the top-level SQL spelling).
                name = "default." + name;
                strictDefault = true;
            } else {
                name = schemaName + "." + name;
            }
        }
        // Validate the call against the registered native signature — never ignored.
        java.util.List<ExprType> sigArgs = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            sigArgs.add(ExprType.one(Type.Primitive.STRING));
        }
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.TABLE_REFERENCE.parseName()), sigArgs);

        final String resolvedName = name;
        Type.RelationType schema = t.model().findTable(dbRef.fullPath(), resolvedName)
                .orElseThrow(() -> new TypeInferenceException(
                        "unknown table '" + resolvedName + "' in database '" + dbRef.fullPath() + "'"));
        String carried = strictDefault ? tableName.value() : resolvedName;
        return new TypedTableReference(dbRef.fullPath(), carried,
                new ExprType(schema, sig.output().multiplicity()));
    }

    /**
     * {@code tableToTDS(table)} — the engine's Table&rarr;TDS wrapper
     * (tableToTDS.pure:22). Over OUR carrier the table reference already
     * IS the relation value: validated against the registered signature,
     * emitted as IDENTITY (the source, schema preserved — downstream
     * project/rows reads keep the resolved columns). A non-relation
     * argument is loud.
     */
    static TypedSpec checkTableToTds(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 1) {
            throw new TypeInferenceException(
                    "tableToTDS expects one table argument; got " + af.parameters().size());
        }
        TypedSpec table = t.synth(af.parameters().get(0), env);
        if (!(table.info().type() instanceof Type.RelationType)) {
            throw new TypeInferenceException(
                    "tableToTDS expects a table reference; got "
                    + table.info().type().typeName());
        }
        // Validate against the registered native signature — never ignored.
        t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.TABLE_TO_TDS.parseName()),
                List.of(table.info()));
        return table;
    }
}
