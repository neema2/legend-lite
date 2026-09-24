package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.PackageableElementPtr;

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

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
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
        // Validate the call against the registered native signature — never
        // ignored: upstream's tableReference(database:Database[1],
        // schemaName:String[1], tableName:String[1]) — the #>{db.T}# desugar
        // is the 'default' schema spelled once
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.TABLE_REFERENCE.parseName()),
                java.util.List.of(
                        ExprType.one(new Type.ClassType(
                                com.legend.compiler.element.type.PlatformTypes.DATABASE)),
                        ExprType.one(Type.Primitive.STRING),
                        ExprType.one(Type.Primitive.STRING)));

        final String resolvedName = name;
        java.util.Optional<Type.RelationType> table = t.model().findTable(dbRef.fullPath(), resolvedName);
        if (table.isEmpty()) {
            // #>{db.View}# — a VIEW is a lifted zero-arg relation function
            // (E.5, docs/VIEWS_COMPILED_ONCE_HOMEWORK_2026_09_22.md §7): its
            // body IS the relation, typed here the way the ~func mapping
            // route consumes a relation function (FromChecker's zero-arg
            // user-call splice) — every user call inlines, a view's too.
            // Reached through the include closure like a table (the lifted
            // function is named by the OWNING database).
            String viewName = strictDefault ? tableName.value() : resolvedName;
            java.util.Optional<com.legend.compiler.element.TypedFunction> lifted =
                    t.model().findViewFunction(dbRef.fullPath(), viewName);
            if (lifted.isPresent()) {
                // a CALL to the lifted function, typed from its DECLARED
                // signature like every call (the view's relation type, read
                // off store facts by the lift — ViewSignatures; the compiler
                // checks the body against it once). Every lowering path
                // inlines user calls, and the inliner keeps a view's name on
                // the inlined body (TypedViewRelation).
                return new com.legend.compiler.spec.typed.TypedUserCall(
                        lifted.get(), List.of(), new ExprType(
                                lifted.get().returnType(), lifted.get().returnMultiplicity()));
            }
            throw new TypeInferenceException(
                    "unknown table '" + resolvedName + "' in database '" + dbRef.fullPath() + "'");
        }
        Type.RelationType schema = table.get();
        String carried = strictDefault ? tableName.value() : resolvedName;
        // the columns the DDL declared quoted: a spelling the SQL keeps
        var def = t.model().findTableDefinition(dbRef.fullPath(), resolvedName);
        java.util.Set<String> quoted = def
                .map(d -> d.columns().stream().filter(c -> c.quoted()).map(c -> c.name())
                        .collect(java.util.stream.Collectors.toUnmodifiableSet()))
                .orElse(java.util.Set.of());
        // a TABULAR FUNCTION is read as a call
        boolean call = def.map(d -> d.function()).orElse(false);
        return new TypedTableReference(dbRef.fullPath(), carried,
                // the literal IS the store accessor (upstream: RelationStoreAccessor<T>
                // extends Relation<T>) — every Relation<T> formal admits it through the
                // declared hierarchy, and write's RelationElementAccessor<T> demands it
                new ExprType(new Type.GenericType(
                        com.legend.compiler.element.type.PlatformTypes.RELATION_STORE_ACCESSOR,
                        java.util.List.of(schema)), sig.output().multiplicity()),
                n == 2, null, quoted, call);
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
        if (!Type.isRelation(table.info().type())) {
            throw new TypeInferenceException(
                    "tableToTDS expects a table reference; got "
                    + table.info().type().typeName());
        }
        // ENGINE: tableToTDS(table:Table[1]) — a relation EXPRESSION is
        // not a Table (audit 22b F5: the leniency accepted
        // tableToTDS(#>{db.T}#->filter(...)) that the engine refuses to
        // compile). Loud beats silently-wider.
        if (!(table instanceof com.legend.compiler.spec.typed
                .TypedTableReference)) {
            throw new TypeInferenceException(
                    "tableToTDS expects a TABLE reference (engine"
                    + " Table[1]); a derived relation expression is not a"
                    + " Table");
        }
        // Validate against the registered native signature — never ignored:
        // upstream's tableToTDS(table:Table[1]) takes the TABLE the accessor
        // denotes (RelationStoreAccessor.sourceElement), proven just above
        // to be a table reference — so the signature sees a Table[1]
        t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.TABLE_TO_TDS.parseName()),
                List.of(ExprType.one(new Type.ClassType(
                        com.legend.compiler.element.type.PlatformTypes.RELATIONAL_TABLE))));
        return table;
    }
}
