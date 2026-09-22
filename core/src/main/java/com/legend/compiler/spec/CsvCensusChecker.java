// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;


/**
 * {@code getRelationalCSVDataFromQuery(query, mapping)} — TDG lane S1
 * (docs/TDG_LANE_CHARTER.md): a COMPILE-TIME REFLECTION fact
 * (model-space: query AST + mapping &rarr; table/column demand, no
 * database — TENET_CHARTER C1.6, the {@code deactivate} precedent), so
 * it FOLDS here at type time: the checker validates the registered
 * signature, runs the production census
 * ({@code TestDataGenerator.necessaryColumns} — Java orchestrates; the
 * engine's pure body is the SPEC, never loaded), and emits the result
 * as INSTANCE LITERALS
 * ({@code ^RelationalCSVData(tables=[^RelationalCSVTable(...), ...])}).
 * Everything downstream is the ordinary pipeline: property reads fold
 * as resolver constant-folding, {@code sortBy} lowers as ORDER BY,
 * {@code map}/{@code joinStrings} lower as always — no executor seam,
 * no host walk, no shadow evaluator (both were built once and REVERTED;
 * charter anti-patterns 1 and 2).
 */
public final class CsvCensusChecker {

    private static final String DATA_FQN =
            "meta::relational::metamodel::data::RelationalCSVData";
    private static final String TABLE_FQN =
            "meta::relational::metamodel::data::RelationalCSVTable";

    private CsvCensusChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        // signature validation rides the generic path (the registered
        // catalog native at this FQN — validate-against-registered-
        // signature); the fold below replaces the emitted call
        // bind-once (family E): let-bound arguments resolve through the
        // alias channel to the inline view (SourceSubst — the mechanism
        // this wall had named as S4 work).
        java.util.List<com.legend.protocol.spec.ValueSpecification> args =
                SourceSubst.resolveStructuralArgs(af.parameters(), env);
        t.checkGeneric(af.withParameters(args), env);
        if (args.size() != 2
                || !(args.get(0) instanceof LambdaFunction qLam)
                || !(args.get(1) instanceof PackageableElementPtr mp)) {
            throw new TypeInferenceException(
                    "getRelationalCSVDataFromQuery folds at CHECK time and"
                            + " needs its query lambda and mapping reference"
                            + " INLINE at the call site");
        }
        // CARRIER, not computation (the deactivate pattern): the census
        // implementation lives ABOVE the compiler (testdatagen -> lineage
        // -> compiler.element; calling it here cycles the slices) — the
        // node captures the inputs and the orchestrator folds.
        return new com.legend.compiler.spec.typed.TypedCsvCensus(qLam,
                mp.fullPath(),
                new ExprType(new Type.ClassType(DATA_FQN),
                        Multiplicity.Bounded.ONE));
    }

    /** The carrier's FOLDED form — instance literals from the census
     * triples. COMPILER-minted (invariant 7: typed-HIR nodes are minted
     * only by compiler layers); the orchestration-time folder
     * (testdatagen) computes the triples and calls back down here. */
    public static TypedSpec literal(java.util.List<String[]> tables,
            ExprType info) {
        ExprType str = new ExprType(Type.Primitive.STRING,
                Multiplicity.Bounded.ONE);
        ExprType tblOne = new ExprType(new Type.ClassType(TABLE_FQN),
                Multiplicity.Bounded.ONE);
        java.util.List<TypedSpec> rows = new java.util.ArrayList<>(tables.size());
        for (String[] row : tables) {
            java.util.Map<String, TypedSpec> props = new java.util.LinkedHashMap<>();
            props.put("schema", new com.legend.compiler.spec.typed
                    .TypedCString(row[0], str));
            props.put("table", new com.legend.compiler.spec.typed
                    .TypedCString(row[1], str));
            props.put("values", new com.legend.compiler.spec.typed
                    .TypedCString(row[2], str));
            rows.add(new com.legend.compiler.spec.typed
                    .TypedNewInstance(TABLE_FQN, props, tblOne));
        }
        java.util.Map<String, TypedSpec> dataProps = new java.util.LinkedHashMap<>();
        dataProps.put("tables", new com.legend.compiler.spec.typed
                .TypedCollection(rows, new ExprType(new Type.ClassType(TABLE_FQN),
                        Multiplicity.Bounded.ZERO_MANY)));
        return new com.legend.compiler.spec.typed
                .TypedNewInstance(DATA_FQN, dataProps, info);
    }

    /** A STRING-collection literal (COMPILER-minted, invariant 7) —
     * the setUpDataSQLs constant fold's result carrier. */
    public static TypedSpec literalStrings(java.util.List<String> values,
            ExprType info) {
        ExprType str = new ExprType(Type.Primitive.STRING,
                Multiplicity.Bounded.ONE);
        java.util.List<TypedSpec> rows = new java.util.ArrayList<>(values.size());
        for (String v : values) {
            rows.add(new com.legend.compiler.spec.typed.TypedCString(v, str));
        }
        return new com.legend.compiler.spec.typed.TypedCollection(rows, info);
    }

    /** The {@code TypedTestDataGen} carrier's folded form (S2) —
     * COMPILER-minted, same invariant-7 contract as {@link #literal}.
     * {@code relationTree} is deliberately ABSENT: a read of it walls
     * loudly until a witness demands the tree as a value. */
    public static TypedSpec literalTestData(String dataCsvString,
            java.util.List<String> sqls, ExprType info) {
        return literalTestData(dataCsvString, sqls, info, null);
    }

    /** The folded carrier WITH its generator node kept as {@code source}
     * (batch 64): the sql-text verdict's chained-fetch arm re-runs the
     * generator for the hop's transcript rows (deterministic reads over
     * static seeds) — the fold discards the rows, never the call. */
    public static TypedSpec literalTestData(String dataCsvString,
            java.util.List<String> sqls, ExprType info,
            @com.legend.base.Nullable TypedSpec source) {
        ExprType str = new ExprType(Type.Primitive.STRING,
                Multiplicity.Bounded.ONE);
        java.util.List<TypedSpec> sqlRows = new java.util.ArrayList<>(sqls.size());
        for (String sql : sqls) {
            sqlRows.add(new com.legend.compiler.spec.typed
                    .TypedCString(sql, str));
        }
        java.util.Map<String, TypedSpec> props = new java.util.LinkedHashMap<>();
        props.put("dataCsvString", new com.legend.compiler.spec.typed
                .TypedCString(dataCsvString, str));
        props.put("sqls", new com.legend.compiler.spec.typed
                .TypedCollection(sqlRows, new ExprType(Type.Primitive.STRING,
                        Multiplicity.Bounded.ZERO_MANY)));
        if (source != null) {
            props.put("source", source);
        }
        return new com.legend.compiler.spec.typed.TypedNewInstance(
                "meta::relational::testDataGeneration::TestDataGenResult",
                props, info);
    }
}
