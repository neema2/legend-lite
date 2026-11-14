// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

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
 * the checker validates the registered signature and emits a CARRIER
 * ({@code TypedCsvCensus}) of the query and the mapping. The walk that
 * folded it in Java was deleted (2026-11-06) and its census with it
 * (2026-11-07, the P1 audit's 5): a body holding the carrier is refused
 * by name ({@code BodyClass.Kind#CSV_CENSUS}, the test-data generator's
 * skipped tests, a ruled register row).
 */
public final class CsvCensusChecker {

    private static final String DATA_FQN =
            "meta::relational::metamodel::data::RelationalCSVData";

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
}
