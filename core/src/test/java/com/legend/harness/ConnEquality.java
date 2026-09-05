// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.ValueSpecification;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The connection-equality contract (engine storeContract.pure:55-65),
 * HOST-evaluated: {@code runRelationalRouterExtensionConnectionEquality
 * (c1, c2)} compares two {@code ^RelationalDatabaseConnection} literals
 * structurally — {@code type}, {@code timeZone}, {@code
 * quoteIdentifiers}, {@code datasourceSpecification} (deep instance
 * equality), {@code authenticationStrategy}
 * (compareObjectsWithPossiblyNoProperties: both-absent is equal) and
 * {@code postProcessors} — while the {@code element} (store name) is
 * excluded by the engine's own rule. The tests never execute SQL; the
 * routed extension machinery (routerExtensions &rarr; match over
 * eval'd lambdas) exists only to DISPATCH to this comparison, so the
 * harness owns the terminal rule directly.
 *
 * <p>{@link #tryEval} returns null when the expression is not this
 * call — the assert falls through to the ordinary pipeline.
 */
final class ConnEquality {

    private ConnEquality() {
    }

    /** THE assert-arm host fold dispatcher: connection equality (the
     * M3-reflection predicate arm died with group H — the expression
     * tree is rows, expressionSequenceReturnsAtLeastToOneDataType a
     * Pure body over them). */
    static @com.legend.Nullable Boolean tryEval(
            @com.legend.Nullable ValueSpecification v,
            com.legend.compiler.element.ModelContext ctx,
            com.legend.model.ImportScope imports) {
        boolean negate = false;
        if (v instanceof AppliedFunction nf
                && EngineTestExecutor.simpleName(nf.function()).equals("not")
                && nf.parameters().size() == 1) {
            v = nf.parameters().get(0);
            negate = true;
        }
        if (!(v instanceof AppliedFunction af
                && EngineTestExecutor.simpleName(af.function()).equals(
                        "runRelationalRouterExtensionConnectionEquality")
                && af.parameters().size() == 2)) {
            return null;
        }
        boolean eq = structEquals(af.parameters().get(0),
                af.parameters().get(1));
        return negate ? !eq : eq;
    }

    /** The LET-arm host folds: JSON-metamodel plumbing defers verbatim
     * (to the assert) and predicate VERDICTS bind as booleans — one funnel
     * so the EngineTestExecutor let arm stays a single call (file at its
     * size cap). (The generateObjectReferences builder left with batch
     * 72b: the platform reads the generator's spelled pk maps.) */
    static @com.legend.Nullable ValueSpecification letFold(
            ValueSpecification rhs, ValueSpecification substituted,
            com.legend.compiler.element.ModelContext ctx,
            com.legend.model.ImportScope imports) {
        if (JsonAssertCanon.isPlumbing(rhs)) {
            return rhs;
        }
        Boolean hf = tryEval(substituted, ctx, imports);
        return hf == null ? null : new com.legend.protocol.spec.CBoolean(hf);
    }

    /** Deep instance equality over the SUBSTITUTED literals: instances
     * compare per-key (union of keys; a key absent on both sides is
     * equal — the engine's no-properties tolerance), {@code element}
     * never compares; everything else rides record equality. */
    private static boolean structEquals(
            @com.legend.Nullable ValueSpecification a,
            @com.legend.Nullable ValueSpecification b) {
        if (a instanceof NewInstance na && b instanceof NewInstance nb) {
            if (!na.className().equals(nb.className())) {
                return false;
            }
            Set<String> keys = new LinkedHashSet<>();
            na.properties().forEach(kb2 -> keys.add(kb2.key()));
            nb.properties().forEach(kb2 -> keys.add(kb2.key()));
            keys.remove("element");
            for (String k : keys) {
                KeyExpression ka = na.first(k);
                KeyExpression kb = nb.first(k);
                if (!structEquals(ka == null ? null : ka.value(),
                        kb == null ? null : kb.value())) {
                    return false;
                }
            }
            return true;
        }
        return java.util.Objects.equals(a, b);
    }
}
