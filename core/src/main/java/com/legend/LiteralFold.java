// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.base.Nullable;

import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;

import java.util.Set;

/**
 * CONSTANT-PLAN classification (the engine's {@code ConstantExecutionNode},
 * lite-shaped): a root that is a BARE LITERAL of an ADMITTED KIND
 * compiles to its value; every other root lowers to SQL. Found by the
 * 2026-08-15 round-trip census: 344k of 357k corpus plan executions were
 * literals riding {@code SELECT '<text>' AS value} — identity with a
 * round-trip tax.
 *
 * <p>THE ADMISSION RULE — both conditions, per kind:
 * <ol>
 *   <li><b>Syntactic value, zero computation.</b> The value appears
 *       VERBATIM in the typed AST (a bare literal node). Never a
 *       composite, variable, or call — folding is UNWRAPPING a field,
 *       or it is the start of a shadow evaluator (the #1 tenet's line).</li>
 *   <li><b>Representation-trivial round trip.</b> The execution frame's
 *       value must be the AST value with NO rules applied. String and
 *       Boolean pass (JDBC identity). Integer FAILS (driver-width
 *       Integer/Long + lattice promotion + the BigInteger extension);
 *       Float/Decimal FAIL (scale/format rules); Date FAILS (partial
 *       dates travel as ISO-prefix string carriers). Folding those
 *       would duplicate a coercion rule in a second place.</li>
 * </ol>
 *
 * <p>The admitted set is PINNED by {@code ConstantPlanParityTest}: each
 * kind proves fold == SQL-path on the live backends, differentially —
 * admitting a kind is a green differential, not an argument. There is
 * no performance case for widening: post-fold the whole corpus retains
 * ~4k scalar executions (~0.7s).
 */
public final class LiteralFold {

    /** Admitted literal kinds — shrink-or-justify; the parity test
     *  enumerates exactly this set. */
    public static final Set<String> ADMITTED = Set.of("String", "Boolean");

    private LiteralFold() {
    }

    static @Nullable ExecutionResult fold(TypedSpec root) {
        if (root instanceof TypedCString s) {
            return new ExecutionResult.Scalar(s.value(), root.info().type());
        }
        if (root instanceof TypedCBoolean b) {
            return new ExecutionResult.Scalar(b.value(), root.info().type());
        }
        return null;
    }
}
