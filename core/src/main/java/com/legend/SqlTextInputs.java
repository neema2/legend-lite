// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.base.Nullable;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * The toSQLString surface's STRUCTURED INPUTS, one reading across its
 * overloads (engine toSQLString.pure): the query lambda, the mapping
 * reference, the dialect argument and the runtime. The direct forms carry
 * them as arguments — {@code toSQLString(f, mapping, DatabaseType, ext)},
 * {@code toSQLStringPretty(f, mapping, runtime, ext)}; the qualified
 * property form {@code toSQL(f, mapping, runtime, ext).toSQLString(
 * dbType, tz, quote, format)} (:46 + :151) carries the lambda, the
 * mapping and the runtime on the {@code toSQL} HANDLE and names the
 * dialect as its own first argument (the engine reads
 * {@code $connection.type} off that same runtime). Consumers (the
 * K-routine's render, the sql-text verdict arm) read the inputs here,
 * never the argument positions.
 *
 * @param query the query argument (let-bound forms are the caller's to
 *     chase — the plan-text arm's rule)
 * @param mapping the mapping argument
 * @param dialect the DatabaseType argument (an enum literal names the
 *     dialect outright; anything else defers to the runtime)
 * @param runtime the runtime the connection is read from (the direct
 *     DatabaseType form has the enum here too — no connection, no
 *     post-processor hooks)
 * @param receiverForm true for the qualified-property form over a
 *     {@code toSQL} handle
 */
record SqlTextInputs(TypedSpec query, TypedSpec mapping, TypedSpec dialect,
        TypedSpec runtime, boolean receiverForm) {

    /** The inputs of a toSQLString-family call; null when the call is the
     * receiver form over something that is not a {@code toSQL} handle, or
     * a direct form short of its three structured arguments (a caller
     * that owns a wall throws its own). */
    static @Nullable SqlTextInputs of(com.legend.compiler.spec.NativeDispatch.RoutineCall call,
            List<TypedSpec> letPrefix) {
        String fqn = call.fqn();
        // the RECEIVER form: SQLResult's qualified property toSQLString(dbType,
        // dbTimeZone, quoteIdentifiers, format) — the routine implements it
        // (NativeFn.JavaRoutine.implementedDerived); the receiver is the
        // toSQL handle
        if (com.legend.builtin.NativeFn.JavaRoutine.ofDerived(fqn).isPresent()
                && call.args().size() == 5) {
            TypedSpec r = com.legend.compiler.spec.typed.Lets.bound(call.args().get(0), letPrefix);
            if (r instanceof TypedNativeCall h
                    && com.legend.compiler.element.type.PlatformTypes.TO_SQL
                            .equals(h.callee().qualifiedName())
                    && h.args().size() >= 3) {
                return new SqlTextInputs(h.args().get(0), h.args().get(1),
                        call.args().get(1), h.args().get(2), true);
            }
            return null;
        }
        if (call.args().size() < 3) {
            return null;
        }
        return new SqlTextInputs(call.args().get(0), call.args().get(1),
                call.args().get(2), call.args().get(2), false);
    }
}
