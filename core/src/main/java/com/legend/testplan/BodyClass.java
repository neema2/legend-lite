// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.model.FunctionId;
import com.legend.planner.ConnectionLets;
import com.legend.planner.QueryDoor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE CLASSES OF BODIES THAT RUN WHOLE ON THE PROGRAM ROAD (the test-is-the-product plan's R16, brief B1, M5): a body
 * is planned whole by the test door and run as one plan when it falls in a class here, by a named rule over its typed
 * shape -- never by trying the test door and falling back on a failure. A body of a moved class the test door refuses
 * FAILS (R14). The classes are one closed list that only grows ({@code BodyClassTest} pins it); the walk runs every
 * other body until M10 moves the last class.
 */
public enum BodyClass {

    /**
     * An EFFECT PROGRAM (M5, the corpus's setup programs): every statement is a database effect whose String
     * arguments are literals (each folded over the lets before it), a let of one that later statements read only as a
     * connection, a value let or statement that reads no store, a {@code print}; at least one effect; no assert. The
     * test door plans each as the walk sends it (M3d: every script byte-identical).
     */
    EFFECT_PROGRAM;

    /** {@code body}'s class on {@code runtime}, or null when the walk runs it. */
    public static @Nullable BodyClass of(TypedQuery body, String runtime) {
        return effectProgram(body, runtime) ? EFFECT_PROGRAM : null;
    }

    private static boolean effectProgram(TypedQuery query, String runtime) {
        QueryDoor.Scan scan = new QueryDoor.Scan(query, runtime);
        List<TypedSpec> body = query.body();
        Map<FunctionId, Boolean> storeMemo = new HashMap<>();
        boolean anEffect = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            if (kinds.contains(QueryDoor.TestOnly.ASSERT) || kinds.contains(QueryDoor.TestOnly.ASSERT_ERROR)) {
                return false;
            }
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            if (value instanceof TypedNativeCall call && NativeFn.Effect.isDbEffect(call.callee().id())) {
                if (!literalEffect(call, body.subList(0, i))
                        || statement instanceof TypedLet let
                                && !ConnectionLets.onlyConnectionReads(body, i + 1, let.name())) {
                    return false;
                }
                anEffect = true;
                continue;
            }
            if (value instanceof TypedNativeCall call && NativeFn.Effect.isInertDiagnostic(call.callee().id())) {
                continue;
            }
            // anything else: a value that reads no store and holds no test kind (a connection, a runtime, a
            // constant)
            if (!kinds.isEmpty() || readsAStore(statement, query, storeMemo)) {
                return false;
            }
        }
        return anEffect;
    }

    /** Whether {@code call}, a database effect, is one the test door writes: each String argument a literal (folded
     *  over {@code lets}, the statements before it), and not {@code loadCsvToDbTable} (its CSV a test resource). */
    private static boolean literalEffect(TypedNativeCall call, List<TypedSpec> lets) {
        if (NativeFn.Effect.of(call.callee().id()).orElse(null) == NativeFn.Effect.LOAD_CSV_TO_DB_TABLE) {
            return false;
        }
        for (TypedSpec arg : EffectPlanner.stringArgs(call)) {
            if (!(Literals.fold(arg, lets) instanceof String)) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code statement} reaches a store read: a class's extent, a table, a {@code from()} (through the user
     *  functions it calls). */
    private static boolean readsAStore(TypedSpec statement, TypedQuery query, Map<FunctionId, Boolean> memo) {
        return StatementEffects.reaches(statement, n -> n instanceof TypedGetAll || n instanceof TypedTableReference
                || n instanceof TypedFrom, query.specs(), memo);
    }
}
