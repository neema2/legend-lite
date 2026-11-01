// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.SystemMetamodel;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.StatementEffects;
import com.legend.compiler.spec.typed.Literals;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedGraphFetch;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedRawSqlRelation;
import com.legend.compiler.spec.typed.TypedSerialize;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.FunctionId;
import com.legend.model.MappingDefinition;
import com.legend.planner.BodyPlanner;
import com.legend.planner.ConnectionLets;
import com.legend.planner.QueryDoor;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * THE CLASSES OF BODIES THAT RUN WHOLE ON THE PROGRAM ROAD (the test-is-the-product plan's R16, brief B1, M5): a body
 * is planned whole by the test door and run as one plan when it falls in a class here, by a named rule over its typed
 * shape -- never by trying the test door and falling back on a failure. A body of a moved class the test door refuses
 * FAILS (R14). The classes are one closed list that only grows ({@code BodyClassTest} pins it); the walk runs every
 * other body until M10 moves the last class.
 */
public enum BodyClass {

    /**
     * An EFFECT PROGRAM (M5, the corpus's setup programs): every statement is one of
     * <ul>
     *   <li>a database effect whose String arguments are literals (each folded over the lets before it), or a let of
     *       one that later statements read only as a connection;</li>
     *   <li>an orchestration handle (a connection, a runtime: {@code BodyPlanner.handleType}), a let of one or its
     *       value;</li>
     *   <li>a String or Boolean literal, a let of one or its value;</li>
     *   <li>a {@code print} whose arguments hold no test kind and read no store (never evaluated, on both roads);</li>
     * </ul>
     * with at least one effect, no assert, and nothing that seeds: no statement names test data (a {@code from()}),
     * and the runtime declares none (the walk keys its seed memo by the seed's sources, the runner by the seeds'
     * content, and orders a statement's seeds against an open script the other way round: the M5 audit's B1). The
     * test door plans each as the walk sends it (M3d: every script byte-identical; {@code BodyRouterParityTest}).
     */
    EFFECT_PROGRAM,

    /**
     * A QUERY PROGRAM (M5f): every statement is one the product's query door plans -- it holds no test kind (no
     * effect, no seeds, no let-bound {@code execute(...)}, no assert) -- and none is a kind the program road plans
     * another way yet: no {@code execute(...)} read where it stands (M9), no staged rows, no store write, no
     * test-data generation, no read of the system metamodel store (M4); the runtime declares no test data (M5's
     * audit B1). The body is a product program the test door plans as the query door would.
     */
    QUERY_PROGRAM;

    /** {@code body}'s class on {@code runtime}, or null when the walk runs it. */
    public static @Nullable BodyClass of(TypedQuery body, String runtime) {
        if (effectProgram(body, runtime)) {
            return EFFECT_PROGRAM;
        }
        return queryProgram(body, runtime) ? QUERY_PROGRAM : null;
    }

    private static boolean queryProgram(TypedQuery query, String runtime) {
        if (QueryDoor.declaresTestData(runtime, query.context())) {
            return false;
        }
        QueryDoor.Scan scan = new QueryDoor.Scan(query, runtime);
        Set<String> systemClasses = systemClasses(query.context());
        Map<FunctionId, Boolean> memo = new HashMap<>();
        for (TypedSpec statement : query.body()) {
            if (!scan.kinds(statement).isEmpty() || StatementEffects.containsTdgGenerator(statement)
                    || StatementEffects.reaches(statement, n -> plannedAnotherWay(n, systemClasses), query.specs(),
                            memo)) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code n} is a kind the program road plans another way than the walk yet: an {@code execute(...)},
     *  staged rows, a raw grid ({@code executeInDb}'s rows, typed as a raw-SQL relation: T8, T12), a store write; a
     *  read of the system metamodel store (M4) -- the extent of a class its mapping binds ({@code systemClasses}), a
     *  table of it, one of its functions, an element used as a value (a mapping, a store: its instance is the
     *  store's row); an object built or a graph answered (their ids and their answer's layout converge on the
     *  product's with MV6). */
    private static boolean plannedAnotherWay(TypedSpec n, Set<String> systemClasses) {
        return n instanceof TypedNativeCall call && (NativeFn.Handle.runsAFrame(call.callee().id())
                        || Pure.Lite.STAGE.equals(call.callee().qualifiedName()))
                || n instanceof TypedRawSqlRelation
                || StatementEffects.isStoreWrite(n)
                || n instanceof TypedGetAll all && (systemClasses.contains(all.classFqn())
                        || SystemMetamodel.isSystemElement(all.classFqn()))
                || n instanceof TypedTableReference table && SystemMetamodel.STORE_FQN.equals(table.store())
                || n instanceof TypedUserCall call && SystemMetamodel.functionIds().contains(call.callee().id())
                || n instanceof TypedPackageableRef
                || n instanceof TypedNewInstance || n instanceof TypedCopyInstance
                || n instanceof TypedGraphFetch || n instanceof TypedSerialize || n instanceof TypedSerializeGraph;
    }

    /** The classes the system metamodel's mapping binds: their extents are the system store's rows. */
    private static Set<String> systemClasses(ModelContext ctx) {
        MappingDefinition system = ctx.findMapping(SystemMetamodel.MAPPING_FQN).orElseThrow(() ->
                new IllegalStateException("the system metamodel's mapping is not in the model -- injection"
                        + " regressed"));
        Set<String> classes = new HashSet<>();
        for (MappingDefinition.ClassBinding binding : system.classBindings()) {
            classes.add(binding.classFqn());
        }
        return classes;
    }

    private static boolean effectProgram(TypedQuery query, String runtime) {
        ModelContext ctx = query.context();
        if (QueryDoor.declaresTestData(runtime, ctx)) {
            return false;
        }
        QueryDoor.Scan scan = new QueryDoor.Scan(query, runtime);
        List<TypedSpec> body = query.body();
        Map<FunctionId, Boolean> storeMemo = new HashMap<>();
        boolean anEffect = false;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(statement);
            TypedSpec value = statement instanceof TypedLet let ? let.value() : statement;
            if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.EFFECT)) && value instanceof TypedNativeCall call
                    && NativeFn.Effect.isDbEffect(call.callee().id())) {
                if (!literalEffect(call, body.subList(0, i))
                        || statement instanceof TypedLet let
                                && !ConnectionLets.onlyConnectionReads(body, i + 1, let.name())) {
                    return false;
                }
                anEffect = true;
                continue;
            }
            // every other statement holds no test kind (no seeds, no assert, no effect inside it) and reads no store
            if (!kinds.isEmpty() || readsAStore(statement, query, storeMemo)) {
                return false;
            }
            boolean inert = value instanceof TypedNativeCall call
                    && NativeFn.Effect.isInertDiagnostic(call.callee().id());
            if (!inert && !literal(value) && BodyPlanner.handleType(value, ctx) == null) {
                // a computed value: the product's lowering would answer it where the walk sends its own statement
                // (the M5 audit's S3), a class of its own
                return false;
            }
        }
        return anEffect;
    }

    /** Whether {@code value} is a String or Boolean literal: a value both roads answer with no statement sent. */
    private static boolean literal(TypedSpec value) {
        return value instanceof TypedCString || value instanceof TypedCBoolean;
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

    /** Whether {@code statement} reaches a store read or write: a class's extent, a table, a {@code from()}, a store
     *  write, staged rows (through the user functions it calls). */
    private static boolean readsAStore(TypedSpec statement, TypedQuery query, Map<FunctionId, Boolean> memo) {
        return StatementEffects.reaches(statement, n -> n instanceof TypedGetAll || n instanceof TypedTableReference
                || n instanceof TypedFrom || StatementEffects.isStoreWrite(n)
                || n instanceof TypedNativeCall stage && Pure.Lite.STAGE.equals(stage.callee().qualifiedName()),
                query.specs(), memo);
    }
}
