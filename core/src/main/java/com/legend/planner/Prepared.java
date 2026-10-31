// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedSpec;

/**
 * A statement's body PREPARED (the planner-driver seam program's S2, the shared preparation, 2026-10-31; the body
 * driver's prelude did it all until then): framed by the execute call it runs under and ready to plan, or the kind
 * of answer it is given with no plan -- decided with no session ({@link BodyPlanner#prepare}). The driver runs what
 * runs: an effect's registered arm, a store write, an effectful map's elements; the other kinds are values written at
 * planning (D30's plan-time DDL text) or no value at all (an orchestration handle).
 */
public sealed interface Prepared {

    /** A body to plan: its statements after the frame's appends (the driver table's keys, the import-data-flow
     *  threads), its root under the execution contexts, the declared type of the outermost {@code from} (a
     *  primitive-many declared root over a relation-shaped source is a value collection), and the frame's
     *  planning half. */
    record Planned(java.util.List<TypedSpec> body, TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo, PlanEnv env)
            implements Prepared {
    }

    /** An effect native (the catalog's EFFECT rows): its registered arm runs it. */
    record Effect(java.util.List<TypedSpec> body, com.legend.compiler.spec.typed.TypedNativeCall call, PlanEnv env)
            implements Prepared {
    }

    /** A write into a store table (write, delete, update, merge): the database runs it, its update count the value. */
    record StoreWrite(java.util.List<TypedSpec> body, TypedSpec write, PlanEnv env) implements Prepared {
    }

    /** A DDL text generator ({@code toDDL}'s deprecated forms): the text, written in the engine's spelling. */
    record DdlText(String text, com.legend.compiler.element.type.Type type) implements Prepared {
    }

    /** A collection of DDL texts and literal strings, each written as one. */
    record DdlTexts(java.util.List<Object> texts) implements Prepared {
    }

    /** An orchestration handle (a connection, a runtime, a connection store): no rows, never lowered. */
    record Handle(com.legend.compiler.element.type.Type type) implements Prepared {
    }

    /** A map over an effectful lambda ({@code $sqls->map(sql|executeInDb(...))}): its source evaluated, then the
     *  lambda's body run per element with the parameter bound. */
    record EffectfulMap(java.util.List<TypedSpec> body, com.legend.compiler.spec.typed.TypedMap map, PlanEnv env)
            implements Prepared {
    }
}
