// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedSpec;

/** POST-CONDITION (core/README rule 9): no {@code TypedGetAll} or
 * {@code TypedUserCall} survives store resolution — a RESOLVER-phase
 * gap named as such, with its ancestry path. */
final class StoreEscapees {

    private StoreEscapees() {
    }

    static void check(TypedSpec n) {
        check(n, "root");
    }

    private static void check(TypedSpec n, String path) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && (com.legend.compiler.element.type.PlatformTypes
                        .EXECUTION_PLAN.equals(nc.callee().qualifiedName())
                        || com.legend.compiler.element.type.PlatformTypes
                                .PREVAL.equals(nc.callee().qualifiedName()))) {
            // the PLAN HANDLE is opaque (#47): its query lambda is
            // compiled by the PLAN LANE at consumption (planToString /
            // plan walks) under the call's own mapping and plan
            // parameters — Phase-H resolution neither descends into it
            // nor demands a bound chain here (getAll-76 lane, §6.1)
            return;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedJoinSlot js) {
            throw new com.legend.error.NotImplementedException(
                    "store resolution left join slot '" + js.alias()
                    + "' unmaterialized — a slot outside a class pipeline or a"
                    + " view body is a resolver gap [at " + path + "]");
        }
        if (n instanceof TypedGetAll ga) {
            throw new com.legend.error.NotImplementedException(
                    "store resolution left getAll(" + ga.classFqn()
                    + ") unresolved — the query shape around it is not"
                    + " supported by the resolver yet [at " + path + "]");
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && com.legend.compiler.spec.typed.StoreElementIdentity.isIdentity(uc)) {
            // a store-element IDENTITY (db->schema('S')->table('T')) is a
            // value the structural natives consume — not an unresolved call
            return;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && com.legend.builtin.Subsumed.of(uc.callee().qualifiedName()).isPresent()) {
            // a SUBSUMED engine program (Subsumed.java): a typed opaque value
            // by decision, never an unresolved call
            return;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedUserCall uc) {
            throw new com.legend.error.NotImplementedException(
                    "store resolution left user call '"
                    + uc.callee().qualifiedName()
                    + "' uninlined — the call shape is not supported by the"
                    + " resolver yet [at " + path + "]");
        }
        String next = path + " > " + n.getClass().getSimpleName();
        for (TypedSpec c : n.children()) {
            check(c, next);
        }
    }
}
