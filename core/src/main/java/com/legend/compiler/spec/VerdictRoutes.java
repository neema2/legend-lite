// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedSpec;

/** Which helper-wrapped assert roots take the verdict route directly
 * (BodyDriver's statement loop): a string-entry read, or an assert
 * over CLASS values (batch 54). Pure predicates over the typed tree. */
public final class VerdictRoutes {

    private VerdictRoutes() {
    }

    /** An assert-family root whose arguments include a CLASS-typed value
     * (two constructed instances compared by their equality keys). */
    public static boolean assertsClassValue(TypedSpec n) {
        return n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && nc.callee().qualifiedName().startsWith(com.legend.compiler.element.type.PlatformTypes.ASSERTS_PACKAGE)
                && nc.args().stream().anyMatch(a -> structValued(a.info().type()));
    }

    /** A STRUCT-valued class (a constructed instance judged by its keys) —
     * never a TDS / relation / result carrier, whose asserts keep the grid
     * and SQL-text arms downstream (the float canon lives there). */
    static boolean structValued(com.legend.compiler.element.type.Type t) {
        if (!(t instanceof com.legend.compiler.element.type.Type.ClassType ct)
                || com.legend.compiler.element.type.Type.isRelation(t)
                || com.legend.compiler.element.type.PlatformTypes.isMapCarrier(t)) {
            return false;
        }
        String f = ct.fqn();
        return !f.equals("meta::pure::tds::TabularDataSet")
                && !f.equals("meta::pure::mapping::Result")
                && !f.equals("meta::pure::tds::TDSRow")
                && !f.startsWith("meta::pure::executionPlan::");
    }

    /** Whether the tree reads a string-entry result (the JSON envelope
     * or a JSON tree read over it). */
    public static boolean readsStringEntry(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedJsonResult
                || n instanceof com.legend.compiler.spec.typed.TypedJsonAccess) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (readsStringEntry(c)) {
                return true;
            }
        }
        return false;
    }

}
