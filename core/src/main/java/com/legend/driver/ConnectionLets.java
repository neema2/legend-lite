// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.List;
import java.util.Set;

/**
 * The CONNECTION-CHAIN read analysis for effectful lets: the corpus's
 * {@code let runtime = initDatabase(); let connection =
 * $runtime.connectionStores...->cast(...); executeInDb(sql, $connection)}
 * shape reads the effectful binding ONLY to extract a connection — and
 * the executor NEVER evaluates connection arguments (one ambient
 * connection per execution context; the chains exist to type-check).
 * Such reads are harmless; anything else keeps the loud wall.
 */
public final class ConnectionLets {

    private ConnectionLets() {
    }

    /** Every later reference to {@code name} — transitively through lets
     *  whose rhs only navigates connection-only bindings — sits in a
     *  never-evaluated executeInDb connection-argument position. */
    public static boolean onlyConnectionReads(List<TypedSpec> stmts,
            int from, String name) {
        Set<String> connNames = new java.util.HashSet<>();
        connNames.add(name);
        for (int j = from; j < stmts.size(); j++) {
            TypedSpec st = stmts.get(j);
            if (st instanceof TypedLet l && references(l.value(), connNames)
                    && rootsOnly(l.value(), connNames)
                    && !(l.value() instanceof TypedVariable)) {
                // a NAVIGATION over connection-only roots joins the set;
                // a BARE rebind (let n = $rs) is a host-side result read
                // and keeps the wall (ExecuteInDbDuckDbTest pin)
                connNames.add(l.name());
                continue;
            }
            if (readOutsideConnArg(st, connNames)) {
                return false;
            }
        }
        return true;
    }

    /** Any variable in {@code v} outside {@code names}? (a pure
     *  navigation chain over connection-only roots is droppable). */
    private static boolean rootsOnly(TypedSpec v, Set<String> names) {
        if (v instanceof TypedVariable tv && !names.contains(tv.name())) {
            return false;
        }
        for (TypedSpec c : v.children()) {
            if (!rootsOnly(c, names)) {
                return false;
            }
        }
        return true;
    }

    private static boolean references(TypedSpec v, Set<String> names) {
        if (v instanceof TypedVariable tv && names.contains(tv.name())) {
            return true;
        }
        for (TypedSpec c : v.children()) {
            if (references(c, names)) {
                return true;
            }
        }
        return false;
    }

    /** A reference to any {@code names} member OUTSIDE an executeInDb
     *  connection argument (K-native or the corpus's own wrapper). */
    private static boolean readOutsideConnArg(TypedSpec n, Set<String> names) {
        if (n instanceof TypedVariable tv) {
            return names.contains(tv.name());
        }
        boolean isExec = n instanceof TypedNativeCall nc
                && com.legend.compiler.element.type.PlatformTypes.EXECUTE_IN_DB
                        .equals(nc.callee().qualifiedName())
                || n instanceof TypedUserCall uc
                && "meta::relational::metamodel::execute::executeInDb"
                        .equals(uc.callee().qualifiedName());
        if (isExec) {
            List<TypedSpec> args = n instanceof TypedNativeCall nc2
                    ? nc2.args()
                    : ((TypedUserCall) n).args();
            // only the SQL argument (position 0) is ever evaluated
            return !args.isEmpty() && readOutsideConnArg(args.get(0), names);
        }
        for (TypedSpec c : n.children()) {
            if (readOutsideConnArg(c, names)) {
                return true;
            }
        }
        return false;
    }
}
