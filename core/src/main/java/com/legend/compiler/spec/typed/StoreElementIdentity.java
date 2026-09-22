// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.PlatformTypes;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * THE store-element IDENTITY spelling (D2: an element's identity is its
 * path): {@code db->schema('S')} and {@code db->schema('S')->table('T')}
 * over a Database REFERENCE with literal names, through {@code toOne}
 * peels. The accessors are the system metamodel's Pure functions
 * (platform_store_relational/functions.pure:227/:249); over an element
 * reference their call IS the element and stays closed — the inliner
 * never opens it, the resolver roots a chain at that element's row
 * (ElementReferences), the structural readers (replaceTables pairs,
 * loadCsvToDbTable, the relational mappers, StoreNav) consume the
 * spelling. Over a ROW variable the body inlines like any other. ONE
 * owner of the shape — every consumer asks here.
 */
public final class StoreElementIdentity {

    private StoreElementIdentity() {
    }

    /** {@code (dbFqn, schema, table)} of a table identity. */
    public record TableRef(String dbFqn, String schema, String table) {
    }

    /** {@code n} through {@code toOne}/{@code toOneMany} peels and the
     * caller's binder (lets, aliases). */
    public static TypedSpec peel(TypedSpec n, UnaryOperator<TypedSpec> bind) {
        TypedSpec cur = bind.apply(n);
        while (cur instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
            cur = bind.apply(c.args().get(0));
        }
        return cur;
    }

    /** The table identity {@code n} spells, lets chased by {@code bind};
     * null when {@code n} is not that shape. */
    public static @com.legend.base.Nullable TableRef tableRef(TypedSpec n,
            UnaryOperator<TypedSpec> bind) {
        TypedSpec t = peel(n, bind);
        if (!PlatformTypes.STORE_TABLE_NAV.equals(Calls.calleeOf(t))) {
            return null;
        }
        List<TypedSpec> ta = Calls.argsOf(t);
        if (ta.size() != 2 || !(bind.apply(ta.get(1)) instanceof TypedCString tn)) {
            return null;
        }
        String[] s = schemaRef(ta.get(0), bind);
        return s == null ? null : new TableRef(s[0], s[1], tn.value());
    }

    /** {@code (dbFqn, schema)} of a schema identity; null otherwise. */
    public static String @com.legend.base.Nullable [] schemaRef(TypedSpec n,
            UnaryOperator<TypedSpec> bind) {
        TypedSpec s = peel(n, bind);
        if (!PlatformTypes.STORE_SCHEMA_NAV.equals(Calls.calleeOf(s))) {
            return null;
        }
        List<TypedSpec> sa = Calls.argsOf(s);
        if (sa.size() != 2
                || !(bind.apply(sa.get(0)) instanceof TypedPackageableRef db)
                || !(bind.apply(sa.get(1)) instanceof TypedCString sn)) {
            return null;
        }
        return new String[]{db.fullPath(), sn.value()};
    }

    /** Whether the call {@code fqn(args)} is a schema or table identity. */
    public static boolean isIdentityCall(String fqn, List<TypedSpec> args) {
        if (args.size() != 2 || !(args.get(1) instanceof TypedCString)) {
            return false;
        }
        UnaryOperator<TypedSpec> id = UnaryOperator.identity();
        if (PlatformTypes.STORE_SCHEMA_NAV.equals(fqn)) {
            return peel(args.get(0), id) instanceof TypedPackageableRef;
        }
        return PlatformTypes.STORE_TABLE_NAV.equals(fqn)
                && schemaRef(args.get(0), id) != null;
    }

    /** Whether {@code n} is a schema or table identity call (no lets). */
    public static boolean isIdentity(TypedSpec n) {
        String fqn = Calls.calleeOf(n);
        return fqn != null && isIdentityCall(fqn, Calls.argsOf(n));
    }

    /** Whether {@code n} is a TABLE identity (the row-rooted kind). */
    public static boolean isTableIdentity(TypedSpec n) {
        return tableRef(n, UnaryOperator.identity()) != null;
    }
}
