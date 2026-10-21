// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * A property its source's own class has, read through a downcast: {@code X->cast(@Sub).p} is {@code X.p} where
 * {@code X}'s class declares or inherits {@code p}. Pure redefines no property, so a member's value of an
 * inherited property is the row's, and the row's own route serves it -- a subtype's binding table carries the
 * subtype's columns only, so an association hop read through it (`asVoucherPayment: { order: ... }`, the GraphQL
 * hardening fixture's hierarchy, 2026-10-20) was refused. The cast stays where the property is the subtype's own.
 * A structural identity of the typed tree; nothing is computed.
 */
final class InheritedCasts {

    private InheritedCasts() {
    }

    static TypedSpec fold(TypedSpec n, ModelContext ctx) {
        TypedSpec m = n.mapChildren(c -> fold(c, ctx));
        if (m instanceof TypedPropertyAccess pa
                && Pipelines.unwrapToOne(pa.source()) instanceof TypedCast tc
                && Type.asClassType(tc.source().info().type()) instanceof Type.ClassType from
                && Type.asClassType(tc.target()) instanceof Type.ClassType to
                && !from.equals(to)
                && ctx.findProperty(from.fqn(), pa.property()).isPresent()) {
            return new TypedPropertyAccess(tc.source(), pa.property(), pa.info());
        }
        return m;
    }
}
