package com.legend.model.spec;

import java.util.Objects;

/**
 * The value side of a single property binding inside a
 * {@link NewInstance}. Holds the bound expression plus an
 * {@code isAdd} flag distinguishing the two assignment forms:
 *
 * <ul>
 *   <li>{@code prop = value} &mdash; {@code isAdd = false}: assign
 *       (replace whatever the property currently holds with the
 *       given value).</li>
 *   <li>{@code prop += value} &mdash; {@code isAdd = true}: append
 *       (add the value to the property's existing collection).
 *       Only meaningful for to-many properties.</li>
 * </ul>
 *
 * <p>The property name itself is <em>not</em> a field of this record
 * &mdash; it lives as the key in the enclosing
 * {@code Map<String, KeyExpression>} on {@link NewInstance}. Putting
 * the name in two places would create an enforceable-but-redundant
 * invariant ({@code map.key().equals(keyExpression.key())}). Single
 * source of truth: the map key.
 *
 * <h2>Engine alignment</h2>
 *
 * <p>This shape preserves the {@code add} flag that legend-engine's
 * protocol carries on its {@code KeyExpression}
 * ({@code _type='keyExpression', add, key, expression}). Engine-lite
 * silently discards the leading {@code +} via
 * {@code if (check(PLUS)) advance();} &mdash; we deliberately
 * preserve it because the {@code +=} vs {@code =} distinction is
 * real semantics, not a cosmetic choice. Engine-pure's
 * {@code KeyExpression} M3 metamodel type carries the same flag
 * ({@code _add(boolean)} in {@code AntlrContextToM3CoreInstance}).
 *
 * <p>This is a {@link ValueSpecification}-adjacent node: not a
 * {@link ValueSpecification} variant itself (a binding is not a
 * standalone expression) but used as the value type of a {@code Map}
 * field on a {@link ValueSpecification}.
 *
 * @param value  the expression being bound to the property
 * @param isAdd  {@code true} if the source used {@code +=};
 *               {@code false} if it used {@code =}
 */
public record KeyExpression(ValueSpecification value, boolean isAdd, boolean isLocal) {
    public KeyExpression {
        Objects.requireNonNull(value, "value");
    }

    /**
     * Convenience constructor for the common {@code =} (assign) case
     * binding a public property.
     */
    public KeyExpression(ValueSpecification value) {
        this(value, false, false);
    }

    /**
     * Backwards-compatible two-arg constructor: assign / add semantics
     * for a public property. Use the three-arg form to mark a local
     * (mapping-private) property declaration ({@code +name=value}).
     */
    public KeyExpression(ValueSpecification value, boolean isAdd) {
        this(value, isAdd, false);
    }
}
