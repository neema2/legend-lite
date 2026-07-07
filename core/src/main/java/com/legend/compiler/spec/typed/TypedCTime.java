package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import com.legend.values.PureTimeLiteral;

import java.util.List;

/**
 * A time-of-day literal {@code %10:30:00} &mdash; {@code StrictTime[1]}
 * (real legend-pure's {@code meta::pure::metamodel::type::StrictTime}).
 *
 * @param value the validated structured literal
 * @param info  {@code StrictTime[1]}
 */
public record TypedCTime(PureTimeLiteral value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
