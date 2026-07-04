package com.legend.compiler.spec.typed;

import com.legend.compiler.spec.ExprType;
import com.legend.parser.spec.PureDateLiteral;

import java.util.List;

/**
 * A date literal {@code %2020-01-01} / {@code %2020-01-01T10:00} &mdash; typed by
 * its precision: year/year-month &rarr; {@code Date[1]} (an imprecise date),
 * a full day &rarr; {@code StrictDate[1]}, any time component &rarr;
 * {@code DateTime[1]} (mirrors engine's CStrictDate/CDateTime split, which core
 * folds into one structured literal).
 *
 * @param value the validated structured literal
 * @param info  {@code Date[1]} / {@code StrictDate[1]} / {@code DateTime[1]}
 */
public record TypedCDate(PureDateLiteral value, ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }
}
