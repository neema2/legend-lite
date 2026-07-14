package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A MILESTONED property navigation &mdash; the property functions real pure
 * GENERATES on an association end whose target class is temporal:
 * {@code $o.product(%date)} (point access), {@code $o.productAllVersions()}
 * (version sweep), {@code $o.productAllVersionsInRange(start, end)} (range).
 * Structurally a property access whose join must filter the TARGET pipeline
 * by its milestoning columns; the date arguments ride here (empty +
 * {@code sweep} = every version).
 *
 * @param source   the instance being navigated
 * @param property the DECLARED association-end name ({@code product} &mdash;
 *                 the suffix spellings normalize to it)
 * @param dates    the temporal arguments (1 = point, 2 = range, 0 = sweep)
 * @param sweep    allVersions/allVersionsInRange spelling
 * @param info     the end's type and multiplicity
 */
public record TypedMilestonedAccess(TypedSpec source, String property,
                                    List<TypedSpec> dates, boolean sweep,
                                    ExprType info) implements TypedSpec {

    public TypedMilestonedAccess {
        dates = List.copyOf(dates);
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        out.addAll(dates);
        return out;
    }
}
