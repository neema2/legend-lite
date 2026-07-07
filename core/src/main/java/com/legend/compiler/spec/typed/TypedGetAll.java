package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code Class.all()} object-graph source (engine {@code TypedGetAll}) &mdash;
 * {@code getAll<T>(Class<T>[1] [, Date[1] [, Date[1]]]):T[*]}, the anchor node
 * store lowering resolves a mapping against. Checked generically ({@code T}
 * binds from the {@code Class<T>} reference); a missing mapping is NOT a type
 * error &mdash; compile succeeds, the back-end surfaces it at the use site
 * (engine's compile-vs-link split). The optional milestoning dates ride along.
 *
 * @param classFqn    the source class, fully qualified
 * @param milestoning the business/processing date arguments, possibly empty
 * @param info        {@code ClassType[*]}
 */
public record TypedGetAll(String classFqn, List<TypedSpec> milestoning, ExprType info) implements TypedSpec {
    public TypedGetAll {
        milestoning = List.copyOf(milestoning);
    }

    @Override
    public List<TypedSpec> children() {
        return new ArrayList<>(milestoning);
    }
}
