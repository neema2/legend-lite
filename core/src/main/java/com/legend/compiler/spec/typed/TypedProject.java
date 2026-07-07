package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.ArrayList;
import java.util.List;

/**
 * A type-checked {@code project} (engine {@code TypedProject}) &mdash; builds a
 * fresh relation from per-source-element mapping lambdas:
 * {@code project<T,Z>(r:Relation<T>[1], ~[cols]):Relation<Z>[1]} and the
 * class-source {@code project<C,T>(cl:C[*], ~[cols]):Relation<T>[1]}. The legacy
 * TDS form {@code project([lambdas], ['names'])} and bare {@code ~prop} columns
 * <em>desugar</em> into the modern colspec form before checking (engine
 * {@code ProjectChecker} does the same rewrite), so one generic rule types all of
 * them; {@code Z} binds from the checked lambda bodies.
 *
 * @param source  the relation or class collection being projected
 * @param columns the projected columns (alias + checked lambda each), in output order
 * @param info    the result &mdash; {@code Relation<Z>} resolved to the concrete schema
 */
public record TypedProject(TypedSpec source, List<TypedFuncCol> columns, ExprType info) implements TypedSpec {
    public TypedProject {
        columns = List.copyOf(columns);
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new ArrayList<>();
        out.add(source);
        columns.forEach(c -> out.add(c.fn()));
        return out;
    }
}
