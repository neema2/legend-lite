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
public record TypedProject(TypedSpec source, List<TypedFuncCol> columns,
        ExprType info) implements TypedRelationOp {
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

    /** The {@code .columns.documentation} static fold: col()'s doc
     * metadata per column (String[0..1] — undocumented columns flatten
     * away). Consumed by the Typer's in-query arm and the K-side
     * splice hook's marker resolution. */
    public TypedSpec docsFold() {
        java.util.List<TypedSpec> docs = new java.util.ArrayList<>();
        for (TypedFuncCol fc : columns) {
            if (fc.documentation() != null) {
                docs.add(new TypedCString(fc.documentation(),
                        com.legend.compiler.element.type.ExprType.one(
                                com.legend.compiler.element.type
                                        .Type.Primitive.STRING)));
            }
        }
        return new TypedCollection(docs,
                new com.legend.compiler.element.type.ExprType(
                        com.legend.compiler.element.type.Type.Primitive.STRING,
                        new com.legend.compiler.element.type.Multiplicity
                                .Bounded(docs.size(), docs.size())));
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1 + columns.size(), "TypedProject");
        java.util.List<TypedFuncCol> cs = new java.util.ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            TypedFuncCol c = columns.get(i);
            cs.add(new TypedFuncCol(c.name(), (TypedLambda) kids.get(1 + i), c.documentation()));
        }
        return new TypedProject(kids.get(0), cs, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedProject(source, columns, info);
    }
}
