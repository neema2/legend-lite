package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * An inline TDS literal {@code #TDS col:Type, … / rows #} (engine
 * {@code TypedTdsLiteral}) &mdash; a relation <em>source</em> whose schema comes
 * from the header (explicit {@code col:Type} annotations, or inference from the
 * first data row), never the signature's {@code Relation<Any>}. The column
 * types live on {@link #info()} (the resolved row-struct); the raw cell grid is
 * carried for lowering (a SQL {@code VALUES} clause).
 *
 * @param rows the data rows as raw cell text, in source order
 * @param info the parsed schema, {@code (col:Type, …)[1]}
 * @param parameters the cells whose value is a plan parameter bound when the plan runs, never text the planner wrote
 *                   (the test-is-the-product plan's M3f, R17 C4: an activity row's trace stamp, minted per execution);
 *                   such a cell's text in {@code rows} is not read
 */
public record TypedTds(List<List<String>> rows, ExprType info, List<Parameter> parameters) implements TypedSpec {
    public TypedTds {
        rows = rows.stream().map(List::copyOf).toList();
        parameters = List.copyOf(parameters);
    }

    /** A TDS literal whose every cell is text. */
    public TypedTds(List<List<String>> rows, ExprType info) {
        this(rows, info, List.of());
    }

    /** The cell at {@code row}, {@code column} is the plan parameter {@code name}. */
    public record Parameter(int row, int column, String name) {
        public Parameter {
            java.util.Objects.requireNonNull(name, "name");
        }
    }

    /** The parameter the cell at {@code row}, {@code column} is, or null when it is text. */
    public @com.legend.base.Nullable String parameterAt(int row, int column) {
        for (Parameter p : parameters) {
            if (p.row() == row && p.column() == column) {
                return p.name();
            }
        }
        return null;
    }

    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedTds");
        return this;
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedTds(rows, info, parameters);
    }
}
