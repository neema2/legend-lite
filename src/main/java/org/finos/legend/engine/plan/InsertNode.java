package org.finos.legend.engine.plan;

import org.finos.legend.engine.store.Table;
import java.util.List;
import java.util.Objects;

/**
 * IR node representing an INSERT operation.
 * 
 * Generated SQL: INSERT INTO "table" ("col1", "col2") VALUES (val1, val2)
 * 
 * @param table   The target table
 * @param columns List of column names
 * @param values  List of values (corresponds to columns)
 */
public record InsertNode(
        Table table,
        List<String> columns,
        List<Expression> values) implements MutationNode {

    public InsertNode {
        Objects.requireNonNull(table, "Table cannot be null");
        Objects.requireNonNull(columns, "Columns cannot be null");
        Objects.requireNonNull(values, "Values cannot be null");
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Columns and values must have same size");
        }
    }

    @Override
    public <T> T accept(MutationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
