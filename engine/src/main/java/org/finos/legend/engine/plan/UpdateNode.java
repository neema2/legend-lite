package org.finos.legend.engine.plan;

import org.finos.legend.engine.store.Table;
import java.util.Map;
import java.util.Objects;

/**
 * IR node representing an UPDATE operation.
 * 
 * Generated SQL: UPDATE "table" SET "col1" = val1 WHERE condition
 * 
 * @param table       The target table
 * @param setClause   Map of column names to new values
 * @param whereClause Optional filter condition (null means update all rows)
 */
public record UpdateNode(
        Table table,
        Map<String, Expression> setClause,
        Expression whereClause) implements MutationNode {

    public UpdateNode {
        Objects.requireNonNull(table, "Table cannot be null");
        Objects.requireNonNull(setClause, "Set clause cannot be null");
        if (setClause.isEmpty()) {
            throw new IllegalArgumentException("Set clause cannot be empty");
        }
    }

    @Override
    public <T> T accept(MutationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
