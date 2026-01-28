package org.finos.legend.engine.plan;

import org.finos.legend.engine.store.Table;
import java.util.Objects;

/**
 * IR node representing a DELETE operation.
 * 
 * Generated SQL: DELETE FROM "table" WHERE condition
 * 
 * @param table       The target table
 * @param whereClause Optional filter condition (null means delete all rows)
 */
public record DeleteNode(
        Table table,
        Expression whereClause) implements MutationNode {

    public DeleteNode {
        Objects.requireNonNull(table, "Table cannot be null");
    }

    @Override
    public <T> T accept(MutationNodeVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
