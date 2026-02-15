package org.finos.legend.engine.plan;

import java.util.Objects;

/**
 * Represents an EXISTS (subquery) expression in the logical plan.
 * 
 * Used when filtering through to-many associations to avoid row explosion.
 * Instead of using an INNER JOIN (which duplicates parent rows for each
 * matching child), EXISTS checks for the presence of at least one match.
 * 
 * Example transformation:
 * <pre>
 * Pure: Person.all()->filter(p | $p.addresses.street->contains('Main'))
 * 
 * WRONG (row explosion with JOIN):
 *   SELECT * FROM T_PERSON p
 *   INNER JOIN T_ADDRESS a ON p.ID = a.PERSON_ID
 *   WHERE a.STREET LIKE '%Main%'
 * 
 * CORRECT (EXISTS - no row explosion):
 *   SELECT * FROM T_PERSON p
 *   WHERE EXISTS (
 *       SELECT 1 FROM T_ADDRESS a
 *       WHERE a.PERSON_ID = p.ID
 *       AND a.STREET LIKE '%Main%'
 *   )
 * </pre>
 * 
 * @param subquery The subquery to check for existence
 * @param negated If true, represents NOT EXISTS
 */
public record ExistsExpression(
        RelationNode subquery,
        boolean negated
) implements Expression {
    
    public ExistsExpression {
        Objects.requireNonNull(subquery, "Subquery cannot be null");
    }
    
    /**
     * Creates an EXISTS expression.
     */
    public static ExistsExpression exists(RelationNode subquery) {
        return new ExistsExpression(subquery, false);
    }
    
    /**
     * Creates a NOT EXISTS expression.
     */
    public static ExistsExpression notExists(RelationNode subquery) {
        return new ExistsExpression(subquery, true);
    }
    
    @Override
    public PureType type() {
        return PureType.BOOLEAN;
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visitExists(this);
    }
    
    @Override
    public String toString() {
        return (negated ? "NOT EXISTS" : "EXISTS") + "(" + subquery + ")";
    }
}
