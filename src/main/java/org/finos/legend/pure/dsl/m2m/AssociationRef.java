package org.finos.legend.pure.dsl.m2m;

import java.util.Objects;

/**
 * Represents an association reference in an M2M mapping.
 * 
 * Syntax: @JoinName
 * 
 * Example:
 * 
 * <pre>
 * PersonWithAddress: Pure
 * {
 *     ~src RawPerson
 *     fullName: $src.firstName + ' ' + $src.lastName,
 *     address: @PersonAddress   // Association reference
 * }
 * </pre>
 * 
 * The @JoinName references a database Join which defines how to navigate
 * from the source class to a related class for nested object fetching.
 * 
 * @param joinName The name of the database Join to use for navigation
 */
public record AssociationRef(String joinName) implements M2MExpression {

    public AssociationRef {
        Objects.requireNonNull(joinName, "Join name cannot be null");
        if (joinName.isBlank()) {
            throw new IllegalArgumentException("Join name cannot be blank");
        }
    }

    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String toString() {
        return "@" + joinName;
    }
}
