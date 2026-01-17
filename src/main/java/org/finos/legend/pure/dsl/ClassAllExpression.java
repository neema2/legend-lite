package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a Class.all() expression - retrieves all instances of a class.
 * 
 * Example: Person.all()
 * 
 * @param className The name of the class to query
 */
public record ClassAllExpression(String className) implements PureExpression {
    public ClassAllExpression {
        Objects.requireNonNull(className, "Class name cannot be null");
    }
}
