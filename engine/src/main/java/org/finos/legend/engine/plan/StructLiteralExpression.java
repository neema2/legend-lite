package org.finos.legend.engine.plan;

import java.util.Map;
import java.util.Objects;

/**
 * IR node representing a struct literal in expression context.
 * This is the IR counterpart of the AST InstanceExpression.
 *
 * Pure: ^Person(firstName='John', age=30)
 * IR:   StructLiteralExpression("Person", {firstName: Literal("John"), age: Literal(30)})
 * SQL:  {'firstName': 'John', 'age': 30}
 *
 * Nested instances become nested StructLiteralExpression nodes.
 */
public record StructLiteralExpression(
        String className,
        Map<String, Expression> fields) implements Expression {

    public StructLiteralExpression {
        Objects.requireNonNull(className, "Class name cannot be null");
        Objects.requireNonNull(fields, "Fields cannot be null");
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public SqlType type() {
        return SqlType.UNKNOWN;
    }

    @Override
    public String toString() {
        return "StructLiteral[" + className + ", " + fields + "]";
    }
}
