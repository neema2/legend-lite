package org.finos.legend.pure.dsl.m2m;

import java.util.List;
import java.util.Objects;

/**
 * Represents string concatenation: expr1 + expr2 + ...
 * 
 * Example in Pure:
 * <pre>
 * fullName: $src.firstName + ' ' + $src.lastName
 * </pre>
 * 
 * @param parts The expressions being concatenated
 */
public record StringConcatExpr(List<M2MExpression> parts) implements M2MExpression {
    
    public StringConcatExpr {
        Objects.requireNonNull(parts, "Parts cannot be null");
        if (parts.size() < 2) {
            throw new IllegalArgumentException("Concatenation requires at least 2 parts");
        }
        parts = List.copyOf(parts);
    }
    
    public static StringConcatExpr of(M2MExpression... parts) {
        return new StringConcatExpr(List.of(parts));
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return String.join(" + ", parts.stream().map(Object::toString).toList());
    }
}
