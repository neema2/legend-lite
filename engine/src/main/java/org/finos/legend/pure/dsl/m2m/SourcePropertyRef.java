package org.finos.legend.pure.dsl.m2m;

import java.util.List;
import java.util.Objects;

/**
 * Represents a reference to a source property: $src.propertyName
 * 
 * Can also represent chained access: $src.address.city
 * 
 * @param propertyChain The chain of property names (e.g., ["firstName"] or ["address", "city"])
 */
public record SourcePropertyRef(List<String> propertyChain) implements M2MExpression {
    
    public SourcePropertyRef {
        Objects.requireNonNull(propertyChain, "Property chain cannot be null");
        if (propertyChain.isEmpty()) {
            throw new IllegalArgumentException("Property chain cannot be empty");
        }
        propertyChain = List.copyOf(propertyChain);
    }
    
    /**
     * Creates a simple property reference (e.g., $src.firstName).
     */
    public static SourcePropertyRef of(String propertyName) {
        return new SourcePropertyRef(List.of(propertyName));
    }
    
    /**
     * Creates a chained property reference (e.g., $src.address.city).
     */
    public static SourcePropertyRef of(String... propertyNames) {
        return new SourcePropertyRef(List.of(propertyNames));
    }
    
    /**
     * @return The first property in the chain
     */
    public String firstProperty() {
        return propertyChain.getFirst();
    }
    
    /**
     * @return The last property in the chain (the actual accessed property)
     */
    public String lastProperty() {
        return propertyChain.getLast();
    }
    
    /**
     * @return True if this is a simple single-property access
     */
    public boolean isSimple() {
        return propertyChain.size() == 1;
    }
    
    @Override
    public <T> T accept(M2MExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
    
    @Override
    public String toString() {
        return "$src." + String.join(".", propertyChain);
    }
}
