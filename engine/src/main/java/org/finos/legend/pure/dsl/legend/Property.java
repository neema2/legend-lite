package org.finos.legend.pure.dsl.legend;

/**
 * Property access: $x.propertyName
 * 
 * Distinct from Function because property access
 * compiles differently than function application.
 */
public record Property(
                Expression source,
                String property) implements Expression {
}
