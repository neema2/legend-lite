package org.finos.legend.pure.dsl.legend;

/**
 * Base interface for PureLegend AST expressions.
 * 
 * Minimal AST inspired by legend-engine's ValueSpecification:
 * - Function: function/method calls (unified)
 * - Property: property access ($x.name)
 * - Variable: variable reference ($x)
 * - Lambda: lambda expressions ({x | body})
 * - Literal: primitive values (42, 'hello', true)
 * - Collection: arrays ([a, b, c])
 */
public sealed interface Expression permits
                Function,
                Property,
                Variable,
                Lambda,
                Literal,
                Collection,
                TdsLiteral {
}
