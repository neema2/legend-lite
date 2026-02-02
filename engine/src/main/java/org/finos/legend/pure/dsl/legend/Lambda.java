package org.finos.legend.pure.dsl.legend;

import java.util.List;

/**
 * Lambda expression: {params | body} or x | body
 * 
 * Examples:
 * - Single: x | $x.name
 * - Multi: {x, y | $x + $y}
 */
public record Lambda(
        List<String> parameters,
        Expression body) implements Expression {

    public static Lambda of(String param, Expression body) {
        return new Lambda(List.of(param), body);
    }
}
