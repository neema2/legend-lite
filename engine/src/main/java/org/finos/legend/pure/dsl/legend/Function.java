package org.finos.legend.pure.dsl.legend;

import java.util.ArrayList;
import java.util.List;

/**
 * Function application: function(arg1, arg2, ...)
 * 
 * This unifies function calls and method calls:
 * - Function: if(cond, then, else) → Function("if", [cond, then, else])
 * - Method:   $x->filter(pred)     → Function("filter", [$x, pred])
 * 
 * In Pure, $x->foo(a, b) is sugar for foo($x, a, b).
 */
public record Function(
        String function,
        List<Expression> parameters) implements Expression {
    
    /**
     * Creates from method-call syntax: prepends source as first parameter.
     */
    public static Function fromMethod(Expression source, String method, List<Expression> args) {
        var allArgs = new ArrayList<Expression>();
        allArgs.add(source);
        allArgs.addAll(args);
        return new Function(method, allArgs);
    }
    
    /**
     * Creates a simple function call with varargs.
     */
    public static Function of(String function, Expression... args) {
        return new Function(function, List.of(args));
    }
}
