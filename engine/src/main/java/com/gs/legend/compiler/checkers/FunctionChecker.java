package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.compiler.TypeChecker;
import com.gs.legend.compiler.TypeInfo;

/**
 * Standard interface for all function type-checkers.
 *
 * <p>Every checker receives 3 arguments:
 * <ol>
 *   <li>{@code af} — the AST node for the function call</li>
 *   <li>{@code source} — pre-compiled TypeInfo for the first argument (param[0])</li>
 *   <li>{@code ctx} — compilation context (variable bindings, mappings, etc.)</li>
 * </ol>
 *
 * <p>Checkers resolve their own function overload internally via
 * {@link AbstractChecker#resolveOverload(String, java.util.List, TypeInfo)},
 * which does structural AST matching to pick the correct signature.
 *
 * <p>The TypeChecker dispatch is standard for all functions:
 * <pre>
 *   TypeInfo source = compileExpr(params.get(0), ctx);
 *   TypeInfo info = checker.check(af, source, ctx);
 *   types.put(af, info);
 * </pre>
 *
 * <p>Checkers that need additional sources (e.g., concatenate's right side,
 * join's right + condition) compile them internally via {@code env.compileExpr()}.
 *
 * @see AbstractChecker
 */
public interface FunctionChecker {
    TypeInfo check(AppliedFunction af, TypeInfo source,
                   TypeChecker.CompilationContext ctx);
}
