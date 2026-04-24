package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.m3.Type;

import java.util.List;
import java.util.Optional;

/**
 * Window extend column: {@code ~alias: <func>(...funcArgs...) OVER (...over...)}
 * optionally wrapped by a scalar {@link OuterWrapper} and/or paired with a
 * {@code reducer} lambda (aggregate windows).
 *
 * <p><strong>Shape (five patterns captured faithfully from legacy
 * {@code generateWindowFuncFromAst}):</strong>
 * <table>
 *   <tr><th>Pattern</th><th>funcArgs</th><th>reducer</th><th>outerWrapper</th></tr>
 *   <tr><td>rowNumber / rank / denseRank</td>
 *       <td>empty</td><td>absent</td><td>absent</td></tr>
 *   <tr><td>ntile(4) / lag(col, offset)</td>
 *       <td>[literal offset / value expr]</td><td>absent</td><td>absent</td></tr>
 *   <tr><td>aggregate window (sum / avg / ...)</td>
 *       <td>[fn1 value expr]</td><td>present (fn2 reducer lambda)</td><td>absent</td></tr>
 *   <tr><td>lag(col).property chain</td>
 *       <td>[property-access expr, ...extra]</td><td>absent</td><td>absent</td></tr>
 *   <tr><td>round(cumulativeDistribution(), 2)</td>
 *       <td>[]</td><td>absent</td>
 *       <td>present: {@link OuterWrapper} whose {@code expr} contains a
 *           {@link TypedVariable} referencing {@code holeName}</td></tr>
 * </table>
 *
 * <p><strong>PlanGen contract</strong>: lowering is purely structural — walk
 * {@code funcArgs} as scalars (with {@code rowParamName} bound to the source
 * alias), emit a {@code WindowCall}, then if {@code outerWrapper} is present
 * bind {@code outerWrapper.holeName()} to the built {@code WindowCall} in the
 * lowering context and lower {@code outerWrapper.expr()}. The
 * {@code TypedVariable} the checker placed at the substitution point resolves
 * via the standard variable-binding pathway — no sentinel node is needed.
 *
 * @param rowParamName  Name of the row lambda parameter (e.g. {@code "x"} for
 *                      {@code ~rn: x|$x->rowNumber()}); bound to the source
 *                      alias at lowering time so {@code funcArgs} resolve
 *                      against the row.
 * @param funcArgs      Arguments inside {@code FUNC(...)}; row/window context
 *                      params are already filtered out by the checker.
 * @param reducer       fn2 reducer lambda when the window is an aggregate
 *                      (identifies SUM/AVG/... by its body's terminal call).
 * @param outerWrapper  When present, the scalar expression surrounding the
 *                      window call together with the fresh name bound to the
 *                      window-call result. See {@link OuterWrapper}.
 */
public record TypedWindowExtendCol(
        String alias,
        NativeFunctionDef func,
        String rowParamName,
        List<TypedSpec> funcArgs,
        Optional<TypedLambda> reducer,
        Optional<OuterWrapper> outerWrapper,
        TypedOver over,
        Type returnType,
        Optional<Type> castType
) implements TypedExtendCol {
    public TypedWindowExtendCol {
        funcArgs = List.copyOf(funcArgs);
    }

    /**
     * Scalar expression surrounding the window call, paired with the fresh
     * symbol that names the window-call result inside {@code expr}.
     *
     * <p>Binding the two together makes the invariant "they travel as one"
     * impossible to violate: there is no way to have a wrapper expression
     * without a name, or a name without a wrapper.
     *
     * @param expr      scalar expression containing exactly one
     *                  {@link TypedVariable} whose {@code name} equals
     *                  {@code holeName}.
     * @param holeName  fresh synthetic identifier allocated via
     *                  {@code TypeCheckEnv.freshSymbol} using the reserved
     *                  {@code $$} prefix so it cannot collide with any
     *                  user-written Pure identifier.
     */
    public record OuterWrapper(TypedSpec expr, String holeName) {}
}
