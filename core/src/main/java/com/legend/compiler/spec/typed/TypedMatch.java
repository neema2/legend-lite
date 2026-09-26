package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * A statically dispatched {@code match} (engine {@code MatchChecker}): the branch
 * whose declared parameter type accepts the input (subtype-aware, first match
 * wins) is selected at <em>compile time</em>, and this node carries the input,
 * the binding name, and the checked branch body &mdash; typed as the body
 * (engine returns the matched branch's &beta;-reduced body).
 *
 * @param input      the matched value
 * @param param      the branch parameter name the body binds
 * @param body       the checked body of the selected branch
 * @param extraParam the branch's second parameter name, for the extra-argument form
 * @param extra      the checked extra argument bound to {@code extraParam}
 * @param info       the body's type
 */
public record TypedMatch(TypedSpec input, String param, TypedSpec body,
                         Optional<String> extraParam, Optional<TypedSpec> extra,
                         ExprType info,
                         @com.legend.base.Nullable ExprType declaredInfo) implements TypedSpec {

    /** Emission-narrowing form (the optional-dispatch builder): the
     * declared type falls back to the selected body's. */
    public TypedMatch(TypedSpec input, String param, TypedSpec body,
            Optional<String> extraParam, Optional<TypedSpec> extra, ExprType info) {
        this(input, param, body, extraParam, extra, info, null);
    }

    /** What PURE's signature says the match expression's type is — the
     * LUB over ALL branch bodies (match&lt;T|m&gt; binds T across every
     * func), independent of which branch the EMISSION selected. The
     * deactivate reflection lane reads THIS; {@code info} (the
     * narrowed emission type) is untouched and stays load-bearing for
     * the whole match family (dossier leg3b D5 — two types, one node,
     * each with a stated owner). */
    public ExprType declared() {
        return declaredInfo != null ? declaredInfo : info;
    }

    @Override
    public List<TypedSpec> children() {
        return extra
                .map(e -> List.of(input, e, body))
                .orElseGet(() -> List.of(input, body));
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, extra.isPresent() ? 3 : 2, "TypedMatch");
        return extra.isPresent()
                ? new TypedMatch(kids.get(0), param, kids.get(2), extraParam,
                        java.util.Optional.of(kids.get(1)), info, declaredInfo)
                : new TypedMatch(kids.get(0), param, kids.get(1), extraParam,
                        java.util.Optional.empty(), info, declaredInfo);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedMatch(input, param, body, extraParam, extra, info, declaredInfo);
    }
}
