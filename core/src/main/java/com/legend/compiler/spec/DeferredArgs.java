package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import java.util.List;

/**
 * DEFERRED ARGUMENTS — the arguments whose typing waits for the enclosing
 * overload to be chosen (bidirectional inference, §3.4): lambdas, colspecs
 * carrying lambdas, and {@code over(...)} windows (real pure binds their
 * {@code T} from the enclosing relation, never from the window's own
 * arguments). The syntactic-shape prefilter that narrows the candidates
 * before any deferred slot is typed lives here too (split out of Typer at
 * its 3,500-line guard, batch 5).
 */
final class DeferredArgs {

    private DeferredArgs() {
    }

    /** An argument whose typing must wait for the chosen signature: a lambda, or a
     * colspec carrying one. An EMPTY colspec array also defers — its flavor
     * (plain/Func/Agg) is nominal-only and the chosen parameter decides it
     * (legacy groupBy([], aggs, ids): a global aggregate's keys). */
    static boolean deferredArg(ValueSpecification p) {
        return p instanceof LambdaFunction
                || isLambdaCollection(p)
                // an over(...) window: its T is bound by no argument — real
                // pure infers it from the enclosing extend's relation, so it
                // types AFTER the enclosing overload is chosen, against the
                // expected _Window<T>
                || isOverCall(p)
                || (p instanceof ColSpec cs && cs.function1() != null)
                || (p instanceof ColSpecArray arr
                        && (arr.colSpecs().isEmpty()
                                || arr.colSpecs().stream()
                                        .anyMatch(c -> c.function1() != null)));
    }

    /** An {@code over(...)} window-building call (CoreFn.OVER), typed like a
     *  lambda: after the enclosing overload is chosen. */
    static boolean isOverCall(ValueSpecification p) {
        return p instanceof AppliedFunction af
                && CoreFn.of(af.function()).orElse(null) == CoreFn.OVER;
    }

    /** A NON-EMPTY collection literal of lambdas — {@code filter([t|...])} /
     * {@code project([t|...x, t|...y], names)}: pure's [f] ≡ f value
     * semantics in call position; each element types against the chosen
     * signature's function parameter. */
    static boolean isLambdaCollection(ValueSpecification p) {
        return p instanceof PureCollection pc && !pc.values().isEmpty()
                && pc.values().stream().allMatch(v -> v instanceof LambdaFunction);
    }

    /**
     * Prefilter candidates by the deferred arguments' <em>syntactic shape</em>
     * (engine dispatches its colspec overloads the same way): a lambda needs a
     * function-typed parameter; {@code ~a:x|…} needs {@code FuncColSpec} (or
     * {@code AggColSpec} when it carries a reducer {@code function2}); the array
     * forms need the {@code …Array} classes. Value-argument scoring cannot see
     * this, since deferred slots are not yet typed.
     */
    static boolean shapesMatch(Typer t, TypedFunction c, List<ValueSpecification> raw) {
        for (int i = 0; i < raw.size(); i++) {
            ValueSpecification p = raw.get(i);
            if (!deferredArg(p)) {
                continue;
            }
            Type pt = c.parameters().get(i).type();
            boolean ok = switch (p) {
                // A SELF-TYPABLE lambda (zero-arg, or fully annotated)
                // also matches a bare type-variable param — it synthesizes
                // standalone and T binds to its function type
                // (evaluateAndDeactivate<T|m>(var:T[m]) over {|...}).
                case LambdaFunction lf -> t.isFunctionTyped(pt)
                        || ((pt instanceof Type.TypeVar || com.legend.compiler.element.type.PlatformTypes.isAny(pt))
                                && selfTypable(lf));
                // a collection of SELF-TYPABLE lambdas also matches a bare
                // type-variable param ([{|q1},{|q2}]->evaluateAndDeactivate())
                // — and an Any param (upstream's size(Any[*]) / count(Any[*]) over
                // a lambda collection: Any accepts a function VALUE as a value)
                case PureCollection pc0
                        when (pt instanceof Type.TypeVar || com.legend.compiler.element.type.PlatformTypes.isAny(pt))
                        && pc0.values().stream().allMatch(v ->
                                v instanceof LambdaFunction plf
                                        && selfTypable(plf)) -> true;
                case PureCollection ignored -> t.isFunctionTyped(pt);
                case AppliedFunction over when isOverCall(over) ->
                        Typer.genericRawIs(pt, com.legend.compiler.element.type.PlatformTypes.WINDOW);
                case ColSpec cs -> Typer.genericRawIs(pt,
                        cs.function2() != null ? com.legend.compiler.element.type.PlatformTypes.AGG_COL_SPEC : com.legend.compiler.element.type.PlatformTypes.FUNC_COL_SPEC);
                case ColSpecArray arr when arr.colSpecs().isEmpty() ->
                        Typer.genericRawIs(pt, com.legend.compiler.element.type.PlatformTypes.COL_SPEC_ARRAY)
                                || Typer.genericRawIs(pt, com.legend.compiler.element.type.PlatformTypes.FUNC_COL_SPEC_ARRAY)
                                || Typer.genericRawIs(pt, com.legend.compiler.element.type.PlatformTypes.AGG_COL_SPEC_ARRAY);
                case ColSpecArray arr -> Typer.genericRawIs(pt,
                        arr.colSpecs().stream().anyMatch(x -> x.function2() != null)
                                ? com.legend.compiler.element.type.PlatformTypes.AGG_COL_SPEC_ARRAY : com.legend.compiler.element.type.PlatformTypes.FUNC_COL_SPEC_ARRAY);
                default -> true;
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    /** A lambda literal that can type WITHOUT an expected signature:
     * zero-arg, or every parameter annotated (Typer's standalone arm). */
    private static boolean selfTypable(LambdaFunction lf) {
        return lf.parameters().isEmpty()
                || lf.parameters().stream().allMatch(pv -> pv.type() != null);
    }

}
