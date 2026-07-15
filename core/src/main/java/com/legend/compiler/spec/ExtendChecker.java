package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedAggColSpec;
import com.legend.compiler.spec.typed.TypedAggColSpecArray;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.ValueSpecification;

import java.util.List;

/**
 * Relation {@code extend} (engine {@code ExtendChecker}) &mdash; all three forms,
 * each CHECKED generically against its registered signature and emitted as its
 * own construct:
 *
 * <ul>
 *   <li><strong>Scalar</strong> {@code extend(~col:x|…)} &mdash;
 *       {@code FuncColSpec<{T[1]->Any[0..1]},Z>} &rarr; {@code Relation<T+Z>};
 *       {@link TypedExtend}.</li>
 *   <li><strong>Aggregate</strong> {@code extend(~col:map:reduce)} &mdash; a
 *       whole-relation windowed aggregation, {@code AggColSpec<…,R>} &rarr;
 *       {@code Relation<T+R>}; {@link TypedExtendAgg}.</li>
 *   <li><strong>Windowed</strong> {@code extend(over(…), ~col:…)} &mdash; window
 *       functions ({@code {p,w,r|…}} lambdas) or windowed aggregates over an
 *       {@code over(…)} definition; the window's partition/sort columns validate
 *       against the source when {@code _Window<T>} unifies (the fragment-rebind
 *       rule); {@link TypedExtendWindow}.</li>
 * </ul>
 */
final class ExtendChecker {

    private ExtendChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 3) {
            Application a = t.checkGeneric(af, env);
            if (!(a.args().get(1) instanceof TypedOver window)) {
                throw new TypeInferenceException(
                        "extend with three arguments expects over(…) as the window, got "
                                + a.args().get(1).info().type().typeName());
            }
            TypedSpec cols = a.args().get(2);
            return isAgg(cols)
                    ? new TypedExtendWindow(a.args().get(0), window, List.of(), Args.aggCols(cols), a.out())
                    : new TypedExtendWindow(a.args().get(0), window, Args.funcCols(cols), List.of(), a.out());
        }
        if (af.parameters().size() != 2) {
            throw new TypeInferenceException("extend expects (source, ~columns) or (source, over(…), ~columns)");
        }
        Application a = t.checkGeneric(af, env);
        if (carriesReducer(af.parameters().get(1))) {
            return new TypedExtendAgg(a.args().get(0), Args.aggCols(a.args().get(1)), a.out());
        }
        return new TypedExtend(a.args().get(0), Args.funcCols(a.args().get(1)), a.out());
    }

    private static boolean isAgg(TypedSpec arg) {
        return arg instanceof TypedAggColSpec || arg instanceof TypedAggColSpecArray;
    }

    /** Whether a colspec argument carries a reducer ({@code function2}) &mdash; the aggregate form. */
    private static boolean carriesReducer(ValueSpecification vs) {
        return (vs instanceof ColSpec cs && cs.function2() != null)
                || (vs instanceof ColSpecArray arr
                        && arr.colSpecs().stream().anyMatch(c -> c.function2() != null));
    }
}
