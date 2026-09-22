package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedAggColSpec;
import com.legend.compiler.spec.typed.TypedAggColSpecArray;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.ValueSpecification;

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
        // FQN spellings canonicalize to the parse name up front (the
        // ProjectChecker lesson): rebuilds and generic resolution key on
        // the name, and an FQN finds only its own narrow catalog entry.
        if (af.function().contains("::")) {
            af = new AppliedFunction("extend", af.parameters());
        }
        af = normalizeLegacyCols(af);
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


    /** Legacy TDS extend spelling: {@code extend([col({r:TDSRow[1]|...},
     * 'name'), ...])} — the COLLECTION of col() calls converts to the
     * modern ColSpecArray (each col(fn,'name') = ~name:fn); the legacy
     * {@code TDSRow} param annotation strips (the modern row is
     * structural — the getters desugar reads it the same way). */
    private static AppliedFunction normalizeLegacyCols(AppliedFunction af) {
        int colsIx = af.parameters().size() == 3 ? 2
                : af.parameters().size() == 2 ? 1 : -1;
        if (colsIx < 0) {
            return af;
        }
        // a single BARE col(fn,'name') (corpus testTdsExtension spelling:
        // extend(col(x:TDSRow[1]|..., 'name')) — no wrapping collection)
        // converts the same way; anything else without a collection passes
        if (!(af.parameters().get(colsIx) instanceof com.legend.protocol.spec.PureCollection pc)) {
            ColSpec bare = legacyColToSpec(af.parameters().get(colsIx));
            if (bare == null) {
                return af;
            }
            java.util.List<ValueSpecification> np =
                    new java.util.ArrayList<>(af.parameters());
            np.set(colsIx, bare);
            return af.withParameters(np);
        }
        if (pc.values().isEmpty()) {
            return af;
        }
        java.util.List<ColSpec> specs = new java.util.ArrayList<>();
        for (ValueSpecification v : pc.values()) {
            ColSpec cs = v instanceof ColSpec direct ? direct : legacyColToSpec(v);
            if (cs == null) {
                return af;
            }
            specs.add(cs);
        }
        java.util.List<ValueSpecification> np =
                new java.util.ArrayList<>(af.parameters());
        np.set(colsIx, new ColSpecArray(specs));
        return af.withParameters(np);
    }

    /** {@code col(fn,'name')} as a ColSpec (legacy TDSRow param annotation
     * stripped); {@code ^BasicColumnSpecification(func=fn, name='name')}
     * converts identically — the engine's col() helper constructs exactly
     * that instance (tds/tdsColumn.pure); null when the shape is anything
     * else. */
    private static @com.legend.base.Nullable ColSpec legacyColToSpec(ValueSpecification v) {
        com.legend.protocol.spec.LambdaFunction fn;
        com.legend.protocol.spec.CString nm;
        if (v instanceof AppliedFunction nw && AppliedFunction.isNew(nw)
                && nw.parameters().size() >= 2
                && nw.parameters().get(0)
                        instanceof com.legend.protocol.spec.PackageableElementPtr pep
                && (pep.fullPath().equals("BasicColumnSpecification")
                        || pep.fullPath().equals(
                                "meta::pure::tds::BasicColumnSpecification"))
                && nw.parameters().get(1)
                        instanceof com.legend.protocol.spec.NewInstance ni
                && ni.first("func") != null
                && ni.first("name") != null
                && ni.first("func").value()
                        instanceof com.legend.protocol.spec.LambdaFunction bf
                && ni.first("name").value()
                        instanceof com.legend.protocol.spec.CString bn) {
            fn = bf;
            nm = bn;
        } else if (v instanceof AppliedFunction cf && com.legend.builtin.TdsLegacy.COL.matches(cf)
                && cf.parameters().size() == 2
                && cf.parameters().get(0) instanceof com.legend.protocol.spec.LambdaFunction cfn
                && cf.parameters().get(1) instanceof com.legend.protocol.spec.CString cnm) {
            fn = cfn;
            nm = cnm;
        } else {
            return null;
        }
        java.util.List<com.legend.protocol.spec.Variable> params = fn.parameters().stream()
                .map(pv -> new com.legend.protocol.spec.Variable(pv.name())).toList();
        return new ColSpec(nm.value(),
                new com.legend.protocol.spec.LambdaFunction(params, fn.body()), null);
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
