package com.legend.compiler.spec;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * TDS ERASURE, the result half (docs/TDS_ERASURE_DESIGN_2026_09_11.md R1 / §4b):
 * upstream's legacy TDS api RETURNS the nominal {@code TabularDataSet}; this
 * platform keeps Model B (a TDS IS a {@code Relation<T>} whose schema the checker
 * knows). A native whose declared output carries TabularDataSet (or a subclass —
 * TableTDS) — at the top, or inside the function type it returns
 * ({@code concatenateTemporalTdsQueries: LambdaFunction<{->TabularDataSet}>}) —
 * returns the argument's ACTUAL relation in that position: a direct relation
 * argument, or the relation a query-lambda argument returns. Applied by the
 * kernel to every resolution's output, beside its parameter half (TabularDataSet
 * admits any relation carrier) — a refinement of the registered signature's
 * output, never a bypass of its checks. The row half — TDSRow IS the erased row —
 * is {@code PlatformTypes.eraseTdsRow}.
 */
final class TdsErasure {

    private TdsErasure() {
    }

    static ExprType refineResult(ModelContext model, List<ExprType> args, ExprType out) {
        if (!mentionsTds(model, out.type())) {
            return out;
        }
        Type carried = null;
        for (ExprType a : args) {
            carried = Type.isRelation(a.type()) ? a.type() : lambdaResultRelation(a.type());
            if (carried != null) {
                break;
            }
        }
        return carried == null ? out : new ExprType(substitute(model, out.type(), carried), out.multiplicity());
    }

    private static boolean isTds(ModelContext model, Type t) {
        String raw = t instanceof Type.ClassType c ? c.fqn()
                : t instanceof Type.GenericType g ? g.rawFqn() : null;
        return raw != null && model.isSubtype(raw, PlatformTypes.TABULAR_DATA_SET);
    }

    private static boolean mentionsTds(ModelContext model, Type t) {
        return switch (t) {
            case Type.FunctionType f -> mentionsTds(model, f.result().type());
            case Type.GenericType g -> isTds(model, g)
                    || g.arguments().stream().anyMatch(a -> mentionsTds(model, a));
            default -> isTds(model, t);
        };
    }

    /** {@code t} with every TabularDataSet position replaced by {@code carried}. */
    private static Type substitute(ModelContext model, Type t, Type carried) {
        if (isTds(model, t)) {
            return carried;
        }
        return switch (t) {
            case Type.FunctionType f -> new Type.FunctionType(f.params(),
                    new Type.Param(substitute(model, f.result().type(), carried), f.result().multiplicity()));
            case Type.GenericType g -> {
                List<Type> args = new ArrayList<>(g.arguments().size());
                for (Type a : g.arguments()) {
                    args.add(substitute(model, a, carried));
                }
                yield new Type.GenericType(g.rawFqn(), args, g.multArguments());
            }
            default -> t;
        };
    }

    /** The relation a query lambda returns — bare function type or a
     *  Function/LambdaFunction carrier — or null. */
    private static @com.legend.base.Nullable Type lambdaResultRelation(Type t) {
        Type.FunctionType ft = t instanceof Type.FunctionType f ? f
                : t instanceof Type.GenericType g && g.arguments().size() == 1
                        && g.arguments().get(0) instanceof Type.FunctionType gf ? gf : null;
        return ft != null && Type.isRelation(ft.result().type()) ? ft.result().type() : null;
    }
}
