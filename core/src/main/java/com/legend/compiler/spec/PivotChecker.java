package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Relation {@code pivot(~cols, ~agg:map:reduce)} (engine {@code PivotChecker}).
 * CHECKED generically (source binds {@code T}, pivot colspecs validate via
 * {@code ⊆}, aggregates type per column); the output schema is then computed
 * engine-style: the STATIC half is the group-by columns (source &minus; pivot
 * &minus; each aggregate's value column, when its map body is a direct column
 * access), and the pivoted columns are data-dependent &mdash; concretized by the
 * standard {@code ->cast(@Relation<(…)>)} idiom. The aggregates ride the node
 * as the dynamic-column templates.
 */
final class PivotChecker {

    private PivotChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        List<TypedSpec> args = a.args();
        if (args.size() < 3) {
            throw new TypeInferenceException("pivot expects (source, ~cols, ~agg:map:reduce)");
        }
        TypedSpec source = args.get(0);
        if (!(source.info().type() instanceof Type.RelationType schema)) {
            throw new TypeInferenceException("pivot requires a relation source");
        }
        List<String> pivotCols = pivotColumns(args.get(1));
        List<TypedAggCol> aggs = Args.aggCols(args.get(args.size() - 1));

        // Group-by columns = source − pivot − aggregate value columns (a value column is
        // identified when the aggregate's map body is a direct column access, engine rule).
        Set<String> removed = aggs.stream()
                .map(agg -> agg.map().body().get(0))
                .filter(b -> b instanceof TypedPropertyAccess)
                .map(b -> ((TypedPropertyAccess) b).property())
                .collect(Collectors.toSet());
        removed.addAll(pivotCols);

        List<Type.Column> groupCols = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            if (!removed.contains(c.name())) {
                groupCols.add(c);
            }
        }
        return new TypedPivot(source, pivotCols, aggs,
                new ExprType(new Type.RelationType(groupCols), a.out().multiplicity()));
    }

    private static List<String> pivotColumns(TypedSpec arg) {
        return switch (arg) {
            case TypedColSpec cs -> List.of(cs.name());
            case TypedColSpecArray arr -> arr.names();
            default -> throw new TypeInferenceException(
                    "pivot expects ~col or ~[cols] pivot columns, got " + arg.getClass().getSimpleName());
        };
    }
}
