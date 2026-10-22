package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.protocol.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
        Type.RelationType schema = Type.relationSchema(source.info().type());
        if (schema == null) {
            throw new TypeInferenceException("pivot requires a relation source");
        }
        List<String> pivotCols = pivotColumns(args.get(1));
        // STATIC form pivot(~col, [values], ~agg): the value list pins the
        // pivoted output columns to exactly those values, whether or not
        // they occur in the data (real pure's 4-arg overload).
        List<TypedSpec> values = args.size() >= 4
                ? (args.get(2) instanceof TypedCollection tc ? tc.elements() : List.of(args.get(2)))
                : List.of();
        List<TypedAggCol> aggs = Args.aggCols(args.get(args.size() - 1));

        // Group-by columns = source − pivot − every column an aggregate's map READS: the engine's rule (its
        // relational pivot groups by what no aggregate touches; DuckDB's PIVOT shares it, a column the USING
        // references being no implicit group key). Until 2026-10-18 only a map that was a bare column access removed
        // its column, and a pivot over an expression (the PCT's testPivot_SingleMultiple_Dynamic_Aggregation:
        // treePlanted * coefficient) kept both columns as static ones the database no longer answered (the plan
        // path program's step 7, found by the channel A shadow run).
        Set<String> removed = new java.util.HashSet<>();
        for (TypedAggCol agg : aggs) {
            for (TypedSpec b : agg.map().body()) {
                columnsRead(b, agg.map().parameters(), removed);
            }
        }
        removed.addAll(pivotCols);

        List<Type.Column> groupCols = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            if (!removed.contains(c.name())) {
                groupCols.add(c);
            }
        }
        // The aggregates double as the DYNAMIC-column templates: every pivoted
        // output column '<value>__|__<agg-name>' carries its aggregate's type
        // (the reduce body's — TypedAggCol's contract). The names are
        // data-dependent, the types are not.
        List<Type.Column> templates = aggs.stream()
                .map(agg -> {
                    ExprType out = agg.reduce().body().get(agg.reduce().body().size() - 1).info();
                    return new Type.Column(agg.name(), out.type(), out.multiplicity());
                })
                .toList();
        // a STATIC pivot (its values given) has its columns now: one per value and aggregate, named as the database
        // names a pivot column (value, separator, aggregate: the bare physical spelling, which the engine presents
        // quoted -- Type.RelationType.presentPivotName, at the header; pivotColumnType matches either), so a plan
        // carries them without asking the database (the plan path's pivot, 2026-10-17); a dynamic pivot keeps its
        // templates
        if (!values.isEmpty() && pivotCols.size() == 1) {
            List<Type.Column> concrete = new ArrayList<>(groupCols);
            for (TypedSpec v : values) {
                for (Type.Column template : templates) {
                    concrete.add(new Type.Column(
                            pivotValueText(v) + Type.RelationType.PIVOT_SEPARATOR + template.name(),
                            template.type(), template.multiplicity()));
                }
            }
            return new TypedPivot(source, pivotCols, values, aggs,
                    new ExprType(Type.relation(new Type.RelationType(concrete, List.of())),
                            a.out().multiplicity()));
        }
        return new TypedPivot(source, pivotCols, values, aggs,
                new ExprType(Type.relation(new Type.RelationType(groupCols, templates)),
                        a.out().multiplicity()));
    }

    /** Every column {@code n} reads off the map's row parameter ({@code $x.col}), anywhere in it. */
    private static void columnsRead(TypedSpec n, List<String> params, Set<String> out) {
        if (n instanceof TypedPropertyAccess pa && pa.source() instanceof TypedVariable v
                && params.contains(v.name())) {
            out.add(pa.property());
        }
        for (TypedSpec c : n.children()) {
            columnsRead(c, params, out);
        }
    }

    /** A pivot value's text in its column's name: a number's digits, a string itself, a boolean's word. */
    private static String pivotValueText(TypedSpec v) {
        return switch (v) {
            case TypedCInteger i -> String.valueOf(i.value());
            case TypedCString str -> str.value();
            case TypedCFloat f -> String.valueOf(f.value());
            case TypedCDecimal d -> d.value().toPlainString();
            case TypedCBoolean b -> String.valueOf(b.value());
            default -> throw new TypeInferenceException("pivot values must be literals, got "
                    + v.getClass().getSimpleName());
        };
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
