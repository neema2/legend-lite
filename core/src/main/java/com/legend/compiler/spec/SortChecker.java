package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSortInfo;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Relation {@code sort} + the {@code asc/desc/ascending/descending} sort keys
 * (engine {@code SortChecker}) &mdash; CHECKED generically against
 * {@code sort<X,T>(Relation<T>[1], SortInfo<X⊆T>[*]):Relation<T>[1]}: the kernel's
 * {@code ⊆} validates every key column against the source (accumulating across a
 * multi-key collection) and the output is the signature's {@code Relation<T>}.
 * This class only decides the SHAPE (relation sort vs collection sort), desugars
 * bare {@code ~col} keys to {@code asc(~col)} (engine's default direction), and
 * emits the construct nodes.
 */
final class SortChecker {

    private SortChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        AppliedFunction normalized = legacyStringSortToModern(af);
        if (!isRelationSortShape(normalized)) {
            return t.applyGeneric(normalized, env);   // collection sort rides the generic path
        }
        Application a = t.checkGeneric(withExplicitDirections(normalized), env);
        return new TypedSort(a.args().get(0), sortKeysOf(a.args().get(1)), a.out());
    }

    /**
     * Desugar the legacy TDS string-key sorts to the colspec form (engine
     * {@code SortChecker}'s legacy paths): {@code sort(rel, 'COL', SortDirection.DESC)}
     * &rarr; {@code sort(rel, desc(~COL))}; {@code sort(rel, ['A','B'])} &rarr;
     * {@code sort(rel, [asc(~A), asc(~B)])}. Non-legacy shapes pass through untouched.
     */
    private static AppliedFunction legacyStringSortToModern(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() == 3 && ps.get(1) instanceof CString col && ps.get(2) instanceof EnumValue dir) {
            // LOUD direction mapping: only ASC/DESC of a sort-direction enum
            // are meaningful — anything else silently sorting ascending was
            // an audit finding, not a feature.
            String fn = switch (dir.value()) {
                case "DESC" -> CoreFn.DESC.parseName();
                case "ASC" -> CoreFn.ASC.parseName();
                default -> throw new TypeInferenceException(
                        "sort direction must be ASC or DESC, got '"
                                + dir.value() + "' of " + dir.fullPath());
            };
            return new AppliedFunction(af.function(), List.of(ps.get(0),
                    new AppliedFunction(fn, List.of(new ColSpec(col.value())))));
        }
        if (ps.size() == 2 && ps.get(1) instanceof CString col) {
            return new AppliedFunction(af.function(), List.of(ps.get(0),
                    new AppliedFunction(CoreFn.ASC.parseName(), List.of(new ColSpec(col.value())))));
        }
        if (ps.size() == 2 && ps.get(1) instanceof PureCollection c && !c.values().isEmpty()
                && c.values().stream().allMatch(v -> v instanceof CString)) {
            List<ValueSpecification> keys = c.values().stream()
                    .<ValueSpecification>map(v -> new AppliedFunction(CoreFn.ASC.parseName(),
                            List.of(new ColSpec(((CString) v).value()))))
                    .toList();
            return new AppliedFunction(af.function(), List.of(ps.get(0), new PureCollection(keys)));
        }
        return af;
    }

    /**
     * Collection {@code sortBy(key)} / {@code sortByReversed(key)} (engine
     * {@code checkCollectionSortBy}): a fixed-direction sort by a key lambda,
     * checked generically against {@code sortBy<T,U|m>(col:T[m], key:{T[1]->U[1]}[0..1]):T[m]}.
     */
    static TypedSpec sortBy(Typer t, AppliedFunction af, Env env, boolean ascending) {
        Application a = t.checkGeneric(af, env);
        return new TypedSortBy(a.args().get(0), Args.lambda(a, 1), ascending, a.out());
    }

    /** {@code asc(~col)} / {@code desc(~col)}: checked generically against its registered signature. */
    static TypedSpec sortInfo(Typer t, AppliedFunction af, Env env, boolean ascending) {
        Application a = t.checkGeneric(af, env);
        return new TypedSortInfo(Args.colSpecName(a.args().get(0)), ascending, a.out());
    }

    /** A relation sort is one whose sort-info arg carries column specs ({@code asc/desc/~col}), not lambdas. */
    private static boolean isRelationSortShape(AppliedFunction af) {
        return af.parameters().size() >= 2 && carriesColSpec(af.parameters().get(1));
    }

    private static boolean carriesColSpec(ValueSpecification vs) {
        return switch (vs) {
            case ColSpec ignored -> true;
            case AppliedFunction f -> isSortDirection(f)
                    && f.parameters().size() == 1 && f.parameters().get(0) instanceof ColSpec;
            case PureCollection c -> !c.values().isEmpty()
                    && c.values().stream().allMatch(SortChecker::carriesColSpec);
            default -> false;
        };
    }

    private static boolean isSortDirection(AppliedFunction f) {
        Optional<CoreFn> fn = CoreFn.of(f.function());
        return fn.isPresent() && (fn.get() == CoreFn.ASC || fn.get() == CoreFn.DESC);
    }

    /**
     * Desugar bare {@code ~col} sort keys to {@code asc(~col)} (engine {@code SortChecker}'s
     * default direction), so the whole key list is uniformly {@code SortInfo}-typed and the
     * call checks against {@code sort<X,T>(Relation<T>[1], SortInfo<X⊆T>[*])} unchanged.
     */
    private static AppliedFunction withExplicitDirections(AppliedFunction af) {
        ValueSpecification keys = af.parameters().get(1);
        ValueSpecification wrapped = (keys instanceof PureCollection c)
                ? new PureCollection(c.values().stream().map(SortChecker::ascIfBare).toList())
                : ascIfBare(keys);
        List<ValueSpecification> params = new ArrayList<>(af.parameters());
        params.set(1, wrapped);
        return new AppliedFunction(af.function(), params);
    }

    private static ValueSpecification ascIfBare(ValueSpecification vs) {
        return vs instanceof ColSpec cs ? new AppliedFunction(CoreFn.ASC.parseName(), List.of(cs)) : vs;
    }

    /** Flatten the checked sort-key argument ({@code SortInfo} value(s)) into lowering-ready keys. */
    private static List<TypedSort.TypedSortKey> sortKeysOf(TypedSpec arg) {
        if (arg instanceof TypedCollection c) {
            return c.elements().stream().map(SortChecker::sortKeyOf).toList();
        }
        return List.of(sortKeyOf(arg));
    }

    private static TypedSort.TypedSortKey sortKeyOf(TypedSpec e) {
        if (e instanceof TypedSortInfo si) {
            return new TypedSort.TypedSortKey(si.column(), si.ascending());
        }
        throw new TypeInferenceException("sort expects asc(~col) or desc(~col) keys, got "
                + e.getClass().getSimpleName());
    }
}
