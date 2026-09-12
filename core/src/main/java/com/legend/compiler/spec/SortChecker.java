package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSortInfo;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

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
        AppliedFunction byMeta = columnsMetaSortToModern(t, af, env);
        AppliedFunction normalized = legacyStringSortToModern(
                byMeta == null ? af : byMeta);
        if (!isRelationSortShape(normalized)) {
            return t.applyGeneric(normalized, env);   // collection sort rides the generic path
        }
        Application a = t.checkGeneric(withExplicitDirections(normalized), env);
        // provenance BEFORE normalization: string-keyed shapes are the
        // legacy TDS drop-in surface (engine-verbatim null placement);
        // colspec shapes are the modern relation API (pure null-largest)
        return new TypedSort(a.args().get(0), sortKeysOf(a.args().get(1)),
                !(legacyStringShape(af) || byMeta != null), a.out());
    }

    /**
     * A legacy TDS {@code sort(TabularDataSet[1], String[*])} whose key
     * argument is a PROPERTY READ the typer folds to a static string list
     * ({@code $tds.columns.name} — Typer.tdsColumnsMetaRead owns that fold:
     * column names are a static fact of the typed relation; engine
     * testConcatenateInQualifierWithComplexReturnType sorts by
     * {@code $result.values.columns.name}). The folded names land as the
     * legacy string-keyed shape {@code sort(tds, ['A','B',...])} for the
     * normalizer below. Null when the key is not such a read or does not
     * fold to a non-empty string list.
     */
    private static @com.legend.Nullable AppliedFunction columnsMetaSortToModern(
            Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() != 2 || !(ps.get(1) instanceof AppliedProperty)) {
            return null;
        }
        TypedSpec keys = t.synth(ps.get(1), env);
        if (!(keys instanceof TypedCollection tc) || tc.elements().isEmpty()
                || !tc.elements().stream().allMatch(e -> e instanceof TypedCString)) {
            return null;
        }
        List<ValueSpecification> names = new ArrayList<>(tc.elements().size());
        for (TypedSpec e : tc.elements()) {
            names.add(new CString(((TypedCString) e).value()));
        }
        return af.withParameters(List.of(ps.get(0), new PureCollection(names)));
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
            return af.withParameters(List.of(ps.get(0),
                    new AppliedFunction(fn, List.of(new ColSpec(col.value())))));
        }
        if (ps.size() == 2 && ps.get(1) instanceof CString col) {
            return af.withParameters(List.of(ps.get(0),
                    new AppliedFunction(CoreFn.ASC.parseName(), List.of(new ColSpec(col.value())))));
        }
        if (ps.size() == 2 && ps.get(1) instanceof PureCollection c && !c.values().isEmpty()
                && c.values().stream().allMatch(v -> v instanceof CString)) {
            List<ValueSpecification> keys = c.values().stream()
                    .<ValueSpecification>map(v -> new AppliedFunction(CoreFn.ASC.parseName(),
                            List.of(new ColSpec(((CString) v).value()))))
                    .toList();
            return af.withParameters(List.of(ps.get(0), new PureCollection(keys)));
        }
        return af;
    }

    /**
     * Collection {@code sortBy(key)} / {@code sortByReversed(key)} (engine
     * {@code checkCollectionSortBy}): a fixed-direction sort by a key lambda,
     * checked generically against {@code sortBy<T,U|m>(col:T[m], key:{T[1]->U[1]}[0..1]):T[m]}.
     */
    static TypedSpec sortBy(Typer t, AppliedFunction af, Env env, boolean ascending) {
        // #/Person/firstName!fn# — the path ALIAS names the engine's
        // o_<alias> sort-key column in the root form: the alias rides the
        // path node (real pure Path.name) and moves onto the typed node
        String keyAlias = af.parameters().size() == 2
                && af.parameters().get(1) instanceof com.legend.protocol.spec.PathLiteral pl
                ? pl.alias() : null;
        Application a = t.checkGeneric(af, env);
        return new TypedSortBy(a.args().get(0), Args.lambda(a, 1), ascending,
                keyAlias, a.out());
    }

    /** The LEGACY TDS string-key sort shapes — {@code sort(rel,'COL',Dir)},
     * {@code sort(rel,'COL')}, {@code sort(rel,['A','B'])}, and
     * {@code sort(rel, asc/desc('COL'))} (string arg, incl. the singleton
     * collection {@code desc(['X'])}). Judged on the ORIGINAL call, before
     * {@link #legacyStringSortToModern}/{@link #sortInfo} erase the shape. */
    private static boolean legacyStringShape(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() == 3 && ps.get(1) instanceof CString) {
            return true;
        }
        if (ps.size() < 2) {
            return false;
        }
        return legacyKey(ps.get(1));
    }

    private static boolean legacyKey(ValueSpecification vs) {
        return switch (vs) {
            case CString ignored -> true;
            case PureCollection c -> !c.values().isEmpty()
                    && c.values().stream().allMatch(SortChecker::legacyKey);
            case AppliedFunction f -> isSortDirection(f)
                    && f.parameters().size() == 1
                    && (f.parameters().get(0) instanceof CString
                            || (f.parameters().get(0) instanceof PureCollection pc
                                    && pc.values().size() == 1
                                    && pc.values().get(0) instanceof CString));
            default -> false;
        };
    }

    /** {@code asc(~col)} / {@code desc(~col)}: checked generically against its registered signature. */
    static TypedSpec sortInfo(Typer t, AppliedFunction af, Env env, boolean ascending) {
        // a SINGLETON collection collapses to its element in pure
        // (desc(['X']) binds desc(String[1]) — ledger cluster 44: the
        // multiplicity rule was encoded as an AST-shape rule)
        if (af.parameters().size() == 1
                && af.parameters().get(0) instanceof PureCollection pc1
                && pc1.values().size() == 1) {
            af = af.withParameters(List.of(pc1.values().get(0)));
        }
        // legacy TDS string key asc('COL') (upstream's tds::asc(String):SortInformation)
        // -> the relation sort key under upstream's MODERN name, ascending(~COL) /
        // descending(~COL) — the ColSpec overload lives only there (batch 5 leg 5c)
        if (af.parameters().size() == 1 && af.parameters().get(0) instanceof CString c) {
            af = new AppliedFunction((ascending ? CoreFn.ASC : CoreFn.DESC).parseName(),
                    List.of(new ColSpec(c.value())), af.candidateFqns(), af.pos(),
                    af.propertyCall(), af.grouped(), af.infix());
        }
        Application a = t.checkGeneric(af, env);
        // ascending(~col, NullOrder.FIRST|LAST) — the two-argument overload
        // (4.145.0): the placement is a literal enum value of NullOrder
        TypedSortInfo.NullOrder order = a.args().size() == 2
                ? nullOrderOf(a.args().get(1)) : null;
        return new TypedSortInfo(Args.colSpecName(a.args().get(0)), ascending, order, a.out());
    }

    /** {@code sortInfo->emptyFirst()} / {@code ->emptyLast()}: the same key with
     *  its null placement set (upstream's {@code ^SortInfo(nullOrder = …)}). */
    static TypedSpec nullOrder(Typer t, AppliedFunction af, Env env, TypedSortInfo.NullOrder order) {
        Application a = t.checkGeneric(af, env);
        if (!(a.args().get(0) instanceof TypedSortInfo si)) {
            throw new TypeInferenceException("emptyFirst/emptyLast expects a sort key (asc(~col) / desc(~col)), got "
                    + a.args().get(0).getClass().getSimpleName());
        }
        return new TypedSortInfo(si.column(), si.ascending(), order, a.out());
    }

    private static TypedSortInfo.NullOrder nullOrderOf(TypedSpec arg) {
        if (arg instanceof TypedEnumValue ev && PlatformTypes.NULL_ORDER.equals(ev.enumFqn())) {
            return TypedSortInfo.NullOrder.valueOf(ev.value());
        }
        throw new TypeInferenceException("a sort key's null order must be a literal NullOrder value, got "
                + arg.getClass().getSimpleName());
    }

    /** A relation sort is one whose sort-info arg carries column specs ({@code asc/desc/~col}), not lambdas. */
    private static boolean isRelationSortShape(AppliedFunction af) {
        return af.parameters().size() >= 2 && carriesColSpec(af.parameters().get(1));
    }

    private static boolean carriesColSpec(ValueSpecification vs) {
        return switch (vs) {
            case ColSpec ignored -> true;
            // ascending(~col[, NullOrder.X]) / descending(…); a key wrapped by
            // emptyFirst()/emptyLast() is the key it wraps (4.145.0 null forms)
            case AppliedFunction f when isNullPlacement(f) && f.parameters().size() == 1
                    -> carriesColSpec(f.parameters().get(0));
            case AppliedFunction f -> isSortDirection(f)
                    && (f.parameters().size() == 1 || f.parameters().size() == 2)
                    && (f.parameters().get(0) instanceof ColSpec
                            || f.parameters().get(0) instanceof CString
                            || (f.parameters().get(0)
                                    instanceof PureCollection pc1
                                    && pc1.values().size() == 1));
            case PureCollection c -> !c.values().isEmpty()
                    && c.values().stream().allMatch(SortChecker::carriesColSpec);
            default -> false;
        };
    }

    private static boolean isSortDirection(AppliedFunction f) {
        Optional<CoreFn> fn = CoreFn.of(f.function());
        return fn.isPresent() && (fn.get() == CoreFn.ASC || fn.get() == CoreFn.DESC);
    }

    private static boolean isNullPlacement(AppliedFunction f) {
        Optional<CoreFn> fn = CoreFn.of(f.function());
        return fn.isPresent() && (fn.get() == CoreFn.EMPTY_FIRST || fn.get() == CoreFn.EMPTY_LAST);
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
        return af.withParameters(params);
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
            return new TypedSort.TypedSortKey(si.column(), si.ascending(), si.nullOrder());
        }
        throw new TypeInferenceException("sort expects asc(~col) or desc(~col) keys, got "
                + e.getClass().getSimpleName());
    }
}
