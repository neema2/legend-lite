package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedColSpecArray;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortInfo;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A window definition {@code over(~partition [, asc(~key)…])} &mdash; checked
 * generically against the registered {@code over<T>(…):_Window<T>[1]} overloads
 * (the partition colspec binds {@code T} to its unsolved fragment, exactly like
 * {@code asc(~col)}); the enclosing {@code extend}'s {@code _Window<T>} parameter
 * later validates the columns against the real source row (the fragment-rebind
 * rule). This class only flattens the checked arguments into the construct node.
 * Frame clauses ({@code rows}/{@code range}) are a later slice and fail loudly.
 */
final class OverChecker {

    private OverChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        List<String> partitions = new ArrayList<>();
        List<TypedSort.TypedSortKey> keys = new ArrayList<>();
        Optional<TypedSpec> frame = Optional.empty();
        for (TypedSpec arg : a.args()) {
            if (isFrame(arg)) {
                frame = Optional.of(arg);   // rows(a,b) / range — a checked Frame value
            } else {
                collect(arg, partitions, keys);
            }
        }
        return new TypedOver(partitions, keys, frame, a.out());
    }

    /** A checked {@code Rows}/{@code _Range}/{@code _RangeInterval} frame value (the signature admits it in last position). */
    private static boolean isFrame(TypedSpec arg) {
        return arg.info().type() instanceof Type.ClassType ct
                && (ct.fqn().equals(Pure.ROWS.qualifiedName())
                        || ct.fqn().equals(Pure._RANGE.qualifiedName())
                        || ct.fqn().equals(Pure._RANGE_INTERVAL.qualifiedName()));
    }

    private static void collect(TypedSpec arg, List<String> partitions,
                                List<TypedSort.TypedSortKey> keys) {
        switch (arg) {
            case TypedColSpec cs -> partitions.add(cs.name());
            case TypedColSpecArray arr -> partitions.addAll(arr.names());
            case TypedSortInfo si -> keys.add(new TypedSort.TypedSortKey(si.column(), si.ascending()));
            case TypedCollection c -> c.elements().forEach(e -> collect(e, partitions, keys));
            default -> throw new TypeInferenceException(
                    "unsupported over(…) argument: " + arg.getClass().getSimpleName());
        }
    }
}
