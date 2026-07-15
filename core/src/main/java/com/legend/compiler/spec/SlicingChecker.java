package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;

/**
 * The slicing family (engine {@code SlicingChecker}): {@code limit}/{@code take}
 * (SQL {@code LIMIT}), {@code drop} (SQL {@code OFFSET}), {@code slice}
 * ({@code LIMIT}+{@code OFFSET} over {@code [start, stop)}) &mdash; all fully
 * generic (schema-preserving {@code Relation<T>}/{@code T[*]} signatures); this
 * class only emits the construct nodes.
 */
final class SlicingChecker {

    private SlicingChecker() {
    }

    static TypedSpec limit(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedLimit(a.args().get(0), a.args().get(1), a.out());
    }

    static TypedSpec drop(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedDrop(a.args().get(0), a.args().get(1), a.out());
    }

    static TypedSpec slice(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedSlice(a.args().get(0), a.args().get(1), a.args().get(2), a.out());
    }
}
