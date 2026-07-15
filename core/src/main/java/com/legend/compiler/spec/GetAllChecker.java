package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;

/**
 * {@code Class.all()} (engine {@code GetAllChecker}) &mdash; the object-graph
 * source anchor. Fully generic type-wise ({@code getAll<T>(Class<T>[1]
 * [, Date[1] [, Date[1]]]):T[*]} &mdash; {@code T} binds from the class
 * reference, the milestoning-date overloads validate on the same path); this
 * class only emits the construct node store lowering resolves a mapping
 * against. A missing mapping is deliberately NOT a type error &mdash; compile
 * succeeds, the back-end surfaces it at the use site (engine's
 * compile-vs-link split).
 */
final class GetAllChecker {

    private GetAllChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        if (a.args().isEmpty() || !(a.args().get(0) instanceof TypedPackageableRef ref)) {
            throw new TypeInferenceException("getAll expects a class reference");
        }
        return new TypedGetAll(ref.fullPath(), a.args().subList(1, a.args().size()), a.out());
    }

    /** {@code Class.allVersions()} / {@code allVersionsInRange(s, e)}: the
     * VERSION-sweep fetch — same anchor node, sweep-flagged; the range
     * dates ride in the milestoning slot. */
    static TypedSpec checkVersions(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        if (a.args().isEmpty() || !(a.args().get(0) instanceof TypedPackageableRef ref)) {
            throw new TypeInferenceException(af.function() + " expects a class reference");
        }
        return new TypedGetAll(ref.fullPath(),
                a.args().subList(1, a.args().size()), true, a.out());
    }
}
