package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

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
        Application a = t.checkGeneric(resolveClassRef(af, env), env);
        if (a.args().isEmpty() || !(a.args().get(0) instanceof TypedPackageableRef ref)) {
            throw new TypeInferenceException("getAll expects a class reference");
        }
        return new TypedGetAll(ref.fullPath(),
                a.args().subList(1, a.args().size()), false, false, a.out());
    }

    /** bind-once (family D): {@code let class = Person; getAll($class)} —
     * the anchor consumes a class reference by NODE KIND; resolve the
     * let-alias channel so the reference survives (engine parallel:
     * use-site inScopeVars resolution). Adopt only a reference — any
     * other rhs keeps its variable and the existing walls. */
    private static AppliedFunction resolveClassRef(AppliedFunction af, Env env) {
        if (af.parameters().isEmpty()) {
            return af;
        }
        com.legend.protocol.spec.ValueSpecification r =
                env.resolveAlias(af.parameters().get(0));
        if (r == af.parameters().get(0)
                || !(r instanceof com.legend.protocol.spec.PackageableElementPtr)) {
            return af;
        }
        java.util.List<com.legend.protocol.spec.ValueSpecification> ps =
                new java.util.ArrayList<>(af.parameters());
        ps.set(0, r);
        return af.withParameters(ps);
    }

    /** {@code Class.allVersions()} / {@code allVersionsInRange(s, e)}: the
     * VERSION-sweep fetch — same anchor node, sweep-flagged; the range
     * dates ride in the milestoning slot. */
    static TypedSpec checkVersions(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(resolveClassRef(af, env), env);
        if (a.args().isEmpty() || !(a.args().get(0) instanceof TypedPackageableRef ref)) {
            throw new TypeInferenceException(af.function() + " expects a class reference");
        }
        return new TypedGetAll(ref.fullPath(),
                a.args().subList(1, a.args().size()), true, false, a.out());
    }

    /** {@code Class->getAllForEachDate(dates)}: the per-date extent —
     * the DATES argument is a full store query whose result column
     * becomes the FROM root; the class table joins onto it with the
     * milestoning window correlated per date (engine
     * getMilestoningContextForAllForEachDate; golden shape pinned in
     * testGetAllForEachDate.pure:85). */
    static TypedSpec checkForEachDate(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(resolveClassRef(af, env), env);
        if (a.args().isEmpty() || !(a.args().get(0) instanceof TypedPackageableRef ref)) {
            throw new TypeInferenceException(
                    "getAllForEachDate expects a class reference");
        }
        return new TypedGetAll(ref.fullPath(),
                a.args().subList(1, a.args().size()), false, true, a.out());
    }
}
