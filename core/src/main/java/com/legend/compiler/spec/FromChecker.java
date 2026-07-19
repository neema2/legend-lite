package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code from} (engine {@code FromChecker}) &mdash; binds a value to an execution
 * context. Fully generic type-wise (a passthrough: {@code from<T>(Relation<T>[1]
 * [, runtime:Any[1]])} and the M2M {@code from<T>(T[*], mapping, runtime)});
 * every non-source argument must be a packageable-element reference (mapping /
 * runtime), slotted onto the node for the back-end:
 * {@code from(src)} &rarr; (&mdash;, &mdash;); {@code from(src, runtime)} &rarr;
 * (&mdash;, runtime); {@code from(src, mapping, runtime)} &rarr; (mapping, runtime).
 */
final class FromChecker {

    private FromChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        List<TypedPackageableRef> refs = new ArrayList<>(a.args().size() - 1);
        for (int i = 1; i < a.args().size(); i++) {
            if (a.args().get(i) instanceof TypedPackageableRef ref) {
                refs.add(ref);
                continue;
            }
            // helper-CONSTRUCTED runtimes — from(src, mapping,
            // testRuntimeUS()) where the helper builds ^Runtime(
            // connectionStores=...): the instance's CONNECTION content is
            // harness-owned (the module runtime supplies connections for
            // every module Database; connection timezone divergences
            // surface as visible row FAILs), so the execution context
            // reduces to the mapping ref and the runtime SLOT stays
            // EMPTY. A runtime-only from() then walls loudly downstream
            // ("class query requires an execution context"). Anything not
            // statically Runtime-typed stays loud here.
            if (a.args().get(i).info().type()
                    instanceof com.legend.compiler.element.type.Type
                            .ClassType ct
                    && ct.fqn().equals("meta::core::runtime::Runtime")) {
                continue;
            }
            throw new TypeInferenceException("from() argument " + i
                    + " must be a mapping or runtime reference, got "
                    + a.args().get(i).getClass().getSimpleName());
        }
        // slotting by KIND when instance-runtimes dropped a ref: a sole
        // surviving ref that is a MAPPING slots as the mapping (the
        // 3-arg from(src, mapping, <instance runtime>) shape)
        boolean soleIsMapping = refs.size() == 1
                && a.args().size() >= 3;
        Optional<TypedPackageableRef> mapping =
                refs.size() >= 2 || soleIsMapping
                        ? Optional.of(refs.get(0)) : Optional.empty();
        Optional<TypedPackageableRef> runtime = switch (refs.size()) {
            case 0 -> Optional.empty();
            case 1 -> soleIsMapping ? Optional.empty()
                    : Optional.of(refs.get(0));
            default -> Optional.of(refs.get(1));
        };
        return new TypedFrom(a.args().get(0), mapping, runtime, a.out());
    }
}
