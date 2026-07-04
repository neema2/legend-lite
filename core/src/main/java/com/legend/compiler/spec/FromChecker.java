package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

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
            if (!(a.args().get(i) instanceof TypedPackageableRef ref)) {
                throw new TypeInferenceException("from() argument " + i
                        + " must be a mapping or runtime reference, got "
                        + a.args().get(i).getClass().getSimpleName());
            }
            refs.add(ref);
        }
        Optional<TypedPackageableRef> mapping = refs.size() >= 2 ? Optional.of(refs.get(0)) : Optional.empty();
        Optional<TypedPackageableRef> runtime = switch (refs.size()) {
            case 0 -> Optional.empty();
            case 1 -> Optional.of(refs.get(0));
            default -> Optional.of(refs.get(1));
        };
        return new TypedFrom(a.args().get(0), mapping, runtime, a.out());
    }
}
