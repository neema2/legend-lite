// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.protocol.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * The catalog FQNs a RESOLVED call names — the one reading of the resolver's
 * output every front-door pass shares: an exact FQN names itself; a bare
 * call carries the resolver's {@code candidateFqns} (user overloads it left
 * for signature matching), and a bare NATIVE stays bare — the bare-name
 * rule ({@link BareNames}) names it at the call's arity (the typer's own rule).
 * No pass matches a spelling or reads imports on its own.
 */
public final class ResolvedNames {

    private ResolvedNames() {
    }

    public static List<String> referents(AppliedFunction af) {
        if (af.function().contains("::")) {
            return List.of(af.function());
        }
        List<String> out = new ArrayList<>(af.candidateFqns());
        BareNames.catalog(af.function()).stream()
                .filter(n -> n.parameters().size() == af.parameters().size())
                .map(com.legend.model.NativeFunctionDefinition::qualifiedName)
                .filter(n -> !out.contains(n)).forEach(out::add);
        return out;
    }

    /** The catalog natives among the call's referents — empty when the call
     *  names no declared native (a form with no signature, a user function, a
     *  property probe). */
    public static List<com.legend.model.NativeFunctionDefinition> declaredNatives(AppliedFunction af) {
        List<com.legend.model.NativeFunctionDefinition> out = new ArrayList<>();
        for (String fqn : referents(af)) {
            out.addAll(com.legend.builtin.Pure.nativeFunctionsAt(fqn));
        }
        return out;
    }

    /** Whether the call names {@code fqn} (exactly, or among its referents). */
    public static boolean names(AppliedFunction af, String fqn) {
        return referents(af).contains(fqn);
    }

    /** The language form a call IS (build rebuild Phase 3): the form owning a full name the call resolves to (its
     *  referents: the exact FQN, or a bare name's — beside a referent no form owns, the argument types decide:
     *  ReceiverOwnedFunctions). A full name no form owns is read as written (the lite desugar's exact spellings); a bare
     *  name with no referent at all, by its spelling — the form's own syntax; a bare name whose referents no form owns
     *  is no form, whatever its spelling. */
    public static java.util.Optional<com.legend.platform.CoreFn> form(AppliedFunction af) {
        List<String> refs = referents(af);
        for (String fqn : refs) {
            java.util.Optional<com.legend.platform.CoreFn> owner = com.legend.platform.CoreFn.owning(fqn);
            if (owner.isPresent()) {
                return owner;
            }
        }
        return refs.isEmpty() || refs.contains(af.function())
                ? com.legend.platform.CoreFn.of(af.function()) : java.util.Optional.empty();
    }
}
