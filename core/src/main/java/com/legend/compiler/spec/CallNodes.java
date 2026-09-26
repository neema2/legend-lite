// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.platform.ImplementationTable;

import java.util.List;

/**
 * THE MINT of a call node for a resolved overload — the resolved callee rides
 * the node, never a name. THE PICK (platform architecture untangle, step 4a):
 * the node's kind is the implementation table's answer, not the declaration's
 * kind. A declaration the platform runs by its own rule or form is minted a
 * NATIVE call whatever it is — a bodied declaration with a registered rule
 * lowers by the rule, never by its body. Only a Body row is minted a user call
 * and inlined.
 */
final class CallNodes {

    private CallNodes() {
    }

    static TypedSpec mint(ImplementationTable implementations, TypedFunction chosen,
            List<TypedSpec> args, ExprType out) {
        return mint(implementations, chosen, args, out, null);
    }

    /** The parsed-call form: the source span (the call-NAME token, the parser's
     * named-call convention) rides the native node — the raise-emission
     * provenance channel (leg 2). */
    static TypedSpec mint(ImplementationTable implementations, TypedFunction chosen,
            List<TypedSpec> args, ExprType out, com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) {
        // A call is a NATIVE node when the platform runs the declaration by its
        // own rule or form (the table's row), or when there is no body to run;
        // a bodied declaration the platform REFUSES stays a user call — the
        // inliner reads the Refused row and raises the wall. (Minting a refused
        // body as a native node bypassed the wall: the corpus's SQL-printer
        // bodies were inlined through their switch arms, 1.2s -> 100s,
        // 2026-09-25.)
        boolean nativeNode = chosen.body().isEmpty()
                || implementations.runsByRule(chosen.definition());
        return nativeNode
                ? NormalizeFolds.foldReflection(new TypedNativeCall(chosen, args, out, pos))
                : new TypedUserCall(chosen, args, out, pos);
    }
}
