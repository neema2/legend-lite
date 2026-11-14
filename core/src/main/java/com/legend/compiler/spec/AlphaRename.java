// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * Renames every lambda binder in an INLINED body to a fresh
 * {@code _nr<N>} (outermost-first; occurrence substitution is
 * shadow-aware, so inner same-named binders keep their own scopes until
 * their own rename) — the capture-avoiding half of the typer's raw
 * β-expansion of a helper body. One counter per typer.
 */
final class AlphaRename {

    private int nrFresh;

    ValueSpecification apply(ValueSpecification v) {
        return switch (v) {
            case LambdaFunction lf -> {
                java.util.Map<String, ValueSpecification> ren = new java.util.LinkedHashMap<>();
                List<com.legend.protocol.spec.Variable> params = new ArrayList<>(lf.parameters().size());
                for (com.legend.protocol.spec.Variable p : lf.parameters()) {
                    String fresh = "_nr" + nrFresh++;
                    ren.put(p.name(), new com.legend.protocol.spec.Variable(
                            fresh, p.type(), p.multiplicity()));
                    params.add(new com.legend.protocol.spec.Variable(
                            fresh, p.type(), p.multiplicity()));
                }
                yield new LambdaFunction(params, lf.body().stream()
                        .map(b -> apply(SourceSubst.substitute(b, ren)))
                        .toList());
            }
            case AppliedFunction af2 -> af2.withParameters(
                    af2.parameters().stream().map(this::apply).toList());
            case com.legend.protocol.spec.AppliedProperty ap -> new com.legend.protocol.spec.AppliedProperty(
                    apply(ap.receiver()), ap.property());
            case com.legend.protocol.spec.PureCollection pc -> new com.legend.protocol.spec.PureCollection(
                    pc.values().stream().map(this::apply).toList());
            case com.legend.protocol.spec.ColSpec cs -> new com.legend.protocol.spec.ColSpec(cs.name(),
                    cs.function1() == null ? null : (LambdaFunction) apply(cs.function1()),
                    cs.function2() == null ? null : (LambdaFunction) apply(cs.function2()),
                    cs.alias(),
                    cs.args().stream().map(this::apply).toList());
            case com.legend.protocol.spec.ColSpecArray ca -> new com.legend.protocol.spec.ColSpecArray(
                    ca.colSpecs().stream()
                            .map(c -> (com.legend.protocol.spec.ColSpec) apply(c)).toList());
            default -> v.mapChildren(this::apply);
        };
    }

    /** A function's body as a call inlines it: {@link #apply}'s binders, and each of its parameters renamed fresh too,
     *  so a caller's name is never captured by a parameter's binding (the second review, 2026-10-28). Each fresh name
     *  is in {@code parameters} at its parameter's place; null for a parameter the body never reads. */
    record Apart(List<String> parameters, ValueSpecification body) {
    }

    Apart apart(List<String> parameters, ValueSpecification body) {
        ValueSpecification own = apply(body);
        java.util.Map<String, ValueSpecification> ren = new java.util.LinkedHashMap<>();
        List<String> fresh = new ArrayList<>(parameters.size());
        for (String p : parameters) {
            // every binder in the body is fresh now, so a read of the parameter's name is the parameter's
            com.legend.protocol.spec.Variable read = firstRead(own, p);
            if (read == null) {
                fresh.add(null);
                continue;
            }
            com.legend.protocol.spec.Variable renamed = read.renamed("_nr" + nrFresh++);
            ren.put(p, renamed);
            fresh.add(renamed.name());
        }
        return new Apart(fresh, SourceSubst.substitute(own, ren));
    }

    private static com.legend.protocol.spec.@com.legend.base.Nullable Variable firstRead(ValueSpecification v, String name) {
        if (v instanceof com.legend.protocol.spec.Variable var) {
            return var.name().equals(name) ? var : null;
        }
        com.legend.protocol.spec.@com.legend.base.Nullable Variable[] found = {null};
        v.mapChildren(x -> {
            if (found[0] == null) {
                found[0] = firstRead(x, name);
            }
            return x;
        });
        return found[0];
    }
}
