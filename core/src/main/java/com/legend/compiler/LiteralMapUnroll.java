// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler;

import com.legend.compiler.spec.SourceSubst;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RAW-SPACE unroll of a STATEMENT-ROOT map over a spelled collection of
 * bound names (batch 72a, 2026-09-05):
 * <pre>
 *   [$result, $result2]->map(r | let orders = $r.values;
 *                                assertEquals(1, $orders->size());
 *                                assertEquals([2], $orders.id););
 * </pre>
 * becomes the element statements in order, each element's lets renamed
 * per element ({@code orders_0}, {@code orders_1}) so the single-
 * assignment scope stays honest. Pure evaluates the mapper once per
 * element in order and discards the statement's value; the unrolled
 * statements are that evaluation spelled out, so the element asserts
 * reach the statement-root verdict channel (AssertVerdicts) like any
 * other assert over an execute binding. Owns exactly this shape: a
 * top-level {@code map} whose collection is spelled variables only and
 * whose mapper is a one-parameter lambda; anything else is untouched.
 * Runs at the query front door ({@link com.legend.Compiler#resolveQuery})
 * before name resolution, beside the validate desugar.
 */
public final class LiteralMapUnroll {

    private LiteralMapUnroll() {
    }

    public static List<ValueSpecification> rewrite(List<ValueSpecification> statements) {
        List<ValueSpecification> out = new ArrayList<>(statements.size());
        for (ValueSpecification st : statements) {
            List<ValueSpecification> unrolled = unroll(st);
            if (unrolled == null) {
                out.add(st);
            } else {
                out.addAll(unrolled);
            }
        }
        return out;
    }

    private static @com.legend.Nullable List<ValueSpecification> unroll(ValueSpecification st) {
        if (!(st instanceof AppliedFunction af) || !isMap(af.function())
                || af.parameters().size() != 2) {
            return null;
        }
        if (!(af.parameters().get(0) instanceof PureCollection c) || c.values().isEmpty()
                || !c.values().stream().allMatch(v -> v instanceof Variable)) {
            return null;
        }
        if (!(af.parameters().get(1) instanceof LambdaFunction lam)
                || lam.parameters().size() != 1 || lam.body().isEmpty()) {
            return null;
        }
        String param = lam.parameters().get(0).name();
        List<ValueSpecification> out = new ArrayList<>();
        for (int k = 0; k < c.values().size(); k++) {
            Map<String, ValueSpecification> subst = new LinkedHashMap<>();
            subst.put(param, c.values().get(k));
            for (ValueSpecification s : lam.body()) {
                CString ln = SourceSubst.letName(s);
                if (ln != null) {
                    AppliedFunction let = (AppliedFunction) s;
                    String fresh = ln.value() + "_" + k;
                    ValueSpecification value = SourceSubst.substitute(
                            let.parameters().get(1), subst);
                    out.add(let.withParameters(List.of(new CString(fresh, ln.pos()), value)));
                    subst.put(ln.value(), new Variable(fresh, null, null, ln.pos()));
                } else {
                    out.add(SourceSubst.substitute(s, subst));
                }
            }
        }
        return out;
    }

    private static boolean isMap(String fn) {
        return fn.equals("map") || fn.equals("meta::pure::functions::collection::map");
    }
}
