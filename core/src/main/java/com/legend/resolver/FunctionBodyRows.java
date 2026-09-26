// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A function VALUE's body as rows (harness burn-down group A, 2026-09-03):
 * {@code $f.expressionSequence} over a lambda (a function reference
 * eta-expands to one) is its statements as ValueSpecification rows under
 * the lambda's scope — {@code functions(id, name)},
 * {@code value_specifications(id, function_id, ordinal, kind)} and each
 * statement's compiler-stamped inferred primary key
 * {@code vs_primary_key_columns(node_id, ordinal, name)} (PkInference).
 * The rows ride the query (constructed-scope inline relations); the
 * system database is never written for them.
 */
final class FunctionBodyRows {

    private FunctionBodyRows() {
    }

    /** The lambda's scope id — a content id (a lambda has no source span
     * of its own; the same object meets the resolver as the chain root). */
    static String scopeId(TypedLambda lam) {
        String text = lam.toString();
        return "fn:" + Integer.toHexString(text.hashCode()) + ":" + text.length();
    }

    static Map<String, List<List<String>>> rows(String scope, TypedLambda lam,
            ModelContext ctx) {
        Map<String, List<List<String>>> out = new LinkedHashMap<>();
        out.put("functions", List.of(List.of(scope, "")));
        List<List<String>> vs = new ArrayList<>();
        List<List<String>> pk = new ArrayList<>();
        for (int i = 0; i < lam.body().size(); i++) {
            TypedSpec stmt = lam.body().get(i);
            String id = scope + "/" + i;
            nodeRows(vs, id, scope, i, null, 0, stmt);
            List<String> cols = PkInference.infer(stmt, ctx);
            for (int k = 0; k < cols.size(); k++) {
                pk.add(List.of(id, Integer.toString(k), cols.get(k)));
            }
        }
        out.put("value_specifications", vs);
        out.put("vs_primary_key_columns", pk);
        return out;
    }

    /** One expression-tree node and, preorder, its parametersValues
     * (the typed node's children — real m3: a FunctionExpression's
     * arguments, a property read's source): {@code [id, function_id,
     * ordinal, kind, parent_id, depth, mult_lower, mult_upper, var_name]}
     * — the compiler's inferred multiplicity stamped as the node's
     * Multiplicity rows (an unbounded upper bound is NULL). */
    private static void nodeRows(List<List<String>> vs, String id, String scope,
            int ordinal, @com.legend.base.Nullable String parent, int depth, TypedSpec n) {
        Multiplicity m = n.info().multiplicity();
        String lower = "0";
        String upper = "";
        if (m instanceof Multiplicity.Bounded b) {
            lower = Integer.toString(b.lower());
            upper = b.upper() == null ? "" : Integer.toString(b.upper());
        }
        List<String> row = new ArrayList<>(List.of(id, scope, Integer.toString(ordinal),
                kindOf(n)));
        row.add(parent == null ? "" : parent);
        row.add(Integer.toString(depth));
        row.add(lower);
        row.add(upper);
        row.add(n instanceof TypedVariable v ? v.name() : "");
        vs.add(row);
        if (n instanceof TypedLambda) {
            return;   // a nested lambda is an InstanceValue holding a function
        }
        List<TypedSpec> kids = n.children();
        for (int k = 0; k < kids.size(); k++) {
            nodeRows(vs, id + "/" + k, scope, k, id, depth + 1, kids.get(k));
        }
    }

    /** The m3 classifier of a typed node: a variable read, an instance
     * (literal, collection, lambda, constructor), else a function
     * expression (every call, property read, cast, if, match ...). */
    static String kindOf(TypedSpec n) {
        return switch (n) {
            case TypedVariable ignored -> "VariableExpression";
            case TypedLambda ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCollection ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedNewInstance ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedTypeRef ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedPackageableRef ignored -> "InstanceValue";
            // the literal node kinds, named (never a class-name prefix)
            case com.legend.compiler.spec.typed.TypedCString ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCInteger ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCFloat ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCDecimal ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCBoolean ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCDate ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCTime ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCLatestDate ignored -> "InstanceValue";
            // a column spec (`~name`, `~[a, b]`) and the CSV census node are
            // instance values in the engine's body model too (the former
            // class-name-prefix rule classified them so; kept explicit)
            case com.legend.compiler.spec.typed.TypedColSpec ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedColSpecArray ignored -> "InstanceValue";
            case com.legend.compiler.spec.typed.TypedCsvCensus ignored -> "InstanceValue";
            default -> "FunctionExpression";
        };
    }
}
