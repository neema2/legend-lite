// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

/** Static literal folds over typed nodes (extracted from StoreResolver
 * at the file-size guardrail): the if(cond)-over-literals const-folder
 * family. */
final class LiteralFolds {

    private LiteralFolds() {
    }

    private static final String EQUAL_FQN =
            "meta::pure::functions::boolean::equal";
    private static final String EQ_FQN =
            "meta::pure::functions::boolean::eq";

    private static final String IS_EMPTY_FQN =
            "meta::pure::functions::collection::isEmpty";
    private static final String IS_NOT_EMPTY_FQN =
            "meta::pure::functions::collection::isNotEmpty";
    private static final String NOT_FQN = "meta::pure::functions::boolean::not";

    static @com.legend.base.Nullable Boolean staticBool(TypedSpec cond) {
        return switch (cond) {
            case TypedCBoolean b -> b.value();
            // emptiness of a LITERAL collection is static (the M3
            // elementOverride read types to the empty literal: the
            // corpus KeyInformation guard `if($x.elementOverride->isNotEmpty(), …)`)
            case TypedNativeCall c when c.args().size() == 1
                    && (IS_EMPTY_FQN.equals(c.callee().qualifiedName())
                            || IS_NOT_EMPTY_FQN.equals(c.callee().qualifiedName()))
                    && c.args().get(0)
                            instanceof com.legend.compiler.spec.typed.TypedCollection tc ->
                    IS_EMPTY_FQN.equals(c.callee().qualifiedName())
                            == tc.elements().isEmpty();
            case TypedNativeCall c when c.args().size() == 1
                    && NOT_FQN.equals(c.callee().qualifiedName()) -> {
                Boolean inner = staticBool(c.args().get(0));
                yield inner == null ? null : !inner;
            }
            case TypedNativeCall c when c.args().size() == 2
                    && (EQUAL_FQN.equals(c.callee().qualifiedName())
                            || EQ_FQN.equals(c.callee().qualifiedName())) -> {
                Object l = literalValue(c.args().get(0));
                Object r = literalValue(c.args().get(1));
                yield l == null || r == null ? null : (Boolean) literalEquals(l, r);
            }
            default -> null;
        };
    }

    static @com.legend.base.Nullable Object literalValue(TypedSpec n) {
        return switch (n) {
            case TypedCBoolean b -> b.value();
            case TypedCInteger i -> i.value();
            // B8: an exact-digit Float literal folds on its EXACT value
            // (literalEquals compares via BigDecimal, so 1.0000000000000001
            // vs ...2 stay distinct — a rounded double compare would lie)
            case TypedCFloat f -> f.exact() != null ? f.exact() : f.value();
            case TypedCString st -> st.value();
            default -> null;
        };
    }

    /**
     * Literal equality with SQL's semantics: NUMBERS compare numerically
     * (1 == 1.0 — SQL '=' would say true, so the fold must too; audit),
     * cross-kind value/string is a plain equals.
     */
    static boolean literalEquals(Object l, Object r) {
        if (l instanceof Number ln && r instanceof Number rn) {
            return new java.math.BigDecimal(ln.toString())
                    .compareTo(new java.math.BigDecimal(rn.toString())) == 0;
        }
        return l.equals(r);
    }

    /** An if() branch is a zero-arg thunk; its single body statement is the value. */
    static TypedSpec unthunk(TypedSpec branch) {
        if (branch instanceof TypedLambda l && l.parameters().isEmpty()
                && l.body().size() == 1) {
            return l.body().get(0);
        }
        return branch;
    }
}
