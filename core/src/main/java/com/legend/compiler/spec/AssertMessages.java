// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.base.Nullable;
import com.legend.builtin.NativeFn;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE MESSAGE OF AN ASSERT CALL (the asserts-as-plan-nodes program, step 2, 2026-11-04;
 * docs/ASSERT_NODES_PLAN_2026_11_04.md): an assert's caller may give its failure message -- a String, a format and its
 * arguments, or a function returning one (upstream's overloads, legend-pure essential/tests/*.pure, each reducing to
 * {@code assert(condition, | message)}). The call splits into its BASE overload (the family's form with no message,
 * whose arm the judges plan as before) and the message, a String-typed expression the plan carries as one more side --
 * so both judges fail with the caller's words, and with the same words by construction. A call of the base overload
 * splits to nothing: its default message is the family's (upstream's text), which each judge spells from the operands.
 *
 * <p>Planning only: a rewrite of the typed tree, nothing evaluated -- a format is the platform's {@code format} call,
 * lowered as any call; a function's message is its body.
 */
public final class AssertMessages {

    private AssertMessages() {
    }

    /** An assert call split: {@code base}, the family's overload with no message, and {@code message}, the caller's
     *  failure text as a String expression. */
    public record Split(TypedSpec base, TypedSpec message) {
    }

    /** {@code call} split, or null where it is no assert of a family with message overloads, or already the base
     *  overload, or its message is a shape this does not split (a function value that is not a one-expression lambda
     *  literal: the call is planned whole, as before). */
    public static @Nullable Split split(TypedSpec call, SpecCompiler specs) {
        if (!(call instanceof TypedNativeCall n)) {
            return null;
        }
        NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(call)).orElse(null);
        if (fn == null) {
            return null;
        }
        int base = baseArity(fn);
        if (base < 0) {
            return null;
        }
        List<TypedSpec> args = n.args();
        int extra = args.size() - base;
        TypedSpec message;
        if (extra == 1) {
            TypedSpec m = args.get(base);
            if (m instanceof TypedLambda l) {
                if (!l.parameters().isEmpty() || l.body().size() != 1) {
                    return null;
                }
                message = l.body().get(0);
            } else if (m.info().type() == Type.Primitive.STRING) {
                message = m;
            } else {
                return null;
            }
        } else if (extra == 2) {
            message = format(args.get(base), args.get(base + 1), specs);
            if (message == null) {
                return null;
            }
        } else {
            return null;
        }
        if (!spelled(message)) {
            // a message that reads (a variable, a query, a navigation): upstream evaluates it only where the assert
            // fails, and planning it beforehand may fail where the assert holds (2026-11-04: 55 corpus test-data
            // generation bodies, a message over a plan the program cannot plan) -- carried once the plan evaluates a
            // side lazily (the Branch node, docs/ASSERT_NODES_PLAN_2026_11_04.md); until then the call is planned whole
            return null;
        }
        TypedFunction overload = overload(fn.fqn(), base, specs);
        if (overload == null) {
            return null;
        }
        return new Split(new TypedNativeCall(overload, args.subList(0, base), n.info(), n.pos()), message);
    }

    /** {@code assert(true)}: an assert the compiler decided holds (a helper whose asserts fold over spelled values),
     *  planned by the judges as any assert. */
    public static TypedSpec holds(SpecCompiler specs) {
        TypedFunction f = java.util.Objects.requireNonNull(overload(PlatformTypes.ASSERT, 1, specs),
                "the platform declares assert(Boolean[1])");
        ExprType one = new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE);
        return new TypedNativeCall(f, List.of(new com.legend.compiler.spec.typed.TypedCBoolean(true, one)), one);
    }

    /** Whether {@code n} is SPELLED: literals, collections of them and calls over them, nothing read -- a message
     *  that always plans, whatever the assert's outcome. */
    static boolean spelled(TypedSpec n) {
        return switch (n) {
            case com.legend.compiler.spec.typed.TypedCString s -> true;
            case com.legend.compiler.spec.typed.TypedCInteger i -> true;
            case com.legend.compiler.spec.typed.TypedCFloat f -> true;
            case com.legend.compiler.spec.typed.TypedCDecimal d -> true;
            case com.legend.compiler.spec.typed.TypedCBoolean b -> true;
            case com.legend.compiler.spec.typed.TypedCDate d -> true;
            case com.legend.compiler.spec.typed.TypedCollection c -> c.elements().stream().allMatch(AssertMessages::spelled);
            case TypedNativeCall c -> c.args().stream().allMatch(AssertMessages::spelled);
            default -> false;
        };
    }

    /** The number of operands {@code fn}'s base overload takes, or -1 for a family with no message overloads. */
    static int baseArity(NativeFn.Verdict fn) {
        return switch (fn) {
            case ASSERT, ASSERT_FALSE, ASSERT_EMPTY, ASSERT_NOT_EMPTY -> 1;
            case ASSERT_EQUALS, ASSERT_NOT_EQUALS, ASSERT_SAME_ELEMENTS, ASSERT_SIZE, ASSERT_CONTAINS, ASSERT_EQ,
                    ASSERT_INSTANCE_OF, ASSERT_IS -> 2;
            case ASSERT_EQ_WITHIN_TOLERANCE -> 3;
            // no message overloads: the JSON strings' (two Strings), the grid equivalence (its fourth argument a
            // second tolerance), toCSV (an operand form)
            case ASSERT_JSON_STRINGS_EQUAL, ASSERT_TDS_EQUIVALENT, TO_CSV -> -1;
        };
    }

    private static @Nullable TypedFunction overload(String fqn, int arity, SpecCompiler specs) {
        return specs.ctx().findFunction(fqn).stream().filter(f -> f.parameters().size() == arity).findFirst()
                .orElse(null);
    }

    /** {@code format(formatString, formatArgs)}: upstream's {@code assert(condition, formatString, formatArgs)} is
     *  {@code assert(condition, | format(formatString, formatArgs))}. */
    private static @Nullable TypedSpec format(TypedSpec formatString, TypedSpec formatArgs, SpecCompiler specs) {
        TypedFunction f = overload(PlatformTypes.FORMAT, 2, specs);
        return f == null ? null : new TypedNativeCall(f, List.of(formatString, formatArgs),
                new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE));
    }
}
