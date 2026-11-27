// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * LITE'S COMPARISON SETTING ({@link CrossTypeRule}), the type-checker's own unless a door hands it another. A
 * comparison across types is a mistake wherever it is written: in the query, and in every body the query reaches --
 * a function's, a derived property's, a constraint's, a mapping's (the edition program's step 4, 2026-11-26: until
 * then a body's was the database's comparison). One exception is no mistake at all: a number literal written against
 * a value of another number type reads as that type when it is exact there ({@link NumberLiterals}; the user's ruling
 * of 2026-11-26), so {@code $x.qty > 0} over a Float is {@code > 0.0}.
 *
 * <p>A function generic over relations is typed into each call, over that call's columns: its column positions are
 * generic, so a comparison of two of them is never a mistake of the code as written ({@link #inGenericBody}) -- it is
 * the CALL's, where that call's columns are of two types, and is refused there, once the functions the query reaches
 * stand in the query ({@link #atTheCall}; the user's ruling of 2026-11-27, "the split with compile error when we
 * know"). The same of a comparison between a type variable's values. A side its author declared {@code Number}, the
 * abstract {@code Date} or {@code Any} is the author's word that the values are compared when the query runs: two
 * types there are never equal, and nothing is refused.
 *
 * <p>Every other comparison across types is typed as its marker, never raised where it is typed (the type-checker
 * tries a call's overloads in turn, and a comparison met in a candidate it then drops was never the program's). The
 * marker is refused by name, naming the two types and the fix: in the query by the planner before it plans anything
 * ({@link CrossTypeComparisons#refuse}); in a body when the body is typed whole ({@code SpecCompiler.compile}), so a
 * model's compile check and a query's run agree.
 */
public final class LiteCrossTypeRule implements CrossTypeRule {

    /** Lite's setting. */
    public static final LiteCrossTypeRule INSTANCE = new LiteCrossTypeRule();

    private LiteCrossTypeRule() {
    }

    @Override
    public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
        if (exact(crossing)) {
            return crossing.comparison();
        }
        TypedSpec read = NumberLiterals.read(crossing);
        return read != null ? read : crossing;
    }

    /** Whether {@code crossing} is between two EXACT numbers, an Integer and a Decimal (the user's ruling of
     *  2026-11-27, "Yes I like the rule, keep ordering strict": numbers are of two families, exact -- Integer and
     *  Decimal -- and approximate -- Float; within a family a comparison is by value, across the two it is a compile
     *  error, as before, and a written literal still reads as the other side's type where nothing is lost). Such a
     *  comparison is the comparison itself: the database compares the two values exactly. */
    private static boolean exact(TypedCrossTypeComparison crossing) {
        if (!exactNumber(crossing.left()) || !exactNumber(crossing.right())) {
            return false;
        }
        if (crossing.comparison() instanceof com.legend.compiler.spec.typed.TypedNativeCall call
                && call.args().size() == 2) {
            // a written whole number no Decimal holds (past its 38 digits) is no value of the other side's type: the
            // comparison stays a mistake at compile, never a database's overflow (the audit of batch A, 2026-11-26)
            boolean leftWhole = crossing.left() == com.legend.compiler.element.type.Type.Primitive.INTEGER;
            if (NumberLiterals.written(call.args().get(leftWhole ? 0 : 1))
                    && NumberLiterals.readWritten(crossing, leftWhole) == null) {
                return false;
            }
        }
        return true;
    }

    private static boolean exactNumber(com.legend.compiler.element.type.Type t) {
        return t == com.legend.compiler.element.type.Type.Primitive.INTEGER
                || t == com.legend.compiler.element.type.Type.Primitive.DECIMAL;
    }

    @Override
    public boolean readsBodies() {
        return true;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return inQuery(crossing);
    }

    /** A generic position's type is the call's, so a comparison of two of them is no mistake of the function as
     *  written, and is never raised where the body is typed: it is kept for its call ({@link #atTheCall}), where two
     *  different types are that call's mistake and are refused, naming the function (the user's ruling of 2026-11-27;
     *  from the edition program's step 4 to its step 6 it was the database's comparison). A number literal against a
     *  generic position still reads as this call's column type, which is what a literal is for. Kept, not raised
     *  here, because the type-checker types a function's candidates in turn: found by step 4's measurement,
     *  2026-11-26, the timeseries library's meeting of two lines compares their period columns, and refuses a day
     *  against a date-time itself, by an overload of its own, when the query runs, naming the fix; a compile error
     *  raised while the general overload was typed pre-empted that refusal. */
    @Override
    public TypedSpec inGenericBody(TypedCrossTypeComparison crossing) {
        if (exact(crossing)) {
            return crossing.comparison();
        }
        TypedSpec read = NumberLiterals.read(crossing);
        // (a body typed into its call: its sides are the caller's own columns, fixed by this call)
        return read != null ? read : new com.legend.compiler.spec.typed.TypedRunTimeComparison(crossing.comparison(),
                crossing.kind(), crossing.operator(), false, false, true, null);
    }

    /** Kept as its record until the function it is written in stands in the query ({@link #atTheCall}): a comparison
     *  between values whose types the caller gives is answered by those types (the equality rule's run-time item). */
    @Override
    public TypedSpec atRunTime(com.legend.compiler.spec.typed.TypedRunTimeComparison comparison) {
        return comparison;
    }

    /**
     * A comparison written in a function, answered by the types its sides have where the function is used (the
     * edition program's step 6; the user's rulings of 2026-11-26 on the run-time rule's decisions):
     * <ul>
     *   <li>one type, or a side still known only when the query runs: the comparison itself (the database compares
     *       the values it holds);</li>
     *   <li>a number literal the function's author wrote against a value of another number type: the literal reads
     *       as that type where it is exact there (decision D5: {@code $x->in([0, 1])} over a Float is by value),
     *       else it is no value of that type and the answer is the types';</li>
     *   <li>two values of two types where the CALL fixes them (the caller's own columns, a type variable's values;
     *       no side declared {@code Number}, the abstract {@code Date} or {@code Any}): this call's mistake, refused
     *       by name, as the same comparison written in the query is (the user's ruling of 2026-11-27);</li>
     *   <li>two values of two types where the author declared a side open: never equal ({@code ==} and {@code eq} false where a side is never empty,
     *       {@code in} and {@code contains} false, {@code indexOf} -1), and ordered by value, then by family, an
     *       exact number below a Float (decision D3: {@code <} and {@code <=} are both {@code <=} where the first
     *       side is the exact one, both {@code <} where it is the Float; {@code >} and {@code >=} the mirror), a fact
     *       of the two types folded into the operator. Any other ordering (two dates, a text against a number,
     *       {@code compare}, {@code max}, {@code min}) is the comparison itself, by value;</li>
     *   <li>an Integer against a Decimal: the comparison itself -- two exact numbers are equal by their value and
     *       ordered by it (the user's ruling of 2026-11-27).</li>
     * </ul>
     */
    @Override
    public TypedSpec atTheCall(com.legend.compiler.spec.typed.TypedRunTimeComparison marked,
            com.legend.compiler.element.ModelContext ctx) {
        if (!(marked.comparison() instanceof com.legend.compiler.spec.typed.TypedNativeCall call)) {
            return marked.comparison();
        }
        java.util.Optional<EqualityClasses.Crossing> known = EqualityClasses.crossing(call.callee(), call.args(), ctx);
        if (known.isEmpty()) {
            return call;
        }
        EqualityClasses.Crossing c = known.get();
        TypedCrossTypeComparison crossing = new TypedCrossTypeComparison(call, c.kind(), c.operator(), c.left(),
                c.right(), call.pos());
        if (exact(crossing)) {
            return call;
        }
        if (marked.leftWritten() != marked.rightWritten()) {
            TypedSpec read = NumberLiterals.readWritten(crossing, marked.leftWritten());
            if (read != null) {
                return read;
            }
        }
        if (marked.fixedByTheCall()) {
            // the caller's own columns, a type variable's values: this call makes the function compare two types,
            // which is the mistake a comparison written in the query is -- refused here, where the types are known
            throw crossing.refusalAtTheCall(marked.function());
        }
        return switch (c.kind()) {
            case EQUALITY -> neverEmpty(call) ? new com.legend.compiler.spec.typed.TypedCBoolean(false, call.info())
                    : call;
            case MEMBERSHIP -> new com.legend.compiler.spec.typed.TypedCBoolean(false, call.info());
            case INDEX -> new com.legend.compiler.spec.typed.TypedCInteger(-1L, call.info());
            case ORDERING -> byValueThenType(call, c, ctx);
        };
    }

    /** {@code equal([], [])} is true: only a side that is never empty makes the answer the types' alone. */
    private static boolean neverEmpty(com.legend.compiler.spec.typed.TypedNativeCall call) {
        for (TypedSpec side : call.args()) {
            if (side.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                    && b.lower() >= 1) {
                return true;
            }
        }
        return false;
    }

    /** A number type's place among equal values: an exact number before a Float; -1 for a type that is no number. */
    private static int place(com.legend.compiler.element.type.Type t) {
        return exactNumber(t) ? 0 : t == com.legend.compiler.element.type.Type.Primitive.FLOAT ? 1 : -1;
    }

    private static TypedSpec byValueThenType(com.legend.compiler.spec.typed.TypedNativeCall call,
            EqualityClasses.Crossing c, com.legend.compiler.element.ModelContext ctx) {
        int left = place(c.left());
        int right = place(c.right());
        boolean less = c.operator().equals("<") || c.operator().equals("<=");
        boolean greater = c.operator().equals(">") || c.operator().equals(">=");
        if (left < 0 || right < 0 || !less && !greater) {
            return call;
        }
        // among equal values the lower type is the lesser: the tie belongs to the comparison where it holds
        boolean tieHolds = less ? left < right : left > right;
        String name = call.callee().qualifiedName();
        String strict = name.endsWith("Equal") ? name.substring(0, name.length() - "Equal".length()) : name;
        String wanted = tieHolds ? strict + "Equal" : strict;
        if (wanted.equals(name)) {
            return call;
        }
        // the same comparison's other operator, over the same declared parameters
        for (com.legend.compiler.element.TypedFunction f : ctx.findFunction(wanted)) {
            if (sameParameters(f, call.callee())) {
                return new com.legend.compiler.spec.typed.TypedNativeCall(f, call.args(), call.info(), call.pos(),
                        call.outsideStore());
            }
        }
        throw new IllegalStateException("the platform declares no " + wanted + " over " + call.callee().id()
                + "'s parameters");
    }

    private static boolean sameParameters(com.legend.compiler.element.TypedFunction a,
            com.legend.compiler.element.TypedFunction b) {
        if (a.parameters().size() != b.parameters().size()) {
            return false;
        }
        for (int i = 0; i < a.parameters().size(); i++) {
            if (!a.parameters().get(i).type().equals(b.parameters().get(i).type())) {
                return false;
            }
        }
        return true;
    }
}
