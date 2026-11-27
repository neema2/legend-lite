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
 * generic, so a comparison of two of them is never a mistake of the code as written ({@link #inGenericBody}).
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
        TypedSpec read = NumberLiterals.read(crossing);
        return read != null ? read : crossing;
    }

    @Override
    public boolean readsBodies() {
        return true;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return inQuery(crossing);
    }

    /** A generic position is typed only when the query runs (the equality rule's second item), so a comparison of
     *  two of them is no compile error, whatever this call's columns are: it is answered by the types the two have at
     *  this call, as any comparison between values whose types a caller gives ({@link #atTheCall}; until the edition
     *  program's step 6 it was the database's comparison). A number literal against a generic position still reads as
     *  this call's column type, which is what a literal is for. Found by step 4's measurement, 2026-11-26: the
     *  timeseries library's meeting of two lines compares their period columns, and refuses a day against a date-time
     *  itself, when the query runs, naming the fix; a compile error here pre-empted that refusal. */
    @Override
    public TypedSpec inGenericBody(TypedCrossTypeComparison crossing) {
        TypedSpec read = NumberLiterals.read(crossing);
        return read != null ? read : new com.legend.compiler.spec.typed.TypedRunTimeComparison(crossing.comparison(),
                crossing.kind(), crossing.operator(), false, false);
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
     *   <li>two values of two types: never equal ({@code ==} and {@code eq} false where a side is never empty,
     *       {@code in} and {@code contains} false, {@code indexOf} -1), and ordered by value, then by type, an
     *       Integer below a Float below a Decimal (decision D3: {@code <} and {@code <=} are both {@code <=} where
     *       the first side's type is the lower, both {@code <} where it is the higher; {@code >} and {@code >=} the
     *       mirror), a fact of the two types folded into the operator. Any other ordering (two dates, a text against
     *       a number, {@code compare}, {@code max}, {@code min}) is the comparison itself, by value.</li>
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
        if (marked.leftWritten() != marked.rightWritten()) {
            TypedSpec read = NumberLiterals.readWritten(crossing, marked.leftWritten());
            if (read != null) {
                return read;
            }
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

    /** The order of the number types among equal values. */
    private static final java.util.List<com.legend.compiler.element.type.Type> NUMBER_TYPES = java.util.List.of(
            com.legend.compiler.element.type.Type.Primitive.INTEGER, com.legend.compiler.element.type.Type.Primitive.FLOAT,
            com.legend.compiler.element.type.Type.Primitive.DECIMAL);

    private static TypedSpec byValueThenType(com.legend.compiler.spec.typed.TypedNativeCall call,
            EqualityClasses.Crossing c, com.legend.compiler.element.ModelContext ctx) {
        int left = NUMBER_TYPES.indexOf(c.left());
        int right = NUMBER_TYPES.indexOf(c.right());
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
