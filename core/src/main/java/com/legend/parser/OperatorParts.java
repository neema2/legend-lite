package com.legend.parser;

import com.legend.protocol.SourceInfo;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The engine's operator-part builders &mdash; a verbatim port of
 * {@code DomainParseTreeWalker}'s {@code booleanPart}/{@code arithmeticPart}/
 * {@code processOp}/{@code buildArithmeticOp}/{@code buildComparisonOp}/
 * {@code buildDivide}, operating on the part records {@link SpecParser}'s
 * combined-expression loop produces. Pure functions of the parts: all
 * token-cursor work stays in the parser; all tree shaping lives here.
 *
 * <p>The accumulator's operator is THREADED alongside the built node (the
 * {@link Acc} pair) rather than read back out of it &mdash; the builder knows
 * what it built, so the dispatch stays typed.
 */
final class OperatorParts {

    private OperatorParts() {
    }

    /**
     * One grammar {@code arithmeticPart} context: a same-op run
     * ({@code (PLUS expression)+} | minus/star/divide runs) or a single
     * comparison ({@code < <= > >= expression}). {@code ctxSpan} is the
     * ANTLR context's span &mdash; first operator token through the last
     * token of the last operand &mdash; which the engine stamps on the
     * built func AND its operand collection
     * ({@code walkerSourceInformation.getSourceInformation(ctx)}).
     */
    record ArithPart(String fn, boolean comparison,
            List<ValueSpecification> operands, SourceInfo ctxSpan) {
    }

    /** One grammar {@code booleanPart} context: {@code (AND|OR) expression}.
     *  Engine convention: and/or span the OPERATOR TOKEN only
     *  ({@code buildBoolean} uses the terminal's symbol). */
    record BoolPart(String fn, ValueSpecification operand, SourceInfo opSpan) {
    }

    /** The threaded accumulator: the built node plus the operator that built
     *  (or, after a last-parameter rewrite, kept) it. */
    private record Acc(AppliedFunction node, String op) {
    }

    /** Engine {@code booleanPart} + {@code processBooleanOp}: one boolean
     *  accumulator threaded across the group's contexts; {@code &&} after
     *  an {@code or} accumulator rewrites the accumulator's LAST parameter
     *  ({@code isLowerPrecedenceBoolean}), so {@code a || b && c} is
     *  {@code or(a, and(b, c))}. */
    static AppliedFunction booleanPart(List<BoolPart> parts, ValueSpecification input) {
        Acc acc = null;
        for (BoolPart p : parts) {
            if (acc == null) {
                acc = new Acc(buildBoolean(p, input), p.fn());
            } else if (acc.op().equals("or") && p.fn().equals("and")) {
                List<ValueSpecification> params = acc.node().parameters();
                AppliedFunction newAf = buildBoolean(p, params.get(params.size() - 1));
                acc = new Acc(new AppliedFunction(acc.node().function(),
                        List.of(params.get(0), newAf), acc.node().candidateFqns(),
                        acc.node().pos(), false, false, true), acc.op());
            } else {
                acc = new Acc(buildBoolean(p, acc.node()), p.fn());
            }
        }
        return Objects.requireNonNull(acc, "booleanPart on empty group").node();
    }

    private static AppliedFunction buildBoolean(BoolPart p, ValueSpecification input) {
        return new AppliedFunction(p.fn(), List.of(input, p.operand()),
                List.of(), p.opSpan(), false, false, true);
    }

    /** Engine {@code arithmeticPart}: comparisons always fold the whole
     *  accumulator ({@code buildComparisonOp}); plus/minus/times/divide
     *  thread through {@link #processOp}. */
    static AppliedFunction arithmeticPart(List<ArithPart> parts, ValueSpecification initial) {
        Acc acc = null;
        for (ArithPart p : parts) {
            if (p.comparison()) {
                acc = new Acc(buildComparisonOp(p,
                        acc == null ? initial : acc.node()), p.fn());
            } else {
                acc = processOp(p, acc, initial);
            }
        }
        return Objects.requireNonNull(acc, "arithmeticPart on empty group").node();
    }

    /**
     * Engine {@code processOp}, verbatim: {@code null} accumulator &rarr;
     * build; strictly-lower-precedence accumulator &rarr; rewrite its LAST
     * parameter with a node built from it; otherwise &rarr; build with the
     * accumulator as the initial (left) value.
     */
    private static Acc processOp(ArithPart p, @com.legend.base.Nullable Acc acc,
            ValueSpecification initial) {
        if (acc == null) {
            return new Acc(buildArithPart(p, initial), p.fn());
        }
        if (isStrictlyLowerPrecedence(acc.op(), p.fn())) {
            // The last-parameter rewrite. When the accumulator is RELATIONAL this
            // reproduces the engine's arithmetic mis-association after a comparison:
            // the tighter operator grabs the comparison's whole RHS, which may itself
            // already be a promoted arithmetic node — 1 < 2 + 3 * 4 becomes
            // lessThan(1, times[Collection[plus[Collection[2,3]], 4]]). The flag is
            // structurally true (see its javadoc for the decided-in-writing record);
            // flipping it means IMPLEMENTING the deep-rewrite correct mode, not
            // toggling a branch.
            AppliedFunction node = acc.node();
            if (acc.op().equals("divide")
                    || (EngineQuirks.RELATIONAL_ARITH_MISASSOCIATION
                            && isRelationalComparison(acc.op()))) {
                List<ValueSpecification> params = node.parameters();
                AppliedFunction newAf = buildArithPart(p, params.get(params.size() - 1));
                return new Acc(new AppliedFunction(node.function(), List.of(params.get(0), newAf), node.candidateFqns(),
                        node.pos(), false, false, true), acc.op());
            }
            // collection carrier (plus/minus/times): pop the last collection
            // element, build from it, re-collect. The engine rebuilds the
            // collection with the CLAIMING context's span (processOp's
            // this.collect(l.with(newAf), getSourceInformation(ctx))).
            PureCollection coll = (PureCollection) node.parameters().get(0);
            List<ValueSpecification> vals = coll.values();
            AppliedFunction newAf = buildArithPart(p, vals.get(vals.size() - 1));
            List<ValueSpecification> rebuilt = new ArrayList<>(vals.subList(0, vals.size() - 1));
            rebuilt.add(newAf);
            return new Acc(new AppliedFunction(node.function(), List.of(new PureCollection(rebuilt, p.ctxSpan())),
                    node.candidateFqns(), node.pos(), false, false, true), acc.op());
        }
        return new Acc(buildArithPart(p, acc.node()), p.fn());
    }

    /** Engine {@code buildArithmeticOp}/{@code buildDivide}: plus/minus/times
     *  wrap ALL operands in one collection parameter; divide left-folds
     *  pairwise. Both stamp the part's whole ctx span on every built node
     *  (and the collection). */
    private static AppliedFunction buildArithPart(ArithPart p, ValueSpecification initial) {
        if (p.fn().equals("divide")) {
            AppliedFunction af = null;
            ValueSpecification left = initial;
            for (ValueSpecification other : p.operands()) {
                af = new AppliedFunction("divide", List.of(left, other),
                        List.of(), p.ctxSpan(), false, false, true);
                left = af;
            }
            return Objects.requireNonNull(af, "divide part without operands");
        }
        List<ValueSpecification> vals = new ArrayList<>();
        vals.add(initial);
        vals.addAll(p.operands());
        return new AppliedFunction(p.fn(),
                List.of(new PureCollection(vals, p.ctxSpan())),
                List.of(), p.ctxSpan(), false, false, true);
    }

    private static AppliedFunction buildComparisonOp(ArithPart p, ValueSpecification initial) {
        return new AppliedFunction(p.fn(), List.of(initial, p.operands().get(0)),
                List.of(), p.ctxSpan(), false, false, true);
    }

    private static boolean isRelationalComparison(String fn) {
        return fn.equals("lessThan") || fn.equals("lessThanEqual")
                || fn.equals("greaterThan") || fn.equals("greaterThanEqual");
    }

    private static boolean isAdditiveOp(String fn) {
        return fn.equals("plus") || fn.equals("minus");
    }

    private static boolean isProductOp(String fn) {
        return fn.equals("times") || fn.equals("divide");
    }

    /** Engine {@code isStrictlyLowerPrecedence}: relational &lt; additive/
     *  product; additive &lt; product. */
    private static boolean isStrictlyLowerPrecedence(String op1, String op2) {
        return (isRelationalComparison(op1) && (isAdditiveOp(op2) || isProductOp(op2)))
                || (isAdditiveOp(op1) && isProductOp(op2));
    }
}
