package com.legend.parser;

import com.legend.protocol.SourceInfo;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins for the ENGINE-SHAPED expression tree at PARSE time &mdash; the
 * audit &sect;1.1/&sect;1.2/&sect;1.3 shapes that used to be patched (or
 * mis-patched) at emission by {@code ProtocolEmitter.rotateFlatBoolean}/
 * {@code naryArithmetic}, both deleted. Ground truth for every expected
 * tree is legend-engine's {@code DomainParseTreeWalker}
 * ({@code combinedExpression}/{@code booleanPart}/{@code arithmeticPart}/
 * {@code processOp}); the parser-equivalence module's composition pin
 * re-derives the same bytes from the live engine.
 */
final class EngineShapeArithmeticTest {

    private static AppliedFunction nary(String fn, ValueSpecification... operands) {
        return new AppliedFunction(fn,
                List.of(new PureCollection(List.of(operands))));
    }

    private static AppliedFunction binary(String fn, ValueSpecification l,
            ValueSpecification r) {
        return new AppliedFunction(fn, List.of(l, r));
    }

    @Test
    void standaloneArithmeticKeepsRealPrecedence() {
        // 2 + 3 * 4 — plus[Collection[2, times[Collection[3, 4]]]] = 14.
        // The engine gets this one RIGHT (processOp's last-element rewrite
        // inside the plus collection); the quirk fires only after a
        // relational accumulator (see below).
        assertEquals(
                nary("plus",
                        new CInteger(2L),
                        nary("times", new CInteger(3L), new CInteger(4L))),
                com.legend.testing.Platform.spec("2 + 3 * 4"));
    }

    @Test
    void standaloneArithmeticSpansAreTheEngineContexts() {
        // Same input, span rules (engine walkerSourceInformation on the
        // ANTLR contexts): the plus func keeps ITS run context ('+ 3',
        // cols 3-5) but its COLLECTION is re-stamped with the CLAIMING
        // times context ('* 4', cols 7-9) by processOp's rebuild — the
        // rule ProtocolEmitter.naryArithmetic used to re-derive at
        // emission and the parse now carries directly.
        AppliedFunction plus = (AppliedFunction) com.legend.testing.Platform.spec("2 + 3 * 4");
        assertEquals(new SourceInfo("", 1, 3, 1, 5), plus.pos());
        PureCollection coll = (PureCollection) plus.parameters().get(0);
        assertEquals(new SourceInfo("", 1, 7, 1, 9), coll.pos());
        AppliedFunction times = (AppliedFunction) coll.values().get(1);
        assertEquals(new SourceInfo("", 1, 7, 1, 9), times.pos());
        assertEquals(new SourceInfo("", 1, 7, 1, 9),
                ((PureCollection) times.parameters().get(0)).pos());
    }

    @Test
    void booleanThenArithmeticWrapsTheBoolean() {
        // 1 && 2 + 3 — audit §1.1 (120 of 170 measured DIFFs): booleanPart
        // is (AND|OR) expression, expression cannot contain arithmetic, so
        // '+ 3' falls out to the combined loop and the plus run WRAPS the
        // finished boolean: plus[Collection[and(1,2), 3]].
        assertEquals(
                nary("plus",
                        binary("and", new CInteger(1L), new CInteger(2L)),
                        new CInteger(3L)),
                com.legend.testing.Platform.spec("1 && 2 + 3"));
    }

    @Test
    void depthTwoBooleanArithmeticInterleaving() {
        // 1 || 2 < 3 && 4 — the depth-2 case the deleted emission-time
        // rotation got WRONG (audit §1.1: rotateFlatBoolean decided on the
        // unrotated child). Engine accumulator threading: or(1,2) folds
        // into the comparison, which folds into the trailing and:
        // and(lessThan(or(1,2), 3), 4).
        assertEquals(
                binary("and",
                        binary("lessThan",
                                binary("or", new CInteger(1L), new CInteger(2L)),
                                new CInteger(3L)),
                        new CInteger(4L)),
                com.legend.testing.Platform.spec("1 || 2 < 3 && 4"));
    }

    @Test
    void interruptedRunBuildsTwoTwoElementCollections() {
        // 1 + 2 * 3 + 4 — audit §1.3: '* 3' CLOSES the first (PLUS
        // expression)+ context; the second '+' opens a NEW one whose
        // initial value is the finished first plus. Two 2-element
        // collections — NOT one flat [1, times, 4]:
        // plus[Collection[plus[Collection[1, times[Collection[2,3]]]], 4]].
        assertEquals(
                nary("plus",
                        nary("plus",
                                new CInteger(1L),
                                nary("times", new CInteger(2L), new CInteger(3L))),
                        new CInteger(4L)),
                com.legend.testing.Platform.spec("1 + 2 * 3 + 4"));
    }

    @Test
    void relationalThenArithmeticReproducesEngineMisassociation() {
        // 1 < 2 + 3 * 4 — EngineQuirks.RELATIONAL_ARITH_MISASSOCIATION
        // (audit §1.2, decided in writing: reproduce byte-for-byte).
        // Once the accumulator is lessThan, processOp rewrites its LAST
        // parameter whole, so '* 4' grabs plus[2,3] instead of 3:
        // lessThan(1, times[Collection[plus[Collection[2,3]], 4]])
        // = 1 < (2+3)*4 = 1 < 20, NOT real Pure's 1 < 14.
        assertEquals(
                binary("lessThan",
                        new CInteger(1L),
                        nary("times",
                                nary("plus", new CInteger(2L), new CInteger(3L)),
                                new CInteger(4L))),
                com.legend.testing.Platform.spec("1 < 2 + 3 * 4"));
    }
}
