package com.legend.compiler.element.type;

import com.legend.compiler.element.type.Type.PrecisionDecimal;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the decimal arithmetic precision/scale derivation rules on
 * {@link PrecisionDecimal} &mdash; the universal MS-SQL &rarr; Spark &rarr;
 * Calcite lineage (PHASE_G_SPEC_COMPILER.md §8). Expected values are computed
 * by hand from the formulas, not from the implementation.
 */
class PrecisionDecimalArithmeticTest {

    private static void assertPS(PrecisionDecimal d, int precision, int scale) {
        assertEquals(precision, d.precision(), "precision");
        assertEquals(scale, d.scale(), "scale");
    }

    @Test
    void multiply_addsPrecisionAndScale() {
        // (p1+p2+1, s1+s2): (18,2) * (5,4) -> (24, 6)
        assertPS(new PrecisionDecimal(18, 2).times(new PrecisionDecimal(5, 4)), 24, 6);
    }

    @Test
    void add_widensForCarry() {
        // scale = max(s1,s2) = 4; prec = max(s1,s2)+max(p1-s1,p2-s2)+1 = 4+8+1 = 13
        assertPS(new PrecisionDecimal(10, 2).plus(new PrecisionDecimal(10, 4)), 13, 4);
    }

    @Test
    void subtract_sharesAddDerivation() {
        assertPS(new PrecisionDecimal(10, 2).minus(new PrecisionDecimal(10, 4)), 13, 4);
    }

    @Test
    void divide_usesComputedScaleWhenAboveFloor() {
        // scale = max(6, s1+p2+1) = max(6, 2+8+1) = 11 (above the floor); prec = p1-s1+s2+scale = 18-2+4+11 = 31
        assertPS(new PrecisionDecimal(18, 2).dividedBy(new PrecisionDecimal(8, 4)), 31, 11);
    }

    @Test
    void divide_appliesMinimumScaleFloor() {
        // s1+p2+1 = 0+4+1 = 5 < 6, so scale floors to 6; prec = p1-s1+s2+scale = 5-0+2+6 = 13
        assertPS(new PrecisionDecimal(5, 0).dividedBy(new PrecisionDecimal(4, 2)), 13, 6);
    }

    @Test
    void divide_operandOrderMatters() {
        // a/b != b/a — guards against a this/other swap in the implementation.
        assertPS(new PrecisionDecimal(8, 4).dividedBy(new PrecisionDecimal(18, 2)), 29, 23);
    }

    @Test
    void overflow_clampsTo38_reducingScaleToFloor() {
        // (38,10)*(10,5): raw (49,15); 49>38 -> intDigits=34, minScale=min(15,6)=6,
        // adjustedScale = max(38-34, 6) = max(4,6) = 6 -> (38, 6)
        assertPS(new PrecisionDecimal(38, 10).times(new PrecisionDecimal(10, 5)), 38, 6);
    }

    @Test
    void overflow_preservesOriginalScaleWhenAlreadyBelowFloor() {
        // (38,3)*(10,2): raw (49,5); 49>38 -> intDigits=44, minScale=min(5,6)=5,
        // adjustedScale = max(38-44, 5) = max(-6,5) = 5 -> (38, 5)  (scale kept, not forced up to 6)
        assertPS(new PrecisionDecimal(38, 3).times(new PrecisionDecimal(10, 2)), 38, 5);
    }

    @Test
    void everyOperationYieldsAValidPrecisionDecimal() {
        // Property check: across a grid that exercises both the normal and overflow-clamp
        // paths, no operation may produce an invalid result (the ctor enforces 0 <= scale <= prec,
        // <= MAX_PRECISION). A formula that violated the invariant would throw here.
        for (int p1 = 1; p1 <= 20; p1++) {
            for (int s1 = 0; s1 <= p1; s1++) {
                for (int p2 = 1; p2 <= 20; p2++) {
                    for (int s2 = 0; s2 <= p2; s2++) {
                        PrecisionDecimal a = new PrecisionDecimal(p1, s1);
                        PrecisionDecimal b = new PrecisionDecimal(p2, s2);
                        for (PrecisionDecimal r : new PrecisionDecimal[]{
                                a.plus(b), a.minus(b), a.times(b), a.dividedBy(b)}) {
                            assertTrue(r.precision() <= PrecisionDecimal.MAX_PRECISION
                                            && r.scale() >= 0 && r.scale() <= r.precision(),
                                    "invalid result " + r + " from " + a + " op " + b);
                        }
                    }
                }
            }
        }
    }
}
