package com.legend.protocol.spec;

import com.legend.protocol.SourceInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Guards the one property the value-specification design rests on:
 * <b>source position is excluded from equality.</b>
 *
 * <p>Value specifications carry a {@code pos} so the parser can emit protocol
 * {@code sourceInformation} without a side table. But they are also compared structurally — by the
 * compiler, and by 111 hand-built assertions of the form
 * {@code assertEquals(new CInteger(42L), spec)}. If {@code equals} ever includes the position,
 * every one of those breaks, and two structurally identical expressions become unequal for no
 * semantic reason.
 *
 * <p>This test exists because deleting the override and letting the record default return is an
 * easy "cleanup". If you are here because this failed, the override was removed — restore it
 * rather than changing these expectations.
 */
class ValueSpecEqualityTest {

    private static final SourceInfo A = new SourceInfo("f.pure", 1, 1, 1, 2);
    private static final SourceInfo B = new SourceInfo("f.pure", 9, 9, 9, 9);

    @Test
    void integersAreEqualRegardlessOfPosition() {
        assertEquals(new CInteger(42L), new CInteger(42L, A));
        assertEquals(new CInteger(42L, A), new CInteger(42L, B));
        assertEquals(new CInteger(42L, A).hashCode(), new CInteger(42L, B).hashCode());
    }

    @Test
    void stringsAreEqualRegardlessOfPosition() {
        assertEquals(new CString("x"), new CString("x", A));
        assertEquals(new CString("x", A), new CString("x", B));
        assertEquals(new CString("x", A).hashCode(), new CString("x", B).hashCode());
    }

    /** Position insensitivity must not become value insensitivity. */
    @Test
    void differentValuesAreStillUnequal() {
        assertNotEquals(new CInteger(1L, A), new CInteger(2L, A));
        assertNotEquals(new CString("x", A), new CString("y", A));
    }

    /** The position is retained — excluded from equality, not discarded. */
    @Test
    void positionIsRetainedEvenThoughItIsNotCompared() {
        assertEquals(A, new CInteger(42L, A).pos());
        assertEquals(A, new CString("x", A).pos());
    }

    /** The literals and islands that once kept the record default (leg 8 step 2: a let's TDS value, written with the
     *  let's span, read back unequal). */
    @Test
    void literalsAndIslandsAreEqualRegardlessOfPosition() {
        AppliedFunction call = new AppliedFunction("f", java.util.List.of(new CInteger(1L)));
        LambdaFunction fn = new LambdaFunction(java.util.List.of(), java.util.List.of(new CInteger(1L)));
        java.util.List<java.util.function.Function<SourceInfo, ValueSpecification>> nodes = java.util.List.of(
                p -> TdsLiteral.of("a\n1", "#TDS{a\n1}#", p),
                p -> new SqlIsland("select 1", p),
                p -> new GqlIsland(new Gql.Document(java.util.List.of()), p),
                p -> new CByteArray("YQ==", p),
                p -> new QuotedTreeCall(call, new CInteger(2L), p),
                p -> new QuotedGrammarCall(call, java.util.List.of(fn), p));
        for (java.util.function.Function<SourceInfo, ValueSpecification> node : nodes) {
            assertEquals(node.apply(A), node.apply(B));
            assertEquals(node.apply(A).hashCode(), node.apply(B).hashCode());
            assertEquals(node.apply(null), node.apply(A));
        }
        assertNotEquals(new SqlIsland("select 1", A), new SqlIsland("select 2", A));
    }

    @Test
    void booleansAreEqualRegardlessOfPosition() {
        assertEquals(new CBoolean(true), new CBoolean(true, A));
        assertEquals(new CBoolean(true, A), new CBoolean(true, B));
        assertEquals(new CBoolean(true, A).hashCode(), new CBoolean(true, B).hashCode());
        assertNotEquals(new CBoolean(true, A), new CBoolean(false, A));
        assertEquals(A, new CBoolean(true, A).pos());
    }
}
