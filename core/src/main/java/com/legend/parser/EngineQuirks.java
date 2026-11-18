package com.legend.parser;

/**
 * Named, deliberate reproductions of legend-engine parser bugs.
 *
 * <p>Each constant names ONE wart of the engine's
 * {@code DomainParseTreeWalker} that legend-lite reproduces on purpose,
 * because the drop-in mandate is byte-identity with the engine's wire,
 * not correctness. Every constant is {@code true} today &mdash; there is
 * deliberately no mode switch and no configuration surface; the flag
 * exists so the reproducing code site is grep-able and the decision is
 * recorded in writing at the place a future maintainer will look first.
 * A "correct mode" arrives only when a consumer that wants it AND an
 * referee that can adjudicate it both exist; until then the single
 * behavior is the engine's.
 */
public final class EngineQuirks {

    private EngineQuirks() {
    }

    /**
     * PARSER_IMPLEMENTATION_AUDIT_2026_08 &sect;1.2 &mdash; decided in
     * writing 2026-08-06: <b>reproduce the engine byte-for-byte.</b>
     *
     * <p>The engine mis-associates arithmetic after a relational
     * operator: {@code 1 < 2 + 3 * 4} parses as
     * {@code lessThan(1, times[Collection[plus[Collection[2,3]], 4]])}
     * &mdash; i.e. {@code 1 < (2+3)*4 = 1 < 20} &mdash; while standalone
     * {@code 2 + 3 * 4} is {@code plus[Collection[2, times[Collection[3,4]]]]}
     * = 14. The engine contradicts itself between the two contexts.
     *
     * <p>Engine source: {@code DomainParseTreeWalker.processOp}
     * (legend-engine {@code .../grammar/from/domain/DomainParseTreeWalker.java},
     * lines ~1847-1885 at HEAD 2026-08) only ever rewrites the
     * accumulator's <em>last parameter</em>; once the accumulator is a
     * relational node ({@code isStrictlyLowerPrecedence}, ~1795-1799,
     * returns true for relational-vs-additive AND relational-vs-product),
     * every subsequent tighter operator re-targets a node that has
     * already been promoted into the comparison's RHS, so {@code * 4}
     * grabs {@code plus[2,3]} whole instead of just {@code 3}.
     *
     * <p>The CORRECT behavior we would switch to (were a consumer +
     * referee to exist): the tighter operator claims only the
     * <em>deepest last operand</em> &mdash; rewrite {@code 3} inside the
     * inner {@code plus} collection, yielding
     * {@code lessThan(1, plus[Collection[2, times[Collection[3,4]]]])}
     * = real Pure's precedence ({@code 1 < 14}), which is also what
     * legend-lite's pre-2026-08 precedence climb produced.
     *
     * <p>Referenced at the reproducing code site:
     * {@code SpecParser.processOp}'s last-parameter rewrite arm.
     */
    public static final boolean RELATIONAL_ARITH_MISASSOCIATION = true;
}
