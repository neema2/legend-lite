package com.legend.parser;

/**
 * Named, deliberate reproductions of the reference parsers' behaviours (legend-engine's, and where they are
 * legend-pure's too, Pure's definition).
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
     * writing 2026-08-06: <b>reproduce the association byte-for-byte.</b>
     *
     * <p>Arithmetic after a relational operator associates LEFT of a
     * tighter operator: {@code 1 < 2 + 3 * 4} parses as
     * {@code lessThan(1, times[Collection[plus[Collection[2,3]], 4]])}
     * &mdash; i.e. {@code 1 < (2+3)*4} &mdash; while standalone
     * {@code 2 + 3 * 4} is {@code plus[Collection[2, times[Collection[3,4]]]]}.
     *
     * <p>This is legend-pure's association as well as legend-engine's, so
     * it is Pure's definition, which the product follows -- not an
     * engine-only bug (J30 of the edge audit, withdrawn 2026-11-18, read
     * from the pinned sources): legend-engine's PCT test
     * {@code testNestedJoinArithmeticComparisonExpression}
     * ({@code core_functions_relation/relation/tests/composition.pure:1933})
     * joins on {@code $r2.val2 >= $r1.val1->toOne() + 8 + 9 * $r1.factor},
     * and legend-pure's expected rows hold only as
     * {@code (val1 + 8 + 9) * factor} (val1 -20 joins val2 8, 5 and 3);
     * the reverse-PCT translations of the same test write it so
     * ({@code xt-python-reversePCT-*}/{@code relation/relation.pure:2401}).
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
     * <p>Referenced at the reproducing code site:
     * {@code OperatorParts.processOp}'s last-parameter rewrite arm.
     */
    public static final boolean RELATIONAL_ARITH_MISASSOCIATION = true;
}
