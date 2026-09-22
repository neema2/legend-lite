// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The tree's own types (TYPED_SQL_IR.md): every expression stores its
 * type fact at construction — literals intrinsically, compositions via
 * the {@link SqlTyping} rule table over their children's stored facts,
 * lambda params through the attachment doors. (The former judge's
 * tests re-homed here when it deleted — same semantic claims, read
 * from the tree.)
 */
class SqlTypingTest {

    private static TypeFact t(SqlExpr e) {
        return e.type();
    }

    @Test
    void literalsAndCastsCarryTheirTypes() {
        assertEquals(SqlTyping.typed(SqlType.Scalar.VARCHAR),
                t(new SqlExpr.StringLit("a")));
        assertEquals(SqlTyping.typed(SqlType.Scalar.BIGINT),
                t(new SqlExpr.IntLit(1)));
        // NUMERIC CHARTER Rule 1 (2026-09-17): a Float literal renders BARE and
        // states the wire's fact — a DECIMAL of its own digits (DuckDB: 2.5 is
        // DECIMAL(2,1)); the declared-kind envelope converts it at the root
        assertEquals(SqlTyping.decimalLitType(new java.math.BigDecimal("2.5")),
                t(new SqlExpr.FloatLit(2.5)));
        assertEquals(SqlTyping.typed(SqlType.Scalar.DATE),
                t(new SqlExpr.Cast(new SqlExpr.StringLit("x"),
                        SqlType.Scalar.DATE)));
    }

    @Test
    void compositionsComputeFromStoredChildren() {
        SqlExpr arr = new SqlExpr.ArrayLit(List.of(
                new SqlExpr.IntLit(1), new SqlExpr.IntLit(2)));
        assertEquals(SqlTyping.typed(
                        new SqlType.Array(SqlType.Scalar.BIGINT)),
                t(arr));
        // LIST_TRANSFORM through the ATTACHMENT door: the param stamps
        // as the collection's element; the body's stored type flows up
        SqlExpr mapped = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, arr,
                SqlExpr.Lambda.bind(new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of(SqlFn.CONCAT,
                                new SqlExpr.Column(null, "x"),
                                new SqlExpr.StringLit("!"))), arr));
        assertEquals(SqlTyping.typed(
                        new SqlType.Array(SqlType.Scalar.VARCHAR)),
                t(mapped));
        SqlExpr struct = new SqlExpr.StructLit(List.of(
                new SqlExpr.StructLit.Field("a", new SqlExpr.IntLit(1)),
                new SqlExpr.StructLit.Field("b",
                        new SqlExpr.StringLit("s"))));
        assertEquals(SqlTyping.typed(new SqlType.Struct(List.of(
                        new SqlType.Struct.Field("a", SqlType.Scalar.BIGINT),
                        new SqlType.Struct.Field("b",
                                SqlType.Scalar.VARCHAR)))),
                t(struct));
    }

    @Test
    void bottomIsTheNullValue() {
        assertEquals(SqlTyping.BOTTOM, t(new SqlExpr.NullLit()));
        // a CASE whose every branch is the NULL value IS the NULL value
        assertEquals(SqlTyping.BOTTOM, t(new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(new SqlExpr.BoolLit(true),
                        new SqlExpr.NullLit())), new SqlExpr.NullLit())));
    }

    @Test
    void arithmeticPromotesPerTheProbedMatrix() {
        // any DOUBLE operand wins; all-integer keeps the widest width
        assertEquals(SqlTyping.typed(SqlType.Scalar.DOUBLE),
                t(SqlExpr.Call.of(SqlFn.PLUS,
                        new SqlExpr.IntLit(1),
                        new SqlExpr.Cast(new SqlExpr.FloatLit(2.0), SqlType.Scalar.DOUBLE))));
        // a BARE float literal is a DECIMAL (Rule 1): integer + DECIMAL(2,1)
        // is the probed DECIMAL(21,1), not a double
        assertEquals(SqlTyping.typed(new SqlType.Decimal(21, 1)),
                t(SqlExpr.Call.of(SqlFn.PLUS,
                        new SqlExpr.IntLit(1), new SqlExpr.FloatLit(2.0))));
        assertEquals(SqlTyping.typed(SqlType.Scalar.BIGINT),
                t(SqlExpr.Call.of(SqlFn.TIMES,
                        new SqlExpr.IntLit(2), new SqlExpr.IntLit(3))));
        // the NULL value propagates — arithmetic is strict
        assertEquals(SqlTyping.BOTTOM, t(SqlExpr.Call.of(SqlFn.PLUS,
                new SqlExpr.IntLit(1), new SqlExpr.NullLit())));
    }

    @Test
    void errorBranchesAreBottomLikeInBranchFamilies() {
        // the checked-extract shape: an error() guard branch plus a
        // LIST_GET over Array(LITERAL) — error() raises, it never
        // yields a value, so the family types from the value branch
        SqlExpr carried = new SqlExpr.Cast(
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("1"))),
                new SqlType.Array(SqlType.Scalar.LITERAL));
        SqlExpr checked = new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(new SqlExpr.BoolLit(true),
                        SqlExpr.Call.of(SqlFn.ERROR,
                                new SqlExpr.StringLit("index out of bounds")))),
                SqlExpr.Call.of(SqlFn.LIST_GET, carried,
                        new SqlExpr.IntLit(1)));
        // §E3: LIST_GET is nullable at the node (probed 1.5.0:
        // out-of-range list_extract -> NULL) and the branch family
        // carries it
        assertEquals(SqlTyping.nullable(
                SqlTyping.typed(SqlType.Scalar.LITERAL)), t(checked));
        // an ALL-error family is bottom-like as a whole (raise dominates)
        assertEquals(SqlTyping.BOTTOM, t(new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(new SqlExpr.BoolLit(false),
                        SqlExpr.Call.of(SqlFn.ERROR,
                                new SqlExpr.StringLit("boom")))),
                new SqlExpr.NullLit())));
    }

    @Test
    void noRuleMeansUnknownNeverAGuess() {
        // DECIMAL arithmetic: probed on the reference jar 2026-08-25
        // (was "deliberately UNKNOWN" until the version-specific
        // formula was pinned by probe) — BIGINT (19,0) + literal
        // (3,2): s=2, w=max(19,1)+2+1=22. RECEIPT: DuckDB 1.5.0
        // typeof(1::BIGINT + 1.50::DECIMAL(3,2)) = DECIMAL(22,2).
        assertEquals(SqlTyping.typed(new SqlType.Decimal(22, 2)),
                t(SqlExpr.Call.of(SqlFn.PLUS,
                        new SqlExpr.IntLit(1),
                        new SqlExpr.DecimalLit(
                                new java.math.BigDecimal("1.50")))));
        // MOD over decimals stays an unprobed corner — no rule, UNKNOWN
        assertEquals(SqlTyping.UNKNOWN, t(SqlExpr.Call.of(SqlFn.MOD,
                new SqlExpr.DecimalLit(new java.math.BigDecimal("1.5")),
                new SqlExpr.DecimalLit(new java.math.BigDecimal("1.0")))));
        // an unstamped column is UNKNOWN until its builder supplies it
        assertEquals(SqlTyping.UNKNOWN, t(new SqlExpr.Column("t", "c")));
    }

    // ------------------------------------------------------------------
    // §E3 M-N1 — THE NULLABILITY DIMENSION. Every non-default arm below
    // is a PROBED EMISSION receipt (DuckDB 1.5.0 reference jar,
    // 2026-08-26 battery — spellings from our own renderers).
    // ------------------------------------------------------------------

    private static SqlExpr nullableCol(String name, SqlType type) {
        return SqlExpr.Column.of("t",
                new OutputCol(name, type, true));
    }

    private static SqlExpr requiredCol(String name, SqlType type) {
        return SqlExpr.Column.of("t",
                new OutputCol(name, type, false));
    }

    private static boolean nul(SqlExpr e) {
        return ((TypeFact.Typed) e.type()).nullable();
    }

    @Test
    void nullabilityComposesAnyOperandByDefault() {
        // the Column doors transport the frame's declared nullability
        SqlExpr n = nullableCol("n", SqlType.Scalar.BIGINT);
        SqlExpr r = requiredCol("r", SqlType.Scalar.BIGINT);
        assertEquals(true, nul(n));
        assertEquals(false, nul(r));
        // default scalar composition: any-operand-nullable (strict SQL)
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.PLUS, n, r)));
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.PLUS, r, r)));
        // an UNKNOWN operand cannot prove presence — the safe side
        // (LENGTH types BIGINT whatever the operand; PLUS would poison
        // the KIND itself)
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.LENGTH,
                new SqlExpr.Column("t", "u"))));
        // Cast TRANSPORTS presence, both directions
        assertEquals(true, nul(new SqlExpr.Cast(n, SqlType.Scalar.DOUBLE)));
        assertEquals(false, nul(new SqlExpr.Cast(r, SqlType.Scalar.DOUBLE)));
        // literals are definite values
        assertEquals(false, nul(new SqlExpr.StringLit("a")));
    }

    @Test
    void nullabilityExceptionsAreProbedEmissions() {
        SqlExpr n = nullableCol("n", SqlType.Scalar.VARCHAR);
        SqlExpr r = requiredCol("r", SqlType.Scalar.VARCHAR);
        // concat() SKIPS NULL args (probed: concat('a', NULL) -> 'a',
        // concat(NULL, NULL) -> '') — never null, engine parity
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.CONCAT, n, n)));
        // the null tests always yield a boolean
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.IS_NULL, n)));
        // COALESCE/greatest/least: NULL only when EVERY operand is
        // (probed: greatest(1, NULL) -> 1)
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.COALESCE, n, r)));
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.COALESCE, n, n)));
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.GREATEST,
                nullableCol("a", SqlType.Scalar.BIGINT),
                requiredCol("b", SqlType.Scalar.BIGINT))));
        // x // 0 and mod(x, 0) -> NULL (probed), whatever the operands
        SqlExpr ri = requiredCol("i", SqlType.Scalar.BIGINT);
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.INT_DIVIDE, ri, ri)));
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.MOD, ri, ri)));
        // out-of-range list_extract -> NULL (probed)
        SqlExpr arr = new SqlExpr.ArrayLit(List.of(
                new SqlExpr.IntLit(1)));
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.LIST_GET, arr,
                new SqlExpr.IntLit(5))));
        // list_concat treats NULL as the empty list (probed) — and an
        // ArrayLit is a definite value even over nullable members
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.LIST_CONCAT,
                arr, arr)));
        // hash(NULL) yields a value through our signed reinterpretation
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.HASH, n)));
        // a missing CASE else is NULL (SQL); a full CASE of required
        // branches is not
        assertEquals(true, nul(new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(new SqlExpr.BoolLit(true), r)),
                null)));
        assertEquals(false, nul(new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(new SqlExpr.BoolLit(true), r)),
                r)));
        // membership: NULL needle -> NULL, NULL collection -> NULL
        // (the node's probed truth table)
        assertEquals(true, nul(new SqlExpr.Membership(n, arr)));
        assertEquals(false, nul(new SqlExpr.Membership(
                new SqlExpr.IntLit(1), arr)));
    }

    @Test
    void reducersAreNullableAtTheNodeExceptCount() {
        // probed: sum/min/string_agg over ZERO rows -> NULL; count -> 0
        SqlExpr r = requiredCol("r", SqlType.Scalar.BIGINT);
        assertEquals(true, nul(SqlAgg.Reducer.of(SqlAgg.Fn.SUM, r)));
        assertEquals(true, nul(SqlAgg.Reducer.of(SqlAgg.Fn.MIN, r)));
        assertEquals(false, nul(SqlAgg.Reducer.of(SqlAgg.Fn.COUNT, r)));
        // a scalar subquery over zero rows is NULL (probed)
        // (element stamps and empty-list reductions carry their own
        // receipts in the rule table; the differential census is the
        // corpus-wide witness roster)
    }

    @Test
    void groupByRefinesReducerSlots() {
        // §E3 M-N2 (probed 1.5.0 battery, 2026-08-27): under GROUP BY
        // the groups are non-empty by construction, so the empty-group
        // NULL drops and only the operand-derived part survives
        SqlExpr n = nullableCol("n", SqlType.Scalar.BIGINT);
        SqlExpr r = requiredCol("r", SqlType.Scalar.BIGINT);
        SqlExpr sumR = SqlAgg.Reducer.of(SqlAgg.Fn.SUM, r);
        SqlExpr sumN = SqlAgg.Reducer.of(SqlAgg.Fn.SUM, n);
        // ungrouped: nullable at the node (zero rows -> NULL)
        assertEquals(true, SqlTyping.slotNullable(sumR, false));
        // grouped over a required operand: a value on every group
        assertEquals(false, SqlTyping.slotNullable(sumR, true));
        // grouped over a NULLABLE operand: an all-NULL group still
        // sums to NULL (probed)
        assertEquals(true, SqlTyping.slotNullable(sumN, true));
        // LIST collects NULLs — non-null on any non-empty group
        // (probed [null])
        assertEquals(false, SqlTyping.slotNullable(
                SqlAgg.Reducer.of(SqlAgg.Fn.LIST, n), true));
        // the SAMP moment family needs n>=2 — NULL on a one-row group
        // (probed); it never refines
        assertEquals(true, SqlTyping.slotNullable(
                SqlAgg.Reducer.of(SqlAgg.Fn.STDDEV_SAMP, r), true));
        // the POP family yields 0.0 on any non-empty group (probed)
        assertEquals(false, SqlTyping.slotNullable(
                SqlAgg.Reducer.of(SqlAgg.Fn.VAR_POP, r), true));
        // COUNT is non-null in either mode
        assertEquals(false, SqlTyping.slotNullable(
                SqlAgg.Reducer.of(SqlAgg.Fn.COUNT, r), false));
        // non-reducer slots pass their node fact through
        assertEquals(true, SqlTyping.slotNullable(n, true));
        assertEquals(false, SqlTyping.slotNullable(r, true));
    }

    @Test
    void scalarSubqueryOneRowProof() {
        // §E3 slack fix 1 (probed: an ungrouped aggregate yields its
        // one row over ANY input — sum() over zero rows -> one NULL
        // row): a COUNT-rooted subquery is provably non-null; a
        // SUM-rooted one keeps the aggregate's own nullability; a
        // plain projection keeps zero-rows-is-NULL
        SqlExpr r = requiredCol("r", SqlType.Scalar.BIGINT);
        SqlSelect count = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        SqlAgg.Reducer.of(SqlAgg.Fn.COUNT, r), null, null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        assertEquals(false, nul(new SqlExpr.ScalarSubquery(count)));
        SqlSelect sum = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        SqlAgg.Reducer.of(SqlAgg.Fn.SUM, r), null, null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        assertEquals(true, nul(new SqlExpr.ScalarSubquery(sum)));
        SqlSelect plain = new SqlSelect(
                List.of(new SqlSelect.Projection(r, null, null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        assertEquals(true, nul(new SqlExpr.ScalarSubquery(plain)));
        // a GROUP BY voids the proof (groups can number zero)
        SqlSelect grouped = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        SqlAgg.Reducer.of(SqlAgg.Fn.COUNT, r), null, null)),
                false, new SqlSource.Dual(), null, List.of(r), null, null,
                List.of(), null, null, List.of());
        assertEquals(true, nul(new SqlExpr.ScalarSubquery(grouped)));
        // §E3-S extension: the aggregate may sit anywhere in the
        // projection — the joinStrings envelope
        // coalesce(string_agg(r, ','), '') is single-row AND non-null
        SqlSelect envelope = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.COALESCE,
                                SqlAgg.Reducer.of(SqlAgg.Fn.STRING_AGG, r,
                                        new SqlExpr.StringLit(",")),
                                new SqlExpr.StringLit("")), null, null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        assertEquals(false, nul(new SqlExpr.ScalarSubquery(envelope)));
        // a WINDOWED aggregate keeps per-row cardinality — no proof
        SqlSelect windowed = new SqlSelect(
                List.of(new SqlSelect.Projection(
                        new SqlExpr.WindowCall(
                                SqlAgg.Reducer.of(SqlAgg.Fn.SUM, r),
                                List.of(), List.of(), null), null, null)),
                false, new SqlSource.Dual(), null, List.of(), null, null,
                List.of(), null, null, List.of());
        assertEquals(true, nul(new SqlExpr.ScalarSubquery(windowed)));
    }

    @Test
    void unnestElementPurity() {
        // §E3 slack fix 2 (probed 1.5.0: unnest(NULL)/unnest([]) yield
        // ZERO rows — UNNEST's nullability is its ELEMENTS'):
        // CompactList strips NULL elements by contract; a pure
        // ArrayLit proves its elements; splitters never yield NULL
        // elements; a raw array read stays may-null
        SqlExpr pure = new SqlExpr.ArrayLit(List.of(
                new SqlExpr.StringLit("a"), new SqlExpr.StringLit("b")));
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.UNNEST,
                new SqlExpr.CompactList(pure))));
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.UNNEST, pure)));
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.UNNEST,
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("a"),
                        new SqlExpr.NullLit())))));
        assertEquals(false, nul(SqlExpr.Call.of(SqlFn.UNNEST,
                SqlExpr.Call.of(SqlFn.SPLIT, new SqlExpr.StringLit("a,b"),
                        new SqlExpr.StringLit(",")))));
        SqlExpr arrCol = SqlExpr.Column.of("t", new OutputCol("xs",
                new SqlType.Array(SqlType.Scalar.BIGINT), false));
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.UNNEST, arrCol)));
        // LIST_GET keeps its out-of-range NULL even over pure lists
        assertEquals(true, nul(SqlExpr.Call.of(SqlFn.LIST_GET,
                new SqlExpr.CompactList(pure), new SqlExpr.IntLit(5))));
    }

    // ------------------------------------------------------------------
    // §E3-S WHERE≡INNER: a null-rejecting WHERE neutralizes the pad.
    // ------------------------------------------------------------------

    private static SqlSelect starJoin(@com.legend.base.Nullable SqlExpr where) {
        SqlSource.Table a = new SqlSource.Table("A", "a0", List.of(
                new OutputCol("ID", SqlType.Scalar.BIGINT, false)));
        SqlSource.Table b = new SqlSource.Table("B", "b0", List.of(
                new OutputCol("BID", SqlType.Scalar.BIGINT, false)));
        SqlSource.Join j = new SqlSource.Join(a, b,
                SqlSource.Join.Kind.LEFT, new SqlExpr.BoolLit(true));
        // born outputs: pad-weakened (the joined() shape)
        List<OutputCol> born = List.of(
                new OutputCol("ID", SqlType.Scalar.BIGINT, false),
                new OutputCol("BID", SqlType.Scalar.BIGINT, true));
        return new SqlSelect(List.of(), false, j, where, List.of(),
                null, null, List.of(), null, null, born);
    }

    private static boolean outNullable(SqlSelect s, String name) {
        return s.outputs().stream().filter(c -> c.name().equals(name))
                .findFirst().orElseThrow().nullable();
    }

    @Test
    void nullRejectingWhereNeutralizesThePad() {
        SqlExpr.Column bid = SqlExpr.Column.of("b0",
                new OutputCol("BID", SqlType.Scalar.BIGINT, false));
        // a strict comparison on the pad side: no padded row survives
        // — DDL truth restored
        SqlSelect eq = starJoin(SqlExpr.Call.of(SqlFn.EQUAL, bid,
                new SqlExpr.IntLit(5)));
        assertEquals(false, outNullable(eq, "BID"));
        assertEquals(false, outNullable(eq, "ID"));
        // IS NULL is null-TOLERANT — the pad stands
        assertEquals(true, outNullable(starJoin(
                SqlExpr.Call.of(SqlFn.IS_NULL, bid)), "BID"));
        // an OR arm can pass NULL rows — the pad stands
        assertEquals(true, outNullable(starJoin(
                SqlExpr.Call.of(SqlFn.OR,
                        SqlExpr.Call.of(SqlFn.EQUAL, bid,
                                new SqlExpr.IntLit(5)),
                        new SqlExpr.BoolLit(true))), "BID"));
        // rejecting only the DRIVING side leaves the pad in place
        SqlExpr.Column id = SqlExpr.Column.of("a0",
                new OutputCol("ID", SqlType.Scalar.BIGINT, false));
        assertEquals(true, outNullable(starJoin(
                SqlExpr.Call.of(SqlFn.EQUAL, id,
                        new SqlExpr.IntLit(1))), "BID"));
        // IS NOT NULL is strict-rejecting — neutralizes
        assertEquals(false, outNullable(starJoin(
                SqlExpr.Call.of(SqlFn.IS_NOT_NULL, bid)), "BID"));
        // no WHERE: the pad stands
        assertEquals(true, outNullable(starJoin(null), "BID"));
    }
}
