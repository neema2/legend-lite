// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.sql.dialect;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A RAISE'S MESSAGE IS ITS KIND (phase D's D1.1, 2026-11-12; the orchestrator's addition 3): the product's dialects
 * write a {@link SqlExpr.RaiseText}'s kind's name and never its operands, so a raise's SQL is byte-identical with its
 * operands and without them -- no value's text built, no parameter bound for one -- on every database; the user's own
 * {@code fail} message is the one operand written. The compatibility edge's dialects write legend-pure's sentence
 * ({@code edge.PureMessages}).
 */
class RaiseTextTest {

    private static SqlSelect select(SqlExpr message) {
        OutputCol v = new OutputCol("v", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED);
        SqlExpr raise = SqlExpr.Call.of(SqlFn.ERROR, message, new SqlExpr.StringLit("3:7"));
        return new SqlSelect(List.of(new SqlSelect.Projection(raise, "v", v)), false, new SqlSource.Dual(), null,
                List.of(), null, null, List.of(), null, null, List.of(v));
    }

    /** {@code dialect}'s text of {@code q}, or its refusal's message: the two renders compared either way. */
    private static String text(SqlDialect dialect, SqlSelect q) {
        try {
            return dialect.render(q);
        } catch (RuntimeException refused) {
            return "refused: " + refused.getMessage();
        }
    }

    private static final SqlExpr OPERAND = new SqlExpr.Cast(new SqlExpr.Column("t", "operand_col"), SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER);

    @Test
    void aProductRaiseWritesItsSitesTokenAndNoOperand() {
        for (DatabaseType type : Databases.implemented()) {
            // a fresh dialect per statement: ClickHouse numbers each raise in its statement (rand(n), main's #81)
            String with = text(Databases.dialect(type), select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO,
                    List.of(OPERAND, new SqlExpr.StringLit("by")))));
            String without = text(Databases.dialect(type), select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO,
                    List.of())));
            if (type == DatabaseType.ClickHouse) {
                // ClickHouse reads a computed value of the raise, kept empty and never NULL, so the server holds the
                // raise back where its guard is false (PR #130, 2026-11-21: a raise reading no column was evaluated on
                // every row after an ARRAY JOIN) -- the value is read, never in the raise's text
                String read = ", substring(coalesce(toString(";
                assertTrue(with.contains(read) && with.contains("operand_col"), with);
                assertEquals(without, with.replaceAll(java.util.regex.Pattern.quote(read) + ".*?operand_col.*?\\), ''\\), 1,"
                        + " 0\\)", ""), type.name());
            } else {
                assertEquals(without, with, type.name());
                assertFalse(with.contains("operand_col"), type + ": " + with);
            }
            if (!with.startsWith("refused: ")) {
                // the site's bare token alone (phase D's Phase 2, raises as SITES, 2026-11-16): no kind's name
                assertTrue(with.contains("LG#1"), type + ": " + with);
                assertFalse(with.contains("DIVIDE_BY_ZERO"), type + ": " + with);
            }
        }
    }

    @Test
    void aUsersFailMessageIsWritten() {
        for (DatabaseType type : Databases.implemented()) {
            String fail = text(Databases.dialect(type), select(new SqlExpr.RaiseText(RaiseKind.USER_FAIL,
                    List.of(new SqlExpr.StringLit("my own words")))));
            assertTrue(fail.startsWith("refused: ") || fail.contains("my own words"), type + ": " + fail);
            assertEquals(fail, text(com.legend.edge.PureText.dialect(type), select(new SqlExpr.RaiseText(
                    RaiseKind.USER_FAIL, List.of(new SqlExpr.StringLit("my own words"))))), type.name());
        }
    }

    @Test
    void aRaisesMessageIsOnlyItsKindAndOperands() {
        // its kind, its operands, and its product words where it is data, a fixed text worded at planning -- never a
        // value (the D part 1 audit's S3 and its nit, 2026-11-15: the type the constructor discarded went)
        assertEquals(List.of("kind", "operands", "asData", "where"), Arrays.stream(SqlExpr.RaiseText.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName).toList());
    }

    /** The edge words legend-pure's sentence of each site after the run, from its record, on every database: its SQL
     *  is the product's (stage B's B1, 2026-11-20: until then the edge's SQL wrote the sentence where an operand was
     *  computed, byte for byte what the product wrote before the move -- PCT's assertErrors compare legend-pure's
     *  text). */
    @Test
    void theEdgeWritesLegendPuresSentence() {
        SqlExpr a = new SqlExpr.Cast(new SqlExpr.Column("t", "operand_col"), SqlType.Scalar.BIGINT, SqlExpr.Reason.USER);
        for (DatabaseType type : Databases.implemented()) {
            // rem's, its dividend computed: the product's token in SQL, the dividend <computed> in the words (D1,
            // register S389)
            SqlExpr rem = new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO, List.of(a));
            assertEquals(text(Databases.dialect(type), select(rem)), text(com.legend.edge.PureText.dialect(type),
                    select(rem)), type + ": rem's");
            String remWords = upstream(com.legend.edge.PureText.dialect(type), select(rem));
            if (remWords != null) {
                assertEquals("Cannot divide " + com.legend.result.RaiseWords.COMPUTED + " by zero", remWords,
                        type + ": rem's site");
            }
            // a site of literal operands: the product's token in SQL, upstream's sentence planned in its site (phase D's
            // Phase 2, 2026-11-16)
            SqlExpr bare = new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO, List.of());
            assertEquals(text(Databases.dialect(type), select(bare)), text(com.legend.edge.PureText.dialect(type),
                    select(bare)), type + ": Integer / Integer's");
            String upstream = upstream(com.legend.edge.PureText.dialect(type), select(bare));
            if (upstream != null) {
                assertEquals("Division by zero", upstream, type + ": Integer / Integer's site");
            }
        }
    }

    /** legend-pure's words of {@code q}'s one raise site, as {@code dialect} writes the statement and the edge words
     *  its site after the run, from its record (stage B's B1, 2026-11-20: {@code edge.PureMessages#worded}); null where
     *  the dialect refuses the statement. */
    private static @com.legend.base.Nullable String upstream(SqlDialect dialect, SqlSelect q) {
        RenderedStatement statement;
        try {
            statement = dialect.renderStatement(q);
        } catch (RuntimeException refused) {
            return null;
        }
        assertEquals(1, statement.sites().size(), dialect.getClass().getSimpleName() + ": " + statement.sites());
        RaiseSite site = statement.sites().get(0);
        return com.legend.result.RaiseWords.sentence(new com.legend.executionplan.ExecutionPlan.Site(1, site.kind(),
                null, site.operands().stream().map(com.legend.executionplan.Operand::of).toList()));
    }

    private static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }

    private static SqlExpr str(SqlExpr x) {
        return new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.USER);
    }

    private static SqlExpr lit(String s) {
        return new SqlExpr.StringLit(s);
    }

    /** Each moved family's raise against the message the lowering wrote before the move (D1.1, families 2-12). */
    @Test
    void theEdgeWritesEachMovedFamilysSentence() {
        SqlExpr a = new SqlExpr.Cast(new SqlExpr.Column("t", "operand_col"), SqlType.Scalar.BIGINT, SqlExpr.Reason.USER);
        SqlExpr b = new SqlExpr.Cast(new SqlExpr.Column("t", "other_col"), SqlType.Scalar.BIGINT, SqlExpr.Reason.USER);
        java.util.Map<SqlExpr, SqlExpr> moved = new java.util.LinkedHashMap<>();
        moved.put(new SqlExpr.RaiseText(RaiseKind.SHIFT_OUT_OF_RANGE, List.of()),
                lit("Unsupported number of bits to shift - max bits allowed is 62"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_PART_ABSENT, List.of(lit("hour"), a)),
                cat(lit("Cannot get hour for "), str(a)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.INDEX_OUT_OF_BOUNDS, List.of(a, b)),
                cat(lit("The system is trying to get an element at offset "), str(a),
                        lit(" where the collection is of size "), str(b)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.INSERT_OUT_OF_BOUNDS, List.of()),
                lit("add(set, index, value): index out of bounds"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SQRT_OF_NEGATIVE, List.of(a)),
                cat(lit("Unable to compute sqrt of "), SqlExpr.Call.of(SqlFn.FLOAT_SHORTEST_TEXT, a)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SIZE_NOT_MULTIPLICITY, List.of(lit("[1..*]"))),
                lit("Cannot cast a collection of size 0 to multiplicity [1..*]"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SIZE_NOT_MULTIPLICITY, List.of(lit("[1]"), b)),
                new SqlExpr.Call(SqlFn.CONCAT, List.of(lit("Cannot cast a collection of size "), str(b),
                        lit(" to multiplicity [1]"))));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SLICE_BOUNDS_INVERTED, List.of(a, b)),
                SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT,
                        lit("The low bound ("), str(a)), lit(") can't be higher than the high bound (")),
                        SqlExpr.Call.of(SqlFn.CONCAT, str(b), lit(") in a slice operation"))));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("day"),
                new SqlExpr.IntLit(2016), new SqlExpr.IntLit(12), new SqlExpr.IntLit(32))), lit("Invalid day: 2016-12-32"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("month"), new SqlExpr.IntLit(13))),
                lit("Invalid month: 13"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("second"),
                new SqlExpr.FloatLit(61.5))), lit("Invalid second: 61.5"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("hour"), a)),
                cat(lit("Invalid hour: "), str(a)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.CAST_IMPOSSIBLE, List.of(lit("Integer"), lit("Date"))),
                lit("Cast exception: Integer cannot be cast to Date"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.RANGE_STEP_ZERO, List.of()), lit("range step must not be 0"));
        // #81's frame guard (merged 2026-11-14): its two sentences, legend-pure's and S170's, as it raised them
        moved.put(new SqlExpr.RaiseText(RaiseKind.WINDOW_FRAME_INVERTED, List.of()), lit("Invalid window frame boundary"
                + " - lower bound of window frame cannot be greater than the upper bound!"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.WINDOW_FRAME_OFFSET_INVALID, List.of()), lit("Invalid window frame"
                + " boundary - a window frame bound written -(n) is n before the current row and any other n after it,"
                + " so a computed n must be neither negative nor empty (docs/SEMANTICS_REGISTER.md S170)"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.CONSTRAINT_UNEVALUABLE, List.of(lit("posInt"))),
                lit("Unable to evaluate constraint [posInt]: data not available - check your mappings"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.USER_FAIL, List.of(lit("my own words"))), lit("my own words"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.MAP_KEYS_MISALIGNED, List.of()),
                lit("The map key list does not align with the map value list."));
        moved.put(new SqlExpr.RaiseText(RaiseKind.MAP_KEY_NULL, List.of()), lit("Map keys can not be NULL."));
        moved.put(new SqlExpr.RaiseText(RaiseKind.MAP_KEYS_NOT_UNIQUE, List.of()), lit("Map keys must be unique."));
        moved.put(new SqlExpr.RaiseText(RaiseKind.TEXT_NOT_INTEGER, List.of(a)), SqlExpr.Call.of(SqlFn.CONCAT,
                lit("Could not convert string '"), a, lit("' to INT64")));
        moved.put(new SqlExpr.RaiseText(RaiseKind.TEXT_NOT_DATE, List.of(a)), SqlExpr.Call.of(SqlFn.CONCAT,
                lit("Could not parse '"), a, lit("' as a date")));
        moved.put(new SqlExpr.RaiseText(RaiseKind.VALUE_NOT_HELD, List.of(lit("an integer past 64 bits has no SQLite"
                + " value"))), lit("an integer past 64 bits has no SQLite value"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.FRACTION_NOT_KEPT, List.of(lit("the second "), a, lit(" has more"))),
                cat(lit("the second "), str(a), lit(" has more")));
        moved.put(new SqlExpr.RaiseText(RaiseKind.CSV_TOO_WIDE, List.of(lit("a record of the CSV file is wide"))),
                lit("a record of the CSV file is wide"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.LIST_LENGTHS_DIFFER, List.of(lit("zip"))),
                lit("zip: the two value lists differ in length"));
        for (DatabaseType type : Databases.implemented()) {
            for (var e : moved.entrySet()) {
                SqlExpr.RaiseText r = (SqlExpr.RaiseText) e.getKey();
                // every raise is the product's site on every door (stage B's B1, 2026-11-20): the edge's SQL the
                // product's token, and legend-pure's sentence worded after the run from the site's record -- byte for
                // byte the one the SQL wrote until then where its operands are literals, a computed one printed
                // <computed> (register S389)
                assertEquals(text(Databases.dialect(type), select(r)), text(com.legend.edge.PureText.dialect(type),
                        select(r)), type + ": " + r);
                if (r.kind() == RaiseKind.USER_FAIL) {
                    continue;   // a user's fail is its own message, no site (aUsersFailMessageIsWritten)
                }
                String upstream = upstream(com.legend.edge.PureText.dialect(type), select(r));
                if (upstream == null) {
                    continue;
                }
                if (e.getValue() instanceof SqlExpr.StringLit sentence) {
                    assertEquals(sentence.value(), upstream, type + ": " + r);
                } else {
                    assertTrue(upstream.contains(com.legend.result.RaiseWords.COMPUTED), type + ": " + r + " -> "
                            + upstream);
                }
            }
        }
    }
}
