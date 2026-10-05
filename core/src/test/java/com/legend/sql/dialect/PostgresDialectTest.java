// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Postgres dialect's plans, pinned as text (2026-10-01 W5.5/P1 Postgres dialect). A
 * runtime whose connection declares {@code type: Postgres} plans Postgres SQL — the
 * browser planner's path, {@code Compiler.dialectOf(ctx, runtime)}. Every snapshot below
 * was executed on PostgreSQL 17 and, through DuckDB's {@code postgres_query} (the
 * product path), compared row for row with the DuckDB plan of the same query.
 */
class PostgresDialectTest {

    private static final String MODEL = """
            ###Relational
            Database pg::DB ( Table trades (id BIGINT PRIMARY KEY, sym VARCHAR(16), px DECIMAL(18,6),
                qty INTEGER, big BIGINT, ok BIT, d DATE, ts TIMESTAMP, f8 DOUBLE) )
            ###Connection
            RelationalDatabaseConnection pg::Conn { store: pg::DB; type: Postgres;
              specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime pg::RT { mappings: []; connections: [ pg::DB: [ c1: pg::Conn ] ]; }
            """;

    private static String plan(String query) {
        String sql = Compiler.query(Compiler.compileModel(MODEL), "#>{pg::DB.trades}#" + query).plan("pg::RT").sql();
        // the product splices the plan into COPY (SELECT … FROM (<sql>) sub): one statement
        assertFalse(sql.strip().endsWith(";"), "a plan must not end in ';': " + sql);
        return sql;
    }

    @Test
    void identifiersAreQuotedAndSortsSpellTheirNulls() {
        assertEquals("""
                SELECT "t0"."id" AS "id", "t0"."sym" AS "sym", "t0"."px" AS "px"
                FROM "trades" AS "t0"
                WHERE ("t0"."qty" IS NOT NULL AND "t0"."qty" > 10)
                ORDER BY "t0"."id" DESC NULLS FIRST
                LIMIT 5""", plan("->filter(r|$r.qty > 10)->select(~[id, sym, px])"
                + "->sort(~id->descending())->limit(5)"));
    }

    @Test
    void aggregatesDeliverTheirDeclaredKinds() {
        // avg(int) is numeric on Postgres: cast to the platform's DOUBLE; median and
        // mode are ordered-set aggregates
        assertEquals("""
                SELECT "t0"."sym" AS "sym", COUNT("t0"."id") AS "n", CAST(AVG(1.0 * "t0"."qty") AS DOUBLE PRECISION) AS "a", \
                percentile_cont(0.5) WITHIN GROUP (ORDER BY "t0"."f8") AS "med", mode() WITHIN GROUP (ORDER BY "t0"."qty") AS "md"
                FROM "trades" AS "t0"
                GROUP BY "t0"."sym"\
                """, plan("->groupBy(~[sym], ~[n: r|$r.id : y|$y->count(),"
                + " a: r|$r.qty : y|$y->average(), med: r|$r.f8 : y|$y->median(),"
                + " md: r|$r.qty : y|$y->mode()])"));
    }

    @Test
    void aBooleanGroupsOneValueThroughBoolOr() {
        // DataCube's "the group's one value" over a boolean column: Postgres has no
        // max(boolean) (found driving DataCube against Postgres, 2026-10-02)
        String sql = plan("->groupBy(~[sym], ~[u: r|$r.ok : y|$y->uniqueValueOnly()])");
        assertTrue(sql.contains("bool_or(\"t0\".\"ok\")"), sql);
        assertFalse(sql.contains("MAX(\"t0\".\"ok\")"), sql);
    }

    @Test
    void aJsonColumnGroupsOneValueInJsonbsOwnOrder() {
        // DataCube's "the group's one value" over a Variant column, read as jsonb: Postgres has no
        // max(jsonb), though jsonb is ordered (found driving DataCube Live on Postgres, 2026-10-02)
        String model = MODEL.replace("f8 DOUBLE)", "f8 DOUBLE, doc SEMISTRUCTURED)");
        String sql = Compiler.query(Compiler.compileModel(model), "#>{pg::DB.trades}#->groupBy(~[sym], ~[u: r|$r.doc : y|$y->uniqueValueOnly()])").plan("pg::RT").sql();
        assertTrue(sql.contains("(array_agg(CAST(\"t0\".\"doc\" AS JSONB) ORDER BY CAST(\"t0\".\"doc\" AS JSONB) DESC NULLS LAST))[1]"), sql);
        assertFalse(sql.contains("MAX(CAST("), sql);
    }

    @Test
    void aConstantKeyIsATypedExpression() {
        // DataCube's root row groups by the constant '[ROOT]': Postgres refuses a bare
        // non-integer constant in GROUP BY and reads an integer as a position (found driving
        // DataCube Live on Postgres, 2026-10-02)
        assertEquals("""
                SELECT '[ROOT]' AS "root", COUNT("t0"."id") AS "n"
                FROM "trades" AS "t0"
                GROUP BY CAST('[ROOT]' AS VARCHAR)""", plan("->extend(~root: r|'[ROOT]')->groupBy(~[root], ~[n: r|$r.id : y|$y->count()])"));
        // a bare 7 would be "the seventh output"
        assertEquals("""
                SELECT 7 AS "k", COUNT("t0"."id") AS "n"
                FROM "trades" AS "t0"
                GROUP BY CAST(7 AS BIGINT)""", plan("->extend(~k: r|7)->groupBy(~[k], ~[n: r|$r.id : y|$y->count()])"));
        // ORDER BY reads a constant the same way (no Pure query of today sorts on one, so
        // the plan is built by hand)
        var k = new com.legend.sql.SqlExpr.StringLit("x");
        var q = new com.legend.sql.SqlSelect(
                java.util.List.of(new com.legend.sql.SqlSelect.Projection(k, "k", null)), false,
                new com.legend.sql.SqlSource.Dual(), null, java.util.List.of(), null, null,
                java.util.List.of(com.legend.sql.SqlSelect.SortKey.asc(k)), null, null,
                java.util.List.of(new com.legend.sql.OutputCol("k", com.legend.sql.SqlType.Scalar.VARCHAR, false)));
        assertEquals("""
                SELECT 'x' AS "k"
                ORDER BY CAST('x' AS VARCHAR) NULLS LAST""", new Postgres().render(q));
    }

    @Test
    void qualifyWrapsAndReadsThroughOutputs() {
        // no QUALIFY on Postgres: the window the filter reads is a hidden inner column,
        // the outer select lists the declared outputs, and the sort and limit run
        // AFTER the filter
        assertEquals("""
                SELECT "qualify_src"."sym" AS "sym", "qualify_src"."id" AS "id"
                FROM (
                  SELECT "t0"."sym" AS "sym", "t0"."id" AS "id", ROW_NUMBER() OVER (PARTITION BY "t0"."sym" ORDER BY "t0"."id" DESC NULLS FIRST) AS "__qualify0"
                  FROM "trades" AS "t0"
                ) AS "qualify_src"
                WHERE "qualify_src"."__qualify0" = 1
                ORDER BY "qualify_src"."sym" NULLS LAST
                LIMIT 3""", plan("->extend(over(~sym, ~id->descending()), ~[rn:{p,w,r|$p->rowNumber($r)}])"
                + "->filter(r|$r.rn == 1)->select(~[sym, id])->sort(~sym->ascending())->limit(3)"));
        assertEquals("""
                SELECT *
                FROM (
                  SELECT "t0"."id" AS "id", "t0"."sym" AS "sym", ROW_NUMBER() OVER (PARTITION BY "t0"."sym" ORDER BY "t0"."id" NULLS LAST) AS "rn"
                  FROM "trades" AS "t0"
                ) AS "qualify_src"
                WHERE "qualify_src"."rn" <= 2
                ORDER BY "qualify_src"."id" NULLS LAST
                LIMIT 4""", plan("->extend(over(~sym, ~id->ascending()), ~[rn:{p,w,r|$p->rowNumber($r)}])"
                + "->filter(r|$r.rn <= 2)->select(~[id, sym, rn])->sort(~id->ascending())->limit(4)"));
    }

    @Test
    void fullOuterJoinStaysNative() {
        String sql = plan("->filter(r|$r.id < 5)->join(#>{pg::DB.trades}#->select(~[id])->rename(~id, ~id2),"
                + " JoinKind.FULL, {a,b|$a.id == $b.id2})->select(~[id, id2])");
        assertTrue(sql.contains("FULL OUTER JOIN") && !sql.contains("UNION ALL"), sql);
    }

    @Test
    void temporalFunctionsAvoidTheSessionZone() {
        // a DATE is cast to a naive TIMESTAMP before date_trunc/to_char (whose date
        // argument would otherwise resolve to timestamptz); date arithmetic is amount
        // times a one-unit interval; date_diff counts boundaries, never age()
        assertEquals("""
                SELECT CAST(date_trunc('month', CAST("t0"."d" AS TIMESTAMP)) AS DATE) AS "fm", \
                "t0"."ts" + -90 * INTERVAL '1 minute' AS "plus", \
                CAST((extract(year FROM DATE '2026-03-01') - extract(year FROM "t0"."d")) * 12 \
                + (extract(month FROM DATE '2026-03-01') - extract(month FROM "t0"."d")) AS BIGINT) AS "mm", \
                to_char(CAST("t0"."d" AS TIMESTAMP), 'FMDay') AS "wd"
                FROM "trades" AS "t0"\
                """, plan("->extend(~[fm: r|$r.d->toOne()->firstDayOfMonth(),"
                + " plus: r|$r.ts->toOne()->adjust(-90, DurationUnit.MINUTES),"
                + " mm: r|dateDiff($r.d->toOne(), %2026-03-01, DurationUnit.MONTHS),"
                + " wd: r|$r.d->toOne()->dayOfWeek()])->select(~[fm, plus, mm, wd])"));
    }

    @Test
    void silentWrongAnswersAreSpelledAway() {
        // matches() anchors (~ is a PARTIAL match here); no ends_with; error() raises
        // lazily in the CASE's own type; a shift widens to BIGINT first (an INTEGER
        // shift is masked to 32 bits); sub-microsecond literal digits truncate (Postgres
        // would ROUND 9999-12-31 23:59:59.999999999 into year 10000)
        assertEquals("""
                SELECT ("t0"."sym" ~ '(?p)^(?:S1.*)$') AS "m", (right("t0"."sym", length('1')) = '1') AS "ew", \
                CASE WHEN "t0"."qty" - 1 = 0 THEN CAST(CAST(CAST(chr(31) || ('Division by zero') || chr(31) AS TIMESTAMPTZ) \
                AS VARCHAR) AS DOUBLE PRECISION) ELSE (CAST("t0"."qty" AS DOUBLE PRECISION) / CAST("t0"."qty" - 1 AS DOUBLE PRECISION)) \
                END AS "dv", (CAST(CAST("t0"."id" AS BIGINT) AS BIGINT) << CAST(40 AS INTEGER)) AS "sh"
                FROM "trades" AS "t0"
                WHERE "t0"."ts" < TIMESTAMP '9999-12-31T23:59:59.999999'""",
                plan("->filter(r|$r.ts->toOne() < %9999-12-31T23:59:59.999999999)"
                        + "->extend(~[m: r|$r.sym->toOne()->matches('S1.*'), ew: r|$r.sym->toOne()->endsWith('1'),"
                        + " dv: r|$r.qty->toOne() / ($r.qty->toOne() - 1),"
                        + " sh: r|$r.id->toOne()->bitShiftLeft(40)])->select(~[m, ew, dv, sh])"));
    }

    @Test
    void intervalFramesAreQuotedSingulars() {
        assertTrue(plan("->extend(over(~ok, ~ts->ascending(), _range(-2, DurationUnit.WEEKS, 3, DurationUnit.HOURS)),"
                + " ~[run:{p,w,r|$r.qty}:y|$y->plus()])->select(~[id, run])")
                .contains("RANGE BETWEEN INTERVAL '14' DAY PRECEDING AND INTERVAL '3' HOUR FOLLOWING"));
    }

    @Test
    void whatPostgresCannotSayIsAWall() {
        // a wall is honest; a guessed spelling is a silent wrong answer
        assertThrows(DialectCapability.class, () -> plan("->extend(~[h: r|$r.sym->toOne()->hashCode()])"
                + "->select(~[id, h])"));
        // round(double precision, int) does not exist: a Float to a scale is scaled, rounded half-even
        // (round(double precision) is rint) and scaled back, as DuckDB's ROUND_EVEN(x, s)
        assertTrue(plan("->extend(~[r: r|$r.f8->toOne()->round(2)])->select(~[id, r])").contains(
                "(round(CAST(\"t0\".\"f8\" AS DOUBLE PRECISION) * power(CAST(10 AS DOUBLE PRECISION), CAST(2 AS INTEGER)))"
                        + " / power(CAST(10 AS DOUBLE PRECISION), CAST(2 AS INTEGER)))"));
        // a dynamic pivot needs a key-discovery round trip a planner cannot make
        assertThrows(DialectCapability.class, () -> plan("->pivot(~[ok], ~[t: r|$r.qty : y|$y->sum()])"));
        // DuckDB's trim-zeros %g has no to_char code
        assertThrows(DialectCapability.class, () -> plan("->extend(~[s: r|$r.ts->toOne()->toString()])"
                + "->select(~[id, s])"));
    }

    @Test
    void theOtherDialectsAreUntouched() {
        // the same query on a DuckDB runtime: bare identifiers, DuckDB's ROUND_EVEN
        String duck = Compiler.query(Compiler.compileModel(MODEL.replace("type: Postgres", "type: DuckDB")), "#>{pg::DB.trades}#->extend(~[r: r|$r.f8->toOne()->round()])->select(~[id, r])").plan("pg::RT").sql();
        assertEquals("""
                SELECT t0.id, CAST(ROUND_EVEN(t0.f8, 0) AS BIGINT) AS r
                FROM trades AS t0""", duck);
        assertEquals("""
                SELECT "t0"."id" AS "id", CAST(round(CAST("t0"."f8" AS DOUBLE PRECISION)) AS BIGINT) AS "r"
                FROM "trades" AS "t0"\
                """, plan("->extend(~[r: r|$r.f8->toOne()->round()])->select(~[id, r])"));
    }
}
