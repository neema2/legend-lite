// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.ValueSpecification;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The translator ({@link SqlToPure}) on its own, reading Postgres ({@link PostgresDialect}, the {@code #SQL{...}#}
 * island's language; named for it, the dialect program's D11, 2026-10-18, as {@code IqToPureTest} is for IQ's): the
 * Pure each query becomes, printed as a person would write it, Postgres's name and NULL rules, and each refusal's kind. The FROM source's columns come from a stand-in for the
 * typer, so no model is compiled here; {@code SqlIslandIntegrationTest} runs the same pipeline end to end.
 */
class PostgresToPureTest {

    /** A source column, as the typer reports it. */
    private record Col(String name, String type, boolean nullable, @com.legend.base.Nullable String storeType) {

        Col(String name, String type, boolean nullable) {
            this(name, type, nullable, null);
        }
    }

    /** The source table: ID Integer[1], firstName String[0..1], age Integer[0..1], dept String[1]. */
    private static final List<Col> COLUMNS = List.of(
            new Col("ID", "Integer", false),
            new Col("firstName", "String", true),
            new Col("age", "Integer", true),
            new Col("dept", "String", false));

    private static String pure(String sql) {
        return pure(sql, COLUMNS);
    }

    private static String pure(String sql, List<Col> columns) {
        ValueSpecification vs = com.legend.testcases.SqlText.translate(sql, (source, column) -> columns.forEach(
                c -> column.column(c.name(), c.type(), c.nullable(), c.storeType())), PostgresDialect.INSTANCE);
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    private static SqlTranslationException refusal(String sql) {
        return assertThrows(SqlTranslationException.class, () -> pure(sql));
    }

    private static SqlTranslationException refusal(String sql, List<Col> columns) {
        return assertThrows(SqlTranslationException.class, () -> pure(sql, columns));
    }

    // ------------------------------------------------------------------ shapes

    @Test
    void aPlainQueryIsFilterExtendSortSelectLimit() {
        assertEquals("#>{s::DB.T}#->filter(r|($r.dept == 'eng') && ($r.ID > 2))"
                        + "->sort([~firstName->descending(meta::pure::functions::relation::NullOrder.FIRST)])"
                        + "->select(~[firstName, ID])->drop(1)->limit(5)",
                pure("select firstName, ID from tb('s::DB.T') where dept = 'eng' and ID > 2"
                        + " order by firstName desc limit 5 offset 1"));
    }

    @Test
    void starIsEveryColumnAndASchemaQualifiedTableKeepsItsSchema() {
        assertEquals("#>{s::DB.sch.T}#->select(~[ID, firstName, age, dept])", pure("select * from tb('s::DB.sch.T')"));
    }

    @Test
    void anAliasedColumnIsRenamedOnlyWhenItsNameIsTaken() {
        assertEquals("#>{s::DB.T}#->extend(~[who:r|$r.dept])->select(~[who])",
                pure("select dept as who from tb('s::DB.T')"));
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|$r.dept])->select(~[_sql_1])->rename(~_sql_1, ~ID)",
                pure("select dept as \"ID\" from tb('s::DB.T')"));
    }

    @Test
    void aGroupedQueryNamesItsAggregatesByTheirOutputs() {
        assertEquals("#>{s::DB.T}#->groupBy(~[dept], ~[n:r|1:y|$y->count(), sum:r|$r.age:y|$y->sum()])"
                        + "->filter(r|$r.n > 1)->select(~[dept, n, sum])",
                pure("select dept, count(*) as n, sum(age) from tb('s::DB.T') group by dept having count(*) > 1"));
    }

    @Test
    void anAggregateWithoutGroupByAggregatesTheWholeTable() {
        assertEquals("#>{s::DB.T}#->aggregate(~[count:r|$r.age:y|$y->distinct()->count()])->select(~[count])",
                pure("select count(distinct age) from tb('s::DB.T')"));
    }

    @Test
    void groupingByAnExpressionComputesItFirst() {
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|$r.dept->toUpper()])"
                        + "->groupBy(~[_sql_1], ~[count:r|1:y|$y->count()])"
                        + "->extend(~[d:r|$r._sql_1])->select(~[d, count])",
                pure("select upper(dept) as d, count(*) from tb('s::DB.T') group by upper(dept)"));
    }

    // ------------------------------------------------------------------ several relations

    @Test
    void aJoinRenamesTheRightSidesTakenNamesAndReadsEachSideByItsQualifier() {
        // both sides have every name: the right side's become b_<name>, and an output named like a taken column is
        // computed under a fresh name and renamed last
        assertEquals("#>{s::DB.T}#->join(#>{s::DB.T}#->rename(~ID, ~b_ID)->rename(~firstName, ~b_firstName)->rename(~age, ~b_age)->rename(~dept, ~b_dept), meta::pure::functions::relation::JoinKind.INNER,"
                        + " {a, b|$b.b_age->isNotEmpty() && ($a.ID == $b.b_age->toOne())})"
                        + "->extend(~[_sql_1:r|$r.b_dept])->select(~[ID, _sql_1])->rename(~_sql_1, ~dept)",
                pure("select a.ID, b.dept from tb('s::DB.T') a join tb('s::DB.T') b on a.ID = b.age"));
    }

    @Test
    void usingMergesTheSharedColumnAndAFullJoinCoalescesIt() {
        // the merged ID is whichever side has a row: COALESCE(a.ID, b.ID)
        assertEquals("#>{s::DB.T}#->join(#>{s::DB.T}#->rename(~ID, ~b_ID)->rename(~firstName, ~b_firstName)->rename(~age, ~b_age)->rename(~dept, ~b_dept), meta::pure::functions::relation::JoinKind.FULL,"
                        + " {a, b|$a.ID == $b.b_ID})->extend(~[_sql_1:r|$r.ID->coalesce($r.b_ID)])"
                        + "->extend(~[_sql_2:r|$r._sql_1])->select(~[_sql_2, dept])->rename(~_sql_2, ~ID)",
                pure("select ID, a.dept from tb('s::DB.T') a full join tb('s::DB.T') b using (ID)"));
    }

    @Test
    void unionIsConcatenateThenDistinctAndUnionAllKeepsDuplicates() {
        // the right side's columns take the left side's names, by position
        assertEquals("#>{s::DB.T}#->select(~[dept])->concatenate(#>{s::DB.T}#->select(~[firstName])"
                        + "->rename(~firstName, ~dept))->distinct()",
                pure("select dept from tb('s::DB.T') union select firstName from tb('s::DB.T')"));
        assertEquals("#>{s::DB.T}#->select(~[ID])->concatenate(#>{s::DB.T}#->select(~[age])->rename(~age, ~ID))"
                        + "->sort([~ID->ascending(meta::pure::functions::relation::NullOrder.LAST)])",
                pure("select ID from tb('s::DB.T') union all select age from tb('s::DB.T') order by 1"));
    }

    @Test
    void aCteIsItsQueryWhereverItIsNamed() {
        assertEquals("#>{s::DB.T}#->select(~[ID])->rename(~ID, ~n)->filter(r|$r.n > 1)->select(~[n])",
                pure("with x (n) as (select ID from tb('s::DB.T')) select n from x where n > 1"));
    }

    @Test
    void setOperationsCheckTheirColumnsAsPostgresDoes() {
        assertEquals("UNION types integer and string cannot be matched",
                refusal("select ID from tb('s::DB.T') union select dept from tb('s::DB.T')").getMessage());
        assertEquals("each UNION query must have the same number of columns",
                refusal("select ID from tb('s::DB.T') union select ID, dept from tb('s::DB.T')")
                .getMessage());
        assertEquals("INTERSECT ALL is not supported yet",
                refusal("select ID from tb('s::DB.T') intersect all select ID from tb('s::DB.T')")
                .getMessage());
    }

    @Test
    void intersectKeepsTheLeftRowsSomeRightRowEqualsOnce() {
        assertEquals("#>{s::DB.T}#->select(~[age])->filter(r|#>{s::DB.T}#->select(~[ID])->exists(s|$r.age == $s.ID))"
                + "->distinct()", pure("select age from tb('s::DB.T') intersect select ID from tb('s::DB.T')"));
        assertEquals("#>{s::DB.T}#->select(~[age])->filter(r|!#>{s::DB.T}#->select(~[ID])->exists(s|$r.age == $s.ID))"
                + "->distinct()", pure("select age from tb('s::DB.T') except select ID from tb('s::DB.T')"));
    }

    // ------------------------------------------------------------------ subqueries in expressions

    @Test
    void existsIsARowOrNoneAndReadsTheEnclosingRow() {
        // the subquery is one level down: its own row is r1, the enclosing query's r
        assertEquals("#>{s::DB.T}#->filter(r|#>{s::DB.T}#->filter(r1|$r1.age->isNotEmpty() && ($r1.age->toOne() =="
                        + " $r.ID))->extend(~['?column?':r1|1])->select(~['?column?'])->exists(s|true))->select(~[ID])",
                pure("select ID from tb('s::DB.T') t where exists (select 1 from tb('s::DB.T') u where u.age = t.ID)"));
    }

    @Test
    void inIsAnyAndKeepsPostgresNullsInBothCases() {
        // TRUE when some row equals; FALSE when every row is known and unequal; else NULL
        assertEquals("#>{s::DB.T}#->extend(~[v:r|if(#>{s::DB.T}#->select(~[age])->exists(s|$s.age->isNotEmpty()"
                        + " && ($r.ID == $s.age->toOne())), |true, |if(!#>{s::DB.T}#->select(~[age])->exists("
                        + "s|!($s.age->isNotEmpty() && ($r.ID != $s.age->toOne()))), |false, |[]))])->select(~[v])",
                pure("select ID in (select age from tb('s::DB.T')) as v from tb('s::DB.T')"));
        // NOT IN holds only when every row is known and unequal: one NULL row makes it NULL, never TRUE
        assertEquals("#>{s::DB.T}#->filter(r|!#>{s::DB.T}#->select(~[age])->exists(s|!($s.age->isNotEmpty()"
                        + " && ($r.ID != $s.age->toOne()))))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where ID not in (select age from tb('s::DB.T'))"));
    }

    @Test
    void allIsTrueOverNoRowsAndFalseWhenSomeRowIsFalse() {
        assertEquals("#>{s::DB.T}#->filter(r|!#>{s::DB.T}#->select(~[age])->exists(s|!($s.age->isNotEmpty()"
                        + " && ($r.ID > $s.age->toOne()))))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where ID > all (select age from tb('s::DB.T'))"));
    }

    @Test
    void aScalarSubqueryIsItsOneValueAndMayBeNull() {
        assertEquals("#>{s::DB.T}#->extend(~[m:r|#>{s::DB.T}#->filter(r1|$r1.dept == $r.dept)"
                        + "->aggregate(~[max:r1|$r1.age:y1|$y1->max()])->select(~[max])->map(s|$s.max)->toOne()])"
                        + "->select(~[m])",
                pure("select (select max(age) from tb('s::DB.T') u where u.dept = t.dept) as m from tb('s::DB.T') t"));
        assertEquals("subquery must return only one column",
                refusal("select (select ID, age from tb('s::DB.T')) from tb('s::DB.T')").getMessage());
        assertEquals("subquery has too many columns",
                refusal("select ID from tb('s::DB.T') where ID in (select ID, age from tb('s::DB.T'))").getMessage());
    }

    @Test
    void anAggregateOfOnlyTheEnclosingQuerysColumnsIsRefused() {
        assertEquals("an aggregate of only an enclosing query's columns is not supported yet",
                refusal("select (select max(t.age) from tb('s::DB.T') u) from tb('s::DB.T') t").getMessage());
    }

    // ------------------------------------------------------------------ windows

    @Test
    void aWindowCallIsAColumnExtendedOverItsWindowBeforeTheSelectList() {
        assertEquals("#>{s::DB.T}#->extend(over(~[dept], [~age->descending(meta::pure::functions::relation::NullOrder"
                        + ".FIRST)]), ~rn:{p, w, r|$p->rowNumber($r)})->select(~[ID, rn])",
                pure("select ID, row_number() over (partition by dept order by age desc) as rn from tb('s::DB.T')"));
    }

    @Test
    void aWindowAggregateMapsEachRowAndReducesTheFrame() {
        // rows(-1, 0): one preceding row to the current row
        assertEquals("#>{s::DB.T}#->extend(over([], [~ID->ascending(meta::pure::functions::relation::NullOrder.LAST)],"
                        + " -1->rows(0)), ~s:{p, w, r|$r.age}:y|$y->sum())->select(~[s])",
                pure("select sum(age) over (order by ID rows between 1 preceding and current row) as s"
                        + " from tb('s::DB.T')"));
    }

    @Test
    void overNothingIsOnePartitionOfEveryRow() {
        assertEquals("#>{s::DB.T}#->extend(over([]), ~n:{p, w, r|1}:y|$y->count())->select(~[n])",
                pure("select count(*) over () as n from tb('s::DB.T')"));
    }

    @Test
    void lagReadsTheOtherRowsColumnAndAnExpressionIsComputedFirst() {
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|if($r.age->isEmpty(), |[], |$r.age->toOne() * 2)])"
                        + "->extend(over([~ID->ascending(meta::pure::functions::relation::NullOrder.LAST)]),"
                        + " ~prev:{p, w, r|$p->lag($r, 2)._sql_1})->select(~[prev])",
                pure("select lag(age * 2, 2) over (order by ID) as prev from tb('s::DB.T')"));
    }

    @Test
    void windowsArePostgresErrorsOutsideTheSelectListAndOrderBy() {
        assertEquals("window functions are not allowed in WHERE",
                refusal("select ID from tb('s::DB.T') where rank() over (order by ID) > 1").getMessage());
        assertEquals("window function calls cannot be nested",
                refusal("select sum(rank() over (order by ID)) over () from tb('s::DB.T')").getMessage());
        assertEquals("window function rank requires an OVER clause",
                refusal("select rank() from tb('s::DB.T')").getMessage());
        // EXCLUDE is read for count, sum, avg, min and max (aggregateAndWindowPermutationsArePostgresDefinitions)
        assertEquals("EXCLUDE CURRENT ROW of stddev(...) is not supported yet", refusal("select stddev(age) over"
                + " (order by ID rows between unbounded preceding and unbounded following exclude current row) from"
                + " tb('s::DB.T')").getMessage());
    }

    // ------------------------------------------------------------------ functions and casts

    @Test
    void roundIsHalfAwayFromZeroAsPostgresNumericRounds() {
        // Pure's round breaks ties to even; Postgres's numeric round goes away from zero
        assertEquals("#>{s::DB.T}#->extend(~[r:r|if($r.ID * 1.5D >= 0, |floor(($r.ID * 1.5D) + 0.5D),"
                + " |(($r.ID * 1.5D) - 0.5D)->ceiling())])->select(~[r])",
                pure("select round(ID * 1.5) as r from tb('s::DB.T')"));
        // to n places, exactly: scaled, rounded half away from zero, scaled back by a decimal constant
        // a strict operand's guard is merged, not re-tested: age is tested once and age * 1.5 written once per use
        assertEquals("#>{s::DB.T}#->extend(~[r:r|if($r.age->isEmpty(), |[], |if(($r.age->toOne() * 1.5D) * 10 >= 0,"
                + " |floor((($r.age->toOne() * 1.5D) * 10) + 0.5D), |((($r.age->toOne() * 1.5D) * 10) - 0.5D)->ceiling())"
                + " * 0.1D)])->select(~[r])",
                pure("select round(age * 1.5, 1) as r from tb('s::DB.T')"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED,
                refusal("select round(age / 3.0, 1) from tb('s::DB.T')").kind());
    }

    @Test
    void aChainOfAndOrIsABalancedTreeReadWithoutRecursion() throws InterruptedException {
        // Kleene's OR is associative: four operands are two pairs, not a left-deep chain of three
        assertEquals("#>{s::DB.T}#->filter(r|(($r.ID == 1) || ($r.ID == 2)) || (($r.ID == 3) || ($r.ID == 4)))"
                + "->select(~[ID])", pure("select ID from tb('s::DB.T') where ID = 1 or ID = 2 or ID = 3 or ID = 4"));
        // the stress corpus's 500-term OR overflowed a default (1 MB) thread stack in the translator
        StringBuilder sql = new StringBuilder("select ID from tb('s::DB.T') where ID = 0");
        for (int i = 1; i < 5000; i++) {
            sql.append(" or ID = ").append(i);
        }
        java.util.concurrent.atomic.AtomicReference<Object> got = new java.util.concurrent.atomic.AtomicReference<>();
        Thread t = new Thread(null, () -> {
            try {
                got.set(pure(sql.toString()));
            } catch (Throwable e) {
                got.set(e);
            }
        }, "default-stack", 1 << 20);
        t.start();
        t.join();
        assertTrue(got.get() instanceof String, () -> "translated on a 1 MB stack: " + got.get());
    }

    @Test
    void aChainOfStrictOperationsTestsItsOperandOnce() {
        // the stress corpus's + chains: each level re-tested and re-read the whole level below it, doubling the
        // expression; the guard of a strict operand is merged instead, so the chain is as long as it is written
        assertEquals("#>{s::DB.T}#->extend(~[v:r|if($r.age->isEmpty(), |[], |(($r.age->toOne() + 1) + 2) + 3)])"
                + "->select(~[v])", pure("select age + 1 + 2 + 3 as v from tb('s::DB.T')"));
    }

    @Test
    void anIntegerCastOfAValueACastAlreadyHoldsIsNotTestedAgain() {
        // nested casts to integer tested the range at every level, writing the value three times a level (the stress
        // corpus's ten casts were 9 MB of SQL): the innermost cast's test holds for the casts around it, through a
        // bigint, and a smallint is within an integer; an integer cast to smallint is tested again
        String once = "#>{s::DB.T}#->extend(~[_sql_1:r|if($r.age->isEmpty(), |[], |if(($r.age->toOne() < -2147483648)"
                + " || ($r.age->toOne() > 2147483647), |[]->cast(@Integer)->toOne(), |$r.age->toOne()))])"
                + "->select(~[_sql_1])->rename(~_sql_1, ~age)";
        assertEquals(once, pure("select cast(cast(cast(age as integer) as integer) as integer) from tb('s::DB.T')"));
        assertEquals(once, pure("select cast(cast(cast(age as integer) as bigint) as int4) from tb('s::DB.T')"));
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|if($r.age->isEmpty(), |[], |if(($r.age->toOne() < -32768)"
                        + " || ($r.age->toOne() > 32767), |[]->cast(@Integer)->toOne(), |$r.age->toOne()))])"
                        + "->select(~[_sql_1])->rename(~_sql_1, ~age)",
                pure("select cast(cast(age as smallint) as integer) from tb('s::DB.T')"));
        assertTrue(pure("select cast(cast(age as integer) as smallint) from tb('s::DB.T')").contains("< -32768"));
        // and the default name, read once a level (twice was 2^n): forty casts name the column age
        String forty = "age";
        for (int i = 0; i < 40; i++) {
            forty = "cast(" + forty + " as integer)";
        }
        assertTrue(pure("select " + forty + " from tb('s::DB.T')").endsWith("->rename(~_sql_1, ~age)"));
    }

    @Test
    void constantIntegerArithmeticIsHeldToItsIntegerType() {
        // 2026-10-17, the regression suite: the fold of constants answered past the type's range, where Postgres
        // raises; int2 with int2 is int2, else the wider, a constant int4 where it fits, a minus before one its value
        String product = pure("select (-32768)::int2 * (-1)::int2 as a from tb('s::DB.T')");
        assertTrue(product.matches("#>\\{s::DB.T}#->extend\\(~\\[a:r\\|if\\(\\(.* \\* .* < -32768\\)"
                + " \\|\\| \\(.* > 32767\\), \\|\\[]->cast\\(@Integer\\)->toOne\\(\\), \\|.*\\)]\\)"
                + "->select\\(~\\[a]\\)"), product);
        assertTrue(pure("select 2147483647 + 1 as a from tb('s::DB.T')").contains("> 2147483647"));
        assertTrue(pure("select '9223372036854775800'::int8 + 100 as a from tb('s::DB.T')")
                .contains("> 9223372036854775807"));
        assertTrue(pure("select -(-2147483648) as a from tb('s::DB.T')").contains("> 2147483647"));
        // a plus is its operand, tested as itself (the independent review, 2026-10-17)
        assertTrue(pure("select +(32767::int2 + 1::int2) as a from tb('s::DB.T')").contains("> 32767"));
        // where a step inside may pass its type's range, which the outer test cannot see, it is refused
        for (String partWay : List.of("(32767::int2 + 1::int2) * 0::int2", "1::int2 / (32767::int2 + 1::int2)",
                "-(32767::int2 + 1::int2)", "('9223372036854775807'::int8 + 1) - 1", "2147483647 + 1 - 1")) {
            assertEquals("integer arithmetic of constants that may pass its type's range part-way is not supported yet",
                    refusal("select " + partWay + " as a from tb('s::DB.T')").getMessage(), partWay);
        }
        // constants that bound it within the type need no test: 1 + 2 is at most 3
        assertEquals("#>{s::DB.T}#->extend(~[a:r|1 + 2])->select(~[a])", pure("select 1 + 2 as a from tb('s::DB.T')"));
        // over a column the database computes in the column's own type: nothing is added
        assertEquals("#>{s::DB.T}#->extend(~[a:r|$r.ID * 2])->select(~[a])",
                pure("select ID * 2 as a from tb('s::DB.T')"));
    }

    @Test
    void aTypedConstantIsItsStringCastToTheType() {
        // int2 '7' is '7'::int2 (2026-10-17, the regression suite's 269 typed constants)
        assertEquals("#>{s::DB.T}#->extend(~[a:r|7, b:r|true, c:r|toFloat(1.5D), d:r|'x'])->select(~[a, b, c, d])",
                pure("select int2 '7' as a, bool 't' as b, float8 '1.5' as c, text 'x' as d from tb('s::DB.T')"));
        assertEquals("value \"40000\" is out of range for type smallint",
                refusal("select int2 '40000' from tb('s::DB.T')").getMessage());
        // beside a value, a smallint constant would compute wider in the database than Postgres's smallint does
        for (String smallint : List.of("age * int2 '2'", "age + 2::int2", "2::smallint - age")) {
            assertEquals("arithmetic of a smallint constant and a value that is not a constant is not supported yet:"
                    + " Postgres computes it as a smallint where the value is one",
                    refusal("select " + smallint + " as a from tb('s::DB.T')").getMessage(), smallint);
        }
        assertTrue(pure("select ID * int4 '2' as a from tb('s::DB.T')").contains("$r.ID * 2"));
    }

    @Test
    void aSmallintKeepsItsWidthThroughFunctionsAndColumns() {
        // the review of 2026-10-17: Postgres computes a smallint's arithmetic as a smallint, raising past 32767, where
        // the island's integer is wider; the type passes through abs, coalesce, CASE, a subquery and a VALUES column
        String t = " from tb('s::DB.T')";
        for (String sql : List.of(
                "select age + abs(int2 '1') as a" + t,
                "select age * coalesce(int2 '2', int2 '3') as a" + t,
                "select age * case when true then int2 '2' end as a" + t,
                "select age * (select int2 '2') as a" + t,
                "select age::int2 + age::int2 as a" + t,
                "select column1 + column1 as a from (values (int2 '30000')) v",
                "select -column1 as a from (values (int2 '-32768')) v",
                "select abs(x) as a from (select 2::smallint as x) s",
                "select x * x as a from (select 5::int2 as x union all select 200::int2) s")) {
            assertTrue(assertThrows(SqlTranslationException.class, () -> pure(sql), sql).getMessage()
                    .contains("smallint"), sql);
        }
        // where its width cannot change the answer: a comparison, a wider constant beside it, constants alone, %
        for (String sql : List.of(
                "select ID as a" + t + " where age = int2 '1'",
                "select age::int2 + 1 as a" + t,
                "select int2 '2' * int2 '3' as a" + t,
                "select ID % int2 '2' as a" + t)) {
            pure(sql);
        }
        // a set operation's column of a smallint and an integer is refused too, though Postgres's is an integer:
        // the island cannot tell an integer from a smallint column
        assertThrows(SqlTranslationException.class,
                () -> pure("select x * x as a from (select 200::int2 as x union all select 5) s"));
    }

    @Test
    void anUntypedNullTakesASetOperationsTypeInItsOwnBranchOnly() {
        // the review of 2026-10-17: a NULL Postgres has already typed as text (a subquery's, a nested set
        // operation's, a DISTINCT's, a VALUES column of NULLs alone) is no untyped NULL
        assertTrue(pure("select null as a union all select 1").contains("concatenate"));
        for (String sql : List.of(
                "values (null) union all select 1",
                "select null union select null union select 1",
                "select distinct null as x union select 1",
                "select * from (select null as x) s union select 1",
                "with w as (select null as x) select x from w union select 1",
                "select x + 1 as y from (values (null)) v(x)",
                "select x + 1 as y from (select null as x) s")) {
            assertThrows(SqlTranslationException.class, () -> pure(sql), sql);
        }
        // a boolean's NULL is a condition's (it threw)
        assertTrue(pure("values (true), (null)").contains("concatenate"));
        // a VALUES list's rows are typed together: NULLs beside a value take its type, though a pair of rows of them
        // is a branch of their own; the same shape written as a UNION ALL is typed a pair at a time
        assertTrue(pure("select i, v from (values (1, 1), (2, 2), (3, null), (4, null)) t(i, v)")
                .contains("concatenate"));
        assertThrows(SqlTranslationException.class,
                () -> pure("select 1 as a union all select 2 union all (select null union all select null)"));
    }

    @Test
    void aDoublePrecisionConstantPastADoublesRangeIsAnError() {
        assertEquals("\"10e-400\" is out of range for type double precision",
                refusal("select '10e-400'::float8 from tb('s::DB.T')").getMessage());
        assertEquals("\"1e309\" is out of range for type double precision",
                refusal("select '1e309'::float8 from tb('s::DB.T')").getMessage());
        assertTrue(pure("select '0.0'::float8 as z from tb('s::DB.T')").contains("0.0"));
    }

    @Test
    void aCharColumnAsTextDropsItsPadding() {
        List<Col> padded = List.of(new Col("c", "String", false, "CHAR(4)"), new Col("v", "String", false,
                "VARCHAR(4)"));
        assertEquals("#>{s::DB.T}#->extend(~[t:r|$r.c->rtrim(), u:r|$r.v])->select(~[t, u])",
                pure("select c::text as t, v::text as u from tb('s::DB.T')", padded));
        assertEquals("#>{s::DB.T}#->select(~[c])", pure("select c from tb('s::DB.T')", padded));
    }

    @Test
    void aStringComparedWithARealIsRefused() {
        List<Col> real = List.of(new Col("f", "Float", false, "REAL"), new Col("d", "Float", false, "DOUBLE"));
        // Postgres reads '1004.3' as a real there, which Pure's Float is not
        String refused = "a string read with a real (float4) value is not supported yet: Postgres reads it as a real";
        assertEquals(refused, refusal("select f from tb('s::DB.T') where f = '1004.3'", real).getMessage());
        assertEquals(refused, refusal("select f from tb('s::DB.T') where f in ('1004.3', '1.5')", real).getMessage());
        assertEquals(refused, refusal("select case when d > 0 then f else '1004.3' end from tb('s::DB.T')", real)
                .getMessage());
        assertEquals(refused, refusal("select coalesce(f, '1004.3') from tb('s::DB.T')", real).getMessage());
        assertEquals("#>{s::DB.T}#->filter(r|$r.d == toFloat(1004.3D))->select(~[d])",
                pure("select d from tb('s::DB.T') where d = '1004.3'", real));
    }

    @Test
    void twoWindowsOfOneNameAreAnError() {
        assertEquals("window \"w\" is already defined", refusal("select count(*) over w from tb('s::DB.T')"
                + " window w as (order by ID), w as (order by ID)").getMessage());
    }

    @Test
    void anUnnamedItemIsNamedByItsStrongestName() {
        // FigureColname's strengths: a cast's type or CASE names an item only where what it holds has no name of its
        // own (2026-10-17, the regression suite)
        named("int2", "select '1'::float8::int2 from tb('s::DB.T')");
        named("int4", "select 1::integer from tb('s::DB.T')");
        named("age", "select age::smallint::bigint from tb('s::DB.T')");
        named("date", "select date '2024-01-31' from tb('s::DB.T')");
        named("dept", "select case when ID > 1 then 'x' else dept end from tb('s::DB.T')");
        named("case", "select case when ID > 1 then dept end from tb('s::DB.T')");
        named("max", "select (select max(t.ID) from tb('s::DB.T') t) from tb('s::DB.T')");
    }

    private static void named(String name, String sql) {
        String pure = pure(sql);
        assertTrue(pure.endsWith("->select(~[" + name + "])") || pure.endsWith(", ~" + name + ")"), pure);
    }

    @Test
    void nullifAndGreatestSpellPostgresNullsOut() {
        // greatest ignores a NULL: age's NULL gives ID
        assertEquals("#>{s::DB.T}#->extend(~[d:r|if($r.dept == 'eng', |[], |$r.dept), g:r|if($r.age->isEmpty(), |$r.ID,"
                        + " |if($r.ID >= $r.age->toOne(), |$r.ID, |$r.age->toOne()))])->select(~[d, g])",
                pure("select nullif(dept, 'eng') as d, greatest(ID, age) as g from tb('s::DB.T')"));
    }

    @Test
    void castsConvertWhereOnePureFunctionGivesPostgresAnswer() {
        assertEquals("#>{s::DB.T}#->extend(~[d:r|%2024-02-29, t:r|$r.ID->toString(), f:r|if($r.age->isEmpty(), |[],"
                        + " |$r.age->toOne()->toFloat())])->select(~[d, t, f])",
                pure("select '2024-02-29'::date as d, ID::text as t, age::double precision as f from tb('s::DB.T')"));
        assertEquals("casts from string to integer are not supported yet",
                refusal("select dept::integer from tb('s::DB.T')").getMessage());
        assertEquals("value \"3000000000\" is out of range for type integer",
                refusal("select '3000000000'::int from tb('s::DB.T')").getMessage());
        // a smallint holds what is in its range, else Postgres's error: an empty value read as one raises
        assertEquals("#>{s::DB.T}#->extend(~[s:r|if(($r.ID < -32768) || ($r.ID > 32767), |[]->cast(@Integer)"
                        + "->toOne(), |$r.ID), v:r|if($r.firstName->isEmpty(), |[], |$r.firstName->toOne()->left(2))])"
                        + "->select(~[s, v])",
                pure("select ID::smallint as s, firstName::varchar(2) as v from tb('s::DB.T')"));
        assertEquals("integer out of range", refusal("select 3000000000::int4 from tb('s::DB.T')").getMessage());
    }

    @Test
    void booleanPercentileAndPairAggregatesAreAGroupsReduce() {
        assertEquals("#>{s::DB.T}#->groupBy(~[dept], ~[a:r|if($r.age->isNotEmpty() && ($r.age->toOne() > 30), |true,"
                        + " |if($r.age->isNotEmpty() && !($r.age->toOne() > 30), |false, |[])):y|$y->and(),"
                        + " m:r|$r.age:y|$y->percentile(0.5, false, false)->cast(@Integer),"
                        // corr: NULL where a value's variance over the pairs is 0 (DuckDB's quotient is NaN there)
                        + " _sql_1:r|$r.age->rowMapper($r.ID):y|$y->corr(),"
                        + " _sql_2:r|$r.age->rowMapper(if($r.ID->isEmpty(), |[], |$r.age)):y|$y->covarPopulation(),"
                        + " _sql_3:r|$r.ID->rowMapper(if($r.age->isEmpty(), |[], |$r.ID)):y|$y->covarPopulation()])"
                        + "->extend(~[c:r|if(($r._sql_2->isNotEmpty() && ($r._sql_2->toOne() == 0))"
                        + " || ($r._sql_3->isNotEmpty() && ($r._sql_3->toOne() == 0)), |[], |$r._sql_1)])"
                        + "->select(~[dept, a, m, c])",
                pure("select dept, bool_and(age > 30) as a, percentile_disc(0.5) within group (order by age desc) as m,"
                        + " corr(age, ID) as c from tb('s::DB.T') group by dept"));
        assertEquals("function bool_and(integer) does not exist",
                refusal("select bool_and(age) from tb('s::DB.T')").getMessage());
        assertEquals("percentile value 1.5 is not between 0 and 1",
                refusal("select percentile_cont(1.5) within group (order by age) from tb('s::DB.T')").getMessage());
        // DuckDB's mode breaks a tie otherwise than Postgres (which takes the first in the order)
        assertEquals("WITHIN GROUP is not supported yet",
                refusal("select mode() within group (order by age) from tb('s::DB.T')").getMessage());
    }

    @Test
    void lagWithADefaultTellsAMissingRowFromANullValue() {
        // the default where there is no previous row; a previous row's NULL stays NULL (the constant 1 tells which)
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|0, _sql_2:r|1])->extend(over([~ID->ascending("
                        + "meta::pure::functions::relation::NullOrder.LAST)]), ~l:{p, w, r|if($p->lag($r)._sql_2"
                        + "->isEmpty(), |$r._sql_1, |$p->lag($r).age)})->select(~[ID, l])",
                pure("select ID, lag(age, 1, 0) over (order by ID) as l from tb('s::DB.T')"));
    }

    @Test
    void theLongTailOfTextAndNumbersIsPostgresDefinitionStepByStep() {
        // translate: each character replaced before any that becomes it, so that no replacement is replaced again
        assertEquals("#>{s::DB.T}#->extend(~[t:r|$r.dept->replace('c', 'd')->replace('b', 'c')->replace('a', 'b')])"
                + "->select(~[t])", pure("select translate(dept, 'abc', 'bcd') as t from tb('s::DB.T')"));
        assertEquals("translate(...) whose characters replace one another in a cycle is not supported yet",
                refusal("select translate(dept, 'ab', 'ba') as t from tb('s::DB.T')").getMessage());
        // concat_ws: each value present after a separator, the first separator taken off
        assertEquals("#>{s::DB.T}#->extend(~[c:r|substring(if($r.firstName->isEmpty(), |'', |', ' + $r.firstName"
                + "->toOne()) + (', ' + $r.dept), length(', ') + 1)])->select(~[c])",
                pure("select concat_ws(', ', firstName, dept) as c from tb('s::DB.T')"));
        // overlay: substring(s, 1, n - 1) || r || substring(s, n + m)
        assertEquals("#>{s::DB.T}#->extend(~[o:r|$r.dept->substring(1, 1) + 'XX' + $r.dept->substring(5)])"
                + "->select(~[o])", pure("select overlay(dept placing 'XX' from 2 for 3) as o from tb('s::DB.T')"));
        assertEquals("unrecognized format() type specifier \"d\"",
                refusal("select format('%d', ID) as f from tb('s::DB.T')").getMessage());
        // width_bucket of an integer with integer bounds is Postgres's double one; with a numeric bound, the numeric
        // one, its floor exact from the remainder
        assertTrue(pure("select width_bucket(ID, 0, 10, 2) as w from tb('s::DB.T')").contains(
                "floor(toFloat(2) * (($r.ID->toFloat() - toFloat(0D)) / (toFloat(10D) - toFloat(0D))))"));
        assertTrue(pure("select width_bucket(ID, 0.0, 10, 2) as w from tb('s::DB.T')").contains(
                "round(((($r.ID - 0.0D) * 2) - rem(($r.ID - 0.0D) * 2, 10D - 0.0D)) / (10D - 0.0D)) + 1"));
        // a numeric's power is exact for an integer base only (Postgres rounds others to about 16 digits)
        assertEquals("#>{s::DB.T}#->extend(~[p:r|$r.ID->toDecimal() * $r.ID->toDecimal() * $r.ID->toDecimal()])"
                + "->select(~[p])", pure("select power(ID::numeric, 3) as p from tb('s::DB.T')"));
        assertEquals("power(...) of numeric values other than an integer to a whole power from 0 to 16 is not"
                + " supported yet (Postgres rounds a numeric's power to about 16 significant digits)",
                refusal("select power(2.5, 2) as p from tb('s::DB.T')").getMessage());
        // the remainder of numerics is exact: Pure's rem
        assertEquals("#>{s::DB.T}#->extend(~[m:r|$r.ID->rem(2.5D)])->select(~[m])",
                pure("select mod(ID, 2.5) as m from tb('s::DB.T')"));
    }

    @Test
    void aggregateAndWindowPermutationsArePostgresDefinitions() {
        // ORDER BY inside an aggregate whose answer has no order is dropped; under DISTINCT it must be the argument
        assertEquals(pure("select sum(age) as s from tb('s::DB.T')"),
                pure("select sum(age order by dept) as s from tb('s::DB.T')"));
        assertEquals("in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
                refusal("select count(distinct age order by ID) as c from tb('s::DB.T')").getMessage());
        // a regression's count: the pairs holding both values, each read as a double
        assertEquals("#>{s::DB.T}#->aggregate(~[n:r|if(if($r.age->isEmpty(), |[], |$r.age->toOne()->toFloat())"
                + "->isEmpty(), |[], |$r.ID->toFloat()):y|$y->count()])->select(~[n])",
                pure("select regr_count(age, ID) as n from tb('s::DB.T')"));
        // FILTER on a window aggregate: the aggregate of CASE WHEN c THEN x END
        assertEquals("#>{s::DB.T}#->extend(~[_sql_1:r|if($r.age->isNotEmpty() && ($r.age->toOne() > 3), |1, |[])])"
                + "->extend(over([]), ~c:{p, w, r|$r._sql_1}:y|$y->count())->select(~[ID, c])",
                pure("select ID, count(*) filter (where age > 3) over () as c from tb('s::DB.T')"));
        assertEquals(SqlTranslationException.Kind.INVALID, refusal("select ID, row_number() filter (where age > 3)"
                + " over () as c from tb('s::DB.T')").kind());
        // EXCLUDE CURRENT ROW: the frame's count and sum less the row's own, NULL where no value remains
        assertEquals("#>{s::DB.T}#->extend(over([], [~ID->ascending(meta::pure::functions::relation::NullOrder.LAST)],"
                + " -1->rows(1)), ~_sql_1:{p, w, r|$r.age}:y|$y->count())->extend(over([], [~ID->ascending("
                + "meta::pure::functions::relation::NullOrder.LAST)], -1->rows(1)), ~_sql_2:{p, w, r|$r.age}:y|$y"
                + "->sum())->extend(~s:r|if($r._sql_1 - if($r.age->isEmpty(), |0, |1) == 0, |[], |if($r._sql_2"
                + "->isEmpty(), |0, |$r._sql_2->toOne()) - if($r.age->isEmpty(), |0, |$r.age->toOne())))"
                + "->select(~[ID, s])", pure("select ID, sum(age) over (order by ID rows between 1 preceding and 1"
                + " following exclude current row) as s from tb('s::DB.T')"));
        // EXCLUDE TIES of a whole partition: the partition's count less the peers', the row's own back
        assertEquals("#>{s::DB.T}#->extend(over([]), ~_sql_1:{p, w, r|$r.age}:y|$y->count())->extend(over(~[dept]),"
                + " ~_sql_2:{p, w, r|$r.age}:y|$y->count())->extend(~s:r|($r._sql_1 - $r._sql_2) + if($r.age"
                + "->isEmpty(), |0, |1))->select(~[ID, s])", pure("select ID, count(age) over (order by dept rows"
                + " between unbounded preceding and unbounded following exclude ties) as s from tb('s::DB.T')"));
        // min over a partial frame, and a RANGE frame not bracketing the row (a NULL key's frame is its peers'),
        // are refused
        assertEquals("EXCLUDE CURRENT ROW of min(...) over this frame is not supported yet", refusal("select ID,"
                + " min(age) over (order by age rows between 1 preceding and 1 following exclude current row) as s"
                + " from tb('s::DB.T')").getMessage());
        assertEquals("EXCLUDE CURRENT ROW of sum(...) over this frame is not supported yet", refusal("select ID,"
                + " sum(age) over (order by age range between 3 preceding and 1 preceding exclude current row) as s"
                + " from tb('s::DB.T')").getMessage());
    }

    @Test
    void theReviewsFindingsOnTheLongTailStayFixed() {
        // log to the constant base 10 is log10, exact at 10's powers; another base is refused (ln(x) / ln(b) misses
        // them: log(10, 1000) by natural logarithms is 2.9999999999999996)
        assertEquals("#>{s::DB.T}#->extend(~[l:r|$r.ID->log10()])->select(~[l])",
                pure("select log(10::numeric, ID) as l from tb('s::DB.T')"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED,
                refusal("select log(2, ID) as l from tb('s::DB.T')").kind());
        // split_part steps over a delimiter by its characters, not its UTF-16 units
        assertTrue(pure("select split_part(dept, '\uD83C\uDF89', 2) as p from tb('s::DB.T')").contains(
                "->indexOf('\\uD83C\\uDF89') + 1)"));
        // only count takes *, FILTER or not; min and max of booleans are Postgres's errors
        assertEquals("sum(*) is not a function; only count(*) is", refusal("select ID, sum(*) filter (where age > 1)"
                + " over () as s from tb('s::DB.T')").getMessage());
        List<Col> flagged = List.of(new Col("ID", "Integer", false), new Col("flag", "Boolean", true));
        assertEquals("function max(boolean) does not exist",
                refusal("select max(flag) as m from tb('s::DB.T')", flagged).getMessage());
        assertEquals("function min(boolean) does not exist",
                refusal("select ID, min(flag) over () as m from tb('s::DB.T')", flagged).getMessage());
        // a sum of doubles less the row's own loses digits: EXCLUDE of it is refused
        List<Col> doubles = List.of(new Col("ID", "Integer", false), new Col("f", "Float", true));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal("select ID, sum(f) over (rows between"
                + " unbounded preceding and unbounded following exclude current row) as s from tb('s::DB.T')",
                doubles).kind());
    }

    @Test
    void dateArithmeticIsPostgresDaysIntervalsAndTruncation() {
        List<Col> dated = List.of(new Col("d", "StrictDate", false), new Col("ts", "DateTime", false));
        // months, then days, then seconds, as Postgres adds an interval; a date is a timestamp at midnight first
        String unit = "meta::pure::functions::date::DurationUnit.";
        assertEquals("#>{s::DB.T}#->extend(~[a:r|$r.d->adjust(1, " + unit + "DAYS), b:r|%2000-01-01->dateDiff($r.d, "
                + unit + "DAYS), c:r|$r.ts->adjust(1, " + unit + "MONTHS)->adjust(2, " + unit + "DAYS), e:r|$r.d"
                + "->adjust(0, " + unit + "HOURS)->adjust(-10800, " + unit + "SECONDS), f:r|$r.ts->firstDayOfMonth()"
                + "->adjust(0, " + unit + "HOURS)])->select(~[a, b, c, e, f])",
                pure("select d + 1 as a, d - date '2000-01-01' as b, ts + interval '1 month 2 days' as c,"
                + " d - interval '3' hour as e, date_trunc('month', ts) as f from tb('s::DB.T')", dated));
        assertEquals("the interval '1.5 days' is not supported yet: whole years, months, weeks, days, hours, minutes"
                + " and seconds are", refusal("select ts + interval '1.5 days' from tb('s::DB.T')", dated)
                .getMessage());
    }

    @Test
    void aQuotientOfNumericsIsInexactSoNothingRoundsOrSpellsIt() {
        // Postgres's numeric quotient is exact, Pure's divide a Float: floor(0.3 / 0.1) is 3 there, 2 here
        assertEquals("floor of a quotient, average or root of numerics (exact in Postgres, a double here) is not"
                + " supported yet", refusal("select floor(0.3 / 0.1) as f from tb('s::DB.T')").getMessage());
        assertEquals("a cast to integer of a quotient, average or root of numerics (exact in Postgres, a double"
                + " here) is not supported yet", refusal("select (ID / 2.0)::integer as i from tb('s::DB.T')")
                .getMessage());
        assertEquals("round(x) of a quotient, average or root of numerics (exact in Postgres, a double here) is not"
                + " supported yet", refusal("select round(avg(age)) as a from tb('s::DB.T')").getMessage());
        assertEquals("|| of a quotient, average or root of numerics (exact in Postgres, a double here) is not"
                + " supported yet", refusal("select dept || ID / 3.0 as t from tb('s::DB.T')").getMessage());
        // a double with an inexact numeric in it is still inexact: Postgres's double of the exact value is not this one
        assertEquals("floor of a quotient, average or root of numerics (exact in Postgres, a double here) is not"
                + " supported yet", refusal("select floor(ID / 3.0 * 1::float8) as f from tb('s::DB.T')").getMessage());
        // a sum of integers is a bigint or a numeric in Postgres by the column's width: its division is refused
        assertEquals("dividing a sum of integers by an integer is not supported yet: Postgres's sum of an integer"
                + " column is a bigint or a numeric by the column's width, which divide differently (cast it:"
                + " sum(x)::bigint / n or sum(x)::numeric / n)",
                refusal("select sum(age) / 2 as s from tb('s::DB.T')").getMessage());
        // a double's quotient is a double in Postgres too: round is rint, half to even, as Pure's round of a Float
        assertEquals("#>{s::DB.T}#->extend(~[r:r|round($r.ID / toFloat(2.0D))->toFloat()])->select(~[r])",
                pure("select round(ID / 2.0::float8) as r from tb('s::DB.T')"));
    }

    // ------------------------------------------------------------------ Postgres's NULL rules

    @Test
    void arithmeticOnANullableColumnIsNullWhenTheColumnIs() {
        assertEquals("#>{s::DB.T}#->extend(~[next:r|if($r.age->isEmpty(), |[], |$r.age->toOne() + 1)])"
                + "->select(~[next])", pure("select age + 1 as next from tb('s::DB.T')"));
        assertEquals("#>{s::DB.T}#->extend(~[next:r|$r.ID + 1])->select(~[next])",
                pure("select ID + 1 as next from tb('s::DB.T')"));
    }

    @Test
    void notOfANullComparisonIsNotTrue() {
        // NOT (age > 1) holds only where age is known and not greater than 1 (Postgres: NOT NULL is NULL)
        assertEquals("#>{s::DB.T}#->filter(r|$r.age->isNotEmpty() && !($r.age->toOne() > 1))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where not (age > 1)"));
        assertEquals("#>{s::DB.T}#->filter(r|!($r.ID > 1))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where not (ID > 1)"));
    }

    @Test
    void aNullableConditionAsAValueIsTrueFalseOrNull() {
        assertEquals("#>{s::DB.T}#->extend(~[old:r|if($r.age->isNotEmpty() && ($r.age->toOne() > 30), |true,"
                        + " |if($r.age->isNotEmpty() && !($r.age->toOne() > 30), |false, |[]))])->select(~[old])",
                pure("select age > 30 as old from tb('s::DB.T')"));
    }

    @Test
    void powerIsPowOverIntegersAndFloats() {
        assertEquals("#>{s::DB.T}#->extend(~[p:r|$r.ID->pow(2)])->select(~[p])",
                pure("select ID ^ 2 as p from tb('s::DB.T')"));
        assertEquals("#>{s::DB.T}#->extend(~[p:r|if($r.age->isEmpty(), |[], |$r.age->toOne()->pow(2))])"
                + "->select(~[p])", pure("select age ^ 2 as p from tb('s::DB.T')"));
        // Postgres computes numeric ^ exactly (rounded to about 16 digits); pow is a Float. An integer base to a
        // whole constant is exact: a product
        assertEquals("^ of numeric values other than an integer to a whole power from 0 to 16 is not supported yet"
                + " (Postgres rounds a numeric's power to about 16 significant digits)",
                refusal("select 2.5 ^ ID from tb('s::DB.T')").getMessage());
        assertEquals("#>{s::DB.T}#->extend(~[p:r|$r.ID->toDecimal() * $r.ID->toDecimal()])->select(~[p])",
                pure("select ID ^ 2.0 as p from tb('s::DB.T')"));
    }

    @Test
    void otherOperatorsAreRefusedByName() {
        SqlTranslationException regex = refusal("select ID from tb('s::DB.T') where dept ~ 'e'");
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, regex.kind());
        assertEquals("regular expression operators (~, ~*, !~, !~*) are not supported yet", regex.getMessage());
        assertEquals("the operator @> is not supported yet",
                refusal("select dept @> 'k' from tb('s::DB.T')").getMessage());
        assertEquals("a select list with no columns is not supported yet",
                refusal("select from tb('s::DB.T')").getMessage());
    }

    // ------------------------------------------------------------------ jsonb

    /** A Variant column: Postgres's jsonb (register S48). */
    private static final List<Col> JSON_COLUMNS = List.of(new Col("ID", "Integer", false),
            new Col("j", "meta::pure::metamodel::variant::Variant", true));

    @Test
    void jsonbNavigationIsVariantGetAndItsTextIsTo() {
        assertEquals("#>{s::DB.T}#->extend(~[a:r|$r.j->get('a'), z:r|$r.j->get(0), b:r|$r.j->get('b')->to(@String)])"
                + "->select(~[a, z, b])", pure("select j -> 'a' as a, j -> 0 as z, j ->> 'b' as b from tb('s::DB.T')", JSON_COLUMNS));
        assertEquals("#>{s::DB.T}#->extend(~[p:r|$r.j->get('a')->get('b c'), t:r|$r.j->get('a')->to(@String),"
                + " k:r|$r.j->get('k')])->select(~[p, t, k])", pure("select j #> '{a,\"b c\"}' as p, j #>> '{a}' as t, j['k'] as k from tb('s::DB.T')",
                JSON_COLUMNS));
        assertEquals("#>{s::DB.T}#->extend(~[v:r|fromJson('{\"a\":1}')->get('a')"
                + "->to(@String), s:r|fromJson('\"x\"')])->select(~[v, s])", pure("select '{\"a\":1}'::jsonb ->> 'a' as v, to_jsonb('x'::text) as s from tb('s::DB.T')",
                JSON_COLUMNS));
    }

    @Test
    void subscriptsAndPathsReadAnElementOrAMemberByJsonbsPathRule() {
        assertEquals("#>{s::DB.T}#->extend(~[e:r|$r.j->get('0')->coalesce($r.j->get(0)), f:r|$r.j->get('1')"
                + "->coalesce($r.j->get(1)), k:r|$r.j->get('k'), p:r|$r.j->get('a')->get(' 2')->coalesce($r.j->get('a')"
                + "->get(2)), x:r|$r.j->get('-1')->coalesce($r.j->get(-1))])->select(~[e, f, k, p, x])",
                pure("select j[0] as e, j['1'] as f, j['k'] as k, j #> '{a,\" 2\"}' as p,"
                + " jsonb_extract_path(j, '-1') as x from tb('s::DB.T')", JSON_COLUMNS));
    }

    @Test
    void caseAndCoalesceResolveOneTypeAsPostgresDoes() {
        assertEquals("#>{s::DB.T}#->extend(~[c:r|$r.ID, d:r|if($r.ID > 1, |1, |2.5D),"
                + " e:r|$r.j->coalesce(fromJson('[]'))])->select(~[c, d, e])", pure("select coalesce(ID, '5') as c, case when ID > 1 then 1 else 2.5 end as d,"
                + " coalesce(j, '[]') as e from tb('s::DB.T')", JSON_COLUMNS));
        assertEquals("the string 'x' read as integer is not supported yet", refusal("select coalesce(ID, 'x') from"
                + " tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("CASE types integer and text cannot be matched", refusal("select case when ID > 1 then ID else"
                + " 'a'::text end from tb('s::DB.T')", JSON_COLUMNS).getMessage());
    }

    @Test
    void jsonbFormsWithoutAFaithfulPureSpellingAreRefusedByName() {
        assertEquals("ORDER BY of a jsonb value is not supported yet (jsonb orders and compares by its own rules)",
                refusal("select ID from tb('s::DB.T') order by j", JSON_COLUMNS).getMessage());
        assertEquals("a jsonb constant with a duplicate key is not supported yet",
                refusal("select '{\"a\":1,\"a\":2}'::jsonb ->> 'a' from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("ORDER BY of a jsonb value is not supported yet (jsonb orders and compares by its own rules)",
                refusal("select j from tb('s::DB.T') union all select j from tb('s::DB.T') order by 1", JSON_COLUMNS)
                        .getMessage());
        assertEquals("JOIN/USING of a jsonb value is not supported yet (jsonb orders and compares by its own rules)",
                refusal("select ID from tb('s::DB.T') a join tb('s::DB.T') b using (j)", JSON_COLUMNS).getMessage());
        assertEquals("operator does not exist: jsonb -> bigint",
                refusal("select j -> 3000000000 from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("subscript type numeric is not supported",
                refusal("select j[1.5] from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("COALESCE types jsonb and text cannot be matched",
                refusal("select coalesce(j, 'x'::text) from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("could not determine polymorphic type because input has type unknown",
                refusal("select to_jsonb('x') from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("reading a jsonb value out of a jsonb constant is not supported yet (its text, with ->> or #>>,"
                + " is)", refusal("select '{\"a\":1}'::jsonb -> 'a' from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals("jsonb_array_length(...) is not supported yet",
                refusal("select jsonb_array_length(j) from tb('s::DB.T')", JSON_COLUMNS).getMessage());
        assertEquals(SqlTranslationException.Kind.INVALID,
                refusal("select ID -> 'a' from tb('s::DB.T')").kind());
    }

    // ------------------------------------------------------------------ LATERAL, set-returning functions, no FROM

    @Test
    void aFunctionInFromIsAFlattenAndReadsTheItemsBeforeItThroughLateral() {
        assertEquals("1->range(3 + 1, 2)->flatten(~g)->select(~[g])",
                pure("select g from generate_series(1, 3, 2) as g"));
        assertEquals("#>{s::DB.T}#->lateral(r|[$r.ID, 2]->flatten(~u)->rename(~u, ~x))->select(~[ID, x])",
                pure("select ID, u.x from tb('s::DB.T') t, unnest(array[ID, 2]) as u(x)"));
        assertEquals("#>{s::DB.T}#->lateral(r|#>{s::DB.T}#->filter(r1|$r1.ID < $r.ID)->aggregate(~[n:r1|1:y1|$y1->count()])->select(~[n]))->select(~[ID, n])",
                pure("select ID, s.n from tb('s::DB.T') t cross join lateral (select count(*) as n"
                + " from tb('s::DB.T') q where q.ID < t.ID) s"));
        assertEquals("#TDS{\n_sql_row\n1\n}#->extend(~[three:r|1 + 2])->select(~[three])",
                pure("select 1 + 2 as three"));
    }

    @Test
    void jsonbSetReturningFunctionsAreToManyAndKeysAndTheSelectListFormIsALateral() {
        assertEquals("#>{s::DB.T}#->lateral(r|if($r.j->isNotEmpty() && $r.j->to(@String)->isEmpty(), |fromJson('0'),"
                + " |$r.j)->toMany(@meta::pure::metamodel::variant::Variant)->flatten(~value))->extend(~[x:r|$r.value"
                + "->get('x')->to(@String)])->select(~[ID, x])",
                pure("select ID, e ->> 'x' as x from tb('s::DB.T') t, jsonb_array_elements(t.j) e",
                JSON_COLUMNS));
        assertEquals("#>{s::DB.T}#->lateral(r|if($r.j->isEmpty(), |[], |if($r.j->isNotEmpty() && $r.j->to(@String)"
                + "->isEmpty(), |fromJson('0'), |$r.j)->to(@meta::pure::functions::collection::Map<String,"
                + " meta::pure::metamodel::variant::Variant>)->toOne()->keys())->flatten(~_sql_set)->rename(~_sql_set,"
                + " ~_sql_value))->extend(~[k:r|$r._sql_value])->select(~[ID, k])",
                pure("select ID, jsonb_object_keys(j) as k from tb('s::DB.T')", JSON_COLUMNS));
        assertEquals("a set-returning function inside an expression is not supported yet",
                refusal("select jsonb_array_elements(j) ->> 'x' from tb('s::DB.T')", JSON_COLUMNS).getMessage());
    }

    @Test
    void isDistinctFromIsPureEquality() {
        assertEquals("#>{s::DB.T}#->filter(r|$r.firstName != $r.dept)->select(~[ID])",
                pure("select ID from tb('s::DB.T') where firstName is distinct from dept"));
    }

    @Test
    void likeByThePatternsShape() {
        assertEquals("#>{s::DB.T}#->filter(r|$r.dept->startsWith('en'))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where dept like 'en%'"));
        assertEquals("#>{s::DB.T}#->filter(r|$r.dept->contains('n_g'))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where dept like '%n\\_g%'"));
        assertEquals("#>{s::DB.T}#->filter(r|$r.dept->matches('(?i)^e.g\\\\.$'))->select(~[ID])",
                pure("select ID from tb('s::DB.T') where dept ilike 'e_g.'"));
    }

    @Test
    void aStringComparedWithADateIsADate() {
        List<Col> dated = List.of(new Col("d", "StrictDate", false));
        assertEquals("#>{s::DB.T}#->filter(r|$r.d >= %2024-01-31)->select(~[d])",
                pure("select d from tb('s::DB.T') where d >= '2024-01-31'", dated));
        // Postgres's other spellings are read by its rules (PostgresDateTime); a time zone's name is refused
        assertEquals("#>{s::DB.T}#->filter(r|$r.d >= %1999-01-08)->select(~[d])",
                pure("select d from tb('s::DB.T') where d >= 'January 8, 1999'", dated));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, assertThrows(SqlTranslationException.class,
                () -> pure("select d from tb('s::DB.T') where d >= 'January 8, 1999 PST'", dated)).kind());
    }

    @Test
    void nullsSortLastAscendingAndFirstDescendingUnlessWritten() {
        String sorted = pure("select ID from tb('s::DB.T') order by age, dept desc, ID nulls first");
        assertTrue(sorted.contains("[~age->ascending(meta::pure::functions::relation::NullOrder.LAST),"
                + " ~dept->descending(meta::pure::functions::relation::NullOrder.FIRST),"
                + " ~ID->ascending(meta::pure::functions::relation::NullOrder.FIRST)]"), sorted);
    }

    // ------------------------------------------------------------------ names

    @Test
    void anUnquotedNameMatchesItsFoldingFirstThenIgnoringCase() {
        assertEquals("#>{s::DB.T}#->select(~[firstName])", pure("select FIRSTNAME from tb('s::DB.T')"));
        List<Col> both = List.of(new Col("name", "String", false),
                new Col("Name", "String", false), new Col("NAME", "String", false));
        // Postgres's own rule decides first: NAME folds to name, which exists
        assertEquals("#>{s::DB.T}#->select(~[name])", pure("select NAME from tb('s::DB.T')", both));
        // a quoted name matches exactly
        assertEquals("#>{s::DB.T}#->select(~[Name])", pure("select \"Name\" from tb('s::DB.T')", both));
    }

    @Test
    void severalNamesThatDifferOnlyInCaseAreAmbiguous() {
        List<Col> two = List.of(new Col("Name", "String", false),
                new Col("NAME", "String", false));
        SqlTranslationException e = assertThrows(SqlTranslationException.class,
                () -> pure("select name from tb('s::DB.T')", two));
        assertEquals(SqlTranslationException.Kind.INVALID, e.kind());
        assertEquals("column reference \"name\" is ambiguous: it matches \"Name\", \"NAME\"; quote the name to choose"
                + " one", e.getMessage());
    }

    @Test
    void aQuotedNameInTheWrongCaseGetsAHint() {
        assertEquals("column \"firstname\" does not exist; perhaps you meant \"firstName\"",
                refusal("select \"firstname\" from tb('s::DB.T')").getMessage());
    }

    // ------------------------------------------------------------------ refusals

    @Test
    void aggregateFilterDistinctAndIntegerDivision() {
        assertEquals("#>{s::DB.T}#->groupBy(~[dept], ~[n:r|if($r.age->isNotEmpty() && ($r.age->toOne() > 30), |1, |[])"
                + ":y|$y->count(), s:r|$r.age:y|$y->distinct()->sum()])->select(~[dept, n, s])", pure("select dept, count(*) filter (where age > 30) as n, sum(distinct age) as s"
                + " from tb('s::DB.T') group by dept"));
        assertEquals("#>{s::DB.T}#->extend(~[q:r|if($r.age->isEmpty(), |[], |if((($r.age->toOne() - $r.age->toOne()->rem(7"
                + ")) / 7) * 0.9999999999999991 >= 0, |floor((($r.age->toOne() - $r.age->toOne()->rem(7)) / 7) * 0.9999"
                + "999999999991), |ceiling((($r.age->toOne() - $r.age->toOne()->rem(7)) / 7) * 0.9999999999999991)) + r"
                + "ound((($r.age->toOne() - $r.age->toOne()->rem(7)) - (if((($r.age->toOne() - $r.age->toOne()->rem(7))"
                + " / 7) * 0.9999999999999991 >= 0, |floor((($r.age->toOne() - $r.age->toOne()->rem(7)) / 7) * 0.999999"
                + "9999999991), |ceiling((($r.age->toOne() - $r.age->toOne()->rem(7)) / 7) * 0.9999999999999991)) * 7))"
                + " / 7))])->select(~[q])",
                pure("select age / 7 as q from tb('s::DB.T')"));
        assertEquals("FILTER specified, but upper is not an aggregate function",
                refusal("select upper(name) filter (where age > 1) from tb('s::DB.T')").getMessage());
    }

    @Test
    void refusalsSayTheirKindAndWhere() {
        SqlTranslationException syntax = refusal("select ID from tb('s::DB.T') where");
        assertEquals(SqlTranslationException.Kind.SYNTAX, syntax.kind());
        assertEquals(1, syntax.line());
        assertEquals(35, syntax.column());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED,
                refusal("select ID from tb('s::DB.T') a, lateral (select 1) b").kind());
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED,
                refusal("select mode() within group (order by age) from tb('s::DB.T')").kind());
        SqlTranslationException grouped = refusal("select dept, age from tb('s::DB.T') group by dept");
        assertEquals(SqlTranslationException.Kind.INVALID, grouped.kind());
        assertEquals("column \"age\" must appear in the GROUP BY clause or be used in an aggregate function",
                grouped.getMessage());
        assertEquals("aggregate functions are not allowed in WHERE",
                refusal("select ID from tb('s::DB.T') where count(*) > 1").getMessage());
        assertEquals("column \"nope\" does not exist", refusal("select nope from tb('s::DB.T')").getMessage());
        // a comparison with NULL is NULL, never TRUE: the filter keeps no row (Postgres's answer)
        assertEquals("#>{s::DB.T}#->filter(r|false)->select(~[ID])", pure("select ID from tb('s::DB.T') where age = null"));
    }
}
