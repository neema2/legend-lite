// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.testbackend.TestBackend;

import java.math.BigDecimal;
import java.util.List;

/**
 * The {@code #SQL{...}#} island's cases (docs/LEGEND_SQL_STUDY_2026_10_11.md §3): Postgres queries over one table, each
 * with the rows REAL POSTGRES 16 returns for it, recorded 2026-10-11 from the pinned embedded Postgres (16.15.0). Postgres
 * is the spec: {@code SqlIslandIntegrationTest} runs each as an island on DuckDB and expects these rows;
 * {@code SqlIslandPostgresTest} runs each on Postgres twice, as plain SQL (a recorded answer that stops being Postgres's
 * fails there) and as an island through the Postgres dialect.
 */
public final class SqlIslandCases {

    private SqlIslandCases() {
    }

    /** The island's source table, as {@code tb(...)} names it. */
    public static final String SOURCE = "tb('store::HR.EMP')";

    /** The table's store. Its mixed-case columns are declared QUOTED: a name declared bare is folded on Postgres
     *  (decision D8, as legend-engine does), and reaches only a column a bare declaration made. */
    private static final String STORE = """
            ###Relational
            Database store::HR
            (
                Table EMP
                (
                    "ID" INTEGER PRIMARY KEY,
                    "firstName" VARCHAR(100),
                    "LAST_NAME" VARCHAR(100),
                    dept VARCHAR(20),
                    salary DOUBLE,
                    bonus DECIMAL(10,2),
                    age INTEGER,
                    hired DATE
                )
            )
            """;

    /** The model: the table's store, a connection of the given {@code type}/{@code specification} lines, and a
     *  runtime. */
    public static String model(String connection) {
        return STORE + """
                ###Connection
                RelationalDatabaseConnection store::Conn
                {
                    %s
                }
                ###Runtime
                Runtime test::RT
                {
                    mappings: [];
                    connections: [ store::HR: [ environment: store::Conn ] ];
                }
                """.formatted(connection);
    }

    /** The runtime {@link #model} declares. */
    public static final String RUNTIME = "test::RT";

    /** The table's rows, as one VALUES list both databases read. */
    public static final String ROWS = "(1,'Ann','Lee','eng',100.0,10.50,30,DATE '2020-01-15'),"
            + "(2,'Bob','Kim','eng',80.0,NULL,40,DATE '2019-03-01'),"
            + "(3,'Cid','Ng','ops',NULL,5.25,25,DATE '2021-07-30'),"
            + "(4,'Dee',NULL,'ops',60.0,2.00,NULL,NULL),"
            + "(5,'Eve','Ito',NULL,90.0,1.00,35,DATE '2018-11-11'),"
            + "(6,'Fay','Lee','eng',100.0,0.00,30,DATE '2020-01-15')";

    /** The table as {@link #STORE} declares it, created as the product creates a declared table on {@code backend}'s
     *  database: its dialect renders the store's declaration ({@code setup.Ddl.createTable}), each name quoted as the
     *  store declared it, so a database that folds a bare name keeps the quoted ones as the model spells them. */
    public static String table(TestBackend backend) {
        for (var element : com.legend.Compiler.parseModel(STORE).elements()) {
            if (element instanceof com.legend.model.DatabaseDefinition db) {
                for (var table : db.tables()) {
                    if (table.name().equals("EMP")) {
                        return backend.dialect().render(com.legend.setup.Ddl.createTable(table, null));
                    }
                }
            }
        }
        throw new IllegalStateException("the store declares no table EMP");
    }

    /** A query, whether its rows are ordered, and Postgres's answer: the column names and the rows, each cell as
     *  {@link #cell} spells it. */
    public record Case(String sql, boolean ordered, List<String> columns, List<List<String>> rows) {

        /** The query as an island over the model's table. */
        public String island() {
            return "#SQL{" + sql + "}#";
        }

        /** The query as plain SQL over the table: what Postgres ran for the case's answer. */
        public String plain() {
            return sql.replace(SOURCE, "EMP");
        }
    }

    /** A value as the cases spell it, whatever database returned it: a number by its value ({@code 10.50} and
     *  {@code 10.5} alike, zero as {@code 0}), a date in ISO 8601, a missing value as {@code null}. */
    public static String cell(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Number n) {
            BigDecimal d = new BigDecimal(n.toString()).stripTrailingZeros();
            return d.signum() == 0 ? "0" : d.toPlainString();
        }
        if (value instanceof java.sql.Date d) {
            return d.toLocalDate().toString();
        }
        return value.toString();
    }

    /** Every case. Postgres's answers, recorded; a query is here only when Postgres and the island agree. */
    public static final List<Case> CASES = List.of(
            new Case("select * from tb('store::HR.EMP')",
                    false, List.of("ID", "firstName", "LAST_NAME", "dept", "salary", "bonus", "age", "hired"),
                    List.of(List.of("1", "Ann", "Lee", "eng", "100", "10.5", "30", "2020-01-15"),
                            List.of("2", "Bob", "Kim", "eng", "80", "null", "40", "2019-03-01"),
                            List.of("3", "Cid", "Ng", "ops", "null", "5.25", "25", "2021-07-30"),
                            List.of("4", "Dee", "null", "ops", "60", "2", "null", "null"),
                            List.of("5", "Eve", "Ito", "null", "90", "1", "35", "2018-11-11"),
                            List.of("6", "Fay", "Lee", "eng", "100", "0", "30", "2020-01-15"))),
            new Case("select \"firstName\", \"LAST_NAME\" as last from tb('store::HR.EMP') where age > 28 order by \"firstName\" desc",
                    true, List.of("firstName", "last"),
                    List.of(List.of("Fay", "Lee"),
                            List.of("Eve", "Ito"),
                            List.of("Bob", "Kim"),
                            List.of("Ann", "Lee"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where not (age > 28)",
                    false, List.of("ID"),
                    List.of(List.of("3"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where age <> 30",
                    false, List.of("ID"),
                    List.of(List.of("2"),
                            List.of("3"),
                            List.of("5"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where dept = 'eng' or dept <> 'eng'",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("2"),
                            List.of("3"),
                            List.of("4"),
                            List.of("6"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where dept not in ('eng')",
                    false, List.of("ID"),
                    List.of(List.of("3"),
                            List.of("4"))),
            new Case("select \"ID\", age + 1 as a1, salary * 2 as s2, upper(dept) as d from tb('store::HR.EMP')",
                    false, List.of("ID", "a1", "s2", "d"),
                    List.of(List.of("1", "31", "200", "ENG"),
                            List.of("2", "41", "160", "ENG"),
                            List.of("3", "26", "null", "OPS"),
                            List.of("4", "null", "120", "OPS"),
                            List.of("5", "36", "180", "null"),
                            List.of("6", "31", "200", "ENG"))),
            new Case("select \"ID\", age > 28 as old from tb('store::HR.EMP')",
                    false, List.of("ID", "old"),
                    List.of(List.of("1", "true"),
                            List.of("2", "true"),
                            List.of("3", "false"),
                            List.of("4", "null"),
                            List.of("5", "true"),
                            List.of("6", "true"))),
            new Case("select dept, count(*) as n from tb('store::HR.EMP') group by 1 having count(*) > 1",
                    false, List.of("dept", "n"),
                    List.of(List.of("eng", "3"),
                            List.of("ops", "2"))),
            new Case("select count(*) as n, count(age) as aged, max(hired) as latest, sum(bonus) as b from tb('store::HR.EMP')",
                    false, List.of("n", "aged", "latest", "b"),
                    List.of(List.of("6", "5", "2021-07-30", "18.75"))),
            new Case("select upper(dept) as d, count(distinct age) from tb('store::HR.EMP') group by upper(dept) order by 1",
                    true, List.of("d", "count"),
                    List.of(List.of("ENG", "2"),
                            List.of("OPS", "1"),
                            List.of("null", "1"))),
            new Case("select distinct dept from tb('store::HR.EMP') order by dept",
                    true, List.of("dept"),
                    List.of(List.of("eng"),
                            List.of("ops"),
                            List.of("null"))),
            new Case("select \"ID\" from tb('store::HR.EMP') order by salary desc limit 2 offset 1",
                    true, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("6"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where \"firstName\" like 'A%' or \"LAST_NAME\" like '%e' or \"firstName\" like '_v_' or \"firstName\" ilike '%ID%'",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("3"),
                            List.of("5"),
                            List.of("6"))),
            new Case("select \"ID\", case when age < 30 then 'young' when age >= 30 then 'old' end as band, coalesce(\"LAST_NAME\", dept, 'none') as who from tb('store::HR.EMP')",
                    false, List.of("ID", "band", "who"),
                    List.of(List.of("1", "old", "Lee"),
                            List.of("2", "old", "Kim"),
                            List.of("3", "young", "Ng"),
                            List.of("4", "null", "ops"),
                            List.of("5", "old", "Ito"),
                            List.of("6", "old", "Lee"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where hired >= '2020-01-01' or hired < DATE '2019-01-01'",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("3"),
                            List.of("5"),
                            List.of("6"))),
            new Case("select \"ID\", salary * 1.5 as a, bonus + 2.5 as b, age * 1.5 as c from tb('store::HR.EMP') where bonus > 2.5",
                    false, List.of("ID", "a", "b", "c"),
                    List.of(List.of("1", "150", "13", "45"),
                            List.of("3", "null", "7.75", "37.5"))),
            new Case("select \"ID\", \"firstName\" || ' ' || \"LAST_NAME\" as full, -age as neg, abs(-age) as ab, age % 7 as r, length(trim(dept)) as l from tb('store::HR.EMP')",
                    false, List.of("ID", "full", "neg", "ab", "r", "l"),
                    List.of(List.of("1", "Ann Lee", "-30", "30", "2", "3"),
                            List.of("2", "Bob Kim", "-40", "40", "5", "3"),
                            List.of("3", "Cid Ng", "-25", "25", "4", "3"),
                            List.of("4", "null", "null", "null", "null", "3"),
                            List.of("5", "Eve Ito", "-35", "35", "0", "null"),
                            List.of("6", "Fay Lee", "-30", "30", "2", "3"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where not (dept = 'eng' and age > 35) and \"LAST_NAME\" is not null",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("3"),
                            List.of("5"),
                            List.of("6"))),
            new Case("select \"ID\", bonus * 2 as b2, salary / 2 as half from tb('store::HR.EMP') where \"ID\" between 2 and 4",
                    false, List.of("ID", "b2", "half"),
                    List.of(List.of("2", "null", "40"),
                            List.of("3", "10.5", "null"),
                            List.of("4", "4", "30"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where \"ID\" not between 2 and 4",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("5"),
                            List.of("6"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where age between 26 and 35 or age is null",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("4"),
                            List.of("5"),
                            List.of("6"))),
            new Case("select dept, min(age) as lo, max(age) as hi, sum(age) as s from tb('store::HR.EMP') where hired is not null group by dept order by dept",
                    true, List.of("dept", "lo", "hi", "s"),
                    List.of(List.of("eng", "30", "40", "100"),
                            List.of("ops", "25", "25", "25"),
                            List.of("null", "35", "35", "35"))),
            new Case("select \"LAST_NAME\", count(*) as n from tb('store::HR.EMP') group by \"LAST_NAME\" order by n desc, \"LAST_NAME\"",
                    true, List.of("LAST_NAME", "n"),
                    List.of(List.of("Lee", "2"),
                            List.of("Ito", "1"),
                            List.of("Kim", "1"),
                            List.of("Ng", "1"),
                            List.of("null", "1"))),
            new Case("select dept, sum(salary) / count(*) as mean from tb('store::HR.EMP') group by dept order by 2 desc nulls last",
                    true, List.of("dept", "mean"),
                    List.of(List.of("eng", "93.33333333333333"),
                            List.of("null", "90"),
                            List.of("ops", "30"))),
            new Case("select \"ID\", case dept when 'eng' then 1 when 'ops' then 2 else 0 end as code from tb('store::HR.EMP') order by \"ID\"",
                    true, List.of("ID", "code"),
                    List.of(List.of("1", "1"),
                            List.of("2", "1"),
                            List.of("3", "2"),
                            List.of("4", "2"),
                            List.of("5", "0"),
                            List.of("6", "1"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where case when age > 30 then true else false end",
                    false, List.of("ID"),
                    List.of(List.of("2"),
                            List.of("5"))),
            new Case("select \"ID\", ceil(salary / 30) as c, floor(salary / 30) as f from tb('store::HR.EMP') order by 1",
                    true, List.of("ID", "c", "f"),
                    List.of(List.of("1", "4", "3"),
                            List.of("2", "3", "2"),
                            List.of("3", "null", "null"),
                            List.of("4", "2", "2"),
                            List.of("5", "3", "3"),
                            List.of("6", "4", "3"))),
            new Case("select distinct age, dept from tb('store::HR.EMP') order by age nulls first, dept",
                    true, List.of("age", "dept"),
                    List.of(List.of("null", "ops"),
                            List.of("25", "ops"),
                            List.of("30", "eng"),
                            List.of("35", "null"),
                            List.of("40", "eng"))),
            new Case("select \"ID\" from tb('store::HR.EMP') order by \"LAST_NAME\" desc, \"ID\" fetch first 3 rows only",
                    true, List.of("ID"),
                    List.of(List.of("4"),
                            List.of("3"),
                            List.of("1"))),
            new Case("select \"ID\", hired from tb('store::HR.EMP') where hired between '2019-01-01' and '2020-12-31' order by hired, \"ID\"",
                    true, List.of("ID", "hired"),
                    List.of(List.of("2", "2019-03-01"),
                            List.of("1", "2020-01-15"),
                            List.of("6", "2020-01-15"))),
            new Case("select \"ID\", \"LAST_NAME\" is not null as has_last, dept is null as no_dept from tb('store::HR.EMP')",
                    false, List.of("ID", "has_last", "no_dept"),
                    List.of(List.of("1", "true", "false"),
                            List.of("2", "true", "false"),
                            List.of("3", "true", "false"),
                            List.of("4", "false", "false"),
                            List.of("5", "true", "true"),
                            List.of("6", "true", "false"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where \"LAST_NAME\" not like 'L%'",
                    false, List.of("ID"),
                    List.of(List.of("2"),
                            List.of("3"),
                            List.of("5"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where age not between 26 and 34",
                    false, List.of("ID"),
                    List.of(List.of("2"),
                            List.of("3"),
                            List.of("5"))),
            new Case("select \"ID\", case when salary > 85 then 'high' end as band from tb('store::HR.EMP') order by \"ID\"",
                    true, List.of("ID", "band"),
                    List.of(List.of("1", "high"),
                            List.of("2", "null"),
                            List.of("3", "null"),
                            List.of("4", "null"),
                            List.of("5", "high"),
                            List.of("6", "high"))),
            new Case("select \"ID\", coalesce(bonus, salary) as pay from tb('store::HR.EMP') order by \"ID\"",
                    true, List.of("ID", "pay"),
                    List.of(List.of("1", "10.5"),
                            List.of("2", "80"),
                            List.of("3", "5.25"),
                            List.of("4", "2"),
                            List.of("5", "1"),
                            List.of("6", "0"))),
            new Case("select dept from tb('store::HR.EMP') group by dept having sum(salary) > 100 order by dept",
                    true, List.of("dept"),
                    List.of(List.of("eng"))),
            new Case("select count(*) as n from tb('store::HR.EMP') where dept = 'nobody'",
                    false, List.of("n"),
                    List.of(List.of("0"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where \"firstName\" in ('Ann', 'Eve', 'Zed') order by \"ID\"",
                    true, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("5"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where age > 30 is not true",
                    false, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("3"),
                            List.of("4"),
                            List.of("6"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where age > 30 is not true order by \"ID\"",
                    true, List.of("ID"),
                    List.of(List.of("1"),
                            List.of("3"),
                            List.of("4"),
                            List.of("6"))),
            new Case("select \"ID\", age > 30 is unknown as u, age > 30 is false as f from tb('store::HR.EMP') order by \"ID\"",
                    true, List.of("ID", "u", "f"),
                    List.of(List.of("1", "false", "true"),
                            List.of("2", "false", "false"),
                            List.of("3", "false", "true"),
                            List.of("4", "true", "false"),
                            List.of("5", "false", "false"),
                            List.of("6", "false", "true"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where \"LAST_NAME\" is distinct from 'Lee' order by \"ID\"",
                    true, List.of("ID"),
                    List.of(List.of("2"),
                            List.of("3"),
                            List.of("4"),
                            List.of("5"))),
            new Case("select \"ID\" from tb('store::HR.EMP') where \"LAST_NAME\" is not distinct from dept order by \"ID\"",
                    true, List.of("ID"),
                    List.of()),
            new Case("select \"ID\" from tb('store::HR.EMP') where dept is not distinct from null order by \"ID\"",
                    true, List.of("ID"),
                    List.of(List.of("5"))),
            // 2026-10-18 (IQ landing 5's review): a scalar subquery an item computes for every row is computed once, a
            // column before the item (SqlToPure.output), in the Postgres front end too
            new Case("select \"ID\", age - (select min(age) from tb('store::HR.EMP')) as d, (select max(age) from"
                    + " tb('store::HR.EMP') e where e.dept = t.dept) * 2 as m from tb('store::HR.EMP') t order by \"ID\"",
                    true, List.of("ID", "d", "m"),
                    List.of(List.of("1", "5", "80"),
                            List.of("2", "15", "80"),
                            List.of("3", "0", "50"),
                            List.of("4", "null", "50"),
                            List.of("5", "10", "null"),
                            List.of("6", "5", "80"))));
}
