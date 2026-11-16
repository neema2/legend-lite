// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.PureComposer;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IQ island, 2026-10-14 (M1e; docs/IQ_SQL_STUDY_2026_10_13.md §4 O2 and O3, §9.7): the names an {@code #IQ{...}#}
 * island binds outside its SQL ({@link IqBindings}) -- an owner-qualified table to the model's tables, a host variable
 * to the enclosing Pure query's variable -- through the one translator, over a stand-in for the compiler's seam (its
 * catalog of tables and its variables in scope). The iq-parity lanes (lite/m1e.yaml) run them end to end.
 */
class IqBindingsTest {

    /** Every table of the stand-in model has these columns: id INTEGER not null, vc VARCHAR(20). */
    private static final List<SqlToPure.CatalogTable> CATALOG = List.of(
            new SqlToPure.CatalogTable("e2e::DB", "default", "t"),
            new SqlToPure.CatalogTable("e2e::DB", "sales", "Orders"),
            new SqlToPure.CatalogTable("e2e::DB", "DBA", "u"),
            new SqlToPure.CatalogTable("e2e::Other", "sales", "returns"),
            new SqlToPure.CatalogTable("e2e::Other", "default", "t"));

    /** The variables in scope: name to type, multiplicity. IQ island, 2026-10-14 (review 5): {@code Name} and
     *  {@code NAME}, two names IQ cannot tell apart; {@code stamp}, a DateTime. */
    private static final Map<String, SqlToPure.VariableType> VARIABLES = Map.of(
            "lo", new SqlToPure.VariableType("Integer", false, false),
            "name", new SqlToPure.VariableType("String", true, false),
            "day", new SqlToPure.VariableType("StrictDate", false, false),
            "stamp", new SqlToPure.VariableType("DateTime", false, false),
            "amt", new SqlToPure.VariableType("Decimal(38,2)", false, false),
            "ok", new SqlToPure.VariableType("Boolean", false, false),
            "ids", new SqlToPure.VariableType("Integer", false, true),
            "Twin", new SqlToPure.VariableType("Integer", false, false),
            "TWIN", new SqlToPure.VariableType("Integer", false, false),
            "s", new SqlToPure.VariableType("String", false, false));

    private static SqlToPure.Sources sources(List<SqlToPure.CatalogTable> catalog) {
        return new SqlToPure.Sources() {
            @Override
            public void columns(ValueSpecification source, SqlToPure.Column column) {
                column.column("id", "Integer", false, "INTEGER");
                column.column("vc", "String", true, "VARCHAR(20)");
            }

            @Override
            public void tables(SqlToPure.Table table) {
                catalog.forEach(t -> table.table(t.database(), t.schema(), t.table()));
            }

            @Override
            public boolean variable(String name, SqlToPure.Scoped type) {
                SqlToPure.VariableType t = VARIABLES.get(name);
                if (t != null) {
                    type.variable(t.type(), t.nullable(), t.many());
                }
                return t != null;
            }

            @Override
            public void variables(java.util.function.Consumer<String> names) {
                VARIABLES.keySet().forEach(names);
            }
        };
    }

    private static String pure(String sql) {
        return pure(sql, CATALOG);
    }

    private static String pure(String sql, List<SqlToPure.CatalogTable> catalog) {
        return pure(sql, catalog, IqProfile.IQ16_DEFAULT.settings());
    }

    private static String pure(String sql, List<SqlToPure.CatalogTable> catalog, IqSettings profile) {
        ValueSpecification vs = com.legend.testcases.SqlText.translate(sql, sources(catalog), IqDialect.of(profile));
        return PureComposer.valueSpecification(vs, PureComposer.Style.STANDARD);
    }

    private static SqlTranslationException refusal(String sql) {
        return assertThrows(SqlTranslationException.class, () -> pure(sql, CATALOG.subList(0, 4)));
    }

    @Test
    void anOwnerIsASchemaAndDbaOwnsTheTablesOfNone() {
        // DBA's: the tables of no schema, and of a schema DBA
        String dba = pure("SELECT id FROM DBA.t", CATALOG.subList(0, 4));
        assertTrue(dba.startsWith("#>{e2e::DB.t}#"), dba);
        assertTrue(pure("SELECT id FROM dba.U").startsWith("#>{e2e::DB.DBA.u}#"));
        // another owner's: its schema's, as IQ matches names; a database's and a server's qualifiers ignored
        assertTrue(pure("SELECT id FROM SALES.orders").startsWith("#>{e2e::DB.sales.Orders}#"));
        assertTrue(pure("SELECT id FROM srv.anydb.sales.ORDERS").startsWith("#>{e2e::DB.sales.Orders}#"));
        // an unqualified name, and T-SQL's db..t, are DBA's
        assertTrue(pure("SELECT id FROM db..u x").startsWith("#>{e2e::DB.DBA.u}#"));
        assertTrue(pure("SELECT t.id FROM t", CATALOG.subList(0, 4)).startsWith("#>{e2e::DB.t}#"));
    }

    @Test
    void theModelIsTheIslandsCatalog() {
        // an owner that is no schema (nor DBA) does not exist: IQ's -140; one's table that is not there: IQ's -141
        SqlTranslationException noUser = refusal("SELECT id FROM nobody.t");
        assertEquals(SqlTranslationException.Kind.INVALID, noUser.kind());
        assertTrue(noUser.getMessage().contains("SQLCODE -140"), noUser.getMessage());
        SqlTranslationException noTable = refusal("SELECT id FROM sales.nosuch");
        assertEquals(SqlTranslationException.Kind.INVALID, noTable.kind());
        assertTrue(noTable.getMessage().contains("SQLCODE -141"), noTable.getMessage());
        // a name two Databases hold is refused by name; so are IQ's own users' tables, and an unqualified name DBA
        // does not own (IQ looks in its catalog too)
        SqlTranslationException twice = assertThrows(SqlTranslationException.class, () -> pure("SELECT id FROM DBA.t"));
        assertEquals(SqlTranslationException.Kind.UNSUPPORTED, twice.kind(), twice.getMessage());
        assertTrue(twice.getMessage().contains("e2e::DB.t, e2e::Other.t"), twice.getMessage());
        for (String sql : new String[] {"SELECT id FROM dbo.t", "SELECT id FROM SYS.SYSTAB", "SELECT * FROM nosuch"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal(sql).kind(), sql);
        }
    }

    @Test
    void aNameOfIqsOwnUsersThatIsNoCatalogNameReadsTheModelsSchemaOfThatName() {
        // IQ island, 2026-10-14 (the third landing): a T-SQL database's dbo.orders is the model's table of a schema dbo
        List<SqlToPure.CatalogTable> tsql = List.of(new SqlToPure.CatalogTable("e2e::DB", "default", "t"),
                new SqlToPure.CatalogTable("e2e::DB", "dbo", "orders"),
                new SqlToPure.CatalogTable("e2e::DB", "dbo", "sysobjects"));
        assertTrue(pure("SELECT id FROM dbo.orders", tsql).startsWith("#>{e2e::DB.dbo.orders}#"));
        assertTrue(pure("SELECT id FROM DBO.Orders o", tsql).startsWith("#>{e2e::DB.dbo.orders}#"));
        // a catalog name stays IQ's catalog's, whatever the model holds; with no such schema, refused as before
        for (String sql : new String[] {"SELECT id FROM dbo.sysobjects", "SELECT id FROM SYS.SYSTAB",
                "SELECT id FROM dbo.t", "SELECT id FROM dbo.nosuch"}) {
            SqlTranslationException e = assertThrows(SqlTranslationException.class, () -> pure(sql, tsql), sql);
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql);
            assertTrue(e.getMessage().contains("one of IQ's own users"), e.getMessage());
        }
    }

    /** An Integer variable read as IQ's BIGINT (IQ island, 2026-10-14, review 5's B1): a value BIGINT cannot hold
     *  raises, tested through differences lite's databases compute wider than 64 bits. */
    private static final String LO = "if(($lo - 9223372036854775807 > 0) || ($lo + 9223372036854775807 < -1),"
            + " |[]->cast(@Integer)->toOne(), |$lo)";

    @Test
    void aHostVariableIsTheEnclosingQuerysVariable() {
        assertEquals("#>{e2e::DB.t}#->filter(r|$r.id > " + LO + ")->select(~[id])",
                pure("SELECT id FROM DBA.t WHERE id > @lo", CATALOG.subList(0, 4)));
        assertEquals("#>{e2e::DB.t}#->filter(r|$r.id > " + LO + ")->select(~[id])",
                pure("SELECT id FROM DBA.t WHERE id > :lo", CATALOG.subList(0, 4)));
        // a column of an @name's name is read before a variable (the research's GRAMMAR L27)
        assertTrue(pure("SELECT @lo AS v FROM (SELECT 1 AS @lo) d").contains("'@lo'"));
        // no variable: IQ's -143 for @name, -188 for :name
        assertTrue(refusal("SELECT @nope AS v").getMessage().contains("SQLCODE -143"));
        assertTrue(refusal("SELECT :nope AS v").getMessage().contains("SQLCODE -188"));
        // a type the island does not read, many values, and an unaliased item over one are refused by name
        for (String sql : new String[] {"SELECT @ok AS v", "SELECT @ids AS v", "SELECT @lo"}) {
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, refusal(sql).kind(), sql);
        }
    }

    /** IQ island, 2026-10-14 (review 5's B1): a BIGINT result over an Integer variable is checked by the island (the
     *  database computes the variable's literal wider than 64 bits near 2^63, so its own overflow never comes); one over
     *  BIGINT columns alone is left to the database. */
    @Test
    void aBigintResultOverAVariableIsChecked() {
        String plus = pure("SELECT id + @lo AS v FROM DBA.t", CATALOG.subList(0, 4));
        assertTrue(plus.contains("> 9223372036854775807), |[]->cast(@Integer)->toOne()"), plus);
        String negated = pure("SELECT -@lo AS v FROM DBA.t", CATALOG.subList(0, 4));
        assertTrue(negated.contains("> 9223372036854775807), |[]->cast(@Integer)->toOne()"), negated);
        // the same over a column alone: the database's own 64-bit overflow
        String column = pure("SELECT id + CAST(id AS BIGINT) AS v FROM DBA.t", CATALOG.subList(0, 4));
        assertTrue(!column.contains("9223372036854775807"), column);
        // a BIGINT constant near 2^63 is written wider too: checked
        String constant = pure("SELECT CAST('9223372036854775807' AS BIGINT) + 1 AS v");
        assertTrue(constant.contains("> 9223372036854775807), |[]->cast(@Integer)->toOne()"), constant);
    }

    /** IQ island, 2026-10-14 (review 5's M1, B3, m1): a variable that may hold no value, a date or a timestamp variable
     *  are refused by name; a name is matched as IQ matches names, without case, refused where two variables differ
     *  only in case. */
    @Test
    void variablesTheIslandDoesNotReadAreRefusedByName() {
        for (String sql : new String[] {"SELECT @name AS v", "SELECT id FROM DBA.t WHERE vc = @name",
            "SELECT @day AS v", "SELECT id FROM DBA.t WHERE id > DATEPART(year, @day)", "SELECT :stamp AS v",
            "SELECT @twin AS v"}) {
            SqlTranslationException e = refusal(sql);
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql + ": " + e.getMessage());
        }
        assertTrue(refusal("SELECT @name AS v").getMessage().contains("may hold no value"));
        assertTrue(refusal("SELECT @day AS v").getMessage().contains("misreads below the year 100"));
        assertTrue(refusal("SELECT @twin AS v").getMessage().contains("Twin, TWIN")
                || refusal("SELECT @twin AS v").getMessage().contains("TWIN, Twin"));
        // one variable of that name but for case: read
        assertEquals(pure("SELECT @lo AS v"), pure("SELECT @LO AS v"));
        assertEquals(pure("SELECT :lo AS v"), pure("SELECT :Lo AS v"));
    }

    /** IQ island, 2026-10-14 (review 6's M1, measured on SQL Anywhere 17): under a TDS profile (tsql_variables On) a
     *  client binds {@code @name} as a placeholder, which reads the bound value even where a column of its name is in
     *  scope; a connection's variable reads the column. Which one the island means there is not decided: refused by
     *  name. {@code :name} never reads a column. */
    @Test
    void underATdsProfileAColumnNamedLikeAVariableIsRefused() {
        String sql = "SELECT @lo AS v FROM (SELECT 1 AS @lo) d";
        for (IqSettings tds : new IqSettings[] {IqProfile.IQ16_TDS.settings(), IqProfile.SQLA17_TDS.settings()}) {
            SqlTranslationException e = assertThrows(SqlTranslationException.class,
                    () -> pure(sql, CATALOG.subList(0, 4), tds));
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), e.getMessage());
            assertTrue(e.getMessage().contains("names a column in scope"), e.getMessage());
            assertTrue(pure("SELECT :lo AS v FROM (SELECT 1 AS @lo) d", CATALOG.subList(0, 4), tds).contains("$lo"));
        }
        assertTrue(pure(sql).contains("'@lo'"), pure(sql));
    }

    /** IQ island, 2026-10-14 (review 6's B1, measured on SQL Anywhere 17): under ansinull Off {@code x = @v} is
     *  two-valued in a search condition for a column (and arithmetic or a CAST over columns), three-valued for a
     *  function's value: such a comparison under a NOT is refused where it may be NULL, and read three-valued through
     *  AND and OR alone (FALSE and UNKNOWN filter alike). A HAVING reads a select-list alias as IQ reads it. */
    @Test
    void underAnsinullOffAVariableIsComparedTwoValuedWithAColumnOnly() {
        List<SqlToPure.CatalogTable> catalog = CATALOG.subList(0, 4);
        String column = "SELECT id FROM DBA.t WHERE NOT (vc = @s)";
        assertTrue(!pure(column, catalog, IqProfile.IQ16_TDS.settings()).equals(pure(column, catalog, IqProfile.IQ16_DEFAULT.settings())));
        assertEquals(pure("SELECT id FROM DBA.t WHERE NOT (vc || '' = @s)", catalog, IqProfile.IQ16_TDS.settings()),
                pure("SELECT id FROM DBA.t WHERE NOT (vc || '' = @s)", catalog, IqProfile.IQ16_DEFAULT.settings()));
        for (String sql : new String[] {"SELECT id FROM DBA.t WHERE NOT (UPPER(vc) = @s)",
            "SELECT id FROM DBA.t WHERE NOT (@s = NULLIF(vc, 'x'))",
            "SELECT id FROM (SELECT id, UPPER(vc) AS z FROM DBA.t) d WHERE NOT (z = @s)",
            "SELECT id FROM DBA.t WHERE NOT ((SELECT u.vc FROM DBA.u u WHERE u.id = t.id) = @s)"}) {
            SqlTranslationException e = assertThrows(SqlTranslationException.class,
                    () -> pure(sql, catalog, IqProfile.SQLA17_TDS.settings()));
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql + ": " + e.getMessage());
            assertTrue(e.getMessage().contains("with this operand"), sql + ": " + e.getMessage());
        }
        String positive = "SELECT id FROM DBA.t WHERE UPPER(vc) = @s OR id = 1";
        assertEquals(pure(positive, catalog, IqProfile.IQ16_DEFAULT.settings()), pure(positive, catalog, IqProfile.IQ16_TDS.settings()));
        // HAVING's alias is its expression (read three-valued before review 6's fix, which placed HAVING as written)
        String having = "SELECT RTRIM(vc) AS z FROM DBA.t GROUP BY RTRIM(vc) HAVING NOT (z = @s)";
        SqlTranslationException alias = assertThrows(SqlTranslationException.class,
                () -> pure(having, catalog, IqProfile.IQ16_TDS.settings()));
        assertTrue(alias.getMessage().contains("with this operand"), alias.getMessage());
    }

    /** IQ island, 2026-10-14 (review 5's M3, measured on SQL Anywhere 17): an unqualified DUMMY, and db..DUMMY, read the
     *  user's own table of that name before SYS.DUMMY: refused by name when the model holds one; SYS.DUMMY is IQ's, and
     *  DBA.DUMMY the model's. */
    @Test
    void aModelTableNamedDummyIsNotIqsDummy() {
        List<SqlToPure.CatalogTable> withDummy = List.of(new SqlToPure.CatalogTable("e2e::DB", "default", "t"),
                new SqlToPure.CatalogTable("e2e::DB", "default", "Dummy"));
        for (String sql : new String[] {"SELECT COUNT(*) AS n FROM DUMMY", "SELECT COUNT(*) AS n FROM mydb..dummy"}) {
            SqlTranslationException e = assertThrows(SqlTranslationException.class, () -> pure(sql, withDummy));
            assertEquals(SqlTranslationException.Kind.UNSUPPORTED, e.kind(), sql);
            assertTrue(e.getMessage().contains("e2e::DB.Dummy"), e.getMessage());
        }
        assertTrue(pure("SELECT dummy_col FROM SYS.DUMMY", withDummy).startsWith("#TDS"),
                pure("SELECT dummy_col FROM SYS.DUMMY", withDummy));
        assertTrue(pure("SELECT id FROM DBA.DUMMY", withDummy).startsWith("#>{e2e::DB.Dummy}#"));
        // without one, DUMMY is IQ's
        assertTrue(pure("SELECT dummy_col FROM DUMMY").startsWith("#TDS"), pure("SELECT dummy_col FROM DUMMY"));
    }
}
