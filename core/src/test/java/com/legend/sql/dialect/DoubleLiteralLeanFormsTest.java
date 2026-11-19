package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import org.junit.jupiter.api.Test;

/**
 * S20'S WASTE LOOK (2026-11-17, the orchestrator's go): once a Float literal is a double, three dialects wrote it twice.
 * <ul>
 *   <li>MariaDB and MySQL wrapped a whole number in the double's half-away formula.</li>
 *   <li>SQLite wrapped its half-even rounding in another {@code round}.</li>
 *   <li>H2 cast its double literal to a double again.</li>
 * </ul>
 * Each lean form is pinned here, as the product plans it.
 */
class DoubleLiteralLeanFormsTest {

    private static String sql(DatabaseType type, String query) {
        String model;
        try {
            model = StorelessRuntime.with("", type);
        } catch (RuntimeException notStoreless) {
            model = StorelessRuntime.onServer("", type, "localhost", 1, "legend");
        }
        return Compiler.query(Compiler.compileModel(model), query).plan(StorelessRuntime.RUNTIME).sql();
    }

    @Test
    void mariaDbAndMySqlCastARoundingToAnIntegerAsItIs() {
        assertEquals("SELECT CAST(ROUND(-17.6E0) AS SIGNED) AS \"value\"",
                sql(DatabaseType.MariaDB, "|meta::pure::functions::math::round(-17.6)"));
        assertEquals("SELECT CAST(ROUND(-17.6E0) AS SIGNED) AS \"value\"",
                sql(DatabaseType.MySQL, "|meta::pure::functions::math::round(-17.6)"));
    }

    @Test
    void sqliteCastsItsHalfEvenRoundingToAnIntegerAsItIs() {
        String s = sql(DatabaseType.SQLite, "|meta::pure::functions::math::round(-17.6)");
        assertEquals("SELECT CAST(((CASE WHEN abs((-17.6 * 1) - trunc((-17.6 * 1))) = 0.5 AND CAST(round((-17.6 * 1))"
                + " AS INTEGER) % 2 <> 0 THEN round((-17.6 * 1)) - sign((-17.6 * 1)) ELSE round((-17.6 * 1)) END) / 1)"
                + " AS INTEGER) AS value", s);
    }

    @Test
    void h2WritesItsDoubleLiteralOnce() {
        assertEquals("SELECT (CAST(1.75 AS DOUBLE PRECISION) / CAST(0.5 AS DOUBLE PRECISION)) AS \"value\"",
                sql(DatabaseType.H2, "|1.75 / 0.5"));
    }
}
