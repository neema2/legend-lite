// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link Ddl#createTableAsSelect} &mdash; materialising a QUERY under a
 * name, the counterpart to {@link Ddl#createTable}'s
 * declared-columns form.
 *
 * <p>This exists because clients were composing the DDL themselves: a
 * cube that freezes or caches a result wrote
 * {@code CREATE OR REPLACE TABLE x AS <sql>} in its own source, which
 * puts a per-dialect DDL spelling wherever that client runs. The
 * statement is GENERATED from structure here, so it never passes
 * through {@link com.legend.sql.dialect.RawSqlBoundary} &mdash; the one
 * sanctioned home for pattern-based SQL rewriting, whose caller set is
 * pinned shrink-only.
 */
class CreateTableAsSelectTest {

    private static final String SELECT = "select \"a\" from \"t\"";

    @Test
    @DisplayName("materialises a select under a qualified name")
    void qualified() {
        assertEquals(
                "Create Or Replace Table s.T as " + SELECT,
                Ddl.createTableAsSelect("s", "T", SELECT,
                        Ddl.Flavor.DUCK_EXEC));
    }

    @Test
    @DisplayName("omits the schema when there is none, as qualify does")
    void unqualified() {
        // Same rule as every other statement in this class: null, empty
        // and "default" are all "no schema".
        for (String none : new String[] {null, "", "default"}) {
            assertEquals(
                    "Create Or Replace Table T as " + SELECT,
                    Ddl.createTableAsSelect(none, "T", SELECT,
                            Ddl.Flavor.DUCK_EXEC),
                    "schema=" + none);
        }
    }

    @Test
    @DisplayName("quotes a table name that is not a plain identifier")
    void quotesTheName() {
        // The name can come from OUTSIDE -- a cube names a frozen table
        // after a file. `pivot.csv` produced a bare `pivot`, which is
        // reserved in DuckDB and failed to parse.
        String sql = Ddl.createTableAsSelect(null, "two words", SELECT,
                Ddl.Flavor.DUCK_EXEC);
        assertTrue(sql.startsWith("Create Or Replace Table \"two words\" as"),
                sql);
    }

    @Test
    @DisplayName("places the select VERBATIM, never re-spelled")
    void selectIsVerbatim() {
        // `selectSql` is IR-rendered output. Inspecting or rewriting it
        // here would be a second SQL-rewriting site, which the R0 rule
        // forbids -- so a select carrying anything at all comes out
        // byte-identical.
        String odd = "select 1 as \"a b\" -- Create Table nope\nunion all "
                + "select 2";
        String sql = Ddl.createTableAsSelect(null, "T", odd,
                Ddl.Flavor.DUCK_EXEC);
        assertTrue(sql.endsWith(odd), sql);
        assertEquals("Create Or Replace Table T as " + odd, sql);
    }

    @Test
    @DisplayName("drop-then-create is available without Or Replace")
    void withoutOrReplace() {
        assertEquals("Create Table T as " + SELECT,
                Ddl.createTableAsSelect(null, "T", SELECT,
                        Ddl.Flavor.H2_EXEC, false));
    }

    @Test
    @DisplayName("REFUSES Or Replace on the engine text flavor")
    void engineTextRefusesOrReplace() {
        // The engine's corpus spells drop-then-create and its parser has
        // no Or Replace, so emitting one would produce text the engine
        // rejects. Loud here beats a failure at execution.
        IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> Ddl.createTableAsSelect(null, "T", SELECT,
                        Ddl.Flavor.ENGINE_TEXT));
        assertTrue(e.getMessage().contains("Or Replace"), e.getMessage());
        // ...and the drop-then-create form is what it points you at.
        assertEquals("Create Table T as " + SELECT,
                Ddl.createTableAsSelect(null, "T", SELECT,
                        Ddl.Flavor.ENGINE_TEXT, false));
        assertEquals("Drop table if exists T;", Ddl.dropTable(null, "T"));
    }
}
