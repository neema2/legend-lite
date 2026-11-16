// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * H2'S OWN KEYWORDS against the renderer's quoting (the third re-audit's MUST-FIX 2(c), 2026-11-12): every keyword H2
 * 2.5.252 reserves -- its driver's own list ({@code org.h2.command.Token}'s keywords, 89), never the engine's lexicon --
 * is quoted by the H2 dialect, so a session that does not free the engine's non-keywords (a bare one: D's, a user's)
 * parses every name the dialect writes. A word H2 reserves that the engine's session frees is quoted as the upper-case
 * identifier H2 makes of the bare word, so it is the same column a session that frees it reads bare. A store's column
 * {@code key}, written bare until then, failed in such a session (found by the D worker). An H2 bump that reserves a new
 * word fails here.
 */
class H2KeywordsTest {

    /** H2's keywords, as its driver lists them (upper case). */
    private static List<String> h2Keywords() throws ReflectiveOperationException {
        Class<?> token = Class.forName("org.h2.command.Token");
        java.lang.reflect.Field tokens = token.getDeclaredField("TOKENS");
        tokens.setAccessible(true);
        String[] all = (String[]) tokens.get(null);
        Class<?> parser = Class.forName("org.h2.util.ParserUtil");
        int first = parser.getField("FIRST_KEYWORD").getInt(null);
        int last = parser.getField("LAST_KEYWORD").getInt(null);
        TreeSet<String> words = new TreeSet<>();
        for (int i = first; i <= last; i++) {
            if (all[i] != null) {
                words.add(all[i]);
            }
        }
        return List.copyOf(words);
    }

    @Test
    void everyKeywordH2ReservesIsQuoted() throws ReflectiveOperationException {
        List<String> words = h2Keywords();
        assertEquals(89, words.size(), "H2 2.5.252's keywords: " + words);
        TreeSet<String> bare = new TreeSet<>();
        for (String w : words) {
            for (String spelled : List.of(w, w.toLowerCase(Locale.ROOT))) {
                if (!new H2().physicalName(spelled).startsWith("\"")) {
                    bare.add(spelled);
                }
            }
        }
        assertEquals(new TreeSet<String>(), bare, "keywords H2 reserves that the H2 dialect writes bare");
    }

    /** On a BARE H2 -- none of the engine's non-keywords freed, none of its settings -- a table whose every column is
     *  one of H2's keywords is made, filled and read by the names the dialect writes. */
    @Test
    void aBareSessionTakesEveryKeywordAsAColumn() throws Exception {
        List<String> words = h2Keywords();
        List<String> columns = new ArrayList<>();
        for (String w : words) {
            columns.add(new H2().physicalName(w.toLowerCase(Locale.ROOT)));
        }
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:h2_keywords_bare");
                Statement st = c.createStatement()) {
            st.execute("CREATE TABLE " + new H2().physicalName("key") + " (" + String.join(", ",
                    columns.stream().map(col -> col + " INTEGER").toList()) + ")");
            st.execute("INSERT INTO " + new H2().physicalName("key") + " (" + String.join(", ", columns)
                    + ") VALUES (" + String.join(", ", columns.stream().map(col -> "1").toList()) + ")");
            try (ResultSet rs = st.executeQuery("SELECT " + String.join(", ", columns) + " FROM "
                    + new H2().physicalName("key"))) {
                assertTrue(rs.next());
                assertEquals(words.size(), rs.getMetaData().getColumnCount());
            }
        }
    }

    /** The Cypher store's property tables ({@code testcases.CypherGraph}'s native store: {@code node_props},
     *  {@code rel_props}, each with a column {@code key}), made by the H2 dialect's own DDL and filled and read by its
     *  spelling on a BARE H2 -- none of the engine's non-keywords freed: the case the D worker measured failing, the
     *  DDL writing {@code key} bare (the third re-audit's MUST-FIX 2(c), 2026-11-12). */
    @Test
    void theCypherStoresTablesAreMadeAndReadOnABareSession() throws SQLException {
        com.legend.sql.SqlDdl.ColumnType text = new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", 4000);
        com.legend.sql.SqlDdl.ColumnType bigint = new com.legend.sql.SqlDdl.ColumnType.Plain(
                com.legend.sql.SqlDdl.ColumnType.Kind.BIGINT);
        H2 h2 = new H2();
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:h2_keywords_cypher");
                Statement st = c.createStatement()) {
            for (String table : List.of("node_props", "rel_props")) {
                String owner = table.equals("node_props") ? "node_id" : "rel_id";
                st.execute(h2.render(new com.legend.sql.SqlDdl.CreateTable(null, table, com.legend.sql.NameQuoting.BARE,
                        List.of(new com.legend.sql.SqlDdl.Column(owner, false, bigint, false, false),
                                new com.legend.sql.SqlDdl.Column("key", false, text, false, false),
                                new com.legend.sql.SqlDdl.Column("slot", false, text, false, false)), false)));
                String key = h2.insertColumnName("key", false);
                assertEquals("\"KEY\"", key, "the DDL's spelling of the column");
                st.executeUpdate("INSERT INTO " + h2.physicalName(table) + " (" + h2.insertColumnName(owner, false)
                        + ", " + key + ", " + h2.insertColumnName("slot", false) + ") VALUES (1, 'name', 'v_str')");
                try (ResultSet rs = st.executeQuery("SELECT " + h2.physicalName("key") + " FROM "
                        + h2.physicalName(table))) {
                    assertTrue(rs.next(), table);
                    assertEquals("name", rs.getString(1), table);
                }
            }
        }
    }

    /** The product's keywords ({@link H2#KEYWORDS}) are H2's own, its driver's list, exactly (W6 of
     *  docs/records/d-plan-2026-11-16.md, 2026-11-16: legend-engine's list is the compatibility edge's). */
    @Test
    void theProductsKeywordsAreH2sOwn() throws ReflectiveOperationException {
        assertEquals(new TreeSet<>(h2Keywords().stream().map(w -> w.toLowerCase(Locale.ROOT)).toList()),
                new TreeSet<>(H2.KEYWORDS));
    }

    /** Every keyword, quoted, names the column the bare word names where a session takes it bare: the identifier H2
     *  makes of a bare word, which it folds to upper case (the product's rule for each of H2's keywords since W6; the
     *  engine's session's freed words were its first, the third re-audit's MUST-FIX 2(c)). The identity is read from the
     *  catalog. */
    @Test
    void aKeywordQuotedIsTheColumnItNamesBare() throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:h2_keywords_upper");
                Statement st = c.createStatement()) {
            List<String> words = new ArrayList<>(new TreeSet<>(H2.KEYWORDS));
            st.execute("CREATE TABLE T_UPPER (plain_name INTEGER, " + String.join(", ", words.stream()
                    .map(w -> new H2().physicalName(w) + " INTEGER").toList()) + ")");
            List<String> stored = new ArrayList<>();
            try (ResultSet rs = st.executeQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME"
                    + " = 'T_UPPER' ORDER BY ORDINAL_POSITION")) {
                while (rs.next()) {
                    stored.add(rs.getString(1));
                }
            }
            List<String> expected = new ArrayList<>(List.of("PLAIN_NAME"));
            for (String w : words) {
                assertEquals("\"" + w.toUpperCase(Locale.ROOT) + "\"", new H2().physicalName(w));
                expected.add(w.toUpperCase(Locale.ROOT));
            }
            assertEquals(expected, stored, "each quoted word is the identifier H2 makes of the bare word");
        }
    }
}
