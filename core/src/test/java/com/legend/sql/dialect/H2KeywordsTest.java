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

    /** A word the engine's session frees, quoted, names the same column the bare word named: the identifier H2 makes of
     *  a bare word, which it folds to upper case (a plain bare name shows it), so the dialect changed how it writes the
     *  name, never which column it names. (Some freed words parse bare in a column list and not in a select list -- bare
     *  {@code any} is read as the quantifier there -- so the identity is read from the catalog.) */
    @Test
    void aFreedWordQuotedIsTheColumnItNamedBare() throws SQLException {
        assertEquals(new TreeSet<>(List.of("any", "asymmetric", "authorization", "cast", "current_path", "current_role",
                "day", "default", "else", "end", "hour", "key", "minute", "month", "second", "session_user", "set", "some",
                "symmetric", "system_user", "to", "uescape", "user", "value", "when", "year")),
                new TreeSet<>(H2.FREED_BY_THE_ENGINE));
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:h2_keywords_freed");
                Statement st = c.createStatement()) {
            st.execute(H2.SESSION_SETUP.get(0));
            st.execute("CREATE TABLE T_FREED (plain_name INTEGER, " + String.join(", ", H2.FREED_BY_THE_ENGINE.stream()
                    .map(w -> new H2().physicalName(w) + " INTEGER").toList()) + ")");
            List<String> stored = new ArrayList<>();
            try (ResultSet rs = st.executeQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME"
                    + " = 'T_FREED' ORDER BY ORDINAL_POSITION")) {
                while (rs.next()) {
                    stored.add(rs.getString(1));
                }
            }
            List<String> expected = new ArrayList<>(List.of("PLAIN_NAME"));
            for (String w : H2.FREED_BY_THE_ENGINE) {
                assertEquals("\"" + w.toUpperCase(Locale.ROOT) + "\"", new H2().physicalName(w));
                expected.add(w.toUpperCase(Locale.ROOT));
            }
            assertEquals(expected, stored, "each quoted word is the identifier H2 makes of the bare word");
        }
    }
}
