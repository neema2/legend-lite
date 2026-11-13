// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * INJECTION FUZZING OF EVERY PLACE A PERSON'S TEXT REACHES A STATEMENT (the next PR's item 3, "separation made
 * foolproof", 2026-11-13; E's oracle). Each payload -- a quote of every kind, a backslash before one, a comment's
 * opening, a line break -- carries a marker word, and is planned in each place a query puts a person's text (a string
 * literal, a filter's value, a column's name, a format's template, a date format's text, a LIKE pattern, a separator)
 * for every relational database lite plans for. Each planned statement is then read by its database's own lexical
 * rules (which quotes open a string or an identifier, whether a backslash escapes, which comments there are): the
 * marker must lie inside a string literal or a quoted identifier, and no literal, identifier or comment may be left
 * unterminated. Anything else is a statement whose structure a person's text changed.
 *
 * <p>The findings are pinned by database and place (KNOWN, shrink-only): each is a defect named until it is fixed, and a
 * new one fails the test.
 */
class InjectionFuzzTest {

    static final String MARK = "ZQXMARK";

    /** The payloads, each carrying the marker. */
    static final List<String> PAYLOADS = List.of(
            MARK,
            "a'" + MARK,
            "a''" + MARK,
            "a\\'" + MARK + " --",
            "a\\\\'" + MARK + " --",
            "a\"" + MARK,
            "a\\\"" + MARK + " , 1 AS v --",
            "a`" + MARK,
            "a\\`" + MARK + " , 1 AS v --",
            MARK + "\\",
            "a -- " + MARK,
            "a /* " + MARK + " */",
            "a */ " + MARK,
            "a # " + MARK,
            "a // " + MARK,
            "a\n" + MARK,
            "a $$ " + MARK + " $$",
            "a ? " + MARK,
            "a\\0" + MARK,
            // a name that STARTS AND ENDS with a quote, a lone one inside (the C4 audit's H2 hole, 2026-11-13: a
            // pre-quoted name passed through unchecked): each dialect's quote and a store declaration's
            "\"a\", " + MARK + " AS \"b\"",
            "\"a\" -- " + MARK + " \"",
            "\"a\\\", " + MARK + " AS \"b\\\"",
            "`a`, " + MARK + " AS `b`",
            "`a\\`, " + MARK + " AS `b\\`");

    /** The places a query puts a person's text, each a query with {@code P} where the (Pure-escaped) payload goes. */
    static final Map<String, String> PLACES = places();

    private static Map<String, String> places() {
        Map<String, String> out = new LinkedHashMap<>();
        out.put("a string literal", "|#>{s::DB.T}#->extend(~s:r|'P')");
        out.put("a filter's value", "|#>{s::DB.T}#->filter(r|$r.NAME == 'P')");
        out.put("a column's name (extend)", "|#>{s::DB.T}#->extend(~['P':r|1])");
        out.put("a column's name (rename)", "|#>{s::DB.T}#->select(~[ID])->rename(~ID, ~'P')");
        out.put("a column's name (aggregate)", "|#>{s::DB.T}#->groupBy(~[ID], ~['P':r|$r.NAME:y|$y->count()])");
        out.put("a format's template", "|#>{s::DB.T}#->extend(~f:r|format('P %s', [$r.NAME]))");
        out.put("a date format's text", "|#>{s::DB.T}#->extend(~f:r|format('%t{yyyy\"P\"}', [$r.TS]))");
        out.put("a LIKE pattern (contains)", "|#>{s::DB.T}#->filter(r|$r.NAME->contains('P'))");
        out.put("a LIKE pattern (startsWith)", "|#>{s::DB.T}#->filter(r|$r.NAME->startsWith('P'))");
        out.put("a LIKE pattern (endsWith)", "|#>{s::DB.T}#->filter(r|$r.NAME->endsWith('P'))");
        out.put("a separator", "|#>{s::DB.T}#->groupBy(~[ID], ~[n:r|$r.NAME:y|$y->joinStrings('P')])");
        return out;
    }

    /** The defects found, by database and place: shrink-only, each named until fixed. */
    static final Map<String, Set<String>> KNOWN = Map.of();

    /** The places where a payload is refused, but not by name (by database): shrink-only, each named until fixed.
     *  Empty since 2026-11-13 (the C4 audit's M3, F-M3-1): its one entry, a date format's text on every database, went
     *  with DateFormats refusing by name -- an unsupported token a NotImplementedException, a malformed pattern a
     *  ModelException of the LOWER phase, each message as it was. */
    static final Map<String, Set<String>> UNNAMED_KNOWN = Map.of();

    /**
     * Whether {@code e} is LITE'S NAMED REFUSAL (the C4 audit's M3, 2026-11-13: until then every fuzz counted any
     * RuntimeException "refused", so a NullPointerException a payload provoked passed as a refusal): a refusal of the
     * text in lite's own vocabulary -- what the server answers with an errorType ({@code PureV1Api.refusal}: a
     * LegendCompileException, a NotImplementedException, a DialectCapability) -- or a malformed request said as one, an
     * IllegalArgumentException of exactly that class thrown by lite's own code (a class of {@code com.legend} or the
     * boundary's {@code planner}; the server answers it 500 as a malformed request, never logged as a bug). Anything
     * else -- an IllegalStateException, an index out of bounds, a JDK's own IllegalArgumentException (a
     * NumberFormatException, a {@code List.of} duplicate) -- is no refusal, and the fuzz fails naming its payload, its
     * place and its database.
     */
    static boolean namedRefusal(RuntimeException e) {
        if (!com.legend.server.PureV1Api.refusal(e).kind().equals("500")) {
            return true;
        }
        if (e.getClass() != IllegalArgumentException.class || e.getStackTrace().length == 0) {
            return false;
        }
        String thrower = e.getStackTrace()[0].getClassName();
        return thrower.startsWith("com.legend.") || thrower.startsWith("planner.");
    }

    /** {@code e} as a finding's words: its class and the first line of its message. */
    static String unnamed(RuntimeException e) {
        String m = String.valueOf(e.getMessage());
        return "refused, but not by name: " + e.getClass().getName() + ": " + m.lines().findFirst().orElse("")
                + (e.getStackTrace().length == 0 ? "" : " (at " + e.getStackTrace()[0] + ")");
    }

    static String model(DatabaseType type) {
        return """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100), TS TIMESTAMP ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME,TS\\n1,x,2020-01-01 00:00:00\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
    }

    /** {@code p} as the body of a Pure string literal. */
    static String pure(String p) {
        return p.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n");
    }

    /** Every statement a plan's JSON carries: each {@code statement}, each of a script's {@code statements}, each
     *  text {@code sql}, and each content blob's statements. */
    static List<String> statements(String planJson) {
        List<String> out = new ArrayList<>();
        collect(com.legend.json.Json.parse(planJson), null, out);
        return out;
    }

    private static void collect(com.legend.json.Json.Node node, String key, List<String> out) {
        switch (node) {
            case com.legend.json.Json.Obj o -> o.fields().forEach((k, v) -> collect(v, k, out));
            case com.legend.json.Json.Arr a -> a.items().forEach(n -> collect(n, key, out));
            case com.legend.json.Json.Str s -> {
                if ("statement".equals(key) || "statements".equals(key) || "sql".equals(key)) {
                    out.add(s.value());
                } else if ("content".equals(key) || s.value().startsWith("[\"")) {
                    // a blob of statements, by its digest
                    try {
                        collect(com.legend.json.Json.parse(s.value()), "statements", out);
                    } catch (RuntimeException notJson) {
                        // a blob that is not statements (a payload's rows): no statement in it
                    }
                }
            }
            default -> {
            }
        }
    }

    // ---- the oracle: a database's own lexical rules

    static final byte CODE = 0;
    static final byte STRING = 1;
    static final byte IDENT = 2;
    static final byte COMMENT = 3;

    /** A database's lexical rules: what opens a string and an identifier, whether a backslash escapes in each, whether
     *  a doubled quote escapes, and its comments. */
    record Rules(String stringQuotes, boolean stringBackslash, boolean stringDoubling, String identQuotes,
            boolean identBackslash, boolean hashComment, boolean slashComment, boolean eStrings) {
    }

    static Rules rules(DatabaseType type) {
        String dialect = com.legend.database.Databases.dialect(type).getClass().getSimpleName();
        return switch (dialect) {
            // standard_conforming_strings: a backslash is a character, save in E'...'
            case "Postgres", "DuckDb" -> new Rules("'", false, true, "\"", false, false, false, true);
            case "H2", "Trino", "SQLite" -> new Rules("'", false, true, "\"`", false, false, false, false);
            // the session's sql_mode: ANSI_QUOTES and NO_BACKSLASH_ESCAPES
            case "MariaDb", "MySql" -> new Rules("'", false, true, "\"`", false, true, false, false);
            // its lexer reads a backslash escape in a string AND in a quoted identifier, and a doubled quote too
            case "ClickHouse" -> new Rules("'", true, true, "\"`", true, true, false, false);
            // Spark: a string in either quote, a backslash escape, no doubled quote ('it''s' is two literals)
            case "Databricks" -> new Rules("'\"", true, false, "`", false, false, false, false);
            case "Snowflake" -> new Rules("'", true, true, "\"", false, false, true, false);
            // GoogleSQL: a backslash escape in a string and in a quoted identifier, no doubled quote
            case "BigQuery" -> new Rules("'\"", true, false, "`", true, true, false, false);
            default -> throw new AssertionError("no lexical rules for " + dialect);
        };
    }

    /** Each character's class, or the reason the statement is left open. */
    record Lexed(byte[] classes, String open) {
    }

    static Lexed lex(String s, Rules r) {
        byte[] c = new byte[s.length()];
        int i = 0;
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (r.stringQuotes().indexOf(ch) >= 0) {
                boolean e = r.eStrings() && i > 0 && (s.charAt(i - 1) == 'E' || s.charAt(i - 1) == 'e')
                        && (i == 1 || !Character.isLetterOrDigit(s.charAt(i - 2)));
                int end = quoted(s, i, ch, r.stringBackslash() || e, r.stringDoubling(), c, STRING);
                if (end < 0) {
                    return new Lexed(c, "a string left open at " + i);
                }
                i = end;
            } else if (r.identQuotes().indexOf(ch) >= 0) {
                int end = quoted(s, i, ch, r.identBackslash(), true, c, IDENT);
                if (end < 0) {
                    return new Lexed(c, "an identifier left open at " + i);
                }
                i = end;
            } else if (s.startsWith("--", i) || (r.hashComment() && ch == '#')
                    || (r.slashComment() && s.startsWith("//", i))) {
                while (i < s.length() && s.charAt(i) != '\n') {
                    c[i++] = COMMENT;
                }
            } else if (s.startsWith("/*", i)) {
                int end = s.indexOf("*/", i + 2);
                if (end < 0) {
                    return new Lexed(c, "a comment left open at " + i);
                }
                for (int k = i; k < end + 2; k++) {
                    c[k] = COMMENT;
                }
                i = end + 2;
            } else {
                c[i++] = CODE;
            }
        }
        return new Lexed(c, null);
    }

    /** Whether the marker at {@code at} is a whole bare word: the payload that is a plain identifier, written as one. */
    private static boolean wholeWord(String s, int at) {
        int end = at + MARK.length();
        return (at == 0 || !Character.isLetterOrDigit(s.charAt(at - 1)) && s.charAt(at - 1) != '_')
                && (end == s.length() || !Character.isLetterOrDigit(s.charAt(end)) && s.charAt(end) != '_');
    }

    /** The quoted token at {@code start} (its quote {@code q}), each of its characters marked {@code as}: its end, or
     *  -1 left open. */
    private static int quoted(String s, int start, char q, boolean backslash, boolean doubling, byte[] c, byte as) {
        c[start] = as;
        int i = start + 1;
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (backslash && ch == '\\') {
                if (i + 1 >= s.length()) {
                    return -1;
                }
                c[i] = as;
                c[i + 1] = as;
                i += 2;
            } else if (ch == q) {
                c[i] = as;
                if (doubling && i + 1 < s.length() && s.charAt(i + 1) == q) {
                    c[i + 1] = as;
                    i += 2;
                } else {
                    return i + 1;
                }
            } else {
                c[i++] = as;
            }
        }
        return -1;
    }

    /** What is wrong with {@code statement} under {@code r}: an open token, or the marker outside a literal or a quoted
     *  identifier -- save the one payload that is itself a plain identifier, written bare as one whole word (no
     *  structure to change); null when nothing is. */
    static String finding(String statement, Rules r) {
        return finding(statement, r, false);
    }

    /** {@link #finding(String, Rules)}, {@code plain} when the payload is the marker alone (a plain identifier). */
    static String finding(String statement, Rules r, boolean plain) {
        Lexed l = lex(statement, r);
        if (l.open() != null) {
            return l.open();
        }
        for (int at = statement.indexOf(MARK); at >= 0; at = statement.indexOf(MARK, at + 1)) {
            for (int k = at; k < at + MARK.length(); k++) {
                byte cls = l.classes()[k];
                if (plain && cls == CODE && wholeWord(statement, at)) {
                    continue;
                }
                if (cls != STRING && cls != IDENT) {
                    return "the marker in " + (cls == COMMENT ? "a comment" : "the statement's code") + " at " + at;
                }
            }
        }
        return null;
    }

    // ---- the oracle held to its own cases

    @Test
    void theOracleReadsEachDatabasesRules() {
        Rules clickHouse = rules(DatabaseType.ClickHouse);
        assertEquals(null, finding("SELECT 'a\\'ZQXMARK' AS \"x\"", clickHouse));
        assertTrue(finding("SELECT 1 AS \"a\\\"\" , ZQXMARK AS v --\"", clickHouse).startsWith("the marker in the statement's code"));
        Rules duckDb = rules(DatabaseType.DuckDB);
        assertEquals(null, finding("SELECT 'a\\''ZQXMARK'", duckDb));
        assertTrue(finding("SELECT 'a' || ZQXMARKS", duckDb).startsWith("the marker in the statement's code"));
        assertEquals(null, finding("SELECT 1 AS ZQXMARK", duckDb, true));
        assertTrue(finding("SELECT 1 AS ZQXMARK", duckDb, false).startsWith("the marker in the statement's code"));
        assertTrue(finding("SELECT 1 -- ZQXMARK", duckDb).startsWith("the marker in a comment"));
        assertEquals("a string left open at 7", finding("SELECT 'ZQXMARK", duckDb));
        assertEquals(null, finding("SELECT E'a\\'ZQXMARK'", duckDb));
        Rules databricks = rules(DatabaseType.Databricks);
        assertEquals(null, finding("SELECT `a``ZQXMARK`", databricks));
        assertTrue(finding("SELECT 'it''s ' ZQXMARK", databricks).startsWith("the marker in the statement's code"));
    }

    /** The refusal rule counts lite's own refusals alone (the C4 audit's M3, 2026-11-13). */
    @Test
    void aRefusalCountsOnlyWhenLiteNamesIt() {
        assertTrue(namedRefusal(new com.legend.error.NotImplementedException("not designed")));
        assertTrue(namedRefusal(new com.legend.sql.DialectCapability("not on this database")));
        assertTrue(namedRefusal(assertThrowsFrom(() -> new com.legend.executionplan.DataRef(
                com.legend.executionplan.DataRef.Namespace.RESOURCE, "a.csv", "not a digest", 0L,
                new com.legend.executionplan.DataRef.Parquet()))), "lite's own IllegalArgumentException");
        assertTrue(!namedRefusal(new NullPointerException("a payload's crash")));
        assertTrue(!namedRefusal(new IllegalStateException("a bare state")));
        assertTrue(!namedRefusal(new IndexOutOfBoundsException("a bare index")));
        assertTrue(!namedRefusal(assertThrowsFrom(() -> Integer.parseInt("x"))), "a JDK's NumberFormatException");
        assertTrue(!namedRefusal(assertThrowsFrom(() -> Set.of(1, 1))), "a JDK's own IllegalArgumentException");
    }

    private static RuntimeException assertThrowsFrom(java.util.function.Supplier<?> call) {
        return org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, call::get);
    }

    // ---- the fuzz

    @Test
    void noPersonsTextChangesAStatementsStructureOnAnyDatabase() {
        Map<String, Set<String>> found = new TreeMap<>();
        Map<String, Set<String>> unnamed = new TreeMap<>();
        Map<String, Integer> byDatabase = new TreeMap<>();
        Map<String, Integer> byPlace = new TreeMap<>();
        List<String> examples = new ArrayList<>();
        int planned = 0;
        int refused = 0;
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            String dialect = com.legend.database.Databases.dialect(type).getClass().getSimpleName();
            if (dialect.equals("MongoDb")) {
                continue;
            }
            Rules r = rules(type);
            var ctx = Compiler.compileModel(model(type));
            for (Map.Entry<String, String> place : PLACES.entrySet()) {
                for (String payload : PAYLOADS) {
                    String query = place.getValue().replace("P", pure(payload));
                    String json;
                    try {
                        json = com.legend.executionplan.PlanJson.write(Compiler.query(ctx, query).executionPlan("s::RT"));
                    } catch (RuntimeException refusal) {
                        if (!namedRefusal(refusal)
                                && unnamed.computeIfAbsent(type.name(), k -> new TreeSet<>()).add(place.getKey())) {
                            examples.add(type.name() + ", " + place.getKey() + ", " + payload.replace("\n", "\\n")
                                    + ": " + unnamed(refusal));
                        }
                        refused++;
                        continue;
                    }
                    planned++;
                    byDatabase.merge(type.name(), 1, Integer::sum);
                    byPlace.merge(place.getKey(), 1, Integer::sum);
                    for (String statement : statements(json)) {
                        String wrong = finding(statement, r, payload.equals(MARK));
                        if (wrong != null) {
                            if (found.computeIfAbsent(type.name(), k -> new TreeSet<>()).add(place.getKey())) {
                                examples.add(type.name() + ", " + place.getKey() + ", " + payload.replace("\n", "\\n")
                                        + ": " + wrong + ": " + statement.replace("\n", " "));
                            }
                        }
                    }
                }
            }
        }
        assertTrue(planned > 0, "nothing planned");
        String said = "planned " + planned + ", refused " + refused + "; one example each:\n" + String.join("\n", examples);
        assertEquals(KNOWN, found, said);
        assertEquals(UNNAMED_KNOWN, unnamed, said);
        // the coverage, pinned (the C4 audit's M3, 2026-11-13): the payloads each database and each place PLANS, so a
        // change that refuses a place's payloads wholesale -- and so finds nothing there -- fails here, never passes
        assertEquals(PLANNED_BY_DATABASE, byDatabase, "the payloads planned, by database");
        assertEquals(PLANNED_BY_PLACE, byPlace, "the payloads planned, by place");
    }

    /** The payloads each database plans, of every place's (measured 2026-11-13, the C4 audit's M3; a move is dated and
     *  names its task). */
    static final Map<String, Integer> PLANNED_BY_DATABASE = java.util.Map.ofEntries(
            Map.entry("BigQuery", 259),
            Map.entry("ClickHouse", 259),
            Map.entry("Databricks", 259),
            Map.entry("DuckDB", 259),
            Map.entry("H2", 259),
            Map.entry("MariaDB", 259),
            Map.entry("MySQL", 259),
            Map.entry("Postgres", 259),
            Map.entry("SQLite", 259),
            Map.entry("Snowflake", 259),
            Map.entry("Trino", 259));

    /** The payloads each place plans, on every database (measured 2026-11-13, the C4 audit's M3). */
    static final Map<String, Integer> PLANNED_BY_PLACE = java.util.Map.ofEntries(
            Map.entry("a LIKE pattern (contains)", 264),
            Map.entry("a LIKE pattern (endsWith)", 264),
            Map.entry("a LIKE pattern (startsWith)", 264),
            Map.entry("a column's name (aggregate)", 264),
            Map.entry("a column's name (extend)", 264),
            Map.entry("a column's name (rename)", 264),
            Map.entry("a date format's text", 209),
            Map.entry("a filter's value", 264),
            Map.entry("a format's template", 264),
            Map.entry("a separator", 264),
            Map.entry("a string literal", 264));
}
