// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A NAME THAT ARRIVES ALREADY QUOTED (the C4 audit's H2 identifier hole, 2026-11-13; the user's ruling M1). A name that
 * starts and ends with a quote character is a store's quoted declaration ({@code "date"}): several writers pass it
 * through as its own spelling. Each now asks the one check ({@link AnsiSqlRenderer#quotedIdentifier}) whether its
 * interior holds that quote only doubled; H2's alias and stored-name writers asked nothing, so {@code "a", FILE_READ(..)
 * AS "b"} walked out of the identifier. Three things are held here, on every dialect and at every place a name is
 * written (an alias, a derived and a stored column, a table):
 * <ul>
 *   <li>a hostile name that starts and ends with the dialect's quote, or with a store declaration's {@code "}, stays
 *   one identifier: the marker inside it is never the statement's code ({@link InjectionFuzzTest#finding});</li>
 *   <li>the audit's H2 statement is carried as data: one column, labelled exactly the name, on a live H2;</li>
 *   <li>an ordinary name is written byte for byte as before the check, on every dialect and engine-text printer too
 *   (the golden {@code pre-quoted-names.tsv}, rendered before the change).</li>
 * </ul>
 */
class PreQuotedNamesTest {

    private static final OutputCol ID = new OutputCol("ID", SqlType.Scalar.INTEGER, false, OutputCol.Origin.PHYSICAL);

    /** Ordinary names, the well-formed quoted declarations among them: their spelling is the golden's. */
    static final List<String> ORDINARY = List.of("ID", "firstName", "first name", "select", "date", "_x", "x$y", "Äb",
            "\"date\"", "\"first name\"", "\"a\"\"b\"", "\"\"\"\"", "a\"b", "`x`", "`a``b`", "a b`c");

    /** Hostile names that start and end with a quote -- {@code "} (a declaration's, and most dialects') or {@code `}
     *  (BigQuery's and Databricks') -- with a lone quote, a backslash or a comment inside, and the marker in code. */
    static final List<String> HOSTILE = hostile();

    private static List<String> hostile() {
        String m = InjectionFuzzTest.MARK;
        List<String> out = new ArrayList<>();
        for (char q : new char[] {'"', '`'}) {
            String Q = String.valueOf(q);
            out.add(Q + "a" + Q + ", " + m + " AS " + Q + "b" + Q);
            out.add(Q + "a" + Q + Q + Q + ", " + m + " AS " + Q + "b" + Q);
            out.add(Q + "a" + Q + " -- " + m + " " + Q);
            out.add(Q + "a\\" + Q + ", " + m + " AS " + Q + "b\\" + Q);
            out.add(Q + Q + Q + " , " + m + " , " + Q + Q + Q);
            out.add(Q + "a" + Q + " FROM (SELECT 1) x0 , " + m + " , (SELECT " + Q + "b" + Q);
        }
        return out;
    }

    /** Each SQL dialect with its database's lexical rules. */
    static Map<SqlDialect, InjectionFuzzTest.Rules> dialects() {
        Map<SqlDialect, InjectionFuzzTest.Rules> out = new LinkedHashMap<>();
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            out.put(com.legend.database.Databases.dialect(type), InjectionFuzzTest.rules(type));
        }
        return out;
    }

    /** The engine-text printers: in the golden (unchanged), not in the hostile fuzz (their spelling is the engine
     *  text's, decision D's). */
    static List<SqlDialect> engineText() {
        return List.of(new EngineStyleH2(), new EngineStyleDB2(), new EngineStyleComposite());
    }

    /** The places a name is written, each one statement around {@code name}. */
    static Map<String, SqlSelect> places(String name) {
        Map<String, SqlSelect> out = new LinkedHashMap<>();
        var t = new SqlSource.Table("T", "t0", List.of(ID), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        out.put("an alias", new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", ID), name, null)),
                false, t, null, List.of(), null, null, List.of(), null, null, List.of(ID)));
        for (OutputCol.Origin origin : List.of(OutputCol.Origin.DERIVED, OutputCol.Origin.PHYSICAL)) {
            OutputCol col = new OutputCol(name, SqlType.Scalar.INTEGER, false, origin);
            var source = new SqlSource.Table("T", "t0", List.of(col), false, Map.of(), com.legend.sql.NameQuoting.BARE);
            out.put("a " + origin.name().toLowerCase(java.util.Locale.ROOT) + " column",
                    new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", col), "c", null)), false,
                            source, null, List.of(), null, null, List.of(), null, null, List.of(col)));
        }
        var named = new SqlSource.Table(name, "t0", List.of(ID), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        out.put("a table", new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", ID), "c", null)),
                false, named, null, List.of(), null, null, List.of(), null, null, List.of(ID)));
        return out;
    }

    /** {@code statement} rendered by {@code dialect}, or its refusal by name. */
    static String rendered(SqlDialect dialect, SqlSelect statement) {
        try {
            return dialect.render(statement);
        } catch (RuntimeException refused) {
            return "REFUSED " + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    /** One line per dialect, place and ordinary name: what the golden holds. */
    static List<String> ordinaryLines() {
        List<SqlDialect> all = new ArrayList<>(dialects().keySet());
        all.addAll(engineText());
        List<String> out = new ArrayList<>();
        for (SqlDialect dialect : all) {
            for (String name : ORDINARY) {
                places(name).forEach((place, statement) -> out.add(dialect.getClass().getSimpleName() + "\t" + place
                        + "\t" + escape(name) + "\t" + escape(rendered(dialect, statement))));
            }
        }
        return out;
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n");
    }

    /** The golden's lines the check changed, each the defect it fixed (2026-11-13): H2's stored-name writer wrapped a
     *  name that is no plain identifier in quotes as given, so a quote inside it ended the identifier; it is delimited
     *  now, its quote doubled. Key: dialect, place and name as the golden writes them; value: what was, what is. */
    static final Map<String, List<String>> FIXED = Map.of("H2\ta physical column\ta\"b", List.of(
            "SELECT \"t0\".\"a\"b\" AS \"c\"\\nFROM T AS \"t0\"",
            "SELECT \"t0\".\"a\"\"b\" AS \"c\"\\nFROM T AS \"t0\""));

    @Test
    void anOrdinaryNameIsWrittenByteForByteAsBeforeOnEveryDialect() throws IOException {
        List<String> golden;
        try (InputStream in = PreQuotedNamesTest.class.getResourceAsStream("pre-quoted-names.tsv")) {
            assertTrue(in != null, "the golden pre-quoted-names.tsv is missing");
            golden = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList();
        }
        List<String> now = ordinaryLines();
        List<String> differ = new ArrayList<>();
        int fixed = 0;
        for (int i = 0; i < Math.max(golden.size(), now.size()); i++) {
            String was = i < golden.size() ? golden.get(i) : "(none)";
            String is = i < now.size() ? now.get(i) : "(none)";
            String key = was.substring(0, Math.max(0, was.lastIndexOf('\t')));
            List<String> change = FIXED.get(key);
            if (change != null) {
                // the defect was in the golden, and is gone now
                assertEquals(key + "\t" + change.get(0), was, "the golden holds the defect FIXED names");
                assertEquals(key + "\t" + change.get(1), is, "the defect FIXED names is fixed");
                fixed++;
            } else if (!was.equals(is)) {
                differ.add("was " + was + "\n is " + is);
            }
        }
        assertEquals(FIXED.size(), fixed, "each FIXED line is in the golden");
        assertEquals(List.of(), differ, golden.size() + " lines in the golden, " + now.size() + " now");
    }

    @Test
    void aHostileNameThatStartsAndEndsWithAQuoteStaysOneIdentifierOnEveryDialectAtEveryPlace() {
        List<String> wrong = new ArrayList<>();
        int written = 0;
        for (Map.Entry<SqlDialect, InjectionFuzzTest.Rules> d : dialects().entrySet()) {
            for (String name : HOSTILE) {
                for (Map.Entry<String, SqlSelect> place : places(name).entrySet()) {
                    String sql = d.getKey().render(place.getValue());
                    written++;
                    String finding = InjectionFuzzTest.finding(sql, d.getValue());
                    if (finding != null) {
                        wrong.add(d.getKey().getClass().getSimpleName() + ", " + place.getKey() + ", " + name + ": "
                                + finding + ": " + sql.replace("\n", " "));
                    }
                }
            }
        }
        // every dialect, every hostile name, every place: written (none refused), none left open or walked out of
        assertEquals(dialects().size() * HOSTILE.size() * 4, written);
        assertEquals(List.of(), wrong, String.join("\n", wrong));
    }

    /** The one check: a delimited name's interior holds its quote only doubled. */
    @Test
    void theCheckReadsAQuotedNameAsItsLexerDoes() {
        assertTrue(AnsiSqlRenderer.quotedIdentifier("\"date\"", '"'));
        assertTrue(AnsiSqlRenderer.quotedIdentifier("\"a\"\"b\"", '"'));
        assertTrue(AnsiSqlRenderer.quotedIdentifier("\"\"\"\"", '"'));
        assertTrue(AnsiSqlRenderer.quotedIdentifier("`a``b`", '`'));
        assertEquals(false, AnsiSqlRenderer.quotedIdentifier("\"a\" , x AS \"b\"", '"'));
        assertTrue(AnsiSqlRenderer.quotedIdentifier("\"a\"\"\"", '"'), "a doubled quote, then the closing one: a\"");
        assertEquals(false, AnsiSqlRenderer.quotedIdentifier("\"a\"\"", '"'), "a lone quote at the end: left open");
        assertEquals(false, AnsiSqlRenderer.quotedIdentifier("\"\"\"", '"'));
        assertEquals(false, AnsiSqlRenderer.quotedIdentifier("\"", '"'));
        assertEquals(false, AnsiSqlRenderer.quotedIdentifier("date", '"'));
        assertEquals(false, AnsiSqlRenderer.quotedIdentifier("\"date\"", '`'));
    }

    /** The audit's statement on H2 (2026-11-13: an alias passed through by {@code aliasIdent}, a FILE_READ after it):
     *  carried as data. Rendered, then run on an in-memory H2: one column, labelled exactly the name -- the file's
     *  read is a name's characters, never a call. */
    @Test
    void theAuditsFileReadOnH2IsOneColumnsLabelNeverACall() throws Exception {
        String name = "\"a\", FILE_READ('/nonexistent/witness') AS \"b\"";
        var select = SqlSelect.starOf(new SqlSource.Dual()).withProjections(
                List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), name, null)));
        String sql = new H2().render(select);
        assertEquals(null, InjectionFuzzTest.finding(sql.replace("FILE_READ", InjectionFuzzTest.MARK),
                InjectionFuzzTest.rules(DatabaseType.H2)), sql);
        try (var c = java.sql.DriverManager.getConnection("jdbc:h2:mem:prequoted");
                var s = c.createStatement(); var rs = s.executeQuery(sql)) {
            assertEquals(1, rs.getMetaData().getColumnCount(), sql);
            assertEquals(name, rs.getMetaData().getColumnLabel(1), sql);
        }
    }
}
