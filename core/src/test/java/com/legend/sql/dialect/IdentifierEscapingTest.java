// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * EVERY DIALECT'S IDENTIFIER IS ESCAPED AS ITS OWN LEXER READS ONE (the user's ruling of 2026-11-13, after the
 * ClickHouse breakout: a name ending in a backslash or holding {@code \"} walked out of ClickHouse's quoted identifier,
 * whose lexer reads backslash escapes there). A column aliased {@code a"b\c} -- a quote and a backslash -- is rendered by
 * each SQL dialect, the engine-text printers among them, and read back by that database's lexical rules
 * ({@link InjectionFuzzTest#rules}): one delimited identifier, spelled exactly as given, and nothing left open.
 */
class IdentifierEscapingTest {

    private static final String NAME = "a\"b\\c";

    private static final OutputCol ID = new OutputCol("ID", SqlType.Scalar.INTEGER, false, OutputCol.Origin.PHYSICAL);

    /** H2's rules, which its engine-text printers write for: a doubled quote, no backslash escape. */
    private static final InjectionFuzzTest.Rules H2_RULES =
            new InjectionFuzzTest.Rules("'", false, true, "\"", false, false, false, false);

    /** Each SQL dialect with the database whose lexer reads its statements. */
    private static Map<SqlDialect, InjectionFuzzTest.Rules> dialects() {
        Map<SqlDialect, InjectionFuzzTest.Rules> out = new LinkedHashMap<>();
        for (var type : com.legend.database.Databases.relational()) {
            out.put(com.legend.database.Databases.dialect(type), InjectionFuzzTest.rules(type));
        }
        out.put(new EngineStyleH2(), H2_RULES);
        out.put(new EngineStyleDB2(), H2_RULES);
        out.put(new EngineStyleComposite(), H2_RULES);
        return out;
    }

    /** Every delimited identifier of {@code s} under {@code r}, each as its database reads it. */
    static List<String> identifiers(String s, InjectionFuzzTest.Rules r) {
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (r.stringQuotes().indexOf(ch) >= 0 || r.identQuotes().indexOf(ch) >= 0) {
                boolean ident = r.identQuotes().indexOf(ch) >= 0;
                boolean backslash = ident ? r.identBackslash() : r.stringBackslash();
                boolean doubling = ident || r.stringDoubling();
                StringBuilder read = new StringBuilder();
                int k = i + 1;
                while (true) {
                    if (k >= s.length()) {
                        throw new AssertionError("left open at " + i + ": " + s);
                    }
                    char c = s.charAt(k);
                    if (backslash && c == '\\') {
                        read.append(s.charAt(k + 1));
                        k += 2;
                    } else if (c == ch && doubling && k + 1 < s.length() && s.charAt(k + 1) == ch) {
                        read.append(ch);
                        k += 2;
                    } else if (c == ch) {
                        break;
                    } else {
                        read.append(c);
                        k++;
                    }
                }
                if (ident) {
                    out.add(read.toString());
                }
                i = k + 1;
            } else {
                i++;
            }
        }
        return out;
    }

    /** The engine-text printer's own two spellings of a name (2026-11-13: until then written as given): a user TDS
     *  join's isolating wrap, each output quoted, and a view frame's projection, written bare where the name is a plain
     *  identifier (legend-engine's text) and delimited otherwise -- each one identifier, spelled exactly, on H2. */
    @Test
    void theEngineTextPrintersOwnSpellingsAreEachOneIdentifier() {
        OutputCol quoted = new OutputCol(NAME, SqlType.Scalar.INTEGER, false, OutputCol.Origin.PHYSICAL);
        var a = new SqlSource.Table("A", "a0", List.of(quoted), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        var b = new SqlSource.Table("B", "b0", List.of(ID), false, Map.of(), com.legend.sql.NameQuoting.BARE);
        var join = new SqlSource.Join(a, b, SqlSource.Join.Kind.CROSS, null, List.of(quoted, ID));
        var star = new SqlSelect(List.of(), false, join, null, List.of(), null, null, List.of(), null, null,
                List.of(quoted, ID));
        String joined = new EngineStyleH2().render(star);
        assertEquals(true, identifiers(joined, H2_RULES).contains(NAME), joined);

        String spaced = "x, 1 AS y";
        OutputCol view = new OutputCol(spaced, SqlType.Scalar.INTEGER, false, OutputCol.Origin.PHYSICAL);
        var inner = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", ID), spaced, null)), false,
                new SqlSource.Table("T", "t0", List.of(ID), false, Map.of(), com.legend.sql.NameQuoting.BARE), null,
                List.of(), null, null, List.of(), null, null, List.of(view));
        var outer = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("root", view), null, null)), false,
                new SqlSource.Subselect(inner, "root", "a_view"), null, List.of(), null, null, List.of(), null, null,
                List.of(view));
        String framed = new EngineStyleH2().render(outer);
        // the frame's alias and the outer read of it: the name twice, each one identifier
        assertEquals(2, identifiers(framed, H2_RULES).stream().filter(spaced::equals).count(), framed);
    }

    @Test
    void anAliasHoldingAQuoteAndABackslashIsOneIdentifierSpelledExactlyOnEveryDialect() {
        List<String> wrong = new ArrayList<>();
        java.util.Set<String> refusing = new java.util.TreeSet<>();
        dialects().forEach((dialect, rules) -> {
            var table = new SqlSource.Table("T", "t0", List.of(ID), false, Map.of(), com.legend.sql.NameQuoting.BARE);
            var select = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", ID), NAME, null)), false,
                    table, null, List.of(), null, null, List.of(), null, null, List.of(ID));
            String sql;
            try {
                sql = dialect.render(select);
            } catch (RuntimeException refused) {
                // refused BY NAME alone (the C4 audit's M3, 2026-11-13: until then any exception passed here)
                if (!InjectionFuzzTest.namedRefusal(refused)) {
                    wrong.add(dialect.getClass().getSimpleName() + ": " + InjectionFuzzTest.unnamed(refused));
                }
                refusing.add(dialect.getClass().getSimpleName());
                return;
            }
            try {
                if (!identifiers(sql, rules).contains(NAME)) {
                    wrong.add(dialect.getClass().getSimpleName() + ": no identifier spelled " + NAME + ": " + sql);
                }
            } catch (AssertionError open) {
                wrong.add(dialect.getClass().getSimpleName() + ": " + open.getMessage());
            }
        });
        assertEquals(List.of(), wrong);
        // the coverage, pinned: the dialects that refuse the name (none, measured 2026-11-13: the C4 audit's M3)
        assertEquals(java.util.Set.of(), refusing, "the dialects refusing the name");
    }
}
