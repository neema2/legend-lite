package com.legend.lowering;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A column whose name needs quoting is usable EVERYWHERE, not just
 * where the resolution happens to go through a typed relation.
 *
 * <p>A relational identifier keeps its quotes as the wire name
 * ({@code DatabaseProtocolParser.parseIdentifier}, matching the
 * engine's protocol), so a Database declaring {@code "total pnl"}
 * stamps an OutputCol named {@code "total pnl"} — quotes included —
 * while every Pure reference to it is bare. {@code Fold} compared
 * the two with {@code equals}.
 *
 * <p>The miss hid for a long time because the common paths do not
 * come through that compare: {@code select} resolves by typed
 * relation and {@code filter} by property path, so both always
 * worked. A SORT KEY and a GROUPBY KEY resolve by name, and both
 * failed with "cannot be resolved after isolation" — for any name
 * needing quotes, so a space or a comma was enough. It surfaced in a
 * DataCube stress run over non-ASCII headers and looked like a
 * unicode problem; it was not.
 */
class QuotedColumnNameTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB
            (
                Table t
                (
                    "total pnl" DOUBLE,
                    "x,y" VARCHAR(32),
                    "a\\"b" VARCHAR(32),
                    plain VARCHAR(32)
                )
            )

            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }

            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static String sql(String query) {
        return com.legend.Compiler.plan(MODEL, query, "local::RT").sql();
    }

    @Test
    void sortsByAColumnWhoseNameNeedsQuoting() {
        String s = sql("#>{local::DB.t}#->select(~['total pnl', plain])"
                + "->sort([~'total pnl'->ascending()])");
        assertTrue(s.contains("ORDER BY"), s);
        assertTrue(s.contains("\"total pnl\""), s);
    }

    @Test
    void groupsByAColumnWhoseNameNeedsQuoting() {
        String s = sql("#>{local::DB.t}#->groupBy(~['total pnl'], "
                + "~[m:x|$x.plain:y|$y->count()])");
        assertTrue(s.contains("GROUP BY"), s);
        assertTrue(s.contains("\"total pnl\""), s);
    }

    @Test
    void handlesACommaInTheName() {
        // A comma is the case that made this look like a CSV problem
        // rather than an identifier problem.
        String s = sql("#>{local::DB.t}#->groupBy(~['x,y'], "
                + "~[m:x|$x.'total pnl':y|$y->sum()])");
        assertTrue(s.contains("GROUP BY"), s);
        assertTrue(s.contains("\"x,y\""), s);
    }

    @Test
    void handlesAQuoteInTheName() {
        // The lexer's escape inside a quoted identifier is a
        // BACKSLASH, and it ends the token at the first unescaped
        // quote — so the wire name carries the backslash and the
        // comparison has to decode it.
        String s = sql("#>{local::DB.t}#->sort([~'a\"b'->ascending()])");
        assertTrue(s.contains("ORDER BY"), s);
    }

    @Test
    void stillResolvesPlainNames() {
        String s = sql("#>{local::DB.t}#->select(~[plain])"
                + "->sort([~plain->ascending()])");
        assertTrue(s.contains("ORDER BY"), s);
        // The bare-name fallback must not start quoting what never
        // needed it.
        assertTrue(s.contains("plain"), s);
        assertTrue(!s.contains("\"plain\""), s);
    }

    @Test
    void stillRefusesAColumnThatDoesNotExist() {
        // The fallback widens what MATCHES; it must not turn a
        // genuine miss into a silent pass.
        assertThrows(RuntimeException.class,
                () -> sql("#>{local::DB.t}#->sort([~'no such column'"
                        + "->ascending()])"));
    }
}
