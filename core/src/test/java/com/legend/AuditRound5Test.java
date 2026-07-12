package com.legend;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins for the fifth audit (post-6d7c3169 range: PCT slices 6–18b). Every
 * fix unwound a silent-wrongness or value-consulting shape:
 *
 * <ul>
 *   <li>regexpIndexOf located matches LEXICALLY (strpos of the match text) —
 *       anchored/repeated patterns returned the wrong position;</li>
 *   <li>Executor guessed value KINDS from fetched values (integral-double →
 *       Long, scale-0 decimal → Long) — both heuristics were dead code once
 *       the SQL identity channel landed, and are deleted;</li>
 *   <li>the group-hash shift constant overflowed BIGINT at hash 2^64-1;</li>
 *   <li>date(y,m,...) validated LITERAL components only — runtime components
 *       leaked DuckDB's message instead of pure's;</li>
 *   <li>formatDate nano-precision fabricated '000' for literals whose
 *       written digits the TIMESTAMP carrier had truncated;</li>
 *   <li>enum-vs-Any equality lowered to a static FALSE (Any is UNDECIDED,
 *       not disjoint);</li>
 *   <li>copy-with-update skipped multiplicity subsumption and emitted a
 *       struct for the bare-array List carrier;</li>
 *   <li>keptDedup's fixed lambda names captured under nesting.</li>
 * </ul>
 */
class AuditRound5Test {

    private static final String MODEL = """
            Enum test::GT { CITY, TOWN }
            Class test::P { name: String[1]; nick: String[*]; }
            Database test::DB
            (
              Table T (X INTEGER NOT NULL)
            )
            """;

    private static Object scalar(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            return Compiler.execute(MODEL, query, c).rows().get(0).get(0);
        }
    }

    // ---- regexpIndexOf is POSITIONAL (the regex engine measures the prefix) ----

    @Test
    @DisplayName("audit: regexpIndexOf of an anchored pattern is the MATCH position, not the first lexical occurrence")
    void regexpIndexOfIsPositional() throws Exception {
        assertEquals(3L, scalar("|'ab ab'->regexpIndexOf('ab$')"));
        assertEquals(3L, scalar("|'It was the best'->regexpIndexOf('was')"));
        assertEquals(-1L, scalar("|'xyz'->regexpIndexOf('was')"));
    }

    @Test
    @DisplayName("audit: regexpIndexOf group position splits the literal pattern at the group's paren")
    void regexpIndexOfGroupIsPositional() throws Exception {
        assertEquals(3L, scalar("|'abc123def'->regexpIndexOf('([a-z]+)([0-9]+)', 2)"));
        assertEquals(0L, scalar("|'abc123def'->regexpIndexOf('([a-z]+)([0-9]+)')"));
    }

    // ---- date() component guards fire for RUNTIME components too ----

    @Test
    @DisplayName("audit: a runtime out-of-range month raises pure's message, not DuckDB's")
    void runtimeDateComponentGuard() {
        Exception e = assertThrows(Exception.class,
                () -> scalar("|date(2016, [13]->at(0))"));
        assertTrue(e.getMessage().contains("Invalid month: 13"), e.getMessage());
    }

    @Test
    @DisplayName("audit: fractional seconds are legal up to (not including) 60")
    void fractionalSecondsStayLegal() throws Exception {
        assertEquals("2016-01-01T00:00:59.999",
                String.valueOf(scalar("|date(2016, 1, 1, 0, 0, 59.999)")));
    }

    // ---- kind recovery: self-describing encodings only, never value guesses ----

    @Test
    @DisplayName("audit: a computed Float that happens to be integral keeps its Float kind")
    void computedIntegralFloatKeepsKind() throws Exception {
        // average is a COMPUTING root: 4.0 must stay a Double, and even
        // element-selecting roots no longer guess kinds from values —
        // the identity channel carries kind from SQL
        Object v = scalar("|[3.0, 5.0]->average()");
        assertEquals(Double.class, v.getClass());
        assertEquals(4.0, (Double) v);
    }

    // ---- copy-with-update: construction-grade validation, carrier-true lowering ----

    @Test
    @DisplayName("audit: ^$var(...) validates multiplicity subsumption like construction")
    void copyValidatesMultiplicity() {
        Exception e = assertThrows(Exception.class, () -> Compiler.compileQuery(MODEL,
                "|let p = ^test::P(name='a'); ^$p(name = ['x', 'y']);"));
        assertTrue(e.getMessage().contains("multiplicity"), e.getMessage());
    }

    @Test
    @DisplayName("audit: copy of a List carrier stays a bare array (values override replaces wholesale)")
    void copyOfListStaysBareArray() throws Exception {
        assertEquals("[x, y]",
                String.valueOf(scalar("|let l = ^List<String>(values=['a']); ^$l(values=['x','y'])->toString();")));
    }

    // ---- keptDedup: nested comparators do not capture accumulator names ----

    @Test
    @DisplayName("audit: nested removeDuplicates comparators execute (capture-freshened accumulators)")
    void nestedDedupComparatorsExecute() throws Exception {
        assertEquals(1L, scalar(
                "|[1,2,2,1,2,2,3]->removeDuplicates({a, b | "
                        + "[$a]->removeDuplicates({x,y|$x==$y})->size() == "
                        + "[$b]->removeDuplicates({x,y|$x==$y})->size()})->size()"));
    }

    // ---- enum-vs-Any equality is undecided (never a silent static FALSE) ----

    @Test
    @DisplayName("audit: enum-vs-Any equality is not constant-folded to false")
    void enumVsAnyIsNotStaticFalse() throws Exception {
        // decidable mismatches stay static FALSE...
        assertEquals(false, scalar("|test::GT.CITY == 1"));
        // ...but an Any operand is undecided: the lowering must not fold it
        // (LOUD or genuinely compared are both acceptable; false-by-fiat is not)
        String sql = Compiler.compile(MODEL, "|[test::GT.CITY, 'x']->first() == test::GT.CITY", "n/a");
        assertTrue(!sql.contains("SELECT FALSE") && !sql.contains("SELECT false"),
                "enum-vs-Any must not constant-fold: " + sql);
    }
}
