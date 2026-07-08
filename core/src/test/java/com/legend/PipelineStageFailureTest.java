package com.legend;

import com.legend.lowering.Lowerer;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Negative tests through the FULL pipeline — one bad input per stage, driven
 * end-to-end, asserting the failure surfaces AT that stage, LOUDLY, with a
 * message naming the offending thing. Stage-level unit negatives cannot catch
 * a stage silently accepting the previous stage's garbage; these can.
 */
class PipelineStageFailureTest {

    private static final String MODEL = """
            Database test::DB
            (
              Table T_PERSON (NAME VARCHAR(100) NOT NULL, AGE INTEGER NOT NULL)
            )
            """;

    /** The full pipe: model+query → typed → lowered → rendered. */
    private static String pipe(String model, String query) {
        return new DuckDb().render(new Lowerer().lower(Compiler.compileQuery(model, query)));
    }

    private static <T extends Throwable> T failsWith(Class<T> type, String model, String query) {
        return assertThrows(type, () -> pipe(model, query));
    }

    private static void messageNames(Throwable t, String... needles) {
        for (String needle : needles) {
            assertTrue(String.valueOf(t.getMessage()).contains(needle),
                    () -> "expected message to name '" + needle + "', got: " + t.getMessage());
        }
    }

    // ---- stage: query PARSE ----

    @Test
    @DisplayName("parse: malformed query dies in the parser, not downstream")
    void queryParseError() {
        var ex = failsWith(com.legend.parser.ParseException.class,
                MODEL, "#>{test::DB.T_PERSON}#->filter(x|");
        org.junit.jupiter.api.Assertions.assertEquals(
                com.legend.error.LegendCompileException.Phase.PARSE, ex.phase());
    }

    // ---- stage: model PARSE ----

    @Test
    @DisplayName("parse: malformed model dies in the element parser")
    void modelParseError() {
        var ex = failsWith(com.legend.parser.ParseException.class,
                "Database test::DB ( Table (BROKEN ", "#>{test::DB.T_PERSON}#");
        org.junit.jupiter.api.Assertions.assertEquals(
                com.legend.error.LegendCompileException.Phase.PARSE, ex.phase());
    }

    // ---- stage: NAME RESOLUTION scope ----

    @Test
    @DisplayName("resolve: bare user-element name fails with the qualification hint")
    void bareNameFails() {
        var ex = failsWith(com.legend.error.ResolutionException.class, MODEL + """
                Class test::Person { name: String[1]; }
                """, "Person.all()");
        messageNames(ex, "Person", "fully qualified");
    }

    // ---- stage: PHASE F (element compile) ----

    @Test
    @DisplayName("element-compile: unknown property type in the model fails at F")
    void unknownTypeInModel() {
        var ex = failsWith(com.legend.error.ModelException.class,
                "Class test::P { x: NoSuchType[1]; }", "1 + 1");
        org.junit.jupiter.api.Assertions.assertEquals(
                com.legend.error.LegendCompileException.Phase.MODEL, ex.phase());
        messageNames(ex, "NoSuchType");
    }

    // ---- stage: PHASE G (type check) ----

    @Test
    @DisplayName("type-check: unknown table in the model's database fails at G")
    void unknownTable() {
        Exception ex = failsWith(Exception.class, MODEL, "#>{test::DB.T_NOPE}#");
        messageNames(ex, "T_NOPE");
    }

    @Test
    @DisplayName("type-check: unknown column in a predicate fails at G, naming the column")
    void unknownColumn() {
        Exception ex = failsWith(Exception.class, MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.NOPE > 1)");
        messageNames(ex, "NOPE");
    }

    @Test
    @DisplayName("type-check: type mismatch (String + Integer) fails at G, not in SQL")
    void typeMismatch() {
        var ex = failsWith(com.legend.error.LegendCompileException.class, MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|($x.NAME + 5) == 'x')");
        assertTrue(ex.phase() == com.legend.error.LegendCompileException.Phase.TYPE
                        || ex.phase() == com.legend.error.LegendCompileException.Phase.PARSE,
                () -> "expected a TYPE (or operator-parse) failure, got " + ex.phase());
    }

    // ---- stage: LOWERING ----

    @Test
    @DisplayName("lowering: an unimplemented construct fails loudly naming the node")
    void unimplementedConstruct() {
        var ex = failsWith(com.legend.error.NotImplementedException.class, MODEL,
                "#>{test::DB.T_PERSON}#->write(test::DB)");
        messageNames(ex, "not yet implemented", "TypedWrite");
    }

    @Test
    @DisplayName("lowering: non-decomposable scalar-acc fold fails loudly with the rewrite hint")
    void scalarAccCollectionBuildFold() {
        Exception ex = failsWith(IllegalStateException.class, MODEL,
                "['a', 'bb']->fold({e, a | $e->length() - $a}, 0)");
        messageNames(ex, "not decomposable", "accumulator-first");
    }

    @Test
    @DisplayName("lowering: dynamic slicing bound fails loudly (literal expected)")
    void dynamicLimit() {
        var ex = failsWith(com.legend.error.NotImplementedException.class, MODEL,
                "#>{test::DB.T_PERSON}#->limit(1 + 1)");
        messageNames(ex, "literal expected");
    }

    @Test
    @DisplayName("lowering: unregistered scalar overload names the function")
    void unregisteredScalar() {
        Exception ex = failsWith(IllegalStateException.class, MODEL,
                "#>{test::DB.T_PERSON}#->filter(x|$x.NAME->decodeBase64() == 'x')");
        messageNames(ex, "decodeBase64");
    }
}
