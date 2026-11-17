package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.edge.PureV1Routes;
import com.legend.json.Json;
import com.legend.server.LiteV1Api;
import org.junit.jupiter.api.Test;
import planner.Boundary;

/**
 * D-1'S FOUR LITE-OWNED EXPORTS (docs/COMPATIBILITY_END_TO_END_2026_11_12.md §5.1; F's separation census line A1,
 * 2026-11-17): the operations the tab and Python asked through legend-engine's {@code pure/v1} --
 * {@code grammar/grammarToJson/model}, {@code grammar/grammarToJson/lambda}, {@code grammar/jsonToGrammar/lambda} and
 * {@code compilation/lambdaRelationType} -- answered by the product's boundary with no edge class, each answer
 * byte-identical to the route's (no answer changes), and a refusal still a refusal.
 */
class BoundaryLiteExportsTest {

    private static final String MODEL = """
            ###Pure
            Class t::P { name: String[1]; age: Integer[1]; }
            ###Relational
            Database t::DB ( Table PERSON ( NAME VARCHAR(200), AGE INTEGER ) )
            ###Mapping
            Mapping t::M ( t::P: Relational { ~mainTable [t::DB] PERSON name: [t::DB] PERSON.NAME, age: [t::DB] PERSON.AGE } )
            """;
    private static final String LAMBDA = "|t::P.all()->project([p|$p.name, p|$p.age], ['name', 'age'])";

    private static String ok(LiteV1Api.Answer a) {
        assertEquals(200, a.status(), () -> "the route refused: " + a.json());
        return a.json();
    }

    @Test
    void modelElementsIsTheRoutesAnswer() {
        for (boolean source : new boolean[] {true, false}) {
            assertEquals(ok(PureV1Routes.grammarToJsonModel(MODEL, source)), Boundary.modelElements(MODEL, source));
        }
    }

    @Test
    void parseLambdaIsTheRoutesAnswer() {
        for (String text : new String[] {LAMBDA, "1 + 2", "|'a'->toUpper()"}) {
            for (boolean source : new boolean[] {true, false}) {
                assertEquals(ok(PureV1Routes.grammarToJsonLambda(text, source)), Boundary.parseLambda(text, source));
            }
        }
    }

    @Test
    void printLambdaIsTheRoutesAnswer() {
        String json = Boundary.parseLambda(LAMBDA, false);
        for (String style : new String[] {"PRETTY", "STANDARD"}) {
            assertEquals(ok(PureV1Routes.jsonToGrammarLambda(json, style)), Boundary.printLambda(json, style));
        }
        assertThrows(IllegalArgumentException.class, () -> Boundary.printLambda(json, "PRETTY_HTML"));
    }

    @Test
    void lambdaRelationTypeIsTheRoutesAnswer() {
        String lambda = Boundary.parseLambda(LAMBDA, false);
        String body = "{\"model\":{\"_type\":\"text\",\"code\":\"" + Json.escape(MODEL) + "\"},\"lambda\":" + lambda + "}";
        assertEquals(ok(PureV1Routes.lambdaRelationType(body)), Boundary.lambdaRelationType(MODEL, lambda));
    }

    @Test
    void aRefusalIsStillARefusal() {
        assertEquals(400, PureV1Routes.grammarToJsonLambda("|$", false).status());
        assertThrows(RuntimeException.class, () -> Boundary.parseLambda("|$", false));
        String unknown = Boundary.parseLambda("|t::Nope.all()", false);
        assertThrows(RuntimeException.class, () -> Boundary.lambdaRelationType(MODEL, unknown));
    }

    // ---- lite's own routes the tab asked through pure/v1's router (the edge audit's H2, 2026-11-17: three typed
    // ---- exports, the orchestrator's Q2), each answer byte-identical to the route's 200

    private static final LiteV1Api.Runner NO_RUN = (query, runtime, output) -> {
        throw new AssertionError("nothing is run here");
    };

    private static final String SOURCE = """
            ###Relational
            Database local::DB ( Table trades ( trade_id INTEGER NOT NULL, notional DOUBLE ) )
            ###Connection
            RelationalDatabaseConnection local::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    @Test
    void modelActionIsTheRoutesAnswer() {
        String body = """
                {"project": {"_type": "text", "code": ""}, "package": "trades", "class": {"source": {"_type": "text",                 "code": %s}, "lambda": %s, "name": "Trade", "columns": {}, "where": {"from": "local::DB", "runtime":                 "local::RT", "as": "Browser"}}}""".formatted(Json.toCompact(SOURCE), Boundary.parseLambda(
                "|#>{local::DB.trades}#", false));
        assertEquals(ok(LiteV1Api.route("/api/lite/v1/model/createClass", null, body, NO_RUN)),
                Boundary.modelAction("createClass", body));
        assertThrows(IllegalArgumentException.class, () -> Boundary.modelAction("dropClass", body));
    }

    @Test
    void tsItemIsTheRoutesAnswer() {
        for (String text : new String[] {"m = avg(CLOSE, 3)", "a = b", "= nothing"}) {
            assertEquals(ok(LiteV1Api.route("/api/lite/v1/grammar/tsItem", null,
                    "{\"text\":" + Json.toCompact(text) + "}", NO_RUN)), Boundary.tsItem(text), text);
        }
    }

    @Test
    void jsonToTsIsTheRoutesAnswer() {
        String model = """
                ###Relational
                Database ts::DB ( Table PRICES ( SYMBOL VARCHAR(10), DAY DATE, CLOSE DOUBLE ) )
                """;
        for (String text : new String[] {
                "|let px = #>{ts::DB.PRICES}#; #TS{SELECT avg(CLOSE, 3) AS m FROM $px ON DAY BY SYMBOL}#;",
                "|let px = #>{ts::DB.PRICES}#->extend(~a: r|$r.CLOSE->toOne()->abs()); #TS{SELECT avg(CLOSE, 3) AS m"
                        + " FROM $px ON DAY BY SYMBOL}#;"}) {
            // the tile's query read as lite reads it (its #TS island lite's own; upstream's grammar has none), then sent
            // as JSON, as the tile sends it (TsIslandTest.lambdaOf)
            String lambda = com.legend.protocol.ProtocolEmitter.emitLambda(
                    (com.legend.protocol.spec.LambdaFunction) com.legend.Compiler.parseQuery(text));
            String body = "{\"model\":{\"_type\":\"text\",\"code\":" + Json.toCompact(model) + "},\"lambda\":" + lambda + "}";
            assertEquals(ok(LiteV1Api.route("/api/lite/v1/grammar/jsonToTs/lambda", null, body, NO_RUN)),
                    Boundary.jsonToTs(lambda, model), text);
        }
    }

    @Test
    void compileErrorsIsTheRoutesAnswer() {
        for (String model : new String[] {MODEL, "Class demo::Desk\n{\n  name: Strin[1];\n}\n",
                "function demo::f(): String[1] { 1 }\nfunction demo::g(): String[1] { 2 }\n"}) {
            String body = "{\"_type\":\"text\",\"code\":" + Json.toCompact(model) + "}";
            assertEquals(ok(LiteV1Api.route("/api/lite/v1/compilation/compile", null, body, NO_RUN)),
                    Boundary.compileErrors(model), model);
        }
    }

    /** A REFUSAL'S BYTES, FOLDED FOR A HOST (B7, 2026-11-17: the orchestrator's condition on the split -- lite's own
     *  refusal kinds, {@link LiteV1Api.RefusalKind}, write the bytes the tab and Python have read since leg 6, unmoved):
     *  a text that does not parse, a model that does not compile, any other failure. */
    @Test
    void aFoldedRefusalsBytesAreUnmoved() {
        RuntimeException parse = assertThrows(RuntimeException.class, () -> com.legend.Compiler.parseQuery("1 +"));
        assertEquals("ERR\nPARSER\n" + parse.getMessage(), planner.Folded.failure(parse));
        RuntimeException compile = assertThrows(RuntimeException.class,
                () -> com.legend.Compiler.compileModel("Class demo::Desk\n{\n  name: Strin[1];\n}\n"));
        assertEquals("ERR\nCOMPILATION\n" + compile.getMessage(), planner.Folded.failure(compile));
        assertEquals("ERR\n500\nIllegalArgumentException: no",
                planner.Folded.failure(new IllegalArgumentException("no")));
    }
}
