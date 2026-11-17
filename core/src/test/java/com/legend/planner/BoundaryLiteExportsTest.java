package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.edge.PureV1Routes;
import com.legend.json.Json;
import com.legend.server.PureV1Api;
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

    private static String ok(PureV1Api.Answer a) {
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
}
