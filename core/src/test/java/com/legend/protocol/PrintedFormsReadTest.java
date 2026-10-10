// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What legend-engine's printer writes, lite reads back (the protocol program's leg 5: its round-trip proof found each
 * of these as a print lite could not parse). Each form is the engine's own -- read as the engine's grammar reads it --
 * and gives the JSON its written form gives.
 */
class PrintedFormsReadTest {

    private static final Json.Config DEEP = new Json.Config(4096);

    private static String json(String text) {
        return PmcdParser.parseDocument(text, false);
    }

    private static Json.Obj element(String text, int i) {
        return (Json.Obj) ((Json.Obj) Json.parse(json(text), DEEP)).getArr("elements").items().get(i);
    }

    /** A function's body statements; each text is written whole, so the own corpus compares it with the engine's. */
    private static List<Json.Node> body(String function) {
        return element(function, 0).getArr("body").items();
    }

    private static int lambdaStatements(Json.Node statement) {
        Json.Obj o = (Json.Obj) statement;
        Json.Obj lambda = "lambda".equals(o.getString("_type")) ? o : (Json.Obj) o.getArr("parameters").items().get(1);
        return lambda.getArr("body").items().size();
    }

    @Test
    void aBracelessLambda_inAStatement_leavesItsSemicolonOnlyToASequenceThatNeedsIt() {
        // the engine prints {|...} as |...; a later statement of a sequence ends with a required ';', which the lambda
        // leaves: the print reads back as written
        String written = """
                function demo::f(): Any[*]
                {
                  let a = 1;
                  let q = {|1 + 1};
                  let r = $q->eval();
                  $r;
                }
                """;
        String printed = written.replace("{|1 + 1}", "|1 + 1");
        assertEquals(json(written), json(printed));
        assertEquals(4, body(printed).size());
        assertEquals(2, body("function demo::f(): Any[*]\n{\n  let a = 1; let f = |1;\n}\n").size(),
                "the ';' before the block's end is the block's");
        List<Json.Node> later = body("function demo::f(): Any[*]\n{\n  let a = 1; |1; 2;\n}\n");
        assertEquals(3, later.size());
        assertEquals(1, lambdaStatements(later.get(1)));
        // a FIRST statement's ';' is optional: ANTLR takes the lambda's reading, and with it the statements after
        // (legend-engine 4.145.0 probed; so the engine's own print of a first-statement {|...} does not read back)
        List<Json.Node> first = body("function demo::f(): Any[*]\n{\n  let q = |1 + 1; let r = $q->eval(); $r;\n}\n");
        assertEquals(1, first.size());
        assertEquals(3, lambdaStatements(first.get(0)));
        assertEquals(2, lambdaStatements(body("function demo::f(): Any[*]\n{\n  |1; 2;\n}\n").get(0)));
        // inside a bracket nothing outside can own the ';', even in a later statement: the block reads on
        assertEquals(2, body("function demo::f(): Any[*]\n{\n  let a = 1; [1, 2]->map(x|let y = $x + 1; $y * 2;);\n}\n")
                .size());
        List<Json.Node> braced = body("function demo::f(): Any[*]\n{\n  {|let a = 1; |1;};\n}\n");
        assertEquals(2, lambdaStatements(braced.get(0)), "a braced body's later statement: its lambda leaves the ';'");
    }

    @Test
    void aMergeMappingsValidation_bracelessAsPrinted_isTheBracedOne() {
        String written = """
                ###Mapping
                Mapping demo::M
                (
                  *demo::P: Operation
                  {
                    meta::pure::router::operations::merge_OperationSetImplementation_1__SetImplementation_MANY_([a,b],{|true})
                  }
                )
                """;
        assertEquals(json(written), json(written.replace("{|true}", "|true")));
    }

    @Test
    void aServicesPostValidationAssertions_asPrinted_areAListSeparatedByCommas() {
        String printed = """
                ###Service
                Service demo::S
                {
                  pattern: '/s';
                  documentation: '';
                  autoActivateUpdates: true;
                  execution: Single
                  {
                    query: |1;
                    mapping: demo::M;
                    runtime: demo::R;
                  }
                  postValidations:
                  [
                    {
                      description: 'd';
                      params:[
                        |'x'
                      ];
                      assertions:[
                        first: tds: meta::pure::tds::TabularDataSet[1]|true,
                        second: tds: meta::pure::tds::TabularDataSet[1]|false
                      ];
                    }
                  ]
                }
                """;
        List<Json.Node> assertions = ((Json.Obj) element(printed, 0).getArr("postValidations").items().get(0))
                .getArr("assertions").items();
        assertEquals(2, assertions.size());
        assertEquals("second", ((Json.Obj) assertions.get(1)).getString("id"));
        // a ';' after an assertion is legal only as a brace-less lambda's own (its code block takes it)
        String withSemicolons = printed.replace("|true,", "|true;,").replace("|false\n", "|false;\n");
        assertEquals(json(printed), json(withSemicolons));
        String notALambda = printed.replace("first: tds: meta::pure::tds::TabularDataSet[1]|true,", "first: 1,");
        json(notALambda);   // the control: a value that is no lambda reads, without the ';'
        assertThrows(RuntimeException.class, () -> json(notALambda.replace("first: 1,", "first: 1;,")));
    }

    @Test
    void aFloatLiteralsSuffix_isRead_whereverTheGrammarTakesAFloat() {
        // the engine's grammar writes f/F on a FLOAT (CoreFragmentGrammar) and reads it with Double.parseDouble
        String database = """
                ###Relational
                Database demo::DB
                (
                  Table T
                  (
                    A DOUBLE
                  )
                  Filter F(T.A = 1.5f)
                  Filter G(T.A = -2.5F)
                )
                """;
        assertTrue(json(database).contains("\"value\":1.5}") && json(database).contains("\"value\":-2.5}"),
                json(database));
        String service = """
                ###Service
                Service demo::S
                {
                  pattern: '/s';
                  documentation: '';
                  autoActivateUpdates: true;
                  execution: Single
                  {
                    query: x: Float[1]|$x;
                    mapping: demo::M;
                    runtime: demo::R;
                  }
                  test: Single
                  {
                    data: 'test';
                    asserts:
                    [
                      { [-1.5f], res: Result<Any|*>[1]|true }
                    ];
                  }
                }
                """;
        // a legacy test's parameter keeps its span without source information, as the engine's does (withoutSpans)
        assertTrue(json(service).contains("\"value\":-1.5}]"), json(service));
        String dataQuality = """
                ###DataQualityValidation
                DataQualityRelationComparison demo::Recon
                {
                   source: src|#>{demo::DB.T}#;
                   target: tgt|#>{demo::DB.T}#;
                   keys: [A];
                   columnsToCompare: [A];
                   strategy: MD5Hash
                   {
                     sourceHashColumn: srcHash;
                     targetHashColumn: tgtHash;
                     aggregatedHash: true;
                   };
                   expectedMatch: 0.99f;
                }
                """;
        assertTrue(json(dataQuality).contains("\"expectedMatch\":0.99"), json(dataQuality));
    }

    @Test
    void aFunctionTestsDocumentation_isRead_andPrintedBack() {
        String text = """
                function demo::f(): Integer[1]
                {
                  1
                }
                {
                  t1 'the doc' | f() => 1;
                }
                """;
        Json.Obj test = (Json.Obj) ((Json.Obj) element(text, 0).getArr("tests").items().get(0)).getArr("tests").items().get(0);
        assertEquals("the doc", test.getString("doc"));
        String printed = ModelComposer.model(json(text), PureComposer.Style.STANDARD);
        assertTrue(printed.contains("t1 'the doc' | f() => 1;"), printed);
        assertEquals(json(text), json(printed));
    }

    @Test
    void aPersistencesTests_takeNoSemicolonAfterTheList_asTheEnginesGrammar() {
        String persistence = """
                ###Persistence
                Persistence demo::P
                {
                  doc: 'd';
                  trigger: Manual;
                  service: demo::S;
                  tests:
                  [
                  ]
                }
                """;
        json(persistence);   // read
        assertThrows(RuntimeException.class, () -> json(persistence.replace("  ]\n}", "  ];\n}")));
    }

    @Test
    void aPathLiteralOverSeveralLines_asThePrettyPrinterBreaksIt_spansAsTheEngines() {
        // the island's first line shifted by the literal's column and length, a later line keeping its own columns,
        // the whole island ending on its end-of-input token (legend-engine 4.145.0's bytes, probed 2026-10-09)
        String printed = """
                function test(): Any[*]
                {
                  #/Person/nameWithPrefixAndSuffix('a', [
                    'a',
                    'b'
                  ])#->print(2)
                }
                """;
        String island = "{\"_type\":\"classInstance\",\"sourceInformation\":{\"endColumn\":9,\"endLine\":6,\"sourceId\":\"\","
                + "\"startColumn\":65,\"startLine\":3},\"type\":\"path\",\"value\":{\"path\":[{\"_type\":\"propertyPath\","
                + "\"parameters\":[{\"_type\":\"string\",\"sourceInformation\":{\"endColumn\":99,\"endLine\":3,\"sourceId\":\"\","
                + "\"startColumn\":97,\"startLine\":3},\"value\":\"a\"},{\"_type\":\"collection\",\"multiplicity\":"
                + "{\"lowerBound\":2,\"upperBound\":2},\"values\":[{\"_type\":\"string\",\"sourceInformation\":{\"endColumn\":7,"
                + "\"endLine\":4,\"sourceId\":\"\",\"startColumn\":5,\"startLine\":4},\"value\":\"a\"},{\"_type\":\"string\","
                + "\"sourceInformation\":{\"endColumn\":7,\"endLine\":5,\"sourceId\":\"\",\"startColumn\":5,\"startLine\":5},"
                + "\"value\":\"b\"}]}],\"property\":\"nameWithPrefixAndSuffix\",\"sourceInformation\":{\"endColumn\":4,"
                + "\"endLine\":6,\"sourceId\":\"\",\"startColumn\":72,\"startLine\":3}}],\"sourceInformation\":{\"endColumn\":9,"
                + "\"endLine\":6,\"sourceId\":\"\",\"startColumn\":65,\"startLine\":3},\"startType\":\"Person\"}}";
        String withSpans = PmcdParser.parseDocument(printed, true);
        assertTrue(withSpans.contains(island), withSpans);
        // a segment and its arguments begun on a later line
        String dated = """
                function test(): Any[*]
                {
                  let x = #/Person/firm(
                  %latest)/name(%2017-6-10)#;
                }
                """;
        String value = "{\"path\":[{\"_type\":\"propertyPath\",\"parameters\":[{\"_type\":\"latestDate\",\"sourceInformation\":"
                + "{\"endColumn\":9,\"endLine\":4,\"sourceId\":\"\",\"startColumn\":3,\"startLine\":4}}],\"property\":\"firm\","
                + "\"sourceInformation\":{\"endColumn\":10,\"endLine\":4,\"sourceId\":\"\",\"startColumn\":61,\"startLine\":3}},"
                + "{\"_type\":\"propertyPath\",\"parameters\":[{\"_type\":\"dateTime\",\"sourceInformation\":{\"endColumn\":26,"
                + "\"endLine\":4,\"sourceId\":\"\",\"startColumn\":17,\"startLine\":4},\"value\":\"2017-6-10\"}],\"property\":"
                + "\"name\",\"sourceInformation\":{\"endColumn\":27,\"endLine\":4,\"sourceId\":\"\",\"startColumn\":11,"
                + "\"startLine\":4}}],\"sourceInformation\":{\"endColumn\":32,\"endLine\":4,\"sourceId\":\"\",\"startColumn\":54,"
                + "\"startLine\":3},\"startType\":\"Person\"}";
        String datedSpans = PmcdParser.parseDocument(dated, true);
        assertTrue(datedSpans.contains(value), datedSpans);
        // the let, ending at the literal, ends on the literal's first line at its start column plus its raw length
        assertTrue(datedSpans.contains("{\"_type\":\"classInstance\",\"sourceInformation\":{\"endColumn\":53,\"endLine\":3,"
                + "\"sourceId\":\"\",\"startColumn\":3,\"startLine\":3},\"type\":\"path\""), datedSpans);
        // and without spans it prints back, as the round trip reads it
        assertEquals(json(printed), json(ModelComposer.model(json(printed), PureComposer.Style.PRETTY)));
    }
}
