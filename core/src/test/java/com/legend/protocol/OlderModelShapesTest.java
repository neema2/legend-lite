// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Model JSON that lite's grammar does not write but legend-engine 4.145.0 reads -- older layouts, and fields a person
 * or an older engine leaves out -- read as the engine reads it and printed as its printer prints it (the protocol
 * program's leg 2: the reader reads every field the engine reads; found by step 3's audits, PROTOCOL_PROGRAM §4.2).
 * Each case starts from a text lite parses and edits its JSON into the shape. A shape lite keeps is written back as
 * read ({@link #exact}); a shape brought up to today's form (SEMANTICS_REGISTER S30, S35) is written in that form
 * ({@link #broughtUp}); either way the printed text is what the engine prints, and what is written back prints the
 * same.
 */
class OlderModelShapesTest {

    private static final Json.Config DEEP = new Json.Config(4096);

    private static final String MAPPING_WITH_AN_OPERATION = """
            ###Mapping
            Mapping my::M
            (
              *my::A[a]: Operation
              {
                meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(b,c)
              }
            )
            """;

    @Test
    void aDataQualityTreeNodesAliasAndArguments() {
        Json.Obj model = edit(parse("""
                ###DataQualityValidation
                DataQualityValidation my::Check
                {
                  context: fromMappingAndRuntime(my::M, my::R);
                  validationTree: $[
                    my::P<mustBeNamed>{
                      name
                    }
                  ]$;
                }
                """), o -> "dataQualityPropertyGraphFetchTree".equals(type(o))
                ? with(with(o, "alias", Json.str("n")), "parameters", new Json.Arr(List.of(integer(1))))
                : o);
        assertTrue(exact(model).contains("'n':name(1)"));
    }

    @Test
    void aDataSpacesFeaturedDiagramsAreDiagramsWithNoTitle() {
        Json.Obj model = edit(parse("""
                ###DataSpace
                DataSpace my::DS
                {
                  executionContexts:
                  [
                    {
                      name: 'Some Context';
                      mapping: my::M;
                      defaultRuntime: my::R;
                    }
                  ];
                  defaultExecutionContext: 'Some Context';
                }
                """), o -> "dataSpace".equals(type(o))
                ? with(o, "featuredDiagrams", (Json.Node) Json.parse("[{\"path\":\"my::D\",\"type\":\"DIAGRAM\"}]", DEEP))
                : o);
        String written = broughtUp(model);
        assertTrue(written.contains("\"diagrams\":[{"), written);
        assertFalse(written.contains("featuredDiagrams"), written);
        assertTrue(ModelComposer.model(model).contains("title: '';\n      diagram: my::D;"));
    }

    @Test
    void anOperationsExtendsAndAMongoMappingWithNoCollection() {
        Json.Obj model = edit(parse(MAPPING_WITH_AN_OPERATION), o -> {
            if ("operation".equals(type(o))) {
                return with(o, "extendsClassMappingId", Json.str("base"));
            }
            if ("mapping".equals(type(o))) {
                List<Json.Node> cms = new ArrayList<>(o.getArr("classMappings").items());
                cms.add(Json.parse("{\"_type\":\"MongoDB\",\"class\":\"my::B\",\"extendsClassMappingId\":\"base\","
                        + "\"root\":false}", DEEP));
                return with(o, "classMappings", new Json.Arr(cms));
            }
            return o;
        });
        String printed = exact(model);
        assertTrue(printed.contains("*my::A[a] extends [base]: Operation"), printed);
        assertTrue(printed.contains("my::B extends [base]: MongoDB\n  {\n  }"), printed);
    }

    /** The engine's grammar drops an operation's extends (lite's model keeps it): the wire carries none. */
    @Test
    void anOperationsExtendsFromTheTextIsNotOnTheWire() {
        Json.Obj model = parse(MAPPING_WITH_AN_OPERATION.replace("*my::A[a]:", "*my::A[a] extends [base]:"));
        assertFalse(Json.toCompact(model).contains("extendsClassMappingId"), Json.toCompact(model));
    }

    /** The engine's grammar keeps an aggregation-aware mapping's extends, and so does lite's now. */
    @Test
    void anAggregationAwareMappingsExtendsFromTheText() {
        Json.Obj model = parse("""
                ###Mapping
                Mapping my::M
                (
                   *my::Trade[t] extends [base]: AggregationAware
                   {
                      Views:
                      [
                         (
                            ~modelOperation:
                            {
                               ~canAggregate false,
                               ~groupByFunctions
                               (
                                  $this.bookId
                               ),
                               ~aggregateValues
                               (
                                  ( ~mapFn: $this.notional, ~aggregateFn: $mapped->sum() )
                               )
                            },
                            ~aggregateMapping: Relational
                            {
                               ~mainTable [my::Db] TRADE_BY_BOOK
                               bookId: [my::Db] TRADE_BY_BOOK.BOOK_ID
                            }
                         )
                      ],
                      ~mainMapping: Relational
                      {
                         ~mainTable [my::Db] TRADE
                         bookId: [my::Db] TRADE.BOOK_ID
                      }
                   }
                )
                """);
        assertTrue(Json.toCompact(model).contains("\"extendsClassMappingId\":\"base\""), Json.toCompact(model));
        assertTrue(exact(model).contains("*my::Trade[t] extends [base]: AggregationAware"));
    }

    @Test
    void aMergeAndAServiceStoreMappingsExtends() {
        Json.Obj model = edit(parse(SERVICE_STORE_MAPPING), o -> {
            if ("serviceStore".equals(type(o)) && o.has("servicesMapping")) {
                return with(o, "extendsClassMappingId", Json.str("base"));
            }
            if ("mapping".equals(type(o))) {
                List<Json.Node> cms = new ArrayList<>(o.getArr("classMappings").items());
                cms.add(Json.parse("{\"_type\":\"mergeOperation\",\"class\":\"my::C\",\"extendsClassMappingId\":\"base\","
                        + "\"id\":\"c\",\"operation\":\"MERGE\",\"parameters\":[\"a\",\"b\"],\"root\":false,"
                        + "\"validationFunction\":{\"_type\":\"lambda\",\"body\":[{\"_type\":\"boolean\",\"value\":true}],"
                        + "\"parameters\":[]}}", DEEP));
                return with(o, "classMappings", new Json.Arr(cms));
            }
            return o;
        });
        String printed = printedAndStable(model);
        assertTrue(printed.contains("*my::P extends [base]: ServiceStore"), printed);
        assertTrue(printed.contains("my::C[c] extends [base]: Operation"), printed);
    }

    @Test
    void aGenerationNodeWithoutItsIdAndDecimalOrNullSettings() {
        Json.Obj model = edit(parse("""
                ###GenerationSpecification
                GenerationSpecification my::GS
                {
                  generationNodes: [
                    {
                      generationElement: my::FG;
                    }
                  ];
                }

                ###FileGeneration
                Avro my::FG
                {
                  scopeElements: [my::P];
                }
                """), o -> {
                    if (o.getStringOr("generationElement", null) != null) {
                        return without(o, "id");
                    }
                    if ("fileGeneration".equals(type(o))) {
                        return with(o, "configurationProperties", (Json.Node) Json.parse(
                                "[{\"name\":\"ratio\",\"value\":1.50},{\"name\":\"none\",\"value\":null}]", DEEP));
                    }
                    return o;
                });
        // the id comes back as the element (the grammar's default), the decimal as the engine keeps it: its text
        String written = broughtUp(model);
        assertTrue(written.contains("\"generationElement\":\"my::FG\",\"id\":\"my::FG\""), written);
        assertTrue(written.contains("\"value\":\"1.50\""), written);
        assertTrue(written.contains("\"value\":null"), written);
        String printed = ModelComposer.model(model);
        assertTrue(printed.contains("ratio: '1.50';"), printed);
        // the engine keeps a null setting as Java null and prints it bare
        assertTrue(printed.contains("none: null;"), printed);
        assertFalse(printed.contains("id:"), printed);
    }

    @Test
    void aServiceTestWithoutKeys() {
        Json.Obj model = edit(parse("""
                ###Service
                Service my::S
                {
                  pattern: '/s';
                  documentation: '';
                  autoActivateUpdates: true;
                  execution: Single
                  {
                    query: |my::P.all();
                    mapping: my::M;
                    runtime: my::R;
                  }
                  testSuites:
                  [
                    s1:
                    {
                      tests:
                      [
                        t1:
                        {
                          asserts:
                          [
                            a1:
                              EqualToJson
                              #{
                                expected:
                                  ExternalFormat
                                  #{
                                    contentType: 'application/json';
                                    data: '{}';
                                  }#;
                              }#
                          ]
                        }
                      ]
                    }
                  ]
                }
                """), o -> "serviceTest".equals(type(o)) ? without(o, "keys") : o);
        assertTrue(broughtUp(model).contains("\"keys\":[]"));
    }

    @Test
    void aCsvTableWithoutValues() {
        Json.Obj model = edit(parse("""
                ###Data
                Data my::D
                {
                  Relational
                  #{
                    default.T:
                      'id,name\\n'+
                      '1,a\\n';
                  }#
                }
                """), o -> o.getStringOr("table", null) != null ? without(o, "values") : o);
        assertTrue(exact(model).contains("default.T:;"));
    }

    @Test
    void aFunctionTestsCsvTableWithoutValues() {
        Json.Obj model = edit(parse("""
                ###Pure
                function my::f(): Integer[1]
                {
                  1
                }
                {
                  mySuite
                  (
                    my::DB:
                      Relational
                      #{
                        default.T:
                          'id\\n'+
                          '1\\n';
                      }#;
                    t1 | f() => 1;
                  )
                }
                """), o -> o.getStringOr("table", null) != null ? without(o, "values") : o);
        assertTrue(exact(model).contains("default.T:;"));
    }

    @Test
    void aColumnWithoutNullableAndADecimalSpelledLonger() {
        Json.Obj model = edit(parse("""
                ###Relational
                Database my::DB
                (
                  Table T
                  (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(20)
                  )
                  Filter F(T.id > 1.5)
                )
                """), o -> "name".equals(o.getStringOr("name", null)) && o.has("nullable") ? without(o, "nullable") : o);
        Json.Obj longer = (Json.Obj) Json.parse(Json.toCompact(model).replace("\"value\":1.5", "\"value\":1.50"), DEEP);
        assertTrue(Json.toCompact(longer).contains("1.50"));
        // nullable left out is the engine's false; the decimal is its Double, written as Java spells it
        String written = broughtUp(longer);
        assertTrue(written.contains("\"nullable\":false"), written);
        assertTrue(written.contains("\"value\":1.5}"), written);
        String printed = ModelComposer.model(longer);
        assertTrue(printed.contains("name VARCHAR(20) NOT NULL"), printed);
        assertTrue(printed.contains("Filter F(T.id > 1.5)"), printed);
    }

    private static final String PERSISTENCE = """
            ###Persistence
            Persistence my::P
            {
              doc: 'a persistence';
              trigger: Manual;
              service: my::S;
              serviceOutputTargets:
              [
                TDS
                {
                  keys:
                  [
                    foo
                  ]
                  datasetType: Delta
                  {
                    actionIndicator: None;
                  }
                  deduplication: None;
                }
                ->
                {
                }
              ];
              tests:
              [
                test1:
                {
                  testBatches:
                  [
                    testBatch1:
                    {
                      data:
                      {
                        connection:
                        {
                          ExternalFormat
                          #{
                            contentType: 'application/x.flatdata';
                            data: 'A\\n1';
                          }#
                        }
                      }
                      asserts:
                      [
                        assert1:
                          EqualToJson
                          #{
                            expected:
                              ExternalFormat
                              #{
                                contentType: 'application/json';
                                data: '{}';
                              }#;
                          }#
                      ]
                    }
                  ]
                  isTestDataFromServiceOutput: false;
                }
              ]
            }
            """;

    @Test
    void aPersistenceTestBatchWithoutDataOrAssertions() {
        Json.Obj model = edit(parse(PERSISTENCE),
                o -> o.has("batchId") ? without(without(o, "testData"), "assertions") : o);
        assertTrue(exact(model).contains("testBatch1:\n        {\n        }"));
    }

    /** Left out, the engine's Boolean is true; written null, it is none (not printed), and written back as null. */
    @Test
    void aPersistenceTestsIsTestDataFromServiceOutputLeftOutOrNull() {
        Json.Obj leftOut = edit(parse(PERSISTENCE),
                o -> "test".equals(type(o)) ? without(o, "isTestDataFromServiceOutput") : o);
        assertTrue(broughtUp(leftOut).contains("\"isTestDataFromServiceOutput\":true"));
        assertTrue(ModelComposer.model(leftOut).contains("isTestDataFromServiceOutput: true;"));
        Json.Obj written = edit(parse(PERSISTENCE),
                o -> "test".equals(type(o)) ? with(o, "isTestDataFromServiceOutput", Json.parse("null", DEEP)) : o);
        assertFalse(exact(written).contains("isTestDataFromServiceOutput"));
    }

    @Test
    void aFunctionTestWithNoAssertion() {
        Json.Obj model = edit(parse("""
                ###Pure
                function my::f(a: Integer[1]): Integer[1]
                {
                  $a + 1
                }
                {
                  t1 | f(1) => 2;
                }
                """), o -> "functionTest".equals(type(o)) ? with(o, "assertions", new Json.Arr(List.of())) : o);
        assertTrue(exact(model).contains("t1 | f(1) => ;"));
    }

    @Test
    void aHostedServicesUserListWithoutUsers() {
        Json.Obj model = edit(parse("""
                ###HostedService
                HostedService my::HS
                {
                   pattern: '/x';
                   ownership: UserList { users: ['a'] };
                   function: my::f():String[1];
                   documentation: 'd';
                   autoActivateUpdates: true;
                }
                """), o -> "userList".equals(type(o)) ? without(o, "users") : o);
        assertTrue(broughtUp(model).contains("\"users\":[]"));
        assertTrue(ModelComposer.model(model).contains("ownership : UserList { users: [\n\n    ] };"));
    }

    private static final String SERVICE_STORE_MAPPING = """
            ###ServiceStore
            ServiceStore my::SS
            (
              Service S
              (
                path : '/s';
                method : GET;
                security : [];
                response : [my::P <- my::B];
              )
            )

            ###Mapping
            Mapping my::SM
            (
              *my::P: ServiceStore
              {
                ~service [my::SS] S
                (
                  ~path $service.response.items
                )
              }
            )
            """;

    @Test
    void aServiceStorePathSegmentsArguments() {
        Json.Obj model = edit(parse(SERVICE_STORE_MAPPING), o -> "propertyPath".equals(type(o))
                ? with(o, "parameters", new Json.Arr(List.of(integer(1), integer(2))))
                : o);
        assertTrue(exact(model).contains("~path $service.response.items(1, 2)"));
    }

    // ---------------------------------------------------------------------

    /** A shape lite keeps: written back exactly as read, and printed (returned) stably. */
    private static String exact(Json.Obj model) {
        String json = Json.toCompact(model);
        assertEquals(json, writtenBack(json), "emit(read(J)) is J");
        return printedAndStable(model);
    }

    /** A shape brought up to today's form: written back differently (returned), printed the same both ways. */
    private static String broughtUp(Json.Obj model) {
        String json = Json.toCompact(model);
        String written = writtenBack(json);
        assertNotEquals(json, written, "brought up, the JSON written back is today's form");
        printedAndStable(model);
        return written;
    }

    /**
     * The model read and written back, in the form these cases build their JSON in: the wire repeats some
     * {@code _type} keys (as the engine's does), which a parsed object keeps once, so both sides are compared parsed.
     */
    private static String writtenBack(String json) {
        return Json.toCompact(Json.parse(ProtocolEmitter.emit(ModelReader.read(json)), DEEP));
    }

    /** The model printed; and written back by lite, it reads and prints the same. */
    private static String printedAndStable(Json.Obj model) {
        String printed = ModelComposer.model(model);
        String written = ProtocolEmitter.emit(ModelReader.read(Json.toCompact(model)));
        assertEquals(printed, ModelComposer.model((Json.Obj) Json.parse(written, DEEP)), "written back, it prints the same");
        return printed;
    }

    private static Json.Node integer(long n) {
        return Json.parse("{\"_type\":\"integer\",\"value\":" + n + "}", DEEP);
    }

    private static String type(Json.Obj o) {
        return o.getStringOr("_type", "");
    }

    private static Json.Obj parse(String text) {
        return (Json.Obj) Json.parse(PmcdParser.parseDocument(text), DEEP);
    }

    /** {@code n} with every object rewritten by {@code f}, children first. */
    private static Json.Obj edit(Json.Obj n, UnaryOperator<Json.Obj> f) {
        return (Json.Obj) editNode(n, f);
    }

    private static Json.Node editNode(Json.Node n, UnaryOperator<Json.Obj> f) {
        if (n instanceof Json.Obj o) {
            LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
            o.fields().forEach((k, v) -> out.put(k, editNode(v, f)));
            return f.apply(new Json.Obj(out));
        }
        if (n instanceof Json.Arr a) {
            List<Json.Node> out = new ArrayList<>();
            for (Json.Node x : a.items()) {
                out.add(editNode(x, f));
            }
            return new Json.Arr(out);
        }
        return n;
    }

    /** {@code o} with {@code key} set, in the wire's alphabetical place among the fields. */
    private static Json.Obj with(Json.Obj o, String key, Json.Node value) {
        java.util.TreeMap<String, Json.Node> sorted = new java.util.TreeMap<>(o.fields());
        sorted.put(key, value);
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
        if (sorted.containsKey("_type")) {
            out.put("_type", sorted.remove("_type"));
        }
        out.putAll(sorted);
        return new Json.Obj(out);
    }

    private static Json.Obj without(Json.Obj o, String key) {
        LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>(o.fields());
        out.remove(key);
        return new Json.Obj(out);
    }
}
