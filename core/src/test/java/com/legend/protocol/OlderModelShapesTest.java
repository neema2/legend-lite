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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Model JSON that lite's grammar does not write but legend-engine 4.145.0 reads -- older layouts, and fields a person
 * or an older engine leaves out -- read as the engine reads it and printed as its printer prints it (the protocol
 * program's leg 2: the reader reads every field the engine reads; found by step 3's audit, PROTOCOL_PROGRAM §4.2). Each
 * case starts from a text lite parses, edits its JSON into the shape, and checks the printed text; and that the model,
 * written back, reads and prints the same.
 */
class OlderModelShapesTest {

    private static final Json.Config DEEP = new Json.Config(4096);

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
        String printed = printedAndStable(model);
        assertTrue(printed.contains("'n':name(1)"), printed);
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
        String printed = printedAndStable(model);
        assertTrue(printed.contains("title: '';\n      diagram: my::D;"), printed);
    }

    @Test
    void everyClassMappingKindExtendsAndAMongoMappingMayNameNoCollection() {
        Json.Obj model = edit(parse("""
                ###Mapping
                Mapping my::M
                (
                  *my::A[a]: Operation
                  {
                    meta::pure::router::operations::union_OperationSetImplementation_1__SetImplementation_MANY_(b,c)
                  }
                )
                """), o -> {
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
        String printed = printedAndStable(model);
        assertTrue(printed.contains("*my::A[a] extends [base]: Operation"), printed);
        assertTrue(printed.contains("my::B extends [base]: MongoDB\n  {\n  }"), printed);
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
        String printed = printedAndStable(model);
        assertTrue(printed.contains("ratio: '1.50';"), printed);
        assertTrue(printed.contains("none: 'null';"), printed);
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
        assertFalse(Json.toCompact(model).contains("\"keys\""));
        printedAndStable(model);
    }

    @Test
    void aCsvTableWithoutValuesAColumnWithoutNullableAndADecimalSpelledLonger() {
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
                """), o -> {
                    if (o.getStringOr("values", null) != null && o.getStringOr("table", null) != null) {
                        return without(o, "values");
                    }
                    if ("name".equals(o.getStringOr("name", null)) && o.has("nullable")) {
                        return without(o, "nullable");
                    }
                    return o;
                });
        Json.Obj longer = (Json.Obj) Json.parse(Json.toCompact(model).replace("\"value\":1.5", "\"value\":1.50"), DEEP);
        assertTrue(Json.toCompact(longer).contains("1.50"));
        String printed = printedAndStable(longer);
        assertTrue(printed.contains("default.T:;"), printed);
        assertTrue(printed.contains("name VARCHAR(20) NOT NULL"), printed);
        assertTrue(printed.contains("Filter F(T.id > 1.5)"), printed);
    }

    @Test
    void aPersistenceTestsOptionalParts() {
        Json.Obj model = edit(parse("""
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
                """), o -> {
                    if (o.has("batchId")) {
                        return without(without(o, "testData"), "assertions");
                    }
                    if ("test".equals(type(o))) {
                        return without(o, "isTestDataFromServiceOutput");
                    }
                    return o;
                });
        String printed = printedAndStable(model);
        assertTrue(printed.contains("testBatch1:\n        {\n        }"), printed);
        // left out, the engine's Boolean is true
        assertTrue(printed.contains("isTestDataFromServiceOutput: true;"), printed);
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
        String printed = printedAndStable(model);
        assertTrue(printed.contains("t1 | f(1) => ;"), printed);
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
        String printed = printedAndStable(model);
        assertTrue(printed.contains("ownership : UserList { users: [\n\n    ] };"), printed);
    }

    @Test
    void aServiceStorePathSegmentsArguments() {
        Json.Obj model = edit(parse("""
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
                """), o -> "propertyPath".equals(type(o))
                ? with(o, "parameters", new Json.Arr(List.of(integer(1), integer(2))))
                : o);
        String printed = printedAndStable(model);
        assertTrue(printed.contains("~path $service.response.items(1, 2)"), printed);
    }

    // ---------------------------------------------------------------------

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
