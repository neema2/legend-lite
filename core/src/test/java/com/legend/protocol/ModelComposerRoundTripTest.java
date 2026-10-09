// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The model printer is the parser's inverse (docs/STUDIO_FULL_PLAN_2026_10_04.md, B1, the exact-round-trip
 * rule): text, parsed by lite's parser to protocol JSON, printed by {@link ModelComposer} and parsed again,
 * gives the same protocol JSON, source information aside -- with the model's section index (a text's own
 * sections) and without it (entity JSON from an SDLC carries none). Byte parity with legend-engine's own
 * printer is pinned separately, over the reference corpus, by parser-equivalence's ModelComposerParityTest.
 */
class ModelComposerRoundTripTest {

    private static final Json.Config DEEP = new Json.Config(4096);

    static Stream<String> models() {
        return Stream.of(
                // the domain: classes with generalization, constraints, defaults, derived properties, annotations
                """
                ###Pure
                Profile my::Tags
                {
                  stereotypes: [important, legacy];
                  tags: [doc, owner];
                }

                Enum <<my::Tags.important>> my::Side
                {
                  BUY,
                  SELL,
                  'Short Sell'
                }

                Class <<my::Tags.legacy>> {my::Tags.owner = 'desk'} my::Base
                {
                  id: Integer[1];
                }

                Class my::Trade extends my::Base
                [
                  positive: $this.quantity > 0,
                  named
                  (
                    ~externalId: 'TRADE_1'
                    ~function: $this.ticker->isNotEmpty()
                    ~enforcementLevel: Warn
                    ~message: 'ticker is required'
                  )
                ]
                {
                  <<my::Tags.important>> ticker: String[0..1];
                  quantity: Float[1] = 1.0;
                  side: my::Side[1];
                  notional() {$this.quantity * 100}: Float[1];
                  scaled(factor: Integer[1]) {$this.quantity * $factor}: Float[1];
                }

                Association my::Trade_Base
                {
                  trade: my::Trade[*];
                  base: my::Base[1];
                }

                Measure my::Mass
                {
                  *Gram: x -> $x;
                  Kilogram: x -> $x * 1000;
                }

                function my::double(x: Integer[1]): Integer[1]
                {
                  $x * 2
                }

                function <<my::Tags.legacy>> my::pair(a: String[1], b: String[*]): String[*]
                {
                  let c = $a + 'x';
                  $b->concatenate($c);
                }
                """,
                // a model-to-model mapping with an enumeration mapping, a filter and a model connection runtime
                """
                ###Pure
                Class my::Source
                {
                  name: String[1];
                  code: Integer[1];
                }

                Class my::Target
                {
                  label: String[1];
                  kind: my::Kind[1];
                }

                Enum my::Kind
                {
                  A,
                  B
                }

                ###Mapping
                Mapping my::M2M
                (
                  *my::Target: Pure
                  {
                    ~src my::Source
                    ~filter $src.code > 0
                    label: $src.name->toUpper(),
                    kind: EnumerationMapping KindMap: $src.code
                  }

                  my::Kind: EnumerationMapping KindMap
                  {
                    A: [1],
                    B: [2, 3]
                  }
                )

                ###Connection
                JsonModelConnection my::SourceJson
                {
                  class: my::Source;
                  url: 'data:application/json,{}';
                }

                ###Runtime
                Runtime my::M2MRuntime
                {
                  mappings:
                  [
                    my::M2M
                  ];
                  connections:
                  [
                    ModelStore:
                    [
                      json: my::SourceJson
                    ]
                  ];
                }
                """,
                // a relational store, its mapping, connection, runtime and a service over them
                """
                ###Pure
                Class my::Firm
                {
                  name: String[1];
                  employees: my::Person[*];
                }

                Class my::Person
                {
                  name: String[1];
                  age: Integer[0..1];
                }

                ###Relational
                Database my::DB
                (
                  Schema hr
                  (
                    Table PERSON
                    (
                      ID INTEGER PRIMARY KEY,
                      NAME VARCHAR(200) NOT NULL,
                      AGE INTEGER,
                      FIRM_ID INTEGER
                    )
                    Table FIRM
                    (
                      ID INTEGER PRIMARY KEY,
                      NAME VARCHAR(200)
                    )
                  )

                  Join Firm_Person(hr.FIRM.ID = hr.PERSON.FIRM_ID)
                  Filter Adults(hr.PERSON.AGE >= 18)
                  Filter Listed(in(hr.PERSON.AGE, [18, 21]) and in(hr.PERSON.NAME, ['A', 'B']))
                )

                ###Mapping
                Mapping my::RelationalMapping
                (
                  my::Person: Relational
                  {
                    ~primaryKey
                    (
                      [my::DB]hr.PERSON.ID
                    )
                    ~mainTable [my::DB]hr.PERSON
                    name: [my::DB]hr.PERSON.NAME,
                    age: [my::DB]hr.PERSON.AGE
                  }
                  my::Firm: Relational
                  {
                    ~mainTable [my::DB]hr.FIRM
                    name: [my::DB]hr.FIRM.NAME,
                    employees: [my::DB]@Firm_Person
                  }
                )

                ###Connection
                RelationalDatabaseConnection my::H2
                {
                  store: my::DB;
                  type: H2;
                  specification: LocalH2
                  {
                  };
                  auth: DefaultH2;
                }

                ###Runtime
                Runtime my::H2Runtime
                {
                  mappings:
                  [
                    my::RelationalMapping
                  ];
                  connections:
                  [
                    my::DB:
                    [
                      h2: my::H2
                    ]
                  ];
                }

                ###Service
                Service my::People
                {
                  pattern: '/people';
                  owners:
                  [
                    'alice'
                  ];
                  documentation: 'every person';
                  autoActivateUpdates: true;
                  execution: Single
                  {
                    query: |my::Person.all()->project([p|$p.name, p|$p.age], ['name', 'age']);
                    mapping: my::RelationalMapping;
                    runtime: my::H2Runtime;
                  }
                }
                """,
                // embedded data, a data space, a diagram, an external format binding and a text
                """
                ###Pure
                Class my::Thing
                {
                  name: String[1];
                }

                ###Data
                Data my::Things
                {
                  ExternalFormat
                  #{
                    contentType: 'application/json';
                    data: '[{"name":"a"}]';
                  }#
                }

                ###ExternalFormat
                Binding my::ThingBinding
                {
                  contentType: 'application/json';
                  modelIncludes: [
                    my::Thing
                  ];
                }

                ###Diagram
                Diagram my::ThingDiagram
                {
                  classView v1
                  {
                    class: my::Thing;
                    position: (10.0,20.5);
                    rectangle: (120.0,40.0);
                  }
                }

                ###Text
                Text my::Notes
                {
                  type: markdown;
                  content: 'some notes';
                }
                """);
    }

    @ParameterizedTest
    @MethodSource("models")
    void printedTextParsesBackToTheSameProtocol(String text) {
        Json.Obj parsed = parse(text);
        String printed = ModelComposer.model(parsed);
        assertEquals(stripped(parsed), stripped(parse(printed)), printed);
        // printing is stable: the printed text prints as itself
        assertEquals(printed, ModelComposer.model(parse(printed)));
    }

    @ParameterizedTest
    @MethodSource("models")
    void withoutTheSectionIndexTheElementsRoundTrip(String text) {
        Json.Obj entities = withoutSectionIndex(parse(text));
        String printed = ModelComposer.model(entities);
        // with no section index the elements print grouped by section, so compare them as a set
        assertEquals(elementSet(entities), elementSet(withoutSectionIndex(parse(printed))), printed);
    }

    private static List<String> elementSet(Json.Obj pmcd) {
        List<String> out = new ArrayList<>();
        for (Json.Node e : pmcd.getArr("elements").items()) {
            out.add(Json.toCompact(SourceInformation.strip(withoutSpans(e))));
        }
        out.sort(String::compareTo);
        return out;
    }

    @Test
    void anElementLiteCannotPrintIsRefusedByName() {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("_type", Json.str("noSuchElement"));
        f.put("package", Json.str("my"));
        f.put("name", Json.str("X"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ModelComposer.element(new Json.Obj(f)));
        assertTrue(e.getMessage().contains("'noSuchElement'"), e.getMessage());
    }

    // ---------------------------------------------------------------------
    // JSON the grammar does not write (a person's or an older engine's): printed as the engine prints it
    // ---------------------------------------------------------------------

    private static final String FUNCTION_WITH_TEST = """
            ###Pure
            function my::f(a: Integer[1]): Integer[1]
            {
              $a + 1
            }
            {
              t1 | f(1) => 2;
            }
            """;

    @Test
    void aFunctionTestsDocumentationIsReadWrittenBackAndPrinted() {
        Json.Obj withDoc = (Json.Obj) edit(parse(FUNCTION_WITH_TEST),
                o -> "functionTest".equals(o.getStringOr("_type", "")) ? with(o, "doc", Json.str("adds one")) : o);
        String json = Json.toCompact(withDoc);
        assertEquals(json, ProtocolEmitter.emit(ModelReader.read(json)), "emit(read(J)) is J");
        String printed = ModelComposer.model(withDoc);
        assertTrue(printed.contains("t1 'adds one' | f(1) => 2;"), printed);
    }

    private static final String SERVICE = """
            ###Service
            Service my::People
            {
              pattern: '/people';
              documentation: '';
              autoActivateUpdates: true;
              execution: Single
              {
                query: |my::Person.all();
                mapping: my::M;
                runtime:
                #{
                  mappings:
                  [
                    my::M
                  ];
                  connections:
                  [
                    ModelStore:
                    [
                      connection_1:
                      #{
                        JsonModelConnection
                        {
                          class: my::Person;
                          url: 'data:application/json,{}';
                        }
                      }#
                    ]
                  ];
                }#;
              }
            }
            """;

    @Test
    void aServiceWithoutAutoActivateUpdatesPrintsTheEnginesDefault() {
        Json.Obj without = (Json.Obj) edit(parse(SERVICE),
                o -> "service".equals(o.getStringOr("_type", "")) ? without(o, "autoActivateUpdates") : o);
        String printed = ModelComposer.model(without);
        assertTrue(printed.contains("autoActivateUpdates: true;"), printed);
    }

    /** An older {@code legacyRuntime} prints as the runtime the engine makes of it ({@code LegacyRuntime.toEngineRuntime}). */
    @Test
    void aServicesLegacyRuntimePrintsAsTheRuntimeTheEngineMakesOfIt() {
        Json.Obj current = parse(SERVICE);
        Json.Obj legacy = (Json.Obj) edit(current, o -> "engineRuntime".equals(o.getStringOr("_type", ""))
                ? (Json.Obj) Json.parse("{\"_type\":\"legacyRuntime\",\"mappings\":[{\"path\":\"my::M\",\"type\":\"MAPPING\"}],"
                        + "\"connections\":[{\"_type\":\"JsonModelConnection\",\"class\":\"my::Person\",\"element\":\"ModelStore\","
                        + "\"url\":\"data:application/json,{}\"}]}", DEEP)
                : o);
        assertEquals(ModelComposer.model(current), ModelComposer.model(legacy));
    }

    /**
     * A model mixing an older function (its wire name without the signature's mangling, as its section names it)
     * with a current one of the same declared name: each is found in the section, once.
     */
    @Test
    void aSectionNamingAnOlderFunctionFindsItBesideACurrentOneOfTheSameName() {
        Json.Obj current = parse("""
                ###Pure
                function my::f(a: String[1]): String[1]
                {
                  $a
                }

                function my::f(a: Integer[1]): Integer[1]
                {
                  $a
                }
                """);
        String mangled = "f_Integer_1__Integer_1_";
        Json.Obj mixed = (Json.Obj) edit(current, o -> {
            if (mangled.equals(o.getStringOr("name", ""))) {
                return with(o, "name", Json.str("f"));
            }
            if ("default".equals(o.getStringOr("_type", "")) || "importAware".equals(o.getStringOr("_type", ""))) {
                List<Json.Node> paths = new ArrayList<>();
                for (Json.Node p : o.getArr("elements").items()) {
                    paths.add(Json.str(("my::" + mangled).equals(((Json.Str) p).value()) ? "my::f" : ((Json.Str) p).value()));
                }
                return with(o, "elements", new Json.Arr(paths));
            }
            return o;
        });
        assertTrue(Json.toCompact(mixed).contains("\"my::f\""), "the section names the older function");
        assertEquals(ModelComposer.model(current), ModelComposer.model(mixed));
    }

    /** {@code n} with every object rewritten by {@code f}, children first. */
    private static Json.Node edit(Json.Node n, java.util.function.UnaryOperator<Json.Obj> f) {
        if (n instanceof Json.Obj o) {
            LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
            o.fields().forEach((k, v) -> out.put(k, edit(v, f)));
            return f.apply(new Json.Obj(out));
        }
        if (n instanceof Json.Arr a) {
            List<Json.Node> out = new ArrayList<>();
            for (Json.Node x : a.items()) {
                out.add(edit(x, f));
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

    private static Json.Obj parse(String text) {
        return (Json.Obj) Json.parse(PmcdParser.parseDocument(text), DEEP);
    }

    private static String stripped(Json.Obj pmcd) {
        return Json.toCompact(SourceInformation.strip(withoutSpans(pmcd)));
    }

    /**
     * The node without the protocol's NAMED spans ({@code classSourceInformation}, {@code propertySourceInformation},
     * ...), which {@link SourceInformation#strip} keeps: they record where a part sat in the text, as
     * {@code sourceInformation} does.
     */
    private static Json.Node withoutSpans(Json.Node n) {
        if (n instanceof Json.Obj o) {
            LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
            o.fields().forEach((k, v) -> {
                if (!k.endsWith("SourceInformation")) {
                    out.put(k, withoutSpans(v));
                }
            });
            return new Json.Obj(out);
        }
        if (n instanceof Json.Arr a) {
            List<Json.Node> out = new ArrayList<>();
            for (Json.Node x : a.items()) {
                out.add(withoutSpans(x));
            }
            return new Json.Arr(out);
        }
        return n;
    }

    private static Json.Obj withoutSectionIndex(Json.Obj pmcd) {
        List<Json.Node> kept = new ArrayList<>();
        for (Json.Node e : pmcd.getArr("elements").items()) {
            if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                kept.add(e);
            }
        }
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(pmcd.fields());
        f.put("elements", new Json.Arr(kept));
        return new Json.Obj(f);
    }
}
