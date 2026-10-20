// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.protocol.schema.ImportedModel.ImportedClass;
import com.legend.protocol.schema.ImportedModel.ImportedProperty;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON Schema and OpenAPI read as a model (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4): what each schema is made, each
 * property's type and multiplicity, inheritance through allOf, emptiness in each of its spellings, the classes and
 * enumerations an inline schema makes, the free-form object a Variant -- and everything with no Pure form refused,
 * every one at once, by where the schema has it.
 */
@DisplayName("JsonSchemaReader -- JSON Schema and OpenAPI read as classes and enumerations")
class JsonSchemaReaderTest {

    private static ImportedModel read(String json) {
        return JsonSchemaReader.read(json.replace('`', '"'), null, List.of());
    }

    /** Each class as {@code Name(Super, ...){property: Type[m], ...}}, owned ones marked {@code owned}. */
    private static List<String> classes(ImportedModel model) {
        List<String> out = new ArrayList<>();
        for (ImportedClass c : model.classes()) {
            List<String> props = new ArrayList<>();
            for (ImportedProperty p : c.properties()) {
                props.add(p.name() + ": " + p.kind().name().charAt(0) + " " + p.type() + p.multiplicity());
            }
            out.add((c.owned() ? "owned " : "") + c.name() + (c.supers().isEmpty() ? "" : c.supers().toString()) + "{"
                    + String.join(", ", props) + "}");
        }
        return out;
    }

    private static List<String> enumerations(ImportedModel model) {
        List<String> out = new ArrayList<>();
        for (ImportedModel.ImportedEnum e : model.enumerations()) {
            out.add(e.name() + e.values());
        }
        return out;
    }

    private static List<String> refusals(String json) {
        return assertThrows(SchemaImportException.class, () -> read(json)).reasons();
    }

    @Test
    @DisplayName("an OpenAPI document: each schema of components.schemas a class or enumeration, each property typed")
    void anOpenApiDocument() {
        ImportedModel m = read("""
                {`openapi`: `3.0.3`, `components`: {`schemas`: {
                  `Side`: {`type`: `string`, `enum`: [`BUY`, `SELL`], `description`: `which way`},
                  `Trade`: {`type`: `object`, `description`: `a trade`, `required`: [`id`, `side`, `customer`, `legs`],
                    `properties`: {
                      `id`: {`type`: `integer`, `format`: `int64`},
                      `price`: {`type`: `number`},
                      `notional`: {`type`: `number`, `format`: `decimal`},
                      `active`: {`type`: `boolean`},
                      `tradeDate`: {`type`: `string`, `format`: `date`},
                      `bookedAt`: {`type`: `string`, `format`: `date-time`},
                      `side`: {`$ref`: `#/components/schemas/Side`},
                      `customer`: {`$ref`: `#/components/schemas/Customer`},
                      `tags`: {`type`: `array`, `items`: {`type`: `string`}},
                      `legs`: {`type`: `array`, `minItems`: 1, `items`: {`$ref`: `#/components/schemas/Leg`}}}},
                  `Customer`: {`type`: `object`, `properties`: {`id`: {`type`: `integer`}}},
                  `Leg`: {`type`: `object`, `properties`: {`amount`: {`$ref`: `#/components/schemas/Money`}}},
                  `Money`: {`type`: `number`}}}}""");
        assertEquals(List.of("Trade{id: P Integer[1], price: P Float[0..1], notional: P Decimal[0..1], "
                        + "active: P Boolean[0..1], tradeDate: P StrictDate[0..1], bookedAt: P DateTime[0..1], "
                        + "side: E Side[1], customer: C Customer[1], tags: P String[*], legs: C Leg[1..*]}",
                "Customer{id: P Integer[0..1]}", "Leg{amount: P Float[0..1]}"), classes(m),
                "Money is no element: a reference to it reads as the number it is");
        assertEquals(List.of("Side[BUY, SELL]"), enumerations(m));
        assertEquals("a trade", m.classes().get(0).doc());
        assertEquals("which way", m.enumerations().get(0).doc());
        assertEquals(List.of(), m.notes(), "a list's bounds are its multiplicity's");
    }

    @Test
    @DisplayName("a JSON Schema document: its root, by its title or the name given, and its $defs")
    void aJsonSchemaDocument() {
        String schema = """
                {`$schema`: `https://json-schema.org/draft/2020-12/schema`, %s `type`: `object`,
                 `properties`: {`name`: {`type`: `string`}, `home`: {`$ref`: `#/$defs/Address`}},
                 `required`: [`name`],
                 `$defs`: {`Address`: {`type`: `object`, `properties`: {`city`: {`type`: `string`}}}}}""";
        assertEquals(List.of("Address{city: P String[0..1]}", "Person{name: P String[1], home: C Address[0..1]}"),
                classes(read(schema.formatted("`title`: `Person`,"))));
        assertEquals(List.of("Address{city: P String[0..1]}", "Person{name: P String[1], home: C Address[0..1]}"),
                classes(JsonSchemaReader.read(schema.formatted("").replace('`', '"'), "Person", List.of())));
        assertEquals(List.of("#: the root schema is an object with no title: name it (the request's 'name')"),
                refusals(schema.formatted("")));
    }

    @Test
    @DisplayName("allOf: a reference to an object schema a superclass, each inline part's properties the class's own")
    void inheritanceThroughAllOf() {
        ImportedModel m = read("""
                {`$defs`: {
                  `Party`: {`type`: `object`, `properties`: {`id`: {`type`: `integer`}}},
                  `Firm`: {`allOf`: [{`$ref`: `#/$defs/Party`},
                    {`type`: `object`, `required`: [`legalName`], `properties`: {`legalName`: {`type`: `string`}}}]}}}""");
        assertEquals(List.of("Party{id: P Integer[0..1]}", "Firm[Party]{legalName: P String[1]}"), classes(m));
    }

    @Test
    @DisplayName("may be empty, in each spelling: a type list with null, nullable, anyOf with null, an enum with null")
    void emptinessInEachSpelling() {
        ImportedModel m = read("""
                {`$defs`: {
                  `Side`: {`enum`: [`BUY`, `SELL`]},
                  `Code`: {`enum`: [`A`, null]},
                  `T`: {`type`: `object`, `required`: [`a`, `b`, `c`, `d`, `e`], `properties`: {
                    `a`: {`type`: [`string`, `null`]},
                    `b`: {`type`: `integer`, `nullable`: true},
                    `c`: {`anyOf`: [{`$ref`: `#/$defs/Side`}, {`type`: `null`}]},
                    `d`: {`$ref`: `#/$defs/Code`},
                    `e`: {`type`: `boolean`}}}}}""");
        assertEquals(List.of("T{a: P String[0..1], b: P Integer[0..1], c: E Side[0..1], d: E Code[0..1], "
                + "e: P Boolean[1]}"), classes(m));
        assertEquals(List.of("Side[BUY, SELL]", "Code[A]"), enumerations(m));
    }

    @Test
    @DisplayName("an inline object a class its owner holds, an inline enum an enumeration, named after owner and property")
    void inlineSchemasAreNamedAfterTheirOwner() {
        ImportedModel m = read("""
                {`$defs`: {`Order`: {`type`: `object`, `properties`: {
                  `status`: {`type`: `string`, `enum`: [`open`, `closed`]},
                  `shipping`: {`type`: `object`, `properties`: {`city`: {`type`: `string`}}},
                  `lines`: {`type`: `array`, `items`: {`type`: `object`, `properties`: {`sku`: {`type`: `string`}}}}}}}}""");
        assertEquals(List.of("Order{status: E OrderStatus[0..1], shipping: C OrderShipping[0..1], "
                        + "lines: C OrderLine[*]}", "owned OrderShipping{city: P String[0..1]}",
                "owned OrderLine{sku: P String[0..1]}"), classes(m), "an inline object follows its owner; a list's "
                + "item named in the singular");
        assertEquals(List.of("OrderStatus[open, closed]"), enumerations(m));
    }

    @Test
    @DisplayName("a free-form object, a map, or any value at all: a Variant, as Pure holds JSON")
    void freeFormIsVariant() {
        ImportedModel m = read("""
                {`$defs`: {`T`: {`type`: `object`, `properties`: {
                  `meta`: {`type`: `object`},
                  `labels`: {`type`: `object`, `additionalProperties`: {`type`: `string`}},
                  `anything`: {}}}}}""");
        String variant = "meta::pure::metamodel::variant::Variant";
        assertEquals(List.of("T{meta: P " + variant + "[0..1], labels: P " + variant + "[0..1], anything: P " + variant
                + "[0..1]}"), classes(m));
    }

    @Test
    @DisplayName("what has no Pure form is refused, every one at once, where the schema has it")
    void everythingWithoutAPureFormIsRefusedAtOnce() {
        assertEquals(List.of(
                "#/$defs/T/properties/either: a union (oneOf) has no Pure form yet",
                "#/$defs/T/properties/elsewhere: the reference 'other.json#/Thing' is outside the document; only the "
                        + "document's own schemas are read",
                "#/$defs/T/properties/never: the keyword 'not' has no Pure form yet",
                "#/$defs/T/properties/pair: a tuple (items as a list) has no Pure form yet",
                "#/$defs/T/properties/mixed: a value of several types (string, integer) has no Pure form yet",
                "#/$defs/T/properties/missing: the reference '#/$defs/Nope' names no schema of the document's (a "
                        + "schema of components.schemas, $defs or definitions)",
                "#/$defs/Level: an enumeration of values that are not text (1) has no Pure form yet",
                "#/$defs/Level: an enumeration of values that are not text (2) has no Pure form yet",
                "#/$defs/Level: an enumeration with no value"),
                refusals("""
                        {`$defs`: {`T`: {`type`: `object`, `properties`: {
                          `either`: {`oneOf`: [{`type`: `string`}, {`type`: `integer`}]},
                          `elsewhere`: {`$ref`: `other.json#/Thing`},
                          `never`: {`not`: {`type`: `string`}},
                          `pair`: {`type`: `array`, `items`: [{`type`: `string`}, {`type`: `integer`}]},
                          `mixed`: {`type`: [`string`, `integer`]},
                          `missing`: {`$ref`: `#/$defs/Nope`},
                          `level`: {`$ref`: `#/$defs/Level`}}},
                         `Level`: {`type`: `integer`, `enum`: [1, 2]}}}"""));
    }

    @Test
    @DisplayName("a validation is noted, not carried; a name Pure cannot hold is made one, and noted")
    void validationsAndRenamesAreNoted() {
        ImportedModel m = read("""
                {`$defs`: {`trade_event`: {`type`: `object`, `properties`: {
                  `trade-id`: {`type`: `integer`, `minimum`: 0},
                  `code`: {`type`: `string`, `pattern`: `^[A-Z]+$`, `x-internal`: true},
                  `side`: {`enum`: [`buy-side`, `sell`]}}}}}""");
        assertEquals(List.of("TradeEvent{tradeId: P Integer[0..1], code: P String[0..1], side: E TradeEventSide[0..1]}"),
                classes(m));
        assertEquals(List.of("TradeEventSide[buy_side, sell]"), enumerations(m));
        assertEquals(List.of(
                "#/$defs/trade_event: 'trade_event' is named TradeEvent",
                "#/$defs/trade_event/properties/trade-id: 'trade-id' is named tradeId",
                "#/$defs/trade_event/properties/trade-id: minimum is not carried (a validation, not a shape)",
                "#/$defs/trade_event/properties/code: pattern is not carried (a validation, not a shape)",
                "#/$defs/trade_event/properties/side: the value 'buy-side' is named buy_side"), m.notes());
    }

    @Test
    @DisplayName("schemas chosen: those named and every schema they reach, the rest not read")
    void chosenSchemasBringWhatTheyReach() {
        String json = """
                {`openapi`: `3.1.0`, `components`: {`schemas`: {
                  `Trade`: {`type`: `object`, `properties`: {`customer`: {`$ref`: `#/components/schemas/Customer`}}},
                  `Customer`: {`type`: `object`, `properties`: {`id`: {`type`: `integer`}}},
                  `Unrelated`: {`type`: `object`, `properties`: {`x`: {`oneOf`: [{`type`: `string`}, {`type`: `integer`}]}}}}}}"""
                .replace('`', '"');
        assertEquals(List.of("Trade{customer: C Customer[0..1]}", "Customer{id: P Integer[0..1]}"),
                classes(JsonSchemaReader.read(json, null, List.of("Trade"))));
        SchemaImportException e = assertThrows(SchemaImportException.class,
                () -> JsonSchemaReader.read(json, null, List.of("Nope")));
        assertEquals(List.of("the schema has no 'Nope' to import"), e.reasons());
        assertTrue(assertThrows(SchemaImportException.class, () -> JsonSchemaReader.read(json, null, List.of()))
                .reasons().get(0).contains("a union (oneOf)"), "all of it, read, refuses the union");
    }

    @Test
    @DisplayName("a document that is not JSON is refused: YAML is not read yet")
    void yamlIsNotReadYet() {
        assertEquals(List.of("the schema is not a JSON document (a YAML document is not read yet: give it as JSON)"),
                refusals("openapi: 3.0.0\ncomponents: {}\n"));
    }
}
