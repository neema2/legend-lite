// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.json.Json;
import com.legend.protocol.schema.ImportedModel.All;
import com.legend.protocol.schema.ImportedModel.Any;
import com.legend.protocol.schema.ImportedModel.Bound;
import com.legend.protocol.schema.ImportedModel.Condition;
import com.legend.protocol.schema.ImportedModel.Has;
import com.legend.protocol.schema.ImportedModel.Holds;
import com.legend.protocol.schema.ImportedModel.ImportedClass;
import com.legend.protocol.schema.ImportedModel.ImportedProperty;
import com.legend.protocol.schema.ImportedModel.Length;
import com.legend.protocol.schema.ImportedModel.Logic;
import com.legend.protocol.schema.ImportedModel.MultipleOf;
import com.legend.protocol.schema.ImportedModel.Not;
import com.legend.protocol.schema.ImportedModel.OneOf;
import com.legend.protocol.schema.ImportedModel.Pattern;
import com.legend.protocol.schema.ImportedModel.Requires;
import com.legend.protocol.schema.ImportedModel.Rule;
import com.legend.protocol.schema.ImportedModel.Unique;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JSON Schema and OpenAPI read as a model (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4): what each schema is made, each
 * property's type and multiplicity, inheritance through allOf, emptiness in each of its spellings, the classes and
 * enumerations an inline schema makes, the free-form object a Variant -- and everything with no Pure form refused,
 * every one at once, by where the schema has it.
 */
@DisplayName("JsonSchemaReader -- JSON Schema and OpenAPI read as classes and enumerations")
class JsonSchemaReaderTest {

    private static ImportedModel read(String json) {
        return JsonSchemaReader.read(json.replace('`', '"'), null, List.of(), false);
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

    /** A document read with the options given; {@code documents} in JSON written with backquotes too. */
    private static ImportedModel read(String json, JsonSchemaReader.Options options) {
        return JsonSchemaReader.read(json.replace('`', '"'), options);
    }

    private static JsonSchemaReader.Options named(String rootName) {
        return JsonSchemaReader.Options.of(rootName, List.of(), false, null, Map.of());
    }

    /** Each class's rules and logic, {@code Class: rule; rule}, compactly: {@code p>=1}, {@code len(p)<=3},
     *  {@code p~/re/}, {@code p%5}, {@code p in [..]}, {@code unique(p)}, {@code p=>[q]}; a condition with {@code p?}
     *  for a property present, {@code !}, {@code &}, {@code |}. */
    private static List<String> rules(ImportedModel model) {
        List<String> out = new ArrayList<>();
        for (ImportedClass c : model.classes()) {
            List<String> all = new ArrayList<>();
            for (Rule r : c.rules()) {
                all.add(rule(r));
            }
            for (Logic l : c.logic()) {
                all.add(l.name() + "{" + condition(l.condition()) + "}");
            }
            if (!all.isEmpty()) {
                out.add(c.name() + ": " + String.join("; ", all));
            }
        }
        return out;
    }

    /** The notes as a person reads them: {@code where: message (n times)}. */
    private static List<String> notes(ImportedModel m) {
        return m.notes().stream().map(ImportedModel.Note::toString).toList();
    }

    /** Annotations as {@code profile.name} (a stereotype) or {@code profile.name=value} (a tag). */
    private static List<String> annotations(List<ImportedModel.Annotation> annotations) {
        return annotations.stream().map(a -> a.profile() + "." + a.name() + (a.value() == null ? "" : "=" + a.value()))
                .toList();
    }

    private static String rule(Rule r) {
        String p = (r.each() ? "each " : "") + r.property();
        return switch (r) {
            case Bound b -> p + (b.lower() ? ">" : "<") + (b.exclusive() ? "" : "=") + b.value().toPlainString();
            case Length l -> "len(" + p + ")" + (l.lower() ? ">=" : "<=") + l.value();
            case Pattern x -> p + "~/" + x.regex() + "/";
            case MultipleOf m -> p + "%" + m.value().toPlainString();
            case OneOf o -> p + " in " + o.values().stream().map(Json::toCompact).toList();
            case Unique u -> "unique(" + p + ")";
            case Requires q -> p + "=>" + q.others();
        };
    }

    private static String condition(Condition c) {
        return switch (c) {
            case Has h -> h.property() + "?";
            case Not n -> "!" + condition(n.condition());
            case Holds h -> rule(h.rule());
            case All a -> "(" + String.join(" & ", a.conditions().stream().map(JsonSchemaReaderTest::condition)
                    .toList()) + ")";
            case Any a -> "(" + String.join(" | ", a.conditions().stream().map(JsonSchemaReaderTest::condition)
                    .toList()) + ")";
        };
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
        assertEquals(List.of(), notes(m), "a list's bounds are its multiplicity's");
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
                classes(JsonSchemaReader.read(schema.formatted("").replace('`', '"'), "Person", List.of(), false)));
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
                  `lines`: {`type`: `array`, `items`: {`type`: `object`, `properties`: {`sku`: {`type`: `string`}}}},
                  `tax_ids`: {`type`: `array`, `items`: {`type`: `object`, `properties`: {`id`: {`type`: `string`}}}},
                  `tax_exempt`: {`type`: `string`, `enum`: [`none`, `exempt`]}}}}}""");
        assertEquals(List.of("Order{status: E OrderStatus[0..1], shipping: C OrderShipping[0..1], "
                        + "lines: C OrderLine[*], tax_ids: C OrderTaxId[*], tax_exempt: E OrderTaxExempt[0..1]}",
                "owned OrderShipping{city: P String[0..1]}", "owned OrderLine{sku: P String[0..1]}",
                "owned OrderTaxId{id: P String[0..1]}"), classes(m), "an inline object follows its owner, the "
                + "property's name in camel case; a list's item named in the singular");
        assertEquals(List.of("OrderStatus[open, closed]", "OrderTaxExempt[none, exempt]"), enumerations(m));
    }

    @Test
    @DisplayName("an object or an enum written in place twice, of one text, is one class or enumeration, named after the "
            + "first; a schema the document names is its own")
    void twinsWrittenInPlaceAreOne() {
        ImportedModel m = read("""
                {`$defs`: {
                  `Order`: {`type`: `object`, `properties`: {
                    `status`: {`type`: `string`, `enum`: [`open`, `closed`]},
                    `address`: {`type`: `object`, `properties`: {`city`: {`type`: `string`}}}}},
                  `Invoice`: {`type`: `object`, `properties`: {
                    `state`: {`type`: `string`, `enum`: [`open`, `closed`]},
                    `billing`: {`type`: `object`, `properties`: {`city`: {`type`: `string`}}},
                    `shipping`: {`type`: `object`, `description`: `where it goes`, `properties`: {
                      `city`: {`type`: `string`}}}}},
                  `City`: {`type`: `object`, `properties`: {`city`: {`type`: `string`}}}}}""");
        assertEquals(List.of("Order{status: E OrderStatus[0..1], address: C OrderAddress[0..1]}",
                "owned OrderAddress{city: P String[0..1]}",
                "Invoice{state: E OrderStatus[0..1], billing: C OrderAddress[0..1], shipping: C InvoiceShipping[0..1]}",
                "owned InvoiceShipping{city: P String[0..1]}", "City{city: P String[0..1]}"), classes(m),
                "billing is address's text: its class; shipping says more (a description): its own; City is named");
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
    @DisplayName("a JSON value of no one Pure type is a Variant, noted: several types, a union not of objects, a tuple")
    void valuesOfNoOneTypeAreVariants() {
        ImportedModel m = read("""
                {`$defs`: {`T`: {`type`: `object`, `properties`: {
                  `either`: {`oneOf`: [{`type`: `string`}, {`type`: `integer`}]},
                  `mixed`: {`type`: [`string`, `integer`]},
                  `pair`: {`type`: `array`, `items`: [{`type`: `string`}, {`type`: `integer`}]},
                  `grid`: {`type`: `array`, `items`: {`type`: `array`, `items`: {`type`: `number`}}},
                  `maybe`: {`anyOf`: [{`type`: `string`}, {`type`: `integer`}, {`type`: `null`}]}}}}}""");
        String variant = "meta::pure::metamodel::variant::Variant";
        assertEquals(List.of("T{either: P " + variant + "[0..1], mixed: P " + variant + "[0..1], pair: P " + variant
                + "[0..1], grid: P " + variant + "[0..1], maybe: P " + variant + "[0..1]}"), classes(m));
        assertEquals(List.of(
                "#/$defs/T/properties/either: a union (oneOf) of values that are not all objects is a Variant (Pure has "
                        + "no union type)",
                "#/$defs/T/properties/mixed: a value of several types (string, integer) is a Variant (Pure has no union "
                        + "type)",
                "#/$defs/T/properties/pair: a tuple (prefixItems, items as a list) is a Variant, the JSON list it is",
                "#/$defs/T/properties/grid: a list of lists is one Variant, the JSON list it is",
                "#/$defs/T/properties/maybe: a union (anyOf) of values that are not all objects is a Variant (Pure has "
                        + "no union type)"), notes(m));
    }

    @Test
    @DisplayName("a union of objects is inheritance: the union a superclass, each alternative a subclass of it")
    void unionsOfObjectsAreInheritance() {
        ImportedModel m = read("""
                {`openapi`: `3.1.0`, `components`: {`schemas`: {
                  `Pet`: {`oneOf`: [{`$ref`: `#/components/schemas/Cat`}, {`$ref`: `#/components/schemas/Dog`}],
                    `discriminator`: {`propertyName`: `petType`},
                    `properties`: {`name`: {`type`: `string`}}},
                  `Cat`: {`type`: `object`, `properties`: {`purrs`: {`type`: `boolean`}}},
                  `Dog`: {`allOf`: [{`$ref`: `#/components/schemas/Pet`},
                    {`type`: `object`, `properties`: {`barks`: {`type`: `boolean`}}}]},
                  `Payment`: {`type`: `object`, `properties`: {
                    `method`: {`oneOf`: [
                      {`title`: `card`, `type`: `object`, `properties`: {`last4`: {`type`: `string`}}},
                      {`type`: `object`, `properties`: {`iban`: {`type`: `string`}}},
                      {`type`: `null`}]},
                    `payee`: {`anyOf`: [{`$ref`: `#/components/schemas/Cat`}, {`$ref`: `#/components/schemas/Dog`}]}}}}}}""");
        assertEquals(List.of(
                "Pet{name: P String[0..1]}",
                "Cat[Pet, CatOrDog]{purrs: P Boolean[0..1]}",
                "Dog[Pet, CatOrDog]{barks: P Boolean[0..1]}",
                "Payment{method: C PaymentMethod[0..1], payee: C CatOrDog[0..1]}",
                "owned PaymentMethod{}",
                "owned PaymentMethodCard[PaymentMethod]{last4: P String[0..1]}",
                "owned PaymentMethodOption2[PaymentMethod]{iban: P String[0..1]}",
                "CatOrDog{}"), classes(m), "Dog's allOf and the union name Pet once; Cat gains it from the union; a "
                + "union written in place of schemas the document names is named after them");
        assertEquals(List.of("#/components/schemas/Payment/properties/payee: anyOf is read as oneOf (an instance is one "
                + "of its alternatives)"), notes(m));
    }

    @Test
    @DisplayName("a union of objects and values: each value a class wrapping it; the unions of the same alternatives one "
            + "superclass, named after its members; a union of no object a Variant")
    void unionsOfObjectsAndValuesWrapTheValues() {
        ImportedModel m = read("""
                {`openapi`: `3.1.0`, `components`: {`schemas`: {
                  `Card`: {`type`: `object`, `properties`: {`last4`: {`type`: `string`}}},
                  `Charge`: {`type`: `object`, `properties`: {
                    `source`: {`description`: `the card, or its id`, `nullable`: true, `anyOf`: [
                      {`type`: `string`, `maxLength`: 10}, {`$ref`: `#/components/schemas/Card`}]},
                    `tags`: {`oneOf`: [{`$ref`: `#/components/schemas/Card`},
                      {`type`: `array`, `items`: {`type`: `string`}}, {`type`: `string`, `format`: `date`}]}}},
                  `Refund`: {`type`: `object`, `properties`: {
                    `source`: {`description`: `the card refunded`, `anyOf`: [
                      {`type`: `string`, `maxLength`: 10}, {`$ref`: `#/components/schemas/Card`}]},
                    `either`: {`oneOf`: [{`type`: `string`}, {`type`: `integer`}]}}}}}}""");
        assertEquals(List.of(
                "Card[CardOrString, CardOrStringListOrStrictDate]{last4: P String[0..1]}",
                "Charge{source: C CardOrString[0..1], tags: C CardOrStringListOrStrictDate[0..1]}",
                "CardOrString{}",
                "owned CardString[CardOrString]{value: P String[1]}",
                "CardOrStringListOrStrictDate{}",
                "owned CardStringList[CardOrStringListOrStrictDate]{value: P String[*]}",
                "owned CardStrictDate[CardOrStringListOrStrictDate]{value: P StrictDate[1]}",
                "Refund{source: C CardOrString[0..1], either: P meta::pure::metamodel::variant::Variant[0..1]}"), classes(m),
                "Charge's and Refund's sources, described apart, are one union; the string a class of the union, as "
                        + "Pure says a union of a class and a primitive, and every reader of Pure reads");
        assertEquals(List.of("CardString: len(value)<=10"), rules(m), "the wrapped value keeps its rules");
        assertEquals(List.of(
                "#/components/schemas/Charge/properties/source: anyOf is read as oneOf (an instance is one of its "
                        + "alternatives)",
                "#/components/schemas/Refund/properties/either: a union (oneOf) of values that are not all objects is "
                        + "a Variant (Pure has no union type)"), notes(m));
    }

    @Test
    @DisplayName("a union and an allOf that name each other: the superclass closing the circle dropped, noted")
    void aCycleOfInheritanceIsBroken() {
        ImportedModel m = read("""
                {`$defs`: {
                  `A`: {`allOf`: [{`$ref`: `#/$defs/B`}], `oneOf`: [{`$ref`: `#/$defs/B`}, {`$ref`: `#/$defs/C`}]},
                  `B`: {`type`: `object`, `properties`: {`b`: {`type`: `string`}}},
                  `C`: {`type`: `object`, `properties`: {`c`: {`type`: `string`}}},
                  `D`: {`allOf`: [{`$ref`: `#/$defs/D`}, {`properties`: {`d`: {`type`: `string`}}}]}}}""");
        assertEquals(List.of("A{}", "B[A]{b: P String[0..1]}", "C[A]{c: P String[0..1]}", "D{d: P String[0..1]}"),
                classes(m));
        assertEquals(List.of("#/$defs/D/allOf/0: an allOf naming its own schema names no superclass, and is skipped",
                "A: a class that would be its own superclass (A extends B extends A) does not extend B"),
                notes(m).stream().sorted().toList());
    }

    @Test
    @DisplayName("lists asked as Variants: a list of primitives or enumeration values one Variant, a list of objects many")
    void listsAsVariants() {
        String json = """
                {`$defs`: {`Pet`: {`type`: `object`, `properties`: {
                  `photoUrls`: {`type`: `array`, `items`: {`type`: `string`}},
                  `moods`: {`type`: `array`, `items`: {`enum`: [`calm`, `wild`]}},
                  `tags`: {`type`: `array`, `items`: {`$ref`: `#/$defs/Tag`}}}},
                 `Tag`: {`type`: `object`, `properties`: {`name`: {`type`: `string`}}}}}""".replace('`', '"');
        assertEquals(List.of("Pet{photoUrls: P String[*], moods: E PetMood[*], tags: C Tag[*]}", "Tag{name: P "
                + "String[0..1]}"), classes(JsonSchemaReader.read(json, null, List.of(), false)));
        ImportedModel m = JsonSchemaReader.read(json, null, List.of(), true);
        String variant = "meta::pure::metamodel::variant::Variant";
        assertEquals(List.of("Pet{photoUrls: P " + variant + "[0..1], moods: P " + variant + "[0..1], tags: C Tag[*]}",
                "Tag{name: P String[0..1]}"), classes(m));
        assertEquals(List.of("#/$defs/Pet/properties/photoUrls: a list of String is one Variant, the JSON list a column "
                + "holds ('lists': 'variant')", "#/$defs/Pet/properties/moods: a list of PetMood is one Variant, the JSON "
                + "list a column holds ('lists': 'variant')"), notes(m));
    }

    @Test
    @DisplayName("what has no class of its own is a Variant or a rule, noted: a reference outside, a not, a number enum")
    void whatHasNoClassIsAVariantOrARule() {
        ImportedModel m = read("""
                {`$defs`: {`T`: {`type`: `object`, `properties`: {
                  `elsewhere`: {`$ref`: `other.json#/Thing`},
                  `never`: {`not`: {`type`: `string`}},
                  `missing`: {`$ref`: `#/$defs/Nope`},
                  `level`: {`$ref`: `#/$defs/Level`}}},
                 `Level`: {`type`: `integer`, `enum`: [1, 2]}}}""");
        String variant = "meta::pure::metamodel::variant::Variant";
        assertEquals(List.of("T{elsewhere: P " + variant + "[0..1], never: P " + variant + "[0..1], missing: P " + variant
                + "[0..1], level: P Integer[0..1]}"), classes(m));
        assertEquals(List.of("T: level in [1, 2]"), rules(m), "an enumeration of numbers: the numbers a rule keeps");
        assertEquals(List.of(
                "#/$defs/T/properties/elsewhere: the reference 'other.json#/Thing' names nothing in the documents "
                        + "given: a Variant (give the document it names in 'documents')",
                "#/$defs/T/properties/missing: the reference '#/$defs/Nope' names nothing in the documents given: a "
                        + "Variant (give the document it names in 'documents')",
                "#/$defs/T/properties/never/not: a not is not carried: Pure's constraint cannot say the type of a "
                        + "Variant"), notes(m).stream().sorted().toList());
    }

    @Test
    @DisplayName("a validation is a rule; a name Pure cannot hold is made one, and noted")
    void validationsAreRulesAndRenamesAreNoted() {
        ImportedModel m = read("""
                {`$defs`: {`trade_event`: {`type`: `object`, `properties`: {
                  `trade-id`: {`type`: `integer`, `minimum`: 0, `exclusiveMaximum`: 1000, `multipleOf`: 5},
                  `code`: {`type`: `string`, `pattern`: `^[A-Z]+$`, `minLength`: 2, `maxLength`: 8, `x-internal`: true},
                  `price`: {`type`: `number`, `minimum`: 0, `exclusiveMinimum`: true, `multipleOf`: 0.01},
                  `tags`: {`type`: `array`, `items`: {`type`: `string`, `minLength`: 1}, `uniqueItems`: true},
                  `side`: {`enum`: [`buy-side`, `sell`]}}}}}""");
        assertEquals(List.of("TradeEvent{tradeId: P Integer[0..1], code: P String[0..1], price: P Float[0..1], "
                + "tags: P String[*], side: E TradeEventSide[0..1]}"), classes(m));
        assertEquals(List.of("TradeEvent: tradeId>=0; tradeId<1000; tradeId%5; len(code)>=2; len(code)<=8; "
                + "code~/^[A-Z]+$/; price>0; len(each tags)>=1; unique(tags)"), rules(m),
                "draft 4's boolean exclusiveMinimum; a step that is not whole not carried");
        assertEquals(List.of("TradeEventSide[buy_side, sell]"), enumerations(m));
        assertEquals(List.of("#/$defs/trade_event/properties/price: multipleOf of a number that is not a whole number "
                + "is not carried"), notes(m), "a rename is no note: the name in the schema is carried");
        String name = "meta::json::schema::JSONSchemaGeneration.name";
        assertEquals(List.of(name + "=trade_event"), annotations(m.classes().get(0).annotations()));
        assertEquals(List.of(name + "=trade-id"), annotations(m.classes().get(0).properties().get(0).annotations()));
        assertEquals(List.of(List.of(name + "=buy-side"), List.of()), m.enumerations().get(0).valueAnnotations().stream()
                .map(JsonSchemaReaderTest::annotations).toList(), "as legend-engine's import keeps a renamed value's text");
    }

    @Test
    @DisplayName("a const, an enum of numbers or of mixed values: their type, the values a rule; a union of consts an "
            + "enumeration, each value documented, each named as x-enum-varnames says")
    void constsAndEnums() {
        ImportedModel m = read("""
                {`$defs`: {
                  `Kind`: {`oneOf`: [{`const`: `a`, `description`: `the first`}, {`const`: `b`, `title`: `the second`}]},
                  `Code`: {`type`: `integer`, `enum`: [200, 404], `x-enum-varnames`: [`Ok`, `NotFound`]},
                  `Status`: {`type`: `string`, `enum`: [`on hold`, `done`], `x-enum-varnames`: [`OnHold`, `Done`],
                    `markdownEnumDescriptions`: [`*waiting*`, ``]},
                  `T`: {`type`: `object`, `properties`: {
                    `version`: {`const`: 2},
                    `flag`: {`const`: true},
                    `mixed`: {`enum`: [`x`, 1]},
                    `type`: {`enum`: [`Feature`]},
                    `kind`: {`$ref`: `#/$defs/Kind`},
                    `status`: {`$ref`: `#/$defs/Status`},
                    `code`: {`$ref`: `#/$defs/Code`}}}}}""");
        assertEquals(List.of("T{version: P Integer[0..1], flag: P Boolean[0..1], mixed: P "
                + "meta::pure::metamodel::variant::Variant[0..1], type: P String[0..1], kind: E Kind[0..1], "
                + "status: E Status[0..1], code: P Integer[0..1]}"), classes(m));
        assertEquals(List.of("T: version in [2]; flag in [true]; type in [\"Feature\"]; code in [200, 404]"), rules(m),
                "an inline enum of one text a constant, not an enumeration");
        assertEquals(List.of("Kind[a, b]", "Status[OnHold, Done]"), enumerations(m));
        assertEquals(java.util.Arrays.asList("the first", "the second"), m.enumerations().get(0).valueDocs());
        assertEquals(List.of("on hold", "done"), m.enumerations().get(1).texts(), "a value spelled as the schema does");
        assertEquals(java.util.Arrays.asList("*waiting*", null), m.enumerations().get(1).valueDocs());
    }

    @Test
    @DisplayName("references: a JSON pointer anywhere, percent-encoded; an $anchor; an $id; draft 4's id; a document "
            + "given, by the name a reference spells")
    void referencesOfEveryKind() {
        ImportedModel m = read("""
                {`$id`: `https://example.com/root.json`, `$defs`: {
                  `T`: {`type`: `object`, `properties`: {
                    `byPointer`: {`$ref`: `#/$defs/Holder/properties/inner`},
                    `byEncoded`: {`$ref`: `#/$defs/a%20b~1c`},
                    `byAnchor`: {`$ref`: `#money`},
                    `byId`: {`$ref`: `https://example.com/address.json`},
                    `byDocument`: {`$ref`: `./common.json#/definitions/Tag`}}},
                  `Holder`: {`type`: `object`, `properties`: {`inner`: {`type`: `object`, `properties`: {
                    `x`: {`type`: `integer`}}}}},
                  `a b/c`: {`type`: `string`, `format`: `date`},
                  `Money`: {`$anchor`: `money`, `type`: `number`, `format`: `decimal`},
                  `Address`: {`$id`: `https://example.com/address.json`, `type`: `object`, `properties`: {
                    `city`: {`type`: `string`}}}}}""", JsonSchemaReader.Options.of(null, List.of(), false, null,
                Map.of("common.json", "{\"definitions\": {\"Tag\": {\"type\": \"object\", \"properties\": "
                        + "{\"name\": {\"type\": \"string\"}}}}}")));
        assertEquals(List.of("T{byPointer: C HolderInner[0..1], byEncoded: P StrictDate[0..1], byAnchor: P Decimal[0..1], "
                + "byId: C Address[0..1], byDocument: C Tag[0..1]}", "HolderInner{x: P Integer[0..1]}",
                "Address{city: P String[0..1]}", "Tag{name: P String[0..1]}", "Holder{inner: C HolderInner[0..1]}"),
                classes(m), "a place reached twice is one class, made where it is first reached");
        ImportedModel draft4 = read("""
                {`$schema`: `http://json-schema.org/draft-04/schema#`, `definitions`: {
                  `T`: {`type`: `object`, `properties`: {`a`: {`$ref`: `#addr`}, `b`: {`$ref`: `item.json`}}},
                  `A`: {`id`: `#addr`, `type`: `object`, `properties`: {`city`: {`type`: `string`}}},
                  `I`: {`id`: `item.json`, `type`: `object`, `properties`: {`n`: {`type`: `integer`}}}}}""",
                named(null));
        assertEquals(List.of("T{a: C A[0..1], b: C I[0..1]}", "A{city: P String[0..1]}", "I{n: P Integer[0..1]}"),
                classes(draft4));
    }

    @Test
    @DisplayName("boolean schemas: true any value, a Variant; false no value, no property; a required property is [1]")
    void booleanSchemas() {
        ImportedModel m = read("""
                {`$defs`: {`T`: {`type`: `object`, `required`: [`any`], `properties`: {
                  `any`: true, `none`: false, `list`: {`type`: `array`, `items`: true}}}}}""");
        String variant = "meta::pure::metamodel::variant::Variant";
        assertEquals(List.of("T{any: P " + variant + "[1], list: P " + variant + "[0..1]}"), classes(m));
        assertEquals(List.of("#/$defs/T/properties/none: a property that can hold no value is not a property",
                "#/$defs/T/properties/list: a list of any values is one Variant, the JSON list it is"), notes(m));
    }

    @Test
    @DisplayName("a root that is a list or a map: its item a class, its entries a class of key and value; a root that is "
            + "a reference alone, what it names")
    void rootsThatAreCollectionsOrReferences() {
        assertEquals(List.of("RowsItem{id: P Integer[1]}"), classes(read("""
                {`type`: `array`, `items`: {`type`: `object`, `required`: [`id`], `properties`: {
                  `id`: {`type`: `integer`}}}}""", named("rows"))));
        ImportedModel map = read("""
                {`title`: `Mime types`, `type`: `object`, `additionalProperties`: false,
                 `patternProperties`: {`^\\\\.[a-z]+$`: {`type`: `string`, `pattern`: `^[a-z]+/[a-z]+$`}}}""",
                named(null));
        assertEquals(List.of("MimeTypesEntry{key: P String[1], value: P String[1]}"), classes(map));
        assertEquals(List.of("MimeTypesEntry: key~/^\\.[a-z]+$/; value~/^[a-z]+/[a-z]+$/"), rules(map));
        ImportedModel values = read("""
                {`type`: `object`, `propertyNames`: {`maxLength`: 3},
                 `additionalProperties`: {`type`: `object`, `properties`: {`n`: {`type`: `integer`}}}}""",
                named("Counts"));
        assertEquals(List.of("CountsEntry{key: P String[1], value: C CountsValue[1]}", "owned CountsValue{n: P "
                + "Integer[0..1]}"), classes(values));
        assertEquals(List.of("CountsEntry: len(key)<=3"), rules(values));
        assertEquals(List.of("Tag{name: P String[0..1]}"), classes(read("""
                {`$ref`: `tag.json`}""", JsonSchemaReader.Options.of(null, List.of(), false, null, Map.of("tag.json",
                "{\"title\": \"Tag\", \"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}}")))));
        assertEquals(List.of("#: the schema is the reference 'https://example.com/s.json' alone, to a document not given: "
                + "give that document in 'documents'"), refusals("{`$ref`: `https://example.com/s.json`}"));
    }

    @Test
    @DisplayName("an OpenAPI document of no component schema: each operation's request and response bodies a class")
    void operationsBodies() {
        ImportedModel m = read("""
                {`openapi`: `3.0.0`, `paths`: {`/pets`: {
                  `post`: {`operationId`: `createPet`, `requestBody`: {`content`: {`application/json`: {`schema`: {
                    `type`: `object`, `properties`: {`name`: {`type`: `string`}}}}}},
                    `responses`: {`201`: {`content`: {`application/json`: {`schema`: {`type`: `array`, `items`: {
                      `type`: `object`, `properties`: {`id`: {`type`: `integer`}}}}}}}}},
                  `get`: {`responses`: {`200`: {`description`: `no body`}}}}}}""");
        assertEquals(List.of("CreatePetRequest{name: P String[0..1]}", "CreatePetResponseItem{id: P Integer[0..1]}"),
                classes(m));
    }

    @Test
    @DisplayName("defaults, deprecation and documentation: carried as the schema says them, VS Code's spellings too")
    void defaultsDeprecationAndDocs() {
        ImportedModel m = read("""
                {`$defs`: {`Mode`: {`enum`: [`fast`, `slow mode`]},
                  `T`: {`type`: `object`, `deprecated`: true, `markdownDescription`: `**the** T`,
                    `required`: [`retries`, `ratio`, `mode`, `on`], `properties`: {
                    `retries`: {`type`: `integer`, `default`: 3},
                    `ratio`: {`type`: `number`, `default`: 1},
                    `mode`: {`$ref`: `#/$defs/Mode`, `default`: `slow mode`},
                    `on`: {`type`: `boolean`, `default`: `yes`},
                    `old`: {`type`: `string`, `deprecationMessage`: `use mode`},
                    `size`: {`type`: `integer`, `default`: 10}}}}}""");
        ImportedClass t = m.classes().get(0);
        assertTrue(t.deprecated());
        assertEquals("**the** T", t.doc());
        List<String> props = new ArrayList<>();
        for (ImportedProperty p : t.properties()) {
            props.add(p.name() + (p.deprecated() ? " deprecated" : "") + (p.defaultValue() == null ? ""
                    : " = " + Json.toCompact(p.defaultValue())));
        }
        assertEquals(List.of("retries = 3", "ratio = 1", "mode = \"slow_mode\"", "on", "old deprecated", "size"),
                props, "a mandatory property's default its own, an enumeration's by its value's Pure name");
        assertEquals(List.of("meta::json::schema::JSONSchemaGeneration.defaultValue=yes"),
                annotations(t.properties().get(3).annotations()), "a default no Pure value: the schema's, a tag");
        assertEquals(List.of("meta::json::schema::JSONSchemaGeneration.defaultValue=10"),
                annotations(t.properties().get(5).annotations()), "an optional property's default a tag: legend-pure "
                        + "has a default only on a mandatory property");
        assertEquals(List.of(), notes(m));
    }

    @Test
    @DisplayName("keywords: another vocabulary's says nothing; a validation not carried is noted; an unknown one noted")
    void keywordsOfOtherVocabularies() {
        ImportedModel m = read("""
                {`$defs`: {`T`: {`type`: `object`, `minProperties`: 1, `abcIsValid`: true,
                  `defaultSnippets`: [], `javaType`: `x.T`, `properties`: {
                    `a`: {`type`: `string`, `errorMessage`: `bad`, `tsType`: `string`}}}}}""");
        assertEquals(List.of("#/$defs/T: minProperties is not carried (a validation Pure states no constraint for here)",
                "#/$defs/T: the keyword 'abcIsValid' is not JSON Schema's, and is not read"), notes(m));
        assertEquals(List.of("rule.not-carried", "keyword.unknown"), m.notes().stream().map(ImportedModel.Note::code)
                .toList(), "each note its code, for a reader of notes to group by");
    }

    @Test
    @DisplayName("conditions: their properties optional; a branch that forbids a property does not hide another's")
    void conditionsGatherOptionalProperties() {
        ImportedModel m = read("""
                {`$defs`: {`Base`: {`type`: `object`, `properties`: {`extra`: {`type`: `integer`}}},
                  `T`: {`type`: `object`, `required`: [`kind`], `properties`: {`kind`: {`enum`: [`a`, `b`]}},
                  `allOf`: [
                    {`if`: {`properties`: {`kind`: {`const`: `a`}}}, `then`: {`properties`: {`size`: {`type`: `null`}}},
                     `else`: {`properties`: {`size`: {`type`: `integer`}}, `required`: [`size`]}},
                    {`if`: {`properties`: {`kind`: {`const`: `b`}}}, `then`: {`$ref`: `#/$defs/Base`}}]}}}""");
        assertEquals(List.of("Base{extra: P Integer[0..1]}", "T{kind: E TKind[1], size: P Integer[0..1], extra: P "
                + "Integer[0..1]}"), classes(m), "a reference inside a condition no superclass: its properties optional");
        assertEquals(List.of("T: condition{((!size? | !kind in [\"a\"]) & (kind in [\"a\"] | size?))}"), rules(m),
                "kind is required: of it, its value alone; a presence test before the rest");
    }

    @Test
    @DisplayName("logic over a whole value: if/then/else, not, a union of requirements, dependencies -- each a condition")
    void logicIsConditions() {
        ImportedModel m = read("""
                {`$defs`: {`Payment`: {`type`: `object`, `required`: [`method`],
                  `properties`: {
                    `method`: {`enum`: [`card`, `bank`]},
                    `card`: {`type`: `string`}, `iban`: {`type`: `string`}, `cvv`: {`type`: `string`},
                    `email`: {`type`: `string`}, `phone`: {`type`: `string`},
                    `amount`: {`type`: `number`, `not`: {`const`: 0}},
                    `kind`: {`type`: `integer`}},
                  `if`: {`properties`: {`method`: {`const`: `card`}}}, `then`: {`required`: [`card`]},
                  `not`: {`required`: [`card`, `iban`]},
                  `oneOf`: [{`required`: [`email`]}, {`required`: [`phone`]}],
                  `dependentRequired`: {`card`: [`cvv`]},
                  `dependentSchemas`: {`iban`: {`properties`: {`kind`: {`minimum`: 1}}}}}}}""");
        assertEquals(List.of("Payment: card=>[cvv]; "
                + "condition{(!method in [\"card\"] | card?)}; "
                + "exclusion{!(card? & iban?)}; "
                + "oneOf{((email? & !phone?) | (phone? & !email?))}; "
                + "ibanDependency{(!iban? | !kind? | kind>=1)}; "
                + "amountNot{(!amount? | !amount in [0])}"), rules(m), "a required property's presence is always");
        assertEquals(List.of(), notes(m));
    }

    @Test
    @DisplayName("logic into an object-typed property, by its path; logic Pure's constraint cannot say is noted, not "
            + "carried")
    void logicThroughPathsAndWhatCannotBeSaid() {
        ImportedModel m = read("""
                {`$defs`: {`Job`: {`type`: `object`, `properties`: {
                  `config`: {`type`: `object`, `properties`: {`mode`: {`enum`: [`batch`, `stream`]}}},
                  `schedule`: {`type`: `string`}, `any`: {}},
                  `allOf`: [
                    {`if`: {`properties`: {`config`: {`properties`: {`mode`: {`const`: `batch`}},
                      `required`: [`mode`]}}, `required`: [`config`]}, `then`: {`required`: [`schedule`]}},
                    {`if`: {`properties`: {`any`: {`const`: 1}}}, `then`: {`required`: [`schedule`]}},
                    {`if`: {`required`: [`nope`]}, `then`: {`required`: [`schedule`]}}]}}}""");
        assertEquals(List.of("Job: condition{(!(config.mode? & config? & config.mode in [\"batch\"]) | schedule?)}"),
                rules(m));
        assertEquals(List.of(
                "#/$defs/Job/allOf/1: a condition (if/then/else) is not carried: Pure's constraint cannot say a const "
                        + "or enum of a Variant",
                "#/$defs/Job/allOf/2: a condition (if/then/else) is not carried: Pure's constraint cannot say 'nope', "
                        + "not a property of the class"), notes(m));
    }

    @Test
    @DisplayName("schemas chosen: those named and every schema they reach, the rest not read")
    void chosenSchemasBringWhatTheyReach() {
        String json = """
                {`openapi`: `3.1.0`, `components`: {`schemas`: {
                  `Trade`: {`type`: `object`, `properties`: {`customer`: {`$ref`: `#/components/schemas/Customer`}}},
                  `Customer`: {`type`: `object`, `properties`: {`id`: {`type`: `integer`}}},
                  `Unrelated`: {`type`: `object`, `properties`: {`x`: {`type`: `string`}}}}}}"""
                .replace('`', '"');
        assertEquals(List.of("Trade{customer: C Customer[0..1]}", "Customer{id: P Integer[0..1]}"),
                classes(JsonSchemaReader.read(json, null, List.of("Trade"), false)));
        SchemaImportException e = assertThrows(SchemaImportException.class,
                () -> JsonSchemaReader.read(json, null, List.of("Nope"), false));
        assertEquals(List.of("the schema has no 'Nope' to import"), e.reasons());
        assertEquals(3, JsonSchemaReader.read(json, null, List.of(), false).classes().size(), "all of it, read");
    }

    @Test
    @DisplayName("a YAML document reads as the same document in JSON does; a document that is neither is refused")
    void yamlReadsAsJson() {
        String yaml = """
                openapi: 3.0.3
                components:
                  schemas:
                    Order:
                      type: object
                      required: [id]
                      properties:
                        id: {type: integer, format: int64}
                        status:
                          type: string
                          description: >-
                            Order
                            status
                          enum:
                            - placed
                            - delivered
                        lines:
                          type: array
                          items:
                            $ref: '#/components/schemas/Line'
                    Line:
                      type: object
                      properties:
                        sku: {type: string}
                """;
        String json = """
                {`openapi`: `3.0.3`, `components`: {`schemas`: {
                  `Order`: {`type`: `object`, `required`: [`id`], `properties`: {
                    `id`: {`type`: `integer`, `format`: `int64`},
                    `status`: {`type`: `string`, `description`: `Order status`, `enum`: [`placed`, `delivered`]},
                    `lines`: {`type`: `array`, `items`: {`$ref`: `#/components/schemas/Line`}}}},
                  `Line`: {`type`: `object`, `properties`: {`sku`: {`type`: `string`}}}}}}""";
        ImportedModel fromYaml = JsonSchemaReader.read(yaml, null, List.of(), false);
        assertEquals(read(json), fromYaml);
        assertEquals(List.of("Order{id: P Integer[1], status: E OrderStatus[0..1], lines: C Line[*]}",
                "Line{sku: P String[0..1]}"), classes(fromYaml));
        assertEquals(List.of("the schema is not YAML: YAML line 2, column 1: ',' or ']' in a flow sequence"),
                refusals("openapi: [3.0.0\n"));
    }
}
