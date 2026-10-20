// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.legend.error.LegendCompileException;
import com.legend.json.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * importSchema against schemas no one would write (docs/MODEL_IMPORT_STUDY_2026_10_20.md §4.6): each of {@link #CASES}
 * documents, made from a fixed seed by a generator of every keyword the reader reads (and some it does not), with
 * names Pure cannot hold, references to nothing and to themselves, unions of anything, conditions over properties the
 * class does not have, patterns RE2 cannot run. Each is imported, or refused by name; never a crash, and never a model
 * that does not compile (the writer's "importer's defect"), which the import itself checks by compiling what it wrote.
 */
@DisplayName("importSchema fuzzed -- every document imported, or refused by name")
class ImportSchemaFuzzTest {

    private static final int CASES = 250;

    private static final List<String> NAMES = List.of("id", "name", "a-b", "1x", "true", "class", "é", "a.b", "x y",
            "Name", "name_2", "$ref", "type", "properties", "UPPER", "value", "key", "_");

    private static final List<String> PATTERNS = List.of("^[a-z]+$", "^(?!x)", "(a)\\1", "\\d{2,}", "[^]", "\\u00e9",
            "\\p{L}+", ".{1,2000}", "[abc", "^\\A$", "(?<n>a)\\k<n>");

    private final Random random = new Random(20261020L);

    @Test
    @DisplayName("each generated document imports, or is refused by name")
    void neverCrashesNeverWritesWhatDoesNotCompile() {
        int imported = 0;
        int refused = 0;
        List<String> failures = new ArrayList<>();
        for (int i = 0; i < CASES; i++) {
            boolean openApi = random.nextInt(4) == 0;
            String doc = Json.toCompact(openApi ? openApi() : document());
            String request = "{\"project\": {\"_type\": \"text\", \"code\": \"\"}, \"package\": \"fz\", \"schema\": "
                    + "{\"format\": \"" + (openApi ? "openApi" : "jsonSchema") + "\", \"name\": \"Root\", \"content\": "
                    + Json.toCompact(doc) + "}}";
            try {
                ModelChanges.importSchema(Json.parseObject(request));
                imported++;
            } catch (LegendCompileException e) {
                if (e.getMessage().contains("the schema cannot be imported")) {
                    refused++;
                } else {
                    failures.add(e.getMessage() + "\n  in " + doc);
                }
            } catch (RuntimeException | StackOverflowError e) {
                failures.add(e + "\n  in " + doc);
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + CASES + " documents failed:\n" + String.join("\n", failures.subList(0,
                    Math.min(5, failures.size()))));
        }
        assertTrue(imported > CASES / 2, "most import (" + imported + " imported, " + refused + " refused)");
    }

    private Map<String, Object> document() {
        Map<String, Object> doc = new LinkedHashMap<>();
        Map<String, Object> defs = new LinkedHashMap<>();
        int n = 1 + random.nextInt(5);
        for (int i = 0; i < n; i++) {
            defs.put(pick(List.of("D" + i, "d-" + i, i + "d", "D" + i)), schema(0, n));
        }
        doc.put("$defs", defs);
        if (random.nextBoolean()) {
            doc.putAll(object(0, n));
        }
        return doc;
    }

    /** An OpenAPI document: component schemas, or none and operations' bodies. */
    private Map<String, Object> openApi() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("openapi", "3.0.3");
        Map<String, Object> schemas = new LinkedHashMap<>();
        if (random.nextBoolean()) {
            for (int i = 0; i < 1 + random.nextInt(4); i++) {
                schemas.put("D" + i, schema(0, 3));
            }
            Map<String, Object> pet = object(1, 3);
            pet.put("discriminator", Map.of("propertyName", pick(NAMES), "mapping", Map.of("x",
                    "#/components/schemas/D0", "y", "#/components/schemas/Nope")));
            pet.put("oneOf", List.of(Map.of("$ref", "#/components/schemas/D0"),
                    Map.of("$ref", "#/components/schemas/D1")));
            schemas.put("Pet", pet);
        }
        doc.put("components", Map.of("schemas", schemas));
        doc.put("paths", Map.of("/x/{id}", Map.of(pick(List.of("get", "post", "put")), Map.of(
                "operationId", pick(List.of("getX", "1op", "do-it")),
                "requestBody", Map.of("content", Map.of("application/json", Map.of("schema", schema(1, 3)))),
                "responses", Map.of("200", Map.of("content", Map.of("application/json", Map.of("schema",
                        schema(1, 3)))))))));
        return doc;
    }

    private Object schema(int depth, int defs) {
        if (depth > 3) {
            return primitive();
        }
        return switch (random.nextInt(20)) {
            case 0, 1, 2 -> primitive();
            case 3, 4 -> object(depth, defs);
            case 5 -> Map.of("type", "array", "items", schema(depth + 1, defs), "minItems", random.nextInt(3),
                    "uniqueItems", random.nextBoolean());
            case 6 -> Map.of("$ref", "#/$defs/" + pick(List.of("D0", "D1", "D9", "d-0", "0d", "D0/properties/id")));
            case 7 -> Map.of(pick(List.of("oneOf", "anyOf", "allOf")), list(depth, defs));
            case 8 -> random.nextBoolean();
            case 9 -> Map.of("enum", List.of("a-b", "a-b", "", " ", "x", 1, true));
            case 10 -> Map.of("const", pick(List.of("c", "1", "true")));
            case 11 -> Map.of("type", List.of("string", "null", "integer"));
            case 12 -> Map.of("not", schema(depth + 1, defs));
            // malformed: a keyword of the wrong kind
            case 13 -> Map.of(pick(List.of("properties", "required", "enum", "items", "allOf", "type", "pattern",
                    "minimum", "$ref", "discriminator")), pick(List.<Object>of(List.of(1, "a"), "s", 3, Map.of("k", 1),
                    true)));
            case 14 -> Map.of("$ref", pick(List.of("#", "#/", "#/$defs", "#a", "#/$defs/D0/items",
                    "http://x/y.json#/z", "", "%%")));
            case 15 -> Map.of("$anchor", "a", "$id", pick(List.of("https://e.x/s", "#a", "")), "type", "object",
                    "properties", Map.of("self", Map.of("$ref", "#a")));
            case 16 -> Map.of("type", "array", "prefixItems", list(depth, defs), "items", random.nextBoolean());
            case 17 -> Map.of("enum", List.of("a", "b"), "x-enum-varnames", List.of("A"),
                    "x-enum-descriptions", List.of("d1", "d2", "d3"));
            case 18 -> Map.of("type", "object", "required", List.of("a"), "properties", Map.of("a",
                    Map.of("$ref", "#/$defs/D0")), "readOnly", true, "title", pick(NAMES), "examples",
                    List.of(Map.of("a", 1)), "deprecated", true, "deprecationMessage", "x");
            default -> Map.of("additionalProperties", schema(depth + 1, defs), "patternProperties",
                    Map.of(pick(PATTERNS), schema(depth + 1, defs)));
        };
    }

    private Map<String, Object> object(int depth, int defs) {
        Map<String, Object> o = new LinkedHashMap<>();
        o.put("type", "object");
        Map<String, Object> props = new LinkedHashMap<>();
        int n = random.nextInt(5);
        for (int i = 0; i < n; i++) {
            props.put(pick(NAMES), schema(depth + 1, defs));
        }
        o.put("properties", props);
        o.put("required", List.of(pick(NAMES), pick(NAMES)));
        switch (random.nextInt(6)) {
            case 0 -> {
                o.put("if", Map.of("properties", Map.of(pick(NAMES), Map.of("const", "x"))));
                o.put("then", Map.of("required", List.of(pick(NAMES))));
                o.put("else", Map.of("properties", Map.of(pick(NAMES), schema(depth + 1, defs))));
            }
            case 1 -> o.put("dependentRequired", Map.of(pick(NAMES), List.of(pick(NAMES))));
            case 2 -> o.put("oneOf", List.of(Map.of("required", List.of(pick(NAMES))),
                    Map.of("required", List.of(pick(NAMES)))));
            case 3 -> o.put("allOf", List.of(Map.of("$ref", "#/$defs/D0"), schema(depth + 1, defs)));
            case 4 -> o.put("dependentSchemas", Map.of(pick(NAMES), schema(depth + 1, defs)));
            default -> o.put("additionalProperties", random.nextBoolean());
        }
        return o;
    }

    private Map<String, Object> primitive() {
        Map<String, Object> p = new LinkedHashMap<>();
        String type = pick(List.of("string", "integer", "number", "boolean"));
        p.put("type", type);
        switch (type) {
            case "string" -> {
                p.put("format", pick(List.of("date", "date-time", "time", "uuid", "email", "x")));
                p.put("pattern", pick(PATTERNS));
                p.put("minLength", random.nextInt(4));
                p.put("default", pick(List.of("d", "2026-01-01")));
            }
            case "integer" -> {
                p.put("minimum", pick(List.of(0, -5, 1e20)));
                p.put("multipleOf", pick(List.of(2, 0.5)));
                p.put("format", pick(List.of("int32", "int64")));
                p.put("default", pick(List.of(1, "one", 1.5)));
            }
            case "number" -> {
                p.put("exclusiveMaximum", pick(List.of(10, true)));
                p.put("maximum", 100);
            }
            default -> p.put("default", pick(List.of(true, "no")));
        }
        p.put("nullable", random.nextBoolean());
        return p;
    }

    private List<Object> list(int depth, int defs) {
        List<Object> out = new ArrayList<>();
        int n = 1 + random.nextInt(3);
        for (int i = 0; i < n; i++) {
            out.add(schema(depth + 1, defs));
        }
        return out;
    }

    private <T> T pick(List<T> options) {
        return options.get(random.nextInt(options.size()));
    }
}
