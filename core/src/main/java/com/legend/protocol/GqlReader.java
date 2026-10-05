// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.Gql;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The GraphQL AST wire read back -- the mirror of {@link GqlEmitter}, rule for rule: a bare-selection
 * operation has no {@code type}, an object type's {@code directives} is always empty, an input value
 * carries {@code defaultValue} only when it has one.
 */
final class GqlReader {

    private GqlReader() {
    }

    static Gql.Document document(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL document");
        w.constant("_type", "executableDocument");
        return w.done(new Gql.Document(w.list("definitions", GqlReader::definition)));
    }

    private static final Map<String, Function<Wire, Gql.Definition>> DEFINITIONS = Map.of(
            "operationDefinition", GqlReader::operation,
            "fragmentDefinition", w -> new Gql.Fragment(w.str("name"), w.str("typeCondition"),
                    directives(w), selections(w, "selectionSet")),
            "objectTypeDefinition", GqlReader::objectType,
            "schemaDefinition", w -> new Gql.SchemaDef(directives(w),
                    w.list("rootOperationTypeDefinitions", GqlReader::rootOp)),
            "directiveDefinition", w -> new Gql.DirectiveDef(w.str("name"),
                    w.list("argumentDefinitions", GqlReader::inputValue),
                    w.strings("executableLocation"), w.strings("typeSystemLocation")),
            "scalarTypeDefinition", w -> new Gql.ScalarType(w.str("name"), directives(w)),
            "interfaceTypeDefinition", w -> new Gql.InterfaceType(w.str("name"), directives(w),
                    w.list("fields", GqlReader::fieldDef), w.strings("_implements")),
            "unionTypeDefinition", w -> new Gql.UnionType(w.str("name"), directives(w), w.strings("members")),
            "enumTypeDefinition", w -> new Gql.EnumType(w.str("name"), directives(w),
                    w.list("values", GqlReader::enumValueDef)),
            "inputObjectTypeDefinition", w -> new Gql.InputObjectType(w.str("name"), directives(w),
                    w.list("fields", GqlReader::inputValue)));

    private static Gql.Definition definition(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL definition");
        return w.done(Wire.rule(DEFINITIONS, w.type(), "GraphQL definition").apply(w));
    }

    private static Gql.Definition operation(Wire w) {
        return new Gql.Operation(w.optStr("type"), w.optStr("name"), w.list("variables", GqlReader::variableDef),
                directives(w), selections(w, "selectionSet"));
    }

    private static Gql.Definition objectType(Wire w) {
        w.emptyArray("directives");
        return new Gql.ObjectType(w.str("name"), w.list("fields", GqlReader::fieldDef), w.strings("_implements"));
    }

    private static Gql.RootOp rootOp(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL root operation");
        return w.done(new Gql.RootOp(w.str("operationType"), w.str("type")));
    }

    private static Gql.EnumValueDef enumValueDef(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL enum value");
        return w.done(new Gql.EnumValueDef(w.str("value"), directives(w)));
    }

    private static Gql.FieldDef fieldDef(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL field definition");
        return w.done(new Gql.FieldDef(w.str("name"), typeRef(w.take("type")),
                w.list("argumentDefinitions", GqlReader::inputValue), directives(w)));
    }

    private static Gql.InputValueDef inputValue(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL input value");
        Json.Node dv = w.opt("defaultValue");
        return w.done(new Gql.InputValueDef(w.str("name"), typeRef(w.take("type")),
                dv == null ? null : value(dv), directives(w)));
    }

    private static Gql.VariableDef variableDef(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL variable definition");
        Json.Node dv = w.opt("defaultValue");
        w.emptyArray("directives");
        return w.done(new Gql.VariableDef(w.str("name"), typeRef(w.take("type")), dv == null ? null : value(dv)));
    }

    private static List<Gql.Selection> selections(Wire w, String key) {
        return w.list(key, GqlReader::selection);
    }

    private static Gql.Selection selection(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL selection");
        String type = w.type();
        if ("field".equals(type)) {
            return w.done(new Gql.Field(w.optStr("alias"), w.str("name"), arguments(w, "arguments"), directives(w),
                    selections(w, "selectionSet")));
        }
        if ("fragmentSpread".equals(type)) {
            return w.done(new Gql.FragmentSpread(w.str("name"), directives(w)));
        }
        throw Wire.refuse("no reader rule for GraphQL selection _type '" + type + "'");
    }

    private static List<Gql.Argument> arguments(Wire w, String key) {
        return w.list(key, GqlReader::argument);
    }

    private static Gql.Argument argument(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL argument");
        return w.done(new Gql.Argument(w.str("name"), value(w.take("value"))));
    }

    private static List<Gql.Directive> directives(Wire w) {
        return w.list("directives", GqlReader::directive);
    }

    private static Gql.Directive directive(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL directive");
        return w.done(new Gql.Directive(w.str("name"), arguments(w, "arguments")));
    }

    private static Gql.TypeRef typeRef(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL type reference");
        String type = w.type();
        if ("namedTypeReference".equals(type)) {
            return w.done(new Gql.NamedType(w.str("name"), w.bool("nullable")));
        }
        if ("listTypeReference".equals(type)) {
            return w.done(new Gql.ListType(typeRef(w.take("itemType")), w.bool("nullable")));
        }
        throw Wire.refuse("no reader rule for GraphQL type reference _type '" + type + "'");
    }

    private static final Map<String, Function<Wire, Gql.Value>> VALUES = Map.of(
            "intValue", w -> new Gql.IntValue(w.lng("value")),
            "floatValue", w -> new Gql.FloatValue(w.decimal("value").doubleValue()),
            "stringValue", w -> new Gql.StringValue(w.str("value")),
            "booleanValue", w -> new Gql.BooleanValue(w.bool("value")),
            "nullValue", w -> new Gql.NullValue(),
            "enumValue", w -> new Gql.EnumValue(w.str("value")),
            "variable", w -> new Gql.VariableRef(w.str("name")),
            "listValue", w -> new Gql.ListValue(w.list("values", GqlReader::value)),
            "objectValue", w -> new Gql.ObjectValue(arguments(w, "fields")));

    private static Gql.Value value(Json.Node node) {
        Wire w = Wire.of(node, "GraphQL value");
        return w.done(Wire.rule(VALUES, w.type(), "GraphQL value").apply(w));
    }
}
