// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The small tail-section elements read back -- the mirror of {@link TailEmitter}'s {@code text},
 * {@code generationSpecification}, {@code fileGeneration}, {@code deephavenStore},
 * {@code elasticsearch7Store}, {@code MongoDatabase}, {@code externalFormatSchemaSet}, {@code binding},
 * the ServiceStore {@code serviceStore} and the {@code diagram}. The engine serializer's DOUBLED keys
 * ({@code _type}, {@code _pure_protocol_type}) arrive once (a JSON object keeps one of each) and are
 * written twice again by the emitter.
 */
final class TailReader {

    private TailReader() {
    }

    static Protocol.Element text(Wire w) {
        return new Protocol.PText(w.str("package"), w.str("name"), w.optStr("type"), w.str("content"), w.span());
    }

    static Protocol.Element generationSpecification(Wire w) {
        return new Protocol.PGenerationSpecification(w.str("package"), w.str("name"),
                w.list("generationNodes", n -> {
                    Wire g = Wire.of(n, "generation node");
                    // an id left out is the element, as the grammar defaults it; the engine's printer prints no id
                    // for either (GenerationGrammarComposerExtension)
                    String element = g.str("generationElement");
                    String id = g.optStr("id");
                    return g.done(new Protocol.PGenerationNode(element, id == null ? element : id, g.span()));
                }),
                w.list("fileGenerations", n -> {
                    Wire p = Wire.of(n, "file generation pointer");
                    p.constant("type", "FILE_GENERATION");
                    return p.done(new Protocol.PPointer("FILE_GENERATION", p.str("path"), p.span()));
                }), w.span());
    }

    static Protocol.Element fileGeneration(Wire w) {
        return new Protocol.PFileGeneration(w.str("package"), w.str("name"), w.str("type"),
                w.span("typeSourceInformation"), w.optStr("generationOutputPath"), w.strings("scopeElements"),
                w.list("configurationProperties", TailReader::configProperty), w.span());
    }

    private static Protocol.PConfigProperty configProperty(Json.Node node) {
        Wire p = Wire.of(node, "configuration property");
        return p.done(new Protocol.PConfigProperty(p.str("name"), configValue(p.take("value")), p.span()));
    }

    /**
     * A config value as the engine's {@code ConfigurationProperty.ValueDeserializer} reads it: an integer, a
     * boolean, a list of strings, a map of strings, and anything else as its text -- a string, and also a decimal
     * (its token as written). A {@code null} never reaches that deserializer: the value is Java null. A list or map
     * holding anything but strings the engine refuses.
     */
    private static Protocol.PConfigValue configValue(Json.Node v) {
        if (v instanceof Json.Str s) {
            return new Protocol.PConfigValue.PCString(s.value());
        }
        if (v instanceof Json.Null) {
            return new Protocol.PConfigValue.PCNull();
        }
        if (v instanceof Json.Num n && !n.isInteger()) {
            // the token as written: the engine's getText (the parser keeps every number's token)
            return new Protocol.PConfigValue.PCString(n.token() != null ? n.token() : Double.toString(n.doubleValue()));
        }
        if (v instanceof Json.Bool b) {
            return new Protocol.PConfigValue.PCBoolean(b.value());
        }
        if (v instanceof Json.Num n && n.isInteger()) {
            return new Protocol.PConfigValue.PCInteger(n.longValue());
        }
        if (v instanceof Json.Arr a) {
            List<String> out = new ArrayList<>();
            for (Json.Node i : a.items()) {
                if (!(i instanceof Json.Str s)) {
                    throw Wire.refuse("a configuration list item that is not a string: " + Wire.abbreviate(i));
                }
                out.add(s.value());
            }
            return new Protocol.PConfigValue.PCStrings(out);
        }
        if (v instanceof Json.Obj o) {
            LinkedHashMap<String, String> out = new LinkedHashMap<>();
            for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
                if (!(e.getValue() instanceof Json.Str s)) {
                    throw Wire.refuse("a configuration map value that is not a string: " + e.getKey());
                }
                out.put(e.getKey(), s.value());
            }
            return new Protocol.PConfigValue.PCMap(out);
        }
        throw Wire.refuse("no reader rule for a configuration value " + Wire.abbreviate(v));
    }

    // ---------------------------------------------------------------------
    // Stores
    // ---------------------------------------------------------------------

    static Protocol.Element deephavenStore(Wire w) {
        w.emptyArray("includedStores");
        return new Protocol.PDeephavenDatabase(w.str("package"), w.str("name"), w.list("tables", n -> {
            Wire t = Wire.of(n, "Deephaven table");
            return t.done(new Protocol.PDeephavenDatabase.PDeephavenTable(t.str("name"),
                    t.list("columns", TailReader::deephavenColumn)));
        }), w.span());
    }

    /** A column's type: its {@code _type} (written twice) is the kind; precision and scale come together. */
    private static Protocol.PDeephavenColumn deephavenColumn(Json.Node node) {
        Wire c = Wire.of(node, "Deephaven column");
        c.constant("_type", "column");
        Wire t = c.obj("type");
        String kind = t.type();
        if (kind == null) {
            throw Wire.refuse("a Deephaven column type without its _type");
        }
        Integer precision = t.optInt("precision");
        Integer scale = t.optInt("scale");
        if ((precision == null) != (scale == null)) {
            throw Wire.refuse("a Deephaven column with one of precision/scale (the wire writes both or neither)");
        }
        t.done(kind);
        return c.done(new Protocol.PDeephavenColumn(c.str("name"), kind, precision, scale));
    }

    static Protocol.Element elasticsearchStore(Wire w) {
        w.emptyArray("includedStores");
        return new Protocol.PElasticsearch7Cluster(w.str("package"), w.str("name"), w.list("indices", n -> {
            Wire i = Wire.of(n, "Elasticsearch index");
            return i.done(new Protocol.PElasticsearch7Cluster.PEsIndex(i.str("indexName"),
                    i.list("properties", TailReader::esIndexProperty)));
        }), w.span());
    }

    private static Protocol.PElasticsearch7Cluster.PEsProperty esIndexProperty(Json.Node node) {
        Wire p = Wire.of(node, "Elasticsearch index property");
        String name = p.str("propertyName");
        return p.done(esProperty(name, p.take("property")));
    }

    /**
     * {@code {"<wireKey>":{"_pure_protocol_type":..,"fields"?:{..},"properties"?:{..},"type":..}}}: the one key
     * is the property's wire key; the child maps are keyed by property name (written sorted).
     */
    private static Protocol.PElasticsearch7Cluster.PEsProperty esProperty(String name, Json.Node node) {
        Wire holder = Wire.of(node, "Elasticsearch property " + name);
        if (holder.json().fields().size() != 1) {
            throw Wire.refuse("an Elasticsearch property holder with " + holder.json().fields().size() + " keys");
        }
        String wireKey = holder.json().fields().keySet().iterator().next();
        Wire body = holder.obj(wireKey);
        holder.done(body);
        String protocolType = body.str("_pure_protocol_type");
        List<Protocol.PElasticsearch7Cluster.PEsProperty> fields = esChildren(body, "fields");
        List<Protocol.PElasticsearch7Cluster.PEsProperty> children = esChildren(body, "properties");
        return body.done(new Protocol.PElasticsearch7Cluster.PEsProperty(name, wireKey, protocolType,
                body.str("type"), fields, children));
    }

    private static @com.legend.base.Nullable List<Protocol.PElasticsearch7Cluster.PEsProperty> esChildren(Wire body,
            String key) {
        Wire m = body.optObj(key);
        if (m == null) {
            return null;
        }
        List<Protocol.PElasticsearch7Cluster.PEsProperty> out = new ArrayList<>();
        for (String child : m.json().fields().keySet()) {
            out.add(esProperty(child, m.take(child)));
        }
        return m.done(out);
    }

    static Protocol.Element mongoDatabase(Wire w) {
        w.emptyArray("includedStores");
        w.emptyArray("views");
        return new Protocol.PMongoDatabase(w.str("package"), w.str("name"), w.list("collections", n -> {
            Wire c = Wire.of(n, "MongoDB collection");
            Wire v = c.obj("validator");
            String action = v.str("validationAction");
            String level = v.str("validationLevel");
            Wire e = v.obj("validatorExpression");
            e.constant("_type", "jsonSchemaExpression");
            Protocol.PBsonSchema schema = e.done(bson(e.take("schemaExpression")));
            v.done(schema);
            return c.done(new Protocol.PMongoDatabase.PMongoCollection(c.str("name"), level, action, schema));
        }), w.span());
    }

    /** A BSON schema node: object kinds carry properties/required/title; arrays carry their defaults. */
    private static Protocol.PBsonSchema bson(Json.Node node) {
        Wire s = Wire.of(node, "BSON schema");
        String type = s.type();
        if (type == null) {
            throw Wire.refuse("a BSON schema node without its _type");
        }
        boolean object = "schema".equals(type) || "objectType".equals(type);
        boolean array = "arrayType".equals(type);
        s.emptyArray("_enum");
        s.emptyArray("allOf");
        s.emptyArray("anyOf");
        s.emptyArray("oneOf");
        if (array) {
            s.constant("additionalItemsAllowed", false);
            s.constant("uniqueItems", false);
        }
        Boolean additional = null;
        List<Map.Entry<String, Protocol.PBsonSchema>> properties = new ArrayList<>();
        List<String> required = List.of();
        String title = null;
        if (object) {
            additional = s.bool("additionalPropertiesAllowed") ? Boolean.TRUE : null;
            for (Json.Node p : s.arr("properties")) {
                Wire e = Wire.of(p, "BSON property");
                properties.add(e.done(Map.entry(e.str("key"), bson(e.take("value")))));
            }
            required = s.strings("required");
            title = s.optStr("title");
        }
        return s.done(new Protocol.PBsonSchema(type, additional, properties, required, title, s.optStr("description"),
                s.optLong("minLength"), s.optLong("maxLength"), s.optList("items", TailReader::bson)));
    }

    static Protocol.Element schemaSet(Wire w) {
        return new Protocol.PSchemaSet(w.str("package"), w.str("name"), w.str("format"), w.list("schemas", n -> {
            Wire s = Wire.of(n, "schema");
            return s.done(new Protocol.PSchema(s.optStr("id"), s.optStr("location"), s.str("content"),
                    s.span("contentSourceInformation"), s.span()));
        }), w.span());
    }

    static Protocol.Element binding(Wire w) {
        Wire unit = w.obj("modelUnit");
        List<String> excludes = unit.strings("packageableElementExcludes");
        List<String> includes = unit.done(unit.strings("packageableElementIncludes"));
        return new Protocol.PBinding(w.str("package"), w.str("name"), w.optStr("schemaSet"), w.optStr("schemaId"),
                w.str("contentType"), includes, excludes, w.span());
    }

    // ---------------------------------------------------------------------
    // ###ServiceStore
    // ---------------------------------------------------------------------

    static Protocol.Element serviceStore(Wire w) {
        w.emptyArray("includedStores");
        return new Protocol.PServiceStoreDefinition(w.str("package"), w.str("name"), null,
                w.list("elements", TailReader::serviceStoreElement), w.span());
    }

    private static Protocol.PServiceStoreElement serviceStoreElement(Json.Node node) {
        Wire e = Wire.of(node, "service store element");
        String type = e.type();
        if ("serviceGroup".equals(type)) {
            return e.done(new Protocol.PSsServiceGroup(e.str("id"), e.str("path"),
                    e.list("elements", TailReader::serviceStoreElement), e.span()));
        }
        if (!"service".equals(type)) {
            throw Wire.refuse("no reader rule for service store element _type '" + type + "'");
        }
        e.emptyArray("security");
        Json.Node body = e.opt("requestBody");
        return e.done(new Protocol.PSsService(e.str("id"), e.str("path"), body == null ? null : typeRef(body),
                e.str("method"), e.optList("parameters", TailReader::ssParam), typeRef(e.take("response")), List.of(),
                e.span()));
    }

    private static Protocol.PSsParam ssParam(Json.Node node) {
        Wire p = Wire.of(node, "service store parameter");
        Wire f = p.obj("serializationFormat");
        Boolean explode = f.optBool("explode");
        SourceInfo explodeSpan = f.span("explodeSourceInformation");
        String style = f.optStr("style");
        SourceInfo styleSpan = f.done(f.span("styleSourceInformation"));
        return p.done(new Protocol.PSsParam(p.str("name"), typeRef(p.take("type")), p.optBool("allowReserved"),
                p.optBool("required"), p.str("location"), style, styleSpan, explode, explodeSpan,
                p.optStr("enumeration"), p.span()));
    }

    /**
     * A type reference: {@code complex} (a class through a binding), or a primitive whose wire
     * {@code _type} is its name in lower case (read back with the grammar's initial capital).
     */
    private static Protocol.PSsTypeRef typeRef(Json.Node node) {
        Wire t = Wire.of(node, "service store type");
        String type = t.type();
        if (type == null || type.isEmpty()) {
            throw Wire.refuse("a service store type without its _type");
        }
        if ("complex".equals(type)) {
            return t.done(new Protocol.PSsTypeRef(null, t.str("type"), t.str("binding"), t.bool("list"), t.span()));
        }
        String primitive = Character.toUpperCase(type.charAt(0)) + type.substring(1);
        return t.done(new Protocol.PSsTypeRef(primitive, null, null, t.bool("list"), t.span()));
    }

    // ---------------------------------------------------------------------
    // ###Diagram
    // ---------------------------------------------------------------------

    static Protocol.Element diagram(Wire w) {
        return new Protocol.PDiagram(w.str("package"), w.str("name"), w.list("classViews", TailReader::classView),
                w.list("propertyViews", TailReader::propertyView),
                w.list("generalizationViews", TailReader::generalizationView), w.span());
    }

    private static Protocol.PClassView classView(Json.Node node) {
        Wire v = Wire.of(node, "class view");
        Wire pos = v.obj("position");
        double x = pos.dbl("x");
        double y = pos.done(pos.dbl("y"));
        Wire rect = v.obj("rectangle");
        double height = rect.dbl("height");
        double width = rect.done(rect.dbl("width"));
        return v.done(new Protocol.PClassView(v.str("id"), v.str("class"), v.span("classSourceInformation"),
                v.optBool("hideProperties"), v.optBool("hideStereotypes"), v.optBool("hideTaggedValues"), x, y, width,
                height, v.span()));
    }

    private static Protocol.PPropertyView propertyView(Json.Node node) {
        Wire v = Wire.of(node, "property view");
        Wire p = v.obj("property");
        String cls = p.str("class");
        String property = p.str("property");
        SourceInfo propertySpan = p.done(p.span());
        return v.done(new Protocol.PPropertyView(cls, property, propertySpan, v.str("sourceView"),
                v.span("sourceViewSourceInformation"), v.str("targetView"), v.span("targetViewSourceInformation"),
                points(v), v.span()));
    }

    private static Protocol.PGeneralizationView generalizationView(Json.Node node) {
        Wire v = Wire.of(node, "generalization view");
        return v.done(new Protocol.PGeneralizationView(v.str("sourceView"), v.span("sourceViewSourceInformation"),
                v.str("targetView"), v.span("targetViewSourceInformation"), points(v), v.span()));
    }

    private static List<Protocol.PDiagramPoint> points(Wire v) {
        Wire line = v.obj("line");
        return line.done(line.list("points", n -> {
            Wire p = Wire.of(n, "point");
            return p.done(new Protocol.PDiagramPoint(p.dbl("x"), p.dbl("y")));
        }));
    }
}
