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

    /**
     * {@code _type:"ingestDefinition"}: the writer's inverse, and Legend's own JSON as legend-studio's round-trip
     * fixtures hold it (LITE-DESIGN-lakehouse). Legend's older owner spelling, a bare {@code appDirDeployment}, and test
     * suites are refused by name: lite writes neither yet.
     */
    static Protocol.Element ingest(Wire w) {
        if (w.has("appDirDeployment")) {
            throw Wire.refuse("an ingest's older owner, 'appDirDeployment': lite reads 'owner'");
        }
        if (w.has("testSuites")) {
            throw Wire.refuse("an ingest's test suites: lite does not read them yet");
        }
        Protocol.PIngestOwner owner = null;
        Wire o = w.optObj("owner");
        if (o != null) {
            o.constant("_type", "appDir");
            Wire pp = o.optObj("prodParallel");
            String prodParallel = pp == null ? null : appDirNode(pp);
            owner = o.done(new Protocol.PIngestOwner(appDirNode(o.obj("production")), prodParallel));
        }
        String group = w.optStr("datasetGroup");
        Wire rm = w.optObj("readMode");
        Protocol.PIngestReadMode readMode = rm == null ? null : ingestReadMode(rm);
        Wire sc = w.optObj("schedule");
        Protocol.PIngestSchedule schedule = sc == null ? null : ingestSchedule(sc);
        String writeMode = ingestWriteMode(w.optObj("writeMode"));
        return new Protocol.PIngest(w.str("package"), w.str("name"), DomainReader.stereotypes(w),
                DomainReader.taggedValues(w), readMode, schedule, owner, group, writeMode,
                w.list("datasets", TailReader::ingestDataset), w.span());
    }

    /** {@code _type:"lakehouseDeployment"}: the writer's inverse (lite's own). */
    static Protocol.Element deployment(Wire w) {
        List<Protocol.PDeploymentSource> sources = w.list("sources", n -> {
            Wire s = Wire.of(n, "deployment source");
            return s.done(new Protocol.PDeploymentSource(s.str("connection"), s.strings("stores")));
        });
        Wire live = w.optObj("live");
        Wire history = w.optObj("history");
        return new Protocol.PDeployment(w.str("package"), w.str("name"), DomainReader.stereotypes(w),
                DomainReader.taggedValues(w), w.strings("deploys"), sources,
                live == null ? null : deploymentTarget(live), history == null ? null : deploymentTarget(history),
                w.span());
    }

    /** Upstream's data product fields lite does not carry yet, refused by name. */
    private static final java.util.Set<String> DATA_PRODUCT_NOT_CARRIED = java.util.Set.of("icon", "nativeModelAccess",
            "supportInfo", "type", "sampleValues", "operationalMetadata", "owner", "testSuites",
            "prodDataProductOverride");

    /** {@code _type:"dataProduct"}: upstream's (legend-studio {@code V1_DataProductSerializationHelper.ts}), the part
     *  lite carries; a list upstream leaves out when empty is read as empty. */
    static Protocol.Element dataProduct(Wire w) {
        for (String k : DATA_PRODUCT_NOT_CARRIED) {
            if (w.has(k)) {
                throw Wire.refuse("a data product's '" + k + "': lite does not carry it yet");
            }
        }
        List<Protocol.PAccessPointGroup> groups = w.list("accessPointGroups", n -> {
            Wire g = Wire.of(n, "access point group");
            String type = g.type();
            boolean model = "modelAccessPointGroup".equals(type);
            if (!model && !"defaultAccessPointGroup".equals(type)) {
                throw Wire.refuse("an access point group '" + type + "' (defaultAccessPointGroup,"
                        + " modelAccessPointGroup)");
            }
            for (String k : List.of("featuredElements", "diagrams", "secureViews", "targets", "title")) {
                if (g.has(k) && !(g.take(k) instanceof Json.Arr a && a.items().isEmpty())) {
                    throw Wire.refuse("an access point group's '" + k + "': lite does not carry it yet");
                }
            }
            String mapping = null;
            if (model) {
                Wire m = g.obj("mapping");
                mapping = m.done(m.str("path"));
            }
            List<Protocol.PLakehouseAccessPoint> points = g.list("accessPoints", TailReader::lakehouseAccessPoint);
            return g.done(new Protocol.PAccessPointGroup(g.str("id"), g.optStr("description"),
                    g.has("stereotypes") ? DomainReader.stereotypes(g) : List.of(), mapping, points));
        });
        return new Protocol.PDataProduct(w.str("package"), w.str("name"),
                w.has("stereotypes") ? DomainReader.stereotypes(w) : List.of(),
                w.has("taggedValues") ? DomainReader.taggedValues(w) : List.of(), w.optStr("title"),
                w.optStr("description"), groups, w.span());
    }

    private static Protocol.PLakehouseAccessPoint lakehouseAccessPoint(Json.Node n) {
        Wire a = Wire.of(n, "access point");
        String type = a.type();
        if (!"lakehouseAccessPoint".equals(type)) {
            throw Wire.refuse("an access point '" + type + "': lite carries lakehouse access points");
        }
        if (a.has("title")) {
            throw Wire.refuse("an access point's 'title': lite does not carry it yet");
        }
        if (a.has("stereotypes") && !(a.take("stereotypes") instanceof Json.Arr s && s.items().isEmpty())) {
            throw Wire.refuse("an access point's stereotypes: lite does not carry them yet");
        }
        boolean reproducible = a.has("reproducible") && a.bool("reproducible");
        return a.done(new Protocol.PLakehouseAccessPoint(a.str("id"), a.optStr("description"),
                a.optStr("targetEnvironment"), a.optStr("classification"),
                ProtocolReader.lambdaNode(a.take("func")), reproducible));
    }

    private static Protocol.PDeploymentTarget deploymentTarget(Wire t) {
        List<Protocol.PKeep> keep = t.list("keep", n -> {
            Wire k = Wire.of(n, "keep");
            Wire r = k.obj("rule");
            String type = r.type();
            Protocol.PKeepRule rule = switch (type == null ? "" : type) {
                case "LastRows" -> r.done(new Protocol.PKeepRows(r.lng("rows")));
                case "Last" -> r.done(new Protocol.PKeepFor(r.str("duration")));
                case "All" -> r.done(new Protocol.PKeepAll());
                case "Current" -> r.done(new Protocol.PKeepCurrent());
                case "Versions" -> r.done(new Protocol.PKeepVersions());
                default -> throw Wire.refuse("a keep rule '" + type + "' (LastRows, Last, All, Current, Versions)");
            };
            return k.done(new Protocol.PKeep(k.str("scope"), rule));
        });
        return t.done(new Protocol.PDeploymentTarget(t.str("connection"), keep));
    }

    private static String appDirNode(Wire n) {
        n.constant("level", "DEPLOYMENT");
        String id = String.valueOf(n.lng("appDirId"));
        if (!IngestWords.isAppDirId(id)) {
            throw Wire.refuse("an application directory id is a positive number, not " + id);
        }
        return n.done(id);
    }

    private static Protocol.PIngestReadMode ingestReadMode(Wire r) {
        String written = r.type();
        String type = written == null ? "" : written;
        String retired = IngestWords.RETIRED_READ_MODES.get(type);
        if (retired != null) {
            throw Wire.refuse(retired);
        }
        if (!IngestWords.READ_MODES.contains(type)) {
            throw Wire.refuse("an ingest's read mode '" + type + "' (Snapshot, Delta)");
        }
        String format = null;
        Wire f = r.optObj("format");
        if (f != null) {
            String written2 = f.type();
            if (written2 == null || !IngestWords.FORMATS.contains(written2)) {
                throw Wire.refuse("an ingest's format '" + written2 + "' (CSV, Parquet, JSON, Avro)");
            }
            format = f.done(written2);
        }
        Protocol.PIngestDeleteIndicator delete = null;
        Wire d = r.optObj("deleteIndicator");
        if (d != null) {
            if (!type.equals("Delta")) {
                throw Wire.refuse("a delete indicator on a " + type + " read mode: it marks a delta's deletes");
            }
            String field = d.str("deleteField");
            List<String> values = d.strings("deleteValues");
            if (values.isEmpty()) {
                throw Wire.refuse("a delete indicator with no value");
            }
            delete = d.done(new Protocol.PIngestDeleteIndicator(field, values));
        }
        return r.done(new Protocol.PIngestReadMode(type, format, delete));
    }

    private static Protocol.PIngestSchedule ingestSchedule(Wire s) {
        String kind = s.type();
        return switch (kind == null ? "" : kind) {
            case "Daily" -> {
                String time = s.str("time");
                String zone = s.str("timeZone");
                if (!IngestWords.isTimeOfDay(time) || !IngestWords.isZone(zone)) {
                    throw Wire.refuse("a daily pull at '" + time + "' in '" + zone + "' (HH:MM in a named zone)");
                }
                yield s.done(new Protocol.PIngestDaily(time, zone));
            }
            case "Every" -> {
                String period = s.str("period");
                if (!IngestWords.isDuration(period)) {
                    throw Wire.refuse("a pull every '" + period + "' (an ISO 8601 duration)");
                }
                yield s.done(new Protocol.PIngestEvery(period));
            }
            default -> throw Wire.refuse("an ingest's pull schedule '" + kind + "' (Daily, Every)");
        };
    }

    private static @com.legend.base.Nullable String ingestWriteMode(@com.legend.base.Nullable Wire m) {
        if (m == null) {
            return null;
        }
        String type = m.type();
        if ("latest_by_key".equals(type)) {
            throw Wire.refuse(IngestWords.RETIRED_LATEST_BY_KEY);
        }
        if (type == null || IngestWords.writeModeWord(type) == null) {
            throw Wire.refuse("an ingest's write mode '" + type
                    + "' (append_only, batch_milestoned, batch_milestoned_business_temporal)");
        }
        return m.done(type);
    }

    private static Protocol.PIngestDataset ingestDataset(Json.Node node) {
        Wire d = Wire.of(node, "ingest dataset");
        if (!d.arrOrEmpty("preprocessors").isEmpty()) {
            throw Wire.refuse("an ingest dataset's preprocessors: lite does not read them yet");
        }
        String privacy = null;
        Wire p = d.optObj("privacyClassification");
        if (p != null) {
            privacy = p.done(p.str("sensitivity"));
        }
        Protocol.PIngestRetention retention = null;
        Wire r = d.optObj("retention");
        if (r != null) {
            String type = r.type();
            retention = switch (type == null ? "" : type) {
                case "Last" -> {
                    String duration = r.str("duration");
                    if (!IngestWords.isDuration(duration)) {
                        throw Wire.refuse("a retention of '" + duration + "' (an ISO 8601 duration)");
                    }
                    yield r.done(new Protocol.PIngestLast(duration));
                }
                case "LastRows" -> {
                    long rows = r.lng("rows");
                    if (rows <= 0) {
                        throw Wire.refuse("a retention of " + rows + " rows");
                    }
                    yield r.done(new Protocol.PIngestLastRows(rows));
                }
                default -> throw Wire.refuse("an ingest dataset's retention '" + type + "' (Last, LastRows)");
            };
        }
        Wire s = d.obj("source");
        String sourceType = s.type();
        Protocol.PIngestSource source = switch (sourceType == null ? "" : sourceType) {
            case "serializedSource" -> {
                Wire schema = s.obj("schema");
                schema.constant("_type", "relationType");
                List<TypeExpression.Column> columns = schema.list("columns", n -> {
                    Wire c = Wire.of(n, "ingest column");
                    return c.done(new TypeExpression.Column(c.str("name"),
                            ProtocolReader.genericType(c.take("genericType")),
                            ProtocolReader.multiplicity(c.take("multiplicity")), c.span()));
                });
                schema.done(columns);
                yield s.done(new Protocol.PIngestColumns(columns));
            }
            case "FunctionSource" -> s.done(new Protocol.PIngestFunction(ProtocolReader.lambdaNode(s.take("function"))));
            default -> throw Wire.refuse("an ingest dataset's source '" + sourceType
                    + "' (serializedSource, FunctionSource)");
        };
        return d.done(new Protocol.PIngestDataset(d.str("name"), d.strings("primaryKey"), source,
                ingestWriteMode(d.optObj("writeMode")), retention, privacy,
                stringsOrEmpty(d, "ingestPartitionColumns"), stringsOrEmpty(d, "storageLayoutPartitionColumns"), stringsOrEmpty(d, "storageLayoutClusterColumns"),
                null));
    }

    private static List<String> stringsOrEmpty(Wire w, String key) {
        List<String> s = w.optStrings(key);
        return s == null ? List.of() : s;
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
        boolean additional = false;
        List<Map.Entry<String, Protocol.PBsonSchema>> properties = new ArrayList<>();
        List<String> required = List.of();
        String title = null;
        if (object) {
            additional = s.bool("additionalPropertiesAllowed");
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
        // a description: the engine's parser writes none; lite's dialect does (S89), into the engine's own field
        return new Protocol.PServiceStoreDefinition(w.str("package"), w.str("name"), w.optStr("description"),
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
