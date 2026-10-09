// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.Protocol.Element;
import com.legend.protocol.Protocol.PureModelContextData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * THE MODEL READER: protocol JSON &rarr; the typed protocol records (the protocol program's read leg,
 * docs/PROTOCOL_PROGRAM_2026_10_05.md). The exact inverse of {@link ProtocolEmitter}: for every JSON
 * the emitter writes, {@code ProtocolEmitter.emit(read(json))} is the same bytes. Structured like the
 * emitter it mirrors, one reader per emitter family, each pair read side by side
 * ({@link DomainReader} for the domain rules, {@link ProtocolReader} for every value specification,
 * and so on through the families registered in {@link #ELEMENTS}).
 *
 * <p>Older wire forms are brought current first, as upstream's converters do on every read
 * ({@link ProtocolUpgrade}). An element {@code _type} with no rule, a field no rule takes, and a value
 * no record can carry are REFUSED by name ({@link IllegalArgumentException}); nothing is dropped or
 * defaulted. Positions are read from {@code sourceInformation} when present and are absent
 * ({@code null}) otherwise. Numbers stay exact. The reader is pure: no I/O, no regex.
 */
public final class ModelReader {

    private ModelReader() {
    }

    /** A whole {@code PureModelContextData}, as text. */
    public static PureModelContextData read(String json) {
        return read(Json.parse(json, DEEP));
    }

    /**
     * A whole {@code PureModelContextData}: its elements, and the {@code serializer} and {@code origin} a model from an
     * SDLC or Depot carries. Older JSON spreads the elements over sections ({@code domain}'s classes, associations,
     * enums, profiles, functions and measures; then {@code sectionIndices}, {@code stores}, {@code mappings}, ...),
     * merged after {@code elements} in the engine's order ({@code PureModelContextData.newPureModelContextData}). The
     * engine drops any other field of the model context; lite refuses it, naming it.
     */
    public static PureModelContextData read(Json.Node json) {
        Wire w = Wire.of(ProtocolUpgrade.upgrade(json), "PureModelContextData");
        w.constant("_type", "data");
        Json.Node serializer = w.opt("serializer");
        Json.Node origin = w.opt("origin");
        List<Element> elements = new ArrayList<>();
        for (Json.Node e : elementNodes(w)) {
            elements.add(element(e));
        }
        return w.done(new PureModelContextData(elements, serializer == null ? null : serializer(serializer),
                origin == null ? null : origin(origin)));
    }

    /**
     * The element objects of a {@code PureModelContextData}, as the engine gathers them -- {@code elements}, then the
     * older sections, in its order -- each still JSON (one read at a time: the parity harness's granularity).
     */
    public static List<Json.Node> elementNodes(Json.Node json) {
        Wire w = Wire.of(ProtocolUpgrade.upgrade(json), "PureModelContextData");
        return elementNodes(w);
    }

    /** The engine's order: elements, the domain's six lists, then each section. */
    private static final List<String> DOMAIN = List.of("classes", "associations", "enums", "profiles", "functions",
            "measures");
    private static final List<String> SECTIONS = List.of("sectionIndices", "stores", "mappings", "services",
            "cacheables", "caches", "pipelines", "flattenSpecifications", "diagrams", "dataStoreSpecifications", "texts",
            "runtimes", "connections", "fileGenerations", "generationSpecifications", "relationalMapper",
            "serializableModelSpecifications");

    private static List<Json.Node> elementNodes(Wire w) {
        List<Json.Node> out = new ArrayList<>(w.arrOrEmpty("elements"));
        Wire domain = w.optObj("domain");
        if (domain != null) {
            for (String list : DOMAIN) {
                out.addAll(domain.arrOrEmpty(list));
            }
            domain.done(domain);
        }
        for (String section : SECTIONS) {
            out.addAll(w.arrOrEmpty(section));
        }
        return out;
    }

    private static Protocol.PSerializer serializer(Json.Node node) {
        Wire s = Wire.of(node, "serializer");
        return s.done(new Protocol.PSerializer(s.optStr("name"), s.optStr("version")));
    }

    /** {@code PureModelContextPointer}: the SDLC coordinates the engine starts as a pure one when left out. */
    private static Protocol.POrigin origin(Json.Node node) {
        Wire o = Wire.of(node, "origin");
        String kind = o.type();
        if (kind != null && !kind.equals("pointer")) {
            throw Wire.refuse("an origin of _type '" + kind + "': a model's origin is a pointer");
        }
        Json.Node serializer = o.opt("serializer");
        Json.Node sdlc = o.opt("sdlcInfo");
        return o.done(new Protocol.POrigin(serializer == null ? null : serializer(serializer),
                sdlc == null ? new Protocol.PSdlc("pure", null, "none", List.of(), null, null, null, null, false)
                        : sdlc(sdlc)));
    }

    private static Protocol.PSdlc sdlc(Json.Node node) {
        Wire s = Wire.of(node, "sdlcInfo");
        String kind = s.type();
        if (kind == null || !"pure".equals(kind) && !"alloy".equals(kind) && !"workspace".equals(kind)) {
            throw Wire.refuse("no reader rule for sdlcInfo _type '" + kind + "'");
        }
        String version = s.optStr("version");
        Boolean group = "workspace".equals(kind) ? s.optBool("isGroupWorkspace") : null;
        return s.done(new Protocol.PSdlc(kind, s.optStr("baseVersion"), version == null ? "none" : version,
                s.listOrEmpty("packageableElementPointers", DomainReader::pointer),
                "pure".equals(kind) ? s.optStr("overrideUrl") : null,
                "pure".equals(kind) ? null : s.optStr("project"),
                "alloy".equals(kind) ? s.optStr("groupId") : null,
                "alloy".equals(kind) ? s.optStr("artifactId") : null,
                group != null && group));
    }

    /** One element, as text. */
    public static Element readElement(String json) {
        return readElement(Json.parse(json, DEEP));
    }

    /** One element. */
    public static Element readElement(Json.Node json) {
        return element(ProtocolUpgrade.upgrade(json));
    }

    /** A whole model nests far deeper than one request's default limit. */
    private static final Json.Config DEEP = new Json.Config(4096);

    /** The reader rule for each element {@code _type} the emitter writes. */
    private static final Map<String, Function<Wire, Element>> ELEMENTS = Map.ofEntries(
            Map.entry("class", DomainReader::pclass),
            Map.entry("association", DomainReader::association),
            Map.entry("profile", DomainReader::profile),
            Map.entry("Enumeration", DomainReader::enumeration),
            Map.entry("function", DomainReader::function),
            Map.entry("measure", DomainReader::measure),
            Map.entry("relational", StoreReader::database),
            Map.entry("relationalMapper", StoreReader::relationalMapper),
            Map.entry("connection", ConnectionReader::connection),
            Map.entry("runtime", ConnectionReader::runtime),
            Map.entry("dataElement", EmbeddedDataReader::dataElement),
            Map.entry("mapping", MappingReader::mapping),
            Map.entry("service", ServiceReader::service),
            Map.entry("executionEnvironmentInstance", ServiceReader::executionEnvironment),
            Map.entry("dataSpace", DataSpaceReader::dataSpace),
            Map.entry("persistence", PersistenceReader::persistence),
            Map.entry("persistenceContext", PersistenceReader::persistenceContext),
            Map.entry("text", TailReader::text),
            Map.entry("generationSpecification", TailReader::generationSpecification),
            Map.entry("fileGeneration", TailReader::fileGeneration),
            Map.entry("deephavenStore", TailReader::deephavenStore),
            Map.entry("elasticsearch7Store", TailReader::elasticsearchStore),
            Map.entry("MongoDatabase", TailReader::mongoDatabase),
            Map.entry("externalFormatSchemaSet", TailReader::schemaSet),
            Map.entry("binding", TailReader::binding),
            Map.entry("serviceStore", TailReader::serviceStore),
            Map.entry("diagram", TailReader::diagram),
            Map.entry("dataQualityValidation", DataQualityReader::validation),
            Map.entry("dataqualityRelationValidation", DataQualityReader::relationValidation),
            Map.entry("dataQualityRelationComparison", DataQualityReader::relationComparison),
            Map.entry("snowflakeApp", ActivatorReader.reader("snowflakeApp")),
            Map.entry("snowflakeM2MUdf", ActivatorReader.reader("snowflakeM2MUdf")),
            Map.entry("memSqlFunction", ActivatorReader.reader("memSqlFunction")),
            Map.entry("bigQueryFunction", ActivatorReader.reader("bigQueryFunction")),
            Map.entry("hostedService", ActivatorReader.reader("hostedService")),
            Map.entry("functionJar", ActivatorReader.reader("functionJar")),
            Map.entry("DeephavenApp", ActivatorReader.reader("DeephavenApp")),
            Map.entry("sectionIndex", ModelReader::sectionIndex));

    private static Element element(Json.Node node) {
        String peek = node instanceof Json.Obj o ? o.getStringOr("_type", null) : null;
        Wire w = Wire.of(node, "element '" + peek + "'");
        String type = w.type();
        return w.done(Wire.rule(ELEMENTS, type, "element").apply(w));
    }

    /** {@code _type:"sectionIndex"}: each section lists its element paths (a literal {@code null} kept). */
    private static Element sectionIndex(Wire w) {
        return new Protocol.PSectionIndex(w.str("package"), w.str("name"), w.list("sections", ModelReader::section));
    }

    private static Protocol.PSection section(Json.Node node) {
        Wire s = Wire.of(node, "section");
        String type = s.type();
        boolean importAware = "importAware".equals(type);
        if (!importAware && !"default".equals(type)) {
            throw Wire.refuse("no reader rule for section _type '" + type + "'");
        }
        List<String> elements = new ArrayList<>();
        for (Json.Node e : s.arrOrEmpty("elements")) {   // the engine's Section starts it empty
            elements.add(e instanceof Json.Null ? null : s.asStr(e, "elements[]"));
        }
        List<String> imports = importAware ? s.strings("imports") : List.of();
        return s.done(new Protocol.PSection(importAware, s.str("parserName"), Collections.unmodifiableList(elements),
                imports, s.span()));
    }
}
