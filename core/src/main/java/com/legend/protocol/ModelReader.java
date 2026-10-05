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

    /** A whole {@code PureModelContextData}. */
    public static PureModelContextData read(Json.Node json) {
        Wire w = Wire.of(ProtocolUpgrade.upgrade(json), "PureModelContextData");
        w.constant("_type", "data");
        return w.done(new PureModelContextData(w.list("elements", ModelReader::element)));
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
        for (Json.Node e : s.arr("elements")) {
            elements.add(e instanceof Json.Null ? null : s.asStr(e, "elements[]"));
        }
        List<String> imports = importAware ? s.strings("imports") : List.of();
        return s.done(new Protocol.PSection(importAware, s.str("parserName"), Collections.unmodifiableList(elements),
                imports, s.span()));
    }
}
