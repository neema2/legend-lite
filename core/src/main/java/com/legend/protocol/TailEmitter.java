// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.List;

/**
 * Wire emission for the TAIL-section elements — Text,
 * GenerationSpecification, FileGeneration, Deephaven store, MongoDB,
 * Elasticsearch and the DataQuality trio. Byte shapes probed
 * (ZTailProbe "tailShapes*"); the DUPLICATED {@code _type} /
 * {@code _pure_protocol_type} keys are the engine serializer's own
 * output and are reproduced literally.
 */
final class TailEmitter {

    private TailEmitter() {
    }

    static void text(StringBuilder b, Protocol.PText t) {
        b.append("{\"_type\":\"text\",\"content\":");
        ProtocolEmitter.str(b, t.content());
        b.append(",\"name\":");
        ProtocolEmitter.str(b, t.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, t.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, t.sourceInformation());
        if (t.type() != null) {
            b.append(",\"type\":");
            ProtocolEmitter.str(b, t.type());
        }
        b.append('}');
    }

    static void generationSpecification(StringBuilder b,
            Protocol.PGenerationSpecification g) {
        b.append("{\"_type\":\"generationSpecification\","
                + "\"fileGenerations\":[");
        for (int i = 0; i < g.fileGenerations().size(); i++) {
            Protocol.PPointer p = g.fileGenerations().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"path\":");
            ProtocolEmitter.str(b, p.path());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, p.sourceInformation());
            b.append(",\"type\":\"FILE_GENERATION\"}");
        }
        b.append("],\"generationNodes\":[");
        for (int i = 0; i < g.generationNodes().size(); i++) {
            Protocol.PGenerationNode n = g.generationNodes().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"generationElement\":");
            ProtocolEmitter.str(b, n.generationElement());
            b.append(",\"id\":");
            ProtocolEmitter.str(b, n.id());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, n.sourceInformation());
            b.append('}');
        }
        b.append("],\"name\":");
        ProtocolEmitter.str(b, g.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, g.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, g.sourceInformation());
        b.append('}');
    }

    static void fileGeneration(StringBuilder b, Protocol.PFileGeneration f) {
        b.append("{\"_type\":\"fileGeneration\","
                + "\"configurationProperties\":[");
        for (int i = 0; i < f.configurationProperties().size(); i++) {
            Protocol.PConfigProperty p = f.configurationProperties().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"name\":");
            ProtocolEmitter.str(b, p.name());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, p.sourceInformation());
            b.append(",\"value\":");
            configValue(b, p.value());
            b.append('}');
        }
        b.append(']');
        if (f.generationOutputPath() != null) {
            b.append(",\"generationOutputPath\":");
            ProtocolEmitter.str(b, f.generationOutputPath());
        }
        b.append(",\"name\":");
        ProtocolEmitter.str(b, f.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, f.pkg());
        b.append(",\"scopeElements\":[");
        for (int i = 0; i < f.scopeElements().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, f.scopeElements().get(i));
        }
        b.append("],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, f.sourceInformation());
        b.append(",\"type\":");
        ProtocolEmitter.str(b, f.type());
        b.append(",\"typeSourceInformation\":");
        ProtocolEmitter.srcInfo(b, f.typeSourceInformation());
        b.append('}');
    }

    private static void configValue(StringBuilder b, Protocol.PConfigValue v) {
        switch (v) {
            case Protocol.PConfigValue.PCString s ->
                    ProtocolEmitter.str(b, s.value());
            case Protocol.PConfigValue.PCBoolean bo -> b.append(bo.value());
            case Protocol.PConfigValue.PCInteger i -> b.append(i.value());
            case Protocol.PConfigValue.PCStrings ss -> {
                b.append('[');
                for (int i = 0; i < ss.values().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    ProtocolEmitter.str(b, ss.values().get(i));
                }
                b.append(']');
            }
            case Protocol.PConfigValue.PCMap m -> {
                b.append('{');
                boolean first = true;
                for (var e : m.entries().entrySet()) {
                    if (!first) {
                        b.append(',');
                    }
                    first = false;
                    ProtocolEmitter.str(b, e.getKey());
                    b.append(':');
                    ProtocolEmitter.str(b, e.getValue());
                }
                b.append('}');
            }
        }
    }

    static void deephavenStore(StringBuilder b, Protocol.PDeephavenDatabase d) {
        b.append("{\"_type\":\"deephavenStore\",\"includedStores\":[],"
                + "\"name\":");
        ProtocolEmitter.str(b, d.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, d.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, d.sourceInformation());
        b.append(",\"tables\":[");
        for (int t = 0; t < d.tables().size(); t++) {
            var table = d.tables().get(t);
            if (t > 0) {
                b.append(',');
            }
            b.append("{\"columns\":[");
            for (int i = 0; i < table.columns().size(); i++) {
                Protocol.PDeephavenColumn col = table.columns().get(i);
                if (i > 0) {
                    b.append(',');
                }
                b.append("{\"_type\":\"column\",\"name\":");
                ProtocolEmitter.str(b, col.name());
                // the engine serializer prints the type discriminator
                // TWICE — reproduced literally for byte parity
                b.append(",\"type\":{\"_type\":\"").append(col.kind())
                        .append("\",\"_type\":\"").append(col.kind())
                        .append('"');
                if (col.precision() != null) {
                    b.append(",\"precision\":").append(col.precision())
                            .append(",\"scale\":").append(col.scale());
                }
                b.append("}}");
            }
            b.append("],\"name\":");
            ProtocolEmitter.str(b, table.name());
            b.append('}');
        }
        b.append("]}");
    }

    /** One typed property value ({@code "<key>":{...}}) — the engine
     *  serializer DOUBLES _pure_protocol_type (probed quirk); nested
     *  {@code fields} entries are keyed maps of the same shape. */
    private static void esPropertyBody(StringBuilder b,
            Protocol.PElasticsearch7Cluster.PEsProperty prop) {
        b.append('"').append(prop.wireKey())
                .append("\":{\"_pure_protocol_type\":\"")
                .append(prop.protocolType())
                .append("\",\"_pure_protocol_type\":\"")
                .append(prop.protocolType()).append('"');
        esChildMap(b, "fields", prop.fields());
        esChildMap(b, "properties", prop.childProperties());
        b.append(",\"type\":\"").append(prop.esType()).append("\"}");
    }

    private static void esChildMap(StringBuilder b, String label,
            @com.legend.Nullable java.util.List<
                    Protocol.PElasticsearch7Cluster.PEsProperty> children) {
        if (children == null) {
            return;
        }
        b.append(",\"").append(label).append("\":{");
        // the walker collects these into a MAP and the engine mapper
        // enables ORDER_MAP_ENTRIES_BY_KEYS — wire order is sorted by
        // property name, not source order (probed "nested multi order")
        children = children.stream().sorted(java.util.Comparator.comparing(
                Protocol.PElasticsearch7Cluster.PEsProperty::propertyName))
                .toList();
        for (int i = 0; i < children.size(); i++) {
            var f = children.get(i);
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, f.propertyName());
            b.append(":{");
            esPropertyBody(b, f);
            b.append('}');
        }
        b.append('}');
    }

    static void elasticsearchStore(StringBuilder b,
            Protocol.PElasticsearch7Cluster s) {
        b.append("{\"_type\":\"elasticsearch7Store\",\"includedStores\":[],"
                + "\"indices\":[");
        for (int i = 0; i < s.indices().size(); i++) {
            var idx = s.indices().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"indexName\":");
            ProtocolEmitter.str(b, idx.indexName());
            b.append(",\"properties\":[");
            for (int p = 0; p < idx.properties().size(); p++) {
                var prop = idx.properties().get(p);
                if (p > 0) {
                    b.append(',');
                }
                b.append("{\"property\":{");
                esPropertyBody(b, prop);
                b.append("},\"propertyName\":");
                ProtocolEmitter.str(b, prop.propertyName());
                b.append('}');
            }
            b.append("]}");
        }
        b.append("],\"name\":");
        ProtocolEmitter.str(b, s.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, s.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, s.sourceInformation());
        b.append('}');
    }

    static void mongoDatabase(StringBuilder b, Protocol.PMongoDatabase m) {
        b.append("{\"_type\":\"MongoDatabase\",\"collections\":[");
        for (int i = 0; i < m.collections().size(); i++) {
            var coll = m.collections().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"name\":");
            ProtocolEmitter.str(b, coll.name());
            b.append(",\"validator\":{\"validationAction\":");
            ProtocolEmitter.str(b, coll.validationAction());
            b.append(",\"validationLevel\":");
            ProtocolEmitter.str(b, coll.validationLevel());
            b.append(",\"validatorExpression\":{\"_type\":"
                    + "\"jsonSchemaExpression\",\"schemaExpression\":");
            bsonSchema(b, coll.schema(), true);
            b.append("}}}");
        }
        b.append("],\"includedStores\":[],\"name\":");
        ProtocolEmitter.str(b, m.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, m.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, m.sourceInformation());
        b.append(",\"views\":[]}");
    }

    /** The BSON schema wire: object nodes carry
     *  additionalPropertiesAllowed/properties/required; scalar nodes only
     *  their facets. Slots alphabetical, arrays always present. */
    private static void bsonSchema(StringBuilder b, Protocol.PBsonSchema s,
            boolean object) {
        b.append("{\"_type\":\"").append(s.wireType())
                .append("\",\"_enum\":[]");
        boolean isObject = "schema".equals(s.wireType())
                || "objectType".equals(s.wireType());
        if ("arrayType".equals(s.wireType())) {
            // engine ArrayType default (C12 byte pin TestMongoDBCompiler)
            b.append(",\"additionalItemsAllowed\":false");
        }
        if (isObject) {
            b.append(",\"additionalPropertiesAllowed\":").append(
                    s.additionalPropertiesAllowed() != null
                            && s.additionalPropertiesAllowed());
        }
        b.append(",\"allOf\":[],\"anyOf\":[]");
        if (s.items() != null) {
            b.append(",\"items\":[");
            for (int i = 0; i < s.items().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                bsonSchema(b, s.items().get(i), false);
            }
            b.append(']');
        }
        if (s.description() != null) {
            b.append(",\"description\":");
            ProtocolEmitter.str(b, s.description());
        }
        if (s.maxLength() != null) {
            b.append(",\"maxLength\":").append(s.maxLength());
        }
        if (s.minLength() != null) {
            b.append(",\"minLength\":").append(s.minLength());
        }
        b.append(",\"oneOf\":[]");
        if ("arrayType".equals(s.wireType())) {
            // engine ArrayType default (C12 byte pin)
            b.append(",\"uniqueItems\":false");
        }
        if (isObject) {
            b.append(",\"properties\":[");
            for (int i = 0; i < s.properties().size(); i++) {
                var e = s.properties().get(i);
                if (i > 0) {
                    b.append(',');
                }
                b.append("{\"key\":");
                ProtocolEmitter.str(b, e.getKey());
                b.append(",\"value\":");
                bsonSchema(b, e.getValue(), false);
                b.append('}');
            }
            b.append("],\"required\":[");
            for (int i = 0; i < s.required().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                ProtocolEmitter.str(b, s.required().get(i));
            }
            b.append(']');
            if (s.title() != null) {
                b.append(",\"title\":");
                ProtocolEmitter.str(b, s.title());
            }
        }
        b.append('}');
    }

    static void schemaSet(StringBuilder b, Protocol.PSchemaSet s) {
        b.append("{\"_type\":\"externalFormatSchemaSet\",\"format\":");
        ProtocolEmitter.str(b, s.format());
        b.append(",\"name\":");
        ProtocolEmitter.str(b, s.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, s.pkg());
        b.append(",\"schemas\":[");
        for (int i = 0; i < s.schemas().size(); i++) {
            Protocol.PSchema sc = s.schemas().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"content\":");
            ProtocolEmitter.str(b, sc.content());
            b.append(",\"contentSourceInformation\":");
            ProtocolEmitter.srcInfo(b, sc.contentSourceInformation());
            if (sc.id() != null) {
                b.append(",\"id\":");
                ProtocolEmitter.str(b, sc.id());
            }
            if (sc.location() != null) {
                b.append(",\"location\":");
                ProtocolEmitter.str(b, sc.location());
            }
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, sc.sourceInformation());
            b.append('}');
        }
        b.append("],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, s.sourceInformation());
        b.append('}');
    }

    static void binding(StringBuilder b, Protocol.PBinding bd) {
        b.append("{\"_type\":\"binding\",\"contentType\":");
        ProtocolEmitter.str(b, bd.contentType());
        b.append(",\"modelUnit\":{\"packageableElementExcludes\":[");
        for (int i = 0; i < bd.modelExcludes().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, bd.modelExcludes().get(i));
        }
        b.append("],\"packageableElementIncludes\":[");
        for (int i = 0; i < bd.modelIncludes().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, bd.modelIncludes().get(i));
        }
        b.append("]},\"name\":");
        ProtocolEmitter.str(b, bd.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, bd.pkg());
        if (bd.schemaId() != null) {
            b.append(",\"schemaId\":");
            ProtocolEmitter.str(b, bd.schemaId());
        }
        if (bd.schemaSet() != null) {
            b.append(",\"schemaSet\":");
            ProtocolEmitter.str(b, bd.schemaSet());
        }
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, bd.sourceInformation());
        b.append('}');
    }

    static void serviceStore(StringBuilder b,
            Protocol.PServiceStoreDefinition s) {
        b.append("{\"_type\":\"serviceStore\",\"elements\":[");
        for (int i = 0; i < s.elements().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            serviceStoreElement(b, s.elements().get(i));
        }
        b.append("],\"includedStores\":[],\"name\":");
        ProtocolEmitter.str(b, s.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, s.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, s.sourceInformation());
        b.append('}');
    }

    private static void serviceStoreElement(StringBuilder b,
            Protocol.PServiceStoreElement e) {
        switch (e) {
            case Protocol.PSsService sv -> ssService(b, sv);
            case Protocol.PSsServiceGroup g -> {
                b.append("{\"_type\":\"serviceGroup\",\"elements\":[");
                for (int i = 0; i < g.elements().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    serviceStoreElement(b, g.elements().get(i));
                }
                b.append("],\"id\":");
                ProtocolEmitter.str(b, g.id());
                b.append(",\"path\":");
                ProtocolEmitter.str(b, g.path());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, g.sourceInformation());
                b.append('}');
            }
        }
    }

    private static void ssService(StringBuilder b, Protocol.PSsService sv) {
        b.append("{\"_type\":\"service\",\"id\":");
        ProtocolEmitter.str(b, sv.id());
        b.append(",\"method\":");
        ProtocolEmitter.str(b, sv.method());
        if (sv.parameters() != null) {
            b.append(",\"parameters\":[");
            for (int i = 0; i < sv.parameters().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                ssParam(b, sv.parameters().get(i));
            }
            b.append(']');
        }
        b.append(",\"path\":");
        ProtocolEmitter.str(b, sv.path());
        if (sv.requestBody() != null) {
            b.append(",\"requestBody\":");
            ssTypeRef(b, sv.requestBody());
        }
        b.append(",\"response\":");
        ssTypeRef(b, sv.response());
        if (!sv.security().isEmpty()) {
            throw new IllegalStateException("non-empty ServiceStore security"
                    + " wire is unprobed");
        }
        b.append(",\"security\":[],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, sv.sourceInformation());
        b.append('}');
    }

    private static void ssParam(StringBuilder b, Protocol.PSsParam p) {
        b.append('{');
        if (p.allowReserved() != null) {
            b.append("\"allowReserved\":").append(p.allowReserved())
                    .append(',');
        }
        if (p.enumeration() != null) {
            b.append("\"enumeration\":");
            ProtocolEmitter.str(b, p.enumeration());
            b.append(',');
        }
        b.append("\"location\":");
        ProtocolEmitter.str(b, p.location());
        b.append(",\"name\":");
        ProtocolEmitter.str(b, p.name());
        if (p.required() != null) {
            b.append(",\"required\":").append(p.required());
        }
        b.append(",\"serializationFormat\":{");
        if (p.explode() != null) {
            b.append("\"explode\":").append(p.explode())
                    .append(",\"explodeSourceInformation\":");
            ProtocolEmitter.srcInfo(b, java.util.Objects.requireNonNull(
                    p.explodeSpan()));
        }
        if (p.style() != null) {
            if (p.explode() != null) {
                b.append(',');
            }
            b.append("\"style\":");
            ProtocolEmitter.str(b, p.style());
            b.append(",\"styleSourceInformation\":");
            ProtocolEmitter.srcInfo(b, java.util.Objects.requireNonNull(
                    p.styleSpan()));
        }
        b.append("},\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, p.sourceInformation());
        b.append(",\"type\":");
        ssTypeRef(b, p.type());
        b.append('}');
    }

    private static void ssTypeRef(StringBuilder b, Protocol.PSsTypeRef t) {
        if (t.complexType() != null) {
            b.append("{\"_type\":\"complex\",\"binding\":");
            ProtocolEmitter.str(b,
                    java.util.Objects.requireNonNull(t.binding()));
            b.append(",\"list\":").append(t.list())
                    .append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, t.sourceInformation());
            b.append(",\"type\":");
            ProtocolEmitter.str(b, t.complexType());
            b.append('}');
        } else {
            b.append("{\"_type\":\"")
                    .append(java.util.Objects.requireNonNull(t.primitive())
                            .toLowerCase(java.util.Locale.ROOT))
                    .append("\",\"list\":").append(t.list())
                    .append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, t.sourceInformation());
            b.append('}');
        }
    }

    static void diagram(StringBuilder b, Protocol.PDiagram d) {
        b.append("{\"_type\":\"diagram\",\"classViews\":[");
        for (int i = 0; i < d.classViews().size(); i++) {
            Protocol.PClassView v = d.classViews().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"class\":");
            ProtocolEmitter.str(b, v.classPath());
            b.append(",\"classSourceInformation\":");
            ProtocolEmitter.srcInfo(b, v.classSpan());
            if (v.hideProperties() != null) {
                b.append(",\"hideProperties\":").append(v.hideProperties());
            }
            if (v.hideStereotypes() != null) {
                b.append(",\"hideStereotypes\":").append(v.hideStereotypes());
            }
            if (v.hideTaggedValues() != null) {
                b.append(",\"hideTaggedValues\":")
                        .append(v.hideTaggedValues());
            }
            b.append(",\"id\":");
            ProtocolEmitter.str(b, v.id());
            b.append(",\"position\":{\"x\":").append(v.x())
                    .append(",\"y\":").append(v.y())
                    .append("},\"rectangle\":{\"height\":").append(v.height())
                    .append(",\"width\":").append(v.width())
                    .append("},\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, v.sourceInformation());
            b.append('}');
        }
        b.append("],\"generalizationViews\":[");
        for (int i = 0; i < d.generalizationViews().size(); i++) {
            Protocol.PGeneralizationView v = d.generalizationViews().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append('{');
            diagramLine(b, v.points());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, v.sourceInformation());
            diagramEnds(b, v.sourceView(), v.sourceViewSpan(),
                    v.targetView(), v.targetViewSpan());
            b.append('}');
        }
        b.append("],\"name\":");
        ProtocolEmitter.str(b, d.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, d.pkg());
        b.append(",\"propertyViews\":[");
        for (int i = 0; i < d.propertyViews().size(); i++) {
            Protocol.PPropertyView v = d.propertyViews().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append('{');
            diagramLine(b, v.points());
            b.append(",\"property\":{\"class\":");
            ProtocolEmitter.str(b, v.propertyClass());
            b.append(",\"property\":");
            ProtocolEmitter.str(b, v.propertyName());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, v.propertySpan());
            b.append("},\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, v.sourceInformation());
            diagramEnds(b, v.sourceView(), v.sourceViewSpan(),
                    v.targetView(), v.targetViewSpan());
            b.append('}');
        }
        b.append("],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, d.sourceInformation());
        b.append('}');
    }

    private static void diagramLine(StringBuilder b,
            List<Protocol.PDiagramPoint> points) {
        b.append("\"line\":{\"points\":[");
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"x\":").append(points.get(i).x())
                    .append(",\"y\":").append(points.get(i).y())
                    .append('}');
        }
        b.append("]}");
    }

    private static void diagramEnds(StringBuilder b, String sourceView,
            SourceInfo sourceViewSpan, String targetView,
            SourceInfo targetViewSpan) {
        b.append(",\"sourceView\":");
        ProtocolEmitter.str(b, sourceView);
        b.append(",\"sourceViewSourceInformation\":");
        ProtocolEmitter.srcInfo(b, sourceViewSpan);
        b.append(",\"targetView\":");
        ProtocolEmitter.str(b, targetView);
        b.append(",\"targetViewSourceInformation\":");
        ProtocolEmitter.srcInfo(b, targetViewSpan);
    }

    static void dataSpace(StringBuilder b, Protocol.PDataSpace d) {
        b.append("{\"_type\":\"dataSpace\"");
        if (d.defaultExecutionContext() != null) {
            b.append(",\"defaultExecutionContext\":");
            ProtocolEmitter.str(b, d.defaultExecutionContext());
        }
        if (d.description() != null) {
            b.append(",\"description\":");
            ProtocolEmitter.str(b, d.description());
        }
        if (d.diagrams() != null) {
            b.append(",\"diagrams\":[");
            for (int i = 0; i < d.diagrams().size(); i++) {
                Protocol.PDataSpaceDiagram g = d.diagrams().get(i);
                if (i > 0) {
                    b.append(',');
                }
                b.append('{');
                if (g.description() != null) {
                    b.append("\"description\":");
                    ProtocolEmitter.str(b, g.description());
                    b.append(',');
                }
                b.append("\"diagram\":{\"path\":");
                ProtocolEmitter.str(b, g.diagram());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, g.diagramSpan());
                b.append("},\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, g.sourceInformation());
                b.append(",\"title\":");
                ProtocolEmitter.str(b, g.title());
                b.append('}');
            }
            b.append(']');
        }
        if (d.elements() != null) {
            b.append(",\"elements\":[");
            for (int i = 0; i < d.elements().size(); i++) {
                Protocol.PDataSpaceElementRef ref = d.elements().get(i);
                if (i > 0) {
                    b.append(',');
                }
                b.append('{');
                if (ref.exclude()) {
                    b.append("\"exclude\":true,");
                }
                b.append("\"path\":");
                ProtocolEmitter.str(b, ref.path());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, ref.sourceInformation());
                b.append('}');
            }
            b.append(']');
        }
        if (d.executables() != null) {
            b.append(",\"executables\":[");
            for (int i = 0; i < d.executables().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                dataSpaceExecutable(b, d.executables().get(i));
            }
            b.append(']');
        }
        if (d.executionContexts() != null) {
            // optional since 4.145.0: unspelled -> the slot is omitted
            b.append(",\"executionContexts\":[");
            for (int i = 0; i < d.executionContexts().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                dataSpaceContext(b, d.executionContexts().get(i));
            }
            b.append(']');
        }
        b.append(",\"name\":");
        ProtocolEmitter.str(b, d.name());
        Protocol.PDataSpaceOperationalMetadata om = d.operationalMetadata();
        if (om != null) {
            b.append(",\"operationalMetadata\":{");
            if (!om.coverageRegions().isEmpty()) {
                // an EMPTY list is omitted on the wire (NON_EMPTY)
                b.append("\"coverageRegions\":[");
                for (int i = 0; i < om.coverageRegions().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    ProtocolEmitter.str(b, om.coverageRegions().get(i));
                }
                b.append("],");
            }
            b.append("\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, om.sourceInformation());
            if (om.updateFrequency() != null) {
                b.append(",\"updateFrequency\":");
                ProtocolEmitter.str(b, om.updateFrequency());
            }
            b.append('}');
        }
        b.append(",\"package\":");
        ProtocolEmitter.str(b, d.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, d.sourceInformation());
        b.append(",\"stereotypes\":");
        ProtocolEmitter.stereotypes(b, d.stereotypes());
        if (d.supportInfo() != null) {
            b.append(",\"supportInfo\":");
            dataSpaceSupport(b, d.supportInfo());
        }
        b.append(",\"taggedValues\":");
        ProtocolEmitter.taggedValues(b, d.taggedValues());
        if (d.title() != null) {
            b.append(",\"title\":");
            ProtocolEmitter.str(b, d.title());
        }
        b.append('}');
    }

    private static void dataSpaceContext(StringBuilder b,
            Protocol.PDataSpaceContext ctx) {
        b.append('{');
        boolean first = true;
        if (ctx.defaultRuntime() != null) {
            b.append("\"defaultRuntime\":");
            pointer(b, ctx.defaultRuntime(), java.util.Objects.requireNonNull(ctx.runtimeSpan()), "RUNTIME");
            first = false;
        }
        if (ctx.description() != null) {
            b.append(first ? "" : ",").append("\"description\":");
            ProtocolEmitter.str(b, ctx.description());
            first = false;
        }
        if (ctx.mapping() != null) {
            b.append(first ? "" : ",").append("\"mapping\":");
            pointer(b, ctx.mapping(), java.util.Objects.requireNonNull(ctx.mappingSpan()), "MAPPING");
            first = false;
        }
        Protocol.PDataSpaceMappingProvider mp = ctx.mappingProvider();
        if (mp != null) {
            // 4.145.0: a TYPELESS element pointer plus its keys
            b.append(first ? "" : ",").append("\"mappingProvider\":{\"element\":{\"path\":");
            ProtocolEmitter.str(b, mp.element());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, mp.elementSpan());
            b.append("},\"keys\":[");
            for (int i = 0; i < mp.keys().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                ProtocolEmitter.str(b, mp.keys().get(i));
            }
            b.append("],\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, mp.sourceInformation());
            b.append('}');
            first = false;
        }
        b.append(first ? "" : ",").append("\"name\":");
        ProtocolEmitter.str(b, ctx.name());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, ctx.sourceInformation());
        Protocol.PDataSpaceTestData td = ctx.testData();
        if (td != null) {
            // Reference -> DATA pointer; DataspaceTestData -> DATASPACE
            // pointer (engine DataspaceDataElementReferenceParser)
            String ptrType = switch (td.kind()) {
                case "Reference" -> "DATA";
                case "DataspaceTestData" -> "DATASPACE";
                default -> throw new IllegalStateException(
                        "unprobed dataspace testData kind: " + td.kind());
            };
            b.append(",\"testData\":{\"_type\":\"reference\","
                    + "\"dataElement\":");
            pointer(b, td.path(), td.sourceInformation(), ptrType);
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, td.sourceInformation());
            b.append('}');
        }
        if (ctx.title() != null) {
            b.append(",\"title\":");
            ProtocolEmitter.str(b, ctx.title());
        }
        b.append('}');
    }

    private static void dataSpaceExecutable(StringBuilder b,
            Protocol.PDataSpaceExecutable e) {
        boolean template = e.query() != null;
        b.append("{\"_type\":\"").append(template
                ? "dataSpaceTemplateExecutable"
                : "dataSpacePackageableElementExecutable").append('"');
        if (e.description() != null) {
            b.append(",\"description\":");
            ProtocolEmitter.str(b, e.description());
        }
        if (!template) {
            String path = java.util.Objects.requireNonNull(e.executable());
            b.append(",\"executable\":{\"path\":");
            ProtocolEmitter.str(b, path);
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, java.util.Objects.requireNonNull(
                    e.executableSpan()));
            // a FUNCTION-pointer executable (signature form) carries a
            // type tag; a plain element pointer does not (DIFF-pinned)
            if (path.indexOf('(') >= 0) {
                b.append(",\"type\":\"FUNCTION\"");
            }
            b.append('}');
        }
        if (e.executionContextKey() != null) {
            b.append(",\"executionContextKey\":");
            ProtocolEmitter.str(b, e.executionContextKey());
        }
        if (e.id() != null) {
            b.append(",\"id\":");
            ProtocolEmitter.str(b, e.id());
        }
        if (template) {
            b.append(",\"query\":");
            ProtocolEmitter.valueSpec(b,
                    java.util.Objects.requireNonNull(e.query()));
        }
        if (e.sampleValues() != null) {
            // 4.145.0: a standalone relation element
            b.append(",\"sampleValues\":");
            MappingEmitter.relationElement(b, e.sampleValues());
        }
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, e.sourceInformation());
        b.append(",\"title\":");
        ProtocolEmitter.str(b, e.title());
        b.append('}');
    }

    private static void dataSpaceLink(StringBuilder b, Protocol.PDataSpaceLink l) {
        b.append('{');
        if (l.label() != null) {
            b.append("\"label\":");
            ProtocolEmitter.str(b, l.label());
            b.append(',');
        }
        b.append("\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, l.sourceInformation());
        b.append(",\"url\":");
        ProtocolEmitter.str(b, l.url());
        b.append('}');
    }

    private static void dataSpaceSupport(StringBuilder b,
            Protocol.PDataSpaceSupport s) {
        switch (s) {
            case Protocol.PDataSpaceSupport.PSupportFull f -> {
                // 4.145.0: the keyword-less full form
                b.append("{\"_type\":\"full\"");
                if (f.documentation() != null) {
                    b.append(",\"documentation\":");
                    dataSpaceLink(b, f.documentation());
                }
                if (f.emails() != null) {
                    b.append(",\"emails\":[");
                    for (int i = 0; i < f.emails().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        Protocol.PDataSpaceEmail em = f.emails().get(i);
                        b.append("{\"address\":");
                        ProtocolEmitter.str(b, em.address());
                        b.append(",\"sourceInformation\":");
                        ProtocolEmitter.srcInfo(b, em.sourceInformation());
                        b.append(",\"title\":");
                        ProtocolEmitter.str(b, em.title());
                        b.append('}');
                    }
                    b.append(']');
                }
                if (f.expertise() != null) {
                    b.append(",\"expertise\":[");
                    for (int i = 0; i < f.expertise().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        Protocol.PDataSpaceExpertise ex = f.expertise().get(i);
                        b.append('{');
                        boolean first = true;
                        if (ex.description() != null) {
                            b.append("\"description\":");
                            ProtocolEmitter.str(b, ex.description());
                            first = false;
                        }
                        if (ex.expertIds() != null) {
                            b.append(first ? "" : ",").append("\"expertIds\":[");
                            for (int j = 0; j < ex.expertIds().size(); j++) {
                                if (j > 0) {
                                    b.append(',');
                                }
                                ProtocolEmitter.str(b, ex.expertIds().get(j));
                            }
                            b.append(']');
                            first = false;
                        }
                        b.append(first ? "" : ",").append("\"sourceInformation\":");
                        ProtocolEmitter.srcInfo(b, ex.sourceInformation());
                        b.append('}');
                    }
                    b.append(']');
                }
                if (f.faqUrl() != null) {
                    b.append(",\"faqUrl\":");
                    dataSpaceLink(b, f.faqUrl());
                }
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, f.sourceInformation());
                if (f.supportUrl() != null) {
                    b.append(",\"supportUrl\":");
                    dataSpaceLink(b, f.supportUrl());
                }
                if (f.website() != null) {
                    b.append(",\"website\":");
                    dataSpaceLink(b, f.website());
                }
                b.append('}');
            }
            case Protocol.PDataSpaceSupport.PSupportEmail e -> {
                b.append("{\"_type\":\"email\",\"address\":");
                ProtocolEmitter.str(b, e.address());
                if (e.documentationUrl() != null) {
                    b.append(",\"documentationUrl\":");
                    ProtocolEmitter.str(b, e.documentationUrl());
                }
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, e.sourceInformation());
                b.append('}');
            }
            case Protocol.PDataSpaceSupport.PSupportCombined cb -> {
                b.append("{\"_type\":\"combined\"");
                if (cb.documentationUrl() != null) {
                    b.append(",\"documentationUrl\":");
                    ProtocolEmitter.str(b, cb.documentationUrl());
                }
                if (cb.emails() != null) {
                    b.append(",\"emails\":[");
                    for (int i = 0; i < cb.emails().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        ProtocolEmitter.str(b, cb.emails().get(i));
                    }
                    b.append(']');
                }
                if (cb.faqUrl() != null) {
                    b.append(",\"faqUrl\":");
                    ProtocolEmitter.str(b, cb.faqUrl());
                }
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, cb.sourceInformation());
                if (cb.supportUrl() != null) {
                    b.append(",\"supportUrl\":");
                    ProtocolEmitter.str(b, cb.supportUrl());
                }
                if (cb.website() != null) {
                    b.append(",\"website\":");
                    ProtocolEmitter.str(b, cb.website());
                }
                b.append('}');
            }
        }
    }

    /** {@code (slot:kind) -> wire _type} for the persistence sub-DSL.
     *  Unknown pairs THROW — the wall names the next probe. */
    private static final java.util.Map<String, String> PERSISTENCE_TYPES =
            java.util.Map.ofEntries(
                    java.util.Map.entry("trigger:Manual", "manualTrigger"),
                    java.util.Map.entry("trigger:Cron", "cronTrigger"),
                    java.util.Map.entry("persister:Batch", "batchPersister"),
                    java.util.Map.entry("persister:Streaming",
                            "streamingPersister"),
                    java.util.Map.entry("sink:Relational", "relationalSink"),
                    java.util.Map.entry("sink:ObjectStorage",
                            "objectStorageSink"),
                    java.util.Map.entry("ingestMode:BitemporalSnapshot",
                            "bitemporalSnapshot"),
                    java.util.Map.entry("ingestMode:NontemporalSnapshot",
                            "nontemporalSnapshot"),
                    java.util.Map.entry("ingestMode:UnitemporalSnapshot",
                            "unitemporalSnapshot"),
                    java.util.Map.entry("ingestMode:NontemporalDelta",
                            "nontemporalDelta"),
                    java.util.Map.entry("ingestMode:UnitemporalDelta",
                            "unitemporalDelta"),
                    java.util.Map.entry("ingestMode:BitemporalDelta",
                            "bitemporalDelta"),
                    java.util.Map.entry("ingestMode:AppendOnly", "appendOnly"),
                    java.util.Map.entry(
                            "transactionMilestoning:BatchIdAndDateTime",
                            "batchIdAndDateTimeTransactionMilestoning"),
                    java.util.Map.entry("transactionMilestoning:BatchId",
                            "batchIdTransactionMilestoning"),
                    java.util.Map.entry("transactionMilestoning:DateTime",
                            "dateTimeTransactionMilestoning"),
                    java.util.Map.entry("validityMilestoning:DateTime",
                            "dateTimeValidityMilestoning"),
                    java.util.Map.entry(
                            "derivation:SourceSpecifiesFromDateTime",
                            "sourceSpecifiesFromDateTime"),
                    java.util.Map.entry(
                            "derivation:SourceSpecifiesFromAndThruDateTime",
                            "sourceSpecifiesFromAndThruDateTime"),
                    java.util.Map.entry(
                            "derivation:SourceSpecifiesInDateTime",
                            "sourceSpecifiesInDateTime"),
                    java.util.Map.entry(
                            "derivation:SourceSpecifiesInAndOutDateTime",
                            "sourceSpecifiesInAndOutDateTime"),
                    java.util.Map.entry("targetShape:Flat", "flatTarget"),
                    java.util.Map.entry("targetShape:MultiFlat",
                            "multiFlatTarget"),
                    java.util.Map.entry("notifyee:Email", "emailNotifyee"),
                    java.util.Map.entry("notifyee:PagerDuty",
                            "pagerDutyNotifyee"),
                    java.util.Map.entry("serviceOutput:TDS",
                            "tdsServiceOutput"),
                    java.util.Map.entry("serviceOutput:GraphFetch",
                            "graphFetchServiceOutput"),
                    java.util.Map.entry("persistenceTarget:Relational",
                            "relationalPersistenceTarget"),
                    java.util.Map.entry("datasetType:Snapshot", "snapshot"),
                    java.util.Map.entry("datasetType:Delta", "delta"),
                    java.util.Map.entry("partitioning:None",
                            "noPartitioning"),
                    java.util.Map.entry("partitioning:FieldBased",
                            "fieldBasedForTds"),
                    java.util.Map.entry("partitioning@gf:FieldBased",
                            "fieldBasedForGraphFetch"),
                    java.util.Map.entry(
                            "deduplicationStrategy:DuplicateCount",
                            "duplicateCountDeduplicationStrategy"),
                    java.util.Map.entry("deduplication:None",
                            "noDeduplication"),
                    java.util.Map.entry("deduplication:AnyVersion",
                            "anyVersion"),
                    java.util.Map.entry("deduplication:MaxVersion",
                            "maxVersionForTds"),
                    java.util.Map.entry("deduplication@gf:MaxVersion",
                            "maxVersionForGraphFetch"),
                    java.util.Map.entry("emptyDatasetHandling:NoOp", "noOp"),
                    java.util.Map.entry(
                            "emptyDatasetHandling:DeleteTargetData",
                            "deleteTargetDataset"),
                    java.util.Map.entry("updatesHandling:Overwrite",
                            "overwrite"),
                    java.util.Map.entry("updatesHandling:AppendOnly",
                            "appendOnly"),
                    java.util.Map.entry("temporality:None", "none"),
                    java.util.Map.entry("temporality:Unitemporal",
                            "unitemporalTemporality"),
                    java.util.Map.entry("temporality:Bitemporal",
                            "bitemporalTemporality"),
                    java.util.Map.entry("auditing@tgt:DateTime",
                            "auditingDateTime"),
                    java.util.Map.entry("connectionData:ExternalFormat",
                            "externalFormat"),
                    java.util.Map.entry("assertion:EqualToJson",
                            "equalToJson"),
                    java.util.Map.entry("platform:AwsGlue", "awsGlue"),
                    java.util.Map.entry("sourceFields:StartAndEnd",
                            "sourceTimeStartAndEnd"),
                    java.util.Map.entry("sourceFields:Start", "sourceTimeStart"),
                    java.util.Map.entry("processingDimension:BatchId",
                            "batchId"),
                    java.util.Map.entry("processingDimension:DateTime",
                            "processingTime"),
                    java.util.Map.entry(
                            "processingDimension:BatchIdAndDateTime",
                            // the RELATIONAL-target protocol names this
                            // subtype batchIdAndProcessingTime, NOT the
                            // kind spelling (ProcessingDimension
                            // @JsonSubTypes; C11 resource sweep caught 4
                            // files)
                            "batchIdAndProcessingTime"),
                    java.util.Map.entry("sourceDerivedDimension:DateTime",
                            "sourceDerivedTime"),
                    java.util.Map.entry("appendStrategy:AllowDuplicates",
                            "allowDuplicates"),
                    java.util.Map.entry("appendStrategy:FailOnDuplicates",
                            "failOnDuplicates"),
                    java.util.Map.entry("appendStrategy:FilterDuplicates",
                            "filterDuplicates"),
                    java.util.Map.entry("deduplicationStrategy:None",
                            "noDeduplicationStrategy"),
                    java.util.Map.entry("deduplicationStrategy:MaxVersion",
                            "maxVersionDeduplicationStrategy"),
                    java.util.Map.entry("deduplicationStrategy:AnyVersion",
                            "anyVersionDeduplicationStrategy"),
                    java.util.Map.entry("actionIndicator@gf:DeleteIndicator",
                            "deleteIndicatorForGraphFetch"),
                    java.util.Map.entry("actionIndicator:None",
                            "noActionIndicator"),
                    java.util.Map.entry("actionIndicator@gf:None",
                            "noActionIndicator"),
                    java.util.Map.entry("actionIndicator:DeleteIndicator",
                            "deleteIndicatorForTds"),
                    java.util.Map.entry("auditing:None", "noAuditing"),
                    java.util.Map.entry("auditing:DateTime",
                            "dateTimeAuditing"),
                    java.util.Map.entry("mergeStrategy:NoDeletes",
                            "noDeletesMergeStrategy"),
                    java.util.Map.entry("mergeStrategy:DeleteIndicator",
                            "deleteIndicatorMergeStrategy"));

    private static String persistenceType(String slot, String kind,
            boolean gf) {
        return persistenceType(slot, kind, gf, false);
    }

    private static String persistenceType(String slot, String kind,
            boolean gf, boolean tgt) {
        String t = tgt ? PERSISTENCE_TYPES.get(slot + "@tgt:" + kind) : null;
        if (t == null && gf) {
            t = PERSISTENCE_TYPES.get(slot + "@gf:" + kind);
        }
        if (t == null) {
            t = PERSISTENCE_TYPES.get(slot + ":" + kind);
        }
        if (t == null) {
            throw new IllegalStateException("unprobed persistence node: "
                    + slot + ":" + kind + (gf ? " (graphFetch)" : "")
                    + (tgt ? " (target)" : ""));
        }
        return t;
    }

    static void persistence(StringBuilder b, Protocol.PPersistence p) {
        b.append("{\"_type\":\"persistence\"");
        if (p.doc() != null) {
            b.append(",\"documentation\":");
            ProtocolEmitter.str(b, p.doc());
        }
        b.append(",\"name\":");
        ProtocolEmitter.str(b, p.name());
        b.append(",\"notifier\":{\"notifyees\":[");
        Protocol.PPersistenceNotifier nf = p.notifier();
        if (nf != null) {
            for (int i = 0; i < nf.notifyees().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                persistenceNode(b, "notifyee", nf.notifyees().get(i), true);
            }
        }
        b.append(']');
        if (nf != null && nf.sourceInformation() != null) {
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, nf.sourceInformation());
        }
        b.append("},\"package\":");
        ProtocolEmitter.str(b, p.pkg());
        if (p.persister() != null) {
            b.append(",\"persister\":");
            persistenceNode(b, "persister", p.persister(), true);
        }
        if (p.service() != null) {
            b.append(",\"service\":");
            pointer(b, p.service(),
                    java.util.Objects.requireNonNull(p.serviceSpan()),
                    "SERVICE");
        }
        b.append(",\"serviceOutputTargets\":[");
        if (p.serviceOutputTargets() != null) {
            for (int i = 0; i < p.serviceOutputTargets().size(); i++) {
                Protocol.PServiceOutputTarget t =
                        p.serviceOutputTargets().get(i);
                if (i > 0) {
                    b.append(',');
                }
                b.append('{');
                if (!"__empty__".equals(t.persistenceTarget().kind())) {
                    b.append("\"persistenceTarget\":");
                    persistenceNode(b, "persistenceTarget",
                            t.persistenceTarget(), true);
                    b.append(',');
                }
                b.append("\"serviceOutput\":");
                persistenceNode(b, "serviceOutput", t.serviceOutput(), true);
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, t.sourceInformation());
                b.append('}');
            }
        }
        b.append("],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, p.sourceInformation());
        if (p.tests() != null) {
            b.append(",\"tests\":[");
            for (int i = 0; i < p.tests().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                persistenceTest(b, p.tests().get(i));
            }
            b.append(']');
        }
        b.append(",\"trigger\":{\"_type\":\"")
                .append(persistenceType("trigger", p.triggerKind(), false))
                .append("\"}}");
    }

    /** Leaf {@code _type}s the wire prints WITHOUT sourceInformation
     *  (DIFF-pinned: noAuditing; manualTrigger handled at its slot). */
    private static final java.util.Set<String> SPANLESS_LEAVES =
            java.util.Set.of("noAuditing", "noDeletesMergeStrategy",
                    "deleteIndicatorMergeStrategy");

    /** The generic node walk: {@code _type} first, entries alphabetical by
     *  key with {@code sourceInformation} merged into sorted position. */
    private static void persistenceNode(StringBuilder b, String slot,
            Protocol.PPersistenceNode n, boolean withSpan) {
        persistenceNode(b, slot, n, withSpan, false, false);
    }

    private static void persistenceNode(StringBuilder b, String slot,
            Protocol.PPersistenceNode n, boolean withSpan, boolean gf) {
        persistenceNode(b, slot, n, withSpan, gf, false);
    }

    /** {@code gf} = inside a graphFetch service output, {@code tgt} =
     *  inside a persistenceTarget — both wire context-dependent
     *  {@code _type}s and span rules. */
    private static void persistenceNode(StringBuilder b, String slot,
            Protocol.PPersistenceNode n, boolean withSpan, boolean gf,
            boolean tgt) {
        boolean part = "__part__".equals(n.kind());
        boolean pathHead = "#path".equals(n.kind());
        if (pathHead) {
            gf = true;
        }
        if ("persistenceTarget".equals(slot)) {
            tgt = true;
        }
        String wireType = part ? null
                : pathHead ? "graphFetchServiceOutput"
                        : persistenceType(slot, n.kind(), gf, tgt);
        // V1-context leaves drop their spans; the SAME kinds inside a
        // persistenceTarget keep them (DIFF-pinned). Among the spelled
        // deduplication strategies only no/duplicateCount are spanless.
        if (wireType != null
                && ((SPANLESS_LEAVES.contains(wireType) && !tgt)
                        || "noDeduplicationStrategy".equals(wireType)
                        // a BARE-keyword strategy (no body) is spanless;
                        // one with entries keeps its span — engine emits
                        // anyVersion bare + duplicateCount spanned in the
                        // SAME fixture (harvest persistenceMultiFlat)
                        || ("deduplicationStrategy".equals(slot)
                                && n.entries().isEmpty()))) {
            withSpan = false;
        }
        b.append('{');
        boolean first = true;
        if (wireType != null) {
            b.append("\"_type\":\"").append(wireType).append('"');
            first = false;
        }
        List<Protocol.PPersistenceEntry> entries =
                new java.util.ArrayList<>(n.entries());
        if (pathHead) {
            entries.add(new Protocol.PPersistenceEntry.PathValue("path",
                    java.util.Objects.requireNonNull(n.headPath())));
        }
        if (("targetShape".equals(slot) && "Flat".equals(n.kind())) || part) {
            // flatTarget and each MultiFlat part inject defaults (probed)
            if (entries.stream().noneMatch(e ->
                    "deduplicationStrategy".equals(e.key()))) {
                entries.add(new Protocol.PPersistenceEntry.Node(
                        "deduplicationStrategy",
                        new Protocol.PPersistenceNode("__noDedup__",
                                List.of(), n.sourceInformation())));
            }
            if (entries.stream().noneMatch(e ->
                    "partitionFields".equals(e.key()))) {
                entries.add(new Protocol.PPersistenceEntry.Strings(
                        "partitionFields", List.of()));
            }
        }
        final boolean tgtCtx = tgt;
        final boolean gfPartition = gf
                && "fieldBasedForGraphFetch".equals(wireType);
        java.util.function.Function<Protocol.PPersistenceEntry, String>
                keyOf = e2 -> {
                    String k = wireKey(e2, tgtCtx);
                    // v2 GRAPH-FETCH partitioning renames the field list
                    // (harvest TestPersistenceGrammarV2Roundtrip
                    // persistenceSnapshot): source `partitionFields:` wires
                    // as `partitionFieldPaths` under fieldBasedForGraphFetch
                    return gfPartition && "partitionFields".equals(k)
                            ? "partitionFieldPaths" : k;
                };
        entries.sort(java.util.Comparator.comparing(keyOf));
        boolean spanEmitted = !withSpan;
        for (Protocol.PPersistenceEntry e : entries) {
            String key = keyOf.apply(e);
            if (!spanEmitted && key.compareTo("sourceInformation") > 0) {
                if (!first) {
                    b.append(',');
                }
                first = false;
                b.append("\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, n.sourceInformation());
                spanEmitted = true;
            }
            if (!first) {
                b.append(',');
            }
            first = false;
            b.append('"').append(key).append("\":");
            switch (e) {
                case Protocol.PPersistenceEntry.Scalar s -> {
                    if (!s.quoted() && ("true".equals(s.value())
                            || "false".equals(s.value())
                            || s.value().chars().allMatch(
                                    Character::isDigit))) {
                        b.append(s.value());
                    } else {
                        ProtocolEmitter.str(b, s.value());
                    }
                }
                case Protocol.PPersistenceEntry.Node nd -> {
                    if ("__noDedup__".equals(nd.node().kind())) {
                        b.append("{\"_type\":\"noDeduplicationStrategy\"}");
                    } else {
                        persistenceNode(b, nd.key(), nd.node(), true, gf, tgt);
                    }
                }
                case Protocol.PPersistenceEntry.Pointer pt -> {
                    if ("binding".equals(pt.key())) {
                        // binding pointers carry NO type tag (DIFF-pinned)
                        b.append("{\"path\":");
                        ProtocolEmitter.str(b, pt.path());
                        b.append(",\"sourceInformation\":");
                        ProtocolEmitter.srcInfo(b, pt.sourceInformation());
                        b.append('}');
                    } else {
                        pointer(b, pt.path(), pt.sourceInformation(),
                                "STORE");
                    }
                }
                case Protocol.PPersistenceEntry.Strings ss -> {
                    b.append('[');
                    for (int i = 0; i < ss.values().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        ProtocolEmitter.str(b, ss.values().get(i));
                    }
                    b.append(']');
                }
                case Protocol.PPersistenceEntry.PathValue pv ->
                        ProtocolEmitter.pathValue(b,
                                (com.legend.protocol.spec.PathLiteral)
                                        pv.spec());
                case Protocol.PPersistenceEntry.PathList pl -> {
                    b.append('[');
                    for (int i = 0; i < pl.specs().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        ProtocolEmitter.pathValue(b,
                                (com.legend.protocol.spec.PathLiteral)
                                        pl.specs().get(i));
                    }
                    b.append(']');
                }
                case Protocol.PPersistenceEntry.NodeList nl -> {
                    b.append('[');
                    for (int i = 0; i < nl.nodes().size(); i++) {
                        if (i > 0) {
                            b.append(',');
                        }
                        persistenceNode(b, nl.key(), nl.nodes().get(i),
                                true, gf, tgt);
                    }
                    b.append(']');
                }
            }
        }
        if (!spanEmitted) {
            if (!first) {
                b.append(',');
            }
            b.append("\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, n.sourceInformation());
        }
        b.append('}');
    }

    /** The wire slot name for an entry — {@code deleteField} path values
     *  wire as {@code deleteFieldPath} (probed). */
    private static String wireKey(Protocol.PPersistenceEntry e) {
        if (e instanceof Protocol.PPersistenceEntry.PathValue) {
            if ("deleteField".equals(e.key())) {
                return "deleteFieldPath";
            }
            if ("versionField".equals(e.key())) {
                return "versionFieldPath";
            }
        }
        return e.key();
    }

    /** V2 TARGET dialect key renames (protocol-class-pinned:
     *  ProcessingDateTime.timeIn/timeOut, SourceDerivedTime
     *  .timeStart/timeEnd, AuditingDateTime.auditingDateTimeName,
     *  SourceDerivedDimension.sourceTimeFields) — V1 keys pass through. */
    private static String wireKey(Protocol.PPersistenceEntry e, boolean tgt) {
        if (!tgt) {
            return wireKey(e);
        }
        return switch (e.key()) {
            case "batchDateTimeName", "dateTimeName" ->
                    "auditingDateTimeName";
            case "dateTimeIn" -> "timeIn";
            case "dateTimeOut" -> "timeOut";
            case "dateTimeStart" -> "timeStart";
            case "dateTimeEnd" -> "timeEnd";
            case "sourceFields" -> "sourceTimeFields";
            default -> wireKey(e);
        };
    }


    /** One legacy assert entry: assert lambda + parametersValues (spec
     *  values; harvest testServiceTestParameters). */
    private static void legacyAssert(StringBuilder b,
            Protocol.PLegacyServiceTest.PLegacyAssert a) {
        b.append("{\"assert\":");
        if (a.assertion() instanceof com.legend.protocol.spec.LambdaFunction) {
            ProtocolEmitter.valueSpec(b, a.assertion());
        } else {
            // a BARE expression assert wires wrapped in a parameterless
            // lambda with NO span (harvest testServiceTestParameters)
            b.append("{\"_type\":\"lambda\",\"body\":[");
            ProtocolEmitter.valueSpec(b, a.assertion());
            b.append("],\"parameters\":[]}");
        }
        b.append(",\"parametersValues\":[");
        for (int i = 0; i < a.parametersValues().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            var v = a.parametersValues().get(i);
            if (v instanceof com.legend.protocol.spec.AppliedFunction af
                    && "list".equals(af.function())
                    && af.parameters().size() == 1
                    && af.parameters().get(0)
                            instanceof com.legend.protocol.spec.PureCollection pc) {
                // list([...]) wires as classInstance/listInstance; outer
                // AND inner si = the whole call's span (probed); a BARE
                // nested [..] stays a plain collection
                b.append("{\"_type\":\"classInstance\","
                        + "\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, java.util.Objects.requireNonNull(
                        af.pos()));
                b.append(",\"type\":\"listInstance\",\"value\":{"
                        + "\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, java.util.Objects.requireNonNull(
                        af.pos()));
                b.append(",\"values\":[");
                for (int j = 0; j < pc.values().size(); j++) {
                    if (j > 0) {
                        b.append(',');
                    }
                    ProtocolEmitter.valueSpec(b, pc.values().get(j));
                }
                b.append("]}}");
            } else {
                ProtocolEmitter.valueSpec(b, v);
            }
        }
        b.append("],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, a.sourceInformation());
        b.append('}');
    }

    static void persistenceContext(StringBuilder b,
            Protocol.PPersistenceContext pc) {
        b.append("{\"_type\":\"persistenceContext\",\"name\":");
        ProtocolEmitter.str(b, pc.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, pc.pkg());
        b.append(",\"persistence\":");
        pointer(b, pc.persistence(), pc.persistenceSpan(), "PERSISTENCE");
        // platform is ALWAYS on the wire — {_type:"default"} when unspelled
        b.append(",\"platform\":");
        if (pc.platform() == null) {
            b.append("{\"_type\":\"default\"}");
        } else if (pc.platform().entries().isEmpty()) {
            // BARE kind: `platform: Default;` — _type is the kind
            // lowercased-first, WITH the key..';' span (a SPELLED default
            // differs from an unspelled one exactly by carrying its span)
            String k = pc.platform().kind();
            b.append("{\"_type\":\"")
                    .append(Character.toLowerCase(k.charAt(0)))
                    .append(k.substring(1))
                    .append("\",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, pc.platform().sourceInformation());
            b.append('}');
        } else {
            persistenceNode(b, "platform", pc.platform(), true);
        }
        b.append(",\"serviceParameters\":[");
        for (int i = 0; i < pc.serviceParameters().size(); i++) {
            Protocol.PCtxParam p = pc.serviceParameters().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"name\":");
            ProtocolEmitter.str(b, p.name());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, p.sourceInformation());
            b.append(",\"value\":");
            switch (p.value()) {
                case Protocol.PCtxParamValue.Primitive pv -> {
                    b.append("{\"_type\":\"primitiveTypeValue\","
                            + "\"primitiveType\":");
                    ProtocolEmitter.valueSpec(b, pv.spec());
                    b.append('}');
                }
                case Protocol.PCtxParamValue.ConnectionPtr cp -> {
                    b.append("{\"_type\":\"connectionValue\","
                            + "\"connection\":{\"_type\":"
                            + "\"connectionPointer\",\"connection\":");
                    ProtocolEmitter.str(b, cp.path());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, cp.sourceInformation());
                    b.append("}}");
                }
                case Protocol.PCtxParamValue.ConnectionVal cv -> {
                    b.append("{\"_type\":\"connectionValue\","
                            + "\"connection\":");
                    ProtocolEmitter.connectionValue(b, cv.connection());
                    b.append('}');
                }
            }
            b.append('}');
        }
        b.append(']');
        if (pc.sinkConnection() != null) {
            b.append(",\"sinkConnection\":");
            ProtocolEmitter.connectionValue(b, pc.sinkConnection());
        }
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, pc.sourceInformation());
        b.append('}');
    }

    private static void persistenceTest(StringBuilder b,
            Protocol.PPersistenceTest t) {
        if (t.graphFetchPath() != null) {
            b.append("{\"_type\":\"test\",\"graphFetchPath\":");
            ProtocolEmitter.pathValue(b,
                    (com.legend.protocol.spec.PathLiteral) t.graphFetchPath());
            b.append(",\"id\":");
            ProtocolEmitter.str(b, t.id());
            b.append(",\"isTestDataFromServiceOutput\":")
                    .append(t.isTestDataFromServiceOutput())
                    .append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, t.sourceInformation());
            b.append(",\"testBatches\":[");
            persistenceTestBatches(b, t);
            b.append("]}");
            return;
        }
        b.append("{\"_type\":\"test\",\"id\":");
        ProtocolEmitter.str(b, t.id());
        b.append(",\"isTestDataFromServiceOutput\":")
                .append(t.isTestDataFromServiceOutput())
                .append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, t.sourceInformation());
        b.append(",\"testBatches\":[");
        persistenceTestBatches(b, t);
        b.append("]}");
    }

    private static void persistenceTestBatches(StringBuilder b,
            Protocol.PPersistenceTest t) {
        for (int i = 0; i < t.testBatches().size(); i++) {
            Protocol.PPersistenceTestBatch tb = t.testBatches().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"assertions\":[");
            for (int a = 0; a < tb.asserts().size(); a++) {
                Protocol.PPersistenceAssert as = tb.asserts().get(a);
                if (a > 0) {
                    b.append(',');
                }
                b.append("{\"_type\":\"")
                        .append(persistenceType("assertion",
                                as.assertion().kind(), false))
                        .append('"');
                for (Protocol.PPersistenceEntry e
                        : as.assertion().entries()) {
                    b.append(",\"").append(e.key()).append("\":");
                    // exhaustive over the sealed type (deep-audit 1e: the
                    // old if/else silently emitted a key with NO value —
                    // invalid JSON — for the other five variants)
                    switch (e) {
                        case Protocol.PPersistenceEntry.Node nd ->
                                persistenceNode(b, "connectionData",
                                        nd.node(), true);
                        case Protocol.PPersistenceEntry.Scalar s ->
                                ProtocolEmitter.str(b, s.value());
                        default -> throw new UnsupportedOperationException(
                                "TailEmitter has no wire rule for persistence"
                                        + " assertion entry '" + e.key() + "' ("
                                        + e.getClass().getSimpleName()
                                        + ") — probe, do not guess.");
                    }
                }
                b.append(",\"id\":");
                ProtocolEmitter.str(b, as.id());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, as.sourceInformation());
                b.append('}');
            }
            // batchId AUTO-NUMBERS in source order (probed)
            b.append("],\"batchId\":").append(i).append(",\"id\":");
            ProtocolEmitter.str(b, tb.id());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, tb.sourceInformation());
            b.append(",\"testData\":{\"connection\":{\"data\":");
            persistenceNode(b, "connectionData", tb.connectionData(), true);
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, tb.connectionSpan());
            b.append("},\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, tb.dataSpan());
            b.append("}}");
        }
    }

    static void service(StringBuilder b, Protocol.PService sv) {
        b.append("{\"_type\":\"service\"");
        b.append(",\"autoActivateUpdates\":").append(
                Boolean.TRUE.equals(sv.autoActivateUpdates()));
        if (sv.documentation() != null) {
            b.append(",\"documentation\":");
            ProtocolEmitter.str(b, sv.documentation());
        }
        b.append(",\"execution\":");
        serviceExecution(b, sv.execution());
        if (sv.mcpServer() != null) {
            b.append(",\"mcpServer\":");
            ProtocolEmitter.str(b, sv.mcpServer());
        }
        b.append(",\"name\":");
        ProtocolEmitter.str(b, sv.name());
        b.append(",\"owners\":[");
        for (int i = 0; i < sv.owners().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, sv.owners().get(i));
        }
        b.append(']');
        if (sv.ownershipKind() != null) {
            if (!"DID".equals(sv.ownershipKind())
                    && !"UserList".equals(sv.ownershipKind())) {
                throw new IllegalStateException("unprobed service ownership"
                        + " kind: " + sv.ownershipKind());
            }
            if (sv.ownershipUsers() != null) {
                b.append(",\"ownership\":{\"_type\":\"userListOwnership\","
                        + "\"users\":[");
                for (int i = 0; i < sv.ownershipUsers().size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    ProtocolEmitter.str(b, sv.ownershipUsers().get(i));
                }
                b.append("]}");
            } else {
                b.append(",\"ownership\":{\"_type\":"
                        + "\"deploymentOwnership\",\"identifier\":");
                ProtocolEmitter.str(b, java.util.Objects.requireNonNull(
                        sv.ownershipId()));
                b.append('}');
            }
        }
        b.append(",\"package\":");
        ProtocolEmitter.str(b, sv.pkg());
        if (sv.pattern() != null) {
            b.append(",\"pattern\":");
            ProtocolEmitter.str(b, sv.pattern());
        }
        b.append(",\"postValidations\":[");
        if (sv.postValidations() != null) {
            for (int i = 0; i < sv.postValidations().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                Protocol.PPostValidation pv = sv.postValidations().get(i);
                b.append("{\"assertions\":[");
                for (int j = 0; j < pv.assertions().size(); j++) {
                    if (j > 0) {
                        b.append(',');
                    }
                    var a = pv.assertions().get(j);
                    b.append("{\"assertion\":");
                    ProtocolEmitter.valueSpec(b, a.assertion());
                    b.append(",\"id\":");
                    ProtocolEmitter.str(b, a.id());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, a.sourceInformation());
                    b.append('}');
                }
                b.append("],\"description\":");
                ProtocolEmitter.str(b, pv.description());
                b.append(",\"parameters\":[");
                for (int j = 0; j < pv.parameters().size(); j++) {
                    if (j > 0) {
                        b.append(',');
                    }
                    ProtocolEmitter.valueSpec(b, pv.parameters().get(j));
                }
                b.append("],\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, pv.sourceInformation());
                b.append('}');
            }
        }
        b.append("],\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, sv.sourceInformation());
        b.append(",\"stereotypes\":");
        ProtocolEmitter.stereotypes(b, sv.stereotypes());
        b.append(",\"taggedValues\":");
        ProtocolEmitter.taggedValues(b, sv.taggedValues());
        Protocol.PLegacyServiceTest t = sv.test();
        if (t != null && "Multi".equals(t.kind())) {
            b.append(",\"test\":{\"_type\":\"multiExecutionTest\","
                    + "\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, t.sourceInformation());
            b.append(",\"tests\":[");
            for (int i = 0; i < t.keyedTests().size(); i++) {
                var kt = t.keyedTests().get(i);
                if (i > 0) {
                    b.append(',');
                }
                b.append("{\"asserts\":[");
                for (int j = 0; j < kt.asserts().size(); j++) {
                    var a = kt.asserts().get(j);
                    if (j > 0) {
                        b.append(',');
                    }
                    legacyAssert(b, a);
                }
                b.append("],\"data\":");
                ProtocolEmitter.str(b, kt.data());
                b.append(",\"key\":");
                ProtocolEmitter.str(b, kt.key());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, kt.sourceInformation());
                b.append('}');
            }
            b.append("]}");
            t = null;
        }
        if (t != null) {
            b.append(",\"test\":{\"_type\":\"singleExecutionTest\","
                    + "\"asserts\":[");
            for (int i = 0; i < t.asserts().size(); i++) {
                var a = t.asserts().get(i);
                if (i > 0) {
                    b.append(',');
                }
                legacyAssert(b, a);
            }
            b.append("],\"data\":");
            ProtocolEmitter.str(b,
                    java.util.Objects.requireNonNull(t.data()));
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, t.sourceInformation());
            b.append('}');
        }
        if (sv.testSuites() != null) {
            b.append(",\"testSuites\":[");
            for (int i = 0; i < sv.testSuites().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                serviceTestSuite(b, sv.testSuites().get(i));
            }
            b.append(']');
        }
        if (sv.title() != null) {
            b.append(",\"title\":");
            ProtocolEmitter.str(b, sv.title());
        }
        b.append('}');
    }

    static void executionEnvironment(StringBuilder b,
            Protocol.PExecutionEnvironment ee) {
        b.append("{\"_type\":\"executionEnvironmentInstance\","
                + "\"executionParameters\":[");
        for (int i = 0; i < ee.executions().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            switch (ee.executions().get(i)) {
                case Protocol.PKeyedExecution single ->
                        singleExecutionParameters(b, single);
                case Protocol.PMultiKeyedExecution m -> {
                    // no span of its own; masterKey then the singles
                    // (harvest testExecutionEnvironmentInMultiExecService)
                    b.append("{\"_type\":\"multiExecutionParameters\","
                            + "\"masterKey\":");
                    ProtocolEmitter.str(b, m.masterKey());
                    b.append(",\"singleExecutionParameters\":[");
                    for (int j = 0; j < m.singles().size(); j++) {
                        if (j > 0) {
                            b.append(',');
                        }
                        singleExecutionParameters(b, m.singles().get(j));
                    }
                    b.append("]}");
                }
            }
        }
        b.append("],\"name\":");
        ProtocolEmitter.str(b, ee.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, ee.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, ee.sourceInformation());
        b.append('}');
    }

    private static void singleExecutionParameters(StringBuilder b,
            Protocol.PKeyedExecution k) {
        {
            b.append("{\"_type\":\"singleExecutionParameters\","
                    + "\"key\":");
            ProtocolEmitter.str(b, k.keyValue());
            if (k.mapping() != null) {
                b.append(",\"mapping\":");
                ProtocolEmitter.str(b, k.mapping());
                b.append(",\"mappingSourceInformation\":");
                ProtocolEmitter.srcInfo(b,
                        java.util.Objects.requireNonNull(k.mappingSpan()));
            }
            if (k.runtime() != null || k.embeddedRuntime() != null) {
                // "runtime" sorts BEFORE "runtimeComponents"; a keyed
                // execution may carry BOTH (harvest
                // testMappingRuntimeCompatibility)
                serviceRuntime(b, k.runtime(), k.runtimeSpan(),
                        k.embeddedRuntime());
            }
            if (k.runtimeComponents() != null) {
                // wire: binding/clazz pointers + runtimePointer,
                // alphabetical (harvest testExecutionEnvironment)
                Protocol.PRuntimeComponents rc = k.runtimeComponents();
                b.append(",\"runtimeComponents\":{\"binding\":");
                ProtocolEmitter.pointer(b, rc.binding());
                b.append(",\"clazz\":");
                ProtocolEmitter.pointer(b, rc.clazz());
                b.append(",\"runtime\":{\"_type\":\"runtimePointer\","
                        + "\"runtime\":");
                ProtocolEmitter.str(b, rc.runtime());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, rc.runtimeSpan());
                b.append("}}");
            }
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, k.sourceInformation());
            b.append('}');
        }
    }

    private static void serviceTestSuite(StringBuilder b,
            Protocol.PServiceTestSuite st) {
        b.append("{\"_type\":\"serviceTestSuite\"");
        if (st.doc() != null) {
            b.append(",\"doc\":");
            ProtocolEmitter.str(b, st.doc());
        }
        b.append(",\"id\":");
        ProtocolEmitter.str(b, st.id());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, st.sourceInformation());
        if (st.testData() != null) {
            b.append(",\"testData\":{");
            var cds = st.testData().connectionsTestData();
            if (!cds.isEmpty()) {
                b.append("\"connectionsTestData\":[");
                for (int i = 0; i < cds.size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    b.append("{\"data\":");
                    MappingEmitter.embeddedDataValue(b, cds.get(i).data());
                    b.append(",\"id\":");
                    ProtocolEmitter.str(b, cds.get(i).id());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b,
                            cds.get(i).sourceInformation());
                    b.append('}');
                }
                b.append("],");
            }
            var rds = st.testData().serviceTestData();
            if (rds != null && !rds.isEmpty()) {
                // COMPACT-form resolver entries (ZServiceV2Probe): the
                // elementPointer carries NO "type" key
                b.append("\"serviceTestData\":[");
                for (int i = 0; i < rds.size(); i++) {
                    if (i > 0) {
                        b.append(',');
                    }
                    var rd = rds.get(i);
                    b.append(rd.data() != null
                            ? "{\"_type\":\"baseDataResolver\",\"data\":"
                            : "{\"_type\":\"referenceDataResolver\"");
                    if (rd.data() != null) {
                        MappingEmitter.embeddedDataValue(b, rd.data());
                    }
                    b.append(",\"elementPointer\":{\"path\":");
                    ProtocolEmitter.str(b, rd.elementPath());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b,
                            rd.elementSourceInformation());
                    b.append("},\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, rd.sourceInformation());
                    b.append('}');
                }
                b.append("],");
            }
            b.append("\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, st.testData().sourceInformation());
            b.append('}');
        }
        b.append(",\"tests\":[");
        for (int i = 0; i < st.tests().size(); i++) {
            Protocol.PServiceTestSuite.PSuiteTest t = st.tests().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"_type\":\"serviceTest\",\"assertions\":[");
            for (int j = 0; j < t.assertions().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PTestAssertion a = t.assertions().get(j);
                switch (a.expected()) {
                    case Protocol.PExternalFormatData ef -> {
                        b.append("{\"_type\":\"equalToJson\","
                                + "\"expected\":");
                        MappingEmitter.externalFormatData(b, ef);
                    }
                    case Protocol.PRelationElement re -> {
                        // COMPACT-form `=> Relation #{...}#`
                        // (ZServiceV2Probe): a BARE relation element
                        b.append("{\"_type\":\"equalToRelation\","
                                + "\"expected\":");
                        MappingEmitter.relationElement(b, re);
                    }
                    case Protocol.PEqualToValue ev -> {
                        b.append("{\"_type\":\"equalTo\","
                                + "\"expected\":");
                        ProtocolEmitter.valueSpec(b, ev.value());
                    }
                }
                b.append(",\"id\":");
                ProtocolEmitter.str(b, a.id());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, a.sourceInformation());
                b.append('}');
            }
            b.append(']');
            if (t.doc() != null) {
                b.append(",\"doc\":");
                ProtocolEmitter.str(b, t.doc());
            }
            b.append(",\"id\":");
            ProtocolEmitter.str(b, t.id());
            b.append(",\"keys\":[");
            for (int j = 0; j < t.keys().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                ProtocolEmitter.str(b, t.keys().get(j));
            }
            b.append(']');
            if (t.parameters() != null) {
                b.append(",\"parameters\":[");
                for (int j = 0; j < t.parameters().size(); j++) {
                    if (j > 0) {
                        b.append(',');
                    }
                    b.append("{\"name\":");
                    ProtocolEmitter.str(b,
                            t.parameters().get(j).name());
                    b.append(",\"value\":");
                    paramValue(b, t.parameters().get(j).value());
                    b.append('}');
                }
                b.append(']');
            }
            if (t.serializationFormat() != null) {
                b.append(",\"serializationFormat\":");
                ProtocolEmitter.str(b, t.serializationFormat());
            }
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, t.sourceInformation());
            b.append('}');
        }
        b.append("]}");
    }

    private static void serviceExecution(StringBuilder b,
            Protocol.PServiceExecution e) {
        switch (e) {
            case Protocol.PSingleExecution se -> {
                b.append("{\"_type\":\"pureSingleExecution\",\"func\":");
                serviceFunc(b, se.query());
                if (se.mapping() != null) {
                    b.append(",\"mapping\":");
                    ProtocolEmitter.str(b, se.mapping());
                    b.append(",\"mappingSourceInformation\":");
                    ProtocolEmitter.srcInfo(b,
                            java.util.Objects.requireNonNull(
                                    se.mappingSpan()));
                }
                serviceRuntime(b, se.runtime(), se.runtimeSpan(),
                        se.embeddedRuntime());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, se.sourceInformation());
                b.append('}');
            }
            case Protocol.PMultiExecution me -> {
                b.append("{\"_type\":\"pureMultiExecution\"");
                if (me.executionKey() != null) {
                    b.append(",\"executionKey\":");
                    ProtocolEmitter.str(b, me.executionKey());
                }
                if (me.executions() == null) {
                    b.append(",\"func\":");
                    serviceFunc(b, me.query());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, me.sourceInformation());
                    b.append('}');
                    break;
                }
                b.append(",\"executionParameters\":[");
                for (int i = 0; i < me.executions().size(); i++) {
                    Protocol.PKeyedExecution k = me.executions().get(i);
                    if (i > 0) {
                        b.append(',');
                    }
                    b.append("{\"key\":");
                    ProtocolEmitter.str(b, k.keyValue());
                    if (k.mapping() != null) {
                        b.append(",\"mapping\":");
                        ProtocolEmitter.str(b, k.mapping());
                        b.append(",\"mappingSourceInformation\":");
                        ProtocolEmitter.srcInfo(b,
                                java.util.Objects.requireNonNull(
                                        k.mappingSpan()));
                    }
                    serviceRuntime(b, k.runtime(), k.runtimeSpan(),
                            k.embeddedRuntime());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, k.sourceInformation());
                    b.append('}');
                }
                b.append("],\"func\":");
                serviceFunc(b, me.query());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, me.sourceInformation());
                b.append('}');
            }
        }
    }

    /** Test-parameter values: the engine's TEST walker wires a REAL
     *  enumValue node (span = the whole dotted text) where expression
     *  position uses property-form; collections rewrite one level deep
     *  (probed service-test-params). */
    private static void paramValue(StringBuilder b,
            com.legend.protocol.spec.ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.EnumValue e) {
            SourceInfo ep = java.util.Objects.requireNonNull(
                    e.enumerationPos());
            SourceInfo vp = java.util.Objects.requireNonNull(e.pos());
            b.append("{\"_type\":\"enumValue\",\"fullPath\":");
            ProtocolEmitter.str(b, e.fullPath());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, new SourceInfo(ep.sourceId(),
                    ep.startLine(), ep.startColumn(), vp.endLine(),
                    vp.endColumn()));
            b.append(",\"value\":");
            ProtocolEmitter.str(b, e.value());
            b.append('}');
            return;
        }
        if (v instanceof com.legend.protocol.spec.PureCollection pc
                && pc.values().stream().anyMatch(x -> x instanceof
                        com.legend.protocol.spec.EnumValue)) {
            b.append("{\"_type\":\"collection\",\"multiplicity\":"
                    + "{\"lowerBound\":").append(pc.values().size())
                    .append(",\"upperBound\":")
                    .append(pc.values().size())
                    .append("},\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, java.util.Objects.requireNonNull(
                    pc.pos()));
            b.append(",\"values\":[");
            for (int i = 0; i < pc.values().size(); i++) {
                if (i > 0) {
                    b.append(',');
                }
                paramValue(b, pc.values().get(i));
            }
            b.append("]}");
            return;
        }
        ProtocolEmitter.valueSpec(b, v);
    }

    /** The wire query is ALWAYS a lambda — a bare expression wraps as a
     *  zero-parameter lambda WITHOUT sourceInformation (DIFF-pinned). */
    private static void serviceFunc(StringBuilder b,
            com.legend.protocol.spec.ValueSpecification q) {
        if (q instanceof com.legend.protocol.spec.LambdaFunction) {
            ProtocolEmitter.valueSpec(b, q);
            return;
        }
        b.append("{\"_type\":\"lambda\",\"body\":[");
        ProtocolEmitter.valueSpec(b, q);
        b.append("],\"parameters\":[]}");
    }

    /** {@code runtimePointer | engineRuntime} — the embedded form reuses
     *  the runtime element's ARRAY emission. */
    private static void serviceRuntime(StringBuilder b,
            @com.legend.Nullable String runtime,
            @com.legend.Nullable SourceInfo runtimeSpan,
            @com.legend.Nullable Protocol.PEmbeddedRuntime embedded) {
        if (runtime != null) {
            b.append(",\"runtime\":{\"_type\":\"runtimePointer\","
                    + "\"runtime\":");
            ProtocolEmitter.str(b, runtime);
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b,
                    java.util.Objects.requireNonNull(runtimeSpan));
            b.append('}');
        } else if (embedded != null) {
            b.append(",\"runtime\":{\"_type\":\"engineRuntime\"");
            ProtocolEmitter.runtimeArrays(b, embedded.connectionStores(),
                    embedded.connections(), embedded.mappings());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, embedded.sourceInformation());
            b.append('}');
        }
    }

    static void dataQualityValidation(StringBuilder b,
            Protocol.PDataQualityValidation v) {
        b.append("{\"_type\":\"dataQualityValidation\",\"context\":");
        dqContext(b, v);
        b.append(",\"dataQualityRootGraphFetchTree\":");
        dqTree(b, v.validationTree(), true);
        if (v.filter() != null) {
            b.append(",\"filter\":");
            ProtocolEmitter.valueSpec(b, v.filter());
        }
        b.append(",\"name\":");
        ProtocolEmitter.str(b, v.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, v.pkg());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, v.sourceInformation());
        b.append(",\"stereotypes\":");
        ProtocolEmitter.stereotypes(b, v.stereotypes());
        b.append(",\"taggedValues\":");
        ProtocolEmitter.taggedValues(b, v.taggedValues());
        b.append('}');
    }

    private static void dqContext(StringBuilder b,
            Protocol.PDataQualityValidation v) {
        switch (v.contextKind()) {
            case "fromMappingAndRuntime" -> {
                b.append("{\"_type\":"
                        + "\"mappingAndRuntimeDataQualityExecutionContext\","
                        + "\"_type\":"
                        + "\"mappingAndRuntimeDataQualityExecutionContext\","
                        + "\"mapping\":");
                pointer(b, v.contextPath(), v.contextPathSpan(), "MAPPING");
                b.append(",\"runtime\":");
                pointer(b, java.util.Objects.requireNonNull(v.contextSecond()),
                        java.util.Objects.requireNonNull(v.contextSecondSpan()),
                        "RUNTIME");
                b.append('}');
            }
            case "fromDataSpace" -> {
                b.append("{\"_type\":"
                        + "\"dataSpaceDataQualityExecutionContext\","
                        + "\"_type\":"
                        + "\"dataSpaceDataQualityExecutionContext\","
                        + "\"context\":");
                ProtocolEmitter.str(b,
                        java.util.Objects.requireNonNull(v.contextSecond()));
                b.append(",\"dataSpace\":");
                pointer(b, v.contextPath(), v.contextPathSpan(), "DATASPACE");
                b.append('}');
            }
            default -> throw new IllegalStateException(
                    "unmapped DataQuality context kind: " + v.contextKind());
        }
    }

    private static void pointer(StringBuilder b, String path,
            SourceInfo span, String type) {
        b.append("{\"path\":");
        ProtocolEmitter.str(b, path);
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, span);
        b.append(",\"type\":\"").append(type).append("\"}");
    }

    private static void dqTree(StringBuilder b, Protocol.PDqTreeNode n,
            boolean root) {
        String t = root ? "dataQualityRootGraphFetchTree"
                : "dataQualityPropertyGraphFetchTree";
        b.append("{\"_type\":\"").append(t).append("\",\"_type\":\"")
                .append(t).append('"');
        if (root) {
            b.append(",\"class\":");
            ProtocolEmitter.str(b,
                    java.util.Objects.requireNonNull(n.className()));
        }
        b.append(",\"constraints\":[");
        for (int i = 0; i < n.constraints().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, n.constraints().get(i));
        }
        b.append(']');
        if (!root) {
            b.append(",\"parameters\":[],\"property\":");
            ProtocolEmitter.str(b,
                    java.util.Objects.requireNonNull(n.property()));
        }
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, n.sourceInformation());
        b.append(",\"subTrees\":[");
        for (int i = 0; i < n.subTrees().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            dqTree(b, n.subTrees().get(i), false);
        }
        b.append(']');
        if (n.subType() != null) {
            b.append(",\"subType\":");
            ProtocolEmitter.str(b, n.subType());
        }
        b.append(",\"subTypeTrees\":[]}");
    }

    static void dataQualityRelationValidation(StringBuilder b,
            Protocol.PDataQualityRelationValidation v) {
        b.append("{\"_type\":\"dataqualityRelationValidation\",\"name\":");
        ProtocolEmitter.str(b, v.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, v.pkg());
        b.append(",\"query\":");
        ProtocolEmitter.valueSpec(b, v.query());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, v.sourceInformation());
        b.append(",\"stereotypes\":");
        ProtocolEmitter.stereotypes(b, v.stereotypes());
        b.append(",\"taggedValues\":");
        ProtocolEmitter.taggedValues(b, v.taggedValues());
        if (v.testSuites() != null) {
            b.append(",\"testSuites\":");
            dqTestSuites(b, v.testSuites(), "dataQualityRelationValidation");
        }
        b.append(",\"validations\":[");
        for (int i = 0; i < v.validations().size(); i++) {
            Protocol.PDqRelationCheck ch = v.validations().get(i);
            if (i > 0) {
                b.append(',');
            }
            b.append("{\"assertion\":");
            ProtocolEmitter.valueSpec(b, ch.assertion());
            if (ch.description() != null) {
                b.append(",\"description\":");
                ProtocolEmitter.str(b, ch.description());
            }
            b.append(",\"name\":");
            ProtocolEmitter.str(b, ch.name());
            if (ch.type() != null) {
                b.append(",\"type\":");
                ProtocolEmitter.str(b, ch.type());
            }
            b.append('}');
        }
        b.append("]}");
    }

    static void dataQualityRelationComparison(StringBuilder b,
            Protocol.PDataQualityRelationComparison v) {
        b.append("{\"_type\":\"dataQualityRelationComparison\","
                + "\"columnsToCompare\":[");
        for (int i = 0; i < v.columnsToCompare().size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, v.columnsToCompare().get(i));
        }
        b.append(']');
        if (v.expectedMatch() != null) {
            b.append(",\"expectedMatch\":").append(v.expectedMatch());
        }
        b.append(",\"keys\":[");
        List<String> keys = v.keys();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            ProtocolEmitter.str(b, keys.get(i));
        }
        b.append("],\"name\":");
        ProtocolEmitter.str(b, v.name());
        b.append(",\"package\":");
        ProtocolEmitter.str(b, v.pkg());
        b.append(",\"source\":");
        ProtocolEmitter.valueSpec(b, v.source());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, v.sourceInformation());
        b.append(",\"strategy\":{\"_type\":\"")
                .append(strategyWire(v.strategy().kind()))
                .append('"');
        if (v.strategy().aggregatedHash() != null) {
            b.append(",\"aggregatedHash\":")
                    .append(v.strategy().aggregatedHash());
        }
        if (v.strategy().sourceHashColumn() != null) {
            b.append(",\"sourceHashColumn\":");
            ProtocolEmitter.str(b, v.strategy().sourceHashColumn());
        }
        if (v.strategy().targetHashColumn() != null) {
            b.append(",\"targetHashColumn\":");
            ProtocolEmitter.str(b, v.strategy().targetHashColumn());
        }
        b.append("},\"target\":");
        ProtocolEmitter.valueSpec(b, v.target());
        if (v.testSuites() != null) {
            b.append(",\"testSuites\":");
            dqTestSuites(b, v.testSuites(), "dataQualityRelationComparison");
        }
        b.append('}');
    }

    /** The Testable block of a relation-level DataQuality element
     *  (4.145.0): suites and tests carry the OWNER's type name prefix;
     *  the data block is an object wrapping FunctionTestData entries. */
    private static void dqTestSuites(StringBuilder b, List<Protocol.PDqTestSuite> suites,
            String owner) {
        b.append('[');
        for (int i = 0; i < suites.size(); i++) {
            if (i > 0) {
                b.append(',');
            }
            Protocol.PDqTestSuite s = suites.get(i);
            b.append("{\"_type\":\"").append(owner).append("TestSuite\",\"id\":");
            ProtocolEmitter.str(b, s.id());
            b.append(",\"sourceInformation\":");
            ProtocolEmitter.srcInfo(b, s.sourceInformation());
            if (s.testData() != null) {
                b.append(",\"testData\":{\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, s.testData().sourceInformation());
                b.append(",\"testData\":[");
                for (int j = 0; j < s.testData().testData().size(); j++) {
                    if (j > 0) {
                        b.append(',');
                    }
                    Protocol.PDqStoreData d = s.testData().testData().get(j);
                    b.append("{\"data\":");
                    MappingEmitter.embeddedDataValue(b, d.data());
                    b.append(",\"packageableElementPointer\":{\"path\":");
                    ProtocolEmitter.str(b, d.store());
                    b.append(",\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, d.storeSpan());
                    b.append(",\"type\":\"STORE\"},\"sourceInformation\":");
                    ProtocolEmitter.srcInfo(b, d.sourceInformation());
                    b.append('}');
                }
                b.append("]}");
            }
            b.append(",\"tests\":[");
            for (int j = 0; j < s.tests().size(); j++) {
                if (j > 0) {
                    b.append(',');
                }
                Protocol.PDqTest t = s.tests().get(j);
                b.append("{\"_type\":\"").append(owner).append("Test\",\"assertions\":[");
                for (int k = 0; k < t.assertions().size(); k++) {
                    if (k > 0) {
                        b.append(',');
                    }
                    testAssertion(b, t.assertions().get(k));
                }
                b.append("],\"id\":");
                ProtocolEmitter.str(b, t.id());
                b.append(",\"sourceInformation\":");
                ProtocolEmitter.srcInfo(b, t.sourceInformation());
                b.append('}');
            }
            b.append("]}");
        }
        b.append(']');
    }

    /** One named test assertion — the three expected-value spellings the
     *  service suites carry, keyed by the assertion's own id. */
    private static void testAssertion(StringBuilder b, Protocol.PTestAssertion a) {
        switch (a.expected()) {
            case Protocol.PExternalFormatData ef -> {
                b.append("{\"_type\":\"equalToJson\",\"expected\":");
                MappingEmitter.externalFormatData(b, ef);
            }
            case Protocol.PRelationElement re -> {
                b.append("{\"_type\":\"equalToRelation\",\"expected\":");
                MappingEmitter.relationElement(b, re);
            }
            case Protocol.PEqualToValue ev -> {
                b.append("{\"_type\":\"equalTo\",\"expected\":");
                ProtocolEmitter.valueSpec(b, ev.value());
            }
        }
        b.append(",\"id\":");
        ProtocolEmitter.str(b, a.id());
        b.append(",\"sourceInformation\":");
        ProtocolEmitter.srcInfo(b, a.sourceInformation());
        b.append('}');
    }

    private static String strategyWire(String strategy) {
        if ("MD5Hash".equals(strategy)) {
            return "md5Hash";
        }
        throw new IllegalStateException(
                "unmapped comparison strategy: " + strategy);
    }
}
