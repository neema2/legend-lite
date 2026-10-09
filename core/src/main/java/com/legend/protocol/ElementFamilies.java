// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The element kinds {@link ModelComposer} prints, by family: the section each prints in and its printer.
 *
 * <p>Upstream's core composer prints the domain, mappings, connections and runtimes, in any section no
 * extension claims, and, for a model with no section index, under their fixed headers after every
 * extension's. Each extension claims its own section and, with no section index, prints its elements as a
 * "free section" of its own -- in the order upstream loads its composer extensions. That order is the
 * reference engine's ({@code ServiceLoader} over its class path, as the parity oracle runs it), recorded
 * here as {@link #EXTENSIONS}' order. Kinds are named by their wire {@code _type} ({@link #typeOf}).
 */
final class ElementFamilies {

    /** A family: its section's parser name, whether core prints it, its printer, its element {@code _type}s. */
    record Family(String parser, boolean core, Function<Protocol.Element, String> print, List<String> types,
            boolean byKind) {

        Family(String parser, boolean core, Function<Protocol.Element, String> print, List<String> types) {
            this(parser, core, print, types, false);
        }

        boolean printsIn(String sectionParser) {
            return core ? !claimed(sectionParser) : parser.equals(sectionParser);
        }

        /**
         * A free section's elements in the order upstream prints them: the model's own, or, for an extension
         * that selects its kinds one after the other ({@code byKind}), kind by kind in {@link #types}' order.
         */
        List<Protocol.Element> freeSectionOrder(List<Protocol.Element> elements) {
            if (!byKind) {
                return elements;
            }
            List<Protocol.Element> out = new ArrayList<>();
            for (String t : types) {
                for (Protocol.Element e : elements) {
                    if (t.equals(typeOf(e))) {
                        out.add(e);
                    }
                }
            }
            return out;
        }
    }

    /** The element as the one kind a single-kind family prints. */
    private static <T extends Protocol.Element> T as(Protocol.Element e, Class<T> kind) {
        if (!kind.isInstance(e)) {
            throw Composing.refused("a " + e.getClass().getSimpleName() + " where a " + kind.getSimpleName()
                    + " prints");
        }
        return kind.cast(e);
    }

    /** Core's kinds, in the order their sections print. */
    static final List<Family> CORE = List.of(
            new Family("Pure", true, DomainComposer::element, DomainComposer.TYPES),
            new Family("Mapping", true, e -> MappingComposer.mapping(as(e, Protocol.PMapping.class)), List.of("mapping")),
            new Family("Connection", true, e -> ConnectionComposer.connection(as(e, Protocol.PConnection.class)),
                    List.of("connection")),
            new Family("Runtime", true, e -> RuntimeComposer.runtime(as(e, Protocol.PRuntime.class)), List.of("runtime")));

    /** The extensions' kinds, in the reference engine's extension order. */
    static final List<Family> EXTENSIONS = List.of(
            new Family("Data", false, e -> DataElementComposer.dataElement(as(e, Protocol.PDataElement.class)),
                    List.of("dataElement")),
            new Family("ExternalFormat", false, ExternalFormatComposer::element, List.of("externalFormatSchemaSet", "binding")),
            new Family("FileGeneration", false, e -> GenerationComposer.fileGeneration(as(e, Protocol.PFileGeneration.class)),
                    List.of("fileGeneration")),
            new Family("GenerationSpecification", false,
                    e -> GenerationComposer.generationSpecification(as(e, Protocol.PGenerationSpecification.class)),
                    List.of("generationSpecification")),
            new Family("Service", false, ServiceComposer::element, List.of("service", "executionEnvironmentInstance")),
            new Family("BigQuery", false,
                    e -> FunctionActivatorComposer.namedFunction("BigQueryFunction", as(e, Protocol.PFunctionActivator.class)),
                    List.of("bigQueryFunction")),
            new Family("DataSpace", false, e -> DataSpaceComposer.dataSpace(as(e, Protocol.PDataSpace.class)),
                    List.of("dataSpace")),
            new Family("DataQualityValidation", false, DataQualityComposer::element,
                    List.of("dataQualityValidation", "dataqualityRelationValidation", "dataQualityRelationComparison")),
            new Family("Relational", false, e -> DatabaseComposer.database(as(e, Protocol.PDatabase.class)),
                    List.of("relational")),
            new Family("QueryPostProcessor", false,
                    e -> DatabaseComposer.relationalMapper(as(e, Protocol.PRelationalMapper.class)), List.of("relationalMapper")),
            new Family("Deephaven", false, DeephavenComposer::element, List.of("deephavenStore", "DeephavenApp")),
            new Family("Diagram", false, e -> DiagramComposer.diagram(as(e, Protocol.PDiagram.class)), List.of("diagram")),
            new Family("Elasticsearch", false, e -> ElasticsearchComposer.store(as(e, Protocol.PElasticsearch7Cluster.class)),
                    List.of("elasticsearch7Store")),
            new Family("FunctionJar", false,
                    e -> FunctionActivatorComposer.functionJar(as(e, Protocol.PFunctionActivator.class)), List.of("functionJar")),
            new Family("HostedService", false,
                    e -> FunctionActivatorComposer.hostedService(as(e, Protocol.PFunctionActivator.class)),
                    List.of("hostedService")),
            new Family("MemSql", false,
                    e -> FunctionActivatorComposer.namedFunction("MemSqlFunction", as(e, Protocol.PFunctionActivator.class)),
                    List.of("memSqlFunction")),
            new Family("MongoDB", false, e -> MongoComposer.store(as(e, Protocol.PMongoDatabase.class)), List.of("MongoDatabase")),
            new Family("Persistence", false, PersistenceComposer::element, List.of("persistenceContext", "persistence"), true),
            new Family("ServiceStore", false,
                    e -> ServiceStoreComposer.serviceStore(as(e, Protocol.PServiceStoreDefinition.class)), List.of("serviceStore")),
            new Family("Snowflake", false, e -> FunctionActivatorComposer.snowflake(as(e, Protocol.PFunctionActivator.class)),
                    List.of("snowflakeApp", "snowflakeM2MUdf")),
            new Family("Text", false, e -> ExternalFormatComposer.text(as(e, Protocol.PText.class)), List.of("text")));

    /**
     * The kinds an extension prints in its own section but leaves out of its free section: with no section
     * index upstream drops them, so lite refuses the model rather than print it without them.
     */
    static final java.util.Set<String> NOT_IN_FREE_SECTIONS = java.util.Set.of("DeephavenApp");

    /** Each element {@code _type}'s family. */
    static final Map<String, Family> BY_TYPE;

    static {
        Map<String, Family> m = new LinkedHashMap<>();
        List<Family> all = new ArrayList<>(CORE);
        all.addAll(EXTENSIONS);
        for (Family f : all) {
            for (String t : f.types()) {
                m.put(t, f);
            }
        }
        BY_TYPE = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(m));
    }

    /** The activators' wire {@code _type}s, by the grammar keyword their record keeps. */
    private static final Map<String, String> ACTIVATOR_TYPES;

    static {
        Map<String, String> m = new LinkedHashMap<>();
        ActivatorReader.KINDS.forEach((wireType, kind) -> m.put(kind, wireType));
        ACTIVATOR_TYPES = java.util.Collections.unmodifiableMap(m);
    }

    private ElementFamilies() {
    }

    /** A section name an extension's section composer claims. */
    static boolean claimed(String sectionParser) {
        for (Family f : EXTENSIONS) {
            if (f.parser().equals(sectionParser)) {
                return true;
            }
        }
        return false;
    }

    /** The element's wire {@code _type}: the name the families, sections and orders know it by. */
    static String typeOf(Protocol.Element e) {
        return switch (e) {
            case Protocol.PClass c -> "class";
            case Protocol.PAssociation a -> "association";
            case Protocol.PEnumeration en -> "Enumeration";
            case Protocol.PFunction f -> "function";
            case Protocol.PProfile p -> "profile";
            case Protocol.PSectionIndex s -> "sectionIndex";
            case Protocol.PMeasure m -> "measure";
            case Protocol.PRuntime r -> "runtime";
            case Protocol.PConnection c -> "connection";
            case Protocol.PDatabase d -> "relational";
            case Protocol.PService s -> "service";
            case Protocol.PExecutionEnvironment ee -> "executionEnvironmentInstance";
            case Protocol.PDataSpace d -> "dataSpace";
            case Protocol.PPersistence p -> "persistence";
            case Protocol.PPersistenceContext c -> "persistenceContext";
            case Protocol.PFunctionActivator a -> {
                String type = ACTIVATOR_TYPES.get(a.kind());
                if (type == null) {
                    throw Composing.refused("a function activator of kind '" + a.kind() + "'");
                }
                yield type;
            }
            case Protocol.PDiagram d -> "diagram";
            case Protocol.PText t -> "text";
            case Protocol.PGenerationSpecification g -> "generationSpecification";
            case Protocol.PFileGeneration g -> "fileGeneration";
            case Protocol.PDeephavenDatabase d -> "deephavenStore";
            case Protocol.PElasticsearch7Cluster c -> "elasticsearch7Store";
            case Protocol.PMongoDatabase m -> "MongoDatabase";
            case Protocol.PDataQualityValidation v -> "dataQualityValidation";
            case Protocol.PDataQualityRelationValidation v -> "dataqualityRelationValidation";
            case Protocol.PDataQualityRelationComparison c -> "dataQualityRelationComparison";
            case Protocol.PSchemaSet s -> "externalFormatSchemaSet";
            case Protocol.PBinding b -> "binding";
            case Protocol.PServiceStoreDefinition s -> "serviceStore";
            case Protocol.PMapping m -> "mapping";
            case Protocol.PDataElement d -> "dataElement";
            case Protocol.PRelationalMapper m -> "relationalMapper";
        };
    }
}
