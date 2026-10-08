// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

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
 * here as {@link #EXTENSIONS}' order.
 */
final class ElementFamilies {

    /** A family: its section's parser name, whether core prints it, its printer, its element {@code _type}s. */
    record Family(String parser, boolean core, Function<Json.Obj, String> print, List<String> types, boolean byKind) {

        Family(String parser, boolean core, Function<Json.Obj, String> print, List<String> types) {
            this(parser, core, print, types, false);
        }

        boolean printsIn(String sectionParser) {
            return core ? !claimed(sectionParser) : parser.equals(sectionParser);
        }

        /**
         * A free section's elements in the order upstream prints them: the model's own, or, for an extension
         * that selects its kinds one after the other ({@code byKind}), kind by kind in {@link #types}' order.
         */
        List<Json.Obj> freeSectionOrder(List<Json.Obj> elements) {
            if (!byKind) {
                return elements;
            }
            List<Json.Obj> out = new ArrayList<>();
            for (String t : types) {
                for (Json.Obj e : elements) {
                    if (t.equals(Composing.type(e))) {
                        out.add(e);
                    }
                }
            }
            return out;
        }
    }

    /** Core's kinds, in the order their sections print. */
    static final List<Family> CORE = List.of(
            new Family("Pure", true, DomainComposer::element, DomainComposer.TYPES),
            new Family("Mapping", true, MappingComposer::mapping, List.of("mapping")),
            new Family("Connection", true, ConnectionComposer::connection, List.of("connection")),
            new Family("Runtime", true, RuntimeComposer::runtime, List.of("runtime")));

    /** The extensions' kinds, in the reference engine's extension order. */
    static final List<Family> EXTENSIONS = List.of(
            new Family("Data", false, DataElementComposer::dataElement, List.of("dataElement")),
            new Family("ExternalFormat", false, ExternalFormatComposer::element, List.of("externalFormatSchemaSet", "binding")),
            new Family("FileGeneration", false, GenerationComposer::fileGeneration, List.of("fileGeneration")),
            new Family("GenerationSpecification", false, GenerationComposer::generationSpecification, List.of("generationSpecification")),
            new Family("Service", false, ServiceComposer::element, List.of("service", "executionEnvironmentInstance")),
            new Family("BigQuery", false, FunctionActivatorComposer::bigQueryFunction, List.of("bigQueryFunction")),
            new Family("DataSpace", false, DataSpaceComposer::dataSpace, List.of("dataSpace")),
            new Family("DataQualityValidation", false, DataQualityComposer::element,
                    List.of("dataQualityValidation", "dataqualityRelationValidation", "dataQualityRelationComparison")),
            new Family("Relational", false, DatabaseComposer::database, List.of("relational")),
            new Family("QueryPostProcessor", false, DatabaseComposer::relationalMapper, List.of("relationalMapper")),
            new Family("Deephaven", false, DeephavenComposer::element, List.of("deephavenStore", "DeephavenApp")),
            new Family("Diagram", false, DiagramComposer::diagram, List.of("diagram")),
            new Family("Elasticsearch", false, ElasticsearchComposer::store, List.of("elasticsearch7Store")),
            new Family("FunctionJar", false, FunctionActivatorComposer::functionJar, List.of("functionJar")),
            new Family("HostedService", false, FunctionActivatorComposer::hostedService, List.of("hostedService")),
            new Family("MemSql", false, FunctionActivatorComposer::memSqlFunction, List.of("memSqlFunction")),
            new Family("MongoDB", false, MongoComposer::store, List.of("MongoDatabase")),
            new Family("Persistence", false, PersistenceComposer::element, List.of("persistenceContext", "persistence"), true),
            new Family("ServiceStore", false, ServiceStoreComposer::serviceStore, List.of("serviceStore")),
            new Family("Snowflake", false, FunctionActivatorComposer::snowflake, List.of("snowflakeApp", "snowflakeM2MUdf")),
            new Family("Text", false, ExternalFormatComposer::text, List.of("text")));

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
}
