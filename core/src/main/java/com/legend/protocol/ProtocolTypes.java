// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.base.Nullable;
import com.legend.json.Json;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The protocol's element types as legend-engine names them: each element {@code _type}'s classifier path, and which
 * types are stores and which are function activators. ONE table, the only copy in the repository, read three ways:
 *
 * <ul>
 *   <li>{@link #classifierPathMapJson}: the engine's {@code GET /api/pure/v1/protocol/pure/getClassifierPathMap};</li>
 *   <li>{@link #subtypeInfoJson}: its {@code GET /api/pure/v1/protocol/pure/getSubtypeInfo};</li>
 *   <li>{@link #classifierPath}: the {@code classifierPath} an SDLC entity carries (the SDLC's rules ask it through
 *       their grammar seam, {@code //sdlc-server/grammar}: they see no Pure code).</li>
 * </ul>
 *
 * <p><b>Where it is read from.</b> The engine computes both routes from the protocol extensions on its class path
 * ({@code PureProtocol.java:45-117} of legend-engine 4.145.0, the pinned release): for each registered subtype
 * ({@code PureProtocolExtension.getExtraProtocolSubTypeInfoCollectors}) that is a {@code PackageableElement} with a
 * classifier path, a row of the map; for each that is a {@code Store}, a store subtype; for each that is a
 * {@code FunctionActivator}, a function activator subtype. {@link #ROUTE}'s types and paths are a 4.145.0 server's own
 * answer to {@code getClassifierPathMap} (measured; kept as {@code sdlc-server/data/classifier-paths.json} until
 * 2026-11-13, when this table became the one copy). Each row's {@link Kind} is read from that release's sources: the
 * protocol class the extension registers under the type, and whether it extends {@code Store} or
 * {@code FunctionActivator} ({@code Database}, {@code ServiceStore}, {@code MongoDatabase} and the two classes
 * generated from Pure, {@code DeephavenStore} and {@code Elasticsearch7Store}; the seven activator classes). Two
 * things a reader might expect and the engine does not answer: {@code binding} is no store subtype ({@code Binding}
 * extends {@code PackageableElement} there), and {@code ModelStore} is a {@code Store} no extension registers.
 * legend-studio's own fallback list, for an engine that does not answer ({@code V1_RemoteEngine.getSubtypeInfo}), is
 * a different, shorter list: it is a client's default and not the engine's answer.
 *
 * <p><b>What differs from the engine</b> ({@code docs/SEMANTICS_REGISTER.md} S401): the vocabulary is the whole of a
 * 4.145.0 server's, including kinds legend-lite does not read (a client asks what a {@code _type} IS, not what this
 * server compiles); and each array's order is this table's (by type, ignoring case), where the engine's is its hash
 * collections' iteration order, which for the map and the activators depends on the order its extensions load.
 */
public final class ProtocolTypes {

    private ProtocolTypes() {
    }

    /** What the engine's protocol class for an element type is, as {@code getSubtypeInfo} asks it. */
    private enum Kind { STORE, FUNCTION_ACTIVATOR, OTHER }

    private record Row(String type, String classifierPath, Kind kind) {
    }

    private static Row row(String type, String classifierPath) {
        return new Row(type, classifierPath, Kind.OTHER);
    }

    private static Row store(String type, String classifierPath) {
        return new Row(type, classifierPath, Kind.STORE);
    }

    private static Row activator(String type, String classifierPath) {
        return new Row(type, classifierPath, Kind.FUNCTION_ACTIVATOR);
    }

    /** The engine's {@code getClassifierPathMap}, legend-engine 4.145.0: 37 rows, by type ignoring case. */
    private static final List<Row> ROUTE = List.of(
            row("association", "meta::pure::metamodel::relationship::Association"),
            row("authenticationDemo", "meta::pure::runtime::connection::authentication::demo::AuthenticationDemo"),
            activator("bigQueryFunction", "meta::external::function::activator::bigQueryFunction::BigQueryFunction"),
            row("bigQueryFunctionConfig",
                    "meta::external::function::activator::bigQueryFunction::BigQueryFunctionDeploymentConfiguration"),
            row("binding", "meta::external::format::shared::binding::Binding"),
            row("class", "meta::pure::metamodel::type::Class"),
            row("dataQualityRelationComparison", "meta::external::dataquality::DataQualityRelationComparison"),
            row("dataqualityRelationValidation", "meta::external::dataquality::DataQualityRelationValidation"),
            row("dataQualityValidation", "meta::external::dataquality::DataQuality"),
            row("dataSpace", "meta::pure::metamodel::dataSpace::DataSpace"),
            activator("DeephavenApp", "meta::external::function::activator::deephavenApp::DeephavenApp"),
            store("deephavenStore", "meta::external::store::deephaven::metamodel::store::DeephavenStore"),
            row("diagram", "meta::pure::metamodel::diagram::Diagram"),
            store("elasticsearch7Store",
                    "meta::external::store::elasticsearch::v7::metamodel::store::Elasticsearch7Store"),
            row("Enumeration", "meta::pure::metamodel::type::Enumeration"),
            row("executionEnvironmentInstance", "meta::legend::service::metamodel::ExecutionEnvironmentInstance"),
            row("externalFormatSchemaSet", "meta::external::format::shared::metamodel::SchemaSet"),
            row("fileGeneration", "meta::pure::generation::metamodel::GenerationConfiguration"),
            row("function", "meta::pure::metamodel::function::ConcreteFunctionDefinition"),
            activator("functionJar", "meta::external::function::activator::functionJar::FunctionJar"),
            row("generationSpecification", "meta::pure::generation::metamodel::GenerationSpecification"),
            activator("hostedService", "meta::external::function::activator::hostedService::HostedService"),
            row("measure", "meta::pure::metamodel::type::Measure"),
            activator("memSqlFunction", "meta::external::function::activator::memSqlFunction::MemSqlFunction"),
            row("memSqlFunctionConfig",
                    "meta::external::function::activator::memSqlFunction::MemSqlFunctionDeploymentConfiguration"),
            store("MongoDatabase", "meta::external::store::mongodb::metamodel::pure::MongoDatabase"),
            row("persistence", "meta::pure::persistence::metamodel::Persistence"),
            row("persistenceContext", "meta::pure::persistence::metamodel::PersistenceContext"),
            row("profile", "meta::pure::metamodel::extension::Profile"),
            store("relational", "meta::relational::metamodel::Database"),
            row("relationalMapper", "meta::relational::metamodel::RelationalMapper"),
            row("sectionIndex", "meta::pure::metamodel::section::SectionIndex"),
            row("service", "meta::legend::service::metamodel::Service"),
            store("serviceStore", "meta::external::store::service::metamodel::ServiceStore"),
            activator("snowflakeApp", "meta::external::function::activator::snowflakeApp::SnowflakeApp"),
            activator("snowflakeM2MUdf", "meta::external::function::activator::snowflakeM2MUdf::SnowflakeM2MUdf"),
            row("text", "meta::pure::metamodel::text::Text"));

    /**
     * Four core elements the route leaves out and the engine's serializer does map
     * ({@code CorePureProtocolExtension.java:194-210}): an entity of one of them carries its path all the same.
     */
    private static final List<Row> SERIALIZER_ONLY = List.of(
            row("mapping", "meta::pure::mapping::Mapping"),
            row("connection", "meta::pure::runtime::PackageableConnection"),
            row("runtime", "meta::pure::runtime::PackageableRuntime"),
            row("dataElement", "meta::pure::data::DataElement"));

    /** Each type's path, the route's and the serializer's: a lookup alone (the arrays' order is {@link #ROUTE}'s). */
    private static final Map<String, String> PATH_BY_TYPE = java.util.Collections.unmodifiableMap(pathByType());

    private static Map<String, String> pathByType() {
        Map<String, String> out = new LinkedHashMap<>();
        for (List<Row> rows : List.of(ROUTE, SERIALIZER_ONLY)) {
            for (Row r : rows) {
                if (out.put(r.type(), r.classifierPath()) != null) {
                    throw new IllegalStateException("the protocol's element type '" + r.type() + "' is listed twice");
                }
            }
        }
        return out;
    }

    /** The classifier path of an element of protocol type {@code type}, or null where the engine maps none. */
    public static @Nullable String classifierPath(String type) {
        return PATH_BY_TYPE.get(type);
    }

    /**
     * The engine's {@code getClassifierPathMap} answer, in the form it writes
     * ({@code [{"type":"class","classifierPath":"meta::pure::metamodel::type::Class"},...]}).
     */
    public static String classifierPathMapJson() {
        StringBuilder out = new StringBuilder(4096).append('[');
        for (Row r : ROUTE) {
            if (out.length() > 1) {
                out.append(',');
            }
            out.append("{\"type\":\"").append(Json.escape(r.type())).append("\",\"classifierPath\":\"")
                    .append(Json.escape(r.classifierPath())).append("\"}");
        }
        return out.append(']').toString();
    }

    /**
     * The engine's {@code getSubtypeInfo} answer, its two fields in the order its {@code SubtypeInfoResult} declares
     * them: {@code {"storeSubtypes":[...],"functionActivatorSubtypes":[...]}}.
     */
    public static String subtypeInfoJson() {
        return "{\"storeSubtypes\":" + types(Kind.STORE) + ",\"functionActivatorSubtypes\":"
                + types(Kind.FUNCTION_ACTIVATOR) + "}";
    }

    private static String types(Kind kind) {
        StringBuilder out = new StringBuilder(256).append('[');
        for (Row r : ROUTE) {
            if (r.kind() == kind) {
                if (out.length() > 1) {
                    out.append(',');
                }
                out.append('"').append(Json.escape(r.type())).append('"');
            }
        }
        return out.append(']').toString();
    }
}
