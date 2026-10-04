package com.legend.sdlc;

import com.legend.base.Nullable;

import java.util.Map;

/**
 * An element's protocol {@code _type} → its {@code classifierPath}, the field every SDLC {@code Entity}
 * carries. The first block is legend-engine 4.145.0's own answer to
 * {@code GET /api/pure/v1/protocol/pure/getClassifierPathMap} (kept verbatim in
 * {@code sdlc-server/data/classifier-paths.json}); that route leaves out four core elements the
 * engine's serializer does map ({@code CorePureProtocolExtension.java:194-210}), the second block.
 */
final class Classifiers {
    private Classifiers() {}

    private static final Map<String, String> BY_TYPE = Map.ofEntries(
            Map.entry("association", "meta::pure::metamodel::relationship::Association"),
            Map.entry("authenticationDemo", "meta::pure::runtime::connection::authentication::demo::AuthenticationDemo"),
            Map.entry("bigQueryFunction", "meta::external::function::activator::bigQueryFunction::BigQueryFunction"),
            Map.entry("bigQueryFunctionConfig", "meta::external::function::activator::bigQueryFunction::BigQueryFunctionDeploymentConfiguration"),
            Map.entry("binding", "meta::external::format::shared::binding::Binding"),
            Map.entry("class", "meta::pure::metamodel::type::Class"),
            Map.entry("dataQualityRelationComparison", "meta::external::dataquality::DataQualityRelationComparison"),
            Map.entry("dataqualityRelationValidation", "meta::external::dataquality::DataQualityRelationValidation"),
            Map.entry("dataQualityValidation", "meta::external::dataquality::DataQuality"),
            Map.entry("dataSpace", "meta::pure::metamodel::dataSpace::DataSpace"),
            Map.entry("DeephavenApp", "meta::external::function::activator::deephavenApp::DeephavenApp"),
            Map.entry("deephavenStore", "meta::external::store::deephaven::metamodel::store::DeephavenStore"),
            Map.entry("diagram", "meta::pure::metamodel::diagram::Diagram"),
            Map.entry("elasticsearch7Store", "meta::external::store::elasticsearch::v7::metamodel::store::Elasticsearch7Store"),
            Map.entry("Enumeration", "meta::pure::metamodel::type::Enumeration"),
            Map.entry("executionEnvironmentInstance", "meta::legend::service::metamodel::ExecutionEnvironmentInstance"),
            Map.entry("externalFormatSchemaSet", "meta::external::format::shared::metamodel::SchemaSet"),
            Map.entry("fileGeneration", "meta::pure::generation::metamodel::GenerationConfiguration"),
            Map.entry("function", "meta::pure::metamodel::function::ConcreteFunctionDefinition"),
            Map.entry("functionJar", "meta::external::function::activator::functionJar::FunctionJar"),
            Map.entry("generationSpecification", "meta::pure::generation::metamodel::GenerationSpecification"),
            Map.entry("hostedService", "meta::external::function::activator::hostedService::HostedService"),
            Map.entry("measure", "meta::pure::metamodel::type::Measure"),
            Map.entry("memSqlFunction", "meta::external::function::activator::memSqlFunction::MemSqlFunction"),
            Map.entry("memSqlFunctionConfig", "meta::external::function::activator::memSqlFunction::MemSqlFunctionDeploymentConfiguration"),
            Map.entry("MongoDatabase", "meta::external::store::mongodb::metamodel::pure::MongoDatabase"),
            Map.entry("persistence", "meta::pure::persistence::metamodel::Persistence"),
            Map.entry("persistenceContext", "meta::pure::persistence::metamodel::PersistenceContext"),
            Map.entry("profile", "meta::pure::metamodel::extension::Profile"),
            Map.entry("relational", "meta::relational::metamodel::Database"),
            Map.entry("relationalMapper", "meta::relational::metamodel::RelationalMapper"),
            Map.entry("sectionIndex", "meta::pure::metamodel::section::SectionIndex"),
            Map.entry("service", "meta::legend::service::metamodel::Service"),
            Map.entry("serviceStore", "meta::external::store::service::metamodel::ServiceStore"),
            Map.entry("snowflakeApp", "meta::external::function::activator::snowflakeApp::SnowflakeApp"),
            Map.entry("snowflakeM2MUdf", "meta::external::function::activator::snowflakeM2MUdf::SnowflakeM2MUdf"),
            Map.entry("text", "meta::pure::metamodel::text::Text"),
            // not in the route, mapped by the engine's serializer
            Map.entry("mapping", "meta::pure::mapping::Mapping"),
            Map.entry("connection", "meta::pure::runtime::PackageableConnection"),
            Map.entry("runtime", "meta::pure::runtime::PackageableRuntime"),
            Map.entry("dataElement", "meta::pure::data::DataElement"));

    static @Nullable String of(String type) {
        return BY_TYPE.get(type);
    }
}
