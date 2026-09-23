// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import java.util.List;

/**
 * Named upstream files and roots, as paths RELATIVE to the legend-engine or
 * legend-pure root: data a generator is handed with the root it runs against,
 * and the test harness resolves against the checkout it was given. One list,
 * so the prelude generator and the corpus admit exactly the same files.
 */
public final class UpstreamFiles {

    private UpstreamFiles() {}

    /** The relational corpus root (engine-relative). */
    public static final String RELATIONAL =
            "legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
            + "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
            + "src/main/resources/core_relational/relational";

    /** The engine's core Pure module root, legend-engine-pure-code-compiled-core (engine-relative). */
    public static final String CORE_PURE =
            "legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/"
            + "src/main/resources/core";

    /** Named LIBRARY files admitted to the global module as elements
     * (never setups): each is a PROGRAM library a corpus family imports
     * (docs/WORLD_MAP.md rule 5 — "a loading rule that did not admit the
     * file"). The prelude generator scans the same list for the shapes
     * their signatures name, so the two stay in step. */
    public static final List<String> LIBRARY_FILES = List.of(
            // the relational compiler's OWN model vocabulary
            // (RelationalDebugContext / IsolationStrategy — tests/advanced
            // testForced*)
            RELATIONAL + "/" + ("pureToSQLQuery/pureToSQLQuery.pure"),
            // toPostgresModel's helper vocabulary (literal, simpleFunctionCall,
            // cast, …) — sqlDialectTranslation family, batch 54
            ("legend-engine-xts-relationalStore/"
                    + "legend-engine-xt-relationalStore-generation/"
                    + "legend-engine-xt-relationalStore-pure/"
                    + "legend-engine-xt-relationalStore-sqlDialectTranslation-pure/"
                    + "src/main/resources/core_external_store_relational_sql_dialect_translation/utils.pure"),
            // ENGINE-CORE TEST FIXTURES (batch 145, USER 2026-09-08): classes and
            // enums the relational corpus imports by name — meta::pure::tds::
            // toRelation::TestClass, the PCT model (meta::pure::functions::tests::
            // model::*), the router preeval fixtures, meta::json::tests::*. A
            // Pure import shortens names, it is not a dependency: nothing ties a
            // package to a file, so the files are NAMED here. Their own test
            // functions are library elements — never discovered as this corpus.
            CORE_PURE + "/" + ("pure/tds/relation/testTdsToRelation.pure"),
            CORE_PURE + "/" + ("pure/corefunctions/tests/testModel.pure"),
            CORE_PURE + "/" + ("pure/router/preeval/tests.pure"),
            CORE_PURE + "/" + ("external/format/json/tests/testToJson.pure"));

    /**
     * SHAPE files (PHASE3_DEMAND_CUT_HOMEWORK D1, USER 2026-09-08: "the most
     * simple thing that makes sense and still sticks to our tenets"): named
     * engine files whose CLASSES AND ENUMS enter the corpus graph and whose
     * functions do not — the corpus needs their shapes (it constructs and
     * reads them); the functions beside them are the engine's own machinery
     * (plan generation, the SQL printer, routing), which this platform
     * implements in Java or walls by name. Before batch 155 these 253 classes
     * rode in the PRELUDE because the corpus named them — T2 says a program's
     * library is graph material, by file. Two lists, two meanings:
     * LIBRARY_FILES are PROGRAMS admitted whole; SHAPE_FILES are declarations.
     */
    public static final List<String> SHAPE_FILES = List.of(
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/external/format/json/fromJSON.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/external/format/json/toJSON.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/binding/binding.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/binding/mapping.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/binding/validation.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/executionPlan/model.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/externalFormat/externalFormatContract.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/schemaSet/metamodel.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/binding/transformation/generation.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/constraints/constraintsExtension.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/corefunctions/testExtension.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/data/data.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/dataQuality/dataQuality.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/executionPlan/executionPlan.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/executionPlan/executionPlanFeature.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/executionPlan/executionPlan_generation.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/executionPlan/extensions.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/executionPlan/platformBinding/platformBinding.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/extensions/extension.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/graphFetch/graphFetchExecutionPlan.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/mapping/mappingExtension.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/mapping/modelToModel.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/mapping/relationFunctionMapping.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/model/modelUnit.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/protocol/vX_X_X/models/core/_extra.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/protocol/vX_X_X/models/core/m3.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/protocol/vX_X_X/models/pureModelContextData.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/metamodel/clustering.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/metamodel/routing.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/platform/metamodel.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/routing/router_routing.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/store/builder.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/store/cluster.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/store/metamodel.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/runtime/runtimeExtension.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/serialization/toPureGrammar.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/store/storeContract.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/tds/relation/tdsToRelation.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/tds/tds.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/tds/tdsSchema.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/test/mft.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/test/testCoverage/testCoverageModel.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/test/testable.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/treepath/treePath.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/store/aggregationAware/aggregationAware.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-json/legend-engine-pure-functions-json-pure/src/main/resources/core_functions_json/json.pure"),
            ("legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-unclassified/legend-engine-pure-functions-unclassified-pure/src/main/resources/core_functions_unclassified/io/http/executeHTTPRaw.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-pure/src/main/resources/core_relational_duckdb/relational/connection/metamodel.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-postgresSql/legend-engine-xt-relationalStore-postgresSqlModel-extensions-pure/src/main/resources/core_external_store_relational_postgres_sql_model_extensions/metamodel_extensions.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-postgresSql/legend-engine-xt-relationalStore-postgresSqlModel-pure/src/main/resources/core_external_store_relational_postgres_sql_model/metamodel.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/executionPlan/executionPlan.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/metamodel/metamodel.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/postprocessor/defaultPostProcessor/dbSpecificProcessor.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/pureToSQLQuery/metamodel.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/runtime/connection/authenticationStrategy.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/runtime/connection/postprocessor.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/sqlQueryToString/dbExtension.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation/functionRegistry/functionRegistry.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation/sqlDialect.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation/sqlDialectTranslator.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation/sqlTyping/sqlTypes.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-sqlDialectTranslation-pure/src/main/resources/core_external_store_relational_sql_dialect_translation/utils.pure"),
            ("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-sqlPlanning-pure/src/main/resources/core_external_store_relational_sql_planning/sqlPlanner.pure"),
            ("legend-engine-xts-service/legend-engine-language-pure-dsl-service-pure/src/main/resources/core_service/service/metamodel.pure"));

    /** legend-pure's platform packages (pure-relative): the spec's own
     *  declaration files, typed by the census and scanned by the prelude. */
    public static final List<String> PLATFORM_ROOTS = List.of(
            "legend-pure-core/legend-pure-m3-core/src/main/resources/platform",
            "legend-pure-core/legend-pure-m3-precisePrimitives/src/main/resources/platform_precise_primitives",
            "legend-pure-dsl/legend-pure-dsl-diagram/legend-pure-m2-dsl-diagram-pure/src/main/resources/platform_dsl_diagram",
            "legend-pure-dsl/legend-pure-dsl-graph/legend-pure-m2-dsl-graph-pure/src/main/resources/platform_dsl_graph",
            "legend-pure-dsl/legend-pure-dsl-mapping/legend-pure-m2-dsl-mapping-pure/src/main/resources/platform_dsl_mapping",
            "legend-pure-dsl/legend-pure-dsl-path/legend-pure-m2-dsl-path-pure/src/main/resources/platform_dsl_path",
            "legend-pure-dsl/legend-pure-dsl-store/legend-pure-m2-dsl-store-pure/src/main/resources/platform_dsl_store",
            "legend-pure-dsl/legend-pure-dsl-tds/legend-pure-m2-dsl-tds-pure/src/main/resources/platform_dsl_tds",
            "legend-pure-store/legend-pure-store-relational/legend-pure-m2-store-relational-pure/src/main/resources/platform_store_relational");
}
