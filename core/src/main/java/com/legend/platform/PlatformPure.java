// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.SystemMetamodel;
import com.legend.model.FunctionDefinition;
import com.legend.model.FunctionId;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * THE PLATFORM'S OWN PURE (build rebuild Phase 3b, item 1b; the rebuild program's decision 1): the system
 * metamodel's function bodies implement their ids over the platform's own rows. The row kind is
 * {@link Implementation.PlatformPure}; this class is its registration (the ids), the merge that gives an upstream
 * declaration the platform's body, and the decisions on upstream's other versions at those names. A version at one
 * of those names with no decision is refused ({@link Implementation.Reason#NO_ROW}), as a version at a catalog
 * native's name is: upstream's body is the spec, never the platform's implementation by accident.
 *
 * <p>Before this (Phase 3, {@code SystemMetamodel.shadows}; the ledger's PARK-12, closed 2026-10-09) a loaded twin was hidden
 * when its id and its parameter spellings matched a system version; 29 twins spelled a type differently
 * ({@code String} against {@code meta::pure::metamodel::type::String}, {@code EnumerationMapping} against
 * {@code EnumerationMapping<T>}) and five upstream files dropped as "defined more than once".
 */
public final class PlatformPure {

    private PlatformPure() {
    }

    /** The ids the platform implements in its own Pure: every function the system metamodel declares. */
    public static Set<FunctionId> ids() {
        return SystemMetamodel.functionIds();
    }

    /**
     * Upstream's declaration with the platform's body: {@code declaration} (loaded from a module, or generated into
     * the default world) keeps its name, type parameters, parameters, return type and annotations; its body is
     * {@code platform}'s. Both have the same id by construction. The body binds the platform's parameter NAMES, so
     * the two declarations must name their parameters alike; a difference is an error naming both, never a silent
     * rebinding (every twin measured on 2026-10-09 agrees: {@code Phase3bProbesTest} H1).
     */
    public static FunctionDefinition adopt(FunctionDefinition declaration, FunctionDefinition platform) {
        FunctionId id = FunctionId.of(declaration);
        if (!id.equals(FunctionId.of(platform))) {
            throw new IllegalArgumentException("not the same function: " + id + " and " + FunctionId.of(platform));
        }
        for (int i = 0; i < declaration.parameters().size(); i++) {
            String theirs = declaration.parameters().get(i).name();
            String ours = platform.parameters().get(i).name();
            if (!theirs.equals(ours)) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "'" + id.qualified() + "' is the platform's own Pure, whose body binds its parameter "
                                + (i + 1) + " as '" + ours + "'; this declaration names it '" + theirs
                                + "' — the two must agree", id.qualified());
            }
        }
        return new FunctionDefinition(declaration.qualifiedName(), declaration.typeParameters(),
                declaration.multiplicityParameters(), declaration.parameters(), declaration.returnType(),
                declaration.returnMultiplicity(), platform.body(), declaration.stereotypes(),
                declaration.taggedValues(), declaration.synthesizedFrom());
    }

    /** Upstream's other versions at the platform's own names whose upstream body runs here, by id, each with why
     *  (decided 2026-10-09 from upstream's bodies: PHASE_3B_HOMEWORK_2026_10_09.md H2). */
    public static Map<FunctionId, String> upstreamBodies() {
        return UPSTREAM_BODIES;
    }

    /** Upstream's other versions at the platform's own names refused by decision, by id, each with its wall. */
    public static Map<FunctionId, WalledBodies.Wall> refusedVersions() {
        return REFUSED_VERSIONS;
    }

    private static final Map<FunctionId, String> UPSTREAM_BODIES = upstreamBodiesTable();
    private static final Map<FunctionId, WalledBodies.Wall> REFUSED_VERSIONS = refusedVersionsTable();

    private static Map<FunctionId, String> upstreamBodiesTable() {
        Map<FunctionId, String> t = new LinkedHashMap<>();
        // platform_dsl_mapping/functions_PropertyMappingsImplementation.pure: the per-kind versions dispatch to, or
        // read through, the platform's propertyMappingsByPropertyName and allPropertyMappings
        t.put(new FunctionId("meta::pure::mapping::propertyMappingsByPropertyName_OtherwiseEmbeddedSetImplementation_1__String_1__PropertyMapping_MANY_"),
                "an otherwise-embedded set's property mappings: the platform's allPropertyMappings plus the set's"
                        + " otherwisePropertyMapping, filtered by name");
        t.put(new FunctionId("meta::pure::mapping::propertyMappingsByPropertyName_AggregationAwareSetImplementation_1__String_1__PropertyMapping_MANY_"),
                "delegates to the aggregation-aware set's main set: the platform's version");
        t.put(new FunctionId("meta::pure::mapping::propertyMappingsByPropertyName_EmbeddedSetImplementation_1__String_1__PropertyMapping_MANY_"),
                "dispatches by the embedded set's kind to the platform's versions");
        // core_relational/relational/helperFunctions/helperFunctions.pure
        t.put(new FunctionId("meta::relational::runtime::extractDBs_Mapping_MANY__Runtime_1__Database_MANY_"),
                "the databases of some mappings and a runtime: the platform's extractDBs(Mapping) per mapping, plus"
                        + " the runtime's database connection stores");
        t.put(new FunctionId("meta::relational::runtime::extractDBs_Mapping_1__Mapping_1__Database_MANY_"),
                "upstream's walk of a mapping's includes (its private helper); only upstream's own extractDBs(Mapping)"
                        + " called it, and the platform's version replaces that body; its body runs for a program that"
                        + " names it");
        t.put(new FunctionId("meta::relational::mapping::resolvePrimaryKey_RelationalInstanceSetImplementation_1__RelationalOperationElement_MANY_"),
                "dispatches an embedded set to its owner and a root set to the platform's version");
        t.put(new FunctionId("meta::relational::mapping::resolvePrimaryKey_RelationFunctionInstanceSetImplementation_1__TableAliasColumn_MANY_"),
                "a relation-function set's keys, upstream's own (getRelationFunctionPkAsTableAliasColumns)");
        t.put(new FunctionId("meta::relational::mapping::resolvePrimaryKey_InstanceSetImplementation_1__RelationalOperationElement_MANY_"),
                "dispatches by the set's kind to the relational and relation-function versions");
        return java.util.Collections.unmodifiableMap(t);
    }

    private static Map<FunctionId, WalledBodies.Wall> refusedVersionsTable() {
        Map<FunctionId, WalledBodies.Wall> t = new LinkedHashMap<>();
        // core_relational/relational/relationalExtension.pure: the two versions that translate the inferred type to
        // one database's spelling. inferRelationalType(rop, failOnMatchFailure) is NOT refused: core_relational's
        // own mapping execution calls it (relationalMappingExecution.pure, getRelationalTypeFromRelationalPropertyMapping:
        // `->inferRelationalType(false)`), so it is a platform version over the rows (SystemMetamodel). Callers of the
        // two refused versions in the closure are engine machinery the platform serves itself: relationalGraphFetch.pure
        // (the engine's graph-fetch planner), testDataGeneration.pure and testRunner.pure (the engine's test-data
        // generator; the platform's is Java, //core testdatagen).
        WalledBodies.Wall translation = new WalledBodies.Wall(WalledBodies.Kind.ENGINE_MACHINERY,
                "the engine's translation of a relational type to one database's spelling"
                        + " (translateCoreTypeToDbSpecificType); the platform's dialects spell types. Its callers in"
                        + " core_relational are the engine's graph-fetch planner (relationalGraphFetch.pure) and its"
                        + " test-data generator (testDataGeneration.pure, testRunner.pure), machinery the platform"
                        + " serves itself");
        t.put(new FunctionId("meta::relational::functions::typeInference::inferRelationalType_RelationalOperationElement_1__TranslationContext_1__DataType_$0_1$_"),
                translation);
        t.put(new FunctionId("meta::relational::functions::typeInference::inferRelationalType_RelationalOperationElement_1__Boolean_1__TranslationContext_1__DataType_$0_1$_"),
                translation);
        return java.util.Collections.unmodifiableMap(t);
    }
}
