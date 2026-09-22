package com.legend.model;

import com.legend.protocol.Realization;

import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * The canonical {@code Mapping} element &mdash; a <strong>binding table</strong>
 * (docs/CLEAN_SHEET_INVERSION.md §2.1). Structure only: each binding pairs a
 * class / association with a realizing function <em>by FQN</em>; no
 * {@code ValueSpecification} and no DSL body lives here. This is the form every
 * phase after E sees and the form the clean-sheet surface parses to directly
 * (Door 1); the legacy DSL ({@link LegacyMappingDefinition}) is rewritten into
 * it by {@code MappingNormalizer}.
 *
 * <p>Bodies live in ordinary {@link FunctionDefinition}s in the model's element
 * list, lifted by Phase E and named per {@code SynthFqn} (the
 * {@code <mapping>$class$<classFqn>} / {@code <mapping>$assoc$<assocFqn>}
 * scheme). A {@link ClassBinding#functionFqn()} is exactly the lifted
 * function's FQN, so dispatch (§6) is: binding table &rarr; FQN &rarr; the one
 * {@code findFunction} index. Nothing reconstructs or parses these strings.
 *
 * @param qualifiedName        fully-qualified mapping name
 * @param includes             included mappings, with optional store substitutions
 * @param classBindings        per-class realizing-function bindings
 * @param associationBindings  per-association predicate-function bindings
 * @param enumerationMappings  per-enumeration mappings (inline static tables &mdash;
 *                             data, not expressions, so they stay structural)
 * @param testSuitesSource     raw {@code testSuites: [...]} text, or {@code null}
 */
public record MappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassBinding> classBindings,
        List<AssociationBinding> associationBindings,
        List<EnumerationMapping> enumerationMappings,
        @com.legend.base.Nullable String testSuitesSource,
        java.util.Map<String, String> resolvedStores,
        NormalizationFacts facts)
        implements PackageableElement {

    /** The common form: no store substitutions, no facts. */
    public MappingDefinition(String qualifiedName,
            List<MappingInclude> includes,
            List<ClassBinding> classBindings,
            List<AssociationBinding> associationBindings,
            List<EnumerationMapping> enumerationMappings,
            @com.legend.base.Nullable String testSuitesSource) {
        this(qualifiedName, includes, classBindings, associationBindings,
                enumerationMappings, testSuitesSource, java.util.Map.of(),
                NormalizationFacts.NONE);
    }

    /**
     * What Phase E LEARNED while compiling this mapping, stamped on the
     * artifact (T4.1 step 2 — never written into a shared index): the
     * per-class poison ledger (a class, {@code class[setId]} set, or
     * association whose synthesis hit a roadmap/user-model wall &mdash;
     * the binding is withheld and the reason raises at use), the
     * mixed-kind Operation unions (class &rarr; member set ids; the
     * resolver synthesizes their arms), the Operation unions' primary-key
     * threads (class &rarr; the engine's importDataFlow columns), and the
     * [1]-property-over-nullable-column census rows this mapping
     * contributes (bucket &rarr; witnesses).
     */
    public record NormalizationFacts(
            java.util.Map<PoisonKey, String> poisons,
            java.util.Map<String, List<String>> mixedUnions,
            java.util.Map<String, List<KeyThread>> unionKeyThreads,
            java.util.Map<String, List<String>> unionMembers,
            java.util.Map<String, java.util.Map<String, String>> routedTargetClasses) {

        public static final NormalizationFacts NONE = new NormalizationFacts(
                java.util.Map.of(), java.util.Map.of(), java.util.Map.of(),
                java.util.Map.of(), java.util.Map.of());

        public NormalizationFacts {
            // T4.1 step 4b — the SURFACE facts Phase F used to re-read off the
            // authored mapping: an Operation union's member CLASSES (member
            // order; only when every member set resolves), the routed target
            // class of a class-typed property per owner class (only when every
            // route of the property lands on one class), and the SOLE set id
            // every route of a property names across the include closure
            // (class-PM joins, otherwise fallbacks, association PMs)
            unionMembers = unionMembers == null ? java.util.Map.of() : java.util.Map.copyOf(unionMembers);
            if (routedTargetClasses == null) {
                routedTargetClasses = java.util.Map.of();
            } else {
                java.util.Map<String, java.util.Map<String, String>> copy = new java.util.LinkedHashMap<>();
                routedTargetClasses.forEach((k, v) -> copy.put(k, java.util.Map.copyOf(v)));
                routedTargetClasses = java.util.Collections.unmodifiableMap(copy);
            }
            poisons = poisons == null ? java.util.Map.of() : java.util.Map.copyOf(poisons);
            mixedUnions = mixedUnions == null ? java.util.Map.of() : java.util.Map.copyOf(mixedUnions);
            unionKeyThreads = unionKeyThreads == null
                    ? java.util.Map.of() : java.util.Map.copyOf(unionKeyThreads);
        }
    }

    public MappingDefinition {
        Objects.requireNonNull(facts, "facts");
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes == null ? List.of() : List.copyOf(includes);
        classBindings = classBindings == null ? List.of() : List.copyOf(classBindings);
        associationBindings = associationBindings == null ? List.of() : List.copyOf(associationBindings);
        enumerationMappings = enumerationMappings == null ? List.of() : List.copyOf(enumerationMappings);
        // original store FQN -> resolved store FQN: the engine's
        // Mapping.resolveStore for every store this mapping's include chain
        // substitutes, STAMPED at Phase E (composed from the includes' own
        // maps, in include order — never re-walked); the system database's
        // mapping_store_resolutions projects it
        resolvedStores = resolvedStores == null
                ? java.util.Map.of() : java.util.Map.copyOf(resolvedStores);
    }

    /**
     * A COMPILED class binding: the realizing function's FQN plus binding
     * metadata &mdash; no {@link Realization} union and no throw-guarded
     * accessor, because the pre-lift shapes live in
     * {@link CleanSheetMappingDefinition} (phase types, not in-band
     * markers). SEALED by binding kind &mdash; the kind is a property of
     * the binding relationship, not derivable from the function
     * (MAPPING_CLEAN_SHEET.md §1), and the variant IS the kind: a
     * {@link Relational} binding carries its physical-source stamp as a
     * NON-NULL component (a door that forgets to stamp does not compile),
     * a {@link Pure} (m2m) binding has no physical source by construction.
     * NO convenience constructors: one silently dropped
     * {@code primaryKeyColumns} (the AssocJoin disease) — every site
     * spells every component.
     */
    public sealed interface ClassBinding permits ClassBinding.Relational, ClassBinding.Pure,
            ClassBinding.Operation {
        String classFqn();
        @com.legend.base.Nullable String setId();
        @com.legend.base.Nullable String extendsSetId();
        boolean root();
        String functionFqn();
        List<String> primaryKeyColumns();

        /** A relational class binding; {@code source} is never null.
         * {@code primaryKeyColumns} is the extends-RESOLVED key (child's
         * else parent's — user-space identity); {@code declared} is the
         * set's OWN key text, stamped at Phase E before the extends
         * pre-pass merges the parent in (the reach-back razor): the
         * metamodel's per-set facts, which the engine's resolvePrimaryKey
         * precedence walks across the extends chain. */
        record Relational(
                String classFqn,
                @com.legend.base.Nullable String setId,
                @com.legend.base.Nullable String extendsSetId,
                boolean root,
                String functionFqn,
                List<String> primaryKeyColumns,
                DeclaredKeys declared,
                RelationalSource source,
                List<AggregateViewFacts> aggregateViews,
                java.util.Map<String, List<String>> propertyPins) implements ClassBinding {
            public Relational {
                Objects.requireNonNull(classFqn, "classFqn");
                Objects.requireNonNull(functionFqn, "functionFqn");
                Objects.requireNonNull(source, "source");
                Objects.requireNonNull(declared, "declared");
                primaryKeyColumns = primaryKeyColumns == null ? List.of()
                        : List.copyOf(primaryKeyColumns);
                aggregateViews = aggregateViews == null ? List.of()
                        : List.copyOf(aggregateViews);
                if (propertyPins == null) {
                    propertyPins = java.util.Map.of();
                } else {
                    java.util.Map<String, List<String>> copy = new java.util.LinkedHashMap<>();
                    propertyPins.forEach((k, v) -> copy.put(k, List.copyOf(v)));
                    propertyPins = java.util.Collections.unmodifiableMap(copy);
                }
            }
        }
        /* propertyPins: the set PINS the set's own property mappings
         * declare (property -> target set ids, {@code prop[setId]}; a set may
         * pin one property to several sets): a
         * FACT read at query time — a pinned navigation lives only when
         * its set is a leaf of the target class under the QUERIED mapping
         * (engine R6; the un-routed thread of a union never matches). */
        /** An AggregationAware main set's view FACTS (the engine's
         * AggregateSpecification, stamped on the compiled binding — the
         * router matches a query's project paths against them): the
         * view's set id (its own compiled binding, non-root), whether it
         * may serve any aggregation, and the specification lambdas as
         * syntax (typed once on first routing). */
        record AggregateViewFacts(String setId, boolean canAggregate,
                List<com.legend.protocol.spec.ValueSpecification> groupByFunctions,
                List<ClassMapping.AggregateValue> aggregateValues) {
            public AggregateViewFacts {
                Objects.requireNonNull(setId, "setId");
                groupByFunctions = groupByFunctions == null ? List.of() : List.copyOf(groupByFunctions);
                aggregateValues = aggregateValues == null ? List.of() : List.copyOf(aggregateValues);
            }
        }

        /** The key text a relational set DECLARED itself (m3: the set's
         * own {@code distinct} / {@code groupBy} / {@code primaryKey} /
         * property mappings): {@code distinct} = ~distinct written;
         * {@code groupByColumns} = the ~groupBy column names;
         * {@code primaryKeyColumns} = the ~primaryKey column names;
         * {@code mappedColumns} = the direct column property mappings'
         * columns in declaration order (a ~distinct set's compiled key);
         * {@code ownProperties} = the property names the set maps ITSELF,
         * in declaration order — stamped before the implicit same-extent
         * inheritance pre-pass merges an ancestor's mappings in (batch 68:
         * the engine's set projects its OWN property mappings; an inherited
         * property is served on access through the declaring ancestor's
         * set, never fetched with the instance). A function-form binding
         * declares none ({@link #NONE}). */
        record DeclaredKeys(boolean distinct, List<String> groupByColumns,
                List<String> primaryKeyColumns, List<String> mappedColumns,
                List<String> ownProperties) {
            public static final DeclaredKeys NONE =
                    new DeclaredKeys(false, List.of(), List.of(), List.of(), List.of());

            public DeclaredKeys {
                groupByColumns = groupByColumns == null ? List.of()
                        : List.copyOf(groupByColumns);
                primaryKeyColumns = primaryKeyColumns == null ? List.of()
                        : List.copyOf(primaryKeyColumns);
                mappedColumns = mappedColumns == null ? List.of()
                        : List.copyOf(mappedColumns);
                ownProperties = ownProperties == null ? List.of()
                        : List.copyOf(ownProperties);
            }
        }

        /** An OPERATION binding (legacy routes as composition, design §9):
         * the class's function is a COMPOSITION of other sets' functions —
         * a union or inheritance operation's members stacked
         * ({@code m1() -> concatenate(m2())}). The kind tag says only that;
         * the arms are what the function's body calls. No physical source
         * of its own; the arms carry theirs. {@code inheritance}: the
         * operation is the class's INHERITANCE operation (its arms are the
         * subclasses' sets) — the one fact the stack builder's same-table
         * collapse reads (the engine's single-table hierarchy: arms over
         * one bare table are the table's rows once, cast per row). */
        record Operation(
                String classFqn,
                @com.legend.base.Nullable String setId,
                @com.legend.base.Nullable String extendsSetId,
                boolean root,
                String functionFqn,
                List<String> primaryKeyColumns,
                boolean inheritance,
                List<String> memberSetIds) implements ClassBinding {
            /** {@code memberSetIds}: the ARMS of the stack in member order —
             * the set ids the function's body concatenates (a set's own id,
             * or the class-derived id of a class-level set), recorded by
             * the synthesis that emitted the body so no reader walks the
             * body. An inheritance operation with ONE mapped member is
             * that member's own synthesis (no stack): one id. */
            public Operation {
                Objects.requireNonNull(classFqn, "classFqn");
                Objects.requireNonNull(functionFqn, "functionFqn");
                primaryKeyColumns = primaryKeyColumns == null ? List.of()
                        : List.copyOf(primaryKeyColumns);
                memberSetIds = memberSetIds == null ? List.of() : List.copyOf(memberSetIds);
            }
        }

        /** A pure (m2m) class binding: no physical source exists. */
        record Pure(
                String classFqn,
                @com.legend.base.Nullable String setId,
                @com.legend.base.Nullable String extendsSetId,
                boolean root,
                String functionFqn,
                List<String> primaryKeyColumns) implements ClassBinding {
            public Pure {
                Objects.requireNonNull(classFqn, "classFqn");
                Objects.requireNonNull(functionFqn, "functionFqn");
                primaryKeyColumns = primaryKeyColumns == null ? List.of()
                        : List.copyOf(primaryKeyColumns);
            }
        }
    }

    /**
     * This mapping's class bindings PLUS its includes' (transitively,
     * OWN-FIRST; cycle-safe; bare include paths resolve in the includer's
     * package). An ENUMERATION for consumers that read binding METADATA
     * verbatim &mdash; row semantics stay in the lifted functions. It is
     * NOT the lookup rule: a first match here is own-first depth-first,
     * while {@code ClassSources.findBinding} applies the engine's R1 (the
     * root wins among a class's sets, a LATER include beats an earlier one,
     * a rootless multi-set class has no class-level binding). This javadoc
     * used to claim the two matched (audit 2026-09-15 P2-3).
     */
    public List<ClassBinding> classBindingsWithIncludes(
            java.util.function.Function<String,
                    java.util.Optional<MappingDefinition>> find) {
        List<ClassBinding> out = new java.util.ArrayList<>(classBindings);
        java.util.Set<String> seen = new java.util.HashSet<>();
        seen.add(qualifiedName);
        collectIncludedBindings(this, find, out, seen);
        return out;
    }

    private static void collectIncludedBindings(MappingDefinition md,
            java.util.function.Function<String,
                    java.util.Optional<MappingDefinition>> find,
            List<ClassBinding> out, java.util.Set<String> seen) {
        for (MappingInclude inc : md.includes()) {
            String path = inc.mappingPath();
            if (!path.contains("::") && md.qualifiedName().contains("::")) {
                String inPkg = md.qualifiedName().substring(0,
                        md.qualifiedName().lastIndexOf("::")) + "::" + path;
                if (find.apply(inPkg).isPresent()) {
                    path = inPkg;
                }
            }
            if (!seen.add(path)) {
                continue;
            }
            MappingDefinition included = find.apply(path).orElse(null);
            if (included == null) {
                continue;
            }
            out.addAll(included.classBindings());
            collectIncludedBindings(included, find, out, seen);
        }
    }

    /**
     * Construction-time facts about a RELATIONAL binding's physical source
     * &mdash; CACHED ANSWERS stamped at Phase E by the same resolution the
     * function synthesis uses, so the derivation exists in exactly one
     * place. THE RAZOR (docs/LEGACY_MAPPING_REACHBACK_CENSUS.md): the
     * lifted function is the ONLY carrier of row semantics; a stamp is
     * read VERBATIM (compared, printed, dispatched on) and never
     * interpreted &mdash; a consumer that must combine stamps to decide
     * what rows mean is doing an analysis and must walk the function
     * instead. Stamps are never authored outside the Phase-E synthesis
     * (lifted functions are immutable post-E: registration is append-only,
     * every constructor/copy-helper caller is construction-time).
     * Null on non-relational and protocol-sourced bindings.
     *
     * SEALED AND TOTAL: {@link Table} (physical main source) or
     * {@link Json} (a JsonModelConnection-backed set &mdash; the source
     * is a URL). There is NO unknown variant (user ruling 2026-08-30):
     * the pre-lift placeholder lives on the phase type
     * ({@link CleanSheetMappingDefinition}); a clean-sheet binding that
     * shares its source through chained user functions derives the
     * Table the chain bottoms out at (the stamper FOLLOWS the chain at
     * Phase E, cycle-guarded); everything else &mdash; unknown ref
     * target, a root that never reaches a store access &mdash; THROWS,
     * riding the per-element wall sink in tolerant builds.
     */
    public sealed interface RelationalSource
            permits RelationalSource.Table, RelationalSource.Json {

        /**
         * @param database              main source's database FQN
         * @param table                 resolved main table (explicit
         *                              {@code ~mainTable} or the
         *                              engine-parity inference &mdash; the
         *                              SAME call the synthesis makes)
         * @param aggregationAwareMain  dispatch flag: this set is the
         *                              AggregationAware main (same species
         *                              as {@code root})
         * @param enumColumns           per-column enum-mapping id spellings
         *                              (serialization metadata; the decode
         *                              SEMANTICS live in the function body)
         */
        record Table(
                String database,
                String table,
                boolean aggregationAwareMain,
                List<EnumColumn> enumColumns) implements RelationalSource {
            public Table {
                Objects.requireNonNull(database, "database");
                Objects.requireNonNull(table, "table");
                enumColumns = enumColumns == null ? List.of()
                        : List.copyOf(enumColumns);
            }
        }

        /** JsonModelConnection-backed set: the source is a URL. */
        record Json(String url) implements RelationalSource {
            public Json {
                Objects.requireNonNull(url, "url");
            }
        }

    }

    /** A column's declared enum-mapping id ({@code prop: EnumerationMapping
     * synonym: T.COL}) &mdash; the id is a SPELLING read verbatim for
     * plan-text parity, never a decode (that is in the function). */
    public record EnumColumn(String table, String column, String enumMappingId) {
        public EnumColumn {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(enumMappingId, "enumMappingId");
        }
    }

    /**
     * An association binding: the association realized by a predicate. The body
     * is a {@link Realization} (a predicate-function ref, or &mdash; B&rarr;E
     * only &mdash; an inline {@code (Source[1], Target[1]) -> Boolean[1]} lambda).
     *
     * @param associationFqn the mapped association
     * @param realization    how the predicate is realized
     */
    public record AssociationBinding(String associationFqn, String predicateFunctionFqn) {
        public AssociationBinding {
            Objects.requireNonNull(associationFqn, "associationFqn");
            Objects.requireNonNull(predicateFunctionFqn, "predicateFunctionFqn");
        }
    }
}
