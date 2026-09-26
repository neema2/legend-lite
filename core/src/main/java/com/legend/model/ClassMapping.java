package com.legend.model;

import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * One per-class mapping inside a {@link LegacyMappingDefinition} &mdash;
 * &quot;how does this Pure class get values from a store?&quot;
 *
 * <p>Sealed because different store kinds require structurally different
 * fields (relational mappings need a main table; M2M mappings need a source
 * class; aggregation-aware mappings carry a list of aggregate specs; etc.).
 * Consumers (NameResolver, Phase F element compilers) dispatch via pattern
 * match.
 *
 * <h2>Variants</h2>
 * <ul>
 *   <li>{@link Relational} (B.4b) &mdash; relational class mappings
 *       anchored to a main table. The syntactic {@code *} prefix is
 *       captured in the {@link Relational#root() root} flag rather than
 *       being lifted to a separate variant &mdash; both root and non-root
 *       relational mappings share the same field layout in lite/engine
 *       (no distinct {@code mainTable}-less form is exposed).</li>
 *   <li>{@link Pure} (B.4e) &mdash; model-to-model (M2M) class mappings
 *       sourced from another Pure class. Property bodies and the
 *       optional {@code ~filter} are Pure value expressions parsed
 *       eagerly by {@code ElementParser} into
 *       {@link ValueSpecification} ASTs.</li>
 * </ul>
 *
 * <p>Mirrors FINOS {@code legend-engine}'s {@code ClassMapping} hierarchy
 * (abstract base + {@code RelationalClassMapping} +
 * {@code RootRelationalClassMapping} + {@code PureInstanceClassMapping} + ...)
 * but collapses the relational sub-hierarchy onto one variant + a {@code root}
 * boolean to match lite/engine's surface, which never instantiates non-root
 * relational class mappings as standalone records (they appear only as
 * {@link PropertyMapping.Embedded} sub-mappings).
 */
public sealed interface ClassMapping permits ClassMapping.Relational,
        ClassMapping.Pure, ClassMapping.Union, ClassMapping.RelationFunction,
        ClassMapping.Inheritance {

    /**
     * NAMING CONTRACT between union synthesis (normalizer) and the
     * resolver's subType registration — both ends call these, neither
     * re-derives by pattern: the union thread column carrying subtype
     * {@code classFqn}'s mapped {@code prop} (its own member thread reads
     * the mapped value, every other thread a typed NULL).
     */
    static String subTypeColumn(String classFqn, String prop) {
        return subTypeColumnPrefix(classFqn) + prop;
    }

    /** Prefix of every subtype-dispatch column of {@code classFqn}. */
    static String subTypeColumnPrefix(String classFqn) {
        return "stc_" + classFqn.replace("::", "__") + "___";
    }

    /** Whether {@code col} is a subtype-dispatch column (of any class). */
    static boolean isSubTypeColumn(String col) {
        return col.startsWith("stc_") && col.contains("___");
    }

    /** The class whose {@link #subTypeColumnPrefix} minted {@code pfx},
     * chosen from {@code candidates} by EXACT re-mangling. The
     * {@code ::}&rarr;{@code __} encoding is LOSSY for names containing
     * {@code __} (my::A__B and my::A::B mint the same prefix), so the
     * inverse is a lookup against the model, never string surgery
     * (text-surgery audit §1.1 #1). Null when no candidate matches. */
    static @com.legend.base.Nullable String classOfWitnessPrefix(
            String pfx, java.util.Collection<String> candidates) {
        for (String fqn : candidates) {
            if (subTypeColumnPrefix(fqn).equals(pfx)) {
                return fqn;
            }
        }
        return null;
    }

    /**
     * Pseudo-prop of a PRIMARY-KEY column read on a class row: the
     * resolver's element-reference rule (D3) filters a metaclass extent
     * on it; never a real property name (the {@code $} prefix is not a
     * Pure identifier). Registered as a binding beside the subtype
     * pseudo-bindings (ClassSources) — the same contract shape.
     */
    static String primaryKeyBinding(String column) {
        return "$pk:" + column;
    }

    /** Whether {@code key} is a primary-key pseudo-binding. */
    static boolean isPrimaryKeyBinding(String key) {
        return key.startsWith("$pk:");
    }

    /**
     * Pseudo-prop of the FOREIGN-KEY IDENTITY of a to-one class-typed
     * navigation {@code prop}: when the slot's join is one equality between a
     * row column and the target set's declared key column, the target's
     * identity (D2) IS that row column — element identity equality
     * ({@code $x.cls == SomeElement}) reads it without the join, in every
     * scope a plain row read resolves. Registered beside the primary-key
     * pseudo-binding (ClassSources); never a real property name.
     */
    static String foreignKeyBinding(String prop) {
        return "$fk:" + prop;
    }

    /**
     * Pseudo-prop of the MEMBERSHIP WITNESS column (emitted only for cast
     * targets with PARTIAL membership; never a real property name).
     */
    static String memberWitness() {
        return "$member";
    }

    /** Whether {@code key} is a membership-WITNESS column, and if so the
     * cast class's column PREFIX it belongs to (audit 23 B4 — key matching
     * through the contract, never substring surgery). Null when not a
     * witness key. */
    static @com.legend.base.Nullable String witnessPrefixOf(String key) {
        String tail = "___" + memberWitness();
        return isSubTypeColumn(key) && key.endsWith(tail)
                ? key.substring(0, key.length() - memberWitness().length())
                : null;
    }

    /** Fully-qualified class name being mapped. */
    String className();

    /**
     * Optional explicit set identifier from {@code ClassName[setId]}.
     * {@code null} when the user wrote {@code ClassName} without brackets;
     * resolution assigns a default in Phase D.
     */
    String setId();

    /**
     * Optional {@code extends [parentSetId]} clause. {@code null} when no
     * extends was written; non-null when this mapping inherits from another.
     */
    String extendsSetId();

    /** {@code true} when the {@code *} prefix marked this as a root mapping. */
    boolean root();

    /**
     * A relational class mapping. Holds everything an engine
     * {@code RootRelationalClassMapping} carries:
     * <pre>
     *   *Person[setId] extends [parentId]: Relational
     *   {
     *     ~mainTable [db::DB] PERSON
     *     ~filter [db::DB] ActivePersonFilter
     *     ~distinct
     *     ~groupBy(PERSON.DEPT)
     *     ~primaryKey(PERSON.ID)
     *     // property mappings ...
     *   }
     * </pre>
     *
     * @param className         fully-qualified class name
     * @param setId             optional set identifier; {@code null} for default
     * @param extendsSetId      optional extends clause; {@code null} for none
     * @param root              {@code *} prefix present
     * @param mainTable         required: {@code ~mainTable [DB] T}
     * @param filter            optional {@code ~filter ...} clause (sealed
     *                          {@link FilterMapping}); {@code null} when absent
     * @param distinct          whether {@code ~distinct} was written
     * @param groupBy           optional {@code ~groupBy(...)} expressions
     * @param primaryKey        optional {@code ~primaryKey(...)} expressions
     * @param propertyMappings  per-property bindings in declaration order
     * @param sourceUrl         optional URL for external data sources (e.g.
     *                          {@code data:application/json,...}, {@code file:},
     *                          {@code http:}). {@code null} for ordinary
     *                          table-backed mappings. Populated by
     *                          {@code ModelBuilder.from()} when a
     *                          {@code RuntimeDefinition}'s
     *                          {@code JsonModelConnection} binds this class
     *                          (engine parity: {@code RelationalMapping.variantIdentity}
     *                          in legend-engine's PureModelBuilder).
     */
    /** The AggregationAware node's aggregate views (engine
     * aggregateSetImplementations, in declaration order). */
    record AggregationAware(List<AggregateView> views) {
        public AggregationAware {
            views = views == null ? List.of() : List.copyOf(views);
        }
    }

    /** One aggregate view: its specification (engine AggregateSpecification —
     * canAggregate, the group-by functions and the map/aggregate value
     * pairs, {@code $this}-lambdas over the class kept as SYNTAX facts and
     * typed once on first routing) and its set (a non-root Relational
     * mapping of the same class, id {@code <outer>_Aggregate_<index>}). */
    record AggregateView(int index, boolean canAggregate,
            List<com.legend.protocol.spec.ValueSpecification> groupByFunctions,
            List<AggregateValue> aggregateValues, Relational set) {
        public AggregateView {
            groupByFunctions = groupByFunctions == null ? List.of() : List.copyOf(groupByFunctions);
            aggregateValues = aggregateValues == null ? List.of() : List.copyOf(aggregateValues);
            Objects.requireNonNull(set, "set");
        }
    }

    /** {@code (~mapFn: ..., ~aggregateFn: ...)}. */
    record AggregateValue(com.legend.protocol.spec.ValueSpecification mapFn,
            com.legend.protocol.spec.ValueSpecification aggregateFn) {
    }

    record Relational(
            String className,
            @com.legend.base.Nullable String setId,
            @com.legend.base.Nullable String extendsSetId,
            boolean root,
            @com.legend.base.Nullable LegacyMappingDefinition.TableReference mainTable,
            @com.legend.base.Nullable FilterMapping filter,
            boolean distinct,
            List<RelationalOperation> groupBy,
            List<RelationalOperation> primaryKey,
            List<PropertyMapping> propertyMappings,
            @com.legend.base.Nullable String sourceUrl,
            java.util.Map<String, String> propertyTargetSets,
            @com.legend.base.Nullable AggregationAware aggregation) implements ClassMapping {

        // NO short overload: a defaulted propertyTargetSets silently dropped
        // prop[setId] routing at rebuild sites (remediation T2.2); every
        // construction names every field.
        // aggregation: non-null = this set IS an AggregationAware mapping's
        // ~mainMapping (the engine's mainSetImplementation) and the NODE
        // carries the aggregate views with their specifications — the
        // engine's AggregationAwareSetImplementation is ONE node (main +
        // aggregateSetImplementations); every rebuild re-spells it, so no
        // sidecar can drop it. The router picks a view per query
        // (AggregationAwareRouting); the views compile as sets like any.

        /** Whether this set is an AggregationAware mapping's main set. */
        public boolean aggregationAwareMain() {
            return aggregation != null;
        }

        public Relational {
            Objects.requireNonNull(className, "Class name cannot be null");
            // mainTable is nullable: per legend-engine grammar
            // (RelationalParserGrammar.g4:248), mappingMainTable is 0..1.
            // A Relational class mapping MAY omit ~mainTable when it
            // inherits from another set-id via extendsSetId, or when
            // its property mappings all resolve to a single table that
            // the compiler can infer (see RelationalCompilerExtension
            // "Can't find the main table" check in legend-engine).
            // Parser accepts; compiler enforces presence-or-inferability.
            // setId, extendsSetId, filter intentionally nullable: each is a
            // presence/absence with no structural variant downstream
            // (setId resolves to a default id; extendsSetId means inherit;
            // filter means subject the rowset to a Filter; sourceUrl means
            // bind to an external data source via JsonModelConnection).
            groupBy = groupBy == null ? List.of() : List.copyOf(groupBy);
            primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
            propertyMappings = propertyMappings == null ? List.of() : List.copyOf(propertyMappings);
            propertyTargetSets = propertyTargetSets == null
                    ? java.util.Map.of() : java.util.Map.copyOf(propertyTargetSets);
        }
    }

    /**
     * A model-to-model (M2M) class mapping. Sources values from another Pure
     * class via {@code $src.*} expressions rather than from a database.
     *
     * <p>Surface:
     * <pre>
     *   *target::Class[setId]: Pure
     *   {
     *     ~src source::Class
     *     ~filter $src.isActive == true
     *     propA: $src.fieldA,
     *     propB: $src.fieldB-&gt;toUpper()
     *   }
     * </pre>
     *
     * <h2>Eager body parsing</h2>
     * Property bindings and the optional {@code ~filter} are Pure value
     * expressions parsed eagerly at parse time by {@code ElementParser}
     * (via {@code SpecParser}) and held as {@link ValueSpecification} ASTs.
     * No separate lazy-parse phase.
     *
     * @param className         fully-qualified target class
     * @param setId             optional set identifier; {@code null} for default
     * @param extendsSetId      optional extends; {@code null} for none
     * @param root              {@code *} prefix present
     * @param sourceClass       fully-qualified path of the {@code ~src} class
     * @param filter            parsed {@code ~filter} expression, or
     *                          {@code null} when no filter was written
     * @param propertyBindings  per-property bindings in declaration order
     */
    record Pure(
            String className,
            @com.legend.base.Nullable String setId,
            @com.legend.base.Nullable String extendsSetId,
            boolean root,
            /**
             * The {@code ~src} class, or {@code null} when the mapping
             * declares none.
             *
             * <p>{@code ~src} is OPTIONAL in Legend — engine's rule is
             * {@code (mappingSrc | mappingFilter)*}, its walker calls
             * {@code validateAndExtractOptionalField}, and its compiler
             * writes {@code _srcClass(null)} without complaint. Its purpose
             * is to declare the TYPE of {@code $src} inside the property
             * lambdas, so a mapping whose bindings never read {@code $src}
             * (constants, say) has nothing to declare.
             *
             * <p>Such a mapping has no source EXTENT, so there is nothing to
             * iterate and it cannot produce rows — see
             * {@code MappingNormalizer#synthM2M}, which refuses it. It is
             * still meaningful to parse, compile and analyse, which is why
             * engine's own corpus contains them.
             */
            @com.legend.base.Nullable String sourceClass,
            @com.legend.base.Nullable ValueSpecification filter,
            List<PropertyBinding> propertyBindings) implements ClassMapping {

        public Pure {
            Objects.requireNonNull(className, "Class name cannot be null");
            if (sourceClass == null && filter != null) {
                // engine builds the implicit `$src` for a parameterless
                // filter as `new PackageableType(srcClass)` with NO null
                // check (ClassMappingFirstPassBuilder:127), so this shape
                // crashes there rather than being refused. A filter reads
                // the source by definition; refusing it is what engine
                // means, stated instead of thrown.
                throw new IllegalArgumentException(
                        "a ~filter needs a ~src: the filter reads $src, so"
                                + " the mapping must declare its type");
            }
            propertyBindings = propertyBindings == null
                    ? List.of()
                    : List.copyOf(propertyBindings);
        }

        /**
         * One property binding inside a {@link Pure} class mapping. The
         * RHS expression is parsed eagerly into a
         * {@link ValueSpecification} at parse time.
         *
         * <p>The three optional heads mirror the engine's
         * {@code PurePropertyMapping} (M3CoreParser.g4 {@code mappingLine}:
         * {@code sourceMappingId/targetMappingId}, {@code explodeProperty},
         * {@code localMappingProperty}). They are RECORDED, never dropped —
         * audit 21a: a {@code [targetSetId]} route selects a specific SET of
         * the routed class, and dropping it is the audit-11 wrong-rows shape
         * mirrored on the Pure side (poison-or-record, never silent).
         *
         * @param propertyName  target property name
         * @param expression    parsed RHS Pure expression
         * @param sourceSetId   {@code prop[src, tgt]:} source route; null if absent
         * @param targetSetId   {@code prop[tgt]:} / {@code prop[src, tgt]:}
         *                      target-set route; null if absent
         * @param explode       {@code prop*:} explosion marker (index-aligned
         *                      zip fan-out — a row-count semantic)
         * @param local         {@code +prop: Type[m]:} mapping-local property
         *                      (engine keeps it DISTINCT from class properties)
         */
        public record PropertyBinding(String propertyName,
                ValueSpecification expression,
                @com.legend.base.Nullable String sourceSetId, @com.legend.base.Nullable String targetSetId,
                boolean explode, boolean local,
                @com.legend.base.Nullable String enumMappingId) {
            public PropertyBinding {
                Objects.requireNonNull(propertyName, "Property name cannot be null");
                Objects.requireNonNull(expression, "Property binding expression cannot be null");
            }

            /** No enum transformer (the overwhelmingly common shape). */
            public PropertyBinding(String propertyName,
                    ValueSpecification expression,
                    @com.legend.base.Nullable String sourceSetId,
                    @com.legend.base.Nullable String targetSetId,
                    boolean explode, boolean local) {
                this(propertyName, expression, sourceSetId, targetSetId,
                        explode, local, null);
            }

            /** Plain binding: no set route, no explosion, not local. */
            public PropertyBinding(String propertyName, ValueSpecification expression) {
                this(propertyName, expression, null, null, false, false, null);
            }

            /** Same heads, different RHS (name-resolution rebuild). */
            public PropertyBinding withExpression(ValueSpecification e) {
                return new PropertyBinding(propertyName, e,
                        sourceSetId, targetSetId, explode, local, enumMappingId);
            }
        }
    }

    /**
     * An Operation UNION class mapping ({@code *Person : Operation {
     * union_...(pSet1, pSet2) }}): the class extent is the UNION ALL of its
     * member sets' extents. Synthesized as
     * {@code concatenate(project(set1), project(set2)) -> map(^Class(...))}
     * over the members' SHARED scalar properties.
     */
    record Union(
            String className,
            @com.legend.base.Nullable String setId,
            @com.legend.base.Nullable String extendsSetId,
            boolean root,
            List<String> memberSetIds) implements ClassMapping {
        public Union {
            Objects.requireNonNull(className, "Class name cannot be null");
            memberSetIds = memberSetIds == null ? List.of() : List.copyOf(memberSetIds);
        }
    }

    /**
     * An inheritance Operation mapping ({@code RoadVehicle : Operation {
     * inheritance_OperationSetImplementation_1__SetImplementation_MANY_() }})
     * — the class's extent is the UNION of every mapped set of its
     * subclasses (members are implicit: the engine's router resolves them
     * from the hierarchy).
     */
    record Inheritance(
            String className,
            @com.legend.base.Nullable String setId,
            @com.legend.base.Nullable String extendsSetId,
            boolean root) implements ClassMapping {
        public Inheritance {
            Objects.requireNonNull(className, "Class name cannot be null");
        }
    }

    /**
     * A Relation-function class mapping — the class's extent is the
     * RELATION a zero-arg function returns; properties bind to its columns
     * by name:
     * <pre>
     *   *Person[person]: Relation
     *   {
     *     ~func my::pkg::personTable():Relation&lt;Any&gt;[1]
     *     firstName: FIRSTNAME,
     *     +firmId: Integer[1]: FIRMID
     *   }
     * </pre>
     * {@code +local} columns declare mapping-local properties (XStore
     * association keys), recorded with {@code local=true}.
     *
     * @param funcRef the {@code ~func} reference as written — a plain FQN,
     *                a signature form {@code f():Relation<Any>[1]}, or the
     *                mangled form {@code f__Relation_1_}
     */
    record RelationFunction(
            String className,
            @com.legend.base.Nullable String setId,
            @com.legend.base.Nullable String extendsSetId,
            boolean root,
            @com.legend.base.Nullable String funcRef,
            @com.legend.base.Nullable com.legend.protocol.spec.ValueSpecification inlineSource,
            List<Col> columns,
            List<String> primaryKey) implements ClassMapping {
        public RelationFunction {
            Objects.requireNonNull(className, "Class name cannot be null");
            if ((funcRef == null) == (inlineSource == null)) {
                // exactly ONE source channel: a named ~func ref or the
                // ~src inline expression (engine #4941 supports both)
                throw new IllegalStateException("a Relation mapping needs"
                        + " exactly one of funcRef/inlineSource");
            }
            columns = columns == null ? List.of() : List.copyOf(columns);
            primaryKey = primaryKey == null ? List.of() : List.copyOf(primaryKey);
        }

        /** A relation mapping with no {@code ~primaryKey} of its own. */
        public RelationFunction(String className,
                @com.legend.base.Nullable String setId,
                @com.legend.base.Nullable String extendsSetId, boolean root,
                String funcRef, List<Col> columns) {
            this(className, setId, extendsSetId, root, funcRef, null, columns,
                    List.of());
        }

        /** One {@code property: COLUMN} binding. */
        public record Col(String property, @com.legend.base.Nullable String column, boolean local,
                @com.legend.base.Nullable String enumMappingId, List<Col> embedded,
                @com.legend.base.Nullable String inlineSetId,
                @com.legend.base.Nullable com.legend.protocol.spec.ValueSpecification expr) {
            public Col {
                embedded = embedded == null ? List.of() : List.copyOf(embedded);
            }

            /** Column-name form (no row expression). */
            public Col(String property, @com.legend.base.Nullable String column, boolean local,
                    @com.legend.base.Nullable String enumMappingId, List<Col> embedded,
                    @com.legend.base.Nullable String inlineSetId) {
                this(property, column, local, enumMappingId, embedded, inlineSetId, null);
            }

            /** Replace every {@code $src} in a row-expression binding with the ROW
             *  variable — the normalizer's inlining step for {@link #expr()} cols. */
            public static com.legend.protocol.spec.ValueSpecification bindSrc(
                    com.legend.protocol.spec.ValueSpecification v,
                    com.legend.protocol.spec.ValueSpecification row) {
                if (v instanceof com.legend.protocol.spec.Variable var
                        && "src".equals(var.name())) {
                    return row;
                }
                return v.mapChildren(c -> bindSrc(c, row));
            }

            /** An EMBEDDED block ({@code prop ( sub: COL, ... )}). */
            public Col(String property, @com.legend.base.Nullable String column, boolean local,
                    @com.legend.base.Nullable String enumMappingId, List<Col> embedded) {
                this(property, column, local, enumMappingId, embedded, null);
            }

            /** Enum-decoded column binding (no embedded block). */
            public Col(String property, @com.legend.base.Nullable String column, boolean local,
                    @com.legend.base.Nullable String enumMappingId) {
                this(property, column, local, enumMappingId, List.of(), null);
            }

            /** Plain (non-enum) column binding. */
            public Col(String property, String column, boolean local) {
                this(property, column, local, null, List.of(), null);
            }

            /** An EMBEDDED block: {@code prop ( sub: COL, ... )} — the
             * property is class-typed, its sub-bindings share this row. */
            public boolean isEmbedded() {
                return !embedded.isEmpty();
            }
        }
    }
}
