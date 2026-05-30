package com.legend.parser.element;

import com.legend.parser.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * One per-class mapping inside a {@link MappingDefinition} &mdash;
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
public sealed interface ClassMapping permits ClassMapping.Relational, ClassMapping.Pure {

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
    record Relational(
            String className,
            String setId,
            String extendsSetId,
            boolean root,
            MappingDefinition.TableReference mainTable,
            FilterMapping filter,
            boolean distinct,
            List<RelationalOperation> groupBy,
            List<RelationalOperation> primaryKey,
            List<PropertyMapping> propertyMappings,
            String sourceUrl) implements ClassMapping {

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
            String setId,
            String extendsSetId,
            boolean root,
            String sourceClass,
            ValueSpecification filter,
            List<PropertyBinding> propertyBindings) implements ClassMapping {

        public Pure {
            Objects.requireNonNull(className, "Class name cannot be null");
            Objects.requireNonNull(sourceClass, "Source class (~src) cannot be null");
            propertyBindings = propertyBindings == null
                    ? List.of()
                    : List.copyOf(propertyBindings);
        }

        /**
         * One property binding inside a {@link Pure} class mapping. The
         * RHS expression is parsed eagerly into a
         * {@link ValueSpecification} at parse time.
         *
         * @param propertyName  target property name
         * @param expression    parsed RHS Pure expression
         */
        public record PropertyBinding(String propertyName, ValueSpecification expression) {
            public PropertyBinding {
                Objects.requireNonNull(propertyName, "Property name cannot be null");
                Objects.requireNonNull(expression, "Property binding expression cannot be null");
            }
        }
    }
}
