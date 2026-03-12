package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.Join;
import org.finos.legend.engine.store.RelationalMapping;

import java.util.List;
import java.util.Map;

/**
 * Per-node type information stored in the compilation side table.
 *
 * <p>
 * During the compile pass, {@link CleanCompiler} computes a {@code TypeInfo}
 * for every {@link ValueSpecification} node in the AST. The SQL generation
 * pass ({@link PlanGenerator}) looks up this info to understand column types,
 * resolve property→column mappings, and validate references.
 *
 * <p>
 * Designed for Rust portability: side table pattern
 * ({@code HashMap<NodeId, TypeInfo>}) works cleanly in both Java and Rust.
 *
 * @param relationType The output columns of this expression (null for scalars)
 * @param mapping      Resolved class→table mapping (null when no class context)
 * @param associations Pre-resolved association targets (property name →
 *                     target).
 *                     Populated by CleanCompiler when multi-hop property paths
 *                     are detected in project/filter lambdas. Empty for most
 *                     nodes.
 * @param inlinedBody  If this node is a user-defined function call, the
 *                     expanded
 *                     body AST (after param substitution + re-parse).
 *                     PlanGenerator
 *                     processes this instead of the original function call
 *                     node.
 *                     Null for all standard nodes.
 */
public record TypeInfo(
        RelationType relationType,
        RelationalMapping mapping,
        Map<String, AssociationTarget> associations,
        List<SortSpec> sortSpecs,
        List<ProjectionSpec> projections,
        List<ColumnSpec> columnSpecs,
        boolean structSource,
        String joinType,
        List<WindowFunctionSpec> windowSpecs,
        ValueSpecification inlinedBody,
        GenericType scalarType,
        boolean lambdaParam,
        /** Table alias prefix for property accesses (e.g. "left_src" in join conditions). */
        String columnAlias,
        /** Pre-resolved pivot specification. */
        PivotSpec pivotSpec) {

    /**
     * Pre-resolved association navigation target.
     * Computed by CleanCompiler so PlanGenerator needs no ModelContext.
     */
    public record AssociationTarget(
            RelationalMapping targetMapping,
            Join join,
            boolean isToMany) {
    }

    /**
     * Pre-resolved sort specification.
     * Computed by CleanCompiler from various AST patterns (asc/desc, sortInfo,
     * etc).
     */
    public record SortSpec(String column, SortDirection direction) {
    }

    /**
     * Pre-resolved projection column.
     * Computed by CleanCompiler from project() lambda paths and mapping resolution.
     */
    public record ProjectionSpec(
            List<String> propertyPath,
            String resolvedColumn,
            String alias,
            ValueSpecification computedExpr) {
        /** Simple property projection (no computed expression). */
        public ProjectionSpec(List<String> propertyPath, String resolvedColumn, String alias) {
            this(propertyPath, resolvedColumn, alias, null);
        }

        public boolean isAssociation() {
            return propertyPath.size() > 1;
        }

        public String property() {
            return propertyPath.getLast();
        }

        public String associationProperty() {
            return propertyPath.get(0);
        }
    }

    /**
     * Pre-resolved column reference for select/rename/groupBy.
     * Populated by CleanCompiler, consumed by PlanGenerator.
     */
    public record ColumnSpec(String columnName, String alias, String aggFunction, List<String> extraArgs) {
        /** Simple column reference (select). */
        public static ColumnSpec col(String name) {
            return new ColumnSpec(name, null, null, List.of());
        }

        /** Renamed column. */
        public static ColumnSpec renamed(String oldName, String newName) {
            return new ColumnSpec(oldName, newName, null, List.of());
        }

        /** Aggregate column (single-arg). */
        public static ColumnSpec agg(String sourceCol, String alias, String func) {
            return new ColumnSpec(sourceCol, alias, func, List.of());
        }

        /** Aggregate column (multi-arg, e.g. CORR(x, y), QUANTILE_CONT(x, 0.5)). */
        public static ColumnSpec aggMulti(String sourceCol, String alias, String func, List<String> extraArgs) {
            return new ColumnSpec(sourceCol, alias, func, extraArgs);
        }

        /** True if this is an aggregate spec. */
        public boolean isAggregate() {
            return aggFunction != null;
        }

        /** True if this is a rename spec. */
        public boolean isRename() {
            return alias != null && aggFunction == null;
        }
    }

    /**
     * Pre-resolved window function specification for extend().
     * Stores <b>Pure-level semantics only</b> — no SQL names or SQL syntax.
     * PlanGenerator maps Pure function names to SQL at generation time.
     */
    public record WindowFunctionSpec(
            String pureFunctionName,
            String sourceColumn,
            List<String> partitionBy,
            List<SortSpec> orderBy,
            FrameSpec frame,
            String alias,
            String wrapperFuncName,
            List<String> wrapperArgs,
            int ntileArg,
            List<String> extraArgs) {

        /** Simple zero-arg window function (rowNumber, rank, etc). */
        public static WindowFunctionSpec ranking(String pureFunc, String alias,
                List<String> partitionBy, List<SortSpec> orderBy, FrameSpec frame) {
            return new WindowFunctionSpec(pureFunc, null, partitionBy, orderBy, frame, alias, null, List.of(), 0,
                    List.of());
        }

        /** Aggregate window function (plus, average, count, etc). */
        public static WindowFunctionSpec aggregate(String pureFunc, String sourceCol, String alias,
                List<String> partitionBy, List<SortSpec> orderBy, FrameSpec frame) {
            return new WindowFunctionSpec(pureFunc, sourceCol, partitionBy, orderBy, frame, alias, null, List.of(), 0,
                    List.of());
        }

        /**
         * Multi-arg aggregate window function (corr, covar, percentile, nthValue,
         * joinStrings).
         */
        public static WindowFunctionSpec aggregateMulti(String pureFunc, String sourceCol, String alias,
                List<String> partitionBy, List<SortSpec> orderBy, FrameSpec frame, List<String> extraArgs) {
            return new WindowFunctionSpec(pureFunc, sourceCol, partitionBy, orderBy, frame, alias, null, List.of(), 0,
                    extraArgs);
        }

        /** Wrapped window function (e.g. round(cumulativeDistribution(), 2)). */
        public static WindowFunctionSpec wrapped(String innerPureFunc, String wrapperPureFunc,
                List<String> extraArgs, String alias,
                List<String> partitionBy, List<SortSpec> orderBy, FrameSpec frame) {
            return new WindowFunctionSpec(innerPureFunc, null, partitionBy, orderBy, frame, alias,
                    wrapperPureFunc, extraArgs, 0, List.of());
        }

        /** NTILE window function. */
        public static WindowFunctionSpec ntile(int buckets, String alias,
                List<String> partitionBy, List<SortSpec> orderBy, FrameSpec frame) {
            return new WindowFunctionSpec("ntile", null, partitionBy, orderBy, frame, alias, null, List.of(), buckets,
                    List.of());
        }

        public boolean isWrapped() {
            return wrapperFuncName != null;
        }

        public boolean isNtile() {
            return "ntile".equals(pureFunctionName);
        }

        public boolean hasSourceColumn() {
            return sourceColumn != null;
        }
    }

    /**
     * Pre-resolved frame specification for window functions.
     * Stores Pure-level semantics (not SQL text).
     */
    public record FrameSpec(String frameType, FrameBound start, FrameBound end) {
        /** Frame types: "rows" or "range". */
    }

    /**
     * A single frame bound: unbounded, currentRow, or offset (positive = following,
     * negative = preceding).
     */
    public record FrameBound(FrameBoundType type, long offset) {
        public static FrameBound unbounded() {
            return new FrameBound(FrameBoundType.UNBOUNDED, 0);
        }

        public static FrameBound currentRow() {
            return new FrameBound(FrameBoundType.CURRENT_ROW, 0);
        }

        public static FrameBound offset(long n) {
            return new FrameBound(FrameBoundType.OFFSET, n);
        }
    }

    public enum FrameBoundType {
        UNBOUNDED, CURRENT_ROW, OFFSET
    }

    public enum SortDirection {
        ASC, DESC
    }

    /**
     * Pre-resolved pivot specification.
     * Computed by CleanCompiler from pivot() AST params.
     */
    public record PivotSpec(
            List<String> pivotColumns,
            List<PivotAggSpec> aggregates) {
    }

    /**
     * Single aggregate in a pivot: AGG(valueCol) AS alias.
     * @param alias       Output column alias suffix
     * @param aggFunction Aggregate function name (SUM, COUNT, etc.)
     * @param valueColumn Column to aggregate (null if expression)
     * @param valueExpr   Pre-compiled expression AST (null if simple column)
     */
    public record PivotAggSpec(
            String alias,
            String aggFunction,
            String valueColumn,
            ValueSpecification valueExpr) {
    }

    /** Creates a TypeInfo for a scalar (non-relational) expression. */
    public static TypeInfo scalar() {
        return builder().build();
    }

    /** Creates a TypeInfo for a scalar with a known type. */
    public static TypeInfo scalarOf(GenericType type) {
        return builder().scalarType(type).build();
    }

    /** Creates a TypeInfo marking a lambda parameter variable with a declared type. */
    public static TypeInfo lambdaParamOf(GenericType type) {
        return builder().scalarType(type).lambdaParam(true).build();
    }

    /** Creates a TypeInfo marking a lambda parameter variable (untyped). */
    public static TypeInfo lambdaParamMarker() {
        return builder().lambdaParam(true).build();
    }

    /** Creates a TypeInfo with type info but no mapping. */
    public static TypeInfo of(RelationType relationType) {
        return builder().relationType(relationType).build();
    }

    /** Full constructor with both type and mapping. */
    public static TypeInfo of(RelationType relationType, RelationalMapping mapping) {
        return builder().relationType(relationType).mapping(mapping).build();
    }

    /** Full constructor with associations. */
    public static TypeInfo of(RelationType relationType, RelationalMapping mapping,
            Map<String, AssociationTarget> associations) {
        return builder().relationType(relationType).mapping(mapping).associations(associations).build();
    }

    /** Constructor for struct-based sources. */
    public static TypeInfo structOf(RelationType relationType) {
        return builder().relationType(relationType).structSource(true).build();
    }

    /** Creates a TypeInfo tagging a property access with a specific join-side alias. */
    public static TypeInfo withAlias(String columnAlias) {
        return builder().columnAlias(columnAlias).build();
    }

    // ===== Builder =====

    /** Creates a fresh builder with all defaults (nulls, empty lists, false booleans). */
    public static Builder builder() {
        return new Builder();
    }

    /** Creates a builder pre-populated from an existing TypeInfo (clone-with-override). */
    public static Builder from(TypeInfo source) {
        return new Builder(source);
    }

    /**
     * Mutable builder for TypeInfo. All fields default to null/empty/false.
     * Adding a new field to the record only requires updating this class.
     */
    public static final class Builder {
        private RelationType relationType;
        private RelationalMapping mapping;
        private Map<String, AssociationTarget> associations = Map.of();
        private List<SortSpec> sortSpecs = List.of();
        private List<ProjectionSpec> projections = List.of();
        private List<ColumnSpec> columnSpecs = List.of();
        private boolean structSource;
        private String joinType;
        private List<WindowFunctionSpec> windowSpecs = List.of();
        private ValueSpecification inlinedBody;
        private GenericType scalarType;
        private boolean lambdaParam;
        private String columnAlias;
        private PivotSpec pivotSpec;

        private Builder() {}

        private Builder(TypeInfo src) {
            this.relationType = src.relationType();
            this.mapping = src.mapping();
            this.associations = src.associations();
            this.sortSpecs = src.sortSpecs();
            this.projections = src.projections();
            this.columnSpecs = src.columnSpecs();
            this.structSource = src.structSource();
            this.joinType = src.joinType();
            this.windowSpecs = src.windowSpecs();
            this.inlinedBody = src.inlinedBody();
            this.scalarType = src.scalarType();
            this.lambdaParam = src.lambdaParam();
            this.columnAlias = src.columnAlias();
            this.pivotSpec = src.pivotSpec();
        }

        public Builder relationType(RelationType v) { this.relationType = v; return this; }
        public Builder mapping(RelationalMapping v) { this.mapping = v; return this; }
        public Builder associations(Map<String, AssociationTarget> v) { this.associations = v; return this; }
        public Builder sortSpecs(List<SortSpec> v) { this.sortSpecs = v; return this; }
        public Builder projections(List<ProjectionSpec> v) { this.projections = v; return this; }
        public Builder columnSpecs(List<ColumnSpec> v) { this.columnSpecs = v; return this; }
        public Builder structSource(boolean v) { this.structSource = v; return this; }
        public Builder joinType(String v) { this.joinType = v; return this; }
        public Builder windowSpecs(List<WindowFunctionSpec> v) { this.windowSpecs = v; return this; }
        public Builder inlinedBody(ValueSpecification v) { this.inlinedBody = v; return this; }
        public Builder scalarType(GenericType v) { this.scalarType = v; return this; }
        public Builder lambdaParam(boolean v) { this.lambdaParam = v; return this; }
        public Builder columnAlias(String v) { this.columnAlias = v; return this; }
        public Builder pivotSpec(PivotSpec v) { this.pivotSpec = v; return this; }

        public TypeInfo build() {
            return new TypeInfo(relationType, mapping, associations, sortSpecs, projections,
                    columnSpecs, structSource, joinType, windowSpecs, inlinedBody, scalarType,
                    lambdaParam, columnAlias, pivotSpec);
        }
    }

    // ===== Derived checks — no SourceKind enum needed =====

    /** True if backed by a class mapping (Person.all()). */
    public boolean isClassBased() {
        return mapping != null;
    }

    /** True if this produces a relational (table-like) result with columns. */
    public boolean isRelational() {
        return relationType != null && !relationType.columns().isEmpty();
    }

    /** True if this is a scalar expression (no columns). */
    public boolean isScalar() {
        return relationType == null || relationType.columns().isEmpty();
    }

    /** True if the scalar type is a list/collection. */
    public boolean isList() {
        return scalarType != null && scalarType.isList();
    }

    /** True if the scalar type is a date (StrictDate, DateTime, Date — not StrictTime). */
    public boolean isDateType() {
        return scalarType != null && scalarType.isDate();
    }

    /** True if this is a List with mixed element types (element type is ANY). */
    public boolean isMixedList() {
        return scalarType != null && scalarType.isList()
                && scalarType.elementType() == GenericType.Primitive.ANY;
    }

    /** True if this node has pre-resolved association targets. */
    public boolean hasAssociations() {
        return associations != null && !associations.isEmpty();
    }

    /** True if this expression is rooted in a struct literal. */
    public boolean isStructSource() {
        return structSource;
    }

    /** True if this node has pre-resolved sort specs. */
    public boolean hasSortSpecs() {
        return sortSpecs != null && !sortSpecs.isEmpty();
    }

    /** Returns the physical table name from the mapping, or null if unmapped. */
    public String tableName() {
        return mapping != null ? mapping.table().name() : null;
    }

    /**
     * Extracts simple name from a qualified name (e.g., "model::Person" →
     * "Person").
     * Shared utility used by both CleanCompiler and PlanGenerator.
     */
    public static String simpleName(String qualifiedName) {
        if (qualifiedName == null)
            return null;
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
}
