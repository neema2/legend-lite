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
        GenericType scalarType) {

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

    /** Creates a TypeInfo for a scalar (non-relational) expression. */
    public static TypeInfo scalar() {
        return new TypeInfo(null, null, Map.of(), List.of(), List.of(), List.of(), false, null, List.of(), null, null);
    }

    /** Creates a TypeInfo for a scalar with a known type. */
    public static TypeInfo scalarOf(GenericType type) {
        return new TypeInfo(null, null, Map.of(), List.of(), List.of(), List.of(), false, null, List.of(), null, type);
    }

    /** Creates a TypeInfo with type info but no mapping. */
    public static TypeInfo of(RelationType relationType) {
        return new TypeInfo(relationType, null, Map.of(), List.of(), List.of(), List.of(), false, null, List.of(),
                null, null);
    }

    /** Full constructor with both type and mapping. */
    public static TypeInfo of(RelationType relationType, RelationalMapping mapping) {
        return new TypeInfo(relationType, mapping, Map.of(), List.of(), List.of(), List.of(), false, null, List.of(),
                null, null);
    }

    /** Full constructor with associations. */
    public static TypeInfo of(RelationType relationType, RelationalMapping mapping,
            Map<String, AssociationTarget> associations) {
        return new TypeInfo(relationType, mapping, associations, List.of(), List.of(), List.of(), false, null, null,
                null, null);
    }

    /** Constructor for struct-based sources. */
    public static TypeInfo structOf(RelationType relationType) {
        return new TypeInfo(relationType, null, Map.of(), List.of(), List.of(), List.of(), true, null, List.of(), null,
                null);
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
