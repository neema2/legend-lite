package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.store.Join;
import com.gs.legend.plan.GenericType;


import java.util.List;
import java.util.Map;

/**
 * Per-node type information stored in the compilation side table.
 *
 * <p>
 * During the compile pass, {@link TypeChecker} computes a {@code TypeInfo}
 * for every {@link ValueSpecification} node in the AST. The SQL generation
 * pass ({@link PlanGenerator}) looks up this info to understand column types,
 * resolve property→column mappings, and validate references.
 *
 * <p>
 * <b>expressionType is always non-null</b>. It is the single source of truth
 * for the type and multiplicity of every expression. Use {@link #type()} and
 * {@link #schema()} convenience accessors.
 *
 * <p>
 * Designed for Rust portability: side table pattern
 * ({@code HashMap<NodeId, TypeInfo>}) works cleanly in both Java and Rust.
 *
 * @param mapping      Resolved class→table mapping (null when no class context)
 * @param associations Pre-resolved association targets (property name → target)
 * @param inlinedBody  If this node is a user-defined function call, the expanded
 *                     body AST. PlanGenerator processes this instead of the
 *                     original function call node. Null for all standard nodes.
 * @param expressionType The type + multiplicity of this expression. Always non-null.
 */
public record TypeInfo(
        ClassMapping mapping,
        Map<String, AssociationTarget> associations,
        List<SortSpec> sortSpecs,
        List<ProjectionSpec> projections,
        List<ColumnSpec> columnSpecs,
        List<AggColumnSpec> aggColumnSpecs,
        String joinType,
        List<WindowSpec> windowSpecs,
        ValueSpecification inlinedBody,
        /** Table alias prefix for property accesses (e.g. "left_src" in join conditions). */
        String columnAlias,
        /** Pre-resolved pivot specification. */
        PivotSpec pivotSpec,
        /** Pre-resolved variant access annotation (for get() calls). */
        VariantAccess variantAccess,
        /**
         * Right-side column renames for join duplicate resolution.
         * Maps original right-side column name → prefixed name.
         * Empty when no conflicts. Used by PlanGenerator to alias right-side columns.
         */
        Map<String, String> joinColumnRenames,

        /**
         * Graph fetch specification for JSON output.
         * When non-null, PlanGenerator wraps SQL output in json_group_array(json_object(...)).
         * Compiler-resolved from parsed GraphFetchTree — no parser types cross this boundary.
         */
        com.gs.legend.plan.GraphFetchSpec graphFetchSpec,
        /**
         * The type + multiplicity of this expression. Always non-null.
         * This is the single source of truth — no scalarType or relationType fields.
         */
        ExpressionType expressionType,
        /** True if this variable is lambda-bound (fold accumulator, map param, etc). */
        boolean lambdaParam,
        /**
         * The definitively resolved NativeFunctionDef, stamped by ScalarChecker.
         * Downstream callers read this instead of re-resolving overloads.
         * Null for non-function expressions.
         */
        NativeFunctionDef resolvedFunc) {

    // ===== Convenience type accessors (delegate to expressionType) =====

    /** The GenericType of this expression. Never null. */
    public GenericType type() {
        return expressionType.type();
    }

    /** The GenericType.Relation.Schema schema if this is a relational expression, otherwise null. */
    public GenericType.Relation.Schema schema() {
        return expressionType.schema();
    }

    /**
     * Pre-resolved association navigation target.
     * Computed by TypeChecker so PlanGenerator needs no ModelContext.
     */
    public record AssociationTarget(
            ClassMapping targetMapping,
            Join join,
            boolean isToMany) {
    }

    /**
     * Pre-resolved variant access pattern.
     * Computed by TypeChecker from get() argument types.
     * PlanGenerator reads this — never inspects AST node types.
     */
    public sealed interface VariantAccess {
        /** Array index access: get(source, 0) → source[0] */
        record IndexAccess(int index) implements VariantAccess {}
        /** Field name access: get(source, "key") → source->>'key' */
        record FieldAccess(String key) implements VariantAccess {}
    }

    /**
     * Pre-resolved sort specification.
     * Two modes:
     * <ul>
     *   <li>Relation sort: column name + direction (ascending/descending ~col)</li>
     *   <li>Collection sort: lambda body + param name + direction ({p|$p.age})</li>
     * </ul>
     * PlanGenerator dispatches on {@link #hasLambda()}: generateScalar for lambdas,
     * ColumnRef for column names.
     *
     * @param column      Column name for Relation sort (null for Collection sort)
     * @param direction   ASC or DESC
     * @param sortExpr    Lambda body AST for Collection sort (null for Relation sort)
     * @param lambdaParam Lambda param name for Collection sort (null for Relation sort)
     */
    public record SortSpec(String column, SortDirection direction,
                           ValueSpecification sortExpr, String lambdaParam) {
        /** Relation sort: column name + direction */
        public SortSpec(String column, SortDirection direction) {
            this(column, direction, null, null);
        }
        /** Collection sort: lambda body + param name + direction */
        public static SortSpec fromLambda(ValueSpecification body, String param,
                                          SortDirection direction) {
            return new SortSpec(null, direction, body, param);
        }
        /** True if this is a Collection sort with a lambda body. */
        public boolean hasLambda() { return sortExpr != null; }
    }

    /**
     * Pre-resolved projection column.
     * Carries type-level info only: which model properties are traversed
     * and the output column alias. PlanGenerator walks the AST directly
     * for code generation (Pattern A).
     */
    public record ProjectionSpec(
            List<String> propertyPath,
            String alias) {

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
     * Populated by TypeChecker, consumed by PlanGenerator.
     */
    public record ColumnSpec(String columnName, String alias, String aggFunction, List<String> extraArgs, String castType) {
        /** Simple column reference (select). */
        public static ColumnSpec col(String name) {
            return new ColumnSpec(name, null, null, List.of(), null);
        }

        /** Renamed column. */
        public static ColumnSpec renamed(String oldName, String newName) {
            return new ColumnSpec(oldName, newName, null, List.of(), null);
        }

        /** Aggregate column (single-arg). */
        public static ColumnSpec agg(String sourceCol, String alias, String func) {
            return new ColumnSpec(sourceCol, alias, func, List.of(), null);
        }

        /** Aggregate column (multi-arg, e.g. CORR(x, y), QUANTILE_CONT(x, 0.5)). */
        public static ColumnSpec aggMulti(String sourceCol, String alias, String func, List<String> extraArgs) {
            return new ColumnSpec(sourceCol, alias, func, extraArgs, null);
        }

        /** Aggregate column with outer CAST (e.g., CAST(SUM(x) AS INTEGER)). */
        public static ColumnSpec aggCast(String sourceCol, String alias, String func, List<String> extraArgs, String castType) {
            return new ColumnSpec(sourceCol, alias, func, extraArgs, castType);
        }

        /** True if this is an aggregate spec. */
        public boolean isAggregate() {
            return aggFunction != null;
        }

        /** True if this is a rename spec. */
        public boolean isRename() {
            return alias != null && aggFunction == null;
        }

        /** True if this aggregate has an outer CAST wrapper. */
        public boolean hasCast() {
            return castType != null;
        }
    }

    /**
     * Pre-resolved aggregate column specification for groupBy/aggregate.
     * Carries only types + resolved function — PlanGenerator reads fn1/fn2 from AST.
     *
     * @param alias        Output column name
     * @param resolvedFunc Resolved aggregate function from registry (identity dispatch key)
     * @param returnType   Compiled return type from fn2 body
     * @param castType     Resolved cast type (null if no cast wrapper)
     */
    public record AggColumnSpec(
            String alias,
            NativeFunctionDef resolvedFunc,
            GenericType returnType,
            GenericType castType) {
    }

    /**
     * Pre-resolved window extend specification.
     * Follows AggColumnSpec pattern: checker resolves types + function,
     * PlanGenerator reads fn1/fn2 structure from AST.
     *
     * @param resolvedFunc Registry-resolved function (identity dispatch — not a string)
     * @param over         Compiled over() clause (partition/sort/frame)
     * @param alias        Output column name
     * @param returnType   Compiled from fn1 (or fn2 if aggregate)
     * @param castType     Optional cast wrapping (null if none)
     */
    public record WindowSpec(
            NativeFunctionDef resolvedFunc,
            OverSpec over,
            String alias,
            GenericType returnType,
            GenericType castType) {
    }

    /**
     * Compiled over() clause — validated against source schema by ExtendChecker.
     * Pure column names only (no expressions — over() accepts ColSpec, not lambdas).
     */
    public record OverSpec(
            List<String> partitionBy,
            List<SortSpec> orderBy,
            FrameSpec frame) {
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
    public record FrameBound(FrameBoundType type, double offset) {
        public static FrameBound unbounded() {
            return new FrameBound(FrameBoundType.UNBOUNDED, 0);
        }

        public static FrameBound currentRow() {
            return new FrameBound(FrameBoundType.CURRENT_ROW, 0);
        }

        public static FrameBound offset(long n) {
            return new FrameBound(FrameBoundType.OFFSET, n);
        }

        public static FrameBound offset(double n) {
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
     * Computed by TypeChecker from pivot() AST params.
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
     * @param lambdaParam Lambda parameter name for valueExpr (null if simple column).
     *                    PlanGenerator passes this as rowParam so property accesses
     *                    render as column refs, not struct field access.
     */
    public record PivotAggSpec(
            String alias,
            String aggFunction,
            String valueColumn,
            ValueSpecification valueExpr,
            String lambdaParam) {
    }

    // All TypeInfo construction goes through builder() or from(). No static shortcuts —
    // this ensures expressionType is always considered at each construction site.

    // ===== Builder =====

    /** Creates a fresh builder with all defaults (nulls, empty lists). */
    public static Builder builder() {
        return new Builder();
    }

    /** Creates a builder pre-populated from an existing TypeInfo (clone-with-override). */
    public static Builder from(TypeInfo source) {
        return new Builder(source);
    }

    /**
     * Mutable builder for TypeInfo. All fields default to null/empty.
     * Adding a new field to the record only requires updating this class.
     */
    public static final class Builder {
        private ClassMapping mapping;
        private Map<String, AssociationTarget> associations = Map.of();
        private List<SortSpec> sortSpecs = List.of();
        private List<ProjectionSpec> projections = List.of();
        private List<ColumnSpec> columnSpecs = List.of();
        private List<AggColumnSpec> aggColumnSpecs = List.of();
        private String joinType;
        private List<WindowSpec> windowSpecs = List.of();
        private ValueSpecification inlinedBody;
        private String columnAlias;
        private PivotSpec pivotSpec;
        private VariantAccess variantAccess;
        private Map<String, String> joinColumnRenames = Map.of();
        private com.gs.legend.plan.GraphFetchSpec graphFetchSpec;
        private ExpressionType expressionType;
        private boolean lambdaParam;
        private NativeFunctionDef resolvedFunc;

        private Builder() {}

        private Builder(TypeInfo src) {
            this.mapping = src.mapping();
            this.associations = src.associations();
            this.sortSpecs = src.sortSpecs();
            this.projections = src.projections();
            this.columnSpecs = src.columnSpecs();
            this.aggColumnSpecs = src.aggColumnSpecs();
            this.joinType = src.joinType();
            this.windowSpecs = src.windowSpecs();
            this.inlinedBody = src.inlinedBody();
            this.columnAlias = src.columnAlias();
            this.pivotSpec = src.pivotSpec();
            this.variantAccess = src.variantAccess();
            this.joinColumnRenames = src.joinColumnRenames();
            this.graphFetchSpec = src.graphFetchSpec();
            this.expressionType = src.expressionType();
            this.lambdaParam = src.lambdaParam();
            this.resolvedFunc = src.resolvedFunc();
        }

        public Builder mapping(ClassMapping v) { this.mapping = v; return this; }
        public Builder associations(Map<String, AssociationTarget> v) { this.associations = v; return this; }
        public Builder sortSpecs(List<SortSpec> v) { this.sortSpecs = v; return this; }
        public Builder projections(List<ProjectionSpec> v) { this.projections = v; return this; }
        public Builder columnSpecs(List<ColumnSpec> v) { this.columnSpecs = v; return this; }
        public Builder aggColumnSpecs(List<AggColumnSpec> v) { this.aggColumnSpecs = v; return this; }
        public Builder joinType(String v) { this.joinType = v; return this; }
        public Builder windowSpecs(List<WindowSpec> v) { this.windowSpecs = v; return this; }
        public Builder inlinedBody(ValueSpecification v) { this.inlinedBody = v; return this; }
        public Builder columnAlias(String v) { this.columnAlias = v; return this; }
        public Builder pivotSpec(PivotSpec v) { this.pivotSpec = v; return this; }
        public Builder variantAccess(VariantAccess v) { this.variantAccess = v; return this; }
        public Builder joinColumnRenames(Map<String, String> v) { this.joinColumnRenames = v; return this; }
        public Builder graphFetchSpec(com.gs.legend.plan.GraphFetchSpec v) { this.graphFetchSpec = v; return this; }
        public Builder expressionType(ExpressionType v) { this.expressionType = v; return this; }
        public Builder lambdaParam(boolean v) { this.lambdaParam = v; return this; }
        public Builder resolvedFunc(NativeFunctionDef v) { this.resolvedFunc = v; return this; }

        public TypeInfo build() {
            if (expressionType == null) {
                throw new IllegalStateException(
                        "TypeInfo.expressionType must not be null — every expression must have a type");
            }
            return new TypeInfo(mapping, associations, sortSpecs, projections,
                    columnSpecs, aggColumnSpecs, joinType, windowSpecs, inlinedBody,
                    columnAlias, pivotSpec, variantAccess,
                    joinColumnRenames, graphFetchSpec, expressionType,
                    lambdaParam, resolvedFunc);
        }
    }

    // ===== Derived checks =====

    /** True if backed by a class mapping (Person.all()). */
    public boolean isClassBased() {
        return mapping != null;
    }

    /** True if this produces a relational (table-like) result with columns. */
    public boolean isRelational() {
        return expressionType.isRelation();
    }

    /** True if this is a scalar expression (not relational). */
    public boolean isScalar() {
        return expressionType.isScalar();
    }

    /** True if this expression has multiplicity [*] (produces N independent values). */
    public boolean isMany() {
        return expressionType.isMany();
    }

    /** True if the type is a date (StrictDate, DateTime, Date — not StrictTime). */
    public boolean isDateType() {
        return type().isDate();
    }

    /**
     * True if this is a List with heterogeneous elements that need VARIANT wrapping.
     * Matches List&lt;Number&gt; (Integer+Float/Decimal) and List&lt;Date&gt; (StrictDate+DateTime).
     */
    public boolean isHeterogeneousList() {
        GenericType t = type();
        if (!t.isList()) return false;
        GenericType elem = t.elementType();
        return elem == GenericType.Primitive.NUMBER
                || elem == GenericType.Primitive.DATE
                || elem == GenericType.Primitive.ANY;
    }

    /** True if this is a heterogeneous list of date/temporal types (List&lt;Date&gt;). */
    public boolean isDateList() {
        return type().isList()
                && type().elementType() == GenericType.Primitive.DATE;
    }

    /** True if this is a List with mixed element types (element type is ANY). */
    public boolean isMixedList() {
        return type().isList()
                && type().elementType() == GenericType.Primitive.ANY;
    }

    /** True if this node has pre-resolved association targets. */
    public boolean hasAssociations() {
        return associations != null && !associations.isEmpty();
    }

    /** True if this node has pre-resolved sort specs. */
    public boolean hasSortSpecs() {
        return sortSpecs != null && !sortSpecs.isEmpty();
    }

    /** Returns the physical table name from the mapping, or null if unmapped. */
    public String tableName() {
        return mapping != null ? mapping.sourceTable().name() : null;
    }

    /**
     * Extracts simple name from a qualified name (e.g., "model::Person" →
     * "Person").
     * Shared utility used by both TypeChecker and PlanGenerator.
     */
    public static String simpleName(String qualifiedName) {
        if (qualifiedName == null)
            return null;
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * For variant typed extraction: returns the SQL type name if the compiler
     * resolved a concrete target type (e.g., "BIGINT" for @Integer), or null if untyped.
     * Used by PlanGenerator for get(@Type), to(@Type).
     */
    public String variantScalarSqlType(com.gs.legend.sqlgen.SQLDialect dialect) {
        GenericType t = type();
        if (t == GenericType.Primitive.ANY || t == GenericType.Primitive.JSON) return null;
        return dialect.sqlTypeName(t.typeName());
    }

    /**
     * For variant array cast: returns the element SQL type name if the compiler
     * resolved a list target type (e.g., "BIGINT" for toMany(@Integer)), or null.
     * Used by PlanGenerator for toMany(@Type).
     */
    public String variantArrayElementSqlType(com.gs.legend.sqlgen.SQLDialect dialect) {
        GenericType t = type();
        if (!t.isList()) return null;
        GenericType elemType = t.elementType();
        if (elemType == null || elemType == GenericType.Primitive.ANY) return null;
        return dialect.sqlTypeName(elemType.typeName());
    }
}
