package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.m3.Type;


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
 * @param instanceLiteral True if this node is a ^Class struct literal (identity-mapped).
 *                     MappingResolver uses this to create identity mappings.
 * @param inlinedBody  If this node is a user-defined function call, the expanded
 *                     body AST. PlanGenerator processes this instead of the
 *                     original function call node. Null for all standard nodes.
 * @param expressionType The type + multiplicity of this expression. Always non-null.
 */
public record TypeInfo(
        boolean instanceLiteral,
        List<SortSpec> sortSpecs,
        List<ProjectionSpec> projections,
        List<ColumnSpec> columnSpecs,
        List<AggColumnSpec> aggColumnSpecs,
        String joinType,
        List<WindowSpec> windowSpecs,
        ValueSpecification inlinedBody,
        /**
         * Right-side column renames for join duplicate resolution.
         * Maps original right-side column name → prefixed name.
         * Empty when no conflicts. Used by PlanGenerator to alias right-side columns.
         */
        Map<String, String> joinColumnRenames,
        /**
         * Parsed TDS literal data (columns + rows).
         * Stamped by TypeChecker for tds() calls, consumed by PlanGenerator to generate VALUES SQL.
         * Null for non-TDS expressions.
         */
        com.gs.legend.ast.TdsLiteral tdsLiteral,
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
        NativeFunctionDef resolvedFunc,
        /**
         * Pre-resolved fold lowering strategy.
         * Stamped by FoldChecker, consumed by PlanGenerator via exhaustive switch.
         * Null for non-fold expressions.
         */
        FoldSpec foldSpec,
        /**
         * Traverse specifications for extend(traverse(...), colSpec).
         * Each TraversalSpec describes a chain of LEFT JOINs. Single-traverse has one element;
         * multi-traverse (multi-join DynaFunction) has multiple.
         * Stamped by ExtendChecker, consumed by PlanGenerator to emit flat JOINs.
         * Null for non-traverse expressions.
         */
        List<TraversalSpec> traversalSpecs,
        /**
         * Resolved physical table name for tableReference() expressions.
         * Stamped by TableReferenceChecker. Used by PlanGenerator and ExtendChecker.
         * Null for non-tableReference expressions.
         */
        String resolvedTableName,
        /**
         * Association property path for multi-hop class property accesses.
         * E.g., $e.firm.legalName → ["firm", "legalName"].
         * Null for direct (non-association) property accesses.
         * Stamped by TypeChecker.compileProperty, consumed by PlanGenerator.generateScalar.
         */
        List<String> associationPath) {

    // ===== Convenience type accessors (delegate to expressionType) =====

    /** The Type of this expression. Never null. */
    public Type type() {
        return expressionType.type();
    }

    /** The Type.Schema schema if this is a relational expression, otherwise null. */
    public Type.Schema schema() {
        return expressionType.schema();
    }

    /**
     * Pre-resolved fold lowering strategy.
     * Computed by FoldChecker, consumed by PlanGenerator via exhaustive switch.
     * Each variant carries exactly the data PlanGenerator needs — zero AST inspection.
     */
    public sealed interface FoldSpec {
        /** Body is add(acc, elem) → SQL: listConcat(init, source). */
        record Concatenation() implements FoldSpec {}

        /** T == V → SQL: listReduce(source, lambda, init). */
        record SameType() implements FoldSpec {}

        /**
         * T ≠ V with decomposable body → SQL: listReduce(listTransform(...), reducer, init).
         * @param elementTransform  f(elem) subtree (acc stripped from body)
         * @param reducerBody       Pre-compiled combineOp(acc, fresh) — fully stamped by FoldChecker
         * @param accParam          Accumulator lambda param name
         * @param freshParam        Fresh element lambda param name
         */
        record MapReduce(ValueSpecification elementTransform,
                         ValueSpecification reducerBody,
                         String accParam, String freshParam) implements FoldSpec {}

        /**
         * T ≠ V, V = List&lt;T&gt;, non-decomposable body → wrap + SQL-level unwrap + listReduce.
         * Marker only — no payload. PlanGenerator handles wrapping/unwrapping.
         */
        record CollectionBuild() implements FoldSpec {}
    }

    /**
     * Pre-resolved sort specification.
     * Two modes:
     * <ul>
     *   <li>Relation sort: column name + direction (ascending/descending ~col)</li>
     *   <li>Collection sort: direction only (PlanGenerator reads key lambda from AST)</li>
     * </ul>
     *
     * @param column    Column name for Relation sort (null for Collection sort)
     * @param direction ASC or DESC
     */
    public record SortSpec(String column, SortDirection direction) {}

    /**
     * Pre-resolved projection column.
     * Carries type-level info only: which model properties are traversed
     * and the output column alias. PlanGenerator walks the AST directly
     * for code generation (Pattern A).
     */
    public record ProjectionSpec(
            List<String> associationPath,
            String alias) {

        /** True if this projection navigates through an association (multi-hop path). */
        public boolean isAssociation() {
            return associationPath != null && associationPath.size() > 1;
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
            Type returnType,
            Type castType) {
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
            Type returnType,
            Type castType) {
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
     * Pre-resolved traverse specification for extend(traverse(...), colSpec).
     * Describes a chain of LEFT JOINs to be emitted as flat joins in SQL.
     *
     * @param hops Ordered list of join hops (first = closest to source, last = terminal)
     */
    public record TraversalSpec(List<TraversalHop> hops) {}

    /**
     * A single hop in a traverse chain.
     *
     * @param tableName      Physical table to LEFT JOIN
     * @param conditionBody  Lambda body for the ON clause (compiled by generateScalar)
     * @param prevParam      Lambda param name bound to the previous table's alias
     * @param hopParam       Lambda param name bound to this hop's table alias
     */
    public record TraversalHop(
            String tableName,
            ValueSpecification conditionBody,
            String prevParam,
            String hopParam) {}



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
        private boolean instanceLiteral;
        private List<SortSpec> sortSpecs = List.of();
        private List<ProjectionSpec> projections = List.of();
        private List<ColumnSpec> columnSpecs = List.of();
        private List<AggColumnSpec> aggColumnSpecs = List.of();
        private String joinType;
        private List<WindowSpec> windowSpecs = List.of();
        private ValueSpecification inlinedBody;
        private Map<String, String> joinColumnRenames = Map.of();
        private com.gs.legend.ast.TdsLiteral tdsLiteral;
        private ExpressionType expressionType;
        private boolean lambdaParam;
        private NativeFunctionDef resolvedFunc;
        private FoldSpec foldSpec;
        private List<TraversalSpec> traversalSpecs;
        private String resolvedTableName;
        private List<String> associationPath;

        private Builder() {}

        private Builder(TypeInfo src) {
            this.instanceLiteral = src.instanceLiteral();
            this.sortSpecs = src.sortSpecs();
            this.projections = src.projections();
            this.columnSpecs = src.columnSpecs();
            this.aggColumnSpecs = src.aggColumnSpecs();
            this.joinType = src.joinType();
            this.windowSpecs = src.windowSpecs();
            this.inlinedBody = src.inlinedBody();
            this.joinColumnRenames = src.joinColumnRenames();
            this.tdsLiteral = src.tdsLiteral();
            this.expressionType = src.expressionType();
            this.lambdaParam = src.lambdaParam();
            this.resolvedFunc = src.resolvedFunc();
            this.foldSpec = src.foldSpec();
            this.traversalSpecs = src.traversalSpecs();
            this.resolvedTableName = src.resolvedTableName();
            this.associationPath = src.associationPath();
        }

        public Builder instanceLiteral(boolean v) { this.instanceLiteral = v; return this; }
        public Builder sortSpecs(List<SortSpec> v) { this.sortSpecs = v; return this; }
        public Builder projections(List<ProjectionSpec> v) { this.projections = v; return this; }
        public Builder columnSpecs(List<ColumnSpec> v) { this.columnSpecs = v; return this; }
        public Builder aggColumnSpecs(List<AggColumnSpec> v) { this.aggColumnSpecs = v; return this; }
        public Builder joinType(String v) { this.joinType = v; return this; }
        public Builder windowSpecs(List<WindowSpec> v) { this.windowSpecs = v; return this; }
        public Builder inlinedBody(ValueSpecification v) { this.inlinedBody = v; return this; }
        public Builder joinColumnRenames(Map<String, String> v) { this.joinColumnRenames = v; return this; }
        public Builder tdsLiteral(com.gs.legend.ast.TdsLiteral v) { this.tdsLiteral = v; return this; }
        public Builder expressionType(ExpressionType v) { this.expressionType = v; return this; }
        public Builder lambdaParam(boolean v) { this.lambdaParam = v; return this; }
        public Builder resolvedFunc(NativeFunctionDef v) { this.resolvedFunc = v; return this; }
        public Builder foldSpec(FoldSpec v) { this.foldSpec = v; return this; }
        public Builder traversalSpec(TraversalSpec v) { this.traversalSpecs = v == null ? null : List.of(v); return this; }
        public Builder traversalSpecs(List<TraversalSpec> v) { this.traversalSpecs = v; return this; }
        public Builder resolvedTableName(String v) { this.resolvedTableName = v; return this; }
        public Builder associationPath(List<String> v) { this.associationPath = v; return this; }

        public TypeInfo build() {
            if (expressionType == null) {
                throw new IllegalStateException(
                        "TypeInfo.expressionType must not be null — every expression must have a type");
            }
            return new TypeInfo(instanceLiteral, sortSpecs, projections,
                    columnSpecs, aggColumnSpecs, joinType, windowSpecs, inlinedBody,
                    joinColumnRenames, tdsLiteral, expressionType,
                    lambdaParam, resolvedFunc, foldSpec, traversalSpecs,
                    resolvedTableName, associationPath);
        }
    }

    // ===== Derived checks =====

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
        if (expressionType == null || !expressionType.isMany()) return false;
        Type elem = type();
        return elem == Type.Primitive.NUMBER
                || elem == Type.Primitive.DATE
                || elem == Type.Primitive.ANY;
    }

    /** True if this is a collection of date/temporal types (Date[*]). */
    public boolean isDateList() {
        return expressionType != null && expressionType.isMany()
                && type() == Type.Primitive.DATE;
    }

    /** True if this is a collection with mixed element types (Any[*]). */
    public boolean isMixedList() {
        return expressionType != null && expressionType.isMany()
                && type() == Type.Primitive.ANY;
    }

    /** True if this node has pre-resolved sort specs. */
    public boolean hasSortSpecs() {
        return sortSpecs != null && !sortSpecs.isEmpty();
    }

}
