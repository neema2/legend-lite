package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.typed.*;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.relation.*;
import com.gs.legend.plan.lowering.scalar.*;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * Root lowerer: dispatches a {@link TypedSpec} to the appropriate rule module
 * in {@code plan.lowering.relation.**} or {@code plan.lowering.scalar.**}.
 *
 * <p>Two exhaustive sealed switches over {@link TypedSpec} — {@link #lowerRelation}
 * for nodes expected to produce a {@link SqlRelation} and {@link #lowerScalar} for
 * nodes expected to produce a {@link SqlExpr}. If a node's kind doesn't match the
 * entry point's shape (e.g., a scalar literal passed to {@link #lowerRelation})
 * that's a compiler bug surfaced as a dedicated exception.
 *
 * <p><strong>Design</strong>: this file is the ONLY place where a sealed switch
 * over {@code TypedSpec} lives at the entry point of plan generation. Rule modules
 * focus on a single operator family; {@code javac}'s sealed-exhaustiveness check
 * ensures we can never silently drop a variant at the dispatch layer.
 */
public final class Lowerer {
    private Lowerer() {}

    /** Lower a {@link TypedSpec} expected to produce a relational tree. */
    public static SqlRelation lowerRelation(TypedSpec node, LoweringContext ctx) {
        return switch (node) {
            // Relation sources
            case TypedGetAll n         -> SourceLowering.lower(n, ctx);
            case TypedTableReference n -> SourceLowering.lower(n, ctx);
            case TypedSourceUrl n      -> SourceLowering.lower(n, ctx);
            case TypedTdsLiteral n     -> SourceLowering.lower(n, ctx);

            // Relation operators
            case TypedFilter n      -> FilterLowering.lower(n, ctx);
            case TypedProject n     -> ProjectLowering.lower(n, ctx);
            case TypedSort n        -> SortLimitLowering.lower(n, ctx);
            case TypedSlice n       -> SortLimitLowering.lower(n, ctx);
            case TypedSelect n      -> SelectRenameLowering.lower(n, ctx);
            case TypedRename n      -> SelectRenameLowering.lower(n, ctx);
            case TypedDistinct n    -> SelectRenameLowering.lower(n, ctx);
            case TypedConcatenate n -> ConcatenateLowering.lower(n, ctx);
            case TypedExtend n      -> ExtendLowering.lower(n, ctx);
            case TypedGroupBy n     -> GroupByAggregateLowering.lower(n, ctx);
            case TypedAggregate n   -> GroupByAggregateLowering.lower(n, ctx);
            case TypedPivot n       -> GroupByAggregateLowering.lower(n, ctx);
            case TypedJoin n        -> JoinLowering.lower(n, ctx);
            case TypedAsOfJoin n    -> JoinLowering.lower(n, ctx);
            case TypedFlatten n     -> FlattenLowering.lower(n, ctx);
            case TypedFrom n        -> FromLowering.lower(n, ctx);
            case TypedGraphFetch n  -> GraphFetchLowering.lower(n, ctx);

            // Pipeline terminators: {@code serialize} performs JSON-envelope
            // wrapping over its lowered source (the format-conversion step
            // declared by Pure's {@code serialize}). {@code write} is still
            // on the stage-5 backlog — its INSERT lowering lands with the
            // dedicated write pass.
            case TypedSerialize n -> SerializeRelLowering.lower(n, ctx);
            // Implicit-serialize marker: synthesized by MappingResolver for
            // bare {@code Class[*]} roots. Routes through the same JSON-envelope
            // mechanism as explicit {@link TypedSerialize}.
            case TypedSerializeImplicit n -> SerializeRelLowering.lower(n, ctx);

            // Operator-specific routing — each rule module owns its own
            // dispatch decision; Lowerer is pure delegation.
            case TypedUserCall    n -> throw new PlanGenNotPortedException(n, "user-call:not-inlined",
                    "TypedUserCall reached Lowerer (relation) — UserCallInliner should have inlined it; fqn=" + n.functionFqn());
            case TypedBlock       n -> ControlFlowLowering.lowerBlock(n, ctx);
            case TypedCast        n -> ControlFlowLowering.lowerCast(n, ctx);
            case TypedVariable    n -> VariableLowering.lowerVariable(n, ctx);
            case TypedNewInstance n -> CollectionLowering.lower(n, ctx);
            case TypedCollection  n -> CollectionLowering.lower(n, ctx);

            // Everything else: one uniform call. {@link LoweringContext#toRelation}
            // checks {@link LoweringContext#isRelationalSource} and either
            // dispatches to {@code lowerRelation} or wraps the scalar form.
            case TypedLet            n -> ctx.toRelation(n);
            case TypedCInteger       n -> ctx.toRelation(n);
            case TypedCFloat         n -> ctx.toRelation(n);
            case TypedCDecimal       n -> ctx.toRelation(n);
            case TypedCString        n -> ctx.toRelation(n);
            case TypedCBoolean       n -> ctx.toRelation(n);
            case TypedCDateTime      n -> ctx.toRelation(n);
            case TypedCStrictDate    n -> ctx.toRelation(n);
            case TypedCStrictTime    n -> ctx.toRelation(n);
            case TypedCLatestDate    n -> ctx.toRelation(n);
            case TypedCByteArray     n -> ctx.toRelation(n);
            case TypedEnumValue      n -> ctx.toRelation(n);
            case TypedPropertyAccess n -> ctx.toRelation(n);
            case TypedFold           n -> ctx.toRelation(n);
            case TypedNativeCall     n -> ctx.toRelation(n);
            case TypedStructExtract  n -> ctx.toRelation(n);
            case TypedIf             n -> RelationalIfLowering.lower(n, ctx);
            case TypedMatch          n -> ctx.toRelation(n);
            case TypedZip            n -> ctx.toRelation(n);
            case TypedEval           n -> ctx.toRelation(n);
            case TypedMap            n -> ctx.toRelation(n);
            case TypedWrite          n -> ctx.toRelation(n);

            // Lambda at a relation entry: result is the last body statement (matches
            // the legacy PlanGen behaviour for {@code LambdaFunction}). Parameters, if
            // any, are bound by the parent caller before dispatching here — a bare
            // root TypedLambda has no parameters to bind.
            case TypedLambda n -> {
                if (n.body().isEmpty()) {
                    throw new PlanGenNotPortedException(n, "dispatch-bug",
                            "TypedLambda has empty body at relation entry point");
                }
                yield lowerRelation(n.body().get(n.body().size() - 1), ctx);
            }
            case TypedPackageableRef n ->
                    throw new PlanGenNotPortedException(n, "dispatch-bug",
                            "TypedPackageableRef is a metadata carrier; expected parent to handle it");
        };
    }

    /** Lower a {@link TypedSpec} expected to produce a scalar {@link SqlExpr}. */
    public static SqlExpr lowerScalar(TypedSpec node, LoweringContext ctx) {
        return switch (node) {
            // Literals
            case TypedCInteger    n -> LiteralLowering.lower(n, ctx);
            case TypedCFloat      n -> LiteralLowering.lower(n, ctx);
            case TypedCDecimal    n -> LiteralLowering.lower(n, ctx);
            case TypedCString     n -> LiteralLowering.lower(n, ctx);
            case TypedCBoolean    n -> LiteralLowering.lower(n, ctx);
            case TypedCDateTime   n -> LiteralLowering.lower(n, ctx);
            case TypedCStrictDate n -> LiteralLowering.lower(n, ctx);
            case TypedCStrictTime n -> LiteralLowering.lower(n, ctx);
            case TypedCLatestDate n -> LiteralLowering.lower(n, ctx);
            case TypedCByteArray  n -> LiteralLowering.lower(n, ctx);
            case TypedEnumValue   n -> LiteralLowering.lower(n, ctx);

            case TypedVariable        n -> VariableLowering.lower(n, ctx);
            case TypedPropertyAccess  n -> PropertyAccessLowering.lower(n, ctx);
            case TypedFold            n -> FoldLowering.lower(n, ctx);
            case TypedNativeCall      n -> ScalarFunctionLowering.lower(n, ctx);

            case TypedNewInstance     n -> StructLowering.lower(n, ctx);
            case TypedStructExtract   n -> StructLowering.lower(n, ctx);
            case TypedCollection      n -> StructLowering.lower(n, ctx);

            case TypedIf    n -> ControlFlowLowering.lower(n, ctx);
            case TypedLet   n -> ControlFlowLowering.lower(n, ctx);
            case TypedBlock n -> ControlFlowLowering.lower(n, ctx);
            case TypedMatch n -> ControlFlowLowering.lower(n, ctx);
            case TypedCast  n -> ControlFlowLowering.lower(n, ctx);
            case TypedZip   n -> ControlFlowLowering.lower(n, ctx);
            case TypedEval  n -> ControlFlowLowering.lower(n, ctx);
            case TypedMap   n -> ControlFlowLowering.lower(n, ctx);

            case TypedSerialize n -> SerializeLowering.lower(n, ctx);
            case TypedWrite     n -> SerializeLowering.lower(n, ctx);
            // Implicit-serialize marker is a relation-position-only construct
            // (a query root wrap). Encountering it as a scalar is a dispatch bug.
            case TypedSerializeImplicit n -> throw notScalar(n);

            case TypedUserCall n -> throw new PlanGenNotPortedException(n, "user-call:not-inlined",
                    "TypedUserCall reached Lowerer (scalar) — UserCallInliner should have inlined it; fqn=" + n.functionFqn());

            // Relational nodes cannot appear in a scalar position directly. A caller
            // asking for a scalar value from one is a bug upstream.
            case TypedGetAll n         -> throw notScalar(n);
            case TypedTableReference n -> throw notScalar(n);
            case TypedSourceUrl n      -> throw notScalar(n);
            case TypedTdsLiteral n     -> throw notScalar(n);
            // Dual-form records: relational arm above (lowerRelation),
            // scalar-list arm below. Routing predicate (isRelationalSource)
            // at outer call-sites picks which switch this node enters.
            case TypedFilter n      -> FilterLowering.lowerAsListExpr(n, ctx);
            case TypedSort n        -> SortLimitLowering.lowerAsListExpr(n, ctx);
            case TypedSlice n       -> SortLimitLowering.lowerAsListExpr(n, ctx);
            case TypedDistinct n    -> SelectRenameLowering.lowerAsListExpr(n, ctx);
            case TypedConcatenate n -> ConcatenateLowering.lowerAsListExpr(n, ctx);
            case TypedFlatten n     -> FlattenLowering.lowerAsListExpr(n, ctx);

            // Relational-only records: Pure has no scalar overload for them.
            case TypedProject n     -> throw notScalar(n);
            case TypedSelect n      -> throw notScalar(n);
            case TypedRename n      -> throw notScalar(n);
            case TypedExtend n      -> throw notScalar(n);
            case TypedGroupBy n     -> throw notScalar(n);
            case TypedAggregate n   -> throw notScalar(n);
            case TypedPivot n       -> throw notScalar(n);
            case TypedJoin n        -> throw notScalar(n);
            case TypedAsOfJoin n    -> throw notScalar(n);
            case TypedFrom n        -> throw notScalar(n);
            case TypedGraphFetch n  -> throw notScalar(n);

            case TypedLambda n -> {
                if (n.body().isEmpty()) {
                    throw new PlanGenNotPortedException(n, "dispatch-bug",
                            "TypedLambda has empty body at scalar entry point");
                }
                yield lowerScalar(n.body().get(n.body().size() - 1), ctx);
            }
            case TypedPackageableRef n ->
                    throw new PlanGenNotPortedException(n, "dispatch-bug",
                            "TypedPackageableRef is a metadata carrier; expected parent to handle it");
        };
    }

    private static PlanGenNotPortedException notScalar(TypedSpec node) {
        return new PlanGenNotPortedException(node, "dispatch-bug",
                "relational node cannot be lowered as a scalar; caller bug");
    }

    /**
     * Output column name produced by {@link LoweringContext#wrapScalar}.
     * Consumers that need to reference the unnested scalar value of a
     * wrapped collection source ({@code [1,2,3]->filter(x|$x>3)}) can bind
     * their lambda parameter to {@code Column(alias, SCALAR_WRAP_COLUMN)}
     * rather than the bare row alias.
     */
    public static final String SCALAR_WRAP_COLUMN = "result";
}
