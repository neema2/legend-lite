package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.m3.Type;
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
            case TypedWrite n     -> wrapScalar(SerializeLowering.lower(n, ctx), ctx, n);

            // UserCall: bind actuals to formals and inline the callee body.
            case TypedUserCall n -> lowerRelation(n.callee().body().hir(), bindUserCallArgs(n, ctx));

            // Scalar-at-root: wrap as a one-row single-column SELECT.
            case TypedCInteger   n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCFloat     n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCDecimal   n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCString    n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCBoolean   n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCDateTime  n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCStrictDate n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCStrictTime n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCLatestDate n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedCByteArray n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            case TypedEnumValue  n -> wrapScalar(LiteralLowering.lower(n, ctx), ctx, n);
            // Variable in relation context: pattern-match the binding kind.
            // {@link LoweringContext.Rel} — a {@code let r = <rel-expr>}
            // captured as typed HIR — re-lowers the captured node in place.
            // Anything else (Scalar, or unbound) falls through to the scalar
            // form wrapped as a one-row relation.
            case TypedVariable n -> ctx.lookupBinding(n.name()) instanceof LoweringContext.Rel rel
                    ? lowerRelation(rel.node(), ctx)
                    : wrapScalar(VariableLowering.lower(n, ctx), ctx, n);
            case TypedPropertyAccess  n -> wrapScalar(PropertyAccessLowering.lower(n, ctx), ctx, n);
            case TypedFold            n -> wrapScalar(FoldLowering.lower(n, ctx), ctx, n);
            case TypedNativeCall      n -> wrapScalar(ScalarFunctionLowering.lower(n, ctx), ctx, n);
            case TypedNewInstance     n -> wrapScalar(StructLowering.lower(n, ctx), ctx, n);
            case TypedStructExtract   n -> wrapScalar(StructLowering.lower(n, ctx), ctx, n);
            case TypedCollection      n -> wrapScalar(StructLowering.lower(n, ctx), ctx, n);
            case TypedIf    n -> wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);
            // {@code let x = e}: forwards to the scalar form when {@code e} is
            // scalar-typed; when {@code e} is relational the value flows into
            // the surrounding {@link TypedBlock}'s rel-binding pass.
            //
            // TODO(cte-migration): the {@code instanceof Type.Relation} dispatch
            // here and in {@link #lowerBlockRelation} is a known smell — the
            // kind is statically known at type-check time but re-derived at
            // every consumer. Folding it into the IR (split {@code TypedLet}
            // into {@code TypedScalarLet}/{@code TypedRelLet}) would give
            // case-dispatch with exhaustiveness checking, and provide a
            // clean attachment point for CTE-specific state (alias name,
            // materialization hint, correlated-capture flag) when {@code
            // bindRel} graduates from "capture HIR + re-lower per reference"
            // to "lower once + emit as a CTE referenced by alias". Deferred
            // to that migration so the IR shape can be designed against
            // the real CTE requirements rather than guessed at now.
            case TypedLet   n -> n.value().type() instanceof Type.Relation
                    ? lowerRelation(n.value(), ctx)
                    : wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);
            // Block: process intermediate {@code let}s (binding scalar values
            // via {@link LoweringContext#bindVar}, relational values via
            // {@link LoweringContext#bindRel}); dispatch the final statement
            // by its own type.
            case TypedBlock n -> lowerBlockRelation(n, ctx);
            case TypedMatch n -> wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);
            case TypedCast  n -> wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);
            case TypedZip   n -> wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);
            case TypedEval  n -> wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);
            case TypedMap   n -> wrapScalar(ControlFlowLowering.lower(n, ctx), ctx, n);

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

            case TypedUserCall n -> lowerScalar(n.callee().body().hir(), bindUserCallArgs(n, ctx));

            // Relational nodes cannot appear in a scalar position directly. A caller
            // asking for a scalar value from one is a bug upstream.
            case TypedGetAll n         -> throw notScalar(n);
            case TypedTableReference n -> throw notScalar(n);
            case TypedSourceUrl n      -> throw notScalar(n);
            case TypedTdsLiteral n     -> throw notScalar(n);
            case TypedFilter n      -> throw notScalar(n);
            case TypedProject n     -> throw notScalar(n);
            case TypedSort n        -> throw notScalar(n);
            case TypedSlice n       -> throw notScalar(n);
            case TypedSelect n      -> throw notScalar(n);
            case TypedRename n      -> throw notScalar(n);
            case TypedDistinct n    -> throw notScalar(n);
            case TypedConcatenate n -> throw notScalar(n);
            case TypedExtend n      -> throw notScalar(n);
            case TypedGroupBy n     -> throw notScalar(n);
            case TypedAggregate n   -> throw notScalar(n);
            case TypedPivot n       -> throw notScalar(n);
            case TypedJoin n        -> throw notScalar(n);
            case TypedAsOfJoin n    -> throw notScalar(n);
            case TypedFlatten n     -> throw notScalar(n);
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
     * Lower a {@link TypedBlock} in relational position. Handles the
     * {@code let x = e1; let y = e2; ...; finalExpr} idiom where any
     * {@code ei} or the {@code finalExpr} may itself be relational.
     *
     * <p>Each intermediate statement is processed in order:
     * <ul>
     *   <li>{@link TypedLet} with relational value type — bound via
     *       {@link LoweringContext#bindRel(String, TypedSpec)} so a
     *       downstream {@code $x} reference re-lowers the original HIR
     *       in its appropriate (scalar or relational) context.</li>
     *   <li>{@link TypedLet} with scalar value type — bound via the
     *       existing scalar-lowering + {@link LoweringContext#bindVar}
     *       path.</li>
     *   <li>Anything else — lowered for side-effect parity with
     *       {@link com.gs.legend.plan.lowering.scalar.ControlFlowLowering}
     *       and discarded.</li>
     * </ul>
     *
     * <p>The final statement is dispatched on its own type: relational
     * statements go through {@link #lowerRelation} directly, scalar
     * statements go through {@link #wrapScalar}. This is what closes the
     * "{@code TypedXxx (dispatch-bug). relational node cannot be lowered
     * as a scalar; caller bug}" gap that surfaced when blocks ended in
     * (or bound) relational expressions like {@code TypedFilter},
     * {@code TypedProject}, {@code TypedTdsLiteral}, etc.
     */
    private static SqlRelation lowerBlockRelation(TypedBlock n, LoweringContext ctx) {
        LoweringContext cur = ctx;
        int last = n.stmts().size() - 1;
        for (int i = 0; i < last; i++) {
            TypedSpec s = n.stmts().get(i);
            if (s instanceof TypedLet let) {
                if (let.value().type() instanceof Type.Relation) {
                    cur = cur.bindRel(let.name(), let.value());
                } else {
                    SqlExpr value = lowerScalar(let.value(), cur);
                    cur = cur.bindVar(let.name(), value, null);
                }
            } else {
                // Side-effecting intermediate — lower for parity, discard.
                lowerScalar(s, cur);
            }
        }
        TypedSpec tail = n.stmts().get(last);
        return tail.type() instanceof Type.Relation
                ? lowerRelation(tail, cur)
                : wrapScalar(lowerScalar(tail, cur), cur, tail);
    }

    /**
     * Bind a {@link TypedUserCall}'s actuals to the callee's formal
     * parameter names so the inlined body can resolve {@code $param}
     * references. Without this, lowering the body emits bare identifiers
     * (e.g. {@code SELECT (a * 2) AS "doubledAge" FROM "T_PERSON" AS "t0"})
     * for unbound formals.
     *
     * <p>Dispatches per arg by typed kind: relational args are bound via
     * {@link LoweringContext#bindRel} (re-lowered at each {@code $r}
     * reference inside the body, matching how {@code TypedVariable} in
     * relation context handles them), scalar args are eagerly lowered via
     * {@link #lowerScalar} and bound via {@link LoweringContext#bindVar}.
     */
    private static LoweringContext bindUserCallArgs(TypedUserCall n, LoweringContext ctx) {
        var formals = n.callee().parameters();
        var actuals = n.args();
        if (formals.size() != actuals.size()) {
            throw new PlanGenNotPortedException(n, "user-call:arity-mismatch",
                    "callee=" + n.functionFqn() + " formals=" + formals.size()
                            + " actuals=" + actuals.size());
        }
        LoweringContext cur = ctx;
        for (int i = 0; i < formals.size(); i++) {
            String name = formals.get(i).name();
            TypedSpec actual = actuals.get(i);
            if (actual.isRelation()) {
                cur = cur.bindRel(name, actual);
            } else {
                SqlExpr value = lowerScalar(actual, cur);
                cur = cur.bindVar(name, value, null);
            }
        }
        return cur;
    }

    /**
     * Output column name produced by {@link #wrapScalar}. Consumers that
     * need to reference the unnested scalar value of a wrapped collection
     * source ({@code [1,2,3]->filter(x|$x>3)}) can bind their lambda
     * parameter to {@code Column(alias, SCALAR_WRAP_COLUMN)} rather than
     * the bare row alias.
     */
    public static final String SCALAR_WRAP_COLUMN = "result";

    /**
     * Wrap a scalar expression as a one-row one-column {@link SqlRelation.SourceExprRel}.
     *
     * <p>When the originating typed node has multiplicity MANY, the scalar
     * expression is wrapped in {@code UNNEST(...)} so a list value (e.g.
     * {@code [1,2,3]} or the result of {@code listConcat}) is unfolded into
     * N independent rows. Mirrors legacy plangen lines 271-277 — single-list
     * scalar values (multiplicity ONE returning a list) are left as-is.
     */
    private static SqlRelation wrapScalar(SqlExpr expr, LoweringContext ctx, TypedSpec node) {
        String alias = ctx.nextAlias();
        SqlExpr value = (node.info() != null && node.info().isMany())
                ? new SqlExpr.Unnest(expr)
                : expr;
        return new SqlRelation.SourceExprRel(
                value, alias,
                java.util.List.of(new SqlRelation.OutputCol(SCALAR_WRAP_COLUMN, null)));
    }
}
