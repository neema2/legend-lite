package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedCollection;
import com.gs.legend.compiler.typed.TypedNewInstance;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.compiler.typed.TypedStructExtract;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Scalar struct / collection literals.
 *
 * <p>{@link TypedCollection} is a scalar list: {@code [1, 2, 3]} in Pure, or the
 * args of an {@code x->in([...])} membership test. Lowered to
 * {@link SqlExpr.ArrayLiteral} so the dialect can choose between array syntax
 * and an anonymous VALUES table. Singleton collections (the common implicit
 * wrapping case) unwrap to the single element.
 */
public final class StructLowering {
    private StructLowering() {}

    public static SqlExpr lower(TypedCollection n, LoweringContext ctx) {
        // Always emit {@link SqlExpr.ArrayLiteral}, even for singleton
        // collections. The TypedCollection's type system multiplicity is
        // MANY (set in {@code TypeChecker.compileCollection}); unwrapping a
        // singleton to a bare scalar breaks any downstream consumer that
        // needs an array shape — most visibly {@code [42]->filter(...)},
        // where {@link com.gs.legend.plan.lowering.Lowerer#wrapScalar}
        // emits {@code UNNEST(...)} on a MANY-typed source and DuckDB
        // rejects {@code UNNEST(42)} (not a list).
        List<SqlExpr> elements = new ArrayList<>(n.values().size());
        for (TypedSpec v : n.values()) elements.add(Lowerer.lowerScalar(v, ctx));
        return new SqlExpr.ArrayLiteral(elements);
    }

    /**
     * {@code ^Class(prop1 = v1, prop2 = v2)} — dialect-rendered struct literal.
     * Field order follows the HIR's {@link java.util.Map#entrySet} iteration,
     * which {@link TypedNewInstance}'s {@code Map.copyOf} stabilises.
     */
    public static SqlExpr lower(TypedNewInstance n, LoweringContext ctx) {
        LinkedHashMap<String, SqlExpr> fields = new LinkedHashMap<>();
        for (Map.Entry<String, TypedSpec> e : n.values().entrySet()) {
            fields.put(e.getKey(), Lowerer.lowerScalar(e.getValue(), ctx));
        }
        return new SqlExpr.StructLiteral(fields);
    }

    /**
     * {@code struct.field} — field access on a struct value. When the struct
     * literal is visible at compile time ({@link TypedNewInstance}), we inline
     * the field's value directly; this is the dominant shape (Pure
     * {@code ^Class(name=…).name}) and avoids any dialect-level struct support.
     * Opaque struct values (e.g., produced by a sub-query) defer to a dialect
     * struct-extract — not yet modelled on {@link SqlExpr}, so surfaced as
     * {@code struct-extract:opaque} for visibility.
     */
    public static SqlExpr lower(TypedStructExtract n, LoweringContext ctx) {
        // Visible struct literal: inline the field's value directly. This is
        // the dominant shape (Pure {@code ^Class(name=…).name}).
        if (n.source() instanceof TypedNewInstance inst) {
            TypedSpec value = inst.values().get(n.field());
            if (value == null) {
                throw new PlanGenNotPortedException(n, "struct-extract",
                        "field '" + n.field() + "' not present on "
                        + inst.className() + " literal");
            }
            return Lowerer.lowerScalar(value, ctx);
        }
        // Property-access source: the compiler synthesises a struct-extract on
        // an association-typed property (e.g., {@code $p.firm.legalName}
        // parses as {@code .legalName} on a {@code TypedPropertyAccess} of
        // {@code $p.firm}). Unfold it into a deeper {@link TypedPropertyAccess}
        // with the extra hop appended to the association path so
        // {@link PropertyAccessLowering} can resolve it uniformly.
        //
        // NOTE: audited 2026-04; this branch fires ~325 times across the
        // engine test suite. The cleaner fix is to have the compiler emit a
        // flat TypedPropertyAccess(path=[firm, legalName]) directly; until
        // that lands, the rewrite stays.
        if (n.source() instanceof com.gs.legend.compiler.typed.TypedPropertyAccess tpa) {
            java.util.List<String> existing = tpa.associationPath()
                    .orElse(java.util.List.of(tpa.property()));
            java.util.ArrayList<String> extended = new java.util.ArrayList<>(existing);
            extended.add(n.field());
            var lifted = new com.gs.legend.compiler.typed.TypedPropertyAccess(
                    tpa.source(), n.field(),
                    java.util.Optional.of(java.util.List.copyOf(extended)),
                    n.info());
            return Lowerer.lowerScalar(lifted, ctx);
        }
        // Variable source bound as deferred HIR (e.g. {@code let p = ^Class(...)}
        // installed via {@link LoweringContext#bindRel} by the TypedBlock
        // lowering): unwrap to the bound node and recurse so the
        // TypedNewInstance branch above resolves the field. The let-binding
        // mechanism replaces the previous TypeChecker-time splice of the let
        // value's HIR into use sites.
        if (n.source() instanceof com.gs.legend.compiler.typed.TypedVariable v
                && ctx.lookupBinding(v.name()) instanceof LoweringContext.Rel rel) {
            return lower(new TypedStructExtract(rel.node(), n.field(), n.info()), ctx);
        }
        throw PlanGenNotPortedException.stage4(n);
    }
}
