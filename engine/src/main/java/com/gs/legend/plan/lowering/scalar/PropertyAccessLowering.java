package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.StoreResolution.JoinResolution;
import com.gs.legend.compiler.typed.TypedPropertyAccess;
import com.gs.legend.compiler.typed.TypedVariable;
import com.gs.legend.plan.PlanGenNotPortedException;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.List;

/**
 * Lowers a {@link TypedPropertyAccess} (Pure {@code $x.property}) into a SQL
 * {@link SqlExpr.Column} reference.
 *
 * <p><strong>Stage 3 scope</strong>: the common lambda-parameter access pattern —
 * {@code $row.col} inside a filter / sort / project / extend lambda. The owning
 * relational rule binds the lambda parameter to a {@link SqlExpr.Identifier}
 * carrying the source alias and installs the active {@link StoreResolution} on
 * the context; this rule resolves the property → physical column name via that
 * resolution (or falls through to the raw property name when no mapping is
 * declared, e.g., a TDS-backed source).
 *
 * <p>Association-path navigation, nested property access and M2M DynaFunction
 * expansion are out of scope for Stage 3 and surface as
 * {@link PlanGenNotPortedException} with the {@code stage-3-propaccess} tag.
 */
public final class PropertyAccessLowering {
    private PropertyAccessLowering() {}

    public static SqlExpr lower(TypedPropertyAccess n, LoweringContext ctx) {
        // Non-variable source: property access on an arbitrary scalar expression
        // (e.g., {@code foo(x).bar}, chained {@code $p.address.city} where the
        // inner hop produces another TypedPropertyAccess). Lower the source
        // recursively as a scalar and emit a SQL field access.
        //
        // Ported from legacy {@code generateScalar} AppliedProperty branch
        // (line 2527-2533 of plangen-legacy-pre-port.java.txt) — "struct field
        // access in lambda". Legacy emitted {@code FieldAccess(Identifier(owner), property)}
        // for {@code $f.legalName}; we generalise to any scalar source.
        if (!(n.source() instanceof TypedVariable v)) {
            SqlExpr base = com.gs.legend.plan.lowering.Lowerer.lowerScalar(n.source(), ctx);
            return new SqlExpr.FieldAccess(base, n.property());
        }
        LoweringContext.VarBinding binding = ctx.lookupBinding(v.name());
        if (!(binding instanceof LoweringContext.Scalar scalar)) {
            // Property access requires a scalar row binding. {@code null}
            // (unbound) and {@link LoweringContext.Rel} (let-bound relational
            // expression) are both invalid here — you can't take {@code .prop}
            // off a relation, only off a row.
            throw PlanGenNotPortedException.stage3(n, "property:unbound-variable:" + v.name());
        }
        if (!(scalar.expression() instanceof SqlExpr.Identifier id)) {
            throw PlanGenNotPortedException.stage3(n, "property-on-column-binding");
        }
        String rowAlias = id.name();
        StoreResolution store = scalar.store();

        List<String> path = n.associationPath().orElse(List.of());
        int hopCount = Math.max(0, path.size() - 1);
        List<String> parentPrefix = List.of();
        for (int i = 0; i < hopCount; i++) {
            String assoc = path.get(i);
            if (store == null) {
                throw PlanGenNotPortedException.stage3(n, "associationPath:no-store");
            }
            JoinResolution jr = store.joins() == null ? null : store.joins().get(assoc);
            if (jr == null) {
                throw PlanGenNotPortedException.stage3(n, "associationPath:unresolved:" + assoc);
            }
            List<String> prefix = List.copyOf(path.subList(0, i + 1));
            if (jr.embedded()) {
                store = jr.targetResolution();
                // parentPrefix advances so that any following non-embedded hop
                // records its parent correctly even across embedded segments.
                parentPrefix = prefix;
                continue;
            }
            if (ctx.navScope() == null) {
                throw PlanGenNotPortedException.stage3(n,
                        "associationPath:non-embedded-without-navscope:" + assoc);
            }
            rowAlias = ctx.navScope().navigate(prefix, parentPrefix, jr, ctx.aliases());
            store = jr.targetResolution();
            parentPrefix = prefix;
        }
        return new SqlExpr.Column(rowAlias, physicalColumnFor(n.property(), store));
    }

    /**
     * @return the physical column name for {@code property}, using the active
     *         {@link StoreResolution} when available. Falls through to the raw
     *         property name when no resolution is in scope or the property
     *         isn't mapped — matching legacy behavior.
     *
     * <p><b>The fallback is a known smell.</b> The codebase currently
     * synthesizes {@link TypedPropertyAccess} nodes carrying physical column
     * names directly (via enum-mapping + association-join synthesis paths in
     * {@code MappingNormalizer}); the fallback returns those untouched so the
     * SQL emits correctly. Tightening to throw on missing-property requires
     * fixing every upstream leak first — tracked as Phase C.2.
     */
    private static String physicalColumnFor(String property, StoreResolution store) {
        if (store == null) return property;
        String col = store.columnFor(property);
        return col != null ? col : property;
    }
}
