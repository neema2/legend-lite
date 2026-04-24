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
        if (!(n.source() instanceof TypedVariable v)) {
            throw PlanGenNotPortedException.stage3(n, "non-variable-source");
        }
        SqlExpr bound = ctx.lookupVar(v.name());
        if (!(bound instanceof SqlExpr.Identifier id)) {
            throw PlanGenNotPortedException.stage3(n, "property-on-column-binding");
        }
        String rowAlias = id.name();
        StoreResolution store = ctx.currentStore();

        // Association-path navigation: {@code associationPath} is the full chain
        // including the terminal property (e.g., {@code $p.firm.name} carries
        // {@code ["firm", "name"]}). The hops that need resolving against the
        // store's joins are everything BEFORE the terminal — the last element
        // is the property name already on {@link TypedPropertyAccess#property()}.
        //
        // Each hop is either:
        //   - embedded: no physical JOIN, same alias, advance resolution;
        //   - non-embedded: uses a LEFT JOIN pre-installed by
        //     {@link com.gs.legend.plan.lowering.relation.AssocJoinLifter}, whose
        //     bindings live on {@link LoweringContext#assocBindings}.
        //   - otherwise surfaces as {@code associationPath:non-embedded:…}.
        List<String> path = n.associationPath().orElse(List.of());
        int hopCount = Math.max(0, path.size() - 1);
        var bindings = ctx.assocBindings();
        for (int i = 0; i < hopCount; i++) {
            String assoc = path.get(i);
            if (store == null) {
                throw PlanGenNotPortedException.stage3(n, "associationPath:no-store");
            }
            JoinResolution jr = store.joins() == null ? null : store.joins().get(assoc);
            if (jr == null) {
                throw PlanGenNotPortedException.stage3(n, "associationPath:unresolved:" + assoc);
            }
            if (jr.embedded()) {
                store = jr.targetResolution();
                continue;
            }
            List<String> prefix = List.copyOf(path.subList(0, i + 1));
            var binding = bindings.get(prefix);
            if (binding == null) {
                throw PlanGenNotPortedException.stage3(n,
                        "associationPath:non-embedded:" + assoc);
            }
            rowAlias = binding.alias();
            store = binding.store();
        }
        return new SqlExpr.Column(rowAlias, physicalColumnFor(n.property(), store));
    }

    /**
     * @return the physical column name for {@code property}, using the active
     *         {@link StoreResolution} when available. Falls through to the raw
     *         property name only when no resolution is in scope (e.g., TDS
     *         literals and raw table refs, which have no mapping layer).
     */
    private static String physicalColumnFor(String property, StoreResolution store) {
        if (store == null) return property;
        String col = store.columnFor(property);
        return col != null ? col : property;
    }
}
