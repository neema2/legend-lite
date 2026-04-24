package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.ResolvedMappings;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.sqlgen.SqlExpr;

import com.gs.legend.plan.lowering.relation.AssocJoinLifter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable-ish context threaded through every lowering rule.
 *
 * <p>Carries:
 * <ul>
 *   <li>{@code resolutions} — per-{@code TypedSpec} {@link StoreResolution} sidecar
 *       produced by {@code MappingResolver}. Accessed via {@link #storeFor(TypedSpec)}.</li>
 *   <li>{@code mode} — {@link PlanGenerator.Mode#SNAPSHOT} vs {@link PlanGenerator.Mode#STREAMING}.
 *       Only affects graph-fetch JSON envelope; relational lowering ignores it.</li>
 *   <li>{@code variableBindings} — lambda-parameter-name &rarr; resolved {@link SqlExpr}.
 *       Extended via {@link #withVar(String, SqlExpr)} when entering a lambda body.</li>
 *   <li>{@code aliases} — fresh-alias supplier (t0, t1, t2, …). The only mutable
 *       corner of the context; sharing a single counter across the whole plan
 *       gives a deterministic and SQL-friendly alias sequence.</li>
 * </ul>
 *
 * <p>Rules that introduce a new lambda binding call {@link #withVar(String, SqlExpr)}
 * and pass the new context down — no leakage across sibling subtrees.
 */
public final class LoweringContext {

    /**
     * Fresh-alias supplier. Shared by every context returned via {@link #withVar}
     * so aliases are globally unique inside a single plan.
     */
    public static final class AliasSupplier {
        private int counter;

        public String next() {
            return "t" + (counter++);
        }
    }

    private final ResolvedMappings mappings;
    private final PlanGenerator.Mode mode;
    private final Map<String, SqlExpr> variableBindings;
    private final StoreResolution currentStore;
    private final AliasSupplier aliases;
    /**
     * Non-embedded association-path bindings installed by
     * {@link AssocJoinLifter} before scalar lowering. Keyed by the path prefix
     * (e.g., {@code [firm]} for {@code $p.firm.legalName}), mapped to the
     * {@link SqlRelation}-level alias and the target {@link StoreResolution}.
     * Consumed by {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
     * when it walks a non-embedded hop.
     */
    private final Map<List<String>, AssocJoinLifter.Binding> assocBindings;

    private LoweringContext(ResolvedMappings mappings,
                            PlanGenerator.Mode mode,
                            Map<String, SqlExpr> variableBindings,
                            StoreResolution currentStore,
                            AliasSupplier aliases,
                            Map<List<String>, AssocJoinLifter.Binding> assocBindings) {
        this.mappings = mappings;
        this.mode = mode;
        this.variableBindings = variableBindings;
        this.currentStore = currentStore;
        this.aliases = aliases;
        this.assocBindings = assocBindings;
    }

    /** Fresh root context for a plan generation run. */
    public static LoweringContext root(ResolvedMappings mappings, PlanGenerator.Mode mode) {
        return new LoweringContext(mappings, mode, Map.of(), null,
                new AliasSupplier(), Map.of());
    }

    /** @return the full committed mapping sidecar produced by MappingResolver. */
    public ResolvedMappings mappings() { return mappings; }
    public PlanGenerator.Mode mode() { return mode; }
    public Map<String, SqlExpr> variableBindings() { return variableBindings; }

    /**
     * Store resolution currently "in scope" for lowering a scalar sub-expression
     * (e.g., the active store when evaluating a filter predicate, sort key, or
     * project lambda). Relational operator rules install this via {@link #withStore}
     * before recursing into their scalar lambdas.
     */
    public StoreResolution currentStore() { return currentStore; }

    /** @return next alias (t0, t1, …). Shared with child contexts. */
    public String nextAlias() { return aliases.next(); }

    /** @return resolved store for this typed node, or null if this node is not relational. */
    public StoreResolution storeFor(TypedSpec node) { return mappings.storeResolutions().get(node); }

    /** @return binding for a lambda parameter, or null if not in scope. */
    public SqlExpr lookupVar(String name) { return variableBindings.get(name); }

    /** Returns a new context with {@code name} bound to {@code value}. */
    public LoweringContext withVar(String name, SqlExpr value) {
        Map<String, SqlExpr> next = new HashMap<>(variableBindings);
        next.put(name, value);
        return new LoweringContext(mappings, mode, Map.copyOf(next), currentStore, aliases, assocBindings);
    }

    /** Returns a new context with all the given bindings added. */
    public LoweringContext withVars(Map<String, SqlExpr> more) {
        Map<String, SqlExpr> next = new HashMap<>(variableBindings);
        next.putAll(more);
        return new LoweringContext(mappings, mode, Map.copyOf(next), currentStore, aliases, assocBindings);
    }

    /** Returns a new context with the given store as the "currently active" one. */
    public LoweringContext withStore(StoreResolution store) {
        return new LoweringContext(mappings, mode, variableBindings, store, aliases, assocBindings);
    }

    /** @return the active association-path → alias+store map (never null; may be empty). */
    public Map<List<String>, AssocJoinLifter.Binding> assocBindings() { return assocBindings; }

    /**
     * Returns a new context carrying {@code bindings} as the active
     * association-path map. Installed by single-param relational rules (Filter
     * / Project / GroupBy / Sort) after {@link AssocJoinLifter#lift}.
     */
    public LoweringContext withAssocBindings(Map<List<String>, AssocJoinLifter.Binding> bindings) {
        return new LoweringContext(mappings, mode, variableBindings, currentStore, aliases,
                bindings == null ? Map.of() : Map.copyOf(bindings));
    }
}
