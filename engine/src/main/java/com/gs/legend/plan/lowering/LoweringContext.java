package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.ResolvedMappings;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.HashMap;
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

    /**
     * Variable binding: the resolved {@link SqlExpr} that a lambda param name
     * resolves to, together with the {@link StoreResolution} associated with
     * that binding (non-null when the variable's value is a relational row).
     *
     * <p>Bundling the store with the binding replaces the ambient {@code currentStore}
     * pattern — each variable carries its own store, so multi-param lambdas
     * (e.g., join conditions) resolve property accesses against the correct
     * side's store.
     */
    public record VarBinding(SqlExpr expression, StoreResolution store) {}

    private final ResolvedMappings mappings;
    private final PlanGenerator.Mode mode;
    private final Map<String, SqlExpr> variableBindings;           // legacy surface (expression only)
    private final Map<String, VarBinding> env;                     // new surface (expression + store)
    private final AliasSupplier aliases;
    /**
     * Per-relational-rule scratch pad for collecting non-embedded association
     * hops encountered during scalar lowering. {@code null} outside a
     * lambda-bearing relational rule. Created fresh by the rule, consumed
     * at rule exit via {@link Relations#install}.
     */
    private final NavScope navScope;

    private LoweringContext(ResolvedMappings mappings,
                            PlanGenerator.Mode mode,
                            Map<String, SqlExpr> variableBindings,
                            Map<String, VarBinding> env,
                            AliasSupplier aliases,
                            NavScope navScope) {
        this.mappings = mappings;
        this.mode = mode;
        this.variableBindings = variableBindings;
        this.env = env;
        this.aliases = aliases;
        this.navScope = navScope;
    }

    /** Fresh root context for a plan generation run. */
    public static LoweringContext root(ResolvedMappings mappings, PlanGenerator.Mode mode) {
        return new LoweringContext(mappings, mode, Map.of(), Map.of(),
                new AliasSupplier(), null);
    }

    /** @return the full committed mapping sidecar produced by MappingResolver. */
    public ResolvedMappings mappings() { return mappings; }
    public PlanGenerator.Mode mode() { return mode; }
    public Map<String, SqlExpr> variableBindings() { return variableBindings; }

    /** @return next alias (t0, t1, …). Shared with child contexts. */
    public String nextAlias() { return aliases.next(); }

    /** @return the underlying alias supplier (for helpers that need raw access). */
    public AliasSupplier aliases() { return aliases; }

    /** @return resolved store for this typed node, or null if this node is not relational. */
    public StoreResolution storeFor(TypedSpec node) { return mappings.storeResolutions().get(node); }

    /** @return binding for a lambda parameter, or null if not in scope. */
    public SqlExpr lookupVar(String name) { return variableBindings.get(name); }

    /** @return rich binding (expression + store) for a lambda parameter, or null if not bound via new surface. */
    public VarBinding lookupBinding(String name) { return env.get(name); }

    /** @return the active nav-scope, or null if not inside a lambda-bearing relational rule. */
    public NavScope navScope() { return navScope; }

    /**
     * Rich variable binding: records both {@code expression} and the
     * associated {@link StoreResolution}. The primary means of binding a
     * lambda parameter — {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
     * reads the bound store directly from the {@link VarBinding}, replacing
     * the ambient {@code currentStore} pattern.
     *
     * <p>Also mirrors into the legacy {@code variableBindings} map so
     * {@link com.gs.legend.plan.lowering.scalar.VariableLowering} can resolve
     * plain {@code $name} references without knowing about the env.
     */
    public LoweringContext bindVar(String name, SqlExpr value, StoreResolution store) {
        Map<String, SqlExpr> legacy = new HashMap<>(variableBindings);
        legacy.put(name, value);
        Map<String, VarBinding> nextEnv = new HashMap<>(env);
        nextEnv.put(name, new VarBinding(value, store));
        return new LoweringContext(mappings, mode, Map.copyOf(legacy), Map.copyOf(nextEnv),
                aliases, navScope);
    }

    /**
     * Returns a new context carrying a fresh {@link NavScope}. Installed by
     * lambda-bearing relational rules (Filter / Project / Sort / GroupBy /
     * Aggregate / Extend) before descending into scalar bodies; consumed at
     * rule exit when the rule installs its collected navigations.
     */
    public LoweringContext withNavScope(NavScope scope) {
        return new LoweringContext(mappings, mode, variableBindings, env,
                aliases, scope);
    }
}
