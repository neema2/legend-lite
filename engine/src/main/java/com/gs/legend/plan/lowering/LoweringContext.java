package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.ResolvedMappings;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

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

    /**
     * Sealed lambda-parameter / let-binding entry. Two variants:
     *
     * <ul>
     *   <li>{@link Scalar} — the bound value is a scalar SQL expression
     *       (a row alias like {@code t0}, a literal, or a computed scalar).
     *       Carries the associated {@link StoreResolution} when the value
     *       represents a relational row, so {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
     *       can resolve {@code $p.prop} against the correct store.</li>
     *   <li>{@link Rel} — the bound value is itself a relational expression
     *       ({@code let r = SomeClass.all()->...}); the typed HIR is
     *       captured and re-lowered at every {@code $r} reference inside a
     *       relational position. In scalar position the binding is invisible
     *       (lookup falls through to identifier emission, mirroring legend-engine).</li>
     * </ul>
     *
     * <p>One sealed type, one binding map: replaces the previous trio of
     * {@code variableBindings} / {@code env} / {@code relBindings} maps
     * that routed bindings by value-kind across separate dictionaries.
     */
    public sealed interface VarBinding permits Scalar, Rel {}

    /** Scalar binding: SQL expression plus optional store for property resolution. */
    public record Scalar(SqlExpr expression, StoreResolution store) implements VarBinding {}

    /** Relational binding: typed HIR re-lowered in place at each reference. */
    public record Rel(TypedSpec node) implements VarBinding {}

    private final ResolvedMappings mappings;
    private final PlanGenerator.Mode mode;
    private final Map<String, VarBinding> env;
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
                            Map<String, VarBinding> env,
                            AliasSupplier aliases,
                            NavScope navScope) {
        this.mappings = mappings;
        this.mode = mode;
        this.env = env;
        this.aliases = aliases;
        this.navScope = navScope;
    }

    /** Fresh root context for a plan generation run. */
    public static LoweringContext root(ResolvedMappings mappings, PlanGenerator.Mode mode) {
        return new LoweringContext(mappings, mode, Map.of(), new AliasSupplier(), null);
    }

    /** @return the full committed mapping sidecar produced by MappingResolver. */
    public ResolvedMappings mappings() { return mappings; }
    public PlanGenerator.Mode mode() { return mode; }

    /** @return next alias (t0, t1, …). Shared with child contexts. */
    public String nextAlias() { return aliases.next(); }

    /** @return the underlying alias supplier (for helpers that need raw access). */
    public AliasSupplier aliases() { return aliases; }

    /** @return resolved store for this typed node, or null if this node is not relational. */
    public StoreResolution storeFor(TypedSpec node) { return mappings.storeResolutions().get(node); }

    /**
     * Scalar lookup. Returns the bound {@link SqlExpr} for a {@link Scalar}
     * binding, {@code null} otherwise (including when {@code name} is
     * {@link Rel}-bound — relational bindings are invisible in scalar
     * position so the caller can fall through to identifier emission).
     */
    public SqlExpr lookupVar(String name) {
        return env.get(name) instanceof Scalar s ? s.expression() : null;
    }

    /** @return raw binding (sealed variant) for {@code name}, or {@code null} if not in scope. */
    public VarBinding lookupBinding(String name) { return env.get(name); }

    /** @return the active nav-scope, or null if not inside a lambda-bearing relational rule. */
    public NavScope navScope() { return navScope; }

    /**
     * Bind a scalar value to {@code name}. The bound expression is what
     * {@code $name} resolves to in scalar position (lambda parameters,
     * scalar {@code let} statements). The optional {@link StoreResolution}
     * is non-null when the binding represents a relational row — it lets
     * {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
     * resolve {@code $name.prop} against the right store without an ambient
     * {@code currentStore} stack.
     */
    public LoweringContext bindVar(String name, SqlExpr value, StoreResolution store) {
        Map<String, VarBinding> next = new HashMap<>(env);
        next.put(name, new Scalar(value, store));
        return new LoweringContext(mappings, mode, Map.copyOf(next), aliases, navScope);
    }

    /**
     * Bind a typed HIR node as a relational value for {@code name}. Used by
     * block-lowering for {@code let r = <rel-expr>} statements. References
     * to {@code $r} in relational position re-lower the captured HIR; in
     * scalar position the binding is invisible (lookup falls through),
     * mirroring legend-engine.
     */
    public LoweringContext bindRel(String name, TypedSpec node) {
        Map<String, VarBinding> next = new HashMap<>(env);
        next.put(name, new Rel(node));
        return new LoweringContext(mappings, mode, Map.copyOf(next), aliases, navScope);
    }

    /**
     * Returns a new context carrying a fresh {@link NavScope}. Installed by
     * lambda-bearing relational rules (Filter / Project / Sort / GroupBy /
     * Aggregate / Extend) before descending into scalar bodies; consumed at
     * rule exit when the rule installs its collected navigations.
     */
    public LoweringContext withNavScope(NavScope scope) {
        return new LoweringContext(mappings, mode, env, aliases, scope);
    }

    /**
     * Type-erasing routing predicate: {@code true} when {@code n} should be
     * lowered as a relation, {@code false} when as a scalar.
     *
     * <p>Two clauses match a relational source:
     * <ul>
     *   <li>The expression's static type is {@link com.gs.legend.model.m3.Type.Relation}
     *       (TDS / table-typed expressions).</li>
     *   <li>{@link #storeFor(TypedSpec)} returns non-null — a {@link StoreResolution}
     *       was stamped, meaning the expression is rooted in a class table
     *       or class-collection literal.</li>
     * </ul>
     */
    public boolean isRelationalSource(TypedSpec n) {
        return n.info().type() instanceof com.gs.legend.model.m3.Type.Relation
                || storeFor(n) != null;
    }

    /**
     * Wrap a precomputed scalar {@link SqlExpr} as a one-row one-column
     * {@link SqlRelation.SourceExprRel}. {@code UNNEST(...)} is added when
     * {@code node.info().isMany()} so list values unfold into N rows.
     *
     * <p>Internal helper for {@link #toRelation(TypedSpec)}; rules that
     * need a scalar-as-relation should call {@code toRelation} (which
     * handles lowering + wrapping in one shot).
     */
    private SqlRelation wrapScalar(SqlExpr expr, TypedSpec node) {
        SqlExpr value = node.info().isMany()
                ? new SqlExpr.Unnest(expr)
                : expr;
        return new SqlRelation.SourceExprRel(
                value, nextAlias(),
                List.of(new SqlRelation.OutputCol(Lowerer.SCALAR_WRAP_COLUMN, null)));
    }

    /**
     * Single entry point for "produce a relation from any typed node":
     * relational sources go through {@link Lowerer#lowerRelation};
     * everything else lowers as scalar and wraps. No special-case rules.
     */
    public SqlRelation toRelation(TypedSpec n) {
        return isRelationalSource(n)
                ? Lowerer.lowerRelation(n, this)
                : wrapScalar(Lowerer.lowerScalar(n, this), n);
    }
}
