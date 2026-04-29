package com.gs.legend.plan.lowering;

import com.gs.legend.compiler.Navigation;
import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.StoreResolution.JoinResolution;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-relational-rule scratch pad where {@link com.gs.legend.plan.lowering.scalar.PropertyAccessLowering}
 * registers non-embedded association hops discovered while lowering a scalar
 * sub-expression. At rule exit, the rule hands the accumulated list to
 * {@link Relations#install} to weave the physical LEFT JOINs onto the source.
 *
 * <p>Lifecycle is strictly lexical: a fresh {@code NavScope} is created when a
 * lambda-bearing relational rule (Filter / Project / Sort / GroupBy / Aggregate
 * / Extend) begins; populated during scalar recursion via {@link #navigate};
 * consumed once via {@link #toList} when the rule installs its JOINs. Not
 * shared across rules; not global.
 *
 * <p>Nav discovery happens naturally during the same scalar recursion that
 * emits the {@link com.gs.legend.sqlgen.SqlExpr.Column} references — one walk,
 * one mechanism. (Replaces an earlier reflection-based pre-pass.)
 *
 * <p>Dedup by prefix: two property accesses with the same prefix
 * (e.g., both {@code $p.firm.name} and {@code $p.firm.city}) allocate a single
 * alias and a single {@link Navigation.JoinNav}. Longer prefixes chain off
 * shorter ones via {@link Navigation.JoinNav#abstractAlias()} — the source
 * side of a nested join is the parent prefix's alias, resolved via the
 * parent's {@link Navigation.JoinNav} (the token installed at the parent's
 * {@code prefix}).
 */
public final class NavScope {

    /** Registered navigation entry: the committed nav plus its parent's prefix (for chained lookups). */
    public record Entry(Navigation nav, List<String> parentPrefix, StoreResolution targetResolution) {}

    private final Map<List<String>, Entry> registry = new LinkedHashMap<>();

    /**
     * Register (or reuse) a navigation hop. Returns the abstract alias token for this hop's
     * target — always identical to the caller's rowAlias going forward for further hops.
     *
     * <p>Sealed switch on {@link JoinResolution}:
     * <ul>
     *   <li>{@link JoinResolution.FkJoin} → {@link Navigation.JoinNav}.</li>
     *   <li>{@link JoinResolution.StructArrayUnnest} → {@link Navigation.UnnestNav}.</li>
     *   <li>{@link JoinResolution.Embedded} must be advanced past by the caller; reaching
     *       this site with an Embedded resolution is a caller bug.</li>
     * </ul>
     *
     * @param prefix        Full path prefix after this hop (e.g., {@code [firm, owner]}).
     * @param parentPrefix  The prefix that precedes this hop (caller's "before" prefix).
     *                      Empty list means "root source".
     * @param jr            Resolved {@link JoinResolution} for this hop (must be non-embedded).
     * @param aliases       Fresh-alias supplier from the enclosing {@link LoweringContext}.
     */
    public String navigate(List<String> prefix, List<String> parentPrefix,
                           JoinResolution jr, LoweringContext.AliasSupplier aliases) {
        Entry existing = registry.get(prefix);
        if (existing != null) return existing.nav.abstractAlias();

        String alias = aliases.next();
        Navigation nav = switch (jr) {
            case JoinResolution.FkJoin fk -> new Navigation.JoinNav(
                    prefix, alias,
                    fk.targetTable(),
                    fk.sourceParam(), fk.targetParam(),
                    fk.joinCondition(),
                    fk.targetResolution(),
                    fk.isToMany());
            case JoinResolution.StructArrayUnnest u -> new Navigation.UnnestNav(
                    prefix, alias,
                    u.arrayProperty(),
                    u.targetResolution());
            case JoinResolution.Embedded ignored -> throw new IllegalStateException(
                    "NavScope.navigate: Embedded must be advanced past by caller, "
                            + "not registered as a nav hop (prefix=" + prefix + ")");
        };
        registry.put(prefix, new Entry(nav, parentPrefix, jr.targetResolution()));
        return alias;
    }

    /**
     * Look up an already-registered nav by prefix. Returns {@code null} if not registered.
     * Used by callers to resolve a parent-prefix's source alias.
     */
    public Entry lookup(List<String> prefix) {
        return registry.get(prefix);
    }

    /**
     * Returns the registered navigations in insertion order (shortest prefixes first
     * because callers register outer hops before inner ones during top-down recursion).
     * Suitable for hand-off to {@link Relations#install}.
     */
    public List<Navigation> toList() {
        return new ArrayList<>(registry.values().stream().map(Entry::nav).toList());
    }

    public boolean isEmpty() {
        return registry.isEmpty();
    }
}
