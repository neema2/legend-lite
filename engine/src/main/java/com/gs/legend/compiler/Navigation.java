package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.TypedSpec;

import java.util.List;

/**
 * A committed physical-navigation shape attached to a lambda-bearing relational
 * op by {@link MappingResolver}. Replaces the "latent navigation" that used to
 * live as an {@code associationPath} on every {@code TypedPropertyAccess}.
 *
 * <p>MappingResolver classifies each prefix navigation to exactly one
 * variant, emits it into {@link ResolvedMappings#navigations}, and binds
 * every access's terminal property to the resulting {@code abstractAlias}
 * via {@link ResolvedMappings#accessBindings}. PlanGenerator reads the list
 * and installs each navigation via a sealed {@code switch} — no store / context
 * / multiplicity branching at lowering time.
 *
 * <h3>Variants</h3>
 * <ul>
 *   <li>{@link JoinNav} — to-one navigation (any context); to-many when the
 *       result flows into an aggregate or a projected position where outer
 *       grouping can absorb row inflation. Renders as {@code LEFT JOIN target
 *       ON condition}, binds a new alias on the enclosing relation.</li>
 *   <li>{@link ExistsNav} — to-many navigation used only inside a filter
 *       predicate as an existence test. Renders as a correlated
 *       {@code EXISTS (SELECT 1 FROM target WHERE condition AND <inner>)}.
 *       Reserved; not emitted in v1.</li>
 *   <li>{@link SubqueryNav} — to-many producing a scalar in a position that
 *       can't be re-grouped. Renders as a correlated scalar subquery in place.
 *       Reserved; not emitted in v1.</li>
 *   <li>{@link LateralNav} — reserved for AsOfJoin's {@code ORDER BY ... LIMIT
 *       1} correlated form and top-N-per-group patterns. Not emitted in v1;
 *       PlanGenerator throws if it encounters one.</li>
 * </ul>
 *
 * <p>All variants carry a shared {@code prefix} (path prefix this nav resolves)
 * and {@code abstractAlias} (MR-assigned token that PlanGen maps to a real SQL
 * alias via {@code variableBindings}).
 */
public sealed interface Navigation permits Navigation.JoinNav,
        Navigation.ExistsNav, Navigation.SubqueryNav, Navigation.LateralNav {

    /** Path prefix this navigation resolves (e.g., {@code ["firm"]} for {@code $p.firm.name}). */
    List<String> prefix();

    /** MR-assigned abstract alias token (e.g., {@code "$__nav_0"}) bound to a real SQL alias by PlanGen. */
    String abstractAlias();

    /**
     * A physical LEFT JOIN. Every field except {@code prefix} / {@code abstractAlias}
     * comes directly from a {@link StoreResolution.JoinResolution} that MR
     * consulted while walking the navigation.
     *
     * @param prefix           Path prefix this nav resolves.
     * @param abstractAlias    MR-assigned token (e.g., {@code "$__nav_0"}).
     * @param targetTable      Physical table on the right-hand side of the JOIN.
     * @param sourceParam      Variable name bound to the source row when lowering {@link #condition}.
     * @param targetParam      Variable name bound to the target row when lowering {@link #condition}.
     * @param condition        2-param typed join predicate body.
     * @param targetResolution Store resolution for the target table (for onward navigation).
     * @param isToMany         Informational — shape is already committed.
     */
    record JoinNav(
            List<String> prefix,
            String abstractAlias,
            String targetTable,
            String sourceParam,
            String targetParam,
            TypedSpec condition,
            StoreResolution targetResolution,
            boolean isToMany) implements Navigation { }

    /**
     * Reserved for {@code filter(... exists ...)} patterns. Step 6 fills in the
     * field set (target, condition, inner predicate source).
     */
    record ExistsNav(
            List<String> prefix,
            String abstractAlias) implements Navigation { }

    /**
     * Reserved for to-many-in-scalar-position fallback. Step 7 fills in the
     * field set (target, aggregate expression).
     */
    record SubqueryNav(
            List<String> prefix,
            String abstractAlias) implements Navigation { }

    /**
     * Reserved for AsOfJoin / top-N-per-group. Not emitted in v1; PlanGenerator
     * throws on encounter.
     */
    record LateralNav(
            List<String> prefix,
            String abstractAlias) implements Navigation { }
}
