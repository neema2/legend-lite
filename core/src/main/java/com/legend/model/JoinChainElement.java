package com.legend.model;

import java.util.Objects;

/**
 * One hop in a join navigation chain like {@code @J1 > (LEFT) [DB2] @J2}.
 *
 * <p>Used by {@link RelationalOperation.JoinNavigation} to model mapping
 * property RHS such as {@code [DB]@PersonFirm > @FirmCountry | COUNTRY.NAME}.
 *
 * @param joinName       the join's simple name (e.g. {@code "PersonFirm"})
 * @param joinType       optional join type spelled inside parens (e.g.
 *                       {@code (LEFT)}, {@code (OUTER)}), canonicalised to
 *                       a {@link JoinType} value; {@code null} when the hop
 *                       carries no explicit annotation
 * @param databaseName   the enclosing database qualifier that applies to this
 *                       hop &mdash; either inherited from the enclosing
 *                       {@code Database} scope, or overridden by an inline
 *                       {@code [DB]} on the hop. Always populated.
 * @param includeSelf    reserved for the mapping-context {@code @self} marker;
 *                       always {@code false} in Database context
 */
public record JoinChainElement(
        String joinName,
        JoinType joinType,
        String databaseName,
        boolean includeSelf) {

    public JoinChainElement {
        Objects.requireNonNull(joinName, "Join name cannot be null");
    }
}
