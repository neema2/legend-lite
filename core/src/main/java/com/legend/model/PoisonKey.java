// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

/**
 * WHAT a normalization poison is recorded against (audit 2026-09-15 P1-1).
 * The ledger used to be one string map with three key grammars — a class
 * FQN, {@code class[setId]}, an association FQN — and the per-SET key had no
 * reader, so a non-root set's recorded reason was unreachable. A sealed key
 * makes the reader compose the same key the writer did, or not compile.
 *
 * <ul>
 *   <li>{@link ForClass}: the class's ROOT binding is withheld (a user-model
 *       error the engine rejects, a roadmap gap, a multi-set class without a
 *       union root);</li>
 *   <li>{@link ForSet}: ONE non-root set of a multi-set class is withheld —
 *       the per-set fault isolation arm;</li>
 *   <li>{@link ForAssociation}: the association's join synthesis is withheld
 *       (the class bindings stay queryable);</li>
 *   <li>{@link ForAssociationEnd}: ONE direction of an association is
 *       refused at planning -- its property mapping names a target set id
 *       no set has (legend-engine refuses that direction at planning and
 *       plans the other: pureToSQLQuery.pure's doJoinToClass, F-U2,
 *       2026-11-14).</li>
 * </ul>
 */
public sealed interface PoisonKey {

    record ForClass(String classFqn) implements PoisonKey {
        @Override public String toString() { return classFqn; }
    }

    record ForSet(String classFqn, String setId) implements PoisonKey {
        @Override public String toString() { return classFqn + "[" + setId + "]"; }
    }

    record ForAssociation(String associationFqn) implements PoisonKey {
        @Override public String toString() { return associationFqn; }
    }

    /** The direction that navigates {@code propertyName} of the association. */
    record ForAssociationEnd(String associationFqn, String propertyName) implements PoisonKey {
        @Override public String toString() { return associationFqn + "." + propertyName; }
    }
}
