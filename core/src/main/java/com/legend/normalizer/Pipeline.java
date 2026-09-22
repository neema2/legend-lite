// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.model.JoinChainElement;
import com.legend.protocol.spec.ValueSpecification;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * Pipeline state: accumulated AST plus aliases for hops that have
 * been emitted. Each alias corresponds to a sub-row (clean
 * {@code join} slot) or a class-instance slot ({@code legacyNavigate}).
 */
final class Pipeline {
    ValueSpecification expr;
    final Map<String, String> aliasToTargetTable = new LinkedHashMap<>();
    final Set<String> classSlots = new HashSet<>();
    // Structural identity of each emitted physical sub-row hop: the
    // ordered list of join names from the main table maps to the slot
    // name actually used in the pipeline. This is the dedup key (so a
    // chain [A, B] never collides with a single join literally named
    // "A__B") AND the lookup readers use to recover the slot name,
    // rather than re-flattening the hop list (which is lossy).
    final Map<List<String>, String> pathToSlot = new LinkedHashMap<>();
    /** Class-typed navigate slots: property name -> MINTED alias (differs
     * when the property name collides with a physical main-table column
     * — the milestoningmap 'exchange' case). */
    final Map<String, String> navSlotByProp = new LinkedHashMap<>();
    /** The Inline embedded set ids being spliced right now, outermost first
     * — the one cycle an embedded materialization can have (set a splices
     * set b splices set a); a set already on the stack is loud
     * (audit 2026-09-15 P0-3: the old class-keyed guard got a fresh set
     * from every caller and recursed to a StackOverflowError). */
    final Set<String> inlineStack = new java.util.LinkedHashSet<>();

    /** Nav-slot OWNER class (property name -> owning class FQN): the
     * collision guards fire only across DIFFERENT owners — same-owner
     * same-name routed siblings dedup into one routed navigate
     * (ledger cluster 66). */
    final Map<String, String> navSlotOwner = new LinkedHashMap<>();
    // Physical (non-class) target tables reached by MORE THAN ONE distinct
    // sub-row slot and which are NOT the main table. A bare column ref to
    // such a table (in a filter/expression/groupBy/column PM) cannot
    // identify which sub-row is meant, so it is left unbound in the row
    // scope and a read fails loudly (see columnRead / translateRelOp)
    // instead of silently resolving to an arbitrary sub-row. Pin the
    // intended sub-row with a join-terminal column (| T.COL) instead.
    final Set<String> ambiguousTables = new HashSet<>();
    /** Routed class-typed navigations: property -> per-PM route entries
     * (target union-member ordinal + join), classified from each PM's
     * OWN {@code Join.targetSetId} (audit 11: the name-keyed map lost
     * same-named duplicates). ONE navigate per property emits the OR
     * over ALL entries, each target-side member-suffixed
     * ({@code FirmID_1}) so exactly the routed members' threads match. */
    final Map<String, List<UnionSynthesis.UnionRoute>> unionRoutes = new LinkedHashMap<>();
    /** Routed properties DROPPED from this synthesis (unresolvable or
     * unsupported route shape — reason on the poison ledger). Their PMs
     * emit nothing and bind no field; demand fails loudly. */
    final Set<String> droppedRoutedProps = new HashSet<>();
    /** The per-MAPPING ledger this synthesis records into (poisons,
     * mixed unions, key threads, the nullable census) and the graph-wide
     * mapped-class fact it reads — Phase E's own state, stamped on the
     * compiled mapping, never written into the model index. Null on a
     * VIEW pipeline: a view emits physical hops only and records
     * nothing; {@link #ledger()} is loud if that ever changes. */
    private final @com.legend.base.Nullable MappingLedger ledgerOrNull;

    Pipeline(ValueSpecification expr, MappingLedger ledger) {
        this(expr, ledger, false);
    }

    private Pipeline(ValueSpecification expr, @com.legend.base.Nullable MappingLedger ledger,
            boolean view) {
        this.expr = expr;
        this.ledgerOrNull = view ? null : java.util.Objects.requireNonNull(ledger, "ledger");
    }

    /** A VIEW's relation pipeline: physical hops only, no ledger. */
    static Pipeline forView(ValueSpecification expr) {
        return new Pipeline(expr, null, true);
    }

    MappingLedger ledger() {
        if (ledgerOrNull == null) {
            throw new IllegalStateException(
                    "a view pipeline carries no mapping ledger (it emits physical hops only)");
        }
        return ledgerOrNull;
    }

    /** The translator-facing view of this pipeline (seam b). */
    RelOpTranslator.PipelineView view() {
        return new RelOpTranslator.PipelineView() {
            @Override public Set<String> ambiguousTables() {
                return ambiguousTables;
            }
            @Override public boolean hasSlots() {
                return true;
            }
            @Override public String slotFor(List<JoinChainElement> chain) {
                return JoinChainEmission.slotFor(Pipeline.this, chain);
            }
            @Override public @com.legend.base.Nullable String targetTable(
                    @com.legend.base.Nullable String alias) {
                return aliasToTargetTable.get(alias);
            }
            @Override public boolean targetHasColumn(
                    @com.legend.base.Nullable String alias, String column) {
                for (String c : aliasToTargetColumns.getOrDefault(alias, Set.of())) {
                    if (c.equalsIgnoreCase(column)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    /** Column names of each hoisted chain slot's TARGET relation (table
     * or view), recorded beside {@link #aliasToTargetTable} — the
     * terminal-column rebase reads them. */
    final Map<String, Set<String>> aliasToTargetColumns = new LinkedHashMap<>();
}
