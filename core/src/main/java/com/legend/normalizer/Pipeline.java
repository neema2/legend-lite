// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.parser.element.JoinChainElement;
import com.legend.parser.spec.ValueSpecification;

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
    // Physical (non-class) target tables reached by MORE THAN ONE distinct
    // sub-row slot and which are NOT the main table. A bare column ref to
    // such a table (in a filter/expression/groupBy/column PM) cannot
    // identify which sub-row is meant, so it is left unbound in the row
    // scope and a read fails loudly (see columnRead / translateRelOp)
    // instead of silently resolving to an arbitrary sub-row. Pin the
    // intended sub-row with a join-terminal column (| T.COL) instead.
    final Set<String> ambiguousTables = new HashSet<>();
    // The VIEW this class's source pipeline materializes (null for plain
    // table-backed classes). Join conditions referencing THIS view's
    // columns may substitute the physical expressions even when the view
    // carries filter/distinct/groupBy — those row semantics already live
    // in the class pipeline; any OTHER non-plain view stays a wall.
    String backingView;
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
    Pipeline(ValueSpecification expr) { this.expr = expr; }

    /** The translator-facing view of this pipeline (seam b). */
    RelOpTranslator.PipelineView view() {
        return new RelOpTranslator.PipelineView() {
            @Override public java.util.Set<String> ambiguousTables() {
                return ambiguousTables;
            }
            @Override public boolean hasSlots() {
                return true;
            }
            @Override public String slotFor(java.util.List<JoinChainElement> chain) {
                return JoinChainEmission.slotFor(Pipeline.this, chain);
            }
            @Override public String targetTable(String alias) {
                return aliasToTargetTable.get(alias);
            }
        };
    }
}
