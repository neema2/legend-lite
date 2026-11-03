// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.executionplan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A KEYED plan's steps that are not statements (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D, the user's D18,
 * 2026-11-03): the keys statement answers each key's text and the names it gives; this writes the body for those keys
 * and says which columns the answer has, in what order, under what names each statement's cells are read. A
 * {@link ExecutionPlan.Keyed} is carriage -- the runner writes only text the plan carries and the keys' positions,
 * and every value and every name is the database's -- so its steps are the plan record's, one function the JVM's
 * runner ({@code exec/PlanRunner}) and a host's boundary both call, not a runner's own. PURE: the record and the
 * keys' texts alone.
 */
public final class KeyedRun {
    private KeyedRun() {
    }

    /** A keys row's cell {@code cell}: the text the keys statement wrote there (a key's text, or a name it gives). */
    public static String cellText(List<?> key, int cell) {
        if (cell >= key.size() || !(key.get(cell) instanceof String s)) {
            throw new IllegalStateException("a keys row's cell " + cell + " is " + (cell < key.size() ? key.get(cell)
                    : "absent") + ": the probe writes each key's text and names");
        }
        return s;
    }

    /**
     * The answer to {@code keyed} for {@code keys}: its {@code columns} in the relation's own order (the first
     * {@code perKeyAt} fixed columns, the per-key ones key by key, then the rest of the fixed ones; a per-key column
     * named by its key's cell); {@code read}, the body's columns in the body's order (its fixed columns first), each
     * under the name the body writes it ({@code _pk<key>_<field>} for a per-key one); and {@code place}, where each
     * body column's cell goes in the answer. A body of more columns than the database answers is refused by name
     * before it is written.
     */
    public record Answer(List<ExecutionPlan.Column> columns, List<ExecutionPlan.Column> read, List<Integer> place) {
        public Answer {
            columns = List.copyOf(columns);
            read = List.copyOf(read);
            place = List.copyOf(place);
        }
    }

    /** {@link Answer} of {@code keyed} for {@code keys} (each a keys row: its texts and names). */
    public static Answer answer(ExecutionPlan.Keyed keyed, List<? extends List<?>> keys) {
        int perKey = (int) keyed.outputs().stream().filter(o -> o instanceof ExecutionPlan.Keyed.PerKey).count();
        int fixed = keyed.outputs().size() - perKey;
        int columns = fixed + keys.size() * perKey;
        if (keyed.maxColumns() != null && columns > keyed.maxColumns()) {
            throw new com.legend.error.NotImplementedException("a dynamic pivot of " + keys.size() + " keys answers "
                    + columns + " columns, more than the database's " + keyed.maxColumns());
        }
        List<ExecutionPlan.Column> answered = new ArrayList<>(columns);
        List<ExecutionPlan.Column> read = new ArrayList<>(columns);
        for (ExecutionPlan.Keyed.Output o : keyed.outputs()) {
            if (o instanceof ExecutionPlan.Keyed.Fixed f) {
                answered.add(f.column());
                read.add(f.column().named(f.field()));
            }
        }
        for (int k = 0; k < keys.size(); k++) {
            for (ExecutionPlan.Keyed.Output o : keyed.outputs()) {
                if (o instanceof ExecutionPlan.Keyed.PerKey p) {
                    ExecutionPlan.Column a = p.column().named(cellText(keys.get(k), p.name()));
                    answered.add(a);
                    read.add(a.named("_pk" + k + "_" + p.field()));
                }
            }
        }
        // the answer in the relation's own order: the first perKeyAt fixed columns, the per-key ones, then the rest of
        // the fixed ones -- read in the body's order (its fixed columns first), each cell placed
        int at = keyed.perKeyAt();
        List<Integer> place = new ArrayList<>(columns);
        for (int i = 0; i < columns; i++) {
            place.add(i < at ? i : i < fixed ? i + (columns - fixed) : i - (fixed - at));
        }
        List<ExecutionPlan.Column> inOrder = new ArrayList<>(Collections.nCopies(columns, (ExecutionPlan.Column) null));
        for (int i = 0; i < columns; i++) {
            inOrder.set(place.get(i), answered.get(i));
        }
        return new Answer(inOrder, read, place);
    }

    /** One placeholder of an expanded statement, in placeholder order: the plan's slot {@code index}'s value, or a
     *  key's text (bound as text, VARCHAR). */
    public sealed interface Bound permits Bound.Slot, Bound.Key {
        /** The plan's slot {@code index} (of the statement's {@link ExecutionPlan.Sql#slots}), bound as the slot says. */
        record Slot(int index) implements Bound {
        }

        /** A key's text, bound as text. */
        record Key(String text) implements Bound {
        }
    }

    /** A statement written for its keys: its text, and what each of its placeholders binds, in order. */
    public record Expanded(String statement, List<Bound> bound) {
        public Expanded {
            bound = List.copyOf(bound);
        }
    }

    /**
     * {@code sql} written for {@code keys}: the text outside its regions as it is, each type hole filled with
     * {@code holeType} of its slot (the slot's index; called only for a slot with a hole, each outside every region);
     * each region once per key ({@code lead} before the first copy, {@code separator} between copies, the key's
     * position at each counter; with no key, {@code empty}, or the statement refused where it has no form without
     * one); each slot outside a region bound once, each inside one bound in every copy, a key's cell to that key's text.
     */
    public static Expanded expand(ExecutionPlan.Sql sql, List<? extends List<?>> keys,
            java.util.function.IntFunction<String> holeType) {
        String text = sql.statement();
        // the type holes, each outside every region (the writer refuses one inside): their places and types, in order
        List<Integer> holes = new ArrayList<>();
        List<String> holeTypes = new ArrayList<>();
        for (int i = 0; i < sql.slots().size(); i++) {
            ExecutionPlan.Slot each = sql.slots().get(i);
            if (each.binding() instanceof ExecutionPlan.Binding.One one && one.hole() != null) {
                holes.add(one.hole().at());
                holeTypes.add(holeType.apply(i));
            }
        }
        StringBuilder out = new StringBuilder(text.length() + keys.size() * 64);
        List<Bound> bound = new ArrayList<>();
        int[] hole = {0};
        int from = 0;
        int slot = 0;
        for (ExecutionPlan.Region r : sql.regions()) {
            outside(out, text, from, r.start(), holes, holeTypes, hole);
            for (; slot < r.firstSlot(); slot++) {
                bound.add(new Bound.Slot(slot));
            }
            if (keys.isEmpty()) {
                if (r.empty() == null) {
                    throw new com.legend.error.NotImplementedException("a dynamic pivot with no fixed column found no key: its"
                            + " relation has no column");
                }
                out.append(r.empty());
            } else {
                out.append(r.lead());
                for (int k = 0; k < keys.size(); k++) {
                    if (k > 0) {
                        out.append(r.separator());
                    }
                    int at = r.start();
                    for (int counter : r.counters()) {
                        out.append(text, at, counter).append(k);
                        at = counter;
                    }
                    out.append(text, at, r.end());
                    for (int s = r.firstSlot(); s < r.firstSlot() + r.slotCount(); s++) {
                        if (sql.slots().get(s).binding() instanceof ExecutionPlan.Binding.Key key) {
                            bound.add(new Bound.Key(cellText(keys.get(k), key.cell())));
                        } else {
                            bound.add(new Bound.Slot(s));
                        }
                    }
                }
            }
            slot = r.firstSlot() + r.slotCount();
            from = r.end();
        }
        outside(out, text, from, text.length(), holes, holeTypes, hole);
        for (; slot < sql.slots().size(); slot++) {
            bound.add(new Bound.Slot(slot));
        }
        return new Expanded(out.toString(), bound);
    }

    /** The statement's text {@code [from, to)} outside its regions, each type hole up to {@code to} filled (one at
     *  {@code to} itself before the region that starts there); {@code next} the first hole not yet written. */
    private static void outside(StringBuilder out, String text, int from, int to, List<Integer> holes,
            List<String> holeTypes, int[] next) {
        int at = from;
        while (next[0] < holes.size() && holes.get(next[0]) <= to) {
            int place = holes.get(next[0]);
            out.append(text, at, place).append(holeTypes.get(next[0]));
            at = place;
            next[0]++;
        }
        out.append(text, at, to);
    }
}
