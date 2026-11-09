// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;

import java.util.List;
import java.util.Objects;

/**
 * HOW A SIDE'S ROWS ARE SHAPED ONCE READ, AS THE PLAN CARRIES IT (docs/PLAN_AS_DATA_2026_11_07.md 2.3, the design's
 * step 4, 2026-11-08): the facts the host judge's shaping reads, decided at planning from the side's declared type --
 * replacing the planned side's compiler types, its wrapped side and the constant's pre-answered value.
 *
 * @param kind             the five forms a side's rows are shaped as
 * @param typeName         the side's declared type, in the plan's spelling
 * @param columns          a grid's declared columns (a TABULAR side's, empty for a raw grid read by its reported
 *                         columns; a KEYED side's fixed columns); empty for the other kinds
 * @param templates        a KEYED side's per-key aggregate templates: a column whose name ends with the pivot separator
 *                         and a template's name takes that template's type; empty for the other kinds
 * @param oneCarrier       the side is ONE carrier in the engine's {@code Result.values} (a relation's or a TDS's type)
 * @param scalar           a VALUES side shaped as one value (else a collection); a CONSTANT side's value one value
 * @param lower            a VALUES side's declared lower bound
 * @param multiplicity     a VALUES side's declared multiplicity, as the compiler spells it in its messages
 * @param tdsNullRoot      a VALUES side whose type is the TDS null cell's class
 * @param nullableRoot     a VALUES side whose type admits a null element (Any, a variant, the TDS null class)
 * @param listCarrier      a VALUES side whose type is a list carrier (a List instance holds its list as one value)
 */
public record SideShape(Kind kind, String typeName, List<Column> columns, List<Column> templates, boolean oneCarrier,
        boolean scalar, long lower, String multiplicity, boolean tdsNullRoot, boolean nullableRoot,
        boolean listCarrier) {

    public SideShape {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(multiplicity, "multiplicity");
        columns = List.copyOf(columns);
        templates = List.copyOf(templates);
    }

    /** The five forms a side's rows are shaped as. */
    public enum Kind { CONSTANT, GRAPH, TABULAR, KEYED, VALUES }

    /** A declared column: its name, its type in the plan's spelling, its multiplicity where the type declares one. */
    public record Column(String name, String type, ExecutionPlan.@Nullable Multiplicity multiplicity) {
        public Column {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }
    }
}
