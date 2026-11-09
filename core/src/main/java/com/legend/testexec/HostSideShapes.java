// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;


import com.legend.exec.PlanRunner;
import com.legend.testexecutionplan.TypedSide;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class HostSideShapes {

    private HostSideShapes() {
    }


    /** The rows the runner read for a verdict's sides, each shaped by the plan's shape at its position (the plan-as-data
     *  design's step 4, 2026-11-08); the base's alone where the verdict carries a caller's message apart -- the
     *  rows one fewer than the shapes, the message {@link #message}'s to read. */
    static List<TypedSide> shapedBase(String name, List<com.legend.testexecutionplan.SideShape> shapes,
            List<PlanRunner.Rows> rows) {
        if (rows.size() != shapes.size() && rows.size() != shapes.size() - 1) {
            throw new IllegalStateException(name + ": the runner handed " + rows.size() + " sides for "
                    + shapes.size() + " planned");
        }
        List<TypedSide> out = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            out.add(com.legend.testexec.SideShapes.shape(shapes.get(i), rows.get(i)));
        }
        return out;
    }

    /** The reader of a verdict's caller's message: the last shape's side, read through {@code read} (the program's
     *  reader) and shaped by the plan -- only where the judgment asks for it, the assert having failed. */
    static java.util.function.Supplier<TypedSide> message(List<com.legend.testexecutionplan.SideShape> shapes,
            java.util.function.Supplier<PlanRunner.Rows> read) {
        com.legend.testexecutionplan.SideShape last = shapes.get(shapes.size() - 1);
        return () -> com.legend.testexec.SideShapes.shape(last, read.get());
    }
}
