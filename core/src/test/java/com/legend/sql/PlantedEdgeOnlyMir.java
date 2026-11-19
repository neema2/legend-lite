// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

/**
 * L-B's planted shapes (EdgeOnlyDataTest.theGuardBites, 2026-11-18): J28's -- a MIR variant only the edge makes -- and
 * J31's -- a component and a constant only the edge reads and names, on a node the product makes with its own kind and
 * reads by its own fields. Read as bytecode alone; nothing runs it.
 */
public final class PlantedEdgeOnlyMir {

    private PlantedEdgeOnlyMir() {
    }

    /** The planted sealed root. */
    public sealed interface Node permits Made, Shared {
    }

    /** J28's shape: a variant the edge alone constructs. */
    public record Made(String text) implements Node {
    }

    /** J31's shape: a template field and a constant only the edge reads and names. */
    public record Shared(String name, String template, Kind kind) implements Node {
    }

    /** A rewriter's two reads (the hole the orchestrator's question found, 2026-11-18, train 18): {@code flag} read to
     *  DECIDE (a product use), {@code copied} read only to rebuild the record (a copy, so no use: "by none"). */
    public record Ruled(String flag, String copied) {
    }

    /** The planted kind: PLAIN the product's, RAW the edge's. */
    public enum Kind { PLAIN, RAW }

    /** The product's own use: it makes a Shared of its own kind and reads its name and kind. */
    public static String productUse() {
        Shared s = new Shared("n", null, Kind.PLAIN);
        return s.name() + s.kind();
    }
}
