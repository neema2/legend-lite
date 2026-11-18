// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * Planted for {@code EdgeOnlyMembersTest} (J40 of the edge audit, 2026-11-18): a "product" class whose members the edge
 * alone uses ({@code edge.PlantedEdgeOnlyCaller}), beside members the class uses too, an override, a member nothing
 * uses, and a hook's implementer. Read as bytecode alone; never called by a test.
 */
@SuppressWarnings("unused")
public final class PlantedEdgeOnlyApi {

    /** Read by the edge alone, in source (inlined, so no bytecode reads it). */
    public static final String EDGE_CONSTANT = "e";

    /** Read by this class too. */
    public static final String SHARED_CONSTANT = "s";

    /** Written by this class, read by the edge alone. */
    public String edgeField = "f";

    /** Made by the edge alone. */
    public PlantedEdgeOnlyApi(String s) {
    }

    /** Made by this class too ({@link #shared}). */
    public PlantedEdgeOnlyApi() {
    }

    /** Called by the edge alone. */
    public static String edgeOnly() {
        return "e";
    }

    /** Called by the edge and by this class. */
    public static String shared() {
        return new PlantedEdgeOnlyApi() + SHARED_CONSTANT;
    }

    /** Called by nobody. */
    public static String unused() {
        return "u";
    }

    /** An override the edge alone calls: reached by dispatch. */
    @Override
    public String toString() {
        return "p";
    }

    String own() {
        return shared();
    }

    /** A hook type (the bite hands it as one). */
    public interface Hook {
        String hooked();
    }

    /** A hook's implementer: its members are the hook's, whoever calls them. */
    public static final class Hooked implements Hook {
        @Override
        public String hooked() {
            return "h";
        }

        public static String also() {
            return "a";
        }
    }
}
