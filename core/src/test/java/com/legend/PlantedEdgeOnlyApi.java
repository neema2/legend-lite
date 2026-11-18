// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * Planted for {@code EdgeOnlyMembersTest} (J40 of the edge audit, 2026-11-18): a "product" class whose members the edge
 * alone uses ({@code edge.PlantedEdgeOnlyCaller}) -- directly, through an edge-only member or its lambda, or by itself
 * besides -- beside members the class uses too, an override, a member nothing uses, a cycle no edge use enters, an
 * enum's values() and a hook's implementer. Read as bytecode alone; never called by a test.
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
        java.util.function.Supplier<String> later = () -> viaLambda();
        return "e" + viaEdgeOnly() + later.get();
    }

    /** Called by {@link #edgeOnly} alone: edge-only by transitivity (the orchestrator's ruling of 2026-11-18, (4)). */
    public static String viaEdgeOnly() {
        return "v";
    }

    /** Called inside {@link #edgeOnly}'s lambda alone: that lambda's uses are its enclosing method's. */
    public static String viaLambda() {
        return "l";
    }

    /** Called by the edge and by itself: a use of a member by itself is neither the edge's nor the product's. */
    public static int countdown(int n) {
        return n <= 0 ? 0 : countdown(n - 1);
    }

    /** A cycle of product members no edge use enters: nobody's, never counted. */
    public static int ping(int n) {
        return n <= 0 ? 0 : pong(n - 1);
    }

    /** The cycle's other half. */
    public static int pong(int n) {
        return n <= 0 ? 0 : ping(n - 1);
    }

    /** Called by nothing, and not public: dead code (or a test's), whose use of {@link #viaDead} is nobody's. */
    static String dead() {
        return viaDead();
    }

    /** Called by nothing in the jars, and public: an entry point, whose use of {@link #viaPublicDead} is the product's. */
    public static String publicDead() {
        return viaPublicDead();
    }

    /** Called by the edge and by a public method nothing calls: the product's (a public method is an entry point). */
    public static String viaPublicDead() {
        return "p";
    }

    /** Called by the edge and by dead non-public code alone: edge-only. */
    public static String viaDead() {
        return "d";
    }

    /** An entry point a host calls: its use of {@link #viaMain} is the product's. */
    public static void main(String[] args) {
        viaMain();
    }

    /** Called by the edge and by {@link #main}: the product's too. */
    public static String viaMain() {
        return "m";
    }

    /** An enum whose compiler-written values() the edge alone calls: never counted (the ruling's (3)). */
    public enum Kind {
        ONE
    }

    /** Read when the class loads: a static initializer is the product's, so {@link #shared} is the product's too. */
    private static final String LOADED = shared();

    /** Called by the edge and by this class's initializer. */
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
