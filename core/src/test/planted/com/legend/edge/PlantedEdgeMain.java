// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * THE PLANTED ARTIFACTS' MAIN CLASS (the PR #118 follow-up, 2026-11-19, R11(b), HOSTS-7): a main class in the edge's
 * package for the two planted binaries the shipped-artifact root check must find ({@code //core:planted_edge_rooted_artifact},
 * {@code //core:planted_opt_in_carrier}; ShippedArtifactRootsTest.theRootCheckBites). The edge has one door, the
 * product server's mount ({@code --engine legend-engine}): {@code EdgeServer} declares no main of its own, so the plants
 * name this one. The query reads the attribute; nothing runs it.
 */
final class PlantedEdgeMain {

    private PlantedEdgeMain() {
    }

    public static void main(String[] args) {
        throw new UnsupportedOperationException("a planted artifact's main class: never run");
    }
}
