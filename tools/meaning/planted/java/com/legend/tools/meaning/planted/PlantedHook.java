// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning.planted;

/** PLANTED (census B2): a two-form hook. Its product form (PlantedProductForm) answers a constant frame and ignores the
 *  words it is handed; its edge form (planted.edge.PlantedEdgeForm) computes both. */
public interface PlantedHook {

    /** The frame a union carries: none in the product, upstream's name at the edge. */
    String frame();

    /** A raise's text: the product's own sentence; upstream's words at the edge. */
    String raise(String kind, String upstreamWords);
}
