// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning.planted;

/** PLANTED (census B2, M3): the product's form -- a constant answer (null), and a parameter never read. */
public final class PlantedProductForm implements PlantedHook {

    @Override
    public String frame() {
        return null;
    }

    @Override
    public String raise(String kind, String upstreamWords) {
        return "the run failed: " + kind;
    }
}
