// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning.planted.edge;

import com.legend.tools.meaning.planted.PlantedHook;

/** PLANTED (census B2, M3): the edge's form -- upstream's frame name, upstream's words. */
public final class PlantedEdgeForm implements PlantedHook {

    private final String alias;

    public PlantedEdgeForm(String alias) {
        this.alias = alias;
    }

    @Override
    public String frame() {
        return alias.isEmpty() ? "unionAlias" : alias;
    }

    @Override
    public String raise(String kind, String upstreamWords) {
        return upstreamWords + " (" + kind + ")";
    }
}
