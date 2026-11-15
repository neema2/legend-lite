// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Protocol;

/**
 * A {@code ###DataProduct} DataProduct as the model holds it (docs/DEPLOYMENT_DESIGN_2026_11_14.md §7, step 2): its
 * access point groups -- a model group's mapping, each access point's function -- kept as the protocol record the parser
 * or the reader made, its names full paths once resolved. A deployment deploys it ({@code planner.DeploymentPlanner});
 * a data space's {@code mappingProvider} names it and a model group ({@link #mappingOf}).
 */
public record DataProductDefinition(String qualifiedName, Protocol.PDataProduct dataProduct) implements PackageableElement {

    /** The mapping of the model access group {@code group}, or null when the product has no such group. */
    public @com.legend.base.Nullable String mappingOf(String group) {
        for (Protocol.PAccessPointGroup g : dataProduct.accessPointGroups()) {
            if (g.id().equals(group)) {
                return g.mapping();
            }
        }
        return null;
    }
}
