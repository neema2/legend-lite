// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Protocol;

/**
 * A {@code ###Lakehouse} Deployment as the model holds it (docs/DEPLOYMENT_DESIGN_2026_11_14.md §8): what one
 * environment lands, from which sources, on which targets, kept as the protocol record the parser or the reader made,
 * its names full paths once resolved. Nothing in the model points at it; {@code planner.DeploymentPlanner} reads it.
 */
public record DeploymentDefinition(String qualifiedName, Protocol.PDeployment deployment) implements PackageableElement {
}
